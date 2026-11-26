"""The live data in this API process's memory: vehicles, trips with their per-stop predictions, alerts. Requests read
it instead of Redis; it follows the worker's change messages (rt:updated, live_layout.LiveChanges).

Each process loads every feed from Redis when it starts, then applies the messages. A feed is loaded again when a
message of it is missing (its `seq` jumps) or comes from another worker process (`run`), and every feed when the
subscription was lost. What the worker stops updating isn't removed here: the freshness rule (Context.fresh) hides
trip updates and vehicles measured longer ago than the feed's max age, as it did for Redis.

Listeners (the WebSocket hub) get each applied message after it was applied.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections import defaultdict
from collections.abc import Callable

import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout
from gtfs_backend.api.data.realtime import StopUpdate, TripState, TripUpdate
from gtfs_backend.logs import log

GRID_DEG = 0.01  # vehicle grid cells for box queries: ~1.1 km x 0.7 km at 50 N
RETRY_S = 2  # after a lost subscription or a failed load

# CompactTrip fields (live_layout.compact_trip)
VERSION, CANCELED, VEHICLE, TIMESTAMP, DELETED, DELAY, STOPS = range(7)
# StopEntry fields
SEQ, PID, ARR, DEP, SDELAY, SKIPPED, SCHED, PROPAGATED = range(8)


def cell(lat: float, lon: float) -> tuple[int, int]:
    return math.floor(lat / GRID_DEG), math.floor(lon / GRID_DEG)


class LiveState:
    def __init__(self, redis: Redis, feed_ids: list[str], kinds: dict[str, list[str]]):
        """`kinds`: each feed's realtime files (RealtimeKind values), whose sequence numbers are followed."""
        self.redis = redis
        self.feed_ids = feed_ids
        self.kinds = kinds
        self.ready = False  # every feed loaded once
        self.vehicles: dict[str, tuple[str, dict]] = {}  # vehicle key -> (LiveVehicle JSON, decoded)
        self._grid: dict[tuple[int, int], set[str]] = defaultdict(set)
        self._cell_of: dict[str, tuple[int, int]] = {}
        self.trips: dict[str, list] = {}  # trip key -> CompactTrip
        self._at_stop: dict[str, set[tuple[str, int]]] = defaultdict(set)  # public_stop_id -> (trip key, entry index)
        self.alerts: dict[str, list[dict]] = {}  # feed_id -> LiveAlert list
        self._seq: dict[tuple[str, str], tuple[str, int]] = {}  # (feed, kind) -> (run, seq) last applied
        self.listeners: list[Callable[[dict], None]] = []
        self._task: asyncio.Task | None = None
        self._loaded = asyncio.Event()

    # ------------------------------------------------------------------------------------------- reading

    def vehicle_values(self, bbox) -> list[str]:
        """JSON values of the vehicles inside a box (geo.Bbox), by vehicle key."""
        lo, hi = cell(bbox.min_lat, bbox.min_lon), cell(bbox.max_lat, bbox.max_lon)
        found = []
        for la in range(lo[0], hi[0] + 1):
            for lo_ in range(lo[1], hi[1] + 1):
                found.extend(self._grid.get((la, lo_), ()))
        return [self.vehicles[k][0] for k in sorted(found)]

    def vehicle(self, key: str) -> dict | None:
        entry = self.vehicles.get(key)
        return entry[1] if entry else None

    def vehicle_value(self, key: str) -> str | None:
        entry = self.vehicles.get(key)
        return entry[0] if entry else None

    def trip_update(self, trip_key: str) -> TripUpdate | None:
        trip = self.trips.get(trip_key)
        if trip is None:
            return None
        stops = {e[SEQ]: StopUpdate(e[ARR], e[DEP], e[SDELAY], bool(e[SKIPPED]), bool(e[PROPAGATED])) for e in trip[STOPS]}
        return TripUpdate.of(trip[VERSION], trip[CANCELED], trip[DELETED], trip[VEHICLE], trip[TIMESTAMP], trip[DELAY], stops)

    def alerts_of(self, feed_ids: list[str]) -> list[dict]:
        return [a for f in feed_ids for a in self.alerts.get(f, ())]

    def board(self, feed_ids: list[str], public_stop_ids: list[str], fresh_since: dict[str, int]) -> LiveBoard:
        return LiveBoard(self, set(feed_ids), public_stop_ids, fresh_since)

    # ------------------------------------------------------------------------------------------- changing

    def _set_vehicle(self, key: str, value: str, v: dict) -> None:
        self._drop_vehicle(key)
        self.vehicles[key] = (value, v)
        c = self._cell_of[key] = cell(v["lat"], v["lon"])
        self._grid[c].add(key)

    def _drop_vehicle(self, key: str) -> None:
        if self.vehicles.pop(key, None) is not None:
            c = self._cell_of.pop(key)
            self._grid[c].discard(key)
            if not self._grid[c]:
                del self._grid[c]

    def _set_trip(self, key: str, trip: list) -> None:
        self._drop_trip(key)
        self.trips[key] = trip
        for i, e in enumerate(trip[STOPS]):
            if e[PID] is not None:
                self._at_stop[e[PID]].add((key, i))

    def _drop_trip(self, key: str) -> None:
        trip = self.trips.pop(key, None)
        if trip is None:
            return
        for i, e in enumerate(trip[STOPS]):
            if e[PID] is not None and (entries := self._at_stop.get(e[PID])) is not None:
                entries.discard((key, i))
                if not entries:
                    del self._at_stop[e[PID]]

    def apply(self, message: dict) -> bool:
        """Apply one change message; False if it can't be (a gap or another worker run): the feed needs loading."""
        feed, kind, run, seq = message["feed_id"], message["kind"], message["run"], message["seq"]
        last = self._seq.get((feed, kind))
        if last is not None and last[0] == run and seq <= last[1]:
            return True  # already in what was loaded
        # no number known (rt:seq was missing when the feed was loaded): this message, newer than the load, starts it
        if last is not None and (last[0] != run or seq != last[1] + 1):
            return False
        self._seq[(feed, kind)] = (run, seq)
        if (vehicles := message.get("vehicles")) is not None:
            for key in vehicles.get("del", ()):
                self._drop_vehicle(key)
            for key, v in vehicles.get("set", {}).items():
                self._set_vehicle(key, orjson.dumps(v).decode(), v)  # the worker's own JSON (both orjson)
        if (trips := message.get("trips")) is not None:
            for key in trips.get("del", ()):
                self._drop_trip(key)
            for key, trip in trips.get("set", {}).items():
                self._set_trip(key, trip)
        if (alerts := message.get("alerts")) is not None:
            self.alerts[feed] = alerts
        for listener in self.listeners:
            listener(message)
        return True

    async def load_feed(self, feed_id: str) -> None:
        """Everything of a feed from Redis, replacing what was here, and the message numbers it includes."""
        r = self.redis
        seqs = await r.mget([live_layout.seq(feed_id, k) for k in self.kinds.get(feed_id, [])])
        members = await r.zrange(live_layout.vehicles_geo(feed_id), 0, -1)
        values = await r.mget([live_layout.vehicle(m) for m in members]) if members else []
        trip_keys = list(await r.hkeys(live_layout.trip_states(feed_id)))
        trips = await r.mget([live_layout.trip(k) for k in trip_keys]) if trip_keys else []
        alerts = await r.get(live_layout.alerts(feed_id))
        prefix = f"{feed_id}:"
        for key in [k for k in self.vehicles if k.startswith(prefix)]:
            self._drop_vehicle(key)
        for key, value in zip(members, values, strict=True):
            if value:
                self._set_vehicle(key, value, orjson.loads(value))
        for key in [k for k in self.trips if k.startswith(prefix)]:
            self._drop_trip(key)
        for key, value in zip(trip_keys, trips, strict=True):
            if value:
                self._set_trip(key, live_layout.compact_trip(orjson.loads(value)))
        self.alerts[feed_id] = orjson.loads(alerts) if alerts else []
        for kind, value in zip(self.kinds.get(feed_id, []), seqs, strict=True):
            if value:
                run, _, n = value.rpartition(":")
                self._seq[(feed_id, kind)] = (run, int(n))
            else:
                self._seq.pop((feed_id, kind), None)
        for listener in self.listeners:
            listener({"feed_id": feed_id, "loaded": True})

    async def load(self) -> None:
        for feed_id in self.feed_ids:
            await self.load_feed(feed_id)
        self.ready = True
        self._loaded.set()

    async def wait_ready(self, timeout: float) -> bool:
        """Wait up to `timeout` for the first load; whether it is done."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._loaded.wait(), timeout)
        return self.ready

    # ------------------------------------------------------------------------------------------- following

    def start(self) -> None:
        self._task = asyncio.create_task(self._follow())

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _follow(self) -> None:
        """Subscribe, load everything, then apply messages; a gap loads that feed again, a lost subscription all."""
        while True:
            try:
                async with self.redis.pubsub() as ps:
                    await ps.subscribe(live_layout.REALTIME_UPDATED)
                    await self.load()  # after subscribing: nothing between the load and the first message is lost
                    async for msg in ps.listen():
                        if msg["type"] != "message":
                            continue
                        message = orjson.loads(msg["data"])
                        if "run" not in message:  # an older worker's notification, without the changes
                            continue
                        if message.get("feed_id") in self.feed_ids and not self.apply(message):
                            log.info(
                                "live_feed_reload", feed=message["feed_id"], kind=message.get("kind"), seq=message.get("seq")
                            )
                            await self.load_feed(message["feed_id"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("live_follow_failed", error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(RETRY_S)


class LiveBoard:
    """A board's realtime from the live state, read like data/realtime.BoardRealtime (which read it from Redis): the
    trip states with their predictions at the board's stops, and trips running early."""

    def __init__(self, live: LiveState, feed_ids: set[str], public_stop_ids: list[str], fresh_since: dict[str, int]):
        self._live = live
        self._feeds = feed_ids
        self._fresh_since = fresh_since
        self._entries: dict[str, list[int]] = defaultdict(list)  # trip key -> its entry indexes at the board's stops
        for pid in public_stop_ids:
            for key, i in live._at_stop.get(pid, ()):
                if key.split(":", 1)[0] in feed_ids:
                    self._entries[key].append(i)
        self._decoded: dict[str, TripState | None] = {}

    def state(self, trip_key: str) -> TripState | None:
        if trip_key in self._decoded:
            return self._decoded[trip_key]
        trip = self._live.trips.get(trip_key)
        feed_id = trip_key.split(":", 1)[0]
        state = None
        if trip is not None and feed_id in self._feeds:
            ts = trip[TIMESTAMP]
            if ts is None or ts >= self._fresh_since.get(feed_id, ts):
                state = TripState(trip[VERSION], trip[CANCELED], trip[VEHICLE], ts, trip[DELETED])
                for i in self._entries.get(trip_key, ()):
                    e = trip[STOPS][i]
                    state._stops[e[SEQ]] = StopUpdate(e[ARR], e[DEP], e[SDELAY], bool(e[SKIPPED]), bool(e[PROPAGATED]))
        self._decoded[trip_key] = state
        return state

    def states(self, trip_keys) -> dict[str, TripState]:
        return {k: state for k in trip_keys if (state := self.state(k)) is not None}

    def earliest_early(self, after: int, since: int) -> int | None:
        """As BoardRealtime.earliest_early: the earliest prediction, from `since` on, of a trip scheduled at one of the
        board's stops after `after`; None without one."""
        best = None
        for key, indexes in self._entries.items():
            trip = self._live.trips.get(key)
            if trip is None or self.state(key) is None:
                continue
            for i in indexes:
                e = trip[STOPS][i]
                predicted = e[DEP] or e[ARR]
                if predicted is not None and e[SCHED] is not None and e[SCHED] > after and predicted >= since:
                    best = predicted if best is None else min(best, predicted)
        return best
