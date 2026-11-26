"""The live data in this API process's memory: vehicles, trips with their per-stop predictions, alerts. Requests read
it, never Redis; it follows the worker's change messages (rt:updated, live_layout.LiveChanges).

Each process loads every feed from Redis when it starts, then applies the messages. A feed is loaded again when a
message of it is missing (its `seq` jumps) or comes from another worker process (`run`), and every feed when the
subscription was lost. What the worker stops updating isn't removed here: the freshness rule (Context.fresh) hides
trip updates, vehicles and alerts measured longer ago than the feed's max age.

Listeners (the WebSocket hub) are told what each applied message touched (Changed).
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

import msgspec
import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout
from gtfs_backend.live_layout import LiveAlerts, LiveVehicle, StopState, TripState
from gtfs_backend.logs import log

GRID_DEG = 0.01  # vehicle grid cells for box queries: ~1.1 km x 0.7 km at 50 N
RETRY_S = 2  # after a lost subscription or a failed load


def cell(lat: float, lon: float) -> tuple[int, int]:
    return math.floor(lat / GRID_DEG), math.floor(lon / GRID_DEG)


def feed_of(key: str) -> str:
    """The feed of a vehicle or trip key."""
    return key.split(":", 1)[0]


@dataclass
class Changed:
    """What an applied message touched: vehicles, trips (changed ones, and those of changed vehicles), the stops of
    those trips (before and after), alerts. `everything`: a feed was loaded again."""

    vehicles: set[str] = field(default_factory=set)
    trips: set[str] = field(default_factory=set)
    stops: set[str] = field(default_factory=set)
    alerts: bool = False
    everything: bool = False


class LiveState:
    def __init__(self, redis: Redis, feed_ids: list[str], kinds: dict[str, list[str]]):
        """`kinds`: each feed's realtime files (RealtimeKind values), whose sequence numbers are followed."""
        self.redis = redis
        self.feed_ids = feed_ids
        self.kinds = kinds
        self.ready = False  # every feed loaded once
        self.vehicles: dict[str, LiveVehicle] = {}  # vehicle key -> its LiveVehicle
        # services/vehicles.py's model and JSON of each vehicle, dropped whenever the vehicle changes
        self.vehicle_pieces: dict[str, object] = {}
        self.trips: dict[str, TripState] = {}
        self.alerts: dict[str, LiveAlerts] = {}
        self._grid: dict[tuple[int, int], set[str]] = defaultdict(set)  # cell -> vehicle keys
        self._at_stop: dict[str, set[tuple[str, int]]] = defaultdict(set)  # public_stop_id -> (trip key, stop index)
        self._seq: dict[tuple[str, str], tuple[str, int]] = {}  # (feed, kind) -> (run, seq) last applied
        self.listeners: list[Callable[[Changed], None]] = []
        self._task: asyncio.Task | None = None
        self._loaded = asyncio.Event()

    # ------------------------------------------------------------------------------------------- reading

    def vehicles_in(self, bbox) -> list[tuple[str, LiveVehicle]]:
        """(key, vehicle) of the vehicles in the grid cells a box (geo.Bbox) touches, by key: cut exactly by callers."""
        lo, hi = cell(bbox.min_lat, bbox.min_lon), cell(bbox.max_lat, bbox.max_lon)
        found = []
        for la in range(lo[0], hi[0] + 1):
            for lo_ in range(lo[1], hi[1] + 1):
                found.extend(self._grid.get((la, lo_), ()))
        return [(k, self.vehicles[k]) for k in sorted(found)]

    def vehicle(self, key: str) -> LiveVehicle | None:
        return self.vehicles.get(key)

    def alerts_of(self, feed_ids: list[str], fresh_since: dict[str, int]) -> list[dict]:
        """The alerts of these feeds whose file isn't older than its max age (Context.fresh_since)."""
        out = []
        for f in feed_ids:
            value = self.alerts.get(f)
            if value is not None and value["timestamp"] >= fresh_since.get(f, value["timestamp"]):
                out += value["alerts"]
        return out

    def board(self, feed_ids: list[str], public_stop_ids: list[str], fresh_since: dict[str, int]) -> LiveBoard:
        return LiveBoard(self, set(feed_ids), public_stop_ids, fresh_since)

    # ------------------------------------------------------------------------------------------- changing

    def _set_vehicle(self, key: str, v: LiveVehicle, changed: Changed) -> None:
        self._drop_vehicle(key, changed)
        self.vehicles[key] = v
        self._grid[cell(v["lat"], v["lon"])].add(key)
        changed.vehicles.add(key)
        changed.trips.add(v["trip"]["key"])

    def _drop_vehicle(self, key: str, changed: Changed) -> None:
        self.vehicle_pieces.pop(key, None)
        v = self.vehicles.pop(key, None)
        if v is None:
            return
        c = cell(v["lat"], v["lon"])
        self._grid[c].discard(key)
        if not self._grid[c]:
            del self._grid[c]
        changed.vehicles.add(key)
        changed.trips.add(v["trip"]["key"])

    def _set_trip(self, key: str, trip: TripState, changed: Changed) -> None:
        self._drop_trip(key, changed)
        self.trips[key] = trip
        for i, stop in enumerate(trip.stops):
            if stop.public_stop_id is not None:
                self._at_stop[stop.public_stop_id].add((key, i))
        changed.trips.add(key)

    def _drop_trip(self, key: str, changed: Changed) -> None:
        trip = self.trips.pop(key, None)
        if trip is None:
            return
        for i, stop in enumerate(trip.stops):
            pid = stop.public_stop_id
            if pid is not None and (entries := self._at_stop.get(pid)) is not None:
                entries.discard((key, i))
                if not entries:
                    del self._at_stop[pid]
                changed.stops.add(pid)
        changed.trips.add(key)

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
        changed = Changed()
        if (vehicles := message.get("vehicles")) is not None:
            for key in vehicles.get("del", ()):
                self._drop_vehicle(key, changed)
            for key, v in vehicles.get("set", {}).items():
                self._set_vehicle(key, v, changed)
        if (trips := message.get("trips")) is not None:
            for key in trips.get("del", ()):
                self._drop_trip(key, changed)
            for key, trip in trips.get("set", {}).items():
                self._set_trip(key, msgspec.convert(trip, TripState), changed)
        if (alerts := message.get("alerts")) is not None:
            self.alerts[feed] = alerts
            changed.alerts = True
        for key in changed.trips:  # a vehicle shows on its trip's boards too
            if (trip := self.trips.get(key)) is not None:
                changed.stops.update(s.public_stop_id for s in trip.stops if s.public_stop_id is not None)
        self._tell(changed)
        return True

    def _tell(self, changed: Changed) -> None:
        for listener in self.listeners:
            listener(changed)

    async def load_feed(self, feed_id: str) -> None:
        """Everything of a feed from Redis, replacing what was here, and the message numbers it includes (one MGET:
        Redis answers it at one moment)."""
        kinds = self.kinds.get(feed_id, [])
        *seqs, vehicles, trips, alerts = await self.redis.mget(
            [*(live_layout.seq(feed_id, k) for k in kinds), live_layout.vehicles(feed_id), live_layout.trips(feed_id),
             live_layout.alerts(feed_id)]
        )  # fmt: skip
        changed = Changed(everything=True)
        for key in [k for k in self.vehicles if feed_of(k) == feed_id]:
            self._drop_vehicle(key, changed)
        for key, v in (orjson.loads(vehicles) if vehicles else {}).items():
            self._set_vehicle(key, v, changed)
        for key in [k for k in self.trips if feed_of(k) == feed_id]:
            self._drop_trip(key, changed)
        for key, trip in (orjson.loads(trips) if trips else {}).items():
            self._set_trip(key, live_layout.trip_state(trip), changed)
        if alerts:
            self.alerts[feed_id] = orjson.loads(alerts)
        else:
            self.alerts.pop(feed_id, None)
        for kind, value in zip(kinds, seqs, strict=True):
            if value:
                run, _, n = value.rpartition(":")
                self._seq[(feed_id, kind)] = (run, int(n))
            else:
                self._seq.pop((feed_id, kind), None)
        self._tell(changed)

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
                        if message["feed_id"] in self.feed_ids and not self.apply(message):
                            log.info("live_feed_reload", feed=message["feed_id"], kind=message["kind"], seq=message["seq"])
                            await self.load_feed(message["feed_id"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("live_follow_failed", error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(RETRY_S)


class LiveBoard:
    """The live state as one board reads it: the trips of its feeds measured recently enough, and their predictions
    at the board's stops. Taken when the board starts: messages applied while it awaits the timetable don't mix in."""

    def __init__(self, live: LiveState, feed_ids: set[str], public_stop_ids: list[str], fresh_since: dict[str, int]):
        self._live = live
        self._feeds = feed_ids
        self._fresh_since = fresh_since
        self._trips: dict[str, TripState] = {}  # the trips at these stops, as they were
        self._stops: dict[tuple[str, int], StopState] = {}  # (trip key, stop_sequence) -> its stop, at these stops
        for pid in public_stop_ids:
            for key, i in live._at_stop.get(pid, ()):
                if (trip := self.trip(key)) is not None:
                    self._trips[key] = trip
                    self._stops[(key, trip.stops[i].stop_sequence)] = trip.stops[i]

    def trip(self, trip_key: str) -> TripState | None:
        """The trip's state, if it is of the board's feeds and fresh (also one without stops here: a canceled trip)."""
        trip = self._trips.get(trip_key) or self._live.trips.get(trip_key)
        feed_id = feed_of(trip_key)
        if trip is None or feed_id not in self._feeds:
            return None
        if trip.timestamp is not None and trip.timestamp < self._fresh_since.get(feed_id, trip.timestamp):
            return None
        return trip

    def stop(self, trip_key: str, stop_sequence: int) -> StopState | None:
        return self._stops.get((trip_key, stop_sequence))

    def extra_departures(self, lo: int, hi: int) -> list[tuple[str, StopState, TripState]]:
        """(trip key, stop, trip) of the trips no timetable has, departing from the board's stops between lo and hi
        (epochs): the rows a board takes from realtime."""
        out = []
        for (key, _), stop in self._stops.items():
            trip = self._trips[key]
            if trip.extra is not None and stop.scheduled_departure is not None and lo <= stop.scheduled_departure <= hi:
                out.append((key, stop, trip))
        return out

    def earliest_early(self, after: int, since: int) -> int | None:
        """The earliest prediction, from `since` on, of a trip scheduled at one of the board's stops after `after`; None
        without one."""
        times = [
            stop.predicted
            for stop in self._stops.values()
            if stop.predicted is not None
            and stop.scheduled_departure is not None
            and stop.scheduled_departure > after
            and stop.predicted >= since
        ]
        return min(times, default=None)
