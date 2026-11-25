"""Writes the live data into Redis (layout: gtfs_backend.live_layout)."""

from __future__ import annotations

import json
from dataclasses import dataclass

from redis.asyncio import Redis

from gtfs_backend import keys, live_layout
from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.live_layout import LiveTrip
from gtfs_backend.worker.realtime.normalize import Result
from gtfs_backend.worker.realtime.trip_updates import merge_stops


@dataclass(frozen=True)
class TTLs:
    """Seconds each kind of live data stays in Redis without a newer poll (WorkerSettings *_TTL)."""

    vehicle: int
    trip: int
    alert: int
    unmatched: int


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _int_json(value: int | None) -> str:
    return "null" if value is None else str(int(value))


def _prediction_json(p: live_layout.StopPrediction) -> str:
    """json.dumps of a StopPrediction (ints, None, a bool), written directly: thousands per poll, a third of the
    write's time with json.dumps."""
    arrival, departure, delay, skipped, version, scheduled = p
    flag = "true" if skipped else "false"
    times = f"{_int_json(arrival)},{_int_json(departure)},{_int_json(delay)}"
    return f"[{times},{flag},{_int_json(version)},{_int_json(scheduled)}]"


def _departure_index(trips: dict[str, LiveTrip]) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """rt:tripstate and rt:stop:* contents of a feed's trips: (trip key -> state, public_stop_id -> field -> prediction)."""
    states = {key: _dumps(live_layout.trip_state(trip)) for key, trip in trips.items()}
    stops: dict[str, dict[str, str]] = {}
    for key, trip in trips.items():
        for row in trip["stops"]:
            if (pid := row["public_stop_id"]) is not None:
                field = live_layout.stop_prediction_field(row["stop_sequence"], key)
                stops.setdefault(pid, {})[field] = _prediction_json(live_layout.stop_prediction(row, trip["version"]))
    return states, stops


def _write_hash(pipe, key: str, last: dict[str, str], new: dict[str, str], *, full: bool = False) -> None:
    """Make the hash at `key` (last written as `last`) hold `new`: only changed fields are sent, or all (`full`)."""
    if not new:
        if last:
            pipe.delete(key)
        return
    if changed := (new if full else {f: v for f, v in new.items() if last.get(f) != v}):
        pipe.hset(key, mapping=changed)
    if gone := last.keys() - new.keys():
        pipe.hdel(key, *gone)


class RealtimeStore:
    def __init__(self, redis: Redis, feed_ids: list[str], ttls: TTLs):
        self.redis = redis
        self.ttls = ttls
        self.feed_ids = feed_ids  # all feeds, for the combined GEO set
        # per feed_id, what its last update wrote: trips (as merged, by trip key) and vehicle keys. The worker is the
        # only writer, so the last trips are taken from here instead of being read back (only after a restart).
        self._trips: dict[str, dict[str, LiveTrip]] = {}
        self._trip_keys: dict[str, set[str]] = {}  # the trip keys in Redis (incl. ones written before a restart)
        self._vehicle_keys: dict[str, set[str]] = {}
        # per feed_id, the departure index as last written (rt:tripstate, rt:stop:*), so a write sends only changes.
        # Missing: unknown (after a start or a failed write), cleared and written in full next time.
        self._states: dict[str, dict[str, str]] = {}  # trip key -> TripState JSON
        self._stop_fields: dict[str, dict[str, dict[str, str]]] = {}  # public_stop_id -> field -> StopPrediction JSON

    async def write_vehicles(self, feed_id: str, res: Result) -> None:
        geo_key = live_layout.vehicles_geo(feed_id)
        tmp = f"{geo_key}:tmp"
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(tmp)
        members = []
        for v in res.vehicles:
            member = keys.vehicle_key(feed_id, v["vehicle_id"])
            members.extend([v["lon"], v["lat"], member])
            pipe.set(live_layout.vehicle(member), _dumps(v), ex=self.ttls.vehicle)
        current = {live_layout.vehicle(m) for m in members[2::3]}
        # vehicles that left the feed (after a restart: unknown, they expire by TTL)
        if gone := self._vehicle_keys.get(feed_id, set()) - current:
            pipe.delete(*gone)
        if members:
            pipe.geoadd(tmp, members)
            pipe.rename(tmp, geo_key)
            pipe.expire(geo_key, self.ttls.vehicle)
        else:
            pipe.delete(geo_key)
        pipe.zunionstore(live_layout.VEHICLES_GEO, [live_layout.vehicles_geo(k) for k in self.feed_ids])
        pipe.expire(live_layout.VEHICLES_GEO, self.ttls.vehicle)
        await pipe.execute()
        self._vehicle_keys[feed_id] = current

    async def _stored_trip_keys(self, feed_id: str) -> set[str]:
        """Trip keys of this feed already in Redis (written before a restart)."""
        prefix = live_layout.trip("")
        found = set()
        async for key in self.redis.scan_iter(match=live_layout.trip(f"{feed_id}:*"), count=1000):
            found.add((key.decode() if isinstance(key, bytes) else key).removeprefix(prefix))
        return found

    async def _restore_trips(self, feed_id: str, current: set[str]) -> None:
        """First update since the worker started: the trips it wrote before are in Redis only."""
        stored = await self._stored_trip_keys(feed_id)
        wanted = sorted(stored & current)
        values = await self.redis.mget([live_layout.trip(k) for k in wanted]) if wanted else []
        self._trip_keys[feed_id] = stored
        self._trips[feed_id] = {k: json.loads(v) for k, v in zip(wanted, values, strict=True) if v}

    async def write_trips(self, feed_id: str, res: Result) -> None:
        current = {trip["key"] for trip in res.trips}
        if feed_id not in self._trip_keys:
            await self._restore_trips(feed_id, current)
        # the feed drops stops it is past: their last times are kept from the previous update
        last = self._trips[feed_id]
        merged = {trip["key"]: merge_stops(last.get(trip["key"]), trip) for trip in res.trips}
        states, stop_fields = _departure_index(merged)
        state_key = live_layout.trip_states(feed_id)
        # rt:tripstate gone although written (it expired in an outage or with a file older than its TTL, or Redis lost
        # its data): the rt:stop hashes may be gone too, so the index is rewritten as after a start
        if self._states.get(feed_id) and not await self.redis.exists(state_key):
            self._states.pop(feed_id)
            self._stop_fields.pop(feed_id, None)
        known = feed_id in self._states
        old_index = [] if known else await self._stored_index_keys(feed_id)
        # no MULTI: readers want each trip whole, not all of them at once, and a transaction would hold them back
        # for the whole write (MBs for the big feeds)
        pipe = self.redis.pipeline(transaction=False)
        for key, trip in merged.items():
            pipe.set(live_layout.trip(key), _dumps(trip), ex=self.ttls.trip)
        # FULL_DATASET: trips missing from this update are gone
        if stale := self._trip_keys[feed_id] - current:
            pipe.delete(*(live_layout.trip(key) for key in stale))
        if old_index:
            pipe.delete(*old_index)
        # trip states first: a board ignores predictions of trips without one. All of them, not only changed ones:
        # a few hundred, and the hash can expire after the check above (a canceled trip's state can stay the same for
        # hours, and would be missing until it changed)
        _write_hash(pipe, state_key, self._states.get(feed_id, {}), states, full=True)
        pipe.expire(state_key, self.ttls.trip)
        last = self._stop_fields.get(feed_id, {})
        for pid in last.keys() | stop_fields.keys():
            _write_hash(pipe, live_layout.stop_predictions(feed_id, pid), last.get(pid, {}), stop_fields.get(pid, {}))
        try:
            await pipe.execute()
        except BaseException:
            # some commands may have run: diffing against the last good write could miss them, so start over
            self._states.pop(feed_id, None)
            self._stop_fields.pop(feed_id, None)
            raise
        # only once written: a failed write is retried with the same update (the poller doesn't mark it as seen)
        self._trip_keys[feed_id], self._trips[feed_id] = current, merged
        self._states[feed_id], self._stop_fields[feed_id] = states, stop_fields

    async def _stored_index_keys(self, feed_id: str) -> list[str]:
        """The departure index of this feed in Redis, as written before a restart (or a failed write)."""
        keys = [live_layout.trip_states(feed_id)]
        async for key in self.redis.scan_iter(match=live_layout.stop_predictions(feed_id, "*"), count=1000):
            keys.append(key.decode() if isinstance(key, bytes) else key)
        return keys

    async def write_alerts(self, feed_id: str, res: Result) -> None:
        await self.redis.set(live_layout.alerts(feed_id), _dumps(res.alerts), ex=self.ttls.alert)

    async def write_unmatched(self, feed_id: str, kind: str, res: Result) -> None:
        if res.unmatched_trips or res.unmatched_vehicles or res.unmatched_alert_entities:
            value: live_layout.Unmatched = {
                "trips": res.unmatched_trips,
                "vehicles": res.unmatched_vehicles,
                "alert_entities": res.unmatched_alert_entities,
            }
            await self.redis.set(live_layout.unmatched(feed_id, kind), _dumps(value), ex=self.ttls.unmatched)

    async def touch(self, feed_id: str, kind: RealtimeKind) -> None:
        """Refresh the TTLs of what the last update of this file wrote (the file hasn't changed since)."""
        pipe = self.redis.pipeline(transaction=False)
        if kind.carries(RealtimeKind.VEHICLE_POSITIONS):
            for key in self._vehicle_keys.get(feed_id, []):
                pipe.expire(key, self.ttls.vehicle)
            pipe.expire(live_layout.vehicles_geo(feed_id), self.ttls.vehicle)
            pipe.expire(live_layout.VEHICLES_GEO, self.ttls.vehicle)
        if kind.carries(RealtimeKind.TRIP_UPDATES):
            for key in self._trip_keys.get(feed_id, set()):
                pipe.expire(live_layout.trip(key), self.ttls.trip)
            pipe.expire(live_layout.trip_states(feed_id), self.ttls.trip)
        if kind.carries(RealtimeKind.SERVICE_ALERTS):
            pipe.expire(live_layout.alerts(feed_id), self.ttls.alert)
        await pipe.execute()

    async def write_meta(self, feed_id: str, kind: str, meta: live_layout.PollMeta) -> None:
        await self.redis.set(live_layout.meta(feed_id, kind), _dumps(meta))

    async def publish(self, feed_id: str, kind: str, ts: int) -> None:
        await self.redis.publish(live_layout.REALTIME_UPDATED, _dumps({"feed_id": feed_id, "kind": kind, "ts": ts}))
