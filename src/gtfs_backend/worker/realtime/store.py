"""Writes the live data into Redis (layout: gtfs_backend.live_layout)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import orjson
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
    """Compact JSON, UTF-8 as is (json.dumps(value, ensure_ascii=False, separators=(",", ":")), a few times faster)."""
    return orjson.dumps(value).decode()


def _trip_states(trips: dict[str, LiveTrip]) -> dict[str, str]:
    """rt:tripstate contents of a feed's trips: trip key -> TripState JSON."""
    return {key: _dumps(live_layout.trip_state(trip)) for key, trip in trips.items()}


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
        # per feed_id, rt:tripstate as last written. Missing: unknown (after a start or a failed write): cleared, with
        # the rt:stop:* hashes older workers wrote, and written in full next time.
        self._states: dict[str, dict[str, str]] = {}  # trip key -> TripState JSON
        # per feed_id, what the change messages (rt:updated) last said of each vehicle / trip, so they carry only changes
        self._vehicle_values: dict[str, dict[str, str]] = {}  # vehicle key -> LiveVehicle JSON
        self._compact: dict[str, dict[str, bytes]] = {}  # trip key -> CompactTrip JSON
        self.run = uuid.uuid4().hex[:12]  # this worker process: change messages count from 1 in each
        self._seq: dict[tuple[str, str], int] = {}

    async def write_vehicles(self, feed_id: str, res: Result) -> dict:
        """Write the feed's vehicles; returns what changed ({"set": {vehicle key: LiveVehicle}, "del": [vehicle key]})."""
        geo_key = live_layout.vehicles_geo(feed_id)
        tmp = f"{geo_key}:tmp"
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(tmp)
        members = []
        values: dict[str, str] = {}
        last_values = self._vehicle_values.get(feed_id, {})
        changed = {}
        for v in res.vehicles:
            member = keys.vehicle_key(feed_id, v["vehicle_id"])
            members.extend([v["lon"], v["lat"], member])
            values[member] = value = _dumps(v)
            pipe.set(live_layout.vehicle(member), value, ex=self.ttls.vehicle)
            if last_values.get(member) != value:
                changed[member] = v
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
        self._vehicle_values[feed_id] = values
        return {"set": changed, "del": sorted(last_values.keys() - values.keys())}

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
        self._trips[feed_id] = {k: orjson.loads(v) for k, v in zip(wanted, values, strict=True) if v}

    async def write_trips(self, feed_id: str, res: Result) -> dict:
        """Write the feed's trips; returns what changed ({"set": {trip key: CompactTrip}, "del": [trip key]})."""
        current = {trip["key"] for trip in res.trips}
        if feed_id not in self._trip_keys:
            await self._restore_trips(feed_id, current)
        # the feed drops stops it is past: their last times are kept from the previous update
        last = self._trips[feed_id]
        merged = {trip["key"]: merge_stops(last.get(trip["key"]), trip) for trip in res.trips}
        states = _trip_states(merged)
        state_key = live_layout.trip_states(feed_id)
        # rt:tripstate gone although written (it expired in an outage or with a file older than its TTL, or Redis lost
        # its data): rewritten as after a start
        if self._states.get(feed_id) and not await self.redis.exists(state_key):
            self._states.pop(feed_id)
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
        try:
            await pipe.execute()
        except BaseException:
            # some commands may have run: diffing against the last good write could miss them, so start over
            self._states.pop(feed_id, None)
            raise
        # only once written: a failed write is retried with the same update (the poller doesn't mark it as seen)
        self._trip_keys[feed_id], self._trips[feed_id] = current, merged
        self._states[feed_id] = states
        last_compact = self._compact.get(feed_id, {})
        compact = {key: live_layout.compact_trip(trip) for key, trip in merged.items()}
        encoded = {key: orjson.dumps(c) for key, c in compact.items()}
        self._compact[feed_id] = encoded
        return {
            "set": {key: compact[key] for key, value in encoded.items() if last_compact.get(key) != value},
            "del": sorted(stale),
        }

    async def _stored_index_keys(self, feed_id: str) -> list[str]:
        """rt:tripstate of this feed as written before a restart (or a failed write), and the rt:stop:* hashes older
        workers wrote (nothing reads them any more)."""
        keys = [live_layout.trip_states(feed_id)]
        async for key in self.redis.scan_iter(match=f"rt:stop:{feed_id}:*", count=1000):
            keys.append(key.decode() if isinstance(key, bytes) else key)
        return keys

    async def write_alerts(self, feed_id: str, res: Result) -> list:
        """Write the feed's alerts; returns them (all: a change message carries the whole list)."""
        await self.redis.set(live_layout.alerts(feed_id), _dumps(res.alerts), ex=self.ttls.alert)
        return res.alerts

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

    async def publish(self, feed_id: str, kind: str, ts: int, changes: dict | None = None) -> None:
        """What one poll of a file changed (live_layout.LiveChanges), numbered per file: on rt:updated, and its number in
        rt:seq so a reader loading from Redis knows which messages it has."""
        seq = self._seq[(feed_id, kind)] = self._seq.get((feed_id, kind), 0) + 1
        message: live_layout.LiveChanges = {
            "run": self.run,
            "feed_id": feed_id,
            "kind": kind,
            "seq": seq,
            "ts": ts,
            **(changes or {}),
        }
        pipe = self.redis.pipeline(transaction=False)
        pipe.set(live_layout.seq(feed_id, kind), f"{self.run}:{seq}")
        pipe.publish(live_layout.REALTIME_UPDATED, orjson.dumps(message))
        await pipe.execute()
