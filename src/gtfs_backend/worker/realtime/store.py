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


class RealtimeStore:
    def __init__(self, redis: Redis, feed_ids: list[str], ttls: TTLs):
        self.redis = redis
        self.ttls = ttls
        self.feed_ids = feed_ids  # all feeds, for the combined GEO set
        # per feed_id, what its last update wrote: trips (as merged, by trip key) and vehicle keys. The worker is the
        # only writer, so the last trips are taken from here instead of being read back (only after a restart).
        self._trips: dict[str, dict[str, LiveTrip]] = {}
        self._trip_keys: dict[str, set[str]] = {}  # the trip keys in Redis (incl. ones written before a restart)
        self._vehicle_keys: dict[str, list[str]] = {}

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
        if members:
            pipe.geoadd(tmp, members)
            pipe.rename(tmp, geo_key)
            pipe.expire(geo_key, self.ttls.vehicle)
        else:
            pipe.delete(geo_key)
        pipe.zunionstore(live_layout.VEHICLES_GEO, [live_layout.vehicles_geo(k) for k in self.feed_ids])
        pipe.expire(live_layout.VEHICLES_GEO, self.ttls.vehicle)
        await pipe.execute()
        self._vehicle_keys[feed_id] = [live_layout.vehicle(m) for m in members[2::3]]

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
        # no MULTI: readers want each trip whole, not all of them at once, and a transaction would hold them back
        # for the whole write (MBs for the big feeds)
        pipe = self.redis.pipeline(transaction=False)
        for key, trip in merged.items():
            pipe.set(live_layout.trip(key), _dumps(trip), ex=self.ttls.trip)
        # FULL_DATASET: trips missing from this update are gone
        if stale := self._trip_keys[feed_id] - current:
            pipe.delete(*(live_layout.trip(key) for key in stale))
        await pipe.execute()
        # only once written: a failed write is retried with the same update (the poller doesn't mark it as seen)
        self._trip_keys[feed_id], self._trips[feed_id] = current, merged

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
        if kind.carries(RealtimeKind.SERVICE_ALERTS):
            pipe.expire(live_layout.alerts(feed_id), self.ttls.alert)
        await pipe.execute()

    async def write_meta(self, feed_id: str, kind: str, meta: live_layout.PollMeta) -> None:
        await self.redis.set(live_layout.meta(feed_id, kind), _dumps(meta))

    async def publish(self, feed_id: str, kind: str, ts: int) -> None:
        await self.redis.publish(live_layout.REALTIME_UPDATED, _dumps({"feed_id": feed_id, "kind": kind, "ts": ts}))
