"""Writes the live data into Redis (layout: gtfs_backend.live_layout)."""

from __future__ import annotations

from dataclasses import dataclass

import orjson
from redis.asyncio import Redis

from gtfs_backend import keys, live_layout
from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.worker.realtime.normalize import Result, merge_stops


@dataclass(frozen=True)
class TTLs:
    """Seconds each kind of live data stays in Redis without a newer poll (WorkerSettings *_TTL)."""

    vehicle: int
    trip: int
    alert: int
    unmatched: int


def map_entry(feed_id: str, v: dict) -> dict:
    """The fields of a vehicle's JSON the map shows (api/services/vehicles.py), under the same names."""
    trip = v.get("trip") or {}
    return {
        "feed_id": feed_id,
        "vehicle_id": v["vehicle_id"],
        "label": v.get("label"),
        "lat": v["lat"],
        "lon": v["lon"],
        "bearing": v.get("bearing"),
        "delay_s": v.get("delay_s"),
        "current_stop_sequence": v.get("current_stop_sequence"),
        "current_status": v.get("current_status"),
        "timestamp": v.get("timestamp"),
        "trip": {k: trip.get(k) for k in ("key", "version", "route_id", "headsign")},
    }


def _dumps(value) -> bytes:
    return orjson.dumps(value)  # compact UTF-8, as json.dumps(ensure_ascii=False, separators=(",", ":")), several times faster


class RealtimeStore:
    def __init__(self, redis: Redis, feed_ids: list[str], ttls: TTLs):
        self.redis = redis
        self.ttls = ttls
        self.feed_ids = feed_ids
        self._trip_keys: dict[str, set[str]] = {}  # per feed_id: trip keys written by the last update
        self._vehicle_keys: dict[str, list[str]] = {}  # per feed_id: vehicle keys written by the last update

    async def write_vehicles(self, feed_id: str, res: Result) -> None:
        pipe = self.redis.pipeline(transaction=True)
        written = []
        for v in res.vehicles:
            key = live_layout.vehicle(keys.vehicle_key(feed_id, v["vehicle_id"]))
            written.append(key)
            pipe.set(key, _dumps(v), ex=self.ttls.vehicle)
        entries = [map_entry(feed_id, v) for v in res.vehicles]
        pipe.set(live_layout.vehicles_map(feed_id), _dumps(entries), ex=self.ttls.vehicle)
        await pipe.execute()
        self._vehicle_keys[feed_id] = written

    async def _stored_trip_keys(self, feed_id: str) -> set[str]:
        """Trip keys of this feed already in Redis (written before a restart)."""
        prefix = live_layout.trip("")
        found = set()
        async for key in self.redis.scan_iter(match=live_layout.trip(f"{feed_id}:*"), count=1000):
            found.add((key.decode() if isinstance(key, bytes) else key).removeprefix(prefix))
        return found

    async def write_trips(self, feed_id: str, res: Result) -> None:
        if feed_id not in self._trip_keys:  # first update since the worker started: earlier trips count as written
            self._trip_keys[feed_id] = await self._stored_trip_keys(feed_id)
        current = {trip["key"] for trip in res.trips}
        # the feed drops stops it is past; read the previous JSON back so their last times (and restarts) survive
        prevs = await self.redis.mget([live_layout.trip(trip["key"]) for trip in res.trips]) if res.trips else []
        pipe = self.redis.pipeline(transaction=True)
        for trip, prev in zip(res.trips, prevs, strict=True):
            merged = merge_stops(orjson.loads(prev) if prev else None, trip)
            pipe.set(live_layout.trip(trip["key"]), _dumps(merged), ex=self.ttls.trip)
        # FULL_DATASET: trips missing from this update are gone
        stale = self._trip_keys.get(feed_id, set()) - current
        if stale:
            pipe.delete(*(live_layout.trip(key) for key in stale))
        await pipe.execute()
        self._trip_keys[feed_id] = current

    async def write_alerts(self, feed_id: str, res: Result) -> None:
        await self.redis.set(live_layout.alerts(feed_id), _dumps(res.alerts), ex=self.ttls.alert)

    async def write_unmatched(self, feed_id: str, kind: str, res: Result) -> None:
        if res.unmatched_trips or res.unmatched_vehicles or res.unmatched_alert_entities:
            await self.redis.set(
                live_layout.unmatched(feed_id, kind),
                _dumps(
                    {
                        "trips": res.unmatched_trips,
                        "vehicles": res.unmatched_vehicles,
                        "alert_entities": res.unmatched_alert_entities,
                    }
                ),
                ex=self.ttls.unmatched,
            )

    async def touch(self, feed_id: str, kind: RealtimeKind) -> None:
        """Refresh the TTLs of what the last update of this file wrote (the file hasn't changed since)."""
        combined = kind == RealtimeKind.COMBINED
        pipe = self.redis.pipeline(transaction=False)
        if combined or kind == RealtimeKind.VEHICLE_POSITIONS:
            for key in self._vehicle_keys.get(feed_id, []):
                pipe.expire(key, self.ttls.vehicle)
            pipe.expire(live_layout.vehicles_map(feed_id), self.ttls.vehicle)
        if combined or kind == RealtimeKind.TRIP_UPDATES:
            for key in self._trip_keys.get(feed_id, set()):
                pipe.expire(live_layout.trip(key), self.ttls.trip)
        if combined or kind == RealtimeKind.SERVICE_ALERTS:
            pipe.expire(live_layout.alerts(feed_id), self.ttls.alert)
        await pipe.execute()

    async def write_meta(self, feed_id: str, kind: str, meta: dict) -> None:
        await self.redis.set(live_layout.meta(feed_id, kind), _dumps(meta))

    async def publish(self, feed_id: str, kind: str, ts: int) -> None:
        await self.redis.publish(live_layout.REALTIME_UPDATED, _dumps({"feed_id": feed_id, "kind": kind, "ts": ts}))
