"""Writes the live data into Redis (layout: gtfs_backend.live_layout)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import msgspec
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


class RealtimeStore:
    """Each poll writes the whole of what its file carries, one value per feed, and returns what changed since the last
    one: the change message the API follows (publish)."""

    def __init__(self, redis: Redis, ttls: TTLs):
        self.redis = redis
        self.ttls = ttls
        # per feed_id, what the last write said, to tell what changed: each vehicle's JSON, each trip as merged (the
        # next update is merged into it) and its TripState's JSON
        self._vehicles: dict[str, dict[str, str]] = {}
        self._trips: dict[str, dict[str, LiveTrip]] = {}
        self._states: dict[str, dict[str, bytes]] = {}
        self.run = uuid.uuid4().hex[:12]  # this worker process: change messages count from 1 in each
        self._seq: dict[tuple[str, str], int] = {}

    async def write_vehicles(self, feed_id: str, res: Result) -> dict:
        """Write the feed's vehicles; returns what changed ({"set": {vehicle key: LiveVehicle}, "del": [vehicle key]})."""
        vehicles = {keys.vehicle_key(feed_id, v["vehicle_id"]): v for v in res.vehicles}
        await self.redis.set(live_layout.vehicles(feed_id), _dumps(vehicles), ex=self.ttls.vehicle)
        last = self._vehicles.get(feed_id, {})
        values = {key: _dumps(v) for key, v in vehicles.items()}
        self._vehicles[feed_id] = values
        return {
            "set": {key: vehicles[key] for key, value in values.items() if last.get(key) != value},
            "del": sorted(last.keys() - values.keys()),
        }

    async def write_trips(self, feed_id: str, res: Result) -> dict:
        """Write the feed's trips; returns what changed ({"set": {trip key: TripState}, "del": [trip key]})."""
        if feed_id not in self._trips:  # the first update since the worker started: the last one is in Redis only
            stored = await self.redis.get(live_layout.trips(feed_id))
            self._trips[feed_id] = orjson.loads(stored) if stored else {}
        last = self._trips[feed_id]
        # the feed drops stops it is past: their last times are kept from the previous update
        merged = {trip["key"]: merge_stops(last.get(trip["key"]), trip) for trip in res.trips}
        # remembered only once written: a failed write is retried with the same update (the poller doesn't mark it seen)
        await self.redis.set(live_layout.trips(feed_id), _dumps(merged), ex=self.ttls.trip)
        self._trips[feed_id] = merged
        last_states = self._states.get(feed_id, {})
        states = {key: live_layout.trip_state(trip) for key, trip in merged.items()}
        encoded = {key: msgspec.json.encode(state) for key, state in states.items()}
        self._states[feed_id] = encoded
        return {
            "set": {key: states[key] for key, value in encoded.items() if last_states.get(key) != value},
            "del": sorted(last.keys() - merged.keys()),
        }

    async def write_alerts(self, feed_id: str, res: Result) -> live_layout.LiveAlerts:
        """Write the feed's alerts; returns them (all of them: a change message carries the whole value)."""
        value: live_layout.LiveAlerts = {"timestamp": res.timestamp, "alerts": res.alerts}
        await self.redis.set(live_layout.alerts(feed_id), _dumps(value), ex=self.ttls.alert)
        return value

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
            pipe.expire(live_layout.vehicles(feed_id), self.ttls.vehicle)
        if kind.carries(RealtimeKind.TRIP_UPDATES):
            pipe.expire(live_layout.trips(feed_id), self.ttls.trip)
        if kind.carries(RealtimeKind.SERVICE_ALERTS):
            pipe.expire(live_layout.alerts(feed_id), self.ttls.alert)
        await pipe.execute()

    async def write_meta(self, feed_id: str, kind: str, meta: live_layout.PollMeta) -> None:
        await self.redis.set(live_layout.meta(feed_id, kind), _dumps(meta))

    def lost(self, feed_id: str, kind: str) -> None:
        """Part of a poll was written but its changes won't be published (a later write failed): the next message skips
        a number, so readers load the feed from Redis instead of missing what was written."""
        self._seq[(feed_id, kind)] = self._seq.get((feed_id, kind), 0) + 1

    async def publish(self, feed_id: str, kind: str, ts: int, changes: dict | None = None) -> None:
        """What one poll of a file changed (live_layout.LiveChanges), numbered per file: on rt:updated, and its number in
        rt:seq so a reader loading from Redis knows which messages it has. The number counts even if this fails, so
        the next message tells readers they missed one."""
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
        pipe.publish(live_layout.REALTIME_UPDATED, msgspec.json.encode(message))  # TripStates as arrays
        await pipe.execute()
