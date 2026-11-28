"""What the worker does to Redis, by the layout (gtfs_backend/live_layout.py), for API tests that play the worker's
part: whole values per feed, and numbered change messages of what changed since the last write."""

from __future__ import annotations

import msgspec
import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout


class LiveWriter:
    def __init__(self, redis: Redis, run: str = "test"):
        self.redis = redis
        self.run = run
        self.seq: dict[tuple[str, str], int] = {}
        self._vehicles: dict[str, dict[str, bytes]] = {}
        self._trips: dict[str, dict[str, bytes]] = {}

    async def write_vehicles(self, feed_id: str, vehicles: list[dict]) -> dict:
        """rt:vehicles:{feed_id}; returns {"set": {key: vehicle}, "del": [key]} against the last write."""
        by_key = {f"{feed_id}:{v['vehicle_id']}": v for v in vehicles}
        await self.redis.set(live_layout.vehicles(feed_id), orjson.dumps(by_key), ex=120)
        last, now = self._vehicles.get(feed_id, {}), {k: orjson.dumps(v) for k, v in by_key.items()}
        self._vehicles[feed_id] = now
        return {"set": {k: by_key[k] for k, v in now.items() if last.get(k) != v}, "del": sorted(last.keys() - now.keys())}

    async def write_trips(self, feed_id: str, trips: list[dict]) -> dict:
        """rt:trips:{feed_id} (LiveTrips); returns {"set": {key: TripState}, "del": [key]} against the last write."""
        by_key = {t["key"]: t for t in trips}
        await self.redis.set(live_layout.trips(feed_id), orjson.dumps(by_key), ex=600)
        states = {k: live_layout.trip_state(t) for k, t in by_key.items()}
        last, now = self._trips.get(feed_id, {}), {k: msgspec.json.encode(s) for k, s in states.items()}
        self._trips[feed_id] = now
        return {"set": {k: states[k] for k, v in now.items() if last.get(k) != v}, "del": sorted(last.keys() - now.keys())}

    async def publish(self, feed_id: str, kind: str, ts: int, changes: dict) -> None:
        seq = self.seq[(feed_id, kind)] = self.seq.get((feed_id, kind), 0) + 1
        message = {"run": self.run, "feed_id": feed_id, "kind": kind, "seq": seq, "ts": ts, **changes}
        await self.redis.set(live_layout.seq(feed_id, kind), f"{self.run}:{seq}")
        await self.redis.publish(live_layout.REALTIME_UPDATED, msgspec.json.encode(message))
