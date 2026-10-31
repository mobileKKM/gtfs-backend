"""The worker's live data in Redis (layout: gtfs_backend.live_layout), as the services read it."""

from __future__ import annotations

import json
from dataclasses import dataclass

from redis.asyncio import Redis

from gtfs_backend import live_layout


async def get_json_many(redis: Redis, keys: list[str]) -> list[dict | list | None]:
    if not keys:
        return []
    return [json.loads(v) if v else None for v in await redis.mget(keys)]


async def vehicle_json(redis: Redis, ids: list[str]) -> list[dict]:
    """Vehicles by vehicle key, skipping ones that have expired."""
    return [v for v in await get_json_many(redis, [live_layout.vehicle(i) for i in ids]) if v]


async def vehicles_near(redis: Redis, lat: float, lon: float, width_km: float, height_km: float) -> list[str]:
    """Vehicle keys in a box around a point."""
    return await redis.geosearch(
        live_layout.VEHICLES_GEO, longitude=lon, latitude=lat, width=width_km, height=height_km, unit="km"
    )


async def alerts_json(redis: Redis, feed_ids: list[str]) -> list[dict]:
    return [a for feed in await get_json_many(redis, [live_layout.alerts(f) for f in feed_ids]) if feed for a in feed]


async def meta_json(redis: Redis, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict | None]:
    """Poll stats per (feed_id, kind)."""
    values = await get_json_many(redis, [live_layout.meta(f, k) for f, k in keys])
    return dict(zip(keys, values, strict=True))


# ------------------------------------------------------------------------------------------------- trip updates


@dataclass(frozen=True)
class StopUpdate:
    predicted_arrival: int | None
    predicted_departure: int | None
    delay_s: int | None
    skipped: bool
    passed: bool

    @property
    def predicted(self) -> int | None:
        """When the vehicle leaves (or arrives, where only that is known)."""
        return self.predicted_departure or self.predicted_arrival


class TripUpdate:
    """A trip's realtime state, used only for the timetable version it was computed for."""

    def __init__(self, raw: dict):
        self.vehicle_id: str | None = raw.get("vehicle_id")
        self.delay_s: int | None = raw.get("delay_s")
        self.canceled = raw.get("schedule_relationship") == "CANCELED"
        self._stops = {
            r["stop_sequence"]: StopUpdate(
                r.get("predicted_arrival"),
                r.get("predicted_departure"),
                r.get("delay_s"),
                r.get("schedule_relationship") == "SKIPPED",
                bool(r.get("passed")),
            )
            for r in raw.get("stops") or []
        }
        self._first = min(self._stops, default=None)

    @classmethod
    def for_version(cls, raw: dict | None, version: int) -> TripUpdate | None:
        """None without an update, or when it is for another version (stop sequences differ between versions)."""
        return cls(raw) if raw is not None and raw.get("version") == version else None

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)

    def passed(self, sequence: int) -> bool:
        """The feed drops passed stops; one before everything it (or we) still know about is passed too."""
        row = self._stops.get(sequence)
        if row is not None:
            return row.passed
        return self._first is not None and sequence < self._first


async def trip_updates(redis: Redis, keys: list[str]) -> dict[str, dict]:
    """trip key -> raw trip update JSON, for the keys that have one."""
    unique = list(dict.fromkeys(keys))
    values = await get_json_many(redis, [live_layout.trip(k) for k in unique])
    return {k: v for k, v in zip(unique, values, strict=True) if v}
