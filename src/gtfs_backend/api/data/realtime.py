"""The worker's live data in Redis (layout: gtfs_backend.live_layout), as the services read it."""

from __future__ import annotations

from dataclasses import dataclass

import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout


async def get_json_many(redis: Redis, keys: list[str]) -> list[dict | list | None]:
    if not keys:
        return []
    return [orjson.loads(v) if v else None for v in await redis.mget(keys)]


async def vehicle_json(redis: Redis, ids: list[str]) -> list[dict]:
    """Vehicles by vehicle key, skipping ones that have expired."""
    return [v for v in await get_json_many(redis, [live_layout.vehicle(i) for i in ids]) if v]


async def vehicles_on_map(redis: Redis, feed_ids: list[str]) -> list[dict]:
    """The map fields of every live vehicle of these feeds (live_layout.vehicles_map)."""
    return [v for feed in await get_json_many(redis, [live_layout.vehicles_map(f) for f in feed_ids]) if feed for v in feed]


async def alerts_json(redis: Redis, feed_ids: list[str]) -> list[dict]:
    return [a for feed in await get_json_many(redis, [live_layout.alerts(f) for f in feed_ids]) if feed for a in feed]


async def meta_json(redis: Redis, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict | None]:
    """Poll stats per (feed_id, kind)."""
    values = await get_json_many(redis, [live_layout.meta(f, k) for f, k in keys])
    return dict(zip(keys, values, strict=True))


def newest(timestamps) -> int | None:
    """The time of the newest realtime data shown, or None when nothing shown is realtime. Responses carry this,
    not the time of the request, so a poll gets a 304 while the data hasn't changed."""
    return max((t for t in timestamps if t is not None), default=None)


# ------------------------------------------------------------------------------------------------- trip updates


@dataclass(frozen=True)
class StopUpdate:
    predicted_arrival: int | None
    predicted_departure: int | None
    delay_s: int | None
    skipped: bool

    @property
    def predicted(self) -> int | None:
        """When the vehicle leaves (or arrives, where only that is known)."""
        return self.predicted_departure or self.predicted_arrival


class TripUpdate:
    """A trip's realtime state, used only for the timetable version it was computed for."""

    def __init__(self, raw: dict):
        self.vehicle_id: str | None = raw.get("vehicle_id")
        self.timestamp: int | None = raw.get("timestamp")  # when the feed measured it
        self.delay_s: int | None = raw.get("delay_s")
        self.canceled = raw.get("schedule_relationship") == "CANCELED"
        # a board needs one stop of each trip: rows are turned into StopUpdates only when asked for
        self._rows = {r["stop_sequence"]: r for r in raw.get("stops") or []}
        self._stops: dict[int, StopUpdate] = {}

    @classmethod
    def for_version(cls, raw: dict | None, version: int) -> TripUpdate | None:
        """None without an update, or when it is for another version (stop sequences differ between versions)."""
        return cls(raw) if raw is not None and raw.get("version") == version else None

    def stop(self, sequence: int) -> StopUpdate | None:
        if (done := self._stops.get(sequence)) is not None:
            return done
        r = self._rows.get(sequence)
        if r is None:
            return None
        self._stops[sequence] = update = StopUpdate(
            r.get("predicted_arrival"),
            r.get("predicted_departure"),
            r.get("delay_s"),
            r.get("schedule_relationship") == "SKIPPED",
        )
        return update


async def trip_updates(redis: Redis, keys: list[str]) -> dict[str, dict]:
    """trip key -> raw trip update JSON, for the keys that have one."""
    unique = list(dict.fromkeys(keys))
    values = await get_json_many(redis, [live_layout.trip(k) for k in unique])
    return {k: v for k, v in zip(unique, values, strict=True) if v}
