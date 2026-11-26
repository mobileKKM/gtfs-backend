"""The poll stats /health reads from Redis (layout: gtfs_backend.live_layout), and the timestamp responses carry. The
live data itself is in memory: api/data/live.py."""

from __future__ import annotations

import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout


async def get_json_many(redis: Redis, keys: list[str]) -> list[dict | list | None]:
    if not keys:
        return []
    return [orjson.loads(v) if v else None for v in await redis.mget(keys)]


async def meta_json(redis: Redis, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict | None]:
    """Poll stats per (feed_id, kind)."""
    values = await get_json_many(redis, [live_layout.meta(f, k) for f, k in keys])
    return dict(zip(keys, values, strict=True))


def newest(timestamps) -> int | None:
    """The time of the newest realtime data shown, or None when nothing shown is realtime. Responses carry this,
    not the time of the request, so a poll gets a 304 while the data hasn't changed."""
    return max((t for t in timestamps if t is not None), default=None)
