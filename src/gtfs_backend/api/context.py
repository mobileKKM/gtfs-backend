"""What every service works with: connections, settings, feed config, the catalog and the clock."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from gtfs_backend.api.data.catalog import Catalog, Clock
from gtfs_backend.api.settings import ApiSettings
from gtfs_backend.feeds import FeedsFile


@dataclass
class Context:
    pool: AsyncConnectionPool
    redis: Redis  # decode_responses=True
    feeds: FeedsFile
    catalog: Catalog
    clock: Clock
    settings: ApiSettings
    # response cache key -> the computation in flight in this process (api/cache.py)
    inflight: dict[str, asyncio.Future] = field(default_factory=dict)
    # "{feed_id}:{kind}" -> feed timestamp of the newest realtime data in Redis, kept by the `realtime:updated` listener
    # (app.py); live responses are cached under it, so new data is never answered from the cache
    realtime_gen: dict[str, int] = field(default_factory=dict)
    realtime_listening: bool = False  # whether realtime_gen is followed now (else live responses use time buckets)

    @property
    def feed_ids(self) -> list[str]:
        return [f.feed_id for f in self.feeds.enabled]

    def now(self) -> int:
        return int(self.clock())

    def realtime_generation(self, feed_ids) -> str:
        """Changes whenever realtime data of one of these feeds does."""
        wanted = set(feed_ids)
        return "|".join(f"{k}={v}" for k, v in sorted(self.realtime_gen.items()) if k.split(":", 1)[0] in wanted)
