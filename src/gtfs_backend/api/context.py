"""What every service works with: connections, settings, feed config, the catalog and the clock."""

from __future__ import annotations

from dataclasses import dataclass

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

    @property
    def feed_ids(self) -> list[str]:
        return [f.feed_id for f in self.feeds.enabled]

    def now(self) -> int:
        return int(self.clock())
