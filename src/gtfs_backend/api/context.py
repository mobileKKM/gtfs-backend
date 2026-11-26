"""What every service works with: connections, settings, feed config, the catalog and the clock."""

from __future__ import annotations

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
    _max_age: dict[str, int] = field(init=False, repr=False)
    # what services keep between requests of this process, built from data that doesn't change while kept (the JSON
    # of a vehicle per Redis value, ...): never a response
    memo: dict = field(default_factory=dict, repr=False)

    def __post_init__(self):
        self._max_age = {f.feed_id: self.settings.max_age(f) for f in self.feeds.feeds}

    @property
    def feed_ids(self) -> list[str]:
        return [f.feed_id for f in self.feeds.enabled]

    def now(self) -> int:
        return int(self.clock())

    def fresh_since(self) -> dict[str, int]:
        """Per feed, the oldest realtime measurement time (epoch) still shown: now - the feed's realtime max age."""
        now = self.now()
        return {feed_id: now - age for feed_id, age in self._max_age.items()}

    def fresh(self, feed_id: str, timestamp: int | None, since: dict[str, int] | None = None) -> bool:
        """Whether realtime measured at `timestamp` is still shown (no timestamp: it is, the TTLs decide). Trip
        updates and vehicles measured longer ago are ignored, so nothing stale stays when the worker or a feed stops."""
        if timestamp is None:
            return True
        since = since if since is not None else self.fresh_since()
        return timestamp >= since.get(feed_id, timestamp)
