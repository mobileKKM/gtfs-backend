"""What every service works with: connections, settings, feed config, the catalog and the clock."""

from __future__ import annotations

from dataclasses import dataclass, field

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from gtfs_backend.api.data.catalog import Catalog, Clock
from gtfs_backend.api.data.live import LiveState
from gtfs_backend.api.errors import LiveUnavailable
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
    live_state: LiveState | None = None  # the live data in memory (api/data/live.py)
    _max_age: dict[str, int] = field(init=False, repr=False)

    def __post_init__(self):
        self._max_age = {f.feed_id: self.settings.max_age(f) for f in self.feeds.feeds}

    @property
    def feed_ids(self) -> list[str]:
        return [f.feed_id for f in self.feeds.enabled]

    def now(self) -> int:
        return int(self.clock())

    @property
    def live(self) -> LiveState:
        """The live data, once loaded; else 503 (LiveUnavailable)."""
        if self.live_state is None or not self.live_state.ready:
            raise LiveUnavailable
        return self.live_state

    def fresh_since(self) -> dict[str, int]:
        """Per feed, the oldest realtime measurement time (epoch) still shown: now - the feed's realtime max age."""
        now = self.now()
        return {feed_id: now - age for feed_id, age in self._max_age.items()}

    def fresh(self, feed_id: str, timestamp: int | None, since: dict[str, int] | None = None) -> bool:
        """Whether realtime measured at `timestamp` is still shown. Trip updates, vehicles and alerts measured longer
        ago are ignored, so nothing stale stays when the worker or a feed stops (the worker gives each one a time: the
        feed header's where it has none of its own)."""
        if timestamp is None:
            return True
        since = since if since is not None else self.fresh_since()
        return timestamp >= since.get(feed_id, timestamp)
