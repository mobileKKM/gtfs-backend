"""Per-feed realtime state, shared by the feed's realtime files: its trip index and its normalization state."""

from __future__ import annotations

import asyncio
import time

from psycopg_pool import AsyncConnectionPool

from gtfs_backend.feeds import FeedConfig
from gtfs_backend.logs import log
from gtfs_backend.servicetime import local_date
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState
from gtfs_backend.worker.realtime.trip_index import TripIndex, ensure_stop_times, load_index


class FeedRuntime:
    """The trip index (reloaded after static updates and on day change) and the recent trip delays and vehicle fixes."""

    def __init__(self, feed: FeedConfig, pool: AsyncConnectionPool, state: FeedRealtimeState | None = None):
        self.feed = feed
        self.pool = pool
        self.state = state or FeedRealtimeState(ignore_service_suffix=feed.ignore_service_suffix)
        self.index: TripIndex | None = None
        self._dirty = True
        self._lock = asyncio.Lock()

    @property
    def feed_id(self) -> str:
        return self.feed.feed_id

    def invalidate(self) -> None:
        self._dirty = True

    async def get_index(self, now: float) -> TripIndex | None:
        async with self._lock:
            stale = self.index is None or self.index.loaded_for != local_date(now, self.index.tz)
            if self._dirty or stale:
                started = time.monotonic()
                self.index = await load_index(self.pool, self.feed_id, now, self.feed.day_start)
                self._dirty = False
                if self.index:
                    log.info(
                        "trip_index_loaded",
                        feed=self.feed_id,
                        trips=len(self.index.trips),
                        days=[(d.date.isoformat(), d.version) for d in self.index.days],
                        seconds=round(time.monotonic() - started, 1),
                    )
            return self.index

    async def load_stop_times(self, index: TripIndex, keys: set[tuple[int, str]]) -> None:
        """The stop times of these (version, trip_id)s, into `index` (the one the caller works with)."""
        await ensure_stop_times(self.pool, index, keys)
