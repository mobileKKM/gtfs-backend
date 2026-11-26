"""Per-feed realtime state, shared by the feed's realtime files: its trip index and its normalization state.

The index is reloaded after static updates and on a new service day. A reload runs in the background while the
previous index keeps serving: it covers yesterday/today/tomorrow, so it still resolves every running trip for a while,
and a failing database never stops the live data. Only the very first load has nothing to serve and is waited for.
"""

from __future__ import annotations

import asyncio
import time

from psycopg_pool import AsyncConnectionPool

from gtfs_backend.feeds import FeedConfig
from gtfs_backend.logs import log
from gtfs_backend.servicetime import local_date
from gtfs_backend.worker.realtime.extra import KINDS
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState
from gtfs_backend.worker.realtime.trip_index import TripIndex, ensure_stop_times, load_index

RELOAD_TIMEOUT_S = 120  # a reload taking longer is given up and retried (normal: ~0.3 s for the largest feed)
RELOAD_RETRY_S = 10  # after a failed reload
STOP_TIMES_TIMEOUT_S = 3  # loading stop times holds up the poll; on failure the update goes out without them
STOP_TIMES_RETRY_S = 30  # and the database isn't asked again for this long


class IndexUnavailable(Exception):
    """No index could be loaded yet."""


def _describe(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__  # TimeoutError has no message


class FeedRuntime:
    """The trip index and the recent trip delays and vehicle fixes of one feed."""

    def __init__(
        self, feed: FeedConfig, pool: AsyncConnectionPool, state: FeedRealtimeState | None = None, max_age: int | None = None
    ):
        """`max_age`: the feed's realtime max age (vehicles with older positions are dropped); default: the feed's own."""
        self.feed = feed
        self.pool = pool
        self.state = state or FeedRealtimeState(
            ignore_service_suffix=feed.ignore_service_suffix, extra_kinds=frozenset(KINDS[k] for k in feed.extra_trips)
        )
        if max_age is not None:
            self.state.max_age_s = max_age
        self.index: TripIndex | None = None
        self._dirty = True
        self._reload: asyncio.Task | None = None
        self._reload_retry_at = 0.0  # time.monotonic()
        self._reload_error: str | None = None
        self._stop_times_retry_at = 0.0
        self.generation = 0  # of the installed index (TripIndex.generation)
        self.log = log.bind(feed=feed.feed_id)

    @property
    def feed_id(self) -> str:
        return self.feed.feed_id

    def invalidate(self) -> None:
        """The static data changed: reload on the next poll."""
        self._dirty = True

    def _wants_reload(self, now: float) -> bool:
        return self._dirty or self.index is None or self.index.loaded_for != local_date(now, self.index.tz)

    async def get_index(self, now: float) -> TripIndex | None:
        """The current index (None: the feed has no timetable yet), starting a background reload when it is due."""
        if self._reload is None and self._wants_reload(now) and time.monotonic() >= self._reload_retry_at:
            self._dirty = False  # an invalidate() during the reload makes it due again
            self._reload = asyncio.create_task(self._load(now), name=f"index:{self.feed_id}")
        if self.index is None and self._reload is not None:
            await asyncio.shield(self._reload)  # nothing to serve yet; a cancelled poll doesn't cancel the load
        if self.index is None and self._reload_error is not None:
            raise IndexUnavailable(self._reload_error)
        return self.index

    async def _load(self, now: float) -> None:
        started = time.monotonic()
        try:
            index = await asyncio.wait_for(
                load_index(self.pool, self.feed_id, now, self.feed.day_start, previous=self.index), RELOAD_TIMEOUT_S
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the previous index keeps serving
            self._dirty = True
            self._reload_retry_at = time.monotonic() + RELOAD_RETRY_S
            self._reload_error = _describe(exc)
            self.log.warning("trip_index_reload_failed", error=self._reload_error, serving_previous=self.index is not None)
            return
        finally:
            self._reload = None
        reused = index is not None and self.index is not None and index.trips is self.index.trips
        if index is not None:
            self.generation += 1
            index.generation = self.generation
        self.index, self._reload_error = index, None
        if index is not None:
            self.log.info(
                "trip_index_loaded",
                trips=len(index.trips),
                days=[(d.date.isoformat(), d.version) for d in index.days],
                reused=reused,
                seconds=round(time.monotonic() - started, 2),
            )

    async def reload(self, now: float) -> bool:
        """Load the index now (after an import) and wait for it, also for a reload that was already running. False if
        it failed (the previous index keeps serving; the next poll retries)."""
        self.invalidate()
        for _ in range(2):  # a reload already under way may have read the database before the import committed
            if self._reload is not None:
                await asyncio.shield(self._reload)
            if not self._dirty:
                break
            self._reload_retry_at = 0.0
            await self.get_index(now)
            if self._reload is not None:
                await asyncio.shield(self._reload)
        return not self._dirty and self.index is not None

    async def load_stop_times(self, index: TripIndex, keys: set[tuple[int, str]]) -> None:
        """The stop times of these (version, trip_id)s, into `index` (the one the caller works with). Fails quietly:
        trips without stop times go out without per-stop predictions until a later poll loads them."""
        if time.monotonic() < self._stop_times_retry_at:
            return
        try:
            await asyncio.wait_for(ensure_stop_times(self.pool, index, keys), STOP_TIMES_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._stop_times_retry_at = time.monotonic() + STOP_TIMES_RETRY_S
            self.log.warning("stop_times_load_failed", error=_describe(exc), retry_s=STOP_TIMES_RETRY_S)

    async def close(self) -> None:
        if self._reload is not None:
            self._reload.cancel()
            await asyncio.gather(self._reload, return_exceptions=True)
            self._reload = None  # a task cancelled before it started never ran its `finally`
