"""One polling task per realtime file (feed x kind)."""

from __future__ import annotations

import asyncio
import statistics
import time
from collections import deque
from datetime import UTC, datetime

import httpx
from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.live_layout import PollMeta
from gtfs_backend.logs import log
from gtfs_backend.worker.http import Fetched, fetch_bytes
from gtfs_backend.worker.realtime.normalize import Result, process_message
from gtfs_backend.worker.realtime.runtime import FeedRuntime
from gtfs_backend.worker.realtime.store import RealtimeStore


def parse_feed(body: bytes) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.ParseFromString(body)
    if not msg.header.gtfs_realtime_version:
        raise ValueError("feed message without header")
    return msg


def median_delay(res: Result) -> float | None:
    delays = [t["delay_s"] for t in res.trips if t["delay_s"] is not None]
    return statistics.median(delays) if delays else None


RECENT_POLLS = 60  # window for poll_seconds_max (10 minutes at the default 10 s interval)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _secs(seconds: float) -> float:
    return round(seconds, 3)


class RealtimeTask:
    """Polls one realtime file and keeps `meta` (written to rt:meta:{feed_id}:{kind} after every poll).

    meta keeps the stats of the last data change while later polls see nothing new or fail, so a failure
    never hides how big the last good update was.

    Polls every `interval` seconds. A poll that finds the file unchanged keeps the last data alive in Redis (TTLs
    refreshed) for up to `max_age` seconds after it last changed: slow feeds (km_kolej: every ~5 min) would
    otherwise expire between updates.
    """

    def __init__(
        self,
        runtime: FeedRuntime,
        kind: RealtimeKind,
        url: str,
        store: RealtimeStore,
        client: httpx.AsyncClient,
        *,
        interval: float,
        max_age: float,
        retries: int,
        retry_delay: float,
    ):
        self.runtime = runtime
        self.kind = kind
        self.url = url
        self.store = store
        self.client = client
        self.interval = interval
        self.max_age = max_age
        self.last_change: float | None = None  # time.monotonic() of the last stored update
        self.retries, self.retry_delay = retries, retry_delay
        self.etag: str | None = None
        self.last_modified: str | None = None
        self.last_ts = 0
        self.failures = 0
        self.recent: deque[float] = deque(maxlen=RECENT_POLLS)
        self.meta: PollMeta = {"started_at": _iso_now()}
        self.last_cycle = time.monotonic()  # when the last poll finished (or the task started): the worker's watchdog
        self.log = log.bind(feed=runtime.feed.feed_id, kind=kind.value)

    @property
    def feed_id(self) -> str:
        return self.runtime.feed.feed_id

    async def run(self) -> None:
        while True:
            started = time.monotonic()
            await self.cycle()
            await asyncio.sleep(max(0.0, self.interval - (time.monotonic() - started)))

    async def cycle(self) -> None:
        """One poll plus its meta write; errors are recorded, never raised."""
        started = time.perf_counter()
        try:
            await self.poll_once()
            self.failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep polling; data in Redis expires by TTL
            self.failures += 1
            # ZTP briefly serves empty/missing files while replacing them; only repeated failures matter
            level = self.log.warning if self.failures >= 3 else self.log.info
            level("realtime_poll_failed", error=f"{type(exc).__name__}: {exc}", failures=self.failures)
            self.meta.update(ok=False, last_error=f"{type(exc).__name__}: {exc}")
        elapsed = time.perf_counter() - started
        self.recent.append(elapsed)
        self.meta.update(
            last_poll=_iso_now(),
            consecutive_failures=self.failures,
            poll_seconds=_secs(elapsed),
            poll_seconds_max=_secs(max(self.recent)),
            # how old the data in Redis is now, not when it was written
            age_s=round(time.time() - self.last_ts) if self.last_ts else None,
        )
        try:
            await self.store.write_meta(self.feed_id, self.kind.value, self.meta)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("meta_write_failed", error=str(exc))
        self.last_cycle = time.monotonic()

    @property
    def name(self) -> str:
        return f"{self.feed_id}:{self.kind.value}"

    async def poll_once(self) -> Result | None:
        t0 = time.perf_counter()
        fetched = await fetch_bytes(
            self.client,
            self.url,
            parse_feed,
            etag=self.etag,
            last_modified=self.last_modified,
            retries=self.retries,
            retry_delay=self.retry_delay,
        )
        self.meta.update(fetch_seconds=_secs(time.perf_counter() - t0), fetch_attempts=fetched.attempts)
        if fetched.not_modified:
            await self._unchanged()
            return None
        msg = fetched.value
        ts = int(msg.header.timestamp)
        if ts and ts <= self.last_ts:
            await self._unchanged()
            return None
        now = time.time()
        t1 = time.perf_counter()
        index = await self.runtime.get_index(now)
        if index is None:
            self.meta.update(ok=False, last_error="no static version loaded yet")
            return None

        async def load(keys: set[tuple[int, str]]) -> None:
            await self.runtime.load_stop_times(index, keys)

        t2 = time.perf_counter()
        res = await process_message(self.feed_id, msg, index, self.runtime.state, load, now)
        t3 = time.perf_counter()
        await self._store(res)
        t4 = time.perf_counter()
        # remembered only once the data is stored, so a failed update is fetched and processed again
        self.etag, self.last_modified = fetched.etag, fetched.last_modified
        self.last_ts = ts
        self.last_change = time.monotonic()
        self._record_change(fetched, ts, res, index_s=t2 - t1, process_s=t3 - t2, write_s=t4 - t3)
        await self.store.publish(self.feed_id, self.kind.value, ts)
        return res

    async def _store(self, res: Result) -> None:
        """Write what this file carries."""
        if self.kind.carries(RealtimeKind.VEHICLE_POSITIONS):
            await self.store.write_vehicles(self.feed_id, res)
        if self.kind.carries(RealtimeKind.TRIP_UPDATES):
            await self.store.write_trips(self.feed_id, res)
        if self.kind.carries(RealtimeKind.SERVICE_ALERTS):
            await self.store.write_alerts(self.feed_id, res)
        await self.store.write_unmatched(self.feed_id, self.kind.value, res)

    def _record_change(self, fetched: Fetched, ts: int, res: Result, *, index_s: float, process_s: float, write_s: float):
        """The stats of a poll that changed data; they stay in meta until the next change."""
        unmatched = res.unmatched_trips + [v["trip_id"] or v["vehicle_id"] for v in res.unmatched_vehicles]
        self.meta.update(
            ok=True,
            last_error=None,
            last_change=_iso_now(),
            index_seconds=_secs(index_s),
            process_seconds=_secs(process_s),  # includes loading missing stop times
            write_seconds=_secs(write_s),
            feed_timestamp=ts,
            bytes=fetched.size,
            entities=res.entities,
            vehicles=len(res.vehicles),
            trips=len(res.trips),
            alerts=len(res.alerts),
            unmatched_trips=len(res.unmatched_trips),
            unmatched_vehicles=len(res.unmatched_vehicles),
            unmatched_alert_entities=len(res.unmatched_alert_entities),
            unmatched_sample=(unmatched + res.unmatched_alert_entities)[:10],
            route_mismatches=res.mismatches,
            # ~0 normally; about +-3600 on a clock-change day would mean the feed's day_start is wrong
            median_delay_s=median_delay(res),
        )

    async def _unchanged(self) -> None:
        """Nothing new: keep the last data alive while it is younger than max_age, then let it expire."""
        if self.last_change is not None and time.monotonic() - self.last_change < self.max_age:
            await self.store.touch(self.feed_id, self.kind)
        self.meta.update(ok=True, last_error=None)
