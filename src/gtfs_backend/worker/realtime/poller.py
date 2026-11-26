"""One polling task per realtime file (feed x kind)."""

from __future__ import annotations

import asyncio
import statistics
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend import metrics
from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.live_layout import PollMeta
from gtfs_backend.logs import log
from gtfs_backend.worker.http import fetch_bytes
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


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def _secs(seconds: float) -> float:
    return round(seconds, 3)


class RealtimeTask:
    """Polls one realtime file and keeps `meta` (written to rt:meta:{feed_id}:{kind} after every poll).

    meta keeps the stats of the last data change while later polls see nothing new or fail, so a failure
    never hides how big the last good update was.

    Polls every `interval` seconds. A poll that finds the file unchanged keeps the last data alive in Redis (TTLs
    refreshed) for up to `max_age` seconds after it last changed: slow feeds (km_kolej: every ~5 min) would
    otherwise expire between updates.

    A download attempt gives up after `attempt_timeout`, so a hanging server delays a fast feed by a few polls at
    most (retries x timeout), not by minutes.

    `clock`: the wall clock the data is compared with (Settings.clock: shifted only in benchmark replays).
    `recorder`: gets the data age (now - feed header timestamp of the newest data) after every poll.
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
        attempt_timeout: float | None = None,
        clock: Callable[[], float] = time.time,
        recorder: metrics.Recorder | None = None,
    ):
        self.runtime = runtime
        self.clock = clock
        self.recorder = recorder
        self.kind = kind
        self.url = url
        self.store = store
        self.client = client
        self.interval = interval
        self.max_age = max_age
        self.last_change: float | None = None  # time.monotonic() of the last stored update
        self.retries, self.retry_delay = retries, retry_delay
        self.attempt_timeout = attempt_timeout  # per download attempt; None: the client's
        self.etag: str | None = None
        self.last_modified: str | None = None
        self.last_ts = 0
        # the last update processed, kept to process it again with a new index right away (after an import), and the
        # generation of the index it was last written with (TripIndex.generation)
        self.last_msg = None
        self.last_size = 0
        self.written_generation = 0
        self.failures = 0
        self.recent: deque[float] = deque(maxlen=RECENT_POLLS)
        self.meta: PollMeta = {"started_at": _iso(clock())}
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
        now = self.clock()
        # how old the newest data is now, not when it was written
        age = now - self.last_ts if self.last_ts else None
        if age is not None and self.recorder is not None:
            self.recorder.observe(metrics.age_name(self.feed_id, self.kind.value), age)
        self.meta.update(
            last_poll=_iso(now),
            consecutive_failures=self.failures,
            poll_seconds=_secs(elapsed),
            poll_seconds_max=_secs(max(self.recent)),
            age_s=round(age) if age is not None else None,
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
            timeout=self.attempt_timeout,
        )
        self.meta.update(fetch_seconds=_secs(time.perf_counter() - t0), fetch_attempts=fetched.attempts)
        if fetched.not_modified:
            return await self._unchanged_or_reindexed()
        msg = fetched.value
        ts = int(msg.header.timestamp)
        if ts and ts <= self.last_ts and not self._went_back(ts):
            if ts == self.last_ts:
                # data we have, under a new ETag (ZTP rewrites its files between timestamp changes): asking with this
                # one gets a 304 next time instead of the whole file. Not for an older copy: that one is looked at
                # again until _went_back takes it
                self.etag, self.last_modified = fetched.etag, fetched.last_modified
            return await self._unchanged_or_reindexed()
        res = await self._process(msg, ts, fetched.size)
        if res is not None:
            # remembered only once the data is stored, so a failed update is fetched and processed again
            self.etag, self.last_modified = fetched.etag, fetched.last_modified
        return res

    def _went_back(self, ts: int) -> bool:
        """An older header timestamp than the data we have is taken (once nothing newer came for `max_age`): a producer
        restarted with its clock behind would otherwise freeze the feed until its clock caught up. Within `max_age` it is
        an older copy (a second server, a cache) and ignored."""
        if ts >= self.last_ts or self.last_change is None or time.monotonic() - self.last_change <= self.max_age:
            return False
        self.log.warning("feed_timestamp_went_back", timestamp=ts, previous=self.last_ts)
        return True

    async def _unchanged_or_reindexed(self) -> Result | None:
        """Nothing new in the file: process the last update again if the index changed since it was written (an import:
        realtime is then written for the new timetable version at once, not at the next change of the file), else keep
        the last data alive."""
        index = self.runtime.index
        if self.last_msg is not None and index is not None and index.generation != self.written_generation:
            return await self._process(self.last_msg, self.last_ts, self.last_size, again=True)
        await self._unchanged()
        return None

    async def _process(self, msg, ts: int, size: int, *, again: bool = False) -> Result | None:
        now = self.clock()
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
        try:
            changes = await self._store(res)
        except BaseException:
            self.store.lost(self.feed_id, self.kind.value)
            raise
        t4 = time.perf_counter()
        await self.store.publish(self.feed_id, self.kind.value, ts, changes)
        # only now: a failed write or publish processes this update again at the next poll
        self.last_msg, self.last_size, self.written_generation = msg, size, index.generation
        self.last_ts = ts
        if not again:  # the data itself didn't change: its age (max_age) counts from when it did
            self.last_change = time.monotonic()
        self._record_change(size, ts, res, index_s=t2 - t1, process_s=t3 - t2, write_s=t4 - t3)
        return res

    async def _store(self, res: Result) -> dict:
        """Write what this file carries; returns what changed (live_layout.LiveChanges' parts)."""
        changes = {}
        if self.kind.carries(RealtimeKind.VEHICLE_POSITIONS):
            changes["vehicles"] = await self.store.write_vehicles(self.feed_id, res)
        if self.kind.carries(RealtimeKind.TRIP_UPDATES):
            changes["trips"] = await self.store.write_trips(self.feed_id, res)
        if self.kind.carries(RealtimeKind.SERVICE_ALERTS):
            changes["alerts"] = await self.store.write_alerts(self.feed_id, res)
        await self.store.write_unmatched(self.feed_id, self.kind.value, res)
        return changes

    def _record_change(self, size: int, ts: int, res: Result, *, index_s: float, process_s: float, write_s: float):
        """The stats of a poll that changed data; they stay in meta until the next change."""
        unmatched = res.unmatched_trips + [v["trip_id"] or v["vehicle_id"] for v in res.unmatched_vehicles]
        self.meta.update(
            ok=True,
            last_error=None,
            last_change=_iso(self.clock()),
            index_seconds=_secs(index_s),
            process_seconds=_secs(process_s),  # includes loading missing stop times
            write_seconds=_secs(write_s),
            feed_timestamp=ts,
            bytes=size,
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
            # GTFS-RT schedule_relationship counts of this update, matched or not (trip level, and per stop)
            trip_relationships=dict(sorted(res.trip_relationships.items())),
            stop_relationships=dict(sorted(res.stop_relationships.items())),
            # trips predicted to reach their last stop after the next trip of their block is due to leave, and of
            # those, how many next trips have no update of their own in this file
            block_late_next=res.block_late_next,
            block_late_next_without_update=res.block_late_next_without_update,
            stale_vehicles=res.stale_vehicles,
        )

    async def _unchanged(self) -> None:
        """Nothing new: keep the last data alive while it is younger than max_age, then let it expire."""
        if self.last_change is not None and time.monotonic() - self.last_change < self.max_age:
            await self.store.touch(self.feed_id, self.kind)
        self.meta.update(ok=True, last_error=None)
