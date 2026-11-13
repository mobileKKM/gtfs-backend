"""The worker: one asyncio task per configured static feed and per realtime file, plus daily maintenance."""

from __future__ import annotations

import asyncio
import multiprocessing
import signal
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from redis.asyncio import Redis

from gtfs_backend import live_layout, procmem
from gtfs_backend.db import async_pool, connect
from gtfs_backend.feeds import FeedConfig, load_feeds
from gtfs_backend.logs import log
from gtfs_backend.worker.feed_sync import sync_feeds
from gtfs_backend.worker.http import make_client
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState
from gtfs_backend.worker.realtime.poller import FeedRuntime, RealtimeTask
from gtfs_backend.worker.realtime.store import RealtimeStore, TTLs
from gtfs_backend.worker.settings import WorkerSettings
from gtfs_backend.worker.static.fetch import PREVIOUSLY_FAILED, check_static, import_order
from gtfs_backend.worker.static.stops.osm import fetch_osm_poles, file_age_s
from gtfs_backend.worker.static.versions import RetirementBusy, retire_versions

# Touched every HEARTBEAT_INTERVAL_S while every realtime task keeps finishing polls; the healthcheck in
# docker-compose.yml reads its age (and hardcodes this path).
HEARTBEAT_FILE = Path("/tmp/gtfs-worker.alive")
HEARTBEAT_INTERVAL_S = 30
MAINTENANCE_TZ = ZoneInfo("Europe/Warsaw")  # only decides when RETIRE_AT is
MAINTENANCE_CHECK_INTERVAL_S = 60
OSM_CHECK_INTERVAL_S = 3600  # how often the OSM file's age is looked at
OSM_FIRST_ATTEMPTS = 3  # the first fetch is tried this often before feeds that need the file start without it
OSM_RETRY_DELAY_S = 30
LOOP_LAG_INTERVAL_S = 0.1
STATS_EVERY_BEATS = 10  # worker_stats is logged every 10 heartbeats (5 minutes)


class LoopLag:
    """How late the event loop wakes up. Every feed's polls share the loop, so anything that blocks it (building a trip
    index, processing a big update) delays all of them; this shows by how much."""

    def __init__(self) -> None:
        self.worst_ms = 0.0

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            await asyncio.sleep(LOOP_LAG_INTERVAL_S)
            self.worst_ms = max(self.worst_ms, (loop.time() - started - LOOP_LAG_INTERVAL_S) * 1000)

    def take(self) -> float:
        """The worst lag since the last call."""
        worst, self.worst_ms = self.worst_ms, 0.0
        return round(worst, 1)


def _import_executor() -> ProcessPoolExecutor:
    """Static imports are CPU-heavy: one at a time, in a separate process. The process exits after each job (an import
    or an OSM refresh): what a job allocated isn't all handed back to the system while the process lives (an import
    of kmk_a kept ~800 MB), and a fresh process costs a second or two once per job."""
    return ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"), max_tasks_per_child=1)


class Worker:
    def __init__(self, settings: WorkerSettings):
        self.settings = settings
        self.feeds = load_feeds(settings.feeds_config)
        self.enabled = self.feeds.enabled
        self.stop = asyncio.Event()
        # feeds that share stops with others wait for the providers' first check (fresh DB: ZTP before MLD)
        self.first_check = {f.feed_id: asyncio.Event() for f in self.enabled}
        # static checks (imports) and the daily retirement, one at a time (see versions.LOCK_TIMEOUT_MS)
        self.static_lock = asyncio.Lock()
        # feeds that share stops wait for the OSM stop poles (fetched on first start), or for the fetch to give up
        self.uses_osm = settings.osm_refresh_interval > 0 and any(f.share_stops_with for f in self.enabled)
        self.osm_ready = asyncio.Event()
        if not self.uses_osm:
            self.osm_ready.set()
        self.realtime_tasks: list[RealtimeTask] = []
        self.loop_lag = LoopLag()
        self._worst_lag = 0.0  # since the last worker_stats line
        self.exit_code = 0

    async def run(self) -> int:
        """Until SIGINT/SIGTERM, or until a task ends (a bug: every loop catches its own errors) or gets stuck.
        Returns the exit code: 1 if the worker stopped because something went wrong."""
        await self._open()
        try:
            tasks = self._start_tasks()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self.stop.set)
            watchers = [asyncio.create_task(self._watch(t)) for t in tasks]
            await self.stop.wait()
            log.info("worker_stopping")
            for t in tasks + watchers:
                t.cancel()
            await asyncio.gather(*tasks, *watchers, return_exceptions=True)
        finally:
            await self._close()
        return self.exit_code

    # ------------------------------------------------------------------------------------------- setup

    async def _open(self) -> None:
        s = self.settings

        def sync() -> None:
            with connect(s.database_url) as conn:
                sync_feeds(conn, self.feeds)

        await asyncio.to_thread(sync)
        self.pool = async_pool(s.database_url, max_size=s.db_pool_max_size)
        await self.pool.open()
        self.redis = Redis.from_url(s.redis_url)
        self.static_client = make_client(s.static_download_timeout, s.http_connect_timeout)
        self.rt_client = make_client(s.realtime_fetch_timeout, s.http_connect_timeout)
        self.executor = _import_executor()
        self.runtimes = {
            f.feed_id: FeedRuntime(f, self.pool, FeedRealtimeState(ignore_service_suffix=f.ignore_service_suffix))
            for f in self.enabled
        }
        ttls = TTLs(vehicle=s.vehicle_ttl, trip=s.trip_ttl, alert=s.alert_ttl, unmatched=s.unmatched_ttl)
        self.store = RealtimeStore(self.redis, [f.feed_id for f in self.enabled], ttls)

    async def _close(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
        await self.static_client.aclose()
        await self.rt_client.aclose()
        await self.redis.aclose()
        await self.pool.close()

    def _start_tasks(self) -> list[asyncio.Task]:
        s = self.settings
        tasks = [asyncio.create_task(self._static_loop(f), name=f"static:{f.feed_id}") for f in import_order(self.feeds)]
        for feed in self.enabled:
            for kind, url in feed.realtime.items():
                task = RealtimeTask(
                    self.runtimes[feed.feed_id],
                    kind,
                    url,
                    self.store,
                    self.rt_client,
                    interval=feed.realtime_poll_interval or s.realtime_poll_interval,
                    max_age=feed.realtime_max_age or s.realtime_max_age,
                    retries=s.http_retries,
                    retry_delay=s.http_retry_delay,
                )
                self.realtime_tasks.append(task)
                tasks.append(asyncio.create_task(task.run(), name=f"realtime:{task.name}"))
        if self.uses_osm:
            tasks.append(asyncio.create_task(self._osm_loop(), name="osm"))
        tasks.append(asyncio.create_task(self._maintenance_loop(), name="maintenance"))
        tasks.append(asyncio.create_task(self._heartbeat_loop(), name="heartbeat"))
        tasks.append(asyncio.create_task(self.loop_lag.run(), name="loop_lag"))
        log.info("worker_started", tasks=[t.get_name() for t in tasks])
        return tasks

    async def _watch(self, task: asyncio.Task) -> None:
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            log.error("task_crashed", task=task.get_name(), error=f"{type(exc).__name__}: {exc}")
        self.exit_code = 1
        self.stop.set()

    def affected_by(self, feed: FeedConfig) -> list[str]:
        """Feeds whose trip index an import of `feed` can change: its own, and those of the namespaces whose stops the
        import updates (its own and the ones it shares), whose stations then belong to other stop groups."""
        namespaces = {feed.namespace, *feed.share_stops_with}
        return [f.feed_id for f in self.enabled if f.namespace in namespaces or namespaces & set(f.share_stops_with)]

    def _invalidate_runtimes(self, feed_ids) -> None:
        for feed_id in feed_ids:
            if rt := self.runtimes.get(feed_id):
                rt.invalidate()

    # ------------------------------------------------------------------------------------------- loops

    async def _static_loop(self, feed: FeedConfig) -> None:
        providers = [f.feed_id for f in self.enabled if f.namespace in feed.share_stops_with]
        for provider in providers:
            await self.first_check[provider].wait()
        if feed.share_stops_with:
            await self.osm_ready.wait()
        while True:
            try:
                await self._static_check(feed)
            finally:
                self.first_check[feed.feed_id].set()
            await asyncio.sleep(self.settings.static_poll_interval)

    async def _osm_loop(self) -> None:
        """Keep the OSM stop poles file fresh. The first check is tried a few times, and only then are the feeds
        that need the file let go (without it they match by position alone)."""
        for attempt in range(1, OSM_FIRST_ATTEMPTS + 1):
            if await self._osm_check() or attempt == OSM_FIRST_ATTEMPTS:
                break
            await asyncio.sleep(OSM_RETRY_DELAY_S)
        self.osm_ready.set()
        while True:
            await asyncio.sleep(OSM_CHECK_INTERVAL_S)
            await self._osm_check()

    async def _osm_check(self) -> bool:
        """Download the OSM poles if the file is missing or older than OSM_REFRESH_INTERVAL. False if that failed.
        Runs in the import process: parsing the extract would stall the polls. The new file is used by the next
        import (stops are matched when a feed version is imported)."""
        path = self.settings.osm_stops_file
        age = file_age_s(path)
        if age is not None and age < self.settings.osm_refresh_interval:
            return True
        try:
            poles = await asyncio.get_running_loop().run_in_executor(self.executor, fetch_osm_poles, path)
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            log.error("osm_refresh_crashed", error=f"{type(exc).__name__}: {exc}")
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.executor = _import_executor()
            return False
        except Exception as exc:  # noqa: BLE001
            log.error("osm_refresh_failed", error=f"{type(exc).__name__}: {exc}", file_present=age is not None)
            return False
        log.info("osm_refreshed", poles=poles, path=str(path))
        return True

    async def _static_check(self, feed: FeedConfig) -> None:
        """One static check of a feed (download, import if new); errors are logged, never raised."""
        bound = log.bind(feed=feed.feed_id)
        try:
            async with self.static_lock:
                result = await check_static(feed, self.settings, self.pool, self.static_client, self.executor)
            if result is not None and result.status != PREVIOUSLY_FAILED:  # that one is logged by check_static
                bound.info(
                    "static_import_done",
                    status=result.status,
                    version=result.version,
                    seconds=result.seconds,
                    error=result.error,
                )
                if result.status == "ready":
                    self._invalidate_runtimes(self.affected_by(feed))
                    await self.redis.publish(live_layout.STATIC_UPDATED, feed.feed_id)
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            # the import process died (e.g. killed for memory); its transaction rolled back, so the zip is
            # imported again next time. A broken pool fails every later job, so it is replaced.
            bound.error("static_import_crashed", error=f"{type(exc).__name__}: {exc}")
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.executor = _import_executor()
        except Exception as exc:  # noqa: BLE001
            bound.error("static_check_failed", error=f"{type(exc).__name__}: {exc}")

    def _retire_all(self) -> tuple[dict[str, list[int]], list[str]]:
        """(retired version numbers per feed, feeds whose tables were busy)."""
        retired, busy = {}, []
        with connect(self.settings.database_url) as conn, conn.transaction():
            for feed in self.feeds.feeds:
                try:
                    retired[feed.feed_id] = retire_versions(conn, feed.feed_id)
                except RetirementBusy:
                    busy.append(feed.feed_id)
        return retired, busy

    async def _retire_once(self) -> bool:
        """One daily retirement; False if it has to be tried again (failed, or some feed's tables were busy)."""
        try:
            # not while an import runs: its transaction holds the tables the retirement drops partitions of
            async with self.static_lock:
                retired, busy = await asyncio.to_thread(self._retire_all)
        except Exception as exc:  # noqa: BLE001
            log.error("daily_retirement_failed", error=str(exc))
            return False
        log.info("daily_retirement", retired=retired, busy=busy)
        self._invalidate_runtimes(feed_id for feed_id, versions in retired.items() if versions)
        return not busy

    async def _maintenance_loop(self) -> None:
        """Daily at RETIRE_AT (MAINTENANCE_TZ): retire versions that no longer serve anything. Retried every
        MAINTENANCE_CHECK_INTERVAL_S until it succeeds."""
        last_retire = None
        while True:
            await asyncio.sleep(MAINTENANCE_CHECK_INTERVAL_S)
            now = datetime.now(MAINTENANCE_TZ)
            if now.time() >= self.settings.retire_at and last_retire != now.date() and await self._retire_once():
                last_retire = now.date()

    def stuck_tasks(self, now: float | None = None) -> list[str]:
        """Realtime tasks that haven't finished a poll for WATCHDOG_TIMEOUT."""
        now = time.monotonic() if now is None else now
        return [t.name for t in self.realtime_tasks if now - t.last_cycle > self.settings.watchdog_timeout]

    async def _heartbeat_loop(self) -> None:
        """Touch HEARTBEAT_FILE while the realtime tasks keep polling (the Docker healthcheck reads its age). A stuck
        task ends this loop, which stops the worker; Docker then restarts it."""
        s = self.settings
        beats = 0
        while True:
            if stuck := self.stuck_tasks():
                log.error("worker_stuck", tasks=stuck, seconds=s.watchdog_timeout)
                return
            try:
                HEARTBEAT_FILE.touch()
            except OSError as exc:
                log.warning("heartbeat_failed", path=str(HEARTBEAT_FILE), error=str(exc))
            try:  # stats must never stop the heartbeat (that would stop the worker)
                self.record_stats(beats)
            except Exception as exc:  # noqa: BLE001
                log.warning("worker_stats_failed", error=f"{type(exc).__name__}: {exc}")
            beats += 1
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    def record_stats(self, beat: int) -> None:
        """Every heartbeat: the loop's worst lag goes into each realtime file's meta (rt:meta). Every STATS_EVERY_BEATS:
        a worker_stats log line with memory and the worst lag since the last one."""
        lag = self.loop_lag.take()
        self._worst_lag = max(self._worst_lag, lag)
        for task in self.realtime_tasks:
            task.meta["loop_lag_ms"] = lag
        if beat % STATS_EVERY_BEATS == 0:
            log.info("worker_stats", rss_mb=procmem.rss_mb(), loop_lag_max_ms=self._worst_lag)
            self._worst_lag = 0.0
