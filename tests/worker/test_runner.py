"""The worker's static check and daily retirement (without running the whole worker)."""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

from gtfs_backend.worker import runner
from gtfs_backend.worker.settings import WorkerSettings

ROOT = Path(__file__).parent.parent.parent


def _worker(**settings) -> runner.Worker:
    worker = runner.Worker(WorkerSettings(feeds_config=ROOT / "feeds.yml", **settings))
    worker.pool = worker.static_client = worker.redis = None
    worker.runtimes = {}
    return worker


async def test_crashed_import_process_is_replaced(monkeypatch):
    worker = _worker()
    worker.executor = broken = runner._import_executor()
    seen = []

    async def check_static(feed, settings, pool, client, executor):
        seen.append(executor)
        if len(seen) == 1:
            raise BrokenProcessPool("the import process died")
        return None

    monkeypatch.setattr(runner, "check_static", check_static)
    feed = worker.enabled[0]
    await worker._static_check(feed)  # logged, not raised
    assert worker.executor is not broken
    await worker._static_check(feed)
    assert seen[1] is worker.executor  # the next check uses the new process pool
    worker.executor.shutdown()


async def test_busy_retirement_is_tried_again(monkeypatch):
    worker = _worker()
    results = iter([({"kmk_t": []}, ["kmk_a"]), ({"kmk_t": [], "kmk_a": [3]}, [])])
    monkeypatch.setattr(worker, "_retire_all", lambda: next(results))
    assert await worker._retire_once() is False
    assert await worker._retire_once() is True


async def test_retirement_waits_for_a_running_import(monkeypatch):
    worker = _worker()
    calls = []
    monkeypatch.setattr(worker, "_retire_all", lambda: calls.append("retire") or ({}, []))
    async with worker.static_lock:  # an import is running
        task = runner.asyncio.create_task(worker._retire_once())
        await runner.asyncio.sleep(0.05)
        assert calls == []
    assert await task is True
    assert calls == ["retire"]


class _Task:
    def __init__(self, name, last_cycle):
        self.name, self.last_cycle = name, last_cycle
        self.meta: dict = {}


async def test_heartbeat_is_touched_while_polling_works_and_a_stuck_task_stops_it(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "HEARTBEAT_FILE", tmp_path / "alive")
    worker = runner.Worker(WorkerSettings(_env_file=None, feeds_config=ROOT / "feeds.yml", watchdog_timeout=60))
    now = runner.time.monotonic()
    worker.realtime_tasks = [_Task("kmk_t:trip_updates", now), _Task("km_kolej:combined", now - 30)]
    assert worker.stuck_tasks(now) == []
    beat = runner.asyncio.create_task(worker._heartbeat_loop())
    await runner.asyncio.sleep(0.05)
    assert (tmp_path / "alive").exists() and not beat.done()

    worker.realtime_tasks[1].last_cycle = now - 61  # no finished poll for longer than WATCHDOG_TIMEOUT
    assert worker.stuck_tasks(now) == ["km_kolej:combined"]
    beat.cancel()
    (tmp_path / "alive").unlink()
    await runner.asyncio.wait_for(worker._heartbeat_loop(), 1)  # ends at once, so _watch stops the worker
    assert not (tmp_path / "alive").exists()


async def test_a_task_that_ends_stops_the_worker_with_exit_code_1():
    worker = _worker()

    async def ends():
        return None

    await worker._watch(runner.asyncio.create_task(ends()))
    assert worker.stop.is_set() and worker.exit_code == 1


def _osm_worker(tmp_path, monkeypatch, **settings):
    """A worker whose OSM download is a function that writes the file; returns (worker, calls)."""
    path = tmp_path / "osm" / "stops.json"
    worker = _worker(osm_stops_file=path, osm_refresh_interval=100, **settings)
    worker.executor = ThreadPoolExecutor(max_workers=1)  # the real one is a process pool
    calls = []

    def fetch(target):
        calls.append(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}")
        return 5

    monkeypatch.setattr(runner, "fetch_osm_poles", fetch)
    return worker, path, calls


async def test_osm_file_is_fetched_when_missing_or_stale_and_left_alone_when_fresh(tmp_path, monkeypatch):
    worker, path, calls = _osm_worker(tmp_path, monkeypatch)
    assert await worker._osm_check() is True and len(calls) == 1  # missing
    assert await worker._osm_check() is True and len(calls) == 1  # fresh
    old = time.time() - 101
    os.utime(path, (old, old))
    assert await worker._osm_check() is True and len(calls) == 2  # older than the refresh interval


async def test_a_failed_osm_fetch_is_logged_not_raised_and_the_old_file_stays(tmp_path, monkeypatch):
    worker, path, _ = _osm_worker(tmp_path, monkeypatch)
    path.parent.mkdir()
    path.write_text("{}")
    old = time.time() - 1000
    os.utime(path, (old, old))

    def boom(target):
        raise OSError("no network")

    monkeypatch.setattr(runner, "fetch_osm_poles", boom)
    assert await worker._osm_check() is False
    assert path.read_text() == "{}"


async def test_feeds_that_share_stops_wait_for_the_first_osm_fetch(tmp_path, monkeypatch):
    worker, _, _ = _osm_worker(tmp_path, monkeypatch)
    assert worker.uses_osm and not worker.osm_ready.is_set()
    checked = []

    async def check(feed):
        checked.append(feed.feed_id)
        raise runner.asyncio.CancelledError  # one check is enough

    monkeypatch.setattr(worker, "_static_check", check)
    mld = next(f for f in worker.enabled if f.share_stops_with)
    for event in worker.first_check.values():
        event.set()
    task = runner.asyncio.create_task(worker._static_loop(mld))
    await runner.asyncio.sleep(0.05)
    assert checked == []  # held back
    worker.osm_ready.set()
    await runner.asyncio.gather(task, return_exceptions=True)
    assert checked == [mld.feed_id]


async def test_osm_loop_gives_up_on_the_first_fetch_after_a_few_tries_and_lets_the_feeds_go(tmp_path, monkeypatch):
    worker, _, _ = _osm_worker(tmp_path, monkeypatch)
    attempts = []

    async def failing():
        attempts.append(1)
        return False

    async def stop(delay):
        if worker.osm_ready.is_set():
            raise runner.asyncio.CancelledError  # the periodic wait after the first check

    monkeypatch.setattr(worker, "_osm_check", failing)
    monkeypatch.setattr(runner.asyncio, "sleep", stop)
    await runner.asyncio.gather(worker._osm_loop(), return_exceptions=True)
    assert len(attempts) == runner.OSM_FIRST_ATTEMPTS
    assert worker.osm_ready.is_set()


async def test_osm_is_not_used_when_refreshing_is_off_or_nobody_shares_stops(tmp_path):
    off = _worker(osm_refresh_interval=0)
    assert not off.uses_osm and off.osm_ready.is_set()


async def test_loop_lag_shows_a_blocked_loop():
    lag = runner.LoopLag()
    task = runner.asyncio.create_task(lag.run())
    await runner.asyncio.sleep(0.15)
    time.sleep(0.3)  # something blocks the event loop
    await runner.asyncio.sleep(0.15)
    task.cancel()
    worst = lag.take()
    assert worst >= 150
    assert lag.take() == 0  # reset by take()


def test_stats_put_the_loop_lag_into_every_realtime_meta():
    worker = _worker()

    worker.realtime_tasks = [_Task("a", 0), _Task("b", 0)]
    worker.loop_lag.worst_ms = 42.0
    worker.record_stats(1)
    assert [t.meta["loop_lag_ms"] for t in worker.realtime_tasks] == [42.0, 42.0]


def test_an_import_invalidates_only_the_feeds_it_can_change():
    worker = _worker()  # feeds.yml: kmk_a/m/t (namespace ztp), km_kolej, km_mld (shares ztp's stops)
    feeds = {f.feed_id: f for f in worker.enabled}
    ztp = sorted(worker.affected_by(feeds["kmk_t"]))
    assert ztp == ["km_mld", "kmk_a", "kmk_m", "kmk_t"]  # the trains' index is left alone
    assert sorted(worker.affected_by(feeds["km_mld"])) == ["km_mld", "kmk_a", "kmk_m", "kmk_t"]
    assert worker.affected_by(feeds["km_kolej"]) == ["km_kolej"]


def test_imports_run_at_lower_cpu_priority(monkeypatch):
    import contextlib

    from gtfs_backend import db
    from gtfs_backend.worker.static import fetch, load

    calls = []
    monkeypatch.setattr(os, "nice", lambda n: calls.append(n) or n)
    monkeypatch.setattr(db, "connect", lambda url: contextlib.nullcontext())
    monkeypatch.setattr(load, "import_zip", lambda *a, **k: load.ImportResult("kmk_t_v1", "kmk_t", 1, "ready"))
    fetch.run_import_job("kmk_t", "ztp", [], None, "x.zip", {"sha256": "abc"}, "postgresql://unused")
    assert calls == [fetch.IMPORT_NICENESS]
