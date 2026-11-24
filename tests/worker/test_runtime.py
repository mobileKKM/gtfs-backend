"""FeedRuntime: index reloads run in the background and never stop the live data."""

import asyncio
from datetime import date
from zoneinfo import ZoneInfo

import pytest

from gtfs_backend.feeds import FeedConfig
from gtfs_backend.worker.realtime import runtime as runtime_mod
from gtfs_backend.worker.realtime.runtime import FeedRuntime, IndexUnavailable
from gtfs_backend.worker.realtime.trip_index import TripIndex

WAW = ZoneInfo("Europe/Warsaw")
NOW = 1792742400.0  # 2026-10-23 10:00 local


def index(n: int) -> TripIndex:
    idx = TripIndex("kmk_t", WAW, loaded_for=date(2026, 10, 23))
    idx.version_ids = {n: f"kmk_t_v{n}"}
    return idx


class FakeLoads:
    """load_index stand-in: each call takes the next outcome (a TripIndex, or an Exception to raise), optionally
    waiting for `gate`."""

    def __init__(self, monkeypatch, *outcomes):
        self.outcomes = list(outcomes)
        self.calls: list[TripIndex | None] = []  # the `previous` of each call
        self.gate: asyncio.Event | None = None
        monkeypatch.setattr(runtime_mod, "load_index", self)

    async def __call__(self, pool, feed_id, now, day_start, previous=None):
        self.calls.append(previous)
        if self.gate is not None:
            await self.gate.wait()
        out = self.outcomes.pop(0)
        if isinstance(out, Exception):
            raise out
        return out


def make_runtime() -> FeedRuntime:
    return FeedRuntime(FeedConfig(feed_id="kmk_t", static="https://x.test/t.zip"), pool=None)


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


async def test_first_load_is_waited_for_later_ones_run_in_the_background(monkeypatch):
    loads = FakeLoads(monkeypatch, index(1), index(2))
    rt = make_runtime()
    first = await rt.get_index(NOW)
    assert first.version_ids == {1: "kmk_t_v1"}
    assert await rt.get_index(NOW) is first and len(loads.calls) == 1  # nothing due

    rt.invalidate()
    loads.gate = asyncio.Event()
    assert await rt.get_index(NOW) is first  # the reload is running: the old index keeps serving
    await settle()
    assert await rt.get_index(NOW) is first and len(loads.calls) == 2  # one reload at a time
    loads.gate.set()
    await settle()
    assert (await rt.get_index(NOW)).version_ids == {2: "kmk_t_v2"}
    assert loads.calls[1] is first  # the previous index is handed over for reuse


async def test_a_new_day_reloads_in_the_background(monkeypatch):
    loads = FakeLoads(monkeypatch, index(1), index(1))
    rt = make_runtime()
    first = await rt.get_index(NOW)
    assert await rt.get_index(NOW + 86400) is first  # served while the new day's index loads
    await settle()
    assert len(loads.calls) == 2


async def test_a_failed_reload_keeps_the_old_index_and_is_retried_later(monkeypatch):
    loads = FakeLoads(monkeypatch, index(1), ConnectionError("db down"), index(2))
    rt = make_runtime()
    first = await rt.get_index(NOW)
    rt.invalidate()
    assert await rt.get_index(NOW) is first
    await settle()
    assert await rt.get_index(NOW) is first and len(loads.calls) == 2  # not retried at once

    rt._reload_retry_at = 0  # the retry delay has passed
    await rt.get_index(NOW)
    await settle()
    assert (await rt.get_index(NOW)).version_ids == {2: "kmk_t_v2"}


async def test_a_failed_first_load_is_reported(monkeypatch):
    FakeLoads(monkeypatch, ConnectionError("db down"))
    rt = make_runtime()
    with pytest.raises(IndexUnavailable, match="ConnectionError: db down"):
        await rt.get_index(NOW)


async def test_an_invalidate_during_a_reload_reloads_again(monkeypatch):
    loads = FakeLoads(monkeypatch, index(1), index(2), index(3))
    rt = make_runtime()
    await rt.get_index(NOW)
    rt.invalidate()
    loads.gate = asyncio.Event()
    await rt.get_index(NOW)
    rt.invalidate()  # e.g. another import finished meanwhile
    loads.gate.set()
    await settle()
    await rt.get_index(NOW)
    await settle()
    assert (await rt.get_index(NOW)).version_ids == {3: "kmk_t_v3"}


async def test_stop_times_that_fail_to_load_do_not_fail_the_poll(monkeypatch):
    calls = []

    async def broken(pool, index, keys):
        calls.append(keys)
        raise ConnectionError("db down")

    monkeypatch.setattr(runtime_mod, "ensure_stop_times", broken)
    rt = make_runtime()
    await rt.load_stop_times(index(1), {(1, "a")})
    await rt.load_stop_times(index(1), {(1, "a")})  # backing off: the database isn't asked again yet
    assert len(calls) == 1


async def test_close_cancels_a_running_reload(monkeypatch):
    loads = FakeLoads(monkeypatch, index(1), index(2))
    rt = make_runtime()
    await rt.get_index(NOW)
    rt.invalidate()
    loads.gate = asyncio.Event()
    await rt.get_index(NOW)
    await rt.close()
    assert rt._reload is None
