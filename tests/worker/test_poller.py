"""Poll meta (rt:meta:{feed_id}:{kind}): written after every poll, keeps the last good stats through failures."""

from datetime import UTC, datetime

from conftest import FIXTURES

from gtfs_backend.feeds import FeedConfig, RealtimeKind
from gtfs_backend.worker.http import Fetched
from gtfs_backend.worker.realtime import poller
from gtfs_backend.worker.realtime.normalize import Result
from gtfs_backend.worker.realtime.poller import RealtimeTask, parse_feed
from gtfs_backend.worker.realtime.runtime import FeedRuntime

MSG = parse_feed((FIXTURES / "dst_2026-10-25" / "TripUpdates_T.pb").read_bytes())


class FakeStore:
    def __init__(self):
        self.meta: dict = {}
        self.published = 0
        self.touched = 0

    async def write_meta(self, feed_id, kind, meta):
        self.meta = dict(meta)

    async def publish(self, feed_id, kind, ts):
        self.published += 1

    async def touch(self, feed_id, kind):
        self.touched += 1

    async def write_trips(self, feed_id, res): ...

    async def write_unmatched(self, feed_id, kind, res): ...


def make_task(monkeypatch, *responses, **task_kwargs):
    """A trip-updates task whose downloads return `responses` in order (an Exception is raised)."""
    runtime = FeedRuntime(FeedConfig(feed_id="kmk_t", static="https://x.test/t.zip"), pool=None)

    async def get_index(now):
        return object()

    async def process_message(feed_id, msg, index, state, load, now):
        res = Result(trips=[{"key": "kmk_t:a:20261025", "delay_s": 60}], entities=1)
        res.trip_relationships.update(CANCELED=1, SCHEDULED=3)
        res.block_late_next, res.block_late_next_without_update = 2, 1
        return res

    queue = list(responses)

    async def fetch_bytes(*args, **kwargs):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(runtime, "get_index", get_index)
    monkeypatch.setattr(poller, "process_message", process_message)
    monkeypatch.setattr(poller, "fetch_bytes", fetch_bytes)
    store = FakeStore()
    return RealtimeTask(
        runtime,
        RealtimeKind.TRIP_UPDATES,
        "https://x.test/tu.pb",
        store,
        client=None,
        interval=10,
        max_age=120,
        retries=1,
        retry_delay=0,
        **task_kwargs,
    ), store


async def test_meta_tracks_changes_unchanged_polls_and_failures(monkeypatch):
    task, store = make_task(
        monkeypatch,
        Fetched(False, MSG, etag='"v1"', size=1234, attempts=2),
        Fetched(True, etag='"v1"'),
        ValueError("feed message without header"),
    )

    await task.cycle()
    meta = store.meta
    assert meta["ok"] is True and meta["started_at"]
    assert meta["trips"] == 1 and meta["bytes"] == 1234 and meta["median_delay_s"] == 60
    assert meta["fetch_attempts"] == 2
    for key in ("poll_seconds", "poll_seconds_max", "fetch_seconds", "index_seconds", "process_seconds", "write_seconds"):
        assert meta[key] >= 0
    assert store.published == 1 and task.etag == '"v1"'
    first_change = meta["last_change"]

    # nothing new: the last update's stats stay
    await task.cycle()
    meta = store.meta
    assert meta["ok"] is True
    assert meta["last_change"] == first_change and meta["trips"] == 1 and meta["fetch_attempts"] == 1
    assert store.published == 1

    # a failed download keeps the last good stats next to the error
    await task.cycle()
    meta = store.meta
    assert meta["ok"] is False and meta["last_error"] == "ValueError: feed message without header"
    assert meta["consecutive_failures"] == 1
    assert meta["trips"] == 1


async def test_etag_is_kept_only_after_the_update_is_stored(monkeypatch):
    task, store = make_task(monkeypatch, Fetched(False, MSG, etag='"v1"'))

    async def broken_write(feed_id, res):
        raise ConnectionError("redis down")

    monkeypatch.setattr(store, "write_trips", broken_write)
    await task.cycle()
    # the next poll downloads and processes the same file again instead of getting 304
    assert task.etag is None and task.last_ts == 0
    assert store.meta["ok"] is False and "last_change" not in store.meta


async def test_unchanged_polls_keep_the_data_alive_until_max_age(monkeypatch):
    task, store = make_task(
        monkeypatch,
        Fetched(True),  # nothing stored yet: nothing to keep alive
        Fetched(False, MSG, etag='"v1"'),
        Fetched(True, etag='"v1"'),  # 304
        Fetched(False, MSG, etag='"v2"'),  # new ETag, same header timestamp
        Fetched(True, etag='"v1"'),
    )
    task.max_age = 600
    await task.cycle()
    assert store.touched == 0
    await task.cycle()
    assert store.touched == 0  # a change is written, not touched
    await task.cycle()
    await task.cycle()
    assert store.touched == 2 and store.meta["ok"] is True

    # the file hasn't changed for longer than max_age: the data is left to expire
    task.last_change -= 601
    await task.cycle()
    assert store.touched == 2


async def test_a_new_etag_for_data_already_stored_is_remembered(monkeypatch):
    task, store = make_task(
        monkeypatch,
        Fetched(False, MSG, etag='"v1"'),
        Fetched(False, MSG, etag='"v2"'),  # the same feed timestamp, rewritten under a new ETag
    )
    await task.cycle()
    await task.cycle()
    assert task.etag == '"v2"'  # the next poll asks with it and gets a 304
    assert store.published == 1  # the rewrite isn't an update


async def test_data_age_is_sampled_every_poll_with_the_tasks_clock(monkeypatch):
    from gtfs_backend import metrics

    ts = MSG.header.timestamp
    now = [ts + 4.0]
    recorder = metrics.Recorder()
    task, store = make_task(
        monkeypatch,
        Fetched(True),
        Fetched(False, MSG, etag='"v1"'),
        Fetched(True, etag='"v1"'),
        clock=lambda: now[0],
        recorder=recorder,
    )
    await task.cycle()  # nothing yet: no age
    assert recorder._counts == {} and store.meta["age_s"] is None
    await task.cycle()
    now[0] += 30  # the file stays unchanged: its data gets older
    await task.cycle()
    name = metrics.age_name("kmk_t", "trip_updates")
    s = metrics.summarize([{f: str(n) for f, n in recorder._counts.items()}])[name]
    assert s.count == 2 and s.quantile(0.5) >= 4 and s.max >= 34
    assert store.meta["age_s"] == 34
    assert store.meta["last_poll"].startswith(datetime.fromtimestamp(now[0], UTC).isoformat(timespec="seconds"))
    assert store.meta["trip_relationships"] == {"CANCELED": 1, "SCHEDULED": 3}  # sorted by name
    assert (store.meta["block_late_next"], store.meta["block_late_next_without_update"]) == (2, 1)
