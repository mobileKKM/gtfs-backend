"""Which problems make a feed's data not fresh (/health `fresh`, `problems`)."""

from datetime import UTC, datetime

from gtfs_backend.api.services.health import StaticState, feed_problems
from gtfs_backend.feeds import FeedConfig

NOW = 1_800_000_000.0
FEED = FeedConfig(
    feed_id="kmk_t",
    static="https://example.invalid/T.zip",
    realtime={"trip_updates": "https://example.invalid/t.pb", "service_alerts": "https://example.invalid/a.pb"},
)
GOOD_STATIC = StaticState(version=3, checked_at="…", checked_age_s=600, latest_status="ready", latest_version=3)


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def meta(poll_ago=5, data_ago=10, ok=True, failures=0, **kw):
    return {
        "ok": ok,
        "consecutive_failures": failures,
        "last_error": None if ok else "HTTPStatusError: 404",
        "last_poll": iso(NOW - poll_ago),
        "feed_timestamp": int(NOW - data_ago) if data_ago is not None else None,
        "started_at": iso(NOW - 3600),
        **kw,
    }


def problems(static=GOOD_STATIC, metas=None, feed=FEED):
    metas = metas if metas is not None else {"trip_updates": meta(), "service_alerts": meta(data_ago=None)}
    return feed_problems(feed, static, metas, NOW, realtime_max_age=300, static_max_age=3 * 3600)


def test_healthy_feed_has_no_problems():
    # an alerts file that hasn't changed since the worker started is normal
    assert problems() == []


def test_static_problems():
    assert problems(StaticState(None, None, None, None, None)) == ["static: no timetable loaded", "static: never checked"]
    assert problems(StaticState(3, "…", 600, "failed", 4)) == ["static: the import of version 4 failed"]
    assert problems(StaticState(3, "…", 4 * 3600, "ready", 3)) == ["static: not checked for 4.0 h"]


def test_realtime_problems():
    alerts = meta(data_ago=None)
    assert problems(metas={"trip_updates": meta(poll_ago=900), "service_alerts": alerts}) == [
        "trip_updates: not polled for 15 min"
    ]
    assert problems(metas={"trip_updates": meta(data_ago=1200), "service_alerts": alerts}) == ["trip_updates: data is 20 min old"]
    assert problems(metas={"service_alerts": alerts}) == ["trip_updates: never polled"]
    failing = meta(ok=False, failures=5)
    assert problems(metas={"trip_updates": failing, "service_alerts": alerts}) == [
        "trip_updates: failing (5 polls): HTTPStatusError: 404"
    ]
    # one or two failures while ZTP replaces a file are normal
    assert problems(metas={"trip_updates": meta(ok=False, failures=2), "service_alerts": alerts}) == []


def test_slow_feeds_get_their_own_max_age():
    slow = FEED.model_copy(update={"realtime_max_age": 600})
    metas = {"trip_updates": meta(data_ago=500), "service_alerts": meta(data_ago=None)}
    assert problems(metas=metas, feed=slow) == []
    assert problems(metas=metas) == ["trip_updates: data is 8 min old"]
