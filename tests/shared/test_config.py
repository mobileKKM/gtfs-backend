from pathlib import Path

import pytest
from pydantic import ValidationError

from gtfs_backend.feeds import RealtimeKind, load_feeds
from gtfs_backend.worker.static.fetch import import_order

ROOT = Path(__file__).parents[2]


def write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "feeds.yml"
    p.write_text(text)
    return p


def test_repo_feeds_yml_is_valid():
    feeds = load_feeds(ROOT / "feeds.yml")
    feed_ids = [f.feed_id for f in feeds.feeds]
    assert feed_ids == ["kmk_a", "kmk_m", "kmk_t", "km_kolej", "km_mld"]
    assert {f.namespace for f in feeds.feeds if f.feed_id.startswith("kmk_")} == {"ztp"}
    assert feeds.get("km_kolej").realtime.items()[0][0] == RealtimeKind.COMBINED
    assert feeds.get("km_mld").share_stops_with == ["ztp"]
    assert len(feeds.get("kmk_a").realtime.items()) == 3
    # ZTP updates every ~5 s, km_kolej every ~5 min; km_mld (every 20 s) uses the env defaults
    assert {feeds.get(f).realtime_poll_interval for f in ("kmk_a", "kmk_m", "kmk_t")} == {5}
    kolej = feeds.get("km_kolej")
    assert (kolej.realtime_poll_interval, kolej.realtime_max_age) == (30, 600)
    mld = feeds.get("km_mld")
    assert mld.realtime_poll_interval is None and mld.realtime_max_age is None
    assert feeds.searchable_namespaces == ["ztp"]
    # ZTP names stations in its codes (`802-01`), the KM feeds have GTFS stations
    assert {feeds.get(f).stop_code_pattern for f in ("kmk_a", "kmk_m", "kmk_t")} == {"(?P<station>.+)-(?P<platform_code>[^-]+)"}
    assert feeds.get("km_kolej").stop_code_pattern is None and mld.stop_code_pattern is None


def test_search_namespaces_default_to_all(tmp_path):
    text = "feeds:\n  - feed_id: b\n    static: https://x.test/b.zip\n  - feed_id: a\n    static: https://x.test/a.zip\n"
    assert load_feeds(write(tmp_path, text)).searchable_namespaces == ["a", "b"]


def test_namespace_defaults_to_feed_id(tmp_path):
    feeds = load_feeds(write(tmp_path, "feeds:\n  - feed_id: abc\n    static: https://x.test/a.zip\n"))
    assert feeds.get("abc").namespace == "abc"


@pytest.mark.parametrize(
    "text",
    [
        "feeds:\n  - feed_id: abc\n    static: not-a-url\n",
        "feeds:\n  - feed_id: Bad-Key\n    static: https://x.test/a.zip\n",
        "feeds:\n  - feed_id: a__b\n    static: https://x.test/a.zip\n",  # `__` separates feed and version
        "feeds:\n  - key: a\n    static: https://x.test/a.zip\n",  # old name of feed_id
        "feeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n  - feed_id: a\n    static: https://x.test/b.zip\n",
        "feeds:\n  - feed_id: a\n    static:\n      url: https://x.test/a.zip\n",  # no url subkey
        "feeds:\n  - feed_id: a\n    name: Buses\n    static: https://x.test/a.zip\n",  # no name field
        "feeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n    realtime:\n      positions: https://x.test/p.pb\n",
        "feeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n    realtime_poll_interval: 0\n",
        "feeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n    realtime_max_age: 0\n",
        "search_namespaces: [nope]\nfeeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n",
        "search_namespaces: []\nfeeds:\n  - feed_id: a\n    static: https://x.test/a.zip\n",
        "feeds:\n  - feed_id: a\n    stop_code_pattern: '(?P<station>.+'\n    static: https://x.test/a.zip\n",  # no regex
        "feeds:\n  - feed_id: a\n    stop_code_pattern: '(.+)-(.+)'\n    static: https://x.test/a.zip\n",  # no named groups
        "feeds:\n  - feed_id: a\n    stop_code_pattern: '(?P<group>.+)-(?P<platform_code>.+)'\n    static: https://x.test/a.zip\n",
        (  # feeds of one namespace share their codes, so they read them the same way
            "feeds:\n  - feed_id: a\n    stop_namespace: z\n    stop_code_pattern: '(?P<station>.+)-.+'\n"
            "    static: https://x.test/a.zip\n  - feed_id: b\n    stop_namespace: z\n    static: https://x.test/b.zip\n"
        ),
    ],
)
def test_invalid_feeds_are_rejected(tmp_path, text):
    with pytest.raises(ValidationError):
        load_feeds(write(tmp_path, text))


def test_disabled_feed_gets_no_tasks(tmp_path):
    feeds = load_feeds(
        write(
            tmp_path,
            (
                "feeds:\n"
                "  - feed_id: a\n    static: https://x.test/a.zip\n"
                "  - feed_id: b\n    enabled: false\n    static: https://x.test/b.zip\n"
            ),
        )
    )
    assert [f.feed_id for f in feeds.enabled] == ["a"]
    assert [f.feed_id for f in import_order(feeds)] == ["a"]


def test_import_order_puts_shared_stop_providers_first(tmp_path):
    feeds = load_feeds(
        write(
            tmp_path,
            (
                "feeds:\n"
                "  - feed_id: mld\n    share_stops_with: [ztp]\n    static: https://x.test/m.zip\n"
                "  - feed_id: train\n    static: https://x.test/t.zip\n"
                "  - feed_id: tram\n    stop_namespace: ztp\n    static: https://x.test/z.zip\n"
            ),
        )
    )
    assert [f.feed_id for f in import_order(feeds)] == ["tram", "train", "mld"]
