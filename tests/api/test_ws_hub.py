"""The WebSocket hub's bookkeeping (api/ws.py), without a server: which topics a change touches, slow clients."""

from types import SimpleNamespace

import pytest

from gtfs_backend.api.data.live import LiveState
from gtfs_backend.api.ws import Connection, Hub, TopicState, parse_topic, tile_bbox, tile_of

KEY = "kmk_t:20261025:t1"


class Stops:
    """catalog.stops: group g1 has poles p1, p2."""

    def find(self, key):
        return SimpleNamespace(id=key, kind="group" if key == "g1" else "stop") if key in ("g1", "p3") else None

    def public_stop_ids(self, stop):
        return ["p1", "p2"] if stop.id == "g1" else [stop.id]


def hub_with_live():
    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})
    live.ready = True
    live._set_vehicle("kmk_t:v1", "{}", {"lat": 50.06, "lon": 19.94, "trip": {"key": KEY}})
    live._set_trip(
        KEY,
        [1, False, "v1", 1000, False, 0, [[1, "p1", None, 10, 0, False, 10, False], [2, "p3", None, 20, 0, False, 20, False]]],
    )
    ctx = SimpleNamespace(catalog=SimpleNamespace(stops=Stops()), live_state=live)
    hub = Hub(ctx)
    hub._rebuild()
    return hub, live


def subscribe(hub, name):
    topic = parse_topic(hub.ctx, name)
    hub.topics[topic.name] = TopicState(topic)
    hub._index(topic, add=True)


def test_tiles_number_as_slippy_maps():
    x, y = tile_of(50.0614, 19.9366, 14)  # Kraków's main square
    assert (x, y) == (9099, 5552)  # y = floor((1 - ln(tan(lat) + sec(lat)) / pi) / 2 * 2^14) = floor(5552.2)
    box = tile_bbox(14, x, y)
    assert box.min_lon <= 19.9366 <= box.max_lon and box.min_lat <= 50.0614 <= box.max_lat


def test_a_moved_vehicle_touches_its_old_and_new_tile_and_its_trip():
    hub, live = hub_with_live()
    old = tile_of(50.06, 19.94, 15)
    new = tile_of(50.07, 19.94, 15)
    for name in (f"tile:15/{old[0]}/{old[1]}", f"tile:15/{new[0]}/{new[1]}", f"trip:{KEY}", "stop:g1", "stop:p3"):
        subscribe(hub, name)
    live._set_vehicle("kmk_t:v1", "{}", {"lat": 50.07, "lon": 19.94, "trip": {"key": KEY}})
    hub._mark({"vehicles": {"set": {"kmk_t:v1": {}}, "del": []}})
    assert hub.dirty == {f"tile:15/{old[0]}/{old[1]}", f"tile:15/{new[0]}/{new[1]}", f"trip:{KEY}", "stop:g1", "stop:p3"}


def test_a_changed_trip_touches_the_boards_of_its_old_and_new_stops():
    hub, live = hub_with_live()
    for name in ("stop:g1", "stop:p3", f"trip:{KEY}"):
        subscribe(hub, name)
    live._set_trip(KEY, [1, False, "v1", 1000, False, 0, [[1, "p2", None, 10, 0, False, 10, False]]])  # now only p2
    hub._mark({"trips": {"set": {KEY: []}, "del": []}})
    assert hub.dirty == {"stop:g1", "stop:p3", f"trip:{KEY}"}  # p3: it left that board
    hub.dirty.clear()
    hub._mark({"trips": {"set": {}, "del": ["kmk_t:20261025:other"]}})
    assert hub.dirty == set()  # nobody watches it


def test_alerts_touch_every_board_and_trip():
    hub, _ = hub_with_live()
    for name in ("stop:g1", f"trip:{KEY}", "tile:14/9099/5552"):
        subscribe(hub, name)
    hub._mark({"alerts": []})
    assert hub.dirty == {"stop:g1", f"trip:{KEY}"}


def test_unsubscribing_cleans_the_indexes():
    hub, _ = hub_with_live()
    conn = Connection(ws=None)
    for name in ("stop:g1", "tile:14/9099/5552"):
        subscribe(hub, name)
        hub.topics[name].subscribers.add(conn)
        conn.topics.add(name)
    assert hub._stop_topics == {"p1": {"stop:g1"}, "p2": {"stop:g1"}} and hub._zooms == {14: 1}
    hub.drop(conn)
    assert hub.topics == {} and hub._stop_topics == {} and hub._zooms == {} and conn.closed


@pytest.mark.parametrize("name", ["tile:9/1/1", "tile:14/99999/1", "tile:a/b/c", "stop:nope", "trip:bad", "x", "lines:1"])
def test_bad_topics_are_refused(name):
    hub, _ = hub_with_live()
    from gtfs_backend.api.ws import TopicError

    with pytest.raises(TopicError):
        parse_topic(hub.ctx, name)


def test_a_slow_client_gets_the_latest_message_per_topic():
    conn = Connection(ws=None)
    conn.push("stop:g1", "old board")
    conn.push("tile:14/1/1", "update 1")
    conn.push("stop:g1", "new board")
    assert conn._pending == {"stop:g1": "new board", "tile:14/1/1": "update 1"}
    assert conn.has_pending("tile:14/1/1")
