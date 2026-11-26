"""The WebSocket hub's bookkeeping (api/ws.py), without a server: which topics a change touches, slow clients."""

from types import SimpleNamespace

import pytest

from gtfs_backend.api.data.live import Changed
from gtfs_backend.api.ws import Connection, Hub, TopicState, parse_topic, tile_bbox, tile_of

KEY = "kmk_t:20261025:t1"


class Stops:
    """catalog.stops: group g1 has poles p1, p2."""

    def find(self, key):
        return SimpleNamespace(id=key, kind="group" if key == "g1" else "stop") if key in ("g1", "p3") else None

    def public_stop_ids(self, stop):
        return ["p1", "p2"] if stop.id == "g1" else [stop.id]


def make_hub():
    ctx = SimpleNamespace(catalog=SimpleNamespace(stops=Stops()))
    return Hub(ctx)


def subscribe(hub, name):
    topic = parse_topic(hub.ctx, name)
    hub.topics[topic.name] = TopicState(topic)
    hub._index(topic, add=True)


def test_tiles_number_as_slippy_maps():
    x, y = tile_of(50.0614, 19.9366, 14)  # Kraków's main square
    assert (x, y) == (9099, 5552)  # y = floor((1 - ln(tan(lat) + sec(lat)) / pi) / 2 * 2^14) = floor(5552.2)
    box = tile_bbox(14, x, y)
    assert box.min_lon <= 19.9366 <= box.max_lon and box.min_lat <= 50.0614 <= box.max_lat


def test_changes_mark_the_topics_they_touch():
    hub = make_hub()
    old, new = tile_of(50.06, 19.94, 15), tile_of(50.07, 19.94, 15)
    watched = [f"tile:15/{old[0]}/{old[1]}", f"tile:15/{new[0]}/{new[1]}", f"trip:{KEY}", "stop:g1", "stop:p3"]
    for name in [*watched, "tile:14/1/1"]:
        subscribe(hub, name)
    # a vehicle moved: its old and new tile, its trip, and that trip's boards (p1 is in group g1)
    hub._mark(Changed(positions=[(50.06, 19.94), (50.07, 19.94)], trips={KEY}, stops={"p1", "p3"}))
    assert hub.dirty == set(watched)
    hub.dirty.clear()
    hub._mark(Changed(trips={"kmk_t:20261025:other"}, stops={"p9"}))
    assert hub.dirty == set()  # nobody watches those
    hub._mark(Changed(alerts=True))
    assert hub.dirty == {f"trip:{KEY}", "stop:g1", "stop:p3"}  # every board and trip, no tile
    hub._mark(Changed(everything=True))
    assert hub.dirty == set(hub.topics)


def test_unsubscribing_cleans_the_indexes():
    hub = make_hub()
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
    hub = make_hub()
    from gtfs_backend.api.ws import TopicError

    with pytest.raises(TopicError):
        parse_topic(hub.ctx, name)


def test_a_slow_client_gets_the_latest_message_per_topic():
    conn = Connection(ws=None)
    conn.push("stop:g1", "old board")
    conn.push("tile:14/1/1", "update 1")
    conn.push("stop:g1", "new board")
    assert conn._pending == {"stop:g1": "new board", "tile:14/1/1": "update 1"}
