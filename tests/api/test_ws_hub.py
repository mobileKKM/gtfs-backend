"""The WebSocket hub's bookkeeping (api/ws.py), without a server: which topics a change touches, what the vehicles
topic sends, slow clients."""

import json
from types import SimpleNamespace

import pytest

from gtfs_backend.api.data.live import Changed
from gtfs_backend.api.services import vehicles
from gtfs_backend.api.ws import VEHICLES, Connection, Hub, TopicState, parse_topic

KEY = "kmk_t:20261025:t1"


class Stops:
    """catalog.stops: group g1 has poles p1, p2."""

    def find(self, key):
        return SimpleNamespace(id=key, kind="group" if key == "g1" else "stop") if key in ("g1", "p3") else None

    def public_stop_ids(self, stop):
        return ["p1", "p2"] if stop.id == "g1" else [stop.id]


class Catalog:
    stops = Stops()

    async def ensure_fresh(self):
        pass


def make_hub():
    return Hub(SimpleNamespace(catalog=Catalog()))


def subscribe(hub, name, conn=None):
    topic = parse_topic(hub.ctx, name)
    state = hub.topics.setdefault(topic.name, TopicState(topic))
    hub._index(topic, add=True)
    if conn is not None:
        state.subscribers.add(conn)
        conn.topics.add(topic.name)
    return state


def test_changes_mark_the_topics_they_touch():
    hub = make_hub()
    watched = [VEHICLES, f"trip:{KEY}", "stop:g1", "stop:p3"]
    for name in watched:
        subscribe(hub, name)
    # a vehicle moved: the vehicles, its trip, and that trip's boards (p1 is in group g1)
    hub._mark(Changed(vehicles={"kmk_t:v1"}, trips={KEY}, stops={"p1", "p3"}))
    assert hub.dirty == set(watched)
    hub.dirty.clear()
    hub._mark(Changed(trips={"kmk_t:20261025:other"}, stops={"p9"}))
    assert hub.dirty == set()  # nobody watches those
    hub._mark(Changed(alerts=True))
    assert hub.dirty == {f"trip:{KEY}", "stop:g1", "stop:p3"}  # every board and trip
    hub._mark(Changed(everything=True))
    assert hub.dirty == set(hub.topics)


async def test_vehicles_send_a_snapshot_then_only_what_changed(monkeypatch):
    city = {"a": b'{"key":"a","lat":1}', "b": b'{"key":"b","lat":2}'}

    async def current(ctx):
        return 1000, dict(city)

    monkeypatch.setattr(vehicles, "city", current)
    hub, conn = make_hub(), Connection(ws=None)
    state = subscribe(hub, VEHICLES, conn)
    await hub._compute(state)
    first = json.loads(conn._pending.pop(VEHICLES))
    assert first["type"] == "snapshot"
    assert first["data"] == {"timestamp": 1000, "vehicles": [{"key": "a", "lat": 1}, {"key": "b", "lat": 2}]}

    city.update(a=b'{"key":"a","lat":1.5}', c=b'{"key":"c","lat":3}')
    del city["b"]
    await hub._compute(state)
    sent = json.loads(conn._pending.pop(VEHICLES))
    assert sent["type"] == "update"
    assert sent["data"] == {"timestamp": 1000, "upsert": [{"key": "a", "lat": 1.5}, {"key": "c", "lat": 3}], "remove": ["b"]}

    await hub._compute(state)  # nothing changed: nothing sent
    assert conn._pending == {}


async def test_a_client_behind_on_vehicles_gets_a_snapshot_instead_of_an_update(monkeypatch):
    city = {"a": b'{"key":"a","lat":1}'}

    async def current(ctx):
        return 1000, dict(city)

    monkeypatch.setattr(vehicles, "city", current)
    hub, conn = make_hub(), Connection(ws=None)
    state = subscribe(hub, VEHICLES, conn)
    await hub._compute(state)
    conn._pending.clear()  # the snapshot was sent
    city["a"] = b'{"key":"a","lat":2}'
    await hub._compute(state)  # an update, not sent yet
    city["b"] = b'{"key":"b","lat":3}'
    await hub._compute(state)  # applying only this one would miss the move of a
    sent = json.loads(conn._pending[VEHICLES])
    assert sent["type"] == "snapshot" and sent["data"]["vehicles"] == [{"key": "a", "lat": 2}, {"key": "b", "lat": 3}]


def test_unsubscribing_cleans_the_indexes():
    hub = make_hub()
    conn = Connection(ws=None)
    for name in ("stop:g1", VEHICLES):
        subscribe(hub, name, conn)
    assert hub._stop_topics == {"p1": {"stop:g1"}, "p2": {"stop:g1"}}
    hub.drop(conn)
    assert hub.topics == {} and hub._stop_topics == {} and conn.closed


@pytest.mark.parametrize("name", ["tile:14/1/1", "vehicles:x", "stop:nope", "trip:bad", "x", "lines:1"])
def test_bad_topics_are_refused(name):
    hub = make_hub()
    from gtfs_backend.api.ws import TopicError

    with pytest.raises(TopicError):
        parse_topic(hub.ctx, name)


def test_a_slow_client_gets_the_latest_message_per_topic():
    conn = Connection(ws=None)
    conn.push("stop:g1", "old board")
    conn.push(VEHICLES, "update 1")
    conn.push("stop:g1", "new board")
    assert conn._pending == {"stop:g1": "new board", VEHICLES: "update 1"}


async def test_subscribers_get_a_snapshot_once_a_failed_first_computation_succeeds(monkeypatch):
    calls = []

    async def current(ctx):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("redis down")
        return 1000, {"a": b'{"key":"a"}'}

    monkeypatch.setattr(vehicles, "city", current)
    hub, conn = make_hub(), Connection(ws=None)
    await hub.subscribe(conn, VEHICLES)
    assert conn._pending == {}  # nothing to send yet
    await hub._compute(hub.topics[VEHICLES])  # the next try (a change or the sweep)
    assert json.loads(conn._pending[VEHICLES])["type"] == "snapshot"
