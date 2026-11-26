"""The API's live state (api/data/live.py) follows the worker's change messages; Redis only at the start and on a gap."""

import asyncio
import random

import pytest
from conftest import TTLS
from redis.asyncio import Redis

from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.data.live import GRID_DEG, Changed, LiveState
from gtfs_backend.worker.realtime.normalize import Result
from gtfs_backend.worker.realtime.store import RealtimeStore

KINDS = {"kmk_t": ["vehicle_positions", "trip_updates"], "kmk_a": ["vehicle_positions"]}


def vehicle(vid, lat=50.06, lon=19.94, ts=1000):
    return {
        "vehicle_id": vid,
        "lat": lat,
        "lon": lon,
        "timestamp": ts,
        "feed_id": "kmk_t",
        "trip": {"key": f"kmk_t:20261025:{vid}"},
    }


def trip(key, *stops, relationship="SCHEDULED"):
    return {
        "key": key,
        "version": 1,
        "vehicle_id": "V1",
        "timestamp": 1000,
        "delay_s": 60,
        "schedule_relationship": relationship,
        "stops": [
            {
                "stop_sequence": q,
                "public_stop_id": pid,
                "predicted_arrival": None,
                "predicted_departure": t,
                "scheduled_departure": t - 60,
                "scheduled_arrival": t - 60,
                "delay_s": 60,
                "schedule_relationship": "SCHEDULED",
                "propagated": False,
            }
            for q, pid, t in stops
        ],
    }


async def until(condition, seconds=3.0):
    for _ in range(int(seconds / 0.02)):
        if condition():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("not reached")


@pytest.fixture
async def redis(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    yield r
    await r.aclose()


async def poll(store, feed_id, kind, res):
    """One poll of a file, as the poller writes and publishes it."""
    changes = {}
    if kind == "vehicle_positions":
        changes["vehicles"] = await store.write_vehicles(feed_id, res)
    else:
        changes["trips"] = await store.write_trips(feed_id, res)
    await store.publish(feed_id, kind, 1000, changes)


async def test_follows_the_workers_changes(redis):
    store = RealtimeStore(redis, TTLS)
    await poll(store, "kmk_t", "vehicle_positions", Result(vehicles=[vehicle("735")]))  # before the API starts
    live = LiveState(redis, ["kmk_t", "kmk_a"], KINDS)
    live.start()
    try:
        assert await live.wait_ready(3)
        assert live.vehicle("kmk_t:735")["lat"] == 50.06  # loaded from Redis
        await poll(store, "kmk_t", "vehicle_positions", Result(vehicles=[vehicle("735", lat=50.07), vehicle("302")]))
        await until(lambda: live.vehicle("kmk_t:302") is not None)
        assert live.vehicle("kmk_t:735")["lat"] == 50.07
        await poll(store, "kmk_t", "trip_updates", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 100), (2, "pb", 200))]))
        await until(lambda: "kmk_t:20261025:a" in live.trips)
        board = live.board(["kmk_t"], ["pb"], {})
        assert board.trip("kmk_t:20261025:a").version == 1
        assert board.stop("kmk_t:20261025:a", 2).predicted == 200 and board.stop("kmk_t:20261025:a", 1) is None
        await poll(store, "kmk_t", "vehicle_positions", Result(vehicles=[vehicle("302")]))  # 735 left
        await until(lambda: live.vehicle("kmk_t:735") is None)
        await poll(store, "kmk_t", "trip_updates", Result())
        await until(lambda: not live.trips)
        assert live._at_stop == {}  # the index follows
    finally:
        await live.close()


async def test_a_missed_message_or_another_worker_loads_the_feed_again(redis):
    store = RealtimeStore(redis, TTLS)
    await poll(store, "kmk_t", "vehicle_positions", Result(vehicles=[vehicle("735")]))
    live = LiveState(redis, ["kmk_t"], KINDS)
    await live.load()
    seq = live._seq[("kmk_t", "vehicle_positions")]
    # a message lost: the next one has the number after it
    await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")]))
    store._seq[("kmk_t", "vehicle_positions")] += 1  # one published while the API wasn't listening
    message = {
        "run": store.run,
        "feed_id": "kmk_t",
        "kind": "vehicle_positions",
        "seq": seq[1] + 2,
        "ts": 0,
        "vehicles": {"set": {}, "del": []},
    }
    assert live.apply(message) is False
    await live.load_feed("kmk_t")
    assert live.vehicle("kmk_t:302") is not None
    # another worker run (a restart) numbers from 1: loaded again too
    assert live.apply({**message, "run": "other", "seq": 1}) is False
    # an old message, already in what was loaded: nothing to do
    assert live.apply({**message, "seq": seq[1]}) is True


def test_boxes_find_exactly_the_vehicles_inside():
    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})
    rng = random.Random(5)
    for i in range(500):
        live._set_vehicle(f"kmk_t:{i}", vehicle(str(i), lat=rng.uniform(49.95, 50.15), lon=rng.uniform(19.75, 20.2)), Changed())
    for _ in range(50):
        lat, lon, h, w = rng.uniform(49.95, 50.1), rng.uniform(19.75, 20.1), rng.uniform(0, 0.08), rng.uniform(0, 0.1)
        box = Bbox(lon, lat, lon + w + 1e-9, lat + h + 1e-9)
        found = {v["vehicle_id"] for _, v in live.vehicles_in(box)}
        inside = {v["vehicle_id"] for v in live.vehicles.values() if box.contains(v["lat"], v["lon"])}
        assert inside <= found  # the cells the box touches: a superset, cut exactly by the caller
        assert all(abs(live.vehicles[f"kmk_t:{i}"]["lat"] - lat) < h + 2 * GRID_DEG for i in found)
    live._set_vehicle("kmk_t:0", vehicle("0", lat=50.0, lon=19.9), Changed())  # moved: in one cell only
    assert sum("kmk_t:0" in keys for keys in live._grid.values()) == 1
    live._drop_vehicle("kmk_t:0", Changed())
    assert all("kmk_t:0" not in keys for keys in live._grid.values())


def test_changes_say_what_they_touched():
    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})
    told = []
    live.listeners.append(told.append)
    base = {"run": "r", "feed_id": "kmk_t", "kind": "vehicle_positions", "ts": 0}
    a = [1, False, False, "735", 1000, 0, [[1, "pa", None, 10, 0, False, 10, False, 10]]]
    live.apply({**base, "kind": "trip_updates", "seq": 1, "trips": {"set": {"kmk_t:20261025:735": a}, "del": []}})
    assert told[-1] == Changed(trips={"kmk_t:20261025:735"}, stops={"pa"})
    live.apply({**base, "seq": 1, "vehicles": {"set": {"kmk_t:735": vehicle("735")}, "del": []}})
    assert told[-1] == Changed(vehicles={"kmk_t:735"}, trips={"kmk_t:20261025:735"}, stops={"pa"})  # its trip's boards
    live.apply({**base, "seq": 2, "vehicles": {"set": {"kmk_t:735": vehicle("735", lat=50.07)}, "del": []}})
    assert told[-1].vehicles == {"kmk_t:735"}
    b = [1, False, False, "735", 1000, 0, [[1, "pb", None, 10, 0, False, 10, False, 10]]]
    live.apply({**base, "kind": "trip_updates", "seq": 2, "trips": {"set": {"kmk_t:20261025:735": b}, "del": []}})
    assert told[-1].stops == {"pa", "pb"}  # it left pa's board
    live.apply({**base, "kind": "alerts", "seq": 1, "alerts": []})
    assert told[-1] == Changed(alerts=True)


def test_alerts_are_hidden_once_older_than_the_feeds_max_age():
    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})
    alert = {"id": "a1"}
    live.apply({"run": "r", "feed_id": "kmk_t", "kind": "service_alerts", "seq": 1, "ts": 1000,
                "alerts": {"timestamp": 1000, "alerts": [alert]}})  # fmt: skip
    assert live.alerts_of(["kmk_t"], {"kmk_t": 1000}) == [alert]
    assert live.alerts_of(["kmk_t"], {"kmk_t": 1001}) == []  # the worker or the feed stopped


def test_a_board_keeps_the_trips_it_started_with():
    from gtfs_backend.live_layout import StopState, TripState

    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})
    key = "kmk_t:20261025:extra"
    trip = TripState(1, False, False, None, 1000, None, [StopState(1, "p", 10, 10, None, False, 10, False, 10)], ("r", "h"))
    live._set_trip(key, trip, Changed())
    board = live.board(["kmk_t"], ["p"], {})
    live._drop_trip(key, Changed())  # a message applied while the board awaits its timetable
    assert [k for k, _, _ in board.extra_departures(0, 100)] == [key]
    assert board.trip(key) is trip


async def test_vehicle_pieces_survive_a_change_while_their_routes_load():
    from types import SimpleNamespace

    from gtfs_backend.api.schemas import RouteRef
    from gtfs_backend.api.services import vehicles

    live = LiveState(redis=None, feed_ids=["kmk_t"], kinds={})

    def raw(vid, lat):
        return {**vehicle(vid, lat=lat), "trip": {"key": f"kmk_t:20261025:{vid}", "version": 1, "route_id": "r1"}}

    live._set_vehicle("kmk_t:1", raw("1", 50.0), Changed())
    live._set_vehicle("kmk_t:2", raw("2", 50.0), Changed())

    async def route_refs(wanted):
        return {k: RouteRef(key="kmk_t:r1", name="1", mode="tram") for k in wanted}

    ctx = SimpleNamespace(live=live, catalog=SimpleNamespace(route_refs=route_refs))
    await vehicles._pieces(ctx, [("kmk_t:1", live.vehicles["kmk_t:1"])])  # 1 is kept

    async def slow_route_refs(wanted):
        # while the routes load, both vehicles move
        live._set_vehicle("kmk_t:1", raw("1", 50.1), Changed())
        live._set_vehicle("kmk_t:2", raw("2", 50.1), Changed())
        return await route_refs(wanted)

    ctx.catalog.route_refs = slow_route_refs
    live._set_vehicle("kmk_t:2", raw("2", 50.05), Changed())  # 2 isn't kept: built in this request
    found = [("kmk_t:1", live.vehicles["kmk_t:1"]), ("kmk_t:2", live.vehicles["kmk_t:2"])]
    pieces = await vehicles._pieces(ctx, found)
    assert [p.lat for p in pieces] == [50.0, 50.05]  # what the request started with, no KeyError
    assert "kmk_t:2" not in live.vehicle_pieces  # built from a position that is no longer the current one
