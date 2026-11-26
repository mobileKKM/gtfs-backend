"""The API's live state (api/data/live.py) follows the worker's change messages; Redis only at the start and on a gap."""

import asyncio
import random

import pytest
from conftest import TTLS
from redis.asyncio import Redis

from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.data.live import GRID_DEG, LiveState
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
    store = RealtimeStore(redis, ["kmk_t", "kmk_a"], TTLS)
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
        assert board.state("kmk_t:20261025:a").stop(2).predicted == 200 and board.state("kmk_t:20261025:a").stop(1) is None
        await poll(store, "kmk_t", "vehicle_positions", Result(vehicles=[vehicle("302")]))  # 735 left
        await until(lambda: live.vehicle("kmk_t:735") is None)
        await poll(store, "kmk_t", "trip_updates", Result())
        await until(lambda: not live.trips)
        assert live._at_stop == {}  # the index follows
    finally:
        await live.close()


async def test_a_missed_message_or_another_worker_loads_the_feed_again(redis):
    store = RealtimeStore(redis, ["kmk_t"], TTLS)
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
        v = vehicle(str(i), lat=rng.uniform(49.95, 50.15), lon=rng.uniform(19.75, 20.2))
        live._set_vehicle(f"kmk_t:{i}", str(i), v)
    for _ in range(50):
        lat, lon, h, w = rng.uniform(49.95, 50.1), rng.uniform(19.75, 20.1), rng.uniform(0, 0.08), rng.uniform(0, 0.1)
        box = Bbox(lon, lat, lon + w + 1e-9, lat + h + 1e-9)
        found = {
            live.vehicles[k][1]["vehicle_id"]
            for k in sorted(live.vehicles)
            if live.vehicles[k][0] in set(live.vehicle_values(box))
        }
        inside = {v["vehicle_id"] for _, v in live.vehicles.values() if box.contains(v["lat"], v["lon"])}
        assert inside <= found  # the cells the box touches: a superset, cut exactly by the caller
        assert all(abs(live.vehicles[f"kmk_t:{i}"][1]["lat"] - lat) < h + 2 * GRID_DEG for i in found)
    live._set_vehicle("kmk_t:0", "0", vehicle("0", lat=50.0, lon=19.9))  # moved: in one cell only
    assert sum("kmk_t:0" in keys for keys in live._grid.values()) == 1
    live._drop_vehicle("kmk_t:0")
    assert all("kmk_t:0" not in keys for keys in live._grid.values())
