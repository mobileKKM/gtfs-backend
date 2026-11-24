"""Redis writes (skipped if Redis isn't running)."""

import json

import pytest
import redis
from conftest import TTLS
from redis.asyncio import Redis

from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.worker.realtime.normalize import Result
from gtfs_backend.worker.realtime.store import RealtimeStore, TTLs


def vehicle(vid, lon=19.96, lat=50.066):
    return {"vehicle_id": vid, "lat": lat, "lon": lon, "trip": {"key": f"kmk_t:{vid}:20261025"}}


def trip(key, *stops, version=1, vehicle="V1", relationship="SCHEDULED"):
    """stops: (stop_sequence, public_stop_id, predicted_departure)"""
    return {
        "key": key,
        "version": version,
        "vehicle_id": vehicle,
        "timestamp": 1000,
        "schedule_relationship": relationship,
        "stops": [
            {
                "stop_sequence": q,
                "public_stop_id": pid,
                "predicted_departure": t,
                "predicted_arrival": None,
                "delay_s": 60,
                "schedule_relationship": "SCHEDULED",
            }
            for q, pid, t in stops
        ],
    }


async def departure_index(r, feed_id):
    """rt:tripstate and rt:stop:* of a feed, decoded."""
    states = {k: json.loads(v) for k, v in (await r.hgetall(f"rt:tripstate:{feed_id}")).items()}
    stops = {}
    for key in sorted(await r.keys(f"rt:stop:{feed_id}:*")):
        stops[key.rsplit(":", 1)[1]] = {f: json.loads(v) for f, v in (await r.hgetall(key)).items()}
    return states, stops


async def test_vehicles_replace_per_feed_and_unmatched_never_reach_the_map(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_a", "kmk_t"], TTLS)
    await store.write_vehicles(
        "kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")], unmatched_vehicles=[{"vehicle_id": "999"}])
    )
    await store.write_vehicles("kmk_a", Result(vehicles=[vehicle("PA1", lon=19.95)]))
    members = await r.geosearch("rt:veh:geo", longitude=19.9597, latitude=50.0658, radius=5, unit="km")
    assert sorted(members) == ["kmk_a:PA1", "kmk_t:302", "kmk_t:735"]
    assert json.loads(await r.get("rt:veh:kmk_t:735"))["vehicle_id"] == "735"

    # next tram poll: 302 is gone; the bus feed is untouched
    await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735")]))
    members = await r.geosearch("rt:veh:geo", longitude=19.9597, latitude=50.0658, radius=5, unit="km")
    assert sorted(members) == ["kmk_a:PA1", "kmk_t:735"]
    assert await r.exists("rt:veh:kmk_t:999") == 0
    await r.aclose()


async def test_trips_mirror_the_latest_update(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_a", "kmk_t"], TTLS)
    await store.write_trips(
        "kmk_t", Result(trips=[trip("kmk_t:a:20261025", (1, "pa", 1)), trip("kmk_t:b:20261025", (1, "pa", 2))])
    )
    # the same trip id in another feed is a different trip
    await store.write_trips("kmk_a", Result(trips=[trip("kmk_a:a:20261025", (1, "pc", 1))]))
    assert sorted(await r.keys("rt:trip:*")) == [
        "rt:trip:kmk_a:a:20261025",
        "rt:trip:kmk_t:a:20261025",
        "rt:trip:kmk_t:b:20261025",
    ]
    # trip b left the tram feed; the bus feed is untouched
    # trip a passed pa (the feed dropped it): kept with its last time
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:a:20261025", (2, "pb", 3))]))
    assert sorted(await r.keys("rt:trip:*")) == ["rt:trip:kmk_a:a:20261025", "rt:trip:kmk_t:a:20261025"]
    stops = json.loads(await r.get("rt:trip:kmk_t:a:20261025"))["stops"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in stops] == [("pa", 1), ("pb", 3)]
    # the departure index: every trip's state, and at each stop its trips' rows (the dropped stop pa included)
    assert await departure_index(r, "kmk_t") == (
        {"kmk_t:a:20261025": [1, False, "V1", 1000]},
        {"pa": {"1|kmk_t:a:20261025": [None, 1, 60, False]}, "pb": {"2|kmk_t:a:20261025": [None, 3, 60, False]}},
    )
    assert (await departure_index(r, "kmk_a"))[1] == {"pc": {"1|kmk_a:a:20261025": [None, 1, 60, False]}}
    await r.aclose()


async def test_the_departure_index_follows_the_trips(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    a, b = "kmk_t:20261025:a", "kmk_t:20261025:b"
    await store.write_trips("kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 20)), trip(b, (1, "pb", 30))]))
    # a's time at pb changed; b canceled, and the feed dropped its stop (kept with its last time, as in rt:trip)
    await store.write_trips("kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 25)), trip(b, relationship="CANCELED")]))
    states, stops = await departure_index(r, "kmk_t")
    assert states == {a: [1, False, "V1", 1000], b: [1, True, "V1", 1000]}
    assert stops == {
        "pa": {f"1|{a}": [None, 10, 60, False]},
        "pb": {f"2|{a}": [None, 25, 60, False], f"1|{b}": [None, 30, 60, False]},
    }
    # b left the feed; a new version of a: a whole new set of rows
    await store.write_trips("kmk_t", Result(trips=[trip(a, (5, "pc", 50), version=2)]))
    assert await departure_index(r, "kmk_t") == ({a: [2, False, "V1", 1000]}, {"pc": {f"5|{a}": [None, 50, 60, False]}})
    await store.write_trips("kmk_t", Result())  # nothing running
    assert await departure_index(r, "kmk_t") == ({}, {})
    await r.aclose()


async def test_the_departure_index_is_rewritten_after_a_restart(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    before = RealtimeStore(r, ["kmk_t"], TTLS)
    gone, on = trip("kmk_t:20261025:gone", (1, "pa", 1)), trip("kmk_t:20261025:on", (1, "pb", 2))
    await before.write_trips("kmk_t", Result(trips=[gone, on]))
    await r.hset("rt:stop:kmk_a:pa", "1|kmk_a:20261025:x", "[null,1,0,false]")  # another feed's

    after = RealtimeStore(r, ["kmk_t"], TTLS)  # restarted: it doesn't know what it wrote
    await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:on", (1, "pb", 3))]))
    assert await departure_index(r, "kmk_t") == (
        {"kmk_t:20261025:on": [1, False, "V1", 1000]},
        {"pb": {"1|kmk_t:20261025:on": [None, 3, 60, False]}},
    )
    assert await r.exists("rt:stop:kmk_a:pa") == 1  # other feeds are theirs to clean
    await r.aclose()


async def test_touch_refreshes_the_ttls_of_the_last_update(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["km_kolej"], TTLs(vehicle=100, trip=200, alert=300, unmatched=120))
    res = Result(vehicles=[vehicle("T1")], trips=[trip("km_kolej:x:20261025", (1, "pa", 1))], alerts=[])
    await store.write_vehicles("km_kolej", res)
    await store.write_trips("km_kolej", res)
    await store.write_alerts("km_kolej", res)
    keys = ["rt:veh:km_kolej:T1", "rt:veh:geo:km_kolej", "rt:veh:geo", "rt:trip:km_kolej:x:20261025", "rt:alerts:km_kolej"]
    for key in keys:
        await r.expire(key, 5)

    await store.touch("km_kolej", RealtimeKind.VEHICLE_POSITIONS)
    assert [await r.ttl(k) for k in keys[:3]] == [100, 100, 100]
    assert 0 < await r.ttl(keys[3]) <= 5 and 0 < await r.ttl(keys[4]) <= 5  # other kinds are untouched

    await r.expire("rt:tripstate:km_kolej", 5)
    await store.touch("km_kolej", RealtimeKind.COMBINED)
    assert (await r.ttl(keys[3]), await r.ttl(keys[4])) == (200, 300)
    assert await r.ttl("rt:tripstate:km_kolej") == 200  # boards ignore predictions of trips without a state
    await r.aclose()


async def test_trips_left_from_before_a_restart_are_removed(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    before = RealtimeStore(r, ["kmk_t"], TTLS)
    await before.write_trips(
        "kmk_t", Result(trips=[trip("kmk_t:20261025:gone", (1, "p", 100)), trip("kmk_t:20261025:on", (1, "p", 100))])
    )
    await r.set("rt:trip:kmk_a:20261025:other_feed", "{}")

    after = RealtimeStore(r, ["kmk_t"], TTLS)  # the worker restarted: it knows nothing it wrote
    await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:on", (1, "p", 100))]))
    assert await r.exists("rt:trip:kmk_t:20261025:gone") == 0
    assert await r.exists("rt:trip:kmk_t:20261025:on") == 1
    assert await r.exists("rt:trip:kmk_a:20261025:other_feed") == 1  # other feeds are theirs to clean
    await r.aclose()


async def test_vehicles_that_left_the_feed_are_removed(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")]))
    await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735")]))
    assert await r.exists("rt:veh:kmk_t:302") == 0  # not shown until its TTL runs out
    assert await r.exists("rt:veh:kmk_t:735") == 1
    await store.write_vehicles("kmk_t", Result())
    assert await r.exists("rt:veh:kmk_t:735") == 0 and await r.exists("rt:veh:geo:kmk_t") == 0
    await r.aclose()


async def test_stops_dropped_before_a_restart_are_kept(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    before = RealtimeStore(r, ["kmk_t"], TTLS)
    await before.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1), (2, "pb", 2))]))

    after = RealtimeStore(r, ["kmk_t"], TTLS)  # restarted: the last update is only in Redis
    await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (3, "pc", 4))]))
    stops = json.loads(await r.get("rt:trip:kmk_t:20261025:a"))["stops"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in stops] == [("pa", 1), ("pb", 3), ("pc", 4)]
    await r.aclose()


async def test_a_failed_trip_write_is_merged_again_from_the_last_written_one(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1), (2, "pb", 2))]))
    store.redis = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)  # Redis unreachable
    with pytest.raises(redis.exceptions.ConnectionError):  # the poller records it and processes the update again
        await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    await store.redis.aclose()
    store.redis = r
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    stops = json.loads(await r.get("rt:trip:kmk_t:20261025:a"))["stops"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in stops] == [("pa", 1), ("pb", 3)]
    # the index too, although the failed write may have changed part of it
    assert (await departure_index(r, "kmk_t"))[1] == {
        "pa": {"1|kmk_t:20261025:a": [None, 1, 60, False]},
        "pb": {"2|kmk_t:20261025:a": [None, 3, 60, False]},
    }
    await r.aclose()
