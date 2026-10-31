"""Redis writes (skipped if Redis isn't running)."""

import json

from conftest import TTLS
from redis.asyncio import Redis

from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.worker.realtime.normalize import Result
from gtfs_backend.worker.realtime.store import RealtimeStore, TTLs


def vehicle(vid, lon=19.96, lat=50.066):
    return {"vehicle_id": vid, "lat": lat, "lon": lon, "trip": {"key": f"kmk_t:{vid}:20261025"}}


def trip(key, *stops, version=1):
    """stops: (stop_sequence, stop_platform_id, predicted_departure)"""
    return {
        "key": key,
        "version": version,
        "stops": [
            {"stop_sequence": q, "stop_platform_id": pid, "predicted_departure": t, "predicted_arrival": None}
            for q, pid, t in stops
        ],
    }


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
    # trip a passed pa (the feed dropped it): kept as passed with its last time
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:a:20261025", (2, "pb", 3))]))
    assert sorted(await r.keys("rt:trip:*")) == ["rt:trip:kmk_a:a:20261025", "rt:trip:kmk_t:a:20261025"]
    stops = json.loads(await r.get("rt:trip:kmk_t:a:20261025"))["stops"]
    assert [(s["stop_platform_id"], s["passed"], s["predicted_departure"]) for s in stops] == [
        ("pa", True, 1),
        ("pb", False, 3),
    ]
    assert await r.keys("rt:stop:*") == []  # no derived departure index
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

    await store.touch("km_kolej", RealtimeKind.COMBINED)
    assert (await r.ttl(keys[3]), await r.ttl(keys[4])) == (200, 300)
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
