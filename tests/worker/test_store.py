"""Redis writes (skipped if Redis isn't running)."""

import json

import msgspec
import pytest
import redis
from conftest import TTLS
from redis.asyncio import Redis

from gtfs_backend.feeds import RealtimeKind
from gtfs_backend.live_layout import StopState, TripState
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
        "delay_s": 60,
        "schedule_relationship": relationship,
        "stops": [
            {
                "stop_sequence": q,
                "public_stop_id": pid,
                "predicted_departure": t,
                "predicted_arrival": None,
                "scheduled_departure": t - 60,
                "scheduled_arrival": t - 60,
                "delay_s": 60,
                "schedule_relationship": "SCHEDULED",
                "propagated": q > 1,  # only the first stop's time from the feed
            }
            for q, pid, t in stops
        ],
    }


async def stored(r, key):
    value = await r.get(key)
    return json.loads(value) if value else None


async def test_vehicles_are_one_value_per_feed(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    await store.write_vehicles(
        "kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")], unmatched_vehicles=[{"vehicle_id": "999"}])
    )
    await store.write_vehicles("kmk_a", Result(vehicles=[vehicle("PA1", lon=19.95)]))
    assert sorted(await stored(r, "rt:vehicles:kmk_t")) == ["kmk_t:302", "kmk_t:735"]  # unmatched never reach the map
    assert (await stored(r, "rt:vehicles:kmk_t"))["kmk_t:735"]["vehicle_id"] == "735"

    # next tram poll: 302 is gone; the bus feed is untouched
    await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735")]))
    assert list(await stored(r, "rt:vehicles:kmk_t")) == ["kmk_t:735"]
    assert list(await stored(r, "rt:vehicles:kmk_a")) == ["kmk_a:PA1"]
    await store.write_vehicles("kmk_t", Result())
    assert await stored(r, "rt:vehicles:kmk_t") == {}
    await r.aclose()


async def test_trips_mirror_the_latest_update(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    await store.write_trips(
        "kmk_t", Result(trips=[trip("kmk_t:a:20261025", (1, "pa", 1)), trip("kmk_t:b:20261025", (1, "pa", 2))])
    )
    # the same trip id in another feed is a different trip
    await store.write_trips("kmk_a", Result(trips=[trip("kmk_a:a:20261025", (1, "pc", 1))]))
    assert sorted(await stored(r, "rt:trips:kmk_t")) == ["kmk_t:a:20261025", "kmk_t:b:20261025"]
    # trip b left the tram feed; the bus feed is untouched
    # trip a passed pa (the feed dropped it): kept with its last time
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:a:20261025", (2, "pb", 3))]))
    trips = await stored(r, "rt:trips:kmk_t")
    assert list(trips) == ["kmk_t:a:20261025"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in trips["kmk_t:a:20261025"]["stops"]] == [
        ("pa", 1),
        ("pb", 3),
    ]
    assert list(await stored(r, "rt:trips:kmk_a")) == ["kmk_a:a:20261025"]
    await r.aclose()


async def test_changes_follow_the_trips(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    a, b = "kmk_t:20261025:a", "kmk_t:20261025:b"
    changes = await store.write_trips("kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 20)), trip(b, (1, "pb", 30))]))
    assert sorted(changes["set"]) == [a, b] and changes["del"] == []
    stops = [StopState(1, "pa", None, 10, 60, False, -50, False, -50), StopState(2, "pb", None, 20, 60, False, -40, True, -40)]
    assert changes["set"][a] == TripState(1, False, False, "V1", 1000, 60, stops)  # no extra: a timetabled trip
    # a's time at pb changed; b canceled, and the feed dropped its stop (kept with its last time)
    changes = await store.write_trips(
        "kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 25)), trip(b, relationship="CANCELED")])
    )
    assert sorted(changes["set"]) == [a, b]
    assert changes["set"][b].canceled and changes["set"][b].stops[0].predicted_departure == 30
    # nothing changed: nothing to say
    same = await store.write_trips(
        "kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 25)), trip(b, relationship="CANCELED")])
    )
    assert same == {"set": {}, "del": []}
    # b left the feed
    changes = await store.write_trips("kmk_t", Result(trips=[trip(a, (5, "pc", 50), version=2)]))
    assert list(changes["set"]) == [a] and changes["set"][a].version == 2 and changes["del"] == [b]
    assert await store.write_trips("kmk_t", Result()) == {"set": {}, "del": [a]}  # nothing running
    assert await stored(r, "rt:trips:kmk_t") == {}
    await r.aclose()


async def test_touch_refreshes_the_ttls_of_the_last_update(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLs(vehicle=100, trip=200, alert=300, unmatched=120))
    res = Result(vehicles=[vehicle("T1")], trips=[trip("km_kolej:x:20261025", (1, "pa", 1))], alerts=[])
    await store.write_vehicles("km_kolej", res)
    await store.write_trips("km_kolej", res)
    assert await store.write_alerts("km_kolej", res) == {"timestamp": 0, "alerts": []}
    keys = ["rt:vehicles:km_kolej", "rt:trips:km_kolej", "rt:alerts:km_kolej"]
    for key in keys:
        await r.expire(key, 5)

    await store.touch("km_kolej", RealtimeKind.VEHICLE_POSITIONS)
    assert await r.ttl(keys[0]) == 100
    assert 0 < await r.ttl(keys[1]) <= 5 and 0 < await r.ttl(keys[2]) <= 5  # other kinds are untouched

    await store.touch("km_kolej", RealtimeKind.COMBINED)
    assert (await r.ttl(keys[1]), await r.ttl(keys[2])) == (200, 300)
    await r.aclose()


async def test_after_a_restart_the_last_trips_come_from_redis(redis_url):
    """The worker restarted: stops the feed dropped before are kept, and trips that left since are said to be gone."""
    r = Redis.from_url(redis_url, decode_responses=True)
    before = RealtimeStore(r, TTLS)
    await before.write_trips(
        "kmk_t", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1), (2, "pb", 2)), trip("kmk_t:20261025:gone", (1, "p", 9))])
    )

    after = RealtimeStore(r, TTLS)
    changes = await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    assert changes["del"] == ["kmk_t:20261025:gone"]
    await after.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (3, "pc", 4))]))
    stops = (await stored(r, "rt:trips:kmk_t"))["kmk_t:20261025:a"]["stops"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in stops] == [("pa", 1), ("pb", 3), ("pc", 4)]
    await r.aclose()


async def test_a_failed_trip_write_is_merged_again_from_the_last_written_one(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1), (2, "pb", 2))]))
    store.redis = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)  # Redis unreachable
    with pytest.raises(redis.exceptions.ConnectionError):  # the poller records it and processes the update again
        await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    await store.redis.aclose()
    store.redis = r
    changes = await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (2, "pb", 3))]))
    stops = (await stored(r, "rt:trips:kmk_t"))["kmk_t:20261025:a"]["stops"]
    assert [(s["public_stop_id"], s["predicted_departure"]) for s in stops] == [("pa", 1), ("pb", 3)]
    assert list(changes["set"]) == ["kmk_t:20261025:a"]  # the failed one was never said
    await r.aclose()


async def test_vehicle_changes_and_published_messages(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    first = await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")]))
    assert sorted(first["set"]) == ["kmk_t:302", "kmk_t:735"] and first["del"] == []
    moved = await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735", lon=19.97)]))
    assert list(moved["set"]) == ["kmk_t:735"] and moved["del"] == ["kmk_t:302"]
    assert await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735", lon=19.97)])) == {"set": {}, "del": []}
    trips = await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1))]))

    async with r.pubsub() as ps:
        await ps.subscribe("rt:updated")
        await ps.get_message(timeout=1)  # the subscription
        await store.publish("kmk_t", "vehicle_positions", 1000, {"vehicles": moved})
        await store.publish("kmk_t", "vehicle_positions", 1005, {"vehicles": {"set": {}, "del": []}})
        await store.publish("kmk_t", "trip_updates", 1005, {"trips": trips})
        messages = [json.loads((await ps.get_message(timeout=1))["data"]) for _ in range(3)]
    assert [(m["feed_id"], m["kind"], m["seq"], m["ts"]) for m in messages] == [
        ("kmk_t", "vehicle_positions", 1, 1000),
        ("kmk_t", "vehicle_positions", 2, 1005),
        ("kmk_t", "trip_updates", 1, 1005),
    ]
    assert messages[0]["run"] == store.run and messages[0]["vehicles"]["del"] == ["kmk_t:302"]
    # trip states travel as arrays, and come back as TripStates
    sent = messages[2]["trips"]["set"]["kmk_t:20261025:a"]
    assert msgspec.convert(sent, TripState) == trips["set"]["kmk_t:20261025:a"]
    assert await r.get("rt:seq:kmk_t:vehicle_positions") == f"{store.run}:2"
    await r.aclose()


async def test_a_lost_write_makes_the_next_message_skip_a_number(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, TTLS)
    await store.publish("kmk_t", "vehicle_positions", 1000)
    store.lost("kmk_t", "vehicle_positions")
    await store.publish("kmk_t", "vehicle_positions", 1010)
    assert await r.get("rt:seq:kmk_t:vehicle_positions") == f"{store.run}:3"  # readers that had 1 load the feed
    await r.aclose()
