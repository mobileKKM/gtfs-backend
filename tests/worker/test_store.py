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


async def trip_states(r, feed_id):
    """rt:tripstate of a feed, decoded."""
    return {k: json.loads(v) for k, v in (await r.hgetall(f"rt:tripstate:{feed_id}")).items()}


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
    assert await trip_states(r, "kmk_t") == {"kmk_t:a:20261025": [1, False, "V1", 1000, False]}
    assert await r.keys("rt:stop:*") == []  # nothing reads per-stop hashes any more: the API keeps trips in memory
    await r.aclose()


async def test_trip_states_and_changes_follow_the_trips(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    a, b = "kmk_t:20261025:a", "kmk_t:20261025:b"
    changes = await store.write_trips("kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 20)), trip(b, (1, "pb", 30))]))
    assert sorted(changes["set"]) == [a, b] and changes["del"] == []
    stops = [[1, "pa", None, 10, 60, False, -50, False, -50], [2, "pb", None, 20, 60, False, -40, True, -40]]
    assert changes["set"][a] == [1, False, "V1", 1000, False, 60, stops, None]  # no extra: a timetabled trip
    # a's time at pb changed; b canceled, and the feed dropped its stop (kept with its last time, as in rt:trip)
    changes = await store.write_trips(
        "kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 25)), trip(b, relationship="CANCELED")])
    )
    assert await trip_states(r, "kmk_t") == {a: [1, False, "V1", 1000, False], b: [1, True, "V1", 1000, False]}
    assert sorted(changes["set"]) == [a, b] and changes["set"][b][1] is True and changes["set"][b][6][0][3] == 30
    # nothing changed: nothing to say
    same = await store.write_trips(
        "kmk_t", Result(trips=[trip(a, (1, "pa", 10), (2, "pb", 25)), trip(b, relationship="CANCELED")])
    )
    assert same == {"set": {}, "del": []}
    # b left the feed
    changes = await store.write_trips("kmk_t", Result(trips=[trip(a, (5, "pc", 50), version=2)]))
    assert list(changes["set"]) == [a] and changes["del"] == [b]
    assert await trip_states(r, "kmk_t") == {a: [2, False, "V1", 1000, False]}
    await store.write_trips("kmk_t", Result())  # nothing running
    assert await trip_states(r, "kmk_t") == {}
    await r.aclose()


async def test_per_stop_hashes_of_older_workers_are_removed(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    await r.hset("rt:stop:kmk_t:pa", "1|kmk_t:20261025:x", "[null,1,0,false]")  # what an older worker wrote
    await r.hset("rt:stop:kmk_a:pa", "1|kmk_a:20261025:x", "[null,1,0,false]")  # another feed's
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    await store.write_trips("kmk_t", Result(trips=[trip("kmk_t:20261025:on", (1, "pb", 3))]))
    assert await r.exists("rt:stop:kmk_t:pa") == 0
    assert await r.exists("rt:stop:kmk_a:pa") == 1  # other feeds are theirs to clean
    await r.aclose()


async def test_trip_states_come_back_when_redis_lost_them(redis_url, monkeypatch):
    """rt:tripstate expires in an outage (or with a file older than its TTL), and Redis can lose everything: the next
    update writes it again, unchanged trips (a canceled one) included."""
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    update = Result(trips=[trip("kmk_t:20261025:a", (1, "pa", 1)), trip("kmk_t:20261025:c", relationship="CANCELED")])
    await store.write_trips("kmk_t", update)
    written = await trip_states(r, "kmk_t")
    assert written["kmk_t:20261025:c"] == [1, True, "V1", 1000, False]

    await r.delete("rt:tripstate:kmk_t")  # expired
    await store.write_trips("kmk_t", update)
    assert await trip_states(r, "kmk_t") == written

    # expired right after the check: the states are written in full anyway
    await r.delete("rt:tripstate:kmk_t")
    monkeypatch.setattr(r, "exists", lambda *keys: _one())
    await store.write_trips("kmk_t", update)
    assert await trip_states(r, "kmk_t") == written
    await r.aclose()


async def _one() -> int:
    return 1


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
    assert await r.ttl("rt:tripstate:km_kolej") == 200  # what the API loads a feed's trips by
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
    assert await trip_states(r, "kmk_t") == {"kmk_t:20261025:a": [1, False, "V1", 1000, False]}
    await r.aclose()


async def test_vehicle_changes_and_published_messages(redis_url):
    r = Redis.from_url(redis_url, decode_responses=True)
    store = RealtimeStore(r, ["kmk_t"], TTLS)
    first = await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735"), vehicle("302")]))
    assert sorted(first["set"]) == ["kmk_t:302", "kmk_t:735"] and first["del"] == []
    moved = await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735", lon=19.97)]))
    assert list(moved["set"]) == ["kmk_t:735"] and moved["del"] == ["kmk_t:302"]
    assert await store.write_vehicles("kmk_t", Result(vehicles=[vehicle("735", lon=19.97)])) == {"set": {}, "del": []}

    async with r.pubsub() as ps:
        await ps.subscribe("rt:updated")
        await ps.get_message(timeout=1)  # the subscription
        await store.publish("kmk_t", "vehicle_positions", 1000, {"vehicles": moved})
        await store.publish("kmk_t", "vehicle_positions", 1005, {"vehicles": {"set": {}, "del": []}})
        messages = [json.loads((await ps.get_message(timeout=1))["data"]) for _ in range(2)]
    assert [(m["feed_id"], m["kind"], m["seq"], m["ts"]) for m in messages] == [
        ("kmk_t", "vehicle_positions", 1, 1000),
        ("kmk_t", "vehicle_positions", 2, 1005),
    ]
    assert messages[0]["run"] == store.run and messages[0]["vehicles"]["del"] == ["kmk_t:302"]
    assert await r.get("rt:seq:kmk_t:vehicle_positions") == f"{store.run}:2"
    await r.aclose()
