"""The HTTP API against the 2026-10-25 snapshot: trimmed ZTP timetables in Postgres, the realtime snapshot in Redis.

The data is loaded once per module; the API's clock is frozen at the tram feed's snapshot time.
"""

from __future__ import annotations

import asyncio
import json

import psycopg
import pytest
import redis
from conftest import FIXTURES, TEST_REDIS_URL, TTLS, do_import, message, reset_db
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from gtfs_backend.api.app import create_app
from gtfs_backend.api.settings import ApiSettings
from gtfs_backend.db import async_pool
from gtfs_backend.feeds import FeedConfig, FeedsFile
from gtfs_backend.worker.feed_sync import sync_feeds
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message
from gtfs_backend.worker.realtime.store import RealtimeStore
from gtfs_backend.worker.realtime.trip_index import ensure_stop_times, load_index

SUFFIX = {"kmk_a": "A", "kmk_m": "M", "kmk_t": "T"}
KINDS = {"TripUpdates": "trip_updates", "VehiclePositions": "vehicle_positions", "ServiceAlerts": "service_alerts"}
FEEDS = FeedsFile(
    feeds=[
        FeedConfig(
            feed_id=feed_id,
            stop_namespace="ztp",
            static=f"https://example.invalid/{s}.zip",
            realtime={kind: f"https://example.invalid/{name}_{s}.pb" for name, kind in KINDS.items()},
            day_start="midnight" if feed_id == "kmk_t" else "noon_minus_12h",
        )
        for feed_id, s in SUFFIX.items()
    ]
)
NOW = message("TripUpdates", "T").header.timestamp


async def _load_realtime(database_url: str) -> None:
    pool = async_pool(database_url)
    await pool.open()
    r = Redis.from_url(TEST_REDIS_URL)
    store = RealtimeStore(r, list(SUFFIX), TTLS)
    try:
        for f in FEEDS.feeds:
            index = await load_index(pool, f.feed_id, NOW, f.day_start)
            state = FeedRealtimeState()

            async def load(keys, index=index):
                await ensure_stop_times(pool, index, keys)

            for kind in KINDS:
                msg = message(kind, SUFFIX[f.feed_id])
                res = await process_message(f.feed_id, msg, index, state, load, NOW)
                if kind == "TripUpdates":
                    await store.write_trips(f.feed_id, res)
                elif kind == "VehiclePositions":
                    await store.write_vehicles(f.feed_id, res)
                else:
                    await store.write_alerts(f.feed_id, res)
                await store.write_meta(f.feed_id, KINDS[kind], {"ok": True, "age_s": 3})
    finally:
        await r.aclose()
        await pool.close()


@pytest.fixture(scope="module")
def loaded(database_url):
    try:
        r = redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=2)
        r.ping()
        r.flushdb()
        r.close()
    except redis.exceptions.ConnectionError as exc:
        pytest.skip(f"Redis not available ({exc})")
    reset_db(database_url)
    with psycopg.connect(database_url) as conn:
        sync_feeds(conn, FEEDS)
        for feed_id, name in (("kmk_t", "GTFS_KRK_T.zip"), ("kmk_a", "GTFS_KRK_A.zip"), ("kmk_m", "GTFS_KRK_M.zip")):
            do_import(conn, feed_id, FIXTURES / "static" / name)
        conn.commit()
    asyncio.run(_load_realtime(database_url))
    return database_url


# Most tests change Redis between requests and want to see it at once: the response cache is off for them.
NO_CACHE = {"cache_ttl_live": 0, "cache_ttl_static": 0, "cache_ttl_alerts": 0}


@pytest.fixture
async def api(loaded):
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, **NO_CACHE)
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        yield c


@pytest.fixture(scope="module")
def sql(loaded):
    def run(query: str, *params):
        with psycopg.connect(loaded) as conn:
            return conn.execute(query, params).fetchall()

    return run


def group_id(sql, code: str) -> str:
    return sql("SELECT id FROM core.stop_group WHERE current_code = %s", code)[0][0]


def platform_id(sql, code: str) -> str:
    return sql("SELECT id FROM core.stop_platform WHERE current_code = %s", code)[0][0]


# ------------------------------------------------------------------------------------------------- vehicles

RONDO_BBOX = "19.95,50.06,19.97,50.07"
CITY_BBOX = "19.8,49.95,20.2,50.15"


async def test_vehicles_in_bbox(api):
    r = await api.get(f"/vehicles?bbox={CITY_BBOX}")
    assert r.status_code == 200
    body = r.json()
    vehicles = body["vehicles"]
    assert len(vehicles) > 300
    assert all(19.8 <= v["lon"] <= 20.2 and 49.95 <= v["lat"] <= 50.15 for v in vehicles)
    assert {v["feed_id"] for v in vehicles} == {"kmk_a", "kmk_m", "kmk_t"}
    tram = next(v for v in vehicles if v["feed_id"] == "kmk_t")
    assert tram["mode"] == "tram" and tram["line"]["name"] and tram["key"] == f"kmk_t:{tram['vehicle_id']}"
    assert tram["trip_key"].startswith("kmk_t:20261025:") and tram["line"]["key"] == f"kmk_t:{tram['line']['route_id']}"
    # live data: clients poll on their own schedule and always revalidate (304 via the ETag)
    assert "next_update" not in body and r.headers["cache-control"] == "no-cache"
    again = await api.get(f"/vehicles?bbox={CITY_BBOX}", headers={"If-None-Match": r.headers["etag"]})
    assert again.status_code == 304

    small = (await api.get(f"/vehicles?bbox={RONDO_BBOX}")).json()["vehicles"]
    assert 0 < len(small) < len(vehicles)

    trams = (await api.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"]
    assert trams and all(v["mode"] == "tram" for v in trams)
    line = tram["line"]["name"]
    on_line = (await api.get(f"/vehicles?bbox={CITY_BBOX}&route={line}")).json()["vehicles"]
    assert on_line and all(v["line"]["name"] == line for v in on_line)


@pytest.mark.parametrize("bbox", ["19,49,21,51", "1,2,3", "20.2,50,19.8,50.1", "a,b,c,d"])
async def test_vehicles_bad_bbox(api, bbox):
    assert (await api.get(f"/vehicles?bbox={bbox}")).status_code == 422


# ------------------------------------------------------------------------------------------------- stops


async def test_stops_on_map_are_platforms(api, sql):
    body = (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()
    assert not body["truncated"] and all(s["kind"] == "platform" for s in body["stops"])
    rondo = [p for p in body["stops"] if p["group_id"] == group_id(sql, "802")]
    assert len(rondo) >= 4 and all(p["name"] == "Rondo Mogilskie" and p["platform"] for p in rondo)
    assert all(p["key"] == f"rondo-mogilskie{p['platform']}-{p['id']}" for p in rondo)
    assert {"bus", "tram"} <= {m for p in rondo for m in p["modes"]}


async def test_stop_search(api, sql):
    results = (await api.get("/stops/search?q=rondo mog")).json()["results"]
    rondo = results[0]
    assert rondo["name"] == "Rondo Mogilskie" and rondo["id"] == group_id(sql, "802")
    assert rondo["key"] == f"rondo-mogilskie-{rondo['id']}"
    assert {"tram", "bus"} <= {line["mode"] for line in rondo["lines"]}
    served = [p for p in rondo["platforms"] if p["directions"]]
    assert len(served) >= 4
    for p in served:
        assert p["group_id"] == rondo["id"]
        for d in p["directions"]:
            assert d["towards"] != "Rondo Mogilskie" and d["lines"] and len(d["headsigns"]) <= 3
    # the hint is the next stop a trip from that platform reaches
    p1 = next(p for p in rondo["platforms"] if p["id"] == platform_id(sql, "802-01"))
    ((nxt,),) = sql(
        """
        SELECT g.name FROM gtfs.stop_times st
        JOIN core.stop_link l ON l.feed_version_id = st.feed_version_id AND l.stop_id = st.stop_id
        JOIN gtfs.stop_times n ON n.feed_version_id = st.feed_version_id AND n.trip_id = st.trip_id
             AND n.stop_sequence = (SELECT min(stop_sequence) FROM gtfs.stop_times x
                                    WHERE x.feed_version_id = st.feed_version_id AND x.trip_id = st.trip_id
                                      AND x.stop_sequence > st.stop_sequence)
        JOIN core.stop_link nl ON nl.feed_version_id = n.feed_version_id AND nl.stop_id = n.stop_id
        JOIN core.stop_platform np ON np.id = nl.stop_platform_id JOIN core.stop_group g ON g.id = np.stop_group_id
        WHERE l.stop_platform_id = %s GROUP BY g.name ORDER BY count(*) DESC LIMIT 1
        """,
        platform_id(sql, "802-01"),
    )
    assert p1["directions"][0]["towards"] == nxt

    results = (await api.get("/stops/search?q=WZGORZA krzeslaw")).json()["results"]
    assert results[0]["name"] == "Wzgórza Krzesławickie"
    near = (await api.get("/stops/search?q=rondo&near=50.0658,19.9597&limit=5")).json()["results"]
    assert near and all(h["distance_m"] is not None for h in near)
    assert (await api.get("/stops/search?q=")).status_code == 422
    assert (await api.get("/stops/search?q=%_%")).json()["results"] == []  # LIKE wildcards are literal


async def test_stops_without_trips_are_never_returned(api, sql, loaded):
    """A platform with no trips in the current feeds (empty modes) is hidden, and so is a stop made only of them."""
    gid = group_id(sql, "802")
    pid = platform_id(sql, "802-01")
    saved = sql("SELECT id, modes FROM core.stop_platform")

    def clear_modes(where: str, param: str):
        with psycopg.connect(loaded) as conn:
            conn.execute(f"UPDATE core.stop_platform SET modes = '{{}}' WHERE {where} = %s", (param,))

    async def map_ids():
        return {s["id"] for s in (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"]}

    try:
        # one platform without trips: gone from the map and from its group, which is still found
        clear_modes("id", pid)
        assert pid not in await map_ids()
        found = (await api.get("/stops/search?q=rondo mog")).json()["results"][0]
        assert found["id"] == gid and found["platforms"] and pid not in {p["id"] for p in found["platforms"]}
        # all of its platforms without trips: the group disappears from search too
        clear_modes("stop_group_id", gid)
        assert gid not in {r["id"] for r in (await api.get("/stops/search?q=rondo mog")).json()["results"]}
        assert not {s["id"] for s in (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"] if s["group_id"] == gid}
    finally:
        with psycopg.connect(loaded) as conn:
            conn.cursor().executemany("UPDATE core.stop_platform SET modes = %s WHERE id = %s", [(m, i) for i, m in saved])


async def test_stop_search_covers_only_search_namespaces(loaded, sql):
    """Search is limited to feeds.yml `search_namespaces` (ZTP); the map and the stop itself are not."""
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, **NO_CACHE)
    feeds = FEEDS.model_copy(update={"search_namespaces": ["km_kolej"]})  # no stops loaded there; skips validation
    app = create_app(settings, feeds=feeds, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        assert (await c.get("/stops/search?q=rondo mog")).json()["results"] == []
        gid = group_id(sql, "802")
        assert any(s["group_id"] == gid for s in (await c.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"])
        assert (await c.get(f"/stops/{gid}/departures")).status_code == 200


async def test_single_stop_endpoint_is_gone(api, sql):
    assert (await api.get(f"/stops/{group_id(sql, '802')}")).status_code == 404


async def test_stop_keys_resolve_by_id(api, sql):
    gid = group_id(sql, "802")
    assert (await api.get(f"/stops/an-old-name-{gid}/departures")).status_code == 200  # only the id after the last dash counts
    assert (await api.get("/stops/rondo-zzzzz/departures")).status_code == 404
    assert (await api.get("/stops/not_an_id/departures")).status_code == 404


# ------------------------------------------------------------------------------------------------- departures


async def test_departure_board_merges_feeds_with_realtime(api, sql):
    gid = group_id(sql, "802")
    r = await api.get(f"/stops/{gid}/departures?limit=60")
    assert r.status_code == 200
    board = r.json()
    deps = board["departures"]
    assert board["stop"]["id"] == gid and board["timestamp"] == NOW and "next_update" not in board
    assert r.headers["cache-control"] == "no-cache"
    assert len(deps) == 60
    assert [d["time"] for d in deps] == sorted(d["time"] for d in deps)
    assert all(d["time"] >= NOW - 30 and d["status"] != "departed" for d in deps)
    assert {d["feed_id"] for d in deps} == {"kmk_a", "kmk_m", "kmk_t"}
    own = {p[0] for p in sql("SELECT id FROM core.stop_platform WHERE stop_group_id = %s", gid)}
    assert all(d["platform"]["id"] in own for d in deps)
    assert all(d["line"]["name"] for d in deps)

    live = [d for d in deps if d["realtime"]]
    assert len(live) > len(deps) / 2 and all(d["status"] == "live" for d in live)
    assert all(d["status"] == "scheduled" for d in deps if not d["realtime"])
    for d in live:
        if d["estimated"] is not None:
            assert d["time"] == d["estimated"]
            assert d["delay_s"] == d["estimated"] - d["scheduled"]
    # trams count from midnight on the clock-change day: with that their delays are minutes, not an hour
    tram_delays = [d["delay_s"] for d in live if d["feed_id"] == "kmk_t"]
    assert tram_delays and sum(abs(x) < 900 for x in tram_delays) >= 0.9 * len(tram_delays)
    # delayed departures that were scheduled before now are still on the board
    assert any(d["scheduled"] < NOW for d in live)

    # the timetable time comes from the right day base (midnight for trams)
    tram = next(d for d in deps if d["feed_id"] == "kmk_t")
    (secs,) = sql(
        "SELECT extract(epoch FROM departure_time)::int FROM gtfs.stop_times "
        "WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s AND stop_sequence = %s",
        tram["trip_id"],
        tram["stop_sequence"],
    )[0]
    assert tram["scheduled"] == 1792879200 + secs  # 2026-10-25 00:00 CEST


async def test_departure_board_pages_both_ways(api, sql):
    url = f"/stops/{group_id(sql, '802')}/departures"
    first = (await api.get(f"{url}?limit=10")).json()
    assert first["alerts"] is not None and first["earlier"] and first["later"]

    # forward: continuous, no duplicates, no alerts on scroll pages
    seen = [(d["trip_key"], d["stop_sequence"]) for d in first["departures"]]
    page = first
    for _ in range(3):
        page = (await api.get(f"{url}?limit=10&cursor={page['later']}")).json()
        assert page["alerts"] == [] and page["departures"]
        assert page["departures"][0]["time"] >= first["departures"][-1]["time"]
        seen += [(d["trip_key"], d["stop_sequence"]) for d in page["departures"]]
    assert len(seen) == len(set(seen)) == 40
    times = [d["time"] for d in first["departures"]]
    big = (await api.get(f"{url}?limit=40")).json()["departures"]
    assert [(d["trip_key"], d["stop_sequence"]) for d in big] == seen and times == [d["time"] for d in big[:10]]

    # back: departures that have left, oldest first, ending right before the first page
    back = (await api.get(f"{url}?limit=10&cursor={first['earlier']}")).json()
    deps = back["departures"]
    assert len(deps) == 10 and back["earlier"] and back["later"]
    assert [d["time"] for d in deps] == sorted(d["time"] for d in deps)
    assert deps[-1]["time"] <= first["departures"][0]["time"]
    assert all(d["status"] in ("departed", "canceled", "skipped") for d in deps)
    # vehicles that passed keep the last time realtime had
    assert any(d["realtime"] and d["estimated"] for d in deps)
    older = (await api.get(f"{url}?limit=10&cursor={back['earlier']}")).json()["departures"]
    assert older and older[-1]["time"] <= deps[0]["time"]

    assert (await api.get(f"{url}?cursor=garbage")).status_code == 422


async def test_departure_board_platform_and_other_days(api, sql):
    pid = platform_id(sql, "802-01")
    deps = (await api.get(f"/stops/{pid}/departures?limit=5")).json()["departures"]
    assert 0 < len(deps) <= 5 and {d["platform"]["id"] for d in deps} == {pid}

    # scrolling back far enough reaches the day before (another service day, its own base, CEST)
    url = f"/stops/{group_id(sql, '360')}/departures"
    cursor, dates = (await api.get(f"{url}?limit=50")).json()["earlier"], set()
    for _ in range(20):
        page = (await api.get(f"{url}?limit=50&cursor={cursor}")).json()
        dates |= {d["service_date"] for d in page["departures"] if d["feed_id"] == "kmk_a"}
        if "20261024" in dates or not page["earlier"]:
            break
        cursor = page["earlier"]
    assert "20261024" in dates
    assert (await api.get("/stops/zzzzz/departures")).status_code == 404


async def test_departure_board_etag(api, sql):
    url = f"/stops/{group_id(sql, '802')}/departures"
    first = await api.get(url)
    assert first.headers["cache-control"] == "no-cache"
    again = await api.get(url, headers={"If-None-Match": first.headers["etag"]})
    assert again.status_code == 304 and again.content == b""


# ------------------------------------------------------------------------------------------------- trips + shapes


def decode_polyline(s: str) -> list[tuple[float, float]]:
    coords, index, lat, lon = [], 0, 0, 0
    while index < len(s):
        for which in (0, 1):
            shift = result = 0
            while True:
                b = ord(s[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if which == 0:
                lat += delta
            else:
                lon += delta
        coords.append((lat / 1e5, lon / 1e5))
    return coords


async def test_trip_view_of_a_live_vehicle(api):
    vehicles = (await api.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"]
    v = next(v for v in vehicles if v["delay_s"] is not None)
    r = await api.get(f"/vehicles/{v['key']}/trip")
    assert r.status_code == 200
    trip = r.json()
    assert trip["key"] == v["trip_key"] and trip["status"] == "live" and "next_update" not in trip
    assert r.headers["cache-control"] == "no-cache"
    assert trip["vehicle"] == v
    assert trip["route"]["short_name"] == v["line"]["name"] and trip["headsign"] == v["headsign"]
    stops = trip["stops"]
    seqs = [s["stop_sequence"] for s in stops]
    assert seqs == sorted(seqs) and all(s["platform"] and s["name"] for s in stops)
    # passed stops come first, then the ones with predictions; current_index is the first not passed
    passed = [s["passed"] for s in stops]
    assert passed == sorted(passed, reverse=True)
    assert trip["current_index"] == passed.index(False)
    assert any(s["estimated_arrival"] or s["estimated_departure"] for s in stops if not s["passed"])

    # the same view by trip key
    assert (await api.get(f"/trips/{v['trip_key']}")).json() == trip

    shape = trip["shape"]
    assert shape["source"] == "shape"
    points = decode_polyline(shape["polyline"])
    min_lon, min_lat, max_lon, max_lat = shape["bbox"]
    assert len(points) > 10
    assert all(min_lat - 1e-4 <= lat <= max_lat + 1e-4 and min_lon - 1e-4 <= lon <= max_lon + 1e-4 for lat, lon in points)
    assert abs(points[0][0] - stops[0]["lat"]) < 0.003 and abs(points[0][1] - stops[0]["lon"]) < 0.003

    route = (await api.get(f"/routes/{trip['route']['key']}/shapes?date={trip['service_date']}")).json()
    assert shape["polyline"] in {s["polyline"] for s in route["shapes"]}
    assert (await api.get("/vehicles/kmk_t:nope/trip")).status_code == 404
    assert (await api.get("/vehicles/nope/trip")).status_code == 422


async def test_trip_view_without_realtime_or_shape(api, sql, loaded):
    # a tram trip of the day without realtime: no vehicle, nothing live
    candidates = sql(
        "SELECT t.trip_id FROM gtfs.trips t JOIN core.service_date sd ON sd.feed_version_id = t.feed_version_id "
        "AND sd.service_id = t.service_id AND sd.date = '2026-10-25' WHERE t.feed_version_id = 'kmk_t_v1' ORDER BY 1"
    )
    r = redis.Redis.from_url(TEST_REDIS_URL)
    trip_id = next(t for (t,) in candidates if not r.exists(f"rt:trip:kmk_t:20261025:{t}"))
    r.close()
    trip = (await api.get(f"/trips/kmk_t:20261025:{trip_id}")).json()
    assert trip["status"] == "scheduled" and trip["vehicle"] is None
    assert not any(s["estimated_arrival"] or s["estimated_departure"] for s in trip["stops"])
    assert trip["shape"]["source"] == "shape" and not any(s["on_demand"] for s in trip["stops"])

    # without a shape it is drawn through the stops
    with psycopg.connect(loaded) as conn:
        (shape_id,) = conn.execute(
            "SELECT shape_id FROM gtfs.trips WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s", (trip_id,)
        ).fetchone()
        conn.execute("UPDATE gtfs.trips SET shape_id = NULL WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s", (trip_id,))
        conn.commit()
        try:
            bare = (await api.get(f"/trips/kmk_t:20261025:{trip_id}")).json()
        finally:
            conn.execute(
                "UPDATE gtfs.trips SET shape_id = %s WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s", (shape_id, trip_id)
            )
            conn.commit()
    assert bare["shape"]["source"] == "stops"
    points = decode_polyline(bare["shape"]["polyline"])
    assert len(points) == len(bare["stops"])
    assert all(abs(p[0] - s["lat"]) < 1e-5 and abs(p[1] - s["lon"]) < 1e-5 for p, s in zip(points, bare["stops"], strict=True))


async def test_trip_and_route_errors(api):
    assert (await api.get("/trips/kmk_t:20261025:nope")).status_code == 404
    assert (await api.get("/trips/kmk_t:20300101:nope")).status_code == 404
    assert (await api.get("/trips/nope:20261025:x")).status_code == 404
    for bad in ("kmk_t:2026-10-25:x", "kmk_t:25102026:x", "kmk_t:20261025", "kmk_t"):
        assert (await api.get(f"/trips/{bad}")).status_code == 422, bad
    assert (await api.get("/routes/kmk_t:nope/shapes")).status_code == 404
    assert (await api.get("/routes/nope/shapes")).status_code == 422
    assert (await api.get("/routes/kmk_t:1/shapes?date=2026-10-25")).status_code == 422


async def test_errors_have_one_shape(api):
    not_found = await api.get("/trips/kmk_t:20261025:nope")
    assert not_found.json() == {"error": {"code": "not_found", "message": "trip not found"}}
    invalid = (await api.get("/stops/search?q=")).json()["error"]
    assert invalid["code"] == "invalid_input" and invalid["message"].startswith("q: ")
    assert (await api.get("/trips/kmk_t")).json()["error"]["code"] == "invalid_input"
    assert (await api.get("/nowhere")).json() == {"error": {"code": "not_found", "message": "Not Found"}}
    assert (await api.post("/health")).json()["error"]["code"] == "method_not_allowed"


# ------------------------------------------------------------------------------------------------- alerts


@pytest.fixture
async def alerts_in_redis(api, sql):
    """Alerts as the worker stores them (the snapshot had none)."""
    deps = (await api.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()["departures"]
    tram = next(d for d in deps if d["feed_id"] == "kmk_t")
    tram["route_id"] = tram["line"]["route_id"]
    pid = tram["platform"]["id"]  # a platform the tram stops at
    (code,) = sql("SELECT current_code FROM core.stop_platform WHERE id = %s", pid)[0]
    far = platform_id(sql, "125-01")

    def alert(id_, **entity):
        base = {
            "agency_id": None,
            "route_id": None,
            "route_type": None,
            "trip_id": None,
            "stop_id": None,
            "stop_platform_id": None,
        }
        return {
            "feed_id": "kmk_t",
            "id": id_,
            "cause": "CONSTRUCTION",
            "effect": "DETOUR",
            "header": f"Header {id_}",
            "description": "Opis\r\nz Polskimi znakami: łódź",
            "url": None,
            "active_periods": [{"start": NOW - 3600, "end": None}],
            "informed_entities": [base | entity],
        }

    raws = [
        alert("route", route_id=tram["route_id"]),
        alert("stop", stop_id=code, stop_platform_id=pid),
        alert("trip", trip_id=tram["trip_id"]),
        alert("feed", agency_id="agency_1"),
        alert("route_elsewhere", route_id=tram["route_id"], stop_id="125-01", stop_platform_id=far),
        alert("other_route", route_id="route_does_not_exist"),
    ]
    r = Redis.from_url(TEST_REDIS_URL)
    await r.set("rt:alerts:kmk_t", json.dumps(raws))
    yield {"raws": raws, "tram": tram, "pid": pid}
    await r.delete("rt:alerts:kmk_t")
    await r.aclose()


async def test_alerts_are_passed_through(api, alerts_in_redis):
    raws = alerts_in_redis["raws"]
    out = (await api.get("/alerts")).json()["alerts"]
    assert [a["id"] for a in out] == [a["id"] for a in raws]
    for got, raw in zip(out, raws, strict=True):
        assert {k: got[k] for k in raw} == raw  # unchanged
        assert got["key"] == f"kmk_t:{raw['id']}"
    assert out[0]["routes"][0]["route_id"] == alerts_in_redis["tram"]["route_id"]
    assert out[1]["stops"][0]["id"] == alerts_in_redis["pid"]
    assert (await api.get("/alerts/kmk_t:stop")).json() == out[1]
    assert (await api.get("/alerts/kmk_t:nope")).status_code == 404
    assert (await api.get("/alerts/nope:stop")).status_code == 404
    assert (await api.get("/alerts?feed=kmk_a")).json()["alerts"] == []


async def test_alert_filters(api, sql, alerts_in_redis):
    route = alerts_in_redis["tram"]["line"]["key"]
    ids = lambda r: {a["id"] for a in r.json()["alerts"]}  # noqa: E731
    # an entity naming route + stop is about that route (the stop isn't asked for)
    assert ids(await api.get(f"/alerts?route={route}")) == {"route", "feed", "route_elsewhere"}
    assert ids(await api.get(f"/alerts?feed=kmk_t&route={route}")) == {"route", "feed", "route_elsewhere"}
    assert (await api.get(f"/alerts?feed=kmk_a&route={route}")).status_code == 422
    assert ids(await api.get(f"/alerts?stop={group_id(sql, '802')}")) == {"stop"}
    # route AND stop given: the route-at-another-stop alert doesn't apply here; feed-wide ones still do
    assert ids(await api.get(f"/alerts?route={route}&stop={group_id(sql, '802')}")) == {"route", "stop", "feed"}


async def test_alerts_on_board_and_trip(api, sql, alerts_in_redis):
    board = (await api.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()
    assert {a["id"] for a in board["alerts"]} == {"route", "stop", "trip", "feed"}
    tram = alerts_in_redis["tram"]
    trip = (await api.get(f"/trips/{tram['trip_key']}")).json()
    assert {"route", "stop", "trip", "feed"} <= {a["id"] for a in trip["alerts"]}
    assert "other_route" not in {a["id"] for a in trip["alerts"]}


# ------------------------------------------------------------------------------------------------- health


async def test_health(api):
    r = await api.get("/health")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["ok"] and body["database"] and body["redis"]
    tram = next(f for f in body["feeds"] if f["feed_id"] == "kmk_t")
    assert tram["version"] == 1
    assert tram["realtime"]["trip_updates"]["ok"] is True
    # the test data has no poll times and its static feeds were never checked: up, but not fresh
    assert body["fresh"] is False
    assert "static: never checked" in tram["problems"] and "trip_updates: never polled" in tram["problems"]


async def test_openapi_lists_every_endpoint(api):
    paths = (await api.get("http://test/api/openapi.json")).json()["paths"]
    assert {
        "/api/v1/vehicles",
        "/api/v1/vehicles/{vehicle_key}/trip",
        "/api/v1/stops",
        "/api/v1/stops/search",
        "/api/v1/stops/{key}/departures",
        "/api/v1/trips/{trip_key}",
        "/api/v1/routes/{route_key}/shapes",
        "/api/v1/alerts",
        "/api/v1/alerts/{alert_key}",
        "/api/v1/health",
    } == set(paths)


# ------------------------------------------------------------------------------------------------- response cache


@pytest.fixture
async def cached_api(loaded, monkeypatch):
    """An API with the response cache on, and a count of the boards it computed."""
    from gtfs_backend.api.services import departures

    r = redis.Redis.from_url(TEST_REDIS_URL)
    for key in r.scan_iter("api:resp:*"):
        r.delete(key)
    r.close()
    calls = []
    real = departures.board

    async def counted(*args, **kwargs):
        calls.append(args[1])
        return await real(*args, **kwargs)

    monkeypatch.setattr(departures, "board", counted)
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, cache_ttl_live=60)
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        yield c, app.state.ctx, calls


async def test_board_is_computed_once_and_revalidates(cached_api, sql):
    api, _, calls = cached_api
    url = f"/stops/{group_id(sql, '802')}/departures"
    first = await api.get(url)
    second = await api.get(url)
    assert first.status_code == second.status_code == 200 and len(calls) == 1
    assert first.content == second.content and first.headers["etag"] == second.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    again = await api.get(url, headers={"If-None-Match": first.headers["etag"]})
    assert again.status_code == 304 and len(calls) == 1


async def test_concurrent_misses_compute_once(cached_api, sql):
    api, _, calls = cached_api
    url = f"/stops/{group_id(sql, '802')}/departures?limit=7"
    responses = await asyncio.gather(*(api.get(url) for _ in range(5)))
    assert {r.status_code for r in responses} == {200} and len({r.content for r in responses}) == 1
    assert len(calls) == 1


async def test_cache_key(cached_api, sql):
    api, ctx, calls = cached_api
    url = f"/stops/{group_id(sql, '802')}/departures?limit=5"
    await api.get(url + "&_=1")
    await api.get(url + "&_=2")  # parameters the route doesn't declare don't count
    assert len(calls) == 1
    await api.get(url.replace("limit=5", "limit=6"))
    assert len(calls) == 2
    ctx.catalog.fingerprint = "other"  # another timetable version (import, new day)
    await api.get(url)
    assert len(calls) == 3


async def test_errors_are_not_cached(cached_api):
    api, _, calls = cached_api
    assert (await api.get("/stops/nowhere-zzzzz/departures")).status_code == 404
    assert (await api.get("/stops/nowhere-zzzzz/departures")).status_code == 404
    assert len(calls) == 2


async def test_cache_trouble_only_skips_the_cache(cached_api, sql, monkeypatch):
    from redis.exceptions import ConnectionError as RedisConnectionError

    api, ctx, calls = cached_api

    async def broken(*args, **kwargs):
        raise RedisConnectionError("down")

    monkeypatch.setattr(ctx.redis, "get", broken)
    monkeypatch.setattr(ctx.redis, "set", broken)
    url = f"/stops/{group_id(sql, '802')}/departures"
    assert (await api.get(url)).status_code == 200 and (await api.get(url)).status_code == 200
    assert len(calls) == 2


async def test_cache_off_with_ttl_zero(loaded, sql, monkeypatch):
    from gtfs_backend.api.services import departures

    calls = []
    real = departures.board

    async def counted(*args, **kwargs):
        calls.append(1)
        return await real(*args, **kwargs)

    monkeypatch.setattr(departures, "board", counted)
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, cache_ttl_live=0)
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        url = f"/stops/{group_id(sql, '802')}/departures"
        await c.get(url)
        await c.get(url)
    assert len(calls) == 2


async def test_boards_and_trips_only_carry_current_alerts(api, sql):
    def feed_wide(id_, *periods):
        return {
            "feed_id": "kmk_t",
            "id": id_,
            "active_periods": [{"start": s, "end": e} for s, e in periods],
            "informed_entities": [{"agency_id": "agency_1"}],
        }

    raws = [
        feed_wide("always"),
        feed_wide("now", (NOW - 60, NOW + 60)),
        feed_wide("open_end", (NOW - 86400, None)),
        feed_wide("soon", (NOW + 2 * 3600, NOW + 3 * 3600)),
        feed_wide("over", (NOW - 7200, NOW - 3600)),
        feed_wide("next_month", (NOW + 30 * 86400, None)),
        feed_wide("over_and_soon", (NOW - 7200, NOW - 3600), (NOW + 3600, None)),
    ]
    current = {"always", "now", "open_end", "soon", "over_and_soon"}
    r = Redis.from_url(TEST_REDIS_URL)
    await r.set("rt:alerts:kmk_t", json.dumps(raws))
    try:
        board = (await api.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()
        assert {a["id"] for a in board["alerts"]} == current
        tram = next(d for d in board["departures"] if d["feed_id"] == "kmk_t")
        assert {a["id"] for a in (await api.get(f"/trips/{tram['trip_key']}")).json()["alerts"]} == current
        # /alerts passes everything on unless asked
        assert len((await api.get("/alerts")).json()["alerts"]) == len(raws)
        assert {a["id"] for a in (await api.get("/alerts?active=true")).json()["alerts"]} == current
    finally:
        await r.delete("rt:alerts:kmk_t")
        await r.aclose()


@pytest.fixture
async def api_no_raise(loaded):
    """Like `api`, but app exceptions become responses (as in production) instead of reaching the test."""
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, **NO_CACHE)
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    transport = ASGITransport(app, raise_app_exceptions=False)
    async with app.router.lifespan_context(app), AsyncClient(transport=transport, base_url="http://test/api/v1") as c:
        yield c


async def test_unexpected_errors_keep_the_error_body(api_no_raise, monkeypatch):
    from gtfs_backend.api.services import trips

    async def bug(ctx, key):
        raise RuntimeError("secret detail")

    async def db_gone(ctx, key):
        raise psycopg.OperationalError("connection to server at 10.0.0.5 failed")

    monkeypatch.setattr(trips, "view_by_key", bug)
    r = await api_no_raise.get("/trips/kmk_t:20261025:x")
    assert r.status_code == 500 and r.json() == {"error": {"code": "internal", "message": "internal error"}}

    monkeypatch.setattr(trips, "view_by_key", db_gone)
    r = await api_no_raise.get("/trips/kmk_t:20261025:x")
    assert r.status_code == 503 and r.headers["retry-after"] == "1"
    assert r.json()["error"]["code"] == "unavailable" and "10.0.0.5" not in r.text
