"""The HTTP API against the 2026-10-25 snapshot: trimmed ZTP timetables in Postgres, the realtime snapshot in Redis.

The data is loaded once per module; the API's clock is frozen at the tram feed's snapshot time.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

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


def stop_by_code(sql, code: str) -> str:
    return sql("SELECT id FROM core.stop WHERE current_code = %s", code)[0][0]


def feed_of(item: dict) -> str:
    """The feed of a vehicle or departure: the first part of its trip key."""
    return item["trip_key"].split(":", 1)[0]


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
    assert {feed_of(v) for v in vehicles} == {"kmk_a", "kmk_m", "kmk_t"}
    tram = next(v for v in vehicles if feed_of(v) == "kmk_t")
    assert tram["route"]["mode"] == "tram" and tram["route"]["name"] and tram["key"].startswith("kmk_t:")
    assert tram["trip_key"].startswith("kmk_t:20261025:") and tram["route"]["key"].startswith("kmk_t:")
    assert set(tram["route"]) == {"key", "name", "mode"}
    # only what the map needs: no ids the key already carries, coordinates to ~1 m
    assert not {"feed_id", "vehicle_id", "mode"} & set(tram)
    assert all(round(v["lat"], 5) == v["lat"] and round(v["lon"], 5) == v["lon"] for v in vehicles)
    # live data: clients poll on their own schedule and always revalidate (304 via the ETag)
    assert "next_update" not in body and r.headers["cache-control"] == "no-cache"
    again = await api.get(f"/vehicles?bbox={CITY_BBOX}", headers={"If-None-Match": r.headers["etag"]})
    assert again.status_code == 304

    small = (await api.get(f"/vehicles?bbox={RONDO_BBOX}")).json()["vehicles"]
    assert 0 < len(small) < len(vehicles)

    trams = (await api.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"]
    assert trams and all(v["route"]["mode"] == "tram" for v in trams)
    line = tram["route"]["name"]
    on_line = (await api.get(f"/vehicles?bbox={CITY_BBOX}&routes={line}")).json()["vehicles"]
    assert on_line and all(v["route"]["name"] == line for v in on_line)


@pytest.mark.parametrize("bbox", ["19,49,21,51", "1,2,3", "20.2,50,19.8,50.1", "a,b,c,d"])
async def test_vehicles_bad_bbox(api, bbox):
    assert (await api.get(f"/vehicles?bbox={bbox}")).status_code == 422


# ------------------------------------------------------------------------------------------------- stops


async def test_stops_on_map_are_stops(api, sql):
    body = (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()
    assert not body["truncated"] and all(s["kind"] == "stop" for s in body["stops"])
    rondo = [p for p in body["stops"] if p["group_id"] == group_id(sql, "802")]
    assert len(rondo) >= 4 and all(p["name"] == "Rondo Mogilskie" and p["platform_code"] for p in rondo)
    assert all(p["key"] == f"rondo-mogilskie{p['platform_code']}-{p['id']}" for p in rondo)
    assert {"bus", "tram"} <= {m for p in rondo for m in p["modes"]}


async def test_stops_on_map_as_groups(api, sql):
    body = (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=group")).json()
    assert not body["truncated"] and body["stops"] and all(s["kind"] == "group" for s in body["stops"])
    assert all(s["group_id"] is None and s["platform_code"] is None for s in body["stops"])
    rondo = next(s for s in body["stops"] if s["id"] == group_id(sql, "802"))
    assert rondo["key"] == f"rondo-mogilskie-{rondo['id']}" and {"bus", "tram"} <= set(rondo["modes"])
    assert (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=bogus")).status_code == 422


@pytest.mark.parametrize("kind", ["stop", "group"])
async def test_stops_on_map_by_mode(api, kind):
    async def ids(modes: str = "") -> set[str]:
        body = (await api.get(f"/stops?bbox={CITY_BBOX}&kind={kind}&modes={modes}")).json()
        assert not body["truncated"]
        assert not modes or all(set(s["modes"]) & set(modes.split(",")) for s in body["stops"])
        return {s["id"] for s in body["stops"]}

    everything, tram, bus = await ids(), await ids("tram"), await ids("bus")
    assert tram and bus and tram < everything and bus < everything
    assert await ids("tram,bus") == tram | bus
    assert await ids("nonexistent") == set()


async def test_stop_search(api, sql):
    results = (await api.get("/stops/search?q=rondo mog")).json()["results"]
    rondo = results[0]
    assert rondo["name"] == "Rondo Mogilskie" and rondo["id"] == group_id(sql, "802")
    assert rondo["key"] == f"rondo-mogilskie-{rondo['id']}"
    assert {"tram", "bus"} <= {line["mode"] for p in rondo["stops"] for d in p["directions"] for line in d["routes"]}
    served = [p for p in rondo["stops"] if p["directions"]]
    assert len(served) >= 4
    assert "kind" not in rondo and not any({"kind", "group_id"} & set(p) for p in rondo["stops"])
    own = {p[0] for p in sql("SELECT id FROM core.stop WHERE stop_group_id = %s", rondo["id"])}
    assert {p["id"] for p in rondo["stops"]} <= own
    for p in served:
        for d in p["directions"]:
            assert d["towards"] != "Rondo Mogilskie" and d["routes"] and len(d["headsigns"]) <= 3
    # the hint is the next stop group a trip from that stop reaches
    p1 = next(p for p in rondo["stops"] if p["id"] == stop_by_code(sql, "802-01"))
    ((nxt,),) = sql(
        """
        SELECT g.name FROM gtfs.stop_times st
        JOIN core.stop_link l ON l.feed_version_id = st.feed_version_id AND l.stop_id = st.stop_id
        JOIN gtfs.stop_times n ON n.feed_version_id = st.feed_version_id AND n.trip_id = st.trip_id
             AND n.stop_sequence = (SELECT min(stop_sequence) FROM gtfs.stop_times x
                                    WHERE x.feed_version_id = st.feed_version_id AND x.trip_id = st.trip_id
                                      AND x.stop_sequence > st.stop_sequence)
        JOIN core.stop_link nl ON nl.feed_version_id = n.feed_version_id AND nl.stop_id = n.stop_id
        JOIN core.stop np ON np.id = nl.public_stop_id JOIN core.stop_group g ON g.id = np.stop_group_id
        WHERE l.public_stop_id = %s GROUP BY g.name ORDER BY count(*) DESC LIMIT 1
        """,
        stop_by_code(sql, "802-01"),
    )
    assert p1["directions"][0]["towards"] == nxt

    assert len((await api.get("/stops/search?q=rondo")).json()["results"]) == 10  # typeahead default
    assert len((await api.get("/stops/search?q=rondo&limit=15")).json()["results"]) == 15

    results = (await api.get("/stops/search?q=WZGORZA krzeslaw")).json()["results"]
    assert results[0]["name"] == "Wzgórza Krzesławickie"
    assert (await api.get("/stops/search?q=")).status_code == 422
    assert (await api.get("/stops/search?q=%_%")).json()["results"] == []  # LIKE wildcards are literal


async def test_stops_without_trips_are_never_returned(api, sql, loaded):
    """A stop with no trips in the current feeds (empty modes) is hidden, and so is a group made only of them."""
    gid = group_id(sql, "802")
    pid = stop_by_code(sql, "802-01")
    saved = sql("SELECT id, modes FROM core.stop")

    def clear_modes(where: str, param: str):
        with psycopg.connect(loaded) as conn:
            conn.execute(f"UPDATE core.stop SET modes = '{{}}' WHERE {where} = %s", (param,))

    async def map_ids():
        return {s["id"] for s in (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"]}

    try:
        # one stop without trips: gone from the map and from its group, which is still found
        clear_modes("id", pid)
        assert pid not in await map_ids()
        found = (await api.get("/stops/search?q=rondo mog")).json()["results"][0]
        assert found["id"] == gid and found["stops"] and pid not in {p["id"] for p in found["stops"]}
        # all of its stops without trips: the group disappears from search too
        clear_modes("stop_group_id", gid)
        assert gid not in {r["id"] for r in (await api.get("/stops/search?q=rondo mog")).json()["results"]}
        assert not {s["id"] for s in (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"] if s["group_id"] == gid}
        assert gid not in {s["id"] for s in (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=group")).json()["stops"]}
    finally:
        with psycopg.connect(loaded) as conn:
            conn.cursor().executemany("UPDATE core.stop SET modes = %s WHERE id = %s", [(m, i) for i, m in saved])


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
    assert board["stop"]["id"] == gid and "next_update" not in board
    assert r.headers["cache-control"] == "no-cache"
    assert len(deps) == 60
    assert [d["time"] for d in deps] == sorted(d["time"] for d in deps)
    assert all(d["time"] >= NOW - 30 and d["status"] != "departed" for d in deps)
    assert {feed_of(d) for d in deps} == {"kmk_a", "kmk_m", "kmk_t"}
    assert not {"feed_id", "trip_id", "service_date", "realtime", "vehicle_id"} & {k for d in deps for k in d}
    own = {p[0] for p in sql("SELECT id FROM core.stop WHERE stop_group_id = %s", gid)}
    assert all(d["stop"]["id"] in own and d["stop"]["group_id"] == gid for d in deps)
    assert all(d["route"]["name"] for d in deps)

    live = [d for d in deps if d["status"] == "live"]
    assert len(live) > len(deps) / 2 and all(d["estimated"] is not None or d["delay_s"] is not None for d in live)
    assert all(d["status"] == "scheduled" and d["estimated"] is None for d in deps if d["status"] != "live")
    assert any((d["vehicle_key"] or "").startswith(feed_of(d) + ":") for d in live)
    for d in live:
        if d["estimated"] is not None:
            assert d["time"] == d["estimated"]
            assert d["delay_s"] == d["estimated"] - d["scheduled"]
    # trams count from midnight on the clock-change day: with that their delays are minutes, not an hour
    tram_delays = [d["delay_s"] for d in live if feed_of(d) == "kmk_t"]
    assert tram_delays and sum(abs(x) < 900 for x in tram_delays) >= 0.9 * len(tram_delays)
    # delayed departures that were scheduled before now are still on the board
    assert any(d["scheduled"] < NOW for d in live)
    # the board's time is its data's: the newest prediction shown, from before the request
    assert board["timestamp"] is not None and board["timestamp"] <= NOW

    # the timetable time comes from the right day base (midnight for trams)
    tram = next(d for d in deps if feed_of(d) == "kmk_t")
    (secs,) = sql(
        "SELECT extract(epoch FROM departure_time)::int FROM gtfs.stop_times "
        "WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s AND stop_sequence = %s",
        tram["trip_key"].split(":", 2)[2],
        tram["stop_sequence"],
    )[0]
    assert tram["scheduled"] == 1792879200 + secs  # 2026-10-25 00:00 CEST


async def test_departure_board_pages_by_time(api, sql):
    url = f"/stops/{group_id(sql, '802')}/departures"
    first = (await api.get(f"{url}?limit=10")).json()
    assert first["alerts"] is not None and "earlier" not in first and "later" not in first

    # forward from the last time: no gaps, repeats only within the same second
    seen = [(d["trip_key"], d["stop_sequence"]) for d in first["departures"]]
    page = first
    for _ in range(3):
        page = (await api.get(f"{url}?limit=10&time={page['departures'][-1]['time']}")).json()
        assert page["alerts"] is not None and page["departures"]
        seen += [
            (d["trip_key"], d["stop_sequence"]) for d in page["departures"] if (d["trip_key"], d["stop_sequence"]) not in seen
        ]
    big = (await api.get(f"{url}?limit=100")).json()["departures"]
    assert [(d["trip_key"], d["stop_sequence"]) for d in big[: len(seen)]] == seen

    # a time in the past: departures that have left show up
    past = (await api.get(f"{url}?limit=100&time={NOW - 3600}")).json()["departures"]
    assert [d["time"] for d in past] == sorted(d["time"] for d in past)
    assert all(d["time"] >= NOW - 3600 for d in past)
    assert any(d["status"] == "departed" for d in past)
    # vehicles that passed keep the last time realtime had
    assert any(d["status"] == "departed" and d["estimated"] for d in past)

    assert (await api.get(f"{url}?time=garbage")).status_code == 422
    assert (await api.get(f"{url}?time=-1")).status_code == 422


async def test_departure_board_filters(api, sql):
    url = f"/stops/{group_id(sql, '802')}/departures"
    deps = (await api.get(f"{url}?limit=100")).json()["departures"]
    assert {feed_of(d) for d in deps} == {"kmk_a", "kmk_m", "kmk_t"}

    only = (await api.get(f"{url}?limit=20&feeds=kmk_t")).json()["departures"]
    assert len(only) == 20 and {feed_of(d) for d in only} == {"kmk_t"}
    both = (await api.get(f"{url}?limit=100&feeds=kmk_t,kmk_a")).json()["departures"]
    assert {feed_of(d) for d in both} == {"kmk_t", "kmk_a"}
    assert both == (await api.get(f"{url}?limit=100&feeds=kmk_t&feeds=kmk_a")).json()["departures"]
    assert (await api.get(f"{url}?feeds=nope")).status_code == 422

    board = (await api.get(f"{url}?feeds=kmk_t&routes=no-such-line")).json()
    assert {"kmk_a", "kmk_m", "kmk_t"} <= {ln["key"].split(":", 1)[0] for ln in board["routes"]}  # not narrowed by the filters
    assert len({ln["key"] for ln in board["routes"]}) == len(board["routes"])  # routes, each once (names may repeat)
    assert {d["route"]["key"] for d in deps} <= {ln["key"] for ln in board["routes"]}
    name = next(d["route"]["name"] for d in deps if feed_of(d) == "kmk_t")
    on_line = (await api.get(f"{url}?limit=20&routes={name}")).json()["departures"]
    assert on_line and all(d["route"]["name"] == name for d in on_line)
    assert (await api.get(f"{url}?routes=no-such-line")).json()["departures"] == []
    narrow = (await api.get(f"{url}?limit=20&feeds=kmk_a&routes={name}")).json()["departures"]
    assert all(feed_of(d) == "kmk_a" and d["route"]["name"] == name for d in narrow)
    # filters are part of the cached response
    assert (await api.get(f"{url}?limit=20&feeds=kmk_t")).json()["departures"] == only


async def test_departure_board_stop_and_other_days(api, sql):
    pid = stop_by_code(sql, "802-01")
    deps = (await api.get(f"/stops/{pid}/departures?limit=5")).json()["departures"]
    assert 0 < len(deps) <= 5 and {d["stop"]["id"] for d in deps} == {pid}

    # starting far enough back reaches the day before (another service day, its own base, CEST)
    url = f"/stops/{group_id(sql, '360')}/departures"
    time, dates = NOW - 36 * 3600, set()
    for _ in range(40):
        page = (await api.get(f"{url}?limit=100&time={time}")).json()["departures"]
        dates |= {d["trip_key"].split(":")[1] for d in page if feed_of(d) == "kmk_a"}
        if "20261024" in dates or not page:
            break
        time = max(page[-1]["time"], time + 1)
    assert "20261024" in dates
    assert (await api.get("/stops/zzzzz/departures")).status_code == 404


async def test_polls_revalidate_while_the_data_is_unchanged(loaded, sql):
    """Responses carry the time of their data, not of the request: a later poll of unchanged data is a 304."""
    clock = [NOW]
    settings = ApiSettings(_env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, **NO_CACHE)
    app = create_app(settings, feeds=FEEDS, clock=lambda: clock[0])
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        vehicles = (await c.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()
        assert vehicles["timestamp"] == max(v["timestamp"] for v in vehicles["vehicles"] if v["timestamp"] is not None)
        v = vehicles["vehicles"][0]
        urls = [
            f"/vehicles?bbox={CITY_BBOX}",
            f"/stops/{group_id(sql, '802')}/departures",
            f"/trips/{v['trip_key']}/live",
            f"/vehicles/{v['key']}/trip",
        ]
        first = {u: await c.get(u) for u in urls}
        clock[0] = NOW + 1
        for u in urls:
            again = await c.get(u, headers={"If-None-Match": first[u].headers["etag"]})
            assert again.status_code == 304, u
        # nothing realtime shown: no timestamp
        assert (await c.get("/vehicles?bbox=19.0,49.0,19.01,49.01")).json() == {"timestamp": None, "vehicles": []}


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


async def test_trip_of_a_live_vehicle(api):
    vehicles = (await api.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"]
    v = next(v for v in vehicles if v["delay_s"] is not None)
    r = await api.get(f"/vehicles/{v['key']}/trip")
    assert r.status_code == 200
    live = r.json()
    assert live["key"] == v["trip_key"] and live["status"] == "live" and "next_update" not in live
    assert r.headers["cache-control"] == "no-cache"
    assert live["vehicle"] == v
    # passed stops come first, then the ones with predictions; current_index is the first not passed
    passed = [s["passed"] for s in live["stops"]]
    assert passed == sorted(passed, reverse=True)
    assert live["current_index"] == passed.index(False)
    assert any(s["estimated_arrival"] or s["estimated_departure"] for s in live["stops"] if not s["passed"])
    # the same live state by trip key
    by_key = await api.get(f"/trips/{v['trip_key']}/live")
    assert by_key.json() == live and by_key.headers["cache-control"] == "no-cache"

    # the trip itself: what doesn't change while it runs, cacheable, with stops in the order of the live ones
    r = await api.get(f"/trips/{v['trip_key']}")
    assert r.headers["cache-control"].startswith("public, max-age=")
    trip = r.json()
    assert trip["key"] == live["key"] and not {"vehicle", "status", "alerts", "current_index"} & set(trip)
    assert trip["route"]["name"] == v["route"]["name"] and trip["headsign"] == v["headsign"]
    stops = trip["stops"]
    seqs = [s["stop_sequence"] for s in stops]
    assert seqs == sorted(seqs) == [s["stop_sequence"] for s in live["stops"]]
    assert all(s["stop"]["name"] and s["stop"]["group_id"] and "estimated_arrival" not in s for s in stops)

    shape = trip["shape"]
    assert shape["source"] == "shape"
    points = decode_polyline(shape["polyline"])
    min_lon, min_lat, max_lon, max_lat = shape["bbox"]
    assert len(points) > 10
    assert all(min_lat - 1e-4 <= lat <= max_lat + 1e-4 and min_lon - 1e-4 <= lon <= max_lon + 1e-4 for lat, lon in points)
    assert abs(points[0][0] - stops[0]["lat"]) < 0.003 and abs(points[0][1] - stops[0]["lon"]) < 0.003

    route = (await api.get(f"/routes/{trip['route']['key']}/shapes?date={trip['service_date']}")).json()
    assert shape["polyline"] in {s["polyline"] for s in route["shapes"]} and "feed_version_id" not in route
    assert (await api.get("/vehicles/kmk_t:nope/trip")).status_code == 404
    assert (await api.get("/vehicles/nope/trip")).status_code == 422


async def test_trip_without_realtime_or_shape(api, sql, loaded):
    # a tram trip of the day without realtime: no vehicle, nothing live
    candidates = sql(
        "SELECT t.trip_id FROM gtfs.trips t JOIN core.service_date sd ON sd.feed_version_id = t.feed_version_id "
        "AND sd.service_id = t.service_id AND sd.date = '2026-10-25' WHERE t.feed_version_id = 'kmk_t_v1' ORDER BY 1"
    )
    r = redis.Redis.from_url(TEST_REDIS_URL)
    trip_id = next(t for (t,) in candidates if not r.exists(f"rt:trip:kmk_t:20261025:{t}"))
    r.close()
    key = f"kmk_t:20261025:{trip_id}"
    live = (await api.get(f"/trips/{key}/live")).json()
    assert live["status"] == "scheduled" and live["vehicle"] is None
    assert not any(s["estimated_arrival"] or s["estimated_departure"] for s in live["stops"])
    # without realtime a stop is passed once its time is over
    trip = (await api.get(f"/trips/{key}")).json()
    for s, ls in zip(trip["stops"], live["stops"], strict=True):
        assert ls["passed"] == ((s.get("scheduled_departure") or s["scheduled_arrival"]) < NOW)
    assert trip["shape"]["source"] == "shape" and not any(s["on_demand"] for s in trip["stops"])

    # without a shape it is drawn through the stops
    with psycopg.connect(loaded) as conn:
        (shape_id,) = conn.execute(
            "SELECT shape_id FROM gtfs.trips WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s", (trip_id,)
        ).fetchone()
        conn.execute("UPDATE gtfs.trips SET shape_id = NULL WHERE feed_version_id = 'kmk_t_v1' AND trip_id = %s", (trip_id,))
        conn.commit()
        try:
            bare = (await api.get(f"/trips/{key}")).json()
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
    assert (await api.get("/trips/kmk_t:20261025:nope/live")).status_code == 404
    assert (await api.get("/trips/kmk_t/live")).status_code == 422
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
    """Alerts as the worker stores them, entities resolved to our ids (the snapshot had none)."""
    deps = (await api.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()["departures"]
    tram = next(d for d in deps if feed_of(d) == "kmk_t")
    tram["route_id"] = tram["route"]["key"].split(":", 1)[1]
    tram["direction_id"] = (await api.get(f"/trips/{tram['trip_key']}")).json()["direction_id"]
    feed, service_date, trip_id = tram["trip_key"].split(":", 2)
    next_week = (date.fromisoformat(service_date) + timedelta(days=7)).strftime("%Y%m%d")
    pid = tram["stop"]["id"]  # a stop the tram stops at
    group = group_id(sql, "802")
    group_pids = [r[0] for r in sql("SELECT id FROM core.stop WHERE stop_group_id = %s ORDER BY id", group)]
    far = stop_by_code(sql, "125-01")

    def alert(id_, severity=None, **entity):
        base = {
            "agency_id": None,
            "route_id": None,
            "route_type": None,
            "direction_id": None,
            "trip_key": None,
            "public_stop_ids": [],
            "stop_group_id": None,
        }
        return {
            "feed_id": "kmk_t",
            "id": id_,
            "cause": "construction",
            "effect": "detour",
            "severity": severity,
            "header": f"Header {id_}",
            "description": "Opis\nz Polskimi znakami: łódź",
            "url": None,
            "active_periods": [{"start": NOW - 3600, "end": None}],
            "entities": [base | entity],
        }

    raws = [
        alert("route", "warning", route_id=tram["route_id"]),
        alert("stop", "severe", public_stop_ids=[pid]),
        alert("trip", trip_key=tram["trip_key"]),
        alert("trip_next_week", trip_key=f"{feed}:{next_week}:{trip_id}"),
        alert("feed", agency_id="agency_1"),
        alert("route_elsewhere", route_id=tram["route_id"], public_stop_ids=[far]),
        alert("other_route", route_id="route_does_not_exist"),
        alert("station", public_stop_ids=group_pids, stop_group_id=group),
        alert("other_direction", route_id=tram["route_id"], direction_id=1 - (tram["direction_id"] or 0)),
    ]
    r = Redis.from_url(TEST_REDIS_URL)
    await r.set("rt:alerts:kmk_t", json.dumps(raws))
    yield {"raws": raws, "tram": tram, "pid": pid, "group": group, "far": far}
    await r.delete("rt:alerts:kmk_t")
    await r.aclose()


async def test_alert_entities(api, alerts_in_redis):
    tram = alerts_in_redis["tram"]
    out = (await api.get("/alerts")).json()["alerts"]
    # severe first, then warnings, then the rest as published
    assert [a["key"].split(":", 1)[1] for a in out][:2] == ["stop", "route"]
    by_id = {a["key"].split(":", 1)[1]: a for a in out}
    assert set(by_id) == {r["id"] for r in alerts_in_redis["raws"]}
    stop = by_id["stop"]
    assert set(stop) == {"key", "cause", "effect", "severity", "header", "description", "url", "active_periods", "entities"}
    assert (stop["cause"], stop["effect"], stop["severity"]) == ("construction", "detour", "severe")
    none = {"mode": None, "route": None, "direction_id": None, "trip_key": None, "stop": None}
    assert by_id["route"]["entities"] == [none | {"route": tram["route"]}]
    assert stop["entities"][0]["stop"]["id"] == alerts_in_redis["pid"] and stop["entities"][0]["stop"]["kind"] == "stop"
    assert by_id["trip"]["entities"] == [none | {"trip_key": tram["trip_key"]}]
    assert by_id["feed"]["entities"] == [none]  # whole feed
    # route AND stop stay one entity: line X at that stop only
    (elsewhere,) = by_id["route_elsewhere"]["entities"]
    assert elsewhere["route"]["key"] == tram["route"]["key"] and elsewhere["stop"]["id"] == alerts_in_redis["far"]
    # a station is shown as its stop group
    (station,) = by_id["station"]["entities"]
    assert station["stop"]["kind"] == "group" and station["stop"]["id"] == alerts_in_redis["group"]
    assert by_id["other_direction"]["entities"][0]["direction_id"] is not None
    assert by_id["other_route"]["entities"] == []  # a route we don't have: applies nowhere
    assert (await api.get("/alerts/kmk_t:stop")).json() == stop
    assert (await api.get("/alerts/kmk_t:nope")).status_code == 404
    assert (await api.get("/alerts/nope:stop")).status_code == 404
    assert (await api.get("/alerts?feed=kmk_a")).json()["alerts"] == []


async def test_alert_filters(api, sql, alerts_in_redis):
    route = alerts_in_redis["tram"]["route"]["key"]
    ids = lambda r: {a["key"].split(":", 1)[1] for a in r.json()["alerts"]}  # noqa: E731
    # an entity naming route + stop is about that route (the stop isn't asked for); directions aren't asked for either
    about_route = {"route", "feed", "route_elsewhere", "other_direction"}
    assert ids(await api.get(f"/alerts?route={route}")) == about_route
    assert ids(await api.get(f"/alerts?feed=kmk_t&route={route}")) == about_route
    assert (await api.get(f"/alerts?feed=kmk_a&route={route}")).status_code == 422
    assert ids(await api.get(f"/alerts?stop={group_id(sql, '802')}")) == {"stop", "station"}
    # route AND stop given: the route-at-another-stop alert doesn't apply here; feed-wide ones still do
    assert ids(await api.get(f"/alerts?route={route}&stop={group_id(sql, '802')}")) == {
        "route",
        "stop",
        "feed",
        "station",
        "other_direction",
    }


async def test_alerts_on_board_and_trip(api, sql, alerts_in_redis):
    board = (await api.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()
    # a board shows both directions of its routes
    assert {a["key"] for a in board["alerts"]} == {
        f"kmk_t:{i}" for i in ("route", "stop", "trip", "feed", "station", "other_direction")
    }
    # boards and trips carry what a banner needs; where it applies is at /alerts/{key}
    ref = next(a for a in board["alerts"] if a["key"] == "kmk_t:stop")
    assert ref == {
        "key": "kmk_t:stop",
        "cause": "construction",
        "effect": "detour",
        "severity": "severe",
        "header": "Header stop",
        "description": "Opis\nz Polskimi znakami: łódź",
        "active_periods": [{"start": NOW - 3600, "end": None}],
    }
    assert board["alerts"][0]["key"] == "kmk_t:stop"  # severe first
    tram = alerts_in_redis["tram"]
    trip = (await api.get(f"/trips/{tram['trip_key']}/live")).json()
    keys_ = {a["key"] for a in trip["alerts"]}
    assert {f"kmk_t:{i}" for i in ("route", "stop", "trip", "feed", "station")} <= keys_
    # another route, the same trip next week, the route's other direction
    assert not {"kmk_t:other_route", "kmk_t:trip_next_week", "kmk_t:other_direction"} & keys_
    assert "alerts" not in (await api.get(f"/trips/{tram['trip_key']}")).json()


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
        "/api/v1/trips/{trip_key}/live",
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
            "entities": [{"agency_id": "agency_1"}],
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
        keys = {f"kmk_t:{a}" for a in current}
        assert {a["key"] for a in board["alerts"]} == keys
        tram = next(d for d in board["departures"] if feed_of(d) == "kmk_t")
        assert {a["key"] for a in (await api.get(f"/trips/{tram['trip_key']}/live")).json()["alerts"]} == keys
        # /alerts lists everything unless asked
        assert len((await api.get("/alerts")).json()["alerts"]) == len(raws)
        assert {a["key"] for a in (await api.get("/alerts?active=true")).json()["alerts"]} == keys
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
