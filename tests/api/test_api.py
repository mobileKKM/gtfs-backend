"""The HTTP API against the 2026-10-25 snapshot: trimmed ZTP timetables in Postgres, the realtime snapshot in Redis.

The data is loaded once per module; the API's clock is frozen at the tram feed's snapshot time.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import psycopg
import pytest
import redis
from conftest import FIXTURES, TEST_REDIS_URL, TTLS, do_import, message, reset_db
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from gtfs_backend import keys
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
# The snapshot's bus files were saved ~220 s before the tram files (NOW): the API, which ignores realtime measured
# longer ago than a feed's max age, gets one that covers that (test_stale_realtime_is_not_shown checks the default).
FIXTURE_MAX_AGE = 600
NO_SNAPSHOT = Path("/nonexistent/gtfs-test-snapshot")  # boards read Postgres unless a test builds one


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


@pytest.fixture
async def api(loaded):
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        c.ctx = app.state.ctx  # for tests that change Redis: the app's live state reloads it (reload_live)
        yield c


async def reload_live(api, feed_id: str = "kmk_t") -> None:
    """The app's live data, read from Redis again (what a change message would have brought): after a test wrote it."""
    await api.ctx.live.load_feed(feed_id)


@pytest.fixture(scope="module")
def sql(loaded):
    def run(query: str, *params):
        with psycopg.connect(loaded) as conn:
            return conn.execute(query, params).fetchall()

    return run


def ident(item: dict, field: str = "key") -> str:
    """Our public id: the last part of a key."""
    return item[field].rsplit("-", 1)[-1]


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
    rondo = [p for p in body["stops"] if ident(p, "group_key") == group_id(sql, "802")]
    assert len(rondo) >= 4 and all(p["name"] == "Rondo Mogilskie" and p["platform_code"] for p in rondo)
    assert all(p["key"] == f"rondo-mogilskie{p['platform_code']}-{ident(p)}" for p in rondo)
    assert {"bus", "tram"} <= {m for p in rondo for m in p["modes"]}


async def test_stops_on_map_as_groups(api, sql):
    body = (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=group")).json()
    assert not body["truncated"] and body["stops"] and all(s["kind"] == "group" for s in body["stops"])
    assert all(s["group_key"] is None and s["platform_code"] is None for s in body["stops"])
    rondo = next(s for s in body["stops"] if ident(s) == group_id(sql, "802"))
    assert rondo["key"] == f"rondo-mogilskie-{ident(rondo)}" and {"bus", "tram"} <= set(rondo["modes"])
    assert (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=bogus")).status_code == 422


@pytest.mark.parametrize("kind", ["stop", "group"])
async def test_stops_on_map_by_mode(api, kind):
    async def ids(modes: str = "") -> set[str]:
        body = (await api.get(f"/stops?bbox={CITY_BBOX}&kind={kind}&modes={modes}")).json()
        assert not body["truncated"]
        assert not modes or all(set(s["modes"]) & set(modes.split(",")) for s in body["stops"])
        return {ident(s) for s in body["stops"]}

    everything, tram, bus = await ids(), await ids("tram"), await ids("bus")
    assert tram and bus and tram < everything and bus < everything
    assert await ids("tram,bus") == tram | bus
    r = await api.get(f"/stops?bbox={CITY_BBOX}&kind={kind}&modes=nonexistent")
    assert r.status_code == 422 and r.json()["error"]["code"] == "invalid_input"


async def test_stop_search(api, sql):
    results = (await api.get("/stops/search?q=rondo mog")).json()["results"]
    rondo = results[0]
    assert rondo["name"] == "Rondo Mogilskie" and ident(rondo) == group_id(sql, "802")
    assert rondo["key"] == f"rondo-mogilskie-{ident(rondo)}"
    assert {"tram", "bus"} <= {line["mode"] for p in rondo["stops"] for d in p["directions"] for line in d["routes"]}
    served = [p for p in rondo["stops"] if p["directions"]]
    assert len(served) >= 4
    assert "kind" not in rondo and not any({"kind", "group_key"} & set(p) for p in rondo["stops"])
    own = {p[0] for p in sql("SELECT id FROM core.stop WHERE stop_group_id = %s", ident(rondo))}
    assert {ident(p) for p in rondo["stops"]} <= own
    for p in served:
        for d in p["directions"]:
            assert d["towards"] != "Rondo Mogilskie" and d["routes"] and len(d["headsigns"]) <= 3
    # the hint is the next stop group a trip from that stop reaches
    p1 = next(p for p in rondo["stops"] if ident(p) == stop_by_code(sql, "802-01"))
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


async def test_stops_without_trips_are_never_returned(api_ctx, sql, loaded):
    """A stop with no trips in the current feeds (empty modes) is hidden, and so is a group made only of them."""
    api, ctx = api_ctx
    gid = group_id(sql, "802")
    pid = stop_by_code(sql, "802-01")
    saved = sql("SELECT id, modes FROM core.stop")

    async def clear_modes(where: str, param: str):
        with psycopg.connect(loaded) as conn:
            conn.execute(f"UPDATE core.stop SET modes = '{{}}' WHERE {where} = %s", (param,))
        await ctx.catalog.reload()  # stops change with imports, which publish `static:updated`

    async def map_ids():
        return {ident(s) for s in (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"]}

    try:
        # one stop without trips: gone from the map and from its group, which is still found
        await clear_modes("id", pid)
        assert pid not in await map_ids()
        found = (await api.get("/stops/search?q=rondo mog")).json()["results"][0]
        assert ident(found) == gid and found["stops"] and pid not in {ident(p) for p in found["stops"]}
        # all of its stops without trips: the group disappears from search too
        await clear_modes("stop_group_id", gid)
        assert gid not in {ident(r) for r in (await api.get("/stops/search?q=rondo mog")).json()["results"]}
        on_map = (await api.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"]
        assert not {ident(s) for s in on_map if ident(s, "group_key") == gid}
        assert gid not in {ident(s) for s in (await api.get(f"/stops?bbox={RONDO_BBOX}&kind=group")).json()["stops"]}
    finally:
        with psycopg.connect(loaded) as conn:
            conn.cursor().executemany("UPDATE core.stop SET modes = %s WHERE id = %s", [(m, i) for i, m in saved])


async def test_stop_search_covers_only_search_namespaces(loaded, sql):
    """Search is limited to feeds.yml `search_namespaces` (ZTP); the map and the stop itself are not."""
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
    feeds = FEEDS.model_copy(update={"search_namespaces": ["km_kolej"]})  # no stops loaded there; skips validation
    app = create_app(settings, feeds=feeds, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        assert (await c.get("/stops/search?q=rondo mog")).json()["results"] == []
        gid = group_id(sql, "802")
        on_map = (await c.get(f"/stops?bbox={RONDO_BBOX}")).json()["stops"]
        assert any(ident(s, "group_key") == gid for s in on_map)
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
    assert ident(board["stop"]) == gid and "next_update" not in board
    assert r.headers["cache-control"] == "no-cache"
    assert len(deps) == 60
    assert [d["time"] for d in deps] == sorted(d["time"] for d in deps)
    assert all(d["time"] >= NOW - 30 and not d["departed"] for d in deps)
    assert {d["status"] for d in deps} <= {"scheduled", "live", "predicted", "canceled", "skipped"}
    assert {"live", "scheduled"} <= {d["status"] for d in deps}
    assert {feed_of(d) for d in deps} == {"kmk_a", "kmk_m", "kmk_t"}
    assert not {"feed_id", "trip_id", "service_date", "realtime", "vehicle_id"} & {k for d in deps for k in d}
    own = {p[0] for p in sql("SELECT id FROM core.stop WHERE stop_group_id = %s", gid)}
    assert all(ident(d["stop"]) in own and ident(d["stop"], "group_key") == gid for d in deps)
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
    assert any(d["departed"] for d in past)
    # vehicles that passed keep the last time realtime had, and where it came from
    assert any(d["departed"] and d["estimated"] and d["status"] in ("live", "predicted") for d in past)

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
    assert 0 < len(deps) <= 5 and {ident(d["stop"]) for d in deps} == {pid}

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
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
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


@pytest.mark.parametrize("arrival_only", [False, True])
async def test_departure_board_widens_for_early_trips(api_ctx, sql, monkeypatch, arrival_only):
    """A trip scheduled to depart past the window but running early enough to come before the page's last departure
    still makes the page, however early it runs (feeds allow any amount): the board finds it in its realtime index.
    Also when the feed predicts only its arrival, and the stop's arrival (+29 min) is inside the window while its
    departure (+31 min), by which the timetable is read, is past it."""
    from gtfs_backend.api.services import departures

    api, ctx = api_ctx

    pid = stop_by_code(sql, "802-01")
    day = date(2026, 10, 25)

    def dep(trip_id, at):
        key = keys.trip_key("kmk_t", day, trip_id)
        return departures.Scheduled("kmk_t", "kmk_t_v1", 1, day, trip_id, 1, pid, at, "X", "r", False, key)

    timetable = [dep(f"every2min_{i}", NOW + 60 + 120 * i) for i in range(40)]
    early = dep("early", NOW + departures.WINDOWS_S[0] + 60)  # departs a minute past the first window
    timetable.append(early)
    scheduled_arrival = early.scheduled_departure - 120
    predicted = NOW + 16 * 60  # a quarter of an hour early

    async def scheduled(conn, days, pids, start, end, lookback=departures.LOOKBACK_S):
        return [s for s in timetable if start - lookback <= s.scheduled_departure <= end]

    if arrival_only:  # its delay is against the scheduled arrival
        entry = [1, pid, predicted, None, predicted - scheduled_arrival, False, early.scheduled_departure, False]
    else:
        entry = [1, pid, None, predicted, predicted - early.scheduled_departure, False, early.scheduled_departure, False]
    live = ctx.live
    monkeypatch.setattr(live, "trips", {early.trip_key: [1, False, None, NOW, False, None, [entry]]})
    monkeypatch.setattr(live, "_at_stop", {pid: {(early.trip_key, 0)}})
    monkeypatch.setattr(departures, "scheduled", scheduled)
    monkeypatch.setattr(departures, "expected_span", lambda *_: departures.WINDOWS_S[0])  # whatever the stop's profile
    # the first window alone has 10 departures (+1 to +19 min); the early one comes after the 8th (+15 min)
    board = (await api.get(f"/stops/{pid}/departures?limit=10")).json()
    trip_keys = [d["trip_key"] for d in board["departures"]]
    assert trip_keys[8] == early.trip_key and trip_keys[:8] + trip_keys[9:] == [s.trip_key for s in timetable[:9]]
    assert board["departures"][8]["time"] == predicted and board["departures"][8]["status"] == "live"


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
    assert any(s["estimated_arrival"] or s["estimated_departure"] for s in live["stops"])
    assert not {"current_index"} & set(live) and not any("passed" in s for s in live["stops"])
    # each stop says where its time comes from: the feed's (live, ZTP predicts every stop) or none (scheduled)
    for s in live["stops"]:
        timed = bool(s["estimated_arrival"] or s["estimated_departure"])
        assert s["status"] in (("live", "predicted") if timed else ("scheduled", "skipped"))
    assert "live" in {s["status"] for s in live["stops"]}
    # the same live state by trip key
    by_key = await api.get(f"/trips/{v['trip_key']}/live")
    assert by_key.json() == live and by_key.headers["cache-control"] == "no-cache"

    # the trip itself: what doesn't change while it runs, cacheable, with stops in the order of the live ones
    r = await api.get(f"/trips/{v['trip_key']}")
    assert r.headers["cache-control"].startswith("public, max-age=")
    trip = r.json()
    assert trip["key"] == live["key"] and not {"vehicle", "status", "alerts"} & set(trip)
    assert trip["route"]["name"] == v["route"]["name"] and trip["headsign"] == v["headsign"]
    stops = trip["stops"]
    seqs = [s["stop_sequence"] for s in stops]
    assert seqs == sorted(seqs) == [s["stop_sequence"] for s in live["stops"]]
    assert all(s["stop"]["name"] and s["stop"]["group_key"] and "estimated_arrival" not in s for s in stops)

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
    trip = (await api.get(f"/trips/{key}")).json()
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
    pid = ident(tram["stop"])  # a stop the tram stops at
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
    await reload_live(api)
    yield {"raws": raws, "tram": tram, "pid": pid, "group": group, "far": far}
    await r.delete("rt:alerts:kmk_t")
    await reload_live(api)
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
    assert ident(stop["entities"][0]["stop"]) == alerts_in_redis["pid"] and stop["entities"][0]["stop"]["kind"] == "stop"
    assert by_id["trip"]["entities"] == [none | {"trip_key": tram["trip_key"]}]
    assert by_id["feed"]["entities"] == [none]  # whole feed
    # route AND stop stay one entity: line X at that stop only
    (elsewhere,) = by_id["route_elsewhere"]["entities"]
    assert elsewhere["route"]["key"] == tram["route"]["key"] and ident(elsewhere["stop"]) == alerts_in_redis["far"]
    # a station is shown as its stop group
    (station,) = by_id["station"]["entities"]
    assert station["stop"]["kind"] == "group" and ident(station["stop"]) == alerts_in_redis["group"]
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
        "/api/v1/stops/{stop_key}/departures",
        "/api/v1/trips/{trip_key}",
        "/api/v1/trips/{trip_key}/live",
        "/api/v1/routes/{route_key}/shapes",
        "/api/v1/alerts",
        "/api/v1/alerts/{alert_key}",
        "/api/v1/health",
        "/api/v1/metrics",
    } == set(paths)


# ------------------------------------------------------------------------------------------------- revalidation


async def test_polls_revalidate_without_a_response_cache(api, sql):
    """Nothing is cached: every request computes, the ETag (the body's hash) gives polling clients a 304."""
    url = f"/stops/{group_id(sql, '802')}/departures"
    first = await api.get(url)
    second = await api.get(url)
    assert first.status_code == second.status_code == 200
    assert first.content == second.content and first.headers["etag"] == second.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    again = await api.get(url, headers={"If-None-Match": first.headers["etag"]})
    assert again.status_code == 304 and again.content == b"" and again.headers["etag"] == first.headers["etag"]
    for path in (f"/stops?bbox={RONDO_BBOX}", "/stops/search?q=rondo", "/alerts"):
        r = await api.get(path)
        assert (await api.get(path, headers={"If-None-Match": r.headers["etag"]})).status_code == 304
    r = redis.Redis.from_url(TEST_REDIS_URL)
    assert list(r.scan_iter("api:*")) == []  # the API writes nothing to Redis
    r.close()


async def test_map_stops_are_serialized_like_the_model(api):
    """/stops joins precomputed JSON pieces: byte for byte what the model serializes to (so ETags stay the same)."""
    from gtfs_backend.api.schemas import StopList

    for query in (f"bbox={CITY_BBOX}", f"bbox={CITY_BBOX}&kind=group", f"bbox={RONDO_BBOX}&modes=tram", "bbox=0,0,1,1"):
        body = (await api.get(f"/stops?{query}")).content
        assert body == StopList.model_validate_json(body).model_dump_json().encode(), query


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
    await reload_live(api)
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
        await reload_live(api)
        await r.aclose()


@pytest.fixture
async def api_no_raise(loaded):
    """Like `api`, but app exceptions become responses (as in production) instead of reaching the test."""
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
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


# ------------------------------------------------------------------------------------------------- catalog, timing


@pytest.fixture
async def api_ctx(loaded):
    """Like `api`, with the app's context."""
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        yield c, app.state.ctx


def server_timing(response) -> dict[str, dict[str, str]]:
    """`db;dur=1.2;desc="3 queries", total;dur=4.0` -> {"db": {"dur": "1.2", "desc": "3 queries"}, "total": {...}}"""
    out = {}
    for metric in response.headers["server-timing"].split(", "):
        name, *params = metric.split(";")
        out[name] = {k: v.strip('"') for k, v in (p.split("=", 1) for p in params)}
    return out


async def test_server_timing(api_ctx, sql):
    api, _ = api_ctx
    r = await api.get(f"/stops/{group_id(sql, '802')}/departures")
    timing = server_timing(r)
    assert set(timing) == {"db", "db-wait", "redis", "serialize", "total"}
    # the timetable from Postgres (no snapshot here), realtime from memory: no Redis call
    assert int(timing["db"]["desc"].split()[0]) > 0 and timing["redis"]["desc"] == "0 calls"
    assert all(float(m["dur"]) >= 0 for m in timing.values())
    assert float(timing["total"]["dur"]) >= float(timing["serialize"]["dur"])
    # an answer without the database: none counted, and nothing left over from the request before
    timing = server_timing(await api.get(f"/vehicles?bbox={CITY_BBOX}"))
    assert timing["db"]["desc"] == "0 queries" and float(timing["serialize"]["dur"]) > 0
    # what still asks Redis: health
    assert server_timing(await api.get("/health"))["redis"]["desc"] != "0 calls"


async def test_server_timing_off(loaded):
    settings = ApiSettings(
        _env_file=None,
        database_url=loaded,
        redis_url=TEST_REDIS_URL,
        realtime_max_age=FIXTURE_MAX_AGE,
        snapshot_dir=NO_SNAPSHOT,
        server_timing=False,
    )
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        assert "server-timing" not in (await c.get(f"/vehicles?bbox={CITY_BBOX}")).headers


async def test_catalog_refreshes_in_the_background(api_ctx, sql, monkeypatch):
    api, ctx = api_ctx
    catalog = ctx.catalog
    url = f"/stops/{group_id(sql, '802')}/departures"
    await api.get(url)  # loads the routes it shows
    feeds, routes = catalog.feeds, dict(catalog._routes)
    assert routes

    # the refresh waits for its connection until released; requests don't wait for the refresh
    release = asyncio.Event()
    real_getconn = catalog.pool.getconn

    async def getconn(timeout=None):
        if asyncio.current_task() is catalog._refresh:
            await release.wait()
        return await real_getconn(timeout)

    monkeypatch.setattr(catalog.pool, "getconn", getconn)
    catalog.invalidate()  # as on `static:updated`
    assert (await api.get(url)).status_code == 200
    assert catalog.feeds is feeds and not catalog._refresh.done()
    catalog.invalidate()  # another import while the refresh runs
    release.set()
    await catalog._refresh
    assert catalog.feeds is not feeds  # reloaded
    assert all(catalog._routes[v] is r for v, r in routes.items())  # the versions are still ready: routes kept
    assert catalog._stale(catalog.clock())  # the second invalidation isn't lost: the next request refreshes again
    assert (await api.get(url)).status_code == 200
    await catalog._refresh
    assert not catalog._stale(catalog.clock())


async def test_catalog_refresh_failure_keeps_serving(api_ctx, sql, monkeypatch):
    api, ctx = api_ctx
    catalog = ctx.catalog
    feeds = catalog.feeds

    async def broken(now):
        raise psycopg.OperationalError("down")

    monkeypatch.setattr(catalog, "_load", broken)
    catalog.invalidate()
    url = f"/stops/{group_id(sql, '802')}/departures"
    assert (await api.get(url)).status_code == 200
    await catalog._refresh
    assert catalog.feeds is feeds
    failed = catalog._refresh
    assert (await api.get(url)).status_code == 200
    assert catalog._refresh is failed  # not retried at once (RETRY_S)


async def test_catalog_days_need_no_query(api_ctx, monkeypatch):
    from gtfs_backend.api.data.catalog import PRELOAD_DAYS

    _, ctx = api_ctx
    catalog = ctx.catalog
    queries = []
    real_getconn = catalog.pool.getconn

    async def getconn(timeout=None):
        queries.append(1)
        return await real_getconn(timeout)

    monkeypatch.setattr(catalog.pool, "getconn", getconn)
    today = catalog.today("kmk_t")
    days = [await catalog.day("kmk_t", today + timedelta(days=i)) for i in range(-1, PRELOAD_DAYS + 1)]
    assert queries == [] and days[1] is not None and days[1].date == today
    far = today + timedelta(days=60)
    assert await catalog.day("kmk_t", far) is not None
    assert len(queries) == 1  # outside the preloaded days: one indexed lookup


async def test_live_data_is_served_from_memory_while_redis_is_down(api_ctx, monkeypatch):
    """Requests read the live state of the process, not Redis: a Redis outage leaves the last data (until it is
    older than the feeds' max age); a process that never loaded it answers 503."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    api, ctx = api_ctx
    v = (await api.get(f"/vehicles?bbox={CITY_BBOX}")).json()["vehicles"][0]
    live = (await api.get(f"/trips/{v['trip_key']}/live")).json()

    async def down(*args, **kwargs):
        raise RedisConnectionError("down")

    for command in ("mget", "get", "hgetall", "geosearch", "pipeline", "execute_command"):
        monkeypatch.setattr(ctx.redis, command, down)
    assert (await api.get(f"/trips/{v['trip_key']}/live")).json() == live
    assert v in (await api.get(f"/vehicles?bbox={CITY_BBOX}")).json()["vehicles"]
    assert (await api.get("/trips/kmk_t:20261025:nope/live")).status_code == 404  # a missing trip still is a 404
    monkeypatch.setattr(ctx.live_state, "ready", False)
    r = await api.get(f"/vehicles?bbox={CITY_BBOX}")
    assert r.status_code == 503 and r.headers["retry-after"] == "1"


# ------------------------------------------------------------------------------------------------- metrics


async def test_metrics_count_requests_by_route_template_across_app_instances(loaded):
    """Each process flushes its histograms to Redis (at the latest when it stops); /metrics adds up all of them."""
    r = Redis.from_url(TEST_REDIS_URL)
    try:
        await r.delete(*[k async for k in r.scan_iter(match="metrics:*")] or ["metrics:none"])
        settings = ApiSettings(
            _env_file=None,
            database_url=loaded,
            redis_url=TEST_REDIS_URL,
            realtime_max_age=FIXTURE_MAX_AGE,
            snapshot_dir=NO_SNAPSHOT,
        )
        app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
        async with (
            app.router.lifespan_context(app),
            AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c,
        ):
            v = (await c.get(f"/vehicles?bbox={CITY_BBOX}")).json()["vehicles"][0]
            assert (await c.get(f"/trips/{v['trip_key']}/live")).status_code == 200
            assert (await c.get("/trips/kmk_t:20261025:nope/live")).status_code == 404
            assert (await c.get("/nothing/here")).status_code == 404
        app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)  # another process reads them
        async with (
            app.router.lifespan_context(app),
            AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c,
        ):
            body = (await c.get("/metrics?window=120")).json()
            assert (await c.get("/metrics?window=10")).status_code == 422
    finally:
        await r.aclose()
    routes = {e["route"]: e for e in body["endpoints"]}
    assert routes["GET /api/v1/vehicles"]["count"] == 1
    live = routes["GET /api/v1/trips/{trip_key}/live"]  # the template, never the trip key
    assert live["count"] == 2 and live["status"] == {"2xx": 1, "4xx": 1}
    assert routes["GET (no route)"]["status"] == {"4xx": 1}
    assert all(v["trip_key"] not in route for route in routes)
    assert 0 < live["p50_ms"] <= live["p90_ms"] <= live["p99_ms"] <= live["max_ms"]
    assert body["window_s"] == 120 and body["bucket_error"] == 0.091


async def test_stale_realtime_is_not_shown(loaded, sql):
    """With the worker stopped (nothing newer in Redis), trip updates and vehicles measured longer ago than the feed's
    max age disappear from the map, boards and trips: the timetable stays."""
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, snapshot_dir=NO_SNAPSHOT
    )  # max age 120 s
    for at, fresh in ((NOW + 60, True), (NOW + 121 + 30, False)):
        app = create_app(settings, feeds=FEEDS, clock=lambda at=at: at)
        async with (
            app.router.lifespan_context(app),
            AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c,
        ):
            trams = (await c.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"]
            board = (await c.get(f"/stops/{group_id(sql, '802')}/departures?limit=60")).json()["departures"]
            assert board  # the timetable either way
            statuses = {d["status"] for d in board if feed_of(d) == "kmk_t"}
            if fresh:
                assert trams and "live" in statuses
                trip = trams[0]["trip_key"]
            else:
                assert trams == [] and statuses == {"scheduled"}
                live = (await c.get(f"/trips/{trip}/live")).json()
                assert live["status"] == "scheduled" and live["vehicle"] is None
                assert {s["status"] for s in live["stops"]} == {"scheduled"}


async def test_deleted_trips_leave_the_board_canceled_ones_stay(api, sql):
    url = f"/stops/{group_id(sql, '802')}/departures?limit=30"
    deps = (await api.get(url)).json()["departures"]
    live = [d for d in deps if d["status"] == "live" and feed_of(d) == "kmk_t"][:2]
    assert len(live) == 2
    r = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    saved = {d["trip_key"]: await r.get(f"rt:trip:{d['trip_key']}") for d in live}
    try:
        for d, relationship in zip(live, ("DELETED", "CANCELED"), strict=True):
            await r.set(
                f"rt:trip:{d['trip_key']}",
                json.dumps({**json.loads(saved[d["trip_key"]]), "schedule_relationship": relationship}),
            )
        await reload_live(api)
        deleted, canceled = live
        after = {d["trip_key"]: d for d in (await api.get(url)).json()["departures"]}
        assert deleted["trip_key"] not in after
        assert after[canceled["trip_key"]]["status"] == "canceled"
        assert (await api.get(f"/trips/{deleted['trip_key']}/live")).json()["status"] == "canceled"
    finally:
        for trip_key, value in saved.items():
            await r.set(f"rt:trip:{trip_key}", value)
        await reload_live(api)
        await r.aclose()


# ------------------------------------------------------------------------------------------------- snapshot


@pytest.fixture(scope="module")
def snapshot_dir(loaded, tmp_path_factory):
    """The worker's timetable snapshot of the test data, built as of NOW."""
    from gtfs_backend.worker.static.snapshot import build, write

    root = tmp_path_factory.mktemp("snapshot")
    with psycopg.connect(loaded) as conn:
        write(root, build(conn, FEEDS, NOW))
    return root


@pytest.fixture
async def api_snapshot(loaded, snapshot_dir):
    """`api` with the snapshot: boards and trips read it instead of Postgres."""
    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=snapshot_dir
    )
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app), base_url="http://test/api/v1") as c:
        assert app.state.ctx.catalog.timetable is not None
        yield c, app.state.ctx


async def test_snapshot_departures_are_the_databases(api_snapshot, sql):
    """For every served stop and a few windows (across midnight too): the same rows as core.stop_departure."""
    from gtfs_backend.api.services import departures

    _, ctx = api_snapshot
    stops = [pid for (pid,) in sql("SELECT id FROM core.stop WHERE active AND cardinality(modes) > 0 ORDER BY id")]
    assert len(stops) > 100
    windows = [(NOW, NOW + 1800), (NOW - 3600, NOW + 7200), (NOW + 6 * 3600, NOW + 30 * 3600)]
    compared = 0
    for start, end in windows:
        days = await ctx.catalog.days_between(start - departures.LOOKBACK_S, end)
        assert ctx.catalog.timetable.days_for(days) is not None  # all in the snapshot: no fallback
        for i in range(0, len(stops), 25):
            pids = stops[i : i + 25]
            mine = await departures.timetable(ctx, days, pids, start, end)
            async with ctx.pool.connection() as conn:
                theirs = await departures.scheduled(conn, days, pids, start, end)
            assert sorted(mine, key=repr) == sorted(theirs, key=repr)
            compared += len(theirs)
    assert compared > 1000


async def test_boards_and_trips_from_the_snapshot_are_the_same(api, api_snapshot, sql):
    with_snapshot, ctx = api_snapshot
    groups = [gid for (gid,) in sql("SELECT id FROM core.stop_group WHERE active ORDER BY id LIMIT 40")]
    trips = set()
    for gid in groups:
        for query in ("?limit=30", f"?limit=20&time={NOW - 1800}", f"?limit=10&time={NOW + 20 * 3600}"):
            a, b = await api.get(f"/stops/{gid}/departures{query}"), await with_snapshot.get(f"/stops/{gid}/departures{query}")
            assert (a.status_code, a.content) == (b.status_code, b.content), (gid, query)
            trips |= {d["trip_key"] for d in a.json().get("departures", [])[:3]}
    assert len(trips) > 20
    for key in sorted(trips):
        for path in (f"/trips/{key}", f"/trips/{key}/live"):
            a, b = await api.get(path), await with_snapshot.get(path)
            assert (a.status_code, a.content) == (b.status_code, b.content), path
    # a trip on a day it doesn't run: 404 from the snapshot too
    some = sorted(trips)[0]
    feed, _, trip_id = some.split(":", 2)
    assert (await with_snapshot.get(f"/trips/{feed}:20261024:{trip_id}")).status_code == (
        await api.get(f"/trips/{feed}:20261024:{trip_id}")
    ).status_code


async def test_boards_use_the_snapshot_without_postgres(api_snapshot, sql, monkeypatch):
    c, ctx = api_snapshot
    queries = []
    real = ctx.pool.getconn

    async def getconn(timeout=None):
        queries.append(1)
        return await real(timeout)

    monkeypatch.setattr(ctx.pool, "getconn", getconn)
    board = await c.get(f"/stops/{group_id(sql, '802')}/departures")
    assert board.status_code == 200 and board.json()["departures"]
    key = board.json()["departures"][0]["trip_key"]
    assert (await c.get(f"/trips/{key}")).status_code == 200
    assert (await c.get(f"/trips/{key}/live")).status_code == 200
    assert queries == []  # the timetable from the snapshot, realtime from Redis


async def test_a_new_snapshot_generation_is_picked_up_and_old_ones_go(loaded, tmp_path):
    from gtfs_backend.snapshot import CURRENT, load_current
    from gtfs_backend.worker.static.snapshot import KEEP_GENERATIONS, build, write

    with psycopg.connect(loaded) as conn:
        built = build(conn, FEEDS, NOW)
    folders = [write(tmp_path, built) for _ in range(KEEP_GENERATIONS + 2)]
    assert (tmp_path / CURRENT).resolve() == folders[-1]
    assert sorted(p for p in tmp_path.iterdir() if p.is_dir() and not p.is_symlink()) == folders[-KEEP_GENERATIONS:]
    snap = load_current(tmp_path)
    assert snap.generation == folders[-1].name and snap.manifest["counts"]["departures"] > 0
    assert {d.feed_id for d in snap.days} == {"kmk_a", "kmk_m", "kmk_t"}


# the stop search's SQL before it moved into memory: what api/data/search.py must rank the same way
SEARCH_SQL = """
    WITH u AS (SELECT core.immutable_unaccent(lower(%(q)s)) AS q),
    x AS (SELECT q, replace(replace(replace(q, '\\', '\\\\'), '%%', '\\%%'), '_', '\\_') AS esc FROM u)
    SELECT g.id, g.name_search LIKE x.esc || '%%', g.name_search LIKE '%%' || x.esc || '%%',
           word_similarity(x.q, g.name_search), similarity(x.q, g.name_search), g.name
    FROM core.stop_group g, x
    WHERE g.active AND g.code_namespace = ANY(%(ns)s)
      AND EXISTS (SELECT 1 FROM core.stop p WHERE p.stop_group_id = g.id AND p.active AND cardinality(p.modes) > 0)
      AND (x.q <%% g.name_search OR g.name_search LIKE '%%' || x.esc || '%%')
    ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC, g.name
    LIMIT %(limit)s
"""


async def test_stop_search_in_memory_ranks_as_the_sql_did(api_ctx, sql):
    """For many queries (prefixes, words, typos, swapped words): the same groups in the same order, up to ties (same
    scores and name), where Postgres' order is arbitrary."""
    import random

    from gtfs_backend.trigram import unaccent_lower

    _, ctx = api_ctx
    names = sorted({n for (n,) in sql("SELECT name FROM core.stop_group WHERE active")})
    rng = random.Random(3)
    queries = ["rondo", "dw", "a", "plac", "nowy", "kurdwanow", "rondo mog", "mogilsk", "teatr bagatela", "x", "12", "zz top"]
    for name in rng.sample(names, 60):
        n = unaccent_lower(name)
        queries += [n[: rng.randint(1, len(n))], n.split()[-1], n[:-1] + "x", " ".join(reversed(n.split())), name.upper()]
    assert len(ctx.catalog.search) > 50
    with psycopg.connect(ctx.settings.database_url) as conn:
        for q in queries:
            params = {"q": q.strip(), "ns": ctx.feeds.searchable_namespaces, "limit": 500}
            rows = conn.execute(SEARCH_SQL, params).fetchall()
            mine = ctx.catalog.search.search(q, 500)
            assert set(mine) == {r[0] for r in rows}, q
            # the same ranking key at every position: ids may differ only within a tie
            key = {r[0]: (r[1], r[2], np.float32(r[3]), np.float32(r[4]), r[5]) for r in rows}
            assert [key[g] for g in mine] == [key[r[0]] for r in rows], q
            assert ctx.catalog.search.search(q, 3) == mine[:3]


async def test_vehicles_joined_from_pieces_are_the_models_json(api_ctx, monkeypatch):
    from gtfs_backend.api.data.geo import Bbox
    from gtfs_backend.api.services import vehicles

    c, ctx = api_ctx
    for modes, routes in ((set(), set()), ({"tram"}, set()), (set(), {"52", "4"}), ({"bus"}, {"52"})):
        bbox = Bbox.parse(CITY_BBOX)
        joined = await vehicles.in_bbox_json(ctx, bbox, modes, routes)
        assert joined == (await vehicles.in_bbox(ctx, bbox, modes, routes)).model_dump_json().encode()
    assert (await c.get(f"/vehicles?bbox={CITY_BBOX}")).content == await vehicles.in_bbox_json(
        ctx, Bbox.parse(CITY_BBOX), set(), set()
    )
    # the next request builds nothing: every vehicle's JSON is kept for its Redis value
    built = []
    real = vehicles.models

    async def counting(ctx_, raws):
        built.append(len(raws))
        return await real(ctx_, raws)

    monkeypatch.setattr(vehicles, "models", counting)
    await c.get(f"/vehicles?bbox={CITY_BBOX}")
    assert built == []


# ------------------------------------------------------------------------------------------------- WebSocket


@pytest.fixture
async def served(loaded):
    """The app on a real port (uvicorn), for WebSocket clients: (ws url, http base url, app)."""
    import uvicorn

    settings = ApiSettings(
        _env_file=None, database_url=loaded, redis_url=TEST_REDIS_URL, realtime_max_age=FIXTURE_MAX_AGE, snapshot_dir=NO_SNAPSHOT
    )
    app = create_app(settings, feeds=FEEDS, clock=lambda: NOW)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on"))
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"ws://127.0.0.1:{port}/api/v1/ws", f"http://127.0.0.1:{port}/api/v1", app
    server.should_exit = True
    await task


async def receive(ws, n, seconds=5.0):
    out = []
    for _ in range(n):
        out.append(json.loads(await asyncio.wait_for(ws.recv(), seconds)))
    return out


async def test_websocket_snapshots_are_the_rest_bodies_and_updates_follow(served, sql):
    from websockets.asyncio.client import connect

    from gtfs_backend.api.ws import tile_bbox, tile_of

    ws_url, http, _ = served
    async with AsyncClient(base_url=http) as c:
        v = (await c.get(f"/vehicles?bbox={CITY_BBOX}&modes=tram")).json()["vehicles"][0]
        z = 14
        x, y = tile_of(v["lat"], v["lon"], z)
        box = tile_bbox(z, x, y)
        tile_rest = (await c.get(f"/vehicles?bbox={box.min_lon},{box.min_lat},{box.max_lon},{box.max_lat}")).json()
        stop = group_id(sql, "802")
        board_rest = (await c.get(f"/stops/{stop}/departures")).json()
        trip_rest = (await c.get(f"/trips/{v['trip_key']}/live")).json()

    topics = [f"tile:{z}/{x}/{y}", f"stop:{stop}", f"trip:{v['trip_key']}"]
    async with connect(ws_url) as ws:
        await ws.send(json.dumps({"op": "subscribe", "topics": topics + ["tile:3/0/0", "stop:nope-zzzzz", "bad"]}))
        got = {m.get("topic"): m for m in await receive(ws, 6)}
        assert got[topics[0]]["type"] == "snapshot" and got[topics[0]]["data"] == tile_rest
        assert got[topics[1]]["data"] == board_rest and got[topics[2]]["data"] == trip_rest
        assert got["tile:3/0/0"]["error"]["code"] == "invalid_input" and got["stop:nope-zzzzz"]["error"]["code"] == "not_found"
        assert got["bad"]["type"] == "error"

        # the worker reports the tram a few metres on: the tile gets it as an update
        r = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
        feed = feed_of(v)
        members = await r.zrange(f"rt:veh:geo:{feed}", 0, -1)
        originals = [json.loads(x) for x in await r.mget([f"rt:veh:{m}" for m in members])]
        moved = [dict(o, lat=o["lat"] + 0.00005) if f"{feed}:{o['vehicle_id']}" == v["key"] else o for o in originals]
        store = RealtimeStore(r, [feed], TTLS)
        try:
            from gtfs_backend.worker.realtime.normalize import Result

            await store.write_vehicles(feed, Result(vehicles=originals))  # what the store last said: the originals
            await store.publish(
                feed, "vehicle_positions", NOW, {"vehicles": await store.write_vehicles(feed, Result(vehicles=moved))}
            )
            for _ in range(10):
                (update,) = await receive(ws, 1)
                if update["topic"] == topics[0]:
                    break
            assert update["type"] == "update" and update["data"]["remove"] == []
            assert [u["key"] for u in update["data"]["upsert"]] == [v["key"]]
            assert update["data"]["upsert"][0]["lat"] == round(v["lat"] + 0.00005, 5)
        finally:
            await store.write_vehicles(feed, Result(vehicles=originals))
            await store.publish(
                feed,
                "vehicle_positions",
                NOW,
                {"vehicles": {"set": {f"{feed}:{o['vehicle_id']}": o for o in originals}, "del": []}},
            )
            await r.aclose()

        await ws.send(json.dumps({"op": "unsubscribe", "topics": topics}))
        await ws.send("not json")
        for _ in range(10):  # updates of the topics still on their way may come first
            (error,) = await receive(ws, 1)
            if error["type"] == "error":
                break
        assert error["type"] == "error" and error["topic"] is None


async def test_extra_trips_come_from_realtime_onto_boards_and_trip_views(api_ctx, sql, monkeypatch):
    """A trip no timetable has (GTFS-RT NEW, built by the worker): its departures on boards, its view and live state."""
    api, ctx = api_ctx
    gid = group_id(sql, "802")
    tram = next(d for d in (await api.get(f"/stops/{gid}/departures?limit=60")).json()["departures"] if feed_of(d) == "kmk_t")
    route_id = tram["route"]["key"].split(":", 1)[1]
    pole, other = ident(tram["stop"]), stop_by_code(sql, "125-01")
    key = "kmk_t:20261025:extra_1"
    at = NOW + 600
    entries = [[1, pole, at, at, None, False, at, False, at], [2, other, at + 300, None, None, False, None, False, at + 300]]
    live = ctx.live
    monkeypatch.setattr(live, "trips", {**live.trips, key: [1, False, None, NOW, False, None, entries, [route_id, "Extra"]]})
    at_stop = {k: set(v) for k, v in live._at_stop.items()}
    at_stop.setdefault(pole, set()).add((key, 0))
    at_stop.setdefault(other, set()).add((key, 1))
    monkeypatch.setattr(live, "_at_stop", at_stop)

    board = (await api.get(f"/stops/{gid}/departures?limit=60")).json()["departures"]
    (extra,) = [d for d in board if d["trip_key"] == key]
    assert (extra["time"], extra["status"], extra["headsign"], extra["route"]["key"]) == (
        at,
        "live",
        "Extra",
        tram["route"]["key"],
    )
    assert extra["scheduled"] == at and extra["delay_s"] is None
    view = (await api.get(f"/trips/{key}")).json()
    assert view["headsign"] == "Extra" and view["route"]["key"] == tram["route"]["key"] and view["shape"]["source"] == "stops"
    assert [(s["stop_sequence"], s["scheduled_departure"]) for s in view["stops"]] == [(1, at), (2, None)]
    assert view["stops"][0]["stop"]["key"] == tram["stop"]["key"]
    trip_live = (await api.get(f"/trips/{key}/live")).json()
    assert trip_live["status"] == "live" and [s["status"] for s in trip_live["stops"]] == ["live", "live"]
