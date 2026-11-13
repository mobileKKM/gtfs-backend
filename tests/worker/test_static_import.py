"""Static import against a real Postgres (skipped if not running)."""

import time
from datetime import date, timedelta
from pathlib import Path

import pytest
from conftest import FIXTURES, ZTP_STOP_CODES, do_import, make_zip, meta, read_zip_csv

from gtfs_backend.worker.static import integrity
from gtfs_backend.worker.static.load import import_zip
from gtfs_backend.worker.static.partitions import FeedVersion
from gtfs_backend.worker.static.spec import TABLES
from gtfs_backend.worker.static.stops.queries import current_versions, namespace_versions
from gtfs_backend.worker.static.versions import retire_versions

STATIC = FIXTURES / "static"


def test_schema_matches_spec(db):
    for name, table in TABLES.items():
        cols = [
            r[0]
            for r in db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'gtfs' AND table_name = %s ORDER BY ordinal_position",
                (name,),
            ).fetchall()
        ]
        assert cols == ["feed_version_id", *table.column_names, *table.generated], name


def test_fixture_round_trip_is_value_identical(db):
    t = do_import(db, "kmk_t", STATIC / "GTFS_KRK_T.zip")
    a = do_import(db, "kmk_a", STATIC / "GTFS_KRK_A.zip")
    assert (t.version, a.version) == (1, 1)  # numbered per feed
    assert (t.feed_version_id, a.feed_version_id) == ("kmk_t_v1", "kmk_a_v1")  # the id every table joins on
    # the integrity check ran inside the import; check every row explicitly here too (no sampling)
    for name in ("stop_times", "shapes", "trips", "stops", "blocks"):
        integrity.SAMPLED_TABLES.clear()
        assert (
            integrity.check_table(db, TABLES[name], FeedVersion(t.feed_version_id, "kmk_t", 1), STATIC / "GTFS_KRK_T.zip") == []
        ), name

    # spot checks from the plan
    assert db.execute("SELECT pg_typeof(route_id)::text FROM gtfs.routes WHERE route_id = '102'").fetchone() == ("text",)
    row = db.execute(
        "SELECT departure_time::text FROM gtfs.stop_times WHERE feed_version_id = %s AND departure_time >= '24:00:00' LIMIT 1",
        (a.feed_version_id,),
    ).fetchone()
    raw = {r["departure_time"] for r in read_zip_csv(STATIC / "GTFS_KRK_A.zip", "stop_times.txt") if r["departure_time"] >= "24"}
    assert row is None or row[0] in raw  # e.g. '24:03:00' reads back unchanged
    assert db.execute("SELECT count(*) FROM gtfs.blocks WHERE feed_version_id = %s", (t.feed_version_id,)).fetchone()[0] == 133
    assert db.execute("SELECT pg_typeof(date)::text FROM gtfs.calendar_dates LIMIT 1").fetchone()[0] == "date"
    assert db.execute(
        "SELECT pg_typeof(stop_code)::text, stop_code FROM gtfs.stops WHERE stop_code = '802-01' LIMIT 1"
    ).fetchone() == ("text", "802-01")


def test_departures_are_the_boardable_stop_times(db):
    """core.departure, rebuilt here from the zip's CSV: every stop time with a departure where boarding is allowed
    (pickup_type not 1) that isn't its trip's last stop, with the trip's service, route and headsign."""
    t = do_import(db, "kmk_t", STATIC / "GTFS_KRK_T.zip")
    trips = {r["trip_id"]: r for r in read_zip_csv(STATIC / "GTFS_KRK_T.zip", "trips.txt")}
    stop_times = read_zip_csv(STATIC / "GTFS_KRK_T.zip", "stop_times.txt")
    last = {}
    for r in stop_times:
        last[r["trip_id"]] = max(last.get(r["trip_id"], -1), int(r["stop_sequence"]))

    def secs(text: str) -> int:
        h, m, sec = text.strip().split(":")
        return int(h) * 3600 + int(m) * 60 + int(sec)

    expected = set()
    for r in stop_times:
        pickup = int(r.get("pickup_type") or 0)
        if not (r.get("departure_time") or "").strip() or pickup == 1 or int(r["stop_sequence"]) >= last[r["trip_id"]]:
            continue
        trip = trips[r["trip_id"]]
        headsign = (r.get("stop_headsign") or "").strip() and r["stop_headsign"] or trip.get("trip_headsign") or None
        expected.add(
            (
                r["stop_id"],
                secs(r["departure_time"]),
                trip["service_id"],
                r["trip_id"],
                int(r["stop_sequence"]),
                trip["route_id"],
                headsign,
                pickup in (2, 3),
            )
        )
    stored = set(
        db.execute(
            "SELECT stop_id, departure_s, service_id, trip_id, stop_sequence, route_id, headsign, on_demand "
            "FROM core.departure WHERE feed_version_id = %s",
            (t.feed_version_id,),
        ).fetchall()
    )
    assert expected and stored == expected


def test_latest_version_is_picked_by_number_not_by_id_text(db):
    # "kmk_t_v10" sorts before "kmk_t_v9" as text
    db.execute("INSERT INTO core.feed (feed_id, stop_namespace) VALUES ('kmk_t', 'ztp') ON CONFLICT DO NOTHING")
    for v in (9, 10):
        db.execute(
            "INSERT INTO core.feed_version (feed_id, version, sha256, status) VALUES ('kmk_t', %s, %s, 'ready')", (v, f"x{v}")
        )
    assert namespace_versions(db.cursor(), ["ztp"]) == ["kmk_t_v10"]
    assert current_versions(db.cursor(), "kmk_a", "kmk_a_v1") == ["kmk_t_v10", "kmk_a_v1"]


def test_shared_codes_link_to_one_stop_and_ids_survive_reimport(db, tmp_path):
    do_import(db, "kmk_t", STATIC / "GTFS_KRK_T.zip")
    do_import(db, "kmk_a", STATIC / "GTFS_KRK_A.zip")
    rows = db.execute(
        """
        SELECT p.current_code, p.modes, array_agg(fv.feed_id ORDER BY fv.feed_id)
        FROM core.stop p
        JOIN core.stop_link l ON l.public_stop_id = p.id
        JOIN core.feed_version fv ON fv.id = l.feed_version_id
        WHERE p.current_code LIKE '802-%' GROUP BY p.current_code, p.modes ORDER BY 1
        """
    ).fetchall()
    shared = {code: (modes, feeds) for code, modes, feeds in rows}
    assert shared["802-01"] == (["bus", "tram"], ["kmk_a", "kmk_t"])
    groups = db.execute("SELECT count(DISTINCT stop_group_id) FROM core.stop WHERE current_code LIKE '802-%'").fetchone()[0]
    assert groups == 1
    before = dict(db.execute("SELECT current_code, id FROM core.stop").fetchall())

    # a new tram version (different bytes, same stops, one renamed) keeps every ID
    src = STATIC / "GTFS_KRK_T.zip"
    changed = tmp_path / "T2.zip"
    import zipfile

    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(changed, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == "stops.txt":
                data = data.replace(b"Rondo Mogilskie", b"Rondo Mogilskie Nowe")
            zout.writestr(item, data)
    assert do_import(db, "kmk_t", changed).version == 2
    after = dict(db.execute("SELECT current_code, id FROM core.stop").fetchall())
    assert after == before
    name, slug = db.execute("SELECT name, slug FROM core.stop WHERE current_code = '802-05'").fetchone()
    assert (name, slug) == ("Rondo Mogilskie Nowe", "rondo-mogilskie-nowe05")


def tiny_feed(path: Path, start: str, dates: list[str], night_trip: bool) -> Path:
    stop_times = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,06:00:00,06:00:00,S1,1\nT1,06:10:00,06:10:00,S2,2\n"
    )
    if night_trip:
        stop_times += "T9,24:50:00,24:50:00,S1,1\nT9,25:10:00,25:10:00,S2,2\n"
    trips = "route_id,service_id,trip_id\nR1,service_4,T1\n" + ("R1,service_4,T9\n" if night_trip else "")
    return make_zip(
        path,
        {
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw",
            "stops.txt": (
                f"stop_id,stop_code,stop_name,stop_lat,stop_lon\n"
                f"S1,900-01,Test {start},50.0,19.9\nS2,900-02,Test {start},50.001,19.9"
            ),
            "routes.txt": "route_id,agency_id,route_short_name,route_type\nR1,A,1,900",
            "trips.txt": trips,
            "stop_times.txt": stop_times,
            "calendar_dates.txt": "service_id,date,exception_type\n" + "\n".join(f"service_4,{d},1" for d in dates),
        },
    )



async def test_trip_index_holds_only_trips_running_on_its_days(db, database_url, tmp_path):
    """The realtime trip index loads yesterday/today/tomorrow; trips of services running on none of them can't match."""
    from gtfs_backend.db import async_pool
    from gtfs_backend.worker.realtime.trip_index import load_index

    today, later = date.today().strftime("%Y%m%d"), (date.today() + timedelta(days=10)).strftime("%Y%m%d")
    path = make_zip(
        tmp_path / "two.zip",
        {
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw",
            "stops.txt": "stop_id,stop_code,stop_name,stop_lat,stop_lon\nS1,900-01,X,50.0,19.9\nS2,900-02,X,50.001,19.9",
            "routes.txt": "route_id,agency_id,route_short_name,route_type\nR1,A,1,900",
            "trips.txt": "route_id,service_id,trip_id\nR1,now,T1\nR1,later,T2",
            "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,06:00:00,06:00:00,S1,1\nT1,06:10:00,06:10:00,S2,2\nT2,06:00:00,06:00:00,S1,1\nT2,06:10:00,06:10:00,S2,2",
            "calendar_dates.txt": f"service_id,date,exception_type\nnow,{today},1\nlater,{later},1",
        },
    )
    do_import(db, "kmk_t", path)
    db.commit()
    pool = async_pool(database_url)
    await pool.open()
    try:
        index = await load_index(pool, "kmk_t", time.time())
    finally:
        await pool.close()
    assert [trip_id for _, trip_id in index.trips] == ["T1"]


def test_stop_directions_name_the_next_other_group(db, tmp_path):
    # X: Alfa 01 -> Alfa 02 -> Beta -> Gamma; Y: Gamma -> Beta (no pickup) -> Alfa 01; Z like X but to Gamma only
    path = make_zip(
        tmp_path / "dirs.zip",
        {
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw",
            "stops.txt": "stop_id,stop_code,stop_name,stop_lat,stop_lon\n"
            "A1,910-01,Alfa,50.0,19.9\nA2,910-02,Alfa,50.0002,19.9\nB1,911-01,Beta,50.01,19.9\nC1,912-01,Gamma,50.02,19.9",
            "routes.txt": "route_id,agency_id,route_short_name,route_type\nR1,A,1,0\nR2,A,2,0",
            "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,s,X,Gamma\nR1,s,Y,Alfa\nR2,s,Z,Gamma",
            "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type\n"
            "X,06:00:00,06:00:00,A1,1,\nX,06:01:00,06:01:00,A2,2,\nX,06:05:00,06:05:00,B1,3,\nX,06:09:00,06:09:00,C1,4,\n"
            "Y,07:00:00,07:00:00,C1,1,\nY,07:04:00,07:04:00,B1,2,1\nY,07:08:00,07:08:00,A1,3,\n"
            "Z,08:00:00,08:00:00,A2,1,\nZ,08:09:00,08:09:00,C1,2,",
            "calendar_dates.txt": "service_id,date,exception_type\ns,20270115,1",
        },
    )
    v = do_import(db, "kmk_t", path).feed_version_id
    rows = db.execute(
        """
        SELECT p.current_code, g.current_code, d.route_id, d.headsign, d.trips FROM core.stop_direction d
        JOIN core.stop p ON p.id = d.public_stop_id JOIN core.stop_group g ON g.id = d.next_group_id
        WHERE d.feed_version_id = %s ORDER BY 1, 2, 3
        """,
        (v,),
    ).fetchall()
    assert rows == [
        ("910-01", "911", "R1", "Gamma", 1),  # not "towards Alfa": the next stop is in the same group
        ("910-02", "911", "R1", "Gamma", 1),
        ("910-02", "912", "R2", "Gamma", 1),
        ("911-01", "912", "R1", "Gamma", 1),  # Y has no pickup at Beta
        ("912-01", "911", "R1", "Alfa", 1),
    ]


def test_overnight_update_keeps_old_version_for_previous_service_day(db, tmp_path):
    # like 22/23 Oct: the old zip has day D (with a 25:10 night trip), the new one starts on D+1
    d = date.today() - timedelta(days=1)
    ymd = [(d + timedelta(days=i)).strftime("%Y%m%d") for i in range(3)]
    old = do_import(db, "kmk_t", tiny_feed(tmp_path / "old.zip", "old", ymd[:2], night_trip=True))
    new = do_import(db, "kmk_t", tiny_feed(tmp_path / "new.zip", "new", ymd[1:], night_trip=False))
    assert (old.version, new.version) == (1, 2)
    days = dict(
        db.execute("SELECT service_date, feed_version_id FROM core.service_day_version WHERE feed_id = 'kmk_t'").fetchall()
    )
    assert days == {
        d: old.feed_version_id,
        d + timedelta(days=1): new.feed_version_id,
        d + timedelta(days=2): new.feed_version_id,
    }

    # both test stops are one group, so there are no directions: add one to see it go with the version
    db.execute(
        "INSERT INTO core.stop_direction SELECT %s, id, stop_group_id, 'R1', '', 1 FROM core.stop LIMIT 1",
        (old.feed_version_id,),
    )
    directions = "SELECT count(*) FROM core.stop_direction WHERE feed_version_id = %s"

    # while day D is yesterday the old version still serves its night trips -> not retired
    assert retire_versions(db, "kmk_t", today=d + timedelta(days=1)) == []
    # once day D is older than yesterday it is retired and its rows are dropped
    assert retire_versions(db, "kmk_t", today=d + timedelta(days=2)) == [1]
    db.commit()
    assert db.execute("SELECT status FROM core.feed_version WHERE feed_id = 'kmk_t' AND version = 1").fetchone()[0] == "retired"
    assert db.execute("SELECT to_regclass('gtfs.stop_times__kmk_t__v1')").fetchone()[0] is None
    assert db.execute(directions, (old.feed_version_id,)).fetchone()[0] == 0
    assert db.execute("SELECT to_regclass('gtfs.stop_times__kmk_t__v2')").fetchone()[0] is not None
    assert (
        db.execute("SELECT count(*) FROM gtfs.stop_times WHERE feed_version_id = %s", (new.feed_version_id,)).fetchone()[0] == 2
    )


def test_invalid_feed_fails_and_keeps_current_version(db, tmp_path):
    good = do_import(db, "kmk_t", tiny_feed(tmp_path / "good.zip", "good", [date.today().strftime("%Y%m%d")], night_trip=False))
    bad = make_zip(
        tmp_path / "bad.zip",
        {
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw",
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,X,50.0,19.9",
            "routes.txt": "route_id,agency_id,route_short_name,route_type\nR1,A,1,900",
            "trips.txt": "route_id,service_id,trip_id\nR1,service_4,T1",
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,6am,06:00:00,S1,1\nT1,06:10:00,06:10:00,NOPE,2"
            ),
            "calendar_dates.txt": "service_id,date,exception_type\nservice_4,20261024,1",
        },
    )
    result = import_zip(db, "kmk_t", bad, meta(bad), "ztp", stop_code_pattern=ZTP_STOP_CODES)
    assert result.status == "failed"
    assert "arrival_time" in result.error and "unknown stops" in result.error
    statuses = dict(db.execute("SELECT version, status FROM core.feed_version WHERE feed_id = 'kmk_t'").fetchall())
    assert statuses == {1: "ready", 2: "failed"}
    assert (good.version, result.version) == (1, 2)
    days = db.execute(
        "SELECT feed_version_id, count(*) FROM core.service_day_version WHERE feed_id = 'kmk_t' GROUP BY 1"
    ).fetchall()
    assert days == [(good.feed_version_id, 1)]


def test_integrity_check_catches_changed_values(db, tmp_path, monkeypatch):
    path = tiny_feed(tmp_path / "t.zip", "x", [date.today().strftime("%Y%m%d")], night_trip=True)
    original = integrity.canonical_db

    def tamper(kind, value):  # simulate a loader that turns 25:10:00 into 01:10:00
        out = original(kind, value)
        return out - 86400 if kind.name == "TIME" and out and out >= 86400 else out

    monkeypatch.setattr(integrity, "canonical_db", tamper)
    result = import_zip(db, "kmk_t", path, meta(path), "ztp", stop_code_pattern=ZTP_STOP_CODES)
    assert result.status == "failed"
    assert "IntegrityError" in result.error and "stop_times" in result.error
    assert db.execute("SELECT count(*) FROM core.feed_version WHERE status = 'ready'").fetchone()[0] == 0


@pytest.mark.parametrize("name", ["GTFS_KRK_A.zip", "GTFS_KRK_M.zip", "GTFS_KRK_T.zip"])
def test_fixtures_validate_cleanly(name):
    from gtfs_backend.worker.static.read import read_gtfs
    from gtfs_backend.worker.static.validate import validate

    res = validate(read_gtfs(STATIC / name))
    assert res.errors == []
