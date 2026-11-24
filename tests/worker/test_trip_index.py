"""load_index against an imported timetable: only trips that can match are loaded, unchanged data is reused."""

import dataclasses

import pytest
from conftest import FIXTURES, do_import, message

from gtfs_backend.db import async_pool
from gtfs_backend.worker.realtime.trip_index import ensure_stop_times, load_index


@pytest.fixture
async def pool(db, database_url):
    do_import(db, "kmk_t", FIXTURES / "static" / "GTFS_KRK_T.zip")
    db.commit()
    pool = async_pool(database_url)
    await pool.open()
    yield pool
    await pool.close()


NOW = message("TripUpdates", "T").header.timestamp


async def test_only_trips_running_on_the_indexed_days_are_loaded(pool, db):
    # a trip of a service that runs on none of the indexed days
    db.execute(
        "INSERT INTO gtfs.trips (feed_version_id, trip_id, route_id, service_id) "
        "SELECT feed_version_id, 'not_these_days', route_id, 'service_99' FROM gtfs.trips "
        "WHERE feed_version_id = 'kmk_t_v1' LIMIT 1"
    )
    db.execute(
        "INSERT INTO gtfs.stop_times (feed_version_id, trip_id, stop_sequence, stop_id, arrival_time, departure_time) "
        "SELECT feed_version_id, 'not_these_days', 1, stop_id, arrival_time, departure_time FROM gtfs.stop_times "
        "WHERE feed_version_id = 'kmk_t_v1' LIMIT 1"
    )
    db.commit()
    idx = await load_index(pool, "kmk_t", NOW, "midnight")
    assert idx.trips and all(info.service_id in idx.services[v] for (v, _), info in idx.trips.items())
    assert "not_these_days" not in idx.by_trip_id
    total = db.execute("SELECT count(*) FROM gtfs.trips WHERE feed_version_id = 'kmk_t_v1'").fetchone()[0]
    assert len(idx.trips) == total - 1


async def test_an_unchanged_timetable_is_reused_and_stations_are_read_again(pool):
    first = await load_index(pool, "kmk_t", NOW, "midnight")
    some = next(iter(first.trips))
    await ensure_stop_times(pool, first, {some})
    again = await load_index(pool, "kmk_t", NOW, "midnight", previous=first)
    assert again.trips is first.trips and again.stop_links is first.stop_links
    assert again.stop_times[some] == first.stop_times[some]
    assert again is not first and again.stations is not first.stations


async def test_changed_services_load_the_trips_again_but_keep_their_stop_times(pool):
    first = await load_index(pool, "kmk_t", NOW, "midnight")
    some = next(iter(first.trips))
    await ensure_stop_times(pool, first, {some})
    # as if the previous index was built for other days
    elsewhere = {v: frozenset({"elsewhere"}) for v in first.services}
    other = dataclasses.replace(first, services=elsewhere, stop_times=first.stop_times)
    again = await load_index(pool, "kmk_t", NOW, "midnight", previous=other)
    assert again.trips is not first.trips and again.trips == first.trips
    assert again.stop_times[some] == first.stop_times[some]
