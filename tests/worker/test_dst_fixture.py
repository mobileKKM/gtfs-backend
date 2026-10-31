"""Regression test for 2026-10-25 (clocks went back).

ZTP's tram system counts times from local midnight, so that day its trips ran one hour earlier than the
GTFS rule (noon - 12h) says. With `day_start: midnight` every tram resolves to its own trip with the
right times. Buses follow the GTFS rule. Uses the realtime snapshot taken that day and trimmed static
fixtures of the same versions.
"""

import collections

import pytest
from conftest import FIXTURES, do_import, message

from gtfs_backend.db import async_pool
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message
from gtfs_backend.worker.realtime.trip_index import ensure_stop_times, load_index

SNAPSHOT = FIXTURES / "dst_2026-10-25"
# route 957 alternates short/long variants, so these have no same-pattern trip an hour away
NO_TWIN = {
    "block_634_trip_13_service_3",
    "block_635_trip_9_service_3",
    "block_636_trip_13_service_3",
    "block_637_trip_12_service_3",
    "block_639_trip_9_service_3",
    "block_640_trip_13_service_3",
}


async def run_feed(database_url: str, feed: str, suffix: str, day_start: str = "noon_minus_12h"):
    pool = async_pool(database_url)
    await pool.open()
    try:
        tu, vp = message("TripUpdates", suffix), message("VehiclePositions", suffix)
        index = await load_index(pool, feed, tu.header.timestamp, day_start)
        state = FeedRealtimeState()

        async def load(keys):
            await ensure_stop_times(pool, index, keys)

        tu_res = await process_message(feed, tu, index, state, load, tu.header.timestamp)
        vp_res = await process_message(feed, vp, index, state, load, vp.header.timestamp)
        return tu_res, vp_res
    finally:
        await pool.close()


@pytest.fixture
def imported(db, database_url):
    for key, name in (("kmk_t", "GTFS_KRK_T.zip"), ("kmk_a", "GTFS_KRK_A.zip"), ("kmk_m", "GTFS_KRK_M.zip")):
        do_import(db, key, FIXTURES / "static" / name)
    db.commit()
    return database_url


def on_time(trips) -> float:
    delays = [t["delay_s"] for t in trips if t["delay_s"] is not None]
    return sum(abs(d) <= 15 * 60 for d in delays) / len(delays)


async def test_trams_counted_from_midnight_keep_their_own_trips(imported):
    tu, vp = await run_feed(imported, "kmk_t", "T", "midnight")
    # every tram is shown under the trip id it reports, except a few ids missing from static
    reported = {e.vehicle.trip.trip_id for e in message("VehiclePositions", "T").entity}
    assert len(vp.unmatched_vehicles) <= 7
    assert {v["trip"]["trip_id"] for v in vp.vehicles} == reported - {u["trip_id"] for u in vp.unmatched_vehicles}
    # the old "twin" remap had to hide these
    assert {v["trip"]["trip_id"] for v in vp.vehicles} >= NO_TWIN
    claims = collections.Counter(v["trip"]["key"] for v in vp.vehicles)
    assert max(claims.values()) == 1  # no trip claimed by two trams
    assert on_time(tu.trips) >= 0.95


async def test_trams_with_the_gtfs_rule_are_an_hour_off(imported):
    # what the GTFS rule alone gives that day: the reason for `day_start: midnight`
    tu, _ = await run_feed(imported, "kmk_t", "T")
    assert on_time(tu.trips) < 0.2


@pytest.mark.parametrize(("feed", "suffix"), [("kmk_a", "A"), ("kmk_m", "M")])
async def test_bus_feeds_follow_gtfs_rule(imported, feed, suffix):
    tu, vp = await run_feed(imported, feed, suffix)
    assert len(vp.vehicles) >= 0.95 * len(message("VehiclePositions", suffix).entity)
    assert on_time(tu.trips) >= 0.95
