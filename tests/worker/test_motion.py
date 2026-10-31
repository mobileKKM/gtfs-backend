"""Filling in bearing and speed from a vehicle's previous fix."""

from gtfs_backend.worker.realtime.motion import MotionTracker, bearing_deg, distance_m

LAT, LON = 50.06, 19.94
NORTH_100M = LAT + 100 / 111_195  # ~100 m north


def test_geometry():
    assert round(distance_m(LAT, LON, NORTH_100M, LON)) == 100
    assert bearing_deg(LAT, LON, NORTH_100M, LON) == 0
    assert bearing_deg(LAT, LON, LAT, LON + 0.01) == 90
    assert bearing_deg(LAT, LON, LAT - 0.01, LON) == 180
    assert bearing_deg(LAT, LON, LAT, LON - 0.01) == 270


def test_first_fix_has_nothing_to_compare_with():
    assert MotionTracker().fill("302", LAT, LON, 1000, None, None) == (None, None)


def test_bearing_and_speed_come_from_the_previous_fix():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, None, None)
    assert m.fill("302", NORTH_100M, LON, 1010, None, None) == (0.0, 10.0)


def test_feed_values_are_kept():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, None, None)
    assert m.fill("302", NORTH_100M, LON, 1010, 45.0, None) == (45.0, 10.0)
    assert m.fill("302", NORTH_100M + 0.001, LON, 1020, None, 3.0) == (0.0, 3.0)


def test_standing_vehicle_keeps_its_last_bearing():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, 90.0, None)
    assert m.fill("302", LAT + 0.00002, LON, 1010, None, None) == (90.0, 0.0)  # ~2 m of GPS jitter
    assert m.fill("302", LAT, LON, 1020, None, None) == (90.0, 0.0)


def test_repeated_fix_repeats_the_estimate():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, None, None)
    m.fill("302", NORTH_100M, LON, 1010, None, None)
    assert m.fill("302", NORTH_100M, LON, 1010, None, None) == (0.0, 10.0)


def test_long_gap_or_gps_jump_gives_no_speed():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, 90.0, None)
    assert m.fill("302", NORTH_100M, LON, 1500, None, None) == (90.0, None)  # 8 min since the last fix
    assert m.fill("302", NORTH_100M + 0.1, LON, 1510, None, None) == (None, None)  # 11 km in 10 s


def test_slow_feed_still_gets_estimates():
    # km_kolej rewrites its file every 5 minutes: train 10738 moved ~3.6 km in 309 s (2026-10-27)
    m = MotionTracker()
    m.fill("10738", 50.47572, 20.01245, 1793102373, None, None)
    bearing, speed = m.fill("10738", 50.44276, 20.00936, 1793102682, None, None)
    assert 180 < bearing < 195 and 11 < speed < 12.5  # south, ~42 km/h


def test_vehicles_are_tracked_separately_and_forgotten():
    m = MotionTracker()
    m.fill("302", LAT, LON, 1000, None, None)
    m.fill("303", NORTH_100M, LON, 1000, None, None)
    m.forget_before(1001)
    assert m.fill("302", NORTH_100M, LON, 1010, None, None) == (None, None)
