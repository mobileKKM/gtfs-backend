"""Laying realtime trip updates over timetabled departures, and paging (no services needed)."""

from datetime import date

from gtfs_backend import keys
from gtfs_backend.api.data.live import LiveState
from gtfs_backend.api.data.realtime import TripUpdate
from gtfs_backend.api.services.departures import (
    GRACE_S,
    WINDOWS_S,
    Row,
    Scheduled,
    expected_span,
    overlay,
    select,
    vehicle_state,
)
from gtfs_backend.api.services.vehicles import stop_state

NOW = 1_800_000_000


def sched(trip="t1", seq=5, dep=NOW + 600, version=1) -> Scheduled:
    day = date(2027, 1, 15)
    key = keys.trip_key("kmk_t", day, trip)
    return Scheduled("kmk_t", "kmk_t_v1", version, day, trip, seq, "plat1", dep, "Bardosa", "r1", False, key)


def rt(*rows, version=1, relationship="SCHEDULED", vehicle="704") -> TripUpdate:
    """rows: (stop_sequence, predicted_departure, delay_s, extra)"""
    return TripUpdate(
        {
            "version": version,
            "vehicle_id": vehicle,
            "schedule_relationship": relationship,
            "stops": [
                {"stop_sequence": q, "predicted_departure": p, "predicted_arrival": None, "delay_s": d, **x}
                for q, p, d, x in rows
            ],
        }
    )


def test_no_update_is_scheduled_only():
    live = overlay(sched(), None, NOW)
    assert (live.status, live.realtime, live.estimated, live.vehicle_id, live.departed) == ("scheduled", False, None, None, False)


def test_without_realtime_departed_after_grace():
    assert not overlay(sched(dep=NOW - GRACE_S + 1), None, NOW).departed
    live = overlay(sched(dep=NOW - GRACE_S - 1), None, NOW)
    assert live.departed and live.status == "scheduled"  # departed is when, status where the time comes from


def test_prediction_for_the_stop():
    live = overlay(sched(), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.realtime, live.estimated, live.delay_s, live.vehicle_id) == ("live", True, NOW + 720, 120, "704")


def test_delay_carried_over_from_an_earlier_stop_is_predicted():
    live = overlay(sched(), rt((5, NOW + 720, 120, {"propagated": True})), NOW)
    assert (live.status, live.estimated, live.delay_s) == ("predicted", NOW + 720, 120)


def test_no_data_stop_is_scheduled():
    # GTFS-RT NO_DATA: no times for the stop (and none propagated past it): the timetable's
    live = overlay(sched(), rt((5, None, None, {"schedule_relationship": "NO_DATA"})), NOW)
    assert (live.status, live.realtime, live.estimated, live.vehicle_id) == ("scheduled", False, None, "704")


def test_arrival_only_prediction_is_used():
    update = rt((5, None, 60, {"predicted_arrival": NOW + 660}))
    assert overlay(sched(), update, NOW).estimated == NOW + 660


def veh(seq, status="IN_TRANSIT_TO", trip="t1", version=1) -> dict:
    return {"trip": {"key": f"kmk_t:20270115:{trip}", "version": version}, "current_stop_sequence": seq, "current_status": status}


def test_departed_once_the_vehicle_reports_a_later_stop():
    update = rt((5, NOW + 60, 240, {}))  # still listed by the feed: the TripUpdate doesn't say passed
    assert overlay(sched(), update, NOW, veh(6)).departed
    assert not overlay(sched(), update, NOW, veh(5)).departed  # heading to it
    assert not overlay(sched(), update, NOW, veh(5, "STOPPED_AT")).departed  # at it, not yet left
    assert not overlay(sched(), update, NOW, veh(4)).departed
    # the last known time stays, and where it came from
    live = overlay(sched(), update, NOW, veh(6))
    assert (live.estimated, live.delay_s, live.status) == (NOW + 60, 240, "live")


def test_vehicle_of_another_trip_or_version_says_nothing():
    update = rt((5, NOW + 600, 0, {}))
    assert overlay(sched(), update, NOW, veh(9, trip="t2")).status == "live"
    assert overlay(sched(), update, NOW, veh(9, version=2)).status == "live"


def test_unlisted_stops_are_not_taken_as_passed():
    # a TripUpdate lists predictions only; an earlier stop it doesn't list is decided by the vehicle or the clock
    live = overlay(sched(seq=2, dep=NOW + 300), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.departed) == ("scheduled", False)
    live = overlay(sched(seq=2, dep=NOW - 300), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.departed) == ("scheduled", True)


def test_stop_beyond_the_predictions_keeps_the_schedule():
    live = overlay(sched(seq=9), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.realtime, live.estimated, live.vehicle_id) == ("scheduled", False, None, "704")


def test_skipped_and_canceled():
    assert overlay(sched(), rt((5, NOW + 600, 0, {"schedule_relationship": "SKIPPED"})), NOW).status == "skipped"
    assert overlay(sched(), rt((5, NOW + 600, 0, {}), relationship="CANCELED"), NOW).status == "canceled"


def test_update_from_another_version_is_ignored():
    live = overlay(sched(), rt((5, NOW + 720, 120, {}), version=2), NOW)
    assert (live.realtime, live.estimated) == (False, None)


def rows(scheduled, updates=None):
    updates = updates or {}
    return [Row(s, overlay(s, updates.get(s.trip_key), NOW)) for s in scheduled]


def test_live_board_sorts_by_live_time_and_keeps_late_departures():
    late = sched("late", dep=NOW - 900)  # scheduled 15 min ago, 20 min late
    early = sched("early", dep=NOW + 300)
    gone = sched("gone", dep=NOW - GRACE_S - 1)  # no realtime, left already
    just_left = sched("just_left", dep=NOW - GRACE_S + 5)
    rs = rows([early, gone, late, just_left], {late.trip_key: rt((5, NOW + 301, 1201, {}))})
    out = select(rs, NOW, limit=10, start=NOW - GRACE_S, end=NOW + 3600, live=True)
    assert [r.s.trip_id for r in out] == ["just_left", "early", "late"]
    assert out[2].time == NOW + 301
    assert [r.s.trip_id for r in select(rs, NOW, limit=1, start=NOW - GRACE_S, end=NOW + 3600, live=True)] == ["just_left"]
    # outside the window
    assert select(rows([sched(dep=NOW + 7200)]), NOW, limit=10, start=NOW, end=NOW + 3600, live=True) == []


def test_board_from_a_time_includes_departed():
    trips = [sched("old1", dep=NOW - 600), sched("old2", dep=NOW - 300), sched("next", dep=NOW + 60)]
    rs = rows(trips)
    out = select(rs, NOW, limit=10, start=NOW - 400, end=NOW + 3600, live=False)
    assert [(r.s.trip_id, r.live.departed) for r in out] == [("old2", True), ("next", False)]
    assert [r.s.trip_id for r in select(rs, NOW, limit=1, start=NOW - 3600, end=NOW + 3600, live=False)] == ["old1"]


def test_paging_forward_by_time_without_gaps():
    # five departures per minute; the next page starts at the last time, repeats stay within that second
    trips = [sched(f"t{i}", dep=NOW + 60 * (i // 5)) for i in range(12)]
    rs = rows(trips)
    seen, start = [], NOW
    while True:
        new = [r.s.trip_id for r in select(rs, NOW, limit=6, start=start, end=NOW + 3600, live=False) if r.s.trip_id not in seen]
        if not new:
            break
        seen += new
        start = max(r.time for r in rs if r.s.trip_id in seen)
    assert len(seen) == 12


def _row(seq=5, status_dep=NOW + 600, update=None, vehicle=None) -> Row:
    s = sched(seq=seq, dep=status_dep)
    return Row(s, overlay(s, update if update is not None else rt((seq, NOW + 600, 0, {})), NOW, vehicle), vehicle)


def test_vehicle_state_only_for_the_stop_it_reports():
    veh = {"current_stop_sequence": 5, "current_status": "STOPPED_AT"}
    assert vehicle_state(_row(seq=5), veh) == "at_stop"
    assert vehicle_state(_row(seq=5), {**veh, "current_status": "INCOMING_AT"}) == "arriving"
    assert vehicle_state(_row(seq=5), {**veh, "current_status": "IN_TRANSIT_TO"}) is None
    assert vehicle_state(_row(seq=6), veh) is None
    assert vehicle_state(_row(seq=5), None) is None


def test_vehicle_state_not_for_departed_or_canceled():
    at = veh(5, "STOPPED_AT")
    assert vehicle_state(_row(vehicle=at), at) == "at_stop"
    assert vehicle_state(_row(vehicle=veh(6)), veh(5, "STOPPED_AT")) is None  # the vehicle is past it: departed
    assert vehicle_state(_row(update=rt((5, NOW + 600, 0, {}), relationship="CANCELED")), at) is None


def test_status_needs_the_stop_sequence():
    assert stop_state({"current_stop_sequence": 5, "current_status": "STOPPED_AT"}) == (5, "stopped_at")
    assert stop_state({"current_status": "STOPPED_AT"}) == (None, None)
    assert stop_state({"current_stop_sequence": 5}) == (5, None)
    assert stop_state({}) == (None, None)


# ------------------------------------------------------------------------------------------------- first window


def test_expected_span_busy_stop():
    """60 an hour: 1.5 pages of 20 within 30 min."""
    assert expected_span((60,) * 24, 8 * 3600, 20) == 30 * 60


def test_expected_span_reads_across_hours_and_midnight():
    hourly = [0] * 24
    hourly[23], hourly[5] = 10, 30  # 10 at 23:00, nothing at night, 30 from 05:00
    # from 23:30: 5 left before midnight, the other 25 of 30 by 05:50
    assert expected_span(tuple(hourly), 23 * 3600 + 1800, 20) == 6 * 3600 + 1200


def test_expected_span_without_departures_reads_a_day():
    assert expected_span((0,) * 24, 12 * 3600, 20) == WINDOWS_S[-1]
    few = (1,) * 24
    assert expected_span(few, 0, 20) == WINDOWS_S[-1]  # 24 a day: 30 aren't expected within it


def live_with(*trips) -> LiveState:
    """A live state holding (trip key, CompactTrip) pairs, no Redis."""
    live = LiveState(redis=None, feed_ids=["kmk_t", "kmk_a"], kinds={})
    for key, trip in trips:
        live._set_trip(key, trip)
    return live


def compact(*entries, version=1, canceled=False, deleted=False, ts=1000):
    """entries: (seq, pid, predicted_arrival, predicted_departure, delay, scheduled_departure, propagated)"""
    stops = [[q, pid, a, d, delay, False, sched, prop] for q, pid, a, d, delay, sched, prop in entries]
    return [version, canceled, None, ts, deleted, None, stops]


def test_board_reads_the_predictions_at_its_stops_only():
    key = "kmk_t:20261025:a"
    live = live_with((key, compact((1, "p1", None, 50, 60, -10, True), (2, "p2", None, 60, 60, 0, False))))
    state = live.board(["kmk_t"], ["p1"], {}).state(key)
    assert state.stop(1).propagated and state.stop(1).predicted == 50
    assert state.stop(2) is None  # not one of the board's stops
    assert overlay(sched(seq=1, dep=-10), state, 0).status == "predicted"
    assert overlay(sched(seq=2, dep=0), live.board(["kmk_t"], ["p2"], {}).state(key), 0).status == "live"
    assert live.board(["kmk_a"], ["p1"], {}).state(key) is None  # another feed's board


def test_board_finds_trips_running_early():
    """Trips scheduled to depart after a time but predicted before the page ends, from `since` on. By the scheduled
    departure, which boards read their rows by."""
    early, last_stop = "kmk_t:20261025:early", "kmk_t:20261025:end"
    live = live_with(
        (early, compact((3, "p", None, 1600, -1100, 2700, False))),  # departs at 2700, 1100 s early
        (last_stop, compact((9, "p", 1400, None, -1000, None, False))),  # no departure: never on a board
    )
    board = live.board(["kmk_t"], ["p"], {})
    assert board.earliest_early(after=1800, since=0) == 1600
    assert board.earliest_early(after=2700, since=0) is None  # its row is read
    assert board.earliest_early(after=1800, since=1700) is None  # before the board starts


def test_board_finds_arrival_only_predictions_by_the_scheduled_departure():
    """An arrival-only prediction's delay is against the scheduled arrival: predicted - delay_s would be the arrival
    (+29 min), inside a 30 min window, though the row is read by its departure (+31 min), outside it."""
    key = "kmk_t:20261025:dwell"
    arrival, departure, predicted = 29 * 60, 31 * 60, 16 * 60
    live = live_with((key, compact((4, "p", predicted, None, predicted - arrival, departure, False))))
    assert live.board(["kmk_t"], ["p"], {}).earliest_early(after=30 * 60, since=0) == predicted


def test_deleted_and_canceled_trips_read_as_such():
    a, b = "kmk_t:20261025:a", "kmk_t:20261025:b"
    live = live_with((a, compact(canceled=True, deleted=True)), (b, compact(canceled=True)))
    board = live.board(["kmk_t"], [], {})
    assert (board.state(a).canceled, board.state(a).deleted) == (True, True)
    assert (board.state(b).canceled, board.state(b).deleted) == (True, False)
    update = live.trip_update(a)
    assert update.canceled and update.deleted
    deleted = TripUpdate({"version": 1, "schedule_relationship": "DELETED", "stops": []})
    assert deleted.canceled and deleted.deleted


def test_stale_trip_states_count_as_no_realtime():
    key = "kmk_t:20261025:a"
    live = live_with((key, compact(ts=1000)))
    assert live.board(["kmk_t"], [], {"kmk_t": 1000}).state(key) is not None
    assert live.board(["kmk_t"], [], {"kmk_t": 1001}).state(key) is None
    assert live.board(["kmk_t"], [], {"kmk_a": 5000}).state(key) is not None  # another feed's limit
