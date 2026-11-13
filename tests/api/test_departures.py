"""Laying realtime trip updates over timetabled departures, and paging (no services needed)."""

from datetime import date

from gtfs_backend.api.services.departures import GRACE_S, Row, Scheduled, overlay, select, vehicle_state
from gtfs_backend.api.services.vehicles import stop_state

NOW = 1_800_000_000


def sched(trip="t1", seq=5, dep=NOW + 600, version=1) -> Scheduled:
    return Scheduled("kmk_t", "kmk_t_v1", version, date(2027, 1, 15), trip, seq, "plat1", dep, "Bardosa", "r1")


def rt(*rows, version=1, relationship="SCHEDULED", vehicle="704") -> dict:
    """rows: (stop_sequence, predicted_departure, delay_s, extra)"""
    return {
        "version": version,
        "vehicle_id": vehicle,
        "schedule_relationship": relationship,
        "stops": [
            {"stop_sequence": q, "predicted_departure": p, "predicted_arrival": None, "delay_s": d, **x} for q, p, d, x in rows
        ],
    }


def test_no_update_is_scheduled_only():
    live = overlay(sched(), None, NOW)
    assert (live.status, live.realtime, live.estimated, live.vehicle_id) == ("scheduled", False, None, None)


def test_without_realtime_departed_after_grace():
    assert overlay(sched(dep=NOW - GRACE_S + 1), None, NOW).status == "scheduled"
    assert overlay(sched(dep=NOW - GRACE_S - 1), None, NOW).status == "departed"


def test_prediction_for_the_stop():
    live = overlay(sched(), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.realtime, live.estimated, live.delay_s, live.vehicle_id) == ("live", True, NOW + 720, 120, "704")


def test_arrival_only_prediction_is_used():
    update = rt((5, None, 60, {}))
    update["stops"][0]["predicted_arrival"] = NOW + 660
    assert overlay(sched(), update, NOW).estimated == NOW + 660


def veh(seq, status="IN_TRANSIT_TO", trip="t1", version=1) -> dict:
    return {"trip": {"key": f"kmk_t:20270115:{trip}", "version": version}, "current_stop_sequence": seq, "current_status": status}


def test_departed_once_the_vehicle_reports_a_later_stop():
    update = rt((5, NOW + 60, 240, {}))  # still listed by the feed: the TripUpdate doesn't say passed
    assert overlay(sched(), update, NOW, veh(6)).status == "departed"
    assert overlay(sched(), update, NOW, veh(5)).status == "live"  # heading to it
    assert overlay(sched(), update, NOW, veh(5, "STOPPED_AT")).status == "live"  # at it, not yet left
    assert overlay(sched(), update, NOW, veh(4)).status == "live"
    # the last known time stays
    live = overlay(sched(), update, NOW, veh(6))
    assert (live.estimated, live.delay_s) == (NOW + 60, 240)


def test_vehicle_of_another_trip_or_version_says_nothing():
    update = rt((5, NOW + 600, 0, {}))
    assert overlay(sched(), update, NOW, veh(9, trip="t2")).status == "live"
    assert overlay(sched(), update, NOW, veh(9, version=2)).status == "live"


def test_unlisted_stops_are_not_taken_as_passed():
    # a TripUpdate lists predictions only; an earlier stop it doesn't list is decided by the vehicle or the clock
    assert overlay(sched(seq=2, dep=NOW + 300), rt((5, NOW + 720, 120, {})), NOW).status == "scheduled"
    assert overlay(sched(seq=2, dep=NOW - 300), rt((5, NOW + 720, 120, {})), NOW).status == "departed"


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
    assert [(r.s.trip_id, r.live.status) for r in out] == [("old2", "departed"), ("next", "scheduled")]
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
