"""Laying realtime trip updates over timetabled departures, and paging (no services needed)."""

from datetime import date

import pytest

from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.services.departures import GRACE_S, Cursor, Row, Scheduled, overlay, select

NOW = 1_800_000_000


def sched(trip="t1", seq=5, dep=NOW + 600, version=1) -> Scheduled:
    return Scheduled("kmk_t", "kmk_t_v1", version, date(2027, 1, 15), trip, seq, "plat1", dep, "Bardosa", "r1", None)


def rt(*rows, version=1, relationship="SCHEDULED", vehicle="704") -> dict:
    """rows: (stop_sequence, predicted_departure, delay_s, extra)"""
    return {
        "version": version,
        "vehicle_id": vehicle,
        "schedule_relationship": relationship,
        "stops": [
            {"stop_sequence": q, "predicted_departure": p, "predicted_arrival": None, "delay_s": d, "passed": False, **x}
            for q, p, d, x in rows
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


def test_passed_stops_are_departed_with_last_known_time():
    live = overlay(sched(dep=NOW - 300), rt((5, NOW - 60, 240, {"passed": True})), NOW)
    assert (live.status, live.estimated, live.delay_s, live.realtime) == ("departed", NOW - 60, 240, True)
    # dropped by the feed before we ever saw it: earlier than every row we have, no time known
    live = overlay(sched(seq=2, dep=NOW - 300), rt((5, NOW + 720, 120, {})), NOW)
    assert (live.status, live.estimated, live.realtime) == ("departed", None, False)


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


def test_first_page_sorts_by_live_time_and_keeps_late_departures():
    late = sched("late", dep=NOW - 900)  # scheduled 15 min ago, 20 min late
    early = sched("early", dep=NOW + 300)
    gone = sched("gone", dep=NOW - GRACE_S - 1)  # no realtime, left already
    just_left = sched("just_left", dep=NOW - GRACE_S + 5)
    rs = rows([early, gone, late, just_left], {late.trip_key: rt((5, NOW + 301, 1201, {}))})
    out = select(rs, None, NOW, limit=10, start=NOW - GRACE_S, end=NOW + 3600)
    assert [r.s.trip_id for r in out] == ["just_left", "early", "late"]
    assert out[2].time == NOW + 301
    assert [r.s.trip_id for r in select(rs, None, NOW, limit=1, start=NOW - GRACE_S, end=NOW + 3600)] == ["just_left"]
    # outside the window
    assert select(rows([sched(dep=NOW + 7200)]), None, NOW, limit=10, start=NOW, end=NOW + 3600) == []


def test_paging_both_ways_without_gaps():
    # five departures at the same minute, then more later; two have left
    trips = [sched(f"t{i}", dep=NOW + 60 * (i // 5)) for i in range(12)] + [
        sched("old1", dep=NOW - 600),
        sched("old2", dep=NOW - 300),
    ]
    rs = rows(trips)
    first = select(rs, None, NOW, limit=4, start=NOW - GRACE_S, end=NOW + 3600)
    seen = [r.s.trip_id for r in first]
    cursor = Cursor("later", first[-1].key)
    while page := select(rs, cursor, NOW, limit=4, start=cursor.key[0], end=NOW + 3600):
        seen += [r.s.trip_id for r in page]
        cursor = Cursor("later", page[-1].key)
    assert sorted(seen) == sorted(f"t{i}" for i in range(12)) and len(seen) == 12

    back = select(rs, Cursor("earlier", first[0].key), NOW, limit=1, start=NOW - 3600, end=first[0].key[0])
    assert [(r.s.trip_id, r.live.status) for r in back] == [("old2", "departed")]
    back = select(rs, Cursor("earlier", first[0].key), NOW, limit=5, start=NOW - 3600, end=first[0].key[0])
    assert [r.s.trip_id for r in back] == ["old1", "old2"]


def test_cursor_round_trip_and_garbage():
    c = Cursor("earlier", (NOW, "kmk_t:a:b|c:20270115", 7))
    assert Cursor.decode(c.encode()) == c
    for bad in ["", "x", "bm9wZQ", Cursor("later", (NOW, "k", 1)).encode()[:-3]]:
        with pytest.raises(InvalidInput):
            Cursor.decode(bad)
