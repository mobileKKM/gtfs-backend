"""Delay propagation (MLD sends only the next stop) and `start_date` in trip descriptors."""

from datetime import date

from google.transit import gtfs_realtime_pb2 as pb
from test_normalize import D22, D23, day, epoch, feed, hms, make_index, noop, trip

from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message, resolve
from gtfs_backend.worker.realtime.trip_index import TripIndex

TRIP = "block_1_trip_9_service_4"  # v1 on the 22nd: A 23:50, B 24:30, A 25:00, C 25:20


async def _trip(*updates, now=None) -> dict:
    """The stored trip for one trip update; updates: (stop_sequence, arrival delay or None, relationship)."""
    now = now or epoch(D22, 24, 20)
    e = pb.FeedEntity(id="t1")
    e.trip_update.trip.trip_id = TRIP
    for seq, delay, rel in updates:
        u = e.trip_update.stop_time_update.add(stop_sequence=seq)
        u.schedule_relationship = rel
        if delay is not None:
            u.arrival.delay = delay
    res = await process_message("kmk_t", feed(e, ts=int(now)), make_index(), FeedRealtimeState(), noop, now)
    return res.trips[0]


def _rows(trip: dict) -> list[tuple]:
    return [(r["stop_sequence"], r["delay_s"], r["propagated"]) for r in trip["stops"]]


SCHEDULED = pb.TripUpdate.StopTimeUpdate.SCHEDULED


async def test_one_update_carries_its_delay_to_the_rest_of_the_trip():
    # MLD style: only the next stop, arrival delay only
    t = await _trip((2, 120, SCHEDULED))
    assert _rows(t) == [(2, 120, False), (3, 120, True), (4, 120, True)]
    last = t["stops"][-1]
    assert last["predicted_arrival"] == epoch(D22, 25, 22) and last["predicted_departure"] == epoch(D22, 25, 22)
    assert t["delay_s"] == 120


async def test_gaps_take_the_delay_of_the_update_before_them():
    t = await _trip((1, 60, SCHEDULED), (3, 180, SCHEDULED))
    assert _rows(t) == [(1, 60, False), (2, 60, True), (3, 180, False), (4, 180, True)]


async def test_no_data_stops_the_propagation_and_skipped_passes_it_on():
    t = await _trip((1, 60, SCHEDULED), (2, None, pb.TripUpdate.StopTimeUpdate.SKIPPED))
    assert _rows(t) == [(1, 60, False), (2, None, False), (3, 60, True), (4, 60, True)]
    t = await _trip((1, 60, SCHEDULED), (3, None, pb.TripUpdate.StopTimeUpdate.NO_DATA))
    assert _rows(t) == [(1, 60, False), (2, 60, True), (3, None, False)]


def _two_day_index() -> TripIndex:
    idx = TripIndex("kmk_t", make_index().tz, loaded_for=D23)
    idx.days = [day(D22, 1, {"s"}), day(D23, 1, {"s"})]
    idx.add_trip(trip(1, "daily", "s", hms(6), hms(6, 40)))
    return idx


def test_start_date_picks_the_service_day():
    idx, now, wide = _two_day_index(), epoch(D23, 6, 10), {"early_s": 2 * 86400, "late_s": 2 * 86400}
    assert resolve(idx, "daily", now, **wide).day.date == D23  # closest in time
    assert resolve(idx, "daily", now, **wide, start_date=D22).day.date == D22
    # a start date that doesn't run the trip: the closest one, as without it
    assert resolve(idx, "daily", now, **wide, start_date=date(2026, 10, 21)).day.date == D23
    # the window still applies: yesterday's trip ended a day ago
    assert resolve(idx, "daily", now, start_date=D22).day.date == D23


async def test_start_date_in_the_feed_is_used():
    idx, now = _two_day_index(), epoch(D23, 5, 30)  # D23's trip is ahead, D22's ended ~23 h ago
    state = FeedRealtimeState(early_window_s=2 * 86400, late_window_s=2 * 86400)
    entities = []
    for i, start in enumerate(("20261022", "")):
        e = pb.FeedEntity(id=f"t{i}")
        e.trip_update.trip.trip_id = "daily"
        e.trip_update.trip.start_date = start
        entities.append(e)
    res = await process_message("kmk_t", feed(*entities, ts=int(now)), idx, state, noop, now)
    assert [t["service_date"] for t in res.trips] == ["2026-10-22", "2026-10-23"]
