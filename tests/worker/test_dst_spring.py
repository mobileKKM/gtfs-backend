"""The spring clock change (2027-03-28, 02:00 CET -> 03:00 CEST), synthetic: no recording of it exists yet (the autumn
one is tests/fixtures/dst_2026-10-25). On that day a feed counting GTFS times from local midnight runs an hour off
the GTFS reading (noon minus 12 h), the other way round from autumn; `median_delay_s` of about +-3600 shows it."""

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.servicetime import day_base
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message
from gtfs_backend.worker.realtime.resolve import resolve
from gtfs_backend.worker.realtime.trip_index import ServiceDay, StopTime, TripIndex, TripInfo

WAW = ZoneInfo("Europe/Warsaw")
SAT, SUN = date(2027, 3, 27), date(2027, 3, 28)


def hms(h, m=0):
    return h * 3600 + m * 60


def make_index(day_start: str) -> TripIndex:
    """A morning trip on the change day (08:00-08:30) and a night trip of the day before (25:30-26:40)."""
    idx = TripIndex("kmk_t", WAW, loaded_for=SUN)
    idx.days = [
        ServiceDay(SAT, 1, day_base(SAT, WAW, day_start), frozenset({"sat"})),
        ServiceDay(SUN, 1, day_base(SUN, WAW, day_start), frozenset({"sun"})),
    ]
    for trip_id, service, first, last in (("morning", "sun", hms(8), hms(8, 30)), ("night", "sat", hms(25, 30), hms(26, 40))):
        idx.add_trip(TripInfo(1, trip_id, service, "R1", 0, None, "Salwator", None, "1", 900, first, last))
        idx.stop_times[(1, trip_id)] = [StopTime(1, "A", first, first), StopTime(2, "B", last, last)]
    return idx


def wall(d: date, h: int, m: int = 0) -> int:
    """Epoch of a wall-clock time in Kraków."""
    return int(datetime.combine(d, time(h, m), WAW).timestamp())


def update(trip_id, arrival):
    e = pb.FeedEntity(id=trip_id)
    e.trip_update.trip.trip_id = trip_id
    e.trip_update.stop_time_update.add(stop_sequence=2).arrival.time = arrival
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = arrival - 600
    msg.entity.append(e)
    return msg


async def noop(keys):
    return None


async def delay(day_start: str, trip_id: str, arrival: int) -> int:
    msg = update(trip_id, arrival)
    res = await process_message("kmk_t", msg, make_index(day_start), FeedRealtimeState(), noop, msg.header.timestamp)
    return res.trips[0]["delay_s"]


async def test_a_feed_following_gtfs_gets_minute_delays_on_the_change_day():
    # 08:30 in the timetable is 08:30 CEST on the wall: arriving 08:32 is 2 min late
    assert await delay("noon_minus_12h", "morning", wall(SUN, 8, 32)) == 120


async def test_counting_from_midnight_is_an_hour_off_that_day():
    # the same arrival, read with midnight as the day start, is 58 min early: what median_delay_s would show if a
    # feed's day_start were wrong (in autumn it is an hour late instead)
    assert await delay("midnight", "morning", wall(SUN, 8, 32)) == 120 - 3600


async def test_a_night_trip_of_the_day_before_runs_across_the_change():
    # Saturday's 26:40 is Sunday 03:40 CEST (02:40 CET does not exist that night: GTFS counts elapsed time)
    idx = make_index("noon_minus_12h")
    night = next(d for d in idx.days if d.date == SAT)
    assert night.base + hms(26, 40) == wall(SUN, 3, 40)
    assert resolve(idx, "night", wall(SUN, 3, 0)).day.date == SAT
    assert await delay("noon_minus_12h", "night", wall(SUN, 3, 45)) == 300
