"""Trips no timetable has as they run (GTFS-RT NEW, DUPLICATED, ADDED), built from the update (worker/realtime/extra.py)."""

from datetime import date
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.servicetime import service_day_base
from gtfs_backend.worker.realtime.extra import KINDS, parse_time
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message
from gtfs_backend.worker.realtime.trip_index import ServiceDay, StopTime, TripIndex, TripInfo

WAW = ZoneInfo("Europe/Warsaw")
D = date(2026, 11, 26)
BASE = service_day_base(D, WAW)
TD = pb.TripDescriptor.ScheduleRelationship


def hms(h, m=0):
    return h * 3600 + m * 60


def make_index() -> TripIndex:
    """T1 runs 06:00 A -> 06:20 B -> 06:40 C on route R1."""
    idx = TripIndex("kmk_a", WAW, loaded_for=D)
    idx.days = [ServiceDay(D, 1, BASE, frozenset({"wd"}))]
    idx.add_trip(TripInfo(1, "T1", "wd", "R1", 0, None, "Salwator", None, "1", 3, hms(6), hms(6, 40)))
    idx.stop_times[(1, "T1")] = [
        StopTime(1, "A", hms(6), hms(6)),
        StopTime(2, "B", hms(6, 20), hms(6, 20)),
        StopTime(3, "C", hms(6, 40), None),
    ]
    idx.routes = {(1, "R1")}
    for s in "ABC":
        idx.stop_links[(1, s)] = f"stop{s.lower()}"
    return idx


def state(*kinds):
    return FeedRealtimeState(extra_kinds=frozenset(KINDS[k] for k in kinds))


def message(*entities, ts):
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    msg.entity.extend(entities)
    return msg


def new_trip(relationship=TD.NEW):
    e = pb.FeedEntity(id="n")
    tu = e.trip_update
    tu.trip.trip_id, tu.trip.route_id, tu.trip.schedule_relationship = "extra_1", "R1", relationship
    tu.trip_properties.trip_headsign = "Nowy Bieżanów"
    for stop, minute in (("A", 30), ("B", 45), ("C", 55)):
        u = tu.stop_time_update.add(stop_id=stop)
        u.arrival.time = u.departure.time = BASE + hms(7, minute)
    return e


def vehicle(trip_id, vid, relationship=TD.SCHEDULED, start_time=""):
    e = pb.FeedEntity(id=vid)
    e.vehicle.trip.trip_id, e.vehicle.trip.schedule_relationship = trip_id, relationship
    if start_time:
        e.vehicle.trip.start_time = start_time
    e.vehicle.vehicle.id = vid
    e.vehicle.position.latitude, e.vehicle.position.longitude = 50.06, 19.94
    return e


async def noop(keys):
    return None


async def run(st, *entities, at=None):
    now = BASE + (hms(7, 20) if at is None else at)
    return await process_message("kmk_a", message(*entities, ts=now), make_index(), st, noop, now)


def test_start_times_are_gtfs_times():
    assert parse_time("09:05:30") == hms(9, 5) + 30 and parse_time("25:10:00") == hms(25, 10)
    assert parse_time("") is None and parse_time("9:61:00") is None and parse_time("x") is None


async def test_a_new_trip_comes_from_its_update_and_its_vehicle_is_matched():
    st = state("new")
    res = await run(st, new_trip())
    (trip,) = res.trips
    assert trip["key"] == "kmk_a:20261126:extra_1" and trip["extra"] is True and trip["schedule_relationship"] == "NEW"
    assert (trip["route_id"], trip["headsign"], trip["version"]) == ("R1", "Nowy Bieżanów", 1)
    stops = trip["stops"]
    assert [s["public_stop_id"] for s in stops] == ["stopa", "stopb", "stopc"]
    assert stops[0]["scheduled_departure"] == stops[0]["predicted_departure"] == BASE + hms(7, 30)
    assert stops[-1]["scheduled_departure"] is None  # its last stop: not a departure
    assert all(s["delay_s"] is None for s in stops)  # no timetable to be late against
    # its vehicle, in the next file: it doesn't say its trip is extra, its id is just in no timetable
    res = await run(st, vehicle("extra_1", "v9"))
    assert [(v["vehicle_id"], v["trip"]["key"]) for v in res.vehicles] == [("v9", "kmk_a:20261126:extra_1")]
    assert res.unmatched_vehicles == []


async def test_extra_trips_only_where_the_feed_shows_them():
    res = await run(state(), new_trip())  # extra_trips: []
    assert res.trips == [] and res.unmatched_trips == ["extra_1 (NEW)"]
    res = await run(state("new"), new_trip(TD.ADDED))  # ADDED is opt-in
    assert res.trips == [] and res.unmatched_trips == ["extra_1 (ADDED)"]
    res = await run(state("new", "added"), new_trip(TD.ADDED))
    assert [t["schedule_relationship"] for t in res.trips] == ["ADDED"]


def duplicate(start_time="09:00:00", copy_id=""):
    e = pb.FeedEntity(id="d")
    tu = e.trip_update
    tu.trip.trip_id, tu.trip.schedule_relationship = "T1", TD.DUPLICATED
    tu.trip_properties.start_time = start_time
    tu.trip_properties.start_date = "20261126"
    if copy_id:
        tu.trip_properties.trip_id = copy_id
    u = tu.stop_time_update.add(stop_sequence=2)
    u.departure.time = BASE + hms(9, 23)  # 3 min late at B (the copy's 09:20)
    return e


def original(minute_late=1):
    e = pb.FeedEntity(id="o")
    e.trip_update.trip.trip_id = "T1"
    u = e.trip_update.stop_time_update.add(stop_sequence=2)
    u.departure.time = BASE + hms(6, 20 + minute_late)
    return e


async def test_a_duplicated_trip_is_the_original_shifted_under_its_own_key():
    st = state("duplicated")
    res = await run(st, original(), duplicate(), at=hms(6, 10))
    trips = {t["key"]: t for t in res.trips}
    copy = trips["kmk_a:20261126:T1#20261126T09:00:00"]
    assert copy["extra"] is True and copy["schedule_relationship"] == "DUPLICATED" and copy["route_id"] == "R1"
    by_seq = {s["stop_sequence"]: s for s in copy["stops"]}
    assert by_seq[2]["scheduled_departure"] == BASE + hms(9, 20) and by_seq[2]["delay_s"] == 180
    assert by_seq[3]["scheduled_arrival"] == BASE + hms(9, 40)  # propagated on, shifted like the rest
    # the original keeps its own realtime
    assert trips["kmk_a:20261126:T1"]["stops"][0]["delay_s"] == 60 and "extra" not in trips["kmk_a:20261126:T1"]
    # the copy's vehicle names the original and its start time
    res = await run(st, vehicle("T1", "v7", TD.DUPLICATED, "09:00:00"), at=hms(6, 10))
    assert [v["trip"]["key"] for v in res.vehicles] == ["kmk_a:20261126:T1#20261126T09:00:00"]


async def test_a_duplicated_trip_takes_the_feeds_id_and_needs_a_start_time():
    res = await run(state("duplicated"), duplicate(copy_id="T1-extra"), at=hms(6, 10))
    assert [t["key"] for t in res.trips] == ["kmk_a:20261126:T1-extra"]
    res = await run(state("duplicated"), duplicate(start_time=""), at=hms(6, 10))
    assert res.trips == [] and res.unmatched_trips == ["T1 (DUPLICATED)"]
