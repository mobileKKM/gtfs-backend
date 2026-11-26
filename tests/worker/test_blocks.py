"""Counting what realtime sends that we don't use yet: late trips delaying the next trip of their block, and the
GTFS-RT schedule relationships of trips and stops (no DB)."""

from datetime import date
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.servicetime import service_day_base
from gtfs_backend.worker.realtime.blocks import next_in_block
from gtfs_backend.worker.realtime.normalize import FeedRealtimeState, process_message
from gtfs_backend.worker.realtime.trip_index import ServiceDay, StopTime, TripIndex, TripInfo

WAW = ZoneInfo("Europe/Warsaw")
D = date(2026, 11, 26)
BASE = service_day_base(D, WAW)
TD = pb.TripDescriptor.ScheduleRelationship
STU = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship


def hms(h, m=0):
    return h * 3600 + m * 60


def info(trip_id, service, first, last, block="B1"):
    return TripInfo(1, trip_id, service, "R1", 0, None, "Salwator", block, "1", 900, first, last)


def make_index() -> TripIndex:
    """Block B1 on weekdays: T1 06:00-06:40, then T2 at 06:45. W2 is B1's weekend trip at 06:42, X1 another block."""
    idx = TripIndex("kmk_a", WAW, loaded_for=D)
    idx.days = [ServiceDay(D, 1, BASE, frozenset({"wd"}))]
    for i in (
        info("T2", "wd", hms(6, 45), hms(7, 30)),
        info("T1", "wd", hms(6), hms(6, 40)),
        info("W2", "we", hms(6, 42), hms(7, 20)),
        info("X1", "wd", hms(6, 41), hms(7), block="B2"),
        info("T0", "wd", hms(5), hms(5, 50)),
    ):
        idx.add_trip(i)
    for trip_id, (first, last) in {
        "T0": (hms(5), hms(5, 50)),
        "T1": (hms(6), hms(6, 40)),
        "T2": (hms(6, 45), hms(7, 30)),
    }.items():
        idx.stop_times[(1, trip_id)] = [
            StopTime(1, "A", first, first),
            StopTime(2, "B", (first + last) // 2, (first + last) // 2),
            StopTime(3, "C", last, None),
        ]
    for s in "ABC":
        idx.stop_links[(1, s)] = f"stop{s.lower()}"
    return idx


def tu(trip_id, *updates, relationship=TD.SCHEDULED):
    """A trip update; `updates` are (stop_sequence, arrival epoch or None, stop relationship)."""
    e = pb.FeedEntity(id=trip_id)
    e.trip_update.trip.trip_id = trip_id
    e.trip_update.trip.schedule_relationship = relationship
    for seq, arrival, rel in updates:
        u = e.trip_update.stop_time_update.add(stop_sequence=seq, schedule_relationship=rel)
        if arrival is not None:
            u.arrival.time = arrival
    return e


def feed(*entities, ts):
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    msg.entity.extend(entities)
    return msg


async def noop(keys):
    return None


async def run(*entities):
    now = BASE + hms(6, 20)
    return await process_message("kmk_a", feed(*entities, ts=now), make_index(), FeedRealtimeState(), noop, now)


def test_next_in_block_is_the_next_trip_of_the_same_service_day():
    idx = make_index()
    day = idx.days[0]
    assert next_in_block(idx, idx.trips[(1, "T1")], day).trip_id == "T2"  # not W2 (weekend), not X1 (other block)
    assert next_in_block(idx, idx.trips[(1, "T0")], day).trip_id == "T1"
    assert next_in_block(idx, idx.trips[(1, "T2")], day) is None  # the block's last trip
    assert next_in_block(idx, idx.trips[(1, "X1")], day) is None


async def test_late_trip_with_next_trip_lacking_an_update_is_counted():
    # T1 reaches its last stop at 06:50, after T2 is due at 06:45; T2 has no update of its own
    res = await run(tu("T1", (2, BASE + hms(6, 30), STU.SCHEDULED)))
    assert res.trips[0]["stops"][-1]["predicted_arrival"] == BASE + hms(6, 50)  # 10 min late, propagated to stop 3
    assert (res.block_late_next, res.block_late_next_without_update) == (1, 1)


async def test_late_trip_whose_next_trip_has_an_update_counts_only_as_late():
    res = await run(tu("T1", (3, BASE + hms(6, 50), STU.SCHEDULED)), tu("T2", (1, BASE + hms(6, 52), STU.SCHEDULED)))
    assert (res.block_late_next, res.block_late_next_without_update) == (1, 0)


async def test_delay_absorbed_by_the_layover_or_unknown_end_is_not_counted():
    on_time_enough = tu("T1", (3, BASE + hms(6, 44), STU.SCHEDULED))  # 4 min late, the layover is 5
    assert (await run(on_time_enough)).block_late_next == 0
    # NO_DATA at the last stop: no prediction for it, so nothing to compare
    no_end = tu("T1", (2, BASE + hms(6, 30), STU.SCHEDULED), (3, None, STU.NO_DATA))
    assert (await run(no_end)).block_late_next == 0
    canceled = tu("T1", (3, BASE + hms(6, 50), STU.SCHEDULED), relationship=TD.CANCELED)
    assert (await run(canceled)).block_late_next == 0


async def test_schedule_relationships_are_counted_matched_or_not():
    res = await run(
        tu("T1", (1, BASE + hms(6, 1), STU.SCHEDULED), (2, None, STU.SKIPPED), (3, None, STU.NO_DATA)),
        tu("T2", relationship=TD.CANCELED),
        tu("extra_1", (1, BASE + hms(6, 30), STU.SCHEDULED), relationship=TD.NEW),  # not in the timetable
    )
    assert res.unmatched_trips == ["extra_1"]
    assert res.trip_relationships == {"SCHEDULED": 1, "CANCELED": 1, "NEW": 1}
    assert res.stop_relationships == {"SCHEDULED": 2, "SKIPPED": 1, "NO_DATA": 1}
