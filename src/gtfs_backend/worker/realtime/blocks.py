"""How often a late trip delays the next trip of its vehicle's block: the numbers that decide whether delays should
ever be carried over to the next trip (only counted, nothing is changed).

GTFS-RT only propagates delays within a trip; for blocks it asks producers to send updates for the vehicle's next
trips themselves (ZTP does). So this counts, per update, the trips predicted to reach their last stop after the next
trip of their block is due to leave, and how many of those next trips have no update of their own.
"""

from __future__ import annotations

from gtfs_backend import keys
from gtfs_backend.live_layout import LiveTrip
from gtfs_backend.worker.realtime.resolve import TripRef
from gtfs_backend.worker.realtime.trip_index import ServiceDay, TripIndex, TripInfo


def next_in_block(index: TripIndex, info: TripInfo, day: ServiceDay) -> TripInfo | None:
    """The trip of the same block that runs next after `info` on the same service day (and timetable version): the
    first one starting at or after `info` ends. None without a block or a later trip that day."""
    if not info.block_id:
        return None
    for other in index.block_trips(info.version, info.block_id):
        if other.first_departure >= info.last_arrival and other.trip_id != info.trip_id and other.service_id in day.services:
            return other
    return None


def _predicted_end(index: TripIndex, ref: TripRef, trip: LiveTrip) -> int | None:
    """The predicted arrival (else departure) at the trip's last stop, if the update predicts that stop."""
    rows = trip["stops"]
    static = index.stop_times.get((ref.info.version, ref.info.trip_id))
    if not rows or not static or rows[-1]["stop_sequence"] != static[-1].sequence:
        return None
    return rows[-1]["predicted_arrival"] or rows[-1]["predicted_departure"]


def block_overruns(feed_id: str, index: TripIndex, trips: list[tuple[TripRef, LiveTrip]]) -> tuple[int, int]:
    """(late, late without update) over one update's trips: those predicted at their last stop after the next trip of
    their block is scheduled to leave, and of those, how many next trips the update has no trip update for. Canceled
    trips and trips without a prediction for their last stop don't count; nor does a next trip on another service day."""
    updated = {trip["key"] for _, trip in trips}
    late = without_update = 0
    for ref, trip in trips:
        if trip["schedule_relationship"] == "CANCELED" or (end := _predicted_end(index, ref, trip)) is None:
            continue
        nxt = next_in_block(index, ref.info, ref.day)
        if nxt is None or end <= ref.day.base + nxt.first_departure:
            continue
        late += 1
        if keys.trip_key(feed_id, ref.day.date, nxt.trip_id) not in updated:
            without_update += 1
    return late, without_update
