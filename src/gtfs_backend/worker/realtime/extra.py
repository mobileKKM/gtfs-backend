"""Trips that aren't a timetabled trip as it is (GTFS-RT NEW, ADDED, DUPLICATED), built from the update itself, for
feeds that enable them (feeds.yml `extra_trips`, default new and duplicated).

NEW (and ADDED, opt-in: its meaning was never specified) is an extra trip: its stops and times are its
stop_time_updates' (each needs a stop_id and an absolute time), its route the descriptor's (if the timetable has it),
its headsign trip_properties'. With no timetable to be late against, scheduled = predicted.

DUPLICATED is a copy of a timetabled trip at another time: the original's stops (the descriptor names it), shifted to
the copy's start_time on its start_date, under trip_properties.trip_id (or "{trip_id}#{date}T{start_time}"), with
predictions matched as for any trip.

Both are marked `extra`: boards take their rows from the live data, as the timetable doesn't have them.
"""

from __future__ import annotations

import dataclasses
from datetime import date, datetime

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend import keys
from gtfs_backend.live_layout import LiveTrip, StopRow
from gtfs_backend.worker.realtime.resolve import TripRef, start_date
from gtfs_backend.worker.realtime.trip_index import TripIndex

TD = pb.TripDescriptor.ScheduleRelationship
STU = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship
KINDS = {"new": TD.NEW, "added": TD.ADDED, "duplicated": TD.DUPLICATED}


def parse_time(text: str) -> int | None:
    """GTFS-RT start_time "HH:MM:SS" (may pass 24:00) as seconds; None if not given or malformed."""
    try:
        h, m, s = (int(p) for p in text.split(":"))
    except ValueError:
        return None
    return h * 3600 + m * 60 + s if 0 <= m < 60 and 0 <= s < 60 and h >= 0 else None


def _day(index: TripIndex, d: date | None):
    """The index's service day of `d`, else today's (the index's own date)."""
    day = index.day(d) if d is not None else None
    if day is None and index.loaded_for is not None:
        day = index.day(index.loaded_for)
    return day


def _serving_version(index: TripIndex, d: date) -> int | None:
    day = _day(index, d)
    return day.version if day else None


def build_new_trip(feed_id: str, tu, index: TripIndex, ts: int) -> LiveTrip | None:
    """An extra trip (NEW, ADDED) from its update; None without a stop with a time."""
    first = next((t for u in tu.stop_time_update for t in (u.arrival.time, u.departure.time) if t), None)
    if first is None or not tu.trip.trip_id:
        return None
    service_date = start_date(tu.trip) or datetime.fromtimestamp(first, index.tz).date()
    version = _serving_version(index, service_date)
    if version is None:
        return None
    rows: list[StopRow] = []
    for i, u in enumerate(tu.stop_time_update):
        arrival = int(u.arrival.time) if u.HasField("arrival") and u.arrival.time else None
        departure = int(u.departure.time) if u.HasField("departure") and u.departure.time else None
        if not u.stop_id or (arrival is None and departure is None):
            continue
        rows.append(
            {
                "stop_sequence": u.stop_sequence if u.HasField("stop_sequence") else i + 1,
                "stop_id": u.stop_id,
                "public_stop_id": index.public_stop(version, u.stop_id),
                "scheduled_arrival": arrival,
                "scheduled_departure": departure,
                "predicted_arrival": arrival,
                "predicted_departure": departure,
                "delay_s": None,  # no timetable to be late against
                "schedule_relationship": STU.Name(u.schedule_relationship),
                "propagated": False,
            }
        )
    if not rows:
        return None
    rows[-1]["scheduled_departure"] = None  # its last stop: not a departure (as the timetable's last stops)
    route_id = tu.trip.route_id if (version, tu.trip.route_id) in index.routes else None
    props = tu.trip_properties
    return {
        "key": keys.trip_key(feed_id, service_date, tu.trip.trip_id),
        "feed_id": feed_id,
        "version": version,
        "trip_id": tu.trip.trip_id,
        "service_date": service_date.isoformat(),
        "route_id": route_id,
        "route_short_name": None,
        "route_type": None,
        "headsign": props.trip_headsign or None,
        "direction_id": tu.trip.direction_id if tu.trip.HasField("direction_id") else None,
        "shape_id": props.shape_id or None,
        "block_id": None,
        "vehicle_id": tu.vehicle.id or None,
        "delay_s": None,
        "timestamp": int(tu.timestamp) or ts,
        "schedule_relationship": TD.Name(tu.trip.schedule_relationship),
        "stops": rows,
        "extra": True,
    }


def duplicate_ref(index: TripIndex, trip, copy_id: str | None = None) -> TripRef | None:
    """The copy a DUPLICATED trip means (`trip`: copy_descriptor's): the original trip on the copy's day, shifted to its
    start time, under its own id (`copy_id`, else "{trip_id}#{date}T{start_time}"). None if the original or the start
    time is unknown."""
    start = parse_time(trip.start_time)
    candidates = index.by_trip_id.get(trip.trip_id, [])
    day = _day(index, start_date(trip))
    if start is None or not candidates or day is None:
        return None
    info = next((i for i in candidates if i.version == day.version), None)
    if info is None:
        return None
    shift = start - info.first_departure  # the original's first departure, seconds from its day's base
    copy_id = copy_id or f"{trip.trip_id}#{day.date:%Y%m%d}T{trip.start_time}"
    return TripRef(dataclasses.replace(day, base=day.base + shift), dataclasses.replace(info, trip_id=copy_id))


def copy_descriptor(tu):
    """Where a DUPLICATED trip update says when its copy runs: trip_properties (the spec) over the descriptor."""
    props = tu.trip_properties
    out = pb.TripDescriptor()
    out.CopyFrom(tu.trip)
    if props.start_date:
        out.start_date = props.start_date
    if props.start_time:
        out.start_time = props.start_time
    return out, (props.trip_id or None)
