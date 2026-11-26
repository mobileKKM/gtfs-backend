"""Turn a GTFS-RT FeedMessage into map-ready vehicles, per-stop predictions and alerts.

Rules (see the plan, "Realtime ingestion flow"):
- `trip_id` is the source of truth. Route, headsign, direction, shape and stop order come from the static trip it
  resolves to (resolve.py); RT route/direction are only compared (mismatches are counted).
- Unmatched trips/vehicles are returned separately and never shown.
- The builders: trip_updates.py, vehicles.py, alerts.py. The JSON they build: live_layout.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.live_layout import LiveAlert, LiveTrip, LiveVehicle, UnmatchedVehicle
from gtfs_backend.worker.realtime.alerts import build_alert
from gtfs_backend.worker.realtime.blocks import block_overruns
from gtfs_backend.worker.realtime.motion import FORGET_S, MotionTracker
from gtfs_backend.worker.realtime.resolve import EARLY_WINDOW_S, LATE_WINDOW_S, TripRef, resolve, start_date
from gtfs_backend.worker.realtime.trip_index import TripIndex
from gtfs_backend.worker.realtime.trip_updates import build_trip
from gtfs_backend.worker.realtime.vehicles import build_vehicle

Loader = Callable[[set[tuple[int, str]]], Awaitable[None]]  # loads the stop times of (version, trip_id)s
DELAY_MEMORY_S = 600  # a trip's last delay is kept this long for its vehicle (the files are polled separately)


@dataclass
class FeedRealtimeState:
    """Per-feed state shared by the feed's realtime files."""

    trip_delays: dict[str, tuple[int | None, int]] = field(default_factory=dict)  # trip key -> (delay_s, feed ts)
    early_window_s: int = EARLY_WINDOW_S  # how far before/after its timetable a trip_id can match
    late_window_s: int = LATE_WINDOW_S
    ignore_service_suffix: bool = False  # FeedConfig.ignore_service_suffix
    motion: MotionTracker = field(default_factory=MotionTracker)  # last fix per vehicle, to fill bearing/speed


@dataclass
class Result:
    vehicles: list[LiveVehicle] = field(default_factory=list)
    trips: list[LiveTrip] = field(default_factory=list)
    alerts: list[LiveAlert] = field(default_factory=list)
    unmatched_vehicles: list[UnmatchedVehicle] = field(default_factory=list)
    unmatched_trips: list[str] = field(default_factory=list)
    unmatched_alert_entities: list[str] = field(default_factory=list)  # "{alert id}: route X"
    entities: int = 0
    mismatches: int = 0
    # GTFS-RT schedule_relationship of every trip update (matched or not) and of every stop time update in them
    trip_relationships: Counter[str] = field(default_factory=Counter)
    stop_relationships: Counter[str] = field(default_factory=Counter)
    # trips predicted at their last stop after the next trip of their block is due (blocks.block_overruns)
    block_late_next: int = 0
    block_late_next_without_update: int = 0


def _relationship(enum, value: int) -> str:
    """The enum name; the number for a value these bindings don't know."""
    try:
        return enum.Name(value)
    except ValueError:
        return str(value)


async def process_message(feed_id: str, msg, index: TripIndex, state: FeedRealtimeState, load: Loader, now: float) -> Result:
    res = Result(entities=len(msg.entity))
    ts = int(msg.header.timestamp) or int(now)

    tus = [e for e in msg.entity if e.HasField("trip_update") and not e.is_deleted]
    vps = [e for e in msg.entity if e.HasField("vehicle") and not e.is_deleted]

    # 1. resolve every reported (trip id, start date) once, and load the stop times of those that resolved
    def ref_key(trip) -> tuple[str, date | None]:
        return trip.trip_id, start_date(trip)

    refs: dict[tuple[str, date | None], TripRef | None] = {}
    for trip in [e.trip_update.trip for e in tus] + [e.vehicle.trip for e in vps if e.vehicle.trip.trip_id]:
        refs.setdefault(ref_key(trip), None)
    for trip_id, start in refs:
        refs[(trip_id, start)] = (
            resolve(
                index,
                trip_id,
                now,
                early_s=state.early_window_s,
                late_s=state.late_window_s,
                start_date=start,
                ignore_service_suffix=state.ignore_service_suffix,
            )
            if trip_id
            else None
        )
    await load({(r.info.version, r.info.trip_id) for r in refs.values() if r})

    # 2. trip updates
    trip_refs: list[tuple[TripRef, LiveTrip]] = []
    for e in tus:
        tu = e.trip_update
        res.trip_relationships[_relationship(pb.TripDescriptor.ScheduleRelationship, tu.trip.schedule_relationship)] += 1
        for u in tu.stop_time_update:
            res.stop_relationships[_relationship(pb.TripUpdate.StopTimeUpdate.ScheduleRelationship, u.schedule_relationship)] += 1
        ref = refs.get(ref_key(tu.trip))
        if ref is None:
            res.unmatched_trips.append(tu.trip.trip_id or f"<entity {e.id}>")
            continue
        if tu.trip.route_id and tu.trip.route_id != ref.info.route_id:
            res.mismatches += 1
        trip = build_trip(feed_id, ref, tu, index.stop_times.get((ref.info.version, ref.info.trip_id), []), index, now, ts)
        res.trips.append(trip)
        trip_refs.append((ref, trip))
        state.trip_delays[trip["key"]] = (trip["delay_s"], ts)
    res.block_late_next, res.block_late_next_without_update = block_overruns(feed_id, index, trip_refs)

    # 3. vehicles
    for e in vps:
        vp = e.vehicle
        ref = refs.get(ref_key(vp.trip)) if vp.trip.trip_id else None
        if ref is None or not vp.HasField("position"):
            res.unmatched_vehicles.append({"vehicle_id": vp.vehicle.id or e.id, "trip_id": vp.trip.trip_id or None})
            continue
        if vp.trip.route_id and vp.trip.route_id != ref.info.route_id:
            res.mismatches += 1
        delay = state.trip_delays.get(ref.key(feed_id), (None, 0))[0]
        res.vehicles.append(build_vehicle(feed_id, ref, vp, index, delay, state.motion, ts))

    # 4. alerts
    for e in msg.entity:
        if e.HasField("alert") and not e.is_deleted:
            alert, unmatched = build_alert(feed_id, e.id, e.alert, index, now, state.ignore_service_suffix)
            res.alerts.append(alert)
            res.unmatched_alert_entities.extend(unmatched)

    # forget delays of trips not seen for a while, and fixes of vehicles gone
    cutoff = ts - DELAY_MEMORY_S
    for k in [k for k, (_, t) in state.trip_delays.items() if t < cutoff]:
        del state.trip_delays[k]
    state.motion.forget_before(ts - FORGET_S)
    return res
