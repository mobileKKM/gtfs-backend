"""Turn a GTFS-RT FeedMessage into map-ready vehicles and per-stop predictions.

Rules (see the plan, "Realtime ingestion flow"):
- `trip_id` is the source of truth. Route, headsign, direction, shape and stop order come from the
  static trip it resolves to; RT route/direction are only compared (mismatches are counted).
- Resolution picks the version + service day: the trip's service must run on an authoritative service
  day (yesterday/today/tomorrow) and "now" must fall in [first departure - 3 h, last arrival + 60 min];
  a `start_date` in the feed picks among the days that qualify.
- Stops without an update get the delay of the last updated stop before them (GTFS-RT delay propagation).
- Unmatched trips/vehicles are returned separately and never shown.
- Delays are always recomputed: predicted - scheduled, scheduled = the feed's day base + GTFS time. The base
  is noon - 12h (GTFS), or local midnight for feeds configured with `day_start: midnight` (ZTP's trams).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend import keys
from gtfs_backend.worker.realtime.motion import FORGET_S, MotionTracker
from gtfs_backend.worker.realtime.trip_index import ServiceDay, StopTime, TripIndex, TripInfo, strip_service_suffix

EARLY_WINDOW_S = 3 * 60 * 60  # trip updates carry predictions for trips starting up to ~2 h ahead
LATE_WINDOW_S = 60 * 60

Loader = Callable[[set[tuple[int, str]]], Awaitable[None]]


@dataclass(frozen=True)
class TripRef:
    day: ServiceDay
    info: TripInfo

    @property
    def key(self) -> str:
        """Unique within a feed: a service day has exactly one serving version, so trip id + date suffice."""
        return f"{self.info.trip_id}:{self.day.date:%Y%m%d}"

    def epoch(self, secs: int | None) -> int | None:
        return None if secs is None else self.day.base + secs


def trip_key(feed_id: str, ref: TripRef) -> str:
    """The global trip key (gtfs_backend/keys.py), also the Redis key suffix."""
    return keys.trip_key(feed_id, ref.day.date, ref.info.trip_id)


@dataclass
class FeedRealtimeState:
    """Per-feed state shared by the feed's realtime tasks."""

    trip_delays: dict[str, tuple[int | None, int]] = field(default_factory=dict)  # trip key -> (delay_s, feed ts)
    early_window_s: int = EARLY_WINDOW_S  # how far before/after its timetable a trip_id can match
    late_window_s: int = LATE_WINDOW_S
    ignore_service_suffix: bool = False  # FeedConfig.ignore_service_suffix
    motion: MotionTracker = field(default_factory=MotionTracker)  # last fix per vehicle, to fill bearing/speed


@dataclass
class Result:
    vehicles: list[dict] = field(default_factory=list)
    trips: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)
    unmatched_vehicles: list[dict] = field(default_factory=list)
    unmatched_trips: list[str] = field(default_factory=list)
    entities: int = 0
    mismatches: int = 0


# --------------------------------------------------------------------------------------------- resolution


def _window_distance(ref: TripRef, now: float, early_s: int, late_s: int) -> float | None:
    start = ref.day.base + ref.info.first_departure
    end = ref.day.base + ref.info.last_arrival
    if not (start - early_s <= now <= end + late_s):
        return None
    return 0.0 if start <= now <= end else min(abs(now - start), abs(now - end))


def resolve(
    index: TripIndex,
    trip_id: str,
    now: float,
    *,
    early_s: int = EARLY_WINDOW_S,
    late_s: int = LATE_WINDOW_S,
    start_date: date | None = None,
    ignore_service_suffix: bool = False,
) -> TripRef | None:
    """Pick (version, service day) for an RT trip_id: the one running closest to now. When the feed gives the
    trip's `start_date` (none of ours does yet), that service day wins if it runs the trip within the window.
    With `ignore_service_suffix`, an id that does not resolve as is is retried without its `_service_N` suffix."""
    ref = _resolve(index.by_trip_id.get(trip_id, []), index, now, early_s, late_s, start_date)
    if ref is None and ignore_service_suffix:
        ref = _resolve(index.by_base_id.get(strip_service_suffix(trip_id), []), index, now, early_s, late_s, start_date)
    return ref


def _resolve(
    candidates: list[TripInfo], index: TripIndex, now: float, early_s: int, late_s: int, start_date: date | None
) -> TripRef | None:
    best: tuple[bool, float, TripRef] | None = None
    for info in candidates:
        for day in index.days:
            if day.version != info.version or info.service_id not in day.services:
                continue
            ref = TripRef(day, info)
            dist = _window_distance(ref, now, early_s, late_s)
            if dist is None:
                continue
            rank = (start_date is not None and day.date != start_date, dist, ref)
            if best is None or rank[:2] < best[:2]:
                best = rank
    return best[2] if best else None


def _start_date(trip) -> date | None:
    """The TripDescriptor's start_date (YYYYMMDD), if given and valid."""
    if not trip.start_date:
        return None
    try:
        return keys.parse_service_date(trip.start_date)
    except keys.InvalidKey:
        return None


# --------------------------------------------------------------------------------------------- trip updates


def _event_time(ev, sched_epoch: int | None) -> int | None:
    if ev.HasField("time") and ev.time:
        return int(ev.time)
    if ev.HasField("delay") and sched_epoch is not None:
        return sched_epoch + ev.delay
    return None


def _update_time(u) -> int | None:
    for ev in (u.arrival, u.departure):
        if ev.HasField("time") and ev.time:
            return int(ev.time)
    return None


def match_updates(updates, stops: list[StopTime], base: int | None = None) -> list[tuple[object, StopTime | None]]:
    """Pair stop_time_updates with static stop times: by stop_sequence, else by stop_id moving forward.

    Without a sequence, a stop visited twice (loop lines) is resolved by the visit whose scheduled time
    is closest to the predicted time (needs the service-day `base`), else the next visit.
    """
    by_seq = {s.sequence: i for i, s in enumerate(stops)}
    out = []
    pos = 0
    for u in updates:
        idx = None
        if u.HasField("stop_sequence") and u.stop_sequence in by_seq:
            idx = by_seq[u.stop_sequence]
            if u.stop_id and stops[idx].stop_id != u.stop_id:
                idx = None
        if idx is None and u.stop_id:
            visits = [i for i in range(pos, len(stops)) if stops[i].stop_id == u.stop_id]
            predicted = _update_time(u)
            if len(visits) > 1 and base is not None and predicted is not None:

                def gap(i: int, predicted: int = predicted) -> float:
                    sched = stops[i].arrival if stops[i].arrival is not None else stops[i].departure
                    return abs(predicted - (base + sched)) if sched is not None else float("inf")

                idx = min(visits, key=gap)
            else:
                idx = visits[0] if visits else None
        if idx is not None:
            pos = idx + 1
        out.append((u, stops[idx] if idx is not None else None))
    return out


def _row(ref: TripRef, index: TripIndex, st: StopTime, pred_arr: int | None, pred_dep: int | None, relationship: str) -> dict:
    sched_arr, sched_dep = ref.epoch(st.arrival), ref.epoch(st.departure)
    delay = None
    if pred_dep is not None and sched_dep is not None:
        delay = pred_dep - sched_dep
    elif pred_arr is not None and sched_arr is not None:
        delay = pred_arr - sched_arr
    return {
        "stop_sequence": st.sequence,
        "stop_id": st.stop_id,
        "public_stop_id": index.public_stop(ref.info.version, st.stop_id),
        "scheduled_arrival": sched_arr,
        "scheduled_departure": sched_dep,
        "predicted_arrival": pred_arr,
        "predicted_departure": pred_dep,
        "delay_s": delay,
        "schedule_relationship": relationship,
        "propagated": False,
    }


def propagate(ref: TripRef, index: TripIndex, stops: list[StopTime], explicit: dict[int, dict]) -> list[dict]:
    """Rows in trip order from the first updated stop on, with GTFS-RT delay propagation: a stop without an update
    takes the delay of the last updated stop before it (`propagated: true`). NO_DATA stops the propagation, a
    SKIPPED stop passes it on. MLD sends only the next stop, KM trains a few; ZTP every stop."""
    rows: list[dict] = []
    carry: int | None = None
    started = False
    for st in stops:
        row = explicit.get(st.sequence)
        if row is not None:
            started = True
            if row["schedule_relationship"] == "NO_DATA":
                carry = None
            elif row["schedule_relationship"] == "SCHEDULED" and row["delay_s"] is not None:
                carry = row["delay_s"]
            rows.append(row)
        elif started and carry is not None:
            arr, dep = ref.epoch(st.arrival), ref.epoch(st.departure)
            rows.append(
                {
                    **_row(
                        ref, index, st, None if arr is None else arr + carry, None if dep is None else dep + carry, "SCHEDULED"
                    ),
                    "propagated": True,
                }
            )
    return rows


def build_trip(feed_id: str, ref: TripRef, tu, stops: list[StopTime], index: TripIndex, now: float, ts: int) -> dict:
    explicit: dict[int, dict] = {}
    for u, st in match_updates(tu.stop_time_update, stops, ref.day.base):
        if st is None:
            continue
        sched_arr, sched_dep = ref.epoch(st.arrival), ref.epoch(st.departure)
        pred_arr = _event_time(u.arrival, sched_arr) if u.HasField("arrival") else None
        pred_dep = _event_time(u.departure, sched_dep) if u.HasField("departure") else None
        relationship = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(u.schedule_relationship)
        explicit[st.sequence] = _row(ref, index, st, pred_arr, pred_dep, relationship)
    rows = propagate(ref, index, stops, explicit)
    upcoming = [
        r for r in rows if r["delay_s"] is not None and (r["predicted_departure"] or r["predicted_arrival"] or 0) >= now - 30
    ]
    with_delay = [r for r in rows if r["delay_s"] is not None]
    delay = upcoming[0]["delay_s"] if upcoming else (with_delay[-1]["delay_s"] if with_delay else None)
    return {
        **trip_fields(feed_id, ref),
        "vehicle_id": tu.vehicle.id or None,
        "delay_s": delay,
        "timestamp": int(tu.timestamp) or ts,
        "schedule_relationship": pb.TripDescriptor.ScheduleRelationship.Name(tu.trip.schedule_relationship),
        "stops": rows,
    }


def merge_passed(prev: dict | None, trip: dict) -> dict:
    """Keep stops the feed has dropped since `prev` (ZTP removes a stop once it is passed).

    Kept rows get `passed: True` and their last known times; rows from the current update get `passed: False`.
    Stop sequences are only comparable within one version, so a version change starts over.
    """
    rows = [{**r, "passed": False} for r in trip["stops"]]
    if prev is None or prev.get("version") != trip["version"]:
        return {**trip, "stops": rows}
    first = min((r["stop_sequence"] for r in rows), default=None)
    kept = [{**r, "passed": True} for r in prev.get("stops", []) if first is None or r["stop_sequence"] < first]
    kept.sort(key=lambda r: r["stop_sequence"])
    return {**trip, "stops": kept + rows}


def trip_fields(feed_id: str, ref: TripRef) -> dict:
    i = ref.info
    return {
        "key": trip_key(feed_id, ref),
        "feed_id": feed_id,
        "version": i.version,
        "trip_id": i.trip_id,
        "service_date": ref.day.date.isoformat(),
        "route_id": i.route_id,
        "route_short_name": i.route_short_name,
        "route_type": i.route_type,
        "headsign": i.headsign,
        "direction_id": i.direction_id,
        "shape_id": i.shape_id,
        "block_id": i.block_id,
    }


# --------------------------------------------------------------------------------------------- vehicles


def build_vehicle(
    feed_id: str, ref: TripRef, vp, index: TripIndex, delay: int | None, motion: MotionTracker | None = None, ts: int = 0
) -> dict:
    """`motion` fills bearing/speed the feed left out (from the vehicle's previous fix at `ts` or vp.timestamp)."""
    pos = vp.position
    # ZTP sends 0 for unknown: speed 0 for most MPK buses (driving ones too), bearing 0 far more often than
    # heading north explains. Treated as missing; a standing vehicle is estimated as speed 0, last bearing.
    bearing = pos.bearing if pos.HasField("bearing") and pos.bearing != 0 else None
    speed = pos.speed if pos.HasField("speed") and pos.speed > 0 else None
    if motion is not None and vp.vehicle.id:
        bearing, speed = motion.fill(vp.vehicle.id, pos.latitude, pos.longitude, int(vp.timestamp) or ts, bearing, speed)
    return {
        "feed_id": feed_id,
        "vehicle_id": vp.vehicle.id,
        "label": vp.vehicle.label or None,
        "license_plate": vp.vehicle.license_plate or None,
        "lat": pos.latitude,
        "lon": pos.longitude,
        "bearing": bearing,
        "speed": speed,
        "timestamp": int(vp.timestamp) or None,
        "current_status": pb.VehiclePosition.VehicleStopStatus.Name(vp.current_status) if vp.HasField("current_status") else None,
        "current_stop_sequence": vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None,
        "stop_id": vp.stop_id or None,
        "public_stop_id": index.public_stop(ref.info.version, vp.stop_id),
        "trip": trip_fields(feed_id, ref),
        "delay_s": delay,
        "occupancy": pb.VehiclePosition.OccupancyStatus.Name(vp.occupancy_status) if vp.HasField("occupancy_status") else None,
        "wheelchair_accessible": (
            pb.VehicleDescriptor.WheelchairAccessible.Name(vp.vehicle.wheelchair_accessible)
            if vp.vehicle.HasField("wheelchair_accessible")
            else None
        ),
    }


# --------------------------------------------------------------------------------------------- alerts


def _text(ts) -> str | None:
    if not ts.translation:
        return None
    return next((t.text for t in ts.translation if t.language in ("pl", "")), ts.translation[0].text)


def build_alert(feed_id: str, entity_id: str, alert, index: TripIndex) -> dict:
    today = next((d for d in index.days if d.date == index.loaded_for), None)
    entities = []
    for ie in alert.informed_entity:
        entities.append(
            {
                "agency_id": ie.agency_id or None,
                "route_id": ie.route_id or None,
                "route_type": ie.route_type if ie.HasField("route_type") else None,
                "trip_id": ie.trip.trip_id or None,
                "stop_id": ie.stop_id or None,
                "public_stop_id": index.public_stop(today.version, ie.stop_id) if today and ie.stop_id else None,
            }
        )
    return {
        "feed_id": feed_id,
        "id": entity_id,
        "cause": pb.Alert.Cause.Name(alert.cause),
        "effect": pb.Alert.Effect.Name(alert.effect),
        "header": _text(alert.header_text),
        "description": _text(alert.description_text),
        "url": _text(alert.url),
        "active_periods": [{"start": p.start or None, "end": p.end or None} for p in alert.active_period],
        "informed_entities": entities,
    }


# --------------------------------------------------------------------------------------------- message


async def process_message(feed_id: str, msg, index: TripIndex, state: FeedRealtimeState, load: Loader, now: float) -> Result:
    res = Result(entities=len(msg.entity))
    ts = int(msg.header.timestamp) or int(now)

    tus = [e for e in msg.entity if e.HasField("trip_update") and not e.is_deleted]
    vps = [e for e in msg.entity if e.HasField("vehicle") and not e.is_deleted]

    # 1. resolve every reported (trip id, start date) once
    def ref_key(trip) -> tuple[str, date | None]:
        return trip.trip_id, _start_date(trip)

    final: dict[tuple[str, date | None], TripRef | None] = {}
    for e in tus:
        final.setdefault(ref_key(e.trip_update.trip), None)
    for e in vps:
        if e.vehicle.trip.trip_id:
            final.setdefault(ref_key(e.vehicle.trip), None)
    for trip_id, start in final:
        final[(trip_id, start)] = (
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
    await load({(r.info.version, r.info.trip_id) for r in final.values() if r})

    # 2. trip updates
    for e in tus:
        tu = e.trip_update
        ref = final.get(ref_key(tu.trip))
        if ref is None:
            res.unmatched_trips.append(tu.trip.trip_id or f"<entity {e.id}>")
            continue
        if tu.trip.route_id and tu.trip.route_id != ref.info.route_id:
            res.mismatches += 1
        trip = build_trip(feed_id, ref, tu, index.stop_times.get((ref.info.version, ref.info.trip_id), []), index, now, ts)
        res.trips.append(trip)
        state.trip_delays[ref.key] = (trip["delay_s"], ts)

    # 3. vehicles
    for e in vps:
        vp = e.vehicle
        ref = final.get(ref_key(vp.trip)) if vp.trip.trip_id else None
        if ref is None or not vp.HasField("position"):
            res.unmatched_vehicles.append({"vehicle_id": vp.vehicle.id or e.id, "trip_id": vp.trip.trip_id or None})
            continue
        if vp.trip.route_id and vp.trip.route_id != ref.info.route_id:
            res.mismatches += 1
        delay = state.trip_delays.get(ref.key, (None, 0))[0]
        res.vehicles.append(build_vehicle(feed_id, ref, vp, index, delay, state.motion, ts))

    # 4. alerts
    for e in msg.entity:
        if e.HasField("alert") and not e.is_deleted:
            res.alerts.append(build_alert(feed_id, e.id, e.alert, index))

    # forget delays of trips not seen for 10 minutes
    cutoff = ts - 600
    for k in [k for k, (_, t) in state.trip_delays.items() if t < cutoff]:
        del state.trip_delays[k]
    state.motion.forget_before(ts - FORGET_S)
    return res
