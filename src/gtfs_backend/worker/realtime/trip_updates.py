"""TripUpdates -> per-stop predictions (live_layout.LiveTrip).

- Stop updates are paired with the static stop times by stop_sequence, else by stop_id moving forward.
- Stops without an update get the delay of the last updated stop before them (GTFS-RT delay propagation).
- Delays are always recomputed: predicted - scheduled, scheduled = the feed's day base + GTFS time. The base
  is noon - 12h (GTFS), or local midnight for feeds configured with `day_start: midnight` (ZTP's trams).
"""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.live_layout import LiveTrip, StopRow, TripFields
from gtfs_backend.worker.realtime.resolve import TripRef
from gtfs_backend.worker.realtime.trip_index import StopTime, TripIndex


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


def _row(ref: TripRef, index: TripIndex, st: StopTime, pred_arr: int | None, pred_dep: int | None, relationship: str) -> StopRow:
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


def propagate(ref: TripRef, index: TripIndex, stops: list[StopTime], explicit: dict[int, StopRow]) -> list[StopRow]:
    """Rows in trip order from the first updated stop on, with GTFS-RT delay propagation: a stop without an update
    takes the delay of the last updated stop before it (`propagated: true`). NO_DATA stops the propagation, a
    SKIPPED stop passes it on. MLD sends only the next stop, KM trains a few; ZTP every stop."""
    rows: list[StopRow] = []
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
            row = _row(ref, index, st, None if arr is None else arr + carry, None if dep is None else dep + carry, "SCHEDULED")
            rows.append({**row, "propagated": True})
    return rows


def with_timetable(ref: TripRef, index: TripIndex, stops: list[StopTime], rows: list[StopRow]) -> list[StopRow]:
    """Every stop of the trip, in order: its row where it has one, else its timetable without a prediction. For trips
    no timetable has (DUPLICATED copies), whose stops come only from realtime. The last stop is no departure, as in
    the timetable's departures."""
    by_sequence = {r["stop_sequence"]: r for r in rows}
    out = [by_sequence.get(st.sequence) or _row(ref, index, st, None, None, "SCHEDULED") for st in stops]
    if out:
        out[-1] = {**out[-1], "scheduled_departure": None}
    return out


def trip_fields(feed_id: str, ref: TripRef) -> TripFields:
    i = ref.info
    return {
        "key": ref.key(feed_id),
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


def build_trip(feed_id: str, ref: TripRef, tu, stops: list[StopTime], index: TripIndex, now: float, ts: int) -> LiveTrip:
    explicit: dict[int, StopRow] = {}
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


def merge_stops(prev: LiveTrip | None, trip: LiveTrip) -> LiveTrip:
    """The current update's stops, preceded by the ones the feed has dropped since `prev` (ZTP removes a stop once
    the vehicle is past it), with their last known times. A trip update only lists predictions, so dropped stops
    are kept for their times and not marked: whether a vehicle is past a stop is up to its VehiclePosition.
    Stop sequences are only comparable within one version, so a version change starts over.
    """
    rows = list(trip["stops"])
    if prev is None or prev.get("version") != trip["version"]:
        return {**trip, "stops": rows}
    first = min((r["stop_sequence"] for r in rows), default=None)
    kept = [r for r in prev.get("stops", []) if first is None or r["stop_sequence"] < first]
    kept.sort(key=lambda r: r["stop_sequence"])
    return {**trip, "stops": kept + rows}
