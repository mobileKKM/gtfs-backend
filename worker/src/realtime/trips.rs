//! TripUpdates -> per-stop predictions (LiveTrip).
//!
//! - Stop updates are paired with the timetable's stop times by stop_sequence, else by stop_id moving forward.
//! - Delays are always recomputed: predicted - scheduled, scheduled = the feed's day base + GTFS time.
//! - Propagation (GTFS-RT): a stop without an update takes the delay of the last updated stop before it; NO_DATA stops
//!   that, SKIPPED passes it on. A trip-level `delay` (TripUpdate.delay) applies from the trip's start until the first
//!   stop with an update of its own.

use super::index::{StopTime, TripIndex};
use super::layout::{LiveTrip, StopRow, TripFields};
use super::pb::{self, trip_update::StopTimeUpdate};
use super::resolve::TripRef;

fn event_time(ev: &pb::trip_update::StopTimeEvent, scheduled: Option<i64>) -> Option<i64> {
    // GTFS-RT: if both are given, time takes precedence; delay alone means scheduled time + delay
    match (ev.time.filter(|t| *t != 0), ev.delay, scheduled) {
        (Some(t), _, _) => Some(t),
        (None, Some(d), Some(s)) => Some(s + d as i64),
        _ => None,
    }
}

fn update_time(u: &StopTimeUpdate) -> Option<i64> {
    [&u.arrival, &u.departure].into_iter().flatten().find_map(|ev| ev.time.filter(|t| *t != 0))
}

/// Pair stop_time_updates with stop times: by stop_sequence (when its stop_id, if given, agrees), else by stop_id
/// moving forward. Without a sequence, a stop visited twice (loop lines) is the visit whose scheduled time is closest to
/// the predicted time (needs the day's `base`), else the next visit.
pub fn match_updates<'a>(
    updates: &'a [StopTimeUpdate],
    stops: &'a [StopTime],
    base: Option<i64>,
) -> Vec<(&'a StopTimeUpdate, Option<&'a StopTime>)> {
    let by_seq: std::collections::HashMap<i64, usize> =
        stops.iter().enumerate().map(|(i, s)| (s.sequence, i)).collect();
    let mut pos = 0;
    let mut out = Vec::with_capacity(updates.len());
    for u in updates {
        let stop_id = u.stop_id();
        let mut idx = u.stop_sequence.and_then(|seq| by_seq.get(&(seq as i64)).copied());
        if let Some(i) = idx
            && !stop_id.is_empty()
            && stops[i].stop_id != stop_id
        {
            idx = None;
        }
        if idx.is_none() && !stop_id.is_empty() {
            let visits: Vec<usize> = (pos..stops.len()).filter(|i| stops[*i].stop_id == stop_id).collect();
            idx = match (visits.len() > 1, base, update_time(u)) {
                (true, Some(base), Some(predicted)) => {
                    let gap = |i: &usize| match stops[*i].arrival.or(stops[*i].departure) {
                        Some(s) => (predicted - (base + s)).abs() as f64,
                        None => f64::INFINITY,
                    };
                    visits.iter().copied().fold(None, |best: Option<usize>, i| match best {
                        Some(b) if gap(&b) <= gap(&i) => Some(b),
                        _ => Some(i),
                    })
                }
                _ => visits.first().copied(),
            };
        }
        if let Some(i) = idx {
            pos = i + 1;
        }
        out.push((u, idx.map(|i| &stops[i])));
    }
    out
}

pub fn stop_relationship_name(value: i32) -> String {
    pb::trip_update::stop_time_update::ScheduleRelationship::try_from(value)
        .map(|r| r.as_str_name().to_string())
        .unwrap_or_else(|_| value.to_string())
}

pub fn trip_relationship_name(value: i32) -> String {
    pb::trip_descriptor::ScheduleRelationship::try_from(value)
        .map(|r| r.as_str_name().to_string())
        .unwrap_or_else(|_| value.to_string())
}

pub fn row(
    r: &TripRef,
    index: &TripIndex,
    st: &StopTime,
    pred_arr: Option<i64>,
    pred_dep: Option<i64>,
    relationship: String,
) -> StopRow {
    let (sched_arr, sched_dep) = (r.epoch(st.arrival), r.epoch(st.departure));
    let delay = match (pred_dep, sched_dep, pred_arr, sched_arr) {
        (Some(p), Some(s), _, _) => Some(p - s),
        (_, _, Some(p), Some(s)) => Some(p - s),
        _ => None,
    };
    StopRow {
        stop_sequence: st.sequence,
        stop_id: st.stop_id.clone(),
        public_stop_id: index.public_stop(r.info.version, &st.stop_id),
        scheduled_arrival: sched_arr,
        scheduled_departure: sched_dep,
        predicted_arrival: pred_arr,
        predicted_departure: pred_dep,
        delay_s: delay,
        schedule_relationship: relationship,
        propagated: false,
    }
}

/// Rows in trip order from the first updated stop on (from the start with a trip-level delay), with propagation.
pub fn propagate(
    r: &TripRef,
    index: &TripIndex,
    stops: &[StopTime],
    explicit: &std::collections::HashMap<i64, StopRow>,
    trip_delay: Option<i64>,
) -> Vec<StopRow> {
    let mut rows = Vec::new();
    let mut carry = trip_delay;
    let mut started = trip_delay.is_some();
    for st in stops {
        if let Some(row) = explicit.get(&st.sequence) {
            started = true;
            match row.schedule_relationship.as_str() {
                "NO_DATA" => carry = None,
                "SCHEDULED" if row.delay_s.is_some() => carry = row.delay_s,
                _ => {}
            }
            rows.push(row.clone());
        } else if started && let Some(c) = carry {
            let (arr, dep) = (r.epoch(st.arrival), r.epoch(st.departure));
            let mut row = row(r, index, st, arr.map(|a| a + c), dep.map(|d| d + c), "SCHEDULED".into());
            row.propagated = true;
            rows.push(row);
        }
    }
    rows
}

/// Every stop of the trip in order: its row where it has one, else its timetable without a prediction. For trips no
/// timetable has (DUPLICATED copies). The last stop is no departure, as in the timetable's departures.
pub fn with_timetable(r: &TripRef, index: &TripIndex, stops: &[StopTime], rows: Vec<StopRow>) -> Vec<StopRow> {
    let mut by_sequence: std::collections::HashMap<i64, StopRow> =
        rows.into_iter().map(|row| (row.stop_sequence, row)).collect();
    let mut out: Vec<StopRow> = stops
        .iter()
        .map(|st| by_sequence.remove(&st.sequence).unwrap_or_else(|| row(r, index, st, None, None, "SCHEDULED".into())))
        .collect();
    if let Some(last) = out.last_mut() {
        last.scheduled_departure = None;
    }
    out
}

pub fn trip_fields(feed_id: &str, r: &TripRef) -> TripFields {
    let i = &r.info;
    TripFields {
        key: r.key(feed_id),
        feed_id: feed_id.into(),
        version: i.version,
        trip_id: i.trip_id.clone(),
        service_date: r.day.date.to_string(),
        route_id: Some(i.route_id.clone()),
        route_short_name: i.route_short_name.clone(),
        route_type: i.route_type,
        headsign: i.headsign.clone(),
        direction_id: i.direction_id,
        shape_id: i.shape_id.clone(),
        block_id: i.block_id.clone(),
    }
}

pub fn build_trip(
    feed_id: &str,
    r: &TripRef,
    tu: &pb::TripUpdate,
    stops: &[StopTime],
    index: &TripIndex,
    now: f64,
    ts: i64,
) -> LiveTrip {
    let mut explicit = std::collections::HashMap::new();
    for (u, st) in match_updates(&tu.stop_time_update, stops, Some(r.day.base)) {
        let Some(st) = st else { continue };
        let (sched_arr, sched_dep) = (r.epoch(st.arrival), r.epoch(st.departure));
        let pred_arr = u.arrival.as_ref().and_then(|ev| event_time(ev, sched_arr));
        let pred_dep = u.departure.as_ref().and_then(|ev| event_time(ev, sched_dep));
        let relationship = stop_relationship_name(u.schedule_relationship.unwrap_or(0));
        explicit.insert(st.sequence, row(r, index, st, pred_arr, pred_dep, relationship));
    }
    let rows = propagate(r, index, stops, &explicit, tu.delay.map(i64::from));
    // the delay at the next stop (from 30 s ago on), else the last known one
    let upcoming = rows.iter().find(|x| {
        x.delay_s.is_some() && x.predicted_departure.or(x.predicted_arrival).unwrap_or(0) as f64 >= now - 30.0
    });
    let delay = upcoming.or_else(|| rows.iter().rev().find(|x| x.delay_s.is_some())).and_then(|x| x.delay_s);
    LiveTrip {
        fields: trip_fields(feed_id, r),
        extra: None,
        vehicle_id: tu.vehicle.as_ref().map(|v| v.id()).filter(|id| !id.is_empty()).map(str::to_string),
        delay_s: delay,
        timestamp: tu.timestamp.filter(|t| *t != 0).map_or(ts, |t| t as i64),
        schedule_relationship: trip_relationship_name(tu.trip.schedule_relationship.unwrap_or(0)),
        stops: rows,
    }
}

/// The current update's stops, preceded by the ones the feed has dropped since `prev` (ZTP removes a stop once the
/// vehicle is past it), with their last known times. Stop sequences only compare within one version: a version change
/// starts over.
pub fn merge_stops(prev: Option<&LiveTrip>, mut trip: LiveTrip) -> LiveTrip {
    let Some(prev) = prev.filter(|p| p.fields.version == trip.fields.version) else {
        return trip;
    };
    let first = trip.stops.iter().map(|r| r.stop_sequence).min();
    let mut kept: Vec<StopRow> =
        prev.stops.iter().filter(|r| first.is_none_or(|f| r.stop_sequence < f)).cloned().collect();
    kept.sort_by_key(|r| r.stop_sequence);
    kept.append(&mut trip.stops);
    trip.stops = kept;
    trip
}
