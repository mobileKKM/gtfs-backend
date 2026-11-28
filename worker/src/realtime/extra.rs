//! Trips that aren't a timetabled trip as it is (GTFS-RT NEW, ADDED, DUPLICATED), built from the update itself for the
//! feeds that show them (feeds.yml `extra_trips`, default new and duplicated), and the block overrun counts.
//!
//! NEW (and ADDED, opt-in: its meaning was never specified) is an extra trip: its stops and times are its
//! stop_time_updates' (each needs a stop_id and an absolute time), its route the descriptor's (if the timetable has it),
//! its headsign trip_properties'. With no timetable to be late against, scheduled = predicted.
//!
//! DUPLICATED is a copy of a timetabled trip at another time: the original's stops, shifted to the copy's start_time on
//! its start_date, under trip_properties.trip_id (or `{trip_id}#{date}T{start_time}`), predicted as any trip.
//!
//! Both are marked `extra`: boards take their rows from the live data, as the timetable doesn't have them.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate};

use super::index::{ServiceDay, TripIndex, TripInfo};
use super::layout::{LiveTrip, StopRow, TripFields};
use super::pb;
use super::resolve::{TripRef, start_date};
use super::trips::{stop_relationship_name, trip_relationship_name};
use crate::util::{parse_gtfs_time, trip_key};

/// The index's service day of `d`, else today's.
fn day_of(index: &TripIndex, d: Option<NaiveDate>) -> Option<&ServiceDay> {
    d.and_then(|d| index.day(d)).or_else(|| index.today())
}

/// An extra trip (NEW, ADDED) from its update; None without a stop with a time.
pub fn build_new_trip(feed_id: &str, tu: &pb::TripUpdate, index: &TripIndex, ts: i64) -> Option<LiveTrip> {
    let trip = &tu.trip;
    let first = tu
        .stop_time_update
        .iter()
        .flat_map(|u| [u.arrival.as_ref().and_then(|e| e.time), u.departure.as_ref().and_then(|e| e.time)])
        .flatten()
        .find(|t| *t != 0)?;
    if trip.trip_id().is_empty() {
        return None;
    }
    let local = DateTime::from_timestamp(first, 0)?.with_timezone(&index.tz).date_naive();
    let service_date = start_date(trip.start_date()).unwrap_or(local);
    let version = day_of(index, Some(service_date))?.version;
    let mut rows: Vec<StopRow> = Vec::new();
    for (i, u) in tu.stop_time_update.iter().enumerate() {
        let arrival = u.arrival.as_ref().and_then(|e| e.time).filter(|t| *t != 0);
        let departure = u.departure.as_ref().and_then(|e| e.time).filter(|t| *t != 0);
        if u.stop_id().is_empty() || (arrival.is_none() && departure.is_none()) {
            continue;
        }
        rows.push(StopRow {
            stop_sequence: u.stop_sequence.map_or(i as i64 + 1, i64::from),
            stop_id: u.stop_id().into(),
            public_stop_id: index.public_stop(version, u.stop_id()),
            scheduled_arrival: arrival,
            scheduled_departure: departure,
            predicted_arrival: arrival,
            predicted_departure: departure,
            delay_s: None, // no timetable to be late against
            schedule_relationship: stop_relationship_name(u.schedule_relationship.unwrap_or(0)),
            propagated: false,
        });
    }
    rows.last_mut()?.scheduled_departure = None; // its last stop: not a departure (as the timetable's last stops)
    let props = tu.trip_properties.clone().unwrap_or_default();
    let opt = |s: &str| (!s.is_empty()).then(|| s.to_string());
    Some(LiveTrip {
        fields: TripFields {
            key: trip_key(feed_id, service_date, trip.trip_id()),
            feed_id: feed_id.into(),
            version,
            trip_id: trip.trip_id().into(),
            service_date: service_date.to_string(),
            route_id: index.has_route(version, trip.route_id()).then(|| trip.route_id().to_string()),
            route_short_name: None,
            route_type: None,
            headsign: opt(props.trip_headsign()),
            direction_id: trip.direction_id.map(|d| d as i32),
            shape_id: opt(props.shape_id()),
            block_id: None,
        },
        extra: Some(true),
        vehicle_id: tu.vehicle.as_ref().and_then(|v| opt(v.id())),
        delay_s: None,
        timestamp: tu.timestamp.filter(|t| *t != 0).map_or(ts, |t| t as i64),
        schedule_relationship: trip_relationship_name(trip.schedule_relationship.unwrap_or(0)),
        stops: rows,
    })
}

/// Where a DUPLICATED trip update says its copy runs: trip_properties (the spec) over the descriptor. Returns the
/// descriptor to use and the copy's own trip id, if given.
pub fn copy_descriptor(tu: &pb::TripUpdate) -> (pb::TripDescriptor, Option<String>) {
    let mut out = tu.trip.clone();
    let props = tu.trip_properties.clone().unwrap_or_default();
    if !props.start_date().is_empty() {
        out.start_date = Some(props.start_date().into());
    }
    if !props.start_time().is_empty() {
        out.start_time = Some(props.start_time().into());
    }
    (out, (!props.trip_id().is_empty()).then(|| props.trip_id().to_string()))
}

/// The copy a DUPLICATED trip means: the original trip on the copy's day, shifted to its start time, under its own id
/// (`copy_id`, else `{trip_id}#{date}T{start_time}`). None if the original or the start time is unknown.
pub fn duplicate_ref(index: &TripIndex, trip: &pb::TripDescriptor, copy_id: Option<String>) -> Option<TripRef> {
    let start = parse_gtfs_time(trip.start_time())? as i64;
    let day = day_of(index, start_date(trip.start_date()))?;
    let info = index.data.by_trip_id.get(trip.trip_id())?.iter().find(|i| i.version == day.version)?;
    let shift = start - info.first_departure; // the original's first departure, seconds from its day's base
    let copy_id =
        copy_id.unwrap_or_else(|| format!("{}#{}T{}", trip.trip_id(), day.date.format("%Y%m%d"), trip.start_time()));
    let shifted = ServiceDay { base: day.base + shift, ..day.clone() };
    Some(TripRef { day: shifted, info: Arc::new(TripInfo { trip_id: copy_id, ..(**info).clone() }) })
}

// ------------------------------------------------------------------------------------------------- blocks

/// How often a late trip runs into the next trip of its vehicle's block: only counted (GTFS-RT asks producers to send
/// the next trips' updates themselves; ZTP does).
///
/// The trip of the same block running next after `info` on the same service day: the first starting at or after
/// `info` ends.
fn next_in_block<'a>(index: &'a TripIndex, info: &TripInfo, day: &ServiceDay) -> Option<&'a Arc<TripInfo>> {
    let block = info.block_id.as_ref()?;
    index.data.blocks.get(&(info.version, block.clone()))?.iter().find(|o| {
        o.first_departure >= info.last_arrival && o.trip_id != info.trip_id && day.services.contains(&o.service_id)
    })
}

/// (late, late without update) over one update's trips: predicted at their last stop after the next trip of their
/// block is due to leave, and of those, how many next trips have no update of their own. Canceled trips and trips
/// without a prediction for their last stop don't count.
pub fn block_overruns(
    feed_id: &str,
    index: &TripIndex,
    trips: &[(TripRef, &LiveTrip)],
    last_sequence: impl Fn(&TripRef) -> Option<i64>,
) -> (usize, usize) {
    let updated: HashSet<&str> = trips.iter().map(|(_, t)| t.fields.key.as_str()).collect();
    let (mut late, mut without) = (0, 0);
    for (r, trip) in trips {
        if trip.schedule_relationship == "CANCELED" {
            continue;
        }
        let Some(last) = trip.stops.last() else {
            continue;
        };
        if Some(last.stop_sequence) != last_sequence(r) {
            continue;
        }
        let Some(end) = last.predicted_arrival.or(last.predicted_departure) else {
            continue;
        };
        let Some(next) = next_in_block(index, &r.info, &r.day) else {
            continue;
        };
        if end <= r.day.base + next.first_departure {
            continue;
        }
        late += 1;
        if !updated.contains(trip_key(feed_id, r.day.date, &next.trip_id).as_str()) {
            without += 1;
        }
    }
    (late, without)
}
