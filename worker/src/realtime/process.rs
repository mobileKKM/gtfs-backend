//! A GTFS-RT FeedMessage -> map vehicles, per-stop predictions and alerts (one feed's state shared by its files).
//!
//! - `trip_id` is the source of truth: route, headsign, direction and stops come from the timetabled trip it resolves
//!   to; realtime route ids are only compared (mismatches are counted).
//! - Unmatched trips and vehicles are returned separately and never shown.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::alerts::build_alert;
use super::extra::{block_overruns, build_new_trip, copy_descriptor, duplicate_ref};
use super::index::{StopTime, TripIndex};
use super::layout::{LiveAlert, LiveTrip, LiveVehicle, TripFields, UnmatchedVehicle};
use super::pb::{self, trip_descriptor::ScheduleRelationship as TD};
use super::resolve::{TripRef, Windows, resolve, start_date};
use super::trips::{build_trip, stop_relationship_name, trip_fields, trip_relationship_name, with_timetable};
use super::vehicles::{FORGET_S, MotionTracker, vehicle_for};
use crate::config::ExtraKind;

/// A trip's last delay is kept this long for its vehicle (the files are polled separately).
const DELAY_MEMORY_S: i64 = 600;

/// How an extra trip's vehicle (in another file) finds it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExtraKey {
    New(String),
    /// original trip_id, start_date, start_time
    Dup(String, String, String),
}

/// One feed's normalization state, shared by its realtime files.
pub struct FeedState {
    pub windows: Windows,
    pub ignore_service_suffix: bool,
    /// vehicles whose position the feed measured longer ago than this (by its header clock) are dropped
    pub max_age_s: i64,
    pub extra_kinds: HashSet<i32>,
    /// trip key -> (delay, feed ts)
    trip_delays: HashMap<String, (Option<i64>, i64)>,
    /// trip key -> feed ts of trips the feed DELETED: their vehicles aren't shown either
    deleted_trips: HashMap<String, i64>,
    extra_trips: HashMap<ExtraKey, (TripFields, i32, i64)>,
    motion: MotionTracker,
}

impl FeedState {
    #[allow(deprecated)] // ADDED
    pub fn new(feed: &crate::config::FeedConfig, max_age_s: i64) -> Self {
        let extra_kinds = feed
            .extra_trips
            .iter()
            .map(|k| match k {
                ExtraKind::New => TD::New as i32,
                ExtraKind::Added => TD::Added as i32,
                ExtraKind::Duplicated => TD::Duplicated as i32,
            })
            .collect();
        Self {
            windows: super::resolve::WINDOWS,
            ignore_service_suffix: feed.ignore_service_suffix,
            max_age_s,
            extra_kinds,
            trip_delays: HashMap::new(),
            deleted_trips: HashMap::new(),
            extra_trips: HashMap::new(),
            motion: MotionTracker::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Processed {
    /// the feed header's (else when processed): how old this data is
    pub timestamp: i64,
    pub vehicles: Vec<LiveVehicle>,
    pub trips: Vec<LiveTrip>,
    pub alerts: Vec<LiveAlert>,
    pub unmatched_vehicles: Vec<UnmatchedVehicle>,
    pub unmatched_trips: Vec<String>,
    /// `{alert id}: route X`
    pub unmatched_alert_entities: Vec<String>,
    pub entities: usize,
    pub mismatches: usize,
    /// GTFS-RT schedule_relationship of every trip update (matched or not) and of every stop time update in them
    pub trip_relationships: BTreeMap<String, usize>,
    pub stop_relationships: BTreeMap<String, usize>,
    pub block_late_next: usize,
    pub block_late_next_without_update: usize,
    /// positions older than the feed's max age, not shown
    pub stale_vehicles: usize,
}

/// Stop times the caller loaded for the trips of a message.
pub type StopTimes = HashMap<(i32, String), Arc<Vec<StopTime>>>;

#[allow(deprecated)] // ADDED: deprecated by GTFS-RT, still sent by some feeds
fn is_extra(rel: i32) -> bool {
    rel == TD::Added as i32 || rel == TD::New as i32 || rel == TD::Duplicated as i32
}

/// The (version, trip_id)s whose stop times processing `msg` needs: those of the trips it resolves to, and of the
/// originals of DUPLICATED copies.
pub fn wanted_stop_times(msg: &pb::FeedMessage, index: &TripIndex, state: &FeedState, now: f64) -> Vec<(i32, String)> {
    let (refs, copies) = resolve_all(msg, index, state, now);
    let mut wanted: Vec<(i32, String)> =
        refs.values().flatten().map(|r| (r.info.version, r.info.trip_id.clone())).collect();
    wanted.extend(copies.values().map(|(c, d)| (c.info.version, d.trip_id().to_string())));
    wanted.sort();
    wanted.dedup();
    wanted
}

type Refs = HashMap<(String, String), Option<TripRef>>;
type Copies = HashMap<String, (TripRef, pb::TripDescriptor)>;

fn descriptor_key(t: Option<&pb::TripDescriptor>) -> (String, String) {
    t.map_or_else(Default::default, |t| (t.trip_id().to_string(), t.start_date().to_string()))
}

/// Every reported (trip id, start date) resolved once, and the copies of DUPLICATED trips.
fn resolve_all(msg: &pb::FeedMessage, index: &TripIndex, state: &FeedState, now: f64) -> (Refs, Copies) {
    let mut refs: Refs = HashMap::new();
    let trips = msg.entity.iter().filter(|e| !e.is_deleted()).flat_map(|e| {
        let tu = e.trip_update.as_ref().map(|t| &t.trip);
        let vp = e.vehicle.as_ref().and_then(|v| v.trip.as_ref()).filter(|t| !t.trip_id().is_empty());
        tu.into_iter().chain(vp)
    });
    for t in trips {
        refs.entry(descriptor_key(Some(t))).or_insert_with(|| {
            (!t.trip_id().is_empty())
                .then(|| {
                    resolve(
                        index,
                        t.trip_id(),
                        now,
                        &state.windows,
                        start_date(t.start_date()),
                        state.ignore_service_suffix,
                    )
                })
                .flatten()
        });
    }
    let mut copies = HashMap::new();
    if state.extra_kinds.contains(&(TD::Duplicated as i32)) {
        for e in msg.entity.iter().filter(|e| !e.is_deleted()) {
            let Some(tu) = &e.trip_update else { continue };
            if tu.trip.schedule_relationship == Some(TD::Duplicated as i32) {
                let (descriptor, copy_id) = copy_descriptor(tu);
                if let Some(copy) = duplicate_ref(index, &descriptor, copy_id) {
                    copies.insert(e.id.clone(), (copy, descriptor));
                }
            }
        }
    }
    (refs, copies)
}

pub fn process_message(
    feed_id: &str,
    msg: &pb::FeedMessage,
    index: &TripIndex,
    state: &mut FeedState,
    stop_times: &StopTimes,
    now: f64,
) -> Processed {
    let header_ts = msg.header.timestamp.filter(|t| *t != 0).map(|t| t as i64);
    let ts = header_ts.unwrap_or(now as i64);
    let mut res = Processed { timestamp: ts, entities: msg.entity.len(), ..Default::default() };
    let (refs, copies) = resolve_all(msg, index, state, now);
    let no_stops: Vec<StopTime> = Vec::new();
    let stops_of = |version: i32, trip_id: &str| -> &[StopTime] {
        stop_times.get(&(version, trip_id.to_string())).map_or(no_stops.as_slice(), |s| s.as_slice())
    };

    // trip updates
    let mut matched: Vec<(TripRef, usize)> = Vec::new(); // (ref, index into res.trips)
    for e in msg.entity.iter().filter(|e| !e.is_deleted()) {
        let Some(tu) = &e.trip_update else { continue };
        let descriptor = tu.trip.clone();
        let rel = descriptor.schedule_relationship.unwrap_or(0);
        *res.trip_relationships.entry(trip_relationship_name(rel)).or_default() += 1;
        for u in &tu.stop_time_update {
            *res.stop_relationships.entry(stop_relationship_name(u.schedule_relationship.unwrap_or(0))).or_default() +=
                1;
        }
        let label = if descriptor.trip_id().is_empty() {
            format!("<entity {}>", e.id)
        } else {
            descriptor.trip_id().to_string()
        };
        if is_extra(rel) {
            let mut trip = None;
            if state.extra_kinds.contains(&rel) && rel == TD::Duplicated as i32 {
                if let Some((copy, d)) = copies.get(&e.id) {
                    let original = stops_of(copy.info.version, d.trip_id());
                    if !original.is_empty() {
                        let mut t = build_trip(feed_id, copy, tu, original, index, now, ts);
                        // no timetable has the copy: every stop of the shifted original, predicted or not
                        t.stops = with_timetable(copy, index, original, std::mem::take(&mut t.stops));
                        t.extra = Some(true);
                        state.trip_delays.insert(t.fields.key.clone(), (t.delay_s, ts)); // for its vehicle
                        let found = (trip_fields(feed_id, copy), copy.info.version, ts);
                        // its vehicle names the original and the copy's start time, the date maybe not
                        let dup = |date: &str| ExtraKey::Dup(d.trip_id().into(), date.into(), d.start_time().into());
                        state.extra_trips.insert(dup(d.start_date()), found.clone());
                        state.extra_trips.insert(dup(""), found);
                        trip = Some(t);
                    }
                }
            } else if state.extra_kinds.contains(&rel)
                && let Some(t) = build_new_trip(feed_id, tu, index, ts)
            {
                state
                    .extra_trips
                    .insert(ExtraKey::New(descriptor.trip_id().into()), (t.fields.clone(), t.fields.version, ts));
                trip = Some(t);
            }
            match trip {
                Some(t) => res.trips.push(t),
                None => res.unmatched_trips.push(format!("{label} ({})", trip_relationship_name(rel))),
            }
            continue;
        }
        let Some(r) = refs.get(&descriptor_key(Some(&descriptor))).cloned().flatten() else {
            res.unmatched_trips.push(label);
            continue;
        };
        if rel == TD::Deleted as i32 {
            state.deleted_trips.insert(r.key(feed_id), ts);
        }
        if !descriptor.route_id().is_empty() && descriptor.route_id() != r.info.route_id {
            res.mismatches += 1;
        }
        let trip = build_trip(feed_id, &r, tu, stops_of(r.info.version, &r.info.trip_id), index, now, ts);
        state.trip_delays.insert(trip.fields.key.clone(), (trip.delay_s, ts));
        res.trips.push(trip);
        matched.push((r, res.trips.len() - 1));
    }
    let pairs: Vec<(TripRef, &LiveTrip)> = matched.iter().map(|(r, i)| (r.clone(), &res.trips[*i])).collect();
    let last_sequence = |r: &TripRef| stops_of(r.info.version, &r.info.trip_id).last().map(|s| s.sequence);
    (res.block_late_next, res.block_late_next_without_update) = block_overruns(feed_id, index, &pairs, last_sequence);

    // vehicles
    for e in msg.entity.iter().filter(|e| !e.is_deleted()) {
        let Some(vp) = &e.vehicle else { continue };
        if let Some(vts) = vp.timestamp.filter(|t| *t != 0)
            && ts - vts as i64 > state.max_age_s
        {
            res.stale_vehicles += 1;
            continue;
        }
        let t = vp.trip.clone().unwrap_or_default();
        let rel = t.schedule_relationship.unwrap_or(0);
        let r = if !t.trip_id().is_empty() && !is_extra(rel) {
            refs.get(&descriptor_key(Some(&t))).cloned().flatten()
        } else {
            None
        };
        if let Some(r) = &r
            && (rel == TD::Deleted as i32 || state.deleted_trips.contains_key(&r.key(feed_id)))
        {
            continue; // its trip is deleted: neither is shown
        }
        if r.is_none() && !t.trip_id().is_empty() && vp.position.is_some() {
            // an extra trip's vehicle (a NEW trip's may not say so: its id is in no timetable)
            let key = if rel == TD::Duplicated as i32 {
                ExtraKey::Dup(t.trip_id().into(), t.start_date().into(), t.start_time().into())
            } else {
                ExtraKey::New(t.trip_id().into())
            };
            if let Some((fields, version, _)) = state.extra_trips.get(&key).cloned() {
                let delay = state.trip_delays.get(&fields.key).and_then(|(d, _)| *d);
                res.vehicles.push(vehicle_for(feed_id, fields, version, vp, index, delay, &mut state.motion, ts));
                continue;
            }
        }
        let (Some(r), true) = (r, vp.position.is_some()) else {
            let vehicle_id = vp.vehicle.as_ref().map(|v| v.id()).filter(|id| !id.is_empty()).unwrap_or(&e.id);
            res.unmatched_vehicles.push(UnmatchedVehicle {
                vehicle_id: vehicle_id.to_string(),
                trip_id: (!t.trip_id().is_empty()).then(|| t.trip_id().to_string()),
            });
            continue;
        };
        if !t.route_id().is_empty() && t.route_id() != r.info.route_id {
            res.mismatches += 1;
        }
        let delay = state.trip_delays.get(&r.key(feed_id)).and_then(|(d, _)| *d);
        res.vehicles.push(vehicle_for(
            feed_id,
            trip_fields(feed_id, &r),
            r.info.version,
            vp,
            index,
            delay,
            &mut state.motion,
            ts,
        ));
    }

    // alerts
    for e in msg.entity.iter().filter(|e| !e.is_deleted()) {
        if let Some(alert) = &e.alert {
            let (a, unmatched) = build_alert(feed_id, &e.id, alert, index, now, state.ignore_service_suffix);
            res.alerts.push(a);
            res.unmatched_alert_entities.extend(unmatched);
        }
    }

    // forget delays of trips not seen for a while, and fixes of vehicles gone
    let cutoff = ts - DELAY_MEMORY_S;
    state.trip_delays.retain(|_, (_, t)| *t >= cutoff);
    state.deleted_trips.retain(|_, t| *t >= cutoff);
    state.extra_trips.retain(|_, (_, _, t)| *t >= cutoff);
    state.motion.forget_before(ts - FORGET_S);
    res
}

/// The median of the trips' delays: ~0 normally; about +-3600 on a clock-change day would mean the feed's day_start is
/// wrong.
pub fn median_delay(res: &Processed) -> Option<f64> {
    let mut d: Vec<i64> = res.trips.iter().filter_map(|t| t.delay_s).collect();
    if d.is_empty() {
        return None;
    }
    d.sort_unstable();
    let n = d.len();
    Some(if n % 2 == 1 { d[n / 2] as f64 } else { (d[n / 2 - 1] + d[n / 2]) as f64 / 2.0 })
}
