//! Which timetabled trip instance (version + service day) a realtime trip_id means.
//!
//! The trip's service must run on one of the index's days (yesterday/today/tomorrow, each with its serving version),
//! and for trip updates and vehicles "now" must fall within [first departure - 3 h, last arrival + 1 h]; the instance
//! running closest to now wins. A TripDescriptor's `start_date` names the instance (GTFS-RT: "the start date of this
//! trip instance"): only that day's is taken.

use std::sync::Arc;

use chrono::NaiveDate;

use super::index::{ServiceDay, TripIndex, TripInfo, strip_service_suffix};
use crate::util::{parse_ymd, trip_key};

/// Trip updates carry predictions for trips starting up to ~2 h ahead.
pub const EARLY_WINDOW_S: i64 = 3 * 3600;
pub const LATE_WINDOW_S: i64 = 3600;

/// One trip instance: a timetabled trip on one service day.
#[derive(Debug, Clone, PartialEq)]
pub struct TripRef {
    pub day: ServiceDay,
    pub info: Arc<TripInfo>,
}

impl TripRef {
    /// The trip key (the API's): a service day has exactly one serving version, so trip id + date suffice.
    pub fn key(&self, feed_id: &str) -> String {
        trip_key(feed_id, self.day.date, &self.info.trip_id)
    }

    pub fn epoch(&self, secs: Option<i64>) -> Option<i64> {
        secs.map(|s| self.day.base + s)
    }

    pub fn start(&self) -> i64 {
        self.day.base + self.info.first_departure
    }

    pub fn end(&self) -> i64 {
        self.day.base + self.info.last_arrival
    }
}

/// A TripDescriptor's start_date, if given and valid.
pub fn start_date(text: &str) -> Option<NaiveDate> {
    if text.is_empty() { None } else { parse_ymd(text) }
}

/// The trips an id can mean, in order of preference: the id as is, then (for feeds whose `_service_N` suffix may not
/// match) the id without it.
fn candidates<'a>(
    index: &'a TripIndex,
    trip_id: &'a str,
    ignore_service_suffix: bool,
) -> impl Iterator<Item = &'a [Arc<TripInfo>]> {
    let exact = index.data.by_trip_id.get(trip_id).map(Vec::as_slice).unwrap_or_default();
    let base = ignore_service_suffix
        .then(|| index.data.by_base_id.get(strip_service_suffix(trip_id)).map(Vec::as_slice).unwrap_or_default());
    std::iter::once(exact).chain(base)
}

/// Every service day of the index on which one of `candidates` runs.
fn instances<'a>(index: &'a TripIndex, candidates: &'a [Arc<TripInfo>]) -> impl Iterator<Item = TripRef> + 'a {
    candidates.iter().flat_map(move |info| {
        index
            .days
            .iter()
            .filter(move |d| d.version == info.version && d.services.contains(&info.service_id))
            .map(move |d| TripRef { day: d.clone(), info: info.clone() })
    })
}

/// 0 while the instance runs, else how far `now` is from it; None outside its window.
pub fn window_distance(r: &TripRef, now: f64, early_s: i64, late_s: i64) -> Option<f64> {
    let (start, end) = (r.start() as f64, r.end() as f64);
    if !(start - early_s as f64 <= now && now <= end + late_s as f64) {
        return None;
    }
    Some(if start <= now && now <= end { 0.0 } else { (now - start).abs().min((now - end).abs()) })
}

pub struct Windows {
    pub early_s: i64,
    pub late_s: i64,
}

pub const WINDOWS: Windows = Windows { early_s: EARLY_WINDOW_S, late_s: LATE_WINDOW_S };

/// The instance a trip update or vehicle means: the one running closest to now (of `start_date`'s day, if given).
pub fn resolve(
    index: &TripIndex,
    trip_id: &str,
    now: f64,
    w: &Windows,
    start_date: Option<NaiveDate>,
    ignore_service_suffix: bool,
) -> Option<TripRef> {
    for cands in candidates(index, trip_id, ignore_service_suffix) {
        let mut best: Option<(f64, TripRef)> = None;
        for r in instances(index, cands) {
            if start_date.is_some_and(|d| r.day.date != d) {
                continue;
            }
            let Some(dist) = window_distance(&r, now, w.early_s, w.late_s) else {
                continue;
            };
            if best.as_ref().is_none_or(|(b, _)| dist < *b) {
                best = Some((dist, r));
            }
        }
        if let Some((_, r)) = best {
            return Some(r);
        }
    }
    None
}

/// The one instance an alert names (a selector's trip is one instance): `start_date`'s if given, else the first one
/// that hasn't ended yet, else the one that ended last.
pub fn resolve_instance(
    index: &TripIndex,
    trip_id: &str,
    now: f64,
    start_date: Option<NaiveDate>,
    ignore_service_suffix: bool,
) -> Option<TripRef> {
    let rank = |r: &TripRef| {
        let ended = (r.end() as f64) < now;
        (ended, if ended { -r.end() } else { r.start() })
    };
    for cands in candidates(index, trip_id, ignore_service_suffix) {
        let mut best: Option<TripRef> = None;
        for r in instances(index, cands).filter(|r| start_date.is_none_or(|d| r.day.date == d)) {
            if best.as_ref().is_none_or(|b| rank(&r) < rank(b)) {
                best = Some(r);
            }
        }
        if best.is_some() {
            return best;
        }
    }
    None
}
