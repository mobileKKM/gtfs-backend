//! The live data in Redis, as the API reads it (src/gtfs_backend/live_layout.py documents the same layout):
//!
//! rt:vehicles:{feed_id}          JSON {vehicle key: LiveVehicle} (TTL VEHICLE_TTL)
//! rt:trips:{feed_id}             JSON {trip key: LiveTrip}, incl. stops the feed has dropped (TTL TRIP_TTL)
//! rt:alerts:{feed_id}            JSON {"timestamp", "alerts": [LiveAlert]} (TTL ALERT_TTL)
//! rt:seq:{feed_id}:{kind}        "{run}:{seq}" of the file's last change message
//! rt:unmatched:{feed_id}:{kind}  unmatched trip ids / vehicles / alert entities (TTL UNMATCHED_TTL)
//! rt:meta:{feed_id}:{kind}       poll stats
//! pub/sub rt:updated             after each write, what changed (LiveChanges)
//! pub/sub static:updated         feed_id, after a static import became ready
//!
//! Times are epoch seconds, delays seconds; enums are GTFS-RT names (alerts: lowercase).

use serde::{Deserialize, Serialize};

pub const REALTIME_UPDATED: &str = "rt:updated";
pub const STATIC_UPDATED: &str = "static:updated";

pub fn vehicles_key(feed_id: &str) -> String {
    format!("rt:vehicles:{feed_id}")
}
pub fn trips_key(feed_id: &str) -> String {
    format!("rt:trips:{feed_id}")
}
pub fn alerts_key(feed_id: &str) -> String {
    format!("rt:alerts:{feed_id}")
}
pub fn seq_key(feed_id: &str, kind: &str) -> String {
    format!("rt:seq:{feed_id}:{kind}")
}
pub fn unmatched_key(feed_id: &str, kind: &str) -> String {
    format!("rt:unmatched:{feed_id}:{kind}")
}
pub fn meta_key(feed_id: &str, kind: &str) -> String {
    format!("rt:meta:{feed_id}:{kind}")
}

/// The static trip a realtime record resolved to (from the serving timetable version).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TripFields {
    pub key: String,
    pub feed_id: String,
    /// per-feed timetable version; stop sequences only compare within one
    pub version: i32,
    pub trip_id: String,
    /// ISO date
    pub service_date: String,
    pub route_id: Option<String>,
    pub route_short_name: Option<String>,
    pub route_type: Option<i32>,
    pub headsign: Option<String>,
    pub direction_id: Option<i32>,
    pub shape_id: Option<String>,
    pub block_id: Option<String>,
}

/// One stop of a trip update, in trip order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopRow {
    pub stop_sequence: i64,
    pub stop_id: String,
    pub public_stop_id: Option<String>,
    pub scheduled_arrival: Option<i64>,
    pub scheduled_departure: Option<i64>,
    pub predicted_arrival: Option<i64>,
    pub predicted_departure: Option<i64>,
    /// predicted - scheduled, always recomputed
    pub delay_s: Option<i64>,
    /// SCHEDULED, SKIPPED, NO_DATA
    pub schedule_relationship: String,
    /// no update for this stop: the delay of the last updated stop before it
    pub propagated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveTrip {
    #[serde(flatten)]
    pub fields: TripFields,
    /// no timetable has it (GTFS-RT NEW, ADDED, DUPLICATED): boards take it from realtime
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<bool>,
    pub vehicle_id: Option<String>,
    /// at the next stop, else the last known one
    pub delay_s: Option<i64>,
    /// when the feed measured it
    pub timestamp: i64,
    /// SCHEDULED, CANCELED, ...
    pub schedule_relationship: String,
    pub stops: Vec<StopRow>,
}

/// A trip as the API keeps it in memory and the change messages carry it: a JSON array (msgspec array_like).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TripState(
    pub i32,                                      // version
    pub bool,                                     // canceled (also when deleted)
    pub bool,                                     // deleted: not to be shown at all
    pub Option<String>,                           // vehicle_id
    pub Option<i64>,                              // timestamp
    pub Option<i64>,                              // delay_s
    pub Vec<StopState>,                           // stops
    pub Option<(Option<String>, Option<String>)>, // extra: (route_id, headsign) of a trip no timetable has
);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StopState(
    pub i64,            // stop_sequence
    pub Option<String>, // public_stop_id
    pub Option<i64>,    // predicted_arrival
    pub Option<i64>,    // predicted_departure
    pub Option<i64>,    // delay_s
    pub bool,           // skipped
    pub Option<i64>,    // scheduled_departure
    pub bool,           // propagated
    pub Option<i64>,    // scheduled_arrival
);

pub fn trip_state(trip: &LiveTrip) -> TripState {
    let deleted = trip.schedule_relationship == "DELETED";
    let stops = trip
        .stops
        .iter()
        .map(|r| {
            StopState(
                r.stop_sequence,
                r.public_stop_id.clone(),
                r.predicted_arrival,
                r.predicted_departure,
                r.delay_s,
                r.schedule_relationship == "SKIPPED",
                r.scheduled_departure,
                r.propagated,
                r.scheduled_arrival,
            )
        })
        .collect();
    let extra = (trip.extra == Some(true)).then(|| (trip.fields.route_id.clone(), trip.fields.headsign.clone()));
    TripState(
        trip.fields.version,
        deleted || trip.schedule_relationship == "CANCELED",
        deleted,
        trip.vehicle_id.clone(),
        Some(trip.timestamp),
        trip.delay_s,
        stops,
        extra,
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveVehicle {
    pub feed_id: String,
    pub vehicle_id: String,
    pub label: Option<String>,
    pub license_plate: Option<String>,
    pub lat: f64,
    pub lon: f64,
    /// filled from the previous fix when the feed leaves it out
    pub bearing: Option<f64>,
    /// m/s, filled likewise
    pub speed: Option<f64>,
    /// when the feed measured the position (without its own: the feed header's)
    pub timestamp: i64,
    /// INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO
    pub current_status: Option<String>,
    pub current_stop_sequence: Option<i64>,
    pub stop_id: Option<String>,
    pub public_stop_id: Option<String>,
    pub trip: TripFields,
    /// of its trip's last update
    pub delay_s: Option<i64>,
    pub occupancy: Option<String>,
    pub wheelchair_accessible: Option<String>,
}

/// Where an alert applies, in our ids. All fields empty: the whole feed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AlertEntity {
    pub agency_id: Option<String>,
    pub route_id: Option<String>,
    pub route_type: Option<i32>,
    pub direction_id: Option<i32>,
    pub trip_key: Option<String>,
    pub public_stop_ids: Vec<String>,
    /// a GTFS station: the group most of its stops are in
    pub stop_group_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivePeriod {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveAlert {
    pub feed_id: String,
    pub id: String,
    pub cause: Option<String>,
    pub effect: Option<String>,
    pub severity: Option<String>,
    pub header: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub active_periods: Vec<ActivePeriod>,
    pub entities: Vec<AlertEntity>,
}

/// rt:alerts:{feed_id}: a feed's alerts and the feed header time they are as of.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveAlerts {
    pub timestamp: i64,
    pub alerts: Vec<LiveAlert>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnmatchedVehicle {
    pub vehicle_id: String,
    pub trip_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Unmatched<'a> {
    pub trips: &'a [String],
    pub vehicles: &'a [UnmatchedVehicle],
    pub alert_entities: &'a [String],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trip_reads_back_and_its_state_is_an_array() {
        // as the Python worker wrote it (an extra trip without route)
        let text = r#"{"key":"kmk_a:20261025:t","feed_id":"kmk_a","version":3,"trip_id":"t","service_date":"2026-10-25",
            "route_id":null,"route_short_name":null,"route_type":null,"headsign":"X","direction_id":null,"shape_id":null,
            "block_id":null,"vehicle_id":"7","delay_s":null,"timestamp":100,"schedule_relationship":"NEW",
            "stops":[{"stop_sequence":1,"stop_id":"a","public_stop_id":"abcde","scheduled_arrival":null,
            "scheduled_departure":90,"predicted_arrival":null,"predicted_departure":90,"delay_s":null,
            "schedule_relationship":"SCHEDULED","propagated":false}],"extra":true}"#;
        let trip: LiveTrip = serde_json::from_str(text).unwrap();
        assert_eq!(trip.extra, Some(true));
        let state = serde_json::to_string(&trip_state(&trip)).unwrap();
        assert_eq!(state, r#"[3,false,false,"7",100,null,[[1,"abcde",null,90,null,false,90,false,null]],[null,"X"]]"#);
        let back: serde_json::Value = serde_json::from_str(&serde_json::to_string(&trip).unwrap()).unwrap();
        assert_eq!(back, serde_json::from_str::<serde_json::Value>(text).unwrap());
    }
}
