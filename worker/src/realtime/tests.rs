//! Realtime processing with a hand-built trip index (no database). Ported from the Python worker's tests, plus the
//! GTFS-RT rules the rewrite added (trip-level delay, current_status default, strict start_date, DIFFERENTIAL).

#![allow(deprecated)] // GTFS-RT ADDED, still tested

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::NaiveDate;
use chrono_tz::Tz;

use super::alerts::build_alert;
use super::index::{ServiceDay, StopTime, TripIndex, TripInfo, VersionData};
use super::layout::LiveTrip;
use super::pb::{
    self, trip_descriptor::ScheduleRelationship as TD, trip_update::stop_time_update::ScheduleRelationship as STU,
};
use super::process::{FeedState, Processed, StopTimes, process_message};
use super::resolve::{WINDOWS, Windows, resolve, resolve_instance};
use super::trips::{match_updates, merge_stops};
use crate::config::{ExtraKind, FeedConfig};
use crate::util::service_day_base;

fn waw() -> Tz {
    "Europe/Warsaw".parse().unwrap()
}
fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}
fn hms(h: i64, m: i64) -> i64 {
    h * 3600 + m * 60
}
fn d22() -> NaiveDate {
    date(2026, 10, 22)
}
fn d23() -> NaiveDate {
    date(2026, 10, 23)
}
fn epoch(d: NaiveDate, h: i64, m: i64) -> i64 {
    service_day_base(d, waw()) + hms(h, m)
}
fn day(d: NaiveDate, version: i32, services: &[&str]) -> ServiceDay {
    ServiceDay {
        date: d,
        version,
        base: service_day_base(d, waw()),
        services: Arc::new(services.iter().map(|s| s.to_string()).collect()),
    }
}
fn info(version: i32, trip_id: &str, service: &str, first: i64, last: i64, block: Option<&str>) -> TripInfo {
    TripInfo {
        version,
        trip_id: trip_id.into(),
        service_id: service.into(),
        route_id: "R1".into(),
        direction_id: Some(0),
        shape_id: Some("S1".into()),
        headsign: Some("Salwator".into()),
        block_id: block.map(str::to_string),
        route_short_name: Some("1".into()),
        route_type: Some(900),
        first_departure: first,
        last_arrival: last,
    }
}
fn st(seq: i64, stop: &str, arr: i64, dep: Option<i64>) -> StopTime {
    StopTime { sequence: seq, stop_id: stop.into(), arrival: Some(arr), departure: dep }
}

const TRIP: &str = "block_1_trip_9_service_4";

/// Old tram version 1 serves the 22nd, new version 2 from the 23rd; both reuse trip ids. v1's is a night trip
/// (23:50 -> 25:20, a loop visiting A twice), v2's a morning one.
fn make_index() -> (TripIndex, StopTimes) {
    let mut data = VersionData::default();
    data.add_trip(info(1, TRIP, "service_4", hms(23, 50), hms(25, 20), None));
    data.add_trip(info(2, TRIP, "service_4", hms(6, 0), hms(6, 40), None));
    for v in [1, 2] {
        for s in ["A", "B", "C"] {
            data.stop_links.insert((v, s.into()), format!("stop{}", s.to_lowercase()));
        }
        data.routes.insert((v, "R1".into()));
    }
    let mut index = TripIndex::empty(waw(), d23());
    index.days = vec![day(d22(), 1, &["service_4"]), day(d23(), 2, &["service_4"])];
    index.data = Arc::new(data);
    index.stations.insert((2, "STATION".into()), ("grp".into(), vec!["stopa".into(), "stopb".into()]));
    let mut stops = StopTimes::new();
    stops.insert(
        (1, TRIP.into()),
        Arc::new(vec![
            st(1, "A", hms(23, 50), Some(hms(23, 50))),
            st(2, "B", hms(24, 30), Some(hms(24, 30))),
            st(3, "A", hms(25, 0), Some(hms(25, 0))),
            st(4, "C", hms(25, 20), Some(hms(25, 20))),
        ]),
    );
    stops.insert(
        (2, TRIP.into()),
        Arc::new(vec![st(1, "A", hms(6, 0), Some(hms(6, 0))), st(2, "C", hms(6, 40), Some(hms(6, 40)))]),
    );
    (index, stops)
}

fn feed_config(extra: &[ExtraKind]) -> FeedConfig {
    let mut f: FeedConfig = serde_yaml::from_str("feed_id: kmk_t\nstatic: https://x.test/t.zip\n").unwrap();
    f.extra_trips = extra.to_vec();
    f
}

fn state(extra: &[ExtraKind]) -> FeedState {
    FeedState::new(&feed_config(extra), 120)
}

fn message(ts: i64, entities: Vec<pb::FeedEntity>) -> pb::FeedMessage {
    pb::FeedMessage {
        header: pb::FeedHeader {
            gtfs_realtime_version: "2.0".into(),
            timestamp: Some(ts as u64),
            ..Default::default()
        },
        entity: entities,
    }
}

fn descriptor(trip_id: &str, rel: TD) -> pb::TripDescriptor {
    pb::TripDescriptor { trip_id: Some(trip_id.into()), schedule_relationship: Some(rel as i32), ..Default::default() }
}

fn trip_update(id: &str, trip: pb::TripDescriptor, updates: Vec<pb::trip_update::StopTimeUpdate>) -> pb::FeedEntity {
    pb::FeedEntity {
        id: id.into(),
        trip_update: Some(pb::TripUpdate { trip, stop_time_update: updates, ..Default::default() }),
        ..Default::default()
    }
}

fn update(seq: Option<u32>, stop: Option<&str>) -> pb::trip_update::StopTimeUpdate {
    pb::trip_update::StopTimeUpdate { stop_sequence: seq, stop_id: stop.map(str::to_string), ..Default::default() }
}

fn at(time: i64) -> Option<pb::trip_update::StopTimeEvent> {
    Some(pb::trip_update::StopTimeEvent { time: Some(time), ..Default::default() })
}

fn late(delay: i32) -> Option<pb::trip_update::StopTimeEvent> {
    Some(pb::trip_update::StopTimeEvent { delay: Some(delay), ..Default::default() })
}

fn vehicle(id: &str, trip: Option<pb::TripDescriptor>, lat: f32, lon: f32) -> pb::FeedEntity {
    pb::FeedEntity {
        id: format!("v{id}"),
        vehicle: Some(pb::VehiclePosition {
            trip,
            vehicle: Some(pb::VehicleDescriptor { id: Some(id.into()), ..Default::default() }),
            position: Some(pb::Position { latitude: lat, longitude: lon, ..Default::default() }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn run(index: &TripIndex, stops: &StopTimes, st: &mut FeedState, now: i64, entities: Vec<pb::FeedEntity>) -> Processed {
    process_message("kmk_t", &message(now, entities), index, st, stops, now as f64)
}

fn wide() -> Windows {
    Windows { early_s: 2 * 86400, late_s: 2 * 86400 }
}

// ------------------------------------------------------------------------------------------------- resolving

#[test]
fn night_trip_of_previous_day_resolves_to_old_version() {
    let (idx, _) = make_index();
    let r = resolve(&idx, TRIP, epoch(d22(), 25, 10) as f64, &WINDOWS, None, false).unwrap(); // 01:10 on the 23rd
    assert_eq!((r.info.version, r.day.date), (1, d22()));
    let r = resolve(&idx, TRIP, epoch(d23(), 6, 10) as f64, &WINDOWS, None, false).unwrap();
    assert_eq!((r.info.version, r.day.date), (2, d23()));
}

#[test]
fn service_suffix_fallback_finds_previous_days_trip() {
    let (idx, _) = make_index();
    let now = epoch(d22(), 25, 10) as f64; // the feed already names the trip with the new day's service
    assert!(resolve(&idx, "block_1_trip_9_service_2", now, &WINDOWS, None, false).is_none());
    let r = resolve(&idx, "block_1_trip_9_service_2", now, &WINDOWS, None, true).unwrap();
    assert_eq!((r.info.version, r.day.date, r.info.trip_id.as_str()), (1, d22(), TRIP));
    assert_eq!(resolve(&idx, TRIP, now, &WINDOWS, None, true).unwrap().info.version, 1);
    assert!(resolve(&idx, "block_2_trip_9_service_2", now, &WINDOWS, None, true).is_none());
    assert!(resolve(&idx, "block_1_trip_9_service_2", epoch(d23(), 15, 0) as f64, &WINDOWS, None, true).is_none());
}

#[test]
fn trip_outside_any_window_does_not_resolve() {
    let (idx, _) = make_index();
    assert!(resolve(&idx, TRIP, epoch(d23(), 15, 0) as f64, &WINDOWS, None, false).is_none());
    assert!(resolve(&idx, "unknown", epoch(d23(), 6, 0) as f64, &WINDOWS, None, false).is_none());
}

fn two_day_index() -> TripIndex {
    let mut data = VersionData::default();
    data.add_trip(info(1, "daily", "s", hms(6, 0), hms(6, 40), None));
    let mut idx = TripIndex::empty(waw(), d23());
    idx.days = vec![day(d22(), 1, &["s"]), day(d23(), 1, &["s"])];
    idx.data = Arc::new(data);
    idx
}

#[test]
fn start_date_names_the_instance() {
    let idx = two_day_index();
    let now = epoch(d23(), 6, 10) as f64;
    assert_eq!(resolve(&idx, "daily", now, &wide(), None, false).unwrap().day.date, d23()); // closest in time
    assert_eq!(resolve(&idx, "daily", now, &wide(), Some(d22()), false).unwrap().day.date, d22());
    // GTFS-RT: start_date is "the start date of this trip instance": a day that doesn't run the trip is no match,
    // and the window still applies (yesterday's trip ended a day ago)
    assert!(resolve(&idx, "daily", now, &wide(), Some(date(2026, 10, 21)), false).is_none());
    assert!(resolve(&idx, "daily", now, &WINDOWS, Some(d22()), false).is_none());
}

#[test]
fn start_date_in_the_feed_is_used() {
    let idx = two_day_index();
    let now = epoch(d23(), 5, 30);
    let mut s = state(&[]);
    s.windows = wide();
    let entities = ["20261022", ""]
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let mut t = descriptor("daily", TD::Scheduled);
            t.start_date = Some(d.to_string());
            trip_update(&format!("t{i}"), t, vec![])
        })
        .collect();
    let res = run(&idx, &StopTimes::new(), &mut s, now, entities);
    let dates: Vec<&str> = res.trips.iter().map(|t| t.fields.service_date.as_str()).collect();
    assert_eq!(dates, ["2026-10-22", "2026-10-23"]);
}

// ------------------------------------------------------------------------------------------------- trip updates

#[test]
fn stop_updates_without_sequence_follow_the_loop_forward() {
    let (_, stops) = make_index();
    let s = &stops[&(1, TRIP.to_string())];
    let updates: Vec<_> = ["B", "A", "C"].iter().map(|id| update(None, Some(id))).collect();
    let matched: Vec<i64> = match_updates(&updates, s, None).iter().map(|(_, st)| st.unwrap().sequence).collect();
    assert_eq!(matched, [2, 3, 4]); // the second visit of A, not the first
}

#[test]
fn loop_stop_listed_first_picks_the_visit_closest_in_time() {
    let (idx, stops) = make_index();
    let s = &stops[&(1, TRIP.to_string())];
    let mut u = update(None, Some("A"));
    u.arrival = at(epoch(d22(), 25, 2));
    let base = idx.days[0].base;
    assert_eq!(match_updates(std::slice::from_ref(&u), s, Some(base))[0].1.unwrap().sequence, 3);
    assert_eq!(match_updates(std::slice::from_ref(&u), s, None)[0].1.unwrap().sequence, 1); // no time info: next visit
}

#[test]
fn tram_style_absolute_times_give_computed_delays_and_unmatched_are_dropped() {
    let (idx, stops) = make_index();
    let now = epoch(d22(), 24, 35);
    let mut a = update(None, Some("A"));
    a.arrival = at(epoch(d22(), 25, 2)); // 2 min late at the second visit of A
    let mut c = update(None, Some("C"));
    c.arrival = at(epoch(d22(), 25, 21));
    let mut tu = trip_update("t1", descriptor(TRIP, TD::Scheduled), vec![a, c]);
    tu.trip_update.as_mut().unwrap().vehicle =
        Some(pb::VehicleDescriptor { id: Some("302".into()), ..Default::default() });
    let mut mismatched = descriptor(TRIP, TD::Scheduled);
    mismatched.route_id = Some("SOMETHING_ELSE".into());
    let mut v = vehicle("302", Some(mismatched), 50.06, 19.96);
    v.vehicle.as_mut().unwrap().stop_id = Some("A".into());
    let ghost = vehicle("999", Some(descriptor("not_in_static", TD::Scheduled)), 50.0, 19.9);
    let idle = vehicle("idle", None, 50.0, 19.9);
    let res = run(&idx, &stops, &mut state(&[]), now, vec![tu, v, ghost, idle]);
    assert_eq!(res.trips.iter().map(|t| t.delay_s).collect::<Vec<_>>(), [Some(120)]);
    let t = &res.trips[0];
    assert_eq!(t.stops.iter().map(|s| s.stop_sequence).collect::<Vec<_>>(), [3, 4]);
    assert_eq!(t.stops[0].public_stop_id.as_deref(), Some("stopa"));
    assert_eq!((t.fields.service_date.as_str(), t.fields.version), ("2026-10-22", 1));
    assert_eq!(t.fields.key, format!("kmk_t:20261022:{TRIP}"));
    assert_eq!(res.vehicles.iter().map(|v| v.vehicle_id.as_str()).collect::<Vec<_>>(), ["302"]);
    let v = &res.vehicles[0];
    assert_eq!(v.trip.route_id.as_deref(), Some("R1")); // trip_id is the source of truth
    assert_eq!(v.delay_s, Some(120));
    assert_eq!(res.mismatches, 1);
    let unmatched: HashSet<&str> = res.unmatched_vehicles.iter().map(|u| u.vehicle_id.as_str()).collect();
    assert_eq!(unmatched, HashSet::from(["999", "idle"]));
}

#[test]
fn feed_delay_is_recomputed_not_trusted() {
    let (idx, stops) = make_index();
    let mut u = update(Some(2), Some("C"));
    u.departure = Some(pb::trip_update::StopTimeEvent {
        time: Some(epoch(d23(), 6, 43)),
        delay: Some(9999),
        ..Default::default()
    });
    let res = run(
        &idx,
        &stops,
        &mut state(&[]),
        epoch(d23(), 6, 5),
        vec![trip_update("t1", descriptor(TRIP, TD::Scheduled), vec![u])],
    );
    assert_eq!(res.trips[0].delay_s, Some(180));
}

fn propagated(updates: Vec<(u32, Option<i32>, STU)>, trip_delay: Option<i32>) -> LiveTrip {
    let (idx, stops) = make_index();
    let updates = updates
        .into_iter()
        .map(|(seq, delay, rel)| {
            let mut u = update(Some(seq), None);
            u.schedule_relationship = Some(rel as i32);
            if let Some(d) = delay {
                u.arrival = late(d);
            }
            u
        })
        .collect();
    let mut e = trip_update("t1", descriptor(TRIP, TD::Scheduled), updates);
    e.trip_update.as_mut().unwrap().delay = trip_delay;
    run(&idx, &stops, &mut state(&[]), epoch(d22(), 24, 20), vec![e]).trips.remove(0)
}

fn rows(t: &LiveTrip) -> Vec<(i64, Option<i64>, bool)> {
    t.stops.iter().map(|r| (r.stop_sequence, r.delay_s, r.propagated)).collect()
}

#[test]
fn one_update_carries_its_delay_to_the_rest_of_the_trip() {
    let t = propagated(vec![(2, Some(120), STU::Scheduled)], None); // MLD style: only the next stop, arrival only
    assert_eq!(rows(&t), [(2, Some(120), false), (3, Some(120), true), (4, Some(120), true)]);
    let last = t.stops.last().unwrap();
    assert_eq!(
        (last.predicted_arrival, last.predicted_departure),
        (Some(epoch(d22(), 25, 22)), Some(epoch(d22(), 25, 22)))
    );
    assert_eq!(t.delay_s, Some(120));
}

#[test]
fn gaps_take_the_delay_of_the_update_before_them() {
    let t = propagated(vec![(1, Some(60), STU::Scheduled), (3, Some(180), STU::Scheduled)], None);
    assert_eq!(rows(&t), [(1, Some(60), false), (2, Some(60), true), (3, Some(180), false), (4, Some(180), true)]);
}

#[test]
fn no_data_stops_the_propagation_and_skipped_passes_it_on() {
    let t = propagated(vec![(1, Some(60), STU::Scheduled), (2, None, STU::Skipped)], None);
    assert_eq!(rows(&t), [(1, Some(60), false), (2, None, false), (3, Some(60), true), (4, Some(60), true)]);
    let t = propagated(vec![(1, Some(60), STU::Scheduled), (3, None, STU::NoData)], None);
    assert_eq!(rows(&t), [(1, Some(60), false), (2, Some(60), true), (3, None, false)]);
}

#[test]
fn a_trip_level_delay_applies_until_the_first_stop_update() {
    // GTFS-RT TripUpdate.delay: stop time updates take precedence; the trip delay propagates up to the first of them
    let t = propagated(vec![], Some(90));
    assert_eq!(rows(&t), [(1, Some(90), true), (2, Some(90), true), (3, Some(90), true), (4, Some(90), true)]);
    let t = propagated(vec![(3, Some(300), STU::Scheduled)], Some(90));
    assert_eq!(rows(&t), [(1, Some(90), true), (2, Some(90), true), (3, Some(300), false), (4, Some(300), true)]);
}

fn rt_trip(version: i32, stops: &[(i64, &str, i64)]) -> LiveTrip {
    let text = serde_json::json!({
        "key": "kmk_a:20261025:x", "feed_id": "kmk_a", "version": version, "trip_id": "x", "service_date": "2026-10-25",
        "route_id": "R", "route_short_name": null, "route_type": null, "headsign": null, "direction_id": null,
        "shape_id": null, "block_id": null, "vehicle_id": null, "delay_s": null, "timestamp": 0,
        "schedule_relationship": "SCHEDULED",
        "stops": stops.iter().map(|(q, s, t)| serde_json::json!({
            "stop_sequence": q, "stop_id": s, "public_stop_id": null, "scheduled_arrival": null,
            "scheduled_departure": null, "predicted_arrival": null, "predicted_departure": t, "delay_s": null,
            "schedule_relationship": "SCHEDULED", "propagated": false})).collect::<Vec<_>>(),
    });
    serde_json::from_value(text).unwrap()
}

fn seqs(t: &LiveTrip) -> Vec<i64> {
    t.stops.iter().map(|r| r.stop_sequence).collect()
}

#[test]
fn dropped_stops_are_kept_with_their_last_known_times() {
    let first = merge_stops(None, rt_trip(1, &[(1, "a", 100), (2, "b", 200), (3, "c", 300)]));
    let second = merge_stops(Some(&first), rt_trip(1, &[(3, "c", 310), (4, "d", 410), (5, "e", 510)]));
    assert_eq!(seqs(&second), [1, 2, 3, 4, 5]);
    let times: Vec<Option<i64>> = second.stops.iter().map(|r| r.predicted_departure).collect();
    assert_eq!(times, [Some(100), Some(200), Some(310), Some(410), Some(510)]);
    let third = merge_stops(Some(&second), rt_trip(1, &[(5, "e", 520)]));
    assert_eq!(seqs(&third), [1, 2, 3, 4, 5]);
    assert_eq!(third.stops[2].predicted_departure, Some(310));
    // a version change drops them; an empty update keeps them all
    assert_eq!(seqs(&merge_stops(Some(&first), rt_trip(2, &[(2, "b", 210)]))), [2]);
    assert_eq!(seqs(&merge_stops(Some(&first), rt_trip(1, &[]))), [1, 2, 3]);
}

// ------------------------------------------------------------------------------------------------- vehicles

#[test]
fn feed_zeros_count_as_unknown_and_motion_fills_them() {
    let (idx, stops) = make_index();
    let mut s = state(&[]);
    let now = epoch(d23(), 6, 5);
    let mut poll = |lon: f32, dt: i64| {
        let mut v = vehicle("302", Some(descriptor(TRIP, TD::Scheduled)), 50.06, lon);
        let vp = v.vehicle.as_mut().unwrap();
        vp.timestamp = Some((now + dt) as u64);
        let pos = vp.position.as_mut().unwrap();
        (pos.speed, pos.bearing) = (Some(0.0), Some(0.0)); // ZTP's placeholders, also for driving buses
        let res = run(&idx, &stops, &mut s, now + dt, vec![v]);
        (res.vehicles[0].bearing, res.vehicles[0].speed)
    };
    let east = 19.96 + 0.0014; // ~100 m east
    assert_eq!(poll(19.96, 0), (None, None));
    assert_eq!(poll(19.96, 10), (None, Some(0.0))); // standing, heading still unknown
    let (b, s2) = poll(east, 20); // driving east, not north at 0 km/h
    assert!((b.unwrap() - 90.0).abs() < 0.2 && (s2.unwrap() - 10.0).abs() < 0.2, "{b:?} {s2:?}");
    assert_eq!(poll(east, 30).1, Some(0.0)); // stopped: keeps its last heading
}

#[test]
fn a_sequence_without_status_is_in_transit_to_it() {
    // GTFS-RT VehiclePosition.current_status: "If current_status is missing IN_TRANSIT_TO is assumed"
    let (idx, stops) = make_index();
    let mut v = vehicle("302", Some(descriptor(TRIP, TD::Scheduled)), 50.06, 19.96);
    v.vehicle.as_mut().unwrap().current_stop_sequence = Some(2);
    let mut plain = vehicle("303", Some(descriptor(TRIP, TD::Scheduled)), 50.06, 19.96);
    plain.vehicle.as_mut().unwrap().current_status = Some(pb::vehicle_position::VehicleStopStatus::StoppedAt as i32);
    let res = run(&idx, &stops, &mut state(&[]), epoch(d23(), 6, 5), vec![v, plain]);
    let status: Vec<(Option<i64>, Option<&str>)> =
        res.vehicles.iter().map(|v| (v.current_stop_sequence, v.current_status.as_deref())).collect();
    assert_eq!(status, [(Some(2), Some("IN_TRANSIT_TO")), (None, Some("STOPPED_AT"))]);
}

#[test]
fn stale_positions_are_dropped() {
    let (idx, stops) = make_index();
    let now = epoch(d23(), 6, 5);
    let mut v = vehicle("302", Some(descriptor(TRIP, TD::Scheduled)), 50.06, 19.96);
    v.vehicle.as_mut().unwrap().timestamp = Some((now - 290) as u64);
    let res = run(&idx, &stops, &mut state(&[]), now, vec![v]);
    assert!(res.vehicles.is_empty());
    assert_eq!(res.stale_vehicles, 1);
}

#[test]
fn deleted_trips_and_their_vehicles_are_not_shown() {
    let (idx, stops) = make_index();
    let now = epoch(d23(), 6, 5);
    let mut s = state(&[]);
    let res = run(&idx, &stops, &mut s, now, vec![trip_update("t", descriptor(TRIP, TD::Deleted), vec![])]);
    assert_eq!(res.trips[0].schedule_relationship, "DELETED");
    // its vehicle comes in another file and doesn't say so
    let res = run(&idx, &stops, &mut s, now, vec![vehicle("302", Some(descriptor(TRIP, TD::Scheduled)), 50.0, 19.9)]);
    assert!(res.vehicles.is_empty() && res.unmatched_vehicles.is_empty());
}

// ------------------------------------------------------------------------------------------------- extra trips

fn extra_index() -> (TripIndex, StopTimes, NaiveDate, i64) {
    let d = date(2026, 11, 26);
    let base = service_day_base(d, waw());
    let mut data = VersionData::default();
    data.add_trip(TripInfo { route_type: Some(3), ..info(1, "T1", "wd", hms(6, 0), hms(6, 40), None) });
    data.routes.insert((1, "R1".into()));
    for s in ["A", "B", "C"] {
        data.stop_links.insert((1, s.into()), format!("stop{}", s.to_lowercase()));
    }
    let mut idx = TripIndex::empty(waw(), d);
    idx.days = vec![ServiceDay { date: d, version: 1, base, services: Arc::new(HashSet::from(["wd".to_string()])) }];
    idx.data = Arc::new(data);
    let mut stops = StopTimes::new();
    stops.insert(
        (1, "T1".into()),
        Arc::new(vec![
            st(1, "A", hms(6, 0), Some(hms(6, 0))),
            st(2, "B", hms(6, 20), Some(hms(6, 20))),
            st(3, "C", hms(6, 40), None),
        ]),
    );
    (idx, stops, d, base)
}

fn new_trip(rel: TD, base: i64) -> pb::FeedEntity {
    let mut t = descriptor("extra_1", rel);
    t.route_id = Some("R1".into());
    let updates = [("A", 30), ("B", 45), ("C", 55)]
        .iter()
        .map(|(s, m)| {
            let mut u = update(None, Some(s));
            (u.arrival, u.departure) = (at(base + hms(7, *m)), at(base + hms(7, *m)));
            u
        })
        .collect();
    let mut e = trip_update("n", t, updates);
    e.trip_update.as_mut().unwrap().trip_properties =
        Some(pb::trip_update::TripProperties { trip_headsign: Some("Nowy Bieżanów".into()), ..Default::default() });
    e
}

fn run_a(idx: &TripIndex, stops: &StopTimes, st: &mut FeedState, now: i64, entities: Vec<pb::FeedEntity>) -> Processed {
    process_message("kmk_a", &message(now, entities), idx, st, stops, now as f64)
}

#[test]
fn a_new_trip_comes_from_its_update_and_its_vehicle_is_matched() {
    let (idx, stops, _, base) = extra_index();
    let mut s = state(&[ExtraKind::New]);
    let now = base + hms(7, 20);
    let res = run_a(&idx, &stops, &mut s, now, vec![new_trip(TD::New, base)]);
    let t = &res.trips[0];
    assert_eq!(
        (t.fields.key.as_str(), t.extra, t.schedule_relationship.as_str()),
        ("kmk_a:20261126:extra_1", Some(true), "NEW")
    );
    assert_eq!(
        (t.fields.route_id.as_deref(), t.fields.headsign.as_deref(), t.fields.version),
        (Some("R1"), Some("Nowy Bieżanów"), 1)
    );
    let pids: Vec<Option<&str>> = t.stops.iter().map(|s| s.public_stop_id.as_deref()).collect();
    assert_eq!(pids, [Some("stopa"), Some("stopb"), Some("stopc")]);
    assert_eq!(t.stops[0].scheduled_departure, Some(base + hms(7, 30)));
    assert_eq!(t.stops[0].predicted_departure, Some(base + hms(7, 30)));
    assert_eq!(t.stops.last().unwrap().scheduled_departure, None); // its last stop: not a departure
    assert!(t.stops.iter().all(|s| s.delay_s.is_none()));
    // its vehicle, in the next file: it doesn't say its trip is extra, its id is just in no timetable
    let res =
        run_a(&idx, &stops, &mut s, now, vec![vehicle("v9", Some(descriptor("extra_1", TD::Scheduled)), 50.06, 19.94)]);
    assert_eq!(
        res.vehicles.iter().map(|v| (v.vehicle_id.as_str(), v.trip.key.as_str())).collect::<Vec<_>>(),
        [("v9", "kmk_a:20261126:extra_1")]
    );
    assert!(res.unmatched_vehicles.is_empty());
}

#[test]
fn extra_trips_only_where_the_feed_shows_them() {
    let (idx, stops, _, base) = extra_index();
    let now = base + hms(7, 20);
    let res = run_a(&idx, &stops, &mut state(&[]), now, vec![new_trip(TD::New, base)]);
    assert!(res.trips.is_empty());
    assert_eq!(res.unmatched_trips, ["extra_1 (NEW)"]);
    let res = run_a(&idx, &stops, &mut state(&[ExtraKind::New]), now, vec![new_trip(TD::Added, base)]); // opt-in
    assert_eq!(res.unmatched_trips, ["extra_1 (ADDED)"]);
    let res =
        run_a(&idx, &stops, &mut state(&[ExtraKind::New, ExtraKind::Added]), now, vec![new_trip(TD::Added, base)]);
    assert_eq!(res.trips[0].schedule_relationship, "ADDED");
}

fn duplicate(start_time: &str, copy_id: &str, base: i64, with_update: bool) -> pb::FeedEntity {
    let mut u = update(Some(2), None);
    u.departure = at(base + hms(9, 23)); // 3 min late at B (the copy's 09:20)
    let mut e = trip_update("d", descriptor("T1", TD::Duplicated), if with_update { vec![u] } else { vec![] });
    e.trip_update.as_mut().unwrap().trip_properties = Some(pb::trip_update::TripProperties {
        start_time: Some(start_time.into()),
        start_date: Some("20261126".into()),
        trip_id: (!copy_id.is_empty()).then(|| copy_id.into()),
        ..Default::default()
    });
    e
}

#[test]
fn a_duplicated_trip_is_the_original_shifted_under_its_own_key() {
    let (idx, stops, _, base) = extra_index();
    let mut s = state(&[ExtraKind::Duplicated]);
    let now = base + hms(6, 10);
    let mut o = update(Some(2), None);
    o.departure = at(base + hms(6, 21));
    let original = trip_update("o", descriptor("T1", TD::Scheduled), vec![o]);
    let res = run_a(&idx, &stops, &mut s, now, vec![original, duplicate("09:00:00", "", base, true)]);
    let trips: HashMap<&str, &LiveTrip> = res.trips.iter().map(|t| (t.fields.key.as_str(), t)).collect();
    let copy = trips["kmk_a:20261126:T1#20261126T09:00:00"];
    assert_eq!(
        (copy.extra, copy.schedule_relationship.as_str(), copy.fields.route_id.as_deref()),
        (Some(true), "DUPLICATED", Some("R1"))
    );
    let by_seq: HashMap<i64, &super::layout::StopRow> = copy.stops.iter().map(|r| (r.stop_sequence, r)).collect();
    assert_eq!(seqs(copy), [1, 2, 3]); // every stop: no timetable has the copy
    assert_eq!((by_seq[&1].scheduled_departure, by_seq[&1].predicted_departure), (Some(base + hms(9, 0)), None));
    assert_eq!((by_seq[&2].scheduled_departure, by_seq[&2].delay_s), (Some(base + hms(9, 20)), Some(180)));
    assert_eq!(by_seq[&3].scheduled_arrival, Some(base + hms(9, 40)));
    assert_eq!(by_seq[&3].scheduled_departure, None);
    let orig = trips["kmk_a:20261126:T1"];
    assert_eq!((orig.stops[0].delay_s, orig.extra), (Some(60), None)); // the original keeps its own realtime
    // the copy's vehicle names the original and its start time
    let mut t = descriptor("T1", TD::Duplicated);
    t.start_time = Some("09:00:00".into());
    let res = run_a(&idx, &stops, &mut s, now, vec![vehicle("v7", Some(t), 50.06, 19.94)]);
    let v = &res.vehicles[0];
    assert_eq!((v.trip.key.as_str(), v.delay_s, v.timestamp), ("kmk_a:20261126:T1#20261126T09:00:00", Some(180), now));
}

#[test]
fn a_duplicated_trip_without_stop_updates_runs_as_its_timetable_and_needs_a_start_time() {
    let (idx, stops, _, base) = extra_index();
    let now = base + hms(6, 10);
    let res =
        run_a(&idx, &stops, &mut state(&[ExtraKind::Duplicated]), now, vec![duplicate("09:00:00", "", base, false)]);
    let got: Vec<(i64, Option<i64>, Option<i64>)> =
        res.trips[0].stops.iter().map(|s| (s.stop_sequence, s.scheduled_arrival, s.predicted_arrival)).collect();
    assert_eq!(
        got,
        [(1, Some(base + hms(9, 0)), None), (2, Some(base + hms(9, 20)), None), (3, Some(base + hms(9, 40)), None)]
    );
    let res = run_a(
        &idx,
        &stops,
        &mut state(&[ExtraKind::Duplicated]),
        now,
        vec![duplicate("09:00:00", "T1-extra", base, true)],
    );
    assert_eq!(res.trips[0].fields.key, "kmk_a:20261126:T1-extra");
    let res = run_a(&idx, &stops, &mut state(&[ExtraKind::Duplicated]), now, vec![duplicate("", "", base, true)]);
    assert!(res.trips.is_empty());
    assert_eq!(res.unmatched_trips, ["T1 (DUPLICATED)"]);
}

// ------------------------------------------------------------------------------------------------- blocks

#[test]
fn late_trips_running_into_the_next_trip_of_their_block_are_counted() {
    let d = date(2026, 11, 26);
    let base = service_day_base(d, waw());
    let mut data = VersionData::default();
    for (id, svc, first, last, block) in [
        ("T2", "wd", hms(6, 45), hms(7, 30), "B1"),
        ("T1", "wd", hms(6, 0), hms(6, 40), "B1"),
        ("W2", "we", hms(6, 42), hms(7, 20), "B1"),
        ("X1", "wd", hms(6, 41), hms(7, 0), "B2"),
    ] {
        data.add_trip(info(1, id, svc, first, last, Some(block)));
    }
    let mut idx = TripIndex::empty(waw(), d);
    idx.days = vec![ServiceDay { date: d, version: 1, base, services: Arc::new(HashSet::from(["wd".to_string()])) }];
    idx.data = Arc::new(data);
    let mut stops = StopTimes::new();
    for (id, first, last) in [("T1", hms(6, 0), hms(6, 40)), ("T2", hms(6, 45), hms(7, 30))] {
        stops.insert(
            (1, id.into()),
            Arc::new(vec![
                st(1, "A", first, Some(first)),
                st(2, "B", (first + last) / 2, Some((first + last) / 2)),
                st(3, "C", last, None),
            ]),
        );
    }
    let tu = |id: &str, seq: u32, arrival: i64| {
        let mut u = update(Some(seq), None);
        u.arrival = at(arrival);
        trip_update(id, descriptor(id, TD::Scheduled), vec![u])
    };
    let now = base + hms(6, 20);
    // T1 reaches its last stop at 06:50, after T2 is due at 06:45; T2 has no update of its own
    let res = run_a(&idx, &stops, &mut state(&[]), now, vec![tu("T1", 2, base + hms(6, 30))]);
    assert_eq!(res.trips[0].stops.last().unwrap().predicted_arrival, Some(base + hms(6, 50)));
    assert_eq!((res.block_late_next, res.block_late_next_without_update), (1, 1));
    let res =
        run_a(&idx, &stops, &mut state(&[]), now, vec![tu("T1", 3, base + hms(6, 50)), tu("T2", 1, base + hms(6, 52))]);
    assert_eq!((res.block_late_next, res.block_late_next_without_update), (1, 0));
    let res = run_a(&idx, &stops, &mut state(&[]), now, vec![tu("T1", 3, base + hms(6, 44))]); // absorbed by the layover
    assert_eq!(res.block_late_next, 0);
    assert_eq!(res.trip_relationships.get("SCHEDULED"), Some(&1));
}

// ------------------------------------------------------------------------------------------------- alerts

#[test]
fn alert_trip_is_one_instance() {
    let (idx, _) = make_index();
    let at = |d, h| epoch(d, h, 0) as f64;
    // the night trip of the 22nd is over at 05:00 on the 23rd: the morning one is meant
    assert_eq!(resolve_instance(&idx, TRIP, at(d23(), 5), None, false).unwrap().day.date, d23());
    assert_eq!(resolve_instance(&idx, TRIP, at(d22(), 24), None, false).unwrap().day.date, d22()); // still running
    assert_eq!(resolve_instance(&idx, TRIP, at(d23(), 12), None, false).unwrap().day.date, d23()); // all over: the last
    assert_eq!(resolve_instance(&idx, TRIP, at(d23(), 5), Some(d22()), false).unwrap().day.date, d22());
    assert!(resolve_instance(&idx, "block_1_trip_9_service_2", at(d23(), 5), None, false).is_none());
    assert_eq!(resolve_instance(&idx, "block_1_trip_9_service_2", at(d23(), 5), None, true).unwrap().day.date, d23());
}

fn text(s: &str, lang: &str) -> Option<pb::TranslatedString> {
    Some(pb::TranslatedString {
        translation: vec![pb::translated_string::Translation { text: s.into(), language: Some(lang.into()) }],
    })
}

#[test]
fn alert_entities_are_resolved_to_ours_and_unknown_ones_dropped() {
    let (idx, _) = make_index();
    let sel = |route: Option<&str>, stop: Option<&str>, dir: Option<u32>, trip: Option<&str>| pb::EntitySelector {
        route_id: route.map(str::to_string),
        stop_id: stop.map(str::to_string),
        direction_id: dir,
        trip: trip.map(|t| pb::TripDescriptor { trip_id: Some(t.into()), ..Default::default() }),
        ..Default::default()
    };
    let alert = pb::Alert {
        effect: Some(pb::alert::Effect::Detour as i32),
        severity_level: Some(pb::alert::SeverityLevel::Warning as i32),
        header_text: text("Utrudnienia", "pl"),
        description_text: text("Linia 1\r\nobjazd \n", "pl"),
        informed_entity: vec![
            sel(Some("R1"), Some("A"), None, None), // line 1 at stop A
            sel(None, Some("STATION"), None, None),
            sel(Some("R1"), None, Some(1), None),
            sel(None, None, None, Some(TRIP)),
            sel(Some("R9"), None, None, None),
            sel(Some("R1"), Some("Z"), None, None), // AND: one unknown field and the entity means nothing
        ],
        ..Default::default()
    };
    let (out, unmatched) = build_alert("kmk_t", "a1", &alert, &idx, epoch(d23(), 5, 0) as f64, false);
    assert_eq!(
        (out.cause.as_deref(), out.effect.as_deref(), out.severity.as_deref()),
        (None, Some("detour"), Some("warning"))
    );
    assert_eq!(out.description.as_deref(), Some("Linia 1\nobjazd"));
    let e = &out.entities;
    assert_eq!((e[0].route_id.as_deref(), e[0].public_stop_ids.clone()), (Some("R1"), vec!["stopa".to_string()]));
    assert_eq!((e[1].stop_group_id.as_deref(), e[1].public_stop_ids.len()), (Some("grp"), 2));
    assert_eq!((e[2].route_id.as_deref(), e[2].direction_id), (Some("R1"), Some(1)));
    assert_eq!(e[3].trip_key.as_deref(), Some("kmk_t:20261023:block_1_trip_9_service_4"));
    assert_eq!(e.len(), 4);
    assert_eq!(unmatched, ["a1: route R9", "a1: stop Z"]);
    // no informed entity: the whole feed
    let plain = pb::Alert { header_text: text("Komunikat", ""), ..Default::default() };
    let (out, unmatched) = build_alert("kmk_t", "a2", &plain, &idx, 0.0, false);
    assert_eq!(out.entities, [super::layout::AlertEntity::default()]);
    assert!(unmatched.is_empty() && out.severity.is_none());
    assert_eq!(out.header.as_deref(), Some("Komunikat"));
}

#[test]
fn feeds_without_a_header_or_differential_ones_are_refused() {
    use super::poller::parse_feed;
    use prost::Message;
    assert!(parse_feed(b"").is_err()); // an empty file is not an empty feed
    let mut m = message(1, vec![]);
    assert!(parse_feed(&m.encode_to_vec()).is_ok());
    m.header.incrementality = Some(pb::feed_header::Incrementality::Differential as i32);
    assert!(parse_feed(&m.encode_to_vec()).is_err());
}
