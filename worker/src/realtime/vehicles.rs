//! VehiclePositions -> map vehicles (LiveVehicle), with bearing and speed filled in where the feed leaves them out.
//!
//! ZTP's trams never send speed, MPK buses send speed 0 for most vehicles (driving ones too), and buses drop bearing
//! now and then or send 0: those zeros count as missing. Values the feed sends are kept; gaps are filled from the
//! vehicle's previous fix: speed = distance / time, bearing = the direction moved. Under MIN_MOVE_M the move is GPS
//! noise: the last bearing stays and speed is 0.

use std::collections::HashMap;

use super::index::TripIndex;
use super::layout::{LiveVehicle, TripFields};
use super::pb;

const EARTH_RADIUS_M: f64 = 6_371_000.0;
const MIN_MOVE_M: f64 = 10.0;
/// Older fixes say too little about the current motion (km_kolej updates every 5 min).
const MAX_GAP_S: i64 = 6 * 60;
/// 180 km/h; faster means a GPS jump (KM trains run up to 160 km/h).
const MAX_SPEED_MS: f64 = 50.0;
pub const FORGET_S: i64 = 600;

/// Python's round(x, 1): correctly rounded to one decimal.
pub fn round1(x: f64) -> f64 {
    format!("{x:.1}").parse().unwrap_or(x)
}

#[derive(Debug, Clone, Copy)]
struct Fix {
    lat: f64,
    lon: f64,
    timestamp: i64,
    bearing: Option<f64>,
    speed: Option<f64>,
}

fn distance_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let (dp, dl) = (p2 - p1, (lon2 - lon1).to_radians());
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

fn bearing_deg(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dl = (lon2 - lon1).to_radians();
    let x = dl.sin() * p2.cos();
    let y = p1.cos() * p2.sin() - p1.sin() * p2.cos() * dl.cos();
    round1(x.atan2(y).to_degrees().rem_euclid(360.0))
}

/// (bearing, speed) from the previous fix; None where it cannot be told.
fn estimate(prev: Option<&Fix>, lat: f64, lon: f64, timestamp: i64) -> (Option<f64>, Option<f64>) {
    let Some(prev) = prev else {
        return (None, None);
    };
    if timestamp == prev.timestamp {
        return (prev.bearing, prev.speed); // the feed repeated the same fix
    }
    let dt = timestamp - prev.timestamp;
    if !(0..=MAX_GAP_S).contains(&dt) {
        return (if dt > 0 { prev.bearing } else { None }, None);
    }
    let d = distance_m(prev.lat, prev.lon, lat, lon);
    if d < MIN_MOVE_M {
        return (prev.bearing, Some(0.0));
    }
    let speed = d / dt as f64;
    if speed > MAX_SPEED_MS {
        return (None, None);
    }
    (Some(bearing_deg(prev.lat, prev.lon, lat, lon)), Some(round1(speed)))
}

/// The last fix per vehicle of one feed.
#[derive(Debug, Default)]
pub struct MotionTracker {
    fixes: HashMap<String, Fix>,
}

impl MotionTracker {
    /// Feed values where present, estimates otherwise; remembers the result for the next fix.
    pub fn fill(
        &mut self,
        vehicle_id: &str,
        lat: f64,
        lon: f64,
        timestamp: i64,
        bearing: Option<f64>,
        speed: Option<f64>,
    ) -> (Option<f64>, Option<f64>) {
        let (mut bearing, mut speed) = (bearing, speed);
        if bearing.is_none() || speed.is_none() {
            let (b, s) = estimate(self.fixes.get(vehicle_id), lat, lon, timestamp);
            bearing = bearing.or(b);
            speed = speed.or(s);
        }
        self.fixes.insert(vehicle_id.to_string(), Fix { lat, lon, timestamp, bearing, speed });
        (bearing, speed)
    }

    pub fn forget_before(&mut self, cutoff: i64) {
        self.fixes.retain(|_, f| f.timestamp >= cutoff);
    }
}

fn status_name(vp: &pb::VehiclePosition) -> Option<String> {
    use pb::vehicle_position::VehicleStopStatus;
    match vp.current_status {
        Some(v) => {
            Some(VehicleStopStatus::try_from(v).map(|s| s.as_str_name().to_string()).unwrap_or_else(|_| v.to_string()))
        }
        // GTFS-RT: "If current_status is missing IN_TRANSIT_TO is assumed" (it only means something with a sequence)
        None if vp.current_stop_sequence.is_some() => Some("IN_TRANSIT_TO".into()),
        None => None,
    }
}

#[allow(clippy::too_many_arguments)]
/// A vehicle on a trip given by its fields (also an extra trip, which has no timetabled one).
pub fn vehicle_for(
    feed_id: &str,
    trip: TripFields,
    version: i32,
    vp: &pb::VehiclePosition,
    index: &TripIndex,
    delay: Option<i64>,
    motion: &mut MotionTracker,
    ts: i64,
) -> LiveVehicle {
    let pos = vp.position.as_ref().expect("a vehicle with a position");
    let descriptor = vp.vehicle.clone().unwrap_or_default();
    // ZTP sends 0 for unknown: treated as missing (a standing vehicle is estimated as speed 0, last bearing)
    let bearing = pos.bearing.filter(|b| *b != 0.0).map(f64::from);
    let speed = pos.speed.filter(|s| *s > 0.0).map(f64::from);
    let timestamp = vp.timestamp.filter(|t| *t != 0).map_or(ts, |t| t as i64);
    let (lat, lon) = (f64::from(pos.latitude), f64::from(pos.longitude));
    let (bearing, speed) = if descriptor.id().is_empty() {
        (bearing, speed)
    } else {
        motion.fill(descriptor.id(), lat, lon, timestamp, bearing, speed)
    };
    let opt = |s: &str| (!s.is_empty()).then(|| s.to_string());
    LiveVehicle {
        feed_id: feed_id.into(),
        vehicle_id: descriptor.id().to_string(),
        label: opt(descriptor.label()),
        license_plate: opt(descriptor.license_plate()),
        lat,
        lon,
        bearing,
        speed,
        timestamp, // without its own: as old as the feed (the API hides old data by it)
        current_status: status_name(vp),
        current_stop_sequence: vp.current_stop_sequence.map(i64::from),
        stop_id: opt(vp.stop_id()),
        public_stop_id: index.public_stop(version, vp.stop_id()),
        trip,
        delay_s: delay,
        occupancy: vp.occupancy_status.map(|o| {
            pb::vehicle_position::OccupancyStatus::try_from(o)
                .map(|s| s.as_str_name().to_string())
                .unwrap_or_else(|_| o.to_string())
        }),
        wheelchair_accessible: descriptor.wheelchair_accessible.map(|w| {
            pb::vehicle_descriptor::WheelchairAccessible::try_from(w)
                .map(|s| s.as_str_name().to_string())
                .unwrap_or_else(|_| w.to_string())
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_speed_and_bearing_come_from_the_previous_fix() {
        let mut m = MotionTracker::default();
        assert_eq!(m.fill("302", 50.06, 19.96, 1000, None, None), (None, None));
        assert_eq!(m.fill("302", 50.06 + 100.0 / 111_195.0, 19.96, 1010, None, None), (Some(0.0), Some(10.0)));
    }

    #[test]
    fn standing_keeps_the_last_bearing() {
        let mut m = MotionTracker::default();
        m.fill("1", 50.06, 19.96, 0, None, None);
        assert_eq!(m.fill("1", 50.06, 19.96, 10, None, None), (None, Some(0.0)));
        let east = 19.96 + 0.0014;
        assert_eq!(m.fill("1", 50.06, east, 20, None, None), (Some(90.0), Some(10.0)));
        assert_eq!(m.fill("1", 50.06, east, 30, None, None), (Some(90.0), Some(0.0)));
        // feed values win; a jump is no motion
        assert_eq!(m.fill("1", 50.06, east, 40, Some(45.0), None), (Some(45.0), Some(0.0)));
        assert_eq!(m.fill("1", 51.06, east, 50, None, None), (None, None));
    }

    #[test]
    fn rounding_is_pythons() {
        assert_eq!(round1(10.05), 10.1); // 10.05 is 10.050000000000000710... in binary
        assert_eq!(round1(0.25), 0.2); // exactly halfway: to even
    }
}
