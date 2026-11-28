//! Writing the live data into Redis (layout.rs): each poll writes the whole of what its file carries, one value per
//! feed, and publishes what changed since the last write. The API follows those messages (src/gtfs_backend/api/data/
//! live.py); a reader that misses one (its `seq` jumps) or sees another `run` loads the feed from Redis again.
//!
//! Every vehicle, trip and trip state is serialized once: the Redis value and the change message are put together from
//! those pieces, and the next poll compares with them.

use std::collections::HashMap;

use anyhow::Result;
use redis::aio::ConnectionManager;

use super::layout::{self, LiveAlerts, LiveTrip, Unmatched, trip_state};
use super::process::Processed;
use super::trips::merge_stops;
use crate::config::RealtimeKind;
use crate::util::vehicle_key;

#[derive(Debug, Clone, Copy)]
pub struct Ttls {
    pub vehicle: u64,
    pub trip: u64,
    pub alert: u64,
    pub unmatched: u64,
}

/// One feed's part of the store: what its last writes said, to tell what changed.
pub struct FeedStore {
    redis: ConnectionManager,
    ttls: Ttls,
    feed_id: String,
    /// this worker process: change messages count from 1 in each
    run: String,
    /// vehicle key -> its JSON
    vehicles: HashMap<String, String>,
    /// None until the first trip write since the start: the last trips are in Redis only
    trips: Option<HashMap<String, LiveTrip>>,
    /// trip key -> its TripState's JSON
    states: HashMap<String, String>,
    seq: HashMap<&'static str, u64>,
}

fn dumps(v: &impl serde::Serialize) -> String {
    serde_json::to_string(v).expect("serializable")
}

/// `{"k1":v1,"k2":v2}` from keys and JSON texts.
fn object<'a>(items: impl Iterator<Item = (&'a str, &'a str)>) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in items.enumerate() {
        if i > 0 {
            out.push(',');
        }
        out += &dumps(&k);
        out.push(':');
        out += v;
    }
    out.push('}');
    out
}

/// `{"set": {...}, "del": [...]}` of a change message.
fn changes(set: String, mut del: Vec<String>) -> String {
    del.sort();
    format!("{{\"set\":{set},\"del\":{}}}", dumps(&del))
}

impl FeedStore {
    pub fn new(redis: ConnectionManager, ttls: Ttls, feed_id: &str, run: &str) -> Self {
        Self {
            redis,
            ttls,
            feed_id: feed_id.into(),
            run: run.into(),
            vehicles: HashMap::new(),
            trips: None,
            states: HashMap::new(),
            seq: HashMap::new(),
        }
    }

    async fn set(&mut self, key: String, value: &str, ttl: u64) -> Result<()> {
        redis::cmd("SET").arg(key).arg(value).arg("EX").arg(ttl).query_async::<()>(&mut self.redis).await?;
        Ok(())
    }

    /// Write what a file of `kind` carries; returns the change message's parts: (name, JSON).
    pub async fn write(&mut self, kind: RealtimeKind, res: &Processed) -> Result<Vec<(&'static str, String)>> {
        let mut parts = Vec::new();
        if kind.carries(RealtimeKind::VehiclePositions) {
            parts.push(("vehicles", self.write_vehicles(res).await?));
        }
        if kind.carries(RealtimeKind::TripUpdates) {
            parts.push(("trips", self.write_trips(res).await?));
        }
        if kind.carries(RealtimeKind::ServiceAlerts) {
            // a change message carries the whole value
            let value = dumps(&LiveAlerts { timestamp: res.timestamp, alerts: res.alerts.clone() });
            let ttl = self.ttls.alert;
            self.set(layout::alerts_key(&self.feed_id), &value, ttl).await?;
            parts.push(("alerts", value));
        }
        if !(res.unmatched_trips.is_empty()
            && res.unmatched_vehicles.is_empty()
            && res.unmatched_alert_entities.is_empty())
        {
            let value = dumps(&Unmatched {
                trips: &res.unmatched_trips,
                vehicles: &res.unmatched_vehicles,
                alert_entities: &res.unmatched_alert_entities,
            });
            let ttl = self.ttls.unmatched;
            self.set(layout::unmatched_key(&self.feed_id, kind.as_str()), &value, ttl).await?;
        }
        Ok(parts)
    }

    async fn write_vehicles(&mut self, res: &Processed) -> Result<String> {
        // by key, in feed order; a vehicle listed twice: the last one, in the first one's place
        let mut order: Vec<String> = Vec::new();
        let mut values: HashMap<String, String> = HashMap::new();
        for v in &res.vehicles {
            let key = vehicle_key(&self.feed_id, &v.vehicle_id);
            if values.insert(key.clone(), dumps(v)).is_none() {
                order.push(key);
            }
        }
        let all = object(order.iter().map(|k| (k.as_str(), values[k].as_str())));
        let ttl = self.ttls.vehicle;
        self.set(layout::vehicles_key(&self.feed_id), &all, ttl).await?;
        let set = object(
            order.iter().filter(|k| self.vehicles.get(*k) != values.get(*k)).map(|k| (k.as_str(), values[k].as_str())),
        );
        let del = self.vehicles.keys().filter(|k| !values.contains_key(*k)).cloned().collect();
        self.vehicles = values;
        Ok(changes(set, del))
    }

    async fn write_trips(&mut self, res: &Processed) -> Result<String> {
        if self.trips.is_none() {
            let stored: Option<String> =
                redis::cmd("GET").arg(layout::trips_key(&self.feed_id)).query_async(&mut self.redis).await?;
            self.trips = Some(stored.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default());
        }
        let last = self.trips.as_ref().unwrap();
        // the feed drops stops it is past: their last times are kept from the previous update
        let mut order: Vec<String> = Vec::new();
        let mut merged: HashMap<String, LiveTrip> = HashMap::new();
        for trip in &res.trips {
            let key = trip.fields.key.clone();
            if merged.insert(key.clone(), merge_stops(last.get(&key), trip.clone())).is_none() {
                order.push(key);
            }
        }
        let del: Vec<String> = last.keys().filter(|k| !merged.contains_key(*k)).cloned().collect();
        let texts: HashMap<&str, String> = order.iter().map(|k| (k.as_str(), dumps(&merged[k]))).collect();
        let all = object(order.iter().map(|k| (k.as_str(), texts[k.as_str()].as_str())));
        // remembered only once written: a failed write is retried with the same update
        let ttl = self.ttls.trip;
        self.set(layout::trips_key(&self.feed_id), &all, ttl).await?;
        let states: HashMap<String, String> =
            order.iter().map(|k| (k.clone(), dumps(&trip_state(&merged[k])))).collect();
        let set = object(
            order.iter().filter(|k| self.states.get(*k) != states.get(*k)).map(|k| (k.as_str(), states[k].as_str())),
        );
        self.states = states;
        self.trips = Some(merged);
        Ok(changes(set, del))
    }

    /// Refresh the TTLs of what the file's last update wrote (it hasn't changed since).
    pub async fn touch(&mut self, kind: RealtimeKind) -> Result<()> {
        let mut pipe = redis::pipe();
        if kind.carries(RealtimeKind::VehiclePositions) {
            pipe.cmd("EXPIRE").arg(layout::vehicles_key(&self.feed_id)).arg(self.ttls.vehicle).ignore();
        }
        if kind.carries(RealtimeKind::TripUpdates) {
            pipe.cmd("EXPIRE").arg(layout::trips_key(&self.feed_id)).arg(self.ttls.trip).ignore();
        }
        if kind.carries(RealtimeKind::ServiceAlerts) {
            pipe.cmd("EXPIRE").arg(layout::alerts_key(&self.feed_id)).arg(self.ttls.alert).ignore();
        }
        pipe.query_async::<()>(&mut self.redis).await?;
        Ok(())
    }

    /// Part of a poll was written but its changes won't be published: the next message skips a number, so readers load
    /// the feed from Redis instead of missing what was written.
    pub fn lost(&mut self, kind: RealtimeKind) {
        *self.seq.entry(kind.as_str()).or_default() += 1;
    }

    /// What one poll of a file changed, numbered per file: on rt:updated, and its number in rt:seq so a reader loading
    /// from Redis knows which messages it has. The number counts even if this fails.
    pub async fn publish(&mut self, kind: RealtimeKind, ts: i64, parts: Vec<(&'static str, String)>) -> Result<()> {
        let seq = self.seq.entry(kind.as_str()).or_default();
        *seq += 1;
        let seq = *seq;
        let mut message = format!(
            "{{\"run\":{},\"feed_id\":{},\"kind\":{},\"seq\":{seq},\"ts\":{ts}",
            dumps(&self.run),
            dumps(&self.feed_id),
            dumps(&kind.as_str())
        );
        for (name, json) in parts {
            message += &format!(",\"{name}\":{json}");
        }
        message.push('}');
        redis::pipe()
            .cmd("SET")
            .arg(layout::seq_key(&self.feed_id, kind.as_str()))
            .arg(format!("{}:{seq}", self.run))
            .ignore()
            .cmd("PUBLISH")
            .arg(layout::REALTIME_UPDATED)
            .arg(message)
            .ignore()
            .query_async::<()>(&mut self.redis)
            .await?;
        Ok(())
    }

    pub async fn write_meta(
        &mut self,
        kind: RealtimeKind,
        meta: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        redis::cmd("SET")
            .arg(layout::meta_key(&self.feed_id, kind.as_str()))
            .arg(dumps(meta))
            .query_async::<()>(&mut self.redis)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pieces_are_valid_json() {
        let o = super::object([("a\"b", "[1]"), ("c", "{}")].into_iter());
        assert_eq!(serde_json::from_str::<serde_json::Value>(&o).unwrap(), serde_json::json!({"a\"b": [1], "c": {}}));
        let c = super::changes("{}".into(), vec!["z".into(), "a".into()]);
        assert_eq!(c, r#"{"set":{},"del":["a","z"]}"#);
    }
}
