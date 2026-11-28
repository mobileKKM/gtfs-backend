//! One polling task per realtime file (feed x kind).
//!
//! A poll that finds the file unchanged keeps the last data alive in Redis (TTLs refreshed) for up to the feed's max age
//! after it last changed: slow feeds (km_kolej, every ~5 min) would otherwise expire between updates. `rt:meta` keeps
//! the stats of the last change while later polls see nothing new or fail, so a failure never hides how big the last
//! good update was.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use prost::Message;
use serde_json::{Map, Value, json};
use tracing::{info, warn};

use super::pb;
use super::process::{median_delay, process_message, wanted_stop_times};
use super::runtime::FeedRuntime;
use crate::config::RealtimeKind;
use crate::http::{Retry, fetch_bytes};
use crate::metrics::Recorder;
use crate::util::iso_utc;

/// window for poll_seconds_max (10 minutes at the default 10 s interval)
const RECENT_POLLS: usize = 60;

pub fn parse_feed(body: &[u8]) -> Result<pb::FeedMessage> {
    let msg = pb::FeedMessage::decode(body)?;
    if msg.header.gtfs_realtime_version.is_empty() {
        bail!("feed message without header");
    }
    // GTFS-RT: DIFFERENTIAL is "unsupported and behavior is unspecified"; taken as a full dataset it would drop most data
    if msg.header.incrementality == Some(pb::feed_header::Incrementality::Differential as i32) {
        bail!("DIFFERENTIAL feeds are not supported");
    }
    Ok(msg)
}

/// What the runner watches of a task.
#[derive(Default)]
pub struct FileStatus {
    /// an update was stored since the start (an import then waits for it to be written with the new index)
    pub has_update: AtomicBool,
    /// the index generation the last update was written with
    pub written_generation: AtomicU64,
    /// ms since `epoch` when the last poll finished (or the task started): the watchdog's
    pub last_cycle_ms: AtomicU64,
}

pub struct FileTask {
    pub runtime: Arc<FeedRuntime>,
    pub kind: RealtimeKind,
    url: String,
    http: reqwest::Client,
    pub interval: f64,
    max_age: f64,
    retry: Retry,
    attempt_timeout: Duration,
    clock: Arc<dyn Fn() -> f64 + Send + Sync>,
    recorder: Arc<Recorder>,
    pub status: Arc<FileStatus>,
    epoch: Instant,
    etag: Option<String>,
    last_modified: Option<String>,
    last_ts: i64,
    /// when the last update was stored (its age counts from then)
    last_change: Option<Instant>,
    /// the last update processed, kept to process it again with a new index (after an import)
    last_msg: Option<Arc<pb::FeedMessage>>,
    last_size: usize,
    failures: u32,
    recent: VecDeque<f64>,
    meta: Map<String, Value>,
    /// replays: wait for index reloads instead of serving the old index meanwhile
    pub synchronous: bool,
    /// (process, write) seconds of every stored update, summed (replays report them)
    pub timings: (f64, f64),
}

pub struct FileConfig {
    pub url: String,
    pub interval: f64,
    pub max_age: f64,
    pub retry: Retry,
    pub attempt_timeout: Duration,
}

fn secs(d: f64) -> Value {
    json!((d * 1000.0).round() / 1000.0)
}

impl FileTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: Arc<FeedRuntime>,
        kind: RealtimeKind,
        cfg: FileConfig,
        http: reqwest::Client,
        clock: Arc<dyn Fn() -> f64 + Send + Sync>,
        recorder: Arc<Recorder>,
        epoch: Instant,
    ) -> Self {
        let mut meta = Map::new();
        meta.insert("started_at".into(), json!(iso_utc(clock())));
        let status = Arc::new(FileStatus::default());
        status.last_cycle_ms.store(epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
        Self {
            runtime,
            kind,
            url: cfg.url,
            http,
            interval: cfg.interval,
            max_age: cfg.max_age,
            retry: cfg.retry,
            attempt_timeout: cfg.attempt_timeout,
            clock,
            recorder,
            status,
            epoch,
            etag: None,
            last_modified: None,
            last_ts: 0,
            last_change: None,
            last_msg: None,
            last_size: 0,
            failures: 0,
            recent: VecDeque::with_capacity(RECENT_POLLS),
            meta,
            synchronous: false,
            timings: (0.0, 0.0),
        }
    }

    pub fn name(&self) -> String {
        format!("{}:{}", self.runtime.feed_id(), self.kind.as_str())
    }

    pub async fn run(mut self) {
        loop {
            let started = Instant::now();
            self.cycle().await;
            let left = Duration::from_secs_f64(self.interval).saturating_sub(started.elapsed());
            tokio::time::sleep(left).await;
        }
    }

    /// One poll plus its meta write; errors are recorded, never raised.
    pub async fn cycle(&mut self) {
        let started = Instant::now();
        match self.poll().await {
            Ok(()) => self.failures = 0,
            Err(e) => {
                self.failures += 1;
                let error = format!("{e:#}");
                // ZTP briefly serves empty/missing files while replacing them; only repeated failures matter
                if self.failures >= 3 {
                    warn!(feed = self.runtime.feed_id(), kind = self.kind.as_str(), error = %error, failures = self.failures, "realtime_poll_failed");
                } else {
                    info!(feed = self.runtime.feed_id(), kind = self.kind.as_str(), error = %error, failures = self.failures, "realtime_poll_failed");
                }
                self.meta.insert("ok".into(), json!(false));
                self.meta.insert("last_error".into(), json!(error));
            }
        }
        let elapsed = started.elapsed().as_secs_f64();
        if self.recent.len() == RECENT_POLLS {
            self.recent.pop_front();
        }
        self.recent.push_back(elapsed);
        let now = (self.clock)();
        // how old the newest data is now, not when it was written
        let age = (self.last_ts != 0).then_some(now - self.last_ts as f64);
        if let Some(age) = age {
            self.recorder.observe(&format!("age {} {}", self.runtime.feed_id(), self.kind.as_str()), age);
        }
        self.meta.insert("last_poll".into(), json!(iso_utc(now)));
        self.meta.insert("consecutive_failures".into(), json!(self.failures));
        self.meta.insert("poll_seconds".into(), secs(elapsed));
        self.meta.insert("poll_seconds_max".into(), secs(self.recent.iter().copied().fold(0.0, f64::max)));
        self.meta.insert("age_s".into(), age.map_or(Value::Null, |a| json!(a.round() as i64)));
        let meta = self.meta.clone();
        if let Err(e) = self.runtime.shared.lock().await.store.write_meta(self.kind, &meta).await {
            warn!(feed = self.runtime.feed_id(), kind = self.kind.as_str(), error = %format!("{e:#}"), "meta_write_failed");
        }
        self.status.last_cycle_ms.store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    async fn poll(&mut self) -> Result<()> {
        let t0 = Instant::now();
        let fetched = fetch_bytes(
            &self.http,
            &self.url,
            parse_feed,
            self.etag.as_deref(),
            self.last_modified.as_deref(),
            &self.retry,
            Some(self.attempt_timeout),
        )
        .await?;
        self.meta.insert("fetch_seconds".into(), secs(t0.elapsed().as_secs_f64()));
        self.meta.insert("fetch_attempts".into(), json!(fetched.attempts));
        let Some(msg) = fetched.value else {
            return self.unchanged_or_reindexed().await;
        };
        self.take(Arc::new(msg), fetched.size, fetched.etag, fetched.last_modified).await
    }

    /// A downloaded update: processed unless it is older than what we have.
    pub async fn take(
        &mut self,
        msg: Arc<pb::FeedMessage>,
        size: usize,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<()> {
        let ts = msg.header.timestamp.unwrap_or(0) as i64;
        if ts != 0 && ts <= self.last_ts && !self.went_back(ts) {
            if ts == self.last_ts {
                // data we have, under a new ETag (ZTP rewrites its files between timestamp changes): asking with it gets a
                // 304 next time. Not for an older copy: that one is looked at again until went_back takes it
                (self.etag, self.last_modified) = (etag, last_modified);
            }
            return self.unchanged_or_reindexed().await;
        }
        if self.process(msg, ts, size, false).await? {
            // remembered only once the data is stored, so a failed update is fetched and processed again
            (self.etag, self.last_modified) = (etag, last_modified);
        }
        Ok(())
    }

    /// An older header timestamp is taken once nothing newer came for max_age (a producer restarted with its clock
    /// behind would otherwise freeze the feed); within max_age it is an older copy (another server, a cache).
    fn went_back(&self, ts: i64) -> bool {
        let Some(changed) = self.last_change else {
            return false;
        };
        if ts >= self.last_ts || changed.elapsed().as_secs_f64() <= self.max_age {
            return false;
        }
        warn!(
            feed = self.runtime.feed_id(),
            kind = self.kind.as_str(),
            timestamp = ts,
            previous = self.last_ts,
            "feed_timestamp_went_back"
        );
        true
    }

    /// Nothing new in the file: process the last update again if the index changed since it was written (an import),
    /// else keep the last data alive.
    async fn unchanged_or_reindexed(&mut self) -> Result<()> {
        if let Some(msg) = self.last_msg.clone()
            && self.runtime.generation() != self.status.written_generation.load(Ordering::Relaxed)
        {
            self.process(msg, self.last_ts, self.last_size, true).await?;
            return Ok(());
        }
        if self.last_change.is_some_and(|c| c.elapsed().as_secs_f64() < self.max_age) {
            self.runtime.shared.lock().await.store.touch(self.kind).await?;
        }
        self.meta.insert("ok".into(), json!(true));
        self.meta.insert("last_error".into(), Value::Null);
        Ok(())
    }

    /// Process and store an update; false if there is no timetable to process it with yet.
    async fn process(&mut self, msg: Arc<pb::FeedMessage>, ts: i64, size: usize, again: bool) -> Result<bool> {
        let now = (self.clock)();
        let t1 = Instant::now();
        let index = if self.synchronous { self.runtime.settle(now).await? } else { self.runtime.index(now).await? };
        let Some((index, generation)) = index else {
            self.meta.insert("ok".into(), json!(false));
            self.meta.insert("last_error".into(), json!("no static version loaded yet"));
            return Ok(false);
        };
        let mut shared = self.runtime.shared.lock().await;
        let shared = &mut *shared;
        let wanted = wanted_stop_times(&msg, &index, &shared.state, now);
        let t2 = Instant::now();
        let stop_times = self.runtime.stop_times(shared, &index, generation, &wanted).await;
        let res = process_message(self.runtime.feed_id(), &msg, &index, &mut shared.state, &stop_times, now);
        let t3 = Instant::now();
        let changes = match shared.store.write(self.kind, &res).await {
            Ok(c) => c,
            Err(e) => {
                shared.store.lost(self.kind);
                return Err(e);
            }
        };
        let t4 = Instant::now();
        shared.store.publish(self.kind, ts, changes).await?;
        self.timings.0 += (t3 - t2).as_secs_f64();
        self.timings.1 += (Instant::now() - t3).as_secs_f64();
        // only now: a failed write or publish processes this update again at the next poll
        self.last_msg = Some(msg);
        self.last_size = size;
        self.status.written_generation.store(generation, Ordering::Relaxed);
        self.status.has_update.store(true, Ordering::Relaxed);
        self.last_ts = ts;
        if !again {
            self.last_change = Some(Instant::now()); // reprocessed data didn't change: its age counts from when it did
        }
        let mut unmatched: Vec<String> = res.unmatched_trips.clone();
        unmatched
            .extend(res.unmatched_vehicles.iter().map(|v| v.trip_id.clone().unwrap_or_else(|| v.vehicle_id.clone())));
        unmatched.extend(res.unmatched_alert_entities.iter().cloned());
        unmatched.truncate(10);
        let m = &mut self.meta;
        let mut put = |k: &str, v: Value| {
            m.insert(k.into(), v);
        };
        put("ok", json!(true));
        put("last_error", Value::Null);
        put("last_change", json!(iso_utc((self.clock)())));
        put("index_seconds", secs((t2 - t1).as_secs_f64()));
        put("process_seconds", secs((t3 - t2).as_secs_f64())); // includes loading missing stop times
        put("write_seconds", secs((t4 - t3).as_secs_f64()));
        put("feed_timestamp", json!(ts));
        put("bytes", json!(size));
        put("entities", json!(res.entities));
        put("vehicles", json!(res.vehicles.len()));
        put("trips", json!(res.trips.len()));
        put("alerts", json!(res.alerts.len()));
        put("unmatched_trips", json!(res.unmatched_trips.len()));
        put("unmatched_vehicles", json!(res.unmatched_vehicles.len()));
        put("unmatched_alert_entities", json!(res.unmatched_alert_entities.len()));
        put("unmatched_sample", json!(unmatched));
        put("route_mismatches", json!(res.mismatches));
        // ~0 normally; about +-3600 on a clock-change day would mean the feed's day_start is wrong
        put("median_delay_s", json!(median_delay(&res)));
        put("trip_relationships", json!(res.trip_relationships));
        put("stop_relationships", json!(res.stop_relationships));
        // trips predicted at their last stop after the next trip of their block is due, and of those, how many next
        // trips have no update of their own in this file
        put("block_late_next", json!(res.block_late_next));
        put("block_late_next_without_update", json!(res.block_late_next_without_update));
        put("stale_vehicles", json!(res.stale_vehicles));
        Ok(true)
    }
}
