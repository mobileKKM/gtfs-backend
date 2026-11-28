//! `gtfs-worker replay`: process recorded realtime files (a bench/replay.py recording's manifest) in time order, each
//! at the time it arrived, through the same pollers' processing and store as `run`, into Redis. For comparing worker
//! versions and for the API tests' fixtures; never used in production.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

use tracing::{info, warn};

use super::layout;
use super::poller::{FileConfig, FileTask, parse_feed};
use super::runtime::FeedRuntime;
use super::store::{FeedStore, Ttls};
use crate::config::{FeedsFile, RealtimeKind, Settings};
use crate::http::Retry;
use crate::metrics::Recorder;

#[derive(Deserialize)]
struct Entry {
    t: f64,
    file: String,
}

/// `{"feeds": {feed_id: {"realtime": {kind: [{"t", "file"}]}}}}`; files of the same time are taken in this order.
fn entries(manifest: &serde_json::Value) -> Result<Vec<(String, String, Vec<Entry>)>> {
    let mut out = Vec::new();
    let feeds = manifest.get("feeds").and_then(|f| f.as_object()).ok_or_else(|| anyhow!("manifest: no feeds"))?;
    for (feed_id, feed) in feeds {
        for (kind, files) in feed.get("realtime").and_then(|r| r.as_object()).into_iter().flatten() {
            out.push((feed_id.clone(), kind.clone(), serde_json::from_value(files.clone())?));
        }
    }
    Ok(out)
}

pub async fn replay(
    settings: &Settings,
    feeds: &FeedsFile,
    manifest: &Path,
    snapshots: Option<PathBuf>,
    every: usize,
) -> Result<()> {
    let base = manifest.parent().unwrap_or(Path::new("."));
    let m: serde_json::Value = serde_json::from_slice(&std::fs::read(manifest)?).context("manifest")?;
    let pool = crate::db::pool(&settings.database_url, settings.db_pool_max_size, Duration::from_secs(30))?;
    let redis = crate::runner::redis_manager(settings).await?;
    let ttls = Ttls {
        vehicle: settings.vehicle_ttl,
        trip: settings.trip_ttl,
        alert: settings.alert_ttl,
        unmatched: settings.unmatched_ttl,
    };
    let now_bits = Arc::new(AtomicU64::new(0));
    let clock: Arc<dyn Fn() -> f64 + Send + Sync> = {
        let n = now_bits.clone();
        Arc::new(move || f64::from_bits(n.load(Ordering::Relaxed)))
    };
    let recorder = Arc::new(Recorder::default());
    let http = reqwest::Client::new();
    let mut tasks: HashMap<(String, RealtimeKind), FileTask> = HashMap::new();
    let mut queue: Vec<(f64, String, RealtimeKind, PathBuf)> = Vec::new();
    let mut runtimes: HashMap<String, Arc<FeedRuntime>> = HashMap::new();
    for (feed_id, kind, files) in entries(&m)? {
        let feed = feeds.get(&feed_id)?;
        let rt = runtimes
            .entry(feed_id.clone())
            .or_insert_with(|| {
                let store = FeedStore::new(redis.clone(), ttls, &feed_id, "replay");
                FeedRuntime::new(feed.clone(), pool.clone(), settings.max_age(feed) as i64, store)
            })
            .clone();
        {
            let kind = RealtimeKind::parse(&kind).ok_or_else(|| anyhow!("unknown kind {kind}"))?;
            let cfg = FileConfig {
                url: String::new(),
                interval: settings.poll_interval(feed),
                max_age: settings.max_age(feed) as f64,
                retry: Retry { retries: 1, delay: 0.0 },
                attempt_timeout: Duration::from_secs(1),
            };
            let mut task =
                FileTask::new(rt.clone(), kind, cfg, http.clone(), clock.clone(), recorder.clone(), Instant::now());
            task.synchronous = true;
            tasks.insert((feed_id.clone(), kind), task);
            queue.extend(files.iter().map(|e| (e.t, feed_id.clone(), kind, base.join(&e.file))));
        }
    }
    queue.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut counts: HashMap<(String, RealtimeKind), usize> = HashMap::new();
    let started = Instant::now();
    let mut redis_read = redis.clone();
    for (t, feed_id, kind, path) in &queue {
        now_bits.store(t.to_bits(), Ordering::Relaxed);
        let task = tasks.get_mut(&(feed_id.clone(), *kind)).unwrap();
        let body = std::fs::read(path).with_context(|| path.display().to_string())?;
        match parse_feed(&body) {
            Ok(msg) => {
                if let Err(e) = task.take(Arc::new(msg), body.len(), None, None).await {
                    warn!(feed = %feed_id, kind = kind.as_str(), error = %format!("{e:#}"), "replay_process_failed");
                }
            }
            Err(e) => {
                warn!(file = %path.display(), error = %format!("{e:#}"), "replay_parse_failed")
            }
        }
        let n = counts.entry((feed_id.clone(), *kind)).or_default();
        *n += 1;
        if let Some(dir) = &snapshots
            && every > 0
            && (*n).is_multiple_of(every)
        {
            snapshot(&mut redis_read, dir, feed_id, *kind, *n).await?;
        }
    }
    if let Some(dir) = &snapshots {
        for ((feed_id, kind), n) in &counts {
            snapshot(&mut redis_read, dir, feed_id, *kind, *n).await?;
        }
    }
    let mut by_feed: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    for ((feed_id, _), t) in &tasks {
        let e = by_feed.entry(feed_id.clone()).or_default();
        (e.0, e.1) = (e.0 + t.timings.0, e.1 + t.timings.1);
    }
    for (feed_id, (process, write)) in &by_feed {
        println!("{feed_id}: process {process:.2} s, write {write:.2} s");
    }
    info!(files = queue.len(), seconds = started.elapsed().as_secs_f64(), "replay_done");
    Ok(())
}

/// What a file's writes left in Redis: `{dir}/{feed}.{kind}.{n}.json`.
async fn snapshot(
    redis: &mut redis::aio::ConnectionManager,
    dir: &Path,
    feed_id: &str,
    kind: RealtimeKind,
    n: usize,
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut out = serde_json::Map::new();
    let parts = [
        (RealtimeKind::VehiclePositions, "vehicles", layout::vehicles_key(feed_id)),
        (RealtimeKind::TripUpdates, "trips", layout::trips_key(feed_id)),
        (RealtimeKind::ServiceAlerts, "alerts", layout::alerts_key(feed_id)),
    ];
    for (part, name, key) in parts {
        if kind.carries(part) {
            let v: Option<String> = redis::cmd("GET").arg(key).query_async(redis).await?;
            out.insert(
                name.into(),
                v.map(|s| serde_json::from_str(&s)).transpose()?.unwrap_or(serde_json::Value::Null),
            );
        }
    }
    std::fs::write(dir.join(format!("{feed_id}.{}.{n:05}.json", kind.as_str())), serde_json::to_vec(&out)?)?;
    Ok(())
}
