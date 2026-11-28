//! The worker: one task per static feed and per realtime file, plus OSM refresh, daily retirement and the heartbeat
//! (which also flushes the metrics).
//!
//! Every loop handles its own errors; a task that ends anyway (a bug) or a realtime task stuck for WATCHDOG_TIMEOUT stops
//! the worker with exit code 1, so Docker restarts it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono_tz::Europe::Warsaw;
use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, Notify, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::config::{FeedConfig, FeedsFile, Settings};
use crate::http::Retry;
use crate::metrics::{FLUSH_EVERY, Recorder};
use crate::realtime::layout::STATIC_UPDATED;
use crate::realtime::poller::{FileConfig, FileStatus, FileTask};
use crate::realtime::runtime::FeedRuntime;
use crate::realtime::store::{FeedStore, Ttls};
use crate::static_feed::stops::osm::{EXTRACT_URL, fetch_osm_poles, file_age_s};
use crate::static_feed::{Status, check_static, versions};

/// Touched while every realtime task keeps finishing polls; the compose healthcheck reads its age.
pub const HEARTBEAT_FILE: &str = "/tmp/gtfs-worker.alive";
const MAINTENANCE_EVERY: Duration = Duration::from_secs(60);
const OSM_CHECK_EVERY: Duration = Duration::from_secs(3600);
/// the first OSM fetch is tried this often before the feeds that need the file start without it
const OSM_FIRST_ATTEMPTS: u32 = 3;
const OSM_RETRY: Duration = Duration::from_secs(30);
/// a realtime download attempt may take its poll interval, but at least this long
const MIN_FETCH_ATTEMPT_S: f64 = 5.0;
/// After an import, `static:updated` waits (at most this long) until the feed's realtime is written for the new version,
/// so the API doesn't switch to a version no realtime is written for yet.
const REWRITE_WAIT_MAX_S: f64 = 90.0;
const REWRITE_CHECK: Duration = Duration::from_millis(250);

pub fn redis_url(settings: &Settings) -> Result<redis::Client> {
    Ok(redis::Client::open(settings.redis_url.as_str())?)
}

pub async fn redis_manager(settings: &Settings) -> Result<ConnectionManager> {
    Ok(ConnectionManager::new(redis_url(settings)?).await?)
}

struct FileInfo {
    feed_id: String,
    name: String,
    interval: f64,
    status: Arc<FileStatus>,
}

struct Worker {
    settings: Settings,
    feeds: FeedsFile,
    runtimes: HashMap<String, Arc<FeedRuntime>>,
    files: Vec<FileInfo>,
    redis: ConnectionManager,
    static_http: reqwest::Client,
    /// imports and the daily retirement, one at a time (versions::LOCK_TIMEOUT_MS)
    static_lock: Mutex<()>,
    first_check: HashMap<String, watch::Sender<bool>>,
    osm_ready: watch::Sender<bool>,
    epoch: Instant,
}

pub async fn run(settings: Settings, feeds: FeedsFile, raw: serde_json::Value) -> Result<i32> {
    {
        let mut client = crate::db::connect(&settings.database_url).await?;
        crate::db::migrate(&mut client).await?;
        crate::static_feed::sync_feeds(&mut client, &feeds, &raw).await?;
    }
    if settings.clock_offset != 0.0 {
        warn!(seconds = settings.clock_offset, "clock_offset");
    }
    let pool = crate::db::pool(&settings.database_url, settings.db_pool_max_size, Duration::from_secs(30))?;
    let redis = redis_manager(&settings).await?;
    let run_id = format!("{:012x}", rand::random::<u64>() & 0xffff_ffff_ffff);
    let ttls = Ttls {
        vehicle: settings.vehicle_ttl,
        trip: settings.trip_ttl,
        alert: settings.alert_ttl,
        unmatched: settings.unmatched_ttl,
    };
    let enabled: Vec<FeedConfig> = feeds.enabled().into_iter().cloned().collect();
    let runtimes: HashMap<String, Arc<FeedRuntime>> = enabled
        .iter()
        .map(|f| {
            let store = FeedStore::new(redis.clone(), ttls, &f.feed_id, &run_id);
            (f.feed_id.clone(), FeedRuntime::new(f.clone(), pool.clone(), settings.max_age(f) as i64, store))
        })
        .collect();
    let rt_http = crate::http::client(settings.realtime_fetch_timeout, settings.http_connect_timeout)?;
    let recorder = Arc::new(Recorder::default());
    let clock: Arc<dyn Fn() -> f64 + Send + Sync> = {
        let offset = settings.clock_offset;
        Arc::new(move || crate::config::now() + offset)
    };
    let epoch = Instant::now();
    let mut tasks: Vec<FileTask> = Vec::new();
    for f in &enabled {
        let interval = settings.poll_interval(f);
        for (kind, url) in f.realtime_files() {
            let cfg = FileConfig {
                url,
                interval,
                max_age: settings.max_age(f) as f64,
                retry: Retry { retries: settings.http_retries, delay: settings.http_retry_delay },
                attempt_timeout: Duration::from_secs_f64(
                    settings.realtime_fetch_timeout.min(interval.max(MIN_FETCH_ATTEMPT_S)),
                ),
            };
            tasks.push(FileTask::new(
                runtimes[&f.feed_id].clone(),
                kind,
                cfg,
                rt_http.clone(),
                clock.clone(),
                recorder.clone(),
                epoch,
            ));
        }
    }
    let uses_osm = settings.osm_refresh_interval > 0.0 && enabled.iter().any(|f| !f.share_stops_with.is_empty());
    let worker = Arc::new(Worker {
        static_http: crate::http::client(settings.static_download_timeout, settings.http_connect_timeout)?,
        files: tasks
            .iter()
            .map(|t| FileInfo {
                feed_id: t.runtime.feed_id().into(),
                name: t.name(),
                interval: t.interval,
                status: t.status.clone(),
            })
            .collect(),
        first_check: enabled.iter().map(|f| (f.feed_id.clone(), watch::channel(false).0)).collect(),
        osm_ready: watch::channel(!uses_osm).0,
        settings,
        feeds,
        runtimes,
        redis: redis.clone(),
        static_lock: Mutex::new(()),
        epoch,
    });

    let mut set: JoinSet<&'static str> = JoinSet::new();
    let mut names: Vec<String> = Vec::new();
    for f in worker.feeds.import_order() {
        let (w, f) = (worker.clone(), f.clone());
        names.push(format!("static:{}", f.feed_id));
        set.spawn(async move {
            w.static_loop(f).await;
            "static"
        });
    }
    for t in tasks {
        names.push(format!("realtime:{}", t.name()));
        set.spawn(async move {
            t.run().await;
            "realtime"
        });
    }
    if uses_osm {
        let w = worker.clone();
        set.spawn(async move {
            w.osm_loop().await;
            "osm"
        });
    }
    let w = worker.clone();
    set.spawn(async move {
        w.maintenance_loop().await;
        "maintenance"
    });
    let w = worker.clone();
    set.spawn(async move {
        w.heartbeat_loop(recorder).await;
        "heartbeat"
    });
    info!(tasks = ?names, "worker_started");

    let stop = Arc::new(Notify::new());
    let s = stop.clone();
    tokio::spawn(async move {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        s.notify_one();
    });
    let code = tokio::select! {
        _ = stop.notified() => {
            info!("worker_stopping");
            0
        }
        ended = set.join_next() => {
            match ended {
                Some(Ok(task)) => error!(task, "task_ended"),
                Some(Err(e)) => error!(error = %e, "task_crashed"),
                None => {}
            }
            1
        }
    };
    set.abort_all();
    while set.join_next().await.is_some() {}
    Ok(code)
}

impl Worker {
    fn invalidate_runtimes(&self) {
        for rt in self.runtimes.values() {
            rt.invalidate(); // stop links of other feeds may change too
        }
    }

    async fn static_loop(self: Arc<Self>, feed: FeedConfig) {
        let providers: Vec<String> = self
            .feeds
            .enabled()
            .iter()
            .filter(|f| feed.share_stops_with.iter().any(|ns| ns == f.namespace()))
            .map(|f| f.feed_id.clone())
            .collect();
        for p in providers {
            let _ = self.first_check[&p].subscribe().wait_for(|done| *done).await;
        }
        if !feed.share_stops_with.is_empty() {
            let _ = self.osm_ready.subscribe().wait_for(|ready| *ready).await;
        }
        loop {
            self.static_check(&feed).await;
            self.first_check[&feed.feed_id].send_replace(true);
            tokio::time::sleep(Duration::from_secs_f64(self.settings.static_poll_interval)).await;
        }
    }

    /// One static check of a feed (download, import if new); errors are logged, never raised. The check runs in its own
    /// task so that even a panic in an import only fails that check.
    async fn static_check(&self, feed: &FeedConfig) {
        let guard = self.static_lock.lock().await;
        let (f, settings, http) = (feed.clone(), self.settings.clone(), self.static_http.clone());
        let result = tokio::spawn(async move { check_static(&f, &settings, &http, true, false).await }).await;
        drop(guard);
        let id = feed.feed_id.as_str();
        match result {
            Err(e) => error!(feed = id, error = %e, "static_import_crashed"),
            Ok(Err(e)) => error!(feed = id, error = %format!("{e:#}"), "static_check_failed"),
            Ok(Ok(None)) => {}
            Ok(Ok(Some(r))) if r.status == Status::PreviouslyFailed => {} // logged by check_static
            Ok(Ok(Some(r))) => {
                info!(feed = id, status = r.status.as_str(), version = r.version, seconds = r.seconds, error = ?r.error, "static_import_done");
                if r.status == Status::Ready {
                    self.invalidate_runtimes();
                    self.rewrite_realtime(feed).await;
                    let mut redis = self.redis.clone();
                    if let Err(e) =
                        redis::cmd("PUBLISH").arg(STATIC_UPDATED).arg(id).query_async::<()>(&mut redis).await
                    {
                        warn!(feed = id, error = %e, "static_updated_not_published");
                    }
                }
            }
        }
    }

    /// Load the feed's new index and wait until each of its realtime files was written with it (each processes its last
    /// update again at its next poll), at most REWRITE_WAIT_MAX_S. Realtime carries the version it was computed for and
    /// the API uses it only for the version serving the day: switching the API first would leave boards without realtime
    /// until the files change (5 min for km_kolej).
    async fn rewrite_realtime(&self, feed: &FeedConfig) {
        let id = feed.feed_id.as_str();
        let Some(rt) = self.runtimes.get(id) else {
            return;
        };
        if !rt.reload(self.settings.now()).await {
            warn!(feed = id, reason = "index not loaded", "realtime_rewrite_skipped");
            return;
        }
        let generation = rt.generation();
        let files: Vec<&FileInfo> =
            self.files.iter().filter(|f| f.feed_id == id && f.status.has_update.load(Ordering::Relaxed)).collect();
        let wait = REWRITE_WAIT_MAX_S.min(2.0 * files.iter().map(|f| f.interval).fold(0.0, f64::max) + 5.0);
        let deadline = Instant::now() + Duration::from_secs_f64(wait);
        let pending = || -> Vec<&str> {
            files
                .iter()
                .filter(|f| f.status.written_generation.load(Ordering::Relaxed) < generation)
                .map(|f| f.name.as_str())
                .collect()
        };
        while !pending().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(REWRITE_CHECK).await;
        }
        match pending() {
            p if p.is_empty() => info!(feed = id, files = files.len(), "realtime_rewritten"),
            p => warn!(feed = id, pending = ?p, waited_s = wait, "realtime_rewrite_incomplete"),
        }
    }

    /// Keep the OSM stop poles file fresh. The first check is tried a few times, and only then are the feeds that need
    /// it let go (without it they match by position alone). The new file is used by the next import.
    async fn osm_loop(self: Arc<Self>) {
        for attempt in 1..=OSM_FIRST_ATTEMPTS {
            if self.osm_check().await || attempt == OSM_FIRST_ATTEMPTS {
                break;
            }
            tokio::time::sleep(OSM_RETRY).await;
        }
        self.osm_ready.send_replace(true);
        loop {
            tokio::time::sleep(OSM_CHECK_EVERY).await;
            self.osm_check().await;
        }
    }

    async fn osm_check(&self) -> bool {
        let path = &self.settings.osm_stops_file;
        let age = file_age_s(path);
        if age.is_some_and(|a| a < self.settings.osm_refresh_interval) {
            return true;
        }
        match fetch_osm_poles(path, EXTRACT_URL).await {
            Ok(poles) => {
                info!(poles, path = %path.display(), "osm_refreshed");
                true
            }
            Err(e) => {
                error!(error = %format!("{e:#}"), file_present = age.is_some(), "osm_refresh_failed");
                false
            }
        }
    }

    /// Daily at RETIRE_AT (Europe/Warsaw): retire versions that serve nothing anymore; retried every minute until done.
    async fn maintenance_loop(self: Arc<Self>) {
        let mut last: Option<chrono::NaiveDate> = None;
        loop {
            tokio::time::sleep(MAINTENANCE_EVERY).await;
            let now = chrono::DateTime::from_timestamp(self.settings.now() as i64, 0)
                .unwrap_or_default()
                .with_timezone(&Warsaw);
            if now.time() >= self.settings.retire_at && last != Some(now.date_naive()) && self.retire_once().await {
                last = Some(now.date_naive());
            }
        }
    }

    /// False if it has to be tried again (failed, or some feed's tables were busy).
    async fn retire_once(&self) -> bool {
        let _guard = self.static_lock.lock().await; // not while an import holds the tables
        let ids: Vec<String> = self.feeds.feeds.iter().map(|f| f.feed_id.clone()).collect();
        let result = async {
            let mut client = crate::db::connect(&self.settings.database_url).await?;
            versions::retire_all(&mut client, &ids, self.settings.now()).await
        }
        .await;
        match result {
            Err(e) => {
                error!(error = %format!("{e:#}"), "daily_retirement_failed");
                false
            }
            Ok((retired, busy)) => {
                info!(?retired, ?busy, "daily_retirement");
                if retired.iter().any(|(_, v)| !v.is_empty()) {
                    self.invalidate_runtimes();
                }
                busy.is_empty()
            }
        }
    }

    /// Every FLUSH_EVERY: touch HEARTBEAT_FILE while the realtime tasks keep polling (a stuck task ends this loop, and
    /// the worker), then flush the metrics. The flush gets FLUSH_EVERY at most: a hanging Redis must not stop the stuck
    /// check (its counts are lost then).
    async fn heartbeat_loop(self: Arc<Self>, recorder: Arc<Recorder>) {
        let limit = (self.settings.watchdog_timeout * 1000.0) as u64;
        let mut redis = self.redis.clone();
        loop {
            let now = self.epoch.elapsed().as_millis() as u64;
            let stuck: Vec<&str> = self
                .files
                .iter()
                .filter(|f| now.saturating_sub(f.status.last_cycle_ms.load(Ordering::Relaxed)) > limit)
                .map(|f| f.name.as_str())
                .collect();
            if !stuck.is_empty() {
                error!(tasks = ?stuck, seconds = self.settings.watchdog_timeout, "worker_stuck");
                return;
            }
            if let Err(e) = touch(Path::new(HEARTBEAT_FILE)) {
                warn!(path = HEARTBEAT_FILE, error = %e, "heartbeat_failed");
            }
            if tokio::time::timeout(FLUSH_EVERY, recorder.flush(&mut redis)).await.is_err() {
                warn!(seconds = FLUSH_EVERY.as_secs(), "metrics_flush_timed_out");
            }
            tokio::time::sleep(FLUSH_EVERY).await;
        }
    }
}

fn touch(path: &Path) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    f.set_modified(std::time::SystemTime::now())
}
