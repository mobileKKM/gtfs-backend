//! Per feed: the trip index (reloaded in the background), and everything its realtime files share, behind one lock
//! (normalization state, stop times, what was last written to Redis). Processing and writing a file's update hold the
//! lock, so a feed's files never interleave; different feeds run in parallel.
//!
//! The index is reloaded after imports and on a new day while the previous one keeps serving (it covers yesterday to
//! tomorrow, so it resolves every running trip for a while): a slow or unreachable Postgres never stops the live data.
//! Only the very first load has nothing to serve and is waited for.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use deadpool_postgres::Pool;
use tokio::sync::Notify;
use tracing::{info, warn};

use super::index::{StopTime, TripIndex, load_index, load_stop_times};
use super::process::FeedState;
use super::store::FeedStore;
use crate::config::FeedConfig;
use crate::util::local_date;

/// A reload taking longer is given up and retried (normal: ~0.3 s for the largest feed).
const RELOAD_TIMEOUT: Duration = Duration::from_secs(120);
const RELOAD_RETRY: Duration = Duration::from_secs(10);
/// Loading stop times holds up the poll; on failure the update goes out without them...
const STOP_TIMES_TIMEOUT: Duration = Duration::from_secs(3);
/// ... and the database isn't asked again for this long.
const STOP_TIMES_RETRY: Duration = Duration::from_secs(30);

struct Slot {
    index: Option<Arc<TripIndex>>,
    /// of the installed index: 1, 2, ... (what realtime was last written with)
    generation: u64,
    dirty: bool,
    running: bool,
    retry_at: Instant,
    error: Option<String>,
}

/// What a feed's files share while processing (under FeedRuntime::shared).
pub struct Shared {
    pub state: FeedState,
    pub store: FeedStore,
    stop_times: HashMap<(i32, String), Arc<Vec<StopTime>>>,
    /// the index generation the stop times are kept for
    stop_times_generation: u64,
    stop_times_retry_at: Instant,
}

pub struct FeedRuntime {
    pub feed: FeedConfig,
    pool: Pool,
    slot: Mutex<Slot>,
    loaded: Notify,
    pub shared: tokio::sync::Mutex<Shared>,
}

fn describe(e: &anyhow::Error) -> String {
    format!("{e:#}")
}

impl FeedRuntime {
    pub fn new(feed: FeedConfig, pool: Pool, max_age_s: i64, store: FeedStore) -> Arc<Self> {
        let state = FeedState::new(&feed, max_age_s);
        Arc::new(Self {
            feed,
            pool,
            slot: Mutex::new(Slot {
                index: None,
                generation: 0,
                dirty: true,
                running: false,
                retry_at: Instant::now(),
                error: None,
            }),
            loaded: Notify::new(),
            shared: tokio::sync::Mutex::new(Shared {
                state,
                store,
                stop_times: HashMap::new(),
                stop_times_generation: 0,
                stop_times_retry_at: Instant::now(),
            }),
        })
    }

    pub fn feed_id(&self) -> &str {
        &self.feed.feed_id
    }

    /// The static data changed: reload at the next poll.
    pub fn invalidate(&self) {
        self.slot.lock().unwrap().dirty = true;
    }

    pub fn generation(&self) -> u64 {
        self.slot.lock().unwrap().generation
    }

    /// The current index and its generation (None: no timetable yet), starting a background reload when one is due.
    pub async fn index(self: &Arc<Self>, now: f64) -> Result<Option<(Arc<TripIndex>, u64)>> {
        let start = {
            let mut s = self.slot.lock().unwrap();
            let stale = s.dirty || s.index.as_ref().is_none_or(|i| i.loaded_for != local_date(now, i.tz));
            if !s.running && stale && Instant::now() >= s.retry_at {
                s.dirty = false; // an invalidate() during the reload makes it due again
                s.running = true;
                true
            } else {
                false
            }
        };
        if start {
            let me = self.clone();
            tokio::spawn(async move { me.load(now).await });
        }
        loop {
            let notified = self.loaded.notified();
            {
                let s = self.slot.lock().unwrap();
                if s.index.is_some() || !s.running {
                    return match (&s.index, &s.error) {
                        (Some(i), _) => Ok(Some((i.clone(), s.generation))),
                        (None, Some(e)) => Err(anyhow!("no trip index: {e}")),
                        (None, None) => Ok(None),
                    };
                }
            }
            notified.await; // nothing to serve yet
        }
    }

    async fn load(self: Arc<Self>, now: f64) {
        let started = Instant::now();
        let previous = self.slot.lock().unwrap().index.clone();
        let result = async {
            let client = self.pool.get().await?;
            tokio::time::timeout(
                RELOAD_TIMEOUT,
                load_index(&**client, self.feed_id(), now, self.feed.day_start, previous.as_deref()),
            )
            .await
            .map_err(|_| anyhow!("timed out after {RELOAD_TIMEOUT:?}"))?
        }
        .await;
        let mut s = self.slot.lock().unwrap();
        s.running = false;
        match result {
            Ok(index) => {
                if let Some(ix) = &index {
                    s.generation += 1;
                    let reused = previous.as_ref().is_some_and(|p| Arc::ptr_eq(&p.data, &ix.data));
                    let days: Vec<String> = ix.days.iter().map(|d| format!("{}:v{}", d.date, d.version)).collect();
                    info!(
                        feed = self.feed_id(),
                        trips = ix.data.trips.len(),
                        ?days,
                        reused,
                        seconds = started.elapsed().as_secs_f64(),
                        "trip_index_loaded"
                    );
                }
                s.index = index.map(Arc::new);
                s.error = None;
            }
            Err(e) => {
                s.dirty = true;
                s.retry_at = Instant::now() + RELOAD_RETRY;
                s.error = Some(describe(&e));
                warn!(feed = self.feed_id(), error = %describe(&e), serving_previous = s.index.is_some(), "trip_index_reload_failed");
            }
        }
        drop(s);
        self.loaded.notify_waiters();
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.loaded.notified();
            if !self.slot.lock().unwrap().running {
                return;
            }
            notified.await;
        }
    }

    /// Load the index now (after an import) and wait for it, also for a reload already running. False if it failed (the
    /// previous index keeps serving; the next poll retries).
    pub async fn reload(self: &Arc<Self>, now: f64) -> bool {
        self.invalidate();
        for _ in 0..2 {
            // a reload already under way may have read the database before the import committed
            self.wait_idle().await;
            if !self.slot.lock().unwrap().dirty {
                break;
            }
            self.slot.lock().unwrap().retry_at = Instant::now();
            let _ = self.index(now).await;
            self.wait_idle().await;
        }
        let s = self.slot.lock().unwrap();
        !s.dirty && s.index.is_some()
    }

    /// Wait for a running reload (replays: deterministic indexes).
    pub async fn settle(self: &Arc<Self>, now: f64) -> Result<Option<(Arc<TripIndex>, u64)>> {
        let _ = self.index(now).await?;
        self.wait_idle().await;
        self.index(now).await
    }

    /// The stop times of these trips, from the cache or Postgres. Fails quietly: trips without stop times go out without
    /// per-stop predictions until a later poll loads them.
    pub async fn stop_times(
        &self,
        shared: &mut Shared,
        index: &TripIndex,
        generation: u64,
        keys: &[(i32, String)],
    ) -> HashMap<(i32, String), Arc<Vec<StopTime>>> {
        if shared.stop_times_generation != generation {
            // a new index: keep what it still has (a version's stop times never change)
            shared.stop_times.retain(|k, _| index.data.trips.contains_key(k));
            shared.stop_times_generation = generation;
        }
        let missing: Vec<(i32, String)> = keys
            .iter()
            .filter(|k| !shared.stop_times.contains_key(*k) && index.data.trips.contains_key(*k))
            .cloned()
            .collect();
        if !missing.is_empty() && Instant::now() >= shared.stop_times_retry_at {
            let loaded = async {
                let client = self.pool.get().await?;
                tokio::time::timeout(STOP_TIMES_TIMEOUT, load_stop_times(&**client, index, &missing))
                    .await
                    .map_err(|_| anyhow!("timed out after {STOP_TIMES_TIMEOUT:?}"))?
            }
            .await;
            match loaded {
                Ok(found) => shared.stop_times.extend(found),
                Err(e) => {
                    shared.stop_times_retry_at = Instant::now() + STOP_TIMES_RETRY;
                    warn!(feed = self.feed_id(), error = %describe(&e), retry_s = STOP_TIMES_RETRY.as_secs(), "stop_times_load_failed");
                }
            }
        }
        keys.iter().filter_map(|k| shared.stop_times.get(k).map(|v| (k.clone(), v.clone()))).collect()
    }
}
