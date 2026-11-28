//! Data-age histograms per realtime file, added up with the API's latency histograms in Redis for `/metrics`
//! (src/gtfs_backend/metrics.py has the reader and the same buckets).
//!
//! metrics:{epoch minute}  HASH "{name}|{bucket}" -> count; expires after RETENTION_S. Buckets are log-scale, 8 per
//! octave: a value counts in the smallest i with value <= 2^(i/8), within [-80, 240].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use redis::aio::ConnectionManager;
use tracing::warn;

const BUCKETS_PER_OCTAVE: f64 = 8.0;
const MIN_BUCKET: i32 = -80;
const MAX_BUCKET: i32 = 240;
const RETENTION_S: u64 = 2 * 3600;
pub const FLUSH_EVERY: Duration = Duration::from_secs(10);

fn upper_bound(i: i32) -> f64 {
    2f64.powf(i as f64 / BUCKETS_PER_OCTAVE)
}

pub fn bucket(value: f64) -> i32 {
    if value.is_nan() || value <= upper_bound(MIN_BUCKET) {
        return MIN_BUCKET;
    }
    if value >= upper_bound(MAX_BUCKET) {
        return MAX_BUCKET;
    }
    // the epsilon keeps exact bounds (1, 2, 4, ...) in their own bucket despite float rounding in log2
    (value.log2() * BUCKETS_PER_OCTAVE - 1e-9).ceil() as i32
}

#[derive(Default)]
pub struct Recorder {
    counts: Mutex<HashMap<String, i64>>,
}

impl Recorder {
    pub fn observe(&self, name: &str, value: f64) {
        *self.counts.lock().unwrap().entry(format!("{name}|{}", bucket(value))).or_default() += 1;
    }

    /// Add what was counted since the last flush to this minute's hash; on failure the counts go with the next one.
    pub async fn flush(&self, redis: &mut ConnectionManager) {
        let counts = std::mem::take(&mut *self.counts.lock().unwrap());
        if counts.is_empty() {
            return;
        }
        let minute = (crate::config::now() / 60.0).floor() as i64; // the wall clock, never shifted by CLOCK_OFFSET
        let key = format!("metrics:{minute}");
        let mut pipe = redis::pipe();
        for (field, n) in &counts {
            pipe.cmd("HINCRBY").arg(&key).arg(field).arg(n).ignore();
        }
        pipe.cmd("EXPIRE").arg(&key).arg(RETENTION_S).ignore();
        if let Err(e) = pipe.query_async::<()>(redis).await {
            warn!(error = %e, "metrics_flush_failed");
            let mut current = self.counts.lock().unwrap();
            for (k, n) in counts {
                *current.entry(k).or_default() += n;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_match_the_python_ones() {
        assert_eq!(bucket(1.0), 0);
        assert_eq!(bucket(2.0), 8);
        assert_eq!(bucket(2.01), 9);
        assert_eq!(bucket(0.0), MIN_BUCKET);
        assert_eq!(bucket(-5.0), MIN_BUCKET);
        assert_eq!(bucket(f64::INFINITY), MAX_BUCKET);
        assert_eq!(bucket(10.0), 27); // ceil(log2(10) * 8) = ceil(26.575)
    }
}
