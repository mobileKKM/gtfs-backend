//! Settings from env vars (and `.env`), and the feeds of `feeds.yml`. Durations are seconds.
//!
//! The names and defaults are those of `.env.example`; the API (Python) reads the same `feeds.yml` and the shared
//! settings (DATABASE_URL, REDIS_URL, REALTIME_MAX_AGE, CLOCK_OFFSET).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use chrono::NaiveTime;
use regex::Regex;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Settings {
    pub database_url: String,
    pub redis_url: String,
    pub feeds_config: PathBuf,
    pub log_level: String,
    pub db_pool_max_size: usize,
    /// Default of a feed's `realtime_max_age`: realtime data stays live this long.
    pub realtime_max_age: u64,
    /// Benchmark replays only: the data is compared with a clock this many seconds ahead (0 in production).
    pub clock_offset: f64,

    pub archive_dir: PathBuf,
    pub osm_stops_file: PathBuf,
    pub osm_refresh_interval: f64,
    pub static_poll_interval: f64,
    pub realtime_poll_interval: f64,

    pub http_connect_timeout: f64,
    pub http_retries: u32,
    pub http_retry_delay: f64,
    pub static_download_timeout: f64,
    pub realtime_fetch_timeout: f64,

    pub vehicle_ttl: u64,
    pub trip_ttl: u64,
    pub alert_ttl: u64,
    pub unmatched_ttl: u64,

    pub watchdog_timeout: f64,
    /// Daily retirement of timetable versions, Europe/Warsaw time.
    pub retire_at: NaiveTime,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn parse<T: FromStr>(name: &str, default: T, valid: impl Fn(&T) -> bool, rule: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let value = match env(name) {
        None => default,
        Some(text) => text.trim().parse::<T>().map_err(|e| anyhow!("{name}: {e}"))?,
    };
    if !valid(&value) {
        bail!("{name} must be {rule}");
    }
    Ok(value)
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv(); // a missing .env is fine; real env vars win over it
        let pos = |v: &f64| *v > 0.0;
        let nonneg = |v: &f64| *v >= 0.0;
        let at_least_1 = |v: &u64| *v >= 1;
        let retire_at = match env("RETIRE_AT") {
            None => NaiveTime::from_hms_opt(3, 30, 0).unwrap(),
            Some(t) => NaiveTime::parse_from_str(t.trim(), "%H:%M")
                .or_else(|_| NaiveTime::parse_from_str(t.trim(), "%H:%M:%S"))
                .map_err(|_| anyhow!("RETIRE_AT must be HH:MM"))?,
        };
        Ok(Self {
            database_url: env("DATABASE_URL").unwrap_or_else(|| "postgresql://gtfs:gtfs@localhost:5432/gtfs".into()),
            redis_url: env("REDIS_URL").unwrap_or_else(|| "redis://localhost:6379/0".into()),
            feeds_config: env("FEEDS_CONFIG").unwrap_or_else(|| "feeds.yml".into()).into(),
            log_level: env("LOG_LEVEL").unwrap_or_else(|| "INFO".into()),
            db_pool_max_size: parse("DB_POOL_MAX_SIZE", 10, |v| *v >= 1, ">= 1")?,
            realtime_max_age: parse("REALTIME_MAX_AGE", 120, at_least_1, ">= 1")?,
            clock_offset: parse("CLOCK_OFFSET", 0.0, |v: &f64| v.is_finite(), "a number")?,
            archive_dir: env("ARCHIVE_DIR").unwrap_or_else(|| "data/archive".into()).into(),
            osm_stops_file: env("OSM_STOPS_FILE").unwrap_or_else(|| "data/osm/stops.json".into()).into(),
            osm_refresh_interval: parse("OSM_REFRESH_INTERVAL", 86400.0, nonneg, ">= 0")?,
            static_poll_interval: parse("STATIC_POLL_INTERVAL", 3600.0, pos, "> 0")?,
            realtime_poll_interval: parse("REALTIME_POLL_INTERVAL", 10.0, pos, "> 0")?,
            http_connect_timeout: parse("HTTP_CONNECT_TIMEOUT", 10.0, pos, "> 0")?,
            http_retries: parse("HTTP_RETRIES", 3, |v| *v >= 1, ">= 1")?,
            http_retry_delay: parse("HTTP_RETRY_DELAY", 1.0, nonneg, ">= 0")?,
            static_download_timeout: parse("STATIC_DOWNLOAD_TIMEOUT", 300.0, pos, "> 0")?,
            realtime_fetch_timeout: parse("REALTIME_FETCH_TIMEOUT", 15.0, pos, "> 0")?,
            vehicle_ttl: parse("VEHICLE_TTL", 120, at_least_1, ">= 1")?,
            trip_ttl: parse("TRIP_TTL", 600, at_least_1, ">= 1")?,
            alert_ttl: parse("ALERT_TTL", 600, at_least_1, ">= 1")?,
            unmatched_ttl: parse("UNMATCHED_TTL", 120, at_least_1, ">= 1")?,
            watchdog_timeout: parse("WATCHDOG_TIMEOUT", 600.0, pos, "> 0")?,
            retire_at,
        })
    }

    /// A feed's realtime max age (seconds).
    pub fn max_age(&self, feed: &FeedConfig) -> u64 {
        feed.realtime_max_age.unwrap_or(self.realtime_max_age)
    }

    pub fn poll_interval(&self, feed: &FeedConfig) -> f64 {
        feed.realtime_poll_interval.unwrap_or(self.realtime_poll_interval)
    }

    /// The wall clock the data is compared with (epoch seconds): now, shifted by CLOCK_OFFSET.
    pub fn now(&self) -> f64 {
        now() + self.clock_offset
    }
}

pub fn now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

// ------------------------------------------------------------------------------------------------- feeds.yml

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RealtimeKind {
    VehiclePositions,
    TripUpdates,
    ServiceAlerts,
    /// One file with any mix of vehicles, trip updates and alerts (Koleje Małopolskie).
    Combined,
}

impl RealtimeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VehiclePositions => "vehicle_positions",
            Self::TripUpdates => "trip_updates",
            Self::ServiceAlerts => "service_alerts",
            Self::Combined => "combined",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        [Self::VehiclePositions, Self::TripUpdates, Self::ServiceAlerts, Self::Combined]
            .into_iter()
            .find(|k| k.as_str() == text)
    }

    /// Whether a file of this kind holds `part`: its own, or all if combined.
    pub fn carries(self, part: Self) -> bool {
        self == Self::Combined || self == part
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DayStart {
    /// GTFS: times count from noon minus 12 h of the service date.
    #[default]
    #[serde(rename = "noon_minus_12h")]
    NoonMinus12h,
    /// Some realtime systems count from local midnight (ZTP's trams); differs only on clock-change days.
    Midnight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtraKind {
    New,
    Duplicated,
    Added,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeUrls {
    pub vehicle_positions: Option<String>,
    pub trip_updates: Option<String>,
    pub service_alerts: Option<String>,
    pub combined: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_extra() -> Vec<ExtraKind> {
    vec![ExtraKind::New, ExtraKind::Duplicated]
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedConfig {
    pub feed_id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Feeds with the same namespace share stop codes (ZTP's three feeds); default: the feed's own id.
    pub stop_namespace: Option<String>,
    /// Namespaces whose physical stops this feed also uses (MLD on ZTP's poles).
    #[serde(default)]
    pub share_stops_with: Vec<String>,
    /// A regex the whole stop_code matches, naming `station` and/or `platform_code` (ZTP: `802-01`).
    pub stop_code_pattern: Option<String>,
    #[serde(rename = "static")]
    pub static_url: String,
    #[serde(default)]
    pub realtime: RealtimeUrls,
    #[serde(default)]
    pub day_start: DayStart,
    #[serde(default)]
    pub ignore_service_suffix: bool,
    pub realtime_poll_interval: Option<f64>,
    pub realtime_max_age: Option<u64>,
    #[serde(default = "default_extra")]
    pub extra_trips: Vec<ExtraKind>,
}

impl FeedConfig {
    pub fn namespace(&self) -> &str {
        self.stop_namespace.as_deref().unwrap_or(&self.feed_id)
    }

    /// Configured (kind, url) pairs, in a fixed order.
    pub fn realtime_files(&self) -> Vec<(RealtimeKind, String)> {
        let r = &self.realtime;
        [
            (RealtimeKind::VehiclePositions, &r.vehicle_positions),
            (RealtimeKind::TripUpdates, &r.trip_updates),
            (RealtimeKind::ServiceAlerts, &r.service_alerts),
            (RealtimeKind::Combined, &r.combined),
        ]
        .into_iter()
        .filter_map(|(k, url)| url.clone().map(|u| (k, u)))
        .collect()
    }

    /// The compiled stop_code_pattern, anchored: the whole code must match.
    pub fn stop_code_regex(&self) -> Option<Regex> {
        self.stop_code_pattern.as_ref().map(|p| Regex::new(&format!("^(?:{p})$")).expect("validated"))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedsFile {
    pub feeds: Vec<FeedConfig>,
    /// Stop search only (the API); validated here too.
    pub search_namespaces: Option<Vec<String>>,
}

fn is_id(text: &str, max: usize) -> bool {
    !text.is_empty()
        && text.len() <= max
        && text.split('_').all(|w| !w.is_empty() && w.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
}

impl FeedsFile {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let feeds: FeedsFile = serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        feeds.validate()?;
        Ok(feeds)
    }

    fn validate(&self) -> Result<()> {
        let mut seen = HashSet::new();
        let mut patterns: HashMap<&str, Option<&str>> = HashMap::new();
        for f in &self.feeds {
            // `__` separates feed and version in partition names, which must stay under Postgres' 63 characters
            if !is_id(&f.feed_id, 32) {
                bail!("feed_id {:?}: lowercase words joined by single underscores, at most 32 characters", f.feed_id);
            }
            if !seen.insert(f.feed_id.as_str()) {
                bail!("duplicate feed_id: {}", f.feed_id);
            }
            if let Some(ns) = &f.stop_namespace
                && (ns.is_empty()
                    || ns.len() > 40
                    || !ns.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
            {
                bail!("{}: stop_namespace must be [a-z0-9_]+", f.feed_id);
            }
            if let Some(p) = &f.stop_code_pattern {
                let re = Regex::new(p).map_err(|e| anyhow!("{}: stop_code_pattern: {e}", f.feed_id))?;
                let groups: BTreeSet<&str> = re.capture_names().flatten().collect();
                if groups.is_empty() || groups.iter().any(|g| !matches!(*g, "station" | "platform_code")) {
                    bail!("{}: stop_code_pattern: named groups must be among [platform_code, station]", f.feed_id);
                }
            }
            if f.realtime_poll_interval.is_some_and(|v| v <= 0.0) {
                bail!("{}: realtime_poll_interval must be > 0", f.feed_id);
            }
            if f.realtime_max_age == Some(0) {
                bail!("{}: realtime_max_age must be >= 1", f.feed_id);
            }
            for url in std::iter::once(&f.static_url).chain(f.realtime_files().iter().map(|(_, u)| u)) {
                reqwest::Url::parse(url).map_err(|e| anyhow!("{}: invalid URL {url}: {e}", f.feed_id))?;
            }
            if *patterns.entry(f.namespace()).or_insert(f.stop_code_pattern.as_deref())
                != f.stop_code_pattern.as_deref()
            {
                bail!("stop_code_pattern: feeds of namespace {} must declare the same one", f.namespace());
            }
        }
        if let Some(ns) = &self.search_namespaces {
            let known: HashSet<&str> = self.feeds.iter().map(|f| f.namespace()).collect();
            let unknown: Vec<&String> = ns.iter().filter(|n| !known.contains(n.as_str())).collect();
            if ns.is_empty() || !unknown.is_empty() {
                bail!("search_namespaces: unknown namespace(s) {unknown:?}");
            }
        }
        Ok(())
    }

    pub fn enabled(&self) -> Vec<&FeedConfig> {
        self.feeds.iter().filter(|f| f.enabled).collect()
    }

    pub fn get(&self, feed_id: &str) -> Result<&FeedConfig> {
        self.feeds.iter().find(|f| f.feed_id == feed_id).ok_or_else(|| anyhow!("unknown feed_id: {feed_id}"))
    }

    /// Enabled feeds, those whose stops others share (ZTP for MLD) first, so attaching finds their stops.
    pub fn import_order(&self) -> Vec<&FeedConfig> {
        let shared: HashSet<&str> =
            self.enabled().iter().flat_map(|f| f.share_stops_with.iter().map(String::as_str)).collect();
        let mut feeds = self.enabled();
        feeds.sort_by_key(|f| (!shared.contains(f.namespace()), !f.share_stops_with.is_empty(), f.feed_id.clone()));
        feeds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feeds(yaml: &str) -> Result<FeedsFile> {
        let f: FeedsFile = serde_yaml::from_str(yaml)?;
        f.validate()?;
        Ok(f)
    }

    #[test]
    fn the_repository_feeds_file_is_valid() {
        let f = FeedsFile::load(std::path::Path::new("../feeds.yml")).unwrap();
        let t = f.get("kmk_t").unwrap();
        assert_eq!(t.day_start, DayStart::Midnight);
        assert!(t.ignore_service_suffix);
        assert_eq!(f.get("km_kolej").unwrap().realtime_files()[0].0, RealtimeKind::Combined);
        let order: Vec<&str> = f.import_order().iter().map(|f| f.feed_id.as_str()).collect();
        assert_eq!(order.last(), Some(&"km_mld")); // it shares ZTP's stops: imported after them
        assert_eq!(f.get("kmk_a").unwrap().extra_trips, vec![ExtraKind::New, ExtraKind::Duplicated]);
    }

    #[test]
    fn bad_configs_are_rejected() {
        let base = "feeds:\n  - feed_id: {id}\n    static: https://x.test/a.zip\n";
        assert!(feeds(&base.replace("{id}", "a__b")).is_err());
        assert!(feeds(&base.replace("{id}", "Ab")).is_err());
        assert!(feeds(&(base.replace("{id}", "a") + "    stop_code_pattern: '(?P<other>.+)'\n")).is_err());
        assert!(feeds(&(base.replace("{id}", "a") + "    unknown: 1\n")).is_err());
        assert!(feeds(&(base.replace("{id}", "a") + "search_namespaces: [nope]\n")).is_err());
        assert!(feeds(&base.replace("{id}", "a")).is_ok());
    }
}
