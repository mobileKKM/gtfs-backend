//! Matching a new version's stops to our stable stops (physical poles) and stop groups (logical stops).
//!
//! A feed's stop is first put into GTFS shape (normalize_stop): its station is its parent_station, or what the feed's
//! stop_code_pattern reads from its code (ZTP: `802-01` is platform_code 01 of station 802). Poles are identified across
//! a namespace's feeds by stop_code, groups by station. Our ids never derive from codes or names, so renames and
//! renumberings keep the id.
//!
//! plan.rs (own namespace) and shared.rs (another namespace's poles: MLD on ZTP's) decide without the database;
//! apply.rs reads their inputs and writes the result. Details: docs/stop-matching.md.

pub mod apply;
pub mod osm;
pub mod plan;
pub mod shared;

use std::collections::BTreeSet;
use std::path::PathBuf;

use regex::Regex;

use crate::util::{normalize_name, slugify};

/// How a feed's stops are matched (from feeds.yml), and where the OSM stop poles are.
#[derive(Debug, Clone, Default)]
pub struct StopRules {
    pub namespace: String,
    pub share_with: Vec<String>,
    pub stop_code_pattern: Option<Regex>,
    /// None (or no file): shared stops match by position alone
    pub osm_stops_file: Option<PathBuf>,
}

impl StopRules {
    pub fn of(feed: &crate::config::FeedConfig, osm_stops_file: Option<PathBuf>) -> Self {
        Self {
            namespace: feed.namespace().to_string(),
            share_with: feed.share_stops_with.clone(),
            stop_code_pattern: feed.stop_code_regex(),
            osm_stops_file,
        }
    }
}

/// A feed's stop (GTFS location_type 0) in GTFS shape.
#[derive(Debug, Clone, PartialEq)]
pub struct NewStop {
    pub stop_id: String,
    pub code: Option<String>,
    pub name: String,
    pub platform_code: Option<String>,
    pub lat: f64,
    pub lon: f64,
    /// `station:{code or id}` of the GTFS parent_station, or the station the stop_code_pattern names (`802`)
    pub station_key: Option<String>,
    pub station_name: Option<String>,
}

impl NewStop {
    /// The station, else the name. Scoped to the namespace.
    pub fn group_key(&self) -> String {
        self.station_key.clone().unwrap_or_else(|| format!("name:{}", normalize_name(&self.name)))
    }

    pub fn group_name(&self) -> &str {
        self.station_name.as_deref().unwrap_or(&self.name)
    }

    /// Stops sharing a code are one pole; stops without a code are their own.
    pub fn pole_key(&self) -> String {
        match &self.code {
            Some(c) => format!("code:{c}"),
            None => format!("stop:{}", self.stop_id),
        }
    }
}

/// A feed's stop in GTFS shape: its own parent_station and platform_code, else what `pattern` reads from its code.
#[allow(clippy::too_many_arguments)]
pub fn normalize_stop(
    stop_id: &str,
    code: Option<&str>,
    name: &str,
    platform_code: Option<&str>,
    lat: f64,
    lon: f64,
    station_key: Option<&str>,
    station_name: Option<&str>,
    pattern: Option<&Regex>,
) -> NewStop {
    let caps = pattern.zip(code).and_then(|(re, code)| re.captures(code));
    let part =
        |n: &str| caps.as_ref().and_then(|c| c.name(n)).map(|m| m.as_str().to_string()).filter(|s| !s.is_empty());
    NewStop {
        stop_id: stop_id.to_string(),
        code: code.map(str::to_string),
        name: name.to_string(),
        platform_code: platform_code.map(str::to_string).or_else(|| part("platform_code")),
        lat,
        lon,
        station_key: station_key.map(str::to_string).or_else(|| part("station")),
        station_name: station_name.map(str::to_string),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicStop {
    pub id: String,
    pub group_id: String,
    pub code: Option<String>,
    pub name: String,
    pub platform_code: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub active: bool,
    /// linked from another feed's current version
    pub linked_elsewhere: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    pub id: String,
    pub code: Option<String>,
    pub name: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

// ------------------------------------------------------------------------------------------------- names

/// Common abbreviations in Kraków-area stop names (`bp. Prandoty` == `Biskupa Prandoty`); None: a filler word.
fn abbreviation(token: &str) -> Option<Option<&'static str>> {
    Some(match token {
        "bp" => Some("biskupa"),
        "ks" => Some("ksiedza"),
        "os" => Some("osiedle"),
        "al" => Some("aleja"),
        "pl" => Some("plac"),
        "sw" => Some("swietego"),
        "gen" => Some("generala"),
        "ul" => Some("ulica"),
        "im" | "nr" => None,
        _ => return None,
    })
}

/// Tokens of the slugged name without a leading `krakow` and a trailing pole number.
pub fn name_tokens(name: &str) -> (BTreeSet<String>, Option<String>) {
    let slug = slugify(name);
    let mut tokens: Vec<&str> = slug.split('-').collect();
    let number = match tokens.last() {
        Some(last) if tokens.len() > 1 && last.len() <= 2 && last.bytes().all(|b| b.is_ascii_digit()) => {
            Some(last.to_string())
        }
        _ => None,
    };
    if number.is_some() {
        tokens.pop();
    }
    let mut out = BTreeSet::new();
    for t in tokens {
        if t == "krakow" {
            continue;
        }
        match abbreviation(t) {
            Some(Some(full)) => {
                out.insert(full.to_string());
            }
            Some(None) => {}
            None if !t.is_empty() => {
                out.insert(t.to_string());
            }
            None => {}
        }
    }
    (out, number)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NameMatch {
    Exact,
    /// one name extends the other (`Pleszów Pętla`)
    Subset,
    /// a typo in one word (`Rutki` / `Rudki`)
    Similar,
}

pub fn names_match(a: &str, b: &str) -> Option<NameMatch> {
    let (ta, tb) = (name_tokens(a).0, name_tokens(b).0);
    if ta.is_empty() || tb.is_empty() {
        return None;
    }
    if ta == tb {
        return Some(NameMatch::Exact);
    }
    if ta.is_subset(&tb) || tb.is_subset(&ta) {
        return Some(NameMatch::Subset);
    }
    // the shared words (often the town) don't count
    let da: Vec<&String> = ta.difference(&tb).collect();
    let db: Vec<&String> = tb.difference(&ta).collect();
    (da.len() == 1 && db.len() == 1 && similarity(da[0].as_bytes(), db[0].as_bytes()) >= 0.8)
        .then_some(NameMatch::Similar)
}

/// Python's difflib.SequenceMatcher(None, a, b).ratio() (Ratcliff/Obershelp; no junk heuristics for short words).
#[allow(clippy::needless_range_loop)] // difflib's own loops, kept recognizable
pub fn similarity(a: &[u8], b: &[u8]) -> f64 {
    fn longest(a: &[u8], b: &[u8], (alo, ahi, blo, bhi): (usize, usize, usize, usize)) -> (usize, usize, usize) {
        let (mut best_i, mut best_j, mut best) = (alo, blo, 0);
        let mut prev = vec![0usize; b.len() + 1];
        for i in alo..ahi {
            let mut cur = vec![0usize; b.len() + 1];
            for j in blo..bhi {
                if a[i] == b[j] {
                    let k = if j > blo { prev[j] } else { 0 } + 1;
                    cur[j + 1] = k;
                    if k > best {
                        (best_i, best_j, best) = (i + 1 - k, j + 1 - k, k);
                    }
                }
            }
            prev = cur;
        }
        (best_i, best_j, best)
    }
    let mut matched = 0;
    let mut queue = vec![(0, a.len(), 0, b.len())];
    while let Some((alo, ahi, blo, bhi)) = queue.pop() {
        let (i, j, k) = longest(a, b, (alo, ahi, blo, bhi));
        if k > 0 {
            matched += k;
            if alo < i && blo < j {
                queue.push((alo, i, blo, j));
            }
            if i + k < ahi && j + k < bhi {
                queue.push((i + k, ahi, j + k, bhi));
            }
        }
    }
    let total = a.len() + b.len();
    if total == 0 { 1.0 } else { 2.0 * matched as f64 / total as f64 }
}

// ------------------------------------------------------------------------------------------------- geometry

pub fn distance_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371008.8;
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let (dp, dl) = (p2 - p1, (lon2 - lon1).to_radians());
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

pub fn angle_diff(a: f64, b: f64) -> f64 {
    let d = (a - b).abs() % 360.0;
    if d > 180.0 { 360.0 - d } else { d }
}

/// How far point 2 lies ahead (+) or behind (-) point 1 in the direction of travel `bearing` (degrees from north).
pub fn along_track_m(lat1: f64, lon1: f64, bearing: f64, lat2: f64, lon2: f64) -> f64 {
    let dy = (lat2 - lat1).to_radians() * 6371008.8;
    let dx = (lon2 - lon1).to_radians() * 6371008.8 * ((lat1 + lat2) / 2.0).to_radians().cos();
    let b = bearing.to_radians();
    dx * b.sin() + dy * b.cos()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const LAT: f64 = 50.0658;
    pub const LON: f64 = 19.9597;
    pub const M: f64 = 1.0 / 111_320.0; // ~1 m in degrees latitude

    pub fn ztp() -> Regex {
        Regex::new("^(?:(?P<station>.+)-(?P<platform_code>[^-]+))$").unwrap()
    }

    /// A feed stop as the import sees it, ZTP's codes read by its stop_code_pattern.
    pub fn stop(stop_id: &str, code: &str, name: &str, dlat_m: f64) -> NewStop {
        let code = (!code.is_empty()).then_some(code);
        normalize_stop(stop_id, code, name, None, LAT + dlat_m * M, LON, None, None, Some(&ztp()))
    }

    pub fn ours(pid: &str, code: &str, name: &str, dlat_m: f64, group: &str) -> PublicStop {
        PublicStop {
            id: pid.into(),
            group_id: group.into(),
            code: Some(code.into()),
            name: name.into(),
            platform_code: code.split('-').nth(1).map(str::to_string),
            lat: Some(LAT + dlat_m * M),
            lon: Some(LON),
            active: true,
            linked_elsewhere: false,
        }
    }

    pub fn group(id: &str, code: &str, name: &str, dlat_m: f64) -> Group {
        Group { id: id.into(), code: Some(code.into()), name: name.into(), lat: Some(LAT + dlat_m * M), lon: Some(LON) }
    }

    #[test]
    fn stop_code_pattern_names_station_and_platform_code() {
        let s = stop("1", "802-01", "Rondo Mogilskie", 0.0);
        assert_eq!(
            (s.station_key.as_deref(), s.platform_code.as_deref(), s.group_key(), s.pole_key()),
            (Some("802"), Some("01"), "802".into(), "code:802-01".into())
        );
        let bay = stop("2", "271-01-06", "X", 0.0); // the last dash splits, as ZTP's bus station bays need
        assert_eq!((bay.station_key.as_deref(), bay.platform_code.as_deref()), (Some("271-01"), Some("06")));
    }

    #[test]
    fn gtfs_parent_station_and_platform_code_win_over_the_pattern() {
        let s = normalize_stop(
            "1",
            Some("802-01"),
            "R",
            Some("A"),
            LAT,
            LON,
            Some("station:9"),
            Some("Dworzec"),
            Some(&ztp()),
        );
        assert_eq!(
            (s.station_key.as_deref(), s.platform_code.as_deref(), s.group_name()),
            (Some("station:9"), Some("A"), "Dworzec")
        );
    }

    #[test]
    fn codes_are_not_split_without_a_pattern() {
        let s = normalize_stop("1", Some("802-01"), "Rondo Mogilskie", None, LAT, LON, None, None, None);
        assert_eq!(
            (s.station_key.clone(), s.platform_code.clone(), s.group_key()),
            (None, None, "name:rondo-mogilskie".into())
        );
        assert_eq!(stop("2", "7620", "X", 0.0).station_key, None);
    }

    #[test]
    fn names_match_tolerates_abbreviations_typos_prefix_and_extra_words() {
        use NameMatch::*;
        assert_eq!(names_match("Biskupa Prandoty", "bp. Prandoty"), Some(Exact));
        assert_eq!(names_match("Kraków Mały Płaszów P+R", "Mały Płaszów P+R"), Some(Exact));
        assert_eq!(names_match("Cło 02", "Cło"), Some(Exact));
        assert_eq!(names_match("AGH/UR", "AGH / UR"), Some(Exact));
        assert_eq!(names_match("Pleszów Pętla", "Pleszów"), Some(Subset));
        assert_eq!(names_match("Morawica Rutki", "Morawica Rudki"), Some(Similar));
        assert_eq!(names_match("Kraków MDA", "Dworzec Główny Wschód"), None);
        assert_eq!(names_match("Rzeszotary Górne", "Rzeszotary Remiza"), None);
        assert_eq!(names_match("Wieliczka Chopina", "Wieliczka Ochronka"), None);
    }

    #[test]
    fn similarity_is_difflibs_ratio() {
        // values from Python: difflib.SequenceMatcher(None, a, b).ratio()
        assert!((similarity(b"rutki", b"rudki") - 0.8).abs() < 1e-9);
        assert!((similarity(b"gorne", b"remiza") - 2.0 * 2.0 / 11.0).abs() < 1e-9);
        assert!((similarity(b"chopina", b"ochronka") - 2.0 * 5.0 / 15.0).abs() < 1e-9);
        assert!((similarity(b"abcab", b"bacba") - 0.6).abs() < 1e-9);
        assert_eq!(similarity(b"abc", b"abc"), 1.0);
    }
}
