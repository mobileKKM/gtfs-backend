//! Small shared pieces: service-day time, keys the API links to, stop slugs and ids, route_type -> mode.

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use rand::Rng;
use unicode_normalization::UnicodeNormalization;

use crate::config::DayStart;

// ------------------------------------------------------------------------------------------------- time

/// GTFS: the epoch a service date's times count from is noon minus 12 h, local time. That is local midnight except on
/// the two days the clocks change, when it is an hour off.
pub fn service_day_base(d: NaiveDate, tz: Tz) -> i64 {
    let noon = d.and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap());
    tz.from_local_datetime(&noon).earliest().map(|t| t.timestamp()).unwrap_or_else(|| noon.and_utc().timestamp())
        - 12 * 3600
}

fn midnight_base(d: NaiveDate, tz: Tz) -> i64 {
    let midnight = d.and_time(NaiveTime::MIN);
    tz.from_local_datetime(&midnight).earliest().map(|t| t.timestamp()).unwrap_or_else(|| service_day_base(d, tz))
}

/// The epoch a feed's GTFS times on service date `d` count from.
pub fn day_base(d: NaiveDate, tz: Tz, day_start: DayStart) -> i64 {
    match day_start {
        DayStart::NoonMinus12h => service_day_base(d, tz),
        DayStart::Midnight => midnight_base(d, tz),
    }
}

pub fn local_date(epoch: f64, tz: Tz) -> NaiveDate {
    let utc = DateTime::<Utc>::from_timestamp(epoch.floor() as i64, 0).unwrap_or_default();
    utc.with_timezone(&tz).date_naive()
}

/// Yesterday, today and tomorrow (local): the service days a running trip can belong to.
pub fn nearby_dates(epoch: f64, tz: Tz) -> [NaiveDate; 3] {
    let today = local_date(epoch, tz);
    [today.pred_opt().unwrap(), today, today.succ_opt().unwrap()]
}

pub fn iso_utc(epoch: f64) -> String {
    DateTime::<Utc>::from_timestamp(epoch.floor() as i64, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S+00:00")
        .to_string()
}

/// GTFS `YYYYMMDD`.
pub fn parse_ymd(text: &str) -> Option<NaiveDate> {
    if text.len() != 8 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    NaiveDate::parse_from_str(text, "%Y%m%d").ok()
}

/// GTFS time `H:MM:SS` (hours may pass 24) as seconds.
pub fn parse_gtfs_time(text: &str) -> Option<i32> {
    let mut parts = text.split(':');
    let (h, m, s) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || h.is_empty() || h.len() > 3 || m.len() != 2 || s.len() != 2 {
        return None;
    }
    if ![h, m, s].iter().all(|p| p.bytes().all(|b| b.is_ascii_digit())) {
        return None;
    }
    let (h, m, s): (i32, i32, i32) = (h.parse().ok()?, m.parse().ok()?, s.parse().ok()?);
    (m < 60 && s < 60).then_some(h * 3600 + m * 60 + s)
}

// ------------------------------------------------------------------------------------------------- keys

/// `{feed_id}:{vehicle_id}`, `{feed_id}:{YYYYMMDD}:{trip_id}`: the source id comes last, kept whole.
pub fn vehicle_key(feed_id: &str, vehicle_id: &str) -> String {
    format!("{feed_id}:{vehicle_id}")
}

pub fn trip_key(feed_id: &str, service_date: NaiveDate, trip_id: &str) -> String {
    format!("{feed_id}:{}:{trip_id}", service_date.format("%Y%m%d"))
}

/// core.feed_version.id of a per-feed version number (`kmk_t_v3`).
pub fn version_id(feed_id: &str, version: i32) -> String {
    format!("{feed_id}_v{version}")
}

/// The partition holding one feed version of a table: `stop_times__kmk_t__v3`.
pub fn partition_name(table: &str, feed_id: &str, version: i32) -> String {
    format!("{table}__{feed_id}__v{version}")
}

// ------------------------------------------------------------------------------------------------- stop ids

/// Lowercase ASCII slug: Polish letters to ASCII, anything else to single dashes.
pub fn slugify(text: &str) -> String {
    let mapped: String = text
        .chars()
        .flat_map(|c| match c {
            'ł' => "l".chars().collect::<Vec<_>>(),
            'Ł' => "L".chars().collect(),
            'đ' => "d".chars().collect(),
            'Đ' => "D".chars().collect(),
            'ß' => "ss".chars().collect(),
            'æ' => "ae".chars().collect(),
            'ø' => "o".chars().collect(),
            c => vec![c],
        })
        .collect();
    let mut out = String::new();
    let mut dash = false;
    for c in mapped.nfkd().filter(|c| !unicode_normalization::char::is_combining_mark(*c)).flat_map(char::to_lowercase)
    {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            if dash && !out.is_empty() {
                out.push('-');
            }
            dash = false;
            out.push(c);
        } else {
            dash = true;
        }
    }
    if out.is_empty() { "stop".into() } else { out }
}

/// Name slug + platform_code without separator: `rondo-mogilskie01`.
pub fn stop_slug(name: &str, platform_code: Option<&str>) -> String {
    let suffix: String =
        platform_code.map(|p| slugify(p).chars().filter(char::is_ascii_alphanumeric).collect()).unwrap_or_default();
    slugify(name) + &suffix
}

pub fn normalize_name(name: &str) -> String {
    if name.is_empty() { String::new() } else { slugify(name) }
}

pub fn new_public_id() -> String {
    let mut rng = rand::thread_rng();
    (0..5).map(|_| rng.gen_range(b'a'..=b'z') as char).collect()
}

// ------------------------------------------------------------------------------------------------- modes

/// mode -> its basic route types and extended (first, last) ranges (gtfs_backend/modes.py has the same table).
#[allow(clippy::type_complexity)]
const MODES: &[(&str, &[i32], &[(i32, i32)])] = &[
    ("tram", &[0], &[(900, 906)]),
    ("bus", &[3], &[(700, 716), (200, 209)]),
    ("trolleybus", &[11, 800], &[]),
    ("metro", &[1], &[(400, 405)]),
    ("rail", &[2], &[(100, 117)]),
    ("ferry", &[4, 1000, 1200], &[]),
    ("cable_tram", &[5], &[]),
    ("aerial_lift", &[6], &[(1300, 1307)]),
    ("funicular", &[7, 1400], &[]),
    ("monorail", &[12], &[]),
];

/// The transport mode of a `route_type` column, as SQL.
pub fn mode_sql() -> String {
    let mut out = String::from("CASE ");
    for (mode, types, ranges) in MODES {
        let conds: Vec<String> = types
            .iter()
            .map(|t| format!("route_type = {t}"))
            .chain(ranges.iter().map(|(lo, hi)| format!("route_type BETWEEN {lo} AND {hi}")))
            .collect();
        out += &format!("WHEN {} THEN '{mode}' ", conds.join(" OR "));
    }
    out + "ELSE 'other' END"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs() {
        assert_eq!(slugify("Rondo Mogilskie"), "rondo-mogilskie");
        assert_eq!(slugify("Łagiewniki ZUS / Żółć"), "lagiewniki-zus-zolc");
        assert_eq!(slugify("  --  "), "stop");
        assert_eq!(stop_slug("Dworzec Główny Zachód", Some("09")), "dworzec-glowny-zachod09");
        assert_eq!(stop_slug("AGH", Some("A-1")), "agha1");
        assert_eq!(normalize_name(""), "");
    }

    #[test]
    fn times_and_days() {
        assert_eq!(parse_gtfs_time("25:10:00"), Some(25 * 3600 + 600));
        assert_eq!(parse_gtfs_time("7:05:03"), Some(7 * 3600 + 303));
        assert_eq!(parse_gtfs_time("6am"), None);
        assert_eq!(parse_gtfs_time("06:60:00"), None);
        let tz: Tz = "Europe/Warsaw".parse().unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 10, 25).unwrap(); // clocks back at 03:00
        // noon minus 12 h is 23:00 the evening before; midnight is an hour earlier
        assert_eq!(service_day_base(d, tz) - midnight_base(d, tz), 3600);
        let normal = NaiveDate::from_ymd_opt(2026, 10, 24).unwrap();
        assert_eq!(service_day_base(normal, tz), midnight_base(normal, tz));
        assert_eq!(trip_key("kmk_t", d, "a:b"), "kmk_t:20261025:a:b");
        assert_eq!(parse_ymd("20261301"), None);
    }

    #[test]
    fn mode_sql_names_every_mode() {
        let sql = mode_sql();
        assert!(sql.contains("WHEN route_type = 0 OR route_type BETWEEN 900 AND 906 THEN 'tram'"));
        assert!(sql.ends_with("ELSE 'other' END"));
    }
}
