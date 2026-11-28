//! Reading a GTFS zip into a version's partitions: each file is parsed and checked value by value on a blocking thread
//! while its rows stream into `COPY` (no file is held in memory whole).
//!
//! Values are stored as published: empty (or blank) fields are NULL, typed columns are trimmed and checked (Postgres
//! parses them: `interval` keeps `25:10:00`), text is kept byte for byte. Problems are collected over the whole file and
//! fail the import together; references between files (trips -> routes, stop_times -> stops, ...) are checked too.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::SinkExt;
use tokio_postgres::Transaction;

use super::spec::{Kind, TABLES, Table};
use crate::util::{parse_gtfs_time, parse_ymd};

const CHUNK_BYTES: usize = 1 << 20;
const SAMPLES: usize = 5;

/// The zip's members by file name (a zip may put them in a folder).
pub fn members(path: &Path) -> Result<HashMap<String, String>> {
    let zip = zip::ZipArchive::new(File::open(path)?).context("not a zip file")?;
    Ok(zip
        .file_names()
        .filter(|n| !n.ends_with('/'))
        .map(|n| (n.rsplit('/').next().unwrap_or(n).to_string(), n.to_string()))
        .collect())
}

/// Every member reads back with a matching checksum (a download cut short or a corrupt zip fails here).
pub fn validate_zip(path: &Path) -> Result<()> {
    let mut zip = zip::ZipArchive::new(File::open(path)?).context("not a zip file")?;
    for i in 0..zip.len() {
        let mut f = zip.by_index(i)?;
        let name = f.name().to_string();
        std::io::copy(&mut f, &mut std::io::sink()).with_context(|| format!("corrupt member in zip: {name}"))?;
    }
    Ok(())
}

fn csv_reader<R: Read>(input: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new().flexible(true).from_reader(input)
}

fn trimmed_headers<R: Read>(rdr: &mut csv::Reader<R>) -> Result<Vec<String>> {
    Ok(rdr.headers()?.iter().map(|h| h.trim().trim_start_matches('\u{feff}').to_string()).collect())
}

/// A member's bytes without a UTF-8 BOM, streamed.
fn without_bom<R: Read>(input: R) -> Result<std::io::BufReader<R>> {
    use std::io::BufRead;
    let mut input = std::io::BufReader::with_capacity(1 << 16, input);
    if input.fill_buf()?.starts_with(&[0xEF, 0xBB, 0xBF]) {
        input.consume(3);
    }
    Ok(input)
}

/// The rows of a small file (agency.txt, feed_info.txt) as column -> value, blanks left out.
pub fn small_table(path: &Path, member: &str) -> Result<Vec<HashMap<String, String>>> {
    let mut zip = zip::ZipArchive::new(File::open(path)?)?;
    let mut rdr = csv_reader(without_bom(zip.by_name(member)?)?);
    let headers = trimmed_headers(&mut rdr)?;
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        out.push(
            headers
                .iter()
                .zip(rec.iter())
                .filter(|(_, v)| !v.trim().is_empty())
                .map(|(h, v)| (h.clone(), v.to_string()))
                .collect(),
        );
    }
    Ok(out)
}

/// What every file needs from the ones before it, and what the import reports.
#[derive(Default)]
pub struct Refs {
    pub routes: HashSet<String>,
    pub stops: HashSet<String>,
    pub services: HashSet<String>,
    pub shapes: Option<HashSet<String>>,
    pub blocks: Option<HashSet<String>>,
    pub trips: HashSet<String>,
    pub stop_times_per_trip: HashMap<String, u32>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub row_counts: BTreeMap<String, i64>,
    pub unknown_columns: BTreeMap<String, Vec<String>>,
}

#[derive(Default)]
struct Problem {
    count: usize,
    samples: Vec<String>,
}

impl Problem {
    fn add(&mut self, value: &str) {
        self.count += 1;
        if self.samples.len() < SAMPLES {
            self.samples.push(value.to_string());
        }
    }
}

#[derive(Default)]
struct Missing {
    rows: usize,
    distinct: HashSet<String>,
    samples: Vec<String>,
}

fn check_value(kind: Kind, v: &str) -> bool {
    match kind {
        Kind::Text => true,
        Kind::Int => v.parse::<i64>().is_ok_and(|n| n.unsigned_abs() <= i32::MAX as u64) && is_int(v),
        Kind::SmallInt => v.parse::<i64>().is_ok_and(|n| n.unsigned_abs() <= i16::MAX as u64) && is_int(v),
        Kind::Float => is_float(v),
        Kind::Time => parse_gtfs_time(v).is_some(),
        Kind::Date => parse_ymd(v).is_some(),
    }
}

fn is_int(v: &str) -> bool {
    let digits = v.strip_prefix('-').unwrap_or(v);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// `-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?`
fn is_float(v: &str) -> bool {
    let s = v.strip_prefix('-').unwrap_or(v);
    let (mantissa, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    let ok_mantissa = digits(int) && digits(frac) && (!int.is_empty() || !frac.is_empty());
    let ok_exp = exp.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && digits(e)
    });
    ok_mantissa && ok_exp && v.parse::<f64>().is_ok_and(f64::is_finite)
}

/// COPY text format: backslash, tab and line breaks escaped.
fn put_text(buf: &mut BytesMut, v: &str) {
    for b in v.bytes() {
        match b {
            b'\\' => buf.put_slice(b"\\\\"),
            b'\t' => buf.put_slice(b"\\t"),
            b'\n' => buf.put_slice(b"\\n"),
            b'\r' => buf.put_slice(b"\\r"),
            b => buf.put_u8(b),
        }
    }
}

/// Parse one file, check it and send its rows as COPY text chunks. Runs on a blocking thread.
fn parse_table(
    path: PathBuf,
    member: String,
    table: &'static Table,
    version_id: String,
    mut refs: Refs,
    tx: tokio::sync::mpsc::Sender<Bytes>,
) -> Result<(Refs, i64)> {
    let name = table.name;
    let mut zip = zip::ZipArchive::new(File::open(&path)?)?;
    let mut rdr = csv_reader(without_bom(zip.by_name(&member)?)?);
    let headers = trimmed_headers(&mut rdr)?;
    // spec column -> position in the file
    let positions: Vec<Option<usize>> =
        table.columns.iter().map(|c| headers.iter().position(|h| h == c.name)).collect();
    let unknown: Vec<String> =
        headers.iter().filter(|h| table.column_index(h).is_none() && !h.is_empty()).cloned().collect();
    if !unknown.is_empty() {
        refs.unknown_columns.insert(name.to_string(), unknown);
    }
    let col = |n: &str| table.column_index(n).expect("spec column");
    let mut bad: Vec<Problem> = table.columns.iter().map(|_| Problem::default()).collect();
    let mut without: Vec<usize> = vec![0; table.columns.len()];
    let mut missing: BTreeMap<(&str, &str), Missing> = BTreeMap::new(); // (column, file it should be in)
    let mut seen_keys: HashSet<String> = HashSet::new();
    let mut timezones: Vec<String> = Vec::new();
    let mut buf = BytesMut::with_capacity(CHUNK_BYTES + 4096);
    let mut rows: i64 = 0;
    let mut sent: i64 = 0; // rows with a problem of their own aren't copied: the import fails with every problem listed
    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {
        if record.iter().all(|v| v.trim().is_empty()) {
            continue; // a blank line
        }
        if record.len() > headers.len() {
            bail!("{name}.txt line {}: more fields than the header has", record.position().map_or(0, |p| p.line()));
        }
        rows += 1;
        let mut values: Vec<Option<&str>> = Vec::with_capacity(table.columns.len());
        let mut row_ok = true;
        for (i, column) in table.columns.iter().enumerate() {
            let raw = positions[i].and_then(|p| record.get(p)).filter(|v| !v.trim().is_empty());
            let v = match (raw, column.kind) {
                (Some(v), Kind::Text) => Some(v),
                (Some(v), _) => Some(v.trim()),
                (None, _) => None,
            };
            match v {
                None if column.required => {
                    without[i] += 1;
                    row_ok = false;
                }
                Some(v) if !check_value(column.kind, v) => {
                    bad[i].add(v);
                    row_ok = false;
                }
                _ => {}
            }
            values.push(v);
        }
        let get = |n: &str| values[col(n)];
        let mut reference =
            |column: &'static str, parent: &'static str, set: Option<&HashSet<String>>, v: Option<&str>| {
                if let (Some(set), Some(v)) = (set, v)
                    && !set.contains(v)
                {
                    let m = missing.entry((column, parent)).or_default();
                    m.rows += 1;
                    if m.distinct.insert(v.to_string()) && m.samples.len() < SAMPLES {
                        m.samples.push(v.to_string());
                    }
                }
            };
        match name {
            "agency" => timezones.extend(get("agency_timezone").map(str::to_string)),
            "routes" => refs.routes.extend(get("route_id").map(str::to_string)),
            "stops" => refs.stops.extend(get("stop_id").map(str::to_string)),
            "calendar" | "calendar_dates" => refs.services.extend(get("service_id").map(str::to_string)),
            "shapes" => {
                if let Some(id) = get("shape_id") {
                    refs.shapes.get_or_insert_default().insert(id.to_string());
                }
            }
            "blocks" => {
                if let Some(id) = get("block_id") {
                    refs.blocks.get_or_insert_default().insert(id.to_string());
                }
            }
            "trips" => {
                reference("route_id", "routes", Some(&refs.routes), get("route_id"));
                reference("service_id", "services", Some(&refs.services), get("service_id"));
                reference("shape_id", "shapes", refs.shapes.as_ref(), get("shape_id"));
                reference("block_id", "blocks", refs.blocks.as_ref(), get("block_id"));
                if let Some(id) = get("trip_id") {
                    refs.trips.insert(id.to_string());
                }
            }
            "stop_times" => {
                reference("trip_id", "trips", Some(&refs.trips), get("trip_id"));
                reference("stop_id", "stops", Some(&refs.stops), get("stop_id"));
                if let Some(id) = get("trip_id") {
                    match refs.stop_times_per_trip.get_mut(id) {
                        Some(n) => *n += 1,
                        None => {
                            refs.stop_times_per_trip.insert(id.to_string(), 1);
                        }
                    }
                }
            }
            _ => {}
        }
        // primary keys of the small files are checked here (with their values); those of stop_times and shapes by
        // Postgres when the partition is attached
        let key = match name {
            "stops" => get("stop_id"),
            "routes" => get("route_id"),
            "trips" => get("trip_id"),
            "calendar" => get("service_id"),
            "blocks" => get("block_id"),
            _ => None,
        };
        if let Some(k) = key
            && !seen_keys.insert(k.to_string())
        {
            refs.errors.push(format!("{name}: rows share a primary key, e.g. {k:?}"));
        }

        if !row_ok {
            continue;
        }
        sent += 1;
        buf.put_slice(version_id.as_bytes());
        for v in &values {
            buf.put_u8(b'\t');
            match v {
                Some(v) => put_text(&mut buf, v),
                None => buf.put_slice(b"\\N"),
            }
        }
        buf.put_u8(b'\n');
        if buf.len() >= CHUNK_BYTES && tx.blocking_send(buf.split().freeze()).is_err() {
            bail!("COPY of {name} ended early");
        }
    }
    if !buf.is_empty() && tx.blocking_send(buf.freeze()).is_err() {
        bail!("COPY of {name} ended early");
    }

    for (i, column) in table.columns.iter().enumerate() {
        if without[i] > 0 {
            refs.errors.push(format!("{name}.{}: {} rows without a required value", column.name, without[i]));
        }
        if bad[i].count > 0 {
            refs.errors.push(format!(
                "{name}.{}: {} values are not a valid {}, e.g. {:?}",
                column.name,
                bad[i].count,
                column.kind.name(),
                bad[i].samples
            ));
        }
    }
    for ((column, parent), m) in missing {
        let msg = format!(
            "{name}.{column}: {} rows reference unknown {parent} ({} distinct), e.g. {:?}",
            m.rows,
            m.distinct.len(),
            m.samples
        );
        // a trip's shape or block that isn't there is a gap in the data, not a broken timetable
        if matches!(parent, "shapes" | "blocks") { refs.warnings.push(msg) } else { refs.errors.push(msg) }
    }
    if name == "agency" {
        let distinct: HashSet<&String> = timezones.iter().collect();
        if distinct.len() != 1 {
            refs.errors.push(format!("agency.txt must have exactly one timezone, found {distinct:?}"));
        }
    }
    refs.row_counts.insert(name.to_string(), rows);
    Ok((refs, sent))
}

/// Create the table's partition for this version, fill it from the zip and attach it. Returns `refs` with this file's
/// part added (its errors too: the caller fails the import once every file was read).
pub async fn load_table(
    tx: &Transaction<'_>,
    zip: &Path,
    member: Option<&str>,
    table: &'static Table,
    fv: &super::FeedVersion,
    refs: Refs,
) -> Result<Refs> {
    let part = format!("gtfs.\"{}\"", fv.partition(table.name));
    tx.batch_execute(&format!(
        "CREATE TABLE {part} (LIKE gtfs.{t} INCLUDING DEFAULTS INCLUDING GENERATED, \
         CONSTRAINT version_chk CHECK (feed_version_id = '{id}'))",
        t = table.name,
        id = fv.id
    ))
    .await?;
    let mut refs = refs;
    if let Some(member) = member {
        let cols: Vec<&str> = table.columns.iter().map(|c| c.name).collect();
        let sink = tx.copy_in(&format!("COPY {part} (feed_version_id, {}) FROM STDIN", cols.join(", "))).await?;
        futures_util::pin_mut!(sink);
        let (chunks_tx, mut chunks) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (path, member, id) = (zip.to_path_buf(), member.to_string(), fv.id.clone());
        let parser = tokio::task::spawn_blocking(move || parse_table(path, member, table, id, refs, chunks_tx));
        let mut sent = Ok(());
        while let Some(chunk) = chunks.recv().await {
            if let Err(e) = sink.send(chunk).await {
                sent = Err(e);
                break;
            }
        }
        drop(chunks); // a failed COPY stops the parser
        let parsed = parser.await.map_err(|e| anyhow!("parsing {}.txt: {e}", table.name))?;
        sent.with_context(|| format!("COPY {}", table.name))?;
        let (parsed, sent) = parsed?;
        refs = parsed;
        let copied = sink.finish().await.with_context(|| format!("COPY {}", table.name))?;
        if copied as i64 != sent {
            bail!("{}: {sent} rows sent, {copied} stored", table.name);
        }
    }
    tx.batch_execute(&format!("ALTER TABLE gtfs.{} ATTACH PARTITION {part} FOR VALUES IN ('{}')", table.name, fv.id))
        .await
        .with_context(|| format!("{}: attaching the partition", table.name))?;
    // fresh partitions have no statistics until autovacuum gets to them; the derive and stop matching queries would be
    // planned blind (nested loops over millions of stop_times)
    tx.batch_execute(&format!("ANALYZE {part}")).await?;
    Ok(refs)
}

/// Files and checks across files once everything is read.
pub fn finish_checks(refs: &mut Refs) {
    let short = refs.stop_times_per_trip.values().filter(|n| **n < 2).count();
    if short > 0 {
        refs.warnings.push(format!("{short} trips have fewer than 2 stop_times"));
    }
    let without = refs.trips.iter().filter(|t| !refs.stop_times_per_trip.contains_key(*t)).count();
    if without > 0 {
        refs.warnings.push(format!("{without} trips have no stop_times"));
    }
}

/// Required files, and the files we don't store (warned about).
pub fn check_files(names: &HashMap<String, String>) -> (Vec<String>, Vec<String>) {
    let mut errors = Vec::new();
    for t in TABLES.iter().filter(|t| t.required_file) {
        if !names.contains_key(&t.filename()) {
            errors.push(format!("missing required file {}", t.filename()));
        }
    }
    if !names.contains_key("calendar.txt") && !names.contains_key("calendar_dates.txt") {
        errors.push("missing calendar.txt and calendar_dates.txt (one is required)".into());
    }
    let known: HashSet<String> = TABLES.iter().map(|t| t.filename()).collect();
    let mut unknown: Vec<String> = names.keys().filter(|n| !known.contains(*n)).cloned().collect();
    unknown.sort();
    (errors, unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_checks() {
        assert!(
            check_value(Kind::Float, "50.0658") && check_value(Kind::Float, "-.5") && check_value(Kind::Float, "1e-3")
        );
        assert!(
            !check_value(Kind::Float, "50,1") && !check_value(Kind::Float, ".") && !check_value(Kind::Float, "nan")
        );
        assert!(check_value(Kind::Int, "-12") && !check_value(Kind::Int, "1.0") && !check_value(Kind::Int, "+1"));
        assert!(!check_value(Kind::SmallInt, "40000") && check_value(Kind::SmallInt, "3"));
        assert!(check_value(Kind::Time, "25:10:00") && !check_value(Kind::Time, "6am"));
        assert!(check_value(Kind::Date, "20261124") && !check_value(Kind::Date, "20260230"));
    }

    #[test]
    fn copy_escapes() {
        let mut b = BytesMut::new();
        put_text(&mut b, "a\tb\\c\nd");
        assert_eq!(&b[..], b"a\\tb\\\\c\\nd");
    }
}
