//! Static timetables: download (conditional, deduplicated by content hash), archive, import as a new version.
//!
//! An import is one transaction: per GTFS file a standalone partition is created, filled with COPY and attached
//! (readers never see half a version); derived data is built, stops are matched and the version becomes `ready`. On any
//! error everything rolls back and a `failed` version row records why: the versions already serving keep serving.

pub mod derive;
pub mod load;
pub mod spec;
pub mod stops;
pub mod versions;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_postgres::Client;
use tracing::{info, warn};

use crate::config::{FeedConfig, Settings};
use crate::http::{Retry, fetch_to_file};
use crate::util::{partition_name, version_id};
use stops::StopRules;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedVersion {
    /// core.feed_version.id (`kmk_t_v3`), the key every table joins on
    pub id: String,
    pub feed_id: String,
    /// 1, 2, 3, ... per feed
    pub version: i32,
}

impl FeedVersion {
    pub fn partition(&self, table: &str) -> String {
        partition_name(table, &self.feed_id, self.version)
    }
}

/// Every table partitioned per version: the GTFS tables and core.stop_departure.
pub fn partitioned_tables() -> Vec<(&'static str, &'static str)> {
    spec::TABLES.iter().map(|t| ("gtfs", t.name)).chain([("core", "stop_departure")]).collect()
}

#[derive(Debug, Clone, Default)]
pub struct SourceMeta {
    pub sha256: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub archive_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Ready,
    Failed,
    /// a zip whose import failed before: not imported again until it changes or `--retry`
    PreviouslyFailed,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Ready => "ready",
            Status::Failed => "failed",
            Status::PreviouslyFailed => "previously_failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImportResult {
    #[allow(dead_code)] // tests read it
    pub feed_version_id: String,
    pub version: i32,
    pub status: Status,
    #[allow(dead_code)] // tests read it
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub seconds: f64,
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut h = Sha256::new();
    std::io::copy(&mut std::fs::File::open(path)?, &mut h)?;
    Ok(hex::encode(h.finalize()))
}

pub fn archive_path(archive_dir: &Path, feed_id: &str, sha256: &str) -> PathBuf {
    archive_dir.join(feed_id).join(format!("{sha256}.zip"))
}

/// Store `src` as `dest` unless it is already there (same name = same content).
fn archive_put(src: &Path, dest: &Path, move_file: bool) -> Result<()> {
    std::fs::create_dir_all(dest.parent().unwrap())?;
    if dest.exists() {
        return Ok(());
    }
    if move_file {
        if std::fs::rename(src, dest).is_err() {
            std::fs::copy(src, dest)?; // another filesystem
        }
    } else {
        std::fs::copy(src, dest)?;
    }
    Ok(())
}

/// The version number of this zip: its existing one (a retry or re-import), else the feed's next number. The counter
/// is bumped in the caller's transaction, so a rolled-back import gives its number back, and the row lock serializes
/// imports of the same feed.
async fn version_number(tx: &impl tokio_postgres::GenericClient, feed_id: &str, sha256: &str) -> Result<i32> {
    if let Some(r) = tx
        .query_opt("SELECT version FROM core.feed_version WHERE feed_id = $1 AND sha256 = $2", &[&feed_id, &sha256])
        .await?
    {
        return Ok(r.get(0));
    }
    match tx
        .query_opt(
            "UPDATE core.feed SET last_version = last_version + 1 WHERE feed_id = $1 RETURNING last_version",
            &[&feed_id],
        )
        .await?
    {
        Some(r) => Ok(r.get(0)),
        None => bail!("unknown feed_id {feed_id:?} (not synced from feeds.yml)"),
    }
}

async fn record_failure(
    client: &mut Client,
    feed_id: &str,
    meta: &SourceMeta,
    error: &str,
    warnings: &[String],
) -> Result<FeedVersion> {
    let tx = client.transaction().await?;
    let number = version_number(&tx, feed_id, &meta.sha256).await?;
    let row = tx
        .query_one(
            "INSERT INTO core.feed_version (feed_id, version, sha256, etag, last_modified, archive_path, status, error, warnings)
             VALUES ($1, $2, $3, $4, $5, $6, 'failed', $7, $8)
             ON CONFLICT (feed_id, version) DO UPDATE SET status = 'failed', error = excluded.error, warnings = excluded.warnings
             RETURNING id",
            &[&feed_id, &number, &meta.sha256, &meta.etag, &meta.last_modified, &meta.archive_path, &error, &json!(warnings)],
        )
        .await?;
    tx.commit().await?;
    Ok(FeedVersion { id: row.get(0), feed_id: feed_id.into(), version: number })
}

/// Import a zip as a new version of a feed. Never fails: a failure is recorded on a `failed` version and returned.
pub async fn import_zip(
    client: &mut Client,
    feed_id: &str,
    zip: &Path,
    meta: &SourceMeta,
    rules: &StopRules,
    now: f64,
) -> ImportResult {
    let started = Instant::now();
    let mut warnings = Vec::new();
    match import(client, feed_id, zip, meta, rules, now, &mut warnings).await {
        Ok((fv, retired)) => {
            let seconds = (started.elapsed().as_secs_f64() * 10.0).round() / 10.0;
            info!(feed = feed_id, version = fv.version, seconds, ?retired, "import_ready");
            ImportResult {
                feed_version_id: fv.id,
                version: fv.version,
                status: Status::Ready,
                warnings,
                error: None,
                seconds,
            }
        }
        Err(e) => {
            let error = format!("{e:#}");
            warn!(feed = feed_id, sha256 = &meta.sha256[..12.min(meta.sha256.len())], error = %error, "import_failed");
            let cut: String = error.chars().take(5000).collect();
            let fv = match record_failure(client, feed_id, meta, &cut, &warnings).await {
                Ok(fv) => fv,
                Err(e2) => {
                    warn!(feed = feed_id, error = %format!("{e2:#}"), "import_failure_not_recorded");
                    FeedVersion { id: String::new(), feed_id: feed_id.into(), version: 0 }
                }
            };
            let seconds = (started.elapsed().as_secs_f64() * 10.0).round() / 10.0;
            ImportResult {
                feed_version_id: fv.id,
                version: fv.version,
                status: Status::Failed,
                warnings,
                error: Some(error),
                seconds,
            }
        }
    }
}

async fn import(
    client: &mut Client,
    feed_id: &str,
    zip: &Path,
    meta: &SourceMeta,
    rules: &StopRules,
    now: f64,
    warnings: &mut Vec<String>,
) -> Result<(FeedVersion, Vec<i32>)> {
    let names = load::members(zip)?;
    let (errors, unknown_files) = load::check_files(&names);
    if !errors.is_empty() {
        bail!("validation failed: {}", errors.join("; "));
    }
    for name in &unknown_files {
        warnings.push(if name == "frequencies.txt" {
            // its trips' stop_times are templates repeated at intervals: they would run once at the template's times
            "frequencies.txt: frequency-based trips are not supported, their trips run once at their stop_times".into()
        } else {
            format!("file not in the spec is not stored: {name}")
        });
    }
    let agency = load::small_table(zip, &names["agency.txt"])?;
    let timezone = agency.iter().find_map(|r| r.get("agency_timezone").map(|t| t.trim().to_string()));
    if let Some(tz) = &timezone
        && tz.parse::<chrono_tz::Tz>().is_err()
    {
        bail!("validation failed: agency_timezone {tz:?} is not a timezone");
    }
    let source_version = match names.get("feed_info.txt") {
        Some(m) => load::small_table(zip, m)?.first().and_then(|r| r.get("feed_version").cloned()),
        None => None,
    };

    let mut tx = client.transaction().await?;
    let number = version_number(&tx, feed_id, &meta.sha256).await?;
    let row = tx
        .query_one(
            "INSERT INTO core.feed_version
                 (feed_id, version, sha256, etag, last_modified, archive_path, status, timezone, source_feed_version)
             VALUES ($1, $2, $3, $4, $5, $6, 'importing', $7, $8)
             -- a retry of a failed version: that row was written without most of these
             ON CONFLICT (feed_id, version) DO UPDATE
             SET status = 'importing', error = NULL, etag = excluded.etag, last_modified = excluded.last_modified,
                 archive_path = excluded.archive_path, timezone = excluded.timezone,
                 source_feed_version = excluded.source_feed_version
             RETURNING id",
            &[
                &feed_id,
                &number,
                &meta.sha256,
                &meta.etag,
                &meta.last_modified,
                &meta.archive_path,
                &timezone,
                &source_version,
            ],
        )
        .await?;
    let fv = FeedVersion { id: row.get(0), feed_id: feed_id.into(), version: number };

    let mut refs = load::Refs::default();
    for table in spec::TABLES {
        let member = names.get(&table.filename()).map(String::as_str);
        refs = load::load_table(&tx, zip, member, table, &fv, refs).await?;
    }
    load::finish_checks(&mut refs);
    for (name, cols) in &refs.unknown_columns {
        warnings.push(format!("{name}: columns not in the GTFS spec are not stored: {cols:?}"));
    }
    warnings.append(&mut refs.warnings);
    for w in warnings.iter() {
        warn!(feed = feed_id, msg = %w, "gtfs_validation_warning");
    }
    if !refs.errors.is_empty() {
        let first: Vec<&str> = refs.errors.iter().take(10).map(String::as_str).collect();
        bail!("validation failed: {}", first.join("; "));
    }
    info!(feed = feed_id, version = number, rows = ?refs.row_counts, "gtfs_loaded");

    let (services, first, last) = derive::build_service_dates(&tx, &fv.id).await?;
    let shapes = derive::build_shape_lines(&tx, &fv.id).await?;
    let matched = stops::apply::apply_matches(&tx, feed_id, &fv.id, rules).await.context("stop matching")?;
    warnings.extend(matched.errors.iter().map(|e| format!("stop matching: {e}")));
    let directions = derive::build_stop_directions(&tx, &fv.id).await?;
    let departures = derive::build_stop_departures(&tx, &fv).await?;
    info!(
        feed = feed_id, version = number, service_dates = services, first = ?first, last = ?last, shape_lines = shapes,
        stops = matched.stops, methods = ?matched.methods, stop_directions = directions, stop_departures = departures,
        "derived"
    );

    let mut counts: BTreeMap<String, serde_json::Value> =
        refs.row_counts.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    counts.insert("unknown_columns".into(), json!(refs.unknown_columns));
    counts.insert("unknown_files".into(), json!(unknown_files));
    counts.insert("stop_matching".into(), json!(matched.methods));
    tx.execute(
        "UPDATE core.feed_version SET status = 'ready', imported_at = now(), row_counts = $1, warnings = $2 WHERE id = $3",
        &[&json!(counts), &json!(warnings), &fv.id],
    )
    .await?;
    versions::recompute_service_day_version(&tx, feed_id).await?;
    let today = versions::feed_today(&tx, feed_id, now).await?;
    let retired = match versions::retire_versions(&mut tx, feed_id, today).await {
        Ok(r) => r,
        Err(e) if e.is::<versions::RetirementBusy>() => Vec::new(), // the daily retirement catches up
        Err(e) => return Err(e),
    };
    tx.commit().await?;
    Ok((fv, retired))
}

// ------------------------------------------------------------------------------------------------- polling

/// (version, status) of a zip this feed has seen before.
async fn known_version(
    c: &impl tokio_postgres::GenericClient,
    feed_id: &str,
    sha256: &str,
) -> Result<Option<(i32, String)>> {
    Ok(c.query_opt(
        "SELECT version, status FROM core.feed_version WHERE feed_id = $1 AND sha256 = $2",
        &[&feed_id, &sha256],
    )
    .await?
    .map(|r| (r.get(0), r.get(1))))
}

async fn save_fetch_state(c: &Client, feed_id: &str, etag: Option<&str>, last_modified: Option<&str>) -> Result<()> {
    c.execute(
        "UPDATE core.feed SET static_etag = $1, static_last_modified = $2, static_checked_at = now() WHERE feed_id = $3",
        &[&etag, &last_modified, &feed_id],
    )
    .await?;
    Ok(())
}

pub fn rules(feed: &FeedConfig, settings: &Settings) -> StopRules {
    StopRules::of(feed, Some(settings.osm_stops_file.clone()))
}

/// Download the feed's zip if it changed and import it. Returns the result of an import, if any. A zip whose import
/// failed before isn't imported again (PreviouslyFailed) unless `retry_failed`: then it is, keeping its number.
pub async fn check_static(
    feed: &FeedConfig,
    settings: &Settings,
    http: &reqwest::Client,
    conditional: bool,
    retry_failed: bool,
) -> Result<Option<ImportResult>> {
    let feed_id = feed.feed_id.as_str();
    let mut client = crate::db::connect(&settings.database_url).await?;
    let (mut etag, mut last_modified) = (None, None);
    if conditional
        && let Some(r) = client
            .query_opt("SELECT static_etag, static_last_modified FROM core.feed WHERE feed_id = $1", &[&feed_id])
            .await?
    {
        (etag, last_modified) = (r.get::<_, Option<String>>(0), r.get::<_, Option<String>>(1));
    }
    let tmp = settings.archive_dir.join("tmp").join(format!("{feed_id}-{:016x}.zip", rand::random::<u64>()));
    let retry = Retry { retries: settings.http_retries, delay: settings.http_retry_delay };
    let fetched = fetch_to_file(
        http,
        &feed.static_url,
        &tmp,
        load::validate_zip,
        etag.as_deref(),
        last_modified.as_deref(),
        &retry,
    )
    .await?;
    if fetched.value.is_none() {
        save_fetch_state(&client, feed_id, etag.as_deref(), last_modified.as_deref()).await?;
        return Ok(None);
    }
    let result = async {
        let tmp2 = tmp.clone();
        let sha = tokio::task::spawn_blocking(move || sha256_file(&tmp2)).await??;
        let short = &sha[..12];
        let (fe, fm) = (fetched.etag.as_deref(), fetched.last_modified.as_deref());
        match known_version(&client, feed_id, &sha).await? {
            Some((version, status)) if status == "ready" || status == "importing" => {
                info!(feed = feed_id, sha256 = short, version, "static_unchanged");
                save_fetch_state(&client, feed_id, fe, fm).await?;
                return Ok(None);
            }
            Some((version, status)) if status == "failed" && !retry_failed => {
                warn!(feed = feed_id, sha256 = short, version, "static_previously_failed");
                save_fetch_state(&client, feed_id, fe, fm).await?;
                return Ok(Some(ImportResult {
                    feed_version_id: version_id(feed_id, version),
                    version,
                    status: Status::PreviouslyFailed,
                    warnings: vec![],
                    error: None,
                    seconds: 0.0,
                }));
            }
            Some((version, status)) if status == "failed" => {
                info!(feed = feed_id, sha256 = short, version, "static_retrying_failed")
            }
            _ => {}
        }
        let stored = archive_path(&settings.archive_dir, feed_id, &sha);
        archive_put(&tmp, &stored, true)?;
        let meta = SourceMeta {
            sha256: sha.clone(),
            etag: fetched.etag.clone(),
            last_modified: fetched.last_modified.clone(),
            archive_path: Some(stored.display().to_string()),
        };
        info!(feed = feed_id, sha256 = short, bytes = fetched.size, "static_importing");
        let result = import_zip(&mut client, feed_id, &stored, &meta, &rules(feed, settings), settings.now()).await;
        if result.status == Status::Ready {
            save_fetch_state(&client, feed_id, fe, fm).await?;
        }
        Ok(Some(result))
    }
    .await;
    let _ = std::fs::remove_file(&tmp);
    result
}

/// `import-static --file`: import a local zip (archived first) unless that exact zip is ready already.
pub async fn import_file(feed: &FeedConfig, settings: &Settings, path: &Path) -> Result<Option<ImportResult>> {
    load::validate_zip(path)?;
    let sha = sha256_file(path)?;
    let stored = archive_path(&settings.archive_dir, &feed.feed_id, &sha);
    archive_put(path, &stored, false)?;
    let mut client = crate::db::connect(&settings.database_url).await?;
    match known_version(&client, &feed.feed_id, &sha).await? {
        Some((version, status)) if status == "ready" => {
            info!(feed = %feed.feed_id, version, "already_imported");
            return Ok(None);
        }
        // a leftover from a crash or a failure: a manual import retries it
        Some((version, _)) => {
            client
                .execute(
                    "DELETE FROM core.feed_version WHERE feed_id = $1 AND version = $2 AND status <> 'ready'",
                    &[&feed.feed_id, &version],
                )
                .await?;
        }
        None => {}
    }
    let meta = SourceMeta { sha256: sha, archive_path: Some(stored.display().to_string()), ..Default::default() };
    Ok(Some(import_zip(&mut client, &feed.feed_id, &stored, &meta, &rules(feed, settings), settings.now()).await))
}

/// Upsert the configured feeds into core.feed; feeds removed from feeds.yml (or disabled) become inactive.
pub async fn sync_feeds(client: &mut Client, feeds: &crate::config::FeedsFile, raw: &serde_json::Value) -> Result<()> {
    let tx = client.transaction().await?;
    let ids: Vec<String> = feeds.feeds.iter().map(|f| f.feed_id.clone()).collect();
    for (i, f) in feeds.feeds.iter().enumerate() {
        let config = raw.get("feeds").and_then(|v| v.get(i)).cloned().unwrap_or(json!({}));
        tx.execute(
            "INSERT INTO core.feed (feed_id, stop_namespace, active, config, updated_at) VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (feed_id) DO UPDATE
             SET stop_namespace = excluded.stop_namespace, active = excluded.active, config = excluded.config, updated_at = now()",
            &[&f.feed_id, &f.namespace(), &f.enabled, &config],
        )
        .await?;
    }
    tx.execute("UPDATE core.feed SET active = false, updated_at = now() WHERE feed_id <> ALL($1)", &[&ids]).await?;
    tx.commit().await?;
    Ok(())
}
