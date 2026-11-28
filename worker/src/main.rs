//! gtfs-worker: GTFS static + realtime ingestion for mobileKKM (the HTTP API is a separate program, gtfs-api).
//!
//!   gtfs-worker run                                 # the worker: migrations, then every static + realtime task
//!   gtfs-worker import-static --feed kmk_a          # download now and import if new (feed_id from feeds.yml)
//!   gtfs-worker import-static --feed kmk_a --file x.zip
//!   gtfs-worker import-static --all                 # every enabled feed, shared-stop providers first
//!   gtfs-worker import-static --feed kmk_a --retry  # also import the zip again if its import failed before
//!   gtfs-worker fetch-osm                           # download and keep OpenStreetMap's stop poles
//!   gtfs-worker retire [--feed kmk_a]               # retire versions no longer needed
//!   gtfs-worker rebuild-service-days [--feed kmk_a]
//!   gtfs-worker migrate                             # apply the schema migrations
//!   gtfs-worker healthcheck                         # exit 1 when the running worker stopped touching its heartbeat
//!   gtfs-worker replay manifest.json                # process recorded realtime files (benchmarks, tests)

mod config;
mod db;
mod http;
mod metrics;
mod realtime;
mod runner;
mod static_feed;
mod util;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

use config::{FeedsFile, Settings};
use static_feed::{ImportResult, Status};

#[derive(Parser)]
#[command(name = "gtfs-worker", about = "mobileKKM GTFS ingestion", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the worker (static + realtime tasks)
    Run,
    /// Import a static feed now
    ImportStatic {
        /// feed_id from feeds.yml
        #[arg(long)]
        feed: Option<String>,
        /// all enabled feeds
        #[arg(long)]
        all: bool,
        /// import this local zip instead of downloading
        #[arg(long)]
        file: Option<PathBuf>,
        /// import the downloaded zip even if its import failed before
        #[arg(long)]
        retry: bool,
    },
    /// Download the OpenStreetMap extract (Geofabrik) and keep its stop poles
    FetchOsm,
    /// Retire versions that no longer serve any day from yesterday on
    Retire {
        #[arg(long)]
        feed: Option<String>,
    },
    /// Recompute which version serves which service day
    RebuildServiceDays {
        #[arg(long)]
        feed: Option<String>,
    },
    /// Apply the schema migrations
    Migrate,
    /// Exit 0 while the running worker keeps touching its heartbeat file (the compose healthcheck)
    Healthcheck {
        #[arg(long, default_value_t = 120)]
        max_age: u64,
    },
    /// Process recorded realtime files into Redis, each at the time it arrived (a bench/replay.py manifest)
    Replay {
        manifest: PathBuf,
        /// write what Redis holds for a file after every `--every` of its updates (and at the end) here
        #[arg(long)]
        snapshots: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        every: usize,
    },
}

/// LOG_LEVEL as the API (Python) spells it too: DEBUG, INFO, WARNING, ERROR, CRITICAL.
fn setup_logging(level: &str) {
    let level = match level.to_lowercase().as_str() {
        "warning" => "warn".to_string(),
        "critical" | "fatal" => "error".to_string(),
        other => other.to_string(),
    };
    let filter =
        tracing_subscriber::EnvFilter::try_new(&level).unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout()); // plain lines for Docker's logs
    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_ansi(ansi).init();
}

fn report(feed_id: &str, r: &ImportResult) -> u8 {
    let error = r.error.as_deref().map(|e| format!(" - {e}")).unwrap_or_default();
    println!("{feed_id}: {} (version {}, {}s){error}", r.status.as_str(), r.version, r.seconds);
    u8::from(r.status != Status::Ready)
}

fn load_feeds(settings: &Settings) -> Result<(FeedsFile, serde_json::Value)> {
    let feeds = FeedsFile::load(&settings.feeds_config)?;
    let raw: serde_json::Value = serde_yaml::from_str(&std::fs::read_to_string(&settings.feeds_config)?)?;
    Ok((feeds, raw))
}

async fn prepare(settings: &Settings, feeds: &FeedsFile, raw: &serde_json::Value) -> Result<()> {
    let mut client = db::connect(&settings.database_url).await?;
    db::migrate(&mut client).await?;
    static_feed::sync_feeds(&mut client, feeds, raw).await
}

async fn main_async(cli: Cli, settings: Settings) -> Result<u8> {
    match cli.command {
        Command::Run => {
            let (feeds, raw) = load_feeds(&settings)?;
            Ok(runner::run(settings, feeds, raw).await? as u8)
        }
        Command::Migrate => {
            let applied = db::migrate(&mut db::connect(&settings.database_url).await?).await?;
            println!("applied migrations: {applied:?}");
            Ok(0)
        }
        Command::ImportStatic { feed, all, file, retry } => {
            let (feeds, raw) = load_feeds(&settings)?;
            prepare(&settings, &feeds, &raw).await?;
            if let Some(file) = file {
                let Some(feed) = feed else { bail!("--file needs --feed") };
                if retry {
                    bail!("--retry is for downloads; --file always imports");
                }
                return Ok(match static_feed::import_file(feeds.get(&feed)?, &settings, &file).await? {
                    None => {
                        println!("{feed}: already imported");
                        0
                    }
                    Some(r) => report(&feed, &r),
                });
            }
            let selected = match (all, feed) {
                (true, _) => feeds.import_order(),
                (false, Some(f)) => vec![feeds.get(&f)?],
                (false, None) => bail!("give --feed FEED_ID or --all"),
            };
            let http = http::client(settings.static_download_timeout, settings.http_connect_timeout)?;
            let mut code = 0;
            for f in selected {
                match static_feed::check_static(f, &settings, &http, false, retry).await {
                    Ok(None) => println!("{}: unchanged", f.feed_id),
                    Ok(Some(r)) if r.status == Status::PreviouslyFailed => {
                        println!(
                            "{}: its import failed before (version {}); --retry imports it again",
                            f.feed_id, r.version
                        );
                        code = 1;
                    }
                    Ok(Some(r)) => code |= report(&f.feed_id, &r),
                    Err(e) => {
                        println!("{}: {e:#}", f.feed_id);
                        code = 1;
                    }
                }
            }
            Ok(code)
        }
        Command::FetchOsm => {
            let n = static_feed::stops::osm::fetch_osm_poles(
                &settings.osm_stops_file,
                static_feed::stops::osm::EXTRACT_URL,
            )
            .await?;
            println!("{n} poles written to {}", settings.osm_stops_file.display());
            Ok(0)
        }
        Command::Retire { feed } => {
            let (feeds, _) = load_feeds(&settings)?;
            let ids: Vec<String> =
                feed.map_or_else(|| feeds.feeds.iter().map(|f| f.feed_id.clone()).collect(), |f| vec![f]);
            let mut client = db::connect(&settings.database_url).await?;
            let (retired, busy) = static_feed::versions::retire_all(&mut client, &ids, settings.now()).await?;
            for (f, v) in retired {
                println!("{f}: retired versions {v:?}");
            }
            for f in &busy {
                println!("{f}: busy (an import holds the tables), try again later");
            }
            Ok(u8::from(!busy.is_empty()))
        }
        Command::RebuildServiceDays { feed } => {
            let (feeds, _) = load_feeds(&settings)?;
            let ids: Vec<String> =
                feed.map_or_else(|| feeds.feeds.iter().map(|f| f.feed_id.clone()).collect(), |f| vec![f]);
            let mut client = db::connect(&settings.database_url).await?;
            let tx = client.transaction().await?;
            for f in &ids {
                println!("{f}: {} service days", static_feed::versions::recompute_service_day_version(&tx, f).await?);
            }
            tx.commit().await?;
            Ok(0)
        }
        Command::Healthcheck { max_age } => {
            let age = static_feed::stops::osm::file_age_s(std::path::Path::new(runner::HEARTBEAT_FILE));
            Ok(u8::from(age.is_none_or(|a| a > max_age as f64)))
        }
        Command::Replay { manifest, snapshots, every } => {
            let (feeds, _) = load_feeds(&settings)?;
            realtime::replay::replay(&settings, &feeds, &manifest, snapshots, every).await?;
            Ok(0)
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let settings = match Settings::from_env() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("settings: {e:#}");
            return ExitCode::from(2);
        }
    };
    if !matches!(cli.command, Command::Healthcheck { .. }) {
        setup_logging(&settings.log_level);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    match runtime.block_on(main_async(cli, settings)) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
