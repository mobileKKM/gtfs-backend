//! Imports, versions and retirement against a real Postgres: TEST_WORKER_DATABASE_URL (default the dev database's
//! `gtfs_test_worker`, recreated per run). Skipped (they pass, saying so) when Postgres isn't reachable.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use chrono::{Duration, NaiveDate};
use tokio::sync::{Mutex, MutexGuard};

use super::{ImportResult, Status, import_file, versions};
use crate::config::{FeedsFile, Settings};

static LOCK: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

fn url() -> String {
    std::env::var("TEST_WORKER_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://gtfs:gtfs@localhost:5432/gtfs_test_worker".into())
}

const FEEDS: &str = "
feeds:
  - feed_id: kmk_t
    stop_namespace: ztp
    stop_code_pattern: '(?P<station>.+)-(?P<platform_code>[^-]+)'
    static: https://example.invalid/T.zip
  - feed_id: kmk_a
    stop_namespace: ztp
    stop_code_pattern: '(?P<station>.+)-(?P<platform_code>[^-]+)'
    static: https://example.invalid/A.zip
";

struct Db {
    _guard: MutexGuard<'static, bool>,
    client: tokio_postgres::Client,
    settings: Settings,
    feeds: FeedsFile,
    dir: tempfile::TempDir,
}

/// A clean test database (one test at a time), or None when there is no Postgres.
async fn db() -> Option<Db> {
    let mut guard = LOCK.lock().await;
    let url = url();
    if !*guard {
        let (base, name) = url.rsplit_once('/').unwrap();
        let Ok(admin) = crate::db::connect(&format!("{base}/postgres")).await else {
            eprintln!("Postgres not reachable at {base}: skipped");
            return None;
        };
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")).await.unwrap();
        admin.batch_execute(&format!("CREATE DATABASE \"{name}\"")).await.unwrap();
        crate::db::migrate(&mut crate::db::connect(&url).await.unwrap()).await.unwrap();
        *guard = true;
    }
    let mut client = crate::db::connect(&url).await.unwrap();
    let parts = client
        .query(
            "SELECT n.nspname, c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r'",
            &[],
        )
        .await
        .unwrap();
    for p in parts {
        client
            .batch_execute(&format!("DROP TABLE {}.\"{}\"", p.get::<_, String>(0), p.get::<_, String>(1)))
            .await
            .unwrap();
    }
    client
        .batch_execute(
            "TRUNCATE core.stop_direction, core.stop_link, core.shape_line, core.service_day_version, core.service_date,
             core.stop, core.stop_group, core.public_id, core.feed_version, core.feed CASCADE",
        )
        .await
        .unwrap();
    let feeds: FeedsFile = serde_yaml::from_str(FEEDS).unwrap();
    super::sync_feeds(&mut client, &feeds, &serde_yaml::from_str(FEEDS).unwrap()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut settings = Settings::from_env().unwrap();
    settings.database_url = url;
    settings.archive_dir = dir.path().join("archive");
    settings.osm_stops_file = dir.path().join("none.json");
    Some(Db { _guard: guard, client, settings, feeds, dir })
}

impl Db {
    fn zip(&self, name: &str, files: &[(&str, &str)]) -> PathBuf {
        let path = self.dir.path().join(name);
        let mut z = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        for (file, text) in files {
            z.start_file(*file, zip::write::SimpleFileOptions::default()).unwrap();
            let lines: Vec<&str> = text.trim().lines().map(str::trim).collect();
            z.write_all((lines.join("\n") + "\n").as_bytes()).unwrap();
        }
        z.finish().unwrap();
        path
    }

    async fn import(&self, feed_id: &str, path: &Path) -> ImportResult {
        import_file(self.feeds.get(feed_id).unwrap(), &self.settings, path).await.unwrap().expect("not imported before")
    }

    async fn ready(&self, feed_id: &str, path: &Path) -> ImportResult {
        let r = self.import(feed_id, path).await;
        assert_eq!(r.status, Status::Ready, "{:?}", r.error);
        r
    }

    async fn rows(&self, sql: &str) -> Vec<tokio_postgres::Row> {
        self.client.query(sql, &[]).await.unwrap()
    }
}

const AGENCY: &str = "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw";

/// Like 22/23 Oct: a zip with service on `dates`, with a 25:10 night trip if `night`.
fn tiny_feed(db: &Db, name: &str, label: &str, dates: &[String], night: bool) -> PathBuf {
    let mut stop_times = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,06:00:00,06:00:00,S1,1\nT1,06:10:00,06:10:00,S2,2\n".to_string();
    let mut trips = "route_id,service_id,trip_id\nR1,service_4,T1\n".to_string();
    if night {
        stop_times += "T9,24:50:00,24:50:00,S1,1\nT9,25:10:00,25:10:00,S2,2\n";
        trips += "R1,service_4,T9\n";
    }
    let stops = format!(
        "stop_id,stop_code,stop_name,stop_lat,stop_lon\nS1,900-01,Test {label},50.0,19.9\nS2,900-02,Test {label},50.001,19.9"
    );
    let dates: String = dates.iter().map(|d| format!("service_4,{d},1\n")).collect();
    db.zip(
        name,
        &[
            ("agency.txt", AGENCY),
            ("stops.txt", &stops),
            ("routes.txt", "route_id,agency_id,route_short_name,route_type\nR1,A,1,900"),
            ("trips.txt", &trips),
            ("stop_times.txt", &stop_times),
            ("calendar_dates.txt", &format!("service_id,date,exception_type\n{dates}")),
        ],
    )
}

fn ymd(d: NaiveDate) -> String {
    d.format("%Y%m%d").to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn fixture_imports_keep_every_value_and_shared_codes_are_one_stop() {
    let Some(db) = db().await else { return };
    let fixtures = Path::new("../tests/fixtures/static");
    let t = db.ready("kmk_t", &fixtures.join("GTFS_KRK_T.zip")).await;
    let a = db.ready("kmk_a", &fixtures.join("GTFS_KRK_A.zip")).await;
    assert_eq!((t.version, a.version, t.feed_version_id.as_str()), (1, 1, "kmk_t_v1"));
    // times past 24:00 read back unchanged, codes stay text
    let r = db.rows("SELECT departure_time::text FROM gtfs.stop_times WHERE feed_version_id = 'kmk_a_v1' AND departure_time >= '24:00:00' LIMIT 1").await;
    assert!(r.iter().all(|r| r.get::<_, String>(0).starts_with("2")));
    assert_eq!(
        db.rows("SELECT count(*) FROM gtfs.blocks WHERE feed_version_id = 'kmk_t_v1'").await[0].get::<_, i64>(0),
        133
    );
    assert_eq!(db.rows("SELECT stop_code FROM gtfs.stops WHERE stop_code = '802-01' LIMIT 1").await.len(), 1);
    let rows = db
        .rows(
            "SELECT p.current_code, p.modes, array_agg(DISTINCT fv.feed_id ORDER BY fv.feed_id)
             FROM core.stop p JOIN core.stop_link l ON l.public_stop_id = p.id JOIN core.feed_version fv ON fv.id = l.feed_version_id
             WHERE p.current_code = '802-01' GROUP BY 1, 2",
        )
        .await;
    assert_eq!(rows[0].get::<_, Vec<String>>(1), ["bus", "tram"]);
    assert_eq!(rows[0].get::<_, Vec<String>>(2), ["kmk_a", "kmk_t"]);
    let groups: i64 =
        db.rows("SELECT count(DISTINCT stop_group_id) FROM core.stop WHERE current_code LIKE '802-%'").await[0].get(0);
    assert_eq!(groups, 1);
    // a new version (other bytes, a stop renamed) keeps every id and renames in place
    let before: Vec<(Option<String>, String)> = db
        .rows("SELECT current_code, id FROM core.stop ORDER BY 2")
        .await
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let src = fixtures.join("GTFS_KRK_T.zip");
    let changed = db.dir.path().join("T2.zip");
    {
        let mut zin = zip::ZipArchive::new(std::fs::File::open(&src).unwrap()).unwrap();
        let mut zout = zip::ZipWriter::new(std::fs::File::create(&changed).unwrap());
        for i in 0..zin.len() {
            let mut f = zin.by_index(i).unwrap();
            let mut data = Vec::new();
            std::io::Read::read_to_end(&mut f, &mut data).unwrap();
            if f.name() == "stops.txt" {
                data = String::from_utf8(data).unwrap().replace("Rondo Mogilskie", "Rondo Mogilskie Nowe").into_bytes();
            }
            zout.start_file(f.name(), zip::write::SimpleFileOptions::default()).unwrap();
            zout.write_all(&data).unwrap();
        }
        zout.finish().unwrap();
    }
    assert_eq!(db.ready("kmk_t", &changed).await.version, 2);
    let after: Vec<(Option<String>, String)> = db
        .rows("SELECT current_code, id FROM core.stop ORDER BY 2")
        .await
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(after, before);
    let r = db.rows("SELECT name, slug FROM core.stop WHERE current_code = '802-05'").await;
    assert_eq!(
        (r[0].get::<_, String>(0), r[0].get::<_, String>(1)),
        ("Rondo Mogilskie Nowe".into(), "rondo-mogilskie-nowe05".into())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_directions_and_departures() {
    let Some(db) = db().await else { return };
    // X: Alfa 01 -> Alfa 02 -> Beta -> Gamma; Y: Gamma -> Beta (no pickup) -> Alfa 01; Z: Alfa 02 -> Gamma
    let path = db.zip(
        "dirs.zip",
        &[
            ("agency.txt", AGENCY),
            (
                "stops.txt",
                "stop_id,stop_code,stop_name,stop_lat,stop_lon\nA1,910-01,Alfa,50.0,19.9\nA2,910-02,Alfa,50.0002,19.9\n\
                 B1,911-01,Beta,50.01,19.9\nC1,912-01,Gamma,50.02,19.9",
            ),
            ("routes.txt", "route_id,agency_id,route_short_name,route_type\nR1,A,1,0\nR2,A,2,0"),
            ("trips.txt", "route_id,service_id,trip_id,trip_headsign\nR1,s,X,Gamma\nR1,s,Y,Alfa\nR2,s,Z,Gamma"),
            (
                "stop_times.txt",
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type\n\
                 X,06:00:00,06:00:00,A1,1,\nX,06:01:00,06:01:00,A2,2,\nX,06:05:00,06:05:00,B1,3,\nX,06:09:00,06:09:00,C1,4,\n\
                 Y,07:00:00,07:00:00,C1,1,\nY,07:04:00,07:04:00,B1,2,1\nY,07:08:00,07:08:00,A1,3,\n\
                 Z,08:00:00,08:00:00,A2,1,\nZ,08:09:00,08:09:00,C1,2,",
            ),
            ("calendar_dates.txt", "service_id,date,exception_type\ns,20270115,1"),
        ],
    );
    db.ready("kmk_t", &path).await;
    let rows = db
        .rows(
            "SELECT p.current_code, g.current_code, d.route_id, d.headsign, d.trips FROM core.stop_direction d
             JOIN core.stop p ON p.id = d.public_stop_id JOIN core.stop_group g ON g.id = d.next_group_id ORDER BY 1, 2, 3",
        )
        .await;
    let got: Vec<(String, String, String, String, i32)> =
        rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))).collect();
    let want = [
        ("910-01", "911", "R1", "Gamma", 1), // not "towards Alfa": the next stop is in the same group
        ("910-02", "911", "R1", "Gamma", 1),
        ("910-02", "912", "R2", "Gamma", 1),
        ("911-01", "912", "R1", "Gamma", 1), // Y has no pickup at Beta
        ("912-01", "911", "R1", "Alfa", 1),
    ];
    assert_eq!(got, want.map(|(a, b, c, d, e)| (a.to_string(), b.to_string(), c.to_string(), d.to_string(), e)));
    // what boards read: no last stops, no stop without pickup
    let rows = db
        .rows(
            "SELECT p.current_code, d.departure_s, d.trip_id, d.on_demand FROM core.stop_departure d
             JOIN core.stop p ON p.id = d.public_stop_id ORDER BY d.departure_s",
        )
        .await;
    let got: Vec<(String, i32, String, bool)> = rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect();
    let want = [
        ("910-01", 21600, "X"),
        ("910-02", 21660, "X"),
        ("911-01", 21900, "X"),
        ("912-01", 25200, "Y"),
        ("910-02", 28800, "Z"),
    ];
    assert_eq!(got, want.map(|(a, b, c)| (a.to_string(), b, c.to_string(), false)));
}

#[tokio::test(flavor = "multi_thread")]
async fn overnight_update_keeps_old_version_for_previous_service_day_until_it_is_over() {
    let Some(mut db) = db().await else { return };
    let d = chrono::Local::now().date_naive() - Duration::days(1);
    let days: Vec<String> = (0..3).map(|i| ymd(d + Duration::days(i))).collect();
    let old_zip = tiny_feed(&db, "old.zip", "old", &days[..2], true);
    let new_zip = tiny_feed(&db, "new.zip", "new", &days[1..], false);
    let old = db.ready("kmk_t", &old_zip).await;
    let new = db.ready("kmk_t", &new_zip).await;
    let served: Vec<(NaiveDate, String)> = db
        .rows("SELECT service_date, feed_version_id FROM core.service_day_version WHERE feed_id = 'kmk_t' ORDER BY 1")
        .await
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let want: Vec<(NaiveDate, String)> = vec![
        (d, old.feed_version_id.clone()),
        (d + Duration::days(1), new.feed_version_id.clone()),
        (d + Duration::days(2), new.feed_version_id.clone()),
    ];
    assert_eq!(served, want);
    let mut tx = db.client.transaction().await.unwrap();
    // while day D is yesterday the old version still serves its night trips
    assert!(versions::retire_versions(&mut tx, "kmk_t", d + Duration::days(1)).await.unwrap().is_empty());
    assert_eq!(versions::retire_versions(&mut tx, "kmk_t", d + Duration::days(2)).await.unwrap(), [1]);
    tx.commit().await.unwrap();
    let status: String = db.rows("SELECT status FROM core.feed_version WHERE id = 'kmk_t_v1'").await[0].get(0);
    assert_eq!(status, "retired");
    for (table, gone) in [
        ("gtfs.stop_times__kmk_t__v1", true),
        ("core.stop_departure__kmk_t__v1", true),
        ("gtfs.stop_times__kmk_t__v2", false),
    ] {
        let r = db.client.query_one("SELECT to_regclass($1)::text", &[&table]).await.unwrap();
        assert_eq!(r.get::<_, Option<String>>(0).is_none(), gone, "{table}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_feed_fails_and_keeps_the_current_version() {
    let Some(db) = db().await else { return };
    let today = ymd(chrono::Local::now().date_naive());
    let good_zip = tiny_feed(&db, "good.zip", "good", &[today], false);
    let good = db.ready("kmk_t", &good_zip).await;
    let bad = db.zip(
        "bad.zip",
        &[
            ("agency.txt", AGENCY),
            ("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nS1,X,50.0,19.9"),
            ("routes.txt", "route_id,agency_id,route_short_name,route_type\nR1,A,1,900"),
            ("trips.txt", "route_id,service_id,trip_id\nR1,service_4,T1"),
            ("stop_times.txt", "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,6am,06:00:00,S1,1\nT1,06:10:00,06:10:00,NOPE,2"),
            ("calendar_dates.txt", "service_id,date,exception_type\nservice_4,20261024,1"),
        ],
    );
    let r = db.import("kmk_t", &bad).await;
    assert_eq!(r.status, Status::Failed);
    let error = r.error.unwrap();
    assert!(error.contains("arrival_time") && error.contains("unknown stops"), "{error}");
    let statuses: Vec<(i32, String)> = db
        .rows("SELECT version, status FROM core.feed_version ORDER BY 1")
        .await
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(statuses, [(1, "ready".to_string()), (2, "failed".to_string())]);
    assert_eq!((good.version, r.version), (1, 2));
    let served: Vec<String> = db
        .rows("SELECT DISTINCT feed_version_id FROM core.service_day_version")
        .await
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(served, [good.feed_version_id]);
    // a failed zip imported again by hand (import-static --file) is a new attempt under the next number
    let again = db.import("kmk_t", &bad).await;
    assert_eq!((again.status, again.version), (Status::Failed, 3));
}

#[tokio::test(flavor = "multi_thread")]
async fn busy_tables_postpone_retirement_and_leave_the_transaction_usable() {
    let Some(mut db) = db().await else { return };
    let d = chrono::Local::now().date_naive() - Duration::days(1);
    let old_zip = tiny_feed(&db, "old.zip", "old", &[ymd(d)], false);
    let new_zip = tiny_feed(&db, "new.zip", "new", &[ymd(d + Duration::days(1))], false);
    db.ready("kmk_t", &old_zip).await;
    db.ready("kmk_t", &new_zip).await;
    // another session reads the parent table in an open transaction (as an API query or an import would)
    let mut other = crate::db::connect(&url()).await.unwrap();
    let reader = other.transaction().await.unwrap();
    reader.query("SELECT count(*) FROM gtfs.stop_times", &[]).await.unwrap();
    let mut tx = db.client.transaction().await.unwrap();
    let started = std::time::Instant::now();
    let err = versions::retire_versions(&mut tx, "kmk_t", d + Duration::days(2)).await.unwrap_err();
    assert!(err.is::<versions::RetirementBusy>(), "{err:#}");
    assert!(started.elapsed().as_secs_f64() < 5.0);
    tx.query("SELECT 1", &[]).await.unwrap(); // still usable
    tx.rollback().await.unwrap();
    reader.rollback().await.unwrap();
    let mut tx = db.client.transaction().await.unwrap();
    assert_eq!(versions::retire_versions(&mut tx, "kmk_t", d + Duration::days(2)).await.unwrap(), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_migrated_by_alembic_is_taken_over() {
    let Some(db) = db().await else { return };
    db.client
        .batch_execute(
            "DELETE FROM public.schema_migration; CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY);
             INSERT INTO public.alembic_version VALUES ('0002')",
        )
        .await
        .unwrap();
    let applied = crate::db::migrate(&mut crate::db::connect(&url()).await.unwrap()).await.unwrap();
    assert!(applied.is_empty());
    let done: Vec<i32> =
        db.rows("SELECT version FROM public.schema_migration ORDER BY 1").await.iter().map(|r| r.get(0)).collect();
    assert_eq!(done, [1, 2]);
    db.client.batch_execute("DROP TABLE public.alembic_version").await.unwrap();
}
