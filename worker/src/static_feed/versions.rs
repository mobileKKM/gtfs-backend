//! Which version serves which service day, and retirement of versions nobody needs anymore.

use anyhow::Result;
use chrono::{Duration, NaiveDate};
use chrono_tz::Tz;
use tokio_postgres::GenericClient;
use tracing::{info, warn};

use super::{FeedVersion, partitioned_tables};

/// Dropping a partition needs an exclusive lock on its parent. While that lock is waited for, every new read of the
/// table queues behind it, so a drop waiting for an import would stall the API that long: waiting is capped and the
/// retirement postponed instead.
pub const LOCK_TIMEOUT_MS: u32 = 2000;

#[derive(Debug)]
pub struct RetirementBusy;

impl std::fmt::Display for RetirementBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the partitions are locked (an import holds them); nothing retired")
    }
}

impl std::error::Error for RetirementBusy {}

/// For each date, the newest ready version that has any service that day serves it (so the night trips of an old
/// version survive an update that starts the next day).
pub async fn recompute_service_day_version(c: &impl GenericClient, feed_id: &str) -> Result<u64> {
    c.execute("DELETE FROM core.service_day_version WHERE feed_id = $1", &[&feed_id]).await?;
    Ok(c.execute(
        "INSERT INTO core.service_day_version (feed_id, service_date, feed_version_id)
             SELECT DISTINCT ON (sd.date) fv.feed_id, sd.date, fv.id
             FROM core.service_date sd JOIN core.feed_version fv ON fv.id = sd.feed_version_id
             WHERE fv.feed_id = $1 AND fv.status = 'ready'
             ORDER BY sd.date, fv.version DESC",
        &[&feed_id],
    )
    .await?)
}

/// Today in the feed's timezone (from its newest version's agency.txt).
pub async fn feed_today(c: &impl GenericClient, feed_id: &str, now: f64) -> Result<NaiveDate> {
    let row = c
        .query_opt(
            "SELECT timezone FROM core.feed_version WHERE feed_id = $1 AND timezone IS NOT NULL ORDER BY version DESC LIMIT 1",
            &[&feed_id],
        )
        .await?;
    let tz: Tz = row.and_then(|r| r.get::<_, String>(0).parse().ok()).unwrap_or(Tz::UTC);
    Ok(crate::util::local_date(now, tz))
}

/// Retire ready versions that serve no service day from yesterday on and drop their rows. Returns their numbers.
/// Runs in a savepoint of `tx`; RetirementBusy (with this call undone and `tx` still usable) when a partition stays
/// locked longer than LOCK_TIMEOUT_MS.
pub async fn retire_versions(
    tx: &mut tokio_postgres::Transaction<'_>,
    feed_id: &str,
    today: NaiveDate,
) -> Result<Vec<i32>> {
    let sp = tx.savepoint("retire").await?;
    let previous: String = sp.query_one("SELECT current_setting('lock_timeout')", &[]).await?.get(0);
    sp.execute("SELECT set_config('lock_timeout', $1, true)", &[&format!("{LOCK_TIMEOUT_MS}ms")]).await?;
    match retire(&sp, feed_id, today).await {
        Ok(retired) => {
            sp.execute("SELECT set_config('lock_timeout', $1, true)", &[&previous]).await?;
            sp.commit().await?;
            Ok(retired)
        }
        Err(e) => {
            let busy = e
                .downcast_ref::<tokio_postgres::Error>()
                .and_then(|e| e.code())
                .is_some_and(|c| *c == tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE);
            sp.rollback().await?;
            if busy {
                warn!(feed = feed_id, error = %e, "retirement_postponed");
                return Err(RetirementBusy.into());
            }
            Err(e)
        }
    }
}

async fn retire(c: &impl GenericClient, feed_id: &str, today: NaiveDate) -> Result<Vec<i32>> {
    let rows = c
        .query(
            "SELECT fv.id, fv.feed_id, fv.version FROM core.feed_version fv
             WHERE fv.feed_id = $1 AND fv.status = 'ready'
               AND NOT EXISTS (
                   SELECT 1 FROM core.service_day_version s WHERE s.feed_version_id = fv.id AND s.service_date >= $2)
             ORDER BY fv.version",
            &[&feed_id, &(today - Duration::days(1))],
        )
        .await?;
    let mut retired = Vec::new();
    for r in rows {
        let fv = FeedVersion { id: r.get(0), feed_id: r.get(1), version: r.get(2) };
        for (schema, table) in partitioned_tables() {
            c.batch_execute(&format!("DROP TABLE IF EXISTS {schema}.\"{}\"", fv.partition(table))).await?;
        }
        for table in ["service_day_version", "service_date", "shape_line", "stop_direction", "stop_link"] {
            c.execute(&format!("DELETE FROM core.{table} WHERE feed_version_id = $1"), &[&fv.id]).await?;
        }
        c.execute("UPDATE core.feed_version SET status = 'retired' WHERE id = $1", &[&fv.id]).await?;
        info!(feed = feed_id, version = fv.version, feed_version_id = %fv.id, "version_retired");
        retired.push(fv.version);
    }
    Ok(retired)
}

/// Daily retirement across feeds in one transaction: (retired numbers per feed, feeds whose tables were busy).
pub async fn retire_all(
    client: &mut tokio_postgres::Client,
    feed_ids: &[String],
    now: f64,
) -> Result<(Vec<(String, Vec<i32>)>, Vec<String>)> {
    let mut tx = client.transaction().await?;
    let (mut retired, mut busy) = (Vec::new(), Vec::new());
    for feed_id in feed_ids {
        let today = feed_today(&tx, feed_id, now).await?;
        match retire_versions(&mut tx, feed_id, today).await {
            Ok(r) => retired.push((feed_id.clone(), r)),
            Err(e) if e.is::<RetirementBusy>() => busy.push(feed_id.clone()),
            Err(e) => return Err(e),
        }
    }
    tx.commit().await?;
    Ok((retired, busy))
}
