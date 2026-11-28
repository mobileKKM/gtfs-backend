//! Postgres: a pool for the realtime side, single connections for imports and commands, and the schema migrations.
//!
//! Migrations are the SQL files in `worker/migrations/` (built in), applied in order and recorded in `public.schema_migration`. A
//! database set up by the earlier Python worker (alembic) is taken over: its `alembic_version` says which files ran.

use std::time::Duration;

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

pub async fn connect(url: &str) -> Result<Client> {
    let config: tokio_postgres::Config = url.parse().context("DATABASE_URL")?;
    let (client, connection) = config.connect(NoTls).await.context("connecting to Postgres")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "postgres_connection_closed");
        }
    });
    Ok(client)
}

/// `wait`: how long a caller waits for a free connection.
pub fn pool(url: &str, max_size: usize, wait: Duration) -> Result<Pool> {
    let config: tokio_postgres::Config = url.parse().context("DATABASE_URL")?;
    let manager = Manager::from_config(config, NoTls, ManagerConfig { recycling_method: RecyclingMethod::Fast });
    Ok(Pool::builder(manager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(wait))
        .create_timeout(Some(Duration::from_secs(10)))
        .build()?)
}

const MIGRATIONS: &[(i32, &str, &str)] = &[
    (1, "initial", include_str!("../migrations/0001_initial.sql")),
    (2, "stop_departure", include_str!("../migrations/0002_stop_departure.sql")),
];

/// Apply the migrations the database hasn't had yet. Returns the numbers applied.
pub async fn migrate(client: &mut Client) -> Result<Vec<i32>> {
    // one migrator at a time (the lock ends with the session)
    client.execute("SELECT pg_advisory_lock(727001)", &[]).await?;
    let result = migrate_locked(client).await;
    let _ = client.execute("SELECT pg_advisory_unlock(727001)", &[]).await;
    result
}

async fn migrate_locked(client: &mut Client) -> Result<Vec<i32>> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.schema_migration (
                version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
        )
        .await?;
    let mut done: Vec<i32> =
        client.query("SELECT version FROM public.schema_migration", &[]).await?.iter().map(|r| r.get(0)).collect();
    if done.is_empty() {
        // a database the Python worker migrated: alembic revisions '0001', '0002' are these files
        let alembic = client.query_opt("SELECT to_regclass('public.alembic_version')::text", &[]).await?;
        if alembic.and_then(|r| r.get::<_, Option<String>>(0)).is_some()
            && let Some(row) = client.query_opt("SELECT version_num FROM public.alembic_version", &[]).await?
        {
            let head: i32 = row.get::<_, String>(0).parse().context("alembic_version")?;
            for (n, name, _) in MIGRATIONS.iter().filter(|(n, _, _)| *n <= head) {
                client
                    .execute("INSERT INTO public.schema_migration (version, name) VALUES ($1, $2)", &[n, name])
                    .await?;
                done.push(*n);
            }
            info!(alembic = head, "migrations_taken_over");
        }
    }
    let mut applied = Vec::new();
    for (n, name, sql) in MIGRATIONS {
        if done.contains(n) {
            continue;
        }
        let tx = client.transaction().await?;
        tx.batch_execute(sql).await.with_context(|| format!("migration {n:04}_{name}"))?;
        if *n == 2 {
            backfill_stop_departures(&tx).await?;
        }
        tx.execute("INSERT INTO public.schema_migration (version, name) VALUES ($1, $2)", &[n, name]).await?;
        tx.commit().await?;
        info!(version = n, name, "migration_applied");
        applied.push(*n);
    }
    Ok(applied)
}

/// Versions imported before core.stop_departure existed get their partition.
async fn backfill_stop_departures(tx: &tokio_postgres::Transaction<'_>) -> Result<()> {
    let rows =
        tx.query("SELECT id, feed_id, version FROM core.feed_version WHERE status = 'ready' ORDER BY id", &[]).await?;
    for r in rows {
        let fv = crate::static_feed::FeedVersion { id: r.get(0), feed_id: r.get(1), version: r.get(2) };
        crate::static_feed::derive::build_stop_departures(tx, &fv).await?;
    }
    Ok(())
}
