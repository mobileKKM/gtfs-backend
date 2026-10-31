"""Which feed version serves which service day, and retirement of versions nobody needs anymore."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg

from gtfs_backend.logs import log
from gtfs_backend.worker.static.partitions import FeedVersion, drop_version_partitions

# Dropping a partition needs an exclusive lock on its parent table. While that lock is waited for, every new read of
# the table queues behind it, so a drop that has to wait (an import in another transaction holds the parent) would
# stall the API for as long as that import runs. Waiting is capped; the retirement is postponed instead.
LOCK_TIMEOUT_MS = 2000


class RetirementBusy(Exception):
    """The partitions couldn't be locked in time; nothing was retired. Try again later."""


def recompute_service_day_version(conn: psycopg.Connection, feed_id: str) -> int:
    """For each date, the newest ready version that has any service on that date wins."""
    cur = conn.cursor()
    cur.execute("DELETE FROM core.service_day_version WHERE feed_id = %s", (feed_id,))
    cur.execute(
        """
        INSERT INTO core.service_day_version (feed_id, service_date, feed_version_id)
        SELECT DISTINCT ON (sd.date) fv.feed_id, sd.date, fv.id
        FROM core.service_date sd
        JOIN core.feed_version fv ON fv.id = sd.feed_version_id
        WHERE fv.feed_id = %s AND fv.status = 'ready'
        ORDER BY sd.date, fv.version DESC
        """,
        (feed_id,),
    )
    return cur.rowcount


def feed_today(conn: psycopg.Connection, feed_id: str, now: datetime | None = None) -> date:
    """Today's date in the feed's timezone (from its newest version's agency.txt)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT timezone FROM core.feed_version WHERE feed_id = %s AND timezone IS NOT NULL ORDER BY version DESC LIMIT 1",
        (feed_id,),
    )
    row = cur.fetchone()
    tz = ZoneInfo(row[0]) if row else ZoneInfo("UTC")
    return (now or datetime.now(tz)).astimezone(tz).date()


def retire_versions(
    conn: psycopg.Connection, feed_id: str, today: date | None = None, *, lock_timeout_ms: int | None = None
) -> list[int]:
    """Retire ready versions that serve no service day from yesterday on; drop their rows. Returns their numbers.

    Runs in a savepoint of the caller's transaction. Raises RetirementBusy (with everything of this call undone and
    the caller's transaction still usable) when a partition stays locked longer than `lock_timeout_ms`.
    """
    today = today or feed_today(conn, feed_id)
    timeout = LOCK_TIMEOUT_MS if lock_timeout_ms is None else lock_timeout_ms
    previous = conn.execute("SELECT current_setting('lock_timeout')").fetchone()[0]
    try:
        with conn.transaction():
            # transaction-local: undone with the savepoint on failure, put back by hand on success
            conn.execute("SELECT set_config('lock_timeout', %s, true)", (f"{timeout}ms",))
            retired = _retire(conn, feed_id, today)
            conn.execute("SELECT set_config('lock_timeout', %s, true)", (previous,))
    except psycopg.errors.LockNotAvailable as exc:
        log.warning("retirement_postponed", feed=feed_id, error=str(exc).strip())
        raise RetirementBusy(feed_id) from exc
    return retired


def _retire(conn: psycopg.Connection, feed_id: str, today: date) -> list[int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fv.id, fv.feed_id, fv.version FROM core.feed_version fv
        WHERE fv.feed_id = %s AND fv.status = 'ready'
          AND NOT EXISTS (
              SELECT 1 FROM core.service_day_version s WHERE s.feed_version_id = fv.id AND s.service_date >= %s
          )
        ORDER BY fv.version
        """,
        (feed_id, today - timedelta(days=1)),
    )
    retired = [FeedVersion(*r) for r in cur.fetchall()]
    for fv in retired:
        drop_version_partitions(conn, fv)
        for table in ("service_day_version", "service_date", "shape_line", "platform_direction", "stop_link"):
            cur.execute(f"DELETE FROM core.{table} WHERE feed_version_id = %s", (fv.id,))
        cur.execute("UPDATE core.feed_version SET status = 'retired' WHERE id = %s", (fv.id,))
        log.info("version_retired", feed=feed_id, version=fv.version, feed_version_id=fv.id)
    return [fv.version for fv in retired]
