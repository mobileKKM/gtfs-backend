"""Import a GTFS zip as a new version of a feed (versions are numbered 1, 2, 3, ... per feed).

Everything happens in one transaction: per table a standalone partition is created, filled with COPY
and attached (readers never see half a version), the integrity check runs, derived data is built and
the version becomes `ready`. On any error the transaction rolls back and a `failed` version row records
why; the versions already serving keep serving.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Iterable
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from gtfs_backend import procmem
from gtfs_backend.logs import log
from gtfs_backend.worker.static import derive, integrity, stops, versions
from gtfs_backend.worker.static.partitions import FeedVersion, attach_version_partition, create_version_partition
from gtfs_backend.worker.static.results import ImportResult, SourceMeta
from gtfs_backend.worker.static.source import GtfsSource, Row
from gtfs_backend.worker.static.spec import TABLES
from gtfs_backend.worker.static.validate import KEYS_IN_DB, duplicate_keys_error, validate

COPY_BUFFER_CHARS = 1 << 20  # COPY data is sent in pieces of about this size


class ImportFailed(Exception):
    pass


def _copy_rows(cur: psycopg.Cursor, part: sql.Identifier, columns: list[str], rows: Iterable[Row], v: str) -> int:
    """COPY the rows into the partition, streamed; returns how many. None is an unquoted empty field, which COPY reads
    as NULL (an empty string can't occur: it reads as NULL)."""
    stmt = sql.SQL("COPY {part} (feed_version_id, {cols}) FROM STDIN (FORMAT csv)").format(
        part=part, cols=sql.SQL(", ").join(map(sql.Identifier, columns))
    )
    count = 0
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    with cur.copy(stmt) as cp:
        for row in rows:
            writer.writerow([v, *row])
            count += 1
            if buf.tell() >= COPY_BUFFER_CHARS:
                cp.write(buf.getvalue())
                buf.seek(0)
                buf.truncate()
        if buf.tell():
            cp.write(buf.getvalue())
    return count


def _version_number(cur: psycopg.Cursor, feed_id: str, sha256: str) -> int:
    """The version number of this zip: its existing one (a retry or re-import), else the feed's next number.

    The counter on core.feed is bumped in the caller's transaction, so a rolled-back import gives its number back,
    and the row lock serializes imports of the same feed.
    """
    cur.execute("SELECT version FROM core.feed_version WHERE feed_id = %s AND sha256 = %s", (feed_id, sha256))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("UPDATE core.feed SET last_version = last_version + 1 WHERE feed_id = %s RETURNING last_version", (feed_id,))
    row = cur.fetchone()
    if row is None:
        raise ImportFailed(f"unknown feed_id {feed_id!r} (not synced from feeds.yml)")
    return row[0]


def _record_failure(conn: psycopg.Connection, feed_id: str, meta: SourceMeta, error: str, warnings: list[str]) -> FeedVersion:
    with conn.transaction():
        cur = conn.cursor()
        version = _version_number(cur, feed_id, meta.sha256)
        cur.execute(
            """
            INSERT INTO core.feed_version
                (feed_id, version, sha256, etag, last_modified, archive_path, status, error, warnings)
            VALUES (%s, %s, %s, %s, %s, %s, 'failed', %s, %s)
            ON CONFLICT (feed_id, version) DO UPDATE SET status = 'failed', error = excluded.error, warnings = excluded.warnings
            RETURNING id
            """,
            (feed_id, version, meta.sha256, meta.etag, meta.last_modified, meta.archive_path, error, Jsonb(warnings)),
        )
        return FeedVersion(cur.fetchone()[0], feed_id, version)


def _source_feed_version(source: GtfsSource) -> str | None:
    """feed_info.txt's feed_version (of its first row)."""
    info = source.records("feed_info")
    return info[0]["feed_version"] if info else None


def import_zip(
    conn: psycopg.Connection,
    feed_id: str,
    zip_path: Path,
    meta: SourceMeta,
    namespace: str | None = None,
    share_with: list[str] | None = None,
    stop_code_pattern: str | None = None,
) -> ImportResult:
    started = time.monotonic()
    zip_path = Path(zip_path)
    bound = log.bind(feed=feed_id, sha256=meta.sha256[:12])
    warnings: list[str] = []

    try:
        source = GtfsSource(zip_path)
        result = validate(source)
        warnings = result.warnings
        for w in warnings:
            bound.warning("gtfs_validation_warning", msg=w)
        if not result.ok:
            raise ImportFailed("validation failed: " + "; ".join(result.errors[:10]))
        tz = next(a["agency_timezone"] for a in source.records("agency") if a["agency_timezone"] is not None)

        with conn.transaction():
            cur = conn.cursor()
            number = _version_number(cur, feed_id, meta.sha256)
            cur.execute(
                """
                INSERT INTO core.feed_version
                    (feed_id, version, sha256, etag, last_modified, archive_path, status, timezone, source_feed_version)
                VALUES (%s, %s, %s, %s, %s, %s, 'importing', %s, %s)
                -- a retry of a failed version: that row was written without most of these
                ON CONFLICT (feed_id, version) DO UPDATE
                SET status = 'importing', error = NULL, etag = excluded.etag, last_modified = excluded.last_modified,
                    archive_path = excluded.archive_path, timezone = excluded.timezone,
                    source_feed_version = excluded.source_feed_version
                RETURNING id
                """,
                (
                    feed_id,
                    number,
                    meta.sha256,
                    meta.etag,
                    meta.last_modified,
                    meta.archive_path,
                    tz,
                    _source_feed_version(source),
                ),
            )
            fv = FeedVersion(cur.fetchone()[0], feed_id, number)
            v = fv.id
            bound = bound.bind(version=number, feed_version_id=v)

            row_counts: dict[str, int] = {}
            for name in source.tables:
                part = create_version_partition(cur, name, fv)
                row_counts[name] = _copy_rows(cur, part, TABLES[name].column_names, source.rows(name), v)
                # the big tables' primary keys are checked here rather than in memory (validate.KEYS_IN_DB)
                if name in KEYS_IN_DB and (dup := duplicate_keys_error(cur, part, name)):
                    raise ImportFailed("validation failed: " + dup)
                attach_version_partition(cur, name, part, fv)
                # fresh partitions have no statistics until autovacuum gets to them; the derive and stop matching
                # queries below would be planned blind (nested loops over millions of stop_times)
                cur.execute(sql.SQL("ANALYZE {}").format(part))
            # tables missing from the zip still get an empty partition, so every version has the full set
            for name in TABLES:
                if name not in source.tables:
                    attach_version_partition(cur, name, create_version_partition(cur, name, fv), fv)
            bound.info("gtfs_loaded", rows=row_counts)

            integrity.check_version(conn, fv, zip_path, source.tables)
            bound.info("integrity_ok")

            services, first, last = derive.build_service_dates(conn, v)
            shapes = derive.build_shape_lines(conn, v)
            stop_stats = stops.apply_matches(conn, feed_id, v, namespace or feed_id, share_with, stop_code_pattern)
            for msg in stop_stats["errors"]:
                warnings.append(f"stop matching: {msg}")
            directions = derive.build_stop_directions(conn, v)
            departures = derive.build_departures(conn, fv)
            bound.info(
                "derived",
                service_dates=services,
                first=first,
                last=last,
                shape_lines=shapes,
                stops=stop_stats["methods"],
                stop_directions=directions,
                departures=departures,
            )

            cur.execute(
                """
                UPDATE core.feed_version
                SET status = 'ready', imported_at = now(), row_counts = %s, warnings = %s
                WHERE id = %s
                """,
                (
                    Jsonb(
                        {
                            **row_counts,
                            "unknown_columns": source.unknown_columns,
                            "unknown_files": source.unknown_files,
                            "stop_matching": stop_stats["methods"],
                        }
                    ),
                    Jsonb(warnings),
                    v,
                ),
            )
            versions.recompute_service_day_version(conn, feed_id)
            try:
                retired = versions.retire_versions(conn, feed_id)
            except versions.RetirementBusy:  # the new version is fine; the daily retirement catches up
                retired = []

        seconds = round(time.monotonic() - started, 1)
        bound.info("import_ready", seconds=seconds, retired=retired, peak_mb=procmem.peak_mb())
        return ImportResult(v, feed_id, number, "ready", row_counts, warnings, retired=retired, seconds=seconds)

    except Exception as exc:  # noqa: BLE001 - every failure is recorded on the version
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            conn.rollback()
        error = f"{type(exc).__name__}: {exc}"
        bound.error("import_failed", error=error, peak_mb=procmem.peak_mb())
        fv = _record_failure(conn, feed_id, meta, error[:5000], warnings)
        return ImportResult(*fv, "failed", warnings=warnings, error=error, seconds=round(time.monotonic() - started, 1))
