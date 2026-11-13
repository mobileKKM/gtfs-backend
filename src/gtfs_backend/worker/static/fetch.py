"""Static feed polling: conditional download, hash dedupe, archive, import in a worker process."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from concurrent.futures import Executor
from dataclasses import asdict
from pathlib import Path

import httpx
from psycopg_pool import AsyncConnectionPool

from gtfs_backend.feeds import FeedConfig, FeedsFile
from gtfs_backend.keys import version_id
from gtfs_backend.logs import log
from gtfs_backend.worker.http import fetch_to_file
from gtfs_backend.worker.settings import WorkerSettings
from gtfs_backend.worker.static import archive
from gtfs_backend.worker.static.archive import sha256_file, validate_zip
from gtfs_backend.worker.static.load import ImportResult, SourceMeta

PREVIOUSLY_FAILED = "previously_failed"  # ImportResult.status of a zip that is known to fail
IMPORT_NICENESS = 10  # added to the import process's nice value (lower CPU priority)


def import_order(feeds: FeedsFile) -> list[FeedConfig]:
    """Feeds whose stops others share (e.g. ZTP for MLD) first, so attaching finds their stops."""
    shared = {ns for f in feeds.enabled for ns in f.share_stops_with}
    return sorted(feeds.enabled, key=lambda f: (f.namespace not in shared, bool(f.share_stops_with), f.feed_id))


def run_import_job(
    feed_id: str,
    namespace: str,
    share_with: list[str],
    stop_code_pattern: str | None,
    zip_path: str,
    meta: dict,
    database_url: str,
    log_level: str = "INFO",
) -> dict:
    """Entry point for the import process (must be top-level for ProcessPoolExecutor)."""
    import os

    from gtfs_backend.db import connect

    # an import is CPU-heavy for a minute: it must not take CPU from realtime polling or the API on the same host
    with contextlib.suppress(OSError):
        os.nice(IMPORT_NICENESS)
    from gtfs_backend.logs import setup_logging
    from gtfs_backend.worker.static.load import import_zip

    setup_logging(log_level)
    with connect(database_url) as conn:
        result = import_zip(conn, feed_id, Path(zip_path), SourceMeta(**meta), namespace, share_with, stop_code_pattern)
    return asdict(result)


async def existing_version(pool: AsyncConnectionPool, feed_id: str, sha256: str) -> tuple[int, str] | None:
    """(version, status) of an already known zip of this feed."""
    async with pool.connection() as conn:
        cur = await conn.execute(archive.KNOWN_VERSION_SQL, (feed_id, sha256))
        return await cur.fetchone()


async def save_fetch_state(pool: AsyncConnectionPool, feed_id: str, etag: str | None, last_modified: str | None) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE core.feed SET static_etag = %s, static_last_modified = %s, static_checked_at = now() WHERE feed_id = %s",
            (etag, last_modified, feed_id),
        )


async def check_static(
    feed: FeedConfig,
    settings: WorkerSettings,
    pool: AsyncConnectionPool,
    client: httpx.AsyncClient,
    executor: Executor,
    *,
    conditional: bool = True,
    retry_failed: bool = False,
) -> ImportResult | None:
    """Download the feed's static zip if it changed and import it. Returns the result of an import, if any.

    A zip whose import failed before isn't imported again (status `previously_failed`) unless `retry_failed`: then it
    is, keeping its version number.
    """
    bound = log.bind(feed=feed.feed_id)
    etag = last_modified = None
    if conditional:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT static_etag, static_last_modified FROM core.feed WHERE feed_id = %s", (feed.feed_id,)
            )
            row = await cur.fetchone()
            if row:
                etag, last_modified = row

    tmp = settings.archive_dir / "tmp" / f"{feed.feed_id}-{uuid.uuid4().hex}.zip"
    fetched = await fetch_to_file(
        client,
        feed.static_url,
        tmp,
        validate_zip,
        etag=etag,
        last_modified=last_modified,
        retries=settings.http_retries,
        retry_delay=settings.http_retry_delay,
    )
    if fetched.not_modified:
        bound.debug("static_not_modified")
        await save_fetch_state(pool, feed.feed_id, etag, last_modified)
        return None

    try:
        sha = await asyncio.to_thread(sha256_file, tmp)
        known = await existing_version(pool, feed.feed_id, sha)
        if known and known[1] in ("ready", "importing"):
            bound.info("static_unchanged", sha256=sha[:12], version=known[0])
            await save_fetch_state(pool, feed.feed_id, fetched.etag, fetched.last_modified)
            return None
        if known and known[1] == "failed" and not retry_failed:
            bound.warning("static_previously_failed", sha256=sha[:12], version=known[0])
            await save_fetch_state(pool, feed.feed_id, fetched.etag, fetched.last_modified)
            return ImportResult(version_id(feed.feed_id, known[0]), feed.feed_id, known[0], PREVIOUSLY_FAILED)
        if known and known[1] == "failed":
            bound.info("static_retrying_failed", sha256=sha[:12], version=known[0])

        stored = archive.archive_path(settings.archive_dir, feed.feed_id, sha)
        archive.put(tmp, stored, move=True)
        meta = SourceMeta(sha, fetched.etag, fetched.last_modified, str(stored))
        bound.info("static_importing", sha256=sha[:12], bytes=fetched.size)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            executor,
            run_import_job,
            feed.feed_id,
            feed.namespace,
            feed.share_stops_with,
            feed.stop_code_pattern,
            str(stored),
            asdict(meta),
            settings.database_url,
            settings.log_level,
        )
        if result["status"] == "ready":
            await save_fetch_state(pool, feed.feed_id, fetched.etag, fetched.last_modified)
        return ImportResult(**result)
    finally:
        tmp.unlink(missing_ok=True)
