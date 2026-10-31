"""A zip whose import failed isn't imported again by itself, but is with `retry_failed` (gtfs-worker ... --retry)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date

import httpx
from conftest import FEEDS
from test_static_import import tiny_feed

from gtfs_backend.db import async_pool
from gtfs_backend.worker.settings import WorkerSettings
from gtfs_backend.worker.static import derive
from gtfs_backend.worker.static.fetch import PREVIOUSLY_FAILED, check_static


async def test_failed_import_is_skipped_until_retried(db, database_url, tmp_path, monkeypatch):
    zip_bytes = tiny_feed(tmp_path / "feed.zip", "x", [date.today().strftime("%Y%m%d")], night_trip=False).read_bytes()
    feed = FEEDS.get("kmk_t")
    settings = WorkerSettings(_env_file=None, database_url=database_url, archive_dir=tmp_path / "archive", log_level="WARNING")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=zip_bytes)))
    pool = async_pool(database_url)
    await pool.open()

    def broken(conn, v):  # a failure that won't happen again (say, the disk was full)
        raise OSError("No space left on device")

    async def check(**kw):
        return await check_static(feed, settings, pool, client, executor, conditional=False, **kw)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with monkeypatch.context() as m:
                m.setattr(derive, "build_shape_lines", broken)
                first = await check()
            assert (first.status, first.version) == ("failed", 1)

            again = await check()
            assert (again.status, again.version) == (PREVIOUSLY_FAILED, 1)

            retried = await check(retry_failed=True)
            assert (retried.status, retried.version) == ("ready", 1), retried.error
    finally:
        await pool.close()
        await client.aclose()

    row = db.execute("SELECT status, timezone, error FROM core.feed_version WHERE feed_id = 'kmk_t' AND version = 1").fetchone()
    assert row == ("ready", "Europe/Warsaw", None)  # the failed row's gaps are filled in
