"""Retirement must never wait long for partition locks: while it waits, every read of the table queues behind it."""

from __future__ import annotations

import os
import time
from datetime import date, timedelta

import psycopg
import pytest
from conftest import ZTP_STOP_CODES, do_import, meta
from test_static_import import tiny_feed

from gtfs_backend.worker.static import versions
from gtfs_backend.worker.static.load import import_zip
from gtfs_backend.worker.static.versions import RetirementBusy, retire_versions

STATUS = "SELECT status FROM core.feed_version WHERE feed_id = 'kmk_t' AND version = 1"
V1_STOP_TIMES = "SELECT to_regclass('gtfs.stop_times__kmk_t__v1')"
V1_DEPARTURES = "SELECT to_regclass('core.departure__kmk_t__v1')"


def _two_versions(db, tmp_path, old_days: list[date], new_days: list[date]) -> None:
    ymd = [d.strftime("%Y%m%d") for d in old_days], [d.strftime("%Y%m%d") for d in new_days]
    do_import(db, "kmk_t", tiny_feed(tmp_path / "old.zip", "old", ymd[0], night_trip=False))
    do_import(db, "kmk_t", tiny_feed(tmp_path / "new.zip", "new", ymd[1], night_trip=False))


def test_busy_tables_postpone_retirement_and_leave_the_transaction_usable(db, database_url, tmp_path):
    d = date.today()
    _two_versions(db, tmp_path, [d], [d + timedelta(days=1)])
    db.commit()
    assert db.execute("SELECT count(*) FROM core.departure WHERE feed_version_id = 'kmk_t_v1'").fetchone()[0] > 0

    # like an import in another transaction: holds the parent table for as long as it runs
    with psycopg.connect(database_url) as importer:
        importer.execute("LOCK TABLE gtfs.stop_times IN SHARE UPDATE EXCLUSIVE MODE")
        with pytest.raises(RetirementBusy):
            retire_versions(db, "kmk_t", today=d + timedelta(days=2), lock_timeout_ms=100)
        # nothing retired, nothing dropped, and the caller's transaction goes on
        assert db.execute(STATUS).fetchone()[0] == "ready"
        assert db.execute(V1_STOP_TIMES).fetchone()[0] is not None
        assert db.execute("SELECT current_setting('lock_timeout')").fetchone()[0] == "0"
        importer.rollback()

    assert retire_versions(db, "kmk_t", today=d + timedelta(days=2), lock_timeout_ms=100) == [1]
    assert db.execute("SELECT current_setting('lock_timeout')").fetchone()[0] == "0"  # put back after the drops
    db.commit()
    assert db.execute(STATUS).fetchone()[0] == "retired"
    # its partitions are gone, the derived departures with the GTFS tables
    assert db.execute(V1_STOP_TIMES).fetchone()[0] is None
    assert db.execute(V1_DEPARTURES).fetchone()[0] is None


def test_import_succeeds_when_its_retirement_is_blocked(db, database_url, tmp_path, monkeypatch):
    monkeypatch.setattr(versions, "LOCK_TIMEOUT_MS", 100)
    d = date.today()
    # the old version serves today until the new one takes it over; then it only has a day long past
    old_days = [(d - timedelta(days=5)).strftime("%Y%m%d"), d.strftime("%Y%m%d")]
    do_import(db, "kmk_t", tiny_feed(tmp_path / "old.zip", "old", old_days, night_trip=False))
    db.commit()
    new = tiny_feed(tmp_path / "new.zip", "new", [d.strftime("%Y%m%d")], night_trip=False)

    # a long read of the old version: doesn't stop the import, but keeps its partition from being dropped
    with psycopg.connect(database_url) as reader:
        reader.execute("SELECT count(*) FROM gtfs.stop_times__kmk_t__v1").fetchone()
        result = import_zip(db, "kmk_t", new, meta(new), "ztp", stop_code_pattern=ZTP_STOP_CODES)
        assert (result.status, result.retired) == ("ready", [])
        assert db.execute(STATUS).fetchone()[0] == "ready"
        reader.rollback()

    assert retire_versions(db, "kmk_t") == [1]  # the daily retirement catches up


def test_archived_zips_of_long_retired_and_failed_versions_are_removed(db, tmp_path):
    from gtfs_backend.worker.static import archive

    d = date.today()
    _two_versions(db, tmp_path, [d - timedelta(days=60)], [d])  # the second import retires the first version
    assert db.execute(STATUS).fetchone()[0] == "retired"
    archive_dir = tmp_path / "archive"
    rows = db.execute("SELECT id, version, sha256 FROM core.feed_version WHERE feed_id = 'kmk_t' ORDER BY version").fetchall()
    for _, _, sha in rows:
        path = archive.archive_path(archive_dir, "kmk_t", sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"zip")
    # a failed version of another feed, downloaded long ago
    db.execute(
        "INSERT INTO core.feed_version (feed_id, version, sha256, status, archive_path, downloaded_at) "
        "VALUES ('kmk_a', 1, 'f00d', 'failed', 'x', now() - interval '40 days')"
    )
    failed = archive.archive_path(archive_dir, "kmk_a", "f00d")
    failed.parent.mkdir(parents=True)
    failed.write_bytes(b"zip")
    (archive_dir / "tmp").mkdir()
    (archive_dir / "tmp" / "left.zip").write_bytes(b"partial")
    os.utime(archive_dir / "tmp" / "left.zip", (time.time() - 2 * 86400,) * 2)
    old, new = (archive.archive_path(archive_dir, "kmk_t", sha) for _, _, sha in rows)

    # retired just now: kept for the retention time
    assert archive.prune(db, archive_dir, days=30) == (2, len(b"zip") + len(b"partial"))
    assert old.exists() and new.exists() and not failed.exists()
    db.execute("UPDATE core.feed_version SET retired_at = now() - interval '31 days' WHERE id = %s", (rows[0][0],))
    assert archive.prune(db, archive_dir, days=0) == (0, 0)  # 0: keep forever
    assert archive.prune(db, archive_dir, days=30) == (1, 3)
    assert not old.exists() and new.exists()  # the ready version's zip stays
    paths = dict(db.execute("SELECT version, archive_path FROM core.feed_version WHERE feed_id = 'kmk_t'").fetchall())
    assert paths[1] is None and paths[2] is not None
