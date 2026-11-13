"""Retirement must never wait long for partition locks: while it waits, every read of the table queues behind it."""

from __future__ import annotations

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
