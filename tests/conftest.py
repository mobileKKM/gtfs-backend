"""Shared fixtures. DB/Redis tests use the dev services (docker-compose.dev.yml) and skip if they're down.

The test database `gtfs_test` is recreated once per session (override with TEST_DATABASE_URL).
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import zipfile
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.feeds import FeedConfig, FeedsFile, load_feeds
from gtfs_backend.logs import setup_logging
from gtfs_backend.worker.feed_sync import sync_feeds
from gtfs_backend.worker.realtime.store import TTLs
from gtfs_backend.worker.static.load import SourceMeta, import_zip
from gtfs_backend.worker.static.spec import TABLES

ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "postgresql://gtfs:gtfs@localhost:5432/gtfs_test")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")

setup_logging("WARNING")

# ZTP names its stations in stop codes (`802-01`): the pattern feeds.yml declares for its feeds
ZTP_STOP_CODES = load_feeds(ROOT / "feeds.yml").get("kmk_a").stop_code_pattern


def _admin_url(url: str) -> str:
    base, _, _ = url.rpartition("/")
    return f"{base}/postgres"


@pytest.fixture(scope="session")
def database_url() -> str:
    dbname = TEST_DB_URL.rsplit("/", 1)[-1]
    try:
        with psycopg.connect(_admin_url(TEST_DB_URL), autocommit=True, connect_timeout=3) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
            admin.execute(f'CREATE DATABASE "{dbname}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not available ({exc}); run `docker compose -f docker-compose.dev.yml up -d`")
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", TEST_DB_URL)
    command.upgrade(cfg, "head")
    return TEST_DB_URL


FEEDS = FeedsFile(
    feeds=[
        FeedConfig(feed_id=f, stop_namespace="ztp", stop_code_pattern=ZTP_STOP_CODES, static=f"https://example.invalid/{f}.zip")
        for f in ("kmk_a", "kmk_m", "kmk_t")
    ]
    + [
        FeedConfig(feed_id="other", static="https://example.invalid/O.zip", share_stops_with=["ztp"]),
    ],
    search_namespaces=["ztp"],
)
TTLS = TTLs(vehicle=120, trip=600, alert=600, unmatched=120)  # the WorkerSettings defaults


def reset_db(database_url: str) -> None:
    """Remove all versions, partitions and stops."""
    with psycopg.connect(database_url, autocommit=True) as conn:
        for name in TABLES:
            parts = conn.execute(
                "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                "JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace "
                "WHERE n.nspname = 'gtfs' AND p.relname = %s",
                (name,),
            ).fetchall()
            for (part,) in parts:
                conn.execute(f'DROP TABLE gtfs."{part}"')
        conn.execute(
            "TRUNCATE core.stop_direction, core.stop_link, core.shape_line, core.service_day_version, core.service_date, "
            "core.stop, core.stop_group, core.public_id, core.feed_version, core.feed CASCADE"
        )


@pytest.fixture
def db(database_url: str):
    """A connection to a clean test database (all versions, partitions and stops removed)."""
    reset_db(database_url)
    with psycopg.connect(database_url) as conn:
        sync_feeds(conn, FEEDS)
        yield conn


@pytest.fixture
def redis_url() -> str:
    import redis

    try:
        r = redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=2)
        r.ping()
        r.flushdb()
        r.close()
    except redis.exceptions.ConnectionError as exc:
        pytest.skip(f"Redis not available ({exc})")
    return TEST_REDIS_URL


def make_zip(path: Path, files: dict[str, str]) -> Path:
    """Write a GTFS zip from {'stops.txt': 'csv text', ...} (leading indentation stripped)."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            lines = [line.strip() for line in text.strip().splitlines()]
            zf.writestr(name, "\n".join(lines) + "\n")
    return path


def read_zip_csv(path: Path, name: str) -> list[dict[str, str]]:
    with zipfile.ZipFile(path) as zf, zf.open(name) as fh:
        return list(csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig")))


# ------------------------------------------------------------------------------------------------- helpers


def meta(path: Path) -> SourceMeta:
    return SourceMeta(sha256=hashlib.sha256(path.read_bytes()).hexdigest(), archive_path=str(path))


def do_import(db, key: str, path: Path, namespace: str | None = "ztp", share=None):
    result = import_zip(db, key, path, meta(path), namespace, share, ZTP_STOP_CODES if namespace == "ztp" else None)
    assert result.status == "ready", result.error
    return result


def message(kind: str, suffix: str) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.ParseFromString((FIXTURES / "dst_2026-10-25" / f"{kind}_{suffix}.pb").read_bytes())
    return msg
