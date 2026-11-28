"""Shared fixtures. DB/Redis tests use the dev services (docker-compose.dev.yml) and skip if they're down.

The test database `gtfs_test` is recreated once per session (override with TEST_DATABASE_URL) and migrated by the worker
binary, which also imports the test timetables and processes the realtime snapshot: `GTFS_WORKER` (default
`worker/target/release/gtfs-worker`; build it with `cargo build --release` in worker/). Tests that need it skip without it.
"""

from __future__ import annotations

import json
import os
import subprocess
import zipfile
from pathlib import Path

import psycopg
import pytest
import yaml
from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.feeds import FeedsFile, load_feeds

ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "postgresql://gtfs:gtfs@localhost:5432/gtfs_test")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
WORKER = Path(os.environ.get("GTFS_WORKER", ROOT / "worker" / "target" / "release" / "gtfs-worker"))

# ZTP names its stations in stop codes (`802-01`): the pattern feeds.yml declares for its feeds
ZTP_STOP_CODES = load_feeds(ROOT / "feeds.yml").get("kmk_a").stop_code_pattern


def _admin_url(url: str) -> str:
    base, _, _ = url.rpartition("/")
    return f"{base}/postgres"


def worker(*args: str, feeds: FeedsFile | None = None, tmp: Path | None = None, **env: str) -> str:
    """Run the worker binary (against the test database and Redis) and return its output; skips without the binary."""
    if not WORKER.exists():
        pytest.skip(f"no worker binary at {WORKER} (cargo build --release in worker/, or set GTFS_WORKER)")
    environment = {**os.environ, "DATABASE_URL": TEST_DB_URL, "REDIS_URL": TEST_REDIS_URL, "LOG_LEVEL": "warn", **env}
    if feeds is not None:
        assert tmp is not None
        path = tmp / "feeds.yml"
        path.write_text(yaml.safe_dump(feeds.model_dump(mode="json", exclude_none=True, exclude_defaults=True)))
        environment["FEEDS_CONFIG"] = str(path)
        environment.setdefault("ARCHIVE_DIR", str(tmp / "archive"))
    done = subprocess.run([str(WORKER), *args], env=environment, capture_output=True, text=True)
    assert done.returncode == 0, f"gtfs-worker {' '.join(args)}: {done.stdout}{done.stderr}"
    return done.stdout


@pytest.fixture(scope="session")
def database_url() -> str:
    dbname = TEST_DB_URL.rsplit("/", 1)[-1]
    try:
        with psycopg.connect(_admin_url(TEST_DB_URL), autocommit=True, connect_timeout=3) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
            admin.execute(f'CREATE DATABASE "{dbname}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not available ({exc}); run `docker compose -f docker-compose.dev.yml up -d`")
    worker("migrate")
    return TEST_DB_URL


def reset_db(database_url: str) -> None:
    """Remove all versions, partitions and stops."""
    with psycopg.connect(database_url, autocommit=True) as conn:
        parts = conn.execute(
            "SELECT n.nspname, c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ('gtfs', 'core') AND c.relkind = 'r'"
        ).fetchall()
        for schema, part in parts:
            conn.execute(f'DROP TABLE {schema}."{part}"')
        conn.execute(
            "TRUNCATE core.stop_direction, core.stop_link, core.shape_line, core.service_day_version, core.service_date, "
            "core.stop, core.stop_group, core.public_id, core.feed_version, core.feed CASCADE"
        )


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


def import_static(feeds: FeedsFile, feed_id: str, path: Path, tmp: Path) -> None:
    """Import a zip as the feed's next version (`gtfs-worker import-static --file`)."""
    out = worker("import-static", "--feed", feed_id, "--file", str(path), feeds=feeds, tmp=tmp)
    assert ": ready" in out, out


def process_realtime(feeds: FeedsFile, files: dict[str, dict[str, Path]], now: float, tmp: Path) -> None:
    """Process realtime files at `now` into Redis (`gtfs-worker replay`): {feed_id: {kind: path}}, in that order."""
    manifest = {
        "feeds": {
            feed_id: {"realtime": {kind: [{"t": now, "file": str(path.resolve())}] for kind, path in kinds.items()}}
            for feed_id, kinds in files.items()
        }
    }
    path = tmp / "manifest.json"
    path.write_text(json.dumps(manifest))
    worker("replay", str(path), feeds=feeds, tmp=tmp)


def make_zip(path: Path, files: dict[str, str]) -> Path:
    """Write a GTFS zip from {'stops.txt': 'csv text', ...} (leading indentation stripped)."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            lines = [line.strip() for line in text.strip().splitlines()]
            zf.writestr(name, "\n".join(lines) + "\n")
    return path


def message(kind: str, suffix: str) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.ParseFromString((FIXTURES / "dst_2026-10-25" / f"{kind}_{suffix}.pb").read_bytes())
    return msg
