"""Worker settings (env vars). Durations are seconds."""

from __future__ import annotations

from datetime import time
from functools import lru_cache
from pathlib import Path

from pydantic import Field

from gtfs_backend.settings import Settings


class WorkerSettings(Settings):
    archive_dir: Path = Path("data/archive")
    # zips of retired (and failed) versions are removed this many days later; 0 keeps them forever
    archive_retention_days: int = Field(default=30, ge=0)
    # OSM stop poles for matching shared stops (`gtfs-worker fetch-osm` writes it; without it stops match by position
    # alone). The worker downloads it itself when it is missing or older than OSM_REFRESH_INTERVAL (0: never).
    osm_stops_file: Path = Path("data/osm/stops.json")
    osm_refresh_interval: float = Field(default=86400, ge=0)
    static_poll_interval: float = Field(default=3600, gt=0)
    # defaults for feeds.yml `realtime_poll_interval` / `realtime_max_age`: how often each realtime file is polled,
    # and how long its last data stays live (TTLs refreshed) while the file doesn't change
    realtime_poll_interval: float = Field(default=10, gt=0)
    realtime_max_age: int = Field(default=120, ge=1)

    # downloads
    http_connect_timeout: float = Field(default=10, gt=0)
    http_retries: int = Field(default=3, ge=1)  # attempts per download, including the first
    http_retry_delay: float = Field(default=1.0, ge=0)
    static_download_timeout: float = Field(default=300, gt=0)
    realtime_fetch_timeout: float = Field(default=15, gt=0)

    # how long each kind of live data stays in Redis without a newer poll
    vehicle_ttl: int = Field(default=120, ge=1)
    trip_ttl: int = Field(default=600, ge=1)  # fallback only: trips are removed when they leave the feed
    alert_ttl: int = Field(default=600, ge=1)
    unmatched_ttl: int = Field(default=120, ge=1)

    # a realtime task without a finished poll for this long stops the worker, so Docker restarts it
    watchdog_timeout: float = Field(default=600, gt=0)
    # daily retirement of timetable versions, Europe/Warsaw time
    retire_at: time = time(3, 30)


@lru_cache
def get_worker_settings() -> WorkerSettings:
    return WorkerSettings()
