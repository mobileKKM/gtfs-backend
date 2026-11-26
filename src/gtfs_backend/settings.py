"""Settings from env vars (and `.env`). The worker and the API each extend these with their own."""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://gtfs:gtfs@localhost:5432/gtfs"
    redis_url: str = "redis://localhost:6379/0"
    feeds_config: Path = Path("feeds.yml")
    log_level: str = "INFO"
    # Connections per process (the worker, and each API process). Postgres allows 100 in all: the worker's pool +
    # API_WORKERS x this (+ imports, psql) must stay below that.
    db_pool_max_size: int = Field(default=10, ge=1)
    # Default for feeds.yml `realtime_max_age`: realtime data stays live this long. The worker keeps the last data of
    # an unchanged file in Redis that long and drops vehicles whose position is older; the API ignores trip updates
    # and vehicles measured longer ago (so nothing goes stale on the map or boards when the worker or a feed stops).
    realtime_max_age: int = Field(default=120, ge=1)

    def max_age(self, feed) -> int:
        """A feed's (FeedConfig) realtime max age."""
        return feed.realtime_max_age or self.realtime_max_age

    # Benchmark replays only (docs/benchmark.md): the worker and the API run as if it were this many seconds later
    # (negative: earlier), so a recorded realtime feed meets the timetable day it was recorded on. 0 in production.
    clock_offset: float = 0

    def clock(self) -> Callable[[], float]:
        """The wall clock (epoch seconds) for what the data is about: time.time(), shifted by CLOCK_OFFSET."""
        offset = self.clock_offset
        if not offset:
            return time.time
        return lambda: time.time() + offset


@lru_cache
def get_settings() -> Settings:
    return Settings()
