"""Settings from env vars (and `.env`). The worker and the API each extend these with their own."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://gtfs:gtfs@localhost:55432/gtfs"
    redis_url: str = "redis://localhost:56379/0"
    feeds_config: Path = Path("feeds.yml")
    log_level: str = "INFO"
    # Connections per process (the worker, and each API process). Postgres allows 100 in all: the worker's pool +
    # API_WORKERS x (this + CATALOG_POOL_MAX_SIZE) (+ imports, psql) must stay below that.
    db_pool_max_size: int = Field(default=10, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
