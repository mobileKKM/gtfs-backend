"""API settings (env vars). Durations are seconds."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field

from gtfs_backend.settings import Settings


class ApiSettings(Settings):
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    api_workers: int = Field(default=1, ge=1)  # processes; each has its own connection pools and caches

    db_pool_timeout: float = Field(default=5, gt=0)  # wait for a free connection, then answer 503
    redis_pool_max_size: int = Field(default=50, ge=1)
    redis_pool_timeout: float = Field(default=5, gt=0)

    catalog_refresh_interval: float = Field(default=300, gt=0)  # also reloaded on `static:updated` and day change
    static_listener_retry_delay: float = Field(default=5, gt=0)

    # Responses shared by all API processes through Redis (api/cache.py), also sent as Cache-Control max-age where
    # the response is public; 0 turns the shared cache off.
    cache_ttl_live: int = Field(default=3, ge=0)  # boards and trips: below ZTP's ~5 s update rate
    cache_ttl_static: int = Field(default=300, ge=0)  # stops, route shapes
    cache_ttl_alerts: int = Field(default=15, ge=0)

    # /health `fresh`: realtime data older than this (or a feed's larger realtime_max_age) is stale; a static
    # feed not checked for this long (3 x STATIC_POLL_INTERVAL) is too
    health_realtime_max_age: int = Field(default=300, ge=1)
    health_static_max_age: int = Field(default=3 * 3600, ge=1)


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()
