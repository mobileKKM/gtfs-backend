"""FastAPI app: `gtfs-api` (or `uvicorn --factory gtfs_backend.api.app:create_app`). Everything is under /api/v1."""

from __future__ import annotations

import asyncio
import contextlib
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from redis.asyncio import BlockingConnectionPool, Redis

from gtfs_backend import live_layout, procmem
from gtfs_backend.api import errors
from gtfs_backend.api.context import Context
from gtfs_backend.api.data.catalog import Catalog, Clock
from gtfs_backend.api.routers import alerts, health, routes, stops, trips, vehicles
from gtfs_backend.api.settings import ApiSettings, get_api_settings
from gtfs_backend.api.timing import TimingMiddleware, Timings
from gtfs_backend.db import async_pool
from gtfs_backend.feeds import FeedsFile, load_feeds
from gtfs_backend.logs import log, setup_logging


async def _listen_static_updates(redis: Redis, catalog: Catalog, retry_delay: float) -> None:
    """The worker publishes `static:updated` after an import; reload the catalog then."""
    while True:
        try:
            async with redis.pubsub() as ps:
                await ps.subscribe(live_layout.STATIC_UPDATED)
                async for msg in ps.listen():
                    if msg["type"] == "message":
                        catalog.invalidate()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("static_updates_listener_failed", error=str(exc))
            await asyncio.sleep(retry_delay)


async def _report_stats(timings: Timings, interval: float) -> None:
    """Every `interval`: this process's request times per route (`api_timing`) and its memory (`api_stats`)."""
    while True:
        await asyncio.sleep(interval)
        for r in timings.take():
            log.info("api_timing", route=r.route, n=r.n, p50_ms=r.p50_ms, p95_ms=r.p95_ms, max_ms=r.max_ms, hit_rate=r.hit_rate)
        log.info("api_stats", rss_mb=procmem.rss_mb())


DESCRIPTION = """Read-only API behind the mobileKKM map: live vehicles, trip details, stop search and departures for
Kraków's public transport (ZTP trams and buses, Koleje Małopolskie trains and feeder buses), from GTFS timetables and
GTFS-Realtime.

**Keys.** Everything the app can open has a `key` that endpoints take: a stop's slug (`rondo-mogilskie01-qphdu`), a
vehicle `{feed_id}:{vehicle_id}`, a trip `{feed_id}:{YYYYMMDD}:{trip_id}`, a route `{feed_id}:{route_id}`, an alert
`{feed_id}:{id}`. A field named `<thing>_key` points at another thing's key.

**Conventions.** Times are epoch seconds, dates `YYYYMMDD`. Every field of a schema is always present (`null` when
unset). Responses carry `ETag`; live ones are `no-cache`, so poll them (about every 10 s) with `If-None-Match`.
Errors are `{"error": {"code", "message"}}`."""


def create_app(settings: ApiSettings | None = None, *, feeds: FeedsFile | None = None, clock: Clock = time.time) -> FastAPI:
    """Called once in every API process (API_WORKERS), so each sets up its own logging, pools and catalog."""
    settings = settings or get_api_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = async_pool(settings.database_url, max_size=settings.db_pool_max_size, timeout=settings.db_pool_timeout)
        await pool.open()
        # bounded: under load requests wait for a connection (then 503) instead of opening ever more
        redis = Redis(
            connection_pool=BlockingConnectionPool.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=settings.redis_pool_max_size,
                timeout=settings.redis_pool_timeout,
            )
        )
        feeds_file = feeds or load_feeds(settings.feeds_config)
        # its own pool: services call the catalog while holding a request connection (see api/data/catalog.py)
        catalog_pool = async_pool(
            settings.database_url, max_size=settings.catalog_pool_max_size, timeout=settings.db_pool_timeout
        )
        await catalog_pool.open()
        catalog = Catalog(catalog_pool, feeds_file, clock, refresh_s=settings.catalog_refresh_interval)
        try:  # warm up, so the first requests don't all wait for it; they retry if the database is down
            await catalog.ensure_fresh()
        except Exception as exc:  # noqa: BLE001
            log.warning("catalog_warmup_failed", error=str(exc))
        app.state.ctx = Context(pool, redis, feeds_file, catalog, clock, settings)
        listener = asyncio.create_task(_listen_static_updates(redis, catalog, settings.static_listener_retry_delay))
        reporter = asyncio.create_task(_report_stats(timings, settings.stats_log_interval))
        try:
            yield
        finally:
            for task in (listener, reporter):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await redis.aclose()
            await catalog_pool.close()
            await pool.close()

    app = FastAPI(
        title="mobileKKM GTFS API",
        version="1.2",
        description=DESCRIPTION,
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    timings = Timings()
    app.state.timings = timings
    app.add_middleware(TimingMiddleware, timings=timings)  # added last: outermost, so it times compression too
    errors.install(app)

    for module in (vehicles, stops, trips, routes, alerts, health):
        app.include_router(module.router, responses=errors.ERROR_RESPONSES)
    return app
