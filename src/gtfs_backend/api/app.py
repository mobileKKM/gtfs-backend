"""FastAPI app: `gtfs-api` (or `uvicorn --factory gtfs_backend.api.app:create_app`). Everything is under /api/v1."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from redis.asyncio import BlockingConnectionPool, Redis

from gtfs_backend import live_layout, metrics
from gtfs_backend.api import errors
from gtfs_backend.api.context import Context
from gtfs_backend.api.data.catalog import Catalog, Clock
from gtfs_backend.api.data.live import LiveState
from gtfs_backend.api.metrics import RequestMetricsMiddleware
from gtfs_backend.api.routers import alerts, health, routes, stops, trips, vehicles
from gtfs_backend.api.routers import metrics as metrics_router
from gtfs_backend.api.settings import ApiSettings, get_api_settings
from gtfs_backend.api.timing import ServerTimingMiddleware, TimedConnection, TimedPool, TimedRedis
from gtfs_backend.db import async_pool
from gtfs_backend.feeds import FeedsFile, load_feeds
from gtfs_backend.logs import log, setup_logging

GZIP_LEVEL = 6
LIVE_WARMUP_S = 10  # how long the start waits for the live data's first load


async def _listen_static_updates(redis: Redis, catalog: Catalog, retry_delay: float) -> None:
    """The worker publishes `static:updated` after an import (reload the catalog, which reloads the snapshot too) and
    `snapshot:updated` after a new day's snapshot (reload only that)."""
    while True:
        try:
            async with redis.pubsub() as ps:
                await ps.subscribe(live_layout.STATIC_UPDATED, live_layout.SNAPSHOT_UPDATED)
                async for msg in ps.listen():
                    if msg["type"] != "message":
                        continue
                    if msg["channel"] == live_layout.SNAPSHOT_UPDATED:
                        catalog.timetable_changed()
                    else:
                        catalog.invalidate()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("static_updates_listener_failed", error=str(exc))
            await asyncio.sleep(retry_delay)


DESCRIPTION = """Read-only API behind the mobileKKM map: live vehicles, trip details, stop search and departures for
Kraków's public transport (ZTP trams and buses, Koleje Małopolskie trains and feeder buses), from GTFS timetables and
GTFS-Realtime.

**Keys.** Everything the app can open has a `key` that endpoints take: a stop's slug (`rondo-mogilskie01-qphdu`), a
vehicle `{feed_id}:{vehicle_id}`, a trip `{feed_id}:{YYYYMMDD}:{trip_id}`, a route `{feed_id}:{route_id}`, an alert
`{feed_id}:{id}`. A field named `<thing>_key` points at another thing's key.

**Conventions.** Times are epoch seconds, dates `YYYYMMDD`. Every field of a schema is always present (`null` when
unset). Responses carry `ETag`; live ones are `no-cache`, so poll them (about every 10 s) with `If-None-Match`.
Errors are `{"error": {"code", "message"}}`."""


def create_app(settings: ApiSettings | None = None, *, feeds: FeedsFile | None = None, clock: Clock | None = None) -> FastAPI:
    """Called once in every API process (API_WORKERS), so each sets up its own logging, pools and catalog.
    `clock`: default time.time(), shifted by CLOCK_OFFSET (benchmark replays)."""
    settings = settings or get_api_settings()
    setup_logging(settings.log_level)
    if clock is None:
        clock = settings.clock()
        if settings.clock_offset:
            log.warning("clock_offset", seconds=settings.clock_offset)
    recorder = metrics.Recorder()  # this process's request latencies (gtfs_backend/metrics.py)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = async_pool(
            settings.database_url,
            max_size=settings.db_pool_max_size,
            timeout=settings.db_pool_timeout,
            pool_class=TimedPool,
            connection_class=TimedConnection,
        )
        await pool.open()
        # bounded: under load requests wait for a connection (then 503) instead of opening ever more
        redis = TimedRedis(
            connection_pool=BlockingConnectionPool.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=settings.redis_pool_max_size,
                timeout=settings.redis_pool_timeout,
            )
        )
        feeds_file = feeds or load_feeds(settings.feeds_config)
        catalog = Catalog(
            pool, feeds_file, clock, refresh_s=settings.catalog_refresh_interval, snapshot_dir=settings.snapshot_dir
        )
        try:  # warm up, so the first requests don't all wait for it; they retry if the database is down
            await catalog.ensure_fresh()
        except Exception as exc:  # noqa: BLE001
            log.warning("catalog_warmup_failed", error=str(exc))
        live = LiveState(
            redis,
            [f.feed_id for f in feeds_file.enabled],
            {f.feed_id: [k.value for k, _ in f.realtime.items()] for f in feeds_file.enabled},
        )
        live.start()
        if not await live.wait_ready(LIVE_WARMUP_S):  # requests before the first load get 503s
            log.warning("live_warmup_failed")
        app.state.ctx = Context(pool, redis, feeds_file, catalog, clock, settings, live)
        listener = asyncio.create_task(_listen_static_updates(redis, catalog, settings.static_listener_retry_delay))
        # a context of its own: flushing is no request's work (api/timing.py)
        flusher = asyncio.create_task(recorder.run(redis), context=contextvars.Context())
        try:
            yield
        finally:
            for task in (listener, flusher):  # the flusher sends what is left when cancelled, so before Redis closes
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await live.close()
            await catalog.close()
            await redis.aclose()
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
    # level 6 (zlib's default), not Starlette's 9: about a quarter of the time for a 5 % bigger body (640 kB of JSON:
    # 5.5 ms instead of 19.7 ms, 77 kB instead of 73 kB)
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=GZIP_LEVEL)
    if settings.server_timing:
        app.add_middleware(ServerTimingMiddleware)  # outside gzip: its `total` includes it
    if settings.request_metrics:
        app.add_middleware(RequestMetricsMiddleware, recorder=recorder)  # outermost: times everything
    errors.install(app)

    for module in (vehicles, stops, trips, routes, alerts, health, metrics_router):
        app.include_router(module.router, responses=errors.ERROR_RESPONSES)
    return app
