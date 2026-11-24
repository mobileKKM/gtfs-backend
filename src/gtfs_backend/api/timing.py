"""Where a request's time goes, sent back as a `Server-Timing` header (browser dev tools show it):

    Server-Timing: db;dur=4.1;desc="3 queries", db-wait;dur=0.0, redis;dur=1.2;desc="4 calls",
                   serialize;dur=0.3, total;dur=7.9

Measured at the drivers, so services need no changes: every `conn.execute` (`db`), every wait for a pooled connection
(`db-wait`), every Redis command or pipeline (`redis`, pool waits included). `serialize` is building the JSON body, `total` the
whole request up to its response headers (gzip included). Work running concurrently (asyncio.gather) adds up, so the
parts can exceed `total`. Requests aren't logged; the numbers only go to whoever made the request.

Background work (catalog refreshes, listeners) runs outside any request and is not counted.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline


@dataclass
class Timings:
    start: float
    db: float = 0.0
    db_n: int = 0
    db_wait: float = 0.0
    redis: float = 0.0
    redis_n: int = 0
    serialize: float = 0.0

    def header(self, now: float) -> str:
        def ms(seconds: float) -> str:
            return f"{seconds * 1000:.1f}"

        parts = [
            f'db;dur={ms(self.db)};desc="{self.db_n} queries"',
            f"db-wait;dur={ms(self.db_wait)}",
            f'redis;dur={ms(self.redis)};desc="{self.redis_n} calls"',
            f"serialize;dur={ms(self.serialize)}",
        ]
        parts.append(f"total;dur={ms(now - self.start)}")
        return ", ".join(parts)


_current: ContextVar[Timings | None] = ContextVar("server_timing", default=None)


@contextmanager
def serializing():
    t = _current.get()
    start = time.perf_counter()
    try:
        yield
    finally:
        if t is not None:
            t.serialize += time.perf_counter() - start


# ------------------------------------------------------------------------------------------------- drivers


class TimedConnection(AsyncConnection):
    async def execute(self, *args, **kwargs):
        t = _current.get()
        if t is None:
            return await super().execute(*args, **kwargs)
        start = time.perf_counter()
        try:
            return await super().execute(*args, **kwargs)
        finally:
            t.db += time.perf_counter() - start
            t.db_n += 1


class TimedPool(AsyncConnectionPool):
    """`pool.connection()` goes through getconn: the time waiting for a free connection."""

    async def getconn(self, timeout: float | None = None):
        t = _current.get()
        if t is None:
            return await super().getconn(timeout)
        start = time.perf_counter()
        try:
            return await super().getconn(timeout)
        finally:
            t.db_wait += time.perf_counter() - start


class TimedPipeline(Pipeline):
    """A pipeline counts as one call (one round trip)."""

    async def execute(self, raise_on_error: bool = True):
        t = _current.get()
        if t is None:
            return await super().execute(raise_on_error)
        start = time.perf_counter()
        try:
            return await super().execute(raise_on_error)
        finally:
            t.redis += time.perf_counter() - start
            t.redis_n += 1


class TimedRedis(Redis):
    """Single commands (GET, MGET, GEOSEARCH, ...) and pipelines. Pub/sub isn't counted (not used on requests)."""

    def pipeline(self, transaction: bool = True, shard_hint: str | None = None) -> Pipeline:
        return TimedPipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)

    async def execute_command(self, *args, **options):
        t = _current.get()
        if t is None:
            return await super().execute_command(*args, **options)
        start = time.perf_counter()
        try:
            return await super().execute_command(*args, **options)
        finally:
            t.redis += time.perf_counter() - start
            t.redis_n += 1


# ------------------------------------------------------------------------------------------------- middleware


class ServerTimingMiddleware:
    """Pure ASGI (keeps the context variable visible to the endpoint). Add it last, so it is the outermost
    middleware and `total` includes gzip."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        timings = Timings(time.perf_counter())
        token = _current.set(timings)

        async def send_with_timing(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"server-timing", timings.header(time.perf_counter()).encode()))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_timing)
        finally:
            _current.reset(token)
