"""Postgres connections (psycopg 3). Sync connections for imports/CLI, an async pool for the worker and the API."""

from __future__ import annotations

import psycopg
from psycopg_pool import AsyncConnectionPool


def connect(url: str, *, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(url, autocommit=autocommit)


def async_pool(url: str, *, max_size: int = 5, timeout: float = 30) -> AsyncConnectionPool:
    """`timeout`: how long `pool.connection()` waits for a free connection before raising PoolTimeout."""
    # open=False: the caller opens it inside the running event loop
    return AsyncConnectionPool(url, min_size=1, max_size=max_size, timeout=timeout, open=False)
