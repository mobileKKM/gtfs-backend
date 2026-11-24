"""Conditional GET with truncation checks and retries (shared by the static and realtime pollers).

ZTP sometimes serves a file while it is being replaced, which gives a short body. We compare the
received size with Content-Length and let the caller validate the payload (zip / protobuf); either
failure is retried a few times before giving up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from gtfs_backend.logs import log

USER_AGENT = "mobileKKM-gtfs-backend/0.1"  # operators like a contact in it (URL or e-mail)
# Answers worth another try within the same poll: server trouble, rate limits, and 404 (ZTP briefly has no file
# while replacing it). Other client errors are raised at once.
RETRY_STATUSES = frozenset({404, 408, 429})


class TruncatedResponse(Exception):
    pass


def _retryable(exc: httpx.HTTPStatusError) -> bool:
    return exc.response.status_code >= 500 or exc.response.status_code in RETRY_STATUSES


@dataclass
class Fetched[T]:
    not_modified: bool
    value: T | None = None
    etag: str | None = None
    last_modified: str | None = None
    size: int = 0
    attempts: int = 1  # downloads tried, including the one that worked


def make_client(timeout: float, connect_timeout: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=connect_timeout),
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )


def _conditional_headers(etag: str | None, last_modified: str | None) -> dict[str, str]:
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    return headers


def _check_length(resp: httpx.Response, received: int) -> None:
    expected = resp.headers.get("Content-Length")
    # Content-Length describes the encoded body; only compare when the body isn't compressed.
    if expected and not resp.headers.get("Content-Encoding") and int(expected) != received:
        raise TruncatedResponse(f"received {received} of {expected} bytes")


async def _retrying[T](
    url: str, attempt: Callable[[int], Awaitable[T]], *, retries: int, retry_delay: float, error_level: str
) -> T:
    """Run `attempt(n)` for n = 1..retries until it returns. HTTP errors are retried only for RETRY_STATUSES / 5xx;
    any other error (truncation, validation, transport) is retried and logged at `error_level`."""
    last_exc: Exception | None = None
    for n in range(1, retries + 1):
        try:
            return await attempt(n)
        except httpx.HTTPStatusError as exc:
            if not _retryable(exc) or n == retries:
                raise
            last_exc = exc
            log.info("fetch_retry", url=url, attempt=n, status=exc.response.status_code)
        except Exception as exc:  # noqa: BLE001 - truncation, parse and transport errors are retried
            last_exc = exc
            if n == retries:
                break
            getattr(log, error_level)("fetch_retry", url=url, attempt=n, error=f"{type(exc).__name__}: {exc}")
        await asyncio.sleep(retry_delay)
    assert last_exc is not None
    raise last_exc


async def fetch_bytes[T](
    client: httpx.AsyncClient,
    url: str,
    parse: Callable[[bytes], T],
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    retries: int,
    retry_delay: float,
    timeout: float | None = None,
) -> Fetched[T]:
    """GET a small file into memory and parse it; retries on truncation, parse errors and RETRY_STATUSES / 5xx.
    `timeout` (seconds) replaces the client's read/write/pool timeouts for each attempt."""
    per_attempt = httpx.USE_CLIENT_DEFAULT if timeout is None else httpx.Timeout(timeout, connect=client.timeout.connect)

    async def attempt(n: int) -> Fetched[T]:
        resp = await client.get(url, headers=_conditional_headers(etag, last_modified), timeout=per_attempt)
        if resp.status_code == 304:
            return Fetched(not_modified=True, etag=etag, last_modified=last_modified, attempts=n)
        resp.raise_for_status()
        body = resp.content
        _check_length(resp, len(body))
        value = parse(body)
        return Fetched(False, value, resp.headers.get("ETag"), resp.headers.get("Last-Modified"), len(body), n)

    return await _retrying(url, attempt, retries=retries, retry_delay=retry_delay, error_level="info")


async def fetch_to_file(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    validate: Callable[[Path], None],
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    retries: int,
    retry_delay: float,
) -> Fetched[Path]:
    """Stream a (large) file to `dest` and validate it; retries on truncation, validation errors and RETRY_STATUSES / 5xx.
    A download that keeps failing that way leaves no file behind."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    async def attempt(n: int) -> Fetched[Path]:
        async with client.stream("GET", url, headers=_conditional_headers(etag, last_modified)) as resp:
            if resp.status_code == 304:
                return Fetched(not_modified=True, etag=etag, last_modified=last_modified, attempts=n)
            resp.raise_for_status()
            received = 0
            with dest.open("wb") as fh:
                async for chunk in resp.aiter_bytes():
                    fh.write(chunk)
                    received += len(chunk)
            if resp.headers.get("Content-Encoding"):
                raise TruncatedResponse("compressed static downloads are not supported")
            _check_length(resp, received)
        await asyncio.to_thread(validate, dest)
        return Fetched(False, dest, resp.headers.get("ETag"), resp.headers.get("Last-Modified"), received, n)

    try:
        return await _retrying(url, attempt, retries=retries, retry_delay=retry_delay, error_level="warning")
    except httpx.HTTPStatusError:
        raise
    except Exception:
        dest.unlink(missing_ok=True)
        raise
