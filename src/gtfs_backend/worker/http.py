"""Conditional GET with truncation checks and retries (shared by the static and realtime pollers).

ZTP sometimes serves a file while it is being replaced, which gives a short body. We compare the
received size with Content-Length and let the caller validate the payload (zip / protobuf); either
failure is retried a few times before giving up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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


async def fetch_bytes[T](
    client: httpx.AsyncClient,
    url: str,
    parse: Callable[[bytes], T],
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    retries: int,
    retry_delay: float,
) -> Fetched[T]:
    """GET a small file into memory and parse it; retries on truncation, parse errors and RETRY_STATUSES / 5xx."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(url, headers=_conditional_headers(etag, last_modified))
            if resp.status_code == 304:
                return Fetched(not_modified=True, etag=etag, last_modified=last_modified, attempts=attempt)
            resp.raise_for_status()
            body = resp.content
            _check_length(resp, len(body))
            value = parse(body)
            return Fetched(False, value, resp.headers.get("ETag"), resp.headers.get("Last-Modified"), len(body), attempt)
        except httpx.HTTPStatusError as exc:
            if not _retryable(exc) or attempt == retries:
                raise
            last_exc = exc
            log.info("fetch_retry", url=url, attempt=attempt, status=exc.response.status_code)
            await asyncio.sleep(retry_delay)
        except Exception as exc:  # noqa: BLE001 - truncation, parse and transport errors are retried
            last_exc = exc
            if attempt < retries:
                log.info("fetch_retry", url=url, attempt=attempt, error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(retry_delay)
    assert last_exc is not None
    raise last_exc


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
    """Stream a (large) file to `dest` and validate it; retries on truncation, validation errors and RETRY_STATUSES / 5xx."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            async with client.stream("GET", url, headers=_conditional_headers(etag, last_modified)) as resp:
                if resp.status_code == 304:
                    return Fetched(not_modified=True, etag=etag, last_modified=last_modified, attempts=attempt)
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
            return Fetched(False, dest, resp.headers.get("ETag"), resp.headers.get("Last-Modified"), received, attempt)
        except httpx.HTTPStatusError as exc:
            if not _retryable(exc) or attempt == retries:
                raise
            last_exc = exc
            log.info("fetch_retry", url=url, attempt=attempt, status=exc.response.status_code)
            await asyncio.sleep(retry_delay)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                log.warning("fetch_retry", url=url, attempt=attempt, error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(retry_delay)
    dest.unlink(missing_ok=True)
    assert last_exc is not None
    raise last_exc
