"""JSON responses with an ETag (304 when unchanged) and Cache-Control, and a response cache shared by all API processes.

The cache keeps finished bodies in Redis for a few seconds (boards, trips) or minutes (search, route shapes), so
many people looking at the same stop cost one computation per TTL, not one per request, whichever process answers.
Every process serves the same body (and ETag) for that time, so polling clients get 304s.

Keys: the route template, its path parameters and only the query parameters the route declares (so made-up ones
can't fill Redis), plus the catalog's fingerprint (a new import or a new day starts fresh), hashed. Concurrent
misses in one process compute once. Redis trouble only turns the cache off for that request.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Iterable

from fastapi import Request, Response
from pydantic import BaseModel
from redis.exceptions import RedisError

from gtfs_backend.api.context import Context
from gtfs_backend.api.timing import CACHE_STATE
from gtfs_backend.logs import log

NO_STORE = "no-store"
# Vehicles, boards, trips: change every few seconds. Clients poll (or follow SSE later) and always revalidate,
# getting a 304 via the ETag while nothing changed.
LIVE = "no-cache"

KEY_PREFIX = "api:resp:"
FALLBACK_LIVE_S = 3  # live responses' cache time while realtime updates aren't followed (listener reconnecting)
MAX_BODY = 512 * 1024  # bigger bodies aren't cached

_query_names: dict[int, frozenset[str]] = {}  # id(route) -> its declared query parameters (aliases)


def feeds_of(ctx: Context, key: str) -> list[str]:
    """The feed a trip or vehicle key (`{feed_id}:...`) belongs to; every feed when the key names none (the request
    then fails anyway)."""
    feed_id = key.split(":", 1)[0]
    return [feed_id] if feed_id in ctx.feed_ids else ctx.feed_ids


def public(max_age: int) -> str:
    """Stops, routes, route shapes and alerts (max-age from settings)."""
    return f"public, max-age={max_age}"


def _etag(body: bytes) -> str:
    return '"' + hashlib.blake2b(body, digest_size=12).hexdigest() + '"'


def _respond(request: Request, body: bytes, etag: str, cache_control: str, status_code: int = 200) -> Response:
    headers = {"ETag": etag, "Cache-Control": cache_control}
    if status_code == 200 and etag in [t.strip() for t in request.headers.get("if-none-match", "").split(",")]:
        return Response(status_code=304, headers=headers)
    return Response(body, status_code=status_code, media_type="application/json", headers=headers)


def json_response(request: Request, model: BaseModel, cache_control: str, status_code: int = 200) -> Response:
    """Serialize `model`; answer 304 if the client already has this body."""
    body = model.model_dump_json().encode()
    return _respond(request, body, _etag(body), cache_control, status_code)


def _query_aliases(dependant) -> set[str]:
    """Query parameters of a route, including those of its dependencies (BboxQuery etc.)."""
    return {p.alias for p in dependant.query_params} | {a for d in dependant.dependencies for a in _query_aliases(d)}


def _declared_query(request: Request) -> frozenset[str]:
    route = request.scope["route"]
    names = _query_names.get(id(route))
    if names is None:
        names = _query_names[id(route)] = frozenset(_query_aliases(route.dependant))
    return names


def cache_key(request: Request, fingerprint: str) -> str:
    names = _declared_query(request)
    raw = json.dumps(
        [
            fingerprint,
            request.scope["route"].path,
            sorted(request.path_params.items()),
            sorted((k, v) for k, v in request.query_params.multi_items() if k in names),
        ],
        separators=(",", ":"),
    )
    return KEY_PREFIX + hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()


async def _get(ctx: Context, key: str) -> tuple[str, bytes] | None:
    try:
        value = await ctx.redis.get(key)
    except RedisError as exc:
        log.warning("response_cache_unavailable", error=str(exc))
        return None
    if not value:
        return None
    etag, _, body = value.partition("\n")
    return etag, body.encode()


async def _set(ctx: Context, key: str, etag: str, body: bytes, ttl: int) -> None:
    if len(body) > MAX_BODY:
        return
    try:
        await ctx.redis.set(key, etag + "\n" + body.decode(), ex=ttl)
    except RedisError as exc:
        log.warning("response_cache_unavailable", error=str(exc))


async def _compute(ctx: Context, key: str, ttl: int, compute: Callable[[], Awaitable[BaseModel]]) -> tuple[str, bytes]:
    body = (await compute()).model_dump_json().encode()
    etag = _etag(body)
    await _set(ctx, key, etag, body, ttl)
    return etag, body


def _done(ctx: Context, key: str, task: asyncio.Future) -> None:
    ctx.inflight.pop(key, None)
    if not task.cancelled():
        task.exception()  # retrieved here too, in case every waiter went away


async def cached(
    request: Request,
    ctx: Context,
    ttl: int,
    cache_control: str,
    compute: Callable[[], Awaitable[BaseModel]],
    live_feeds: Iterable[str] | None = None,
) -> Response:
    """The response of `compute()`, from the shared cache when another request made it less than `ttl` s ago.
    Errors (404, 422, ...) are raised as usual and never cached. ttl 0: no caching.

    `live_feeds`: the response shows these feeds' realtime data. It is cached under their current realtime generation,
    so as soon as the worker stores newer data the next request computes afresh; `ttl` then only bounds how long
    time alone (a departure's grace period) may go unnoticed."""
    if ttl <= 0:
        return json_response(request, await compute(), cache_control)
    fingerprint = ctx.catalog.fingerprint
    if live_feeds is not None:
        # without the listener, updates could be missed: then a live response is cached FALLBACK_LIVE_S at most
        generation = ctx.realtime_generation(live_feeds) if ctx.realtime_listening else f"t{ctx.now() // FALLBACK_LIVE_S}"
        fingerprint += "/" + generation
    key = cache_key(request, fingerprint)
    hit = await _get(ctx, key)
    setattr(request.state, CACHE_STATE, "miss" if hit is None else "hit")
    if hit is None:
        task = ctx.inflight.get(key)
        if task is None:
            task = asyncio.ensure_future(_compute(ctx, key, ttl, compute))
            ctx.inflight[key] = task
            task.add_done_callback(lambda t: _done(ctx, key, t))
        # shield: one waiter going away (client disconnected) mustn't cancel the others' result
        hit = await asyncio.shield(task)
    etag, body = hit
    return _respond(request, body, etag, cache_control)
