"""JSON responses with an ETag (304 when unchanged) and Cache-Control.

Every response is computed when it is asked for: there is no response cache. The ETag is the body's hash, so a
client polling unchanged data gets a 304 (the body is computed, compared and not sent). Polled responses carry the
time of the newest realtime data they show, not of the request, so their ETag only changes with the data.
"""

from __future__ import annotations

import hashlib

from fastapi import Request, Response
from pydantic import BaseModel

from gtfs_backend.api import timing

NO_STORE = "no-store"
# Vehicles, boards, trips: change every few seconds. Clients poll and always revalidate, getting a 304 via the ETag
# while nothing changed.
LIVE = "no-cache"


def public(max_age: int) -> str:
    """Stops, trips, route shapes, search and alerts (max-age from settings)."""
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
    with timing.serializing():
        body = model.model_dump_json().encode()
        etag = _etag(body)
    return _respond(request, body, etag, cache_control, status_code)


def json_bytes_response(request: Request, body: bytes, cache_control: str) -> Response:
    """An already serialized body; answer 304 if the client already has it."""
    with timing.serializing():
        etag = _etag(body)
    return _respond(request, body, etag, cache_control)
