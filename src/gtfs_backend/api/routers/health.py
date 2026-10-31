from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import NO_STORE, json_response
from gtfs_backend.api.deps import BareCtx
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import Health
from gtfs_backend.api.services import health

router = APIRouter(prefix=PREFIX, tags=["health"])


@router.get("/health", response_model=Health, responses={503: {"model": Health}})
async def get_health(request: Request, ctx: BareCtx):
    """Database and Redis reachability, plus each feed's static version and realtime poll stats. 503 if a store is down."""
    body = await health.check(ctx)
    return json_response(request, body, NO_STORE, status_code=200 if body.ok else 503)
