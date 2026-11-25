from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import json_response, public
from gtfs_backend.api.deps import Ctx, DateQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import RouteShapes
from gtfs_backend.api.services import routes

router = APIRouter(prefix=PREFIX, tags=["routes"])


@router.get("/routes/{route_key:path}/shapes", response_model=RouteShapes)
async def route_shapes(request: Request, ctx: Ctx, route_key: str, date: DateQuery):
    """Every shape a route (its `key`, `{feed_id}:{route_id}`) runs on a service day (default today), most used
    first per direction."""
    return json_response(request, await routes.shapes(ctx, route_key, date), public(ctx.settings.cache_ttl_static))
