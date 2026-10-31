from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import cached, public
from gtfs_backend.api.deps import Ctx, DateQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import RouteShapes
from gtfs_backend.api.services import routes

router = APIRouter(prefix=PREFIX, tags=["routes"])


@router.get("/routes/{route_key:path}/shapes", response_model=RouteShapes)
async def route_shapes(request: Request, ctx: Ctx, route_key: str, date: DateQuery):
    """Every shape a route (a line's `key`, `{feed_id}:{route_id}`) runs on a service day (default today), most used
    first per direction."""
    s = ctx.settings
    cache_control = public(s.cache_ttl_static)
    return await cached(request, ctx, s.cache_ttl_static, cache_control, lambda: routes.shapes(ctx, route_key, date))
