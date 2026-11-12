from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from gtfs_backend.api.cache import cached, public
from gtfs_backend.api.deps import Ctx
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import Alert, AlertList
from gtfs_backend.api.services import alerts

router = APIRouter(prefix=PREFIX, tags=["alerts"])


@router.get("/alerts", response_model=AlertList)
async def list_alerts(
    request: Request,
    ctx: Ctx,
    feed: Annotated[str | None, Query(description="only this feed's alerts")] = None,
    route: Annotated[str | None, Query(description="route key (`{feed_id}:{route_id}`, a route's `key`)")] = None,
    stop: Annotated[str | None, Query(description="stop key or id (a stop group covers its stops)")] = None,
    active: Annotated[bool, Query(description="only alerts active now or starting within a day")] = False,
):
    """Service alerts as the feeds publish them (texts, active periods and entities unchanged). Boards and trip views
    only carry the active ones (`active=true` here)."""
    s = ctx.settings
    cache_control = public(s.cache_ttl_alerts)
    return await cached(request, ctx, s.cache_ttl_alerts, cache_control, lambda: alerts.search(ctx, feed, route, stop, active))


@router.get("/alerts/{alert_key:path}", response_model=Alert)
async def get_alert(request: Request, ctx: Ctx, alert_key: str):
    """One alert by its `key` (`{feed_id}:{id}`)."""
    s = ctx.settings
    cache_control = public(s.cache_ttl_alerts)
    return await cached(request, ctx, s.cache_ttl_alerts, cache_control, lambda: alerts.get(ctx, alert_key))
