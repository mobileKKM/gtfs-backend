from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from gtfs_backend.api.cache import json_response, public
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
    """Service alerts, severe first, with `entities` saying where each applies in our terms (routes, trips, stops,
    stop groups). Boards and trip views only carry the active ones (`active=true` here)."""
    body = await alerts.search(ctx, feed, route, stop, active)
    return json_response(request, body, public(ctx.settings.cache_ttl_alerts))


@router.get("/alerts/{alert_key:path}", response_model=Alert)
async def get_alert(request: Request, ctx: Ctx, alert_key: str):
    """One alert by its `key` (`{feed_id}:{id}`)."""
    return json_response(request, await alerts.get(ctx, alert_key), public(ctx.settings.cache_ttl_alerts))
