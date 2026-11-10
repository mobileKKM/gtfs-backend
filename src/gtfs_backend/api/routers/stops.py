from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from gtfs_backend.api.cache import LIVE, cached, json_response, public
from gtfs_backend.api.deps import BboxQuery, Ctx, ModesQuery, NearQuery, StopKindQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import DepartureBoard, SearchResult, StopList
from gtfs_backend.api.services import departures, stops

router = APIRouter(prefix=PREFIX, tags=["stops"])


@router.get("/stops", response_model=StopList)
async def stops_in_bbox(request: Request, ctx: Ctx, bbox: BboxQuery, modes: ModesQuery, kind: StopKindQuery):
    """Active stops inside a bounding box, for the map: physical ones (platforms) by default, or logical ones
    (`kind=group`, one marker per stop at its platforms' centroid) for far zoom levels.

    `modes` keeps stops served by any of them, e.g. `kind=group&modes=rail` for train stations."""
    return json_response(request, await stops.in_bbox(ctx, bbox, modes, kind), public(ctx.settings.cache_ttl_static))


@router.get("/stops/search", response_model=SearchResult)
async def search_stops(
    request: Request,
    ctx: Ctx,
    q: Annotated[str, Query(min_length=1, max_length=100)],
    near: NearQuery,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    """Logical ZTP stops (where the metropolitan card is valid) by name; Polish letters and case don't matter
    ("rondo mog" finds "Rondo Mogilskie"). Other feeds' stops are on the map and in departures, but not searched.

    Each result has its platforms with directions (`towards` = the next stop trips go to, with lines) and the
    lines departing from it today."""
    s = ctx.settings
    cache_control = public(s.cache_ttl_static)
    return await cached(request, ctx, s.cache_ttl_static, cache_control, lambda: stops.search(ctx, q, near, limit))


@router.get("/stops/{key}/departures", response_model=DepartureBoard)
async def departure_board(
    request: Request,
    ctx: Ctx,
    key: str,
    cursor: Annotated[str | None, Query(description="`earlier` or `later` of a previous page; none: from now")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    """Departures from a logical stop (all its platforms) or one platform, with realtime, a page at a time.

    Without a cursor: the next departures (delayed ones stay until they leave). Follow `later` to scroll forward and
    `earlier` to scroll back, where departures that have left show as `departed`. Canceled trips and skipped stops
    stay with their status. Alerts come with the first page. Poll the first page again for updates.
    """
    return await cached(request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: departures.board(ctx, key, cursor, limit))
