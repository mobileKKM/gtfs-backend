from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from gtfs_backend.api.cache import LIVE, cached, json_response, public
from gtfs_backend.api.deps import BboxQuery, Ctx, FeedsQuery, ModesQuery, RoutesQuery, StopKindQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import DepartureBoard, SearchResult, StopList
from gtfs_backend.api.services import departures, stops

router = APIRouter(prefix=PREFIX, tags=["stops"])


@router.get("/stops", response_model=StopList)
async def stops_in_bbox(request: Request, ctx: Ctx, bbox: BboxQuery, modes: ModesQuery, kind: StopKindQuery):
    """Active stops inside a bounding box, for the map: physical ones (`kind=stop`) by default, or stop groups
    (`kind=group`, one marker per logical stop at its stops' centroid) for far zoom levels.

    `modes` keeps stops served by any of them, e.g. `kind=group&modes=rail` for train stations."""
    return json_response(request, await stops.in_bbox(ctx, bbox, modes, kind), public(ctx.settings.cache_ttl_static))


@router.get("/stops/search", response_model=SearchResult)
async def search_stops(
    request: Request,
    ctx: Ctx,
    q: Annotated[str, Query(min_length=1, max_length=100)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
):
    """ZTP stop groups (where the metropolitan card is valid) by name; Polish letters and case don't matter
    ("rondo mog" finds "Rondo Mogilskie"). Other feeds' stops are on the map and in departures, but not searched.

    Each result has its stops with directions (`towards` = the next stop group trips go to, with lines)."""
    s = ctx.settings
    cache_control = public(s.cache_ttl_static)
    return await cached(request, ctx, s.cache_ttl_static, cache_control, lambda: stops.search(ctx, q, limit))


@router.get("/stops/{key}/departures", response_model=DepartureBoard)
async def departure_board(
    request: Request,
    ctx: Ctx,
    key: str,
    feeds: FeedsQuery,
    routes: RoutesQuery,
    time: Annotated[int | None, Query(ge=0, description="epoch seconds: departures from then on; none: from now")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    """Departures from a stop group (all its stops) or one stop, with realtime.

    Without `time`: the next departures (delayed ones stay until they leave). With it: the departures from then on,
    where those that have left show as `departed`. Page forward by passing the last departure's `time` (departures
    in the same second can repeat: tell them apart by `trip_key` and `stop_sequence`). `feeds` and `routes` (line
    names) narrow the board and its alerts. Canceled trips and skipped stops stay with their status.
    """
    return await cached(
        request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: departures.board(ctx, key, time, feeds, routes, limit)
    )
