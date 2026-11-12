from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import LIVE, cached, public
from gtfs_backend.api.deps import Ctx
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import TripLive, TripView
from gtfs_backend.api.services import trips

router = APIRouter(prefix=PREFIX, tags=["trips"])


# before /trips/{trip_key:path}, which would take `/live` into the key
@router.get("/trips/{trip_key:path}/live", response_model=TripLive)
async def get_trip_live(request: Request, ctx: Ctx, trip_key: str):
    """A trip's realtime state, to poll: every stop's predictions and whether it is passed or skipped (match them to
    the trip view's `stops` by `stop_sequence`), the live vehicle (if any) and the alerts that apply."""
    return await cached(request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: trips.live_by_key(ctx, trip_key))


@router.get("/trips/{trip_key:path}", response_model=TripView)
async def get_trip(request: Request, ctx: Ctx, trip_key: str):
    """The trip view by trip key (`{feed_id}:{YYYYMMDD}:{trip_id}`, e.g. a departure's `trip_key`): route, shape and
    every stop with its scheduled times. It doesn't change while the trip runs: poll `/live` for realtime. Trips
    without a shape (Koleje Małopolskie trains) are drawn through their stops (`shape.source`).
    """
    s = ctx.settings
    cache_control = public(s.cache_ttl_static)
    return await cached(request, ctx, s.cache_ttl_static, cache_control, lambda: trips.view_by_key(ctx, trip_key))
