from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import LIVE, cached
from gtfs_backend.api.deps import Ctx
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import TripView
from gtfs_backend.api.services import trips

router = APIRouter(prefix=PREFIX, tags=["trips"])


@router.get("/trips/{trip_key:path}", response_model=TripView)
async def get_trip(request: Request, ctx: Ctx, trip_key: str):
    """The trip view by trip key (`{feed_id}:{YYYYMMDD}:{trip_id}`, e.g. a departure's `trip_key`): shape, every
    stop with scheduled and estimated times, the live vehicle (if any) and alerts. Trips without a shape (Koleje
    Małopolskie trains) are drawn through their stops (`shape.source`).
    """
    return await cached(request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: trips.view_by_key(ctx, trip_key))
