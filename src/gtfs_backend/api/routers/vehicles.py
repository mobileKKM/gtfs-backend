from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import LIVE, cached, json_response
from gtfs_backend.api.deps import BboxQuery, Ctx, ModesQuery, RouteNamesQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import TripView, VehicleList
from gtfs_backend.api.services import trips, vehicles

router = APIRouter(prefix=PREFIX, tags=["vehicles"])


@router.get("/vehicles", response_model=VehicleList)
async def list_vehicles(request: Request, ctx: Ctx, bbox: BboxQuery, modes: ModesQuery, route: RouteNamesQuery):
    """Live vehicles inside a bounding box (at most 50 km across), for the map.
    Only vehicles matched to a timetabled trip appear."""
    body = await vehicles.in_bbox(ctx, bbox, modes, route)
    return json_response(request, body, LIVE)


@router.get("/vehicles/{vehicle_key:path}/trip", response_model=TripView)
async def vehicle_trip(request: Request, ctx: Ctx, vehicle_key: str):
    """The trip view of the trip a vehicle (`key` from /vehicles, `{feed_id}:{vehicle_id}`) runs now. Polling this
    follows the vehicle onto its next trip. 404 once the vehicle has left the feed."""
    return await cached(request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: trips.for_vehicle(ctx, vehicle_key))
