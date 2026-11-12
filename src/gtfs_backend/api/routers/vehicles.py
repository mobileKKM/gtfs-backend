from __future__ import annotations

from fastapi import APIRouter, Request

from gtfs_backend.api.cache import LIVE, cached, json_response
from gtfs_backend.api.deps import BboxQuery, Ctx, ModesQuery, RoutesQuery
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas import TripLive, VehicleList
from gtfs_backend.api.services import trips, vehicles

router = APIRouter(prefix=PREFIX, tags=["vehicles"])


@router.get("/vehicles", response_model=VehicleList)
async def list_vehicles(request: Request, ctx: Ctx, bbox: BboxQuery, modes: ModesQuery, routes: RoutesQuery):
    """Live vehicles inside a bounding box (at most 50 km across), for the map.
    Only vehicles matched to a timetabled trip appear."""
    body = await vehicles.in_bbox(ctx, bbox, modes, routes)
    return json_response(request, body, LIVE)


@router.get("/vehicles/{vehicle_key:path}/trip", response_model=TripLive)
async def vehicle_trip(request: Request, ctx: Ctx, vehicle_key: str):
    """The live state of the trip a vehicle (`key` from /vehicles, `{feed_id}:{vehicle_id}`) runs now, as
    /trips/{key}/live. Polling this follows the vehicle onto its next trip: when `key` changes, fetch /trips/{key}
    again. 404 once the vehicle has left the feed."""
    return await cached(request, ctx, ctx.settings.cache_ttl_live, LIVE, lambda: trips.for_vehicle(ctx, vehicle_key))
