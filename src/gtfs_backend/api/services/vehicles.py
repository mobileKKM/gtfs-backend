"""Live vehicles (Redis only)."""

from __future__ import annotations

from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.schemas import Line, MapVehicle, VehicleList
from gtfs_backend.keys import vehicle_key, version_id

MAX_BBOX_KM = 50
COORD_DIGITS = 5  # ~1 m


def _model(raw: dict, lines: dict[tuple[str, str], Line]) -> MapVehicle:
    trip = raw["trip"]
    line = lines.get((version_id(raw["feed_id"], trip["version"]), trip["route_id"]))
    return MapVehicle(
        key=vehicle_key(raw["feed_id"], raw["vehicle_id"]),
        label=raw.get("label"),
        lat=round(raw["lat"], COORD_DIGITS),
        lon=round(raw["lon"], COORD_DIGITS),
        bearing=raw.get("bearing"),
        line=line,
        headsign=trip.get("headsign"),
        delay_s=raw.get("delay_s"),
        trip_key=trip["key"],
        timestamp=raw.get("timestamp"),
    )


async def models(ctx: Context, raws: list[dict]) -> list[MapVehicle]:
    lines = await ctx.catalog.lines((version_id(v["feed_id"], v["trip"]["version"]), v["trip"]["route_id"]) for v in raws)
    return [_model(v, lines) for v in raws]


async def by_ids(ctx: Context, ids: list[str]) -> list[MapVehicle]:
    """Vehicles by key; expired ones are skipped."""
    return await models(ctx, await realtime.vehicle_json(ctx.redis, ids))


async def in_bbox(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> VehicleList:
    """Vehicles inside the box, optionally only some modes / line names."""
    width, height = bbox.size_km()
    if max(width, height) > MAX_BBOX_KM:
        raise InvalidInput(f"bbox is larger than {MAX_BBOX_KM:g} km")
    center = bbox.center
    # GEOSEARCH boxes are approximate: search a little wider, then cut exactly
    ids = await realtime.vehicles_near(ctx.redis, center.lat, center.lon, width * 1.02 + 0.01, height * 1.02 + 0.01)
    out = [
        v
        for v in await by_ids(ctx, sorted(ids))
        if bbox.contains(v.lat, v.lon)
        and (not modes or (v.line is not None and v.line.mode in modes))
        and (not route_names or (v.line is not None and v.line.name in route_names))
    ]
    return VehicleList(timestamp=realtime.newest(v.timestamp for v in out), vehicles=out)
