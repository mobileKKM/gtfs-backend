"""Live vehicles, from the process's live state (api/data/live.py)."""

from __future__ import annotations

from dataclasses import dataclass

from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.schemas import RouteRef, Vehicle, VehicleList
from gtfs_backend.keys import vehicle_key, version_id

MAX_BBOX_KM = 50
COORD_DIGITS = 5  # ~1 m


def stop_state(raw: dict) -> tuple[int | None, str | None]:
    """(stop_sequence, status) the feed reports; the status only counts together with the sequence."""
    seq = raw.get("current_stop_sequence")
    status = raw.get("current_status")
    if seq is None or status is None:
        return seq, None
    return seq, status.lower()


def _model(raw: dict, refs: dict[tuple[str, str], RouteRef]) -> Vehicle:
    trip = raw["trip"]
    route = refs.get((version_id(raw["feed_id"], trip["version"]), trip["route_id"]))
    stop_sequence, status = stop_state(raw)
    return Vehicle(
        key=vehicle_key(raw["feed_id"], raw["vehicle_id"]),
        label=raw.get("label"),
        lat=round(raw["lat"], COORD_DIGITS),
        lon=round(raw["lon"], COORD_DIGITS),
        bearing=raw.get("bearing"),
        route=route,
        headsign=trip.get("headsign"),
        delay_s=raw.get("delay_s"),
        stop_sequence=stop_sequence,
        status=status,
        trip_key=trip["key"],
        timestamp=raw.get("timestamp"),
    )


async def models(ctx: Context, raws: list[dict]) -> list[Vehicle]:
    refs = await ctx.catalog.route_refs((version_id(v["feed_id"], v["trip"]["version"]), v["trip"]["route_id"]) for v in raws)
    return [_model(v, refs) for v in raws]


async def by_ids(ctx: Context, ids: list[str]) -> list[Vehicle]:
    """Vehicles by key; gone ones and positions older than their feed's realtime max age are skipped."""
    live = ctx.live
    return await models(ctx, fresh(ctx, [v for i in ids if (v := live.vehicle(i)) is not None]))


def fresh(ctx: Context, raws: list[dict]) -> list[dict]:
    since = ctx.fresh_since()
    return [v for v in raws if ctx.fresh(v["feed_id"], v.get("timestamp"), since)]


async def in_bbox(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> VehicleList:
    """Vehicles inside the box, optionally only some modes / route names (as models: in_bbox_json is the endpoint's)."""
    pieces = await _in_bbox(ctx, bbox, modes, route_names)
    return VehicleList(timestamp=realtime.newest(p.timestamp for p in pieces), vehicles=[p.model for p in pieces])


async def in_bbox_json(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> bytes:
    """GET /vehicles: a VehicleList body, joined from each vehicle's JSON (built once per position, _pieces):
    byte for byte what in_bbox(...).model_dump_json() gives."""
    pieces = await _in_bbox(ctx, bbox, modes, route_names)
    newest = realtime.newest(p.timestamp for p in pieces)
    head = b'{"timestamp":' + (b"null" if newest is None else str(newest).encode()) + b',"vehicles":['
    return head + b",".join(p.json for p in pieces) + b"]}"


async def _in_bbox(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> list[Piece]:
    width, height = bbox.size_km()
    if max(width, height) > MAX_BBOX_KM:
        raise InvalidInput(f"bbox is larger than {MAX_BBOX_KM:g} km")
    pieces = await _pieces(ctx, ctx.live.vehicles_in(bbox))  # the grid cells the box touches: cut exactly below
    since = ctx.fresh_since()
    return [
        p
        for p in pieces
        if ctx.fresh(p.feed_id, p.timestamp, since)
        and bbox.contains(p.lat, p.lon)
        and (not modes or p.mode in modes)
        and (not route_names or p.route_name in route_names)
    ]


@dataclass(frozen=True, slots=True)
class Piece:
    """One vehicle of /vehicles: what the filters look at, the model and its JSON."""

    feed_id: str
    lat: float  # as shown (rounded)
    lon: float
    mode: str | None
    route_name: str | None
    timestamp: int | None
    model: Vehicle
    json: bytes


async def _pieces(ctx: Context, found: list[tuple[str, dict]]) -> list[Piece]:
    """Each vehicle's Piece, built once per position: the live state keeps it (vehicle_pieces) until the vehicle
    changes, so the next ~thousand requests of the map only filter and join. One whose route isn't known yet (its
    version's routes not loaded) isn't kept."""
    kept = ctx.live.vehicle_pieces
    missing = [(key, raw) for key, raw in found if key not in kept]
    built = await models(ctx, [raw for _, raw in missing]) if missing else []
    new: dict[str, Piece] = {}
    for (key, _), model in zip(missing, built, strict=True):
        new[key] = piece = Piece(
            feed_id=key.split(":", 1)[0],
            lat=model.lat,
            lon=model.lon,
            mode=model.route.mode if model.route else None,
            route_name=model.route.name if model.route else None,
            timestamp=model.timestamp,
            model=model,
            json=model.model_dump_json().encode(),
        )
        if model.route is not None:
            kept[key] = piece
    return [kept.get(key) or new[key] for key, _ in found]
