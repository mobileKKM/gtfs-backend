"""Live vehicles (Redis only)."""

from __future__ import annotations

import time
from dataclasses import dataclass

import orjson

from gtfs_backend.api import timing
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
    """Vehicles by key; expired ones and positions older than their feed's realtime max age are skipped."""
    return await models(ctx, fresh(ctx, await realtime.vehicle_json(ctx.redis, ids)))


def fresh(ctx: Context, raws: list[dict]) -> list[dict]:
    since = ctx.fresh_since()
    return [v for v in raws if ctx.fresh(v["feed_id"], v.get("timestamp"), since)]


async def in_bbox(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> VehicleList:
    """Vehicles inside the box, optionally only some modes / route names (as models: in_bbox_json is the endpoint's)."""
    pieces = await _in_bbox(ctx, bbox, modes, route_names)
    return VehicleList(timestamp=realtime.newest(p.timestamp for p in pieces), vehicles=[p.model for p in pieces])


async def in_bbox_json(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> bytes:
    """GET /vehicles: a VehicleList body, joined from each vehicle's JSON (built once per Redis value, VehiclePieces):
    byte for byte what in_bbox(...).model_dump_json() gives."""
    pieces = await _in_bbox(ctx, bbox, modes, route_names)
    with timing.serializing():
        newest = realtime.newest(p.timestamp for p in pieces)
        head = b'{"timestamp":' + (b"null" if newest is None else str(newest).encode()) + b',"vehicles":['
        return head + b",".join(p.json for p in pieces) + b"]}"


async def _in_bbox(ctx: Context, bbox: Bbox, modes: set[str], route_names: set[str]) -> list[Piece]:
    width, height = bbox.size_km()
    if max(width, height) > MAX_BBOX_KM:
        raise InvalidInput(f"bbox is larger than {MAX_BBOX_KM:g} km")
    center = bbox.center
    # GEOSEARCH boxes are approximate: search a little wider, then cut exactly
    ids = await realtime.vehicles_near(ctx.redis, center.lat, center.lon, width * 1.02 + 0.01, height * 1.02 + 0.01)
    values = await realtime.vehicle_values(ctx.redis, sorted(ids))
    pieces = await ctx.memo.setdefault("vehicles", VehiclePieces()).get(ctx, values)
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


class VehiclePieces:
    """Each vehicle's model and JSON, built once per distinct rt:veh value (a new one comes when the vehicle moves, every
    few seconds) and kept while requests still see that value: the next ~thousand requests of the map only filter and
    join. A value whose route isn't known yet (its version's routes not loaded) isn't kept."""

    SWEEP_S = 30  # values not seen for this long are dropped

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self._pieces: dict[str, Piece] = {}
        self._seen: set[str] = set()
        self._swept = clock()

    async def get(self, ctx: Context, values: list[str]) -> list[Piece]:
        missing = [v for v in dict.fromkeys(values) if v not in self._pieces]
        new: dict[str, Piece] = {}
        if missing:
            for value, model in zip(missing, await models(ctx, [orjson.loads(v) for v in missing]), strict=True):
                new[value] = piece = Piece(
                    feed_id=model.key.split(":", 1)[0],
                    lat=model.lat,
                    lon=model.lon,
                    mode=model.route.mode if model.route else None,
                    route_name=model.route.name if model.route else None,
                    timestamp=model.timestamp,
                    model=model,
                    json=model.model_dump_json().encode(),
                )
                if model.route is not None:
                    self._pieces[value] = piece
        out = [self._pieces.get(v) or new[v] for v in values]
        self._seen.update(values)
        if self.clock() - self._swept > self.SWEEP_S:
            self._pieces = {v: p for v, p in self._pieces.items() if v in self._seen}
            self._seen, self._swept = set(), self.clock()
        return out
