"""Response models: vehicles."""

from __future__ import annotations

from pydantic import BaseModel

from gtfs_backend.api.schemas.common import Line


class MapVehicle(BaseModel):
    key: str  # `{feed_id}:{vehicle_id}`, stable across polls: /vehicles/{key}/trip
    label: str | None = None
    lat: float
    lon: float
    bearing: float | None = None
    line: Line | None = None  # its mode is the vehicle's
    headsign: str | None = None
    delay_s: int | None = None
    trip_key: str  # /trips/{trip_key}
    timestamp: int | None = None


class VehicleList(BaseModel):
    timestamp: int | None  # of the newest position shown; None: no vehicles
    vehicles: list[MapVehicle]
