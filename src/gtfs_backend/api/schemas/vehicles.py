"""Response models: vehicles."""

from __future__ import annotations

from pydantic import BaseModel

from gtfs_backend.api.schemas.common import Line


class MapVehicle(BaseModel):
    key: str  # `{feed_id}:{vehicle_id}`, stable across polls: /vehicles/{key}/trip
    feed_id: str
    vehicle_id: str
    label: str | None = None
    lat: float
    lon: float
    bearing: float | None = None
    mode: str | None = None
    line: Line | None = None
    headsign: str | None = None
    delay_s: int | None = None
    trip_key: str  # /trips/{trip_key}
    timestamp: int | None = None


class VehicleList(BaseModel):
    timestamp: int
    vehicles: list[MapVehicle]
