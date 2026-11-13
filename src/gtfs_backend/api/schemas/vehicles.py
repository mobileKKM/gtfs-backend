"""Response models: vehicles."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.common import RouteRef

VehicleStatus = Literal["stopped_at", "incoming_at", "in_transit_to"]


class Vehicle(BaseModel):
    key: str  # `{feed_id}:{vehicle_id}`, stable across polls: /vehicles/{key}/trip
    label: str | None = None
    lat: float
    lon: float
    bearing: float | None = None
    route: RouteRef | None = None  # its mode is the vehicle's
    headsign: str | None = None
    delay_s: int | None = None
    stop_sequence: int | None = None  # the trip stop `status` refers to; only set by feeds that report it
    status: VehicleStatus | None = None  # relative to that stop; None: the feed doesn't say (set only with stop_sequence)
    trip_key: str  # /trips/{trip_key}
    timestamp: int | None = None


class VehicleList(BaseModel):
    timestamp: int | None  # of the newest position shown; None: no vehicles
    vehicles: list[Vehicle]
