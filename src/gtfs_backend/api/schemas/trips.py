"""Response models: trips."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.alerts import Alert
from gtfs_backend.api.schemas.common import PlatformRef, Route
from gtfs_backend.api.schemas.vehicles import MapVehicle


class TripStop(BaseModel):
    stop_sequence: int
    platform: PlatformRef | None = None
    group_id: str | None = None
    group_key: str | None = None  # stop key of the group
    name: str | None = None
    lat: float | None = None
    lon: float | None = None
    scheduled_arrival: int | None = None
    scheduled_departure: int | None = None
    estimated_arrival: int | None = None
    estimated_departure: int | None = None
    delay_s: int | None = None
    passed: bool
    skipped: bool = False
    on_demand: bool = False  # request stop: signal the driver / call ahead (GTFS pickup or drop-off type 2, 3)


class TripShape(BaseModel):
    polyline: str  # Google encoded polyline, precision 5
    bbox: list[float]  # minLon, minLat, maxLon, maxLat
    source: Literal["shape", "stops"]  # `stops`: the feed has no shape, drawn through the stops


class TripView(BaseModel):
    key: str  # `{feed_id}:{YYYYMMDD}:{trip_id}`
    feed_id: str
    trip_id: str
    service_date: str
    route: Route | None = None
    headsign: str | None = None
    direction_id: int | None = None
    status: Literal["scheduled", "live", "canceled"]
    delay_s: int | None = None
    timestamp: int
    vehicle: MapVehicle | None = None
    shape: TripShape | None = None  # None only when no stop has coordinates
    stops: list[TripStop]
    current_index: int | None = None  # index in `stops` of the next stop not yet passed
    alerts: list[Alert]
