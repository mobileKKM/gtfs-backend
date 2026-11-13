"""Response models: trips."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.alerts import AlertRef
from gtfs_backend.api.schemas.common import DirectionId, Route, StopRef
from gtfs_backend.api.schemas.vehicles import Vehicle


class TripStop(BaseModel):
    stop_sequence: int
    stop: StopRef | None = None  # None: the feed's stop isn't matched to one of ours
    lat: float | None = None
    lon: float | None = None
    scheduled_arrival: int | None = None
    scheduled_departure: int | None = None
    on_demand: bool = False  # request stop: signal the driver / call ahead (GTFS pickup or drop-off type 2, 3)


class TripShape(BaseModel):
    polyline: str  # Google encoded polyline, precision 5
    bbox: list[float]  # minLon, minLat, maxLon, maxLat
    source: Literal["shape", "stops"]  # `stops`: the feed has no shape, drawn through the stops


class TripView(BaseModel):
    """What doesn't change while the trip runs: fetch once, poll /trips/{key}/live."""

    key: str  # `{feed_id}:{YYYYMMDD}:{trip_id}`
    service_date: str
    route: Route | None = None
    headsign: str | None = None
    direction_id: DirectionId | None = None
    shape: TripShape | None = None  # None only when no stop has coordinates
    stops: list[TripStop]


class TripStopLive(BaseModel):
    stop_sequence: int
    estimated_arrival: int | None = None
    estimated_departure: int | None = None
    delay_s: int | None = None
    passed: bool
    skipped: bool = False


class TripLive(BaseModel):
    """A trip's realtime state. Match `stops` to the TripView's by `stop_sequence`: after a timetable import the cached
    TripView can be of the previous version for a few minutes; a live stop without a match means fetch it again."""

    key: str  # `{feed_id}:{YYYYMMDD}:{trip_id}`: /trips/{key}
    status: Literal["scheduled", "live", "canceled"]
    delay_s: int | None = None
    timestamp: int | None  # of the newest realtime data shown (trip update, vehicle position); None: none
    vehicle: Vehicle | None = None
    stops: list[TripStopLive]
    current_index: int | None = None  # index in these `stops` of the next stop not yet passed
    alerts: list[AlertRef]
