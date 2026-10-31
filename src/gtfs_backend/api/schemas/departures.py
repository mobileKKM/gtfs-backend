"""Response models: departures."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.alerts import Alert
from gtfs_backend.api.schemas.common import Line, PlatformRef
from gtfs_backend.api.schemas.stops import Stop

DepartureStatus = Literal["scheduled", "live", "departed", "canceled", "skipped"]


class Departure(BaseModel):
    trip_key: str  # `{feed_id}:{YYYYMMDD}:{trip_id}`: /trips/{trip_key}
    feed_id: str
    trip_id: str
    service_date: str
    stop_sequence: int
    line: Line | None = None
    headsign: str | None = None
    platform: PlatformRef
    scheduled: int
    estimated: int | None = None  # realtime prediction (for departed ones the last known)
    time: int  # estimated if known, else scheduled
    delay_s: int | None = None
    status: DepartureStatus
    realtime: bool
    vehicle_id: str | None = None  # set: the trip view can follow a vehicle
    on_demand: bool = False  # request stop: signal the driver / call ahead to board (GTFS pickup type 2, 3)
    wheelchair_accessible: int | None = None


class DepartureBoard(BaseModel):
    stop: Stop
    timestamp: int
    departures: list[Departure]
    earlier: str | None = None  # cursor for the page before, None when there is nothing earlier
    later: str | None = None  # cursor for the page after, None when there is nothing later
    alerts: list[Alert]  # first page only
