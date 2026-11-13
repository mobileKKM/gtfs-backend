"""Response models: departures."""

from __future__ import annotations

from enum import IntEnum
from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.alerts import AlertRef
from gtfs_backend.api.schemas.common import RouteRef, StopRef
from gtfs_backend.api.schemas.stops import Stop

DepartureStatus = Literal["scheduled", "live", "departed", "canceled", "skipped"]


class Wheelchair(IntEnum):
    """GTFS wheelchair_accessible of the trip."""

    UNKNOWN = 0
    ACCESSIBLE = 1
    NOT_ACCESSIBLE = 2


class Departure(BaseModel):
    trip_key: str  # `{feed_id}:{YYYYMMDD}:{trip_id}`: /trips/{trip_key}
    stop_sequence: int
    route: RouteRef | None = None
    headsign: str | None = None
    stop: StopRef
    scheduled: int
    estimated: int | None = None  # realtime prediction (for departed ones the last known)
    time: int  # estimated if known, else scheduled
    delay_s: int | None = None
    status: DepartureStatus
    vehicle_key: str | None = None  # set: /vehicles/{vehicle_key}/trip follows the vehicle
    on_demand: bool = False  # request stop: signal the driver / call ahead to board (GTFS pickup type 2, 3)
    wheelchair_accessible: Wheelchair | None = None


class DepartureBoard(BaseModel):
    stop: Stop
    timestamp: int | None  # of the newest realtime prediction shown; None: no departure has realtime
    departures: list[Departure]
    routes: list[RouteRef]  # departing from the stop in today's timetables (any day), whatever the filters; names can repeat
    alerts: list[AlertRef]
