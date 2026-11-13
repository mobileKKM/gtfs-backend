"""Response models: departures."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.api.schemas.alerts import AlertRef
from gtfs_backend.api.schemas.common import RouteRef, StopRef
from gtfs_backend.api.schemas.stops import Stop

DepartureStatus = Literal["scheduled", "live", "departed", "canceled", "skipped"]
VehicleState = Literal["at_stop", "arriving"]


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
    vehicle_state: VehicleState | None = None  # vehicle reports being at / arriving at this stop; None: unknown
    on_demand: bool = False  # request stop: signal the driver / call ahead to board (GTFS pickup type 2, 3)


class DepartureBoard(BaseModel):
    stop: Stop
    timestamp: int | None  # of the newest realtime prediction shown; None: no departure has realtime
    departures: list[Departure]
    routes: list[RouteRef]  # departing from the stop in today's timetables (any day), whatever the filters; names can repeat
    alerts: list[AlertRef]
