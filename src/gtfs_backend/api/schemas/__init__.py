"""API response models, one module per resource. Times are epoch seconds, coordinates WGS84."""

from gtfs_backend.api.schemas.alerts import ActivePeriod, Alert, AlertList, AlertRef, InformedEntity
from gtfs_backend.api.schemas.common import Agency, Line, Route, StopRef
from gtfs_backend.api.schemas.departures import Departure, DepartureBoard, DepartureStatus
from gtfs_backend.api.schemas.health import FeedHealth, Health
from gtfs_backend.api.schemas.routes import RouteShape, RouteShapes
from gtfs_backend.api.schemas.stops import Direction, SearchResult, Stop, StopGroupInfo, StopInfo, StopList
from gtfs_backend.api.schemas.trips import TripLive, TripShape, TripStop, TripStopLive, TripView
from gtfs_backend.api.schemas.vehicles import MapVehicle, VehicleList

__all__ = [
    "ActivePeriod",
    "Agency",
    "Alert",
    "AlertList",
    "AlertRef",
    "Departure",
    "DepartureBoard",
    "DepartureStatus",
    "Direction",
    "FeedHealth",
    "Health",
    "InformedEntity",
    "Line",
    "MapVehicle",
    "Route",
    "RouteShape",
    "RouteShapes",
    "SearchResult",
    "Stop",
    "StopGroupInfo",
    "StopInfo",
    "StopList",
    "StopRef",
    "TripLive",
    "TripShape",
    "TripStop",
    "TripStopLive",
    "TripView",
    "VehicleList",
]
