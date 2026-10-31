"""API response models, one module per resource. Times are epoch seconds, coordinates WGS84."""

from gtfs_backend.api.schemas.alerts import ActivePeriod, Alert, AlertList, InformedEntity
from gtfs_backend.api.schemas.common import Agency, Line, PlatformRef, Route
from gtfs_backend.api.schemas.departures import Departure, DepartureBoard, DepartureStatus
from gtfs_backend.api.schemas.health import FeedHealth, Health
from gtfs_backend.api.schemas.routes import RouteShape, RouteShapes
from gtfs_backend.api.schemas.stops import Direction, PlatformInfo, SearchResult, Stop, StopGroupInfo, StopList
from gtfs_backend.api.schemas.trips import TripShape, TripStop, TripView
from gtfs_backend.api.schemas.vehicles import MapVehicle, VehicleList

__all__ = [
    "ActivePeriod",
    "Agency",
    "Alert",
    "AlertList",
    "Departure",
    "DepartureBoard",
    "DepartureStatus",
    "Direction",
    "FeedHealth",
    "Health",
    "InformedEntity",
    "Line",
    "MapVehicle",
    "PlatformInfo",
    "PlatformRef",
    "Route",
    "RouteShape",
    "RouteShapes",
    "SearchResult",
    "Stop",
    "StopGroupInfo",
    "StopList",
    "TripShape",
    "TripStop",
    "TripView",
    "VehicleList",
]
