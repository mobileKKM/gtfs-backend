"""Response models: routes."""

from __future__ import annotations

from pydantic import BaseModel

from gtfs_backend.api.schemas.common import Route


class RouteShape(BaseModel):
    direction_id: int | None = None
    headsign: str | None = None
    trips: int
    polyline: str
    bbox: list[float]


class RouteShapes(BaseModel):
    route: Route
    service_date: str
    shapes: list[RouteShape]
