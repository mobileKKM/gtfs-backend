"""Response models: alerts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from gtfs_backend.api.schemas.common import Route
from gtfs_backend.api.schemas.stops import Stop


class ActivePeriod(BaseModel):
    start: int | None = None
    end: int | None = None


class InformedEntity(BaseModel):
    agency_id: str | None = None
    route_id: str | None = None
    route_type: int | None = None
    trip_id: str | None = None
    stop_id: str | None = None
    stop_platform_id: str | None = None


class Alert(BaseModel):
    """A service alert exactly as the feed gives it, plus resolved routes and stops for display."""

    key: str  # `{feed_id}:{id}`: /alerts/{key}
    feed_id: str
    id: str
    cause: str | None = None
    effect: str | None = None
    header: str | None = None
    description: str | None = None
    url: str | None = None
    active_periods: list[ActivePeriod] = Field(default_factory=list)
    informed_entities: list[InformedEntity] = Field(default_factory=list)
    routes: list[Route] = Field(default_factory=list)
    stops: list[Stop] = Field(default_factory=list)


class AlertList(BaseModel):
    alerts: list[Alert]
