"""Response models. Times are epoch seconds, coordinates WGS84."""

from __future__ import annotations

from pydantic import BaseModel


class Agency(BaseModel):
    name: str
    url: str | None = None
    phone: str | None = None


class Route(BaseModel):
    key: str  # `{feed_id}:{route_id}`: /routes/{key}/shapes, /alerts?route={key}
    feed_id: str
    route_id: str
    short_name: str | None = None
    long_name: str | None = None
    route_type: int | None = None
    mode: str
    agency: Agency | None = None


class Line(BaseModel):
    """A route as lists show it: number and mode. Colors are up to the client."""

    key: str  # `{feed_id}:{route_id}`, as Route.key
    feed_id: str
    name: str  # short name, or route_id when there is none
    mode: str


class StopRef(BaseModel):
    id: str
    key: str  # stop key, as Stop.key
    name: str
    group_id: str  # its stop group: /stops/{group_id}/departures
    platform_code: str | None = None
