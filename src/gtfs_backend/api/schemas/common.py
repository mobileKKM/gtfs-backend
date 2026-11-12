"""Response models. Times are epoch seconds, coordinates WGS84."""

from __future__ import annotations

from pydantic import BaseModel


class Agency(BaseModel):
    name: str
    url: str | None = None
    phone: str | None = None


class Route(BaseModel):
    key: str  # `{feed_id}:{route_id}`: /routes/{key}/shapes, /alerts?route={key}
    route_id: str  # the feed's, as alert entities name it
    short_name: str | None = None
    long_name: str | None = None
    route_type: int | None = None
    mode: str
    agency: Agency | None = None


class RouteRef(BaseModel):
    """A route as lists show it: name and mode; the rest at /routes/{key}/shapes. Colors are up to the client. Feeds
    split some lines into several routes (km_mld has two `A18`), so names can repeat."""

    key: str  # `{feed_id}:{route_id}`, as Route.key
    name: str  # short name, or route_id when there is none: what `?routes=` filters match
    mode: str


class StopRef(BaseModel):
    id: str
    key: str  # stop key, as Stop.key
    name: str
    group_id: str  # its stop group: /stops/{group_id}/departures
    platform_code: str | None = None
