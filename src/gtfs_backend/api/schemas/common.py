"""Response models. Times are epoch seconds, coordinates WGS84."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from gtfs_backend.modes import Mode

DirectionId = Literal[0, 1]  # GTFS direction_id


class Agency(BaseModel):
    name: str
    url: str | None = None
    phone: str | None = None


class RouteRef(BaseModel):
    """A route as lists show it: name and mode; the rest at /routes/{key}/shapes. Colors are up to the client. Feeds
    split some lines into several routes (km_mld has two `A18`), so names can repeat."""

    key: str  # `{feed_id}:{route_id}`, as Route.key
    name: str  # short name, or route_id when there is none: what `?routes=` filters match
    mode: Mode


class Route(RouteRef):
    """A route in full, where a view is about one (`/routes/{key}/shapes`, a trip)."""

    route_type: int | None = None  # GTFS route_type as given, basic or extended
    agency: Agency | None = None


class StopRef(BaseModel):
    key: str  # stop key, as Stop.key
    name: str
    group_key: str  # its stop group: /stops/{group_key}/departures
    platform_code: str | None = None
