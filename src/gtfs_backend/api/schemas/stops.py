"""Response models: stops."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from gtfs_backend.api.schemas.common import RouteRef
from gtfs_backend.ids import parse_url_key
from gtfs_backend.modes import Mode


class Stop(BaseModel):
    """A stop group (`group`: our logical stop, merged across feeds) or a stop (`stop`: one physical pole)."""

    kind: Literal["group", "stop"]
    key: str  # e.g. `rondo-mogilskie01-qphdu`: /stops/{stop_key}/departures, /alerts?stop={key}
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    group_key: str | None = None  # stops only: its stop group, /stops/{group_key}/departures
    platform_code: str | None = None  # stops only: GTFS platform_code, e.g. "01"

    @property
    def id(self) -> str:
        """Our public 5-letter id, the last part of the key (internal: not in responses)."""
        return parse_url_key(self.key) or self.key


class StopList(BaseModel):
    truncated: bool
    stops: list[Stop]


class Direction(BaseModel):
    """Where trips from a stop go next, as a hint like "towards Rondo Mogilskie"."""

    towards: str  # name of the next stop group
    towards_key: str  # the next stop group: /stops/{towards_key}/departures
    routes: list[RouteRef]
    headsigns: list[str]  # most frequent first, at most 3


class StopInfo(BaseModel):
    """A stop inside its group (search): no `kind` or `group_key`, the group says."""

    key: str
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    platform_code: str | None = None
    directions: list[Direction] = Field(default_factory=list)  # most served first


class StopGroupInfo(BaseModel):
    """A stop group with its stops (search)."""

    key: str
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    stops: list[StopInfo] = Field(default_factory=list)  # active ones


class SearchResult(BaseModel):
    query: str
    results: list[StopGroupInfo]
