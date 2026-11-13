"""Response models: stops."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from gtfs_backend.api.schemas.common import RouteRef
from gtfs_backend.modes import Mode


class Stop(BaseModel):
    """A stop group (`group`: our logical stop, merged across feeds) or a stop (`stop`: one physical pole)."""

    kind: Literal["group", "stop"]
    id: str
    key: str  # e.g. `rondo-mogilskie-qphdu`: /stops/{key}/departures, /alerts?stop={key}
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    group_id: str | None = None  # stops only
    platform_code: str | None = None  # stops only: GTFS platform_code, e.g. "01"


class StopList(BaseModel):
    truncated: bool
    stops: list[Stop]


class Direction(BaseModel):
    """Where trips from a stop go next, as a hint like "towards Rondo Mogilskie"."""

    towards: str  # name of the next stop group
    towards_id: str
    routes: list[RouteRef]
    headsigns: list[str]  # most frequent first, at most 3


class StopInfo(BaseModel):
    """A stop inside its group (search): no `kind` or `group_id`, the group says."""

    id: str
    key: str
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    platform_code: str | None = None
    directions: list[Direction] = Field(default_factory=list)  # most served first


class StopGroupInfo(BaseModel):
    """A stop group with its stops (search)."""

    id: str
    key: str
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[Mode] = Field(default_factory=list)
    stops: list[StopInfo] = Field(default_factory=list)  # active ones


class SearchResult(BaseModel):
    query: str
    results: list[StopGroupInfo]
