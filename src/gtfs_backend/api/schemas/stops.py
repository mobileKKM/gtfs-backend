"""Response models: stops."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from gtfs_backend.api.schemas.common import Line


class Stop(BaseModel):
    """A logical stop (`group`) or a physical one (`platform`)."""

    kind: Literal["group", "platform"]
    id: str
    key: str  # e.g. `rondo-mogilskie-qphdu`: /stops/{key}/departures, /alerts?stop={key}
    name: str
    lat: float | None = None
    lon: float | None = None
    modes: list[str] = Field(default_factory=list)
    group_id: str | None = None  # platforms only
    platform: str | None = None  # platforms only: the platform number, e.g. "01"


class StopList(BaseModel):
    truncated: bool
    stops: list[Stop]


class Direction(BaseModel):
    """Where trips from a platform go next, as a hint like "towards Rondo Mogilskie"."""

    towards: str  # name of the next stop group
    towards_id: str
    lines: list[Line]
    headsigns: list[str]  # most frequent first, at most 3


class PlatformInfo(Stop):
    directions: list[Direction] = Field(default_factory=list)  # most served first


class StopGroupInfo(Stop):
    platforms: list[PlatformInfo] = Field(default_factory=list)  # active ones
    lines: list[Line] = Field(default_factory=list)  # lines departing here today
    distance_m: float | None = None  # search with `near` only


class SearchResult(BaseModel):
    query: str
    results: list[StopGroupInfo]
