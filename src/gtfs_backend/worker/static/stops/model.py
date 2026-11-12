"""A new feed version's stops, and our existing stops and stop groups, as the matchers see them."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache

from gtfs_backend.ids import normalize_name


@dataclass(frozen=True)
class NewStop:
    """A feed's stop (GTFS location_type 0) in GTFS shape: see normalize_stop."""

    stop_id: str
    code: str | None
    name: str
    platform_code: str | None
    lat: float
    lon: float
    # 'station:{code or id}' of the GTFS parent_station, or the station the feed's stop_code_pattern names ('802')
    station_key: str | None = None
    station_name: str | None = None

    @property
    def group_key(self) -> str:
        """The station, else the name. Scoped to the namespace."""
        return self.station_key or f"name:{normalize_name(self.name)}"

    @property
    def group_name(self) -> str:
        return self.station_name or self.name

    @property
    def pole_key(self) -> str:
        """Stops sharing a code are one pole; stops without a code are their own."""
        return f"code:{self.code}" if self.code else f"stop:{self.stop_id}"


@cache
def _compiled(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


def normalize_stop(
    stop_id: str,
    code: str | None,
    name: str,
    platform_code: str | None,
    lat: float,
    lon: float,
    station_key: str | None = None,
    station_name: str | None = None,
    *,
    stop_code_pattern: str | None = None,
) -> NewStop:
    """A feed's stop in GTFS shape: its own parent_station and platform_code, else what the feed's
    stop_code_pattern reads from its code (feeds.yml)."""
    m = _compiled(stop_code_pattern).fullmatch(code) if stop_code_pattern and code else None
    parts = m.groupdict() if m else {}
    return NewStop(
        stop_id,
        code,
        name,
        platform_code or parts.get("platform_code"),
        lat,
        lon,
        station_key or parts.get("station"),
        station_name,
    )


@dataclass
class PublicStop:
    id: str
    group_id: str
    code: str | None
    name: str
    platform_code: str | None
    lat: float | None
    lon: float | None
    active: bool
    linked_elsewhere: bool = False  # linked from another feed's current version


@dataclass
class Group:
    id: str
    code: str | None
    name: str
    lat: float | None
    lon: float | None
