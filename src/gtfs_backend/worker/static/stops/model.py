"""A new feed version's stops, and our existing platforms and groups, as the matchers see them."""

from __future__ import annotations

from dataclasses import dataclass

from gtfs_backend.ids import normalize_name


def split_code(code: str | None) -> tuple[str | None, str | None]:
    """'802-01' -> ('802', '01'). Codes without a dash have no group prefix."""
    if not code:
        return None, None
    if "-" in code:
        prefix, suffix = code.rsplit("-", 1)
        return (prefix or None), (suffix or None)
    return None, None


@dataclass(frozen=True)
class NewStop:
    stop_id: str
    code: str | None
    name: str
    platform: str | None  # platform_code
    lat: float
    lon: float
    station_key: str | None = None  # 'station:{parent code or id}' when the stop has a parent_station
    station_name: str | None = None

    @property
    def group_key(self) -> str:
        """Parent station, else the code prefix ('802' for '802-01'), else the name. Scoped to the namespace."""
        if self.station_key:
            return self.station_key
        prefix, _ = split_code(self.code)
        if prefix:
            return prefix
        return f"name:{normalize_name(self.name)}"

    @property
    def group_name(self) -> str:
        return self.station_name or self.name

    @property
    def platform_key(self) -> str:
        """Stops sharing a code are one platform; stops without a code are their own."""
        return f"code:{self.code}" if self.code else f"stop:{self.stop_id}"


@dataclass
class Platform:
    id: str
    group_id: str
    code: str | None
    name: str
    platform: str | None
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


def platform_number(stop: NewStop) -> str | None:
    if stop.platform:
        return stop.platform.strip() or None
    return split_code(stop.code)[1]
