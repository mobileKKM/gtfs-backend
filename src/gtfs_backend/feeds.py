"""Feed definitions from feeds.yml."""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

from gtfs_backend.servicetime import DayStart


class RealtimeKind(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    SERVICE_ALERTS = "service_alerts"
    COMBINED = "combined"  # one file with any mix of vehicles, trip updates and alerts (Koleje Małopolskie)


class RealtimeUrls(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vehicle_positions: HttpUrl | None = None
    trip_updates: HttpUrl | None = None
    service_alerts: HttpUrl | None = None
    combined: HttpUrl | None = None

    def items(self) -> list[tuple[RealtimeKind, str]]:
        """Configured (kind, url) pairs; kinds left out are skipped."""
        out = []
        for kind in RealtimeKind:
            url = getattr(self, kind.value)
            if url is not None:
                out.append((kind, str(url)))
        return out


STOP_CODE_GROUPS = {"station", "platform_code"}


class FeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Lowercase words joined by single underscores (`__` separates feed and version in partition names);
    # at most 32 characters so partition names stay under Postgres' 63-character limit.
    feed_id: str = Field(pattern=r"^[a-z0-9]+(_[a-z0-9]+)*$", max_length=32)
    enabled: bool = True
    # Feeds with the same namespace share stop codes (ZTP's three feeds); default: codes are private to the feed.
    stop_namespace: str | None = Field(default=None, pattern=r"^[a-z0-9_]+$", max_length=40)
    # Namespaces whose physical stops this feed also uses (attached by location + name, see worker/static/stops).
    share_stops_with: list[str] = Field(default_factory=list)
    # For feeds that put their stations into stop codes instead of GTFS stations: a regex the whole stop_code
    # matches, naming the station (`station`) and the platform_code (`platform_code`). ZTP's `802-01` is stop 01
    # of station 802. A stop's own parent_station and platform_code win over it.
    stop_code_pattern: str | None = None
    static: HttpUrl
    realtime: RealtimeUrls = Field(default_factory=RealtimeUrls)
    # What the realtime feed counts GTFS times from. GTFS says noon minus 12h; some systems use local
    # midnight. The two differ only on the two clock-change days.
    day_start: DayStart = "noon_minus_12h"
    # The realtime feed may end a trip id with another `_service_N` than the static one (ZTP's trams after midnight
    # name a still-running trip of yesterday's timetable with today's service). If an id is not found as is, match
    # it without that suffix and let the time window pick the trip.
    ignore_service_suffix: bool = False
    # How often each realtime file is polled (default REALTIME_POLL_INTERVAL), and how long the last data stays
    # live while the file doesn't change (default REALTIME_MAX_AGE). Slow feeds need a longer max age.
    realtime_poll_interval: float | None = Field(default=None, gt=0)
    realtime_max_age: int | None = Field(default=None, ge=1)

    @field_validator("stop_code_pattern")
    @classmethod
    def _pattern_groups(cls, pattern: str | None) -> str | None:
        if pattern is None:
            return None
        try:
            groups = set(re.compile(pattern).groupindex)
        except re.error as exc:
            raise ValueError(f"stop_code_pattern: {exc}") from exc
        if not groups or groups - STOP_CODE_GROUPS:
            raise ValueError(f"stop_code_pattern: named groups must be among {sorted(STOP_CODE_GROUPS)}")
        return pattern

    @property
    def static_url(self) -> str:
        return str(self.static)

    @property
    def namespace(self) -> str:
        return self.stop_namespace or self.feed_id


class FeedsFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    feeds: list[FeedConfig]
    # stop search only finds stops of these namespaces (default: all)
    search_namespaces: list[str] | None = Field(default=None, min_length=1)

    @field_validator("feeds")
    @classmethod
    def _unique_ids(cls, feeds: list[FeedConfig]) -> list[FeedConfig]:
        seen: set[str] = set()
        for feed in feeds:
            if feed.feed_id in seen:
                raise ValueError(f"duplicate feed_id: {feed.feed_id}")
            seen.add(feed.feed_id)
        return feeds

    @model_validator(mode="after")
    def _known_namespaces(self) -> FeedsFile:
        unknown = set(self.search_namespaces or ()) - {f.namespace for f in self.feeds}
        if unknown:
            raise ValueError(f"search_namespaces: unknown namespace(s) {sorted(unknown)}")
        return self

    @model_validator(mode="after")
    def _one_pattern_per_namespace(self) -> FeedsFile:
        patterns: dict[str, str | None] = {}
        for feed in self.feeds:
            if patterns.setdefault(feed.namespace, feed.stop_code_pattern) != feed.stop_code_pattern:
                raise ValueError(f"stop_code_pattern: feeds of namespace {feed.namespace} must declare the same one")
        return self

    @property
    def searchable_namespaces(self) -> list[str]:
        return self.search_namespaces or sorted({f.namespace for f in self.feeds})

    @property
    def enabled(self) -> list[FeedConfig]:
        return [f for f in self.feeds if f.enabled]

    def get(self, feed_id: str) -> FeedConfig:
        for feed in self.feeds:
            if feed.feed_id == feed_id:
                return feed
        raise KeyError(f"unknown feed_id: {feed_id}")


def load_feeds(path: Path | str) -> FeedsFile:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return FeedsFile.model_validate(data)
