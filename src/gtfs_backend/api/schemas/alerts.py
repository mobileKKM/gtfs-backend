"""Response models: alerts."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from gtfs_backend.api.schemas.common import DirectionId, RouteRef
from gtfs_backend.api.schemas.stops import Stop
from gtfs_backend.modes import Mode


class AlertCause(StrEnum):
    """GTFS-RT Alert.Cause, lowercase; UNKNOWN_CAUSE is None instead."""

    OTHER_CAUSE = "other_cause"
    TECHNICAL_PROBLEM = "technical_problem"
    STRIKE = "strike"
    DEMONSTRATION = "demonstration"
    ACCIDENT = "accident"
    HOLIDAY = "holiday"
    WEATHER = "weather"
    MAINTENANCE = "maintenance"
    CONSTRUCTION = "construction"
    POLICE_ACTIVITY = "police_activity"
    MEDICAL_EMERGENCY = "medical_emergency"
    SPECIAL_EVENT = "special_event"


class AlertEffect(StrEnum):
    """GTFS-RT Alert.Effect, lowercase; UNKNOWN_EFFECT is None instead."""

    NO_SERVICE = "no_service"
    REDUCED_SERVICE = "reduced_service"
    SIGNIFICANT_DELAYS = "significant_delays"
    DETOUR = "detour"
    ADDITIONAL_SERVICE = "additional_service"
    MODIFIED_SERVICE = "modified_service"
    OTHER_EFFECT = "other_effect"
    STOP_MOVED = "stop_moved"
    NO_EFFECT = "no_effect"
    ACCESSIBILITY_ISSUE = "accessibility_issue"


class AlertSeverity(StrEnum):
    """GTFS-RT Alert.SeverityLevel, lowercase; UNKNOWN_SEVERITY is None instead."""

    INFO = "info"
    WARNING = "warning"
    SEVERE = "severe"


class ActivePeriod(BaseModel):
    start: int | None = None
    end: int | None = None


class AlertEntity(BaseModel):
    """Where an alert applies: everything set here at once (GTFS-RT: AND), e.g. line 52 *at* this stop, not line 52
    everywhere and not every line at this stop. Nothing set: the whole feed. An alert applies wherever any of its
    entities does."""

    mode: Mode | None = None  # every line of this mode (as `RouteRef.mode`)
    route: RouteRef | None = None  # vehicles and departures carry the same `key`
    direction_id: DirectionId | None = None  # with `route`: only its trips in this direction (GTFS direction_id)
    trip_key: str | None = None  # one dated trip: /trips/{trip_key}
    stop: Stop | None = None  # a stop, or a stop group when the feed names a whole station


class AlertRef(BaseModel):
    """An alert as boards and trip views carry it: enough for a banner; where it applies at /alerts/{key}."""

    key: str  # `{feed_id}:{id}`: /alerts/{key}
    cause: AlertCause | None = None  # None: unknown
    effect: AlertEffect | None = None  # None: unknown
    severity: AlertSeverity | None = None  # None: not given
    header: str | None = None
    description: str | None = None  # plain text, lines separated by "\n"
    active_periods: list[ActivePeriod] = Field(default_factory=list)


class Alert(AlertRef):
    """A service alert with what it applies to in our terms."""

    url: str | None = None
    entities: list[AlertEntity] = Field(default_factory=list)  # empty: names nothing in today's timetables


class AlertList(BaseModel):
    alerts: list[Alert]
