"""Redis layout for live data: the worker writes it, the API reads it. Timetables stay in Postgres.

Every key carries the feed_id, because trip and vehicle ids are only unique within a feed (see keys.py).

rt:veh:geo                          GEO set of all feeds' vehicles (member = vehicle key), the union of:
rt:veh:geo:{feed_id}                GEO set per feed, replaced atomically on every vehicle poll
rt:veh:{vehicle_key}                vehicle JSON (TTL VEHICLE_TTL)
rt:trip:{trip_key}                  trip update JSON with per-stop predictions, incl. stops the feed has
                                    already dropped (their last known times); the JSON has the version.
                                    Removed when the trip leaves the feed (TTL TRIP_TTL as a fallback)
rt:tripstate:{feed_id}              HASH trip key -> TripState of every trip in rt:trip (TTL TRIP_TTL)
rt:stop:{feed_id}:{public_stop_id}  HASH "{stop_sequence}|{trip key}" -> StopPrediction: the trips' rows of rt:trip at
                                    this stop, for departure boards (which read only their stops' hashes, not whole
                                    trips). Only valid for trips in rt:tripstate, with the same version; no TTL,
                                    rewritten in full when the worker starts or finds rt:tripstate gone
rt:alerts:{feed_id}                 alerts JSON (TTL ALERT_TTL)
rt:unmatched:{feed_id}:{kind}       unmatched trip ids / vehicles for debugging (TTL UNMATCHED_TTL), never shown
rt:meta:{feed_id}:{kind}            poll stats: last poll, last data change, timings, counters
pub/sub rt:updated                  {"feed_id", "kind", "ts"} after each write
pub/sub static:updated              feed_id, after a static import became ready

The JSON values are typed below (LiveVehicle, LiveTrip, LiveAlert, Unmatched, PollMeta): the worker builds exactly
these, the API reads them. Times are epoch seconds, delays seconds; enums are GTFS-RT names (alerts: lowercase).
"""

from __future__ import annotations

from typing import NotRequired, TypedDict

VEHICLES_GEO = "rt:veh:geo"
REALTIME_UPDATED = "rt:updated"
STATIC_UPDATED = "static:updated"


def vehicles_geo(feed_id: str) -> str:
    return f"{VEHICLES_GEO}:{feed_id}"


def vehicle(vehicle_key: str) -> str:
    return f"rt:veh:{vehicle_key}"


def trip(trip_key: str) -> str:
    return f"rt:trip:{trip_key}"


def trip_states(feed_id: str) -> str:
    return f"rt:tripstate:{feed_id}"


def stop_predictions(feed_id: str, public_stop_id: str) -> str:
    return f"rt:stop:{feed_id}:{public_stop_id}"


def stop_prediction_field(stop_sequence: int, trip_key: str) -> str:
    return f"{stop_sequence}|{trip_key}"


def parse_stop_prediction_field(field: str) -> tuple[int, str]:
    sequence, _, trip_key = field.partition("|")
    return int(sequence), trip_key


def alerts(feed_id: str) -> str:
    return f"rt:alerts:{feed_id}"


def unmatched(feed_id: str, kind: str) -> str:
    return f"rt:unmatched:{feed_id}:{kind}"


def meta(feed_id: str, kind: str) -> str:
    return f"rt:meta:{feed_id}:{kind}"


# ------------------------------------------------------------------------------------------------- JSON values


class TripFields(TypedDict):
    """The static trip a realtime record resolved to (from the serving timetable version)."""

    key: str  # trip key, keys.trip_key
    feed_id: str
    version: int  # per-feed timetable version; stop sequences are only comparable within one
    trip_id: str
    service_date: str  # ISO date
    route_id: str
    route_short_name: str | None
    route_type: int | None
    headsign: str | None
    direction_id: int | None
    shape_id: str | None
    block_id: str | None


class StopRow(TypedDict):
    """One stop of a trip update, in trip order."""

    stop_sequence: int
    stop_id: str
    public_stop_id: str | None
    scheduled_arrival: int | None
    scheduled_departure: int | None
    predicted_arrival: int | None
    predicted_departure: int | None
    delay_s: int | None  # predicted - scheduled, always recomputed
    schedule_relationship: str  # SCHEDULED, SKIPPED, NO_DATA
    propagated: bool  # no update for this stop: the delay of the last updated stop before it


class LiveTrip(TripFields):
    """rt:trip:{trip_key}"""

    vehicle_id: str | None
    delay_s: int | None  # at the next stop, else the last known one
    timestamp: int  # when the feed measured it
    schedule_relationship: str  # SCHEDULED, CANCELED, ...
    stops: list[StopRow]  # incl. stops the feed has dropped since (their last known times)


# JSON arrays, not objects: boards read thousands of them.
# rt:tripstate value: [version, canceled, vehicle_id, timestamp], from the LiveTrip
TripState = tuple[int, bool, str | None, int]
# rt:stop value: [predicted_arrival, predicted_departure, delay_s, skipped, version, scheduled_departure], from the
# LiveTrip's StopRow. The version: writes and reads aren't atomic, so a board may read a trip state and predictions
# of different updates, and stop sequences are only comparable within one version. The scheduled departure: what a
# board's timetable rows are read by, to find trips running early (not predicted - delay_s: an arrival-only
# prediction's delay is against the scheduled arrival).
StopPrediction = tuple[int | None, int | None, int | None, bool, int, int | None]


def trip_state(trip: LiveTrip) -> TripState:
    return trip["version"], trip["schedule_relationship"] == "CANCELED", trip["vehicle_id"], trip["timestamp"]


def stop_prediction(row: StopRow, version: int) -> StopPrediction:
    skipped = row["schedule_relationship"] == "SKIPPED"
    return row["predicted_arrival"], row["predicted_departure"], row["delay_s"], skipped, version, row["scheduled_departure"]


class LiveVehicle(TypedDict):
    """rt:veh:{vehicle_key}"""

    feed_id: str
    vehicle_id: str
    label: str | None
    license_plate: str | None
    lat: float
    lon: float
    bearing: float | None  # filled from the previous fix when the feed leaves it out
    speed: float | None  # m/s, filled likewise
    timestamp: int | None
    current_status: str | None  # INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO
    current_stop_sequence: int | None
    stop_id: str | None
    public_stop_id: str | None
    trip: TripFields
    delay_s: int | None  # of its trip's last update
    occupancy: str | None
    wheelchair_accessible: str | None


class AlertEntity(TypedDict):
    """Where an alert applies, in our ids. All fields None/empty: the whole feed."""

    agency_id: str | None
    route_id: str | None
    route_type: int | None
    direction_id: int | None
    trip_key: str | None
    public_stop_ids: list[str]
    stop_group_id: str | None  # a GTFS station: the group most of its stops are in


class ActivePeriod(TypedDict):
    start: int | None
    end: int | None


class LiveAlert(TypedDict):
    """An element of rt:alerts:{feed_id} (a JSON list)."""

    feed_id: str
    id: str
    cause: str | None
    effect: str | None
    severity: str | None
    header: str | None
    description: str | None
    url: str | None
    active_periods: list[ActivePeriod]
    entities: list[AlertEntity]


class UnmatchedVehicle(TypedDict):
    vehicle_id: str
    trip_id: str | None


class Unmatched(TypedDict):
    """rt:unmatched:{feed_id}:{kind}"""

    trips: list[str]
    vehicles: list[UnmatchedVehicle]
    alert_entities: list[str]  # "{alert id}: route X"


class PollMeta(TypedDict):
    """rt:meta:{feed_id}:{kind}. The stats of the last data change stay while later polls see nothing new or fail."""

    started_at: str
    last_poll: NotRequired[str]
    ok: NotRequired[bool]
    last_error: NotRequired[str | None]
    consecutive_failures: NotRequired[int]
    poll_seconds: NotRequired[float]
    poll_seconds_max: NotRequired[float]
    age_s: NotRequired[int | None]  # how old the data in Redis is now
    fetch_seconds: NotRequired[float]
    fetch_attempts: NotRequired[int]
    # the last poll that changed data
    last_change: NotRequired[str]
    index_seconds: NotRequired[float]
    process_seconds: NotRequired[float]  # includes loading missing stop times
    write_seconds: NotRequired[float]
    feed_timestamp: NotRequired[int]
    bytes: NotRequired[int]
    entities: NotRequired[int]
    vehicles: NotRequired[int]
    trips: NotRequired[int]
    alerts: NotRequired[int]
    unmatched_trips: NotRequired[int]
    unmatched_vehicles: NotRequired[int]
    unmatched_alert_entities: NotRequired[int]
    unmatched_sample: NotRequired[list[str]]
    route_mismatches: NotRequired[int]
    median_delay_s: NotRequired[float | None]
