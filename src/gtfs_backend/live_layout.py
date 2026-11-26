"""Redis layout for live data: the worker writes it, the API reads it. Timetables stay in Postgres.

Every key carries the feed_id, because trip and vehicle ids are only unique within a feed (see keys.py).

rt:veh:geo                          GEO set of all feeds' vehicles (member = vehicle key), the union of:
rt:veh:geo:{feed_id}                GEO set per feed, replaced atomically on every vehicle poll
rt:veh:{vehicle_key}                vehicle JSON (TTL VEHICLE_TTL)
rt:trip:{trip_key}                  trip update JSON with per-stop predictions, incl. stops the feed has
                                    already dropped (their last known times); the JSON has the version.
                                    Removed when the trip leaves the feed (TTL TRIP_TTL as a fallback)
rt:tripstate:{feed_id}              HASH trip key -> TripState of every trip in rt:trip (TTL TRIP_TTL)
rt:alerts:{feed_id}                 alerts JSON (TTL ALERT_TTL)
rt:unmatched:{feed_id}:{kind}       unmatched trip ids / vehicles for debugging (TTL UNMATCHED_TTL), never shown
rt:meta:{feed_id}:{kind}            poll stats: last poll, last data change, timings, counters
rt:seq:{feed_id}:{kind}             "{run}:{seq}" of the last change message of this file (see rt:updated)
pub/sub rt:updated                  after each write, what changed (LiveChanges): the API's live state follows it
pub/sub static:updated              feed_id, after a static import became ready (and its snapshot was written)
pub/sub snapshot:updated            generation, after the timetable snapshot was rebuilt for a new day

The JSON values are typed below (LiveVehicle, LiveTrip, LiveAlert, Unmatched, PollMeta): the worker builds exactly
these, the API reads them. Times are epoch seconds, delays seconds; enums are GTFS-RT names (alerts: lowercase).
"""

from __future__ import annotations

from typing import NotRequired, TypedDict

VEHICLES_GEO = "rt:veh:geo"
REALTIME_UPDATED = "rt:updated"
STATIC_UPDATED = "static:updated"
SNAPSHOT_UPDATED = "snapshot:updated"


def vehicles_geo(feed_id: str) -> str:
    return f"{VEHICLES_GEO}:{feed_id}"


def vehicle(vehicle_key: str) -> str:
    return f"rt:veh:{vehicle_key}"


def trip(trip_key: str) -> str:
    return f"rt:trip:{trip_key}"


def trip_states(feed_id: str) -> str:
    return f"rt:tripstate:{feed_id}"


def alerts(feed_id: str) -> str:
    return f"rt:alerts:{feed_id}"


def seq(feed_id: str, kind: str) -> str:
    return f"rt:seq:{feed_id}:{kind}"


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
# rt:tripstate value: [version, canceled, vehicle_id, timestamp, deleted], from the LiveTrip. Deleted (GTFS-RT DELETED):
# removed and not to be shown at all, also canceled. Readers accept the 4-element form of older workers.
TripState = tuple[int, bool, str | None, int, bool]
# A trip as the API keeps it in memory (change messages and its first load from rt:trip):
# [version, canceled, vehicle_id, timestamp, deleted, delay_s, [StopEntry, ...]], with
# StopEntry = [stop_sequence, public_stop_id, predicted_arrival, predicted_departure, delay_s, skipped,
#              scheduled_departure, propagated]
CompactTrip = list
StopEntry = list


def compact_trip(trip: LiveTrip) -> CompactTrip:
    version, canceled, vehicle_id, timestamp, deleted = trip_state(trip)
    stops = [
        [
            r["stop_sequence"],
            r["public_stop_id"],
            r["predicted_arrival"],
            r["predicted_departure"],
            r["delay_s"],
            r["schedule_relationship"] == "SKIPPED",
            r["scheduled_departure"],
            r["propagated"],
        ]
        for r in trip["stops"]
    ]
    return [version, canceled, vehicle_id, timestamp, deleted, trip["delay_s"], stops]


class LiveChanges(TypedDict):
    """A message on rt:updated: what one poll of one file changed. `run` identifies the worker process (a new one starts
    at 1 again), `seq` counts this file's messages: a reader that misses one reads the feed from Redis again."""

    run: str
    feed_id: str
    kind: str
    seq: int
    ts: int  # the feed header timestamp
    vehicles: NotRequired[dict]  # {"set": {vehicle key: LiveVehicle}, "del": [vehicle key]}
    trips: NotRequired[dict]  # {"set": {trip key: CompactTrip}, "del": [trip key]}
    alerts: NotRequired[list]  # the feed's alerts (LiveAlert), all of them


def trip_state(trip: LiveTrip) -> TripState:
    relationship = trip["schedule_relationship"]
    deleted = relationship == "DELETED"
    return trip["version"], deleted or relationship == "CANCELED", trip["vehicle_id"], trip["timestamp"], deleted


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
    # GTFS-RT schedule_relationship -> count in the last update: of its trip updates (matched or not; CANCELED, NEW,
    # DUPLICATED, ...) and of their stop time updates (SKIPPED, NO_DATA, ...)
    trip_relationships: NotRequired[dict[str, int]]
    stop_relationships: NotRequired[dict[str, int]]
    # trips predicted at their last stop after the next trip of their block is due, and of those, how many next trips
    # had no trip update of their own (worker/realtime/blocks.py)
    block_late_next: NotRequired[int]
    block_late_next_without_update: NotRequired[int]
    stale_vehicles: NotRequired[int]  # positions older than the feed's realtime max age, dropped
