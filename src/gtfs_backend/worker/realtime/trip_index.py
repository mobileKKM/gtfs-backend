"""In-memory view of the static data the realtime normalizer needs, per feed.

Loaded for the service days yesterday/today/tomorrow (only their authoritative versions, and only the trips that
run on them). Stop times are fetched lazily for trips that appear in realtime data, and kept across reloads.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from zoneinfo import ZoneInfo

from psycopg_pool import AsyncConnectionPool

from gtfs_backend.servicetime import DayStart, day_base, nearby_dates

_SERVICE_SUFFIX = re.compile(r"_service_\w+$")


def strip_service_suffix(trip_id: str) -> str:
    return _SERVICE_SUFFIX.sub("", trip_id)


@dataclass(frozen=True)
class TripInfo:
    version: int  # per-feed version number (unique within the index: one feed)
    trip_id: str
    service_id: str
    route_id: str
    direction_id: int | None
    shape_id: str | None
    headsign: str | None
    block_id: str | None
    route_short_name: str | None
    route_type: int | None
    first_departure: int  # seconds since service-day base
    last_arrival: int


@dataclass(frozen=True)
class ServiceDay:
    date: date
    version: int  # per-feed version number (unique within the index: one feed)
    base: int  # epoch the GTFS times count from (noon - 12h, or midnight for some feeds)
    services: frozenset[str]


@dataclass(frozen=True)
class StopTime:
    sequence: int
    stop_id: str
    arrival: int | None  # seconds since service-day base
    departure: int | None


@dataclass
class TripIndex:
    feed_id: str
    tz: ZoneInfo
    days: list[ServiceDay] = field(default_factory=list)
    trips: dict[tuple[int, str], TripInfo] = field(default_factory=dict)
    by_trip_id: dict[str, list[TripInfo]] = field(default_factory=lambda: defaultdict(list))
    by_base_id: dict[str, list[TripInfo]] = field(default_factory=lambda: defaultdict(list))  # trip id without `_service_N`
    stop_links: dict[tuple[int, str], str] = field(default_factory=dict)  # (version, stop_id) -> public_stop_id
    # (version, station stop_id) -> (its stop group, the public_stop_ids of its stops): GTFS stations (location_type 1)
    stations: dict[tuple[int, str], tuple[str, tuple[str, ...]]] = field(default_factory=dict)
    routes: set[tuple[int, str]] = field(default_factory=set)  # (version, route_id)
    stop_times: dict[tuple[int, str], list[StopTime]] = field(default_factory=dict)
    version_ids: dict[int, str] = field(default_factory=dict)  # version number -> core.feed_version.id
    services: dict[int, frozenset[str]] = field(default_factory=dict)  # version -> its services on `days`
    loaded_for: date | None = None
    generation: int = 0  # set by FeedRuntime when it installs the index: 1, 2, ... (what realtime was written with)
    # (version, block_id) -> the block's trips by first departure, built on first use (block_trips)
    blocks: dict[tuple[int, str], list[TripInfo]] | None = field(default=None, repr=False, compare=False)

    def block_trips(self, version: int, block_id: str) -> list[TripInfo]:
        """The trips of a block (all its services) in this index, by first departure."""
        if self.blocks is None:
            blocks: dict[tuple[int, str], list[TripInfo]] = defaultdict(list)
            for info in self.trips.values():
                if info.block_id:
                    blocks[(info.version, info.block_id)].append(info)
            for trips in blocks.values():
                trips.sort(key=lambda t: (t.first_departure, t.trip_id))
            self.blocks = dict(blocks)
        return self.blocks.get((version, block_id), [])

    def add_trip(self, info: TripInfo) -> None:
        self.trips[(info.version, info.trip_id)] = info
        self.by_trip_id[info.trip_id].append(info)
        self.by_base_id[strip_service_suffix(info.trip_id)].append(info)

    def day(self, d: date) -> ServiceDay | None:
        return next((x for x in self.days if x.date == d), None)

    def public_stop(self, version: int, stop_id: str | None) -> str | None:
        return self.stop_links.get((version, stop_id)) if stop_id else None


YIELD_EVERY_ROWS = 5000  # building the index yields to the event loop this often, so polls of other feeds keep going

_TRIPS_SQL = """
    SELECT t.trip_id, t.service_id, t.route_id, t.direction_id, t.shape_id, t.trip_headsign, t.block_id,
           r.route_short_name, r.route_type,
           extract(epoch FROM min(coalesce(st.departure_time, st.arrival_time)))::int,
           extract(epoch FROM max(coalesce(st.arrival_time, st.departure_time)))::int
    FROM gtfs.trips t
    JOIN gtfs.routes r ON r.feed_version_id = t.feed_version_id AND r.route_id = t.route_id
    JOIN gtfs.stop_times st ON st.feed_version_id = t.feed_version_id AND st.trip_id = t.trip_id
    -- one literal version on every table, so only its partitions are read
    WHERE t.feed_version_id = %(v)s AND r.feed_version_id = %(v)s AND st.feed_version_id = %(v)s
      AND t.service_id = ANY(%(services)s)
    GROUP BY t.feed_version_id, t.trip_id, r.route_short_name, r.route_type
"""

# a station's stops can be split over groups: it is shown as the group most of them are in
_STATIONS_SQL = """
    SELECT p.feed_version_id, p.stop_id, mode() WITHIN GROUP (ORDER BY s.stop_group_id),
           array_agg(DISTINCT l.public_stop_id ORDER BY l.public_stop_id)
    FROM gtfs.stops p
    JOIN gtfs.stops c ON c.feed_version_id = p.feed_version_id AND c.parent_station = p.stop_id
    JOIN core.stop_link l ON l.feed_version_id = c.feed_version_id AND l.stop_id = c.stop_id
    JOIN core.stop s ON s.id = l.public_stop_id
    WHERE p.feed_version_id = ANY(%s) AND p.location_type = 1
    GROUP BY p.feed_version_id, p.stop_id
"""


async def load_index(
    pool: AsyncConnectionPool,
    feed_id: str,
    now: float,
    day_start: DayStart = "noon_minus_12h",
    previous: TripIndex | None = None,
) -> TripIndex | None:
    """Build the index for the service days around `now`. None if the feed has no ready version.

    Only trips whose service runs on one of those days are loaded: no other trip can match. A version's trips, stop
    links, routes and stop times never change, so when the versions and their services are those of `previous`, its
    data is reused and only the service days and the stations (their stop groups can change) are read again.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT timezone FROM core.feed_version WHERE feed_id = %s AND status = 'ready' ORDER BY version DESC LIMIT 1",
            (feed_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        tz = ZoneInfo(row[0] or "UTC")
        dates = nearby_dates(now, tz)
        index = TripIndex(feed_id, tz, loaded_for=dates[1])

        cur = await conn.execute(
            """
            SELECT s.service_date, fv.version, fv.id,
                   coalesce(array_agg(d.service_id) FILTER (WHERE d.service_id IS NOT NULL), '{}')
            FROM core.service_day_version s
            JOIN core.feed_version fv ON fv.id = s.feed_version_id
            LEFT JOIN core.service_date d ON d.feed_version_id = s.feed_version_id AND d.date = s.service_date
            WHERE s.feed_id = %s AND s.service_date = ANY(%s)
            GROUP BY s.service_date, fv.version, fv.id
            """,
            (feed_id, dates),
        )
        services: dict[int, set[str]] = defaultdict(set)
        for d, v, fv_id, day_services in await cur.fetchall():
            index.days.append(ServiceDay(d, v, day_base(d, tz, day_start), frozenset(day_services)))
            index.version_ids[v] = fv_id
            services[v].update(day_services)
        index.services = {v: frozenset(s) for v, s in services.items()}
        if not index.version_ids:
            return index
        number = {fv_id: v for v, fv_id in index.version_ids.items()}
        ids = sorted(number)

        if previous is not None and (previous.version_ids, previous.services) == (index.version_ids, index.services):
            index.trips, index.by_trip_id, index.by_base_id = previous.trips, previous.by_trip_id, previous.by_base_id
            index.stop_links, index.routes, index.stop_times = previous.stop_links, previous.routes, previous.stop_times
        else:
            await _load_version_data(conn, index, previous)

        cur = await conn.execute(_STATIONS_SQL, (ids,))
        for fv_id, stop_id, group_id, pids in await cur.fetchall():
            index.stations[(number[fv_id], stop_id)] = (group_id, tuple(pids))
    return index


async def _load_version_data(conn, index: TripIndex, previous: TripIndex | None) -> None:
    """Trips (of the services on the index's days), stop links and routes of the index's versions; stop times
    already loaded into `previous` are kept."""
    number = {fv_id: v for v, fv_id in index.version_ids.items()}
    ids = sorted(number)
    for v, fv_id in sorted(index.version_ids.items()):
        if not index.services.get(v):
            continue
        cur = await conn.execute(_TRIPS_SQL, {"v": fv_id, "services": sorted(index.services[v])})
        for i, row in enumerate(await cur.fetchall()):
            if row[9] is not None:
                index.add_trip(TripInfo(v, *row))
            if i % YIELD_EVERY_ROWS == YIELD_EVERY_ROWS - 1:
                await asyncio.sleep(0)

    cur = await conn.execute(
        "SELECT feed_version_id, stop_id, public_stop_id FROM core.stop_link WHERE feed_version_id = ANY(%s)", (ids,)
    )
    for fv_id, stop_id, pid in await cur.fetchall():
        index.stop_links[(number[fv_id], stop_id)] = pid

    cur = await conn.execute("SELECT feed_version_id, route_id FROM gtfs.routes WHERE feed_version_id = ANY(%s)", (ids,))
    index.routes = {(number[fv_id], route_id) for fv_id, route_id in await cur.fetchall()}

    if previous is not None:
        same = {v for v, fv_id in index.version_ids.items() if previous.version_ids.get(v) == fv_id}
        index.stop_times = {k: st for k, st in previous.stop_times.items() if k[0] in same and k in index.trips}


async def ensure_stop_times(pool: AsyncConnectionPool, index: TripIndex, keys: set[tuple[int, str]]) -> None:
    """Fetch stop times for trips not cached yet (one query per version)."""
    missing: dict[int, list[str]] = defaultdict(list)
    for v, trip_id in keys:
        if (v, trip_id) not in index.stop_times and (v, trip_id) in index.trips:
            missing[v].append(trip_id)
    if not missing:
        return
    async with pool.connection() as conn:
        for v, trip_ids in missing.items():
            cur = await conn.execute(
                """
                SELECT trip_id, stop_sequence, stop_id,
                       extract(epoch FROM arrival_time)::int, extract(epoch FROM departure_time)::int
                FROM gtfs.stop_times WHERE feed_version_id = %s AND trip_id = ANY(%s)
                ORDER BY trip_id, stop_sequence
                """,
                (index.version_ids[v], trip_ids),
            )
            grouped: dict[str, list[StopTime]] = defaultdict(list)
            for trip_id, seq, stop_id, arr, dep in await cur.fetchall():
                grouped[trip_id].append(StopTime(seq, stop_id, arr, dep))
            for trip_id in trip_ids:
                index.stop_times[(v, trip_id)] = grouped.get(trip_id, [])
