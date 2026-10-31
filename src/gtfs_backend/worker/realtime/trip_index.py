"""In-memory view of the static data the realtime normalizer needs, per feed.

Loaded for the service days yesterday/today/tomorrow (only their authoritative versions). Stop times
are fetched lazily for trips that appear in realtime data and cached until the next reload.
"""

from __future__ import annotations

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
    stop_platform: dict[tuple[int, str], str] = field(default_factory=dict)  # (version, stop_id) -> platform id
    stop_times: dict[tuple[int, str], list[StopTime]] = field(default_factory=dict)
    version_ids: dict[int, str] = field(default_factory=dict)  # version number -> core.feed_version.id
    loaded_for: date | None = None

    def add_trip(self, info: TripInfo) -> None:
        self.trips[(info.version, info.trip_id)] = info
        self.by_trip_id[info.trip_id].append(info)
        self.by_base_id[strip_service_suffix(info.trip_id)].append(info)

    def day(self, d: date) -> ServiceDay | None:
        return next((x for x in self.days if x.date == d), None)

    def platform(self, version: int, stop_id: str | None) -> str | None:
        return self.stop_platform.get((version, stop_id)) if stop_id else None


async def load_index(
    pool: AsyncConnectionPool, feed_id: str, now: float, day_start: DayStart = "noon_minus_12h"
) -> TripIndex | None:
    """Build the index for the service days around `now`. None if the feed has no ready version."""
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
        for d, v, fv_id, services in await cur.fetchall():
            index.days.append(ServiceDay(d, v, day_base(d, tz, day_start), frozenset(services)))
            index.version_ids[v] = fv_id
        if not index.version_ids:
            return index
        number = {fv_id: v for v, fv_id in index.version_ids.items()}
        ids = sorted(number)

        cur = await conn.execute(
            """
            SELECT t.feed_version_id, t.trip_id, t.service_id, t.route_id, t.direction_id, t.shape_id,
                   t.trip_headsign, t.block_id,
                   r.route_short_name, r.route_type,
                   extract(epoch FROM min(coalesce(st.departure_time, st.arrival_time)))::int,
                   extract(epoch FROM max(coalesce(st.arrival_time, st.departure_time)))::int
            FROM gtfs.trips t
            JOIN gtfs.routes r ON r.feed_version_id = t.feed_version_id AND r.route_id = t.route_id
            JOIN gtfs.stop_times st ON st.feed_version_id = t.feed_version_id AND st.trip_id = t.trip_id
            WHERE t.feed_version_id = ANY(%s)
            GROUP BY t.feed_version_id, t.trip_id, r.route_short_name, r.route_type
            """,
            (ids,),
        )
        for row in await cur.fetchall():
            if row[10] is not None:
                index.add_trip(TripInfo(number[row[0]], *row[1:]))

        cur = await conn.execute(
            "SELECT feed_version_id, stop_id, stop_platform_id FROM core.stop_link WHERE feed_version_id = ANY(%s)", (ids,)
        )
        for fv_id, stop_id, pid in await cur.fetchall():
            index.stop_platform[(number[fv_id], stop_id)] = pid
    return index


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
