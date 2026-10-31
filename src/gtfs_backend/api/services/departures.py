"""Departure boards: the timetable from Postgres with realtime predictions from Redis laid over it.

The timetable query covers every authoritative service day that can run in the window (a night trip of
yesterday's service can still be running) and reaches back LOOKBACK_S so delayed trips stay on the board until
they leave (and GRACE_S after).

Boards page both ways with opaque cursors. The first page (no cursor) starts now and leaves out departures that
have left; `earlier` pages scroll into the past and include them as `departed`, with the last time realtime knew.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import date
from typing import Literal

from psycopg import AsyncConnection

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.catalog import Catalog, Day
from gtfs_backend.api.data.realtime import TripUpdate
from gtfs_backend.api.data.sql import union_per_version
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.schemas import Departure, DepartureBoard, DepartureStatus, Line
from gtfs_backend.api.services import alerts, stops

LOOKBACK_S = 60 * 60  # delayed departures stay this long past their time
GRACE_S = 30  # keep a departure this long after it leaves
WINDOWS_S = (2 * 3600, 6 * 3600, 24 * 3600)  # widened until a page is full
MAX_GTFS_TIME_S = 48 * 3600  # GTFS times past 24:00 belong to night trips; none run longer than a second day

SortKey = tuple[int, str, int]  # (time, trip_key, stop_sequence)


@dataclass(frozen=True)
class Scheduled:
    feed_id: str
    feed_version_id: str
    version: int
    service_date: date
    trip_id: str
    stop_sequence: int
    stop_platform_id: str
    scheduled_departure: int  # epoch
    headsign: str | None
    route_id: str
    wheelchair_accessible: int | None
    on_demand: bool = False

    @property
    def trip_key(self) -> str:
        return keys.trip_key(self.feed_id, self.service_date, self.trip_id)


@dataclass(frozen=True)
class Live:
    estimated: int | None
    delay_s: int | None
    realtime: bool
    status: DepartureStatus
    vehicle_id: str | None


@dataclass(frozen=True)
class Row:
    s: Scheduled
    live: Live

    @property
    def time(self) -> int:
        return self.live.estimated or self.s.scheduled_departure

    @property
    def key(self) -> SortKey:
        return (self.time, self.s.trip_key, self.s.stop_sequence)


# ------------------------------------------------------------------------------------------------- cursors


@dataclass(frozen=True)
class Cursor:
    direction: Literal["earlier", "later"]
    key: SortKey

    def encode(self) -> str:
        raw = json.dumps([self.direction[0], *self.key], separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @classmethod
    def decode(cls, text: str) -> Cursor:
        try:
            d, t, trip_key, seq = json.loads(base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)))
            direction = {"e": "earlier", "l": "later"}[d]
            if not (isinstance(t, int) and isinstance(trip_key, str) and isinstance(seq, int)):
                raise TypeError
        except (binascii.Error, UnicodeDecodeError, ValueError, KeyError, TypeError):
            raise InvalidInput("invalid cursor") from None
        return cls(direction, (t, trip_key, seq))


# ------------------------------------------------------------------------------------------------- timetable + realtime


async def scheduled(
    conn: AsyncConnection, days: list[Day], platforms: list[str], start: int, end: int, lookback: int = LOOKBACK_S
) -> list[Scheduled]:
    """Timetabled departures at `platforms` between start - lookback and end (epochs).

    Skips stops without pickup and the trip's last stop (arrivals only).
    """
    days = [d for d in days if end - d.base >= 0 and start - lookback - d.base <= MAX_GTFS_TIME_S]
    if not days or not platforms:
        return []
    sql, params = union_per_version(
        """
        SELECT {i}, st.trip_id, st.stop_sequence, l.stop_platform_id,
               extract(epoch FROM st.departure_time)::int, coalesce(st.stop_headsign, t.trip_headsign),
               t.route_id, t.wheelchair_accessible, coalesce(st.pickup_type, 0) IN (2, 3)
        FROM core.stop_link l
        JOIN gtfs.stop_times st ON st.feed_version_id = %(fv{i})s AND st.stop_id = l.stop_id
             AND st.departure_time BETWEEN %(lo{i})s * interval '1 second' AND %(hi{i})s * interval '1 second'
        JOIN gtfs.trips t ON t.feed_version_id = %(fv{i})s AND t.trip_id = st.trip_id
        WHERE l.feed_version_id = %(fv{i})s AND l.stop_platform_id = ANY(%(platforms)s)
          AND coalesce(st.pickup_type, 0) <> 1
          AND EXISTS (SELECT 1 FROM core.service_date sd
                      WHERE sd.feed_version_id = %(fv{i})s AND sd.date = %(date{i})s AND sd.service_id = t.service_id)
          AND EXISTS (SELECT 1 FROM gtfs.stop_times n
                      WHERE n.feed_version_id = %(fv{i})s AND n.trip_id = st.trip_id AND n.stop_sequence > st.stop_sequence)
        """,
        [{"fv": d.feed_version_id, "date": d.date, "lo": start - lookback - d.base, "hi": end - d.base} for d in days],
        {"platforms": platforms},
    )
    cur = await conn.execute(sql, params)
    out = []
    for i, trip_id, seq, pid, dep, headsign, route_id, wheelchair, on_demand in await cur.fetchall():
        d = days[i]
        out.append(
            Scheduled(
                d.feed_id,
                d.feed_version_id,
                d.version,
                d.date,
                trip_id,
                seq,
                pid,
                d.base + dep,
                headsign,
                route_id,
                wheelchair,
                on_demand,
            )
        )
    return out


def overlay(s: Scheduled, raw: dict | None, now: int, grace: int = GRACE_S) -> Live:
    """Realtime state of one scheduled departure.

    `departed` once the vehicle has passed the stop, or (without that knowledge) once its time is `grace` past.
    Passed stops keep the last time realtime had for them.
    """
    update = TripUpdate.for_version(raw, s.version)
    stop = update.stop(s.stop_sequence) if update else None
    estimated = stop.predicted if stop else None
    delay = stop.delay_s if stop else None
    is_live = estimated is not None or delay is not None
    if update is not None and update.canceled:
        status = "canceled"
    elif stop is not None and stop.skipped:
        status = "skipped"
    elif (update is not None and update.passed(s.stop_sequence)) or (estimated or s.scheduled_departure) + grace < now:
        status = "departed"
    else:
        status = "live" if is_live else "scheduled"
    return Live(estimated, delay, is_live, status, update.vehicle_id if update else None)


def _upcoming(row: Row, now: int, grace: int) -> bool:
    return row.live.status != "departed" and row.time >= now - grace


def select(rows: list[Row], cursor: Cursor | None, now: int, limit: int, start: int, end: int, grace: int = GRACE_S) -> list[Row]:
    """One page, ascending. First page: upcoming rows from now on. Later/earlier: the rows after/before the cursor.
    Only rows whose time is within [start, end] count: outside it, the timetable query may have missed some.
    """
    rows = sorted((r for r in rows if start <= r.time <= end), key=lambda r: r.key)
    if cursor is None:
        return [r for r in rows if _upcoming(r, now, grace)][:limit]
    if cursor.direction == "later":
        return [r for r in rows if r.key > cursor.key][:limit]
    return [r for r in rows if r.key < cursor.key][-limit:]


async def _page(ctx: Context, conn: AsyncConnection, pids: list[str], cursor: Cursor | None, now: int, limit: int) -> list[Row]:
    """Rows of one page, widening the window until it is full."""
    anchor = now if cursor is None else cursor.key[0]
    page: list[Row] = []
    for span in WINDOWS_S:
        start, end = (anchor - span, anchor) if cursor and cursor.direction == "earlier" else (anchor, anchor + span)
        if cursor is None:
            start = anchor - GRACE_S
        found = await scheduled(conn, await ctx.catalog.days_between(start, end), pids, start, end)
        updates = await realtime.trip_updates(ctx.redis, [s.trip_key for s in found])
        rows = [Row(f, overlay(f, updates.get(f.trip_key), now)) for f in found]
        page = select(rows, cursor, now, limit, start, end)
        if len(page) >= limit:
            break
    return page


def _departure(row: Row, line: Line | None, platform) -> Departure:
    s, live = row.s, row.live
    return Departure(
        trip_key=s.trip_key,
        feed_id=s.feed_id,
        trip_id=s.trip_id,
        service_date=keys.service_date_str(s.service_date),
        stop_sequence=s.stop_sequence,
        line=line,
        headsign=s.headsign,
        platform=stops.platform_ref(platform),
        scheduled=s.scheduled_departure,
        estimated=live.estimated,
        time=row.time,
        delay_s=live.delay_s,
        status=live.status,
        realtime=live.realtime,
        vehicle_id=live.vehicle_id,
        on_demand=s.on_demand,
        wheelchair_accessible=s.wheelchair_accessible,
    )


async def _alerts(ctx: Context, conn: AsyncConnection, catalog: Catalog, pids: list[str], out: list[Departure]):
    """Alerts for the stop and every line departing from it today, not only the ones on this page."""
    served = await stops.served_routes(conn, catalog, pids)
    routes = await catalog.routes({fv for fv, _ in served})
    scope = alerts.Scope(
        routes={(routes[k].feed_id, routes[k].route_id): routes[k].route_type for k in served if k in routes},
        platforms=set(pids),
        trips={(d.feed_id, d.trip_id) for d in out},
        feeds={routes[k].feed_id for k in served if k in routes} | {d.feed_id for d in out},
    )
    for d in out:
        if d.line:
            scope.routes.setdefault((d.feed_id, d.line.route_id), None)
    return await alerts.in_scope(ctx, conn, scope)


async def board(ctx: Context, key: str, cursor_text: str | None, limit: int) -> DepartureBoard:
    """A page of departures from a logical stop (all its platforms) or one platform, with realtime; alerts on the
    first page only.
    """
    cursor = Cursor.decode(cursor_text) if cursor_text else None
    now = ctx.now()
    async with ctx.pool.connection() as conn:
        stop = await stops.get(conn, key)
        pids = await stops.platform_ids(conn, stop)
        page = await _page(ctx, conn, pids, cursor, now, limit)

        platforms = await stops.platforms_by_id(conn, {r.s.stop_platform_id for r in page})
        lines = await ctx.catalog.lines((r.s.feed_version_id, r.s.route_id) for r in page)
        out = [_departure(r, lines.get((r.s.feed_version_id, r.s.route_id)), platforms[r.s.stop_platform_id]) for r in page]
        board_alerts = []
        if cursor is None:
            board_alerts = await _alerts(ctx, conn, ctx.catalog, pids, out)

    # Cursors from the page's ends; a partial page still gets them, the next request searches on from there.
    # An empty page has none in its direction (nothing within the widest window); the first page can always go
    # back, from now.
    earlier = Cursor("earlier", page[0].key if page else (now, "", 0)).encode()
    later = Cursor("later", page[-1].key).encode() if page else None
    if cursor is not None and cursor.direction == "earlier" and not page:
        earlier = None

    return DepartureBoard(
        stop=stop,
        timestamp=now,
        departures=out,
        earlier=earlier,
        later=later,
        alerts=board_alerts,
    )
