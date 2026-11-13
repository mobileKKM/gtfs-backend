"""Departure boards: the timetable from Postgres with realtime predictions from Redis laid over it.

The timetable query covers every authoritative service day that can run in the window (a night trip of
yesterday's service can still be running) and reaches back LOOKBACK_S so delayed trips stay on the board until
they leave (and GRACE_S after).

A board starts at `time` (epoch). Without it, it starts now and leaves out departures that have left; with it, rows
from that time on include departed ones as `departed`, with the last time realtime knew. Clients page by passing
the last departure's time. `feeds` and `routes` (route names) narrow the board, and its alerts with it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from psycopg import AsyncConnection

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.catalog import Catalog, Day, RouteInfo
from gtfs_backend.api.data.realtime import TripUpdate
from gtfs_backend.api.data.sql import union_per_version
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.schemas import Departure, DepartureBoard, DepartureStatus, RouteRef
from gtfs_backend.api.schemas.departures import VehicleState
from gtfs_backend.api.services import alerts, stops, vehicles

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
    public_stop_id: str
    scheduled_departure: int  # epoch
    headsign: str | None
    route_id: str
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
    timestamp: int | None = None  # of the trip update


@dataclass(frozen=True)
class Row:
    s: Scheduled
    live: Live
    vehicle: dict | None = None  # its vehicle's JSON, when it runs this trip

    @property
    def time(self) -> int:
        return self.live.estimated or self.s.scheduled_departure

    @property
    def key(self) -> SortKey:
        return (self.time, self.s.trip_key, self.s.stop_sequence)


# ------------------------------------------------------------------------------------------------- timetable + realtime


async def scheduled(
    conn: AsyncConnection, days: list[Day], public_stop_ids: list[str], start: int, end: int, lookback: int = LOOKBACK_S
) -> list[Scheduled]:
    """Timetabled departures at `public_stop_ids` between start - lookback and end (epochs).

    Skips stops without pickup and the trip's last stop (arrivals only).
    """
    days = [d for d in days if end - d.base >= 0 and start - lookback - d.base <= MAX_GTFS_TIME_S]
    if not days or not public_stop_ids:
        return []
    sql, params = union_per_version(
        """
        SELECT {i}, st.trip_id, st.stop_sequence, l.public_stop_id,
               extract(epoch FROM st.departure_time)::int, coalesce(st.stop_headsign, t.trip_headsign),
               t.route_id, coalesce(st.pickup_type, 0) IN (2, 3)
        FROM core.stop_link l
        JOIN gtfs.stop_times st ON st.feed_version_id = %(fv{i})s AND st.stop_id = l.stop_id
             AND st.departure_time BETWEEN %(lo{i})s * interval '1 second' AND %(hi{i})s * interval '1 second'
        JOIN gtfs.trips t ON t.feed_version_id = %(fv{i})s AND t.trip_id = st.trip_id
        WHERE l.feed_version_id = %(fv{i})s AND l.public_stop_id = ANY(%(public_stop_ids)s)
          AND coalesce(st.pickup_type, 0) <> 1
          AND EXISTS (SELECT 1 FROM core.service_date sd
                      WHERE sd.feed_version_id = %(fv{i})s AND sd.date = %(date{i})s AND sd.service_id = t.service_id)
          AND EXISTS (SELECT 1 FROM gtfs.stop_times n
                      WHERE n.feed_version_id = %(fv{i})s AND n.trip_id = st.trip_id AND n.stop_sequence > st.stop_sequence)
        """,
        [{"fv": d.feed_version_id, "date": d.date, "lo": start - lookback - d.base, "hi": end - d.base} for d in days],
        {"public_stop_ids": public_stop_ids},
    )
    cur = await conn.execute(sql, params)
    out = []
    for i, trip_id, seq, pid, dep, headsign, route_id, on_demand in await cur.fetchall():
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
                on_demand,
            )
        )
    return out


def runs(vehicle: dict | None, s: Scheduled) -> bool:
    """Whether the vehicle is on this trip (it may have moved on to the next one) in this timetable version."""
    return vehicle is not None and vehicle["trip"]["key"] == s.trip_key and vehicle["trip"]["version"] == s.version


def overlay(s: Scheduled, raw: dict | None, now: int, vehicle: dict | None = None, grace: int = GRACE_S) -> Live:
    """Realtime state of one scheduled departure.

    `departed` once the vehicle reports a later stop of the trip (GTFS-RT VehiclePosition.current_stop_sequence),
    or, without that, once its time is `grace` past. A TripUpdate says nothing about passed stops: it only lists
    predictions, so a stop it doesn't list isn't taken as passed.
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
    elif _vehicle_past(vehicle, s) or (estimated or s.scheduled_departure) + grace < now:
        status = "departed"
    else:
        status = "live" if is_live else "scheduled"
    if update is None:
        return Live(estimated, delay, is_live, status, None)
    return Live(estimated, delay, is_live, status, update.vehicle_id, update.timestamp)


def _vehicle_past(vehicle: dict | None, s: Scheduled) -> bool:
    seq, _ = vehicles.stop_state(vehicle) if runs(vehicle, s) else (None, None)
    return seq is not None and seq > s.stop_sequence


def _upcoming(row: Row, now: int, grace: int) -> bool:
    return row.live.status != "departed" and row.time >= now - grace


def select(rows: list[Row], now: int, limit: int, start: int, end: int, live: bool, grace: int = GRACE_S) -> list[Row]:
    """One page, ascending: the rows from `start` on. `live` (no time asked): only upcoming rows, delayed ones
    included, those that have left excluded.
    Only rows whose time is within [start, end] count: outside it, the timetable query may have missed some.
    """
    rows = sorted((r for r in rows if start <= r.time <= end), key=lambda r: r.key)
    if live:
        rows = [r for r in rows if _upcoming(r, now, grace)]
    return rows[:limit]


async def _vehicles(ctx: Context, found: list[Scheduled], updates: dict[str, dict]) -> dict[str, dict]:
    """trip_key -> JSON of the vehicle running it, for the trips whose update names one (one Redis call)."""
    wanted = {
        s.trip_key: keys.vehicle_key(s.feed_id, u["vehicle_id"])
        for s in found
        if (u := updates.get(s.trip_key)) and u.get("vehicle_id")
    }
    raws = await realtime.vehicle_json(ctx.redis, sorted(set(wanted.values())))
    by_key = {keys.vehicle_key(v["feed_id"], v["vehicle_id"]): v for v in raws}
    return {t: v for t, k in wanted.items() if (v := by_key.get(k)) and v["trip"]["key"] == t}


async def _page(
    ctx: Context,
    conn: AsyncConnection,
    pids: list[str],
    start: int,
    live: bool,
    feeds: set[str],
    route_names: set[str],
    now: int,
    limit: int,
) -> tuple[list[Row], dict[tuple[str, str], RouteRef]]:
    """Rows of one page with their routes, widening the window until it is full."""
    page: list[Row] = []
    refs: dict[tuple[str, str], RouteRef] = {}
    for span in WINDOWS_S:
        lo, hi = (start - GRACE_S if live else start), start + span
        days = [d for d in await ctx.catalog.days_between(lo, hi) if not feeds or d.feed_id in feeds]
        found = await scheduled(conn, days, pids, lo, hi)
        refs = await ctx.catalog.route_refs({(f.feed_version_id, f.route_id) for f in found})
        if route_names:
            found = [f for f in found if (ref := refs.get((f.feed_version_id, f.route_id))) and ref.name in route_names]
        updates = await realtime.trip_updates(ctx.redis, [s.trip_key for s in found])
        by_vehicle = await _vehicles(ctx, found, updates)
        rows = [
            Row(f, overlay(f, updates.get(f.trip_key), now, by_vehicle.get(f.trip_key)), by_vehicle.get(f.trip_key))
            for f in found
        ]
        page = select(rows, now, limit, lo, hi, live)
        if len(page) >= limit:
            break
    return page, refs


VEHICLE_STATES = {"stopped_at": "at_stop", "incoming_at": "arriving"}


def vehicle_state(row: Row, vehicle: dict | None) -> VehicleState | None:
    """Whether the vehicle is at / arriving at this very stop; the feed's stop report says so only for one stop."""
    if vehicle is None or row.live.status in ("departed", "canceled", "skipped"):
        return None
    seq, status = vehicles.stop_state(vehicle)
    if seq != row.s.stop_sequence:
        return None
    return VEHICLE_STATES.get(status or "")


def _departure(row: Row, route: RouteRef | None, stop, state: VehicleState | None = None) -> Departure:
    s, live = row.s, row.live
    return Departure(
        trip_key=s.trip_key,
        stop_sequence=s.stop_sequence,
        route=route,
        headsign=s.headsign,
        stop=stops.stop_ref(stop),
        scheduled=s.scheduled_departure,
        estimated=live.estimated,
        time=row.time,
        delay_s=live.delay_s,
        status=live.status,
        vehicle_key=keys.vehicle_key(s.feed_id, live.vehicle_id) if live.vehicle_id else None,
        vehicle_state=state,
        on_demand=s.on_demand,
    )


async def _served(conn: AsyncConnection, catalog: Catalog, pids: list[str]) -> list[RouteInfo]:
    """The routes departing from these stops in today's timetables (on any day), by feed and name."""
    served = await stops.served_routes(conn, catalog, pids)
    infos = await catalog.routes({fv for fv, _ in served})
    return sorted((infos[k] for k in served if k in infos), key=lambda i: (i.feed_id, i.ref().name, i.route_id))


async def _alerts(
    ctx: Context, pids: list[str], page: list[Row], served: list[RouteInfo], feeds: set[str], route_names: set[str]
):
    """Alerts for the stop and every route departing from it today (within the filters), not only the ones on this page."""
    shown = [i for i in served if (not feeds or i.feed_id in feeds) and (not route_names or i.ref().name in route_names)]
    scope = alerts.Scope(
        routes={(i.feed_id, i.route_id): i.route_type for i in shown},
        public_stop_ids=set(pids),
        trips={r.s.trip_key for r in page},
        feeds={i.feed_id for i in shown} | {r.s.feed_id for r in page},
    )
    for r in page:
        scope.routes.setdefault((r.s.feed_id, r.s.route_id), None)
    return await alerts.in_scope(ctx, scope)


async def board(ctx: Context, key: str, time: int | None, feeds: set[str], route_names: set[str], limit: int) -> DepartureBoard:
    """A page of departures from a stop group (all its stops) or one stop, from `time` (default now) on,
    with realtime and the alerts that apply. `routes` are all the routes that depart from the stop in today's
    timetables (on any day), whatever the filters, so clients know what they can filter by.
    """
    if unknown := feeds - set(ctx.feed_ids):
        raise InvalidInput(f"unknown feed: {', '.join(sorted(unknown))}")
    now = ctx.now()
    async with ctx.pool.connection() as conn:
        stop = await stops.get(conn, key)
        pids = await stops.public_stop_ids(conn, stop)
        page, refs = await _page(ctx, conn, pids, now if time is None else time, time is None, feeds, route_names, now, limit)

        by_id = await stops.stops_by_id(conn, {r.s.public_stop_id for r in page})
        out = [
            _departure(r, refs.get((r.s.feed_version_id, r.s.route_id)), by_id[r.s.public_stop_id], vehicle_state(r, r.vehicle))
            for r in page
        ]
        served = await _served(conn, ctx.catalog, pids)
    board_alerts = await _alerts(ctx, pids, page, served, feeds, route_names)

    timestamp = realtime.newest(r.live.timestamp for r in page)
    return DepartureBoard(stop=stop, timestamp=timestamp, routes=[i.ref() for i in served], departures=out, alerts=board_alerts)
