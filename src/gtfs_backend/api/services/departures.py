"""Departure boards: the timetable from Postgres (core.stop_departure) with realtime predictions from Redis
(rt:tripstate, rt:stop:*: only the board's stops) laid over it.

The timetable query covers every authoritative service day that can run in the window (a night trip of
yesterday's service can still be running) and reaches back LOOKBACK_S so delayed trips stay on the board until
they leave (and GRACE_S after).

A board starts at `time` (epoch). Without it, it starts now and leaves out departures that have left; with it, rows
from that time on include departed ones (`departed: true`), with the last time realtime knew. Clients page by passing
the last departure's time. `feeds` and `routes` (route names) narrow the board, and its alerts with it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from psycopg import AsyncConnection

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.catalog import Catalog, Day, RouteInfo
from gtfs_backend.api.data.realtime import TripState, TripUpdate
from gtfs_backend.api.data.sql import union_per_version
from gtfs_backend.api.data.stopdata import Hourly
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.api.schemas import Departure, DepartureBoard, DepartureStatus, RouteRef
from gtfs_backend.api.schemas.departures import VehicleState
from gtfs_backend.api.services import alerts, stops, vehicles

LOOKBACK_S = 60 * 60  # delayed departures stay this long past their time
GRACE_S = 30  # keep a departure this long after it leaves
# The first window is where the stop's hourly profile (today's timetables) expects PAGE_MARGIN pages; if that
# isn't enough (filters, another day's timetable, a trip from past the window running early), it widens through
# these. Each step reads only the time it adds.
WINDOWS_S = (30 * 60, 2 * 3600, 6 * 3600, 24 * 3600)
PAGE_MARGIN = 1.5
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
    on_demand: bool
    # keys.trip_key of the trip, built by scheduled() from one prefix per service day (formatting the date for every
    # row costs more than the rest of a row); sorting a board reads it thousands of times
    trip_key: str = field(compare=False)


@dataclass(frozen=True)
class Live:
    estimated: int | None
    delay_s: int | None
    realtime: bool
    status: DepartureStatus
    departed: bool
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
    """Timetabled departures at `public_stop_ids` between start - lookback and end (epochs), from core.stop_departure
    (precomputed at import: no stops without pickup, no trip's last stop). One index range per stop and day.
    """
    days = [d for d in days if end - d.base >= 0 and start - lookback - d.base <= MAX_GTFS_TIME_S]
    if not days or not public_stop_ids:
        return []
    sql, params = union_per_version(
        """
        SELECT {i}, d.trip_id, d.stop_sequence, d.public_stop_id, d.departure_s, d.headsign, d.route_id, d.on_demand
        FROM core.stop_departure d
        WHERE d.feed_version_id = %(fv{i})s AND d.public_stop_id = ANY(%(public_stop_ids)s)
          AND d.service_id = ANY(ARRAY(SELECT sd.service_id FROM core.service_date sd
                                       WHERE sd.feed_version_id = %(fv{i})s AND sd.date = %(date{i})s))
          AND d.departure_s BETWEEN %(lo{i})s AND %(hi{i})s
        """,
        [{"fv": d.feed_version_id, "date": d.date, "lo": start - lookback - d.base, "hi": end - d.base} for d in days],
        {"public_stop_ids": public_stop_ids},
    )
    cur = await conn.execute(sql, params)
    out = []
    prefixes = [keys.trip_key(d.feed_id, d.date, "") for d in days]
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
                prefixes[i] + trip_id,
            )
        )
    return out


def runs(vehicle: dict | None, s: Scheduled) -> bool:
    """Whether the vehicle is on this trip (it may have moved on to the next one) in this timetable version."""
    return vehicle is not None and vehicle["trip"]["key"] == s.trip_key and vehicle["trip"]["version"] == s.version


def overlay(
    s: Scheduled, update: TripUpdate | TripState | None, now: int, vehicle: dict | None = None, grace: int = GRACE_S
) -> Live:
    """Realtime state of one scheduled departure.

    `status`: `canceled` (the trip), `skipped` (this stop), `live` (the feed predicts this stop), `predicted` (a delay
    carried over from an earlier stop), else `scheduled`. `departed` once the vehicle reports a later stop of the trip
    (GTFS-RT VehiclePosition.current_stop_sequence), or, without that, once its time is `grace` past. A TripUpdate says
    nothing about passed stops: it only lists predictions, so a stop it doesn't list isn't taken as passed.
    """
    if update is not None and update.version != s.version:
        update = None  # computed for another timetable version: stop sequences differ
    stop = update.stop(s.stop_sequence) if update else None
    estimated = stop.predicted if stop else None
    delay = stop.delay_s if stop else None
    is_live = estimated is not None
    if update is not None and update.canceled:
        status = "canceled"
    elif stop is not None and stop.skipped:
        status = "skipped"
    elif is_live:
        status = "predicted" if stop.propagated else "live"
    else:
        status = "scheduled"
    departed = _vehicle_past(vehicle, s) or (estimated or s.scheduled_departure) + grace < now
    if update is None:
        return Live(estimated, delay, is_live, status, departed, None)
    return Live(estimated, delay, is_live, status, departed, update.vehicle_id, update.timestamp)


def _vehicle_past(vehicle: dict | None, s: Scheduled) -> bool:
    seq, _ = vehicles.stop_state(vehicle) if runs(vehicle, s) else (None, None)
    return seq is not None and seq > s.stop_sequence


def _upcoming(row: Row, now: int, grace: int) -> bool:
    return not row.live.departed and row.time >= now - grace


def select(rows: list[Row], now: int, limit: int, start: int, end: int, live: bool, grace: int = GRACE_S) -> list[Row]:
    """One page, ascending: the rows from `start` on. `live` (no time asked): only upcoming rows, delayed ones
    included, those that have left excluded.
    Only rows whose time is within [start, end] count: outside it, the timetable query may have missed some.
    """
    rows = sorted((r for r in rows if start <= r.time <= end), key=lambda r: r.key)
    if live:
        rows = [r for r in rows if _upcoming(r, now, grace)]
    return rows[:limit]


async def _vehicles(ctx: Context, found: list[Scheduled], states: dict[str, TripState]) -> dict[str, dict]:
    """trip_key -> JSON of the vehicle running it, for the trips whose realtime names one (one Redis call)."""
    wanted = {
        s.trip_key: keys.vehicle_key(s.feed_id, u.vehicle_id) for s in found if (u := states.get(s.trip_key)) and u.vehicle_id
    }
    raws = await realtime.vehicle_json(ctx.redis, sorted(set(wanted.values())))
    since = ctx.fresh_since()
    by_key = {
        keys.vehicle_key(v["feed_id"], v["vehicle_id"]): v for v in raws if ctx.fresh(v["feed_id"], v.get("timestamp"), since)
    }
    return {t: v for t, k in wanted.items() if (v := by_key.get(k)) and v["trip"]["key"] == t}


async def _page(
    ctx: Context,
    pids: list[str],
    start: int,
    live: bool,
    feeds: set[str],
    route_names: set[str],
    now: int,
    limit: int,
) -> tuple[list[Row], dict[tuple[str, str], RouteRef]]:
    """Rows of one page with their routes, widening the window until it is full. A wider window only reads the time
    it adds (and the realtime of the trips that brings)."""
    lo = start - GRACE_S if live else start
    rows: list[Row] = []
    refs: dict[tuple[str, str], RouteRef] = {}
    page: list[Row] = []
    fetched_to: int | None = None  # departures are read up to here (from lo - LOOKBACK_S)
    board_feeds = [f for f in ctx.feed_ids if not feeds or f in feeds]
    live_data = await realtime.BoardRealtime.read(ctx.redis, board_feeds, pids, ctx.fresh_since())
    first_span = expected_span(ctx.catalog.stops.hourly(pids), _second_of_day(ctx.catalog, start), limit)
    for span in [first_span, *(w for w in WINDOWS_S if w > first_span)]:
        hi = start + span
        if fetched_to is None:
            first, lookback = lo, LOOKBACK_S
        else:
            first, lookback = fetched_to + 1, 0
        days = [d for d in await ctx.catalog.days_between(first, hi) if not feeds or d.feed_id in feeds]
        # a connection only for the query: held across the Redis calls below, boards would queue for the pool
        async with ctx.pool.connection() as conn:
            found = await scheduled(conn, days, pids, first, hi, lookback)
        fetched_to = hi
        refs |= await ctx.catalog.route_refs({(f.feed_version_id, f.route_id) for f in found})
        if route_names:
            found = [f for f in found if (ref := refs.get((f.feed_version_id, f.route_id))) and ref.name in route_names]
        # only what these stops need of each trip (rt:stop:*), not whole trips
        states = live_data.states({f.trip_key for f in found})
        # the feed deleted the trip (GTFS-RT DELETED, unlike CANCELED): not shown at all
        found = [f for f in found if not ((st := states.get(f.trip_key)) and st.deleted and st.version == f.version)]
        # the hour before `lo` only for delays: without realtime (of its version) such a departure has left (live)
        # or is before the board's start (with a time), so it can't be on the page. Most of that hour is gone here.
        found = [f for f in found if f.scheduled_departure >= lo or _realtime_of(states, f)]
        by_vehicle = await _vehicles(ctx, found, states)
        rows += [
            Row(f, overlay(f, states.get(f.trip_key), now, by_vehicle.get(f.trip_key)), by_vehicle.get(f.trip_key)) for f in found
        ]
        page = select(rows, now, limit, lo, hi, live)
        # full, unless a trip scheduled after the window runs early enough to come before its last departure
        if len(page) >= limit and ((early := live_data.earliest_early(hi, lo)) is None or early > page[-1].time):
            break
    return page, refs


def expected_span(hourly: Hourly, second_of_day: int, limit: int, margin: float = PAGE_MARGIN) -> int:
    """Seconds from `second_of_day` until `hourly` expects margin * limit departures; the widest window at most (a stop
    without departures reads it at once)."""
    need = limit * margin
    t, end = second_of_day, second_of_day + WINDOWS_S[-1]
    while t < end:
        rate = hourly[t // 3600 % 24]  # departures in this hour
        hour_end = (t // 3600 + 1) * 3600
        if rate and rate * (hour_end - t) / 3600 >= need:
            return min(t - second_of_day + int(need / rate * 3600), WINDOWS_S[-1])
        need -= rate * (hour_end - t) / 3600
        t = hour_end
    return WINDOWS_S[-1]


def _second_of_day(catalog: Catalog, epoch: int) -> int:
    """Local time of day at `epoch` (the feeds' timezone), what hourly profiles count in."""
    tz = next((info.tz for info in catalog.feeds.values()), None)
    t = datetime.fromtimestamp(epoch, tz)
    return t.hour * 3600 + t.minute * 60 + t.second


def _realtime_of(states: dict[str, TripState], s: Scheduled) -> bool:
    state = states.get(s.trip_key)
    return state is not None and state.version == s.version


VEHICLE_STATES = {"stopped_at": "at_stop", "incoming_at": "arriving"}


def vehicle_state(row: Row, vehicle: dict | None) -> VehicleState | None:
    """Whether the vehicle is at / arriving at this very stop; the feed's stop report says so only for one stop."""
    if vehicle is None or row.live.departed or row.live.status in ("canceled", "skipped"):
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
        departed=live.departed,
        vehicle_key=keys.vehicle_key(s.feed_id, live.vehicle_id) if live.vehicle_id else None,
        vehicle_state=state,
        on_demand=s.on_demand,
    )


async def _served(catalog: Catalog, pids: list[str]) -> list[RouteInfo]:
    """The routes departing from these stops in today's timetables (on any day), by feed and name."""
    served = stops.served_routes(catalog, pids)
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
    stop = stops.get(ctx.catalog, key)
    pids = ctx.catalog.stops.public_stop_ids(stop)
    page, refs = await _page(ctx, pids, now if time is None else time, time is None, feeds, route_names, now, limit)
    by_id = ctx.catalog.stops.stops_by_id({r.s.public_stop_id for r in page})
    out = [
        _departure(r, refs.get((r.s.feed_version_id, r.s.route_id)), by_id[r.s.public_stop_id], vehicle_state(r, r.vehicle))
        for r in page
    ]
    served = await _served(ctx.catalog, pids)
    board_alerts = await _alerts(ctx, pids, page, served, feeds, route_names)

    timestamp = realtime.newest(r.live.timestamp for r in page)
    return DepartureBoard(stop=stop, timestamp=timestamp, routes=[i.ref() for i in served], departures=out, alerts=board_alerts)
