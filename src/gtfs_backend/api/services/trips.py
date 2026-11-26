"""One trip on one service day, in two parts: the view (shape, route, every stop's place and timetable), which stays
the same while the trip runs, and its live state (every stop's predictions, the vehicle, alerts), which clients poll.

The timetable part comes from the snapshot (api/data/timetable.py) for the days it has, else from Postgres.
"""

from __future__ import annotations

from datetime import date

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.catalog import Day
from gtfs_backend.api.data.geo import bbox_of, encode_polyline
from gtfs_backend.api.data.live import PID, SCHED, SCHED_ARR, SEQ, STOPS, extra_of
from gtfs_backend.api.data.realtime import StopUpdate, TripUpdate
from gtfs_backend.api.data.timetable import NOT_COVERED, TripStatic
from gtfs_backend.api.errors import NotFound
from gtfs_backend.api.schemas import StopRef, TripLive, TripShape, TripStop, TripStopLive, TripView, Vehicle
from gtfs_backend.api.schemas.trips import TripStopStatus
from gtfs_backend.api.services import alerts, vehicles
from gtfs_backend.api.services.routes import POLYLINE_SQL

ON_DEMAND = (2, 3)  # GTFS pickup/drop_off_type: phone the agency, coordinate with the driver

# the trip runs on that service date (a trip key names a date it may not run on)
RUNS_SQL = """EXISTS (SELECT 1 FROM core.service_date sd
                     WHERE sd.feed_version_id = %(fv)s AND sd.date = %(date)s AND sd.service_id = t.service_id)"""

# One round trip each. The view: the trip, its shape and every stop (as JSON rows, in stop order).
VIEW_SQL = f"""
    SELECT t.route_id, t.trip_headsign, t.direction_id, sh.polyline, sh.bbox, st.stops
    FROM gtfs.trips t
    LEFT JOIN LATERAL (
        SELECT {POLYLINE_SQL} FROM core.shape_line s WHERE s.feed_version_id = %(fv)s AND s.shape_id = t.shape_id
    ) sh (polyline, bbox) ON true
    CROSS JOIN LATERAL (
        SELECT json_agg(json_build_array(
                   st.stop_sequence, extract(epoch FROM st.arrival_time)::int, extract(epoch FROM st.departure_time)::int,
                   st.pickup_type, st.drop_off_type,
                   p.url_key, p.name, g.url_key, p.platform_code, ST_Y(p.geom), ST_X(p.geom)
               ) ORDER BY st.stop_sequence) AS stops
        FROM gtfs.stop_times st
        LEFT JOIN core.stop_link l ON l.feed_version_id = %(fv)s AND l.stop_id = st.stop_id
        LEFT JOIN core.stop p ON p.id = l.public_stop_id
        LEFT JOIN core.stop_group g ON g.id = p.stop_group_id
        WHERE st.feed_version_id = %(fv)s AND st.trip_id = t.trip_id
    ) st
    WHERE t.feed_version_id = %(fv)s AND t.trip_id = %(trip)s AND {RUNS_SQL}
"""
# The live state needs only the stop sequences, and the stops alerts can name.
LIVE_SQL = f"""
    SELECT t.route_id, t.direction_id, st.seqs, st.pids
    FROM gtfs.trips t
    CROSS JOIN LATERAL (
        SELECT array_agg(st.stop_sequence ORDER BY st.stop_sequence) AS seqs,
               array_agg(l.public_stop_id ORDER BY st.stop_sequence) AS pids
        FROM gtfs.stop_times st
        LEFT JOIN core.stop_link l ON l.feed_version_id = %(fv)s AND l.stop_id = st.stop_id
        WHERE st.feed_version_id = %(fv)s AND st.trip_id = t.trip_id
    ) st
    WHERE t.feed_version_id = %(fv)s AND t.trip_id = %(trip)s AND {RUNS_SQL}
"""


async def _day(ctx: Context, feed_id: str, service_date: date) -> Day:
    day = await ctx.catalog.day(feed_id, service_date)
    if day is None:
        raise NotFound("trip")
    return day


async def _row(ctx: Context, sql: str, params: dict) -> tuple:
    """The trip's row, if it runs that day."""
    async with ctx.pool.connection() as conn:
        row = await (await conn.execute(sql, params)).fetchone()
    if row is None:
        raise NotFound("trip")
    return row


def _stop(row, day: Day) -> TripStop:
    seq, arr, dep, pickup, drop_off, pkey, pname, gkey, pcode, lat, lon = row
    return TripStop(
        stop_sequence=seq,
        stop=StopRef(key=pkey, name=pname, group_key=gkey, platform_code=pcode) if pkey else None,
        lat=lat,
        lon=lon,
        scheduled_arrival=day.base + arr if arr is not None else None,
        scheduled_departure=day.base + dep if dep is not None else None,
        on_demand=pickup in ON_DEMAND or drop_off in ON_DEMAND,
    )


def _shape(polyline: str | None, bbox: list[float] | None, stops: list[TripStop]) -> TripShape | None:
    """The trip's shape, or a line through its stops when the feed has none (Koleje Małopolskie trains)."""
    if polyline and bbox:
        return TripShape(polyline=polyline, bbox=bbox, source="shape")
    points = [(s.lat, s.lon) for s in stops if s.lat is not None and s.lon is not None]
    if not points:
        return None
    return TripShape(polyline=encode_polyline(points), bbox=bbox_of(points), source="stops")


def _static(ctx: Context, day: Day, trip_id: str):
    """The trip from the snapshot: TripStatic, NOT_COVERED (read Postgres), or NotFound if it doesn't run that day."""
    tt = ctx.catalog.timetable
    if tt is None:
        return NOT_COVERED
    static = tt.trip(day.feed_id, day.date, day.feed_version_id, trip_id)
    if static is None:
        raise NotFound("trip")
    return static


def _snapshot_stops(ctx: Context, static: TripStatic, day: Day) -> list[TripStop] | None:
    """The view's stops from the snapshot and the catalog's stops; None if the catalog lacks one of them (a stop
    imported since it loaded: Postgres has it)."""
    wanted = {pid for *_, pid in static.stops if pid}
    by_id = ctx.catalog.stops.stops_by_id(wanted)
    if len(by_id) != len(wanted):
        return None
    out = []
    for seq, arr, dep, on_demand, pid in static.stops:
        stop = by_id.get(pid) if pid else None
        out.append(
            TripStop(
                stop_sequence=seq,
                stop=StopRef(key=stop.key, name=stop.name, group_key=stop.group_key, platform_code=stop.platform_code)
                if stop
                else None,
                lat=stop.lat if stop else None,
                lon=stop.lon if stop else None,
                scheduled_arrival=day.base + arr if arr is not None else None,
                scheduled_departure=day.base + dep if dep is not None else None,
                on_demand=on_demand,
            )
        )
    return out


def _extra(ctx: Context, key: str) -> list | None:
    """The live state's trip, if it is one no timetable has (GTFS-RT NEW, DUPLICATED, ...)."""
    live = ctx.live_state
    return live.trips.get(key) if live is not None and live.ready and extra_of(live.trips.get(key)) else None


def _extra_view(ctx: Context, trip: list) -> tuple:
    """(route_id, headsign, direction_id, polyline, bbox, stops) of an extra trip, from realtime: drawn through its stops."""
    route_id, headsign = extra_of(trip)
    entries = trip[STOPS]
    by_id = ctx.catalog.stops.stops_by_id({e[PID] for e in entries if e[PID]})
    stops = []
    for e in entries:
        stop = by_id.get(e[PID]) if e[PID] else None
        stops.append(
            TripStop(
                stop_sequence=e[SEQ],
                stop=StopRef(key=stop.key, name=stop.name, group_key=stop.group_key, platform_code=stop.platform_code)
                if stop
                else None,
                lat=stop.lat if stop else None,
                lon=stop.lon if stop else None,
                scheduled_arrival=e[SCHED_ARR] if len(e) > SCHED_ARR else None,
                scheduled_departure=e[SCHED],
                on_demand=False,
            )
        )
    return route_id, headsign, None, None, None, stops


async def view(ctx: Context, feed_id: str, service_date: date, trip_id: str) -> TripView:
    day = await _day(ctx, feed_id, service_date)
    if (extra := _extra(ctx, keys.trip_key(feed_id, service_date, trip_id))) is not None:
        route_id, headsign, direction_id, polyline, bbox, stops = _extra_view(ctx, extra)
        info = await ctx.catalog.route(day.feed_version_id, route_id) if route_id else None
        return TripView(
            key=keys.trip_key(feed_id, service_date, trip_id),
            service_date=keys.service_date_str(service_date),
            route=info.model() if info else None,
            headsign=headsign,
            direction_id=None,
            shape=_shape(None, None, stops),
            stops=stops,
        )
    static = _static(ctx, day, trip_id)
    stops = _snapshot_stops(ctx, static, day) if static is not NOT_COVERED else None
    if stops is not None:
        route_id, headsign, direction_id, polyline, bbox = (
            static.route_id,
            static.headsign,
            static.direction_id,
            static.polyline,
            static.bbox,
        )
    else:
        params = {"fv": day.feed_version_id, "trip": trip_id, "date": service_date}
        route_id, headsign, direction_id, polyline, bbox, rows = await _row(ctx, VIEW_SQL, params)
        stops = [_stop(r, day) for r in rows or []]
    info = await ctx.catalog.route(day.feed_version_id, route_id)
    return TripView(
        key=keys.trip_key(feed_id, service_date, trip_id),
        service_date=keys.service_date_str(service_date),
        route=info.model() if info else None,
        headsign=headsign,
        direction_id=direction_id,
        shape=_shape(polyline, bbox, stops),
        stops=stops,
    )


def stop_status(live: StopUpdate | None) -> TripStopStatus:
    """`skipped`; `live` / `predicted` with a time from the feed for this stop / carried over from an earlier one; else
    `scheduled` (also where the feed says it has no data: NO_DATA)."""
    if live is None:
        return "scheduled"
    if live.skipped:
        return "skipped"
    if live.predicted is None:
        return "scheduled"
    return "predicted" if live.propagated else "live"


def _live_stop(row, update: TripUpdate | None) -> TripStopLive:
    seq, _ = row
    live = update.stop(seq) if update else None
    return TripStopLive(
        stop_sequence=seq,
        status=stop_status(live),
        estimated_arrival=live.predicted_arrival if live else None,
        estimated_departure=live.predicted_departure if live else None,
        delay_s=live.delay_s if live else None,
    )


async def live(ctx: Context, feed_id: str, service_date: date, trip_id: str, vehicle: Vehicle | None = None) -> TripLive:
    """`vehicle`: the vehicle the trip was opened from; otherwise the one realtime says runs the trip, if live."""
    day = await _day(ctx, feed_id, service_date)
    params = {"fv": day.feed_version_id, "trip": trip_id, "date": service_date}
    key = keys.trip_key(feed_id, service_date, trip_id)
    live_state = ctx.live
    extra = _extra(ctx, key)
    static = NOT_COVERED if extra is not None else _static(ctx, day, trip_id)
    if extra is not None:
        route_id, direction_id = extra_of(extra)[0], None
        seqs, pids = [e[SEQ] for e in extra[STOPS]], [e[PID] for e in extra[STOPS]]
    elif static is not NOT_COVERED:
        route_id, direction_id = static.route_id, static.direction_id
        seqs, pids = [st[0] for st in static.stops], [st[4] for st in static.stops]
    else:
        route_id, direction_id, seqs, pids = await _row(ctx, LIVE_SQL, params)
    alert_raws = live_state.alerts_of(ctx.feed_ids)
    rows = list(zip(seqs or [], pids or [], strict=True))
    update = live_state.trip_update(key)
    if update is not None and update.version != day.version:
        update = None  # computed for another timetable version: stop sequences differ
    if update is not None and not ctx.fresh(feed_id, update.timestamp):
        update = None  # measured too long ago: the worker or the feed stopped
    stops = [_live_stop(r, update) for r in rows]

    if vehicle is None and update and update.vehicle_id:
        found = await vehicles.by_ids(ctx, [f"{feed_id}:{update.vehicle_id}"])
        vehicle = next((v for v in found if v.trip_key == key), None)  # it may have moved on to its next trip

    info = await ctx.catalog.route(day.feed_version_id, route_id)
    scope = alerts.Scope(
        routes={(feed_id, route_id): info.route_type if info else None},
        directions={(feed_id, route_id, direction_id)} if direction_id is not None else set(),
        trips={key},
        public_stop_ids={pid for _, pid in rows if pid},
        feeds={feed_id},
    )
    if update is not None and update.canceled:
        status = "canceled"
    elif update is not None:
        status = "live"
    else:
        status = "scheduled"
    return TripLive(
        key=key,
        status=status,
        delay_s=update.delay_s if update else None,
        timestamp=realtime.newest([update.timestamp if update else None, vehicle.timestamp if vehicle else None]),
        vehicle=vehicle,
        stops=stops,
        alerts=await alerts.in_scope(ctx, scope, alert_raws),
    )


async def view_by_key(ctx: Context, trip_key: str) -> TripView:
    return await view(ctx, *keys.parse_trip_key(trip_key))


async def live_by_key(ctx: Context, trip_key: str) -> TripLive:
    return await live(ctx, *keys.parse_trip_key(trip_key))


async def for_vehicle(ctx: Context, vehicle_key: str) -> TripLive:
    """The live state of the trip the vehicle runs now; its `key` changes when the vehicle starts its next trip."""
    feed_id, _ = keys.split_feed(vehicle_key, "vehicle key")
    raws = vehicles.fresh(ctx, [v] if (v := ctx.live.vehicle(vehicle_key)) is not None else [])
    if not raws:
        raise NotFound("vehicle")
    (vehicle,) = await vehicles.models(ctx, raws)
    trip = raws[0]["trip"]
    return await live(ctx, feed_id, date.fromisoformat(trip["service_date"]), trip["trip_id"], vehicle=vehicle)
