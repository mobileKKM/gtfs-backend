"""One trip on one service day, in two parts: the view (shape, route, every stop's place and timetable), which stays
the same while the trip runs, and its live state (every stop's predictions, the vehicle, alerts), which clients poll.
"""

from __future__ import annotations

from datetime import date

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.data.catalog import Day
from gtfs_backend.api.data.geo import bbox_of, encode_polyline
from gtfs_backend.api.data.realtime import TripUpdate
from gtfs_backend.api.errors import NotFound
from gtfs_backend.api.schemas import MapVehicle, StopRef, TripLive, TripShape, TripStop, TripStopLive, TripView
from gtfs_backend.api.services import alerts, vehicles
from gtfs_backend.api.services.routes import POLYLINE_SQL

ON_DEMAND = (2, 3)  # GTFS pickup/drop_off_type: phone the agency, coordinate with the driver

TRIP_SQL = """
    SELECT t.route_id, t.trip_headsign, t.direction_id, t.shape_id
    FROM gtfs.trips t
    WHERE t.feed_version_id = %(fv)s AND t.trip_id = %(trip)s
      AND EXISTS (SELECT 1 FROM core.service_date sd
                  WHERE sd.feed_version_id = t.feed_version_id AND sd.date = %(date)s AND sd.service_id = t.service_id)
"""
SHAPE_SQL = f"SELECT {POLYLINE_SQL} FROM core.shape_line s WHERE s.feed_version_id = %(fv)s AND s.shape_id = %(shape)s"
STOPS_SQL = """
    SELECT st.stop_sequence, extract(epoch FROM st.arrival_time)::int, extract(epoch FROM st.departure_time)::int,
           st.pickup_type, st.drop_off_type,
           p.id, p.url_key, p.name, p.stop_group_id, p.platform_code, ST_Y(p.geom), ST_X(p.geom)
    FROM gtfs.stop_times st
    LEFT JOIN core.stop_link l ON l.feed_version_id = st.feed_version_id AND l.feed_stop_id = st.stop_id
    LEFT JOIN core.stop p ON p.id = l.stop_id
    WHERE st.feed_version_id = %(fv)s AND st.trip_id = %(trip)s
    ORDER BY st.stop_sequence
"""
# the live state needs only what decides `passed` without realtime, and the stops alerts can name
STOPS_LIVE_SQL = """
    SELECT st.stop_sequence, extract(epoch FROM st.arrival_time)::int, extract(epoch FROM st.departure_time)::int,
           l.stop_id
    FROM gtfs.stop_times st
    LEFT JOIN core.stop_link l ON l.feed_version_id = st.feed_version_id AND l.feed_stop_id = st.stop_id
    WHERE st.feed_version_id = %(fv)s AND st.trip_id = %(trip)s
    ORDER BY st.stop_sequence
"""


async def _day(ctx: Context, feed_id: str, service_date: date) -> Day:
    day = await ctx.catalog.day(feed_id, service_date)
    if day is None:
        raise NotFound("trip")
    return day


async def _trip(conn, params: dict) -> tuple:
    """(route_id, headsign, direction_id, shape_id) of a trip running that day."""
    trip = await (await conn.execute(TRIP_SQL, params)).fetchone()
    if trip is None:
        raise NotFound("trip")
    return trip


def _stop(row, day: Day) -> TripStop:
    seq, arr, dep, pickup, drop_off, pid, pkey, pname, gid, pcode, lat, lon = row
    return TripStop(
        stop_sequence=seq,
        stop=StopRef(id=pid, key=pkey, name=pname, group_id=gid, platform_code=pcode) if pid else None,
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


async def view(ctx: Context, feed_id: str, service_date: date, trip_id: str) -> TripView:
    day = await _day(ctx, feed_id, service_date)
    params = {"fv": day.feed_version_id, "trip": trip_id, "date": service_date, "shape": None}
    async with ctx.pool.connection() as conn:
        route_id, headsign, direction_id, params["shape"] = await _trip(conn, params)
        stops = [_stop(r, day) for r in await (await conn.execute(STOPS_SQL, params)).fetchall()]
        polyline = bbox = None
        if params["shape"] is not None and (row := await (await conn.execute(SHAPE_SQL, params)).fetchone()):
            polyline, bbox = row
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


def _live_stop(row, day: Day, update: TripUpdate | None, now: int) -> TripStopLive:
    seq, arr, dep, _ = row
    live = update.stop(seq) if update else None
    if update is None:  # no realtime: passed once its time is over
        last = dep if dep is not None else arr
        passed = last is not None and day.base + last < now
    else:
        passed = update.passed(seq)
    return TripStopLive(
        stop_sequence=seq,
        estimated_arrival=live.predicted_arrival if live else None,
        estimated_departure=live.predicted_departure if live else None,
        delay_s=live.delay_s if live else None,
        passed=passed,
        skipped=bool(live and live.skipped),
    )


async def live(ctx: Context, feed_id: str, service_date: date, trip_id: str, vehicle: MapVehicle | None = None) -> TripLive:
    """`vehicle`: the vehicle the trip was opened from; otherwise the one realtime says runs the trip, if live."""
    day = await _day(ctx, feed_id, service_date)
    params = {"fv": day.feed_version_id, "trip": trip_id, "date": service_date}
    key = keys.trip_key(feed_id, service_date, trip_id)
    async with ctx.pool.connection() as conn:
        route_id, _, _, _ = await _trip(conn, params)
        rows = await (await conn.execute(STOPS_LIVE_SQL, params)).fetchall()

    update = TripUpdate.for_version((await realtime.trip_updates(ctx.redis, [key])).get(key), day.version)
    now = ctx.now()
    stops = [_live_stop(r, day, update, now) for r in rows]

    if vehicle is None and update and update.vehicle_id:
        found = await vehicles.by_ids(ctx, [f"{feed_id}:{update.vehicle_id}"])
        vehicle = next((v for v in found if v.trip_key == key), None)  # it may have moved on to its next trip

    info = await ctx.catalog.route(day.feed_version_id, route_id)
    scope = alerts.Scope(
        routes={(feed_id, route_id): info.route_type if info else None},
        trips={(feed_id, trip_id)},
        stop_ids={pid for *_, pid in rows if pid},
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
        current_index=next((i for i, s in enumerate(stops) if not s.passed), None),
        alerts=await alerts.in_scope(ctx, scope),
    )


async def view_by_key(ctx: Context, trip_key: str) -> TripView:
    return await view(ctx, *keys.parse_trip_key(trip_key))


async def live_by_key(ctx: Context, trip_key: str) -> TripLive:
    return await live(ctx, *keys.parse_trip_key(trip_key))


async def for_vehicle(ctx: Context, vehicle_key: str) -> TripLive:
    """The live state of the trip the vehicle runs now; its `key` changes when the vehicle starts its next trip."""
    feed_id, _ = keys.split_feed(vehicle_key, "vehicle key")
    raws = await realtime.vehicle_json(ctx.redis, [vehicle_key])
    if not raws:
        raise NotFound("vehicle")
    (vehicle,) = await vehicles.models(ctx, raws)
    trip = raws[0]["trip"]
    return await live(ctx, feed_id, date.fromisoformat(trip["service_date"]), trip["trip_id"], vehicle=vehicle)
