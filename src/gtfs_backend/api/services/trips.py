"""The trip view: one trip on one service day with its shape, every stop's timetable and realtime state, the live
vehicle, and where the map should look.
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
from gtfs_backend.api.schemas import MapVehicle, PlatformRef, TripShape, TripStop, TripView
from gtfs_backend.api.services import alerts, vehicles
from gtfs_backend.api.services.routes import POLYLINE_SQL

ON_DEMAND = (2, 3)  # GTFS pickup/drop_off_type: phone the agency, coordinate with the driver

TRIP_SQL = f"""
    SELECT t.route_id, t.trip_headsign, t.direction_id, sh.*
    FROM gtfs.trips t
    LEFT JOIN LATERAL (
        SELECT {POLYLINE_SQL} FROM core.shape_line s WHERE s.feed_version_id = t.feed_version_id AND s.shape_id = t.shape_id
    ) sh ON true
    WHERE t.feed_version_id = %(fv)s AND t.trip_id = %(trip)s
      AND EXISTS (SELECT 1 FROM core.service_date sd
                  WHERE sd.feed_version_id = t.feed_version_id AND sd.date = %(date)s AND sd.service_id = t.service_id)
"""
STOPS_SQL = """
    SELECT st.stop_sequence, extract(epoch FROM st.arrival_time)::int, extract(epoch FROM st.departure_time)::int,
           st.pickup_type, st.drop_off_type,
           p.id, p.url_key, p.name, p.platform, ST_Y(p.geom), ST_X(p.geom), g.id, g.url_key
    FROM gtfs.stop_times st
    LEFT JOIN core.stop_link l ON l.feed_version_id = st.feed_version_id AND l.stop_id = st.stop_id
    LEFT JOIN core.stop_platform p ON p.id = l.stop_platform_id
    LEFT JOIN core.stop_group g ON g.id = p.stop_group_id
    WHERE st.feed_version_id = %(fv)s AND st.trip_id = %(trip)s
    ORDER BY st.stop_sequence
"""


def _stop(row, day: Day, update: TripUpdate | None, now: int) -> TripStop:
    seq, arr, dep, pickup, drop_off, pid, pkey, pname, pnum, lat, lon, gid, gkey = row
    sched_arr = day.base + arr if arr is not None else None
    sched_dep = day.base + dep if dep is not None else None
    live = update.stop(seq) if update else None
    if update is None:  # no realtime: passed once its time is over
        last = sched_dep or sched_arr
        passed = last is not None and last < now
    else:
        passed = update.passed(seq)
    return TripStop(
        stop_sequence=seq,
        platform=PlatformRef(id=pid, key=pkey, name=pname, platform=pnum) if pid else None,
        group_id=gid,
        group_key=gkey,
        name=pname,
        lat=lat,
        lon=lon,
        scheduled_arrival=sched_arr,
        scheduled_departure=sched_dep,
        estimated_arrival=live.predicted_arrival if live else None,
        estimated_departure=live.predicted_departure if live else None,
        delay_s=live.delay_s if live else None,
        passed=passed,
        skipped=bool(live and live.skipped),
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


async def view(ctx: Context, feed_id: str, service_date: date, trip_id: str, vehicle: MapVehicle | None = None) -> TripView:
    """`vehicle`: the vehicle the view was opened from; otherwise the one realtime says runs the trip, if live."""
    day = await ctx.catalog.day(feed_id, service_date)
    if day is None:
        raise NotFound("trip")
    params = {"fv": day.feed_version_id, "trip": trip_id, "date": service_date}
    key = keys.trip_key(feed_id, service_date, trip_id)
    async with ctx.pool.connection() as conn:
        trip = await (await conn.execute(TRIP_SQL, params)).fetchone()
        if trip is None:
            raise NotFound("trip")
        route_id, headsign, direction_id, polyline, bbox = trip
        rows = await (await conn.execute(STOPS_SQL, params)).fetchall()

        update = TripUpdate.for_version((await realtime.trip_updates(ctx.redis, [key])).get(key), day.version)
        now = ctx.now()
        stops = [_stop(r, day, update, now) for r in rows]

        if vehicle is None and update and update.vehicle_id:
            found = await vehicles.by_ids(ctx, [f"{feed_id}:{update.vehicle_id}"])
            vehicle = next((v for v in found if v.trip_key == key), None)  # it may have moved on to its next trip

        info = await ctx.catalog.route(day.feed_version_id, route_id)
        scope = alerts.Scope(
            routes={(feed_id, route_id): info.route_type if info else None},
            trips={(feed_id, trip_id)},
            platforms={s.platform.id for s in stops if s.platform},
            feeds={feed_id},
        )
        trip_alerts = await alerts.in_scope(ctx, conn, scope)

    shape = _shape(polyline, bbox, stops)
    current = next((i for i, s in enumerate(stops) if not s.passed), None)
    if update is not None and update.canceled:
        status = "canceled"
    elif update is not None:
        status = "live"
    else:
        status = "scheduled"
    return TripView(
        key=key,
        feed_id=feed_id,
        trip_id=trip_id,
        service_date=keys.service_date_str(service_date),
        route=info.model() if info else None,
        headsign=headsign,
        direction_id=direction_id,
        status=status,
        delay_s=update.delay_s if update else None,
        timestamp=now,
        vehicle=vehicle,
        shape=shape,
        stops=stops,
        current_index=current,
        alerts=trip_alerts,
    )


async def view_by_key(ctx: Context, trip_key: str) -> TripView:
    feed_id, service_date, trip_id = keys.parse_trip_key(trip_key)
    return await view(ctx, feed_id, service_date, trip_id)


async def for_vehicle(ctx: Context, vehicle_key: str) -> TripView:
    """The trip view of the trip the vehicle runs now."""
    feed_id, _ = keys.split_feed(vehicle_key, "vehicle key")
    raws = await realtime.vehicle_json(ctx.redis, [vehicle_key])
    if not raws:
        raise NotFound("vehicle")
    (vehicle,) = await vehicles.models(ctx, raws)
    trip = raws[0]["trip"]
    return await view(ctx, feed_id, date.fromisoformat(trip["service_date"]), trip["trip_id"], vehicle=vehicle)
