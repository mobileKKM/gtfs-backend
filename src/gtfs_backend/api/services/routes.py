"""Route shapes as encoded polylines (precision 5), from core.shape_line."""

from __future__ import annotations

from datetime import date

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.errors import NotFound
from gtfs_backend.api.schemas import RouteShape, RouteShapes

POLYLINE_SQL = "ST_AsEncodedPolyline(s.geom, 5), ARRAY[ST_XMin(s.geom), ST_YMin(s.geom), ST_XMax(s.geom), ST_YMax(s.geom)]"


async def shapes(ctx: Context, route_key: str, service_date: date | None) -> RouteShapes:
    """Every shape the route runs on a service day (default today), most used first per direction."""
    feed_id, route_id = keys.split_feed(route_key, "route key")
    d = service_date or ctx.catalog.today(feed_id)
    day = await ctx.catalog.day(feed_id, d) if d else None
    info = await ctx.catalog.route(day.feed_version_id, route_id) if day else None
    if day is None or info is None:
        raise NotFound("route")
    async with ctx.pool.connection() as conn:
        cur = await conn.execute(
            f"""
            SELECT t.shape_id, t.direction_id, mode() WITHIN GROUP (ORDER BY t.trip_headsign), count(*), {POLYLINE_SQL}
            FROM gtfs.trips t
            JOIN core.service_date sd
                 ON sd.feed_version_id = t.feed_version_id AND sd.date = %(d)s AND sd.service_id = t.service_id
            JOIN core.shape_line s ON s.feed_version_id = t.feed_version_id AND s.shape_id = t.shape_id
            WHERE t.feed_version_id = %(fv)s AND t.route_id = %(route)s
            GROUP BY t.shape_id, t.direction_id, s.geom
            ORDER BY t.direction_id NULLS LAST, count(*) DESC, t.shape_id
            """,
            {"d": d, "fv": day.feed_version_id, "route": route_id},
        )
        shapes = [
            RouteShape(direction_id=direction, headsign=headsign, trips=n, polyline=polyline, bbox=bbox)
            for _, direction, headsign, n, polyline, bbox in await cur.fetchall()
        ]
    return RouteShapes(route=info.model(), service_date=keys.service_date_str(d), shapes=shapes)
