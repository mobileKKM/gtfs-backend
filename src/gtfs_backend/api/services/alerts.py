"""Service alerts from Redis, passed through as the feeds give them, and matching them to a board/trip/route/stop.

GTFS-RT: the fields of one informed entity are combined with AND (route 52 *at* stop X), an alert applies if any
of its entities does. A scope says what is being looked at; dimensions it leaves as None aren't checked.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import AsyncConnection

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.errors import InvalidInput, NotFound
from gtfs_backend.api.schemas import Alert, AlertList, AlertRef
from gtfs_backend.api.services import stops


@dataclass
class Scope:
    routes: dict[tuple[str, str], int | None] | None = None  # (feed_id, route_id) -> route_type
    platforms: set[str] | None = None
    trips: set[tuple[str, str]] | None = None  # (feed_id, trip_id)
    feeds: set[str] | None = None  # feed-wide alerts (agency / no selector) of these feeds apply


def entity_matches(feed_id: str, ie: dict, scope: Scope) -> bool:
    hit = False
    if ie.get("route_id") is not None and scope.routes is not None:
        if (feed_id, ie["route_id"]) not in scope.routes:
            return False
        hit = True
    if ie.get("trip_id") is not None and scope.trips is not None:
        if (feed_id, ie["trip_id"]) not in scope.trips:
            return False
        hit = True
    if ie.get("stop_id") is not None and scope.platforms is not None:
        if ie.get("stop_platform_id") not in scope.platforms:
            return False
        hit = True
    if ie.get("route_type") is not None and scope.routes is not None:
        if ie["route_type"] not in {t for (f, _), t in scope.routes.items() if f == feed_id}:
            return False
        hit = True
    if hit:
        return True
    feed_wide = all(ie.get(k) is None for k in ("route_id", "trip_id", "stop_id", "route_type"))
    return feed_wide and scope.feeds is not None and feed_id in scope.feeds


# boards and trip views show alerts active now or starting within this (GET /alerts?active=true too)
UPCOMING_S = 24 * 3600


def is_current(alert: dict, now: int, upcoming: int) -> bool:
    """Active now, or starting within `upcoming` seconds. No active periods: always active (GTFS-RT); a period's
    missing start or end is open."""
    periods = alert.get("active_periods") or []
    if not periods:
        return True
    return any(
        (p.get("start") is None or p["start"] <= now + upcoming) and (p.get("end") is None or p["end"] >= now) for p in periods
    )


def current(ctx: Context, raws: list[dict]) -> list[dict]:
    now = ctx.now()
    return [a for a in raws if is_current(a, now, UPCOMING_S)]


def matches(alert: dict, scope: Scope) -> bool:
    entities = alert.get("informed_entities") or [{}]
    return any(entity_matches(alert["feed_id"], ie, scope) for ie in entities)


async def to_models(ctx: Context, conn: AsyncConnection, raws: list[dict]) -> list[Alert]:
    """Add the routes (today's timetable) and stops named by the informed entities; the alert itself stays as given."""

    def entities(a: dict) -> list[dict]:
        return a.get("informed_entities") or []

    def route_keys(a: dict) -> list[tuple[str | None, str]]:
        version = ctx.catalog.today_version(a["feed_id"])
        return list(dict.fromkeys((version, ie["route_id"]) for ie in entities(a) if ie.get("route_id")))

    def platform_ids(a: dict) -> list[str]:
        return list(dict.fromkeys(ie["stop_platform_id"] for ie in entities(a) if ie.get("stop_platform_id")))

    routes = await ctx.catalog.route_models(k for a in raws for k in route_keys(a))
    platforms = await stops.platforms_by_id(conn, {p for a in raws for p in platform_ids(a)})
    return [
        Alert(
            key=keys.alert_key(a["feed_id"], a["id"]),
            **a,
            routes=[routes[k] for k in route_keys(a) if k in routes],
            stops=[platforms[p] for p in platform_ids(a) if p in platforms],
        )
        for a in raws
    ]


def ref(a: dict) -> AlertRef:
    return AlertRef(
        key=keys.alert_key(a["feed_id"], a["id"]),
        cause=a.get("cause"),
        effect=a.get("effect"),
        header=a.get("header"),
        active_periods=a.get("active_periods") or [],
    )


async def in_scope(ctx: Context, scope: Scope) -> list[AlertRef]:
    """The current alerts (active now or soon) that apply to what is being looked at, as boards and trips carry them."""
    raws = current(ctx, await realtime.alerts_json(ctx.redis, ctx.feed_ids))
    return [ref(a) for a in raws if matches(a, scope)]


# ------------------------------------------------------------------------------------------------- endpoints


async def search(ctx: Context, feed: str | None, route_key: str | None, stop_key: str | None, active: bool = False) -> AlertList:
    """All alerts, or the ones of a feed / about a route / about a stop's platforms (combined with AND); `active`:
    only those active now or starting within UPCOMING_S."""
    route_id = None
    if route_key is not None:
        route_feed, route_id = keys.split_feed(route_key, "route key")
        if feed is not None and feed != route_feed:
            raise InvalidInput("route is of another feed")
        feed = route_feed
    if feed is not None and feed not in ctx.feed_ids:
        raise NotFound("feed")
    async with ctx.pool.connection() as conn:
        raws = await realtime.alerts_json(ctx.redis, [feed] if feed else ctx.feed_ids)
        if active:
            raws = current(ctx, raws)
        if route_id is not None or stop_key is not None:
            scope = Scope()
            if route_id is not None:
                version = ctx.catalog.today_version(feed)
                info = await ctx.catalog.route(version, route_id) if version else None
                scope.routes = {(feed, route_id): info.route_type if info else None}
                scope.feeds = {feed}
            if stop_key is not None:
                scope.platforms = set(await stops.platform_ids(conn, await stops.get(conn, stop_key)))
            raws = [a for a in raws if matches(a, scope)]
        return AlertList(alerts=await to_models(ctx, conn, raws))


async def get(ctx: Context, alert_key: str) -> Alert:
    feed_id, alert_id = keys.split_feed(alert_key, "alert key")
    if feed_id not in ctx.feed_ids:
        raise NotFound("alert")
    raw = next((a for a in await realtime.alerts_json(ctx.redis, [feed_id]) if a["id"] == alert_id), None)
    if raw is None:
        raise NotFound("alert")
    async with ctx.pool.connection() as conn:
        return (await to_models(ctx, conn, [raw]))[0]
