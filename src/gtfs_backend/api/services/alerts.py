"""Service alerts from Redis, with what they apply to in our terms, and matching them to a board/trip/route/stop.

The worker stores each informed entity resolved (trip key, our public stop ids). GTFS-RT: the fields of one entity
are combined with AND (route 52 *at* stop X), an alert applies if any of its entities does. A scope says what is being
looked at; dimensions it leaves as None aren't checked.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import AsyncConnection

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.errors import InvalidInput, NotFound
from gtfs_backend.api.schemas import Alert, AlertEntity, AlertList, AlertRef
from gtfs_backend.api.services import stops
from gtfs_backend.modes import mode_of


@dataclass
class Scope:
    routes: dict[tuple[str, str], int | None] | None = None  # (feed_id, route_id) -> route_type
    directions: set[tuple[str, str, int]] | None = None  # (feed_id, route_id, direction_id) running here
    public_stop_ids: set[str] | None = None
    trips: set[str] | None = None  # trip keys
    feeds: set[str] | None = None  # feed-wide alerts (agency / no selector) of these feeds apply


def entity_matches(feed_id: str, ie: dict, scope: Scope) -> bool:
    hit = False
    route_id = ie.get("route_id")
    if route_id is not None and scope.routes is not None:
        if (feed_id, route_id) not in scope.routes:
            return False
        direction = ie.get("direction_id")
        if direction is not None and scope.directions is not None and (feed_id, route_id, direction) not in scope.directions:
            return False
        hit = True
    if ie.get("trip_key") is not None and scope.trips is not None:
        if ie["trip_key"] not in scope.trips:
            return False
        hit = True
    if ie.get("public_stop_ids") and scope.public_stop_ids is not None:
        if scope.public_stop_ids.isdisjoint(ie["public_stop_ids"]):
            return False
        hit = True
    if ie.get("route_type") is not None and scope.routes is not None:
        if ie["route_type"] not in {t for (f, _), t in scope.routes.items() if f == feed_id}:
            return False
        hit = True
    if hit:
        return True
    feed_wide = not any(ie.get(k) for k in ("route_id", "trip_key", "public_stop_ids")) and ie.get("route_type") is None
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
    return any(entity_matches(alert["feed_id"], ie, scope) for ie in alert.get("entities") or [])


SEVERITY_ORDER = {"severe": 0, "warning": 1, "info": 2}


def _order(a: dict) -> tuple[int, int]:
    """Severe first, then the ones that started (or start) earliest."""
    starts = [p.get("start") or 0 for p in a.get("active_periods") or []]
    return SEVERITY_ORDER.get(a.get("severity"), len(SEVERITY_ORDER)), min(starts, default=0)


def ref(a: dict) -> AlertRef:
    return AlertRef(
        key=keys.alert_key(a["feed_id"], a["id"]),
        cause=a.get("cause"),
        effect=a.get("effect"),
        severity=a.get("severity"),
        header=a.get("header"),
        description=a.get("description"),
        active_periods=a.get("active_periods") or [],
    )


async def to_models(ctx: Context, conn: AsyncConnection, raws: list[dict]) -> list[Alert]:
    """Each alert with its entities as routes (today's timetable), trips and stops or stop groups. An entity whose route
    or stop we can't show is left out: without it, it would read as applying more widely than it does."""
    raws = sorted(raws, key=_order)

    def entities(a: dict) -> list[dict]:
        return a.get("entities") or []

    def route_key(a: dict, ie: dict) -> tuple[str | None, str] | None:
        return (ctx.catalog.today_version(a["feed_id"]), ie["route_id"]) if ie.get("route_id") else None

    routes = await ctx.catalog.route_refs(k for a in raws for ie in entities(a) if (k := route_key(a, ie)))
    named = [ie for a in raws for ie in entities(a)]
    groups = await stops.groups_by_id(conn, {ie["stop_group_id"] for ie in named if ie.get("stop_group_id")})
    poles = await stops.stops_by_id(
        conn, {ie["public_stop_ids"][0] for ie in named if ie.get("public_stop_ids") and not ie.get("stop_group_id")}
    )

    def entity(a: dict, ie: dict) -> AlertEntity | None:
        route = stop = None
        if (k := route_key(a, ie)) is not None and (route := routes.get(k)) is None:
            return None
        if ie.get("stop_group_id"):
            stop = groups.get(ie["stop_group_id"])
        elif ie.get("public_stop_ids"):
            stop = poles.get(ie["public_stop_ids"][0])
        if ie.get("public_stop_ids") and stop is None:
            return None
        return AlertEntity(
            mode=mode_of(ie["route_type"]) if ie.get("route_type") is not None else None,
            route=route,
            direction_id=ie.get("direction_id"),
            trip_key=ie.get("trip_key"),
            stop=stop,
        )

    return [
        Alert(
            **ref(a).model_dump(),
            url=a.get("url"),
            entities=[e for ie in entities(a) if (e := entity(a, ie)) is not None],
        )
        for a in raws
    ]


async def in_scope(ctx: Context, scope: Scope) -> list[AlertRef]:
    """The current alerts (active now or soon) that apply to what is being looked at, as boards and trips carry them."""
    raws = current(ctx, await realtime.alerts_json(ctx.redis, ctx.feed_ids))
    return [ref(a) for a in sorted(raws, key=_order) if matches(a, scope)]


# ------------------------------------------------------------------------------------------------- endpoints


async def search(ctx: Context, feed: str | None, route_key: str | None, stop_key: str | None, active: bool = False) -> AlertList:
    """All alerts, or the ones of a feed / about a route / about a stop or its group's stops (combined with AND); `active`:
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
                scope.public_stop_ids = set(await stops.public_stop_ids(conn, await stops.get(conn, stop_key)))
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
