"""Small static data the API needs on every request, cached in memory.

Stops, stop groups and today's directions (stopdata.py). Per feed: timezone, day start, and the authoritative
version of yesterday, today and the next PRELOAD_DAYS (with the epoch its GTFS times count from), so boards paging
into the coming week need no query; other dates are looked up when asked for. Routes (with agency) are kept per feed
version, loaded on first use. Reloaded in the background when the local date changes, when the worker publishes
`static:updated`, and every CATALOG_REFRESH_INTERVAL (5 minutes).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from psycopg_pool import AsyncConnectionPool

from gtfs_backend.api.data.stopdata import StopData, load_stop_data
from gtfs_backend.api.schemas import Route, RouteRef
from gtfs_backend.feeds import FeedsFile
from gtfs_backend.keys import route_key
from gtfs_backend.logs import log
from gtfs_backend.modes import MODE_SQL, Mode
from gtfs_backend.servicetime import DayStart, day_base, local_date

REFRESH_S = 300
PRELOAD_DAYS = 7  # service days loaded ahead of today
RETRY_S = 10  # after a failed background refresh

Clock = Callable[[], float]


@dataclass(frozen=True)
class Day:
    feed_id: str
    date: date
    feed_version_id: str
    version: int
    base: int  # epoch the day's GTFS times count from


@dataclass(frozen=True)
class RouteInfo:
    feed_id: str
    route_id: str
    short_name: str | None
    route_type: int
    mode: Mode
    agency: dict | None  # {name, url, phone} as given in agency.txt

    @property
    def key(self) -> str:
        return route_key(self.feed_id, self.route_id)

    @property
    def name(self) -> str:
        return self.short_name or self.route_id

    def model(self) -> Route:
        return Route(
            key=self.key,
            name=self.name,
            mode=self.mode,
            route_type=self.route_type,
            agency=self.agency,
        )

    def ref(self) -> RouteRef:
        return RouteRef(
            key=self.key,
            name=self.name,
            mode=self.mode,
        )


@dataclass
class FeedInfo:
    feed_id: str
    tz: ZoneInfo
    day_start: DayStart
    days: list[Day] = field(default_factory=list)
    covered: frozenset[date] = frozenset()  # the dates `days` was loaded for (a date without a Day has no service)


def preload_dates(epoch: float, tz: ZoneInfo, ahead: int = PRELOAD_DAYS) -> list[date]:
    """Yesterday (its night trips still run), today and the next `ahead` days."""
    today = local_date(epoch, tz)
    return [today + timedelta(days=i) for i in range(-1, ahead + 1)]


class Catalog:
    """Loaded once before the first request; after that, refreshes run in the background while requests keep using
    what is loaded (a slow or unreachable database never holds a request up). A failed refresh is retried after
    RETRY_S. Feed versions never change once ready, so routes are kept until their version is no longer ready."""

    def __init__(self, pool: AsyncConnectionPool, feeds: FeedsFile, clock: Clock = time.time, refresh_s: float = REFRESH_S):
        self.pool = pool
        self.refresh_s = refresh_s
        self.config = {f.feed_id: f for f in feeds.enabled}
        self.clock = clock
        self.feeds: dict[str, FeedInfo] = {}
        self._routes: dict[str, dict[str, RouteInfo]] = {}  # feed_version_id -> route_id -> info
        self._routes_lock = asyncio.Lock()  # concurrent misses load a version once
        self._loaded_at = 0.0
        self._loaded_for: dict[str, date] = {}
        # invalidate() bumps _generation; a load records the one it started at, so one arriving mid-load isn't lost
        self._generation = 0
        self._loaded_generation = -1  # -1: never loaded
        self._lock = asyncio.Lock()
        self._refresh: asyncio.Task | None = None
        self._retry_at = 0.0  # time.monotonic()
        self.stops = StopData.empty()  # stops, groups, today's directions (api/data/stopdata.py)

    @property
    def loaded(self) -> bool:
        return self._loaded_generation >= 0

    def invalidate(self) -> None:
        self._generation += 1

    def _stale(self, now: float) -> bool:
        if self._loaded_generation != self._generation or now - self._loaded_at > self.refresh_s:
            return True
        return any(local_date(now, f.tz) != self._loaded_for.get(f.feed_id) for f in self.feeds.values())

    async def ensure_fresh(self) -> None:
        """Waits only for the first load (raising if the database is down); later ones run in the background."""
        now = self.clock()
        if not self._stale(now):
            return
        if not self.loaded:
            async with self._lock:
                if not self.loaded:
                    await self._load(now)
            return
        self._refresh_soon()

    def _refresh_soon(self) -> None:
        if self._refresh is not None and not self._refresh.done():
            return
        if time.monotonic() < self._retry_at:
            return
        self._refresh = asyncio.create_task(self._refresh_now())

    async def _refresh_now(self) -> None:
        try:
            async with self._lock:
                await self._load(self.clock())
        except Exception as exc:  # noqa: BLE001
            self._retry_at = time.monotonic() + RETRY_S
            log.warning("catalog_refresh_failed", error=str(exc))

    async def reload(self) -> None:
        """Load now and wait for it (as `static:updated` does in the background)."""
        self.invalidate()
        async with self._lock:
            await self._load(self.clock())

    async def close(self) -> None:
        if self._refresh is not None:
            self._refresh.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresh

    async def _load(self, now: float) -> None:
        generation = self._generation
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT id, feed_id, timezone FROM core.feed_version WHERE status = 'ready' AND feed_id = ANY(%s) "
                "ORDER BY feed_id, version",
                (list(self.config),),
            )
            ready: set[str] = set()
            tz_names: dict[str, str | None] = {}
            for fv_id, feed_id, tz_name in await cur.fetchall():
                ready.add(fv_id)
                tz_names[feed_id] = tz_name  # by version: the newest one's
            feeds = {f: FeedInfo(f, ZoneInfo(tz or "UTC"), self.config[f].day_start) for f, tz in tz_names.items()}
            wanted = {f: preload_dates(now, info.tz) for f, info in feeds.items()}
            if feeds:
                cur = await conn.execute(
                    """
                    SELECT s.feed_id, s.service_date, fv.id, fv.version FROM core.service_day_version s
                    JOIN core.feed_version fv ON fv.id = s.feed_version_id
                    WHERE s.feed_id = ANY(%s) AND s.service_date BETWEEN %s AND %s ORDER BY s.feed_id, s.service_date
                    """,
                    (list(feeds), min(min(d) for d in wanted.values()), max(max(d) for d in wanted.values())),
                )
                for feed_id, d, fv_id, v in await cur.fetchall():
                    info = feeds[feed_id]
                    if d in wanted[feed_id]:
                        info.days.append(Day(feed_id, d, fv_id, v, day_base(d, info.tz, info.day_start)))
            dates = {f: local_date(now, info.tz) for f, info in feeds.items()}
            # today's (version, date) per feed: what the stops' hourly profiles count
            today_days = [d for info in feeds.values() for d in info.days if d.date == dates[d.feed_id]]
            today = tuple(sorted((d.feed_version_id, d.date) for d in today_days))
            stops = await load_stop_data(conn, today, self.stops)
        for feed_id, info in feeds.items():
            info.covered = frozenset(wanted[feed_id])
        self.feeds = feeds
        self.stops = stops
        self._loaded_for = {f: local_date(now, info.tz) for f, info in feeds.items()}
        self._routes = {v: routes for v, routes in self._routes.items() if v in ready}  # retired ones go
        # the routes of every version a preloaded day uses, so no request waits for them
        if missing := sorted({d.feed_version_id for info in feeds.values() for d in info.days} - set(self._routes)):
            async with self._routes_lock:
                await self._load_routes(missing)
        self._loaded_at = now
        self._loaded_generation = generation

    # ------------------------------------------------------------------------------------------- days

    def today(self, feed_id: str) -> date | None:
        info = self.feeds.get(feed_id)
        return local_date(self.clock(), info.tz) if info else None

    def today_days(self) -> list[Day]:
        return [d for info in self.feeds.values() for d in info.days if d.date == local_date(self.clock(), info.tz)]

    def today_version(self, feed_id: str) -> str | None:
        """The feed version serving today."""
        return next((d.feed_version_id for d in self.today_days() if d.feed_id == feed_id), None)

    async def day(self, feed_id: str, d: date) -> Day | None:
        """The authoritative version of any service date: preloaded ones without a query (most), others with one."""
        info = self.feeds.get(feed_id)
        if info is None:
            return None
        for day in info.days:
            if day.date == d:
                return day
        if d in info.covered:
            return None
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT fv.id, fv.version FROM core.service_day_version s
                JOIN core.feed_version fv ON fv.id = s.feed_version_id
                WHERE s.feed_id = %s AND s.service_date = %s
                """,
                (feed_id, d),
            )
            row = await cur.fetchone()
        return Day(feed_id, d, row[0], row[1], day_base(d, info.tz, info.day_start)) if row else None

    async def days_between(self, start: float, end: float) -> list[Day]:
        """Service days whose trips can run between two epochs: from the day before `start` to the day of `end`."""
        out = []
        for info in self.feeds.values():
            d, last = local_date(start, info.tz) - timedelta(days=1), local_date(end, info.tz)
            while d <= last:
                if day := await self.day(info.feed_id, d):
                    out.append(day)
                d += timedelta(days=1)
        return out

    # ------------------------------------------------------------------------------------------- routes

    async def routes(self, feed_version_ids: set[str] | list[str]) -> dict[tuple[str, str], RouteInfo]:
        """(feed_version_id, route_id) -> route, for the given versions."""
        wanted = set(feed_version_ids)
        if any(v not in self._routes for v in wanted):
            async with self._routes_lock:
                if missing := [v for v in wanted if v not in self._routes]:
                    await self._load_routes(missing)
        return {(v, rid): info for v in wanted for rid, info in self._routes.get(v, {}).items()}

    async def _load_routes(self, missing: list[str]) -> None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"""
                SELECT r.feed_version_id, fv.feed_id, r.route_id, r.route_short_name, r.route_type,
                       {MODE_SQL} AS mode, a.agency_name, a.agency_url, a.agency_phone
                FROM gtfs.routes r
                JOIN core.feed_version fv ON fv.id = r.feed_version_id
                LEFT JOIN LATERAL (
                    SELECT agency_name, agency_url, agency_phone FROM gtfs.agency a
                    WHERE a.feed_version_id = r.feed_version_id AND (a.agency_id = r.agency_id OR r.agency_id IS NULL)
                    ORDER BY a.agency_id = r.agency_id DESC NULLS LAST LIMIT 1
                ) a ON true
                WHERE r.feed_version_id = ANY(%s)
                """,
                (missing,),
            )
            loaded: dict[str, dict[str, RouteInfo]] = {v: {} for v in missing}
            for (
                fv_id,
                feed_id,
                route_id,
                short,
                rtype,
                mode,
                a_name,
                a_url,
                a_phone,
            ) in await cur.fetchall():
                agency = {"name": a_name, "url": a_url, "phone": a_phone} if a_name is not None else None
                loaded[fv_id][route_id] = RouteInfo(feed_id, route_id, short, rtype, mode, agency)
        self._routes.update(loaded)

    async def route(self, feed_version_id: str, route_id: str) -> RouteInfo | None:
        return (await self.routes([feed_version_id])).get((feed_version_id, route_id))

    async def route_models(self, keys) -> dict[tuple[str, str], Route]:
        """(feed_version_id, route_id) -> API route, for the keys that exist."""
        keys = set(keys)
        routes = await self.routes({fv for fv, _ in keys})
        return {k: routes[k].model() for k in keys if k in routes}

    async def route_refs(self, keys) -> dict[tuple[str, str], RouteRef]:
        """(feed_version_id, route_id) -> compact route, for the keys that exist."""
        keys = set(keys)
        routes = await self.routes({fv for fv, _ in keys})
        return {k: routes[k].ref() for k in keys if k in routes}
