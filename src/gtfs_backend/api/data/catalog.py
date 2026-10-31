"""Small static data the API needs on every request, cached in memory.

Per feed: timezone, day start, and the authoritative version of yesterday/today/tomorrow (with the epoch its GTFS
times count from). Routes (with agency) are cached per feed version, loaded on first use. Everything is
reloaded when the local date changes, when the worker publishes `static:updated`, and every CATALOG_REFRESH_INTERVAL (5 minutes).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from psycopg_pool import AsyncConnectionPool

from gtfs_backend.api.schemas import Line, Route
from gtfs_backend.feeds import FeedsFile
from gtfs_backend.keys import route_key
from gtfs_backend.modes import MODE_SQL
from gtfs_backend.servicetime import DayStart, day_base, local_date, nearby_dates

REFRESH_S = 300

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
    long_name: str | None
    route_type: int
    mode: str
    agency: dict | None  # {name, url, phone} as given in agency.txt

    @property
    def key(self) -> str:
        return route_key(self.feed_id, self.route_id)

    def model(self) -> Route:
        return Route(
            key=self.key,
            feed_id=self.feed_id,
            route_id=self.route_id,
            short_name=self.short_name,
            long_name=self.long_name,
            route_type=self.route_type,
            mode=self.mode,
            agency=self.agency,
        )

    def line(self) -> Line:
        return Line(
            key=self.key,
            feed_id=self.feed_id,
            route_id=self.route_id,
            name=self.short_name or self.route_id,
            mode=self.mode,
        )


@dataclass
class FeedInfo:
    feed_id: str
    tz: ZoneInfo
    day_start: DayStart
    days: list[Day] = field(default_factory=list)


class Catalog:
    def __init__(self, pool: AsyncConnectionPool, feeds: FeedsFile, clock: Clock = time.time, refresh_s: float = REFRESH_S):
        self.pool = pool
        self.refresh_s = refresh_s
        self.config = {f.feed_id: f for f in feeds.enabled}
        self.clock = clock
        self.feeds: dict[str, FeedInfo] = {}
        self._routes: dict[str, dict[str, RouteInfo]] = {}  # feed_version_id -> route_id -> info
        self._loaded_at = 0.0
        self._loaded_for: dict[str, date] = {}
        self._dirty = True
        self._lock = asyncio.Lock()
        self.fingerprint = ""  # changes with the versions loaded (new import, new day); part of response cache keys

    def invalidate(self) -> None:
        self._dirty = True

    def _stale(self, now: float) -> bool:
        if self._dirty or now - self._loaded_at > self.refresh_s:
            return True
        return any(local_date(now, f.tz) != self._loaded_for.get(f.feed_id) for f in self.feeds.values())

    async def ensure_fresh(self) -> None:
        now = self.clock()
        if not self._stale(now):
            return
        async with self._lock:
            if self._stale(now):
                await self._load(now)

    async def _load(self, now: float) -> None:
        feeds: dict[str, FeedInfo] = {}
        loaded_for: dict[str, date] = {}
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT DISTINCT ON (feed_id) feed_id, timezone FROM core.feed_version
                WHERE status = 'ready' AND feed_id = ANY(%s) ORDER BY feed_id, version DESC
                """,
                (list(self.config),),
            )
            for feed_id, tz_name in await cur.fetchall():
                feeds[feed_id] = FeedInfo(feed_id, ZoneInfo(tz_name or "UTC"), self.config[feed_id].day_start)
            for info in feeds.values():
                dates = nearby_dates(now, info.tz)
                loaded_for[info.feed_id] = dates[1]
                cur = await conn.execute(
                    """
                    SELECT s.service_date, fv.id, fv.version FROM core.service_day_version s
                    JOIN core.feed_version fv ON fv.id = s.feed_version_id
                    WHERE s.feed_id = %s AND s.service_date = ANY(%s) ORDER BY s.service_date
                    """,
                    (info.feed_id, dates),
                )
                info.days = [
                    Day(info.feed_id, d, fv_id, v, day_base(d, info.tz, info.day_start)) for d, fv_id, v in await cur.fetchall()
                ]
        self.feeds = feeds
        self._loaded_for = loaded_for
        versions = sorted(f"{d.feed_id}/{d.date}/{d.feed_version_id}" for info in feeds.values() for d in info.days)
        self.fingerprint = hashlib.blake2b("|".join(versions).encode(), digest_size=6).hexdigest()
        self._routes.clear()  # versions may have been retired
        self._loaded_at = now
        self._dirty = False

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
        """The authoritative version of any service date (cached ones first)."""
        info = self.feeds.get(feed_id)
        if info is None:
            return None
        for day in info.days:
            if day.date == d:
                return day
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
        missing = [v for v in set(feed_version_ids) if v not in self._routes]
        if missing:
            async with self.pool.connection() as conn:
                cur = await conn.execute(
                    f"""
                    SELECT r.feed_version_id, fv.feed_id, r.route_id, r.route_short_name, r.route_long_name, r.route_type,
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
                    long,
                    rtype,
                    mode,
                    a_name,
                    a_url,
                    a_phone,
                ) in await cur.fetchall():
                    agency = {"name": a_name, "url": a_url, "phone": a_phone} if a_name is not None else None
                    loaded[fv_id][route_id] = RouteInfo(feed_id, route_id, short, long, rtype, mode, agency)
            self._routes.update(loaded)
        return {(v, rid): info for v in set(feed_version_ids) for rid, info in self._routes.get(v, {}).items()}

    async def route(self, feed_version_id: str, route_id: str) -> RouteInfo | None:
        return (await self.routes([feed_version_id])).get((feed_version_id, route_id))

    async def route_models(self, keys) -> dict[tuple[str, str], Route]:
        """(feed_version_id, route_id) -> API route, for the keys that exist."""
        keys = set(keys)
        routes = await self.routes({fv for fv, _ in keys})
        return {k: routes[k].model() for k in keys if k in routes}

    async def lines(self, keys) -> dict[tuple[str, str], Line]:
        """(feed_version_id, route_id) -> compact line, for the keys that exist."""
        keys = set(keys)
        routes = await self.routes({fv for fv, _ in keys})
        return {k: routes[k].line() for k in keys if k in routes}
