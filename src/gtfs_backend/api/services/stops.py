"""Logical stops (groups) and physical stops (platforms): lookups, map queries, search, directions and lines."""

from __future__ import annotations

from collections import Counter, defaultdict

from psycopg import AsyncConnection

from gtfs_backend.api.context import Context
from gtfs_backend.api.data.catalog import Catalog
from gtfs_backend.api.data.geo import Bbox, Point
from gtfs_backend.api.errors import NotFound
from gtfs_backend.api.schemas import (
    Direction,
    Line,
    PlatformInfo,
    PlatformRef,
    SearchResult,
    Stop,
    StopGroupInfo,
    StopList,
)
from gtfs_backend.ids import parse_url_key

MIN_DIRECTION_SHARE = 0.05  # rarer directions are short turns, depot runs etc.
MAX_RESULTS = 3000  # platforms per map request

# An active platform with no modes has no trips in any current feed version (see refresh_stop_state): never returned.
SERVED = "p.active AND cardinality(p.modes) > 0"

# Both select the columns stop_from_row reads.
GROUP_COLS = f"""
    'group', g.id, g.url_key, g.name, ST_Y(g.geom), ST_X(g.geom),
    coalesce((SELECT array_agg(DISTINCT m ORDER BY m) FROM core.stop_platform p, unnest(p.modes) m
              WHERE p.stop_group_id = g.id AND {SERVED}), '{{}}'),
    NULL, NULL
"""
PLATFORM_COLS = """
    'platform', p.id, p.url_key, p.name, ST_Y(p.geom), ST_X(p.geom), p.modes, p.stop_group_id, p.platform
"""


def stop_from_row(row) -> Stop:
    kind, sid, key, name, lat, lon, modes, group_id, platform = row[:9]
    return Stop(
        kind=kind, id=sid, key=key, name=name, lat=lat, lon=lon, modes=list(modes or []), group_id=group_id, platform=platform
    )


def platform_ref(stop: Stop) -> PlatformRef:
    return PlatformRef(id=stop.id, key=stop.key, name=stop.name, platform=stop.platform)


# ------------------------------------------------------------------------------------------------- lookups


async def find(conn: AsyncConnection, key: str) -> Stop | None:
    """A group or platform by key (`rondo-mogilskie-qphdu`) or bare id; the slug part is ignored."""
    sid = parse_url_key(key)
    if sid is None:
        return None
    cur = await conn.execute(
        f"SELECT {GROUP_COLS} FROM core.stop_group g WHERE g.id = %(id)s "
        f"UNION ALL SELECT {PLATFORM_COLS} FROM core.stop_platform p WHERE p.id = %(id)s",
        {"id": sid},
    )
    row = await cur.fetchone()
    return stop_from_row(row) if row else None


async def get(conn: AsyncConnection, key: str) -> Stop:
    stop = await find(conn, key)
    if stop is None:
        raise NotFound("stop")
    return stop


async def group_platforms(conn: AsyncConnection, group_ids: list[str]) -> dict[str, list[Stop]]:
    """Active platforms of each group, by platform number."""
    out: dict[str, list[Stop]] = {g: [] for g in group_ids}
    cur = await conn.execute(
        f"SELECT {PLATFORM_COLS} FROM core.stop_platform p WHERE p.stop_group_id = ANY(%s) AND {SERVED} "
        "ORDER BY p.platform NULLS LAST, p.id",
        (group_ids,),
    )
    for r in await cur.fetchall():
        out[r[7]].append(stop_from_row(r))
    return out


async def platforms_by_id(conn: AsyncConnection, ids) -> dict[str, Stop]:
    ids = list(ids)
    if not ids:
        return {}
    cur = await conn.execute(f"SELECT {PLATFORM_COLS} FROM core.stop_platform p WHERE p.id = ANY(%s)", (ids,))
    return {r[1]: stop_from_row(r) for r in await cur.fetchall()}


async def platform_ids(conn: AsyncConnection, stop: Stop) -> list[str]:
    """The platforms a stop covers: all of a group's (active or not, the timetables decide), or itself."""
    if stop.kind == "platform":
        return [stop.id]
    cur = await conn.execute("SELECT id FROM core.stop_platform WHERE stop_group_id = %s", (stop.id,))
    return [r[0] for r in await cur.fetchall()]


def route_sort_key(r: Line):
    """Modes together, then numeric names numerically (2 < 10 < 52 < 152 < 902 < N1)."""
    return (r.mode, not r.name.isdigit(), int(r.name) if r.name.isdigit() else 0, r.name)


def sorted_lines(lines) -> list[Line]:
    """One chip per line name: feeds may split a line into several route_ids (MLD's A67)."""
    return sorted({(x.feed_id, x.name): x for x in lines}.values(), key=route_sort_key)


async def _direction_rows(conn: AsyncConnection, catalog: Catalog, platforms: list[str]) -> list[tuple]:
    """(platform, next group id, its name, feed_version_id, route_id, headsign, trips) in today's timetables."""
    versions = sorted({d.feed_version_id for d in catalog.today_days()})
    if not platforms or not versions:
        return []
    cur = await conn.execute(
        """
        SELECT d.stop_platform_id, d.next_group_id, g.name, d.feed_version_id, d.route_id, d.headsign, d.trips
        FROM core.platform_direction d JOIN core.stop_group g ON g.id = d.next_group_id
        WHERE d.feed_version_id = ANY(%s) AND d.stop_platform_id = ANY(%s)
        """,
        (versions, platforms),
    )
    return await cur.fetchall()


async def directions(
    conn: AsyncConnection, catalog: Catalog, platforms: list[str], min_share: float = MIN_DIRECTION_SHARE
) -> tuple[dict[str, list[Direction]], dict[str, list[Line]]]:
    """Where trips from each platform go next in today's timetables (all feeds, so ZTP's shared platforms get
    trams and buses), most served first, and every line departing from each platform. Platforms without
    departures today get neither.
    """
    rows = await _direction_rows(conn, catalog, platforms)
    lines = await catalog.lines((fv, route) for _, _, _, fv, route, _, _ in rows)

    # platform -> next group -> [name, trips, lines, headsign counts]
    acc: dict[str, dict[str, list]] = defaultdict(dict)
    all_lines: dict[str, list[Line]] = defaultdict(list)
    for pid, gid, name, fv, route_id, headsign, trips in rows:
        d = acc[pid].setdefault(gid, [name, 0, [], Counter()])
        d[1] += trips
        if line := lines.get((fv, route_id)):
            d[2].append(line)
            all_lines[pid].append(line)
        if headsign:
            d[3][headsign] += trips

    out = {}
    for pid, by_group in acc.items():
        total = sum(d[1] for d in by_group.values())
        ranked = sorted(by_group.items(), key=lambda kv: (-kv[1][1], kv[1][0]))
        out[pid] = [
            Direction(
                towards=name,
                towards_id=gid,
                lines=sorted_lines(group_lines),
                headsigns=[h for h, _ in headsigns.most_common(3)],
            )
            for i, (gid, (name, trips, group_lines, headsigns)) in enumerate(ranked)
            if i == 0 or trips >= min_share * total
        ]
    return out, {pid: sorted_lines(ls) for pid, ls in all_lines.items()}


async def group_info(ctx: Context, conn: AsyncConnection, groups: list[Stop]) -> list[StopGroupInfo]:
    """Groups with their active platforms, each platform's directions, and the lines departing from the group."""
    by_group = await group_platforms(conn, [g.id for g in groups])
    platforms = [p.id for ps in by_group.values() for p in ps]
    dirs, lines = await directions(conn, ctx.catalog, platforms, MIN_DIRECTION_SHARE)
    return [
        StopGroupInfo(
            **g.model_dump(),
            platforms=[PlatformInfo(**p.model_dump(), directions=dirs.get(p.id, [])) for p in by_group[g.id]],
            lines=sorted_lines(line for p in by_group[g.id] for line in lines.get(p.id, [])),
        )
        for g in groups
    ]


async def served_routes(conn: AsyncConnection, catalog: Catalog, platforms: list[str]) -> set[tuple[str, str]]:
    """(feed_version_id, route_id) of the routes departing from these platforms today."""
    return {(fv, route) for _, _, _, fv, route, _, _ in await _direction_rows(conn, catalog, platforms)}


# ------------------------------------------------------------------------------------------------- endpoints


async def in_bbox(ctx: Context, bbox: Bbox, modes: set[str], kind: str) -> StopList:
    """Active platforms (or groups, `kind="group"`) inside the box, at most MAX_RESULTS. With `modes`, only stops
    served by any of them; a group is placed (and so found) at its centroid."""
    limit = MAX_RESULTS
    envelope = "ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)"
    has_mode = "(cardinality(%(modes)s::text[]) = 0 OR p.modes && %(modes)s::text[])"
    if kind == "group":
        query = (
            f"SELECT {GROUP_COLS} FROM core.stop_group g WHERE g.active AND g.geom && {envelope} "
            f"AND EXISTS (SELECT 1 FROM core.stop_platform p WHERE p.stop_group_id = g.id AND {SERVED} AND {has_mode}) "
            "ORDER BY g.id LIMIT %(limit)s"
        )
    else:
        query = (
            f"SELECT {PLATFORM_COLS} FROM core.stop_platform p WHERE {SERVED} AND {has_mode} "
            f"AND p.geom && {envelope} ORDER BY p.id LIMIT %(limit)s"
        )
    params = {
        "min_lon": bbox.min_lon,
        "min_lat": bbox.min_lat,
        "max_lon": bbox.max_lon,
        "max_lat": bbox.max_lat,
        "modes": sorted(modes),
        "limit": limit + 1,
    }
    async with ctx.pool.connection() as conn:
        cur = await conn.execute(query, params)
        rows = await cur.fetchall()
    return StopList(truncated=len(rows) > limit, stops=[stop_from_row(r) for r in rows[:limit]])


async def search(ctx: Context, query: str, near: Point | None, limit: int) -> SearchResult:
    """Logical stops by name: prefix matches first, then substring, then fuzzy (trigram); accents and case ignored.
    Only stops of feeds.yml `search_namespaces` (ZTP) are found, including other feeds' stops merged into them."""
    async with ctx.pool.connection() as conn:
        cur = await conn.execute(
            f"""
            WITH u AS (SELECT core.immutable_unaccent(lower(%(q)s)) AS q),
            x AS (SELECT q, replace(replace(replace(q, '\\', '\\\\'), '%%', '\\%%'), '_', '\\_') AS esc FROM u)
            SELECT {GROUP_COLS},
                   CASE WHEN %(lon)s::float8 IS NOT NULL
                        THEN ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography)
                   END AS dist
            FROM core.stop_group g, x
            WHERE g.active AND g.code_namespace = ANY(%(ns)s)
              AND EXISTS (SELECT 1 FROM core.stop_platform p WHERE p.stop_group_id = g.id AND {SERVED})
              AND (x.q <%% g.name_search OR g.name_search LIKE '%%' || x.esc || '%%')
            ORDER BY g.name_search LIKE x.esc || '%%' DESC,
                     g.name_search LIKE '%%' || x.esc || '%%' DESC,
                     word_similarity(x.q, g.name_search) DESC,
                     similarity(x.q, g.name_search) DESC,
                     dist NULLS LAST, g.name
            LIMIT %(limit)s
            """,
            {
                "q": query.strip(),
                "lon": near.lon if near else None,
                "lat": near.lat if near else None,
                "limit": limit,
                "ns": ctx.feeds.searchable_namespaces,
            },
        )
        rows = await cur.fetchall()
        results = await group_info(ctx, conn, [stop_from_row(r) for r in rows])
    for info, r in zip(results, rows, strict=True):
        info.distance_m = round(r[9], 1) if r[9] is not None else None
    return SearchResult(query=query, results=results)
