"""Stop groups (logical) and stops (physical poles): lookups, map queries, search, directions and routes."""

from __future__ import annotations

from collections import Counter, defaultdict

from psycopg import AsyncConnection

from gtfs_backend.api.context import Context
from gtfs_backend.api.data.catalog import Catalog
from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.errors import NotFound
from gtfs_backend.api.schemas import (
    Direction,
    RouteRef,
    SearchResult,
    Stop,
    StopGroupInfo,
    StopInfo,
    StopList,
    StopRef,
)
from gtfs_backend.ids import parse_url_key
from gtfs_backend.keys import split_feed

MIN_DIRECTION_SHARE = 0.05  # rarer directions are short turns, depot runs etc.
MAX_RESULTS = 3000  # stops per map request
# what search results take from a Stop: its kind and group are implied
GROUP_INFO_FIELDS = set(StopGroupInfo.model_fields) - {"stops"}
STOP_INFO_FIELDS = set(StopInfo.model_fields) - {"directions"}

# An active stop with no modes has no trips in any current feed version (see refresh_stop_state): never returned.
SERVED = "p.active AND cardinality(p.modes) > 0"

# Both select the columns stop_from_row reads.
GROUP_COLS = f"""
    'group', g.id, g.url_key, g.name, ST_Y(g.geom), ST_X(g.geom),
    coalesce((SELECT array_agg(DISTINCT m ORDER BY m) FROM core.stop p, unnest(p.modes) m
              WHERE p.stop_group_id = g.id AND {SERVED}), '{{}}'),
    NULL, NULL
"""
STOP_COLS = """
    'stop', p.id, p.url_key, p.name, ST_Y(p.geom), ST_X(p.geom), p.modes, p.stop_group_id, p.platform_code
"""


def stop_from_row(row) -> Stop:
    kind, sid, key, name, lat, lon, modes, group_id, platform_code = row[:9]
    return Stop(
        kind=kind,
        id=sid,
        key=key,
        name=name,
        lat=lat,
        lon=lon,
        modes=list(modes or []),
        group_id=group_id,
        platform_code=platform_code,
    )


def stop_ref(stop: Stop) -> StopRef:
    return StopRef(id=stop.id, key=stop.key, name=stop.name, group_id=stop.group_id, platform_code=stop.platform_code)


# ------------------------------------------------------------------------------------------------- lookups


async def find(conn: AsyncConnection, key: str) -> Stop | None:
    """A stop group or stop by key (`rondo-mogilskie-qphdu`) or bare id; the slug part is ignored."""
    sid = parse_url_key(key)
    if sid is None:
        return None
    cur = await conn.execute(
        f"SELECT {GROUP_COLS} FROM core.stop_group g WHERE g.id = %(id)s "
        f"UNION ALL SELECT {STOP_COLS} FROM core.stop p WHERE p.id = %(id)s",
        {"id": sid},
    )
    row = await cur.fetchone()
    return stop_from_row(row) if row else None


async def get(conn: AsyncConnection, key: str) -> Stop:
    stop = await find(conn, key)
    if stop is None:
        raise NotFound("stop")
    return stop


async def group_stops(conn: AsyncConnection, group_ids: list[str]) -> dict[str, list[Stop]]:
    """Active stops of each group, by platform_code."""
    out: dict[str, list[Stop]] = {g: [] for g in group_ids}
    cur = await conn.execute(
        f"SELECT {STOP_COLS} FROM core.stop p WHERE p.stop_group_id = ANY(%s) AND {SERVED} "
        "ORDER BY p.platform_code NULLS LAST, p.id",
        (group_ids,),
    )
    for r in await cur.fetchall():
        out[r[7]].append(stop_from_row(r))
    return out


async def stops_by_id(conn: AsyncConnection, ids) -> dict[str, Stop]:
    ids = list(ids)
    if not ids:
        return {}
    cur = await conn.execute(f"SELECT {STOP_COLS} FROM core.stop p WHERE p.id = ANY(%s)", (ids,))
    return {r[1]: stop_from_row(r) for r in await cur.fetchall()}


async def groups_by_id(conn: AsyncConnection, ids) -> dict[str, Stop]:
    ids = list(ids)
    if not ids:
        return {}
    cur = await conn.execute(f"SELECT {GROUP_COLS} FROM core.stop_group g WHERE g.id = ANY(%s)", (ids,))
    return {r[1]: stop_from_row(r) for r in await cur.fetchall()}


async def public_stop_ids(conn: AsyncConnection, stop: Stop) -> list[str]:
    """The stops a stop or group covers: all of a group's (active or not, the timetables decide), or itself."""
    if stop.kind == "stop":
        return [stop.id]
    cur = await conn.execute("SELECT id FROM core.stop WHERE stop_group_id = %s", (stop.id,))
    return [r[0] for r in await cur.fetchall()]


def route_sort_key(r: RouteRef):
    """Modes together, then numeric names numerically (2 < 10 < 52 < 152 < 902 < N1)."""
    return (r.mode, not r.name.isdigit(), int(r.name) if r.name.isdigit() else 0, r.name)


def sorted_routes(routes) -> list[RouteRef]:
    """One chip per name: feeds may split a line into several route_ids (MLD's A67)."""
    return sorted({(split_feed(x.key)[0], x.name): x for x in routes}.values(), key=route_sort_key)


async def _direction_rows(conn: AsyncConnection, catalog: Catalog, stops: list[str]) -> list[tuple]:
    """(stop, next group id, its name, feed_version_id, route_id, headsign, trips) in the timetable versions serving
    today: all their trips, whatever day they run."""
    versions = sorted({d.feed_version_id for d in catalog.today_days()})
    if not stops or not versions:
        return []
    cur = await conn.execute(
        """
        SELECT d.public_stop_id, d.next_group_id, g.name, d.feed_version_id, d.route_id, d.headsign, d.trips
        FROM core.stop_direction d JOIN core.stop_group g ON g.id = d.next_group_id
        WHERE d.feed_version_id = ANY(%s) AND d.public_stop_id = ANY(%s)
        """,
        (versions, stops),
    )
    return await cur.fetchall()


async def directions(
    conn: AsyncConnection, catalog: Catalog, stops: list[str], min_share: float = MIN_DIRECTION_SHARE
) -> dict[str, list[Direction]]:
    """Where trips from each stop go next in today's timetables (all feeds, so ZTP's shared stops get
    trams and buses), most served first. Stops without departures today get none.
    """
    rows = await _direction_rows(conn, catalog, stops)
    refs = await catalog.route_refs((fv, route) for _, _, _, fv, route, _, _ in rows)

    # stop -> next group -> [name, trips, routes, headsign counts]
    acc: dict[str, dict[str, list]] = defaultdict(dict)
    for pid, gid, name, fv, route_id, headsign, trips in rows:
        d = acc[pid].setdefault(gid, [name, 0, [], Counter()])
        d[1] += trips
        if ref := refs.get((fv, route_id)):
            d[2].append(ref)
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
                routes=sorted_routes(group_routes),
                headsigns=[h for h, _ in headsigns.most_common(3)],
            )
            for i, (gid, (name, trips, group_routes, headsigns)) in enumerate(ranked)
            if i == 0 or trips >= min_share * total
        ]
    return out


async def group_info(ctx: Context, conn: AsyncConnection, groups: list[Stop]) -> list[StopGroupInfo]:
    """Groups with their active stops, and each stop's directions."""
    by_group = await group_stops(conn, [g.id for g in groups])
    stops = [p.id for ps in by_group.values() for p in ps]
    dirs = await directions(conn, ctx.catalog, stops, MIN_DIRECTION_SHARE)
    return [
        StopGroupInfo(
            **g.model_dump(include=GROUP_INFO_FIELDS),
            stops=[StopInfo(**p.model_dump(include=STOP_INFO_FIELDS), directions=dirs.get(p.id, [])) for p in by_group[g.id]],
        )
        for g in groups
    ]


async def served_routes(conn: AsyncConnection, catalog: Catalog, stops: list[str]) -> set[tuple[str, str]]:
    """(feed_version_id, route_id) of the routes departing from these stops in today's timetable versions, on any day
    (weekend routes too)."""
    return {(fv, route) for _, _, _, fv, route, _, _ in await _direction_rows(conn, catalog, stops)}


# ------------------------------------------------------------------------------------------------- endpoints


async def in_bbox(ctx: Context, bbox: Bbox, modes: set[str], kind: str) -> StopList:
    """Active stops (or groups, `kind="group"`) inside the box, at most MAX_RESULTS. With `modes`, only stops
    served by any of them; a group is placed (and so found) at its centroid."""
    limit = MAX_RESULTS
    envelope = "ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)"
    has_mode = "(cardinality(%(modes)s::text[]) = 0 OR p.modes && %(modes)s::text[])"
    if kind == "group":
        query = (
            f"SELECT {GROUP_COLS} FROM core.stop_group g WHERE g.active AND g.geom && {envelope} "
            f"AND EXISTS (SELECT 1 FROM core.stop p WHERE p.stop_group_id = g.id AND {SERVED} AND {has_mode}) "
            "ORDER BY g.id LIMIT %(limit)s"
        )
    else:
        query = (
            f"SELECT {STOP_COLS} FROM core.stop p WHERE {SERVED} AND {has_mode} "
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


async def search(ctx: Context, query: str, limit: int) -> SearchResult:
    """Stop groups by name: prefix matches first, then substring, then fuzzy (trigram); accents and case ignored.
    Only stops of feeds.yml `search_namespaces` (ZTP) are found, including other feeds' stops merged into them."""
    async with ctx.pool.connection() as conn:
        cur = await conn.execute(
            f"""
            WITH u AS (SELECT core.immutable_unaccent(lower(%(q)s)) AS q),
            x AS (SELECT q, replace(replace(replace(q, '\\', '\\\\'), '%%', '\\%%'), '_', '\\_') AS esc FROM u)
            SELECT {GROUP_COLS}
            FROM core.stop_group g, x
            WHERE g.active AND g.code_namespace = ANY(%(ns)s)
              AND EXISTS (SELECT 1 FROM core.stop p WHERE p.stop_group_id = g.id AND {SERVED})
              AND (x.q <%% g.name_search OR g.name_search LIKE '%%' || x.esc || '%%')
            ORDER BY g.name_search LIKE x.esc || '%%' DESC,
                     g.name_search LIKE '%%' || x.esc || '%%' DESC,
                     word_similarity(x.q, g.name_search) DESC,
                     similarity(x.q, g.name_search) DESC,
                     g.name
            LIMIT %(limit)s
            """,
            {
                "q": query.strip(),
                "limit": limit,
                "ns": ctx.feeds.searchable_namespaces,
            },
        )
        rows = await cur.fetchall()
        results = await group_info(ctx, conn, [stop_from_row(r) for r in rows])
    return SearchResult(query=query, results=results)
