"""Stop groups (logical) and stops (physical poles): lookups, map queries, search, directions and routes."""

from __future__ import annotations

from collections import Counter, defaultdict

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
    StopRef,
)
from gtfs_backend.keys import split_feed

MIN_DIRECTION_SHARE = 0.05  # rarer directions are short turns, depot runs etc.
MAX_RESULTS = 3000  # stops per map request
# what search results take from a Stop: its kind and group are implied
GROUP_INFO_FIELDS = set(StopGroupInfo.model_fields) - {"stops"}
STOP_INFO_FIELDS = set(StopInfo.model_fields) - {"directions"}

# An active stop with no modes has no trips in any current feed version (see refresh_stop_state): never returned.
SERVED = "p.active AND cardinality(p.modes) > 0"


def stop_ref(stop: Stop) -> StopRef:
    return StopRef(key=stop.key, name=stop.name, group_key=stop.group_key, platform_code=stop.platform_code)


# ------------------------------------------------------------------------------------------------- lookups (memory)


def get(catalog: Catalog, key: str) -> Stop:
    """A stop group or stop by key (catalog.stops.find), else 404. Other lookups: catalog.stops directly."""
    stop = catalog.stops.find(key)
    if stop is None:
        raise NotFound("stop")
    return stop


def route_sort_key(r: RouteRef):
    """Modes together, then numeric names numerically (2 < 10 < 52 < 152 < 902 < N1)."""
    return (r.mode, not r.name.isdigit(), int(r.name) if r.name.isdigit() else 0, r.name)


def sorted_routes(routes) -> list[RouteRef]:
    """One chip per name: feeds may split a line into several route_ids (MLD's A67)."""
    return sorted({(split_feed(x.key)[0], x.name): x for x in routes}.values(), key=route_sort_key)


async def directions(catalog: Catalog, stops: list[str], min_share: float = MIN_DIRECTION_SHARE) -> dict[str, list[Direction]]:
    """Where trips from each stop go next in today's timetables (all feeds, so ZTP's shared stops get
    trams and buses), most served first. Stops without departures today get none.
    """
    rows = catalog.stops.direction_rows(stops)
    refs = await catalog.route_refs((fv, route) for _, _, _, _, fv, route, _, _ in rows)

    # stop -> next group -> [name, trips, routes, headsign counts, key]
    acc: dict[str, dict[str, list]] = defaultdict(dict)
    for pid, gid, name, gkey, fv, route_id, headsign, trips in rows:
        d = acc[pid].setdefault(gid, [name, 0, [], Counter(), gkey])
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
                towards_key=gkey,
                routes=sorted_routes(group_routes),
                # most trips first; ties by name (it was the database's row order, which changes with the plan)
                headsigns=[h for h, _ in sorted(headsigns.items(), key=lambda hc: (-hc[1], hc[0]))[:3]],
            )
            for i, (gid, (name, trips, group_routes, headsigns, gkey)) in enumerate(ranked)
            if i == 0 or trips >= min_share * total
        ]
    return out


async def group_info(ctx: Context, groups: list[Stop]) -> list[StopGroupInfo]:
    """Groups with their active stops, and each stop's directions."""
    by_group = ctx.catalog.stops.group_stops([g.id for g in groups])
    stops = [p.id for ps in by_group.values() for p in ps]
    dirs = await directions(ctx.catalog, stops, MIN_DIRECTION_SHARE)
    return [
        StopGroupInfo(
            **g.model_dump(include=GROUP_INFO_FIELDS),
            stops=[StopInfo(**p.model_dump(include=STOP_INFO_FIELDS), directions=dirs.get(p.id, [])) for p in by_group[g.id]],
        )
        for g in groups
    ]


def served_routes(catalog: Catalog, stops: list[str]) -> set[tuple[str, str]]:
    """(feed_version_id, route_id) of the routes departing from these stops in today's timetable versions, on any day
    (weekend routes too)."""
    return {(fv, route) for _, _, _, _, fv, route, _, _ in catalog.stops.direction_rows(stops)}


# ------------------------------------------------------------------------------------------------- endpoints


def in_bbox_json(ctx: Context, bbox: Bbox, modes: set[str], kind: str) -> bytes:
    """A StopList body: active stops (or groups, `kind="group"`) inside the box, at most MAX_RESULTS. With `modes`,
    only stops served by any of them; a group is placed (and so found) at its centroid. Joined from each stop's JSON
    (serialized when the stops load): byte for byte what StopList.model_dump_json() gives, without building it."""
    found = ctx.catalog.stops.in_bbox_json(bbox, modes, kind, MAX_RESULTS + 1)
    truncated = b"true" if len(found) > MAX_RESULTS else b"false"
    return b'{"truncated":' + truncated + b',"stops":[' + b",".join(found[:MAX_RESULTS]) + b"]}"


async def search(ctx: Context, query: str, limit: int) -> SearchResult:
    """Stop groups by name: prefix matches first, then substring, then fuzzy (trigram); accents and case ignored.
    Only stops of feeds.yml `search_namespaces` (ZTP) are found, including other feeds' stops merged into them.
    Postgres ranks (trigram index); the groups themselves come from memory."""
    async with ctx.pool.connection() as conn:
        cur = await conn.execute(
            f"""
            WITH u AS (SELECT core.immutable_unaccent(lower(%(q)s)) AS q),
            x AS (SELECT q, replace(replace(replace(q, '\\', '\\\\'), '%%', '\\%%'), '_', '\\_') AS esc FROM u)
            SELECT g.id
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
        ids = [gid for (gid,) in await cur.fetchall()]
    by_id = ctx.catalog.stops.groups_by_id(ids)  # a group imported since the catalog loaded waits for its reload
    results = await group_info(ctx, [by_id[g] for g in ids if g in by_id])
    return SearchResult(query=query, results=results)
