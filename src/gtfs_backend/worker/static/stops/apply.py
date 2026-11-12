"""Matching a new version's stops and writing the result: stop groups, stops, stop links."""

from __future__ import annotations

from collections import Counter, defaultdict

import psycopg

from gtfs_backend.ids import new_public_id, slugify, stop_slug
from gtfs_backend.logs import log
from gtfs_backend.modes import MODE_SQL
from gtfs_backend.worker.settings import get_worker_settings
from gtfs_backend.worker.static.stops.model import Group, NewStop, PublicStop, normalize_stop
from gtfs_backend.worker.static.stops.osm import load_osm_poles
from gtfs_backend.worker.static.stops.plan import plan_matches
from gtfs_backend.worker.static.stops.queries import (
    current_versions,
    namespace_versions,
    public_stop_bearings,
    public_stop_next_groups,
    served_modes,
    stop_bearings,
    stop_next_stops,
)
from gtfs_backend.worker.static.stops.shared import SharedPlan, plan_shared
from gtfs_backend.worker.static.stops.state import refresh_stop_state


def _allocate_id(cur: psycopg.Cursor, kind: str, taken: set[str]) -> str:
    while True:
        pid = new_public_id()
        if pid not in taken:
            taken.add(pid)
            cur.execute("INSERT INTO core.public_id (id, kind) VALUES (%s, %s)", (pid, kind))
            return pid


def apply_matches(
    conn: psycopg.Connection,
    feed_id: str,
    feed_version_id: str,
    namespace: str | None = None,
    share_with: list[str] | None = None,
    stop_code_pattern: str | None = None,
) -> dict:
    """Match the stops of `feed_version_id`, write stop_link and refresh stops/groups. Returns stats.

    Stops are first put into GTFS shape (normalize_stop, with the feed's `stop_code_pattern`). They then try to
    attach to stops of the `share_with` namespaces (never modified by this feed); the rest are matched within the
    feed's own stop-code namespace.
    """
    namespace = namespace or feed_id
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.stop_id, NULLIF(btrim(s.stop_code), ''), coalesce(s.stop_name, s.stop_id), NULLIF(btrim(s.platform_code), ''),
               s.stop_lat, s.stop_lon,
               CASE WHEN p.stop_id IS NOT NULL THEN 'station:' || coalesce(NULLIF(btrim(p.stop_code), ''), p.stop_id) END,
               p.stop_name
        FROM gtfs.stops s
        LEFT JOIN gtfs.stops p
            ON p.feed_version_id = s.feed_version_id AND p.stop_id = s.parent_station AND p.location_type = 1
        WHERE s.feed_version_id = %(v)s AND coalesce(s.location_type, 0) = 0
          AND s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL
        """,
        {"v": feed_version_id},
    )
    all_stops = [normalize_stop(*r, stop_code_pattern=stop_code_pattern) for r in cur.fetchall()]

    shared_plan = SharedPlan()
    if share_with:
        cur.execute(
            """
            SELECT id, stop_group_id, current_code, name, platform_code, ST_Y(geom), ST_X(geom), active
            FROM core.stop WHERE code_namespace = ANY(%s) AND active
            """,
            (share_with,),
        )
        shared = [PublicStop(*r) for r in cur.fetchall()]
        cur.execute(
            "SELECT id, current_code, name, ST_Y(geom), ST_X(geom) FROM core.stop_group "
            "WHERE code_namespace = ANY(%s) AND active",
            (share_with,),
        )
        shared_groups = [Group(*r) for r in cur.fetchall()]
        cur.execute(f"SELECT DISTINCT {MODE_SQL} FROM gtfs.routes WHERE feed_version_id = %s", (feed_version_id,))
        feed_modes = {r[0] for r in cur.fetchall()}
        stop_bearing = {sid: b for (_, sid), b in stop_bearings(cur, [feed_version_id]).items()}
        shared_versions = namespace_versions(cur, share_with)
        shared_modes = served_modes(cur, shared_versions)  # the shared feeds' own service, not our links
        shared_bearing = public_stop_bearings(cur, shared_versions)
        shared_plan = plan_shared(
            all_stops,
            shared,
            shared_groups,
            stop_bearing,
            shared_bearing,
            shared_modes,
            feed_modes,
            stop_next_stops(cur, feed_version_id),
            public_stop_next_groups(cur, shared_versions),
            load_osm_poles(get_worker_settings().osm_stops_file),
        )
        for w in shared_plan.warnings:
            log.info("stop_share", feed=feed_id, msg=w)
        log.info("stop_share_summary", feed=feed_id, **shared_plan.methods)
    shared_links = shared_plan.links
    group_override = shared_plan.group_for_key
    stops = [s for s in all_stops if s.stop_id not in shared_links]

    others = [v for v in current_versions(cur, feed_id, feed_version_id) if v != feed_version_id]
    cur.execute(
        """
        SELECT p.id, p.stop_group_id, p.current_code, p.name, p.platform_code, ST_Y(p.geom), ST_X(p.geom), p.active,
               EXISTS (SELECT 1 FROM core.stop_link l WHERE l.public_stop_id = p.id AND l.feed_version_id = ANY(%s))
        FROM core.stop p
        WHERE p.code_namespace = %s
        """,
        (others, namespace),
    )
    existing = [PublicStop(*r) for r in cur.fetchall()]
    cur.execute(
        "SELECT id, current_code, name, ST_Y(geom), ST_X(geom) FROM core.stop_group WHERE code_namespace = %s", (namespace,)
    )
    groups = [Group(*r) for r in cur.fetchall()]

    plan = plan_matches(stops, existing, groups)
    for w in plan.warnings:
        log.warning("stop_match", feed=feed_id, msg=w)
    for e in plan.errors:
        log.error("stop_match", feed=feed_id, msg=e)

    cur.execute("SELECT id FROM core.public_id")
    taken = {r[0] for r in cur.fetchall()}
    reps: dict[str, NewStop] = {}
    for s in sorted(stops, key=lambda x: x.stop_id):
        reps.setdefault(s.pole_key, s)

    # groups first (stops reference them)
    group_id_for_key: dict[str, str] = {}
    by_gkey: dict[str, list[NewStop]] = defaultdict(list)
    for s in reps.values():
        by_gkey[s.group_key].append(s)
    for gkey, members in by_gkey.items():
        if gkey in group_override:  # shared group: join it, never rename it
            group_id_for_key[gkey] = group_override[gkey]
            continue
        name = Counter(m.group_name for m in members).most_common(1)[0][0]
        code = None if gkey.startswith("name:") else gkey
        gid = plan.group_for_key.get(gkey)
        if gid is None:
            gid = _allocate_id(cur, "stop_group", taken)
            cur.execute(
                "INSERT INTO core.stop_group (id, slug, code_namespace, current_code, name) VALUES (%s, %s, %s, %s, %s)",
                (gid, slugify(name), namespace, code, name),
            )
        else:
            cur.execute(
                "UPDATE core.stop_group SET slug = %s, current_code = %s, name = %s WHERE id = %s",
                (slugify(name), code, name, gid),
            )
        group_id_for_key[gkey] = gid

    # stops: deactivate displaced holders of a code first (the active-code index is unique)
    new_public_ids: dict[str, str] = {}
    for key, s in reps.items():
        pid = plan.stop_for_key.get(key)
        gid = group_id_for_key[s.group_key]
        if s.code:
            cur.execute(
                """
                UPDATE core.stop SET active = false
                WHERE code_namespace = %s AND current_code = %s AND active AND id IS DISTINCT FROM %s
                """,
                (namespace, s.code, pid),
            )
        if pid is None:
            pid = _allocate_id(cur, "stop", taken)
            cur.execute(
                """
                INSERT INTO core.stop (id, stop_group_id, slug, code_namespace, current_code, name, platform_code, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                """,
                (pid, gid, stop_slug(s.name, s.platform_code), namespace, s.code, s.name, s.platform_code, s.lon, s.lat),
            )
        else:
            cur.execute(
                """
                UPDATE core.stop
                SET stop_group_id = %s, slug = %s, current_code = %s, name = %s, platform_code = %s,
                    geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326), active = true
                WHERE id = %s
                """,
                (gid, stop_slug(s.name, s.platform_code), s.code, s.name, s.platform_code, s.lon, s.lat, pid),
            )
        new_public_ids[key] = pid

    with cur.copy("COPY core.stop_link (feed_version_id, stop_id, public_stop_id) FROM STDIN") as cp:
        for s in stops:
            cp.write_row((feed_version_id, s.stop_id, new_public_ids[s.pole_key]))
        for stop_id, pid in shared_links.items():
            cp.write_row((feed_version_id, stop_id, pid))

    # the rows written above are uncommitted, so autovacuum can't have analyzed them; refresh_stop_state and the
    # stop directions query join them with millions of stop_times and would be planned blind
    cur.execute("ANALYZE core.stop_link, core.stop")
    refresh_stop_state(conn, current_versions(cur, feed_id, feed_version_id))
    methods = Counter(plan.method_for_key.values())
    for k, n in shared_plan.methods.items():
        methods[f"shared_{k}"] = n
    return {
        "stops": len(reps),
        "methods": dict(methods),
        "warnings": plan.warnings + shared_plan.warnings,
        "errors": plan.errors,
    }
