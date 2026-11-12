"""Reads the matchers need: travel directions through feed stops and our stops, and current feed versions."""

from __future__ import annotations

import math
from collections import defaultdict

import psycopg

from gtfs_backend.modes import MODE_SQL


def feed_stop_bearings(cur: psycopg.Cursor, versions: list[str], ids: list[str] | None = None) -> dict[tuple[str, str], float]:
    """Direction of travel (degrees from north) through each feed stop: previous stop -> next stop, averaged over
    trips. Keyed by (feed_version_id, stop_id); `ids` limits it to those feed stops."""
    if not versions:
        return {}
    cur.execute(
        """
        WITH seq AS (
            SELECT st.feed_version_id AS v, st.stop_id,
                   coalesce(lag(s.stop_lat) OVER w, s.stop_lat) AS plat, coalesce(lag(s.stop_lon) OVER w, s.stop_lon) AS plon,
                   coalesce(lead(s.stop_lat) OVER w, s.stop_lat) AS nlat, coalesce(lead(s.stop_lon) OVER w, s.stop_lon) AS nlon
            FROM gtfs.stop_times st
            JOIN gtfs.stops s ON s.feed_version_id = st.feed_version_id AND s.stop_id = st.stop_id
            WHERE st.feed_version_id = ANY(%(v)s)
              AND (%(ids)s::text[] IS NULL OR st.trip_id IN (
                  SELECT trip_id FROM gtfs.stop_times x
                  WHERE x.feed_version_id = st.feed_version_id AND x.stop_id = ANY(%(ids)s)))
            WINDOW w AS (PARTITION BY st.feed_version_id, st.trip_id ORDER BY st.stop_sequence)
        ), vec AS (
            SELECT v, stop_id, (nlon - plon) * cos(radians(nlat)) AS dx, nlat - plat AS dy FROM seq
            WHERE (nlat, nlon) <> (plat, plon) AND (%(ids)s::text[] IS NULL OR stop_id = ANY(%(ids)s))
        )
        SELECT v, stop_id, degrees(atan2(sum(dx / sqrt(dx*dx + dy*dy)), sum(dy / sqrt(dx*dx + dy*dy))))
        FROM vec GROUP BY v, stop_id
        """,
        {"v": versions, "ids": ids},
    )
    return {(v, sid): (b + 360) % 360 for v, sid, b in cur.fetchall() if b is not None}


def stop_bearings(cur: psycopg.Cursor, versions: list[str]) -> dict[str, float]:
    """Direction of travel per stop of ours, from all its linked feed stops in the given versions."""
    if not versions:
        return {}
    by_stop = feed_stop_bearings(cur, versions)
    cur.execute("SELECT feed_version_id, feed_stop_id, stop_id FROM core.stop_link WHERE feed_version_id = ANY(%s)", (versions,))
    vectors: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for v, sid, pid in cur.fetchall():
        b = by_stop.get((v, sid))
        if b is not None:
            vectors[pid][0] += math.sin(math.radians(b))
            vectors[pid][1] += math.cos(math.radians(b))
    return {pid: (math.degrees(math.atan2(x, y)) + 360) % 360 for pid, (x, y) in vectors.items() if x or y}


def served_modes(cur: psycopg.Cursor, versions: list[str]) -> dict[str, set[str]]:
    """Modes the given versions' own trips serve at each stop of ours.

    Not core.stop.modes: that counts every feed linked to the stop, so a bus feed wrongly linked to a
    tram pole would make the pole look like a bus pole.
    """
    if not versions:
        return {}
    cur.execute(
        f"""
        SELECT l.stop_id, array_agg(DISTINCT {MODE_SQL})
        FROM (
            SELECT DISTINCT st.feed_version_id, st.stop_id, t.route_id
            FROM gtfs.stop_times st
            JOIN gtfs.trips t ON t.feed_version_id = st.feed_version_id AND t.trip_id = st.trip_id
            WHERE st.feed_version_id = ANY(%s)
        ) s
        JOIN gtfs.routes r ON r.feed_version_id = s.feed_version_id AND r.route_id = s.route_id
        JOIN core.stop_link l ON l.feed_version_id = s.feed_version_id AND l.feed_stop_id = s.stop_id
        GROUP BY l.stop_id
        """,
        (versions,),
    )
    return {pid: set(modes) for pid, modes in cur.fetchall()}


def feed_stop_next_stops(cur: psycopg.Cursor, version: str) -> dict[str, set[str]]:
    """Stops a version's trips go to right after each stop (not after the trip's last stop)."""
    cur.execute(
        """
        SELECT DISTINCT stop_id, nxt FROM (
            SELECT stop_id, lead(stop_id) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS nxt
            FROM gtfs.stop_times WHERE feed_version_id = %s
        ) t WHERE nxt IS NOT NULL AND nxt <> stop_id
        """,
        (version,),
    )
    out: dict[str, set[str]] = defaultdict(set)
    for sid, nxt in cur.fetchall():
        out[sid].add(nxt)
    return out


def stop_next_groups(cur: psycopg.Cursor, versions: list[str]) -> dict[str, set[str]]:
    """Groups the trips of the given versions go to right after each stop of ours."""
    if not versions:
        return {}
    cur.execute(
        "SELECT DISTINCT stop_id, next_group_id FROM core.stop_direction WHERE feed_version_id = ANY(%s)",
        (versions,),
    )
    out: dict[str, set[str]] = defaultdict(set)
    for pid, gid in cur.fetchall():
        out[pid].add(gid)
    return out


def namespace_versions(cur: psycopg.Cursor, namespaces: list[str]) -> list[str]:
    """Latest ready version of every active feed in the given stop namespaces."""
    cur.execute(
        """
        SELECT DISTINCT ON (fv.feed_id) fv.id
        FROM core.feed_version fv JOIN core.feed f ON f.feed_id = fv.feed_id AND f.active
        WHERE fv.status = 'ready' AND f.stop_namespace = ANY(%s)
        ORDER BY fv.feed_id, fv.version DESC
        """,
        (namespaces,),
    )
    return [r[0] for r in cur.fetchall()]


def current_versions(cur: psycopg.Cursor, feed_id: str, new_version_id: str) -> list[str]:
    """Latest ready version of every other active feed, plus the version being imported."""
    cur.execute(
        """
        SELECT DISTINCT ON (fv.feed_id) fv.id
        FROM core.feed_version fv JOIN core.feed f ON f.feed_id = fv.feed_id AND f.active
        WHERE fv.status = 'ready' AND fv.feed_id <> %s
        ORDER BY fv.feed_id, fv.version DESC
        """,
        (feed_id,),
    )
    return [r[0] for r in cur.fetchall()] + [new_version_id]
