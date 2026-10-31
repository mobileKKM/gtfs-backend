"""Active flags, modes and group centroids of stops, from every feed's current version."""

from __future__ import annotations

import psycopg

from gtfs_backend.modes import MODE_SQL


def refresh_stop_state(conn: psycopg.Connection, current_versions: list[str]) -> None:
    """Active flags, modes and group centroids from the current version of every feed."""
    cur = conn.cursor()
    cur.execute(
        """
        WITH linked AS (SELECT DISTINCT stop_platform_id FROM core.stop_link WHERE feed_version_id = ANY(%s))
        UPDATE core.stop_platform p
        SET active = (l.stop_platform_id IS NOT NULL),
            last_seen = CASE WHEN l.stop_platform_id IS NOT NULL THEN now() ELSE p.last_seen END
        FROM core.stop_platform p2 LEFT JOIN linked l ON l.stop_platform_id = p2.id
        WHERE p.id = p2.id
        """,
        (current_versions,),
    )
    cur.execute(
        f"""
        WITH served AS (
            SELECT DISTINCT st.feed_version_id, st.stop_id, t.route_id
            FROM gtfs.stop_times st
            JOIN gtfs.trips t ON t.feed_version_id = st.feed_version_id AND t.trip_id = st.trip_id
            WHERE st.feed_version_id = ANY(%s)
        ), modes AS (
            SELECT l.stop_platform_id, array_agg(DISTINCT {MODE_SQL} ORDER BY {MODE_SQL}) AS modes
            FROM served s
            JOIN gtfs.routes r ON r.feed_version_id = s.feed_version_id AND r.route_id = s.route_id
            JOIN core.stop_link l ON l.feed_version_id = s.feed_version_id AND l.stop_id = s.stop_id
            GROUP BY l.stop_platform_id
        )
        UPDATE core.stop_platform p SET modes = coalesce(m.modes, '{{}}')
        FROM core.stop_platform p2 LEFT JOIN modes m ON m.stop_platform_id = p2.id
        WHERE p.id = p2.id AND p.active
        """,
        (current_versions,),
    )
    cur.execute(
        """
        UPDATE core.stop_group g
        SET active = s.any_active,
            last_seen = CASE WHEN s.any_active THEN now() ELSE g.last_seen END,
            geom = coalesce(s.active_centroid, s.centroid, g.geom)
        FROM (
            SELECT g2.id,
                   coalesce(bool_or(p.active), false) AS any_active,
                   ST_Centroid(ST_Collect(p.geom) FILTER (WHERE p.active)) AS active_centroid,
                   ST_Centroid(ST_Collect(p.geom)) AS centroid
            FROM core.stop_group g2 LEFT JOIN core.stop_platform p ON p.stop_group_id = g2.id
            GROUP BY g2.id
        ) s
        WHERE g.id = s.id
        """
    )
