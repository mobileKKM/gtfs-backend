"""Data derived from a loaded feed version: service dates and shape lines."""

from __future__ import annotations

import psycopg


def build_service_dates(conn: psycopg.Connection, feed_version_id: str) -> tuple[int, str | None, str | None]:
    """Expand calendar + calendar_dates into core.service_date. Returns (rows, first_date, last_date)."""
    v = feed_version_id
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO core.service_date (feed_version_id, date, service_id)
        SELECT %(v)s, d, service_id FROM (
            (
                SELECT c.service_id, d::date AS d
                FROM gtfs.calendar c, generate_series(c.start_date, c.end_date, interval '1 day') AS d
                WHERE c.feed_version_id = %(v)s
                  AND (ARRAY[c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday])
                      [extract(isodow FROM d)::int] = 1
                UNION
                SELECT service_id, date FROM gtfs.calendar_dates WHERE feed_version_id = %(v)s AND exception_type = 1
            )
            EXCEPT
            SELECT service_id, date FROM gtfs.calendar_dates WHERE feed_version_id = %(v)s AND exception_type = 2
        ) s
        """,
        {"v": v},
    )
    rows = cur.rowcount
    cur.execute("SELECT min(date)::text, max(date)::text FROM core.service_date WHERE feed_version_id = %s", (v,))
    first, last = cur.fetchone()
    cur.execute(
        "UPDATE core.feed_version SET first_service_date = %s, last_service_date = %s WHERE id = %s",
        (first, last, v),
    )
    return rows, first, last


def build_shape_lines(conn: psycopg.Connection, feed_version_id: str) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO core.shape_line (feed_version_id, shape_id, geom)
        SELECT feed_version_id, shape_id, ST_MakeLine(geom ORDER BY shape_pt_sequence)
        FROM gtfs.shapes
        WHERE feed_version_id = %s
        GROUP BY feed_version_id, shape_id
        HAVING count(*) >= 2
        """,
        (feed_version_id,),
    )
    return cur.rowcount


def build_platform_directions(conn: psycopg.Connection, feed_version_id: str) -> int:
    """Fill core.platform_direction: for every departure (pickup allowed, not the trip's last stop), the next stop
    group the trip reaches. A following stop in the same group is skipped (up to two of them), so a trip calling at
    two platforms of one stop still reads "towards" the stop after it. Needs core.stop_link of the version.

    One window pass over the version's stop_times in trip order.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO core.platform_direction (feed_version_id, stop_platform_id, next_group_id, route_id, headsign, trips)
        SELECT %(v)s, pid, next_gid, route_id, headsign, count(*)
        FROM (
            SELECT pid, route_id, headsign,
                   CASE WHEN n1 <> gid THEN n1 WHEN n2 <> gid THEN n2 WHEN n3 <> gid THEN n3 END AS next_gid
            FROM (
                SELECT l.stop_platform_id AS pid, p.stop_group_id AS gid, t.route_id,
                       coalesce(st.stop_headsign, t.trip_headsign, '') AS headsign, coalesce(st.pickup_type, 0) AS pickup,
                       lead(p.stop_group_id, 1) OVER w AS n1, lead(p.stop_group_id, 2) OVER w AS n2,
                       lead(p.stop_group_id, 3) OVER w AS n3
                FROM gtfs.stop_times st
                JOIN gtfs.trips t ON t.feed_version_id = %(v)s AND t.trip_id = st.trip_id
                JOIN core.stop_link l ON l.feed_version_id = %(v)s AND l.stop_id = st.stop_id
                JOIN core.stop_platform p ON p.id = l.stop_platform_id
                WHERE st.feed_version_id = %(v)s
                WINDOW w AS (PARTITION BY st.trip_id ORDER BY st.stop_sequence)
            ) s
            WHERE pickup <> 1
        ) d
        WHERE next_gid IS NOT NULL
        GROUP BY pid, next_gid, route_id, headsign
        """,
        {"v": feed_version_id},
    )
    return cur.rowcount
