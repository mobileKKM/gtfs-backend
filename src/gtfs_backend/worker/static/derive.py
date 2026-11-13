"""Data derived from a loaded feed version: service dates, shape lines, stop directions and departures."""

from __future__ import annotations

import psycopg
from psycopg import sql

from gtfs_backend.worker.static.partitions import FeedVersion, attach_version_partition, create_version_partition


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


def build_stop_directions(conn: psycopg.Connection, feed_version_id: str) -> int:
    """Fill core.stop_direction: for every departure (pickup allowed, not the trip's last stop), the next stop
    group the trip reaches. A following stop in the same group is skipped (up to two of them), so a trip calling at
    two stops of one group still reads "towards" the stop after it. Needs core.stop_link of the version.

    One window pass over the version's stop_times in trip order.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO core.stop_direction (feed_version_id, public_stop_id, next_group_id, route_id, headsign, trips)
        SELECT %(v)s, pid, next_gid, route_id, headsign, count(*)
        FROM (
            SELECT pid, route_id, headsign,
                   CASE WHEN n1 <> gid THEN n1 WHEN n2 <> gid THEN n2 WHEN n3 <> gid THEN n3 END AS next_gid
            FROM (
                SELECT l.public_stop_id AS pid, p.stop_group_id AS gid, t.route_id,
                       coalesce(st.stop_headsign, t.trip_headsign, '') AS headsign, coalesce(st.pickup_type, 0) AS pickup,
                       lead(p.stop_group_id, 1) OVER w AS n1, lead(p.stop_group_id, 2) OVER w AS n2,
                       lead(p.stop_group_id, 3) OVER w AS n3
                FROM gtfs.stop_times st
                JOIN gtfs.trips t ON t.feed_version_id = %(v)s AND t.trip_id = st.trip_id
                JOIN core.stop_link l ON l.feed_version_id = %(v)s AND l.stop_id = st.stop_id
                JOIN core.stop p ON p.id = l.public_stop_id
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


# Kept in step with migrations/versions/0002_departure.py, which fills the versions that existed before it.
DEPARTURES_SQL = """
INSERT INTO {part}
    (feed_version_id, stop_id, departure_s, service_id, trip_id, stop_sequence, route_id, headsign, on_demand)
SELECT s.feed_version_id, s.stop_id, s.departure_s, t.service_id, s.trip_id, s.stop_sequence, t.route_id,
       coalesce(s.stop_headsign, t.trip_headsign), s.pickup IN (2, 3)
FROM (
    SELECT st.feed_version_id, st.stop_id, extract(epoch FROM st.departure_time)::int AS departure_s, st.trip_id,
           st.stop_sequence, st.stop_headsign, coalesce(st.pickup_type, 0) AS pickup,
           max(st.stop_sequence) OVER (PARTITION BY st.trip_id) AS last_sequence
    FROM gtfs.stop_times st
    WHERE st.feed_version_id = {v}
) s
JOIN gtfs.trips t ON t.feed_version_id = s.feed_version_id AND t.trip_id = s.trip_id
WHERE s.departure_s IS NOT NULL AND s.pickup <> 1 AND s.stop_sequence < s.last_sequence
"""


def build_departures(conn: psycopg.Connection, fv: FeedVersion) -> int:
    """Fill the version's partition of core.departure: every stop time a passenger can board at (pickup allowed, not the
    trip's last stop), with what a departure board shows. Boards read it by stop and time, and the day's services are
    checked in its index, so they never touch the trips that don't run that day (88% of a busy stop's rows; 49 -> 1 ms).
    """
    cur = conn.cursor()
    part = create_version_partition(cur, "departure", fv, schema="core")
    cur.execute(sql.SQL(DEPARTURES_SQL).format(part=part, v=sql.Literal(fv.id)))
    rows = cur.rowcount
    cur.execute(sql.SQL("ANALYZE {}").format(part))
    attach_version_partition(cur, "departure", part, fv, schema="core")
    return rows
