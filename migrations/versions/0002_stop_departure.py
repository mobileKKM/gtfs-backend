"""Precomputed stop departures for departure boards (core.stop_departure), one partition per feed version.

Imports fill a new version's partition (worker/static/derive.py, build_stop_departures); this backfills the versions
that are ready already.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-24
"""

from alembic import op

from gtfs_backend.worker.static.partitions import STOP_DEPARTURE
from gtfs_backend.worker.static.spec import partition_name

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

# A copy of derive.STOP_DEPARTURES_SQL as of this revision: a migration must not change when the live code does.
STOP_DEPARTURES_SQL = """
    INSERT INTO {part} (feed_version_id, public_stop_id, service_id, departure_s, trip_id, stop_sequence, route_id,
                        headsign, on_demand)
    SELECT %(v)s, l.public_stop_id, t.service_id, extract(epoch FROM s.departure_time)::int, s.trip_id, s.stop_sequence,
           t.route_id, coalesce(s.stop_headsign, t.trip_headsign), coalesce(s.pickup_type, 0) IN (2, 3)
    FROM (
        SELECT st.trip_id, st.stop_sequence, st.stop_id, st.departure_time, st.pickup_type, st.stop_headsign,
               lead(st.stop_sequence) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_sequence
        FROM gtfs.stop_times st
        WHERE st.feed_version_id = %(v)s
    ) s
    JOIN core.stop_link l ON l.feed_version_id = %(v)s AND l.stop_id = s.stop_id
    JOIN gtfs.trips t ON t.feed_version_id = %(v)s AND t.trip_id = s.trip_id
    WHERE s.next_sequence IS NOT NULL AND coalesce(s.pickup_type, 0) <> 1 AND s.departure_time IS NOT NULL
    -- a board's rows end up next to each other on disk
    ORDER BY l.public_stop_id, t.service_id, 4
"""

DDL = """
-- What departs where: every stop time a board can show (pickup allowed, not the trip's last stop), with what a board
-- needs of its trip. A board reads one index range per stop and service day: no joins.
CREATE TABLE core.stop_departure (
    feed_version_id text NOT NULL,
    public_stop_id text NOT NULL,
    service_id text NOT NULL,
    departure_s integer NOT NULL,  -- GTFS departure_time in seconds (past 24:00 for night trips)
    trip_id text NOT NULL,
    stop_sequence integer NOT NULL,
    route_id text NOT NULL,
    headsign text,  -- stop_headsign, else trip_headsign
    on_demand boolean NOT NULL  -- pickup_type 2/3
) PARTITION BY LIST (feed_version_id);

CREATE INDEX stop_departure_idx ON core.stop_departure (public_stop_id, service_id, departure_s);
"""


def upgrade() -> None:
    op.execute(DDL)
    bind = op.get_bind()
    versions = bind.exec_driver_sql(
        "SELECT id, feed_id, version FROM core.feed_version WHERE status = 'ready' ORDER BY id"
    ).fetchall()
    for fv_id, feed_id, version in versions:
        # feed ids are [a-z0-9_] (core.feed's check), so the name needs no quoting beyond this
        part = f'core."{partition_name(STOP_DEPARTURE, feed_id, version)}"'
        bind.exec_driver_sql(
            f"CREATE TABLE {part} (LIKE core.stop_departure INCLUDING DEFAULTS, "
            f"CONSTRAINT version_chk CHECK (feed_version_id = '{fv_id}'))"
        )
        bind.exec_driver_sql(STOP_DEPARTURES_SQL.format(part=part), {"v": fv_id})
        bind.exec_driver_sql(f"ALTER TABLE core.stop_departure ATTACH PARTITION {part} FOR VALUES IN ('{fv_id}')")
        bind.exec_driver_sql(f"ANALYZE {part}")


def downgrade() -> None:
    op.execute("DROP TABLE core.stop_departure")
