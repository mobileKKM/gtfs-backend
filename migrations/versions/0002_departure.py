"""core.departure: boardable stop times per feed version, for departure boards (static/derive.py build_departures).

Boards used to read gtfs.stop_times and look up every row's trip before they could drop the trips not running that day
(88% of a busy stop's rows). core.departure carries the trip's service, route and headsign, and its index has the
service as a key column, so a board reads only the departures of the day's services.

Partitioned per feed version like the GTFS tables (`departure__kmk_a__v3`, dropped with them on retirement). Versions
imported before this migration are filled here; the SQL is frozen, like 0001's.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-13
"""

from alembic import op
from sqlalchemy import text

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

CREATE_SQL = """
CREATE TABLE core.departure (
    feed_version_id text NOT NULL,
    stop_id text NOT NULL,
    departure_s integer NOT NULL,  -- GTFS departure_time in seconds (may exceed 24 h)
    service_id text NOT NULL,
    trip_id text NOT NULL,
    stop_sequence integer NOT NULL,
    route_id text NOT NULL,
    headsign text,  -- stop_headsign, else trip_headsign
    on_demand boolean NOT NULL  -- pickup_type 2 or 3
) PARTITION BY LIST (feed_version_id);

CREATE INDEX departure_stop_time_idx ON core.departure (feed_version_id, stop_id, departure_s, service_id);
"""

FILL_SQL = """
INSERT INTO core.departure
    (feed_version_id, stop_id, departure_s, service_id, trip_id, stop_sequence, route_id, headsign, on_demand)
SELECT s.feed_version_id, s.stop_id, s.departure_s, t.service_id, s.trip_id, s.stop_sequence, t.route_id,
       coalesce(s.stop_headsign, t.trip_headsign), s.pickup IN (2, 3)
FROM (
    SELECT st.feed_version_id, st.stop_id, extract(epoch FROM st.departure_time)::int AS departure_s, st.trip_id,
           st.stop_sequence, st.stop_headsign, coalesce(st.pickup_type, 0) AS pickup,
           max(st.stop_sequence) OVER (PARTITION BY st.trip_id) AS last_sequence
    FROM gtfs.stop_times st
    WHERE st.feed_version_id = :v
) s
JOIN gtfs.trips t ON t.feed_version_id = s.feed_version_id AND t.trip_id = s.trip_id
WHERE s.departure_s IS NOT NULL AND s.pickup <> 1 AND s.stop_sequence < s.last_sequence
"""


def upgrade() -> None:
    bind = op.get_bind()
    for statement in CREATE_SQL.split(";"):
        if statement.strip():
            bind.execute(text(statement))
    versions = bind.execute(
        text("SELECT id, feed_id, version FROM core.feed_version WHERE status IN ('ready', 'importing') ORDER BY id")
    ).fetchall()
    for fv_id, feed_id, version in versions:
        part = f'core."departure__{feed_id}__v{version}"'
        literal = fv_id.replace("'", "''")
        bind.execute(text(f"CREATE TABLE {part} PARTITION OF core.departure FOR VALUES IN ('{literal}')"))
        bind.execute(text(FILL_SQL), {"v": fv_id})
        bind.execute(text(f"ANALYZE {part}"))


def downgrade() -> None:
    op.execute("DROP TABLE core.departure")
