-- Precomputed stop departures for departure boards, one partition per feed version (alembic revision 0002).
-- Imports fill a new version's partition; versions already ready when this runs are backfilled by the worker.
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
