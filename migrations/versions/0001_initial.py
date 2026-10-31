"""Initial schema: core bookkeeping + stop model, 1:1 GTFS tables partitioned per feed version.

Revision ID: 0001
Revises:
Create Date: 2026-10-25
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

CORE_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SCHEMA IF NOT EXISTS core;

CREATE SCHEMA IF NOT EXISTS gtfs;

-- unaccent() is only STABLE; this wrapper pins the dictionary so it can be used in indexes / generated columns.
CREATE FUNCTION core.immutable_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
    RETURN public.unaccent('public.unaccent'::regdictionary, $1);

CREATE TABLE core.feed (
    feed_id text PRIMARY KEY CHECK (feed_id ~ '^[a-z0-9]+(_[a-z0-9]+)*$' AND length(feed_id) <= 32),
    stop_namespace text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    config jsonb NOT NULL DEFAULT '{}',
    -- highest version number handed out; versions count 1, 2, 3, ... per feed and are never reused
    last_version integer NOT NULL DEFAULT 0,
    static_etag text,
    static_last_modified text,
    static_checked_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Versions count 1, 2, 3, ... per feed (core.feed.last_version). id is the key every table joins on, built from
-- both: `kmk_t_v3`. Partition names use the same parts (stop_times__kmk_t__v3).
CREATE TABLE core.feed_version (
    feed_id text NOT NULL REFERENCES core.feed (feed_id),
    version integer NOT NULL CHECK (version > 0),
    id text GENERATED ALWAYS AS (feed_id || '_v' || version::text) STORED PRIMARY KEY,
    sha256 text NOT NULL,
    etag text,
    last_modified text,
    source_feed_version text,
    timezone text,
    downloaded_at timestamptz NOT NULL DEFAULT now(),
    imported_at timestamptz,
    status text NOT NULL CHECK (status IN ('importing', 'ready', 'failed', 'retired')),
    first_service_date date,
    last_service_date date,
    archive_path text,
    row_counts jsonb NOT NULL DEFAULT '{}',
    warnings jsonb NOT NULL DEFAULT '[]',
    error text,
    UNIQUE (feed_id, version),
    UNIQUE (feed_id, sha256)
);

CREATE INDEX feed_version_feed_status_idx ON core.feed_version (feed_id, status);

CREATE TABLE core.service_date (
    feed_version_id text NOT NULL REFERENCES core.feed_version (id) ON DELETE CASCADE,
    date date NOT NULL,
    service_id text NOT NULL,
    PRIMARY KEY (feed_version_id, date, service_id)
);

CREATE TABLE core.service_day_version (
    feed_id text NOT NULL REFERENCES core.feed (feed_id),
    service_date date NOT NULL,
    feed_version_id text NOT NULL REFERENCES core.feed_version (id) ON DELETE CASCADE,
    PRIMARY KEY (feed_id, service_date)
);

CREATE INDEX service_day_version_version_idx ON core.service_day_version (feed_version_id);

-- Stop codes are only comparable within a namespace (feeds.yml `stop_namespace`, default: the feed_id).
-- Public stop IDs (5 lowercase letters), unique across stop groups and stop platforms.
CREATE TABLE core.public_id (
    id text PRIMARY KEY CHECK (id ~ '^[a-z]{5}$'),
    kind text NOT NULL CHECK (kind IN ('stop_group', 'stop_platform'))
);

CREATE TABLE core.stop_group (
    id text PRIMARY KEY REFERENCES core.public_id (id),
    slug text NOT NULL,
    code_namespace text NOT NULL,
    current_code text,
    name text NOT NULL,
    geom geometry(Point, 4326),
    active boolean NOT NULL DEFAULT true,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    name_search text GENERATED ALWAYS AS (core.immutable_unaccent(lower(name))) STORED,
    url_key text GENERATED ALWAYS AS (slug || '-' || id) STORED
);

CREATE INDEX stop_group_name_trgm_idx ON core.stop_group USING gin (name_search gin_trgm_ops);

CREATE INDEX stop_group_geom_idx ON core.stop_group USING gist (geom);

CREATE INDEX stop_group_code_idx ON core.stop_group (code_namespace, current_code);

CREATE TABLE core.stop_platform (
    id text PRIMARY KEY REFERENCES core.public_id (id),
    stop_group_id text NOT NULL REFERENCES core.stop_group (id),
    slug text NOT NULL,
    code_namespace text NOT NULL,
    current_code text,
    name text NOT NULL,
    platform text,
    geom geometry(Point, 4326),
    modes text[] NOT NULL DEFAULT '{}',
    active boolean NOT NULL DEFAULT true,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    url_key text GENERATED ALWAYS AS (slug || '-' || id) STORED
);

CREATE UNIQUE INDEX stop_platform_active_code_uidx ON core.stop_platform (code_namespace, current_code)
    WHERE active AND current_code IS NOT NULL;

CREATE INDEX stop_platform_group_idx ON core.stop_platform (stop_group_id);

CREATE INDEX stop_platform_geom_idx ON core.stop_platform USING gist (geom);

CREATE TABLE core.stop_link (
    feed_version_id text NOT NULL REFERENCES core.feed_version (id) ON DELETE CASCADE,
    stop_id text NOT NULL,
    stop_platform_id text NOT NULL REFERENCES core.stop_platform (id),
    PRIMARY KEY (feed_version_id, stop_id)
);

CREATE INDEX stop_link_platform_idx ON core.stop_link (stop_platform_id);

CREATE TABLE core.shape_line (
    feed_version_id text NOT NULL REFERENCES core.feed_version (id) ON DELETE CASCADE,
    shape_id text NOT NULL,
    geom geometry(LineString, 4326) NOT NULL,
    PRIMARY KEY (feed_version_id, shape_id)
);

CREATE INDEX shape_line_geom_idx ON core.shape_line USING gist (geom);

-- Where trips go from a platform: the next stop group on the trip, per route and headsign, with trip counts.
-- Built at import (static/derive.py) for the stop search's "towards ..." hints.
CREATE TABLE core.platform_direction (
    feed_version_id text NOT NULL REFERENCES core.feed_version (id) ON DELETE CASCADE,
    stop_platform_id text NOT NULL REFERENCES core.stop_platform (id),
    next_group_id text NOT NULL REFERENCES core.stop_group (id),
    route_id text NOT NULL,
    headsign text NOT NULL,
    trips integer NOT NULL,
    PRIMARY KEY (feed_version_id, stop_platform_id, next_group_id, route_id, headsign)
);
"""

# Generated from gtfs_backend.static.spec.all_parent_ddl() and frozen here. One partition per feed version.
# Later spec changes need a new migration (tests/test_static_import.py checks DB columns == spec).
GTFS_SQL = """
CREATE TABLE gtfs.agency (
    feed_version_id text NOT NULL,
    agency_id text,
    agency_name text NOT NULL,
    agency_url text NOT NULL,
    agency_timezone text NOT NULL,
    agency_lang text,
    agency_phone text,
    agency_fare_url text,
    agency_email text,
    cemv_support smallint
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.stops (
    feed_version_id text NOT NULL,
    stop_id text NOT NULL,
    stop_code text,
    stop_name text,
    tts_stop_name text,
    stop_desc text,
    stop_lat double precision,
    stop_lon double precision,
    zone_id text,
    stop_url text,
    location_type smallint,
    parent_station text,
    stop_timezone text,
    wheelchair_boarding smallint,
    level_id text,
    platform_code text,
    stop_access smallint,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)) STORED,
    PRIMARY KEY (feed_version_id, stop_id)
) PARTITION BY LIST (feed_version_id);

CREATE INDEX stops_idx1 ON gtfs.stops USING gist (geom);

CREATE INDEX stops_idx2 ON gtfs.stops (feed_version_id, stop_code);

CREATE TABLE gtfs.routes (
    feed_version_id text NOT NULL,
    route_id text NOT NULL,
    agency_id text,
    route_short_name text,
    route_long_name text,
    route_desc text,
    route_type integer NOT NULL,
    route_url text,
    route_color text,
    route_text_color text,
    route_sort_order integer,
    continuous_pickup smallint,
    continuous_drop_off smallint,
    network_id text,
    cemv_support smallint,
    PRIMARY KEY (feed_version_id, route_id)
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.trips (
    feed_version_id text NOT NULL,
    route_id text NOT NULL,
    service_id text NOT NULL,
    trip_id text NOT NULL,
    trip_headsign text,
    trip_short_name text,
    direction_id smallint,
    block_id text,
    shape_id text,
    wheelchair_accessible smallint,
    bikes_allowed smallint,
    cars_allowed smallint,
    PRIMARY KEY (feed_version_id, trip_id)
) PARTITION BY LIST (feed_version_id);

CREATE INDEX trips_idx1 ON gtfs.trips (feed_version_id, service_id);

CREATE INDEX trips_idx2 ON gtfs.trips (feed_version_id, route_id);

CREATE INDEX trips_idx3 ON gtfs.trips (feed_version_id, block_id);

CREATE TABLE gtfs.stop_times (
    feed_version_id text NOT NULL,
    trip_id text NOT NULL,
    arrival_time interval,
    departure_time interval,
    stop_id text,
    location_group_id text,
    location_id text,
    stop_sequence integer NOT NULL,
    stop_headsign text,
    start_pickup_drop_off_window interval,
    end_pickup_drop_off_window interval,
    pickup_type smallint,
    drop_off_type smallint,
    continuous_pickup smallint,
    continuous_drop_off smallint,
    shape_dist_traveled double precision,
    timepoint smallint,
    pickup_booking_rule_id text,
    drop_off_booking_rule_id text,
    PRIMARY KEY (feed_version_id, trip_id, stop_sequence)
) PARTITION BY LIST (feed_version_id);

CREATE INDEX stop_times_idx1 ON gtfs.stop_times (feed_version_id, stop_id, departure_time);

CREATE TABLE gtfs.calendar (
    feed_version_id text NOT NULL,
    service_id text NOT NULL,
    monday smallint NOT NULL,
    tuesday smallint NOT NULL,
    wednesday smallint NOT NULL,
    thursday smallint NOT NULL,
    friday smallint NOT NULL,
    saturday smallint NOT NULL,
    sunday smallint NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    PRIMARY KEY (feed_version_id, service_id)
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.calendar_dates (
    feed_version_id text NOT NULL,
    service_id text NOT NULL,
    date date NOT NULL,
    exception_type smallint NOT NULL,
    PRIMARY KEY (feed_version_id, service_id, date)
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.shapes (
    feed_version_id text NOT NULL,
    shape_id text NOT NULL,
    shape_pt_lat double precision NOT NULL,
    shape_pt_lon double precision NOT NULL,
    shape_pt_sequence integer NOT NULL,
    shape_dist_traveled double precision,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)) STORED,
    PRIMARY KEY (feed_version_id, shape_id, shape_pt_sequence)
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.feed_info (
    feed_version_id text NOT NULL,
    feed_publisher_name text NOT NULL,
    feed_publisher_url text NOT NULL,
    feed_lang text NOT NULL,
    default_lang text,
    feed_start_date date,
    feed_end_date date,
    feed_version text,
    feed_contact_email text,
    feed_contact_url text
) PARTITION BY LIST (feed_version_id);

CREATE TABLE gtfs.blocks (
    feed_version_id text NOT NULL,
    block_id text NOT NULL,
    shift text,
    PRIMARY KEY (feed_version_id, block_id)
) PARTITION BY LIST (feed_version_id);
"""


def _run(sql: str) -> None:
    for stmt in sql.split(";\n"):
        if stmt.strip():
            op.execute(stmt)


def upgrade() -> None:
    _run(CORE_SQL)
    _run(GTFS_SQL)


def downgrade() -> None:
    op.execute("DROP SCHEMA gtfs CASCADE")
    op.execute("DROP SCHEMA core CASCADE")
