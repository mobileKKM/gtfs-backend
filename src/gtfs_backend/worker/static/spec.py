"""GTFS table/column/type spec.

Single source of truth for:
- the partitioned parent tables in the `gtfs` schema (see `parent_ddl`, used to write the migration),
- the column lists used for COPY,
- how values are validated and converted back to GTFS text for the integrity check.

Column names are exactly the GTFS names. Types are proper Postgres types, chosen so that every value
converts back to the published value (see the plan's type table).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class Kind(StrEnum):
    TEXT = "text"  # ids, names, urls, colors, ...
    INT = "integer"  # sequences, route_type, sort orders
    SMALLINT = "smallint"  # enums and flags
    FLOAT = "double precision"  # coordinates, distances
    TIME = "interval"  # GTFS time, may exceed 24:00:00
    DATE = "date"  # GTFS date YYYYMMDD


@dataclass(frozen=True)
class Column:
    name: str
    kind: Kind = Kind.TEXT
    required: bool = False  # value must be present in every row


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    required_file: bool = False
    # Extra (derived) columns: name -> SQL definition. Never replace an original column.
    generated: dict[str, str] = field(default_factory=dict)
    # Non-standard extension tables (ZTP's blocks.txt): read like the others, but present with only a header too.
    extension: bool = False

    @property
    def filename(self) -> str:
        return f"{self.name}.txt"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


C = Column
K = Kind

TABLES: dict[str, Table] = {
    t.name: t
    for t in [
        Table(
            "agency",
            (
                C("agency_id"),
                C("agency_name", required=True),
                C("agency_url", required=True),
                C("agency_timezone", required=True),
                C("agency_lang"),
                C("agency_phone"),
                C("agency_fare_url"),
                C("agency_email"),
                C("cemv_support", K.SMALLINT),
            ),
            required_file=True,
        ),
        Table(
            "stops",
            (
                C("stop_id", required=True),
                C("stop_code"),
                C("stop_name"),
                C("tts_stop_name"),
                C("stop_desc"),
                C("stop_lat", K.FLOAT),
                C("stop_lon", K.FLOAT),
                C("zone_id"),
                C("stop_url"),
                C("location_type", K.SMALLINT),
                C("parent_station"),
                C("stop_timezone"),
                C("wheelchair_boarding", K.SMALLINT),
                C("level_id"),
                C("platform_code"),
                C("stop_access", K.SMALLINT),
            ),
            primary_key=("stop_id",),
            required_file=True,
            generated={
                "geom": "geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)) STORED",
            },
        ),
        Table(
            "routes",
            (
                C("route_id", required=True),
                C("agency_id"),
                C("route_short_name"),
                C("route_long_name"),
                C("route_desc"),
                C("route_type", K.INT, required=True),
                C("route_url"),
                C("route_color"),
                C("route_text_color"),
                C("route_sort_order", K.INT),
                C("continuous_pickup", K.SMALLINT),
                C("continuous_drop_off", K.SMALLINT),
                C("network_id"),
                C("cemv_support", K.SMALLINT),
            ),
            primary_key=("route_id",),
            required_file=True,
        ),
        Table(
            "trips",
            (
                C("route_id", required=True),
                C("service_id", required=True),
                C("trip_id", required=True),
                C("trip_headsign"),
                C("trip_short_name"),
                C("direction_id", K.SMALLINT),
                C("block_id"),
                C("shape_id"),
                C("wheelchair_accessible", K.SMALLINT),
                C("bikes_allowed", K.SMALLINT),
                C("cars_allowed", K.SMALLINT),
            ),
            primary_key=("trip_id",),
            required_file=True,
        ),
        Table(
            "stop_times",
            (
                C("trip_id", required=True),
                C("arrival_time", K.TIME),
                C("departure_time", K.TIME),
                C("stop_id"),
                C("location_group_id"),
                C("location_id"),
                C("stop_sequence", K.INT, required=True),
                C("stop_headsign"),
                C("start_pickup_drop_off_window", K.TIME),
                C("end_pickup_drop_off_window", K.TIME),
                C("pickup_type", K.SMALLINT),
                C("drop_off_type", K.SMALLINT),
                C("continuous_pickup", K.SMALLINT),
                C("continuous_drop_off", K.SMALLINT),
                C("shape_dist_traveled", K.FLOAT),
                C("timepoint", K.SMALLINT),
                C("pickup_booking_rule_id"),
                C("drop_off_booking_rule_id"),
            ),
            primary_key=("trip_id", "stop_sequence"),
            required_file=True,
        ),
        Table(
            "calendar",
            (
                C("service_id", required=True),
                C("monday", K.SMALLINT, required=True),
                C("tuesday", K.SMALLINT, required=True),
                C("wednesday", K.SMALLINT, required=True),
                C("thursday", K.SMALLINT, required=True),
                C("friday", K.SMALLINT, required=True),
                C("saturday", K.SMALLINT, required=True),
                C("sunday", K.SMALLINT, required=True),
                C("start_date", K.DATE, required=True),
                C("end_date", K.DATE, required=True),
            ),
            primary_key=("service_id",),
        ),
        Table(
            "calendar_dates",
            (
                C("service_id", required=True),
                C("date", K.DATE, required=True),
                C("exception_type", K.SMALLINT, required=True),
            ),
            primary_key=("service_id", "date"),
        ),
        Table(
            "shapes",
            (
                C("shape_id", required=True),
                C("shape_pt_lat", K.FLOAT, required=True),
                C("shape_pt_lon", K.FLOAT, required=True),
                C("shape_pt_sequence", K.INT, required=True),
                C("shape_dist_traveled", K.FLOAT),
            ),
            primary_key=("shape_id", "shape_pt_sequence"),
            generated={
                "geom": "geometry(Point, 4326) GENERATED ALWAYS AS "
                "(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)) STORED",
            },
        ),
        Table(
            "feed_info",
            (
                C("feed_publisher_name", required=True),
                C("feed_publisher_url", required=True),
                C("feed_lang", required=True),
                C("default_lang"),
                C("feed_start_date", K.DATE),
                C("feed_end_date", K.DATE),
                C("feed_version"),
                C("feed_contact_email"),
                C("feed_contact_url"),
            ),
        ),
        # ZTP tram feed extension: block_id -> shift (kept as published).
        Table(
            "blocks",
            (C("block_id", required=True), C("shift")),
            primary_key=("block_id",),
            extension=True,
        ),
    ]
}

# Order matters only for readability of logs; COPY order is irrelevant (single transaction).
TABLE_ORDER = list(TABLES)

# Secondary indexes on the partitioned parents (created once, inherited by every partition).
INDEXES: dict[str, list[str]] = {
    "stops": ["USING gist (geom)", "(feed_version_id, stop_code)"],
    "trips": ["(feed_version_id, service_id)", "(feed_version_id, route_id)", "(feed_version_id, block_id)"],
    "stop_times": ["(feed_version_id, stop_id, departure_time)"],
}


def parent_ddl(table: Table) -> list[str]:
    """CREATE statements for the partitioned parent of a GTFS table (one partition per feed version)."""
    cols = ["feed_version_id text NOT NULL"]
    for c in table.columns:
        cols.append(f"{c.name} {c.kind.value}{' NOT NULL' if c.required else ''}")
    for name, definition in table.generated.items():
        cols.append(f"{name} {definition}")
    if table.primary_key:
        cols.append(f"PRIMARY KEY (feed_version_id, {', '.join(table.primary_key)})")
    body = ",\n    ".join(cols)
    stmts = [f"CREATE TABLE gtfs.{table.name} (\n    {body}\n) PARTITION BY LIST (feed_version_id)"]
    for i, idx in enumerate(INDEXES.get(table.name, []), start=1):
        stmts.append(f"CREATE INDEX {table.name}_idx{i} ON gtfs.{table.name} {idx}")
    return stmts


def all_parent_ddl() -> list[str]:
    return [stmt for t in TABLES.values() for stmt in parent_ddl(t)]


def partition_name(table: str, feed_id: str, version: int) -> str:
    """Partition holding one feed version of a GTFS table, named by feed and per-feed version: `stop_times__kmk_t__v3`."""
    return f"{table}__{feed_id}__v{version}"
