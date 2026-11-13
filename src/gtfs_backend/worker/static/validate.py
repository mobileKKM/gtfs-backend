"""Structural validation of a GTFS feed before it is loaded (gtfs-kit 13 has no validator, and isn't used any more).

Errors fail the import (the current versions stay active); warnings are logged and stored on the version.

The feed is read row by row (static/source.py), one pass per table; what is kept between rows is small: the ids that
other tables reference, and per-trip stop counts. Duplicate primary keys of the two big tables (stop_times, shapes)
are found in Postgres after their COPY (duplicate_keys_error), which needs no memory here.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

import psycopg
from psycopg import sql

from gtfs_backend.worker.static.source import GtfsSource
from gtfs_backend.worker.static.spec import TABLES, Kind

TIME_RE = r"^\d{1,3}:[0-5]\d:[0-5]\d$"
DATE_RE = r"^\d{8}$"
INT_RE = r"^-?\d+$"
FLOAT_RE = r"^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$"
SMALLINT_MAX = 32767
INT_MAX = 2147483647
SAMPLE = 5
# primary keys checked in Postgres after COPY instead of in memory
KEYS_IN_DB = frozenset({"stop_times", "shapes"})

_PATTERNS = {Kind.TIME: TIME_RE, Kind.DATE: DATE_RE, Kind.INT: INT_RE, Kind.SMALLINT: INT_RE, Kind.FLOAT: FLOAT_RE}


@dataclass
class ValidationResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def _valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return False
    return True


class _Column:
    """One column's checks, fed value by value."""

    def __init__(self, table: str, column) -> None:
        self.label = f"{table}.{column.name}"
        self.column = column
        self.missing = 0
        self.pattern = None if column.kind == Kind.TEXT else re.compile(_PATTERNS[column.kind])
        self.bad: list[str] = []
        self.bad_count = 0
        self.bad_dates: list[str] = []
        self.out_of_range = False
        self.limit = SMALLINT_MAX if column.kind == Kind.SMALLINT else INT_MAX

    def feed(self, value: str | None) -> None:
        if value is None:
            self.missing += 1
            return
        if self.pattern is None:
            return
        if not self.pattern.match(value.strip()):
            self.bad_count += 1
            if len(self.bad) < SAMPLE:
                self.bad.append(value)
        elif self.column.kind == Kind.DATE:
            if len(self.bad_dates) < SAMPLE and not _valid_date(value):
                self.bad_dates.append(value)
        elif self.column.kind in (Kind.INT, Kind.SMALLINT) and not self.out_of_range:
            self.out_of_range = abs(int(value)) > self.limit

    def errors(self) -> list[str]:
        out = []
        if self.column.required and self.missing:
            out.append(f"{self.label}: {self.missing} rows without a required value")
        if self.bad_count:
            out.append(f"{self.label}: {self.bad_count} values are not a valid {self.column.kind.name.lower()}, e.g. {self.bad}")
        elif self.bad_dates:
            out.append(f"{self.label}: invalid calendar dates, e.g. {self.bad_dates}")
        elif self.out_of_range:
            out.append(f"{self.label}: values out of range for {self.column.kind.value}")
        return out


def _duplicates_message(table: str, pk: list[str], count: int, sample: list[dict]) -> str:
    return f"{table}: {count} rows share a primary key ({', '.join(pk)}), e.g. {sample}"


def _duplicate_keys(table: str, pk_rows: list[tuple]) -> str | None:
    """The duplicate-key error of a small table, from its key tuples in file order (None: all unique)."""
    counts = Counter(pk_rows)
    if len(counts) == len(pk_rows):
        return None
    pk = list(TABLES[table].primary_key)
    dup = [k for k in pk_rows if counts[k] > 1]
    return _duplicates_message(table, pk, len(dup), [dict(zip(pk, k, strict=True)) for k in dup[:3]])


def duplicate_keys_error(cur: psycopg.Cursor, part: sql.Identifier, table: str) -> str | None:
    """The duplicate-key error of a big table (KEYS_IN_DB), from its freshly filled partition: rows in file order are
    in physical (ctid) order there. Values are shown as stored."""
    pk = list(TABLES[table].primary_key)
    cols = sql.SQL(", ").join(map(sql.Identifier, pk))
    cur.execute(
        sql.SQL("SELECT count(*), sum(n) FROM (SELECT count(*) AS n FROM {part} GROUP BY {cols} HAVING count(*) > 1) d").format(
            part=part, cols=cols
        )
    )
    groups, rows = cur.fetchone()
    if not groups:
        return None
    text_cols = sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(c)) for c in pk)
    cur.execute(
        sql.SQL(
            "SELECT {text_cols} FROM {part} WHERE ({cols}) IN "
            "(SELECT {cols} FROM {part} GROUP BY {cols} HAVING count(*) > 1) ORDER BY ctid LIMIT 3"
        ).format(text_cols=text_cols, part=part, cols=cols)
    )
    return _duplicates_message(table, pk, int(rows), [dict(zip(pk, r, strict=True)) for r in cur.fetchall()])


def _ref_message(child: str, col: str, parent: str, values: Counter[str], ids: set[str]) -> str | None:
    """`values`: how often each value occurs in the child column, in order of first appearance (distinct values only,
    so stop_times' trip ids cost their ~100k ids, not ~2M rows)."""
    missing = [v for v in values if v not in ids]
    if not missing:
        return None
    return (
        f"{child}.{col}: {sum(values[v] for v in missing)} rows reference unknown {parent} ({len(missing)} distinct), "
        f"e.g. {missing[:SAMPLE]}"
    )


def validate(source: GtfsSource) -> ValidationResult:
    res = ValidationResult()
    present = set(source.tables)

    for name, table in TABLES.items():
        if table.required_file and name not in present:
            res.errors.append(f"missing required file {table.filename}")
    if "calendar" not in present and "calendar_dates" not in present:
        res.errors.append("missing calendar.txt and calendar_dates.txt (one is required)")
    if res.errors:
        return res

    for name, cols in source.unknown_columns.items():
        res.warnings.append(f"{name}: columns not in the GTFS spec are not stored: {cols}")
    for name in source.unknown_files:
        res.warnings.append(f"file not in the spec is not stored: {name}")

    ids: dict[str, set[str]] = {"routes": set(), "trips": set(), "stops": set(), "services": set()}
    ids_of = {"routes": "route_id", "trips": "trip_id", "stops": "stop_id", "shapes": "shape_id", "blocks": "block_id"}
    # value -> rows, in order of first appearance (Counter keeps insertion order)
    trip_refs: dict[str, Counter[str]] = {c: Counter() for c in ("route_id", "service_id", "shape_id", "block_id")}
    stop_time_refs: dict[str, Counter[str]] = {c: Counter() for c in ("trip_id", "stop_id")}
    timezones: list[str] = []

    for name in source.tables:  # spec order: agency, stops, routes, trips, stop_times, ... (parents of stop_times first)
        table = TABLES[name]
        cols = table.column_names
        checks = [_Column(name, c) for c in table.columns]
        pk_at = [cols.index(c) for c in table.primary_key]
        pk_rows: list[tuple] | None = [] if table.primary_key and name not in KEYS_IN_DB else None
        pk_has_null = False
        id_at = cols.index(ids_of[name]) if name in ids_of else None
        if name in ids_of and name not in ids:
            ids[name] = set()
        service_at = cols.index("service_id") if name in ("calendar", "calendar_dates") else None
        trip_at = {c: cols.index(c) for c in trip_refs} if name == "trips" else {}
        st_at = {c: cols.index(c) for c in stop_time_refs} if name == "stop_times" else {}
        tz_at = cols.index("agency_timezone") if name == "agency" else None

        for row in source.rows(name):
            for check, value in zip(checks, row, strict=True):
                check.feed(value)
            if pk_at:
                key = tuple(row[i] for i in pk_at)
                if None in key:
                    pk_has_null = True
                elif pk_rows is not None:
                    pk_rows.append(key)
            if id_at is not None and row[id_at] is not None:
                ids[name].add(row[id_at])
            if service_at is not None and row[service_at] is not None:
                ids["services"].add(row[service_at])
            for c, i in trip_at.items():
                if row[i] is not None:
                    trip_refs[c][row[i]] += 1
            for c, i in st_at.items():
                if row[i] is not None:
                    stop_time_refs[c][row[i]] += 1
            if tz_at is not None and row[tz_at] is not None:
                timezones.append(row[tz_at])

        for check in checks:
            res.errors += check.errors()
        if pk_rows is not None and not pk_has_null and (dup := _duplicate_keys(name, pk_rows)):
            res.errors.append(dup)

    refs = [
        ("trips", "route_id", "routes", trip_refs["route_id"], ids["routes"], True),
        ("trips", "service_id", "services", trip_refs["service_id"], ids["services"], True),
        ("stop_times", "trip_id", "trips", stop_time_refs["trip_id"], ids["trips"], True),
        ("stop_times", "stop_id", "stops", stop_time_refs["stop_id"], ids["stops"], True),
    ]
    if "shapes" in present:
        refs.append(("trips", "shape_id", "shapes", trip_refs["shape_id"], ids["shapes"], False))
    if "blocks" in present:
        refs.append(("trips", "block_id", "blocks", trip_refs["block_id"], ids["blocks"], False))
    for child, col, parent, values, parent_ids, error in refs:
        if msg := _ref_message(child, col, parent, values, parent_ids):
            (res.errors if error else res.warnings).append(msg)

    distinct_tz = list(dict.fromkeys(timezones))
    if len(distinct_tz) != 1:
        res.errors.append(f"agency.txt must have exactly one timezone, found {distinct_tz}")

    stops_per_trip = stop_time_refs["trip_id"]
    short = sum(1 for n in stops_per_trip.values() if n < 2)
    if short:
        res.warnings.append(f"{short} trips have fewer than 2 stop_times")
    without = ids["trips"] - stops_per_trip.keys()
    if without:
        res.warnings.append(f"{len(without)} trips have no stop_times")
    return res
