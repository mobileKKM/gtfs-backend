"""Structural validation of a GTFS feed before it is loaded (gtfs-kit 13 has no validator).

Errors fail the import (the current versions stay active); warnings are logged and stored on the version.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from gtfs_backend.worker.static.read import GtfsData
from gtfs_backend.worker.static.spec import TABLES, Kind

TIME_RE = r"^\d{1,3}:[0-5]\d:[0-5]\d$"
DATE_RE = r"^\d{8}$"
INT_RE = r"^-?\d+$"
FLOAT_RE = r"^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$"
SMALLINT_MAX = 32767
INT_MAX = 2147483647


@dataclass
class ValidationResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def _sample(values: pd.Series, n: int = 5) -> list[str]:
    return [str(v) for v in values.head(n).tolist()]


def _check_formats(name: str, df: pd.DataFrame, res: ValidationResult) -> None:
    table = TABLES[name]
    for col in table.columns:
        s = df[col.name]
        present = s.dropna()
        if col.required and s.isna().any():
            res.errors.append(f"{name}.{col.name}: {int(s.isna().sum())} rows without a required value")
        if present.empty or col.kind == Kind.TEXT:
            continue
        pattern = {Kind.TIME: TIME_RE, Kind.DATE: DATE_RE, Kind.INT: INT_RE, Kind.SMALLINT: INT_RE, Kind.FLOAT: FLOAT_RE}[
            col.kind
        ]
        bad = present[~present.str.strip().str.match(pattern)]
        if not bad.empty:
            res.errors.append(
                f"{name}.{col.name}: {len(bad)} values are not a valid {col.kind.name.lower()}, e.g. {_sample(bad)}"
            )
            continue
        if col.kind == Kind.DATE:
            parsed = pd.to_datetime(present, format="%Y%m%d", errors="coerce")
            if parsed.isna().any():
                res.errors.append(f"{name}.{col.name}: invalid calendar dates, e.g. {_sample(present[parsed.isna()])}")
        elif col.kind in (Kind.INT, Kind.SMALLINT):
            limit = SMALLINT_MAX if col.kind == Kind.SMALLINT else INT_MAX
            nums = pd.to_numeric(present, errors="coerce")
            if (nums.abs() > limit).any():
                res.errors.append(f"{name}.{col.name}: values out of range for {col.kind.value}")


def _check_unique(name: str, df: pd.DataFrame, res: ValidationResult) -> None:
    pk = list(TABLES[name].primary_key)
    if not pk or df[pk].isna().any().any():
        return
    dup = df.duplicated(subset=pk, keep=False)
    if dup.any():
        sample = df.loc[dup, pk].head(3).to_dict("records")
        res.errors.append(f"{name}: {int(dup.sum())} rows share a primary key ({', '.join(pk)}), e.g. {sample}")


def _check_ref(
    child: str, col: str, parent_values: set, parent: str, data: GtfsData, res: ValidationResult, *, error: bool = True
) -> None:
    df = data.get(child)
    if df is None:
        return
    values = df[col].dropna()
    missing = values[~values.isin(parent_values)]
    if not missing.empty:
        msg = (
            f"{child}.{col}: {len(missing)} rows reference unknown {parent} ({missing.nunique()} distinct), "
            f"e.g. {_sample(missing.drop_duplicates())}"
        )
        (res.errors if error else res.warnings).append(msg)


def validate(data: GtfsData) -> ValidationResult:
    res = ValidationResult()

    for name, table in TABLES.items():
        if table.required_file and name not in data.tables:
            res.errors.append(f"missing required file {table.filename}")
    if "calendar" not in data.tables and "calendar_dates" not in data.tables:
        res.errors.append("missing calendar.txt and calendar_dates.txt (one is required)")
    if res.errors:
        return res

    for name, cols in data.unknown_columns.items():
        res.warnings.append(f"{name}: columns not in the GTFS spec are not stored: {cols}")
    for name in data.unknown_files:
        res.warnings.append(f"file not in the spec is not stored: {name}")

    for name, df in data.tables.items():
        _check_formats(name, df, res)
        _check_unique(name, df, res)

    routes = set(data.tables["routes"]["route_id"].dropna())
    trips = set(data.tables["trips"]["trip_id"].dropna())
    stops = set(data.tables["stops"]["stop_id"].dropna())
    services: set = set()
    for name in ("calendar", "calendar_dates"):
        if name in data.tables:
            services |= set(data.tables[name]["service_id"].dropna())

    _check_ref("trips", "route_id", routes, "routes", data, res)
    _check_ref("trips", "service_id", services, "services", data, res)
    _check_ref("stop_times", "trip_id", trips, "trips", data, res)
    _check_ref("stop_times", "stop_id", stops, "stops", data, res)
    if "shapes" in data.tables:
        _check_ref("trips", "shape_id", set(data.tables["shapes"]["shape_id"].dropna()), "shapes", data, res, error=False)
    if "blocks" in data.tables:
        _check_ref("trips", "block_id", set(data.tables["blocks"]["block_id"].dropna()), "blocks", data, res, error=False)

    agencies = data.tables["agency"]
    if agencies["agency_timezone"].dropna().nunique() != 1:
        res.errors.append(
            f"agency.txt must have exactly one timezone, found {agencies['agency_timezone'].dropna().unique().tolist()}"
        )

    counts = data.tables["stop_times"].groupby("trip_id").size()
    short = counts[counts < 2]
    if not short.empty:
        res.warnings.append(f"{len(short)} trips have fewer than 2 stop_times")
    without = trips - set(counts.index)
    if without:
        res.warnings.append(f"{len(without)} trips have no stop_times")
    return res
