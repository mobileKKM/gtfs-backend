"""Integrity check: the stored rows must convert back to exactly the published values.

The raw CSV is read independently of gtfs-kit (plain `csv` module) and compared with the loaded
partition: row counts for every table, and values for every row of small tables plus a deterministic
sample (every Nth row) of stop_times and shapes. Values are compared by meaning: times as seconds,
dates as YYYYMMDD, numbers numerically, NULL == empty field.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path, PurePosixPath

import psycopg

from gtfs_backend.worker.static.partitions import FeedVersion
from gtfs_backend.worker.static.spec import TABLES, Kind, Table

SAMPLED_TABLES = {"stop_times": 50, "shapes": 50}  # table -> compare every Nth row
NULL_TEXT = {"", " "}  # empty and single-space fields are stored as NULL (gtfs-kit na_values)
MAX_REPORTED = 10


class IntegrityError(Exception):
    pass


def _raw_rows(zip_path: Path, filename: str) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(zip_path) as zf:
        names = {PurePosixPath(i.filename).name: i.filename for i in zf.infolist()}
        with zf.open(names[filename]) as fh:
            reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline=""))
            header = [h.strip() for h in next(reader)]
            for row in reader:
                if not row or all(v == "" for v in row) and len(row) <= 1:
                    continue
                yield dict(zip(header, row, strict=False))


def _time_secs(text: str) -> int:
    h, m, s = text.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def canonical_raw(kind: Kind, value: str | None):
    if value is None or value in NULL_TEXT:
        return None
    if kind == Kind.TEXT:
        return value
    v = value.strip()
    if kind in (Kind.INT, Kind.SMALLINT):
        return int(v)
    if kind == Kind.FLOAT:
        return float(v)
    if kind == Kind.TIME:
        return _time_secs(v)
    if kind == Kind.DATE:
        return v
    raise ValueError(kind)


def canonical_db(kind: Kind, value):
    if value is None:
        return None
    if kind == Kind.TIME:
        assert isinstance(value, timedelta)
        return int(value.total_seconds())
    if kind == Kind.DATE:
        assert isinstance(value, date)
        return value.strftime("%Y%m%d")
    if kind == Kind.FLOAT:
        return float(value)
    return value


def _key(table: Table, values: dict) -> tuple:
    return tuple(values[c] for c in table.primary_key)


def check_table(conn: psycopg.Connection, table: Table, fv: FeedVersion, zip_path: Path) -> list[str]:
    part = f"gtfs.{fv.partition(table.name)}"
    cols = table.column_names
    every = SAMPLED_TABLES.get(table.name, 1)
    problems: list[str] = []

    raw_count = 0
    sample: list[dict] = []
    for i, row in enumerate(_raw_rows(zip_path, table.filename)):
        raw_count += 1
        if i % every == 0:
            sample.append({c: canonical_raw(table.column(c).kind, row.get(c)) for c in cols})

    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {part}")
    db_count = cur.fetchone()[0]
    if db_count != raw_count:
        return [f"{table.name}: {raw_count} rows in the file, {db_count} stored"]

    select = ", ".join(cols)
    if table.primary_key and every > 1:
        # fetch only the sampled keys
        key_cols = list(table.primary_key)
        arrays = [[r[c] for r in sample] for c in key_cols]
        casts = ["int[]" if table.column(c).kind in (Kind.INT, Kind.SMALLINT) else "text[]" for c in key_cols]
        unnest = ", ".join(f"%s::{cast}" for cast in casts)
        on = " AND ".join(f"t.{c} = k.{c}" for c in key_cols)
        cur.execute(
            f"SELECT {', '.join('t.' + c for c in cols)} FROM {part} t JOIN unnest({unnest}) AS k({', '.join(key_cols)}) ON {on}",
            arrays,
        )
    else:
        cur.execute(f"SELECT {select} FROM {part}")
    stored = [{c: canonical_db(table.column(c).kind, v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]

    if table.primary_key:
        by_key = {_key(table, r): r for r in stored}
        for raw in sample:
            db = by_key.get(_key(table, raw))
            if db is None:
                problems.append(f"{table.name} {_key(table, raw)}: row missing")
            elif db != raw:
                diff = {c: (raw[c], db[c]) for c in cols if raw[c] != db[c]}
                problems.append(f"{table.name} {_key(table, raw)}: file vs stored {diff}")
            if len(problems) >= MAX_REPORTED:
                break
    else:

        def sort_key(r: dict) -> tuple:
            return tuple((v is None, str(v)) for v in r.values())

        if sorted(sample, key=sort_key) != sorted(stored, key=sort_key):
            problems.append(f"{table.name}: stored rows differ from the file")
    return problems


def check_version(conn: psycopg.Connection, fv: FeedVersion, zip_path: Path, tables: list[str]) -> None:
    problems: list[str] = []
    for name in tables:
        problems += check_table(conn, TABLES[name], fv, zip_path)
    if problems:
        raise IntegrityError("; ".join(problems[:MAX_REPORTED]))
