"""Integrity check: the stored rows must convert back to exactly the published values.

The raw CSV is read independently of the loader (static/source.py) and compared with the loaded
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
NULL_TEXT = {"", " "}  # stored as NULL; the loader also stores its other NA_VALUES (`NA`, `None`, ...) as NULL, which
# this check reports as changed values in the rows it compares: such a feed can fail here (as before; see source.py)
MAX_REPORTED = 10
STREAM_BATCH = 5000  # stored rows fetched at a time when every row is compared
SAMPLE_CHUNK = 5000  # sampled rows looked up at a time


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

    def canonical(row: dict) -> dict:
        return {c: canonical_raw(table.column(c).kind, row.get(c)) for c in cols}

    if table.primary_key and every == 1:
        return _compare_every_row(conn, table, part, zip_path, canonical)
    if table.primary_key:
        return _compare_sample(conn, table, part, fv.id, zip_path, canonical, every)

    # no primary key (small tables only): every row, compared as a multiset
    raw = [canonical(row) for row in _raw_rows(zip_path, table.filename)]
    if len(raw) != (db_count := _stored_count(conn, part)):
        return _count_problem(table, len(raw), db_count)
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(cols)} FROM {part}")
    stored = [{c: canonical_db(table.column(c).kind, v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]

    def sort_key(r: dict) -> tuple:
        return tuple((v is None, str(v)) for v in r.values())

    return (
        [] if sorted(raw, key=sort_key) == sorted(stored, key=sort_key) else [f"{table.name}: stored rows differ from the file"]
    )


def _stored_count(conn: psycopg.Connection, part: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {part}")
    return cur.fetchone()[0]


def _count_problem(table: Table, raw_count: int, db_count: int) -> list[str]:
    return [f"{table.name}: {raw_count} rows in the file, {db_count} stored"]


def _compare(table: Table, raw: dict, db: dict | None, problems: list[str]) -> None:
    if db is None:
        problems.append(f"{table.name} {_key(table, raw)}: row missing")
    elif db != raw:
        diff = {c: (raw[c], db[c]) for c in table.column_names if raw[c] != db[c]}
        problems.append(f"{table.name} {_key(table, raw)}: file vs stored {diff}")


def _compare_sample(
    conn: psycopg.Connection, table: Table, part: str, fv_id: str, zip_path: Path, canonical, every: int
) -> list[str]:
    """Every `every`th row of the file against the stored row with its key, looked up SAMPLE_CHUNK keys at a time, in
    one pass over the file (which also counts its rows: a count that differs is the only problem reported)."""
    cols, key_cols = table.column_names, list(table.primary_key)
    casts = ["int[]" if table.column(c).kind in (Kind.INT, Kind.SMALLINT) else "text[]" for c in key_cols]
    unnest = ", ".join(f"%s::{cast}" for cast in casts)
    on = " AND ".join(f"t.{c} = k.{c}" for c in key_cols)
    # feed_version_id leads the partition's primary key: with it each key is an index lookup (without: a hash join over
    # every row of the table, per chunk)
    query = (
        f"SELECT {', '.join('t.' + c for c in cols)} FROM {part} t JOIN unnest({unnest}) AS k({', '.join(key_cols)}) "
        f"ON t.feed_version_id = %s AND {on}"
    )
    cur = conn.cursor()
    problems: list[str] = []

    def check(chunk: list[dict]) -> None:
        if len(problems) >= MAX_REPORTED:
            return
        cur.execute(query, [*([r[c] for r in chunk] for c in key_cols), fv_id])
        stored = [{c: canonical_db(table.column(c).kind, v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]
        by_key = {_key(table, r): r for r in stored}
        for raw in chunk:
            _compare(table, raw, by_key.get(_key(table, raw)), problems)
            if len(problems) >= MAX_REPORTED:
                return

    raw_count, chunk = 0, []
    for i, row in enumerate(_raw_rows(zip_path, table.filename)):
        raw_count += 1
        if i % every == 0:
            chunk.append(canonical(row))
            if len(chunk) == SAMPLE_CHUNK:
                check(chunk)
                chunk = []
    if chunk:
        check(chunk)
    if raw_count != (db_count := _stored_count(conn, part)):
        return _count_problem(table, raw_count, db_count)
    return problems[:MAX_REPORTED]


def _compare_every_row(conn: psycopg.Connection, table: Table, part: str, zip_path: Path, canonical) -> list[str]:
    """Every row of the file against the stored one, streamed side by side: a freshly filled partition has its rows in
    file order (COPY order), so neither side is held in memory (it used to be both, all 86k trips of kmk_a twice)."""
    db_count = _stored_count(conn, part)
    cols = table.column_names
    problems: list[str] = []
    raw_count = 0
    with conn.cursor(name=f"integrity_{table.name}") as stored:  # server-side: fetched in batches
        stored.itersize = STREAM_BATCH
        stored.execute(f"SELECT {', '.join(cols)} FROM {part} ORDER BY ctid")
        db_rows = iter(stored)
        for raw_row in _raw_rows(zip_path, table.filename):
            raw_count += 1
            if raw_count > db_count or len(problems) >= MAX_REPORTED:
                continue  # only counted from here on
            raw = canonical(raw_row)
            db = {c: canonical_db(table.column(c).kind, v) for c, v in zip(cols, next(db_rows), strict=True)}
            _compare(table, raw, db if _key(table, db) == _key(table, raw) else None, problems)
    if raw_count != db_count:
        return _count_problem(table, raw_count, db_count)
    return problems


def check_version(conn: psycopg.Connection, fv: FeedVersion, zip_path: Path, tables: list[str]) -> None:
    problems: list[str] = []
    for name in tables:
        problems += check_table(conn, TABLES[name], fv, zip_path)
    if problems:
        raise IntegrityError("; ".join(problems[:MAX_REPORTED]))
