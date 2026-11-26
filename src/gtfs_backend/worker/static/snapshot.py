"""Builds the timetable snapshot (format: gtfs_backend/snapshot.py) from Postgres: each feed's authoritative version on
yesterday, today and tomorrow, its trips running those days, their stop times and shapes, and every departure a board
can show (core.stop_departure), sorted by pole and time.

Runs in the import process (a few seconds for all feeds) after every import and on every new day. Written to a new
folder, then `current` is switched to it in one rename; the previous generation stays (API processes may still map
it), older ones are removed.
"""

from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import psycopg

from gtfs_backend.feeds import FeedsFile, load_feeds
from gtfs_backend.logs import log
from gtfs_backend.servicetime import day_base, nearby_dates
from gtfs_backend.snapshot import COLUMNS, CURRENT, FORMAT, NO_TIME

KEEP_GENERATIONS = 2  # the current one and the one before (an API process may still read it)
ON_DEMAND = (2, 3)  # GTFS pickup/drop_off_type: phone the agency, coordinate with the driver


class Strings:
    """A string table: index of each distinct string, in first-seen order."""

    def __init__(self):
        self.index: dict[str, int] = {}

    def __call__(self, s: str | None) -> int:
        if s is None:
            return -1
        i = self.index.get(s)
        if i is None:
            i = self.index[s] = len(self.index)
        return i

    def list(self) -> list[str]:
        return list(self.index)


def _days(conn: psycopg.Connection, feeds: FeedsFile, now: float) -> tuple[list[dict], dict[str, list[date]]]:
    """Per feed: its authoritative (version, base, services) on yesterday, today and tomorrow, and those dates."""
    days, covered = [], {}
    for feed in feeds.enabled:
        row = conn.execute(
            "SELECT timezone FROM core.feed_version WHERE feed_id = %s AND status = 'ready' ORDER BY version DESC LIMIT 1",
            (feed.feed_id,),
        ).fetchone()
        if row is None:
            continue
        tz = ZoneInfo(row[0] or "UTC")
        dates = nearby_dates(now, tz)
        covered[feed.feed_id] = dates
        rows = conn.execute(
            """
            SELECT s.service_date, fv.id, fv.version,
                   coalesce(array_agg(d.service_id) FILTER (WHERE d.service_id IS NOT NULL), '{}')
            FROM core.service_day_version s
            JOIN core.feed_version fv ON fv.id = s.feed_version_id
            LEFT JOIN core.service_date d ON d.feed_version_id = s.feed_version_id AND d.date = s.service_date
            WHERE s.feed_id = %s AND s.service_date = ANY(%s)
            GROUP BY s.service_date, fv.id, fv.version
            ORDER BY s.service_date
            """,
            (feed.feed_id, dates),
        ).fetchall()
        for d, fv_id, version, services in rows:
            days.append(
                {
                    "feed_id": feed.feed_id,
                    "date": d,
                    "feed_version_id": fv_id,
                    "version": version,
                    "base": day_base(d, tz, feed.day_start),
                    "service_ids": sorted(services),
                }
            )
    return days, covered


def build(conn: psycopg.Connection, feeds: FeedsFile, now: float) -> dict[str, object]:
    """The snapshot's columns and tables (not written yet)."""
    days, covered = _days(conn, feeds, now)
    poles, trip_ids, routes, headsigns, services, versions = (Strings() for _ in range(6))
    shapes_wanted: dict[tuple[str, str], int] = {}  # (version, shape_id) -> index
    trips: dict[tuple[str, str], int] = {}  # (version, trip_id) -> row
    t_cols: dict[str, list] = defaultdict(list)
    stops_of: dict[int, list[tuple]] = {}
    dep_rows: list[tuple[int, int, int, int, int, int, bool]] = []  # pole, epoch, day, trip, seq, headsign, on_demand

    # every version once: its trips running on any of its days, their stop times
    by_version: dict[str, set[str]] = defaultdict(set)
    for d in days:
        by_version[d["feed_version_id"]].update(d["service_ids"])
    for fv_id, service_ids in sorted(by_version.items()):
        v = versions(fv_id)
        cur = conn.execute(
            """
            SELECT trip_id, route_id, trip_headsign, direction_id, shape_id, service_id FROM gtfs.trips
            WHERE feed_version_id = %s AND service_id = ANY(%s) ORDER BY trip_id
            """,
            (fv_id, sorted(service_ids)),
        )
        for trip_id, route_id, headsign, direction, shape_id, service_id in cur.fetchall():
            row = trips[(fv_id, trip_id)] = len(trips)
            shape = shapes_wanted.setdefault((fv_id, shape_id), len(shapes_wanted)) if shape_id else -1
            for name, value in (
                ("trip_version", v),
                ("trip_id", trip_ids(trip_id)),
                ("trip_route", routes(route_id)),
                ("trip_headsign", headsigns(headsign)),
                ("trip_direction", -1 if direction is None else direction),
                ("trip_shape", shape),
                ("trip_service", services(service_id)),
            ):
                t_cols[name].append(value)
            stops_of[row] = []
        cur = conn.execute(
            """
            SELECT st.trip_id, st.stop_sequence,
                   extract(epoch FROM st.arrival_time)::int, extract(epoch FROM st.departure_time)::int,
                   coalesce(st.pickup_type, 0), coalesce(st.drop_off_type, 0), l.public_stop_id
            FROM gtfs.stop_times st
            JOIN gtfs.trips t ON t.feed_version_id = %(v)s AND t.trip_id = st.trip_id AND t.service_id = ANY(%(s)s)
            LEFT JOIN core.stop_link l ON l.feed_version_id = %(v)s AND l.stop_id = st.stop_id
            WHERE st.feed_version_id = %(v)s
            ORDER BY st.trip_id, st.stop_sequence
            """,
            {"v": fv_id, "s": sorted(service_ids)},
        )
        for trip_id, seq, arr, dep, pickup, drop_off, pid in cur.fetchall():
            stops_of[trips[(fv_id, trip_id)]].append((seq, arr, dep, pickup in ON_DEMAND or drop_off in ON_DEMAND, pid))

    # departures, per day (a trip running on two of the days departs on both)
    for day_index, d in enumerate(days):
        fv_id = d["feed_version_id"]
        cur = conn.execute(
            """
            SELECT public_stop_id, departure_s, trip_id, stop_sequence, headsign, on_demand FROM core.stop_departure
            WHERE feed_version_id = %s AND service_id = ANY(%s)
            """,
            (fv_id, d["service_ids"]),
        )
        base = d["base"]
        for pid, departure_s, trip_id, seq, headsign, on_demand in cur.fetchall():
            dep_rows.append(
                (poles(pid), base + departure_s, day_index, trips[(fv_id, trip_id)], seq, headsigns(headsign), on_demand)
            )

    # trip stops, flattened in trip order; poles of stops that have no departure get indexes too
    offsets, ts = [0], defaultdict(list)
    for row in range(len(trips)):
        for seq, arr, dep, on_demand, pid in stops_of[row]:
            ts["ts_sequence"].append(seq)
            ts["ts_arrival"].append(NO_TIME if arr is None else arr)
            ts["ts_departure"].append(NO_TIME if dep is None else dep)
            ts["ts_pole"].append(poles(pid))
            ts["ts_on_demand"].append(on_demand)
        offsets.append(len(ts["ts_sequence"]))

    dep_rows.sort(key=lambda r: (r[0], r[1], r[3], r[4]))
    n_poles = len(poles.index)
    pole_col = np.fromiter((r[0] for r in dep_rows), np.int64, len(dep_rows))
    dep_offsets = np.searchsorted(pole_col, np.arange(n_poles + 1), side="left")

    shape_rows = []
    if shapes_wanted:
        keys = sorted(shapes_wanted, key=shapes_wanted.get)
        cur = conn.execute(
            """
            SELECT s.feed_version_id, s.shape_id, ST_AsEncodedPolyline(s.geom, 5),
                   ARRAY[ST_XMin(s.geom), ST_YMin(s.geom), ST_XMax(s.geom), ST_YMax(s.geom)]
            FROM unnest(%s::text[], %s::text[]) AS w (fv, shape_id)
            JOIN core.shape_line s ON s.feed_version_id = w.fv AND s.shape_id = w.shape_id
            """,
            ([k[0] for k in keys], [k[1] for k in keys]),
        )
        found = {(fv, sid): (poly, bbox) for fv, sid, poly, bbox in cur.fetchall()}
        shape_rows = [[versions(fv), sid, *(found.get((fv, sid)) or (None, None))] for fv, sid in keys]

    columns = {
        **{name: np.asarray(values, COLUMNS[name]) for name, values in t_cols.items()},
        "trip_stop_offsets": np.asarray(offsets, np.int64),
        **{name: np.asarray(values, COLUMNS[name]) for name, values in ts.items()},
        "dep_pole_offsets": dep_offsets.astype(np.int64),
        "dep_epoch": np.fromiter((r[1] for r in dep_rows), np.int64, len(dep_rows)),
        "dep_day": np.fromiter((r[2] for r in dep_rows), np.int16, len(dep_rows)),
        "dep_trip": np.fromiter((r[3] for r in dep_rows), np.int32, len(dep_rows)),
        "dep_sequence": np.fromiter((r[4] for r in dep_rows), np.int32, len(dep_rows)),
        "dep_headsign": np.fromiter((r[5] for r in dep_rows), np.int32, len(dep_rows)),
        "dep_on_demand": np.fromiter((r[6] for r in dep_rows), np.bool_, len(dep_rows)),
    }
    for name, dtype in COLUMNS.items():  # empty snapshot: every column exists all the same
        columns.setdefault(name, np.zeros(0, dtype))
    manifest = {
        "format": FORMAT,
        "built_at": time.time(),
        "built_for": now,
        "days": [
            {
                "feed_id": d["feed_id"],
                "date": d["date"].isoformat(),
                "feed_version_id": d["feed_version_id"],
                "version": d["version"],
                "base": d["base"],
                "services": sorted(services.index[s] for s in d["service_ids"] if s in services.index),
            }
            for d in days
        ],
        "covered": {f: [x.isoformat() for x in dates] for f, dates in covered.items()},
        "counts": {"days": len(days), "trips": len(trips), "stop_times": offsets[-1], "departures": len(dep_rows)},
    }
    strings = {
        "poles": poles.list(),
        "trip_ids": trip_ids.list(),
        "routes": routes.list(),
        "headsigns": headsigns.list(),
        "services": services.list(),
        "versions": versions.list(),
    }
    return {"manifest": manifest, "strings": strings, "shapes": shape_rows, "columns": columns}


def write(root: Path, built: dict) -> Path:
    """A new generation folder, then `current` switched to it; generations before the previous one are removed."""
    root.mkdir(parents=True, exist_ok=True)
    generation = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
    tmp = root / f".tmp-{generation}"
    tmp.mkdir()
    for name, values in built["columns"].items():
        np.save(tmp / f"{name}.npy", values, allow_pickle=False)
    for name in ("strings", "shapes"):
        (tmp / f"{name}.json").write_text(json.dumps(built[name], ensure_ascii=False, separators=(",", ":")))
    (tmp / "manifest.json").write_text(json.dumps(built["manifest"], indent=1))  # last: a folder with it is complete
    folder = root / generation
    tmp.rename(folder)
    link = root / f".{CURRENT}-{generation}"
    link.symlink_to(folder.name)
    os.replace(link, root / CURRENT)  # atomic: readers see the old or the new one
    generations = sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".") and not p.is_symlink())
    for old in generations[:-KEEP_GENERATIONS]:
        shutil.rmtree(old, ignore_errors=True)
    for stale in root.glob(".tmp-*"):  # a build that died half way
        if stale != tmp:
            shutil.rmtree(stale, ignore_errors=True)
    return folder


def run_snapshot_job(database_url: str, feeds_config: str, root: str, now: float, log_level: str = "INFO") -> dict:
    """Entry point for the import process (top-level, for ProcessPoolExecutor). Returns the manifest's counts."""
    from gtfs_backend.db import connect
    from gtfs_backend.logs import setup_logging

    setup_logging(log_level)
    started = time.monotonic()
    with connect(database_url) as conn:
        built = build(conn, load_feeds(feeds_config), now)
    folder = write(Path(root), built)
    counts = built["manifest"]["counts"]
    log.info(
        "snapshot_built",
        generation=folder.name,
        built_for=datetime.fromtimestamp(now).isoformat(timespec="seconds"),
        seconds=round(time.monotonic() - started, 2),
        **counts,
    )
    return {"generation": folder.name, **counts}
