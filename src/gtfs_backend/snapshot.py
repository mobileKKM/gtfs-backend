"""The timetable snapshot: what boards and trip views read instead of Postgres, for yesterday, today and tomorrow.

The worker builds it (worker/static/snapshot.py) after every import and on every new day; the API maps its columns
into memory (np.load, mmap: the page cache holds one copy for every API process) and swaps the whole snapshot at once.

    SNAPSHOT_DIR/current -> {generation}/      symlink, replaced atomically (old generations are removed later)
      manifest.json      days (feed, date, version, base, services), the dates each feed was looked at, counts
      strings.json       poles (public stop ids), trip ids, route ids, headsigns, service ids, versions
      shapes.json        [version index, shape_id, encoded polyline, bbox] of the trips' shapes
      trip_*.npy         one row per (version, trip): its version, id, route, headsign, direction, shape, service;
                         trip_stop_offsets: its stops in ts_* (stop_sequence order)
      ts_*.npy           every stop time of those trips: sequence, arrival/departure (seconds from the day's base,
                         -1: none), pole (-1: no stop of ours), on_demand
      dep_*.npy          every departure a board can show (core.stop_departure) of each day, sorted by (pole, time):
                         epoch, day, trip, stop_sequence, headsign, on_demand; dep_pole_offsets: each pole's rows

A day is a feed's authoritative version on a service date; its trips are those whose service runs that day. Readers
use a day only while it has the version their catalog has for that date (an import may have changed it since).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np

FORMAT = 1
CURRENT = "current"
NO_TIME = -1  # an arrival/departure the stop time doesn't have

COLUMNS = {
    "trip_version": np.int16,
    "trip_id": np.int32,
    "trip_route": np.int32,
    "trip_headsign": np.int32,  # -1: none
    "trip_direction": np.int8,  # -1: none
    "trip_shape": np.int32,  # -1: none
    "trip_service": np.int32,
    "trip_stop_offsets": np.int64,
    "ts_sequence": np.int32,
    "ts_arrival": np.int32,
    "ts_departure": np.int32,
    "ts_pole": np.int32,
    "ts_on_demand": np.bool_,
    "dep_pole_offsets": np.int64,
    "dep_epoch": np.int64,
    "dep_day": np.int16,
    "dep_trip": np.int32,
    "dep_sequence": np.int32,
    "dep_headsign": np.int32,  # -1: none
    "dep_on_demand": np.bool_,
}


@dataclass(frozen=True)
class SnapshotDay:
    feed_id: str
    date: date
    feed_version_id: str
    version: int
    base: int  # epoch the day's GTFS times count from
    services: frozenset[int]  # indexes into strings["services"]


class Snapshot:
    """A built snapshot, read-only. Columns are memory-mapped; strings and indexes are in memory."""

    def __init__(self, folder: Path):
        self.folder = folder
        self.manifest = json.loads((folder / "manifest.json").read_text())
        if self.manifest.get("format") != FORMAT:
            raise ValueError(f"snapshot format {self.manifest.get('format')}, expected {FORMAT}")
        self.strings = json.loads((folder / "strings.json").read_text())
        self.shapes = json.loads((folder / "shapes.json").read_text())
        self.columns = {name: np.load(folder / f"{name}.npy", mmap_mode="r") for name in COLUMNS}
        self.days = [
            SnapshotDay(
                d["feed_id"],
                date.fromisoformat(d["date"]),
                d["feed_version_id"],
                d["version"],
                d["base"],
                frozenset(d["services"]),
            )
            for d in self.manifest["days"]
        ]
        self.day_index = {(d.feed_id, d.date): i for i, d in enumerate(self.days)}
        self.covered = {f: {date.fromisoformat(x) for x in dates} for f, dates in self.manifest["covered"].items()}
        self.pole_index = {p: i for i, p in enumerate(self.strings["poles"])}
        versions = self.strings["versions"]
        trip_ids, trip_versions = self.strings["trip_ids"], self.columns["trip_version"]
        self.trip_index = {
            (versions[v], trip_ids[t]): i
            for i, (v, t) in enumerate(zip(trip_versions.tolist(), self.columns["trip_id"].tolist(), strict=True))
        }

    @property
    def generation(self) -> str:
        return self.folder.name

    def day(self, feed_id: str, d: date, feed_version_id: str) -> int | None:
        """The index of a day with this version, or None (not in the snapshot, or another version)."""
        i = self.day_index.get((feed_id, d))
        return i if i is not None and self.days[i].feed_version_id == feed_version_id else None


def load_current(root: Path) -> Snapshot | None:
    """The snapshot `current` points at, or None if there is none (yet)."""
    link = root / CURRENT
    if not link.exists():
        return None
    return Snapshot(link.resolve())
