"""CPU of the paths that moved from Postgres into memory (phase 2): a board's timetable rows from the snapshot, and
stop search. Synthetic snapshot of production size: 1,500 poles x 900 departures over three days (~1.35 M rows)."""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from gtfs_backend.api.data.catalog import Day
from gtfs_backend.api.data.search import StopSearch
from gtfs_backend.api.data.timetable import Timetable
from gtfs_backend.snapshot import COLUMNS, FORMAT, Snapshot
from gtfs_backend.worker.static.snapshot import write

FIXTURES = Path(__file__).parents[2] / "tests" / "fixtures" / "static"
POLES, PER_POLE, TRIPS = 1500, 900, 30000
BASE = 1_795_000_000
DAYS = [date(2026, 11, 25), date(2026, 11, 26), date(2026, 11, 27)]


@pytest.fixture(scope="module")
def timetable(tmp_path_factory):
    rng = np.random.default_rng(1)
    pole = np.repeat(np.arange(POLES), PER_POLE)
    day = rng.integers(0, 3, len(pole)).astype(np.int16)
    epoch = BASE + day.astype(np.int64) * 86400 + rng.integers(4 * 3600, 26 * 3600, len(pole))
    order = np.lexsort((epoch, pole))
    columns = {name: np.zeros(0, dtype) for name, dtype in COLUMNS.items()}
    columns |= {
        "trip_version": np.zeros(TRIPS, np.int16),
        "trip_id": np.arange(TRIPS, dtype=np.int32),
        "trip_route": rng.integers(0, 200, TRIPS).astype(np.int32),
        "trip_headsign": rng.integers(0, 300, TRIPS).astype(np.int32),
        "trip_direction": np.zeros(TRIPS, np.int8),
        "trip_shape": np.full(TRIPS, -1, np.int32),
        "trip_service": np.zeros(TRIPS, np.int32),
        "trip_stop_offsets": np.zeros(TRIPS + 1, np.int64),
        "dep_pole_offsets": np.searchsorted(pole[order], np.arange(POLES + 1)).astype(np.int64),
        "dep_epoch": epoch[order],
        "dep_day": day[order],
        "dep_trip": rng.integers(0, TRIPS, len(pole)).astype(np.int32)[order],
        "dep_sequence": rng.integers(1, 40, len(pole)).astype(np.int32)[order],
        "dep_headsign": rng.integers(0, 300, len(pole)).astype(np.int32)[order],
        "dep_on_demand": np.zeros(len(pole), np.bool_),
    }
    days = [
        {
            "feed_id": "kmk_a",
            "date": d.isoformat(),
            "feed_version_id": "kmk_a_v3",
            "version": 3,
            "base": BASE + i * 86400,
            "services": [0],
        }
        for i, d in enumerate(DAYS)
    ]
    built = {
        "manifest": {"format": FORMAT, "days": days, "covered": {"kmk_a": [d.isoformat() for d in DAYS]}, "counts": {}},
        "strings": {
            "poles": [f"p{i:04d}" for i in range(POLES)],
            "trip_ids": [f"block_{i}_trip_1_service_1" for i in range(TRIPS)],
            "routes": [f"r{i}" for i in range(200)],
            "headsigns": [f"Headsign {i}" for i in range(300)],
            "services": ["service_1"],
            "versions": ["kmk_a_v3"],
        },
        "shapes": [],
        "columns": columns,
    }
    folder = write(tmp_path_factory.mktemp("snapshot"), built)
    return Timetable(Snapshot(folder)), [Day("kmk_a", d, "kmk_a_v3", 3, BASE + i * 86400) for i, d in enumerate(DAYS)]


def test_board_rows_from_the_snapshot(benchmark, timetable):
    """A board of 8 poles, 30 min plus the hour back: the rows scheduled() used to read from Postgres (~2-3 ms)."""
    tt, days = timetable
    by_index = tt.days_for(days)
    pids = [f"p{i:04d}" for i in range(100, 108)]
    start = BASE + 86400 + 8 * 3600
    rows = benchmark(tt.departures, by_index, pids, start - 3600, start + 1800)
    assert 100 < len(rows) < 1000


@pytest.fixture(scope="module")
def stop_search():
    names = set()
    for zip_name in ("GTFS_KRK_A.zip", "GTFS_KRK_M.zip", "GTFS_KRK_T.zip"):
        with zipfile.ZipFile(FIXTURES / zip_name) as zf:
            names |= {r["stop_name"] for r in csv.DictReader(io.TextIOWrapper(zf.open("stops.txt"), encoding="utf-8-sig"))}
    return StopSearch([(f"g{i:05d}", n) for i, n in enumerate(sorted(names))])


@pytest.mark.parametrize("query", ["a", "rondo", "kurdwanow", "rondo mog", "brnowice"])
def test_stop_search(benchmark, stop_search, query):
    """Stop search over the fixtures' stop names: short (many prefix hits), a word, two words, a typo (fuzzy only)."""
    benchmark(stop_search.search, query, 10)
