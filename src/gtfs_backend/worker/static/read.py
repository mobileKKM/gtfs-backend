"""Read a GTFS zip with gtfs-kit, keeping every value as the published text.

gtfs-kit parses known columns into numbers and guesses types for columns it doesn't know (which could
turn "01" into 1). For exact round-trips we temporarily tell it to read every spec column as a string;
Postgres does the typed parsing during COPY. Non-standard files (ZTP's blocks.txt) are dropped by
gtfs-kit, so extension tables are read directly with the same CSV options.
"""

from __future__ import annotations

import threading
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import gtfs_kit
import gtfs_kit.constants as gk_constants
import pandas as pd

from gtfs_backend.worker.static.spec import TABLES

# Same CSV options gtfs-kit uses, so extension tables behave like the others.
CSV_OPTIONS = {
    "na_values": ["", " ", "nan", "NaN", "null"],
    "keep_default_na": True,
    "encoding": "utf-8-sig",
}

_dtype_lock = threading.Lock()


@dataclass
class GtfsData:
    tables: dict[str, pd.DataFrame]  # only files present in the zip; columns in spec order
    unknown_files: list[str] = field(default_factory=list)
    unknown_columns: dict[str, list[str]] = field(default_factory=dict)
    missing_columns: dict[str, list[str]] = field(default_factory=dict)

    def get(self, name: str) -> pd.DataFrame | None:
        return self.tables.get(name)


@contextmanager
def _all_columns_as_strings():
    with _dtype_lock:
        saved = {name: dict(cols) for name, cols in gk_constants.DTYPES.items()}
        try:
            for name, table in TABLES.items():
                if not table.extension:
                    gk_constants.DTYPES[name] = dict.fromkeys(table.column_names, "string")
            yield
        finally:
            gk_constants.DTYPES.clear()
            gk_constants.DTYPES.update(saved)


def _zip_members(path: Path) -> dict[str, zipfile.ZipInfo]:
    with zipfile.ZipFile(path) as zf:
        return {PurePosixPath(i.filename).name: i for i in zf.infolist() if not i.is_dir()}


def _read_extension(path: Path, filename: str) -> pd.DataFrame | None:
    with zipfile.ZipFile(path) as zf:
        names = {PurePosixPath(i.filename).name: i.filename for i in zf.infolist()}
        if filename not in names:
            return None
        with zf.open(names[filename]) as fh:
            df = pd.read_csv(fh, dtype="string", **CSV_OPTIONS)
    df.columns = [c.strip() for c in df.columns]
    return df


def read_gtfs(path: Path | str) -> GtfsData:
    path = Path(path)
    members = _zip_members(path)

    with _all_columns_as_strings():
        feed = gtfs_kit.read_feed(path, dist_units="km")

    data = GtfsData(tables={})
    for name, table in TABLES.items():
        df = _read_extension(path, table.filename) if table.extension else getattr(feed, name, None)
        if df is None:
            continue
        cols = [c for c in df.columns]
        extra = [c for c in cols if c not in table.column_names]
        missing = [c for c in table.column_names if c not in cols]
        if extra:
            data.unknown_columns[name] = extra
        if missing:
            data.missing_columns[name] = missing
        out = df.reindex(columns=table.column_names)
        for c in missing:
            out[c] = pd.array([pd.NA] * len(out), dtype="string")
        data.tables[name] = out.astype("string")

    known = {t.filename for t in TABLES.values()}
    data.unknown_files = sorted(n for n in members if n not in known)
    return data
