"""A GTFS zip read row by row, every value as the published text (Postgres does the typed parsing during COPY).

Only the current row is in memory: an import never holds a whole table (stop_times: ~2M rows). The import used to read
zips with gtfs-kit into pandas, peaking near 1 GB; this reads them exactly as that did:

- spec files are found by name (`stops.txt`, in any folder; the last one wins); an empty one, or one with only a
  header, counts as absent. Extension files (`blocks.txt`) count as present with only a header too
- `utf-8-sig` (a BOM is dropped), CSV with `"` quoting; column names are stripped of whitespace, values are not
- blank lines are skipped; a row with fewer fields than the header has NULL for the rest, one with more is an error
- NA_VALUES become NULL: gtfs-kit's list plus pandas' defaults, so `NA` and `None` are NULL too (inherited behaviour)
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

from gtfs_backend.worker.static.spec import TABLES

# gtfs-kit's na_values plus pandas' default NA strings (keep_default_na=True), matched exactly (not stripped)
NA_VALUES = frozenset(
    {"", " ", "nan", "NaN", "null"}
    | {"#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA"}
    | {"NULL", "None", "n/a"}
)


class SourceError(ValueError):
    """The zip can't be read as GTFS CSV (e.g. a row with more fields than the header)."""


Row = list[str | None]


def _reader(fh) -> Iterator[list[str]]:
    return csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline=""))


def _column_names(header: list[str]) -> list[str]:
    """Stripped names; pandas names an empty one `Unnamed: {position}`."""
    return [name.strip() or f"Unnamed: {i}" for i, name in enumerate(header)]


class GtfsSource:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._members: dict[str, str] = {}  # table -> member name in the zip
        self._headers: dict[str, list[str]] = {}
        self.unknown_columns: dict[str, list[str]] = {}
        self.missing_columns: dict[str, list[str]] = {}
        with zipfile.ZipFile(self.path) as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            by_basename = {PurePosixPath(i.filename).name: i for i in infos}
            known = {t.filename for t in TABLES.values()}
            self.unknown_files = sorted(n for n in by_basename if n not in known)
            for name, table in TABLES.items():
                if table.extension:
                    candidates = [by_basename[table.filename]] if table.filename in by_basename else []
                else:  # gtfs-kit: every `.txt` member whose stem is the table, in zip order; the last non-empty wins
                    candidates = [
                        i for i in infos if PurePosixPath(i.filename).stem == name and i.filename.endswith(".txt") and i.file_size
                    ]
                for info in reversed(candidates):
                    header, has_rows = self._peek(zf, info.filename)
                    if header is None:  # not even a header (pandas: EmptyDataError)
                        raise SourceError(f"{info.filename}: no columns to parse")
                    if has_rows or table.extension:
                        self._members[name], self._headers[name] = info.filename, header
                        break
        self.tables = [name for name in TABLES if name in self._members]
        for name in self.tables:
            cols, spec = self._headers[name], TABLES[name].column_names
            if extra := [c for c in cols if c not in spec]:
                self.unknown_columns[name] = extra
            if missing := [c for c in spec if c not in cols]:
                self.missing_columns[name] = missing

    @staticmethod
    def _peek(zf: zipfile.ZipFile, member: str) -> tuple[list[str] | None, bool]:
        """(column names, whether a data row follows); None for a file without a header."""
        with zf.open(member) as fh:
            reader = _reader(fh)
            header = next((r for r in reader if r), None)
            if header is None:
                return None, False
            return _column_names(header), any(r for r in reader)

    def rows(self, name: str) -> Iterator[Row]:
        """The table's rows, values in spec column order (TABLES[name].column_names); None for NULL."""
        header = self._headers[name]
        width = len(header)
        positions = [header.index(c) if c in header else None for c in TABLES[name].column_names]
        with zipfile.ZipFile(self.path) as zf, zf.open(self._members[name]) as fh:
            reader = _reader(fh)
            next(r for r in reader if r)  # the header
            for row in reader:
                if not row:
                    continue
                if len(row) > width:
                    raise SourceError(
                        f"{TABLES[name].filename}: expected {width} fields in line {reader.line_num}, saw {len(row)}"
                    )
                yield [None if p is None or p >= len(row) or row[p] in NA_VALUES else row[p] for p in positions]

    def records(self, name: str) -> list[dict[str, str | None]]:
        """A small table's rows as dicts (agency, feed_info); none if the table is absent."""
        if name not in self._members:
            return []
        cols = TABLES[name].column_names
        return [dict(zip(cols, row, strict=True)) for row in self.rows(name)]
