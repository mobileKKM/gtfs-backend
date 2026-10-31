"""Build the trimmed static fixtures from full ZTP zips (dev tool, output is committed).

    uv run python tests/fixtures/make_fixtures.py GTFS_KRK_A.zip GTFS_KRK_M.zip GTFS_KRK_T.zip

Keeps only the trips referenced by the 2026-10-25 realtime snapshot (plus, for trams, every Sunday trip
between 15:00 and 21:30, so trips an hour either side of the reported ones exist), and everything those trips reference.
Rows are copied as raw CSV text, so the fixtures keep the published formatting.
"""

from __future__ import annotations

import csv
import io
import sys
import zipfile
from pathlib import Path, PurePosixPath

from google.transit import gtfs_realtime_pb2 as pb

HERE = Path(__file__).parent
SNAPSHOT = HERE / "dst_2026-10-25"
OUT = HERE / "static"


def read_csv(zf: zipfile.ZipFile, name: str) -> tuple[list[str], list[list[str]]] | None:
    names = {PurePosixPath(i.filename).name: i.filename for i in zf.infolist()}
    if name not in names:
        return None
    with zf.open(names[name]) as fh:
        rows = list(csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline="")))
    return rows[0], rows[1:]


def snapshot_trips(suffix: str) -> set[str]:
    trips = set()
    for kind in ("TripUpdates", "VehiclePositions"):
        msg = pb.FeedMessage()
        msg.ParseFromString((SNAPSHOT / f"{kind}_{suffix}.pb").read_bytes())
        for e in msg.entity:
            t = e.trip_update.trip if e.HasField("trip_update") else e.vehicle.trip
            if t.trip_id:
                trips.add(t.trip_id)
    return trips


def secs(t: str) -> int:
    h, m, s = t.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def trim(src: Path, suffix: str) -> Path:
    keep_trips = snapshot_trips(suffix)
    with zipfile.ZipFile(src) as zf:
        tables = {
            n: read_csv(zf, f"{n}.txt")
            for n in (
                "agency",
                "stops",
                "routes",
                "trips",
                "stop_times",
                "calendar",
                "calendar_dates",
                "shapes",
                "feed_info",
                "blocks",
            )
        }
    th, trows = tables["trips"]
    sh, srows = tables["stop_times"]
    ti, si = {c: i for i, c in enumerate(th)}, {c: i for i, c in enumerate(sh)}

    if suffix == "T":  # all Sunday trips in the evening window (clock-change hour either side)
        first: dict[str, int] = {}
        for r in srows:
            dep = secs(r[si["departure_time"]])
            first[r[si["trip_id"]]] = min(first.get(r[si["trip_id"]], dep), dep)
        for r in trows:
            if r[ti["service_id"]] == "service_3" and 15 * 3600 <= first.get(r[ti["trip_id"]], 0) <= 21.5 * 3600:
                keep_trips.add(r[ti["trip_id"]])

    trips = [r for r in trows if r[ti["trip_id"]] in keep_trips]
    stop_times = [r for r in srows if r[si["trip_id"]] in keep_trips]
    services = {r[ti["service_id"]] for r in trips}
    routes = {r[ti["route_id"]] for r in trips}
    shapes = {r[ti["shape_id"]] for r in trips}
    blocks = {r[ti["block_id"]] for r in trips}
    stop_ids = {r[si["stop_id"]] for r in stop_times}

    out: dict[str, tuple[list[str], list[list[str]]]] = {"trips": (th, trips), "stop_times": (sh, stop_times)}
    for name, key, keep in (
        ("routes", "route_id", routes),
        ("calendar", "service_id", services),
        ("calendar_dates", "service_id", services),
        ("blocks", "block_id", blocks),
        ("stops", "stop_id", stop_ids),
    ):
        if tables[name]:
            h, rows = tables[name]
            out[name] = (h, [r for r in rows if r[h.index(key)] in keep])
    if tables["shapes"] and suffix == "T":  # shapes only for trams (keeps the fixtures small)
        h, rows = tables["shapes"]
        out["shapes"] = (h, [r for r in rows if r[h.index("shape_id")] in shapes])
    for name in ("agency", "feed_info"):
        out[name] = tables[name]

    OUT.mkdir(exist_ok=True)
    dest = OUT / f"GTFS_KRK_{suffix}.zip"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, (header, rows) in out.items():
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\r\n" if suffix == "T" else "\n")
            w.writerow(header)
            w.writerows(rows)
            zf.writestr(f"{name}.txt", buf.getvalue())
    print(dest, {k: len(v[1]) for k, v in out.items()})
    return dest


if __name__ == "__main__":
    for path in sys.argv[1:]:
        trim(Path(path), Path(path).stem.rsplit("_", 1)[-1])
