"""The timetable snapshot (gtfs_backend/snapshot.py) as boards and trip views read it: departures of some poles in a
time window, and one trip's stops and shape. Used for a day only while the snapshot has the version the catalog has for
it; otherwise (a date past tomorrow, a version just imported) callers read Postgres as before.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np

from gtfs_backend.api.data.catalog import Day
from gtfs_backend.snapshot import NO_TIME, Snapshot

NOT_COVERED = object()  # the snapshot can't say (another version or date): ask Postgres


@dataclass(frozen=True)
class SnapshotDeparture:
    day: Day
    trip_id: str
    stop_sequence: int
    public_stop_id: str
    epoch: int
    headsign: str | None
    route_id: str
    on_demand: bool


@dataclass(frozen=True)
class TripStatic:
    """What doesn't change while a trip runs: as VIEW_SQL/LIVE_SQL (services/trips.py) read it."""

    route_id: str
    headsign: str | None
    direction_id: int | None
    polyline: str | None
    bbox: list[float] | None
    # (stop_sequence, arrival s, departure s (None: none), on_demand, public_stop_id or None), in stop order
    stops: list[tuple[int, int | None, int | None, bool, str | None]]


class Timetable:
    def __init__(self, snapshot: Snapshot):
        self.snapshot = snapshot
        s = snapshot.strings
        self._poles, self._trip_ids, self._routes, self._headsigns = s["poles"], s["trip_ids"], s["routes"], s["headsigns"]
        c = snapshot.columns
        self._offsets = c["dep_pole_offsets"]
        self._epoch, self._day, self._trip, self._seq = c["dep_epoch"], c["dep_day"], c["dep_trip"], c["dep_sequence"]
        self._dep_headsign, self._on_demand = c["dep_headsign"], c["dep_on_demand"]
        self._trip_id_col, self._trip_route = c["trip_id"], c["trip_route"]

    @property
    def generation(self) -> str:
        return self.snapshot.generation

    def days_for(self, days: list[Day]) -> dict[int, Day] | None:
        """Snapshot day index -> catalog Day, for each of `days`; None if the snapshot lacks one (or has another
        version of it), so the caller reads Postgres."""
        out = {}
        for d in days:
            i = self.snapshot.day(d.feed_id, d.date, d.feed_version_id)
            if i is None:
                return None
            out[i] = d
        return out

    def departures(self, days: dict[int, Day], public_stop_ids: list[str], lo: int, hi: int) -> list[SnapshotDeparture]:
        """Departures of these days at these poles with lo <= time <= hi (epochs): what scheduled() reads from SQL."""
        out = []
        for pid in public_stop_ids:
            p = self.snapshot.pole_index.get(pid)
            if p is None:
                continue
            a, b = int(self._offsets[p]), int(self._offsets[p + 1])
            if a == b:
                continue
            times = self._epoch[a:b]
            i = a + int(np.searchsorted(times, lo, side="left"))
            j = a + int(np.searchsorted(times, hi, side="right"))
            if i >= j:
                continue
            rows = zip(
                self._day[i:j].tolist(),
                self._trip[i:j].tolist(),
                self._seq[i:j].tolist(),
                self._epoch[i:j].tolist(),
                self._dep_headsign[i:j].tolist(),
                self._on_demand[i:j].tolist(),
                strict=True,
            )
            for day_i, trip, seq, epoch, headsign, on_demand in rows:
                if (day := days.get(day_i)) is None:
                    continue
                out.append(
                    SnapshotDeparture(
                        day,
                        self._trip_ids[int(self._trip_id_col[trip])],
                        seq,
                        pid,
                        epoch,
                        self._headsigns[headsign] if headsign >= 0 else None,
                        self._routes[int(self._trip_route[trip])],
                        bool(on_demand),
                    )
                )
        return out

    def trip(self, feed_id: str, d: date, feed_version_id: str, trip_id: str):
        """The trip on that service day: TripStatic; None if it doesn't run that day; NOT_COVERED if the snapshot
        doesn't have the day in that version."""
        s = self.snapshot
        day_i = s.day(feed_id, d, feed_version_id)
        if day_i is None:
            return NOT_COVERED
        row = s.trip_index.get((feed_version_id, trip_id))
        c = s.columns
        if row is None or int(c["trip_service"][row]) not in s.days[day_i].services:
            return None
        headsign, direction, shape = int(c["trip_headsign"][row]), int(c["trip_direction"][row]), int(c["trip_shape"][row])
        a, b = int(c["trip_stop_offsets"][row]), int(c["trip_stop_offsets"][row + 1])
        stops = []
        for seq, arr, dep, on_demand, pole in zip(
            c["ts_sequence"][a:b].tolist(),
            c["ts_arrival"][a:b].tolist(),
            c["ts_departure"][a:b].tolist(),
            c["ts_on_demand"][a:b].tolist(),
            c["ts_pole"][a:b].tolist(),
            strict=True,
        ):
            stops.append(
                (
                    seq,
                    None if arr == NO_TIME else arr,
                    None if dep == NO_TIME else dep,
                    bool(on_demand),
                    self._poles[pole] if pole >= 0 else None,
                )
            )
        polyline = bbox = None
        if shape >= 0:
            _, _, polyline, bbox = s.shapes[shape]
        return TripStatic(
            route_id=self._routes[int(c["trip_route"][row])],
            headsign=self._headsigns[headsign] if headsign >= 0 else None,
            direction_id=None if direction < 0 else direction,
            polyline=polyline,
            bbox=bbox,
            stops=stops,
        )
