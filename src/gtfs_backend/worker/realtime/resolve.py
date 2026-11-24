"""Which static trip instance (timetable version + service day) a realtime trip_id means.

A trip's service must run on one of the index's service days (yesterday/today/tomorrow, each with its serving
version). For trip updates and vehicles "now" must also fall in [first departure - 3 h, last arrival + 60 min],
and the instance running closest to now wins; a `start_date` in the feed picks among the days that qualify.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

from gtfs_backend import keys
from gtfs_backend.worker.realtime.trip_index import ServiceDay, TripIndex, TripInfo, strip_service_suffix

EARLY_WINDOW_S = 3 * 60 * 60  # trip updates carry predictions for trips starting up to ~2 h ahead
LATE_WINDOW_S = 60 * 60


@dataclass(frozen=True)
class TripRef:
    """One trip instance: a static trip on one service day."""

    day: ServiceDay
    info: TripInfo

    def key(self, feed_id: str) -> str:
        """The global trip key (gtfs_backend/keys.py), also the Redis key suffix. A service day has exactly one
        serving version, so trip id + date suffice."""
        return keys.trip_key(feed_id, self.day.date, self.info.trip_id)

    def epoch(self, secs: int | None) -> int | None:
        return None if secs is None else self.day.base + secs

    @property
    def start(self) -> int:
        return self.day.base + self.info.first_departure

    @property
    def end(self) -> int:
        return self.day.base + self.info.last_arrival


def start_date(trip) -> date | None:
    """A TripDescriptor's start_date (YYYYMMDD), if given and valid."""
    if not trip.start_date:
        return None
    try:
        return keys.parse_service_date(trip.start_date)
    except keys.InvalidKey:
        return None


def _candidates(index: TripIndex, trip_id: str, ignore_service_suffix: bool) -> Iterator[list[TripInfo]]:
    """The static trips an id can mean, in order of preference: the id as is, then (if the feed's ids carry a
    `_service_N` suffix that may not match) the id without it."""
    yield index.by_trip_id.get(trip_id, [])
    if ignore_service_suffix:
        yield index.by_base_id.get(strip_service_suffix(trip_id), [])


def _instances(index: TripIndex, candidates: list[TripInfo]) -> Iterator[TripRef]:
    """Every service day of the index on which one of `candidates` runs."""
    for info in candidates:
        for day in index.days:
            if day.version == info.version and info.service_id in day.services:
                yield TripRef(day, info)


def window_distance(ref: TripRef, now: float, early_s: int, late_s: int) -> float | None:
    """0 while the instance runs, else how far `now` is from it; None outside its window."""
    if not (ref.start - early_s <= now <= ref.end + late_s):
        return None
    return 0.0 if ref.start <= now <= ref.end else min(abs(now - ref.start), abs(now - ref.end))


def resolve(
    index: TripIndex,
    trip_id: str,
    now: float,
    *,
    early_s: int = EARLY_WINDOW_S,
    late_s: int = LATE_WINDOW_S,
    start_date: date | None = None,
    ignore_service_suffix: bool = False,
) -> TripRef | None:
    """The instance a trip update or vehicle means: the one running closest to now. When the feed gives the trip's
    `start_date` (none of ours does yet), that service day wins if it runs the trip within the window."""
    for candidates in _candidates(index, trip_id, ignore_service_suffix):
        best: tuple[bool, float, TripRef] | None = None
        for ref in _instances(index, candidates):
            dist = window_distance(ref, now, early_s, late_s)
            if dist is None:
                continue
            rank = (start_date is not None and ref.day.date != start_date, dist, ref)
            if best is None or rank[:2] < best[:2]:
                best = rank
        if best is not None:
            return best[2]
    return None


def resolve_instance(
    index: TripIndex, trip_id: str, now: float, start_date: date | None = None, ignore_service_suffix: bool = False
) -> TripRef | None:
    """The one trip instance an alert names (GTFS-RT: a selector's trip is a single instance). The `start_date` day if
    given, else the first instance that hasn't ended yet, else the one that ended last."""

    def rank(r: TripRef) -> tuple[bool, float]:
        ended = r.end < now
        return (ended, -r.end if ended else r.start)

    for candidates in _candidates(index, trip_id, ignore_service_suffix):
        refs = [r for r in _instances(index, candidates) if start_date is None or r.day.date == start_date]
        if refs:
            return min(refs, key=rank)
    return None
