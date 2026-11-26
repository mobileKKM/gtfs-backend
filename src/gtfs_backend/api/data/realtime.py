"""Realtime as the services read it: the decoded forms of the live state's trips (api/data/live.py keeps the live data
in memory), and the poll stats /health reads from Redis (layout: gtfs_backend.live_layout)."""

from __future__ import annotations

from dataclasses import dataclass

import orjson
from redis.asyncio import Redis

from gtfs_backend import live_layout


async def get_json_many(redis: Redis, keys: list[str]) -> list[dict | list | None]:
    if not keys:
        return []
    return [orjson.loads(v) if v else None for v in await redis.mget(keys)]


async def meta_json(redis: Redis, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict | None]:
    """Poll stats per (feed_id, kind)."""
    values = await get_json_many(redis, [live_layout.meta(f, k) for f, k in keys])
    return dict(zip(keys, values, strict=True))


def newest(timestamps) -> int | None:
    """The time of the newest realtime data shown, or None when nothing shown is realtime. Responses carry this,
    not the time of the request, so a poll gets a 304 while the data hasn't changed."""
    return max((t for t in timestamps if t is not None), default=None)


# ------------------------------------------------------------------------------------------------- trip updates


@dataclass(frozen=True)
class StopUpdate:
    predicted_arrival: int | None
    predicted_departure: int | None
    delay_s: int | None
    skipped: bool
    propagated: bool = False  # no time from the feed for this stop: the delay of an earlier one carried over

    @property
    def predicted(self) -> int | None:
        """When the vehicle leaves (or arrives, where only that is known)."""
        return self.predicted_departure or self.predicted_arrival


class TripUpdate:
    """A trip's realtime state, used only for the timetable version it was computed for."""

    def __init__(self, raw: dict):
        self.version: int | None = raw.get("version")
        self.vehicle_id: str | None = raw.get("vehicle_id")
        self.timestamp: int | None = raw.get("timestamp")  # when the feed measured it
        self.delay_s: int | None = raw.get("delay_s")
        self.deleted = raw.get("schedule_relationship") == "DELETED"  # GTFS-RT: not to be shown at all
        self.canceled = self.deleted or raw.get("schedule_relationship") == "CANCELED"
        self._stops = {
            r["stop_sequence"]: StopUpdate(
                r.get("predicted_arrival"),
                r.get("predicted_departure"),
                r.get("delay_s"),
                r.get("schedule_relationship") == "SKIPPED",
                bool(r.get("propagated")),
            )
            for r in raw.get("stops") or []
        }

    @classmethod
    def of(cls, version, canceled, deleted, vehicle_id, timestamp, delay_s, stops: dict[int, StopUpdate]) -> TripUpdate:
        """From the parts (the API's live state keeps trips compact: live_layout.CompactTrip)."""
        update = cls.__new__(cls)
        update.version, update.vehicle_id, update.timestamp, update.delay_s = version, vehicle_id, timestamp, delay_s
        update.deleted, update.canceled, update._stops = deleted, canceled, stops
        return update

    @classmethod
    def for_version(cls, raw: dict | None, version: int) -> TripUpdate | None:
        """None without an update, or when it is for another version (stop sequences differ between versions)."""
        return cls(raw) if raw is not None and raw.get("version") == version else None

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)


class TripState:
    """What a board needs of a trip's realtime, with its predictions at the board's stops only (live.LiveBoard).
    Reads like a TripUpdate."""

    __slots__ = ("version", "canceled", "vehicle_id", "timestamp", "deleted", "_stops")

    def __init__(self, version: int, canceled: bool, vehicle_id: str | None, timestamp: int | None, deleted: bool = False):
        self.version = version
        self.canceled = canceled
        self.vehicle_id = vehicle_id
        self.timestamp = timestamp
        self.deleted = deleted  # GTFS-RT DELETED: its departures aren't shown at all
        self._stops: dict[int, StopUpdate] = {}

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)
