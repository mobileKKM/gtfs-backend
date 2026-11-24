"""The worker's live data in Redis (layout: gtfs_backend.live_layout), as the services read it."""

from __future__ import annotations

import json
from dataclasses import dataclass

from redis.asyncio import Redis

from gtfs_backend import live_layout


async def get_json_many(redis: Redis, keys: list[str]) -> list[dict | list | None]:
    if not keys:
        return []
    return [json.loads(v) if v else None for v in await redis.mget(keys)]


async def vehicle_json(redis: Redis, ids: list[str]) -> list[dict]:
    """Vehicles by vehicle key, skipping ones that have expired."""
    return [v for v in await get_json_many(redis, [live_layout.vehicle(i) for i in ids]) if v]


async def vehicles_near(redis: Redis, lat: float, lon: float, width_km: float, height_km: float) -> list[str]:
    """Vehicle keys in a box around a point."""
    return await redis.geosearch(
        live_layout.VEHICLES_GEO, longitude=lon, latitude=lat, width=width_km, height=height_km, unit="km"
    )


async def alerts_json(redis: Redis, feed_ids: list[str]) -> list[dict]:
    return [a for feed in await get_json_many(redis, [live_layout.alerts(f) for f in feed_ids]) if feed for a in feed]


async def trip_and_alerts(redis: Redis, trip_key: str, feed_ids: list[str]) -> tuple[dict | None, list[dict]]:
    """A trip's raw update (None without one) and all alerts of these feeds, in one call."""
    values = await get_json_many(redis, [live_layout.trip(trip_key)] + [live_layout.alerts(f) for f in feed_ids])
    return values[0], [a for feed in values[1:] if feed for a in feed]


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
        self.canceled = raw.get("schedule_relationship") == "CANCELED"
        self._stops = {
            r["stop_sequence"]: StopUpdate(
                r.get("predicted_arrival"),
                r.get("predicted_departure"),
                r.get("delay_s"),
                r.get("schedule_relationship") == "SKIPPED",
            )
            for r in raw.get("stops") or []
        }

    @classmethod
    def for_version(cls, raw: dict | None, version: int) -> TripUpdate | None:
        """None without an update, or when it is for another version (stop sequences differ between versions)."""
        return cls(raw) if raw is not None and raw.get("version") == version else None

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)


# ------------------------------------------------------------------------------------------------- departure boards


class TripState:
    """What a board needs of a trip's realtime (rt:tripstate), with its predictions at the board's stops (rt:stop:*)
    only. Reads like a TripUpdate."""

    __slots__ = ("version", "canceled", "vehicle_id", "timestamp", "_stops")

    def __init__(self, version: int, canceled: bool, vehicle_id: str | None, timestamp: int | None):
        self.version = version
        self.canceled = canceled
        self.vehicle_id = vehicle_id
        self.timestamp = timestamp
        self._stops: dict[int, StopUpdate] = {}

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)


class BoardRealtime:
    """The realtime a departure board can show, read once (one pipelined call): every feed's rt:tripstate and the
    board's stops' rt:stop:* hashes, which hold every trip at the stop whatever its time, so a board widening its
    window needs no second read. Decoded per trip on demand (a board shows few of the trips it reads)."""

    def __init__(self, states: dict[str, dict[str, str]], stops: list[dict[str, str]]):
        self._states = states  # feed_id -> trip key -> TripState JSON
        self._predictions: dict[str, list[tuple[int, str]]] = {}  # trip key -> (stop_sequence, StopPrediction JSON)
        for fields in stops:
            for field, value in fields.items():
                sequence, key = live_layout.parse_stop_prediction_field(field)
                self._predictions.setdefault(key, []).append((sequence, value))
        self._decoded: dict[str, TripState | None] = {}

    @classmethod
    async def read(cls, redis: Redis, feed_ids: list[str], public_stop_ids: list[str]) -> BoardRealtime:
        if not feed_ids or not public_stop_ids:
            return cls({}, [])
        pipe = redis.pipeline(transaction=False)
        for feed_id in feed_ids:
            # the whole hash (a few hundred running trips): asking for a board's trips by name costs 4x as much, as
            # most of them have no realtime
            pipe.hgetall(live_layout.trip_states(feed_id))
        for feed_id in feed_ids:
            for pid in public_stop_ids:
                pipe.hgetall(live_layout.stop_predictions(feed_id, pid))
        results = await pipe.execute()
        return cls(dict(zip(feed_ids, results[: len(feed_ids)], strict=True)), results[len(feed_ids) :])

    def state(self, trip_key: str) -> TripState | None:
        """The trip's state with its predictions at the board's stops; None without realtime."""
        if trip_key in self._decoded:
            return self._decoded[trip_key]
        value = self._states.get(trip_key.split(":", 1)[0], {}).get(trip_key)
        state = TripState(*json.loads(value)) if value else None
        if state is not None:
            for sequence, prediction in self._predictions.get(trip_key, []):
                arrival, departure, delay, skipped = json.loads(prediction)
                state._stops[sequence] = StopUpdate(arrival, departure, delay, skipped)
        self._decoded[trip_key] = state
        return state

    def states(self, trip_keys) -> dict[str, TripState]:
        return {k: state for k in trip_keys if (state := self.state(k)) is not None}
