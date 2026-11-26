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
    def for_version(cls, raw: dict | None, version: int) -> TripUpdate | None:
        """None without an update, or when it is for another version (stop sequences differ between versions)."""
        return cls(raw) if raw is not None and raw.get("version") == version else None

    def stop(self, sequence: int) -> StopUpdate | None:
        return self._stops.get(sequence)


# ------------------------------------------------------------------------------------------------- departure boards


class TripState:
    """What a board needs of a trip's realtime (rt:tripstate), with its predictions at the board's stops (rt:stop:*)
    only. Reads like a TripUpdate."""

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


class BoardRealtime:
    """The realtime a departure board can show, read once (one pipelined call): every feed's rt:tripstate and the
    board's stops' rt:stop:* hashes, which hold every trip at the stop whatever its time, so a board widening its
    window needs no second read. Decoded per trip on demand (a board shows few of the trips it reads), and all at once
    only to find trips running early (earliest_early)."""

    def __init__(self, states: dict[str, dict[str, str]], stops: list[dict[str, str]], fresh_since: dict[str, int] | None = None):
        """`fresh_since`: per feed, the oldest trip state timestamp still used (older ones count as no realtime)."""
        self._states = states  # feed_id -> trip key -> TripState JSON
        self._fresh_since = fresh_since or {}
        self._predictions: dict[str, list[tuple[int, str]]] = {}  # trip key -> (stop_sequence, StopPrediction JSON)
        for fields in stops:
            for field, value in fields.items():
                sequence, key = live_layout.parse_stop_prediction_field(field)
                self._predictions.setdefault(key, []).append((sequence, value))
        self._decoded: dict[str, TripState | None] = {}
        self._timed: list[tuple[int, int, str, int]] | None = None  # (scheduled, predicted, trip key, version)

    @classmethod
    async def read(
        cls, redis: Redis, feed_ids: list[str], public_stop_ids: list[str], fresh_since: dict[str, int] | None = None
    ) -> BoardRealtime:
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
        return cls(dict(zip(feed_ids, results[: len(feed_ids)], strict=True)), results[len(feed_ids) :], fresh_since)

    def state(self, trip_key: str) -> TripState | None:
        """The trip's state with its predictions at the board's stops; None without realtime."""
        if trip_key in self._decoded:
            return self._decoded[trip_key]
        feed_id = trip_key.split(":", 1)[0]
        value = self._states.get(feed_id, {}).get(trip_key)
        state = TripState(*json.loads(value)) if value else None
        if (
            state is not None
            and state.timestamp is not None
            and state.timestamp < self._fresh_since.get(feed_id, state.timestamp)
        ):
            state = None  # measured too long ago: the worker or the feed stopped
        if state is not None:
            for sequence, prediction in self._predictions.get(trip_key, []):
                values = json.loads(prediction)  # live_layout.StopPrediction (6 elements from older workers)
                arrival, departure, delay, skipped, version = values[:5]
                if version == state.version:  # else of another update than the state, with other stop sequences
                    propagated = len(values) > 6 and bool(values[6])
                    state._stops[sequence] = StopUpdate(arrival, departure, delay, skipped, propagated)
        self._decoded[trip_key] = state
        return state

    def states(self, trip_keys) -> dict[str, TripState]:
        return {k: state for k in trip_keys if (state := self.state(k)) is not None}

    def earliest_early(self, after: int, since: int) -> int | None:
        """The earliest predicted time, from `since` on, of a trip scheduled at one of the board's stops after `after`
        (epochs): one running early, whose timetable row a board that read up to `after` doesn't have. Feeds allow
        any amount early. None without one."""
        if self._timed is None:
            self._timed = []
            for key, rows in self._predictions.items():
                for _, value in rows:
                    arrival, departure, _, _, version, scheduled = json.loads(value)[:6]
                    # no scheduled departure: not a departure (a trip's last stop), never on a board
                    if (predicted := departure or arrival) is not None and scheduled is not None:
                        self._timed.append((scheduled, predicted, key, version))
        early = [
            predicted
            for scheduled, predicted, key, version in self._timed
            if scheduled > after and predicted >= since and (state := self.state(key)) is not None and state.version == version
        ]
        return min(early, default=None)
