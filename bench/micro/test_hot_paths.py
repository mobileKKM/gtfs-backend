"""CPU cost of the request and poll hot paths on synthetic data of production size (no DB, Redis or network).

    uv run pytest bench/micro                                   # table of timings
    uv run pytest bench/micro --benchmark-save=phase0           # keep them (.benchmarks/), compare later with
    uv run pytest bench/micro --benchmark-compare               # ... the last saved run

Sizes follow docs/benchmark.md: ~1,000 vehicles in a city-wide box, ~1,500 trips with realtime across 5 feeds, a busy
stop group of 8 poles that ~300 of them pass, ~25 stops per trip.
"""

from __future__ import annotations

import gzip
import json
import random
from datetime import date

import pytest

from gtfs_backend import keys, live_layout
from gtfs_backend.api.cache import _etag
from gtfs_backend.api.data.realtime import BoardRealtime
from gtfs_backend.api.schemas import RouteRef, VehicleList
from gtfs_backend.api.services import departures, vehicles
from gtfs_backend.worker.realtime.store import _departure_index

FEEDS = ["kmk_a", "kmk_m", "kmk_t", "km_mld", "km_kolej"]
TRIPS_PER_FEED = 300
STOPS_PER_TRIP = 25
POLES = [f"p{i:04d}" for i in range(8)]
BOARD_SHARE = 5  # a busy stop group: ~300 of the ~1,500 running trips pass it
NOW = 1_795_000_000
DAY = date(2026, 11, 26)


@pytest.fixture(scope="module")
def rng():
    return random.Random(42)


@pytest.fixture(scope="module")
def vehicle_raws(rng):
    """1,000 rt:veh JSON values, as Redis returns them."""
    out = []
    for i in range(1000):
        feed = FEEDS[i % 3]
        out.append(
            json.dumps(
                {
                    "feed_id": feed,
                    "vehicle_id": f"v{i}",
                    "label": f"BH{i:03d}",
                    "license_plate": None,
                    "lat": 50.0 + rng.random() * 0.12,
                    "lon": 19.85 + rng.random() * 0.25,
                    "bearing": rng.random() * 360,
                    "speed": 6.5,
                    "timestamp": NOW - rng.randrange(30),
                    "current_status": "IN_TRANSIT_TO",
                    "current_stop_sequence": 7,
                    "stop_id": "s1",
                    "public_stop_id": "abcde",
                    "trip": {
                        "key": keys.trip_key(feed, DAY, f"block_{i}_trip_3_service_1"),
                        "feed_id": feed,
                        "version": 3,
                        "trip_id": f"block_{i}_trip_3_service_1",
                        "service_date": DAY.isoformat(),
                        "route_id": f"r{i % 150}",
                        "route_short_name": str(i % 150),
                        "route_type": 3,
                        "headsign": "Czerwone Maki P+R",
                        "direction_id": i % 2,
                        "shape_id": "sh1",
                        "block_id": "b1",
                    },
                    "delay_s": rng.randrange(-60, 600),
                    "occupancy": None,
                    "wheelchair_accessible": None,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    return out


def test_vehicles_city_wide_response(benchmark, vehicle_raws):
    """GET /vehicles for ~1,000 vehicles after the MGET: decode, build models, serialize, hash, gzip."""
    refs = {
        (keys.version_id(f, 3), f"r{i}"): RouteRef(key=f"{f}:r{i}", name=str(i), mode="bus") for f in FEEDS for i in range(150)
    }

    def respond():
        raws = [json.loads(v) for v in vehicle_raws]
        models = [vehicles._model(v, refs) for v in raws]
        body = VehicleList(timestamp=NOW, vehicles=models).model_dump_json().encode()
        _etag(body)
        return gzip.compress(body, 6)

    assert benchmark(respond)


def _live_trips(rng) -> dict[str, dict[str, live_layout.LiveTrip]]:
    """Per feed, TRIPS_PER_FEED merged LiveTrips; every BOARD_SHARE-th trip passes one of the board's POLES."""
    out = {}
    for feed in FEEDS:
        trips = {}
        for t in range(TRIPS_PER_FEED):
            key = keys.trip_key(feed, DAY, f"trip_{t}")
            start, delay = NOW - 1800 + rng.randrange(7200), rng.randrange(-30, 400)
            stops = []
            for s in range(STOPS_PER_TRIP):
                sched = start + 90 * s
                pid = POLES[t % len(POLES)] if s == 10 and t % BOARD_SHARE == 0 else f"{feed}_{t % 400}_{s}"
                stops.append(
                    {
                        "stop_sequence": s + 1,
                        "stop_id": f"{feed}_{s}",
                        "public_stop_id": pid,
                        "scheduled_arrival": sched,
                        "scheduled_departure": sched,
                        "predicted_arrival": sched + delay,
                        "predicted_departure": sched + delay,
                        "delay_s": delay,
                        "schedule_relationship": "SCHEDULED",
                        "propagated": s > 3,
                    }
                )
            trips[key] = {
                "key": key,
                "version": 3,
                "vehicle_id": f"v{t}",
                "timestamp": NOW - 5,
                "schedule_relationship": "SCHEDULED",
                "stops": stops,
            }
        out[feed] = trips
    return out


@pytest.fixture(scope="module")
def live_trips(rng):
    return _live_trips(rng)


def test_worker_departure_index_of_a_big_feed(benchmark, live_trips):
    """The rt:tripstate and rt:stop:* contents the worker builds on every trip-updates poll (one feed, 300 trips)."""
    assert benchmark(_departure_index, live_trips["kmk_a"])


@pytest.fixture(scope="module")
def board_redis(live_trips):
    """What a board of 8 poles reads in its one pipeline: every feed's rt:tripstate, and each (feed, pole) rt:stop."""
    states, stop_fields = {}, {}
    for feed, trips in live_trips.items():
        s, fields = _departure_index(trips)
        states[feed] = s
        stop_fields[feed] = fields
    stops = [stop_fields[f].get(p, {}) for f in FEEDS for p in POLES]
    return states, stops


def test_board_realtime_decode(benchmark, board_redis, live_trips):
    """Decoding a board's realtime: the states of 60 shown trips, and the scan for trips running early."""
    states, stops = board_redis
    shown = [k for feed in FEEDS for k in list(live_trips[feed])[:12]]

    def decode():
        rt = BoardRealtime(states, stops)
        rt.states(shown)
        return rt.earliest_early(NOW + 1800, NOW - 30)

    benchmark(decode)


def test_board_overlay_and_select(benchmark, rng):
    """Overlaying realtime on ~600 timetable rows of a window and picking the page of 20."""
    rows = []
    for i in range(600):
        feed = FEEDS[i % 3]
        rows.append(
            departures.Scheduled(
                feed,
                keys.version_id(feed, 3),
                3,
                DAY,
                f"trip_{i}",
                11,
                POLES[i % 8],
                NOW - 600 + 6 * i,
                "Salwator",
                f"r{i % 40}",
                False,
                keys.trip_key(feed, DAY, f"trip_{i}"),
            )
        )
    states = {}
    for s in rows[::2]:
        state = departures.TripState(3, False, f"v{s.trip_id}", NOW - 5)
        state._stops[11] = departures.realtime.StopUpdate(s.scheduled_departure + 120, s.scheduled_departure + 120, 120, False)
        states[s.trip_key] = state

    def page():
        out = [departures.Row(s, departures.overlay(s, states.get(s.trip_key), NOW)) for s in rows]
        return departures.select(out, NOW, 20, NOW - 30, NOW + 3600, True)

    assert len(benchmark(page)) == 20
