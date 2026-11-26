# Benchmarks

Every performance change reports numbers from the same three places, before and after:

| layer | what it measures | how |
|---|---|---|
| **micro** | CPU of the request and poll hot paths, on synthetic data of production size; no Postgres, Redis or network | `uv run pytest bench/micro` |
| **load** | latency the app sees with ~2,000 clients polling a replayed weekday peak (client side, k6) | `bench/replay.py` + `bench/k6/rest_mix.js` |
| **server** | p50/p90/p99 per endpoint (all API processes) and realtime data age per feed, also in production | `GET /api/v1/metrics?window=600` |

Numbers are only comparable on the same machine, with the same recording (its `manifest.json` sha256), the same k6
settings and the same `API_WORKERS`. Say which in the results.

## Micro

```sh
uv run pytest bench/micro                          # table of timings
uv run pytest bench/micro --benchmark-save=NAME    # keep a run (.benchmarks/)
uv run pytest bench/micro --benchmark-compare      # against the last saved run
```

| benchmark | what |
|---|---|
| `vehicles_city_wide_response` | `/vehicles` after its MGET: 1,000 vehicles decoded, built, serialized, hashed, gzipped |
| `board_realtime_decode` | a board's realtime: the states of 60 shown trips, and the scan for trips running early (a busy group of 8 poles, ~300 trips) |
| `board_overlay_and_select` | realtime laid over 600 timetable rows, page of 20 picked |
| `worker_departure_index_of_a_big_feed` | the `rt:tripstate` / `rt:stop:*` contents the worker builds every trip-updates poll (300 trips x 25 stops) |

## Load: record once, replay as often as needed

A replay needs the worker and the API to run *at the recorded time*: realtime trips are matched to the timetable by
service day and time of day. `CLOCK_OFFSET` (seconds, both programs) shifts their clock; `bench/replay.py serve`
prints the value. Redis TTLs and the metrics' minutes stay on the real clock.

**1. Record** (once; polls the operators' servers at the worker's cadence, so don't run it next to a production worker
for long). A weekday 07:00–08:00 is the peak. 30 minutes are a few hundred MB (ZTP's bus trip updates alone are
~200 KB every 5 s); keep recordings out of git (`bench/out/` is ignored).

```sh
uv run python bench/replay.py record bench/out/rec-20261127 --minutes 30
```

**2. A separate database and Redis db**, so dev data stays as it is:

```sh
PGPASSWORD=gtfs createdb -h localhost -U gtfs gtfs_bench   # once (the dev Postgres of docker-compose.dev.yml)
export DATABASE_URL=postgresql://gtfs:gtfs@localhost:5432/gtfs_bench REDIS_URL=redis://localhost:6379/14
export FEEDS_CONFIG=bench/out/feeds.replay.yml ARCHIVE_DIR=bench/out/archive OSM_REFRESH_INTERVAL=0
uv run alembic upgrade head
uv run python bench/replay.py feeds bench/out/rec-20261127 -o bench/out/feeds.replay.yml
```

`OSM_REFRESH_INTERVAL=0`: use the OSM stop poles file that is there (`OSM_STOPS_FILE`), don't download one.

**3. Import the recorded timetables** (minutes; the server only has to serve the zips):

```sh
uv run python bench/replay.py serve bench/out/rec-20261127 &      # stop it afterwards
uv run gtfs-worker import-static --all
```

**4. Replay from the start**: restart the server and run the worker and the API with its offset:

```sh
uv run python bench/replay.py serve bench/out/rec-20261127        # prints CLOCK_OFFSET=-12345.678
export CLOCK_OFFSET=$(curl -s http://127.0.0.1:8765/clock_offset)
uv run gtfs-worker run &
API_WORKERS=4 uv run gtfs-api --port 8000 &
curl -s http://127.0.0.1:8000/api/v1/health | jq .fresh         # true once every feed has data (~1 min)
```

**5. Load** ([k6](https://k6.io), `brew install k6`). The defaults: 2,000 clients, 1 min ramp, 10 min steady, polling
every 10 s, so the recording must cover warm-up + 11 min.

```sh
mkdir -p bench/out
k6 run bench/k6/rest_mix.js                                       # summary also in bench/out/k6-summary.json
curl -s 'http://127.0.0.1:8000/api/v1/metrics?window=600' | jq   # the server's view of the same run
```

The mix (`rest_mix.js`): every client opens a view and polls it every 10 s with `If-None-Match`: the map 50 %
(`/stops?bbox` once, then `/vehicles?bbox` for a 2–8 km box), a board 30 %, a trip 15 % (`/trips/{key}` once, then
`/live`), a followed vehicle 5 %, plus a stop search before 10 % of views. 404s of trips and vehicles that left the
feed count as answered.

## Server: `/api/v1/metrics`

Every API process counts its requests per route template (`GET /api/v1/stops/{stop_key}/departures`, never the URL)
into log-scale histogram buckets and adds them to Redis every 10 s (`metrics:{minute}`, kept 2 h); the worker does the
same with each realtime file's data age at every poll (now minus the feed header timestamp of its newest data). The
endpoint adds up all processes over the last `window` seconds (60–3600, whole minutes). A value is reported as its
bucket's upper bound: up to 9 % (`bucket_error`) above the real one.

## Results

Fill in one row set per phase, with the commit, recording and machine.

**Baseline micro** (commit of phase 1, Apple M4 Pro, 12 cores, Python 3.14.5; median of a `pytest bench/micro` run):

| benchmark | median |
|---|---|
| vehicles_city_wide_response | 10.1 ms |
| worker_departure_index_of_a_big_feed | 5.8 ms |
| board_realtime_decode | 0.67 ms |
| board_overlay_and_select | 0.58 ms |

**Baseline load**: not run yet (needs a recording; see above).

| request | p50 ms | p90 ms | p99 ms | 304 share |
|---|---|---|---|---|
| vehicles | | | | |
| board | | | | |
| trip_live | | | | |
| … | | | | |
