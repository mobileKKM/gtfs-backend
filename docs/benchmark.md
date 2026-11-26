# Benchmarks

Every performance change reports numbers from the same three places, before and after:

| layer | what it measures | how |
|---|---|---|
| **micro** | CPU of the request and poll hot paths, on synthetic data of production size; no Postgres, Redis or network | `uv run pytest bench/micro` |
| **load** | latency the app sees with ~2,000 clients polling a replayed weekday peak, and the API's CPU and memory | `bench/load.py` (+ `bench/replay.py`, `bench/loadgen.py`) |
| **server** | p50/p90/p99 per endpoint (all API processes) and realtime data age per feed, also in production | `GET /api/v1/metrics?window=600` |

Numbers are only comparable on the same machine, with the same recording (its `manifest.json` sha256), the same load
settings and the same `API_WORKERS`; `bench/load.py` writes them all into each result.

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
| `board_rows_from_the_snapshot` | a board's timetable rows (8 poles, 90 min) from a 1.35 M-row snapshot: what was a Postgres query |
| `stop_search[...]` | stop search in memory over the fixtures' stop names |

## Load: record once, replay as often as needed

A replay needs the worker and the API to run *at the recorded time*: realtime trips are matched to the timetable by
service day and time of day. `CLOCK_OFFSET` (seconds, both programs) shifts their clock; the replay server says which
offset. Redis TTLs and the metrics' minutes stay on the real clock.

**1. Record** (once; polls the operators' servers at the worker's cadence). A weekday 07:00–08:00 is the peak.
30 minutes are a few hundred MB (ZTP's bus trip updates alone are ~200 KB every 5 s); recordings stay out of git
(`bench/out/` is ignored).

```sh
uv run python bench/replay.py record bench/out/rec-peak --minutes 40
```

**2. A database of its own** (once), so dev data stays as it is: `bench/load.py` uses `gtfs_replay` and Redis db 14
(`BENCH_DATABASE_URL`, `BENCH_REDIS_URL`; the Redis db is flushed before every run).

```sh
PGPASSWORD=gtfs createdb -h localhost -U gtfs gtfs_replay
DATABASE_URL=postgresql://gtfs:gtfs@localhost:5432/gtfs_replay uv run alembic upgrade head
uv run python bench/load.py import bench/out/rec-peak     # the recording's timetables (minutes)
```

**3. Run**: replay server -> worker + API at the recording's time -> wait for `/health` `fresh` -> warm-up -> load ->
`/metrics`, `/health`, the API's CPU and memory, all into `--out`:

```sh
uv run python bench/load.py run bench/out/rec-peak --out bench/out/results/NAME   # 2,000 clients, 60 s ramp, 600 s
```

`--code PATH` runs another checkout's worker and API with the same load (a git worktree of the commit before a
change: `git worktree add ../wt-before <commit>`). The load always comes from this checkout's `bench/loadgen.py`.

The mix (`bench/loadgen.py`, the same as `bench/k6/rest_mix.js`): every client has its own connection and opens a
view, polling it every 10 s with `If-None-Match`: the map 50 % (`/stops?bbox` once, then `/vehicles?bbox` for a 2–8
km box), a board 30 %, a trip 15 % (`/trips/{key}` once, then `/live`), a followed vehicle 5 %, plus a stop search
before 10 % of views. Latency is until the whole body arrived, steady state only (after the ramp); 404s of trips and
vehicles that left the feed count as answered. The generator reports its own event loop lag, which would inflate
the latencies if it got high.

k6 (`bench/k6/rest_mix.js`) does the same from a k6 install; run in Docker on macOS it loses connections in Docker's
network (`dial: i/o timeout` from 200 clients on), so `bench/load.py` uses the Python generator.

## Server: `/api/v1/metrics`

Every API process counts its requests per route template (`GET /api/v1/stops/{stop_key}/departures`, never the URL)
into log-scale histogram buckets and adds them to Redis every 10 s (`metrics:{minute}`, kept 2 h); the worker does the
same with each realtime file's data age at every poll (now minus the feed header timestamp of its newest data). The
endpoint adds up all processes over the last `window` seconds (60–3600, whole minutes). A value is reported as its
bucket's upper bound: up to 9 % (`bucket_error`) above the real one.

## Results

See the tables below; every run's raw files are in `bench/out/results/` (not in git).
