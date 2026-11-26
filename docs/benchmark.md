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
| `board_realtime_decode` | a board's realtime from the live state: the states of 60 shown trips, and the scan for trips running early (a busy group of 8 poles, ~300 trips) |
| `board_overlay_and_select` | realtime laid over 600 timetable rows, page of 20 picked |
| `worker_trip_states_of_a_big_feed` | what the worker builds every trip-updates poll besides `rt:trips`: each trip's `TripState` and its JSON, which tell what the change message carries (300 trips x 25 stops) |
| `vehicles_city_wide_response_from_pieces` | `/vehicles` since phase 3: kept per-vehicle JSON filtered and joined, hashed, gzipped |

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

`--mode ws` has the clients subscribe over the WebSocket instead of polling (the same views: the map's
`vehicles`, a board, a trip). Both modes sample, every second, how old the newest realtime data each client holds is
(`data_age_s`, in replayed time): what polling adds to freshness and push saves.

`--code PATH` runs another checkout's worker and API with the same load (a git worktree of the commit before a
change: `git worktree add ../wt-before <commit>`). The load always comes from this checkout's `bench/loadgen.py`.

The mix (`bench/loadgen.py`): every client has its own connection and opens a
view, polling it every 10 s with `If-None-Match`: the map 50 % (`/stops?bbox` once, then `/vehicles?bbox` for a 2–8
km box), a board 30 %, a trip 15 % (`/trips/{key}` once, then `/live`), a followed vehicle 5 %, plus a stop search
before 10 % of views. Latency is until the whole body arrived, steady state only (after the ramp); 404s of trips and
vehicles that left the feed count as answered. The generator reports its own event loop lag, which would inflate
the latencies if it got high.

The generator is Python rather than k6: k6 in Docker on macOS loses connections in Docker's network (`dial: i/o
timeout` from 200 clients on).

## Server: `/api/v1/metrics`

Every API process counts its requests per route template (`GET /api/v1/stops/{stop_key}/departures`, never the URL)
into log-scale histogram buckets and adds them to Redis every 10 s (`metrics:{minute}`, kept 2 h); the worker does the
same with each realtime file's data age at every poll (now minus the feed header timestamp of its newest data). The
endpoint adds up all processes over the last `window` seconds (60–3600, whole minutes). A value is reported as its
bucket's upper bound: up to 9 % (`bucket_error`) above the real one.

## Results

These are the phases of PR #3 as measured. Afterwards (2026-11-26) the parts that cost more than they gave were taken
out again, to keep the code small: the timetable snapshot (phase 2: boards and trips read their timetable from Postgres
again, about 1 ms of the board's p50 at this load, and ~350 MB less API memory), the in-memory stop search (search is
one Postgres query again), the WebSocket's map tiles (the map subscribes to `vehicles`, the
whole city, and gets only the vehicles that changed: an estimated ~1 kB/s per client compressed at the peak, not
measured with the load yet), `Server-Timing` (`/metrics`
remains), and the per-vehicle / per-trip Redis keys (one value per feed now; only an API process starting or catching
up reads it). The live state in memory (phase 4a), where most of the gain came from, stays.

**Load** on the 2026-11-26 morning peak (recorded 06:40–07:20, `manifest.json` sha256 `0e8e46d7…`), Apple M4 Pro
(12 cores), Python 3.14.5, `API_WORKERS=4`, 2,000 clients, 60 s ramp, 300 s measured, one run each, back to back,
nothing else running. Each commit is the end of a phase (`bench/load.py run … --code <worktree of it>`); latencies are
the client's (until the whole body arrived), data age is that of the newest realtime data a client holds, sampled
every second.

| run | commit | vehicles p50/p99 ms | board p50/p99 ms | trip_live p50/p99 ms | map data age p50/p90 s | board data age p50/p90 s | API CPU % (mean) | API RSS MB |
|---|---|---|---|---|---|---|---|---|
| p1-baseline | 8b96320 | 4.8 / 12.5 | 8.6 / 17.0 | 4.6 / 11.9 | 15.4 / 22.3 | 18.4 / 26.3 | 37.6 | 757.9 |
| trackC | 33b6c5b | 4.7 / 12.4 | 8.5 / 16.9 | 4.5 / 11.6 | 17.4 / 24.3 | 20.4 / 28.4 | 37.0 | 764.4 |
| p2-snapshot | 0029746 | 5.5 / 14.2 | 7.6 / 16.7 | 3.5 / 10.4 | 15.3 / 22.3 | 18.3 / 26.3 | 34.8 | 1114.3 |
| p3-serialization | 6f25a85 | 4.9 / 13.1 | 7.5 / 15.8 | 3.4 / 10.0 | 15.5 / 22.4 | 18.5 / 26.4 | 29.6 | 1189.4 |
| p4a-live-state | f2e153b | 2.8 / 10.3 | 3.0 / 10.2 | 2.2 / 9.1 | 14.6 / 21.6 | 19.6 / 28.6 | 22.0 | 1372.9 |
| final-rest | 718891a* | 2.8 / 9.3 | 3.0 / 9.3 | 2.2 / 8.8 | 15.5 / 22.5 | 18.5 / 26.5 | 22.5 | 1363.3 |
| final-ws | 718891a* | 1.3 / 17.4 (snapshot) | 1.6 / 21.6 (snapshot) | 1.0 / 17.6 (snapshot) | 9.6 / 13.9 | 13.8 / 19.8 | 24.3 | 1389.5 |

- Boards 8.6 → 3.0 ms p50 (17.0 → 9.3 p99), `/vehicles` 4.8 → 2.8 ms, live trips 4.6 → 2.2 ms, API CPU −40 %. Most of
  it comes with the live state in memory (4a): until then every board read Redis (the snapshot, phase 2, only took the
  Postgres query out).
- Push (`final-ws`): the map's data is 9.6 s old (p50, 13.9 s p90) when polled ones are 15.5 s (22.5 s): the 10 s
  poll is gone. Its "latency" columns are the time from subscribing to the snapshot; their p99 (17–22 ms) includes the
  load generator's own event loop lag (p99 19 ms, 2,000 sockets in one Python process).
- Push sends much more: 607,115 messages, 1.37 GB (before permessage-deflate) to 2,000 clients in 300 s, ~7.6 KB/s per
  client, mostly tile updates (every moved vehicle once a second) and whole boards every 2 s while they change. Worth
  tuning before mobile clients use it (longer intervals per topic, field diffs).
- API RSS sums the 4 processes and counts the memory-mapped snapshot once per process (shared pages), so it overstates
  the growth (758 → 1,363 MB); the live state itself is a few MB per process.
- Data age differences under ~2 s between REST runs are run-to-run (where polls fall against the files' changes).
- \* The final runs had km_mld polled every 5 s and km_kolej every 15 s (a commit since dropped: polling stays at
  10 s / 30 s so the operators get no more requests). It doesn't show in the map's age, which ZTP's vehicles
  dominate.

**Micro** (`uv run pytest bench/micro`, medians, same machine):

| benchmark | phase 1 | now |
|---|---|---|
| vehicles_city_wide_response (old path) → …_from_pieces | 10.1 ms | 2.9 ms |
| board realtime decode (Redis values → live state) | 0.67 ms | 0.32 ms |
| worker per-poll derived work (rt:stop index → states + changes) | 5.8 ms | 1.2 ms |
| board timetable rows (Postgres query → snapshot; Postgres again since) | ~1–3 ms + pool | 0.16 ms |
| board_overlay_and_select | 0.58 ms | 0.60 ms |
| stop search (Postgres → memory; Postgres again since) | a query | 0.15–0.68 ms |
