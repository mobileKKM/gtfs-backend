# Performance and memory baseline

Measured on 2026-11-13, before the latency and memory work on branch `perf-memory`. Later changes are checked
against these numbers.

**Setup.** MacBook (12 cores) with the dev stack (`docker-compose.dev.yml`, default Postgres settings), all five feeds
imported fresh from their live URLs, the worker polling live realtime for ~30 minutes. Load came from a small async
client on the same machine: 32 concurrent clients, 15–20 s per scenario. Client-side times include the client's own
overhead, so **server times (`Server-Timing: app;dur`) are the numbers to compare**. Memory is macOS RSS, which is
only roughly comparable to Linux.

## Realtime freshness (worker)

`lag_s` is the feed's header timestamp to our Redis write, sampled on every change for 3 minutes.

| File | Updates in 3 min | `lag_s` p50 | min–max |
|---|---|---|---|
| kmk_a vehicle_positions | 18 (every 10 s) | 5.8 s | 5.8–5.9 |
| kmk_a trip_updates | 18 | 6.1 s | 6.0–6.2 |
| kmk_m vehicle_positions | 18 | 6.8 s | 6.8–6.9 |
| kmk_m trip_updates | 18 | 6.9 s | 6.9–7.1 |
| kmk_t vehicle_positions | 19 | 1.8 s | 1.8–2.8 |
| kmk_t trip_updates | 18 | 7.0 s | 6.0–7.1 |
| km_mld (both) | 9 (every 20 s) | 7.8 s | 7.8–7.9 |
| km_kolej combined | 2 | 87 s | 27–147 |

- **Our share is small:** fetch + process + write take 0.1–0.3 s per update (`rt:meta` `fetch_seconds`,
  `process_seconds`, `write_seconds`). Most of the lag exists before we fetch the file. One check of ZTP's
  `Last-Modified` showed kmk_a files published ~7–8 s after their own timestamp, other files 0–3 s (1 s resolution,
  clocks unverified).
- **ZTP's bus files change every 10 s,** not every ~5 s as `feeds.yml` says. The lag per file is nearly constant,
  so our 5 s polls meet each change at a fixed offset.
- Event-loop lag (`worker_stats loop_lag_max_ms`): 230 ms at startup (trip-index builds; kmk_a's takes 1.4 s and
  holds that feed's polls), 77–100 ms in steady state (processing the larger trip-update files).
- ZTP occasionally serves a file without a header or a truncated one; the retry after `HTTP_RETRY_DELAY` (1 s) adds
  that second to the poll.

## API

Server time per request in ms (p50 / p95), requests per second, and 503 responses.

| Scenario | 1 process | 2 processes | 4 processes |
|---|---|---|---|
| Boards, uncached | 5006 / 10109, 5 rps, **68 × 503** | 5167 / 10216, 4 rps, **43 × 503** | 201 / 534, 92 rps, **18 × 503** |
| Boards, cached (prod settings) | 1.7 / 223, 80 rps, **31 × 503** | 1.5 / 49, 223 rps, **15 × 503** | 1.4 / 62, 305 rps, **5 × 503** |
| Vehicles (city bbox, never cached) | 165 / 238, 164 rps | 63 / 161, 248 rps | 15 / 50, 171 rps |
| Trip live, cached | 0.8 / 2.7 | 0.8 / 2.4 | 0.8 / 2.8 |
| Stop search, cached | 1.2 / 2.8 | 1.2 / 2.6 | 1.2 / 2.7 |

**Departure boards are the only problem, and the cause is a bug.** Each board holds a pool connection
([departures.py](../src/gtfs_backend/api/services/departures.py) `board()`) and, inside that block, can take a
**second** one:
- `Catalog.day()` queries any service day not cached, and doesn't remember misses. Here kmk_t and km_kolej had no
  version serving yesterday, so every board queried it again.
- `Catalog.routes()` loads routes after every catalog reload (every 5 minutes).

With more concurrent boards than `DB_POOL_MAX_SIZE` (10) in one process, every connection ends up held by a board
waiting for a second one, until `DB_POOL_TIMEOUT` (5 s) answers 503. More processes only spread the load thinner.

**A single uncached board takes 82 ms**, measured sequentially for Rondo Grunwaldzkie:
- `scheduled()` SQL: 49 ms
- `realtime.trip_updates`: 15 ms (parses each trip's full JSON for one stop)
- rest in Python: ~18 ms

The SQL reads every stop_time in a 3-hour window (1 h lookback + 2 h ahead): 3,334 rows for kmk_a. For each of
them it checks "a next stop exists" and joins the trip (37 ms together), and only then filters by the day's
services, which keeps 387 of them (12%). The planner expects 21 rows, so it puts the cheap service filter last.

**Vehicles are CPU-bound** (one process: 165 ms p50 under load): 541 vehicles become pydantic models, JSON and gzip
(level 9) on every request, without caching.

## Memory

| Process | RSS |
|---|---|
| Worker main process, after 30 min | 350 MB (~170 MB of it is pandas, gtfs-kit, pyarrow and osmium imports it never uses) |
| Import child (kmk_a), Linux, measured in the worker image | ~970 MB peak, ~800 MB kept after the import |
| API, 4 processes + supervisor | 341 MB |
| API, 2 processes + supervisor | 225–243 MB |

The current pandas import takes 33.6 s for kmk_a (macOS, dev Postgres).

## Proposed targets (to confirm)

- **No 503s** in any scenario above at 32 concurrent clients, with 1 process and with 4.
- **Uncached board:** p50 ≤ 25 ms sequentially, server p95 ≤ 100 ms under the 4-process load.
- **Vehicles:** server p95 ≤ 50 ms with 2 processes.
- **Freshness:** API responses never older than Redis (live cache follows `realtime:updated`); worker event-loop lag
  ≤ 50 ms, also during trip-index reloads.
- **Memory:** worker main process ≤ 200 MB, no import child kept between imports, import peak ≤ 150 MB.

## After the work on `perf-memory` (same setup, 2026-11-13 evening)

### API (server time p50 / p95 ms, requests per second, 503s)

| Scenario | 1 process | 2 processes | 4 processes |
|---|---|---|---|
| Boards, uncached | 294 / 347, 105 rps, **0** (was 5 rps, 68 × 503) | 200 / 266, 155 rps, **0** | 209 / 325, 140 rps, **0** (was 92 rps, 18 × 503) |
| Boards, cached | 1.5 / 37, 268 rps, **0** (was 31 × 503) | 1.5 / 7–35, 311–331 rps, **0** | 1.5 / 8–9, 300–317 rps, **0** (was 62 ms p95, 5 × 503) |
| Vehicles (city bbox) | 135 / 184 | 7.1 / 17 (was 63 / 161) | 6.9 / 14 (was 15 / 50) |
| Trip live, cached | 0.9 / 2.8 | 0.9 / 2.3 | 0.9 / 2.1 |
| Stop search, cached | 1.1 / 2.5 | 1.2 / 2.7 | 1.2 / 2.5 |

- **One uncached board: 22 ms** (82 ms before), measured sequentially for Rondo Grunwaldzkie. The timetable query
  takes 2.3 ms (49 ms before) and returns identical rows for all 180 boards compared. Most of the rest is reading
  ~365 trips' full JSON from Redis (~11 ms). Under load, uncached boards stop scaling at ~150 rps at any process
  count, which points at Redis bandwidth (~1.8 MB per board): the per-stop prediction layout is the next step if
  that's ever needed.
- One city vehicle list costs 11 instead of 14 µs per vehicle; most of what remains is the Redis round trip through
  Docker Desktop's VM network and building ~580 response models.
- Live responses are never older than Redis now (cached per realtime generation, CACHE_TTL_LIVE only bounds time).
- `API_WORKERS` stays 4: with 2, the p95 of cached boards and vehicles was 23–270% higher at 32 concurrent clients,
  though within the targets. 2 would save ~155 MB.

### Worker and memory

| | Before | After |
|---|---|---|
| Worker process, after 30+ min | 350 MB | 113 MB (macOS) / ~170 MB (Linux, estimated from imports) |
| Import process, kmk_a (Linux) | ~970 MB peak, ~800 MB kept until the next job | 84 MB peak, exits after each job |
| OSM refresh (Linux) | 607 MB peak | 156 MB peak |
| API, 4 processes + supervisor | 341 MB | 395 MB (cached run; uncached-run baseline wasn't recorded) |
| Worker image | 953 MB | 343 MB |
| kmk_a trip index | 85,907 trips | 26,721 trips |
| Event-loop lag (worst per 5 min) | 77–100 ms, 230 ms at startup | 35–77 ms |

- An import of kmk_a takes ~10% longer (two CSV passes in Python instead of pandas; ~4 s more for core.departure).
- Realtime freshness is unchanged, as expected: 6–7 s for buses arises at ZTP before we fetch the file.

### Targets

| Target | Result |
|---|---|
| No 503s, 32 clients, 1 and 4 processes | ✅ none |
| Uncached board ≤ 25 ms sequentially; server p95 ≤ 100 ms with 4 processes under load | ✅ 22 ms; ❌ 325 ms p95 at 32 concurrent uncached boards (queueing; cached p95 9 ms) |
| Vehicles server p95 ≤ 50 ms with 2 processes | ✅ 17 ms |
| API responses never older than Redis | ✅ |
| Event-loop lag ≤ 50 ms, also during reloads | ❌ 35–77 ms (processing kmk_a's trip updates on the loop); reloads no longer stall polling |
| Worker process ≤ 200 MB, no import process kept, import peak ≤ 150 MB | ✅ |
