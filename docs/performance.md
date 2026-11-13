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
