# gtfs-backend

GTFS ingestion and map API for **mobileKKM**: the worker loads static timetables into Postgres/PostGIS and live
vehicles and predictions into Redis; the API serves the app's map view (live vehicles, trip details, stop search,
departures) from both. They are separate programs (`gtfs-worker`, `gtfs-api`) with separate images.

Feeds ([`feeds.yml`](feeds.yml)): `kmk_a` MPK buses, `kmk_m` Mobilis buses, `kmk_t` trams (all ZTP; vehicles, trip
updates, alerts), `km_kolej` Koleje Małopolskie trains (one combined `realtime.pb`), `km_mld` Koleje Małopolskie
feeder buses (vehicles, trip updates, alerts).

Realtime files are polled every `REALTIME_POLL_INTERVAL` (10 s) unless a feed sets `realtime_poll_interval`: ZTP
(updates every ~5 s) every 5 s, `km_kolej` (every ~5 min) every 30 s. Data stays live for `REALTIME_MAX_AGE` (120 s)
after the last change, or the feed's `realtime_max_age` (`km_kolej`: 600).

## Running it

Requirements: Python 3.12+ with [uv](https://docs.astral.sh/uv/), Docker.

```sh
docker compose -f docker-compose.dev.yml up -d   # Postgres/PostGIS + Redis only
cp .env.example .env
uv sync
uv run alembic upgrade head
uv run gtfs-worker run                           # all static + realtime tasks (imports static data on start)
uv run gtfs-api                                  # API on :8000, docs at /api/docs (--workers N / API_WORKERS)
```

Other commands:

```sh
uv run gtfs-worker import-static --all | --feed kmk_t [--retry] [--file x.zip]
uv run gtfs-worker fetch-osm                # OpenStreetMap stop poles now (the worker also fetches and refreshes them itself)
uv run gtfs-worker retire                  # drop versions nobody needs anymore
uv run gtfs-worker rebuild-service-days
```

**Production**: `POSTGRES_PASSWORD=... docker compose up -d --build` runs Postgres, Redis, the worker (migrates on
start) and the API (`API_PORT`, default 8000; `API_WORKERS`, default 4). The API also joins the external
`nginx_proxy` network (`docker network create nginx_proxy` once), so a proxy there reaches it as `http://api:8000`.
The Dockerfile has a target per program (`worker`, `api`); the API image lacks the worker's dependencies (pandas,
gtfs-kit, pyarrow, protobuf). On Apple Silicon set `POSTGIS_IMAGE=imresamu/postgis:17-3.5` (the official image is
amd64-only).

Settings come from env vars or `.env`; [`.env.example`](.env.example) lists all with defaults. Realtime timings can
also be set per feed in `feeds.yml`.

## Code layout

```
src/gtfs_backend/
  *.py       shared: settings, feeds.yml models, db, logging, service-day time, stop ids, keys,
             Redis layout (live_layout.py), route_type -> mode
  worker/    cli.py, runner.py (task loop), http.py (downloads)
    static/  fetch -> archive -> read -> validate -> load (+ derive, integrity, partitions, versions)
      stops/ stop matching: plan.py and shared.py decide (no DB), apply.py writes, osm.py reads OSM poles
    realtime/ poller -> normalize (match to timetables) -> store (Redis)
  api/       cli.py, app.py, routers/ (HTTP only), services/ (logic), schemas/, data/ (catalog cache, Redis, SQL/geo)
```

Shared modules import neither `worker` nor `api`, and those don't import each other; `tests/test_boundaries.py`
enforces this and checks the API loads none of the worker's dependencies.

## How it works

One worker process runs one asyncio task per static feed and per realtime file, plus a daily retirement task.
Static imports are CPU-heavy and run in a separate process.

### Static data (Postgres)

- `gtfs.*`: one table per GTFS file with exactly its columns (plus `feed_version_id`), partitioned by version
  (`gtfs.stop_times__kmk_t__v3`). Real Postgres types (`interval` for times, so `25:10:00` survives). An
  **integrity check** compares stored values with the raw CSV after every import; any difference fails it. ZTP's
  non-standard `blocks.txt` is kept as `gtfs.blocks`.
- **Versions**: every new zip is a `core.feed_version`, imported in one transaction, numbered per feed (never
  reused) and joined via `feed_version_id` (`kmk_t_v3`). Find the newest by `version`, not by id (`kmk_t_v10` sorts
  before `kmk_t_v9` as text). For each service date the newest ready version with service that day is
  authoritative (`core.service_day_version`), so night trips survive an overnight update. A version is retired
  (rows dropped, zip kept in `ARCHIVE_DIR`) once it serves nothing from yesterday on. A failed import never
  replaces anything. Dropping partitions needs an exclusive lock, so retirement never runs during a worker import
  and gives up after 2 s of waiting (it is retried). A crashed import process is replaced.
- **Stops** (`core.stop_group` logical, `core.stop_platform` physical, `core.stop_link` feed stop to platform) have
  our own stable 5-letter ids and URL keys like `rondo-mogilskie01-qphdu`; only the part after the last dash counts.
  - ZTP's feeds share stop codes (`stop_namespace: ztp`): `802-01` is the same platform in all, `802` the stop.
  - Imports match stops by code, then name + platform number nearby, then location; ambiguous cases get a new id
    instead of a guess.
  - Koleje Małopolskie stops are grouped by parent station.
  - MLD buses mostly use ZTP's poles (`share_stops_with: [ztp]`). OpenStreetMap is the source of truth for whether an
    MLD stop is a pole of its own: a pole with the stop's own code, or a code or bay ZTP doesn't have, keeps it
    separate; a ZTP code links it to that pole. Without such a pole nearby, position, direction, modes and next
    stop decide, and an MLD stop with no ZTP pole keeps its own. The OSM poles come from a local file
    (downloaded by the worker on first start and refreshed daily, or by `gtfs-worker fetch-osm`; without it stops match
    by position alone).
    Details: [docs/stop-matching.md](docs/stop-matching.md).

### Live data (Redis)

Redis mirrors only the latest realtime updates plus poll stats; departures combine the Postgres timetable with
`rt:trip:*`. The layout lives in `gtfs_backend/live_layout.py` (worker writes, API reads).

| key | content |
|---|---|
| `rt:veh:geo` | GEO set of all vehicles (member `{feed_id}:{vehicle_id}`) for bbox/radius queries |
| `rt:veh:{feed_id}:{vehicle_id}` | vehicle JSON: trip, route, headsign, delay, platform (TTL `VEHICLE_TTL` 120 s, refreshed while the file is unchanged, up to `realtime_max_age`) |
| `rt:trip:{feed_id}:{YYYYMMDD}:{trip_id}` | per-stop scheduled/predicted times and delays plus the `version`; stops the feed dropped stay with `passed: true`; removed when the trip leaves the feed |
| `rt:alerts:{feed_id}` | service alerts |
| `rt:meta:{feed_id}:{kind}` | poll stats after every poll: `ok`/`last_error`, `last_poll`/`last_change`, `age_s`, timings, counts, unmatched, route mismatches, `median_delay_s` |
| `rt:unmatched:{feed_id}:{kind}` | unmatched ids, for debugging |
| pub/sub `rt:updated`, `static:updated` | change notifications |
| `api:resp:{hash}` | the API's response cache |

Rules:

- **`trip_id` is the source of truth.** Route, headsign, direction and stops come from the static trip it resolves
  to (version + service day around "now"); realtime route ids are only compared and counted. A feed's `start_date`
  would pick the service day (none sends one yet).
- **Unmatched means not shown**: unknown trips, trips outside their service window and idle vehicles never reach
  the map or departures.
- **Delays are recomputed** as predicted − scheduled, scheduled = the feed's day start + GTFS time (noon minus 12 h
  unless `day_start: midnight`). They **propagate** (GTFS-RT): a stop without an update takes the last updated
  stop's delay (`propagated: true`); `NO_DATA` stops that, `SKIPPED` passes it on.
- **Clock-change days**: ZTP's trams count times from local midnight, not noon minus 12 h, so on the two
  clock-change days they run an hour off the GTFS reading (seen 2026-10-25, checked against tram GPS). `kmk_t`
  therefore has `day_start: midnight`. `median_delay_s` of about ±3600 on such a day would mean ZTP changed this.
- **Service suffix**: after midnight ZTP's trams name a still-running trip of yesterday's timetable with today's
  `_service_N` (seen 2026-10-31, when a new timetable started), an id that exists in no version. `kmk_t` has
  `ignore_service_suffix: true`: an id not found as is is retried without the suffix and the time window picks the trip.
- Downloads are checked against `Content-Length` and parsed before use. ZTP briefly serves empty or partial files
  while replacing them; those, 5xx and 429 are retried and never overwrite good data. Requests send `USER_AGENT`
  (`worker/http.py`); put a contact for the feed operators in it.
- **Liveness**: the worker touches `/tmp/gtfs-worker.alive` every 30 s while all realtime tasks keep polling (the
  compose healthcheck reads it). A task without a finished poll for `WATCHDOG_TIMEOUT` (10 min) stops the worker
  with exit code 1 and Docker restarts it.
- A zip whose import failed is not retried (`static_previously_failed`, shown by `/health`) until the publisher
  changes it or someone runs `import-static --feed … --retry`.

## API

Read-only, under `/api/v1` (OpenAPI at `/api/docs`). Routers only parse input and serialize (`deps.py`, `cache.py`
for ETag/Cache-Control); logic is in `services/`, which raises `NotFound`/`InvalidInput`. Times are epoch seconds,
dates `YYYYMMDD`.

**Keys.** Everything the app can open has a `key`, and endpoints take it:

| thing | key | endpoint |
|---|---|---|
| stop | `rondo-mogilskie01-qphdu` (or the bare id) | `/stops/{key}/departures` |
| vehicle | `{feed_id}:{vehicle_id}` | `/vehicles/{key}/trip` |
| trip | `{feed_id}:{YYYYMMDD}:{trip_id}` (`trip_key`) | `/trips/{key}`, `/trips/{key}/live` |
| route | `{feed_id}:{route_id}` | `/routes/{key}/shapes`, `/alerts?route={key}` |
| alert | `{feed_id}:{id}` | `/alerts/{key}` |

The source id comes last and is kept whole, so ids with `:` or `/` work. Responses carry no id a key already
holds (no `feed_id`/`trip_id` next to `trip_key`). Every field of a schema is always present (`null` when unset).
Polled responses (vehicles, boards, live trips) carry the `timestamp` of the newest realtime data they show, not
of the request, so their ETag only changes with the data.

| endpoint | what |
|---|---|
| `GET /vehicles?bbox=minLon,minLat,maxLon,maxLat[&modes=tram,bus][&routes=52]` | live vehicles in a box (≤ 50 km): position, bearing, line (with its mode), headsign, delay, `trip_key` |
| `GET /vehicles/{vehicle_key}/trip` | live state of the vehicle's current trip, as `/trips/{key}/live` (polling follows it onto the next: fetch `/trips/{key}` again when `key` changes) |
| `GET /stops?bbox=…[&kind=group][&modes=…]` | platforms with `group_id`, or logical stops (`kind=group`, for far zoom); only some modes, e.g. `kind=group&modes=rail` for stations (≤ 3000, `truncated`) |
| `GET /stops/search?q=…` | logical stops by name (accent/case-insensitive; only `search_namespaces`, `ztp`) with platforms and directions; `limit` default 10 |
| `GET /stops/{key}/departures[?time=…][&feeds=…][&routes=…][&limit=20]` | timetable + realtime from `time` (epoch, default now) on, filtered by feed ids and line names; `vehicle_key` to follow; alerts as refs |
| `GET /trips/{trip_key}` | what doesn't change while the trip runs: route, shape polyline, every stop with place and scheduled times (cached like stops) |
| `GET /trips/{trip_key}/live` | to poll: every stop's predictions and passed/skipped (match to the trip's `stops` by `stop_sequence`), status, live vehicle, alerts as refs |
| `GET /routes/{route_key}/shapes[?date=YYYYMMDD]` | the route's shapes that day, most used first per direction |
| `GET /alerts[?feed=…][&route=…][&stop=…][&active=true]`, `GET /alerts/{alert_key}` | alerts as published (boards and trips carry only `key`, `cause`, `effect`, `header`, `active_periods`) |
| `GET /health` | DB/Redis reachability (503 if down), static versions, poll stats, `fresh` with each feed's `problems` |

**Errors**: `{"error": {"code": "not_found" | "invalid_input" | "method_not_allowed" | …, "message": "…"}}`: 404
unknown things, 422 malformed keys/dates/boxes/times, unknown feeds, 503 `unavailable` with `Retry-After` when connections are
busy or a store is down, 500 `internal` without details.

**Several processes.** `API_WORKERS` processes share one port, each with bounded pools (`DB_POOL_MAX_SIZE`,
`REDIS_POOL_MAX_SIZE`) and its own catalog copy, reloaded on `static:updated`. `DB_POOL_MAX_SIZE` is the same for the
worker and every API process: keep `(API_WORKERS + 1) × DB_POOL_MAX_SIZE` below Postgres' `max_connections` (100).

**Response cache.** Boards and live trips (`CACHE_TTL_LIVE`, 3 s), search, trips and shapes (`CACHE_TTL_STATIC`, 300 s) and
alerts (`CACHE_TTL_ALERTS`, 15 s) are cached in Redis for all processes, so clients share bodies and ETags and
polling gets 304s. Keys include the loaded versions; only declared query parameters count; errors aren't cached.
`/vehicles` and `/stops` bbox queries aren't cached.

**Privacy.** No accounts, cookies, identifiers or client addresses; requests aren't logged (uvicorn's access log is
off) and `X-Forwarded-For` is ignored. A reverse proxy or CDN in front should do the same (no IPs or query strings
in logs).

Notes:

- **Directions**: each platform lists where its trips go next (`towards`: the next stop group), with lines and
  headsigns, most served first; under 5 % of trips are left out. Precomputed per version (`core.platform_direction`).
  Route colors are not passed on (often unset or wrong).
- **Departure boards** cover every authoritative service day that can run in the window and reach back an hour so
  delayed departures stay. Predictions are used only for the same version. `status`: `scheduled`, `live`,
  `departed`, `canceled`, `skipped`; `time` is the estimate if known. Last stops and stops without pickup are not
  departures; `on_demand` marks GTFS pickup type 2/3.
- **Paging**: a board starts at `time` (epoch seconds, default now: departed ones are left out; with a time they
  show as `departed`). Page forward by passing the last departure's `time`; departures in that second can repeat
  (tell them apart by `trip_key` + `stop_sequence`). The window widens (2 h, 6 h, 24 h) until a page is full.
- **Filters**: `feeds` (feed ids) and `routes` (line names), comma separated or repeated; they narrow the board and
  its alerts, and an unknown feed is a 422. The board lists `lines`, every route departing from the stop today, so
  clients know what to filter by. Feeds publish line variants as separate routes (km_mld has two `A18`), so a name
  can repeat: dedupe by name for display. `/vehicles` takes `routes` as well.
- **Trip view**: `shape.source` is `shape`, or `stops` when the feed has none (trains). `current_index` is the
  first stop not yet passed; `vehicle` is set while one runs the trip.
- **Polling**: live responses are `no-cache`; clients refetch the open view about every 10 s with `If-None-Match`
  and stop in the background.
- Alerts follow GTFS-RT matching (an entity's fields combine with AND). Boards and trips carry alerts active now or
  starting within a day; `/alerts` returns all unless `active=true`.
- **Health**: `ok` means Postgres and Redis answer. `fresh` is false while any feed has `problems`: no timetable,
  failed last import, static not checked for `HEALTH_STATIC_MAX_AGE` (3 h), or a realtime file not polled, failing
  (5 polls) or older than `HEALTH_REALTIME_MAX_AGE` (5 min; a larger `realtime_max_age` wins). Alerts files are
  exempt from the age check.
- Responses carry `ETag` and `Cache-Control` (static data 5 min, alerts 15 s); bodies over 1 kB are gzipped.
- Queries name one feed version per block: joining `feed_version_id` across tables defeats partition pruning
  (a 20 ms board became 1.5 s).

## Tests

```sh
docker compose -f docker-compose.dev.yml up -d && uv run pytest
```

DB and Redis tests use a separate `gtfs_test` database / Redis db 15 and are skipped when the services are down.
`tests/fixtures/dst_2026-10-25/` is the realtime snapshot from the clock change; `tests/fixtures/static/` holds the
matching timetables (made by `tests/fixtures/make_fixtures.py`).

## License

[GPL v3](LICENSE) or later.
