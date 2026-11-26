# gtfs-backend

GTFS ingestion and map API for **mobileKKM**: the worker loads static timetables into Postgres/PostGIS and live
vehicles and predictions into Redis; the API serves the app's map view (live vehicles, trip details, stop search,
departures) from both. They are separate programs (`gtfs-worker`, `gtfs-api`) with separate images.

Feeds ([`feeds.yml`](feeds.yml)): `kmk_a` MPK buses, `kmk_m` Mobilis buses, `kmk_t` trams (all ZTP; vehicles, trip
updates, alerts), `km_kolej` Koleje Małopolskie trains (one combined `realtime.pb`), `km_mld` Koleje Małopolskie
feeder buses (vehicles, trip updates, alerts).

Realtime files are polled every `REALTIME_POLL_INTERVAL` (10 s) unless a feed sets `realtime_poll_interval`: ZTP
(updates every ~5 s) every 5 s, `km_kolej` (every ~5 min) every 30 s. Data stays live for `REALTIME_MAX_AGE` (120 s)
after the last change, or the feed's `realtime_max_age` (`km_kolej`: 600): the API ignores trip updates and vehicles
measured longer ago (so nothing stale stays on boards or the map when the worker or a feed stops), and the worker
drops vehicle positions older than that within a file (ZTP sends some minutes old).

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
    realtime/ poller (one task per file) -> normalize -> store (Redis)
             runtime.py: per feed, the trip index (trip_index.py) and recent delays/fixes;
             normalize: resolve.py (trip_id -> timetable trip), trip_updates.py, vehicles.py, alerts.py
  api/       cli.py, app.py, routers/ (HTTP only), services/ (logic), schemas/, data/ (catalog + stops in memory, Redis, SQL/geo)
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
- **Derived at import**: `core.service_date` (calendar expanded), `core.shape_line`, `core.stop_direction` (where
  trips go next) and `core.stop_departure`: every stop time a board can show (pickup allowed, not the trip's last
  stop), with its route and headsign, indexed by (stop, service, time), so a board reads one index range per stop
  and day with no joins. It is partitioned per version like `gtfs.*` (`core.stop_departure__kmk_t__v3`) and
  dropped with it (about 60 % of the size of `stop_times`).
- **Timetable snapshot** (`gtfs_backend/snapshot.py`, `SNAPSHOT_DIR`): what boards and trip views read instead of
  Postgres. The worker builds it in its import process (a few seconds) after every import, before `static:updated`,
  and on every new local day (`snapshot:updated`): each feed's authoritative version on yesterday, today and tomorrow,
  their trips, stop times and shapes, and every departure (`core.stop_departure`) sorted by stop and time, as numpy
  columns. A new generation folder is written and `current` switched to it in one rename; the API maps the columns
  (`np.load`, mmap: one copy in the page cache for all its processes) and swaps the whole snapshot at once. A day is
  used only while the snapshot has the version the API's catalog has for it; other dates (paging past tomorrow) and a
  missing snapshot read Postgres as before.
- **Versions**: every new zip is a `core.feed_version`, imported in one transaction, numbered per feed (never
  reused) and joined via `feed_version_id` (`kmk_t_v3`). Find the newest by `version`, not by id (`kmk_t_v10` sorts
  before `kmk_t_v9` as text). For each service date the newest ready version with service that day is
  authoritative (`core.service_day_version`), so night trips survive an overnight update. A version is retired
  (rows dropped, zip kept in `ARCHIVE_DIR`) once it serves nothing from yesterday on. A failed import never
  replaces anything. Dropping partitions needs an exclusive lock, so retirement never runs during a worker import
  and gives up after 2 s of waiting (it is retried). A crashed import process is replaced.
- **Stops** (`core.stop_group` logical, `core.stop` physical poles, `core.stop_link` feed stop to our stop) have
  our own stable 5-letter ids and URL keys like `rondo-mogilskie01-qphdu`; only the part after the last dash counts.
  GTFS words keep their GTFS meaning: a *station* is a feed's `location_type=1` stop and `platform_code` is the
  GTFS field. Stop groups are ours. `stop_id` is always a feed's GTFS id; our stop, wherever a column or payload
  points at it, is `public_stop_id` (its public 5-letter id, the last part of the `key` the API shows).
  - A feed's stations seed the stop groups. ZTP has none and names them in its stop codes instead, which
    `stop_code_pattern` in feeds.yml reads: `802-01` is `platform_code` 01 of station 802. A feed that follows GTFS
    needs no pattern; a stop's own `parent_station` and `platform_code` always win.
  - ZTP's feeds share stop codes (`stop_namespace: ztp`): `802-01` is the same stop in all of them.
  - Imports match stops by code, then name + `platform_code` nearby, then location; ambiguous cases get a new id
    instead of a guess.
  - MLD buses mostly use ZTP's poles (`share_stops_with: [ztp]`). OpenStreetMap is the source of truth for whether an
    MLD stop is a pole of its own: a pole with the stop's own code, or a code or bay ZTP doesn't have, keeps it
    separate; a ZTP code links it to that pole. Without such a pole nearby, position, direction, modes and next
    stop decide, and an MLD stop with no ZTP pole keeps its own. The OSM poles come from a local file
    (downloaded by the worker on first start and refreshed daily, or by `gtfs-worker fetch-osm`; without it stops match
    by position alone).
    Details: [docs/stop-matching.md](docs/stop-matching.md).

### Live data (Redis, and each API process's memory)

Redis holds only the latest realtime updates plus poll stats. After every write the worker publishes what changed
(`rt:updated`: changed vehicles and trips, the feed's alerts, numbered per file); each API process loads every feed
from Redis when it starts and then follows those messages, keeping vehicles (in a grid for box queries), trips with
their per-stop predictions (indexed by stop) and alerts in memory (`api/data/live.py`). Requests read that, not Redis:
`/vehicles`, boards, trips and alerts make no Redis call. A message missed (its number jumps) or from another worker
run loads that feed from Redis again; a lost subscription loads all. While the worker or Redis is down the last data
stays, hidden once older than the feed's max age. The layout lives in `gtfs_backend/live_layout.py`.

| key | content |
|---|---|
| `rt:veh:geo` | GEO set of all vehicles (member `{feed_id}:{vehicle_id}`) for bbox/radius queries |
| `rt:veh:{feed_id}:{vehicle_id}` | vehicle JSON: trip, route, headsign, delay, stop; removed when the vehicle leaves the feed (TTL `VEHICLE_TTL` 120 s, refreshed while the file is unchanged, up to `realtime_max_age`) |
| `rt:trip:{feed_id}:{YYYYMMDD}:{trip_id}` | per-stop scheduled/predicted times and delays plus the `version`; stops the feed dropped stay with their last known times; removed when the trip leaves the feed |
| `rt:tripstate:{feed_id}` | hash: trip key → `[version, canceled, vehicle_id, timestamp, deleted]` of every trip in `rt:trip` (TTL `TRIP_TTL`) |
| `rt:alerts:{feed_id}` | service alerts, informed entities resolved to ours (trip key, public stop ids, station → stop group) |
| `rt:meta:{feed_id}:{kind}` | poll stats after every poll: `ok`/`last_error`, `last_poll`/`last_change`, `age_s`, timings, counts, unmatched, route mismatches, `median_delay_s`, `trip_relationships`/`stop_relationships` (GTFS-RT `schedule_relationship` counts: `CANCELED`, `NEW`, `DUPLICATED`, `SKIPPED`, `NO_DATA`, ...), `block_late_next` (trips predicted at their last stop after the next trip of their block is due) and `block_late_next_without_update` (of those, next trips without an update of their own) |
| `rt:unmatched:{feed_id}:{kind}` | unmatched ids (trips, vehicles, alert entities), for debugging |
| `rt:seq:{feed_id}:{kind}` | `{run}:{seq}` of the file's last change message: what an API process loading from Redis already has |
| pub/sub `rt:updated` | what each write changed (`LiveChanges`: `run`, `seq`, `vehicles` `{set, del}`, `trips` `{set, del}` as compact trips, `alerts`) |
| pub/sub `static:updated`, `snapshot:updated` | a new timetable version (and its snapshot) / a new day's snapshot |
| `metrics:{epoch minute}` | latency (API, per route template) and data age (worker, per realtime file) histograms of all processes, for `/metrics` (`gtfs_backend/metrics.py`, kept 2 h) |

Rules:

- **`trip_id` is the source of truth.** Route, headsign, direction and stops come from the static trip it resolves
  to (version + service day around "now"); realtime route ids are only compared and counted. A feed's `start_date`
  would pick the service day (none sends one yet).
- **Unmatched means not shown**: unknown trips, trips outside their service window and idle vehicles never reach
  the map or departures. Nor do trips that aren't the timetabled trip their id names (GTFS-RT `NEW`, `ADDED`,
  `DUPLICATED`: counted as unmatched with their kind, and a `/health` problem) until they are supported.
  `CANCELED` trips show as canceled; `DELETED` ones (and their vehicles) aren't shown at all.
- **Counted, not used yet** (`rt:meta`): `schedule_relationship` values, and how often a late trip runs into the next
  trip of its block (GTFS-RT asks producers to send the next trips' updates; ZTP does).
- **Delays are recomputed** as predicted − scheduled, scheduled = the feed's day start + GTFS time (noon minus 12 h
  unless `day_start: midnight`). They **propagate** (GTFS-RT): a stop without an update takes the last updated
  stop's delay (`propagated: true`); `NO_DATA` stops that, `SKIPPED` passes it on.
- **Clock-change days**: ZTP's trams count times from local midnight, not noon minus 12 h, so on the two
  clock-change days they run an hour off the GTFS reading (seen 2026-10-25, checked against tram GPS). `kmk_t`
  therefore has `day_start: midnight`. `median_delay_s` of about ±3600 on such a day would mean ZTP changed this.
- **Service suffix**: after midnight ZTP's trams name a still-running trip of yesterday's timetable with today's
  `_service_N` (seen 2026-10-31, when a new timetable started), an id that exists in no version. `kmk_t` has
  `ignore_service_suffix: true`: an id not found as is is retried without the suffix and the time window picks the trip.
- **Timetables in memory**: per feed, the trips running yesterday/today/tomorrow (`trip_index.py`). It is reloaded
  after an import and on a new day **in the background**, while the previous index keeps serving: a slow or
  unreachable Postgres never stops the live data (a failed reload is retried after 10 s). Unchanged versions are
  reused, so only an import of the feed itself reads its trips again. Stop times are loaded per trip when it first
  appears; if that fails, its update goes out without per-stop times and the load is retried after 30 s.
- **After an import** the worker loads the feed's new index, each of its realtime files processes its last update
  again at its next poll, and only then `static:updated` goes out (at most 90 s later): the API uses realtime only
  for the version serving the day, so switching it first would leave boards without realtime until the files change.
- A feed whose header timestamp goes back (a producer restarted with its clock behind) is taken once nothing newer
  came for its max age; before that an older copy (another server, a cache) is ignored.
- Downloads are checked against `Content-Length` and parsed before use. ZTP briefly serves empty or partial files
  while replacing them; those, 5xx and 429 are retried and never overwrite good data. Each attempt gives up after
  the feed's poll interval (at least 5 s, at most `REALTIME_FETCH_TIMEOUT`). Requests send `USER_AGENT`
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
| stop or stop group | `rondo-mogilskie01-qphdu` (or the bare id) | `/stops/{stop_key}/departures` |
| vehicle | `{feed_id}:{vehicle_id}` | `/vehicles/{vehicle_key}/trip` |
| trip | `{feed_id}:{YYYYMMDD}:{trip_id}` (`trip_key`) | `/trips/{trip_key}`, `/trips/{trip_key}/live` |
| route | `{feed_id}:{route_id}` | `/routes/{route_key}/shapes`, `/alerts?route={key}` |
| alert | `{feed_id}:{id}` | `/alerts/{alert_key}` |

The source id comes last and is kept whole, so ids with `:` or `/` work. Responses carry no id a key already
holds (no `feed_id`/`trip_id` next to `trip_key`, no `feed_id` next to a route's `key`): the feed id is a key's
first part. Routes appear in full (`Route`: a `RouteRef` plus `route_type` and `agency`) or as a `RouteRef` (`key`, `name`, `mode`)
in lists. No feed-internal id (`route_id`, `stop_id`, `trip_id`) appears on its own, alerts included. Every field of a schema is always present (`null` when unset).
Polled responses (vehicles, boards, live trips) carry the `timestamp` of the newest realtime data they show, not
of the request, so their ETag only changes with the data.

| endpoint | what |
|---|---|
| `GET /vehicles?bbox=minLon,minLat,maxLon,maxLat[&modes=tram,bus][&routes=52]` | live vehicles in a box (≤ 50 km): position, bearing, route (`RouteRef`, with its mode), headsign, delay, `trip_key`, and where the feed reports it `stop_sequence` + `status` (`stopped_at`, `incoming_at`, `in_transit_to`) |
| `GET /vehicles/{vehicle_key}/trip` | live state of the vehicle's current trip, as `/trips/{key}/live` (polling follows it onto the next: fetch `/trips/{key}` again when `key` changes) |
| `GET /stops?bbox=…[&kind=group][&modes=…]` | stops (`kind=stop`, default) with `group_key` and `platform_code`, or stop groups (`kind=group`, for far zoom); only some modes, e.g. `kind=group&modes=rail` for train stations (≤ 3000, `truncated`) |
| `GET /stops/search?q=…` | stop groups by name (accent/case-insensitive; only `search_namespaces`, `ztp`) with their `stops` and directions; `limit` default 10. In memory, ranked as pg_trgm (prefix, substring, `word_similarity`, `similarity`, name: `gtfs_backend/trigram.py`, checked against the database) |
| `GET /search?q=…` | everything a query names: `stops` (as `/stops/search`) and `routes` of today's timetables by name (`52`, `N1`: the exact name first, then names starting with it) |
| `GET /stops/{stop_key}/departures[?time=…][&feeds=…][&routes=…][&limit=20]` | timetable + realtime from `time` (epoch, default now) on, filtered by feed ids and route names; `vehicle_key` to follow; alerts as refs |
| `GET /trips/{trip_key}` | what doesn't change while the trip runs: route, shape polyline, every stop with place and scheduled times (`Cache-Control` like stops) |
| `GET /trips/{trip_key}/live` | to poll: every stop's predictions and `status` (match to the trip's `stops` by `stop_sequence`), the trip's status, live vehicle, alerts as refs |
| `GET /routes/{route_key}/shapes[?date=YYYYMMDD]` | the route's shapes that day, most used first per direction |
| `GET /alerts[?feed=…][&route=…][&stop=…][&active=true]`, `GET /alerts/{alert_key}` | alerts, severe first, with `entities` saying where each applies (boards and trips carry refs: no `url`, no `entities`) |
| `GET /health` | DB/Redis reachability (503 if down), static versions, poll stats, `fresh` with each feed's `problems` |
| `GET /ws` (WebSocket) | push instead of polling: `{"op": "subscribe" \| "unsubscribe", "topics": [...]}`; `tile:{z}/{x}/{y}` (z 10–16: vehicles, updates as `upsert`/`remove`), `stop:{key}` (the board), `trip:{key}` (its live state); a `snapshot` (the REST body) on subscribe, then `update`s |
| `GET /metrics[?window=300]` | p50/p90/p99 per endpoint (route template, all API processes) and each realtime file's data age (now minus its feed header timestamp), over the last `window` seconds (60–3600) |

**Errors**: `{"error": {"code": "not_found" | "invalid_input" | "method_not_allowed" | …, "message": "…"}}`: 404
unknown things, 422 malformed keys/dates/boxes/times, unknown feeds, 503 `unavailable` with `Retry-After` when connections are
busy or a store is down, 500 `internal` without details.

**Several processes.** `API_WORKERS` processes share one port, each with bounded pools (`DB_POOL_MAX_SIZE`,
`REDIS_POOL_MAX_SIZE`) and its own catalog copy (feed versions of yesterday to a week ahead, routes, and all stops,
stop groups and today's directions, the routes of those versions, and the timetable snapshot, so boards, trips, the
map, search and alerts need no Postgres), loaded
before the first request and refreshed in the background on `static:updated`, a new day and every
`CATALOG_REFRESH_INTERVAL` while requests keep using the loaded one. `DB_POOL_MAX_SIZE` is the same for the
worker and every API process: keep `(API_WORKERS + 1) × DB_POOL_MAX_SIZE` below Postgres' `max_connections` (100).

**No response cache.** Every response is computed when asked for (`/metrics` and `Server-Timing` show how long it
takes; docs/benchmark.md has the numbers), so live data is as fresh as Redis.
The ETag is the body's hash: polling clients get a 304 while nothing changed. Public responses carry `Cache-Control: public, max-age=…` for clients and proxies:
`CACHE_TTL_STATIC` (300 s: stops, search, trips, shapes) and `CACHE_TTL_ALERTS` (15 s).

**Privacy.** No accounts, cookies, identifiers or client addresses; requests aren't logged (uvicorn's access log is
off) and `X-Forwarded-For` is ignored. A reverse proxy or CDN in front should do the same (no IPs or query strings
in logs).

Notes:

- **Directions**: each stop lists where its trips go next (`towards`: the next stop group), with routes and
  headsigns, most served first; under 5 % of trips are left out. Precomputed per version (`core.stop_direction`).
  Route colors are not passed on (often unset or wrong).
- **Departure boards** cover every authoritative service day that can run in the window and reach back an hour so
  delayed departures stay. The timetable comes from the snapshot (`core.stop_departure` for days it lacks), realtime
  from the live state's predictions at the board's stops. Predictions are used only for the same version. `status` says
  where the time comes from: `scheduled` (timetable; also where the feed says `NO_DATA`), `live` (the feed's
  prediction for this stop), `predicted` (a delay the feed gave for an earlier stop, carried over), `canceled`,
  `skipped`; `time` is the estimate if known. Trip views' stops (`/live`) carry the same `status`. `departed: true`
  when the vehicle reports a later stop of the trip (VehiclePosition `current_stop_sequence`), else when the time is
  past; a TripUpdate not listing a stop doesn't count as passing it. `vehicle_state` (`at_stop`, `arriving`) is set when the vehicle reports this stop. Last stops and stops without pickup are not
  departures; `on_demand` marks GTFS pickup type 2/3.
- **Paging**: a board starts at `time` (epoch seconds, default now: departed ones are left out; with a time they
  come with `departed: true`). Page forward by passing the last departure's `time`; departures in that second can repeat
  (tell them apart by `trip_key` + `stop_sequence`). The first window is where the stop's hourly departures
  (today's timetables, counted once a day) expect 1.5 pages, so a board needs one read; if that isn't enough
  (filters, another day's timetable) it widens through 2 h, 6 h and 24 h, each step reading only the time it adds. A
  page is full unless a trip scheduled past the window is predicted before its last departure (feeds allow any
  amount early; the board finds such trips among its stops' predictions).
- **Filters**: `feeds` (feed ids) and `routes` (route names, `RouteRef.name`), comma separated or repeated; they
  narrow the board and its alerts, and an unknown feed is a 422. The board lists `routes`, every route departing from
  the stop in today's timetables on any day (weekend-only ones too), so clients know what to filter by. Feeds publish
  line variants as separate routes (km_mld has two `A18`), so a name can repeat: dedupe by name for display.
  `/vehicles` takes `routes` as well.
- **Trip view**: `shape.source` is `shape`, or `stops` when the feed has none (trains). `vehicle` is set while one runs the
  trip; its `stop_sequence` and `status` (where the feed reports them) say which stop it is at or heading to.
- **Polling**: live responses are `no-cache`; clients refetch the open view about every 10 s with `If-None-Match`
  and stop in the background.
- **Push** (`/ws`, `api/ws.py`): one WebSocket per client, JSON text messages (compressed with permessage-deflate). A
  subscription gets a `snapshot`, the body the REST endpoint would return; then every change the process's live state
  applies marks the topics it touches (a vehicle's old and new tile, its trip and the boards of its stops; a trip's
  boards and itself; alerts every board and trip), and every 15 s all of them. A marked topic is computed again at
  most every second (boards every 2 s) and sent only if it changed: tiles as `{"timestamp", "upsert": [Vehicle],
  "remove": [key]}`, boards and trips whole. Each message is serialized once and sent to every subscriber; a slow
  connection keeps only the latest one per topic (a tile's whole snapshot rather than a second update). At most 50
  topics per connection; errors come as `{"type": "error", "topic", "error": {"code", "message"}}`. A reverse proxy
  needs to pass the upgrade (nginx: `proxy_http_version 1.1`, `Upgrade`/`Connection` headers, a `proxy_read_timeout`
  above the 20 s ping interval).
- **Alerts**: `cause`/`effect` are the GTFS-RT enums in lowercase (`construction`, `detour`), `severity` is `info`,
  `warning` or `severe`; unknown or unset is `null`. Each of `entities` is one place the alert applies, its fields
  combined with AND (GTFS-RT): `{route, stop}` is that line at that stop only, `{route, direction_id}` one direction,
  `{trip_key}` one dated trip, `{mode}` every line of the mode, all `null` the whole feed. `stop` is a stop, or a stop
  group when the feed names a whole station, with coordinates for the map. Entities naming something not in today's
  timetables are dropped (and counted as unmatched); an alert left with none applies nowhere. Boards and trips carry
  alerts active now or starting within a day; `/alerts` returns all unless `active=true`.
- **Health**: `ok` means Postgres and Redis answer. `fresh` is false while any feed has `problems`: no timetable,
  failed last import, static not checked for `HEALTH_STATIC_MAX_AGE` (3 h), or a realtime file not polled, failing
  (5 polls) or older than `HEALTH_REALTIME_MAX_AGE` (5 min; a larger `realtime_max_age` wins), or sending trips of a
  kind not shown yet (`NEW`, `DUPLICATED`, ...). Alerts files are exempt from the age check.
- Responses carry `ETag` and `Cache-Control` (static data 5 min, alerts 15 s); bodies over 1 kB are gzipped (level 6).
- **`Server-Timing`** on every response (`SERVER_TIMING`, on by default) says where the time went: `db` (queries),
  `db-wait` (waiting for a pooled connection), `redis` (commands; a pipeline is one), `serialize` and `total`;
  browser dev tools show it. Concurrent work adds up, so the parts can exceed `total`.
- Queries name one feed version per block: joining `feed_version_id` across tables defeats partition pruning
  (a 20 ms board became 1.5 s).

## Benchmarks

Micro benchmarks (`uv run pytest bench/micro`), a load test replaying recorded feeds (`bench/replay.py`,
`bench/k6/rest_mix.js`) and the server's `/metrics`: see [docs/benchmark.md](docs/benchmark.md). `CLOCK_OFFSET` (seconds)
lets the worker and the API run at a recording's time; it is 0 in production.

## Tests

```sh
docker compose -f docker-compose.dev.yml up -d && uv run pytest
```

DB and Redis tests use a separate `gtfs_test` database / Redis db 15 and are skipped when the services are down.
`tests/fixtures/dst_2026-10-25/` is the realtime snapshot from the clock change; `tests/fixtures/static/` holds the
matching timetables (made by `tests/fixtures/make_fixtures.py`).

## License

[GPL v3](LICENSE) or later.
