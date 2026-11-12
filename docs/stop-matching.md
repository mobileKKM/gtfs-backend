# Stop matching

Every feed names its stops differently, and the same physical bus stop shows up in several feeds. Stop matching decides
which feed stops are **one physical pole**, so that the app shows a single stop with the departures of every operator, and
so that the ids in our API stay the same when a feed renames or renumbers things.

This page explains how the decision is made. It is meant for anyone changing the matching or wondering why a stop ended up
where it did.

## Terms

| Term | Meaning |
|---|---|
| **Stop** (`core.stop`) | One physical pole or bay, e.g. "Rondo Mogilskie 02": a GTFS stop (`location_type=0`), merged across feeds. |
| **Stop group** (`core.stop_group`) | Our logical stop: all the poles of "Rondo Mogilskie". |
| **Station** | A feed's own grouping: a GTFS `location_type=1` stop, or what its `stop_code_pattern` names. Seeds a stop group. |
| **platform_code** | The GTFS field, e.g. `02`; from the feed, or from its `stop_code_pattern`. |
| **Link** (`core.stop_link`) | Says which of our stops (`stop_id`) a feed's stop (`feed_stop_id`, its GTFS `stop_id`) is. |
| **Namespace** | Feeds that number their stops the same way. ZTP's three feeds share one (`802-01` is the same pole in all). |
| **Shared namespace** | A feed that mostly uses another namespace's poles (`share_stops_with: [ztp]`): MLD feeder buses in Kraków. |

Our ids are 5 random letters that never derive from a code or a name, so renames keep them. The code is in
[`src/gtfs_backend/worker/static/stops/`](../src/gtfs_backend/worker/static/stops/): `plan.py` and `shared.py` decide
without the database (and are unit-tested in `tests/worker/test_stops_match.py`), `apply.py` reads the inputs and writes the
links.

## A feed's stops in GTFS shape

Before matching, every stop of the feed gets a station and a `platform_code` (`normalize_stop` in `model.py`):

- its own GTFS `parent_station` and `platform_code` where it has them (Koleje Małopolskie's trains and MLD have stations);
- otherwise what the feed's `stop_code_pattern` (feeds.yml) reads from its `stop_code`. ZTP has no stations and names them
  in its codes: `(?P<station>.+)-(?P<platform_code>[^-]+)` reads `802-01` as platform 01 of station 802;
- otherwise no station: stops are grouped by name.

A new feed that follows GTFS needs nothing; one that encodes its stations in codes needs one pattern. Without a pattern a
code is never split, so another operator's dashes can't be mistaken for ZTP's numbering.

## Two kinds of matching

**Within a namespace** (`plan.py`). A new version of a ZTP feed matches its stops to the existing stops by code, then by
name plus `platform_code` nearby, then by location. When it can't tell, it creates a new id rather than guess. Stops of
one station form one stop group.

**Onto another namespace's poles** (`shared.py`). This is the hard case: MLD's own stop list has no numbering in common with
ZTP's, and its coordinates are often 10–60 m off, so a stop has to be recognised as a ZTP pole, or as a pole of its own that
ZTP doesn't publish. The rest of this page is about that. The shared namespace is never modified.

## How a stop of the importing feed is decided

```
            ┌─ 0. OpenStreetMap says which pole it is ──→ link, or "own pole"
  MLD stop ─┤
            ├─ 1. a ZTP pole within 8 m ───────────────→ link
            │
            └─ 2. which logical stop is it? ──→ 3. which pole of that stop? ──→ link, or "own pole"
```

The first step that has something to say wins. A stop that nothing links stays an **own pole**: a stop of the importing
feed's namespace, still placed in the right group.

### 0. OpenStreetMap

OSM is the source of truth for whether an MLD stop is a pole of its own. Mappers tag poles with the operator's code in
`ref`, and say whether buses or trams stop there. The nearest OSM bus pole within 60 m of the stop is read by its `ref`:

| `ref` on the OSM pole | What it tells us | Result |
|---|---|---|
| a ZTP code ZTP publishes (`889-03`) | this is that ZTP pole | **link** to it, however far the coordinates are |
| a ZTP code ZTP doesn't publish (`802-07`) | an MLD pole ZTP doesn't have | **own pole** |
| the stop's own code (`200`) | OSM maps it as the operator's pole | **own pole** |
| a bay of a bus station (`271-01-06`) | a bay of its own | **link** if a ZTP pole sits on that same bay, else **own pole** |
| nothing, or another numbering (`26`) | OSM says nothing | steps 1–3 decide |

Some guards keep OSM from over-reaching:

- A stop is called an own pole only when OSM really has a pole for it. A ZTP-style code or a bay must be on an OSM pole
  within **20 m**. Further off it may be a neighbouring pole and the stop's GPS position is just wrong, so the rules below
  decide. Only the stop's own code is identity enough to count up to 60 m away.
- A ref whose ZTP pole is more than 100 m from the OSM pole is treated as a typo. A ref whose ZTP pole serves the opposite
  direction is skipped.
- Only about 4% of MLD stops carry a ZTP code on their OSM pole, but a further ~980 carry the stop's own code, so OSM
  decides roughly a quarter of the feed.

### 1. Same pole by position

A ZTP pole within **8 m** is the same pole, as long as ZTP serves it with the importing feed's mode (a bus stop next to a
tram-only pole never matches). If two poles are about equally close (within 3 m), direction of travel decides, then the lowest
code: two codes on one spot are one pole.

### 2. Which logical stop

All of the stop's station mates that already linked vote for their group. Otherwise, a ZTP group with the same name within
300 m (or a similar name within 150 m). Names tolerate abbreviations ("bp." for "Biskupa"), typos and a "Kraków" prefix.
This decides the group even for stops that end up as own poles.

### 3. Which pole of that logical stop

The nearest pole of the group that is within **60 m**, is served in the same direction (within 90°), and has the right mode.
Beyond 30 m it needs more proof, because far-off coordinates are the most common source of wrong matches:

- the pole must lie within **40 m along the line of travel**. Sideways is just inexact coordinates, but a pole 47 m further up
  the road is the *next* stop (Czyżyny Dworzec 3470); and
- it must lead to the **same next logical stop** as the importing feed's stop.

OSM can also veto a pole here: if the stop and the pole each sit within 10 m of an OSM pole, and those two OSM poles are
more than 25 m apart, they are different poles (Czernichów Rynek 866). A stop with no known direction is never attached in
this step. If no pole qualifies, the stop keeps its own pole.

## What it reads

| Input | Where from |
|---|---|
| Direction of travel | previous stop → next stop, averaged over trips (`queries.py`) |
| Modes a pole serves | the shared feeds' own trips only (`served_modes`), **not** `core.stop.modes`, which counts every linked feed: a wrongly linked bus stop would turn a tram pole into a "bus pole" |
| Next stops | `stop_times` of the new version and `core.stop_direction` of the shared feeds |
| OSM poles | `data/osm/stops.json` (`OSM_STOPS_FILE`) |

The worker keeps the OSM file up to date itself. On start it downloads the [Geofabrik](https://download.geofabrik.de/europe/poland/malopolskie.html)
Małopolskie extract (about 200 MB, parsed in the import process, then discarded) and keeps the bus and tram poles; after that
it checks hourly and downloads again once the file is older than `OSM_REFRESH_INTERVAL` (1 day, the extract is published daily). Feeds that share stops
wait for the first download, trying three times before they start without it. A failed refresh is logged and the old file
stays. The file is replaced in one step, so an import never reads half of it. A new file is used by the next import:
stops are matched when a feed version is imported, so existing links don't change until then.

To do it by hand (the file is also needed outside the worker, e.g. for a local import):

```sh
uv run gtfs-worker fetch-osm
```

Without the file, steps 1–3 run alone and the OSM guarantees above are gone. Set `OSM_REFRESH_INTERVAL=0` to turn the
automatic download off.

## Constants

All distances live at the top of [`shared.py`](../src/gtfs_backend/worker/static/stops/shared.py).

| Constant | Value | Used for |
|---|---|---|
| `OSM_LINK_M` | 60 m | how far a stop may be from the OSM pole whose `ref` names its pole |
| `OSM_OWN_M` | 20 m | OSM pole of an unknown code or bay must be this close to call a stop an own pole |
| `OSM_REF_MAX_M` | 100 m | a ref's ZTP pole further than this from the OSM pole is a typo |
| `OSM_SNAP_M` / `OSM_SAME_POLE_M` | 10 m / 25 m | the OSM veto in step 3 |
| `SAME_POLE_M` | 8 m | step 1 |
| `IN_GROUP_POLE_M` | 60 m | furthest pole step 3 considers |
| `CONFIRMED_POLE_M` | 30 m | beyond this step 3 needs next-stop and along-line proof |
| `ALONG_TRACK_M` | 40 m | how far up or down the line a pole may be |
| `MAX_DIRECTION_DEG` | 90° | more apart than this is "the other side of the road" |

## Working on it

**Try a change without importing.** The matcher is a pure function, so it is easy to run on an already imported version and
compare with the stored links. The stops that moved are the ones to look at.

**Re-match after changing the rules.** The importer skips a zip it has already imported, so a version has to be deleted and
imported again. Stop the worker first, or it may import the same zip with the old rules:

```sh
# worker stopped
psql ... -c "DELETE FROM core.feed_version WHERE id = 'km_mld_v9'"
uv run gtfs-worker import-static --feed km_mld --file data/archive/km_mld/<sha256>.zip
```

**Cases the tests keep right:**

| Stop | Result | Why |
|---|---|---|
| Rondo Mogilskie 7620 | own pole | OSM has it as 802-07, which ZTP doesn't publish |
| Czyżyny Dworzec 3470 | own pole | OSM bay 06, ZTP's pole is on bay 01 |
| Giedroycia 3485 | own pole | the only ZTP pole nearby (214-01) is tram-only |
| Wieliczka Centrum 200 | own pole | OSM maps it, with the stop's own code, apart from ZTP's 2795-02 |
| Czernichów Rynek 866 | own pole | OSM has a pole at each, 29 m apart |
| Fort Mogiła 3482, 3483 | joined | OSM refs `889-04` and `889-03` |
| Niepołomice Kolejowa 3074 | joined | same direction and next stop as 2600-01 |
| Pleszowska 2833 | joined | the only OSM pole near, `419-02`, is 58 m away |

**Known limits.**
- Most stops have no OSM ref, so position, direction and next stop still decide for them.
- OSM can be coarse. At a bus loop where one ref covers many objects (Pleszów 218-05), a stop on the loop links to that pole.
- A stop whose GPS is far from every OSM pole, and whose candidate ZTP pole fails the checks in step 3, ends up an own pole
  by default. OSM never decided that; it is just what is left when nothing links.
