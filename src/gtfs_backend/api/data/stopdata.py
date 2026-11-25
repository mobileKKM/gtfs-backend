"""Stops, stop groups and where trips go from each stop today, in memory: a few thousand rows that boards, the map,
search and alerts read on every request. Loaded with the catalog (api/data/catalog.py), which reloads on
`static:updated` (the only time stops change: imports match them) and when the day changes (directions are today's).

The same rules as the SQL they replace: a stop is *served* when it is active and has modes (trips in a current
version); groups take the modes of their served stops; map and search only show served ones.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from psycopg import AsyncConnection

from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.schemas import Stop
from gtfs_backend.ids import parse_url_key

# (public_stop_id, next group id, its name, its key, feed_version_id, route_id, headsign, trips)
DirectionRow = tuple[str, str, str, str, str, str, str, int]
# departures per hour of the day (0-23, times past 24:00 folded onto the morning) in today's timetables
Hourly = tuple[int, ...]
# what an Hourly profile was counted for: today's (feed_version_id, date) per feed
HourlyKey = tuple[tuple[str, date], ...]
NO_DEPARTURES: Hourly = (0,) * 24


@dataclass(frozen=True)
class _Pole:
    stop: Stop
    group_id: str | None
    served: bool


@dataclass(frozen=True)
class _Group:
    stop: Stop
    active: bool
    namespace: str | None


class StopData:
    def __init__(
        self,
        poles: dict[str, _Pole],
        groups: dict[str, _Group],
        directions: dict[str, list[DirectionRow]],
        hourly: dict[str, Hourly] | None = None,
        hourly_for: HourlyKey = (),
    ):
        self._poles = poles
        self._groups = groups
        self._directions = directions
        self._hourly = hourly or {}
        self.hourly_for = hourly_for
        self._members: dict[str, list[str]] = defaultdict(list)  # group id -> all its stops (active or not)
        for pid, pole in poles.items():
            if pole.group_id is not None:
                self._members[pole.group_id].append(pid)
        # active stops of each group, in platform order (as group_stops sorted them in SQL)
        self._served_members = {
            gid: sorted(
                (pid for pid in pids if poles[pid].served),
                key=lambda pid: (poles[pid].stop.platform_code is None, poles[pid].stop.platform_code or "", pid),
            )
            for gid, pids in self._members.items()
        }
        # map queries: plain tuples in id order (attribute access on thousands of models costs more than the rest),
        # each with its JSON, serialized once here instead of on every request
        self._map_poles = [
            (p.stop.lat, p.stop.lon, frozenset(p.stop.modes), p.stop.model_dump_json().encode())
            for pid in sorted(poles)
            if (p := poles[pid]).served and p.stop.lat is not None and p.stop.lon is not None
        ]
        self._map_groups = [
            (
                g.stop.lat,
                g.stop.lon,
                frozenset(m for p in members for m in poles[p].stop.modes),
                g.stop.model_dump_json().encode(),
            )
            for gid in sorted(groups)
            if (g := groups[gid]).active
            and (members := self._served_members.get(gid))
            and g.stop.lat is not None
            and g.stop.lon is not None
        ]

    @classmethod
    def empty(cls) -> StopData:
        return cls({}, {}, {})

    # ------------------------------------------------------------------------------------------- lookups

    def find(self, key: str) -> Stop | None:
        """A stop group or stop by key (`rondo-mogilskie-qphdu`) or bare id; the slug part is ignored."""
        sid = parse_url_key(key)
        if sid is None:
            return None
        if (group := self._groups.get(sid)) is not None:
            return group.stop
        pole = self._poles.get(sid)
        return pole.stop if pole else None

    def public_stop_ids(self, stop: Stop) -> list[str]:
        """The stops a stop or group covers: all of a group's (active or not, the timetables decide), or itself."""
        if stop.kind == "stop":
            return [stop.id]
        return sorted(self._members.get(stop.id, []))

    def stops_by_id(self, ids) -> dict[str, Stop]:
        return {i: pole.stop for i in ids if (pole := self._poles.get(i)) is not None}

    def groups_by_id(self, ids) -> dict[str, Stop]:
        return {i: group.stop for i in ids if (group := self._groups.get(i)) is not None}

    def group_stops(self, group_ids: list[str]) -> dict[str, list[Stop]]:
        """Active stops of each group, by platform_code."""
        return {g: [self._poles[p].stop for p in self._served_members.get(g, [])] for g in group_ids}

    def direction_rows(self, stops: list[str]) -> list[DirectionRow]:
        """Where trips from these stops go next, in the timetable versions serving today."""
        return [row for pid in stops for row in self._directions.get(pid, [])]

    def hourly(self, stops: list[str]) -> Hourly:
        """Departures per hour of the day from these stops together, in today's timetables."""
        profiles = [h for pid in stops if (h := self._hourly.get(pid)) is not None]
        return tuple(map(sum, zip(*profiles, strict=True))) if profiles else NO_DEPARTURES

    # ------------------------------------------------------------------------------------------- map

    def in_bbox_json(self, bbox: Bbox, modes: set[str], kind: str, limit: int) -> list[bytes]:
        """The JSON of served stops (or active groups with a served stop), inside the box and of any of `modes` (all
        without), by id, at most `limit`. A group has the modes of its served stops."""
        out: list[bytes] = []
        min_lon, min_lat, max_lon, max_lat = bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat
        for lat, lon, stop_modes, body in self._map_groups if kind == "group" else self._map_poles:
            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon and (not modes or not modes.isdisjoint(stop_modes)):
                out.append(body)
                if len(out) >= limit:
                    break
        return out


async def load_hourly(conn: AsyncConnection, today: HourlyKey) -> dict[str, Hourly]:
    """Each stop's departures per hour in today's timetables (a few hundred ms: counted once a day, see load_stop_data)."""
    if not today:
        return {}
    cur = await conn.execute(
        """
        SELECT d.public_stop_id, d.departure_s / 3600 %% 24, count(*)
        FROM unnest(%s::text[], %s::date[]) AS t (fv, day)
        JOIN core.service_date sd ON sd.feed_version_id = t.fv AND sd.date = t.day
        JOIN core.stop_departure d ON d.feed_version_id = t.fv AND d.service_id = sd.service_id
        GROUP BY 1, 2
        """,
        ([fv for fv, _ in today], [d for _, d in today]),
    )
    counts: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    for pid, hour, n in await cur.fetchall():
        counts[pid][hour] += n
    return {pid: tuple(c) for pid, c in counts.items()}


async def load_stop_data(conn: AsyncConnection, today: HourlyKey, previous: StopData | None = None) -> StopData:
    """`today`: the (feed_version_id, date) serving today per feed. The hourly profiles of `previous` are kept while
    they were counted for the same."""
    today_versions = [fv for fv, _ in today]
    cur = await conn.execute("SELECT id, url_key, name, ST_Y(geom), ST_X(geom), active, code_namespace FROM core.stop_group")
    group_rows = await cur.fetchall()
    group_keys = {gid: key for gid, key, *_ in group_rows}
    cur = await conn.execute(
        "SELECT id, url_key, name, ST_Y(geom), ST_X(geom), modes, stop_group_id, platform_code, active FROM core.stop"
    )
    poles: dict[str, _Pole] = {}
    group_modes: dict[str, set[str]] = defaultdict(set)
    for pid, key, name, lat, lon, modes, gid, platform_code, active in await cur.fetchall():
        served = bool(active and modes)
        stop = Stop(
            kind="stop",
            key=key,
            name=name,
            lat=lat,
            lon=lon,
            modes=list(modes or []),
            group_key=group_keys.get(gid),
            platform_code=platform_code,
        )
        poles[pid] = _Pole(stop, gid, served)
        if served and gid is not None:
            group_modes[gid].update(modes)
    groups = {
        gid: _Group(
            Stop(kind="group", key=key, name=name, lat=lat, lon=lon, modes=sorted(group_modes.get(gid, ()))),
            active,
            namespace,
        )
        for gid, key, name, lat, lon, active, namespace in group_rows
    }
    directions: dict[str, list[DirectionRow]] = defaultdict(list)
    if today_versions:
        cur = await conn.execute(
            """
            SELECT d.public_stop_id, d.next_group_id, g.name, g.url_key, d.feed_version_id, d.route_id, d.headsign, d.trips
            FROM core.stop_direction d JOIN core.stop_group g ON g.id = d.next_group_id
            WHERE d.feed_version_id = ANY(%s)
            -- the primary key's order: directions with as many trips (and headsigns) keep it, so ties stay put
            ORDER BY d.feed_version_id, d.public_stop_id, d.next_group_id, d.route_id, d.headsign
            """,
            (sorted(today_versions),),
        )
        for row in await cur.fetchall():
            directions[row[0]].append(tuple(row))
    if previous is not None and previous.hourly_for == today:
        hourly = previous._hourly
    else:
        hourly = await load_hourly(conn, today)
    return StopData(poles, groups, dict(directions), hourly, today)
