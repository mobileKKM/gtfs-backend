"""Attaching a feed's stops to another namespace's stops and stop groups (pure; apply.py does the DB work)."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from gtfs_backend.worker.static.stops.geometry import along_track_m, angle_diff, distance_m
from gtfs_backend.worker.static.stops.model import Group, NewStop, PublicStop
from gtfs_backend.worker.static.stops.names import names_match
from gtfs_backend.worker.static.stops.osm import OsmPole

OSM_LINK_M = 60.0  # the nearest OSM bus pole this close names the stop's pole by its ref (MLD coordinates are often off)
OSM_SNAP_M = 10.0  # a stop or pole this close to an OSM pole is that OSM pole
OSM_SAME_POLE_M = (
    25.0  # OSM poles further apart than this are different poles (an OSM platform and its stop_position are within ~20 m)
)
OSM_OWN_M = 20.0  # an OSM pole of a code ZTP doesn't have claims a stop as its own pole only this close (GPS can be off)
OSM_ON_STOP_M = 25.0  # an OSM pole this close to a shared stop is that stop's
BAY_REF = re.compile(r"\d+-\d+-\d+")  # OSM 'GGG-PP-NN': a bay of a bus station
OSM_REF_MAX_M = 100.0  # the pole a ref names must be this close to the OSM pole, or the ref is a typo
SAME_POLE_M = 8.0  # this close it is the same pole, whatever the names say
SHARED_AMBIGUOUS_M = 3.0  # second candidate this close to the best one -> decide by direction or don't guess
GROUP_EXACT_NAME_M = 300.0
GROUP_SIMILAR_NAME_M = 150.0
IN_GROUP_POLE_M = 60.0  # inside a known shared stop: the furthest a pole can be and still be the same one
ALONG_TRACK_M = 40.0  # a pole further up or down the line than this is the next or previous stop, not this one
CONFIRMED_POLE_M = 30.0  # up to here direction is enough; further off the pole must also lead to the same next stop
MAX_DIRECTION_DEG = 90.0  # travel directions further apart than this: the other side of the road


@dataclass
class SharedPlan:
    links: dict[str, str] = field(default_factory=dict)  # stop_id -> shared stop id
    group_for_key: dict[str, str] = field(default_factory=dict)  # group key -> shared group id
    methods: Counter = field(default_factory=Counter)
    warnings: list[str] = field(default_factory=list)


def plan_shared(
    stops: list[NewStop],
    shared: list[PublicStop],
    shared_groups: list[Group],
    stop_bearing: dict[str, float] | None = None,
    shared_bearing: dict[str, float] | None = None,
    shared_modes: dict[str, set[str]] | None = None,
    feed_modes: set[str] | None = None,
    stop_next: dict[str, set[str]] | None = None,
    shared_next: dict[str, set[str]] | None = None,
    osm: list[OsmPole] | None = None,
) -> SharedPlan:
    """Attach stops to the stops/groups of a shared namespace (MLD buses use ZTP's poles in Kraków).

    0. OpenStreetMap: the nearest OSM bus pole within 60 m, when it has a ref that says which pole it is. A ZTP code
       ('889-03') or a bay ('271-01-06') that a shared stop is on links the stop to that stop, however far
       the coordinates are apart. A ZTP code no shared stop has (OSM 802-07: an MLD pole ZTP doesn't publish),
       a bay no shared stop is on (Czyżyny Dworzec 06) or the operator's own code (Wieliczka Centrum 200) keeps
       the stop's own pole: it is none of ZTP's poles, so no rule below may attach it to one. Only the operator's own
       code names the pole at any distance; the other two need the OSM pole within 20 m. Without such a pole
       (no OSM data, no ref, a ref of another scheme, the pole's side of the road is the other direction) the rules
       below decide.
    1. Same pole: a shared stop within 8 m that the shared feeds serve with the feed's mode. Equally close
       candidates are decided by direction, then by the lowest code.
    2. Logical stop: the group most of a station's attached poles belong to; otherwise a shared group with
       the same name within 300 m (of its nearest pole), or a similar name within 150 m.
    3. Pole within the logical stop: the nearest stop of that group within 60 m that is served in the
       same direction (within 90°) by the same modes (bus). Beyond 30 m the pole must also lie within 40 m along
       the line (sideways is just inexact coordinates, but a pole further up the road is the next stop: MLD 3470
       vs ZTP Czyżyny Dworzec 01, 47 m ahead) and lead to the same next logical stop as the new stop. A pole is also
       out when OSM has the stop and the pole each on an OSM pole of their own, more than 25 m apart (Czernichów Rynek).
       Stops with no such pole keep their own one: a bus stop next to tram-only stops, or a side of the
       road the shared operator doesn't serve.
    Stops whose names clearly differ and that are not on the same pole (Kraków MDA) stay separate stops.
    """
    stop_bearing = stop_bearing or {}
    stop_next = stop_next or {}
    shared_next = shared_next or {}
    shared_bearing = shared_bearing or {}
    shared_modes = shared_modes or {}
    plan = SharedPlan()

    def direction(s: NewStop, p: PublicStop) -> float | None:
        sb, pb = stop_bearing.get(s.stop_id), shared_bearing.get(p.id)
        return None if sb is None or pb is None else angle_diff(sb, pb)

    def same_mode(p: PublicStop) -> bool:
        modes = shared_modes.get(p.id)
        return not feed_modes or not modes or bool(modes & feed_modes)

    cells: dict[tuple[int, int], list[PublicStop]] = defaultdict(list)
    for p in shared:
        if p.lat is not None and p.active:
            cells[(int(p.lat * 1000), int(p.lon * 1000))].append(p)  # ~110 m x 70 m cells

    def same_pole(s: NewStop) -> list[tuple[float, PublicStop]]:
        """Shared stops of a mode the shared feeds serve within 8 m of the stop, nearest first."""
        cx, cy = int(s.lat * 1000), int(s.lon * 1000)
        out = [
            (d, p)
            for dx in (-1, 0, 1)
            for dy in (-1, 0, 1)
            for p in cells.get((cx + dx, cy + dy), [])
            if (d := distance_m(s.lat, s.lon, p.lat, p.lon)) <= SAME_POLE_M and same_mode(p)
        ]
        return sorted(out, key=lambda c: c[0])

    def pick(s: NewStop, cands: list[tuple[float, PublicStop]]) -> PublicStop:
        """Nearest candidate; equally close ones are decided by direction of travel, else the lowest code.

        Two codes on one spot (e.g. at a turn-around) are one pole, so an undecided tie is still that pole.
        """
        close = [(d, p) for d, p in cands if d - cands[0][0] < SHARED_AMBIGUOUS_M]
        if len(close) == 1:
            return close[0][1]
        same_way = [p for _, p in close if (diff := direction(s, p)) is not None and diff <= MAX_DIRECTION_DEG]
        if len(same_way) == 1:
            return same_way[0]
        return min(close, key=lambda c: (round(c[0]), c[1].code or "", c[1].id))[1]

    by_code = {p.code: p for p in shared if p.active and p.code}
    osm_cells: dict[tuple[int, int], list[OsmPole]] = defaultdict(list)
    for o in osm or []:
        if o.bus:
            osm_cells[(int(o.lat * 1000), int(o.lon * 1000))].append(o)

    def osm_near(lat: float, lon: float, radius: float) -> list[tuple[float, OsmPole]]:
        cx, cy = int(lat * 1000), int(lon * 1000)
        found = [
            (d, o)
            for dx in (-1, 0, 1)
            for dy in (-1, 0, 1)
            for o in osm_cells.get((cx + dx, cy + dy), [])
            if (d := distance_m(lat, lon, o.lat, o.lon)) <= radius
        ]
        return sorted(found, key=lambda c: c[0])

    # a bay of a bus station ('271-01-06') is a pole of its own; a shared stop is on a bay when that OSM pole is
    # within 25 m of it
    by_bay: dict[str, PublicStop] = {}
    if osm:
        for p in shared:
            if p.active and p.lat is not None:
                for _, o in osm_near(p.lat, p.lon, OSM_ON_STOP_M):
                    if o.ref and BAY_REF.fullmatch(o.ref):
                        by_bay.setdefault(o.ref, p)
                        break

    snapped: dict[str, OsmPole | None] = {}

    def osm_pole_of(key: str, lat: float, lon: float) -> OsmPole | None:
        if key not in snapped:
            near = osm_near(lat, lon, OSM_SNAP_M)
            snapped[key] = near[0][1] if near else None
        return snapped[key]

    def osm_apart(s: NewStop, p: PublicStop) -> bool:
        """OSM has the feed's stop and the shared stop each on a pole of its own, and those poles are not the same one."""
        a, b = osm_pole_of(f"s{s.stop_id}", s.lat, s.lon), osm_pole_of(p.id, p.lat, p.lon)
        return a is not None and b is not None and distance_m(a.lat, a.lon, b.lat, b.lon) > OSM_SAME_POLE_M

    def osm_says(s: NewStop, o: OsmPole) -> tuple[PublicStop | None] | None:
        """(stop,) when this OSM pole is a shared stop, (None,) when it is none of them, None: no say."""
        if not o.ref:
            return None
        if o.code:
            p = by_code.get(o.code)
        elif BAY_REF.fullmatch(o.ref):
            p = by_bay.get(o.ref)
        elif o.ref == s.code:  # the operator's own code: OSM maps this as the stop's own pole
            p = None
        else:
            return None  # a ref of some other scheme says nothing about ZTP's poles
        if p is not None and (p.lat is None or distance_m(o.lat, o.lon, p.lat, p.lon) > OSM_REF_MAX_M):
            return None  # the ref is wrong or the pole moved
        return (p,)

    def from_osm(s: NewStop) -> tuple[PublicStop | None] | None:
        """(stop,) when OSM says which shared pole the stop is, (None,) when it says none of them; None: no say."""
        poles = []
        for d, o in osm_near(s.lat, s.lon, OSM_LINK_M):
            said = osm_says(s, o)
            if said and said[0] and (diff := direction(s, said[0])) is not None and diff > MAX_DIRECTION_DEG:
                continue  # a pole on the other side of the road is not the stop's, however close
            poles.append((d, o, said))
        if not poles or poles[0][2] is None:
            return None
        d, o, said = poles[0]
        if said[0] is None and o.ref != s.code and d > OSM_OWN_M:
            return None  # a pole that far off may just be a neighbour: the stop's GPS position says too little
        return said

    # 0./1. same pole
    unresolved: list[NewStop] = []
    for s in stops:
        if (found := from_osm(s)) is not None:
            if found[0]:
                plan.links[s.stop_id] = found[0].id
                plan.methods["osm_ref"] += 1
            else:
                plan.methods["osm_own_pole"] += 1
            continue
        if cands := same_pole(s):
            plan.links[s.stop_id] = pick(s, cands).id
            plan.methods["same_pole"] += 1
            continue
        unresolved.append(s)

    by_group: dict[str, list[PublicStop]] = defaultdict(list)
    for p in shared:
        if p.active and p.lat is not None:
            by_group[p.group_id].append(p)

    # 2. logical stop per station / name
    group_of = {p.id: p.group_id for p in shared}
    members: dict[str, list[NewStop]] = defaultdict(list)
    for s in stops:
        members[s.group_key].append(s)
    for gkey, ms in members.items():
        votes = Counter(group_of[plan.links[s.stop_id]] for s in ms if s.stop_id in plan.links)
        if votes:
            plan.group_for_key[gkey] = votes.most_common(1)[0][0]
            continue
        lat = sum(m.lat for m in ms) / len(ms)
        lon = sum(m.lon for m in ms) / len(ms)
        name = Counter(m.group_name for m in ms).most_common(1)[0][0]
        best: tuple[int, float, str] | None = None
        for g in shared_groups:
            if g.lat is None or abs(g.lat - lat) > 0.012:  # ~1.3 km: cheap pre-filter (groups can be spread out)
                continue
            # distance to the group's nearest pole, not its centre (some stops spread their poles over 1 km)
            poles = by_group.get(g.id) or []
            d = min(
                (distance_m(m.lat, m.lon, p.lat, p.lon) for m in ms for p in poles), default=distance_m(lat, lon, g.lat, g.lon)
            )
            if d > GROUP_EXACT_NAME_M or not (kind := names_match(name, g.name)):
                continue
            if d <= (GROUP_EXACT_NAME_M if kind == "exact" else GROUP_SIMILAR_NAME_M):
                rank = (0 if kind == "exact" else 1, d, g.id)
                best = min(best, rank) if best else rank
        if best:
            plan.group_for_key[gkey] = best[2]

    # 3. pole within the logical stop
    key_of = {s.stop_id: s.group_key for s in stops}

    def next_groups(s: NewStop) -> set[str]:
        return {g for n in stop_next.get(s.stop_id, ()) if (g := plan.group_for_key.get(key_of.get(n, "")))}

    for s in unresolved:
        gid = plan.group_for_key.get(s.group_key)
        if gid is None:
            continue
        same_way = []
        for p in by_group.get(gid, []):
            d = distance_m(s.lat, s.lon, p.lat, p.lon)
            diff = direction(s, p)
            if d > IN_GROUP_POLE_M or not same_mode(p) or diff is None or osm_apart(s, p):
                continue
            if d > CONFIRMED_POLE_M and (
                abs(along_track_m(s.lat, s.lon, stop_bearing[s.stop_id], p.lat, p.lon)) > ALONG_TRACK_M
                or not next_groups(s) & shared_next.get(p.id, set())
            ):
                continue
            if diff <= MAX_DIRECTION_DEG:
                same_way.append((d, p))
        if same_way:
            plan.links[s.stop_id] = min(same_way, key=lambda c: c[0])[1].id
            plan.methods["in_group"] += 1
        else:
            plan.methods["own_pole_in_shared_group"] += 1
            plan.warnings.append(f"stop {s.stop_id} ({s.name}): no pole in the same direction, kept as own pole of the stop")
    return plan
