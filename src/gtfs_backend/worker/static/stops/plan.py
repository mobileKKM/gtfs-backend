"""Matching a feed's stops within its own stop-code namespace (pure; apply.py does the DB work)."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from gtfs_backend.ids import normalize_name
from gtfs_backend.worker.static.stops.geometry import distance_m
from gtfs_backend.worker.static.stops.model import Group, NewStop, Platform, platform_number

CODE_MATCH_M = 300.0  # same code: same platform (observed cross-feed spread for one code: up to 131 m)
NAME_MATCH_M = 50.0  # same name + platform number: same platform whose code changed
LOCATION_MATCH_M = 15.0  # same spot, name and code both changed
GROUP_NAME_MATCH_M = 300.0


@dataclass
class MatchPlan:
    # platform_key -> existing platform id, or None for a new platform
    platform_for_key: dict[str, str | None] = field(default_factory=dict)
    method_for_key: dict[str, str] = field(default_factory=dict)
    # group_key -> existing group id, or None for a new group
    group_for_key: dict[str, str | None] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _near(p: Platform, s: NewStop, limit: float) -> bool:
    return p.lat is not None and p.lon is not None and distance_m(p.lat, p.lon, s.lat, s.lon) <= limit


def plan_matches(stops: list[NewStop], platforms: list[Platform], groups: list[Group]) -> MatchPlan:
    plan = MatchPlan()
    # one representative stop per platform key (stops sharing a code are the same platform)
    reps: dict[str, NewStop] = {}
    for s in sorted(stops, key=lambda x: x.stop_id):
        reps.setdefault(s.platform_key, s)
    batch_codes = {s.code for s in reps.values() if s.code}

    by_code: dict[str, list[Platform]] = defaultdict(list)
    by_name: dict[tuple[str, str | None], list[Platform]] = defaultdict(list)
    for p in platforms:
        if p.code:
            by_code[p.code].append(p)
        by_name[(normalize_name(p.name), (p.platform or "").strip() or None)].append(p)

    claimed: set[str] = set()
    unresolved: list[tuple[str, NewStop]] = []

    # a. same code (nearby)
    for key, s in reps.items():
        if s.code:
            cands = [p for p in by_code.get(s.code, []) if _near(p, s, CODE_MATCH_M)]
            if cands:
                best = sorted(cands, key=lambda p: (not p.active, distance_m(p.lat, p.lon, s.lat, s.lon)))[0]
                plan.platform_for_key[key] = best.id
                plan.method_for_key[key] = "code"
                claimed.add(best.id)
                continue
            if by_code.get(s.code):
                plan.warnings.append(f"stop {s.stop_id}: code {s.code} exists but more than {CODE_MATCH_M:.0f} m away")
        unresolved.append((key, s))

    def free(p: Platform) -> bool:
        return p.id not in claimed and (p.code is None or p.code not in batch_codes)

    # b. same name + platform number (nearby), c. same spot. Tentative until claims are unique.
    tentative: dict[str, tuple[str, str]] = {}
    still: list[tuple[str, NewStop]] = []
    for key, s in unresolved:
        cands = [
            p for p in by_name.get((normalize_name(s.name), platform_number(s)), []) if free(p) and _near(p, s, NAME_MATCH_M)
        ]
        if len(cands) == 1:
            tentative[key] = (cands[0].id, "name")
        elif len(cands) > 1:
            plan.errors.append(f"stop {s.stop_id} ({s.name}): {len(cands)} platforms match by name, creating a new one")
            plan.platform_for_key[key], plan.method_for_key[key] = None, "ambiguous"
        else:
            still.append((key, s))
    for key, s in still:
        cands = [p for p in platforms if free(p) and not p.linked_elsewhere and _near(p, s, LOCATION_MATCH_M)]
        if len(cands) == 1:
            tentative[key] = (cands[0].id, "location")
        elif len(cands) > 1:
            plan.errors.append(f"stop {s.stop_id} ({s.name}): {len(cands)} platforms on the same spot, creating a new one")
            plan.platform_for_key[key], plan.method_for_key[key] = None, "ambiguous"
        else:
            plan.platform_for_key[key], plan.method_for_key[key] = None, "new"

    claims = Counter(pid for pid, _ in tentative.values())
    for key, (pid, method) in tentative.items():
        s = reps[key]
        if claims[pid] > 1:
            plan.errors.append(f"stop {s.stop_id} ({s.name}): platform {pid} claimed by {claims[pid]} stops, creating a new one")
            plan.platform_for_key[key], plan.method_for_key[key] = None, "ambiguous"
            continue
        plan.platform_for_key[key], plan.method_for_key[key] = pid, method
        if method == "location":
            plan.warnings.append(f"stop {s.stop_id} ({s.name}, {s.code}): matched platform {pid} by location only")

    # groups follow platforms
    platform_group = {p.id: p.group_id for p in platforms}
    groups_by_code: dict[str, list[Group]] = defaultdict(list)
    for g in groups:
        if g.code:
            groups_by_code[g.code].append(g)
    batch: dict[str, list[str]] = defaultdict(list)  # group_key -> platform keys
    for key, s in reps.items():
        batch[s.group_key].append(key)

    used: set[str] = set()
    for gkey in sorted(batch, key=lambda k: (-len(batch[k]), k)):
        keys = batch[gkey]
        votes = Counter(
            platform_group[pid] for k in keys if (pid := plan.platform_for_key.get(k)) and platform_group.get(pid) not in used
        )
        chosen: str | None = None
        if votes:
            top = max(votes.values())
            chosen = sorted(g for g, n in votes.items() if n == top)[0]
        if chosen is None and not gkey.startswith("name:"):  # ZTP prefix or feed-scoped station key
            same_code = [g for g in groups_by_code.get(gkey, []) if g.id not in used]
            if len(same_code) == 1:
                chosen = same_code[0].id
        if chosen is None:
            first = reps[keys[0]]
            lat = sum(reps[k].lat for k in keys) / len(keys)
            lon = sum(reps[k].lon for k in keys) / len(keys)
            near = [
                g
                for g in groups
                if g.id not in used
                and normalize_name(g.name) == normalize_name(first.group_name)
                and g.lat is not None
                and distance_m(g.lat, g.lon, lat, lon) <= GROUP_NAME_MATCH_M
            ]
            if len(near) == 1:
                chosen = near[0].id
        plan.group_for_key[gkey] = chosen
        if chosen:
            used.add(chosen)
    return plan
