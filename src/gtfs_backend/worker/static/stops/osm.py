"""OpenStreetMap stop poles: a second opinion on which pole an operator's stop is (read from a local file).

OSM mappers tag poles with the ZTP code as `ref` ('802-02') and say what stops there (`bus=yes`, `tram=yes`), so a stop
that sits next to a pole can be told apart from the tram pole beside it, or from a pole ZTP doesn't publish at all
(Rondo Mogilskie 07, where MLD buses stop). `fetch-osm` reads a Geofabrik extract into a small file of poles; the
matcher only reads that file.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import httpx

from gtfs_backend.logs import log

EXTRACT_URL = "https://download.geofabrik.de/europe/poland/malopolskie-latest.osm.pbf"  # ~150 MB, updated daily
USER_AGENT = "mobileKKM-gtfs-backend/0.1"


@dataclass(frozen=True)
class OsmPole:
    osm_id: str  # 'node/123'
    lat: float
    lon: float
    kind: str  # 'platform' | 'stop_position'
    ref: str | None
    name: str | None
    bus: bool
    tram: bool

    @property
    def code(self) -> str | None:
        """The ZTP-style code in `ref` ('802-2' -> '802-02'); None for other schemes ('214-1t')."""
        return normalize_ref(self.ref)


def normalize_ref(ref: str | None) -> str | None:
    m = re.fullmatch(r"\s*(\d+)-(\d{1,2})\s*", ref or "")
    return f"{m[1]}-{int(m[2]):02d}" if m else None


def make_pole(osm_id: str, tags, lat: float, lon: float) -> OsmPole | None:
    """A pole from an OSM object's tags; None when it isn't a stop pole or doesn't say what stops there."""
    kind = tags.get("public_transport")
    if kind not in ("platform", "stop_position"):
        if tags.get("highway") != "bus_stop":
            return None
        kind = "platform"
    bus = tags.get("bus") == "yes" or tags.get("highway") == "bus_stop"
    tram = tags.get("tram") == "yes" or tags.get("railway") == "tram_stop"
    if not (bus or tram):
        return None
    return OsmPole(osm_id, lat, lon, kind, (tags.get("ref") or "").strip() or None, tags.get("name"), bus, tram)


def load_osm_poles(path: Path) -> list[OsmPole]:
    """The poles of a `fetch-osm` file; none when there is no file (the matcher then goes by position alone)."""
    try:
        return [OsmPole(**p) for p in json.loads(path.read_text())["poles"]]
    except FileNotFoundError:
        return []


def read_extract(pbf: Path) -> list[OsmPole]:
    """The stop poles in an OSM extract: tagged nodes, and platforms drawn as ways, placed at their middle node.

    Two passes instead of one with osmium's location index: that keeps every node's location of the region in memory
    (Malopolska: ~600 MB) to place a few thousand ways. The first pass reads only the ways and keeps the platforms'
    node ids, the second only the nodes. osmium is imported only here: the worker process never loads it (the extract
    is read in the import process)."""
    import osmium

    class Ways(osmium.SimpleHandler):
        def __init__(self) -> None:
            super().__init__()
            self.platforms: list[tuple[OsmPole, list[int]]] = []  # pole without its position yet, node ids

        def way(self, w) -> None:
            if pole := make_pole(f"way/{w.id}", w.tags, 0.0, 0.0):  # whether it is one only depends on its tags
                self.platforms.append((pole, [n.ref for n in w.nodes]))

    class Nodes(osmium.SimpleHandler):
        def __init__(self, wanted: set[int]) -> None:
            super().__init__()
            self.wanted = wanted
            self.poles: list[OsmPole] = []
            self.locations: dict[int, tuple[float, float]] = {}

        def node(self, n) -> None:
            if not n.location.valid():
                return
            if n.id in self.wanted:
                self.locations[n.id] = (n.location.lat, n.location.lon)
            if p := make_pole(f"node/{n.id}", n.tags, n.location.lat, n.location.lon):
                self.poles.append(p)

    ways = Ways()
    ways.apply_file(str(pbf))
    nodes = Nodes({ref for _, refs in ways.platforms for ref in refs})
    nodes.apply_file(str(pbf))
    poles = nodes.poles
    for pole, refs in ways.platforms:
        located = [nodes.locations[ref] for ref in refs if ref in nodes.locations]
        if located:  # its middle located node stands for it
            lat, lon = located[len(located) // 2]
            poles.append(replace(pole, lat=lat, lon=lon))
    return poles


def fetch_osm_poles(path: Path, *, url: str = EXTRACT_URL) -> int:
    """Download the extract, write the poles in it to `path` (replaced in one step: readers never see half a file).
    Returns how many poles there are."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pbf, tmp = path.with_name(path.name + ".osm.pbf"), path.with_name(path.name + ".tmp")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300, headers={"User-Agent": USER_AGENT}) as resp:
            resp.raise_for_status()
            with pbf.open("wb") as f:
                for chunk in resp.iter_bytes(1 << 20):
                    f.write(chunk)
        poles = read_extract(pbf)
        tmp.write_text(json.dumps({"poles": [asdict(p) for p in poles]}, ensure_ascii=False))
        tmp.replace(path)
    finally:
        pbf.unlink(missing_ok=True)
        tmp.unlink(missing_ok=True)
    log.info("osm_extract", url=url, poles=len(poles))
    return len(poles)


def file_age_s(path: Path, now: float | None = None) -> float | None:
    """Seconds since `path` was written; None when it doesn't exist."""
    try:
        return (time.time() if now is None else now) - path.stat().st_mtime
    except FileNotFoundError:
        return None
