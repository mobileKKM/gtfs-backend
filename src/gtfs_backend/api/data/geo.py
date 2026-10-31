"""Bounding boxes and points as the API takes them (`minLon,minLat,maxLon,maxLat`, `lat,lon`)."""

from __future__ import annotations

import math
from dataclasses import dataclass

from gtfs_backend.api.errors import InvalidInput

BBOX_FORMAT = "minLon,minLat,maxLon,maxLat"
POINT_FORMAT = "lat,lon"


def _floats(text: str, n: int, fmt: str) -> list[float]:
    try:
        values = [float(x) for x in text.split(",")]
    except ValueError:
        values = []
    if len(values) != n or not all(math.isfinite(v) for v in values):
        raise InvalidInput(f"expected {fmt}")
    return values


@dataclass(frozen=True)
class Bbox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @classmethod
    def parse(cls, text: str) -> Bbox:
        box = cls(*_floats(text, 4, BBOX_FORMAT))
        if not (-180 <= box.min_lon < box.max_lon <= 180 and -90 <= box.min_lat < box.max_lat <= 90):
            raise InvalidInput(f"bbox must be {BBOX_FORMAT} with min < max")
        return box

    @property
    def center(self) -> Point:
        return Point((self.min_lat + self.max_lat) / 2, (self.min_lon + self.max_lon) / 2)

    def size_km(self) -> tuple[float, float]:
        """(width, height), approximately."""
        mid = math.radians(self.center.lat)
        return (self.max_lon - self.min_lon) * 111.32 * math.cos(mid), (self.max_lat - self.min_lat) * 110.57

    def contains(self, lat: float, lon: float) -> bool:
        return self.min_lon <= lon <= self.max_lon and self.min_lat <= lat <= self.max_lat


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float

    @classmethod
    def parse(cls, text: str) -> Point:
        lat, lon = _floats(text, 2, POINT_FORMAT)
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise InvalidInput(f"expected {POINT_FORMAT}")
        return cls(lat, lon)


def encode_polyline(points: list[tuple[float, float]]) -> str:
    """Google encoded polyline (precision 5) of (lat, lon) points."""
    out, prev = [], (0, 0)
    for lat, lon in points:
        cur = (round(lat * 1e5), round(lon * 1e5))
        for delta in (cur[0] - prev[0], cur[1] - prev[1]):
            v = ~(delta << 1) if delta < 0 else delta << 1
            while v >= 0x20:
                out.append(chr((0x20 | (v & 0x1F)) + 63))
                v >>= 5
            out.append(chr(v + 63))
        prev = cur
    return "".join(out)


def bbox_of(points: list[tuple[float, float]]) -> list[float]:
    """[minLon, minLat, maxLon, maxLat] of (lat, lon) points."""
    lats, lons = [p[0] for p in points], [p[1] for p in points]
    return [min(lons), min(lats), max(lons), max(lats)]
