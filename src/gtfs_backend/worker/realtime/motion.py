"""Fill in bearing and speed when a feed leaves them out, from the vehicle's previous position.

ZTP's trams never send speed, MPK buses send speed 0 for most vehicles (driving ones too), and buses drop
bearing now and then (mostly while standing) or send bearing 0. normalize treats those zeros as missing.
Values the feed sends are kept; we only fill gaps:
- speed (m/s, as in GTFS-RT) = distance / time between two fixes of the same vehicle.
- bearing (degrees clockwise from north) = direction from the previous fix to this one. Under
  MIN_MOVE_M the move is GPS noise: the last known bearing is kept and speed is 0.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

EARTH_RADIUS_M = 6_371_000
MIN_MOVE_M = 10  # below this a move between fixes is GPS jitter
MAX_GAP_S = 6 * 60  # older fixes say too little about the current motion; km_kolej updates every 5 min
MAX_SPEED_MS = 50  # 180 km/h; faster means a GPS jump, not motion (KM trains run up to 160 km/h)
FORGET_S = 600


@dataclass(frozen=True)
class Fix:
    lat: float
    lon: float
    timestamp: int
    bearing: float | None
    speed: float | None


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return round(math.degrees(math.atan2(x, y)) % 360, 1)


def estimate(prev: Fix | None, lat: float, lon: float, timestamp: int) -> tuple[float | None, float | None]:
    """(bearing, speed) from the previous fix; None where it cannot be told."""
    if prev is None:
        return None, None
    if timestamp == prev.timestamp:  # the feed repeated the same fix
        return prev.bearing, prev.speed
    dt = timestamp - prev.timestamp
    if dt < 0 or dt > MAX_GAP_S:
        return prev.bearing if dt > 0 else None, None
    d = distance_m(prev.lat, prev.lon, lat, lon)
    if d < MIN_MOVE_M:
        return prev.bearing, 0.0
    speed = d / dt
    if speed > MAX_SPEED_MS:
        return None, None
    return bearing_deg(prev.lat, prev.lon, lat, lon), round(speed, 1)


class MotionTracker:
    """Last fix per vehicle of one feed."""

    def __init__(self) -> None:
        self._fixes: dict[str, Fix] = {}

    def fill(
        self, vehicle_id: str, lat: float, lon: float, timestamp: int, bearing: float | None, speed: float | None
    ) -> tuple[float | None, float | None]:
        """Feed values where present, estimates otherwise; remembers the result for the next fix."""
        if bearing is None or speed is None:
            est_bearing, est_speed = estimate(self._fixes.get(vehicle_id), lat, lon, timestamp)
            bearing = est_bearing if bearing is None else bearing
            speed = est_speed if speed is None else speed
        self._fixes[vehicle_id] = Fix(lat, lon, timestamp, bearing, speed)
        return bearing, speed

    def forget_before(self, cutoff: int) -> None:
        for k in [k for k, f in self._fixes.items() if f.timestamp < cutoff]:
            del self._fixes[k]
