"""Distances and travel directions."""

from __future__ import annotations

import math


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def angle_diff(a: float, b: float) -> float:
    d = abs(a - b) % 360
    return 360 - d if d > 180 else d


def along_track_m(lat1: float, lon1: float, bearing: float, lat2: float, lon2: float) -> float:
    """How far point 2 lies ahead (+) or behind (-) point 1 in the direction of travel `bearing` (degrees from north)."""
    dy = math.radians(lat2 - lat1) * 6371008.8
    dx = math.radians(lon2 - lon1) * 6371008.8 * math.cos(math.radians((lat1 + lat2) / 2))
    b = math.radians(bearing)
    return dx * math.sin(b) + dy * math.cos(b)
