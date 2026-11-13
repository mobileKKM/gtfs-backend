"""Redis layout for live data: the worker writes it, the API reads it. Timetables stay in Postgres.

Every key carries the feed_id, because trip and vehicle ids are only unique within a feed (see keys.py).

rt:veh:map:{feed_id}                the feed's vehicles as the map shows them: a JSON list of their map fields (the
                                    vehicle JSON's, same names), replaced on every vehicle poll (TTL VEHICLE_TTL).
                                    The map reads these few keys and filters the box itself: a GEOSEARCH plus an MGET
                                    of every full vehicle JSON cost twice as much
rt:veh:{vehicle_key}                vehicle JSON (TTL VEHICLE_TTL)
rt:trip:{trip_key}                  trip update JSON with per-stop predictions, incl. stops the feed has
                                    already dropped (their last known times); the JSON has the version.
                                    Removed when the trip leaves the feed (TTL TRIP_TTL as a fallback)
rt:alerts:{feed_id}                 alerts JSON (TTL ALERT_TTL)
rt:unmatched:{feed_id}:{kind}       unmatched trip ids / vehicles for debugging (TTL UNMATCHED_TTL), never shown
rt:meta:{feed_id}:{kind}            poll stats: last poll, last data change, timings, counters
pub/sub rt:updated                  {"feed_id", "kind", "ts"} after each write
pub/sub static:updated              feed_id, after a static import became ready
"""

from __future__ import annotations

REALTIME_UPDATED = "rt:updated"
STATIC_UPDATED = "static:updated"


def vehicles_map(feed_id: str) -> str:
    return f"rt:veh:map:{feed_id}"


def vehicle(vehicle_key: str) -> str:
    return f"rt:veh:{vehicle_key}"


def trip(trip_key: str) -> str:
    return f"rt:trip:{trip_key}"


def alerts(feed_id: str) -> str:
    return f"rt:alerts:{feed_id}"


def unmatched(feed_id: str, kind: str) -> str:
    return f"rt:unmatched:{feed_id}:{kind}"


def meta(feed_id: str, kind: str) -> str:
    return f"rt:meta:{feed_id}:{kind}"
