"""Keys for the things the API links to. The worker uses the same keys in Redis, the API in its paths.

vehicle   {feed_id}:{vehicle_id}
trip      {feed_id}:{YYYYMMDD}:{trip_id}
route     {feed_id}:{route_id}
alert     {feed_id}:{alert_id}

The feed_id never contains `:` (see FeedConfig.feed_id) and the source id comes last, kept whole, so source ids
containing `:` or `/` still work. A trip key stays the same when a new timetable version takes over its service
day; the trip JSON's `version` says which one it uses.
"""

from __future__ import annotations

import re
from datetime import date, datetime

FEED_ID_RE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


class InvalidKey(ValueError):
    pass


def service_date_str(d: date) -> str:
    return f"{d:%Y%m%d}"


def parse_service_date(text: str) -> date:
    """`YYYYMMDD`."""
    try:
        if len(text) != 8 or not text.isdigit():
            raise ValueError
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        raise InvalidKey("date must be YYYYMMDD") from None


def version_id(feed_id: str, version: int) -> str:
    """core.feed_version.id of a per-feed version number (`kmk_t_v3`)."""
    return f"{feed_id}_v{version}"


def _join(feed_id: str, *parts: str) -> str:
    return ":".join((feed_id, *parts))


def vehicle_key(feed_id: str, vehicle_id: str) -> str:
    return _join(feed_id, vehicle_id)


def trip_key(feed_id: str, service_date: date, trip_id: str) -> str:
    return _join(feed_id, service_date_str(service_date), trip_id)


def route_key(feed_id: str, route_id: str) -> str:
    return _join(feed_id, route_id)


def alert_key(feed_id: str, alert_id: str) -> str:
    return _join(feed_id, alert_id)


def split_feed(key: str, what: str = "key") -> tuple[str, str]:
    """(feed_id, rest) of `{feed_id}:{rest}`."""
    feed_id, sep, rest = key.partition(":")
    if not sep or not rest or not FEED_ID_RE.match(feed_id):
        raise InvalidKey(f"{what} must be {{feed_id}}:{{id}}")
    return feed_id, rest


def parse_trip_key(key: str) -> tuple[str, date, str]:
    """(feed_id, service date, trip_id) of `{feed_id}:{YYYYMMDD}:{trip_id}`."""
    feed_id, rest = split_feed(key, "trip key")
    day, sep, trip_id = rest.partition(":")
    if not sep or not trip_id:
        raise InvalidKey("trip key must be {feed_id}:{YYYYMMDD}:{trip_id}")
    return feed_id, parse_service_date(day), trip_id
