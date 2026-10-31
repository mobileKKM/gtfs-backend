"""GTFS service-day time arithmetic.

GTFS times count from "noon minus 12h" of the service date (local time). That is local midnight except
on the two days the clocks change, when it is one hour off. Some realtime systems count from local
midnight anyway (ZTP's trams), so the day start is set per feed. Scheduled epoch = day base + seconds.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

DayStart = Literal["noon_minus_12h", "midnight"]


def service_day_base(d: date, tz: ZoneInfo) -> int:
    return int(datetime.combine(d, time(12), tz).timestamp()) - 12 * 3600


def midnight_base(d: date, tz: ZoneInfo) -> int:
    return int(datetime.combine(d, time(0), tz).timestamp())


def day_base(d: date, tz: ZoneInfo, day_start: DayStart = "noon_minus_12h") -> int:
    """Epoch that a feed's GTFS times on service date `d` count from."""
    return midnight_base(d, tz) if day_start == "midnight" else service_day_base(d, tz)


def local_date(epoch: float, tz: ZoneInfo) -> date:
    return datetime.fromtimestamp(epoch, tz).date()


def nearby_dates(epoch: float, tz: ZoneInfo) -> list[date]:
    """Yesterday, today and tomorrow (local): the service days a running trip can belong to."""
    today = local_date(epoch, tz)
    return [today - timedelta(days=1), today, today + timedelta(days=1)]
