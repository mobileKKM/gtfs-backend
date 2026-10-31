from datetime import date, datetime
from zoneinfo import ZoneInfo

from gtfs_backend.servicetime import day_base, service_day_base

WAW = ZoneInfo("Europe/Warsaw")


def local(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, WAW).strftime("%Y-%m-%d %H:%M %Z")


def test_normal_day_base_is_midnight():
    assert local(service_day_base(date(2026, 10, 26), WAW)) == "2026-10-26 00:00 CET"
    assert day_base(date(2026, 10, 26), WAW, "midnight") == day_base(date(2026, 10, 26), WAW)


def test_clocks_go_back():
    d = date(2026, 10, 25)
    # 17:43 in the timetable is 17:43 wall clock (CET) on this day
    assert local(service_day_base(d, WAW) + 17 * 3600 + 43 * 60) == "2026-10-25 17:43 CET"
    # counted from midnight (ZTP trams) the same 17:43 lands one hour earlier
    assert day_base(d, WAW, "midnight") == day_base(d, WAW) - 3600
    assert local(day_base(d, WAW, "midnight") + 17 * 3600 + 43 * 60) == "2026-10-25 16:43 CET"


def test_clocks_go_forward():
    d = date(2027, 3, 28)
    assert local(service_day_base(d, WAW) + 8 * 3600) == "2027-03-28 08:00 CEST"
    assert day_base(d, WAW, "midnight") == day_base(d, WAW) + 3600
    assert local(day_base(d, WAW, "midnight") + 8 * 3600) == "2027-03-28 09:00 CEST"


def test_after_midnight_times_belong_to_the_previous_service_day():
    # 25:10 on service day 2026-10-22 is 01:10 on the 23rd
    assert local(service_day_base(date(2026, 10, 22), WAW) + 25 * 3600 + 600) == "2026-10-23 01:10 CEST"
