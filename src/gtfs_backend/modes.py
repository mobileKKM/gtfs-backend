"""Transport mode of a GTFS route_type (basic and extended types), in Python and as SQL over a `route_type` column."""

from enum import StrEnum


class Mode(StrEnum):
    TRAM = "tram"
    BUS = "bus"
    TROLLEYBUS = "trolleybus"
    METRO = "metro"
    RAIL = "rail"
    FERRY = "ferry"
    CABLE_TRAM = "cable_tram"
    AERIAL_LIFT = "aerial_lift"
    FUNICULAR = "funicular"
    MONORAIL = "monorail"
    OTHER = "other"  # any route type not listed here


# mode -> its basic route types and extended (first, last) ranges
MODES: dict[Mode, tuple[tuple[int, ...], tuple[tuple[int, int], ...]]] = {
    Mode.TRAM: ((0,), ((900, 906),)),
    Mode.BUS: ((3,), ((700, 716), (200, 209))),
    Mode.TROLLEYBUS: ((11, 800), ()),
    Mode.METRO: ((1,), ((400, 405),)),
    Mode.RAIL: ((2,), ((100, 117),)),
    Mode.FERRY: ((4, 1000, 1200), ()),
    Mode.CABLE_TRAM: ((5,), ()),
    Mode.AERIAL_LIFT: ((6,), ((1300, 1307),)),
    Mode.FUNICULAR: ((7, 1400), ()),
    Mode.MONORAIL: ((12,), ()),
}


def mode_of(route_type: int) -> Mode:
    for mode, (types, ranges) in MODES.items():
        if route_type in types or any(lo <= route_type <= hi for lo, hi in ranges):
            return mode
    return Mode.OTHER


def _when(types: tuple[int, ...], ranges: tuple[tuple[int, int], ...]) -> str:
    return " OR ".join([f"route_type = {t}" for t in types] + [f"route_type BETWEEN {lo} AND {hi}" for lo, hi in ranges])


_WHENS = "".join(f"    WHEN {_when(*spec)} THEN '{mode.value}'\n" for mode, spec in MODES.items())
MODE_SQL = f"\nCASE\n{_WHENS}    ELSE '{Mode.OTHER.value}'\nEND\n"
