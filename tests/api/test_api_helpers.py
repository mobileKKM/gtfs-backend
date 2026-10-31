"""Small service helpers that need no database."""

from datetime import date

import pytest

from gtfs_backend.api.data.geo import Bbox, Point, bbox_of, encode_polyline
from gtfs_backend.api.data.sql import union_per_version
from gtfs_backend.api.deps import parse_date, split_csv
from gtfs_backend.api.errors import InvalidInput


def test_bbox():
    box = Bbox.parse("19.9,50.0,20.0,50.1")
    assert box.contains(50.05, 19.95) and not box.contains(50.2, 19.95)
    width, height = box.size_km()
    assert 7 < width < 7.3 and 11 < height < 11.2


@pytest.mark.parametrize("text", ["1,2,3", "a,b,c,d", "20,50,19,51", "19,50,20,91", "nan,1,2,3"])
def test_bad_bbox(text):
    with pytest.raises(InvalidInput):
        Bbox.parse(text)


def test_point():
    assert Point.parse("50.06,19.94") == Point(50.06, 19.94)
    with pytest.raises(InvalidInput):
        Point.parse("19.94")


def test_dates_and_lists():
    assert parse_date("20261025") == date(2026, 10, 25)
    for bad in ("2026-10-25", "25.10.2026", "20261325", "2026102"):
        with pytest.raises(InvalidInput):
            parse_date(bad)
    assert split_csv(["tram,bus", " rail ", ""]) == {"tram", "bus", "rail"}


def test_union_per_version_numbers_parameters():
    sql, params = union_per_version(
        "SELECT '{}' FROM t WHERE v = %(fv{i})s AND x = ANY(%(xs)s)", [{"fv": "a_v1"}, {"fv": "b_v2"}], {"xs": [1]}
    )
    block = "SELECT '{}' FROM t WHERE v = %(fv{i})s AND x = ANY(%(xs)s)"
    assert sql == block.replace("{i}", "0") + " UNION ALL " + block.replace("{i}", "1")
    assert params == {"xs": [1], "fv0": "a_v1", "fv1": "b_v2"}


def test_encode_polyline():
    # the example from Google's polyline documentation
    points = [(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)]
    assert encode_polyline(points) == "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    assert bbox_of(points) == [-126.453, 38.5, -120.2, 43.252]
