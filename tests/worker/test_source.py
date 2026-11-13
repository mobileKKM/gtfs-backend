"""Reading a GTFS zip row by row (static/source.py), as the import read it with gtfs-kit and pandas before."""

import pytest
from conftest import make_zip

from gtfs_backend.worker.static.source import GtfsSource, SourceError

AGENCY = "agency_id,agency_name,agency_url,agency_timezone\nA,ZTP,http://ztp.test,Europe/Warsaw\n"


def stops(source: GtfsSource) -> list[dict]:
    return source.records("stops")


def test_values_are_the_published_text_and_na_tokens_are_null(tmp_path):
    src = GtfsSource(
        make_zip(
            tmp_path / "a.zip",
            {
                "agency.txt": AGENCY,
                "stops.txt": "stop_id,stop_code,stop_name,stop_lat,stop_lon\n"
                'S1,01, padded ,50.10,19.9\nS2,NA,"a, ""quoted""\nname",50.0,19.9\nS3, ,None,null,\n',
            },
        )
    )
    s1, s2, s3 = stops(src)
    assert (s1["stop_code"], s1["stop_name"], s1["stop_lat"]) == ("01", " padded ", "50.10")  # as published
    assert (s2["stop_code"], s2["stop_name"]) == (None, 'a, "quoted"\nname')  # NA is NULL (inherited)
    assert (s3["stop_code"], s3["stop_name"], s3["stop_lat"], s3["stop_lon"]) == (None, None, None, None)
    assert s1["stop_desc"] is None  # a spec column the file doesn't have
    assert src.missing_columns["stops"][0] == "tts_stop_name"


def test_headers_bom_blank_lines_and_short_rows(tmp_path):
    src = GtfsSource(
        make_zip(
            tmp_path / "a.zip",
            {
                "agency.txt": "﻿ agency_id ,agency_name,agency_url,agency_timezone,extra,\r\n"
                "A,ZTP,http://ztp.test,Europe/Warsaw,x,\r\n\r\nB,KM\r\n",
            },
        )
    )
    a, b = src.records("agency")
    assert a["agency_id"] == "A" and a["agency_timezone"] == "Europe/Warsaw"
    assert (b["agency_name"], b["agency_url"]) == ("KM", None)  # a short row: NULL for the rest
    assert src.unknown_columns == {"agency": ["extra", "Unnamed: 5"]}  # pandas' name for an empty header


def test_which_files_count(tmp_path):
    src = GtfsSource(
        make_zip(
            tmp_path / "a.zip",
            {
                "agency.txt": AGENCY,
                "feed/stops.txt": "stop_id,stop_name\nS1,One\n",  # in a folder: still found
                "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n",  # only a header: absent
                "blocks.txt": "block_id\n",  # an extension file with only a header: present, empty
                "notes.txt": "hello\n",
            },
        )
    )
    assert src.tables == ["agency", "stops", "blocks"]
    assert list(src.rows("blocks")) == []
    assert src.unknown_files == ["notes.txt"]


def test_a_row_with_more_fields_than_the_header_is_an_error(tmp_path):
    src = GtfsSource(make_zip(tmp_path / "a.zip", {"agency.txt": AGENCY, "stops.txt": "stop_id,stop_name\nS1,One,extra\n"}))
    with pytest.raises(SourceError, match="stops.txt: expected 2 fields in line 2, saw 3"):
        list(src.rows("stops"))


def test_a_file_without_a_header_is_an_error(tmp_path):
    with pytest.raises(SourceError, match="no columns"):
        GtfsSource(make_zip(tmp_path / "a.zip", {"agency.txt": AGENCY, "stops.txt": "\n\n"}))
