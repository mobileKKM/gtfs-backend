import pytest

from gtfs_backend.ids import ID_RE, new_public_id, parse_url_key, slugify, stop_slug, url_key


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        ("Dworzec Główny Zachód", "dworzec-glowny-zachod"),
        ("AKF / PK", "akf-pk"),
        ("Łagiewniki ZUS", "lagiewniki-zus"),
        ("Plac Centralny im. R.Reagana", "plac-centralny-im-r-reagana"),
        ("KRAKÓW GŁÓWNY", "krakow-glowny"),
        ("Żabiniec", "zabiniec"),
        ("  --  ", "stop"),
    ],
)
def test_slugify(name, slug):
    assert slugify(name) == slug


def test_stop_slug_appends_platform_code_without_separator():
    assert stop_slug("Dworzec Główny Zachód", "09") == "dworzec-glowny-zachod09"
    assert stop_slug("Rondo Mogilskie", "01") == "rondo-mogilskie01"
    assert stop_slug("KRAKÓW GŁÓWNY", None) == "krakow-glowny"


def test_url_key_roundtrip_and_outdated_slug():
    pid = new_public_id()
    assert ID_RE.match(pid)
    assert parse_url_key(url_key("dworzec-glowny-zachod09", pid)) == pid
    # after a rename the old slug still resolves: only the id counts
    assert parse_url_key(f"some-old-name01-{pid}") == pid
    assert parse_url_key(pid) == pid
    assert parse_url_key("dworzec-glowny-ABCDE") is None
    assert parse_url_key("dworzec-glowny-abcd") is None
