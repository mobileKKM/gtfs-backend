"""Stop matching decisions (pure functions, no DB)."""

from gtfs_backend.worker.static.stops import (
    Group,
    NewStop,
    Platform,
    names_match,
    plan_matches,
    plan_shared,
)
from gtfs_backend.worker.static.stops.osm import OsmPole, normalize_ref

LAT, LON = 50.0658, 19.9597
M = 1 / 111_320  # ~1 m in degrees latitude


def stop(stop_id, code, name="Rondo Mogilskie", dlat_m=0.0, platform=None, **kw):
    return NewStop(stop_id, code, name, platform, LAT + dlat_m * M, LON, **kw)


def plat(pid, code, name="Rondo Mogilskie", dlat_m=0.0, group="grp01", number=None, active=True, linked_elsewhere=False):
    return Platform(
        pid, group, code, name, number or (code.split("-")[1] if code else None), LAT + dlat_m * M, LON, active, linked_elsewhere
    )


GROUP = Group("grp01", "802", "Rondo Mogilskie", LAT, LON)


def test_same_code_keeps_id_and_rename_is_in_place():
    plan = plan_matches([stop("1", "802-01", name="Rondo Mogilskie Nowe")], [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:802-01"] == "aaaaa"
    assert plan.method_for_key["code:802-01"] == "code"
    assert plan.group_for_key["802"] == "grp01"


def test_renumbered_code_matches_by_name_and_platform_number():
    plan = plan_matches([stop("1", "803-01", dlat_m=10)], [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:803-01"] == "aaaaa"
    assert plan.method_for_key["code:803-01"] == "name"
    assert plan.group_for_key["803"] == "grp01"  # group follows its platforms


def test_name_and_code_changed_same_spot_matches_by_location_with_warning():
    plan = plan_matches([stop("1", "990-01", name="Opera Krakowska", dlat_m=5)], [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:990-01"] == "aaaaa"
    assert plan.method_for_key["code:990-01"] == "location"
    assert any("location only" in w for w in plan.warnings)


def test_new_stop_gets_new_id():
    plan = plan_matches([stop("1", "555-01", name="Nowy Przystanek", dlat_m=500)], [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:555-01"] is None
    assert plan.method_for_key["code:555-01"] == "new"
    assert plan.group_for_key["555"] is None


def test_platform_whose_code_is_still_in_use_is_not_taken_by_name():
    # 802-01 still exists in the new data, so a new 802-09 next to it must not steal it
    stops = [stop("1", "802-01"), stop("2", "802-09", platform="01", dlat_m=3)]
    plan = plan_matches(stops, [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:802-01"] == "aaaaa"
    assert plan.platform_for_key["code:802-09"] is None


def test_ambiguous_match_creates_new_id_instead_of_guessing():
    platforms = [plat("aaaaa", "802-01", dlat_m=5), plat("bbbbb", "802-11", number="01", dlat_m=-5)]
    plan = plan_matches([stop("1", "803-01")], platforms, [GROUP])
    assert plan.platform_for_key["code:803-01"] is None
    assert plan.method_for_key["code:803-01"] == "ambiguous"
    assert plan.errors


def test_two_new_stops_claiming_one_platform_both_get_new_ids():
    stops = [stop("1", "990-01", name="A", dlat_m=2), stop("2", "991-01", name="B", dlat_m=-2)]
    plan = plan_matches(stops, [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:990-01"] is None
    assert plan.platform_for_key["code:991-01"] is None
    assert len(plan.errors) == 2


def test_code_far_away_is_not_the_same_platform():
    plan = plan_matches([stop("1", "802-01", dlat_m=2000)], [plat("aaaaa", "802-01")], [GROUP])
    assert plan.platform_for_key["code:802-01"] is None
    assert any("away" in w for w in plan.warnings)


def test_station_hierarchy_groups_by_parent_station():
    s1 = stop("p1", None, name="KRAKÓW GŁÓWNY", station_key="station:85", station_name="KRAKÓW GŁÓWNY")
    assert s1.group_key == "station:85"
    assert stop("x", "802-01").group_key == "802"
    assert stop("y", None, name="Bez Kodu").group_key == "name:bez-kodu"


# ------------------------------------------------------------------ attaching to a shared namespace


def shared(stops, platforms, groups=(GROUP,), stop_bearing=None, platform_bearing=None, modes=None, feed_modes=None):
    return plan_shared(list(stops), list(platforms), list(groups), stop_bearing, platform_bearing, modes, feed_modes)


def test_names_match_tolerates_abbreviations_typos_prefix_and_extra_words():
    assert names_match("Biskupa Prandoty", "bp. Prandoty") == "exact"
    assert names_match("Kraków Mały Płaszów P+R", "Mały Płaszów P+R") == "exact"
    assert names_match("Cło 02", "Cło") == "exact"
    assert names_match("AGH/UR", "AGH / UR") == "exact"
    assert names_match("Pleszów Pętla", "Pleszów") == "subset"
    assert names_match("Morawica Rutki", "Morawica Rudki") == "similar"
    assert names_match("Kraków MDA", "Dworzec Główny Wschód") is None
    assert names_match("Rzeszotary Górne", "Rzeszotary Remiza") is None
    assert names_match("Wieliczka Chopina", "Wieliczka Ochronka") is None  # shared town name isn't enough


def test_spread_out_stop_is_matched_by_its_nearest_pole():
    # ZTP spreads 'Libertów Góra Libertowska' over ~1 km: centre 450 m away, nearest pole 87 m
    group = Group("g2557", "2557", "Libertów Góra Libertowska", LAT + 450 * M, LON)
    poles = [
        plat("p1", "2557-01", name="Libertów Góra Libertowska", group="g2557", dlat_m=87),
        plat("p2", "2557-05", name="Libertów Góra Libertowska", group="g2557", dlat_m=900),
    ]
    st = stop("m", "1", name="Libertów Góra Libertowska", station_key="station:10932", station_name="Libertów Góra Libertowska")
    assert shared([st], poles, [group]).group_for_key == {"station:10932": "g2557"}


def test_same_pole_whatever_the_name():
    # 'Wieliczka Gdowska' (MLD) is the pole ZTP calls 'Wieliczka Ochronka'
    p = plat("aaaaa", "2797-01", name="Wieliczka Ochronka", group="g2797")
    plan = shared(
        [stop("m1", "196", name="Wieliczka Gdowska", dlat_m=1)], [p], [Group("g2797", "2797", "Wieliczka Ochronka", LAT, LON)]
    )
    assert plan.links == {"m1": "aaaaa"}
    assert plan.group_for_key == {"name:wieliczka-gdowska": "g2797"}


def test_two_codes_on_one_spot_take_the_nearest_then_lowest_code():
    platforms = [plat("p0071", "2487-71", name="Radziszów PKP"), plat("p0001", "2487-01", name="Radziszów PKP")]
    plan = shared(
        [stop("m", "6780", name="Radziszów PKP")],
        platforms,
        stop_bearing={"m": 72},
        platform_bearing={"p0001": 162, "p0071": 163},
    )
    assert plan.links == {"m": "p0001"}


def test_attaches_within_the_logical_stop_up_to_30_m_by_direction():
    group = Group("g711", "711", "Mały Płaszów P+R", LAT, LON)
    platforms = [plat("aaaaa", "711-02", name="Mały Płaszów P+R", group="g711")]
    args = ([group], {"m1": 90}, {"aaaaa": 90}, {"aaaaa": {"bus"}}, {"bus"})
    stops = [stop("m1", "1017", name="Kraków Mały Płaszów P+R", dlat_m=20)]
    assert plan_shared(stops, platforms, *args).links == {"m1": "aaaaa"}
    # no direction known: not attached
    assert plan_shared(stops, platforms, [group], {}, {}).links == {}


def test_different_name_and_not_the_same_pole_stays_separate():
    groups = [Group("g461", "461", "Dworzec Główny Wschód", LAT, LON)]
    plan = shared(
        [stop("m", "656", name="Kraków MDA", dlat_m=67)],
        [plat("p1", "461-01", name="Dworzec Główny Wschód", group="g461")],
        groups,
    )
    assert plan.links == {} and plan.group_for_key == {}


def test_direction_of_travel_picks_the_right_side_of_the_road():
    # Kryspinów Stopień Wodny: two ZTP platforms, MLD stops in between (real bearings from 2026-10-25)
    platforms = [
        plat("p0001", "2793-01", name="Kryspinów Stopień Wodny", dlat_m=12, number="01"),
        plat("p0002", "2793-02", name="Kryspinów Stopień Wodny", dlat_m=-12, number="02"),
    ]
    group = Group("g2793", "2793", "Kryspinów Stopień Wodny", LAT, LON)
    platforms = [plat(p.id, p.code, name=p.name, dlat_m=(p.lat - LAT) / M, group="g2793") for p in platforms]
    stops = [stop("859", "907", name="Kryspinów Stopień Wodny"), stop("877", "889", name="Kryspinów Stopień Wodny")]
    plan = shared(stops, platforms, [group], stop_bearing={"859": 59, "877": 240}, platform_bearing={"p0001": 240, "p0002": 59})
    assert plan.links == {"859": "p0002", "877": "p0001"}


def test_opposite_direction_is_rejected_even_when_nearest():
    platforms = [
        plat("p0001", "376-02", name="Glogera Granica Miasta", dlat_m=10, number="02"),
        plat("p0002", "376-01", name="Glogera Granica Miasta", dlat_m=20, number="01"),
    ]
    group = Group("g376", "376", "Glogera Granica Miasta", LAT, LON)
    platforms = [plat(p.id, p.code, name=p.name, dlat_m=(p.lat - LAT) / M, group="g376") for p in platforms]
    plan = shared(
        [stop("m", "1", name="Glogera Granica Miasta")],
        platforms,
        [group],
        stop_bearing={"m": 159},
        platform_bearing={"p0001": 339, "p0002": 159},
    )
    assert plan.links == {"m": "p0002"}


def test_pole_in_the_same_stop_and_direction_is_used_even_when_coordinates_are_off():
    # 'Czyżyny Dworzec': MLD's coordinates are 28 m from ZTP's pole of the same direction
    group = Group("g271", "271", "Czyżyny Dworzec", LAT, LON)
    platforms = [
        plat("p1", "271-01", name="Czyżyny Dworzec", group="g271"),
        plat("p2", "271-02", name="Czyżyny Dworzec", group="g271", dlat_m=14),
    ]
    plan = shared(
        [stop("m", "3470", name="Czyżyny Dworzec", dlat_m=28)],
        platforms,
        [group],
        {"m": 90},
        {"p1": 95, "p2": 275},
        {"p1": {"bus"}, "p2": {"bus"}},
        {"bus"},
    )
    assert plan.links == {"m": "p1"}


def test_pole_beyond_30_m_needs_the_same_next_stop():
    # Wieliczka Centrum / Fort Mogiła: 41-49 m from the ZTP pole and on to the same next stop = the same pole.
    # Rondo Mogilskie 7620: 50 m from pole 02 but on to another stop = its own pole. Niepołomice Kolejowa 02: 71 m.
    g2 = Group("g2", "803", "Cystersów", LAT + 100 * M, LON)
    next_pole = plat("q", "803-01", name="Cystersów", group="g2", dlat_m=100)

    def plan(dlat_m, platform_next):
        platforms = [plat("p2", "802-02", dlat_m=dlat_m), next_pole]
        stops = [stop("m", "7620"), stop("n", "9001", name="Cystersów", dlat_m=100)]
        return plan_shared(
            stops,
            platforms,
            [GROUP, g2],
            {"m": 90},
            {"p2": 90},
            {"p2": {"bus"}, "q": {"bus"}},
            {"bus"},
            {"m": {"n"}},
            platform_next,
        )

    assert plan(45, {"p2": {"g2"}}).links["m"] == "p2"
    other = plan(50, {"p2": {"g9"}})
    assert "m" not in other.links
    assert other.group_for_key["name:rondo-mogilskie"] == "grp01"  # still the same logical stop
    assert "m" not in plan(45, {}).links  # nothing known about where the pole goes: not confirmed
    assert "m" not in plan(71, {"p2": {"g2"}}).links  # too far, whatever it leads to
    assert plan(25, {}).links["m"] == "p2"  # within 30 m direction is enough


def test_pole_further_up_the_line_is_the_next_stop_not_this_one():
    # Czyżyny Dworzec 3470: 49 m from ZTP pole 01 and on to the same stop, but the pole is 47 m ahead of it
    g2 = Group("g2", "803", "Cystersów", LAT + 100 * M, LON)
    q = plat("q", "803-01", name="Cystersów", group="g2", dlat_m=100)
    stops = [stop("m", "3470"), stop("n", "9001", name="Cystersów", dlat_m=100)]
    modes = {"p1": {"bus"}, "q": {"bus"}}
    east = 1 / (111_320 * 0.6425)  # ~1 m in degrees longitude at this latitude

    def plan(dlon_m, dlat_m):
        p1 = Platform("p1", "grp01", "802-01", "Rondo Mogilskie", "01", LAT + dlat_m * M, LON + dlon_m * east, True)
        return plan_shared(stops, [p1, q], [GROUP, g2], {"m": 90}, {"p1": 90}, modes, {"bus"}, {"m": {"n"}}, {"p1": {"g2"}})

    assert "m" not in plan(47, 0).links  # up the road: the next stop
    assert "m" not in plan(-47, 0).links  # back down the road: the previous one
    assert plan(0, 47).links["m"] == "p1"  # across the road: same pole, inexact coordinates


def test_bus_stop_next_to_tram_only_platforms_keeps_its_own_pole():
    group = Group("g215", "215", "Brama nr 5", LAT, LON)
    platforms = [plat("t1", "215-02", name="Brama nr 5", group="g215", dlat_m=40)]
    plan = shared([stop("m", "3486", name="Brama nr 5")], platforms, [group], {"m": 241}, {"t1": 250}, {"t1": {"tram"}}, {"bus"})
    assert plan.links == {}
    assert plan.group_for_key == {"name:brama-nr-5": "g215"}  # still the same logical stop


def osm(ref, dlat_m=0.0, bus=True, tram=False):
    return OsmPole(f"node/{ref}{dlat_m}", LAT + dlat_m * M, LON, "platform", ref, "Rondo Mogilskie", bus, tram)


def test_normalize_ref_reads_ztp_codes_only():
    assert normalize_ref("802-2") == "802-02"
    assert normalize_ref("802-07") == "802-07"
    assert normalize_ref("214-1t") is None  # tram scheme
    assert normalize_ref(None) is None


def test_osm_ref_links_a_stop_whatever_its_coordinates():
    # the ZTP pole is 45 m away and MLD's stop sits next to the OSM pole that carries its code
    platforms = [plat("p2", "802-02", dlat_m=45)]
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    plan = plan_shared([stop("m", "7620")], platforms, *args, osm=[osm("802-02", dlat_m=5)])
    assert plan.links == {"m": "p2"}
    assert plan.methods["osm_ref"] == 1


def test_osm_pole_ztp_does_not_publish_keeps_the_stops_own_pole():
    # MLD 7620 is OSM 802-07; ZTP's 802-02 is 12 m away in the same direction but is another pole
    platforms = [plat("p2", "802-02", dlat_m=12)]
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    assert plan_shared([stop("m", "7620")], platforms, *args).links == {"m": "p2"}  # by position alone it would match
    plan = plan_shared([stop("m", "7620")], platforms, *args, osm=[osm("802-07", dlat_m=2)])
    assert plan.links == {}
    assert plan.methods["osm_own_pole"] == 1
    assert list(plan.group_for_key.values()) == ["grp01"]  # still the same logical stop


def test_osm_without_a_usable_pole_leaves_it_to_position():
    platforms = [plat("p2", "802-02", dlat_m=12)]
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    one = lambda **kw: plan_shared([stop("m", "7620")], platforms, *args, **kw).links  # noqa: E731
    assert one(osm=[osm(None, dlat_m=2)]) == {"m": "p2"}  # a pole without a ref says nothing
    assert one(osm=[osm("802-07", dlat_m=60)]) == {"m": "p2"}  # too far to be the stop's pole
    assert one(osm=[osm("214-1t", dlat_m=2, bus=False, tram=True)]) == {"m": "p2"}  # a tram pole
    assert one(osm=[osm("802-07", dlat_m=2), osm("802-02", dlat_m=3)]) == {}  # the nearest pole decides


def test_bus_stop_is_never_attached_to_a_tram_only_pole_by_name_or_position():
    # Giedroycia 3485: 27 m from ZTP 214-01, which only trams serve
    group = Group("g214", "214", "Giedroycia", LAT, LON)
    platforms = [plat("t1", "214-01", name="Giedroycia", group="g214", dlat_m=27)]
    args = ([group], {"m": 90}, {"t1": 90}, {"t1": {"tram"}}, {"bus"})
    assert plan_shared([stop("m", "3485", name="Giedroycia")], platforms, *args).links == {}
    close = [plat("t1", "214-01", name="Giedroycia", group="g214", dlat_m=5)]
    assert plan_shared([stop("m", "3485", name="Giedroycia")], close, *args).links == {}


def test_osm_ref_decides_up_to_60_m():
    # Fort Mogiła 3483: the OSM pole with its ZTP code is 40 m away
    platforms = [plat("p3", "889-03", name="Fort Mogiła", group="g889", dlat_m=60)]
    group = Group("g889", "889", "Fort Mogiła", LAT, LON)
    args = ([group], {"m": 90}, {"p3": 90}, {"p3": {"bus"}}, {"bus"}, {}, {})
    s = stop("m", "3483", name="Fort Mogiła")
    assert plan_shared([s], platforms, *args, osm=[osm("889-03", dlat_m=40)]).links == {"m": "p3"}
    # a ZTP code ZTP doesn't have, on a pole 30 m off: not the stop's own pole (see the test below)
    assert plan_shared([s], platforms, *args, osm=[osm("889-07", dlat_m=30)]).methods["osm_own_pole"] == 0


def test_osm_operator_code_and_bay_refs():
    platforms = [plat("p1", "802-01", dlat_m=45)]
    args = ([GROUP], {"m": 90}, {"p1": 90}, {"p1": {"bus"}}, {"bus"}, {}, {})
    # Wieliczka Centrum 200: OSM maps a pole tagged with MLD's own code: its own pole, though ZTP's is near
    plan = plan_shared([stop("m", "200")], platforms, *args, osm=[osm("200", dlat_m=4)])
    assert plan.links == {} and plan.methods["osm_own_pole"] == 1
    # another operator's code says nothing
    assert plan_shared([stop("m", "200")], platforms, *args, osm=[osm("26", dlat_m=4)]).methods["osm_own_pole"] == 0
    # Czyżyny Dworzec: bay 06 is not the bay ZTP's pole is on (bay 01)
    bays = [osm("271-01-01", dlat_m=44), osm("271-01-06", dlat_m=5)]
    plan = plan_shared([stop("m", "3470")], platforms, *args, osm=bays)
    assert plan.links == {} and plan.methods["osm_own_pole"] == 1
    # the stop at bay 01 is ZTP's pole
    on_bay = plan_shared([stop("m", "3470", dlat_m=44)], platforms, *args, osm=bays)
    assert on_bay.links == {"m": "p1"} and on_bay.methods["osm_ref"] == 1


def test_osm_poles_at_the_same_distance_do_not_break_the_lookup():
    platforms = [plat("p2", "802-02")]
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    poles = [osm("802-02", dlat_m=5), osm("802-02", dlat_m=-5)]  # a platform and a stop position, equally close
    assert plan_shared([stop("m", "7620")], platforms, *args, osm=poles).links == {"m": "p2"}


def test_stop_and_pole_on_different_osm_poles_are_not_the_same_pole():
    # Czernichów Rynek 866: 24 m from ZTP 2046-01, same direction, but OSM has a pole at each (29 m apart)
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"})
    two = [osm(None, dlat_m=3), osm(None, dlat_m=31)]
    assert plan_shared([stop("m", "866")], [plat("p2", "802-02", dlat_m=28)], *args, osm=two).links == {}
    # without OSM, or with one OSM pole by both, the position rules attach it
    assert plan_shared([stop("m", "866")], [plat("p2", "802-02", dlat_m=28)], *args).links == {"m": "p2"}
    one = [osm(None, dlat_m=14)]
    assert plan_shared([stop("m", "866", dlat_m=10)], [plat("p2", "802-02", dlat_m=28)], *args, osm=one).links == {"m": "p2"}


def test_osm_calls_a_stop_an_own_pole_only_when_it_has_a_pole_there():
    platforms = [plat("p2", "802-02", dlat_m=12)]
    args = ([GROUP], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    near = plan_shared([stop("m", "7620")], platforms, *args, osm=[osm("802-07", dlat_m=2)])
    assert near.links == {} and near.methods["osm_own_pole"] == 1
    # the same code on a pole 30 m off may be a neighbour (wrong GPS): the position rules decide
    far = plan_shared([stop("m", "7620")], platforms, *args, osm=[osm("802-07", dlat_m=30)])
    assert far.links == {"m": "p2"} and far.methods["osm_own_pole"] == 0
    # the stop's own code is identity, however far the GPS is off
    own = plan_shared([stop("m", "7620")], platforms, *args, osm=[osm("7620", dlat_m=35)])
    assert own.links == {} and own.methods["osm_own_pole"] == 1


def test_osm_ref_names_the_pole_even_when_the_stops_gps_is_60_m_off():
    # Pleszowska 2833: the only OSM pole near is ZTP's 419-02, 58 m away, in the stop's direction
    platforms = [plat("p2", "419-02", name="Pleszowska", group="g419", dlat_m=40)]
    args = ([Group("g419", "419", "Pleszowska", LAT, LON)], {"m": 90}, {"p2": 90}, {"p2": {"bus"}}, {"bus"}, {}, {})
    plan = plan_shared([stop("m", "2833", name="Pleszowska")], platforms, *args, osm=[osm("419-02", dlat_m=58)])
    assert plan.links == {"m": "p2"}
