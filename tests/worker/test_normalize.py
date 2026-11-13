"""Realtime normalization with a hand-built trip index (no DB)."""

from datetime import date
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.servicetime import service_day_base
from gtfs_backend.worker.realtime.normalize import (
    FeedRealtimeState,
    Result,
    build_alert,
    match_updates,
    merge_stops,
    process_message,
    resolve,
    resolve_instance,
)
from gtfs_backend.worker.realtime.trip_index import ServiceDay, StopTime, TripIndex, TripInfo

WAW = ZoneInfo("Europe/Warsaw")
D22, D23 = date(2026, 10, 22), date(2026, 10, 23)


def hms(h, m=0, s=0):
    return h * 3600 + m * 60 + s


def day(d, version, services):
    return ServiceDay(d, version, service_day_base(d, WAW), frozenset(services))


def trip(version, trip_id, service, first, last, route="R1", **kw):
    return TripInfo(
        version,
        trip_id,
        service,
        route,
        kw.get("direction", 0),
        kw.get("shape", "S1"),
        kw.get("headsign", "Salwator"),
        kw.get("block"),
        kw.get("short", "1"),
        900,
        first,
        last,
    )


def make_index():
    """Old tram version 1 serves the 22nd, new version 2 from the 23rd; both reuse trip ids."""
    idx = TripIndex("kmk_t", WAW, loaded_for=D23)
    idx.days = [day(D22, 1, {"service_4"}), day(D23, 2, {"service_4"})]
    # same trip id in both versions: a night trip in v1 (23:50 -> 25:20), a morning trip in v2
    idx.add_trip(trip(1, "block_1_trip_9_service_4", "service_4", hms(23, 50), hms(25, 20)))
    idx.add_trip(trip(2, "block_1_trip_9_service_4", "service_4", hms(6), hms(6, 40)))
    stops = [
        StopTime(1, "A", hms(23, 50), hms(23, 50)),
        StopTime(2, "B", hms(24, 30), hms(24, 30)),
        StopTime(3, "A", hms(25, 0), hms(25, 0)),
        StopTime(4, "C", hms(25, 20), hms(25, 20)),
    ]  # loop: A twice
    idx.stop_times[(1, "block_1_trip_9_service_4")] = stops
    idx.stop_times[(2, "block_1_trip_9_service_4")] = [StopTime(1, "A", hms(6), hms(6)), StopTime(2, "C", hms(6, 40), hms(6, 40))]
    for v in (1, 2):
        for s in "ABC":
            idx.stop_links[(v, s)] = f"stop{s.lower()}"
    return idx


def epoch(d, h, m=0):
    return service_day_base(d, WAW) + hms(h, m)


async def noop(keys):
    return None


def test_night_trip_of_previous_day_resolves_to_old_version():
    idx = make_index()
    ref = resolve(idx, "block_1_trip_9_service_4", epoch(D22, 25, 10))  # 01:10 on the 23rd
    assert ref.info.version == 1 and ref.day.date == D22
    ref = resolve(idx, "block_1_trip_9_service_4", epoch(D23, 6, 10))
    assert ref.info.version == 2 and ref.day.date == D23


def test_service_suffix_fallback_finds_previous_days_trip():
    idx = make_index()
    now = epoch(D22, 25, 10)  # 01:10 on the 23rd; the feed already names the trip with the new day's service
    assert resolve(idx, "block_1_trip_9_service_2", now) is None
    ref = resolve(idx, "block_1_trip_9_service_2", now, ignore_service_suffix=True)
    assert ref.info.version == 1 and ref.day.date == D22 and ref.info.trip_id == "block_1_trip_9_service_4"
    # an exact id still wins; unknown blocks and trips outside the window stay unmatched
    assert resolve(idx, "block_1_trip_9_service_4", now, ignore_service_suffix=True).info.version == 1
    assert resolve(idx, "block_2_trip_9_service_2", now, ignore_service_suffix=True) is None
    assert resolve(idx, "block_1_trip_9_service_2", epoch(D23, 15), ignore_service_suffix=True) is None


def test_trip_outside_any_window_does_not_resolve():
    assert resolve(make_index(), "block_1_trip_9_service_4", epoch(D23, 15)) is None
    assert resolve(make_index(), "unknown", epoch(D23, 6)) is None


def test_stop_updates_without_sequence_follow_the_loop_forward():
    stops = make_index().stop_times[(1, "block_1_trip_9_service_4")]
    tu = pb.TripUpdate()
    for sid in ("B", "A", "C"):
        tu.stop_time_update.add(stop_id=sid)
    matched = [st.sequence for _, st in match_updates(tu.stop_time_update, stops)]
    assert matched == [2, 3, 4]  # the second visit of A, not the first


def feed(*entities, ts):
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    for e in entities:
        msg.entity.append(e)
    return msg


async def test_tram_style_absolute_times_give_computed_delays_and_unmatched_are_dropped():
    idx = make_index()
    now = epoch(D22, 24, 35)
    tu = pb.FeedEntity(id="t1")
    tu.trip_update.trip.trip_id = "block_1_trip_9_service_4"
    tu.trip_update.vehicle.id = "302"
    u = tu.trip_update.stop_time_update.add(stop_id="A")
    u.arrival.time = epoch(D22, 25, 2)  # 2 min late at the second visit of A
    u = tu.trip_update.stop_time_update.add(stop_id="C")
    u.arrival.time = epoch(D22, 25, 21)
    vp = pb.FeedEntity(id="v1")
    vp.vehicle.trip.trip_id = "block_1_trip_9_service_4"
    vp.vehicle.trip.route_id = "SOMETHING_ELSE"
    vp.vehicle.vehicle.id = "302"
    vp.vehicle.position.latitude, vp.vehicle.position.longitude = 50.06, 19.96
    vp.vehicle.stop_id = "A"
    ghost = pb.FeedEntity(id="v2")
    ghost.vehicle.trip.trip_id = "not_in_static"
    ghost.vehicle.vehicle.id = "999"
    ghost.vehicle.position.latitude, ghost.vehicle.position.longitude = 50.0, 19.9
    idle = pb.FeedEntity(id="v3")
    idle.vehicle.vehicle.id = "idle"
    idle.vehicle.position.latitude, idle.vehicle.position.longitude = 50.0, 19.9

    state = FeedRealtimeState()
    res = await process_message("kmk_t", feed(tu, vp, ghost, idle, ts=int(now)), idx, state, noop, now)

    assert [t["delay_s"] for t in res.trips] == [120]
    assert [s["stop_sequence"] for s in res.trips[0]["stops"]] == [3, 4]
    assert res.trips[0]["stops"][0]["public_stop_id"] == "stopa"
    assert res.trips[0]["service_date"] == "2026-10-22" and res.trips[0]["version"] == 1
    assert res.trips[0]["key"] == f"kmk_t:20261022:{res.trips[0]['trip_id']}"  # feed-prefixed, no version

    assert [v["vehicle_id"] for v in res.vehicles] == ["302"]  # ghost + idle never shown
    v = res.vehicles[0]
    assert v["trip"]["route_id"] == "R1"  # trip_id is the source of truth
    assert v["delay_s"] == 120
    assert res.mismatches == 1
    assert {u["vehicle_id"] for u in res.unmatched_vehicles} == {"999", "idle"}


async def test_feed_delay_is_recomputed_not_trusted():
    idx = make_index()
    now = epoch(D23, 6, 5)
    tu = pb.FeedEntity(id="t1")
    tu.trip_update.trip.trip_id = "block_1_trip_9_service_4"
    u = tu.trip_update.stop_time_update.add(stop_sequence=2, stop_id="C")
    u.departure.time = epoch(D23, 6, 43)
    u.departure.delay = 9999  # wrong on purpose: we compute predicted - scheduled ourselves
    res = await process_message("kmk_t", feed(tu, ts=int(now)), idx, FeedRealtimeState(), noop, now)
    assert res.trips[0]["delay_s"] == 180


def test_loop_stop_listed_first_picks_the_visit_closest_in_time():
    idx = make_index()
    stops = idx.stop_times[(1, "block_1_trip_9_service_4")]
    tu = pb.TripUpdate()
    u = tu.stop_time_update.add(stop_id="A")
    u.arrival.time = epoch(D22, 25, 2)
    base = idx.days[0].base
    assert [st.sequence for _, st in match_updates(tu.stop_time_update, stops, base)] == [3]
    assert [st.sequence for _, st in match_updates(tu.stop_time_update, stops)] == [1]  # without time info: next visit


def rt_trip(version, *stops):
    """stops: (stop_sequence, stop_id, predicted_departure)"""
    return {
        "key": "kmk_a:x:20261025",
        "version": version,
        "stops": [{"stop_sequence": q, "stop_id": sid, "predicted_departure": t} for q, sid, t in stops],
    }


def seqs(trip):
    return [r["stop_sequence"] for r in trip["stops"]]


def test_dropped_stops_are_kept_with_their_last_known_times():
    first = merge_stops(None, rt_trip(1, (1, "a", 100), (2, "b", 200), (3, "c", 300)))
    assert seqs(first) == [1, 2, 3]
    second = merge_stops(first, rt_trip(1, (3, "c", 310), (4, "d", 410), (5, "e", 510)))
    assert seqs(second) == [1, 2, 3, 4, 5]
    assert [r["predicted_departure"] for r in second["stops"]] == [100, 200, 310, 410, 510]
    # dropped rows stay on later polls
    third = merge_stops(second, rt_trip(1, (5, "e", 520)))
    assert seqs(third) == [1, 2, 3, 4, 5]
    assert third["stops"][2]["predicted_departure"] == 310
    assert not any("passed" in r for r in third["stops"])


def test_version_change_drops_the_kept_stops():
    prev = merge_stops(None, rt_trip(1, (1, "a", 100), (2, "b", 200)))
    assert seqs(merge_stops(prev, rt_trip(2, (2, "b", 210)))) == [2]


def test_empty_update_keeps_all_rows():
    prev = merge_stops(None, rt_trip(1, (1, "a", 100), (2, "b", 200)))
    assert seqs(merge_stops(prev, rt_trip(1))) == [1, 2]


def test_loop_trip_keeps_both_visits_of_a_stop():
    prev = merge_stops(None, rt_trip(1, (1, "a", 100), (2, "b", 200), (3, "a", 300)))
    merged = merge_stops(prev, rt_trip(1, (3, "a", 305)))
    assert [(r["stop_sequence"], r["stop_id"], r["predicted_departure"]) for r in merged["stops"]] == [
        (1, "a", 100),
        (2, "b", 200),
        (3, "a", 305),
    ]


async def test_missing_speed_and_bearing_are_filled_from_the_previous_poll():
    idx = make_index()
    state = FeedRealtimeState()
    now = epoch(D23, 6, 5)

    def message(lat, t):
        vp = pb.FeedEntity(id="v1")
        vp.vehicle.trip.trip_id = "block_1_trip_9_service_4"
        vp.vehicle.vehicle.id = "302"
        vp.vehicle.timestamp = t
        vp.vehicle.position.latitude, vp.vehicle.position.longitude = lat, 19.96
        return feed(vp, ts=t)

    res = await process_message("kmk_t", message(50.06, int(now)), idx, state, noop, now)
    assert (res.vehicles[0]["bearing"], res.vehicles[0]["speed"]) == (None, None)
    res = await process_message("kmk_t", message(50.06 + 100 / 111_195, int(now) + 10), idx, state, noop, now + 10)
    assert (res.vehicles[0]["bearing"], res.vehicles[0]["speed"]) == (0.0, 10.0)


async def test_feed_zeros_count_as_unknown():
    idx = make_index()
    state = FeedRealtimeState()
    now = int(epoch(D23, 6, 5))

    async def poll(lon, t):
        vp = pb.FeedEntity(id="v1")
        vp.vehicle.trip.trip_id = "block_1_trip_9_service_4"
        vp.vehicle.vehicle.id = "302"
        vp.vehicle.timestamp = now + t
        vp.vehicle.position.latitude, vp.vehicle.position.longitude = 50.06, lon
        vp.vehicle.position.speed = 0  # ZTP's placeholders, also for driving buses
        vp.vehicle.position.bearing = 0
        res = await process_message("kmk_a", feed(vp, ts=now + t), idx, state, noop, now + t)
        return res.vehicles[0]["bearing"], res.vehicles[0]["speed"]

    east = 19.96 + 0.0014  # ~100 m east
    assert await poll(19.96, 0) == (None, None)
    assert await poll(19.96, 10) == (None, 0.0)  # standing, heading still unknown
    assert await poll(east, 20) == (90.0, 10.0)  # driving east, not north at 0 km/h
    assert await poll(east, 30) == (90.0, 0.0)  # stopped: keeps its last heading


# --------------------------------------------------------------------------------------------- alerts


def alert_index():
    idx = make_index()
    idx.routes = {(1, "R1"), (2, "R1")}
    idx.stations[(2, "STATION")] = ("grp", ("stopa", "stopb"))
    return idx


def test_alert_trip_is_one_instance():
    idx = alert_index()
    trip_id = "block_1_trip_9_service_4"
    # the night trip of the 22nd is over at 05:00 on the 23rd: the morning one is meant
    assert resolve_instance(idx, trip_id, epoch(D23, 5)).day.date == D23
    assert resolve_instance(idx, trip_id, epoch(D22, 24)).day.date == D22  # still running
    assert resolve_instance(idx, trip_id, epoch(D23, 12)).day.date == D23  # all over: the last one
    assert resolve_instance(idx, trip_id, epoch(D23, 5), start_date=D22).day.date == D22
    assert resolve_instance(idx, "block_1_trip_9_service_2", epoch(D23, 5)) is None
    assert resolve_instance(idx, "block_1_trip_9_service_2", epoch(D23, 5), ignore_service_suffix=True).day.date == D23


def test_alert_entities_are_resolved_to_ours_and_unknown_ones_dropped():
    a = pb.Alert()
    a.effect = pb.Alert.DETOUR
    a.severity_level = pb.Alert.WARNING
    a.header_text.translation.add(text="Utrudnienia", language="pl")
    a.description_text.translation.add(text="Linia 1\r\nobjazd \n", language="pl")
    a.informed_entity.add(route_id="R1", stop_id="A")  # line 1 at stop A
    a.informed_entity.add(stop_id="STATION")
    a.informed_entity.add(route_id="R1", direction_id=1)
    a.informed_entity.add().trip.trip_id = "block_1_trip_9_service_4"
    a.informed_entity.add(route_id="R9")
    a.informed_entity.add(route_id="R1", stop_id="Z")  # AND: one unknown field and the entity means nothing
    res = Result()
    out = build_alert("kmk_t", "a1", a, alert_index(), FeedRealtimeState(), epoch(D23, 5), res)
    assert (out["cause"], out["effect"], out["severity"]) == (None, "detour", "warning")
    assert out["description"] == "Linia 1\nobjazd"
    pick = lambda e: {k: v for k, v in e.items() if v not in (None, [])}  # noqa: E731
    assert [pick(e) for e in out["entities"]] == [
        {"route_id": "R1", "public_stop_ids": ["stopa"]},
        {"public_stop_ids": ["stopa", "stopb"], "stop_group_id": "grp"},
        {"route_id": "R1", "direction_id": 1},
        {"trip_key": "kmk_t:20261023:block_1_trip_9_service_4"},
    ]
    assert res.unmatched_alert_entities == ["a1: route R9", "a1: stop Z"]


def test_alert_without_entities_is_feed_wide():
    a = pb.Alert()
    a.header_text.translation.add(text="Komunikat")
    out = build_alert("kmk_t", "a2", a, alert_index(), FeedRealtimeState(), epoch(D23, 5), Result())
    assert len(out["entities"]) == 1 and not any(out["entities"][0].values())
    assert out["header"] == "Komunikat" and out["severity"] is None


def test_alert_enums_match_gtfs_rt():
    """The API's enums list exactly what the worker stores: the protobuf names, lowercase, without the UNKNOWN_*."""
    from gtfs_backend.api.schemas import AlertCause, AlertEffect, AlertSeverity

    for api, proto in ((AlertCause, pb.Alert.Cause), (AlertEffect, pb.Alert.Effect), (AlertSeverity, pb.Alert.SeverityLevel)):
        assert {m.value for m in api} == {n.lower() for n in proto.keys() if not n.startswith("UNKNOWN_")}  # noqa: SIM118 (not iterable)
