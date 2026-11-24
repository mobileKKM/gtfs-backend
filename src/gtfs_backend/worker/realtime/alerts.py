"""Alerts -> live_layout.LiveAlert, with the informed entities in our ids (trip keys, public stop ids)."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.live_layout import AlertEntity, LiveAlert
from gtfs_backend.worker.realtime.resolve import resolve_instance, start_date
from gtfs_backend.worker.realtime.trip_index import ServiceDay, TripIndex

EMPTY_ENTITY: AlertEntity = {
    "agency_id": None,
    "route_id": None,
    "route_type": None,
    "direction_id": None,
    "trip_key": None,
    "public_stop_ids": [],
    "stop_group_id": None,
}


def _text(ts) -> str | None:
    if not ts.translation:
        return None
    text = next((t.text for t in ts.translation if t.language in ("pl", "")), ts.translation[0].text)
    return text.replace("\r\n", "\n").strip() or None


def _enum(alert, field: str, enum, unknown: str) -> str | None:
    """A Cause/Effect/SeverityLevel as lowercase name; unset or unknown is None."""
    if not alert.HasField(field):
        return None
    name = enum.Name(getattr(alert, field))
    return None if name == unknown else name.lower()


def _entity(
    feed_id: str, ie, index: TripIndex, today: ServiceDay | None, now: float, ignore_service_suffix: bool
) -> AlertEntity | str:
    """One informed entity with the trip and stop resolved to ours, or the reason it can't be: an entity applies where
    all of its fields do, so one unknown field makes the whole entity meaningless."""
    route_id = ie.route_id or None
    if route_id and (today is None or (today.version, route_id) not in index.routes):
        return f"route {route_id}"
    trip_key = None
    if ie.trip.trip_id:
        ref = resolve_instance(index, ie.trip.trip_id, now, start_date(ie.trip), ignore_service_suffix)
        if ref is None:
            return f"trip {ie.trip.trip_id}"
        trip_key = ref.key(feed_id)
    public_stop_ids, group_id = [], None
    if ie.stop_id:
        pid = index.public_stop(today.version, ie.stop_id) if today else None
        if pid is not None:
            public_stop_ids = [pid]
        elif today and (today.version, ie.stop_id) in index.stations:
            group_id, pids = index.stations[(today.version, ie.stop_id)]
            public_stop_ids = list(pids)
        else:
            return f"stop {ie.stop_id}"
    return {
        "agency_id": ie.agency_id or None,
        "route_id": route_id,
        "route_type": ie.route_type if ie.HasField("route_type") else None,
        "direction_id": ie.direction_id if route_id and ie.HasField("direction_id") else None,
        "trip_key": trip_key,
        "public_stop_ids": public_stop_ids,
        "stop_group_id": group_id,
    }


def build_alert(
    feed_id: str, entity_id: str, alert, index: TripIndex, now: float, ignore_service_suffix: bool = False
) -> tuple[LiveAlert, list[str]]:
    """The alert, and its informed entities that name something we don't have ("{entity_id}: route X"): those are
    dropped. No informed entity at all: the whole feed (one empty entity)."""
    today = index.day(index.loaded_for) if index.loaded_for else None
    entities: list[AlertEntity] = []
    unmatched: list[str] = []
    for ie in alert.informed_entity:
        e = _entity(feed_id, ie, index, today, now, ignore_service_suffix)
        if isinstance(e, str):
            unmatched.append(f"{entity_id}: {e}")
        else:
            entities.append(e)
    if not alert.informed_entity:
        entities.append(EMPTY_ENTITY)
    out: LiveAlert = {
        "feed_id": feed_id,
        "id": entity_id,
        "cause": _enum(alert, "cause", pb.Alert.Cause, "UNKNOWN_CAUSE"),
        "effect": _enum(alert, "effect", pb.Alert.Effect, "UNKNOWN_EFFECT"),
        "severity": _enum(alert, "severity_level", pb.Alert.SeverityLevel, "UNKNOWN_SEVERITY"),
        "header": _text(alert.header_text),
        "description": _text(alert.description_text),
        "url": _text(alert.url),
        "active_periods": [{"start": p.start or None, "end": p.end or None} for p in alert.active_period],
        "entities": entities,
    }
    return out, unmatched
