"""VehiclePositions -> map vehicles (live_layout.LiveVehicle)."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.live_layout import LiveVehicle, TripFields
from gtfs_backend.worker.realtime.motion import MotionTracker
from gtfs_backend.worker.realtime.resolve import TripRef
from gtfs_backend.worker.realtime.trip_index import TripIndex
from gtfs_backend.worker.realtime.trip_updates import trip_fields


def _enum_name(msg, field: str, enum) -> str | None:
    return enum.Name(getattr(msg, field)) if msg.HasField(field) else None


def build_vehicle(
    feed_id: str, ref: TripRef, vp, index: TripIndex, delay: int | None, motion: MotionTracker | None = None, ts: int = 0
) -> LiveVehicle:
    """`motion` fills bearing/speed the feed left out (from the vehicle's previous fix at `ts` or vp.timestamp)."""
    return vehicle_for(feed_id, trip_fields(feed_id, ref), ref.info.version, vp, index, delay, motion, ts)


def vehicle_for(
    feed_id: str, trip: TripFields, version: int, vp, index: TripIndex, delay: int | None, motion: MotionTracker | None, ts: int
) -> LiveVehicle:
    """A vehicle on a trip given by its fields (also an extra trip, which has no timetabled one)."""
    pos = vp.position
    # ZTP sends 0 for unknown: speed 0 for most MPK buses (driving ones too), bearing 0 far more often than
    # heading north explains. Treated as missing; a standing vehicle is estimated as speed 0, last bearing.
    bearing = pos.bearing if pos.HasField("bearing") and pos.bearing != 0 else None
    speed = pos.speed if pos.HasField("speed") and pos.speed > 0 else None
    if motion is not None and vp.vehicle.id:
        bearing, speed = motion.fill(vp.vehicle.id, pos.latitude, pos.longitude, int(vp.timestamp) or ts, bearing, speed)
    return {
        "feed_id": feed_id,
        "vehicle_id": vp.vehicle.id,
        "label": vp.vehicle.label or None,
        "license_plate": vp.vehicle.license_plate or None,
        "lat": pos.latitude,
        "lon": pos.longitude,
        "bearing": bearing,
        "speed": speed,
        "timestamp": int(vp.timestamp) or ts,  # without its own: as old as the feed (the API hides old data by it)
        "current_status": _enum_name(vp, "current_status", pb.VehiclePosition.VehicleStopStatus),
        "current_stop_sequence": vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None,
        "stop_id": vp.stop_id or None,
        "public_stop_id": index.public_stop(version, vp.stop_id),
        "trip": trip,
        "delay_s": delay,
        "occupancy": _enum_name(vp, "occupancy_status", pb.VehiclePosition.OccupancyStatus),
        "wheelchair_accessible": _enum_name(vp.vehicle, "wheelchair_accessible", pb.VehicleDescriptor.WheelchairAccessible),
    }
