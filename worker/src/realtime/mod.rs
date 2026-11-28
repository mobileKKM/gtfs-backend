//! Realtime: one poller per GTFS-RT file -> normalize against the in-memory timetable -> Redis + change messages.

pub mod alerts;
pub mod extra;
pub mod index;
pub mod layout;
pub mod poller;
pub mod process;
pub mod replay;
pub mod resolve;
pub mod runtime;
pub mod store;
pub mod trips;
pub mod vehicles;

#[cfg(test)]
mod tests;

/// The GTFS-realtime protobuf messages (proto/gtfs-realtime.proto).
#[allow(clippy::all, deprecated)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime.rs"));
}
