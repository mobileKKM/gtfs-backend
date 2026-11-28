//! The GTFS files we store: one table per file with exactly its columns (the migration's `gtfs.*` tables), typed.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Text,
    Int,
    SmallInt,
    Float,
    /// GTFS time, may pass 24:00:00 (`interval`)
    Time,
    /// GTFS date YYYYMMDD
    Date,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Text => "text",
            Kind::Int => "int",
            Kind::SmallInt => "smallint",
            Kind::Float => "float",
            Kind::Time => "time",
            Kind::Date => "date",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub required: bool,
}

#[derive(Debug)]
pub struct Table {
    pub name: &'static str,
    pub columns: &'static [Column],
    pub required_file: bool,
}

impl Table {
    pub fn filename(&self) -> String {
        format!("{}.txt", self.name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

const fn c(name: &'static str) -> Column {
    Column { name, kind: Kind::Text, required: false }
}
const fn req(name: &'static str) -> Column {
    Column { name, kind: Kind::Text, required: true }
}
const fn t(name: &'static str, kind: Kind) -> Column {
    Column { name, kind, required: false }
}
const fn treq(name: &'static str, kind: Kind) -> Column {
    Column { name, kind, required: true }
}

use Kind::*;

/// In load order: a file comes after the files it references (trips after routes, stop_times after trips and stops).
pub static TABLES: &[Table] = &[
    Table {
        name: "agency",
        columns: &[
            c("agency_id"),
            req("agency_name"),
            req("agency_url"),
            req("agency_timezone"),
            c("agency_lang"),
            c("agency_phone"),
            c("agency_fare_url"),
            c("agency_email"),
            t("cemv_support", SmallInt),
        ],
        required_file: true,
    },
    Table {
        name: "stops",
        columns: &[
            req("stop_id"),
            c("stop_code"),
            c("stop_name"),
            c("tts_stop_name"),
            c("stop_desc"),
            t("stop_lat", Float),
            t("stop_lon", Float),
            c("zone_id"),
            c("stop_url"),
            t("location_type", SmallInt),
            c("parent_station"),
            c("stop_timezone"),
            t("wheelchair_boarding", SmallInt),
            c("level_id"),
            c("platform_code"),
            t("stop_access", SmallInt),
        ],
        required_file: true,
    },
    Table {
        name: "routes",
        columns: &[
            req("route_id"),
            c("agency_id"),
            c("route_short_name"),
            c("route_long_name"),
            c("route_desc"),
            treq("route_type", Int),
            c("route_url"),
            c("route_color"),
            c("route_text_color"),
            t("route_sort_order", Int),
            t("continuous_pickup", SmallInt),
            t("continuous_drop_off", SmallInt),
            c("network_id"),
            t("cemv_support", SmallInt),
        ],
        required_file: true,
    },
    Table {
        name: "calendar",
        columns: &[
            req("service_id"),
            treq("monday", SmallInt),
            treq("tuesday", SmallInt),
            treq("wednesday", SmallInt),
            treq("thursday", SmallInt),
            treq("friday", SmallInt),
            treq("saturday", SmallInt),
            treq("sunday", SmallInt),
            treq("start_date", Date),
            treq("end_date", Date),
        ],
        required_file: false,
    },
    Table {
        name: "calendar_dates",
        columns: &[req("service_id"), treq("date", Date), treq("exception_type", SmallInt)],
        required_file: false,
    },
    Table {
        name: "shapes",
        columns: &[
            req("shape_id"),
            treq("shape_pt_lat", Float),
            treq("shape_pt_lon", Float),
            treq("shape_pt_sequence", Int),
            t("shape_dist_traveled", Float),
        ],
        required_file: false,
    },
    Table {
        name: "feed_info",
        columns: &[
            req("feed_publisher_name"),
            req("feed_publisher_url"),
            req("feed_lang"),
            c("default_lang"),
            t("feed_start_date", Date),
            t("feed_end_date", Date),
            c("feed_version"),
            c("feed_contact_email"),
            c("feed_contact_url"),
        ],
        required_file: false,
    },
    // ZTP's tram feed: block_id -> shift
    Table { name: "blocks", columns: &[req("block_id"), c("shift")], required_file: false },
    Table {
        name: "trips",
        columns: &[
            req("route_id"),
            req("service_id"),
            req("trip_id"),
            c("trip_headsign"),
            c("trip_short_name"),
            t("direction_id", SmallInt),
            c("block_id"),
            c("shape_id"),
            t("wheelchair_accessible", SmallInt),
            t("bikes_allowed", SmallInt),
            t("cars_allowed", SmallInt),
        ],
        required_file: true,
    },
    Table {
        name: "stop_times",
        columns: &[
            req("trip_id"),
            t("arrival_time", Time),
            t("departure_time", Time),
            c("stop_id"),
            c("location_group_id"),
            c("location_id"),
            treq("stop_sequence", Int),
            c("stop_headsign"),
            t("start_pickup_drop_off_window", Time),
            t("end_pickup_drop_off_window", Time),
            t("pickup_type", SmallInt),
            t("drop_off_type", SmallInt),
            t("continuous_pickup", SmallInt),
            t("continuous_drop_off", SmallInt),
            t("shape_dist_traveled", Float),
            t("timepoint", SmallInt),
            c("pickup_booking_rule_id"),
            c("drop_off_booking_rule_id"),
        ],
        required_file: true,
    },
];
