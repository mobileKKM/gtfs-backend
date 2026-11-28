//! What realtime processing needs of the timetables, in memory, per feed: the trips running on the service days
//! yesterday/today/tomorrow (each day's serving version). Stop times are loaded per trip when it first appears.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use anyhow::Result;
use chrono::NaiveDate;
use chrono_tz::Tz;
use regex::Regex;
use tokio_postgres::GenericClient;

use crate::config::DayStart;
use crate::util::{day_base, nearby_dates};

static SERVICE_SUFFIX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"_service_\w+$").unwrap());

pub fn strip_service_suffix(trip_id: &str) -> &str {
    SERVICE_SUFFIX.find(trip_id).map_or(trip_id, |m| &trip_id[..m.start()])
}

#[derive(Debug, Clone, PartialEq)]
pub struct TripInfo {
    /// per-feed version number (unique within one feed's index)
    pub version: i32,
    pub trip_id: String,
    pub service_id: String,
    pub route_id: String,
    pub direction_id: Option<i32>,
    pub shape_id: Option<String>,
    pub headsign: Option<String>,
    pub block_id: Option<String>,
    pub route_short_name: Option<String>,
    pub route_type: Option<i32>,
    /// seconds since the service-day base
    pub first_departure: i64,
    pub last_arrival: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceDay {
    pub date: NaiveDate,
    pub version: i32,
    /// the epoch the GTFS times count from (noon - 12 h, or midnight for some feeds)
    pub base: i64,
    pub services: Arc<HashSet<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StopTime {
    pub sequence: i64,
    pub stop_id: String,
    /// seconds since the service-day base
    pub arrival: Option<i64>,
    pub departure: Option<i64>,
}

/// What never changes for a set of versions and their services on the index's days: reused across reloads.
#[derive(Debug, Default)]
pub struct VersionData {
    pub trips: HashMap<(i32, String), Arc<TripInfo>>,
    pub by_trip_id: HashMap<String, Vec<Arc<TripInfo>>>,
    /// trip id without `_service_N`
    pub by_base_id: HashMap<String, Vec<Arc<TripInfo>>>,
    /// (version, stop_id) -> public_stop_id
    pub stop_links: HashMap<(i32, String), String>,
    /// (version, route_id)
    pub routes: HashSet<(i32, String)>,
    /// (version, block_id) -> the block's trips by first departure
    pub blocks: HashMap<(i32, String), Vec<Arc<TripInfo>>>,
}

impl VersionData {
    pub fn add_trip(&mut self, info: TripInfo) {
        let info = Arc::new(info);
        self.trips.insert((info.version, info.trip_id.clone()), info.clone());
        self.by_trip_id.entry(info.trip_id.clone()).or_default().push(info.clone());
        self.by_base_id.entry(strip_service_suffix(&info.trip_id).to_string()).or_default().push(info.clone());
        if let Some(b) = &info.block_id {
            let trips = self.blocks.entry((info.version, b.clone())).or_default();
            let pos =
                trips.partition_point(|t| (t.first_departure, &t.trip_id) < (info.first_departure, &info.trip_id));
            trips.insert(pos, info);
        }
    }
}

#[derive(Debug)]
pub struct TripIndex {
    pub tz: Tz,
    pub days: Vec<ServiceDay>,
    pub data: Arc<VersionData>,
    /// (version, station stop_id) -> (its stop group, the public stop ids of its stops): GTFS stations (location_type 1)
    pub stations: HashMap<(i32, String), (String, Vec<String>)>,
    /// version number -> core.feed_version.id
    pub version_ids: BTreeMap<i32, String>,
    /// version -> its services on `days`
    pub services: BTreeMap<i32, BTreeSet<String>>,
    pub loaded_for: NaiveDate,
}

impl TripIndex {
    pub fn day(&self, d: NaiveDate) -> Option<&ServiceDay> {
        self.days.iter().find(|x| x.date == d)
    }

    pub fn today(&self) -> Option<&ServiceDay> {
        self.day(self.loaded_for)
    }

    pub fn public_stop(&self, version: i32, stop_id: &str) -> Option<String> {
        if stop_id.is_empty() {
            return None;
        }
        self.data.stop_links.get(&(version, stop_id.to_string())).cloned()
    }

    pub fn has_route(&self, version: i32, route_id: &str) -> bool {
        self.data.routes.contains(&(version, route_id.to_string()))
    }

    /// An empty index (tests, or a feed whose versions serve none of the days).
    pub fn empty(tz: Tz, loaded_for: NaiveDate) -> Self {
        TripIndex {
            tz,
            days: Vec::new(),
            data: Arc::new(VersionData::default()),
            stations: HashMap::new(),
            version_ids: BTreeMap::new(),
            services: BTreeMap::new(),
            loaded_for,
        }
    }
}

/// Build the index for the service days around `now`; None if the feed has no ready version. A version's trips, stop
/// links and routes never change, so when the versions and their services are those of `previous`, its data is reused
/// and only the service days and the stations (their groups can change) are read again.
pub async fn load_index(
    c: &impl GenericClient,
    feed_id: &str,
    now: f64,
    day_start: DayStart,
    previous: Option<&TripIndex>,
) -> Result<Option<TripIndex>> {
    let Some(row) = c
        .query_opt(
            "SELECT timezone FROM core.feed_version WHERE feed_id = $1 AND status = 'ready' ORDER BY version DESC LIMIT 1",
            &[&feed_id],
        )
        .await?
    else {
        return Ok(None);
    };
    let tz: Tz = row.get::<_, Option<String>>(0).and_then(|t| t.parse().ok()).unwrap_or(Tz::UTC);
    let dates = nearby_dates(now, tz);
    let mut index = TripIndex::empty(tz, dates[1]);
    let rows = c
        .query(
            "SELECT s.service_date, fv.version, fv.id,
                    coalesce(array_agg(d.service_id) FILTER (WHERE d.service_id IS NOT NULL), '{}')
             FROM core.service_day_version s
             JOIN core.feed_version fv ON fv.id = s.feed_version_id
             LEFT JOIN core.service_date d ON d.feed_version_id = s.feed_version_id AND d.date = s.service_date
             WHERE s.feed_id = $1 AND s.service_date = ANY($2)
             GROUP BY s.service_date, fv.version, fv.id
             ORDER BY s.service_date",
            &[&feed_id, &dates.to_vec()],
        )
        .await?;
    for r in rows {
        let (d, v, fv_id, services): (NaiveDate, i32, String, Vec<String>) = (r.get(0), r.get(1), r.get(2), r.get(3));
        index.services.entry(v).or_default().extend(services.iter().cloned());
        index.days.push(ServiceDay {
            date: d,
            version: v,
            base: day_base(d, tz, day_start),
            services: Arc::new(services.into_iter().collect()),
        });
        index.version_ids.insert(v, fv_id);
    }
    if index.version_ids.is_empty() {
        return Ok(Some(index));
    }
    let ids: Vec<String> = index.version_ids.values().cloned().collect();
    let number: HashMap<String, i32> = index.version_ids.iter().map(|(v, id)| (id.clone(), *v)).collect();

    match previous {
        Some(p) if p.version_ids == index.version_ids && p.services == index.services => index.data = p.data.clone(),
        _ => index.data = Arc::new(load_version_data(c, &index).await?),
    }

    // a station's stops can be split over groups: it is shown as the group most of them are in
    let rows = c
        .query(
            "SELECT p.feed_version_id, p.stop_id, mode() WITHIN GROUP (ORDER BY s.stop_group_id),
                    array_agg(DISTINCT l.public_stop_id ORDER BY l.public_stop_id)
             FROM gtfs.stops p
             JOIN gtfs.stops c ON c.feed_version_id = p.feed_version_id AND c.parent_station = p.stop_id
             JOIN core.stop_link l ON l.feed_version_id = c.feed_version_id AND l.stop_id = c.stop_id
             JOIN core.stop s ON s.id = l.public_stop_id
             WHERE p.feed_version_id = ANY($1) AND p.location_type = 1
             GROUP BY p.feed_version_id, p.stop_id",
            &[&ids],
        )
        .await?;
    for r in rows {
        let fv_id: String = r.get(0);
        index.stations.insert((number[&fv_id], r.get(1)), (r.get(2), r.get(3)));
    }
    Ok(Some(index))
}

async fn load_version_data(c: &impl GenericClient, index: &TripIndex) -> Result<VersionData> {
    let mut data = VersionData::default();
    for (v, fv_id) in &index.version_ids {
        let Some(services) = index.services.get(v).filter(|s| !s.is_empty()) else {
            continue;
        };
        let services: Vec<&String> = services.iter().collect();
        let rows = c
            .query(
                "SELECT t.trip_id, t.service_id, t.route_id, t.direction_id::int, t.shape_id, t.trip_headsign, t.block_id,
                        r.route_short_name, r.route_type,
                        extract(epoch FROM min(coalesce(st.departure_time, st.arrival_time)))::bigint,
                        extract(epoch FROM max(coalesce(st.arrival_time, st.departure_time)))::bigint
                 FROM gtfs.trips t
                 JOIN gtfs.routes r ON r.feed_version_id = t.feed_version_id AND r.route_id = t.route_id
                 JOIN gtfs.stop_times st ON st.feed_version_id = t.feed_version_id AND st.trip_id = t.trip_id
                 -- one literal version on every table, so only its partitions are read
                 WHERE t.feed_version_id = $1 AND r.feed_version_id = $1 AND st.feed_version_id = $1
                   AND t.service_id = ANY($2)
                 GROUP BY t.feed_version_id, t.trip_id, r.route_short_name, r.route_type",
                &[fv_id, &services],
            )
            .await?;
        for r in rows {
            let (Some(first), Some(last)) = (r.get::<_, Option<i64>>(9), r.get::<_, Option<i64>>(10)) else {
                continue;
            };
            data.add_trip(TripInfo {
                version: *v,
                trip_id: r.get(0),
                service_id: r.get(1),
                route_id: r.get(2),
                direction_id: r.get(3),
                shape_id: r.get(4),
                headsign: r.get(5),
                block_id: r.get(6),
                route_short_name: r.get(7),
                route_type: r.get(8),
                first_departure: first,
                last_arrival: last,
            });
        }
    }
    let ids: Vec<String> = index.version_ids.values().cloned().collect();
    let number: HashMap<String, i32> = index.version_ids.iter().map(|(v, id)| (id.clone(), *v)).collect();
    for r in c
        .query(
            "SELECT feed_version_id, stop_id, public_stop_id FROM core.stop_link WHERE feed_version_id = ANY($1)",
            &[&ids],
        )
        .await?
    {
        let fv_id: String = r.get(0);
        data.stop_links.insert((number[&fv_id], r.get(1)), r.get(2));
    }
    for r in
        c.query("SELECT feed_version_id, route_id FROM gtfs.routes WHERE feed_version_id = ANY($1)", &[&ids]).await?
    {
        let fv_id: String = r.get(0);
        data.routes.insert((number[&fv_id], r.get(1)));
    }
    Ok(data)
}

/// The stop times of these (version, trip_id)s, one query per version.
pub async fn load_stop_times(
    c: &impl GenericClient,
    index: &TripIndex,
    keys: &[(i32, String)],
) -> Result<HashMap<(i32, String), Arc<Vec<StopTime>>>> {
    let mut by_version: BTreeMap<i32, Vec<&String>> = BTreeMap::new();
    for (v, trip_id) in keys {
        by_version.entry(*v).or_default().push(trip_id);
    }
    let mut out = HashMap::new();
    for (v, trip_ids) in by_version {
        let Some(fv_id) = index.version_ids.get(&v) else {
            continue;
        };
        let rows = c
            .query(
                "SELECT trip_id, stop_sequence::bigint, stop_id,
                        extract(epoch FROM arrival_time)::bigint, extract(epoch FROM departure_time)::bigint
                 FROM gtfs.stop_times WHERE feed_version_id = $1 AND trip_id = ANY($2)
                 ORDER BY trip_id, stop_sequence",
                &[fv_id, &trip_ids],
            )
            .await?;
        let mut grouped: HashMap<String, Vec<StopTime>> = HashMap::new();
        for r in rows {
            grouped.entry(r.get(0)).or_default().push(StopTime {
                sequence: r.get(1),
                stop_id: r.get::<_, Option<String>>(2).unwrap_or_default(),
                arrival: r.get(3),
                departure: r.get(4),
            });
        }
        for trip_id in trip_ids {
            out.insert((v, trip_id.clone()), Arc::new(grouped.remove(trip_id).unwrap_or_default()));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn service_suffix() {
        assert_eq!(super::strip_service_suffix("block_1_trip_9_service_4"), "block_1_trip_9");
        assert_eq!(super::strip_service_suffix("plain"), "plain");
    }
}
