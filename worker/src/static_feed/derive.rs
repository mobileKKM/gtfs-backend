//! Data derived from a loaded version: service dates, shape lines, stop directions and stop departures.

use anyhow::Result;
use chrono::NaiveDate;
use tokio_postgres::Transaction;

use super::FeedVersion;

/// Expand calendar + calendar_dates into core.service_date. Returns (rows, first date, last date).
pub async fn build_service_dates(tx: &Transaction<'_>, v: &str) -> Result<(u64, Option<NaiveDate>, Option<NaiveDate>)> {
    let rows = tx
        .execute(
            "INSERT INTO core.service_date (feed_version_id, date, service_id)
             SELECT $1, d, service_id FROM (
                 (
                     SELECT c.service_id, d::date AS d
                     FROM gtfs.calendar c, generate_series(c.start_date, c.end_date, interval '1 day') AS d
                     WHERE c.feed_version_id = $1
                       AND (ARRAY[c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday])
                           [extract(isodow FROM d)::int] = 1
                     UNION
                     SELECT service_id, date FROM gtfs.calendar_dates WHERE feed_version_id = $1 AND exception_type = 1
                 )
                 EXCEPT
                 SELECT service_id, date FROM gtfs.calendar_dates WHERE feed_version_id = $1 AND exception_type = 2
             ) s",
            &[&v],
        )
        .await?;
    let row = tx
        .query_one(
            "UPDATE core.feed_version SET first_service_date = r.lo, last_service_date = r.hi
             FROM (SELECT min(date) AS lo, max(date) AS hi FROM core.service_date WHERE feed_version_id = $1) r
             WHERE id = $1 RETURNING r.lo, r.hi",
            &[&v],
        )
        .await?;
    Ok((rows, row.get(0), row.get(1)))
}

pub async fn build_shape_lines(tx: &Transaction<'_>, v: &str) -> Result<u64> {
    Ok(tx
        .execute(
            "INSERT INTO core.shape_line (feed_version_id, shape_id, geom)
             SELECT feed_version_id, shape_id, ST_MakeLine(geom ORDER BY shape_pt_sequence)
             FROM gtfs.shapes WHERE feed_version_id = $1
             GROUP BY feed_version_id, shape_id HAVING count(*) >= 2",
            &[&v],
        )
        .await?)
}

/// core.stop_direction: for every departure (pickup allowed, not the trip's last stop), the next stop group the trip
/// reaches; a following stop of the same group is skipped (up to two of them). Needs the version's core.stop_link.
pub async fn build_stop_directions(tx: &Transaction<'_>, v: &str) -> Result<u64> {
    Ok(tx
        .execute(
            "INSERT INTO core.stop_direction (feed_version_id, public_stop_id, next_group_id, route_id, headsign, trips)
             SELECT $1, pid, next_gid, route_id, headsign, count(*)
             FROM (
                 SELECT pid, route_id, headsign,
                        CASE WHEN n1 <> gid THEN n1 WHEN n2 <> gid THEN n2 WHEN n3 <> gid THEN n3 END AS next_gid
                 FROM (
                     SELECT l.public_stop_id AS pid, p.stop_group_id AS gid, t.route_id,
                            coalesce(st.stop_headsign, t.trip_headsign, '') AS headsign,
                            coalesce(st.pickup_type, 0) AS pickup,
                            lead(p.stop_group_id, 1) OVER w AS n1, lead(p.stop_group_id, 2) OVER w AS n2,
                            lead(p.stop_group_id, 3) OVER w AS n3
                     FROM gtfs.stop_times st
                     JOIN gtfs.trips t ON t.feed_version_id = $1 AND t.trip_id = st.trip_id
                     JOIN core.stop_link l ON l.feed_version_id = $1 AND l.stop_id = st.stop_id
                     JOIN core.stop p ON p.id = l.public_stop_id
                     WHERE st.feed_version_id = $1
                     WINDOW w AS (PARTITION BY st.trip_id ORDER BY st.stop_sequence)
                 ) s
                 WHERE pickup <> 1
             ) d
             WHERE next_gid IS NOT NULL
             GROUP BY pid, next_gid, route_id, headsign",
            &[&v],
        )
        .await?)
}

/// The version's partition of core.stop_departure: every departure a board can show (pickup allowed, not the trip's
/// last stop: that is an arrival), with a departure time, at a stop linked to ours. Created standalone, filled and
/// attached, like the GTFS partitions (readers keep reading). Needs the version's core.stop_link.
pub async fn build_stop_departures(tx: &Transaction<'_>, fv: &FeedVersion) -> Result<u64> {
    let part = format!("core.\"{}\"", fv.partition("stop_departure"));
    tx.batch_execute(&format!(
        "CREATE TABLE {part} (LIKE core.stop_departure INCLUDING DEFAULTS, CONSTRAINT version_chk CHECK (feed_version_id = '{}'))",
        fv.id
    ))
    .await?;
    let rows = tx
        .execute(
            &format!(
                // The join is planned on its own (MATERIALIZED), then sorted: asked for sorted output, the planner may
                // walk stop_link in public_stop_id order and scan every departure once per stop (seen 2026-11-27 on
                // kmk_a: 4.5 billion comparisons, 6 minutes instead of 3 s)
                "WITH d AS MATERIALIZED (
                     SELECT l.public_stop_id, t.service_id, extract(epoch FROM s.departure_time)::int AS departure_s, s.trip_id,
                            s.stop_sequence, t.route_id, coalesce(s.stop_headsign, t.trip_headsign) AS headsign,
                            coalesce(s.pickup_type, 0) IN (2, 3) AS on_demand
                     FROM (
                         SELECT st.trip_id, st.stop_sequence, st.stop_id, st.departure_time, st.pickup_type, st.stop_headsign,
                                lead(st.stop_sequence) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_sequence
                         FROM gtfs.stop_times st WHERE st.feed_version_id = $1
                     ) s
                     JOIN core.stop_link l ON l.feed_version_id = $1 AND l.stop_id = s.stop_id
                     JOIN gtfs.trips t ON t.feed_version_id = $1 AND t.trip_id = s.trip_id
                     WHERE s.next_sequence IS NOT NULL AND coalesce(s.pickup_type, 0) <> 1 AND s.departure_time IS NOT NULL
                 )
                 INSERT INTO {part} (feed_version_id, public_stop_id, service_id, departure_s, trip_id, stop_sequence,
                                     route_id, headsign, on_demand)
                 SELECT $1, public_stop_id, service_id, departure_s, trip_id, stop_sequence, route_id, headsign, on_demand
                 FROM d
                 -- a board's rows end up next to each other on disk
                 ORDER BY public_stop_id, service_id, departure_s"
            ),
            &[&fv.id],
        )
        .await?;
    tx.batch_execute(&format!(
        "ALTER TABLE core.stop_departure ATTACH PARTITION {part} FOR VALUES IN ('{}'); ANALYZE {part}",
        fv.id
    ))
    .await?;
    Ok(rows)
}
