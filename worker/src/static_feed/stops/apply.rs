//! Matching a new version's stops and writing the result: stop groups, stops, stop links and their state.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures_util::SinkExt;
use tokio_postgres::Transaction;
use tracing::{error, info, warn};

use super::osm::load_osm_poles;
use super::plan::{plan_matches, representatives};
use super::shared::{SharedInputs, SharedPlan, plan_shared};
use super::{Group, NewStop, PublicStop, StopRules, normalize_stop};
use crate::util::{mode_sql, new_public_id, slugify, stop_slug};

pub struct MatchStats {
    pub stops: usize,
    pub methods: BTreeMap<String, usize>,
    pub errors: Vec<String>,
}

fn public_stop(r: &tokio_postgres::Row, linked: bool) -> PublicStop {
    PublicStop {
        id: r.get(0),
        group_id: r.get(1),
        code: r.get(2),
        name: r.get(3),
        platform_code: r.get(4),
        lat: r.get(5),
        lon: r.get(6),
        active: r.get(7),
        linked_elsewhere: linked && r.get::<_, bool>(8),
    }
}

fn group(r: &tokio_postgres::Row) -> Group {
    Group { id: r.get(0), code: r.get(1), name: r.get(2), lat: r.get(3), lon: r.get(4) }
}

/// Match the stops of version `v`, write core.stop_link and refresh stops/groups. Stops first try to attach to the
/// stops of the `share_with` namespaces (never modified by this feed); the rest are matched in the feed's own namespace.
pub async fn apply_matches(tx: &Transaction<'_>, feed_id: &str, v: &str, rules: &StopRules) -> Result<MatchStats> {
    let namespace = rules.namespace.as_str();
    let rows = tx
        .query(
            "SELECT s.stop_id, NULLIF(btrim(s.stop_code), ''), coalesce(s.stop_name, s.stop_id),
                    NULLIF(btrim(s.platform_code), ''), s.stop_lat, s.stop_lon,
                    CASE WHEN p.stop_id IS NOT NULL THEN 'station:' || coalesce(NULLIF(btrim(p.stop_code), ''), p.stop_id) END,
                    p.stop_name
             FROM gtfs.stops s
             LEFT JOIN gtfs.stops p ON p.feed_version_id = s.feed_version_id AND p.stop_id = s.parent_station AND p.location_type = 1
             WHERE s.feed_version_id = $1 AND coalesce(s.location_type, 0) = 0
               AND s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL
             ORDER BY s.stop_id",
            &[&v],
        )
        .await?;
    let pattern = rules.stop_code_pattern.as_ref();
    let all_stops: Vec<NewStop> = rows
        .iter()
        .map(|r| {
            normalize_stop(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), pattern)
        })
        .collect();

    let mut shared_plan = SharedPlan::default();
    if !rules.share_with.is_empty() {
        let shared: Vec<PublicStop> = tx
            .query(
                "SELECT id, stop_group_id, current_code, name, platform_code, ST_Y(geom), ST_X(geom), active
                 FROM core.stop WHERE code_namespace = ANY($1) AND active ORDER BY id",
                &[&rules.share_with],
            )
            .await?
            .iter()
            .map(|r| public_stop(r, false))
            .collect();
        let shared_groups: Vec<Group> = tx
            .query(
                "SELECT id, current_code, name, ST_Y(geom), ST_X(geom) FROM core.stop_group
                 WHERE code_namespace = ANY($1) AND active ORDER BY id",
                &[&rules.share_with],
            )
            .await?
            .iter()
            .map(group)
            .collect();
        let feed_modes = tx
            .query(&format!("SELECT DISTINCT {} FROM gtfs.routes WHERE feed_version_id = $1", mode_sql()), &[&v])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let shared_versions = namespace_versions(tx, &rules.share_with).await?;
        let inputs = SharedInputs {
            stop_bearing: stop_bearings(tx, &[v.to_string()]).await?.into_iter().map(|((_, s), b)| (s, b)).collect(),
            shared_bearing: public_stop_bearings(tx, &shared_versions).await?,
            shared_modes: served_modes(tx, &shared_versions).await?, // the shared feeds' own service, not our links
            feed_modes,
            stop_next: stop_next_stops(tx, v).await?,
            shared_next: public_stop_next_groups(tx, &shared_versions).await?,
            osm: match &rules.osm_stops_file {
                Some(path) => load_osm_poles(path)?,
                None => Vec::new(),
            },
        };
        shared_plan = tokio::task::block_in_place(|| plan_shared(&all_stops, &shared, &shared_groups, &inputs));
        for w in &shared_plan.warnings {
            info!(feed = feed_id, msg = %w, "stop_share");
        }
        info!(feed = feed_id, methods = ?shared_plan.methods, "stop_share_summary");
    }
    let stops: Vec<NewStop> =
        all_stops.iter().filter(|s| !shared_plan.links.contains_key(&s.stop_id)).cloned().collect();

    let others: Vec<String> = current_versions(tx, feed_id).await?;
    let existing: Vec<PublicStop> = tx
        .query(
            "SELECT p.id, p.stop_group_id, p.current_code, p.name, p.platform_code, ST_Y(p.geom), ST_X(p.geom), p.active,
                    EXISTS (SELECT 1 FROM core.stop_link l WHERE l.public_stop_id = p.id AND l.feed_version_id = ANY($1))
             FROM core.stop p WHERE p.code_namespace = $2 ORDER BY p.id",
            &[&others, &namespace],
        )
        .await?
        .iter()
        .map(|r| public_stop(r, true))
        .collect();
    let groups: Vec<Group> = tx
        .query(
            "SELECT id, current_code, name, ST_Y(geom), ST_X(geom) FROM core.stop_group WHERE code_namespace = $1 ORDER BY id",
            &[&namespace],
        )
        .await?
        .iter()
        .map(group)
        .collect();

    let plan = tokio::task::block_in_place(|| plan_matches(&stops, &existing, &groups));
    for w in &plan.warnings {
        warn!(feed = feed_id, msg = %w, "stop_match");
    }
    for e in &plan.errors {
        error!(feed = feed_id, msg = %e, "stop_match");
    }

    let mut taken: HashSet<String> =
        tx.query("SELECT id FROM core.public_id", &[]).await?.iter().map(|r| r.get(0)).collect();
    let reps = representatives(&stops);

    // groups first (stops reference them)
    let mut group_id_for_key: HashMap<String, String> = HashMap::new();
    let mut by_gkey: Vec<(String, Vec<&NewStop>)> = Vec::new();
    for (_, s) in &reps {
        let key = s.group_key();
        match by_gkey.iter_mut().find(|(k, _)| *k == key) {
            Some((_, members)) => members.push(s),
            None => by_gkey.push((key, vec![s])),
        }
    }
    for (gkey, members) in &by_gkey {
        if let Some(shared) = shared_plan.group_for_key.get(gkey) {
            group_id_for_key.insert(gkey.clone(), shared.clone()); // a shared group: joined, never renamed
            continue;
        }
        let name = most_common_name(members);
        let code = (!gkey.starts_with("name:")).then_some(gkey.as_str());
        let gid = match plan.group_for_key.get(gkey).cloned().flatten() {
            Some(gid) => {
                tx.execute(
                    "UPDATE core.stop_group SET slug = $1, current_code = $2, name = $3 WHERE id = $4",
                    &[&slugify(name), &code, &name, &gid],
                )
                .await?;
                gid
            }
            None => {
                let gid = allocate_id(tx, "stop_group", &mut taken).await?;
                tx.execute(
                    "INSERT INTO core.stop_group (id, slug, code_namespace, current_code, name) VALUES ($1, $2, $3, $4, $5)",
                    &[&gid, &slugify(name), &namespace, &code, &name],
                )
                .await?;
                gid
            }
        };
        group_id_for_key.insert(gkey.clone(), gid);
    }

    // stops: deactivate displaced holders of a code first (the active-code index is unique)
    let mut public_ids: HashMap<String, String> = HashMap::new();
    for (key, s) in &reps {
        let pid = plan.stop_for_key.get(key).cloned().flatten();
        let gid = &group_id_for_key[&s.group_key()];
        if let Some(code) = &s.code {
            tx.execute(
                "UPDATE core.stop SET active = false
                 WHERE code_namespace = $1 AND current_code = $2 AND active AND id IS DISTINCT FROM $3",
                &[&namespace, code, &pid],
            )
            .await?;
        }
        let slug = stop_slug(&s.name, s.platform_code.as_deref());
        let pid = match pid {
            None => {
                let pid = allocate_id(tx, "stop", &mut taken).await?;
                tx.execute(
                    "INSERT INTO core.stop (id, stop_group_id, slug, code_namespace, current_code, name, platform_code, geom)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, ST_SetSRID(ST_MakePoint($8, $9), 4326))",
                    &[&pid, gid, &slug, &namespace, &s.code, &s.name, &s.platform_code, &s.lon, &s.lat],
                )
                .await?;
                pid
            }
            Some(pid) => {
                tx.execute(
                    "UPDATE core.stop SET stop_group_id = $1, slug = $2, current_code = $3, name = $4, platform_code = $5,
                         geom = ST_SetSRID(ST_MakePoint($6, $7), 4326), active = true
                     WHERE id = $8",
                    &[gid, &slug, &s.code, &s.name, &s.platform_code, &s.lon, &s.lat, &pid],
                )
                .await?;
                pid
            }
        };
        public_ids.insert(key.clone(), pid);
    }

    let mut buf = BytesMut::new();
    let links = stops
        .iter()
        .map(|s| (s.stop_id.as_str(), public_ids[&s.pole_key()].as_str()))
        .chain(shared_plan.links.iter().map(|(a, b)| (a.as_str(), b.as_str())));
    for (stop_id, pid) in links {
        // ids are plain text (feed ids from CSV may hold tabs in theory: escaped as COPY wants)
        for (i, value) in [v, stop_id, pid].into_iter().enumerate() {
            if i > 0 {
                buf.put_u8(b'\t');
            }
            for b in value.bytes() {
                match b {
                    b'\\' => buf.put_slice(b"\\\\"),
                    b'\t' => buf.put_slice(b"\\t"),
                    b'\n' => buf.put_slice(b"\\n"),
                    b'\r' => buf.put_slice(b"\\r"),
                    b => buf.put_u8(b),
                }
            }
        }
        buf.put_u8(b'\n');
    }
    let sink = tx.copy_in("COPY core.stop_link (feed_version_id, stop_id, public_stop_id) FROM STDIN").await?;
    futures_util::pin_mut!(sink);
    sink.send(buf.freeze()).await?;
    sink.finish().await?;

    // the rows written above are uncommitted, so autovacuum can't have analyzed them; the queries below and the stop
    // directions join them with millions of stop_times and would be planned blind
    tx.batch_execute("ANALYZE core.stop_link, core.stop").await?;
    let mut current = current_versions(tx, feed_id).await?;
    current.push(v.to_string());
    refresh_stop_state(tx, &current).await?;

    let mut methods: BTreeMap<String, usize> = BTreeMap::new();
    for m in plan.method_for_key.values() {
        *methods.entry(m.to_string()).or_default() += 1;
    }
    for (k, n) in &shared_plan.methods {
        methods.insert(format!("shared_{k}"), *n);
    }
    Ok(MatchStats { stops: reps.len(), methods, errors: plan.errors })
}

fn most_common_name<'a>(members: &[&'a NewStop]) -> &'a str {
    let mut counts: Vec<(&str, usize)> = Vec::new();
    for m in members {
        match counts.iter_mut().find(|(k, _)| *k == m.group_name()) {
            Some((_, n)) => *n += 1,
            None => counts.push((m.group_name(), 1)),
        }
    }
    let top = counts.iter().map(|(_, n)| *n).max().unwrap_or(0);
    counts.into_iter().find(|(_, n)| *n == top).map(|(k, _)| k).unwrap_or("")
}

async fn allocate_id(tx: &Transaction<'_>, kind: &str, taken: &mut HashSet<String>) -> Result<String> {
    loop {
        let id = new_public_id();
        if taken.insert(id.clone()) {
            tx.execute("INSERT INTO core.public_id (id, kind) VALUES ($1, $2)", &[&id, &kind]).await?;
            return Ok(id);
        }
    }
}

/// Active flags, modes and group centroids from the current version of every feed.
pub async fn refresh_stop_state(tx: &Transaction<'_>, current: &[String]) -> Result<()> {
    tx.execute(
        "WITH linked AS (SELECT DISTINCT public_stop_id FROM core.stop_link WHERE feed_version_id = ANY($1))
         UPDATE core.stop p
         SET active = (l.public_stop_id IS NOT NULL),
             last_seen = CASE WHEN l.public_stop_id IS NOT NULL THEN now() ELSE p.last_seen END
         FROM core.stop p2 LEFT JOIN linked l ON l.public_stop_id = p2.id
         WHERE p.id = p2.id",
        &[&current],
    )
    .await?;
    let mode = mode_sql();
    tx.execute(
        &format!(
            "WITH served AS (
                 SELECT DISTINCT st.feed_version_id, st.stop_id, t.route_id
                 FROM gtfs.stop_times st
                 JOIN gtfs.trips t ON t.feed_version_id = st.feed_version_id AND t.trip_id = st.trip_id
                 WHERE st.feed_version_id = ANY($1)
             ), modes AS (
                 SELECT l.public_stop_id, array_agg(DISTINCT {mode} ORDER BY {mode}) AS modes
                 FROM served s
                 JOIN gtfs.routes r ON r.feed_version_id = s.feed_version_id AND r.route_id = s.route_id
                 JOIN core.stop_link l ON l.feed_version_id = s.feed_version_id AND l.stop_id = s.stop_id
                 GROUP BY l.public_stop_id
             )
             UPDATE core.stop p SET modes = coalesce(m.modes, '{{}}')
             FROM core.stop p2 LEFT JOIN modes m ON m.public_stop_id = p2.id
             WHERE p.id = p2.id AND p.active"
        ),
        &[&current],
    )
    .await?;
    tx.batch_execute(
        "UPDATE core.stop_group g
         SET active = s.any_active,
             last_seen = CASE WHEN s.any_active THEN now() ELSE g.last_seen END,
             geom = coalesce(s.active_centroid, s.centroid, g.geom)
         FROM (
             SELECT g2.id, coalesce(bool_or(p.active), false) AS any_active,
                    ST_Centroid(ST_Collect(p.geom) FILTER (WHERE p.active)) AS active_centroid,
                    ST_Centroid(ST_Collect(p.geom)) AS centroid
             FROM core.stop_group g2 LEFT JOIN core.stop p ON p.stop_group_id = g2.id
             GROUP BY g2.id
         ) s
         WHERE g.id = s.id",
    )
    .await?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------- reads

/// Direction of travel (degrees from north) through each feed stop: previous stop -> next stop, averaged over trips.
pub async fn stop_bearings(tx: &Transaction<'_>, versions: &[String]) -> Result<HashMap<(String, String), f64>> {
    if versions.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = tx
        .query(
            "WITH seq AS (
                 SELECT st.feed_version_id AS v, st.stop_id,
                        coalesce(lag(s.stop_lat) OVER w, s.stop_lat) AS plat, coalesce(lag(s.stop_lon) OVER w, s.stop_lon) AS plon,
                        coalesce(lead(s.stop_lat) OVER w, s.stop_lat) AS nlat, coalesce(lead(s.stop_lon) OVER w, s.stop_lon) AS nlon
                 FROM gtfs.stop_times st
                 JOIN gtfs.stops s ON s.feed_version_id = st.feed_version_id AND s.stop_id = st.stop_id
                 WHERE st.feed_version_id = ANY($1)
                 WINDOW w AS (PARTITION BY st.feed_version_id, st.trip_id ORDER BY st.stop_sequence)
             ), vec AS (
                 SELECT v, stop_id, (nlon - plon) * cos(radians(nlat)) AS dx, nlat - plat AS dy FROM seq
                 WHERE (nlat, nlon) <> (plat, plon)
             )
             SELECT v, stop_id, degrees(atan2(sum(dx / sqrt(dx*dx + dy*dy)), sum(dy / sqrt(dx*dx + dy*dy))))
             FROM vec GROUP BY v, stop_id",
            &[&versions],
        )
        .await?;
    Ok(rows
        .iter()
        .filter_map(|r| r.get::<_, Option<f64>>(2).map(|b| ((r.get(0), r.get(1)), (b + 360.0) % 360.0)))
        .collect())
}

/// Direction of travel per stop of ours, from all its linked feed stops in the given versions.
async fn public_stop_bearings(tx: &Transaction<'_>, versions: &[String]) -> Result<HashMap<String, f64>> {
    if versions.is_empty() {
        return Ok(HashMap::new());
    }
    let by_stop = stop_bearings(tx, versions).await?;
    let rows = tx
        .query(
            "SELECT feed_version_id, stop_id, public_stop_id FROM core.stop_link WHERE feed_version_id = ANY($1)",
            &[&versions],
        )
        .await?;
    let mut vectors: HashMap<String, (f64, f64)> = HashMap::new();
    for r in rows {
        if let Some(b) = by_stop.get(&(r.get(0), r.get(1))) {
            let v = vectors.entry(r.get(2)).or_insert((0.0, 0.0));
            v.0 += b.to_radians().sin();
            v.1 += b.to_radians().cos();
        }
    }
    Ok(vectors
        .into_iter()
        .filter(|(_, (x, y))| *x != 0.0 || *y != 0.0)
        .map(|(pid, (x, y))| (pid, (x.atan2(y).to_degrees() + 360.0) % 360.0))
        .collect())
}

/// Modes the given versions' own trips serve at each stop of ours (not core.stop.modes: that counts every feed linked
/// to the stop, so a bus feed wrongly linked to a tram pole would make it look like a bus pole).
async fn served_modes(tx: &Transaction<'_>, versions: &[String]) -> Result<HashMap<String, HashSet<String>>> {
    if versions.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = tx
        .query(
            &format!(
                "SELECT l.public_stop_id, array_agg(DISTINCT {})
                 FROM (
                     SELECT DISTINCT st.feed_version_id, st.stop_id, t.route_id
                     FROM gtfs.stop_times st
                     JOIN gtfs.trips t ON t.feed_version_id = st.feed_version_id AND t.trip_id = st.trip_id
                     WHERE st.feed_version_id = ANY($1)
                 ) s
                 JOIN gtfs.routes r ON r.feed_version_id = s.feed_version_id AND r.route_id = s.route_id
                 JOIN core.stop_link l ON l.feed_version_id = s.feed_version_id AND l.stop_id = s.stop_id
                 GROUP BY l.public_stop_id",
                mode_sql()
            ),
            &[&versions],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get::<_, Vec<String>>(1).into_iter().collect())).collect())
}

/// Stops a version's trips go to right after each stop.
async fn stop_next_stops(tx: &Transaction<'_>, v: &str) -> Result<HashMap<String, HashSet<String>>> {
    let rows = tx
        .query(
            "SELECT DISTINCT stop_id, nxt FROM (
                 SELECT stop_id, lead(stop_id) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS nxt
                 FROM gtfs.stop_times WHERE feed_version_id = $1
             ) t WHERE nxt IS NOT NULL AND nxt <> stop_id",
            &[&v],
        )
        .await?;
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for r in rows {
        out.entry(r.get(0)).or_default().insert(r.get(1));
    }
    Ok(out)
}

/// Groups the trips of the given versions go to right after each stop of ours.
async fn public_stop_next_groups(
    tx: &Transaction<'_>,
    versions: &[String],
) -> Result<HashMap<String, HashSet<String>>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    if versions.is_empty() {
        return Ok(out);
    }
    let rows = tx
        .query(
            "SELECT DISTINCT public_stop_id, next_group_id FROM core.stop_direction WHERE feed_version_id = ANY($1)",
            &[&versions],
        )
        .await?;
    for r in rows {
        out.entry(r.get(0)).or_default().insert(r.get(1));
    }
    Ok(out)
}

/// The newest ready version of every active feed in these stop namespaces (by number: `v10` sorts before `v9`).
pub async fn namespace_versions(c: &impl tokio_postgres::GenericClient, namespaces: &[String]) -> Result<Vec<String>> {
    Ok(c.query(
        "SELECT DISTINCT ON (fv.feed_id) fv.id
             FROM core.feed_version fv JOIN core.feed f ON f.feed_id = fv.feed_id AND f.active
             WHERE fv.status = 'ready' AND f.stop_namespace = ANY($1)
             ORDER BY fv.feed_id, fv.version DESC",
        &[&namespaces],
    )
    .await?
    .iter()
    .map(|r| r.get(0))
    .collect())
}

/// The newest ready version of every other active feed.
pub async fn current_versions(c: &impl tokio_postgres::GenericClient, feed_id: &str) -> Result<Vec<String>> {
    Ok(c.query(
        "SELECT DISTINCT ON (fv.feed_id) fv.id
             FROM core.feed_version fv JOIN core.feed f ON f.feed_id = fv.feed_id AND f.active
             WHERE fv.status = 'ready' AND fv.feed_id <> $1
             ORDER BY fv.feed_id, fv.version DESC",
        &[&feed_id],
    )
    .await?
    .iter()
    .map(|r| r.get(0))
    .collect())
}
