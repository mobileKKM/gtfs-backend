//! Attaching a feed's stops to another namespace's stops and groups (MLD buses on ZTP's poles). No database.
//!
//! 0. OpenStreetMap: the nearest OSM bus pole within 60 m, when its ref says which pole it is. A ZTP code (`889-03`) or
//!    a bay (`271-01-06`) that a shared stop is on links the stop to that stop, however far the coordinates are apart. A
//!    ZTP code no shared stop has (OSM 802-07: a pole ZTP doesn't publish), a bay no shared stop is on, or the
//!    operator's own code keeps the stop's own pole: no rule below may attach it. Only the operator's own code names the
//!    pole at any distance; the other two need the OSM pole within 20 m. Without such a pole the rules below decide.
//! 1. Same pole: a shared stop within 8 m that the shared feeds serve with the feed's mode. Equally close candidates are
//!    decided by direction, then by the lowest code.
//! 2. Logical stop: the group most of a station's attached poles belong to; else a shared group with the same name
//!    within 300 m (of its nearest pole), or a similar name within 150 m.
//! 3. Pole within the logical stop: the nearest stop of that group within 60 m served in the same direction (within
//!    90°) by the same modes. Beyond 30 m the pole must also lie within 40 m along the line (further up the road is the
//!    next stop) and lead to the same next logical stop. A pole is also out when OSM has the stop and the pole each on
//!    an OSM pole of their own, more than 25 m apart. Stops with no such pole keep their own one.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

use super::osm::OsmPole;
use super::{Group, NameMatch, NewStop, PublicStop, along_track_m, angle_diff, distance_m, names_match};

/// The nearest OSM bus pole this close names the stop's pole by its ref (MLD coordinates are often off).
const OSM_LINK_M: f64 = 60.0;
/// A stop or pole this close to an OSM pole is on that OSM pole.
const OSM_SNAP_M: f64 = 10.0;
/// OSM poles further apart are different poles (an OSM platform and its stop_position are within ~20 m).
const OSM_SAME_POLE_M: f64 = 25.0;
/// An OSM pole of a code ZTP doesn't have claims a stop as its own pole only this close (GPS can be off).
const OSM_OWN_M: f64 = 20.0;
/// An OSM pole this close to a shared stop is that stop's.
const OSM_ON_STOP_M: f64 = 25.0;
/// The pole a ref names must be this close to the OSM pole, or the ref is a typo.
const OSM_REF_MAX_M: f64 = 100.0;
/// This close it is the same pole, whatever the names say.
const SAME_POLE_M: f64 = 8.0;
/// A second candidate this close to the best one: decide by direction or don't guess.
const SHARED_AMBIGUOUS_M: f64 = 3.0;
const GROUP_EXACT_NAME_M: f64 = 300.0;
const GROUP_SIMILAR_NAME_M: f64 = 150.0;
/// Inside a known shared stop: the furthest a pole can be and still be the same one.
const IN_GROUP_POLE_M: f64 = 60.0;
/// A pole further up or down the line than this is the next or previous stop, not this one.
const ALONG_TRACK_M: f64 = 40.0;
/// Up to here direction is enough; further off the pole must also lead to the same next stop.
const CONFIRMED_POLE_M: f64 = 30.0;
/// Travel directions further apart than this: the other side of the road.
const MAX_DIRECTION_DEG: f64 = 90.0;

/// OSM `GGG-PP-NN`: a bay of a bus station.
static BAY_REF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d+-\d+-\d+$").unwrap());

#[derive(Debug, Default)]
pub struct SharedPlan {
    /// stop_id -> shared stop id
    pub links: BTreeMap<String, String>,
    /// group key -> shared group id
    pub group_for_key: BTreeMap<String, String>,
    pub methods: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
}

impl SharedPlan {
    fn count(&mut self, method: &str) {
        *self.methods.entry(method.to_string()).or_default() += 1;
    }

    #[cfg(test)]
    pub fn method(&self, method: &str) -> usize {
        self.methods.get(method).copied().unwrap_or(0)
    }
}

#[derive(Default)]
pub struct SharedInputs {
    pub stop_bearing: HashMap<String, f64>,
    pub shared_bearing: HashMap<String, f64>,
    pub shared_modes: HashMap<String, HashSet<String>>,
    pub feed_modes: HashSet<String>,
    /// feed stop -> the feed stops its trips go to next
    pub stop_next: HashMap<String, HashSet<String>>,
    /// shared stop -> the groups the shared feeds' trips go to next
    pub shared_next: HashMap<String, HashSet<String>>,
    pub osm: Vec<OsmPole>,
}

fn cell(lat: f64, lon: f64) -> (i64, i64) {
    ((lat * 1000.0) as i64, (lon * 1000.0) as i64) // ~110 m x 70 m
}

/// Python's round(): halves to even.
fn round_even(x: f64) -> i64 {
    x.round_ties_even() as i64
}

pub fn plan_shared(
    stops: &[NewStop],
    shared: &[PublicStop],
    shared_groups: &[Group],
    inp: &SharedInputs,
) -> SharedPlan {
    let mut plan = SharedPlan::default();
    let direction = |s: &NewStop, p: &PublicStop| -> Option<f64> {
        Some(angle_diff(*inp.stop_bearing.get(&s.stop_id)?, *inp.shared_bearing.get(&p.id)?))
    };
    let same_mode = |p: &PublicStop| match inp.shared_modes.get(&p.id) {
        Some(modes) if !inp.feed_modes.is_empty() && !modes.is_empty() => !modes.is_disjoint(&inp.feed_modes),
        _ => true,
    };
    let pos = |p: &PublicStop| (p.lat.unwrap(), p.lon.unwrap());
    let located = |p: &&PublicStop| p.active && p.lat.is_some() && p.lon.is_some();

    let mut cells: HashMap<(i64, i64), Vec<&PublicStop>> = HashMap::new();
    for p in shared.iter().filter(located) {
        cells.entry(cell(p.lat.unwrap(), p.lon.unwrap())).or_default().push(p);
    }
    let same_pole = |s: &NewStop| -> Vec<(f64, &PublicStop)> {
        let (cx, cy) = cell(s.lat, s.lon);
        let mut out = Vec::new();
        for dx in -1..=1 {
            for dy in -1..=1 {
                for p in cells.get(&(cx + dx, cy + dy)).into_iter().flatten() {
                    let (lat, lon) = pos(p);
                    let d = distance_m(s.lat, s.lon, lat, lon);
                    if d <= SAME_POLE_M && same_mode(p) {
                        out.push((d, *p));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        out
    };
    // the nearest candidate; equally close ones by direction of travel, else the lowest code. Two codes on one spot
    // (a turn-around) are one pole, so an undecided tie is still that pole.
    let pick = |s: &NewStop, cands: &[(f64, &PublicStop)]| -> String {
        let close: Vec<(f64, &PublicStop)> =
            cands.iter().copied().filter(|(d, _)| d - cands[0].0 < SHARED_AMBIGUOUS_M).collect();
        if close.len() == 1 {
            return close[0].1.id.clone();
        }
        let same_way: Vec<&PublicStop> = close
            .iter()
            .filter(|(_, p)| direction(s, p).is_some_and(|d| d <= MAX_DIRECTION_DEG))
            .map(|(_, p)| *p)
            .collect();
        if same_way.len() == 1 {
            return same_way[0].id.clone();
        }
        let mut best = close[0];
        for c in &close[1..] {
            let key = |(d, p): &(f64, &PublicStop)| (round_even(*d), p.code.clone().unwrap_or_default(), p.id.clone());
            if key(c) < key(&best) {
                best = *c;
            }
        }
        best.1.id.clone()
    };

    let mut by_code: HashMap<&str, &PublicStop> = HashMap::new();
    for p in shared.iter().filter(|p| p.active) {
        if let Some(c) = &p.code {
            by_code.insert(c, p); // the last one wins, as a dict comprehension
        }
    }
    let mut osm_cells: HashMap<(i64, i64), Vec<&OsmPole>> = HashMap::new();
    for o in inp.osm.iter().filter(|o| o.bus) {
        osm_cells.entry(cell(o.lat, o.lon)).or_default().push(o);
    }
    let osm_near = |lat: f64, lon: f64, radius: f64| -> Vec<(f64, &OsmPole)> {
        let (cx, cy) = cell(lat, lon);
        let mut found = Vec::new();
        for dx in -1..=1 {
            for dy in -1..=1 {
                for o in osm_cells.get(&(cx + dx, cy + dy)).into_iter().flatten() {
                    let d = distance_m(lat, lon, o.lat, o.lon);
                    if d <= radius {
                        found.push((d, *o));
                    }
                }
            }
        }
        found.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        found
    };

    // a bay of a bus station is a pole of its own; a shared stop is on a bay when that OSM pole is within 25 m of it
    let mut by_bay: HashMap<&str, &PublicStop> = HashMap::new();
    if !inp.osm.is_empty() {
        for p in shared.iter().filter(located) {
            let (lat, lon) = pos(p);
            if let Some((_, o)) = osm_near(lat, lon, OSM_ON_STOP_M)
                .into_iter()
                .find(|(_, o)| o.r#ref.as_deref().is_some_and(|r| BAY_REF.is_match(r)))
            {
                by_bay.entry(o.r#ref.as_deref().unwrap()).or_insert(p);
            }
        }
    }

    let mut snapped: HashMap<String, Option<&OsmPole>> = HashMap::new();
    let mut osm_pole_of = |key: String, lat: f64, lon: f64| -> Option<&OsmPole> {
        *snapped.entry(key).or_insert_with(|| osm_near(lat, lon, OSM_SNAP_M).first().map(|(_, o)| *o))
    };

    // Some(Some(p)): this OSM pole is shared stop p; Some(None): none of them; None: no say
    let osm_says = |s: &NewStop, o: &OsmPole| -> Option<Option<&PublicStop>> {
        let r = o.r#ref.as_deref()?;
        let p = if let Some(code) = o.code() {
            by_code.get(code.as_str()).copied()
        } else if BAY_REF.is_match(r) {
            by_bay.get(r).copied()
        } else if Some(r) == s.code.as_deref() {
            None // the operator's own code: OSM maps this as the stop's own pole
        } else {
            return None; // a ref of some other scheme says nothing about the shared poles
        };
        if let Some(p) = p {
            match (p.lat, p.lon) {
                (Some(lat), Some(lon)) if distance_m(o.lat, o.lon, lat, lon) <= OSM_REF_MAX_M => {}
                _ => return None, // the ref is wrong or the pole moved
            }
        }
        Some(p)
    };
    let from_osm = |s: &NewStop| -> Option<Option<&PublicStop>> {
        let mut poles = Vec::new();
        for (d, o) in osm_near(s.lat, s.lon, OSM_LINK_M) {
            let said = osm_says(s, o);
            if let Some(Some(p)) = said
                && direction(s, p).is_some_and(|diff| diff > MAX_DIRECTION_DEG)
            {
                continue; // a pole on the other side of the road is not the stop's, however close
            }
            poles.push((d, o, said));
        }
        let (d, o, said) = poles.into_iter().next()?;
        let said = said?;
        if said.is_none() && o.r#ref.as_deref() != s.code.as_deref() && d > OSM_OWN_M {
            return None; // a pole that far off may just be a neighbour
        }
        Some(said)
    };

    // 0./1. same pole
    let mut unresolved: Vec<&NewStop> = Vec::new();
    for s in stops {
        if let Some(found) = from_osm(s) {
            match found {
                Some(p) => {
                    plan.links.insert(s.stop_id.clone(), p.id.clone());
                    plan.count("osm_ref");
                }
                None => plan.count("osm_own_pole"),
            }
            continue;
        }
        let cands = same_pole(s);
        if !cands.is_empty() {
            plan.links.insert(s.stop_id.clone(), pick(s, &cands));
            plan.count("same_pole");
            continue;
        }
        unresolved.push(s);
    }

    let mut by_group: HashMap<&str, Vec<&PublicStop>> = HashMap::new();
    for p in shared.iter().filter(located) {
        by_group.entry(&p.group_id).or_default().push(p);
    }

    // 2. logical stop per station / name
    let group_of: HashMap<&str, &str> = shared.iter().map(|p| (p.id.as_str(), p.group_id.as_str())).collect();
    let mut members: Vec<(String, Vec<&NewStop>)> = Vec::new(); // in order of first appearance
    let mut member_index: HashMap<String, usize> = HashMap::new();
    for s in stops {
        let key = s.group_key();
        let i = *member_index.entry(key.clone()).or_insert_with(|| {
            members.push((key, Vec::new()));
            members.len() - 1
        });
        members[i].1.push(s);
    }
    for (gkey, ms) in &members {
        if let Some(g) =
            most_common(ms.iter().filter_map(|s| plan.links.get(&s.stop_id)).map(|pid| group_of[pid.as_str()]))
        {
            plan.group_for_key.insert(gkey.clone(), g.to_string());
            continue;
        }
        let n = ms.len() as f64;
        let lat = ms.iter().map(|m| m.lat).sum::<f64>() / n;
        let lon = ms.iter().map(|m| m.lon).sum::<f64>() / n;
        let name = most_common(ms.iter().map(|m| m.group_name())).unwrap();
        let mut best: Option<(u8, f64, &str)> = None;
        for g in shared_groups {
            let (Some(glat), Some(glon)) = (g.lat, g.lon) else {
                continue;
            };
            if (glat - lat).abs() > 0.012 {
                continue; // ~1.3 km: a cheap pre-filter (groups can be spread out)
            }
            // distance to the group's nearest pole, not its centre (some stops spread their poles over 1 km)
            let poles = by_group.get(g.id.as_str()).map(Vec::as_slice).unwrap_or_default();
            let d = ms
                .iter()
                .flat_map(|m| poles.iter().map(move |p| distance_m(m.lat, m.lon, p.lat.unwrap(), p.lon.unwrap())))
                .fold(None, |acc: Option<f64>, d| Some(acc.map_or(d, |a| a.min(d))))
                .unwrap_or_else(|| distance_m(lat, lon, glat, glon));
            if d > GROUP_EXACT_NAME_M {
                continue;
            }
            let Some(kind) = names_match(name, &g.name) else {
                continue;
            };
            let limit = if kind == NameMatch::Exact { GROUP_EXACT_NAME_M } else { GROUP_SIMILAR_NAME_M };
            if d <= limit {
                let rank = (u8::from(kind != NameMatch::Exact), d, g.id.as_str());
                if best.is_none_or(|b| rank.partial_cmp(&b).unwrap().is_lt()) {
                    best = Some(rank);
                }
            }
        }
        if let Some((_, _, gid)) = best {
            plan.group_for_key.insert(gkey.clone(), gid.to_string());
        }
    }

    // 3. pole within the logical stop
    let key_of: HashMap<&str, String> = stops.iter().map(|s| (s.stop_id.as_str(), s.group_key())).collect();
    let next_groups = |plan: &SharedPlan, s: &NewStop| -> HashSet<String> {
        inp.stop_next
            .get(&s.stop_id)
            .into_iter()
            .flatten()
            .filter_map(|n| key_of.get(n.as_str()).and_then(|k| plan.group_for_key.get(k)).cloned())
            .collect()
    };
    for s in unresolved {
        let Some(gid) = plan.group_for_key.get(&s.group_key()).cloned() else {
            continue;
        };
        let mut same_way: Vec<(f64, &PublicStop)> = Vec::new();
        for p in by_group.get(gid.as_str()).into_iter().flatten() {
            let (lat, lon) = pos(p);
            let d = distance_m(s.lat, s.lon, lat, lon);
            let diff = direction(s, p);
            if d > IN_GROUP_POLE_M || !same_mode(p) || diff.is_none() {
                continue;
            }
            // OSM has the feed's stop and the shared stop each on a pole of its own, and those aren't the same one
            let a = osm_pole_of(format!("s{}", s.stop_id), s.lat, s.lon);
            let b = osm_pole_of(p.id.clone(), lat, lon);
            if let (Some(a), Some(b)) = (a, b)
                && distance_m(a.lat, a.lon, b.lat, b.lon) > OSM_SAME_POLE_M
            {
                continue;
            }
            if d > CONFIRMED_POLE_M {
                let ahead = along_track_m(s.lat, s.lon, inp.stop_bearing[&s.stop_id], lat, lon);
                let leads_on = inp.shared_next.get(&p.id).is_some_and(|n| !n.is_disjoint(&next_groups(&plan, s)));
                if ahead.abs() > ALONG_TRACK_M || !leads_on {
                    continue;
                }
            }
            if diff.unwrap() <= MAX_DIRECTION_DEG {
                same_way.push((d, p));
            }
        }
        if let Some((_, p)) = same_way.iter().fold(None, |best: Option<(f64, &PublicStop)>, c| match best {
            Some(b) if b.0 <= c.0 => Some(b),
            _ => Some(*c),
        }) {
            plan.links.insert(s.stop_id.clone(), p.id.clone());
            plan.count("in_group");
        } else {
            plan.count("own_pole_in_shared_group");
            plan.warnings.push(format!(
                "stop {} ({}): no pole in the same direction, kept as own pole of the stop",
                s.stop_id, s.name
            ));
        }
    }
    plan
}

/// Python's Counter(items).most_common(1): the most frequent; ties: the first seen.
fn most_common<'a>(items: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut counts: Vec<(&str, usize)> = Vec::new();
    for item in items {
        match counts.iter_mut().find(|(k, _)| *k == item) {
            Some((_, n)) => *n += 1,
            None => counts.push((item, 1)),
        }
    }
    let top = counts.iter().map(|(_, n)| *n).max()?;
    counts.into_iter().find(|(_, n)| *n == top).map(|(k, _)| k)
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;

    const RM: &str = "Rondo Mogilskie";

    fn grp() -> Group {
        group("grp01", "802", RM, 0.0)
    }

    fn st(id: &str, code: &str, name: &str, dlat: f64) -> NewStop {
        stop(id, code, name, dlat)
    }

    fn station(id: &str, code: &str, name: &str, dlat: f64, key: &str) -> NewStop {
        let mut s = stop(id, code, name, dlat);
        s.station_key = Some(key.into());
        s.station_name = Some(name.into());
        s
    }

    fn map<V: Clone>(items: &[(&str, V)]) -> HashMap<String, V> {
        items.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn modes(items: &[(&str, &[&str])]) -> HashMap<String, HashSet<String>> {
        items.iter().map(|(k, v)| (k.to_string(), v.iter().map(|m| m.to_string()).collect())).collect()
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn links(plan: &SharedPlan) -> Vec<(&str, &str)> {
        plan.links.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect()
    }

    /// direction + bus modes: the common setup of the OSM tests
    fn bus(stop: &str, pole: &str, bearing: f64) -> SharedInputs {
        SharedInputs {
            stop_bearing: map(&[(stop, bearing)]),
            shared_bearing: map(&[(pole, bearing)]),
            shared_modes: modes(&[(pole, &["bus"])]),
            feed_modes: set(&["bus"]),
            ..Default::default()
        }
    }

    fn osm(r: Option<&str>, dlat_m: f64, bus: bool, tram: bool) -> OsmPole {
        OsmPole {
            osm_id: format!("node/{}{dlat_m}", r.unwrap_or("None")),
            lat: LAT + dlat_m * M,
            lon: LON,
            kind: "platform".into(),
            r#ref: r.map(str::to_string),
            name: Some(RM.into()),
            bus,
            tram,
        }
    }

    fn with_osm(mut inp: SharedInputs, poles: Vec<OsmPole>) -> SharedInputs {
        inp.osm = poles;
        inp
    }

    #[test]
    fn spread_out_stop_is_matched_by_its_nearest_pole() {
        let g = group("g2557", "2557", "Libertów Góra Libertowska", 450.0);
        let poles = [
            ours("p1", "2557-01", "Libertów Góra Libertowska", 87.0, "g2557"),
            ours("p2", "2557-05", "Libertów Góra Libertowska", 900.0, "g2557"),
        ];
        let s = station("m", "1", "Libertów Góra Libertowska", 0.0, "station:10932");
        let plan = plan_shared(&[s], &poles, &[g], &SharedInputs::default());
        assert_eq!(plan.group_for_key.get("station:10932").map(String::as_str), Some("g2557"));
    }

    #[test]
    fn same_pole_whatever_the_name() {
        let p = ours("aaaaa", "2797-01", "Wieliczka Ochronka", 0.0, "g2797");
        let g = group("g2797", "2797", "Wieliczka Ochronka", 0.0);
        let plan = plan_shared(&[st("m1", "196", "Wieliczka Gdowska", 1.0)], &[p], &[g], &SharedInputs::default());
        assert_eq!(links(&plan), [("m1", "aaaaa")]);
        assert_eq!(plan.group_for_key.get("name:wieliczka-gdowska").map(String::as_str), Some("g2797"));
    }

    #[test]
    fn two_codes_on_one_spot_take_the_nearest_then_lowest_code() {
        let poles = [
            ours("p0071", "2487-71", "Radziszów PKP", 0.0, "grp01"),
            ours("p0001", "2487-01", "Radziszów PKP", 0.0, "grp01"),
        ];
        let inp = SharedInputs {
            stop_bearing: map(&[("m", 72.0)]),
            shared_bearing: map(&[("p0001", 162.0), ("p0071", 163.0)]),
            ..Default::default()
        };
        assert_eq!(
            links(&plan_shared(&[st("m", "6780", "Radziszów PKP", 0.0)], &poles, &[grp()], &inp)),
            [("m", "p0001")]
        );
    }

    #[test]
    fn attaches_within_the_logical_stop_up_to_30_m_by_direction() {
        let g = group("g711", "711", "Mały Płaszów P+R", 0.0);
        let poles = [ours("aaaaa", "711-02", "Mały Płaszów P+R", 0.0, "g711")];
        let stops = [st("m1", "1017", "Kraków Mały Płaszów P+R", 20.0)];
        let inp = SharedInputs {
            stop_bearing: map(&[("m1", 90.0)]),
            shared_bearing: map(&[("aaaaa", 90.0)]),
            shared_modes: modes(&[("aaaaa", &["bus"])]),
            feed_modes: set(&["bus"]),
            ..Default::default()
        };
        assert_eq!(links(&plan_shared(&stops, &poles, std::slice::from_ref(&g), &inp)), [("m1", "aaaaa")]);
        assert!(plan_shared(&stops, &poles, &[g], &SharedInputs::default()).links.is_empty()); // no direction known
    }

    #[test]
    fn different_name_and_not_the_same_pole_stays_separate() {
        let g = group("g461", "461", "Dworzec Główny Wschód", 0.0);
        let p = ours("p1", "461-01", "Dworzec Główny Wschód", 0.0, "g461");
        let plan = plan_shared(&[st("m", "656", "Kraków MDA", 67.0)], &[p], &[g], &SharedInputs::default());
        assert!(plan.links.is_empty() && plan.group_for_key.is_empty());
    }

    #[test]
    fn direction_of_travel_picks_the_right_side_of_the_road() {
        let name = "Kryspinów Stopień Wodny";
        let poles = [ours("p0001", "2793-01", name, 12.0, "g2793"), ours("p0002", "2793-02", name, -12.0, "g2793")];
        let stops = [st("859", "907", name, 0.0), st("877", "889", name, 0.0)];
        let inp = SharedInputs {
            stop_bearing: map(&[("859", 59.0), ("877", 240.0)]),
            shared_bearing: map(&[("p0001", 240.0), ("p0002", 59.0)]),
            ..Default::default()
        };
        let plan = plan_shared(&stops, &poles, &[group("g2793", "2793", name, 0.0)], &inp);
        assert_eq!(links(&plan), [("859", "p0002"), ("877", "p0001")]);
    }

    #[test]
    fn opposite_direction_is_rejected_even_when_nearest() {
        let name = "Glogera Granica Miasta";
        let poles = [ours("p0001", "376-02", name, 10.0, "g376"), ours("p0002", "376-01", name, 20.0, "g376")];
        let inp = SharedInputs {
            stop_bearing: map(&[("m", 159.0)]),
            shared_bearing: map(&[("p0001", 339.0), ("p0002", 159.0)]),
            ..Default::default()
        };
        let plan = plan_shared(&[st("m", "1", name, 0.0)], &poles, &[group("g376", "376", name, 0.0)], &inp);
        assert_eq!(links(&plan), [("m", "p0002")]);
    }

    #[test]
    fn pole_in_the_same_stop_and_direction_is_used_even_when_coordinates_are_off() {
        let name = "Czyżyny Dworzec";
        let poles = [ours("p1", "271-01", name, 0.0, "g271"), ours("p2", "271-02", name, 14.0, "g271")];
        let inp = SharedInputs {
            stop_bearing: map(&[("m", 90.0)]),
            shared_bearing: map(&[("p1", 95.0), ("p2", 275.0)]),
            shared_modes: modes(&[("p1", &["bus"]), ("p2", &["bus"])]),
            feed_modes: set(&["bus"]),
            ..Default::default()
        };
        let plan = plan_shared(&[st("m", "3470", name, 28.0)], &poles, &[group("g271", "271", name, 0.0)], &inp);
        assert_eq!(links(&plan), [("m", "p1")]);
    }

    #[test]
    fn pole_beyond_30_m_needs_the_same_next_stop() {
        let g2 = group("g2", "803", "Cystersów", 100.0);
        let next_pole = ours("q", "803-01", "Cystersów", 100.0, "g2");
        let plan = |dlat_m: f64, shared_next: &[(&str, &[&str])]| {
            let poles = [ours("p2", "802-02", RM, dlat_m, "grp01"), next_pole.clone()];
            let stops = [st("m", "7620", RM, 0.0), st("n", "9001", "Cystersów", 100.0)];
            let inp = SharedInputs {
                stop_bearing: map(&[("m", 90.0)]),
                shared_bearing: map(&[("p2", 90.0)]),
                shared_modes: modes(&[("p2", &["bus"]), ("q", &["bus"])]),
                feed_modes: set(&["bus"]),
                stop_next: [("m".to_string(), set(&["n"]))].into(),
                shared_next: modes(shared_next),
                ..Default::default()
            };
            plan_shared(&stops, &poles, &[grp(), g2.clone()], &inp)
        };
        assert_eq!(plan(45.0, &[("p2", &["g2"])]).links.get("m").map(String::as_str), Some("p2"));
        let other = plan(50.0, &[("p2", &["g9"])]);
        assert!(!other.links.contains_key("m"));
        assert_eq!(other.group_for_key.get("name:rondo-mogilskie").map(String::as_str), Some("grp01"));
        assert!(!plan(45.0, &[]).links.contains_key("m")); // nothing known about where the pole goes
        assert!(!plan(71.0, &[("p2", &["g2"])]).links.contains_key("m")); // too far
        assert_eq!(plan(25.0, &[]).links.get("m").map(String::as_str), Some("p2")); // within 30 m direction is enough
    }

    #[test]
    fn pole_further_up_the_line_is_the_next_stop_not_this_one() {
        let g2 = group("g2", "803", "Cystersów", 100.0);
        let q = ours("q", "803-01", "Cystersów", 100.0, "g2");
        let stops = [st("m", "3470", RM, 0.0), st("n", "9001", "Cystersów", 100.0)];
        let east = 1.0 / (111_320.0 * 0.6425);
        let plan = |dlon_m: f64, dlat_m: f64| {
            let mut p1 = ours("p1", "802-01", RM, dlat_m, "grp01");
            p1.lon = Some(LON + dlon_m * east);
            let inp = SharedInputs {
                stop_bearing: map(&[("m", 90.0)]),
                shared_bearing: map(&[("p1", 90.0)]),
                shared_modes: modes(&[("p1", &["bus"]), ("q", &["bus"])]),
                feed_modes: set(&["bus"]),
                stop_next: [("m".to_string(), set(&["n"]))].into(),
                shared_next: modes(&[("p1", &["g2"])]),
                ..Default::default()
            };
            plan_shared(&stops, &[p1, q.clone()], &[grp(), g2.clone()], &inp)
        };
        assert!(!plan(47.0, 0.0).links.contains_key("m")); // up the road: the next stop
        assert!(!plan(-47.0, 0.0).links.contains_key("m")); // back down the road: the previous one
        assert_eq!(plan(0.0, 47.0).links.get("m").map(String::as_str), Some("p1")); // across the road
    }

    #[test]
    fn bus_stop_next_to_tram_only_stops_keeps_its_own_pole() {
        let g = group("g215", "215", "Brama nr 5", 0.0);
        let poles = [ours("t1", "215-02", "Brama nr 5", 40.0, "g215")];
        let inp = SharedInputs {
            stop_bearing: map(&[("m", 241.0)]),
            shared_bearing: map(&[("t1", 250.0)]),
            shared_modes: modes(&[("t1", &["tram"])]),
            feed_modes: set(&["bus"]),
            ..Default::default()
        };
        let plan = plan_shared(&[st("m", "3486", "Brama nr 5", 0.0)], &poles, &[g], &inp);
        assert!(plan.links.is_empty());
        assert_eq!(plan.group_for_key.get("name:brama-nr-5").map(String::as_str), Some("g215"));
    }

    #[test]
    fn normalize_ref_reads_ztp_codes_only() {
        use super::super::osm::normalize_ref;
        assert_eq!(normalize_ref(Some("802-2")).as_deref(), Some("802-02"));
        assert_eq!(normalize_ref(Some("802-07")).as_deref(), Some("802-07"));
        assert_eq!(normalize_ref(Some("214-1t")), None);
        assert_eq!(normalize_ref(None), None);
    }

    #[test]
    fn osm_ref_links_a_stop_whatever_its_coordinates() {
        let poles = [ours("p2", "802-02", RM, 45.0, "grp01")];
        let inp = with_osm(bus("m", "p2", 90.0), vec![osm(Some("802-02"), 5.0, true, false)]);
        let plan = plan_shared(&[st("m", "7620", RM, 0.0)], &poles, &[grp()], &inp);
        assert_eq!(links(&plan), [("m", "p2")]);
        assert_eq!(plan.method("osm_ref"), 1);
    }

    #[test]
    fn osm_pole_ztp_does_not_publish_keeps_the_stops_own_pole() {
        let poles = [ours("p2", "802-02", RM, 12.0, "grp01")];
        let s = [st("m", "7620", RM, 0.0)];
        assert_eq!(links(&plan_shared(&s, &poles, &[grp()], &bus("m", "p2", 90.0))), [("m", "p2")]);
        let plan = plan_shared(
            &s,
            &poles,
            &[grp()],
            &with_osm(bus("m", "p2", 90.0), vec![osm(Some("802-07"), 2.0, true, false)]),
        );
        assert!(plan.links.is_empty());
        assert_eq!(plan.method("osm_own_pole"), 1);
        assert_eq!(plan.group_for_key.values().collect::<Vec<_>>(), ["grp01"]);
    }

    #[test]
    fn osm_without_a_usable_pole_leaves_it_to_position() {
        let poles = [ours("p2", "802-02", RM, 12.0, "grp01")];
        let one = |o: Vec<OsmPole>| {
            let plan = plan_shared(&[st("m", "7620", RM, 0.0)], &poles, &[grp()], &with_osm(bus("m", "p2", 90.0), o));
            plan.links.into_iter().collect::<Vec<_>>()
        };
        let p2 = vec![("m".to_string(), "p2".to_string())];
        assert_eq!(one(vec![osm(None, 2.0, true, false)]), p2); // a pole without a ref says nothing
        assert_eq!(one(vec![osm(Some("802-07"), 60.0, true, false)]), p2); // too far to be the stop's pole
        assert_eq!(one(vec![osm(Some("214-1t"), 2.0, false, true)]), p2); // a tram pole
        assert!(one(vec![osm(Some("802-07"), 2.0, true, false), osm(Some("802-02"), 3.0, true, false)]).is_empty());
    }

    #[test]
    fn bus_stop_is_never_attached_to_a_tram_only_pole_by_name_or_position() {
        let g = group("g214", "214", "Giedroycia", 0.0);
        let inp = SharedInputs {
            stop_bearing: map(&[("m", 90.0)]),
            shared_bearing: map(&[("t1", 90.0)]),
            shared_modes: modes(&[("t1", &["tram"])]),
            feed_modes: set(&["bus"]),
            ..Default::default()
        };
        for dlat in [27.0, 5.0] {
            let poles = [ours("t1", "214-01", "Giedroycia", dlat, "g214")];
            assert!(
                plan_shared(&[st("m", "3485", "Giedroycia", 0.0)], &poles, std::slice::from_ref(&g), &inp)
                    .links
                    .is_empty()
            );
        }
    }

    #[test]
    fn osm_ref_decides_up_to_60_m() {
        let poles = [ours("p3", "889-03", "Fort Mogiła", 60.0, "g889")];
        let g = group("g889", "889", "Fort Mogiła", 0.0);
        let s = [st("m", "3483", "Fort Mogiła", 0.0)];
        let plan = plan_shared(
            &s,
            &poles,
            std::slice::from_ref(&g),
            &with_osm(bus("m", "p3", 90.0), vec![osm(Some("889-03"), 40.0, true, false)]),
        );
        assert_eq!(links(&plan), [("m", "p3")]);
        let plan = plan_shared(
            &s,
            &poles,
            &[g],
            &with_osm(bus("m", "p3", 90.0), vec![osm(Some("889-07"), 30.0, true, false)]),
        );
        assert_eq!(plan.method("osm_own_pole"), 0);
    }

    #[test]
    fn osm_operator_code_and_bay_refs() {
        let poles = [ours("p1", "802-01", RM, 45.0, "grp01")];
        let run = |s: NewStop, o: Vec<OsmPole>| plan_shared(&[s], &poles, &[grp()], &with_osm(bus("m", "p1", 90.0), o));
        let plan = run(st("m", "200", RM, 0.0), vec![osm(Some("200"), 4.0, true, false)]);
        assert!(plan.links.is_empty() && plan.method("osm_own_pole") == 1);
        assert_eq!(run(st("m", "200", RM, 0.0), vec![osm(Some("26"), 4.0, true, false)]).method("osm_own_pole"), 0);
        let bays = || vec![osm(Some("271-01-01"), 44.0, true, false), osm(Some("271-01-06"), 5.0, true, false)];
        let plan = run(st("m", "3470", RM, 0.0), bays());
        assert!(plan.links.is_empty() && plan.method("osm_own_pole") == 1);
        let on_bay = run(st("m", "3470", RM, 44.0), bays());
        assert_eq!(links(&on_bay), [("m", "p1")]);
        assert_eq!(on_bay.method("osm_ref"), 1);
    }

    #[test]
    fn osm_poles_at_the_same_distance_do_not_break_the_lookup() {
        let poles = [ours("p2", "802-02", RM, 0.0, "grp01")];
        let o = vec![osm(Some("802-02"), 5.0, true, false), osm(Some("802-02"), -5.0, true, false)];
        let plan = plan_shared(&[st("m", "7620", RM, 0.0)], &poles, &[grp()], &with_osm(bus("m", "p2", 90.0), o));
        assert_eq!(links(&plan), [("m", "p2")]);
    }

    #[test]
    fn stop_and_pole_on_different_osm_poles_are_not_the_same_pole() {
        let poles = [ours("p2", "802-02", RM, 28.0, "grp01")];
        let run = |dlat: f64, o: Vec<OsmPole>| {
            plan_shared(&[st("m", "866", RM, dlat)], &poles, &[grp()], &with_osm(bus("m", "p2", 90.0), o)).links
        };
        assert!(run(0.0, vec![osm(None, 3.0, true, false), osm(None, 31.0, true, false)]).is_empty());
        assert_eq!(run(0.0, vec![]).get("m").map(String::as_str), Some("p2"));
        assert_eq!(run(10.0, vec![osm(None, 14.0, true, false)]).get("m").map(String::as_str), Some("p2"));
    }

    #[test]
    fn osm_calls_a_stop_an_own_pole_only_when_it_has_a_pole_there() {
        let poles = [ours("p2", "802-02", RM, 12.0, "grp01")];
        let run = |o: OsmPole| {
            plan_shared(&[st("m", "7620", RM, 0.0)], &poles, &[grp()], &with_osm(bus("m", "p2", 90.0), vec![o]))
        };
        let near = run(osm(Some("802-07"), 2.0, true, false));
        assert!(near.links.is_empty() && near.method("osm_own_pole") == 1);
        let far = run(osm(Some("802-07"), 30.0, true, false));
        assert_eq!(links(&far), [("m", "p2")]);
        assert_eq!(far.method("osm_own_pole"), 0);
        let own = run(osm(Some("7620"), 35.0, true, false));
        assert!(own.links.is_empty() && own.method("osm_own_pole") == 1);
    }

    #[test]
    fn osm_ref_names_the_pole_even_when_the_stops_gps_is_60_m_off() {
        let poles = [ours("p2", "419-02", "Pleszowska", 40.0, "g419")];
        let inp = with_osm(bus("m", "p2", 90.0), vec![osm(Some("419-02"), 58.0, true, false)]);
        let plan = plan_shared(
            &[st("m", "2833", "Pleszowska", 0.0)],
            &poles,
            &[group("g419", "419", "Pleszowska", 0.0)],
            &inp,
        );
        assert_eq!(links(&plan), [("m", "p2")]);
    }

    #[test]
    fn most_common_takes_the_first_of_a_tie() {
        assert_eq!(most_common(["b", "a", "a", "b"].into_iter()), Some("b"));
        assert_eq!(most_common(["b", "a", "a"].into_iter()), Some("a"));
    }
}
