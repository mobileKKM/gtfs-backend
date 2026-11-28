//! Matching a feed's stops within its own stop-code namespace (no database: apply.rs reads and writes).

use std::collections::{BTreeMap, HashMap, HashSet};

use super::{Group, NewStop, PublicStop, distance_m};
use crate::util::normalize_name;

/// Same code: same stop (observed cross-feed spread for one code: up to 131 m).
pub const CODE_MATCH_M: f64 = 300.0;
/// Same name + platform_code: the same stop, its code changed.
pub const NAME_MATCH_M: f64 = 50.0;
/// Same spot, name and code both changed.
pub const LOCATION_MATCH_M: f64 = 15.0;
pub const GROUP_NAME_MATCH_M: f64 = 300.0;

#[derive(Debug, Default)]
pub struct MatchPlan {
    /// pole_key -> existing stop id, or None for a new stop
    pub stop_for_key: HashMap<String, Option<String>>,
    pub method_for_key: HashMap<String, &'static str>,
    /// group_key -> existing group id, or None for a new group
    pub group_for_key: HashMap<String, Option<String>>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

fn near(p: &PublicStop, s: &NewStop, limit: f64) -> bool {
    matches!((p.lat, p.lon), (Some(lat), Some(lon)) if distance_m(lat, lon, s.lat, s.lon) <= limit)
}

fn dist(p: &PublicStop, s: &NewStop) -> f64 {
    distance_m(p.lat.unwrap_or(f64::NAN), p.lon.unwrap_or(f64::NAN), s.lat, s.lon)
}

/// One representative stop per pole key (stops sharing a code are the same pole), in pole-key order of first appearance
/// when the stops are sorted by stop_id.
pub fn representatives(stops: &[NewStop]) -> Vec<(String, NewStop)> {
    let mut sorted: Vec<&NewStop> = stops.iter().collect();
    sorted.sort_by(|a, b| a.stop_id.cmp(&b.stop_id));
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for s in sorted {
        let key = s.pole_key();
        if seen.insert(key.clone()) {
            out.push((key, s.clone()));
        }
    }
    out
}

pub fn plan_matches(stops: &[NewStop], existing: &[PublicStop], groups: &[Group]) -> MatchPlan {
    let mut plan = MatchPlan::default();
    let reps = representatives(stops);
    let rep_of: HashMap<&str, &NewStop> = reps.iter().map(|(k, s)| (k.as_str(), s)).collect();
    let batch_codes: HashSet<&str> = reps.iter().filter_map(|(_, s)| s.code.as_deref()).collect();

    let mut by_code: HashMap<&str, Vec<&PublicStop>> = HashMap::new();
    let mut by_name: HashMap<(String, Option<String>), Vec<&PublicStop>> = HashMap::new();
    for p in existing {
        if let Some(c) = &p.code {
            by_code.entry(c).or_default().push(p);
        }
        let pc = p.platform_code.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
        by_name.entry((normalize_name(&p.name), pc)).or_default().push(p);
    }

    let mut claimed: HashSet<String> = HashSet::new();
    let mut unresolved: Vec<(&str, &NewStop)> = Vec::new();

    // a. same code (nearby)
    for (key, s) in &reps {
        if let Some(code) = &s.code {
            let mut cands: Vec<&&PublicStop> = by_code
                .get(code.as_str())
                .map(|v| v.iter().filter(|p| near(p, s, CODE_MATCH_M)).collect())
                .unwrap_or_default();
            if !cands.is_empty() {
                cands.sort_by(|a, b| (!a.active, dist(a, s)).partial_cmp(&(!b.active, dist(b, s))).unwrap());
                let best = cands[0];
                plan.stop_for_key.insert(key.clone(), Some(best.id.clone()));
                plan.method_for_key.insert(key.clone(), "code");
                claimed.insert(best.id.clone());
                continue;
            }
            if by_code.get(code.as_str()).is_some_and(|v| !v.is_empty()) {
                plan.warnings
                    .push(format!("stop {}: code {code} exists but more than {CODE_MATCH_M:.0} m away", s.stop_id));
            }
        }
        unresolved.push((key, s));
    }

    let free = |p: &PublicStop| !claimed.contains(&p.id) && p.code.as_deref().is_none_or(|c| !batch_codes.contains(c));

    // b. same name + platform_code (nearby), c. same spot. Tentative until claims are unique.
    let mut tentative: Vec<(&str, String, &'static str)> = Vec::new();
    let mut still: Vec<(&str, &NewStop)> = Vec::new();
    for (key, s) in &unresolved {
        let name_key = (normalize_name(&s.name), s.platform_code.clone());
        let cands: Vec<&&PublicStop> = by_name
            .get(&name_key)
            .map(|v| v.iter().filter(|p| free(p) && near(p, s, NAME_MATCH_M)).collect())
            .unwrap_or_default();
        match cands.len() {
            1 => tentative.push((key, cands[0].id.clone(), "name")),
            0 => still.push((key, s)),
            n => {
                plan.errors
                    .push(format!("stop {} ({}): {n} stops match by name, creating a new one", s.stop_id, s.name));
                plan.stop_for_key.insert(key.to_string(), None);
                plan.method_for_key.insert(key.to_string(), "ambiguous");
            }
        }
    }
    for (key, s) in &still {
        let cands: Vec<&PublicStop> =
            existing.iter().filter(|p| free(p) && !p.linked_elsewhere && near(p, s, LOCATION_MATCH_M)).collect();
        match cands.len() {
            1 => tentative.push((key, cands[0].id.clone(), "location")),
            0 => {
                plan.stop_for_key.insert(key.to_string(), None);
                plan.method_for_key.insert(key.to_string(), "new");
            }
            n => {
                plan.errors
                    .push(format!("stop {} ({}): {n} stops on the same spot, creating a new one", s.stop_id, s.name));
                plan.stop_for_key.insert(key.to_string(), None);
                plan.method_for_key.insert(key.to_string(), "ambiguous");
            }
        }
    }
    let mut claims: HashMap<&str, usize> = HashMap::new();
    for (_, pid, _) in &tentative {
        *claims.entry(pid.as_str()).or_default() += 1;
    }
    for (key, pid, method) in &tentative {
        let s = rep_of[key];
        if claims[pid.as_str()] > 1 {
            plan.errors.push(format!(
                "stop {} ({}): stop {pid} claimed by {} stops, creating a new one",
                s.stop_id,
                s.name,
                claims[pid.as_str()]
            ));
            plan.stop_for_key.insert(key.to_string(), None);
            plan.method_for_key.insert(key.to_string(), "ambiguous");
            continue;
        }
        plan.stop_for_key.insert(key.to_string(), Some(pid.clone()));
        plan.method_for_key.insert(key.to_string(), method);
        if *method == "location" {
            plan.warnings.push(format!(
                "stop {} ({}, {}): matched stop {pid} by location only",
                s.stop_id,
                s.name,
                s.code.as_deref().unwrap_or("None")
            ));
        }
    }

    // groups follow stops
    let stop_group: HashMap<&str, &str> = existing.iter().map(|p| (p.id.as_str(), p.group_id.as_str())).collect();
    let mut groups_by_code: HashMap<&str, Vec<&Group>> = HashMap::new();
    for g in groups {
        if let Some(c) = &g.code {
            groups_by_code.entry(c).or_default().push(g);
        }
    }
    let mut batch: BTreeMap<String, Vec<&str>> = BTreeMap::new(); // group_key -> pole keys
    for (key, s) in &reps {
        batch.entry(s.group_key()).or_default().push(key);
    }
    let mut order: Vec<&String> = batch.keys().collect();
    order.sort_by(|a, b| batch[*b].len().cmp(&batch[*a].len()).then(a.cmp(b)));
    let mut used: HashSet<String> = HashSet::new();
    for gkey in order {
        let keys = &batch[gkey];
        let mut votes: BTreeMap<&str, usize> = BTreeMap::new();
        for k in keys {
            if let Some(Some(pid)) = plan.stop_for_key.get(*k)
                && let Some(g) = stop_group.get(pid.as_str())
                && !used.contains(*g)
            {
                *votes.entry(g).or_default() += 1;
            }
        }
        // the most votes; ties: the lowest id (BTreeMap order)
        let top = votes.values().copied().max();
        let mut chosen: Option<String> =
            top.and_then(|top| votes.iter().find(|(_, n)| **n == top).map(|(g, _)| g.to_string()));
        if chosen.is_none() && !gkey.starts_with("name:") {
            // a station: the feed's, or one its stop_code_pattern names
            let same_code: Vec<&&Group> = groups_by_code
                .get(gkey.as_str())
                .map(|v| v.iter().filter(|g| !used.contains(&g.id)).collect())
                .unwrap_or_default();
            if same_code.len() == 1 {
                chosen = Some(same_code[0].id.clone());
            }
        }
        if chosen.is_none() {
            let first = rep_of[keys[0]];
            let n = keys.len() as f64;
            let lat = keys.iter().map(|k| rep_of[k].lat).sum::<f64>() / n;
            let lon = keys.iter().map(|k| rep_of[k].lon).sum::<f64>() / n;
            let wanted = normalize_name(first.group_name());
            let found: Vec<&Group> = groups
                .iter()
                .filter(|g| {
                    !used.contains(&g.id)
                        && normalize_name(&g.name) == wanted
                        && matches!((g.lat, g.lon), (Some(glat), Some(glon)) if distance_m(glat, glon, lat, lon) <= GROUP_NAME_MATCH_M)
                })
                .collect();
            if found.len() == 1 {
                chosen = Some(found[0].id.clone());
            }
        }
        if let Some(g) = &chosen {
            used.insert(g.clone());
        }
        plan.group_for_key.insert(gkey.clone(), chosen);
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;

    const RM: &str = "Rondo Mogilskie";

    fn grp() -> Group {
        group("grp01", "802", RM, 0.0)
    }

    fn s(id: &str, code: &str, name: &str, dlat: f64) -> NewStop {
        stop(id, code, name, dlat)
    }

    fn o(pid: &str, code: &str, dlat: f64) -> PublicStop {
        ours(pid, code, RM, dlat, "grp01")
    }

    #[test]
    fn same_code_keeps_id_and_rename_is_in_place() {
        let plan =
            plan_matches(&[s("1", "802-01", "Rondo Mogilskie Nowe", 0.0)], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:802-01"].as_deref(), Some("aaaaa"));
        assert_eq!(plan.method_for_key["code:802-01"], "code");
        assert_eq!(plan.group_for_key["802"].as_deref(), Some("grp01"));
    }

    #[test]
    fn renumbered_code_matches_by_name_and_platform_code() {
        let plan = plan_matches(&[s("1", "803-01", RM, 10.0)], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:803-01"].as_deref(), Some("aaaaa"));
        assert_eq!(plan.method_for_key["code:803-01"], "name");
        assert_eq!(plan.group_for_key["803"].as_deref(), Some("grp01")); // the group follows its stops
    }

    #[test]
    fn name_and_code_changed_same_spot_matches_by_location_with_warning() {
        let plan = plan_matches(&[s("1", "990-01", "Opera Krakowska", 5.0)], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:990-01"].as_deref(), Some("aaaaa"));
        assert_eq!(plan.method_for_key["code:990-01"], "location");
        assert!(plan.warnings.iter().any(|w| w.contains("location only")));
    }

    #[test]
    fn new_stop_gets_new_id() {
        let plan = plan_matches(&[s("1", "555-01", "Nowy Przystanek", 500.0)], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:555-01"], None);
        assert_eq!(plan.method_for_key["code:555-01"], "new");
        assert_eq!(plan.group_for_key["555"], None);
    }

    #[test]
    fn stop_whose_code_is_still_in_use_is_not_taken_by_name() {
        let mut second = s("2", "802-09", RM, 3.0);
        second.platform_code = Some("01".into());
        let plan = plan_matches(&[s("1", "802-01", RM, 0.0), second], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:802-01"].as_deref(), Some("aaaaa"));
        assert_eq!(plan.stop_for_key["code:802-09"], None);
    }

    #[test]
    fn ambiguous_match_creates_new_id_instead_of_guessing() {
        let mut b = o("bbbbb", "802-11", -5.0);
        b.platform_code = Some("01".into());
        let plan = plan_matches(&[s("1", "803-01", RM, 0.0)], &[o("aaaaa", "802-01", 5.0), b], &[grp()]);
        assert_eq!(plan.stop_for_key["code:803-01"], None);
        assert_eq!(plan.method_for_key["code:803-01"], "ambiguous");
        assert!(!plan.errors.is_empty());
    }

    #[test]
    fn two_new_stops_claiming_one_stop_both_get_new_ids() {
        let stops = [s("1", "990-01", "A", 2.0), s("2", "991-01", "B", -2.0)];
        let plan = plan_matches(&stops, &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:990-01"], None);
        assert_eq!(plan.stop_for_key["code:991-01"], None);
        assert_eq!(plan.errors.len(), 2);
    }

    #[test]
    fn code_far_away_is_not_the_same_stop() {
        let plan = plan_matches(&[s("1", "802-01", RM, 2000.0)], &[o("aaaaa", "802-01", 0.0)], &[grp()]);
        assert_eq!(plan.stop_for_key["code:802-01"], None);
        assert!(plan.warnings.iter().any(|w| w.contains("away")));
    }

    #[test]
    fn station_hierarchy_groups_by_parent_station() {
        let s1 = normalize(None, "KRAKÓW GŁÓWNY", Some("station:85"));
        assert_eq!(s1.group_key(), "station:85");
        assert_eq!(s("x", "802-01", RM, 0.0).group_key(), "802");
        assert_eq!(normalize(None, "Bez Kodu", None).group_key(), "name:bez-kodu");
    }

    fn normalize(code: Option<&str>, name: &str, station: Option<&str>) -> NewStop {
        super::super::normalize_stop("p", code, name, None, LAT, LON, station, station.map(|_| name), Some(&ztp()))
    }
}
