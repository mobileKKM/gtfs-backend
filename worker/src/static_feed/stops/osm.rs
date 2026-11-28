//! OpenStreetMap stop poles: a second opinion on which pole an operator's stop is (read from a local file).
//!
//! OSM mappers tag poles with the ZTP code as `ref` (`802-02`) and say what stops there (`bus=yes`, `tram=yes`), so a
//! stop next to a pole can be told apart from the tram pole beside it, or from a pole ZTP doesn't publish at all.
//! `fetch-osm` reads a Geofabrik extract into a small JSON file of poles; the matcher only reads that file.

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use regex::Regex;
use serde::{Deserialize, Serialize};

pub const EXTRACT_URL: &str = "https://download.geofabrik.de/europe/poland/malopolskie-latest.osm.pbf"; // ~150 MB, daily

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OsmPole {
    pub osm_id: String,
    pub lat: f64,
    pub lon: f64,
    /// `platform` | `stop_position`
    pub kind: String,
    pub r#ref: Option<String>,
    pub name: Option<String>,
    pub bus: bool,
    pub tram: bool,
}

impl OsmPole {
    /// The ZTP-style code in `ref` (`802-2` -> `802-02`); None for other schemes (`214-1t`).
    pub fn code(&self) -> Option<String> {
        normalize_ref(self.r#ref.as_deref())
    }
}

static ZTP_REF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s*(\d+)-(\d{1,2})\s*$").unwrap());

pub fn normalize_ref(r: Option<&str>) -> Option<String> {
    let c = ZTP_REF.captures(r?)?;
    Some(format!("{}-{:02}", &c[1], c[2].parse::<u32>().ok()?))
}

/// A pole from an OSM object's tags; None when it isn't a stop pole or doesn't say what stops there.
pub fn make_pole(osm_id: String, tags: &HashMap<&str, &str>, lat: f64, lon: f64) -> Option<OsmPole> {
    let highway_stop = tags.get("highway") == Some(&"bus_stop");
    let kind = match tags.get("public_transport").copied() {
        Some(k @ ("platform" | "stop_position")) => k.to_string(),
        _ if highway_stop => "platform".to_string(),
        _ => return None,
    };
    let bus = tags.get("bus") == Some(&"yes") || highway_stop;
    let tram = tags.get("tram") == Some(&"yes") || tags.get("railway") == Some(&"tram_stop");
    if !(bus || tram) {
        return None;
    }
    let r = tags.get("ref").map(|r| r.trim()).filter(|r| !r.is_empty()).map(str::to_string);
    Some(OsmPole { osm_id, lat, lon, kind, r#ref: r, name: tags.get("name").map(|n| n.to_string()), bus, tram })
}

#[derive(Serialize, Deserialize)]
struct PolesFile {
    poles: Vec<OsmPole>,
}

/// The poles of a `fetch-osm` file; none when there is no file (the matcher then goes by position alone).
pub fn load_osm_poles(path: &Path) -> Result<Vec<OsmPole>> {
    match std::fs::read(path) {
        Ok(data) => {
            Ok(serde_json::from_slice::<PolesFile>(&data).with_context(|| format!("reading {}", path.display()))?.poles)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// The poles in an extract: nodes, then ways (platforms drawn as areas or lines: their middle node stands for them).
/// Two passes, since a way's nodes come before it: the ways first, then the nodes and the ways' middle nodes.
pub fn read_extract(pbf: &Path) -> Result<Vec<OsmPole>> {
    // pass 1: ways that are poles, and the node standing for each
    let mut ways: Vec<(i64, i64, HashMap<String, String>)> = Vec::new();
    ElementReader::from_path(pbf)?.for_each(|el| {
        if let Element::Way(w) = el {
            let tags: HashMap<&str, &str> = w.tags().collect();
            let refs: Vec<i64> = w.refs().collect();
            if !refs.is_empty() && make_pole(String::new(), &tags, 0.0, 0.0).is_some() {
                let owned = tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
                ways.push((w.id(), refs[refs.len() / 2], owned));
            }
        }
    })?;
    let wanted: std::collections::HashSet<i64> = ways.iter().map(|(_, n, _)| *n).collect();
    // pass 2: node poles in file order, and the locations of the ways' nodes
    let mut poles = Vec::new();
    let mut located: HashMap<i64, (f64, f64)> = HashMap::new();
    let mut node = |id: i64, lat: f64, lon: f64, tags: HashMap<&str, &str>| {
        if wanted.contains(&id) {
            located.insert(id, (lat, lon));
        }
        if let Some(p) = make_pole(format!("node/{id}"), &tags, lat, lon) {
            poles.push(p);
        }
    };
    ElementReader::from_path(pbf)?.for_each(|el| match el {
        Element::Node(n) => node(n.id(), n.lat(), n.lon(), n.tags().collect()),
        Element::DenseNode(n) => node(n.id(), n.lat(), n.lon(), n.tags().collect()),
        _ => {}
    })?;
    for (id, middle, tags) in ways {
        if let Some((lat, lon)) = located.get(&middle) {
            let tags: HashMap<&str, &str> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            poles.extend(make_pole(format!("way/{id}"), &tags, *lat, *lon));
        }
    }
    Ok(poles)
}

/// Download the extract and write its poles to `path` (replaced in one step: readers never see half a file).
pub async fn fetch_osm_poles(path: &Path, url: &str) -> Result<usize> {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    tokio::fs::create_dir_all(dir).await?;
    let pbf = path.with_extension("osm.pbf");
    let tmp = path.with_extension("tmp");
    let result = async {
        let client = reqwest::Client::builder()
            .user_agent(crate::http::USER_AGENT)
            .timeout(std::time::Duration::from_secs(600))
            .build()?;
        crate::http::download(&client, url, &pbf).await?;
        let pbf2 = pbf.clone();
        let poles = tokio::task::spawn_blocking(move || read_extract(&pbf2)).await??;
        tokio::fs::write(&tmp, serde_json::to_vec(&PolesFile { poles: poles.clone() })?).await?;
        tokio::fs::rename(&tmp, path).await?;
        anyhow::Ok(poles.len())
    }
    .await;
    let _ = tokio::fs::remove_file(&pbf).await;
    let _ = tokio::fs::remove_file(&tmp).await;
    result
}

/// Seconds since `path` was written; None when it doesn't exist.
pub fn file_age_s(path: &Path) -> Option<f64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    Some(std::time::SystemTime::now().duration_since(modified).map(|d| d.as_secs_f64()).unwrap_or(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poles_from_tags() {
        let tags: HashMap<&str, &str> = [("highway", "bus_stop"), ("ref", " 802-07 ")].into();
        let p = make_pole("node/1".into(), &tags, 50.0, 19.9).unwrap();
        assert_eq!((p.kind.as_str(), p.bus, p.tram, p.r#ref.as_deref()), ("platform", true, false, Some("802-07")));
        let tram: HashMap<&str, &str> = [("public_transport", "stop_position"), ("tram", "yes")].into();
        assert!(make_pole("n".into(), &tram, 0.0, 0.0).unwrap().tram);
        let neither: HashMap<&str, &str> = [("public_transport", "platform")].into();
        assert!(make_pole("n".into(), &neither, 0.0, 0.0).is_none());
    }

    #[test]
    fn the_python_workers_file_reads() {
        let path = Path::new("../data/osm/stops.json");
        if path.exists() {
            assert!(load_osm_poles(path).unwrap().len() > 1000);
        }
        assert!(load_osm_poles(Path::new("/nonexistent/stops.json")).unwrap().is_empty());
    }
}
