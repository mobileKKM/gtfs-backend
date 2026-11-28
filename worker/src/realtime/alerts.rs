//! Alerts -> LiveAlert, the informed entities resolved to ours (trip keys, public stop ids).

use super::index::TripIndex;
use super::layout::{ActivePeriod, AlertEntity, LiveAlert};
use super::pb;
use super::resolve::{resolve_instance, start_date};

fn text(ts: &Option<pb::TranslatedString>) -> Option<String> {
    let translations = &ts.as_ref()?.translation;
    let first = translations.first()?;
    let t = translations.iter().find(|t| matches!(t.language(), "pl" | "")).unwrap_or(first);
    let out = t.text.replace("\r\n", "\n").trim().to_string();
    (!out.is_empty()).then_some(out)
}

/// A Cause/Effect/SeverityLevel as lowercase name; unset, UNKNOWN_* or a value we don't know is None.
fn enum_name(value: Option<i32>, name: impl Fn(i32) -> Option<&'static str>) -> Option<String> {
    let n = name(value?)?;
    (!n.starts_with("UNKNOWN_")).then(|| n.to_lowercase())
}

/// One informed entity in our ids, or the reason it can't be: an entity applies where all of its fields do (AND), so
/// one unknown field makes it meaningless.
fn entity(
    feed_id: &str,
    ie: &pb::EntitySelector,
    index: &TripIndex,
    now: f64,
    ignore_suffix: bool,
) -> Result<AlertEntity, String> {
    let today = index.today();
    let route_id = (!ie.route_id().is_empty()).then(|| ie.route_id().to_string());
    if let Some(r) = &route_id
        && !today.is_some_and(|d| index.has_route(d.version, r))
    {
        return Err(format!("route {r}"));
    }
    let mut trip_key = None;
    if let Some(trip) = ie.trip.as_ref().filter(|t| !t.trip_id().is_empty()) {
        match resolve_instance(index, trip.trip_id(), now, start_date(trip.start_date()), ignore_suffix) {
            Some(r) => trip_key = Some(r.key(feed_id)),
            None => return Err(format!("trip {}", trip.trip_id())),
        }
    }
    let (mut public_stop_ids, mut stop_group_id) = (Vec::new(), None);
    if !ie.stop_id().is_empty() {
        let stop_id = ie.stop_id().to_string();
        match today {
            Some(d) if index.public_stop(d.version, &stop_id).is_some() => {
                public_stop_ids = vec![index.public_stop(d.version, &stop_id).unwrap()]
            }
            Some(d) if index.stations.contains_key(&(d.version, stop_id.clone())) => {
                let (g, pids) = &index.stations[&(d.version, stop_id)];
                (stop_group_id, public_stop_ids) = (Some(g.clone()), pids.clone());
            }
            _ => return Err(format!("stop {stop_id}")),
        }
    }
    Ok(AlertEntity {
        agency_id: (!ie.agency_id().is_empty()).then(|| ie.agency_id().to_string()),
        // GTFS-RT: direction_id needs a route_id
        direction_id: if route_id.is_some() { ie.direction_id.map(|d| d as i32) } else { None },
        route_id,
        route_type: ie.route_type,
        trip_key,
        public_stop_ids,
        stop_group_id,
    })
}

/// The alert, and its informed entities that name something we don't have (`{entity_id}: route X`): those are dropped.
/// No informed entity at all: the whole feed (one empty entity).
#[allow(deprecated)] // active_period: GTFS-RT marks it deprecated in favour of communication_period, ZTP sends it
pub fn build_alert(
    feed_id: &str,
    entity_id: &str,
    alert: &pb::Alert,
    index: &TripIndex,
    now: f64,
    ignore_suffix: bool,
) -> (LiveAlert, Vec<String>) {
    let mut entities = Vec::new();
    let mut unmatched = Vec::new();
    for ie in &alert.informed_entity {
        match entity(feed_id, ie, index, now, ignore_suffix) {
            Ok(e) => entities.push(e),
            Err(why) => unmatched.push(format!("{entity_id}: {why}")),
        }
    }
    if alert.informed_entity.is_empty() {
        entities.push(AlertEntity::default());
    }
    let out = LiveAlert {
        feed_id: feed_id.into(),
        id: entity_id.into(),
        cause: enum_name(alert.cause, |v| pb::alert::Cause::try_from(v).ok().map(|c| c.as_str_name())),
        effect: enum_name(alert.effect, |v| pb::alert::Effect::try_from(v).ok().map(|c| c.as_str_name())),
        severity: enum_name(alert.severity_level, |v| {
            pb::alert::SeverityLevel::try_from(v).ok().map(|c| c.as_str_name())
        }),
        header: text(&alert.header_text),
        description: text(&alert.description_text),
        url: text(&alert.url),
        // 0 or missing: unbounded
        active_periods: alert
            .active_period
            .iter()
            .map(|p| ActivePeriod {
                start: p.start.filter(|s| *s != 0).map(|s| s as i64),
                end: p.end.filter(|e| *e != 0).map(|e| e as i64),
            })
            .collect(),
        entities,
    };
    (out, unmatched)
}
