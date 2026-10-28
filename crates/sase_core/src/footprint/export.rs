//! Weekly identity-free export projection.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use rand::{rngs::OsRng, RngCore};
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};

use super::catalog::parse_day;
use super::spool::feature_id_valid;
use super::store::{bounded_busy_timeout, open_read_store};
use super::week::{week_bounds, week_label};
use super::wire::{
    ns_of, require_member, ExportRequestWire, PackFeatureWire, PackWire,
    ACTORS, EXPORT_OS, EXPORT_SASE_PATTERN, EXPORT_SCHEMA, N_BUCKETS, SURFACES,
};
use super::FootprintError;

static SASE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(EXPORT_SASE_PATTERN).expect("export sase regex")
});

/// Project complete ISO weeks into identity-free export packs.
pub fn export(
    request: &ExportRequestWire,
    busy_timeout: Duration,
) -> Result<Vec<PackWire>, FootprintError> {
    let today = parse_day(&request.today)?;
    if let Some(0) = request.weeks {
        return Err(FootprintError::validation(
            "weeks must be greater than zero",
        ));
    }
    let sase = normalize_sase(&request.sase)?;
    require_member(&request.os, EXPORT_OS, "os")?;
    let timeout = bounded_busy_timeout(busy_timeout);
    let store_path = Path::new(&request.store_path);
    if !store_path.exists() {
        return Ok(Vec::new());
    }
    let conn = open_read_store(store_path, timeout)?;
    let data_days = load_local_days(&conn)?;
    if data_days.is_empty() {
        return Ok(Vec::new());
    }
    let mut week_map: BTreeMap<String, (chrono::NaiveDate, chrono::NaiveDate)> =
        BTreeMap::new();
    for day in &data_days {
        let (start, end) = week_bounds(*day)?;
        if end < today {
            week_map.insert(week_label(*day), (start, end));
        }
    }
    let mut weeks: Vec<(String, chrono::NaiveDate, chrono::NaiveDate)> =
        week_map
            .into_iter()
            .map(|(label, (start, end))| (label, start, end))
            .collect();
    weeks.sort_by(|left, right| right.0.cmp(&left.0));
    if let Some(limit) = request.weeks {
        weeks.truncate(limit as usize);
    }
    weeks.sort_by(|left, right| left.0.cmp(&right.0));
    let mut packs = Vec::new();
    for (label, start, end) in weeks {
        packs.push(build_pack(&conn, &label, start, end, &sase, &request.os)?);
    }
    Ok(packs)
}

fn normalize_sase(value: &str) -> Result<String, FootprintError> {
    let trimmed = value.trim();
    let major_minor = match trimmed.split('.').collect::<Vec<_>>()[..] {
        [major, minor, ..] => format!("{major}.{minor}"),
        _ => trimmed.to_string(),
    };
    if !SASE_RE.is_match(&major_minor) {
        return Err(FootprintError::validation(format!(
            "invalid sase version {value:?}; want major.minor"
        )));
    }
    Ok(major_minor)
}

fn load_local_days(
    conn: &Connection,
) -> Result<BTreeSet<chrono::NaiveDate>, FootprintError> {
    let mut days = BTreeSet::new();
    let mut stmt = conn
        .prepare("SELECT DISTINCT day FROM daily")
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(FootprintError::from_sqlite)?;
    for day in rows {
        if let Ok(parsed) =
            parse_day(&day.map_err(FootprintError::from_sqlite)?)
        {
            days.insert(parsed);
        }
    }
    let mut stmt = conn
        .prepare("SELECT DISTINCT day FROM coverage")
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(FootprintError::from_sqlite)?;
    for day in rows {
        if let Ok(parsed) =
            parse_day(&day.map_err(FootprintError::from_sqlite)?)
        {
            days.insert(parsed);
        }
    }
    Ok(days)
}

fn build_pack(
    conn: &Connection,
    week: &str,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    sase: &str,
    os: &str,
) -> Result<PackWire, FootprintError> {
    let start_s = start.to_string();
    let end_s = end.to_string();
    let mut observed = BTreeMap::new();
    for surface in SURFACES {
        let days = observed_days(conn, surface, &start_s, &end_s)?;
        observed.insert((*surface).to_string(), days);
    }
    let not_measured: Vec<String> = SURFACES
        .iter()
        .filter(|surface| observed.get(**surface).copied().unwrap_or(0) == 0)
        .map(|surface| (*surface).to_string())
        .collect();
    let features = export_features(conn, &start_s, &end_s)?;
    Ok(PackWire {
        schema: EXPORT_SCHEMA.to_string(),
        nonce: new_nonce(),
        week: week.to_string(),
        sase: sase.to_string(),
        os: os.to_string(),
        observed,
        features,
        not_measured,
    })
}

fn observed_days(
    conn: &Connection,
    surface: &str,
    start: &str,
    end: &str,
) -> Result<u32, FootprintError> {
    let count: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(DISTINCT day) FROM coverage
            WHERE surface = ?1 AND day >= ?2 AND day <= ?3 AND measured = 1
            "#,
            params![surface, start, end],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?
        .unwrap_or(0);
    Ok(count.clamp(0, 7) as u32)
}

fn export_features(
    conn: &Connection,
    start: &str,
    end: &str,
) -> Result<Vec<PackFeatureWire>, FootprintError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT d.feature_id, d.actor, COUNT(DISTINCT d.day), SUM(d.n),
                   COALESCE(f.ns, ''), COALESCE(f.kind, '')
            FROM daily d
            LEFT JOIN features f ON f.feature_id = d.feature_id
            WHERE d.day >= ?1 AND d.day <= ?2
            GROUP BY d.feature_id, d.actor
            ORDER BY d.feature_id, d.actor
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map(params![start, end], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut features = Vec::new();
    for row in rows {
        let (id, actor, days, n, ns, kind) =
            row.map_err(FootprintError::from_sqlite)?;
        let ns = if ns.is_empty() {
            ns_of(&id).unwrap_or("").to_string()
        } else {
            ns
        };
        if !exportable_feature(&id, &ns, &kind) {
            continue;
        }
        if !ACTORS.contains(&actor.as_str()) {
            continue;
        }
        let Some(bucket) = bucket_n(n) else {
            continue;
        };
        if days <= 0 {
            continue;
        }
        features.push(PackFeatureWire {
            id,
            actor,
            days: days.clamp(0, 7) as u32,
            n: bucket.to_string(),
        });
    }
    Ok(features)
}

pub(crate) fn exportable_feature(id: &str, ns: &str, kind: &str) -> bool {
    if !feature_id_valid(id) {
        return false;
    }
    if id.contains('~') || id.contains('#') {
        return false;
    }
    if ns == "config" || ns == "plugin" {
        return false;
    }
    if kind == "option" {
        return false;
    }
    true
}

pub(crate) fn bucket_n(n: i64) -> Option<&'static str> {
    if n <= 0 {
        None
    } else if n == 1 {
        Some(N_BUCKETS[0])
    } else if n <= 5 {
        Some(N_BUCKETS[1])
    } else if n <= 20 {
        Some(N_BUCKETS[2])
    } else {
        Some(N_BUCKETS[3])
    }
}

pub(crate) fn bucket_min(bucket: &str) -> i64 {
    match bucket {
        "1" => 1,
        "2-5" => 2,
        "6-20" => 6,
        "21+" => 21,
        _ => 0,
    }
}

fn new_nonce() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    format!("r_{}", hex::encode(bytes))
}
