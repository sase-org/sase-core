//! Merge imported weekly packs into report figures.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use rusqlite::Connection;

use super::super::export::bucket_min;
use super::super::store::table_exists;
use super::super::week::{parse_week, week_overlaps_window, weeks_in_window};
use super::super::wire::{ACTORS, SURFACES};
use super::super::FootprintError;

#[derive(Debug, Clone)]
pub(crate) struct ImportedWeekly {
    pub week: String,
    pub feature_id: String,
    pub actor: String,
    pub days: u32,
    pub n_bucket: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ImportedObserved {
    pub week: String,
    pub surface: String,
    pub days: u32,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ImportedData {
    pub pack_count: u64,
    pub imported_day: Option<String>,
    pub weekly: Vec<ImportedWeekly>,
    pub observed: Vec<ImportedObserved>,
}

pub(crate) fn load_imported(
    conn: &Connection,
) -> Result<ImportedData, FootprintError> {
    if !table_exists(conn, "import_packs")? {
        return Ok(ImportedData::default());
    }
    let pack_count: u64 = conn
        .query_row("SELECT COUNT(*) FROM import_packs", [], |row| row.get(0))
        .map_err(FootprintError::from_sqlite)?;
    let imported_day: Option<String> = conn
        .query_row("SELECT MAX(imported_day) FROM import_packs", [], |row| {
            row.get::<_, Option<String>>(0)
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut weekly = Vec::new();
    if table_exists(conn, "imported_weekly")? {
        let mut stmt = conn
            .prepare(
                r#"
                SELECT week, feature_id, actor, days, n_bucket
                FROM imported_weekly
                "#,
            )
            .map_err(FootprintError::from_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ImportedWeekly {
                    week: row.get(0)?,
                    feature_id: row.get(1)?,
                    actor: row.get(2)?,
                    days: row.get::<_, i64>(3)? as u32,
                    n_bucket: row.get(4)?,
                })
            })
            .map_err(FootprintError::from_sqlite)?;
        for row in rows {
            weekly.push(row.map_err(FootprintError::from_sqlite)?);
        }
    }
    let mut observed = Vec::new();
    if table_exists(conn, "imported_observed")? {
        let mut stmt = conn
            .prepare("SELECT week, surface, days FROM imported_observed")
            .map_err(FootprintError::from_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ImportedObserved {
                    week: row.get(0)?,
                    surface: row.get(1)?,
                    days: row.get::<_, i64>(2)? as u32,
                })
            })
            .map_err(FootprintError::from_sqlite)?;
        for row in rows {
            observed.push(row.map_err(FootprintError::from_sqlite)?);
        }
    }
    Ok(ImportedData {
        pack_count,
        imported_day: imported_day.filter(|day| !day.is_empty()),
        weekly,
        observed,
    })
}

pub(crate) fn overlapping_weeks(
    imported: &ImportedData,
    window_start: NaiveDate,
    today: NaiveDate,
) -> BTreeSet<String> {
    let mut weeks: BTreeSet<String> = weeks_in_window(window_start, today)
        .into_iter()
        .map(|(label, _, _)| label)
        .collect();
    for label in imported
        .weekly
        .iter()
        .map(|row| row.week.as_str())
        .chain(imported.observed.iter().map(|row| row.week.as_str()))
    {
        if let Ok((start, end)) = parse_week(label) {
            if week_overlaps_window(start, end, window_start, today) {
                weeks.insert(label.to_string());
            }
        }
    }
    weeks
}

/// Per-week `max(local, imported)` summed across overlapping weeks.
pub(crate) fn merge_surface_days(
    local_days: &BTreeMap<String, BTreeSet<String>>,
    imported: &ImportedData,
    weeks: &BTreeSet<String>,
    window_start: NaiveDate,
    today: NaiveDate,
) -> BTreeMap<String, u32> {
    let mut imported_by_week: BTreeMap<(String, String), u32> = BTreeMap::new();
    for row in &imported.observed {
        if !weeks.contains(&row.week) {
            continue;
        }
        let slot = imported_by_week
            .entry((row.week.clone(), row.surface.clone()))
            .or_insert(0);
        *slot = (*slot).max(row.days);
    }
    let mut merged = BTreeMap::new();
    for surface in SURFACES {
        let mut total = 0u32;
        for week in weeks {
            let Ok((start, end)) = parse_week(week) else {
                continue;
            };
            let local = count_dayset_in_week(
                local_days.get(*surface),
                start,
                end,
                window_start,
                today,
            );
            let imported_days = imported_by_week
                .get(&(week.clone(), (*surface).to_string()))
                .copied()
                .unwrap_or(0);
            total += local.max(imported_days).min(7);
        }
        merged.insert((*surface).to_string(), total);
    }
    merged
}

pub(crate) struct FeatureMerge {
    pub scope: u32,
    pub human: u32,
    pub agent: u32,
    pub anyone: u32,
    pub extra_uses: Vec<(String, i64)>,
    pub last_used: Option<(String, String)>,
}

pub(crate) struct LocalFeatureDays<'a> {
    pub human: &'a BTreeSet<String>,
    pub agent: &'a BTreeSet<String>,
    pub scope: &'a BTreeSet<String>,
    pub anyone: &'a BTreeSet<String>,
}

pub(crate) fn merge_feature_week_days(
    local: LocalFeatureDays<'_>,
    feature_id: &str,
    actor_scope: &str,
    imported: &ImportedData,
    weeks: &BTreeSet<String>,
    window_start: NaiveDate,
    today: NaiveDate,
) -> FeatureMerge {
    let mut by_week_actor: BTreeMap<(String, String), u32> = BTreeMap::new();
    let mut n_by_actor: BTreeMap<String, i64> = BTreeMap::new();
    let mut last_used: Option<(String, String)> = None;
    for row in &imported.weekly {
        if row.feature_id != feature_id || !weeks.contains(&row.week) {
            continue;
        }
        if !ACTORS.contains(&row.actor.as_str()) {
            continue;
        }
        *by_week_actor
            .entry((row.week.clone(), row.actor.clone()))
            .or_insert(0) += row.days;
        *n_by_actor.entry(row.actor.clone()).or_insert(0) +=
            bucket_min(&row.n_bucket);
        if let Ok((_, end)) = parse_week(&row.week) {
            if scope_matches(actor_scope, &row.actor) {
                let week_end = end.to_string();
                if last_used
                    .as_ref()
                    .is_none_or(|(day, _)| week_end.as_str() > day.as_str())
                {
                    last_used = Some((week_end, row.actor.clone()));
                }
            }
        }
    }
    let mut scope = 0u32;
    let mut human = 0u32;
    let mut agent = 0u32;
    let mut anyone = 0u32;
    for week in weeks {
        let Ok((start, end)) = parse_week(week) else {
            continue;
        };
        let imported_human = by_week_actor
            .get(&(week.clone(), "human".to_string()))
            .copied()
            .unwrap_or(0);
        let imported_agent = by_week_actor
            .get(&(week.clone(), "agent".to_string()))
            .copied()
            .unwrap_or(0);
        let imported_scope: u32 = ACTORS
            .iter()
            .filter(|actor| scope_matches(actor_scope, actor))
            .map(|actor| {
                by_week_actor
                    .get(&(week.clone(), (*actor).to_string()))
                    .copied()
                    .unwrap_or(0)
            })
            .sum();
        let imported_anyone: u32 = ACTORS
            .iter()
            .map(|actor| {
                by_week_actor
                    .get(&(week.clone(), (*actor).to_string()))
                    .copied()
                    .unwrap_or(0)
            })
            .sum();
        human += min_7(
            count_dayset_in_week(
                Some(local.human),
                start,
                end,
                window_start,
                today,
            ) + imported_human,
        );
        agent += min_7(
            count_dayset_in_week(
                Some(local.agent),
                start,
                end,
                window_start,
                today,
            ) + imported_agent,
        );
        scope += min_7(
            count_dayset_in_week(
                Some(local.scope),
                start,
                end,
                window_start,
                today,
            ) + imported_scope,
        );
        anyone += min_7(
            count_dayset_in_week(
                Some(local.anyone),
                start,
                end,
                window_start,
                today,
            ) + imported_anyone,
        );
    }
    FeatureMerge {
        scope,
        human,
        agent,
        anyone,
        extra_uses: n_by_actor.into_iter().collect(),
        last_used,
    }
}

fn count_dayset_in_week(
    days: Option<&BTreeSet<String>>,
    start: NaiveDate,
    end: NaiveDate,
    window_start: NaiveDate,
    today: NaiveDate,
) -> u32 {
    let Some(days) = days else {
        return 0;
    };
    days.iter()
        .filter(|day| {
            let Ok(parsed) = NaiveDate::parse_from_str(day, "%Y-%m-%d") else {
                return false;
            };
            parsed >= start
                && parsed <= end
                && parsed >= window_start
                && parsed <= today
        })
        .count() as u32
}

fn min_7(value: u32) -> u32 {
    value.min(7)
}

fn scope_matches(scope: &str, actor: &str) -> bool {
    match scope {
        "you" => actor == "human",
        "agents" => actor == "agent",
        "automation" => actor == "automation",
        _ => true,
    }
}
