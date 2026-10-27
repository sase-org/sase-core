//! Idempotent derived-origin upserts.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use rusqlite::params;

use super::catalog::{known_feature_ids, parse_day};
use super::spool::feature_id_valid;
use super::store::{begin_immediate, bounded_busy_timeout, open_write_store};
use super::wire::{
    remap_unknown_id, require_member, DerivedCoverageRowWire,
    DerivedDailyRowWire, ACTORS, ORIGINS, OUTCOMES, SURFACES, VIAS,
};
use super::FootprintError;

/// Replace exactly `origin`'s rows for `days`, then insert `rows`/`coverage`.
pub fn upsert_derived(
    store_path: &Path,
    origin: &str,
    days: &[String],
    rows: &[DerivedDailyRowWire],
    coverage: &[DerivedCoverageRowWire],
    busy_timeout: Duration,
) -> Result<(), FootprintError> {
    require_member(origin, ORIGINS, "origin")?;
    if origin == "live" {
        return Err(FootprintError::validation(
            "derived upsert origin must be a derived.* value",
        ));
    }
    for day in days {
        parse_day(day)?;
    }
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_write_store(store_path, timeout)?;
    let known = known_feature_ids(&conn)?;
    let tx = begin_immediate(&mut conn)?;
    for day in days {
        tx.execute(
            "DELETE FROM daily WHERE origin = ?1 AND day = ?2",
            params![origin, day],
        )
        .map_err(FootprintError::from_sqlite)?;
        tx.execute(
            "DELETE FROM coverage WHERE origin = ?1 AND day = ?2",
            params![origin, day],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    let day_set: BTreeSet<&str> = days.iter().map(String::as_str).collect();
    for row in rows {
        if !day_set.contains(row.day.as_str()) {
            return Err(FootprintError::validation(format!(
                "derived row day {} is not in the replaced day set",
                row.day
            )));
        }
        validate_daily_row(row)?;
        let mut feature_id = row.feature_id.clone();
        if !known.contains(&feature_id) {
            feature_id = remap_unknown_id(&feature_id);
        }
        tx.execute(
            r#"
            INSERT INTO daily(
                day, feature_id, actor, via, outcome, origin,
                n, dur_ms_sum, dur_ms_max
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(day, feature_id, actor, via, outcome, origin)
            DO UPDATE SET
                n = excluded.n,
                dur_ms_sum = excluded.dur_ms_sum,
                dur_ms_max = excluded.dur_ms_max
            "#,
            params![
                row.day,
                feature_id,
                row.actor,
                row.via,
                row.outcome,
                origin,
                row.n,
                row.dur_ms_sum,
                row.dur_ms_max
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    for row in coverage {
        if !day_set.contains(row.day.as_str()) {
            return Err(FootprintError::validation(format!(
                "derived coverage day {} is not in the replaced day set",
                row.day
            )));
        }
        parse_day(&row.day)?;
        require_member(&row.surface, SURFACES, "surface")?;
        tx.execute(
            r#"
            INSERT INTO coverage(
                day, surface, origin, measured, human_active, records,
                torn, dropped, unknown_ids, incomplete
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(day, surface, origin) DO UPDATE SET
                measured = excluded.measured,
                human_active = excluded.human_active,
                records = excluded.records,
                torn = excluded.torn,
                dropped = excluded.dropped,
                unknown_ids = excluded.unknown_ids,
                incomplete = excluded.incomplete
            "#,
            params![
                row.day,
                row.surface,
                origin,
                row.measured,
                row.human_active,
                row.records,
                row.torn,
                row.dropped,
                row.unknown_ids,
                row.incomplete
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    tx.commit().map_err(FootprintError::from_sqlite)?;
    super::fold::refresh_first_record_day(&conn)?;
    Ok(())
}

fn validate_daily_row(row: &DerivedDailyRowWire) -> Result<(), FootprintError> {
    parse_day(&row.day)?;
    if !feature_id_valid(&row.feature_id) {
        return Err(FootprintError::validation(format!(
            "derived feature id {} is invalid",
            row.feature_id
        )));
    }
    require_member(&row.actor, ACTORS, "actor")?;
    require_member(&row.via, VIAS, "via")?;
    require_member(&row.outcome, OUTCOMES, "outcome")?;
    if row.n < 0 || row.dur_ms_sum < 0 || row.dur_ms_max < 0 {
        return Err(FootprintError::validation(
            "derived counts and durations must be non-negative",
        ));
    }
    Ok(())
}
