//! Local review decisions for the Footprint ledger.

use std::path::Path;
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use rusqlite::params;

use super::super::catalog::parse_day;
use super::super::spool::feature_id_valid;
use super::super::store::{
    begin_immediate, bounded_busy_timeout, open_write_store,
};
use super::super::wire::{
    require_member, DecideResultWire, FOOTPRINT_WIRE_SCHEMA_VERSION,
    KEEP_REASONS, REVIEW_DECISIONS,
};
use super::super::FootprintError;

/// Record, snooze, or clear a local review decision.
pub fn decide(
    store_path: &Path,
    feature_id: &str,
    decision: &str,
    reason: Option<&str>,
    today: &str,
    snooze_days: u32,
    busy_timeout: Duration,
) -> Result<DecideResultWire, FootprintError> {
    if !feature_id_valid(feature_id) {
        return Err(FootprintError::validation(format!(
            "invalid feature id {feature_id}"
        )));
    }
    let today = parse_day(today)?;
    let today_s = today.to_string();
    let reason = reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(reason) = reason.as_deref() {
        require_member(reason, KEEP_REASONS, "reason")?;
    }
    if decision == "clear" {
        return clear_decision(store_path, feature_id, busy_timeout);
    }
    require_member(decision, REVIEW_DECISIONS, "decision")?;
    let snooze_until = if decision == "later" {
        let until = today
            .checked_add_signed(ChronoDuration::days(i64::from(snooze_days)))
            .unwrap_or(today);
        Some(until.to_string())
    } else {
        None
    };
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_write_store(store_path, timeout)?;
    let tx = begin_immediate(&mut conn)?;
    tx.execute(
        r#"
        INSERT INTO reviews(
            feature_id, decision, reason, decided_day, snooze_until
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(feature_id) DO UPDATE SET
            decision = excluded.decision,
            reason = excluded.reason,
            decided_day = excluded.decided_day,
            snooze_until = excluded.snooze_until
        "#,
        params![feature_id, decision, reason, today_s, snooze_until,],
    )
    .map_err(FootprintError::from_sqlite)?;
    tx.commit().map_err(FootprintError::from_sqlite)?;
    Ok(DecideResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        feature_id: feature_id.to_string(),
        decision: Some(decision.to_string()),
        reason,
        decided_day: Some(today_s),
        snooze_until,
    })
}

fn clear_decision(
    store_path: &Path,
    feature_id: &str,
    busy_timeout: Duration,
) -> Result<DecideResultWire, FootprintError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_write_store(store_path, timeout)?;
    let tx = begin_immediate(&mut conn)?;
    tx.execute(
        "DELETE FROM reviews WHERE feature_id = ?1",
        params![feature_id],
    )
    .map_err(FootprintError::from_sqlite)?;
    tx.commit().map_err(FootprintError::from_sqlite)?;
    Ok(DecideResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        feature_id: feature_id.to_string(),
        decision: None,
        reason: None,
        decided_day: None,
        snooze_until: None,
    })
}
