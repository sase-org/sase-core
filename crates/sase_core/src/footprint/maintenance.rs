//! Prune and clear for the Footprint ledger.

use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use rusqlite::params;

use super::catalog::parse_day;
use super::fold::{list_spool_files, refresh_first_record_day};
use super::store::{
    begin_immediate, bounded_busy_timeout, meta_get_u64, meta_set,
    open_write_store,
};
use super::wire::{
    ClearResultWire, PruneResultWire, FOOTPRINT_WIRE_SCHEMA_VERSION,
};
use super::FootprintError;

/// Delete daily and coverage rows older than `retention_days` before `today`.
pub fn prune(
    store_path: &Path,
    today: &str,
    retention_days: u32,
    busy_timeout: Duration,
) -> Result<PruneResultWire, FootprintError> {
    let today = parse_day(today)?;
    let cutoff = today
        .checked_sub_signed(ChronoDuration::days(i64::from(retention_days)))
        .unwrap_or(today);
    let cutoff_s = cutoff.to_string();
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_write_store(store_path, timeout)?;
    let tx = begin_immediate(&mut conn)?;
    let daily_deleted = tx
        .execute("DELETE FROM daily WHERE day < ?1", params![cutoff_s])
        .map_err(FootprintError::from_sqlite)? as u64;
    let coverage_deleted =
        tx.execute("DELETE FROM coverage WHERE day < ?1", params![cutoff_s])
            .map_err(FootprintError::from_sqlite)? as u64;
    tx.commit().map_err(FootprintError::from_sqlite)?;
    refresh_first_record_day(&conn)?;
    Ok(PruneResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        daily_deleted,
        coverage_deleted,
    })
}

/// Delete data rows and spool files, then bump `meta.generation`.
pub fn clear(
    store_path: &Path,
    spool_dir: &Path,
    busy_timeout: Duration,
) -> Result<ClearResultWire, FootprintError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_write_store(store_path, timeout)?;
    let tx = begin_immediate(&mut conn)?;
    for table in [
        "daily",
        "coverage",
        "features",
        "catalog_revs",
        "fold_checkpoints",
        "reviews",
    ] {
        tx.execute(&format!("DELETE FROM {table}"), [])
            .map_err(FootprintError::from_sqlite)?;
    }
    tx.execute(
        "DELETE FROM meta WHERE key IN ('first_record_day', 'last_fold_at', 'catalog_rev', 'catalog_count')",
        [],
    )
    .map_err(FootprintError::from_sqlite)?;
    let generation = meta_get_u64(&tx, "generation")?.saturating_add(1);
    meta_set(&tx, "generation", &generation.to_string())?;
    tx.commit().map_err(FootprintError::from_sqlite)?;
    if spool_dir.exists() {
        for path in list_spool_files(spool_dir)? {
            let _ = fs::remove_file(path);
        }
    }
    let _ = conn.execute_batch("VACUUM");
    Ok(ClearResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        generation,
    })
}
