//! Status snapshot for the Footprint ledger.

use std::path::Path;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate};
use rusqlite::params;

use super::catalog::parse_day;
use super::fold::list_spool_files;
use super::store::{
    bounded_busy_timeout, meta_get, meta_get_u64, open_read_store,
    open_write_store, store_size,
};
use super::wire::{
    CoverageRowWire, SpoolPendingWire, StatusWire,
    FOOTPRINT_WIRE_SCHEMA_VERSION,
};
use super::FootprintError;

/// Build the status snapshot. Creates the store if it is missing so schema
/// and generation are defined; spool stats still come from the filesystem.
pub fn status(
    store_path: &Path,
    spool_dir: &Path,
    today: &str,
    busy_timeout: Duration,
) -> Result<StatusWire, FootprintError> {
    let today = parse_day(today)?;
    let timeout = bounded_busy_timeout(busy_timeout);
    if !store_path.exists() {
        open_write_store(store_path, timeout)?;
    }
    let conn = open_read_store(store_path, timeout)?;
    let generation = meta_get_u64(&conn, "generation")?;
    let first_record_day = meta_get(&conn, "first_record_day")?;
    let last_fold_at = meta_get(&conn, "last_fold_at")?;
    let catalog_rev = meta_get(&conn, "catalog_rev")?;
    let catalog_count = meta_get_u64(&conn, "catalog_count")?;
    let coverage = load_recent_coverage(&conn, today)?;
    let (torn_total, dropped_total, unknown_ids_total) = fault_totals(&conn)?;
    let pending = pending_spool(&conn, spool_dir)?;
    let spool_bytes_pending: u64 =
        pending.iter().map(|file| file.bytes_pending).sum();
    Ok(StatusWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        generation,
        first_record_day,
        last_fold_at,
        spool_files_pending: pending.len() as u64,
        spool_bytes_pending,
        spool_pending: pending,
        ledger_bytes: store_size(store_path),
        catalog_rev,
        catalog_count,
        coverage,
        torn_total,
        dropped_total,
        unknown_ids_total,
    })
}

fn load_recent_coverage(
    conn: &rusqlite::Connection,
    today: NaiveDate,
) -> Result<Vec<CoverageRowWire>, FootprintError> {
    let start = today
        .checked_sub_signed(ChronoDuration::days(6))
        .unwrap_or(today);
    let mut stmt = conn
        .prepare(
            r#"
            SELECT day, surface, origin, measured, human_active, records,
                   torn, dropped, unknown_ids, incomplete
            FROM coverage
            WHERE day >= ?1 AND day <= ?2
            ORDER BY day, surface, origin
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map(params![start.to_string(), today.to_string()], |row| {
            Ok(CoverageRowWire {
                day: row.get(0)?,
                surface: row.get(1)?,
                origin: row.get(2)?,
                measured: row.get(3)?,
                human_active: row.get(4)?,
                records: row.get(5)?,
                torn: row.get(6)?,
                dropped: row.get(7)?,
                unknown_ids: row.get(8)?,
                incomplete: row.get(9)?,
            })
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut coverage = Vec::new();
    for row in rows {
        coverage.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(coverage)
}

fn fault_totals(
    conn: &rusqlite::Connection,
) -> Result<(u64, u64, u64), FootprintError> {
    conn.query_row(
        "SELECT COALESCE(SUM(torn), 0), COALESCE(SUM(dropped), 0), COALESCE(SUM(unknown_ids), 0) FROM coverage",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
            ))
        },
    )
    .map_err(FootprintError::from_sqlite)
}

fn pending_spool(
    conn: &rusqlite::Connection,
    spool_dir: &Path,
) -> Result<Vec<SpoolPendingWire>, FootprintError> {
    let files = list_spool_files(spool_dir)?;
    let mut pending = Vec::new();
    for path in files {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let size = std::fs::metadata(&path)?.len();
        let offset: u64 = conn
            .query_row(
                "SELECT offset FROM fold_checkpoints WHERE file = ?1",
                params![name],
                |row| row.get::<_, i64>(0),
            )
            .ok()
            .map(|offset| offset as u64)
            .unwrap_or(0);
        if offset < size {
            pending.push(SpoolPendingWire {
                file: name,
                bytes_pending: size - offset,
            });
        }
    }
    Ok(pending)
}
