//! `tool_run_live_glance`: every unsettled run on the machine, newest
//! first, capped so a pile of unreconciled zombies stays bounded.

use std::path::Path;
use std::time::Duration;

use super::super::store::connection::{
    unix_now, validate_schema, with_read_store,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::shared::{
    collect_lean, lean_projection_sql, missing_store_diagnostic,
    read_last_write_ts, BatchContext,
};
use super::wire::{
    ToolRunLiveGlanceRequestWire, ToolRunLiveGlanceResultWire,
    TOOL_RUN_GLANCE_MAX_RUNS, TOOL_RUN_SILENT_AFTER_SECONDS,
};

/// Read-only glance over unsettled runs. Never reconciles, never writes.
pub fn tool_run_live_glance(
    store_path: &Path,
    request: ToolRunLiveGlanceRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunLiveGlanceResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    if !store_path.exists() {
        return Ok(ToolRunLiveGlanceResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: false,
            last_write_ts: None,
            silent_after_s: TOOL_RUN_SILENT_AFTER_SECONDS,
            truncated: false,
            runs: Vec::new(),
            diagnostics: vec![missing_store_diagnostic()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        let cap = TOOL_RUN_GLANCE_MAX_RUNS as usize + 1;
        let sql = lean_projection_sql(
            conn,
            "state IN ('created', 'running')",
            &format!("ORDER BY created_ts DESC, run_id DESC LIMIT {cap}"),
        )?;
        let mut rows = collect_lean(conn, &sql, Vec::new())?;
        let truncated = rows.len() > TOOL_RUN_GLANCE_MAX_RUNS as usize;
        rows.truncate(TOOL_RUN_GLANCE_MAX_RUNS as usize);
        let ids: Vec<String> =
            rows.iter().map(|row| row.run_id.clone()).collect();
        let mut context = BatchContext::load(conn, &ids, now)?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in &rows {
            runs.push(context.glance_for_row(conn, row)?);
        }
        Ok(ToolRunLiveGlanceResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: true,
            last_write_ts: read_last_write_ts(conn),
            silent_after_s: TOOL_RUN_SILENT_AFTER_SECONDS,
            truncated,
            runs,
            diagnostics: Vec::new(),
        })
    })
}
