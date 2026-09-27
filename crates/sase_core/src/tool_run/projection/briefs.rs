//! `tool_run_briefs`: the lean filtered run list the Admin pane uses.

use std::path::Path;
use std::time::Duration;

use super::super::store::connection::{
    unix_now, validate_schema, with_read_store,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::shared::{
    collect_lean, format_cursor, lean_projection_sql, missing_store_diagnostic,
    parse_cursor, placeholders, BatchContext,
};
use super::wire::{
    ToolRunBriefsRequestWire, ToolRunBriefsResultWire,
    TOOL_RUN_BRIEFS_MAX_LIMIT,
};

/// Read-only filtered brief list, newest first. Never reconciles.
pub fn tool_run_briefs(
    store_path: &Path,
    request: ToolRunBriefsRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunBriefsResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.limit == 0 || request.limit > TOOL_RUN_BRIEFS_MAX_LIMIT {
        return Err(ToolRunError::invalid(format!(
            "limit must be 1..={TOOL_RUN_BRIEFS_MAX_LIMIT}"
        )));
    }
    if !store_path.exists() {
        return Ok(ToolRunBriefsResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: false,
            runs: Vec::new(),
            next_cursor: None,
            diagnostics: vec![missing_store_diagnostic()],
        });
    }
    let limit = request.limit;
    with_read_store(store_path, busy_timeout, |conn| {
        let now = unix_now();
        let mut clauses: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();
        if let Some(project) = &request.project {
            clauses.push("project = ?".to_string());
            values.push(project.clone());
        }
        if let Some(tool) = &request.tool {
            clauses.push("tool_name = ?".to_string());
            values.push(tool.clone());
        }
        if !request.states.is_empty() {
            let placeholders = placeholders(request.states.len());
            clauses.push(format!("state IN ({placeholders})"));
            for state in &request.states {
                values.push(state.as_str().to_string());
            }
        }
        if !request.agents.is_empty() || !request.owners.is_empty() {
            let mut attribution: Vec<String> = Vec::new();
            if !request.agents.is_empty() {
                let placeholders = placeholders(request.agents.len());
                attribution.push(format!("agent IN ({placeholders})"));
                values.extend(request.agents.iter().cloned());
            }
            for owner in &request.owners {
                attribution
                    .push("(owner_kind = ? AND owner_id = ?)".to_string());
                values.push(owner.kind.clone());
                values.push(owner.id.clone());
            }
            clauses.push(format!("({})", attribution.join(" OR ")));
        }
        if let Some(since_ts) = request.since_ts {
            clauses.push("created_ts >= ?".to_string());
            values.push(since_ts.to_string());
        }
        if let Some(cursor) = &request.cursor {
            let (ts, run_id) = parse_cursor(cursor)?;
            clauses.push(
                "(created_ts < ? OR (created_ts = ? AND run_id < ?))"
                    .to_string(),
            );
            values.push(ts.to_string());
            values.push(ts.to_string());
            values.push(run_id);
        }
        let where_sql = if clauses.is_empty() {
            "1 = 1".to_string()
        } else {
            clauses.join(" AND ")
        };
        let fetch = limit as usize + 1;
        let sql = lean_projection_sql(
            conn,
            &where_sql,
            &format!("ORDER BY created_ts DESC, run_id DESC LIMIT {fetch}"),
        )?;
        let mut rows = collect_lean(conn, &sql, values)?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next_cursor = if has_more {
            rows.last()
                .map(|row| format_cursor(row.created_ts, &row.run_id))
        } else {
            None
        };
        let ids: Vec<String> =
            rows.iter().map(|row| row.run_id.clone()).collect();
        let mut context = BatchContext::load(conn, &ids, now)?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in &rows {
            runs.push(context.brief_for_row(conn, row)?);
        }
        Ok(ToolRunBriefsResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: true,
            runs,
            next_cursor,
            diagnostics: Vec::new(),
        })
    })
}
