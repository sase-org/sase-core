//! `tool_run_node_summaries`: per-node history for the selected TUI node.
//!
//! A selector matches a run when `agent` is in `agents` or
//! `(owner_kind, owner_id)` is in `owners`, and `created_ts` is at or after
//! `since_ts`. An empty selector (no agents and no owners) matches nothing:
//! node scope is always an explicit identity, never the whole machine.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use super::super::store::connection::{
    unix_now, validate_schema, with_read_store,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::shared::{
    collect_lean, lean_projection_sql, lean_rows_for_ids,
    missing_store_diagnostic, placeholders, BatchContext, LeanRunRow,
};
use super::wire::{
    ToolRunNodeSelectorWire, ToolRunNodeSummariesRequestWire,
    ToolRunNodeSummariesResultWire, ToolRunNodeSummaryWire,
    TOOL_RUN_GLANCE_MAX_RUNS, TOOL_RUN_NODE_MAX_LIMIT,
    TOOL_RUN_NODE_MAX_SELECTORS, TOOL_RUN_SILENT_AFTER_SECONDS,
};

/// Window of newest matches each node scans for its one-per-label list.
/// `runs` is the first `per_node_limit` of this window.
const NODE_WINDOW: usize = 500;

fn node_filter(selector: &ToolRunNodeSelectorWire) -> (String, Vec<String>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    if selector.agents.is_empty() && selector.owners.is_empty() {
        return ("1 = 0".to_string(), values);
    }
    let mut attribution: Vec<String> = Vec::new();
    if !selector.agents.is_empty() {
        let placeholders = placeholders(selector.agents.len());
        attribution.push(format!("agent IN ({placeholders})"));
        values.extend(selector.agents.iter().cloned());
    }
    for owner in &selector.owners {
        attribution.push("(owner_kind = ? AND owner_id = ?)".to_string());
        values.push(owner.kind.clone());
        values.push(owner.id.clone());
    }
    clauses.push(format!("({})", attribution.join(" OR ")));
    if let Some(since_ts) = selector.since_ts {
        clauses.push("created_ts >= ?".to_string());
        values.push(since_ts.to_string());
    }
    (clauses.join(" AND "), values)
}

struct NodeWork {
    key: String,
    total_runs: u32,
    window_ids: Vec<String>,
    live_ids: Vec<String>,
    live_capped: bool,
}

/// Read-only per-node summaries. Never reconciles, never writes.
pub fn tool_run_node_summaries(
    store_path: &Path,
    request: ToolRunNodeSummariesRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunNodeSummariesResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.nodes.is_empty()
        || request.nodes.len() > TOOL_RUN_NODE_MAX_SELECTORS
    {
        return Err(ToolRunError::invalid(format!(
            "nodes must have 1..={TOOL_RUN_NODE_MAX_SELECTORS} entries"
        )));
    }
    if request.per_node_limit == 0
        || request.per_node_limit > TOOL_RUN_NODE_MAX_LIMIT
    {
        return Err(ToolRunError::invalid(format!(
            "per_node_limit must be 1..={TOOL_RUN_NODE_MAX_LIMIT}"
        )));
    }
    if !store_path.exists() {
        return Ok(ToolRunNodeSummariesResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: false,
            silent_after_s: TOOL_RUN_SILENT_AFTER_SECONDS,
            nodes: Vec::new(),
            diagnostics: vec![missing_store_diagnostic()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        let now = unix_now();
        let mut works: Vec<NodeWork> = Vec::with_capacity(request.nodes.len());
        for selector in &request.nodes {
            let (where_sql, values) = node_filter(selector);
            let total: i64 = conn.query_row(
                &format!("SELECT COUNT(*) FROM runs WHERE {where_sql}"),
                rusqlite::params_from_iter(values.iter()),
                |row| row.get(0),
            )?;
            let window_sql = lean_projection_sql(
                conn,
                &where_sql,
                &format!(
                    "ORDER BY created_ts DESC, run_id DESC LIMIT {NODE_WINDOW}"
                ),
            )?;
            let window_rows = collect_lean(conn, &window_sql, values.clone())?;
            let live_sql = lean_projection_sql(
                conn,
                &format!("({where_sql}) AND state IN ('created', 'running')"),
                &format!(
                    "ORDER BY created_ts DESC, run_id DESC LIMIT {}",
                    TOOL_RUN_GLANCE_MAX_RUNS as usize + 1
                ),
            )?;
            let live_rows = collect_lean(conn, &live_sql, values)?;
            works.push(NodeWork {
                key: selector.key.clone(),
                total_runs: total.max(0) as u32,
                window_ids: window_rows
                    .iter()
                    .map(|row| row.run_id.clone())
                    .collect(),
                live_ids: live_rows
                    .iter()
                    .take(TOOL_RUN_GLANCE_MAX_RUNS as usize)
                    .map(|row| row.run_id.clone())
                    .collect(),
                live_capped: live_rows.len()
                    > TOOL_RUN_GLANCE_MAX_RUNS as usize,
            });
        }
        let mut all_ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for work in &works {
            for id in work.window_ids.iter().chain(work.live_ids.iter()) {
                if seen.insert(id.clone()) {
                    all_ids.push(id.clone());
                }
            }
        }
        // One lean statement for every needed row, newest first.
        let lean_rows = lean_rows_for_ids(conn, &all_ids)?;
        let lean_by_id: HashMap<&str, &LeanRunRow> = lean_rows
            .iter()
            .map(|row| (row.run_id.as_str(), row))
            .collect();
        let mut context = BatchContext::load(conn, &all_ids, now)?;
        let mut nodes = Vec::with_capacity(works.len());
        for work in works {
            let mut live = Vec::new();
            for id in &work.live_ids {
                if let Some(row) = lean_by_id.get(id.as_str()) {
                    live.push(context.glance_for_row(conn, row)?);
                }
            }
            let mut runs = Vec::new();
            let mut latest_by_tool = Vec::new();
            let mut seen_labels: HashSet<String> = HashSet::new();
            for id in
                work.window_ids.iter().take(request.per_node_limit as usize)
            {
                if let Some(row) = lean_by_id.get(id.as_str()) {
                    let brief = context.brief_for_row(conn, row)?;
                    if seen_labels.insert(brief.label.clone()) {
                        latest_by_tool.push(brief.clone());
                    }
                    runs.push(brief);
                }
            }
            // Labels whose newest run sits deeper in the window still get
            // one entry, newest first.
            if work.window_ids.len() > request.per_node_limit as usize {
                for id in
                    work.window_ids.iter().skip(request.per_node_limit as usize)
                {
                    if let Some(row) = lean_by_id.get(id.as_str()) {
                        let brief = context.brief_for_row(conn, row)?;
                        if seen_labels.insert(brief.label.clone()) {
                            latest_by_tool.push(brief);
                        }
                    }
                }
            }
            nodes.push(ToolRunNodeSummaryWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                truncated: work.total_runs > runs.len() as u32
                    || work.live_capped,
                total_runs: work.total_runs,
                live,
                latest_by_tool,
                runs,
                key: work.key,
            });
        }
        Ok(ToolRunNodeSummariesResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: true,
            silent_after_s: TOOL_RUN_SILENT_AFTER_SECONDS,
            nodes,
            diagnostics: Vec::new(),
        })
    })
}
