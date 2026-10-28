//! Compact launch-boundary macro usage for Footprint derived rows.
//!
//! Walks the artifact index once and emits identity-free rows: launch
//! timestamp, macro name, whether the launch had a parent agent, and the
//! reference count. Callers bucket timestamps into local calendar days.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::agent_scan::AgentArtifactRecordWire;

use super::super::wire::{
    MacroUsageResponseWire, MacroUsageRowWire, MACRO_USAGE_WIRE_SCHEMA_VERSION,
};
use super::lifecycle::{launch_timestamp, record_is_user_hidden};
use super::types::{IndexRunRow, INDEX_BUSY_TIMEOUT};

/// Return every launch-boundary macro reference in the artifact index.
pub fn query_macro_usage(
    index_path: &Path,
) -> Result<MacroUsageResponseWire, String> {
    let conn = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        format!(
            "failed to open agent artifact index {}: {error}",
            index_path.display()
        )
    })?;
    conn.busy_timeout(INDEX_BUSY_TIMEOUT)
        .map_err(|error| error.to_string())?;

    let mut statement = conn
        .prepare(
            r#"
            SELECT project_name, workflow_dir_name, workflow_name, timestamp,
                   status, cl_name, agent_name, model, llm_provider,
                   started_at, finished_at, record_json
            FROM agent_artifacts
            ORDER BY timestamp ASC
            "#,
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok(IndexRunRow {
                project_name: row.get(0)?,
                workflow_dir_name: row.get(1)?,
                workflow_name: row.get(2)?,
                timestamp: row.get(3)?,
                status: row.get(4)?,
                cl_name: row.get(5)?,
                agent_name: row.get(6)?,
                model: row.get(7)?,
                provider: row.get(8)?,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
                record_json: row.get(11)?,
            })
        })
        .map_err(|error| error.to_string())?;

    let mut response = MacroUsageResponseWire {
        schema_version: MACRO_USAGE_WIRE_SCHEMA_VERSION,
        rows: Vec::new(),
        malformed_rows_skipped: 0,
    };
    for row in rows {
        let row = row.map_err(|error| error.to_string())?;
        let Ok(record) =
            serde_json::from_str::<AgentArtifactRecordWire>(&row.record_json)
        else {
            response.malformed_rows_skipped += 1;
            continue;
        };
        if record_is_user_hidden(&record) || record.used_macros.is_empty() {
            continue;
        }
        let Some(launch_ts) = launch_timestamp(&row) else {
            continue;
        };
        let has_parent = has_parent_agent(&record);
        let ts = launch_ts.round() as i64;
        for used in &record.used_macros {
            if used.name.is_empty() {
                continue;
            }
            response.rows.push(MacroUsageRowWire {
                ts,
                name: used.name.clone(),
                has_parent,
                n: used.references.max(1),
            });
        }
    }
    Ok(response)
}

fn has_parent_agent(record: &AgentArtifactRecordWire) -> bool {
    record.agent_meta.as_ref().is_some_and(|meta| {
        non_empty(&meta.parent_agent_name)
            || non_empty(&meta.parent_agent_timestamp)
            || non_empty(&meta.parent_timestamp)
    })
}

fn non_empty(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|text| !text.trim().is_empty())
}
