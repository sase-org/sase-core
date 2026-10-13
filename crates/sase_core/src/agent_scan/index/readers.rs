//! Read-only no-migrate index probes and alias writes for Python callers
//! that currently open `agent_artifact_index.sqlite` with stdlib sqlite3.

use super::index_wire::{
    AgentArtifactAliasWire, AgentArtifactIndexProjectionWire,
};
use super::storage::{
    open_index, open_index_read_only_no_migrate, DEFAULT_INDEX_BUSY_TIMEOUT,
};
use rusqlite::{params, types::Value as SqlValue};
use serde_json::{Number, Value as JsonValue};
use std::path::Path;
use std::time::Duration;

/// Default busy timeout for the catalog and provenance projections.
pub const AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT: Duration =
    Duration::from_millis(250);

/// Real `agent_artifacts` columns that a projection may name.
///
/// Names are matched exactly and interpolated only after this check.
const AGENT_ARTIFACTS_COLUMNS: &[&str] = &[
    "agent_clan",
    "agent_clan_generation",
    "agent_meta_sig",
    "agent_name",
    "agent_session",
    "agent_type",
    "artifact_dir",
    "clan_summary",
    "clan_tribe",
    "cl_name",
    "done_outcome",
    "done_sig",
    "finished_at",
    "gate_turn_id",
    "has_done_marker",
    "has_running_marker",
    "has_waiting_marker",
    "has_workflow_state",
    "hidden",
    "imported_owner_machine",
    "indexed_at",
    "llm_provider",
    "macros_sig",
    "model",
    "model_alias_origin",
    "parent_timestamp",
    "pending_question_sig",
    "plan_path_sig",
    "project_dir",
    "project_file",
    "project_name",
    "projects_root",
    "prompt_steps_sig",
    "record_json",
    "retried_as_timestamp",
    "retry_attempt",
    "retry_chain_root_timestamp",
    "retry_of_timestamp",
    "running_sig",
    "source_machine",
    "started_at",
    "status",
    "step_index",
    "step_name",
    "timestamp",
    "waiting_sig",
    "workflow_dir_name",
    "workflow_name",
    "workflow_state_sig",
    "workflow_status",
];

/// Read the stored schema version without migrating or creating the file.
///
/// Returns `None` when the file is absent or unreadable, `0` when the
/// `meta` table or `schema_version` key is missing, and the parsed
/// integer otherwise.
pub fn read_agent_artifact_index_schema_version(
    index_path: &Path,
) -> Option<i64> {
    if !index_path.is_file() {
        return None;
    }
    let conn =
        open_index_read_only_no_migrate(index_path, DEFAULT_INDEX_BUSY_TIMEOUT)
            .ok()?;
    match conn.query_row(
        "SELECT value FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    ) {
        Ok(raw) => Some(parse_schema_version_int(&raw)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Some(0),
        Err(err) => {
            let message = err.to_string().to_ascii_lowercase();
            if message.contains("no such table") {
                Some(0)
            } else {
                None
            }
        }
    }
}

/// Load selected `agent_artifacts` columns without migrating.
pub fn load_agent_artifact_index_projection<S: AsRef<str>>(
    index_path: &Path,
    columns: &[S],
    busy_timeout: Duration,
) -> Result<AgentArtifactIndexProjectionWire, String> {
    if columns.is_empty() {
        return Err("columns must not be empty".to_string());
    }
    let mut quoted = Vec::with_capacity(columns.len());
    for column in columns {
        let name = column.as_ref();
        if !AGENT_ARTIFACTS_COLUMNS.contains(&name) {
            return Err(format!("unsupported agent_artifacts column {name:?}"));
        }
        quoted.push(name);
    }
    let conn = open_index_read_only_no_migrate(index_path, busy_timeout)?;
    let schema_version = stored_schema_version(&conn);
    let sql = format!("SELECT {} FROM agent_artifacts", quoted.join(", "));
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows_iter = stmt.query([]).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    while let Some(row) = rows_iter.next().map_err(|e| e.to_string())? {
        let mut values = Vec::with_capacity(quoted.len());
        for index in 0..quoted.len() {
            let value: SqlValue = row.get(index).map_err(|e| e.to_string())?;
            values.push(sqlite_value_to_json(value)?);
        }
        rows.push(values);
    }
    Ok(AgentArtifactIndexProjectionWire {
        schema_version,
        rows,
    })
}

/// Insert or replace one layout alias using the migrating read-write open.
pub fn upsert_agent_artifact_alias(
    index_path: &Path,
    alias_path: &str,
    artifact_dir: &str,
) -> Result<(), String> {
    let conn = open_index(index_path)?;
    conn.execute(
        "INSERT OR REPLACE INTO agent_artifact_aliases(alias_path, artifact_dir) \
         VALUES (?1, ?2)",
        params![alias_path, artifact_dir],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Delete aliases matching `alias_path` or `artifact_dir`.
///
/// A missing index file is a no-op, matching the Python layout CLI.
pub fn delete_agent_artifact_alias(
    index_path: &Path,
    alias_path: &str,
    artifact_dir: &str,
) -> Result<(), String> {
    if !index_path.exists() {
        return Ok(());
    }
    let conn = open_index(index_path)?;
    conn.execute(
        "DELETE FROM agent_artifact_aliases \
         WHERE alias_path = ?1 OR artifact_dir = ?2",
        params![alias_path, artifact_dir],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// List layout aliases. A missing index file yields an empty list.
pub fn list_agent_artifact_aliases(
    index_path: &Path,
) -> Result<Vec<AgentArtifactAliasWire>, String> {
    if !index_path.exists() {
        return Ok(Vec::new());
    }
    let conn = open_index(index_path)?;
    let mut stmt = conn
        .prepare(
            "SELECT alias_path, artifact_dir FROM agent_artifact_aliases \
             ORDER BY alias_path",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut aliases = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        aliases.push(AgentArtifactAliasWire {
            alias_path: row.get(0).map_err(|e| e.to_string())?,
            artifact_dir: row.get(1).map_err(|e| e.to_string())?,
        });
    }
    Ok(aliases)
}

fn stored_schema_version(conn: &rusqlite::Connection) -> i64 {
    match conn.query_row(
        "SELECT value FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    ) {
        Ok(raw) => parse_schema_version_int(&raw),
        Err(_) => 0,
    }
}

fn parse_schema_version_int(raw: &str) -> i64 {
    raw.trim().parse::<i64>().unwrap_or(0)
}

fn sqlite_value_to_json(value: SqlValue) -> Result<JsonValue, String> {
    match value {
        SqlValue::Null => Ok(JsonValue::Null),
        SqlValue::Integer(value) => Ok(JsonValue::Number(value.into())),
        SqlValue::Real(value) => Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| "non-finite REAL column value".to_string()),
        SqlValue::Text(value) => Ok(JsonValue::String(value)),
        SqlValue::Blob(_) => {
            Err("unsupported BLOB column in artifact-index projection".into())
        }
    }
}
