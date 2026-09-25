use super::index_wire::AGENT_ARTIFACT_INDEX_SCHEMA_VERSION;
use super::maintenance::{
    upsert_model_aliases_for_record, upsert_output_variables_for_record,
};
use super::record_summary::{
    gate_shell_id_from_record, machine_projection_from_marker_files,
    machine_projection_from_record, source_machine_from_marker_files,
    source_machine_from_record, MachineProjection, RecordSummary,
};
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const DEFAULT_INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn open_index(index_path: &Path) -> Result<Connection, String> {
    open_index_with_busy_timeout(index_path, DEFAULT_INDEX_BUSY_TIMEOUT)
}

pub(super) fn open_index_with_busy_timeout(
    index_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, String> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut conn = Connection::open(index_path).map_err(|e| e.to_string())?;
    conn.busy_timeout(busy_timeout).map_err(|e| e.to_string())?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS agent_artifacts (
            artifact_dir TEXT PRIMARY KEY,
            projects_root TEXT NOT NULL,
            project_name TEXT NOT NULL,
            project_dir TEXT NOT NULL,
            project_file TEXT NOT NULL,
            workflow_dir_name TEXT NOT NULL,
            workflow_name TEXT,
            agent_clan TEXT,
            agent_clan_generation TEXT,
            clan_tribe TEXT,
            clan_summary TEXT,
            agent_session TEXT,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            agent_type TEXT NOT NULL,
            cl_name TEXT,
            agent_name TEXT,
            model TEXT,
            llm_provider TEXT,
            started_at TEXT,
            finished_at REAL,
            done_outcome TEXT,
            source_machine TEXT,
            imported_owner_machine TEXT,
            gate_shell_id TEXT,
            has_done_marker INTEGER NOT NULL,
            has_running_marker INTEGER NOT NULL,
            has_waiting_marker INTEGER NOT NULL,
            has_workflow_state INTEGER NOT NULL,
            workflow_status TEXT,
            hidden INTEGER NOT NULL,
            parent_timestamp TEXT,
            step_index INTEGER,
            step_name TEXT,
            retry_of_timestamp TEXT,
            retried_as_timestamp TEXT,
            retry_chain_root_timestamp TEXT,
            retry_attempt INTEGER,
            agent_meta_sig TEXT,
            done_sig TEXT,
            running_sig TEXT,
            waiting_sig TEXT,
            pending_question_sig TEXT,
            workflow_state_sig TEXT,
            plan_path_sig TEXT,
            prompt_steps_sig TEXT,
            xprompts_sig TEXT,
            record_json TEXT NOT NULL,
            indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_active
            ON agent_artifacts(hidden, has_done_marker, workflow_status, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_recent_completed
            ON agent_artifacts(hidden, has_done_marker, finished_at, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_cl_name
            ON agent_artifacts(cl_name);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_project_workflow
            ON agent_artifacts(project_name, workflow_dir_name, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_workflow_name
            ON agent_artifacts(workflow_name, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_parent_timestamp
            ON agent_artifacts(project_name, workflow_dir_name, parent_timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_retry_of_timestamp
            ON agent_artifacts(project_name, workflow_dir_name, retry_of_timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_retried_as_timestamp
            ON agent_artifacts(project_name, workflow_dir_name, retried_as_timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_retry_chain_root_timestamp
            ON agent_artifacts(project_name, workflow_dir_name, retry_chain_root_timestamp);
        CREATE TABLE IF NOT EXISTS dismissed_agents (
            agent_type TEXT NOT NULL,
            cl_name TEXT NOT NULL,
            raw_suffix TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (agent_type, cl_name, raw_suffix)
        );
        -- Covers dismissed-suffix lookups used by visibility filters and
        -- the set-based agent-session-dismissal reconcile (no schema bump: this
        -- index already existed before the N+1 rewrite).
        CREATE INDEX IF NOT EXISTS idx_dismissed_agents_suffix
            ON dismissed_agents(raw_suffix, cl_name, agent_type);
        CREATE TABLE IF NOT EXISTS agent_artifact_aliases (
            alias_path TEXT PRIMARY KEY,
            artifact_dir TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_agent_artifact_aliases_artifact_dir
            ON agent_artifact_aliases(artifact_dir);
        CREATE TABLE IF NOT EXISTS agent_output_variables (
            artifact_dir TEXT NOT NULL,
            variable_key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            value_scalar_text TEXT,
            projects_root TEXT NOT NULL,
            project_name TEXT NOT NULL,
            project_dir TEXT NOT NULL,
            project_file TEXT NOT NULL,
            workflow_dir_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            agent_name TEXT,
            cl_name TEXT,
            hidden INTEGER NOT NULL,
            has_done_marker INTEGER NOT NULL,
            finished_at REAL,
            status TEXT NOT NULL,
            agent_type TEXT NOT NULL,
            indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (artifact_dir, variable_key),
            FOREIGN KEY (artifact_dir)
                REFERENCES agent_artifacts(artifact_dir)
                ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_agent_output_variables_recent_key
            ON agent_output_variables(variable_key, timestamp, project_name, artifact_dir);
        CREATE INDEX IF NOT EXISTS idx_agent_output_variables_agent_key_time
            ON agent_output_variables(agent_name, variable_key, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_output_variables_project_time
            ON agent_output_variables(project_name, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_output_variables_value_json
            ON agent_output_variables(variable_key, value_json);
        CREATE TABLE IF NOT EXISTS agent_artifact_model_aliases (
            artifact_dir TEXT NOT NULL,
            alias        TEXT NOT NULL,
            position     INTEGER NOT NULL,
            PRIMARY KEY (artifact_dir, alias),
            FOREIGN KEY (artifact_dir)
                REFERENCES agent_artifacts(artifact_dir)
                ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_agent_artifact_model_aliases_alias
            ON agent_artifact_model_aliases(alias, artifact_dir);
        "#,
    )
    .map_err(|e| e.to_string())?;

    let prior_version: Option<u32> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok());

    if prior_version.is_some_and(|v| v < 2) {
        migrate_recompute_hidden_v2(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 3) {
        ensure_agent_artifacts_column(&conn, "pending_question_sig", "TEXT")?;
    }
    if prior_version.is_none_or(|v| v < 4) {
        ensure_agent_artifacts_column(&conn, "workflow_name", "TEXT")?;
        ensure_agent_artifacts_column(
            &conn,
            super::AGENT_SESSION_INDEX_COLUMN,
            "TEXT",
        )?;
    }
    if prior_version.is_none_or(|v| v < 5) {
        migrate_record_json_refresh_v5(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 6) {
        migrate_record_json_refresh_v6(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 7) {
        migrate_record_json_refresh_v7(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 8) {
        migrate_record_json_refresh_v8(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 9) {
        migrate_record_json_refresh_v9(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 10) {
        migrate_record_json_refresh_v10(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 11) {
        ensure_agent_artifacts_column(&conn, "agent_clan", "TEXT")?;
        migrate_record_json_refresh_v11(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 12) {
        migrate_record_json_refresh_v12(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 13) {
        migrate_record_json_refresh_v13(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 14) {
        migrate_record_json_refresh_v14(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 15) {
        ensure_agent_artifacts_column(&conn, "agent_clan_generation", "TEXT")?;
        ensure_agent_artifacts_column(&conn, "clan_tribe", "TEXT")?;
        ensure_agent_artifacts_column(&conn, "clan_summary", "TEXT")?;
        migrate_clan_context_projection_v15(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 16) {
        migrate_record_json_refresh_v16(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 18) {
        migrate_record_json_refresh_v18(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 19) {
        ensure_agent_artifacts_column(&conn, "xprompts_sig", "TEXT")?;
        migrate_record_json_refresh_v19(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 20) {
        migrate_record_json_refresh_v20(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 21) {
        migrate_output_variable_projection_v21(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 22) {
        ensure_agent_artifacts_column(&conn, "model_alias_origin", "TEXT")?;
        migrate_model_alias_projection_v22(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 23) {
        migrate_record_json_refresh_v23(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 24) {
        ensure_agent_artifacts_column(&conn, "done_outcome", "TEXT")?;
        migrate_done_outcome_projection_v24(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 26) {
        migrate_record_json_refresh_v26(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 27) {
        migrate_record_json_refresh_v27(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 28) {
        ensure_agent_artifacts_column(&conn, "source_machine", "TEXT")?;
        migrate_source_machine_projection_v28(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 29) {
        migrate_record_json_refresh_v29(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 30) {
        ensure_agent_artifacts_column(&conn, "imported_owner_machine", "TEXT")?;
        migrate_imported_owner_machine_projection_v30(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 31) {
        ensure_agent_artifacts_column(&conn, "gate_shell_id", "TEXT")?;
        migrate_gate_shell_id_projection_v31(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 32) {
        migrate_record_json_refresh_v32(&mut conn)?;
    }
    if prior_version.is_none_or(|v| v < 33) {
        migrate_agent_session_column_v33(&conn)?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent_session \
         ON agent_artifacts(agent_session, timestamp); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent_clan \
         ON agent_artifacts(agent_clan, timestamp); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_done_outcome \
         ON agent_artifacts(done_outcome); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_source_machine \
         ON agent_artifacts(source_machine); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_imported_owner_machine \
         ON agent_artifacts(imported_owner_machine); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_clan_context \
         ON agent_artifacts(agent_clan, agent_clan_generation, timestamp); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_gate_shell_id \
         ON agent_artifacts(gate_shell_id, project_name, timestamp);",
    )
    .map_err(|e| e.to_string())?;

    // Every open used to rewrite this row unconditionally, including opens
    // from callers that only ever read. Skip the write once the stored
    // version already matches so a no-op open is actually a no-op.
    if prior_version != Some(AGENT_ARTIFACT_INDEX_SCHEMA_VERSION) {
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
            [AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string()],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(conn)
}

/// Open the index for a query path that never writes.
///
/// `open_index` unconditionally opens READ_WRITE|CREATE, replays every
/// `CREATE TABLE/INDEX IF NOT EXISTS` statement, and re-writes the
/// `schema_version` row on every call, even for a logically read-only
/// query. None of that belongs on a read path. Callers that only ever
/// select rows should use this instead; callers that may revalidate or
/// otherwise write must keep using `open_index`.
///
/// Falls back to `open_index` when the index file does not exist yet,
/// since a read-only connection cannot create it and the first caller
/// needs a valid (empty) schema to query against.
pub(super) fn open_index_read_only(
    index_path: &Path,
) -> Result<Connection, String> {
    if !index_path.exists() {
        return open_index(index_path);
    }
    let conn = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|e| e.to_string())?;
    conn.busy_timeout(DEFAULT_INDEX_BUSY_TIMEOUT)
        .map_err(|e| e.to_string())?;
    // A read-only connection cannot run migrations. If the on-disk schema
    // is anything other than current (stale, corrupt, or not yet
    // created), fall back to the migrating read-write open so a query
    // path never reads against an un-migrated schema.
    if read_index_schema_version(&conn).ok()
        != Some(AGENT_ARTIFACT_INDEX_SCHEMA_VERSION)
    {
        drop(conn);
        return open_index(index_path);
    }
    Ok(conn)
}

pub(super) fn read_index_schema_version(
    conn: &Connection,
) -> Result<u32, String> {
    let raw: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    raw.parse::<u32>().map_err(|e| e.to_string())
}

pub(super) fn resolve_index_artifact_dir(
    conn: &Connection,
    artifact_dir: &str,
) -> Result<String, String> {
    conn.query_row(
        "SELECT artifact_dir FROM agent_artifact_aliases WHERE alias_path = ?1",
        [artifact_dir],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|e| e.to_string())
    .map(|value| value.unwrap_or_else(|| artifact_dir.to_string()))
}

pub(super) fn count_table_rows(
    conn: &Connection,
    table: &str,
) -> Result<u64, String> {
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(|e| e.to_string())?;
    u64::try_from(count).map_err(|e| e.to_string())
}

fn agent_artifacts_has_column(
    conn: &Connection,
    column: &str,
) -> Result<bool, String> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(agent_artifacts)")
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let existing: String = row.get(1).map_err(|e| e.to_string())?;
        if existing == column {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn ensure_agent_artifacts_column(
    conn: &Connection,
    column: &str,
    column_type: &str,
) -> Result<(), String> {
    if agent_artifacts_has_column(conn, column)? {
        return Ok(());
    }
    conn.execute(
        &format!(
            "ALTER TABLE agent_artifacts ADD COLUMN {column} {column_type}"
        ),
        [],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) fn open_index_for_rebuild(
    index_path: &Path,
) -> Result<Connection, String> {
    match open_index(index_path) {
        Ok(conn) => Ok(conn),
        Err(err)
            if index_path.exists()
                && is_sqlite_index_corruption_error(&err) =>
        {
            replace_unusable_index_file(index_path)?;
            open_index(index_path).map_err(|retry_err| {
                format!(
                    "{retry_err} (after replacing corrupt artifact index: {err})"
                )
            })
        }
        Err(err) => Err(err),
    }
}

pub(super) fn is_sqlite_index_corruption_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database disk image is malformed")
        || lower.contains("file is not a database")
        || lower.contains("not a database")
        || lower.contains("malformed database schema")
        || lower.contains("unsupported file format")
}

pub(super) fn replace_unusable_index_file(
    index_path: &Path,
) -> Result<(), String> {
    let quarantined = corrupt_index_quarantine_path(index_path);
    match fs::rename(index_path, &quarantined) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(format!(
                "failed to quarantine unusable artifact index {}: {err}",
                index_path.display()
            ));
        }
    }
    for suffix in ["-wal", "-shm"] {
        let sidecar = sqlite_sidecar_path(index_path, suffix);
        let quarantined_sidecar = sqlite_sidecar_path(&quarantined, suffix);
        match fs::rename(&sidecar, &quarantined_sidecar) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "failed to quarantine unusable artifact index sidecar {}: {err}",
                    sidecar.display()
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn corrupt_index_quarantine_path(index_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = index_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "agent_artifact_index.sqlite".into());
    index_path.with_file_name(format!("{file_name}.corrupt-{nanos}"))
}

pub(super) fn sqlite_sidecar_path(index_path: &Path, suffix: &str) -> PathBuf {
    let mut raw = index_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

/// One-shot v1 → v2 migration: recompute `hidden` for previously-indexed
/// rows that the old projection marked hidden purely because the workflow
/// was anonymous (`is_anonymous = true`). Idempotent; safe to run on an
/// already-migrated index (no rows will change because `is_anonymous` no
/// longer participates in `RecordSummary::from_record`).
pub(super) fn migrate_recompute_hidden_v2(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let updates: Vec<(String, i64)> = {
        let mut stmt = tx
            .prepare(
                "SELECT artifact_dir, record_json FROM agent_artifacts \
                 WHERE hidden = 1",
            )
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut updates: Vec<(String, i64)> = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let Ok(record) = decode_agent_artifact_record_json(&record_json)
            else {
                continue;
            };
            let new_hidden = RecordSummary::from_record(&record).hidden;
            if !new_hidden {
                updates.push((artifact_dir, 0));
            }
        }
        updates
    };
    for (artifact_dir, hidden) in updates {
        tx.execute(
            "UPDATE agent_artifacts SET hidden = ?1 WHERE artifact_dir = ?2",
            params![hidden, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v5 adds `agent_meta.linked_repos` inside `record_json`.
///
/// There is no DDL to apply; callers that need existing rows refreshed run a
/// full rebuild so each row is reserialized from source marker files.
pub(super) fn migrate_record_json_refresh_v5(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v6 adds `agent_meta.reasoning_effort` and
/// `prompt_steps[*].reasoning_effort` inside `record_json` so the ACE TUI can
/// render the resolved effort uniformly across providers.
///
/// There is no DDL to apply; callers that need existing rows refreshed run a
/// full rebuild so each row is reserialized from source marker files.
pub(super) fn migrate_record_json_refresh_v6(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v7 is a reserved `record_json` refresh migration.
///
/// There is no DDL to apply; callers that need existing rows refreshed run a
/// full rebuild so each row is reserialized from source marker files.
pub(super) fn migrate_record_json_refresh_v7(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v8 adds `agent_meta.plan_committed` inside `record_json`. The Python
/// lifecycle checks the stored version before opening the Rust index and
/// performs a source rebuild so existing rows receive the new projection.
pub(super) fn migrate_record_json_refresh_v8(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v9 adds `agent_meta.output_path` inside `record_json` so failed workflow
/// rows can expose their runner log without re-reading marker files.
pub(super) fn migrate_record_json_refresh_v9(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v10 adds `agent_meta.agent_family_parallel` inside `record_json` so
/// indexed consumers can distinguish parallel members from serial children.
pub(super) fn migrate_record_json_refresh_v10(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v11 adds the denormalized `agent_clan` projection and refreshes
/// `record_json` with `agent_meta.agent_clan`.
pub(super) fn migrate_record_json_refresh_v11(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v12 refreshes `record_json` with `agent_meta.agent_clan_generation` and
/// `agent_meta.clan_tribe` for clan-level tribe resolution.
pub(super) fn migrate_record_json_refresh_v12(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v13 refreshes ``record_json`` so agent metadata serializes the canonical
/// ``tribe`` field.  Startup detects the old version without opening this
/// index and schedules the source rebuild off the UI thread.
pub(super) fn migrate_record_json_refresh_v13(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v14 refreshes `record_json` with `agent_meta.clan_summary` for clan-level
/// summary resolution.
pub(super) fn migrate_record_json_refresh_v14(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v15 denormalizes generation-scoped clan declarations so bounded index
/// queries can resolve semantic context without parsing historical row JSON.
/// The Python lifecycle rebuilds older indexes from source after detecting
/// this schema bump, populating the new columns for existing records.
pub(super) fn migrate_clan_context_projection_v15(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v16 refreshes `record_json` with `agent_meta.epic_plan_ref` so indexed
/// snapshot consumers retain the phase's parent-epic relationship after the
/// phase-authored plan replaces `sdd_plan_path`.
pub(super) fn migrate_record_json_refresh_v16(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v18 refreshes `record_json` with `agent_meta.wait_priority` so indexed
/// snapshot consumers retain authored runner-slot priority without requiring
/// a live `waiting.json` marker.
pub(super) fn migrate_record_json_refresh_v18(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v19 adds the launch-boundary `used_xprompts` projection to `record_json`
/// and signs `xprompts.json` so late writes refresh cached rows.
///
/// The Python lifecycle rebuilds older indexes from source after detecting
/// this schema bump, populating the projection for historical records.
pub(super) fn migrate_record_json_refresh_v19(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v20 adds `agent_meta.model_alias` and `prompt_steps[*].model_alias` to
/// `record_json` so the ACE `Model:` field can render launch-time alias
/// provenance.
pub(super) fn migrate_record_json_refresh_v20(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v22 adds a regenerable child projection for launch-time model aliases.
///
/// The migration is a pure `record_json` re-projection: it performs no
/// filesystem reads and skips malformed legacy payloads so index open
/// cannot fail on a single bad row.
pub(super) fn migrate_model_alias_projection_v22(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    tx.execute("DELETE FROM agent_artifact_model_aliases", [])
        .map_err(|e| e.to_string())?;
    let rows: Vec<(String, String, AgentArtifactRecordWire)> = {
        let mut stmt = tx
            .prepare(
                "SELECT artifact_dir, projects_root, record_json \
                 FROM agent_artifacts",
            )
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let projects_root: String =
                row.get(1).map_err(|e| e.to_string())?;
            let record_json: String = row.get(2).map_err(|e| e.to_string())?;
            let Ok(record) = decode_agent_artifact_record_json(&record_json)
            else {
                continue;
            };
            records.push((artifact_dir, projects_root, record));
        }
        records
    };
    for (artifact_dir, projects_root, record) in rows {
        upsert_model_aliases_for_record(
            &tx,
            Path::new(&projects_root),
            &record,
        )?;
        let origin = record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.model_alias_origin.clone());
        tx.execute(
            "UPDATE agent_artifacts SET model_alias_origin = ?1 \
             WHERE artifact_dir = ?2",
            params![origin, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v23 refreshes `record_json` with flat gate-shell metadata projected from
/// `agent_meta.json` and `done.json`.
pub(super) fn migrate_record_json_refresh_v23(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v24 adds the scalar `done_outcome` projection for abandoned-row repair.
pub(super) fn migrate_done_outcome_projection_v24(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>)> = {
        let mut stmt = tx
            .prepare("SELECT artifact_dir, record_json FROM agent_artifacts")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut outcomes = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let Ok(record) = decode_agent_artifact_record_json(&record_json)
            else {
                continue;
            };
            let outcome =
                record.done.as_ref().and_then(|done| done.outcome.clone());
            outcomes.push((artifact_dir, outcome));
        }
        outcomes
    };
    for (artifact_dir, outcome) in rows {
        tx.execute(
            "UPDATE agent_artifacts SET done_outcome = ?1 \
             WHERE artifact_dir = ?2",
            params![outcome, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v26 refreshes `record_json` with `agent_meta.queue_weight` and
/// `waiting.queue_weight` projections.
pub(super) fn migrate_record_json_refresh_v26(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v27 refreshes `record_json` with `agent_meta.runner_claim_owner_key`.
pub(super) fn migrate_record_json_refresh_v27(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v28 adds the scalar source-machine projection for machine candidate filters.
pub(super) fn migrate_source_machine_projection_v28(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>)> = {
        let mut stmt = tx
            .prepare("SELECT artifact_dir, record_json FROM agent_artifacts")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut machines = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let from_record = decode_agent_artifact_record_json(&record_json)
                .ok()
                .and_then(|record| source_machine_from_record(&record));
            let source_machine = from_record.or_else(|| {
                source_machine_from_marker_files(Path::new(&artifact_dir))
            });
            machines.push((artifact_dir, source_machine));
        }
        machines
    };
    for (artifact_dir, source_machine) in rows {
        tx.execute(
            "UPDATE agent_artifacts SET source_machine = ?1 \
             WHERE artifact_dir = ?2",
            params![source_machine, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v29 refreshes `record_json` with `agent_meta.queue_capacity` and
/// `waiting.queue_capacity` so indexed running/history rows keep authored
/// budgets after waiting markers disappear.
pub(super) fn migrate_record_json_refresh_v29(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v32 refreshes `record_json` so indexed rows include authored
/// `queue_capacity_multiplier` values from agent metadata and wait markers.
pub(super) fn migrate_record_json_refresh_v32(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v33 renames the legacy `agent_family` column to `agent_session` in place,
/// so an upgraded index keeps every indexed session lane instead of failing
/// to open. The legacy index would follow the rename onto the new column, so
/// it is dropped; the caller recreates the index under its new name.
pub(super) fn migrate_agent_session_column_v33(
    conn: &Connection,
) -> Result<(), String> {
    const LEGACY_AGENT_SESSION_INDEX_COLUMN: &str = "agent_family";
    conn.execute_batch("DROP INDEX IF EXISTS idx_agent_artifacts_agent_family")
        .map_err(|e| e.to_string())?;
    if !agent_artifacts_has_column(conn, LEGACY_AGENT_SESSION_INDEX_COLUMN)? {
        return Ok(());
    }
    conn.execute(
        &format!(
            "ALTER TABLE agent_artifacts RENAME COLUMN \
             {LEGACY_AGENT_SESSION_INDEX_COLUMN} TO {}",
            super::AGENT_SESSION_INDEX_COLUMN
        ),
        [],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// v30 adds the imported-owner machine projection so candidate filters can
/// match every live index-resident machine value, not only `source_machine`.
pub(super) fn migrate_imported_owner_machine_projection_v30(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>, Option<String>)> = {
        let mut stmt = tx
            .prepare("SELECT artifact_dir, record_json FROM agent_artifacts")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut machines = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let from_record = decode_agent_artifact_record_json(&record_json)
                .ok()
                .map(|record| machine_projection_from_record(&record))
                .unwrap_or_default();
            let from_markers =
                machine_projection_from_marker_files(Path::new(&artifact_dir));
            let projection = MachineProjection {
                source_machine: from_record
                    .source_machine
                    .or(from_markers.source_machine),
                imported_owner_machine: from_record
                    .imported_owner_machine
                    .or(from_markers.imported_owner_machine),
            };
            machines.push((
                artifact_dir,
                projection.source_machine,
                projection.imported_owner_machine,
            ));
        }
        machines
    };
    for (artifact_dir, source_machine, imported_owner_machine) in rows {
        tx.execute(
            "UPDATE agent_artifacts SET source_machine = ?1, \
             imported_owner_machine = ?2 WHERE artifact_dir = ?3",
            params![source_machine, imported_owner_machine, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v31 adds the indexed `gate_shell_id` projection so an exact gate-id
/// lookup can use `WHERE gate_shell_id = ?` instead of decoding every
/// historical row. Only rows that are a real gate-shell member (not a
/// descendant that merely inherited the gate id) get a non-null value.
pub(super) fn migrate_gate_shell_id_projection_v31(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>)> = {
        let mut stmt = tx
            .prepare("SELECT artifact_dir, record_json FROM agent_artifacts")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut projected = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let gate_shell_id = decode_agent_artifact_record_json(&record_json)
                .ok()
                .and_then(|record| gate_shell_id_from_record(&record));
            projected.push((artifact_dir, gate_shell_id));
        }
        projected
    };
    for (artifact_dir, gate_shell_id) in rows {
        tx.execute(
            "UPDATE agent_artifacts SET gate_shell_id = ?1 \
             WHERE artifact_dir = ?2",
            params![gate_shell_id, artifact_dir],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// v21 adds a regenerable child projection for indexed output variables.
pub(super) fn migrate_output_variable_projection_v21(
    conn: &mut Connection,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    tx.execute("DELETE FROM agent_output_variables", [])
        .map_err(|e| e.to_string())?;
    let rows: Vec<(String, AgentArtifactRecordWire)> = {
        let mut stmt = tx
            .prepare("SELECT projects_root, record_json FROM agent_artifacts")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let projects_root: String =
                row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let Ok(record) = decode_agent_artifact_record_json(&record_json)
            else {
                continue;
            };
            records.push((projects_root, record));
        }
        records
    };
    for (projects_root, record) in rows {
        upsert_output_variables_for_record(
            &tx,
            Path::new(&projects_root),
            &record,
        )?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}
