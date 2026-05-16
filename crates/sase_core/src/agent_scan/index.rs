//! SQLite materialized view for agent artifact summaries.
//!
//! The artifact tree remains the source of truth. This module stores one
//! row per artifact directory with denormalized query fields and the
//! scanner's canonical `AgentArtifactRecordWire` JSON payload so indexed
//! queries can return loader-equivalent records without walking every
//! historical timestamp directory.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::{params, params_from_iter, Connection};
use serde::{Deserialize, Serialize};

use super::scanner::{scan_agent_artifact_dir, scan_agent_artifacts};
use super::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire, AgentMetaWire,
    AGENT_SCAN_WIRE_SCHEMA_VERSION,
};

pub const AGENT_ARTIFACT_INDEX_SCHEMA_VERSION: u32 = 3;

const MARKER_FILES: &[&str] = &[
    "agent_meta.json",
    "done.json",
    "running.json",
    "waiting.json",
    "workflow_state.json",
    "plan_path.json",
];

fn default_true() -> bool {
    true
}

/// Query knobs for the persistent artifact index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexQueryWire {
    #[serde(default)]
    pub include_active: bool,
    #[serde(default)]
    pub include_recent_completed: bool,
    #[serde(default)]
    pub include_full_history: bool,
    #[serde(default)]
    pub recent_completed_limit: Option<u32>,
    #[serde(default)]
    pub include_hidden: bool,
    /// When ``true`` (default), the completed-row branch returns every
    /// matching row regardless of dismissal state. When ``false``, completed
    /// rows whose ``(cl_name, timestamp)`` identity appears in the
    /// ``dismissed_agents`` sidecar are excluded. Active/incomplete rows are
    /// never filtered by dismissal state so a still-RUNNING alias of a
    /// dismissed completion remains visible.
    #[serde(default = "default_true")]
    pub include_dismissed: bool,
    #[serde(default)]
    pub parent_timestamps: Vec<String>,
}

impl Default for AgentArtifactIndexQueryWire {
    fn default() -> Self {
        Self {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            recent_completed_limit: Some(200),
            include_hidden: false,
            include_dismissed: true,
            parent_timestamps: Vec::new(),
        }
    }
}

/// One dismissed-agent identity persisted alongside the artifact index.
///
/// Identity matches the Python ``(AgentType, cl_name, raw_suffix)`` tuple
/// stored in ``~/.sase/dismissed_agents.json``. ``raw_suffix`` may be ``None``
/// for identities that dismiss every artifact-backed row sharing the
/// ``(agent_type, cl_name)`` prefix; when ``Some(...)`` it must equal the
/// artifact directory's ``timestamp`` for the match to apply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DismissedAgentIdentityWire {
    pub agent_type: String,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub dismissed_at: Option<String>,
}

/// Summary of one index mutation/rebuild.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexUpdateWire {
    pub schema_version: u32,
    pub index_path: String,
    pub projects_root: String,
    pub rows_indexed: u64,
    pub rows_deleted: u64,
    pub rows_skipped: u64,
}

/// Rebuild the index from the canonical artifact tree.
pub fn rebuild_agent_artifact_index(
    index_path: &Path,
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let snapshot = scan_agent_artifacts(projects_root, options);
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    tx.execute("DELETE FROM agent_artifacts", [])
        .map_err(|e| e.to_string())?;

    let mut rows_indexed = 0u64;
    for record in &snapshot.records {
        upsert_record(&tx, projects_root, record)?;
        rows_indexed += 1;
    }
    tx.commit().map_err(|e| e.to_string())?;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        rows_indexed,
        rows_deleted: 0,
        rows_skipped: 0,
    })
}

/// Upsert one artifact directory row by reparsing its marker files.
pub fn upsert_agent_artifact_index_row(
    index_path: &Path,
    projects_root: &Path,
    artifact_dir: &Path,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let Some(record) =
        scan_agent_artifact_dir(projects_root, artifact_dir, &options)
    else {
        return Ok(AgentArtifactIndexUpdateWire {
            schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
            index_path: index_path.to_string_lossy().into_owned(),
            projects_root: projects_root.to_string_lossy().into_owned(),
            rows_indexed: 0,
            rows_deleted: 0,
            rows_skipped: 1,
        });
    };

    let tx = conn.transaction().map_err(|e| e.to_string())?;
    upsert_record(&tx, projects_root, &record)?;
    tx.commit().map_err(|e| e.to_string())?;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        rows_indexed: 1,
        rows_deleted: 0,
        rows_skipped: 0,
    })
}

/// Delete one artifact directory row from the index.
pub fn delete_agent_artifact_index_row(
    index_path: &Path,
    artifact_dir: &Path,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let conn = open_index(index_path)?;
    let deleted = conn
        .execute(
            "DELETE FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
        )
        .map_err(|e| e.to_string())? as u64;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: 0,
        rows_deleted: deleted,
        rows_skipped: 0,
    })
}

/// Upsert one dismissed-agent identity into the sidecar table.
///
/// ``identity.raw_suffix`` of ``None`` is persisted as the empty-string
/// sentinel meaning "every artifact suffix sharing this
/// ``(agent_type, cl_name)`` prefix is dismissed".
pub fn upsert_dismissed_agent_visibility(
    index_path: &Path,
    identity: DismissedAgentIdentityWire,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let conn = open_index(index_path)?;
    upsert_dismissed_row(&conn, &identity)?;
    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: 1,
        rows_deleted: 0,
        rows_skipped: 0,
    })
}

/// Delete one dismissed-agent identity from the sidecar table.
pub fn delete_dismissed_agent_visibility(
    index_path: &Path,
    agent_type: &str,
    cl_name: &str,
    raw_suffix: Option<&str>,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let conn = open_index(index_path)?;
    let suffix = raw_suffix.unwrap_or("");
    let deleted = conn
        .execute(
            "DELETE FROM dismissed_agents
             WHERE agent_type = ?1 AND cl_name = ?2 AND raw_suffix = ?3",
            params![agent_type, cl_name, suffix],
        )
        .map_err(|e| e.to_string())? as u64;
    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: 0,
        rows_deleted: deleted,
        rows_skipped: 0,
    })
}

/// Replace the entire dismissed-agent sidecar with the supplied set.
///
/// Mirrors the bulk-sync path the Python TUI uses when it loads the legacy
/// ``dismissed_agents.json`` file at startup. The replace is transactional:
/// either every supplied row is visible afterwards or the table is
/// unchanged.
pub fn replace_dismissed_agent_visibility(
    index_path: &Path,
    identities: Vec<DismissedAgentIdentityWire>,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let deleted = tx
        .execute("DELETE FROM dismissed_agents", [])
        .map_err(|e| e.to_string())? as u64;
    let mut rows_indexed = 0u64;
    for identity in &identities {
        upsert_dismissed_row(&tx, identity)?;
        rows_indexed += 1;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed,
        rows_deleted: deleted,
        rows_skipped: 0,
    })
}

fn upsert_dismissed_row(
    conn: &Connection,
    identity: &DismissedAgentIdentityWire,
) -> Result<(), String> {
    let suffix = identity.raw_suffix.clone().unwrap_or_default();
    conn.execute(
        "INSERT INTO dismissed_agents (agent_type, cl_name, raw_suffix, dismissed_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(agent_type, cl_name, raw_suffix) DO UPDATE SET
             dismissed_at = excluded.dismissed_at",
        params![
            identity.agent_type,
            identity.cl_name,
            suffix,
            identity.dismissed_at,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Query indexed rows and return scanner-shaped records.
pub fn query_agent_artifact_index(
    index_path: &Path,
    projects_root: &Path,
    query: AgentArtifactIndexQueryWire,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactScanWire, String> {
    let conn = open_index(index_path)?;
    let mut stats = AgentArtifactScanStatsWire::default();
    let mut by_dir: BTreeMap<String, AgentArtifactRecordWire> = BTreeMap::new();

    if !query.parent_timestamps.is_empty() {
        select_records_for_parent_timestamps(
            &conn,
            &query.parent_timestamps,
            options.include_prompt_step_markers,
            &mut stats,
            &mut by_dir,
        )?;
    }

    if query.include_active {
        select_records(
            &conn,
            &active_where(query.include_hidden),
            None,
            options.include_prompt_step_markers,
            &mut stats,
            &mut by_dir,
        )?;
    }

    if query.include_recent_completed {
        select_records(
            &conn,
            &completed_where(query.include_hidden, !query.include_dismissed),
            query.recent_completed_limit,
            options.include_prompt_step_markers,
            &mut stats,
            &mut by_dir,
        )?;
    }

    if query.include_full_history {
        select_records(
            &conn,
            &visible_where(query.include_hidden, !query.include_dismissed),
            None,
            options.include_prompt_step_markers,
            &mut stats,
            &mut by_dir,
        )?;
    }

    let mut records: Vec<AgentArtifactRecordWire> =
        by_dir.into_values().collect();
    records.sort_by(|a, b| {
        (
            a.project_name.as_str(),
            a.workflow_dir_name.as_str(),
            a.timestamp.as_str(),
        )
            .cmp(&(
                b.project_name.as_str(),
                b.workflow_dir_name.as_str(),
                b.timestamp.as_str(),
            ))
    });
    stats.artifact_dirs_visited = records.len() as u64;

    Ok(AgentArtifactScanWire {
        schema_version: AGENT_SCAN_WIRE_SCHEMA_VERSION,
        projects_root: projects_root.to_string_lossy().into_owned(),
        options,
        stats,
        records,
    })
}

fn open_index(index_path: &Path) -> Result<Connection, String> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let conn = Connection::open(index_path).map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
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
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            agent_type TEXT NOT NULL,
            cl_name TEXT,
            agent_name TEXT,
            model TEXT,
            llm_provider TEXT,
            started_at TEXT,
            finished_at REAL,
            has_done_marker INTEGER NOT NULL,
            has_running_marker INTEGER NOT NULL,
            has_waiting_marker INTEGER NOT NULL,
            has_pending_question_marker INTEGER NOT NULL DEFAULT 0,
            has_agent_meta_active_signal INTEGER NOT NULL DEFAULT 0,
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
            workflow_state_sig TEXT,
            plan_path_sig TEXT,
            prompt_steps_sig TEXT,
            record_json TEXT NOT NULL,
            indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_recent_completed
            ON agent_artifacts(hidden, has_done_marker, finished_at, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_cl_name
            ON agent_artifacts(cl_name);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_project_workflow
            ON agent_artifacts(project_name, workflow_dir_name, timestamp);
        CREATE TABLE IF NOT EXISTS dismissed_agents (
            agent_type TEXT NOT NULL,
            cl_name TEXT NOT NULL,
            raw_suffix TEXT NOT NULL DEFAULT '',
            dismissed_at TEXT,
            PRIMARY KEY (agent_type, cl_name, raw_suffix)
        );
        CREATE INDEX IF NOT EXISTS idx_dismissed_agents_cl
            ON dismissed_agents(cl_name, raw_suffix);
        "#,
    )
    .map_err(|e| e.to_string())?;
    ensure_agent_artifact_columns(&conn)?;
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_active
            ON agent_artifacts(
                hidden,
                has_done_marker,
                has_running_marker,
                has_waiting_marker,
                has_pending_question_marker,
                has_agent_meta_active_signal,
                has_workflow_state,
                workflow_status,
                timestamp
            );
        "#,
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
        [AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string()],
    )
    .map_err(|e| e.to_string())?;
    Ok(conn)
}

fn ensure_agent_artifact_columns(conn: &Connection) -> Result<(), String> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(agent_artifacts)")
        .map_err(|e| e.to_string())?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    if !columns
        .iter()
        .any(|name| name == "has_pending_question_marker")
    {
        conn.execute(
            "ALTER TABLE agent_artifacts
             ADD COLUMN has_pending_question_marker INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(|e| e.to_string())?;
    }
    if !columns
        .iter()
        .any(|name| name == "has_agent_meta_active_signal")
    {
        conn.execute(
            "ALTER TABLE agent_artifacts
             ADD COLUMN has_agent_meta_active_signal INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn upsert_record(
    conn: &Connection,
    projects_root: &Path,
    record: &AgentArtifactRecordWire,
) -> Result<(), String> {
    let summary = RecordSummary::from_record(record);
    let signatures = MarkerSignatures::from_artifact_dir(&record.artifact_dir);
    let record_json =
        serde_json::to_string(record).map_err(|e| e.to_string())?;
    conn.execute(
        r#"
        INSERT INTO agent_artifacts (
            artifact_dir, projects_root, project_name, project_dir, project_file,
            workflow_dir_name, timestamp, status, agent_type, cl_name,
            agent_name, model, llm_provider, started_at, finished_at,
            has_done_marker, has_running_marker, has_waiting_marker,
            has_pending_question_marker, has_agent_meta_active_signal,
            has_workflow_state, workflow_status, hidden, parent_timestamp,
            step_index, step_name, retry_of_timestamp, retried_as_timestamp,
            retry_chain_root_timestamp, retry_attempt, agent_meta_sig, done_sig,
            running_sig, waiting_sig, workflow_state_sig, plan_path_sig,
            prompt_steps_sig, record_json, indexed_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, CURRENT_TIMESTAMP
        )
        ON CONFLICT(artifact_dir) DO UPDATE SET
            projects_root = excluded.projects_root,
            project_name = excluded.project_name,
            project_dir = excluded.project_dir,
            project_file = excluded.project_file,
            workflow_dir_name = excluded.workflow_dir_name,
            timestamp = excluded.timestamp,
            status = excluded.status,
            agent_type = excluded.agent_type,
            cl_name = excluded.cl_name,
            agent_name = excluded.agent_name,
            model = excluded.model,
            llm_provider = excluded.llm_provider,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            has_done_marker = excluded.has_done_marker,
            has_running_marker = excluded.has_running_marker,
            has_waiting_marker = excluded.has_waiting_marker,
            has_pending_question_marker = excluded.has_pending_question_marker,
            has_agent_meta_active_signal = excluded.has_agent_meta_active_signal,
            has_workflow_state = excluded.has_workflow_state,
            workflow_status = excluded.workflow_status,
            hidden = excluded.hidden,
            parent_timestamp = excluded.parent_timestamp,
            step_index = excluded.step_index,
            step_name = excluded.step_name,
            retry_of_timestamp = excluded.retry_of_timestamp,
            retried_as_timestamp = excluded.retried_as_timestamp,
            retry_chain_root_timestamp = excluded.retry_chain_root_timestamp,
            retry_attempt = excluded.retry_attempt,
            agent_meta_sig = excluded.agent_meta_sig,
            done_sig = excluded.done_sig,
            running_sig = excluded.running_sig,
            waiting_sig = excluded.waiting_sig,
            workflow_state_sig = excluded.workflow_state_sig,
            plan_path_sig = excluded.plan_path_sig,
            prompt_steps_sig = excluded.prompt_steps_sig,
            record_json = excluded.record_json,
            indexed_at = CURRENT_TIMESTAMP
        "#,
        params![
            record.artifact_dir,
            projects_root.to_string_lossy().as_ref(),
            record.project_name,
            record.project_dir,
            record.project_file,
            record.workflow_dir_name,
            record.timestamp,
            summary.status,
            summary.agent_type,
            summary.cl_name,
            summary.agent_name,
            summary.model,
            summary.llm_provider,
            summary.started_at,
            summary.finished_at,
            record.has_done_marker as i64,
            record.running.is_some() as i64,
            record.waiting.is_some() as i64,
            record.pending_question.is_some() as i64,
            summary.has_agent_meta_active_signal as i64,
            record.workflow_state.is_some() as i64,
            summary.workflow_status,
            summary.hidden as i64,
            summary.parent_timestamp,
            summary.step_index,
            summary.step_name,
            summary.retry_of_timestamp,
            summary.retried_as_timestamp,
            summary.retry_chain_root_timestamp,
            summary.retry_attempt,
            signatures.agent_meta,
            signatures.done,
            signatures.running,
            signatures.waiting,
            signatures.workflow_state,
            signatures.plan_path,
            signatures.prompt_steps,
            record_json,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn select_records(
    conn: &Connection,
    where_sql: &str,
    limit: Option<u32>,
    include_prompt_step_markers: bool,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
) -> Result<(), String> {
    let mut sql = format!(
        "SELECT artifact_dir, record_json FROM agent_artifacts {where_sql}"
    );
    if limit.is_some() {
        sql.push_str(" LIMIT ?1");
    }
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = if let Some(limit) = limit {
        stmt.query([limit]).map_err(|e| e.to_string())?
    } else {
        stmt.query([]).map_err(|e| e.to_string())?
    };

    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
        let record_json: String = row.get(1).map_err(|e| e.to_string())?;
        match serde_json::from_str::<AgentArtifactRecordWire>(&record_json) {
            Ok(mut record) => {
                if include_prompt_step_markers {
                    stats.prompt_step_markers_parsed +=
                        record.prompt_steps.len() as u64;
                } else {
                    record.prompt_steps.clear();
                }
                by_dir.insert(artifact_dir, record);
            }
            Err(_) => {
                stats.json_decode_errors += 1;
            }
        }
    }
    Ok(())
}

fn select_records_for_parent_timestamps(
    conn: &Connection,
    parent_timestamps: &[String],
    include_prompt_step_markers: bool,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
) -> Result<(), String> {
    let placeholders = std::iter::repeat("?")
        .take(parent_timestamps.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT artifact_dir, record_json FROM agent_artifacts \
         WHERE timestamp IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(parent_timestamps.iter()))
        .map_err(|e| e.to_string())?;

    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
        let record_json: String = row.get(1).map_err(|e| e.to_string())?;
        match serde_json::from_str::<AgentArtifactRecordWire>(&record_json) {
            Ok(mut record) => {
                if include_prompt_step_markers {
                    stats.prompt_step_markers_parsed +=
                        record.prompt_steps.len() as u64;
                } else {
                    record.prompt_steps.clear();
                }
                by_dir.insert(artifact_dir, record);
            }
            Err(_) => {
                stats.json_decode_errors += 1;
            }
        }
    }
    Ok(())
}

fn active_where(include_hidden: bool) -> String {
    // Active/incomplete rows are never filtered by dismissal state. A
    // still-RUNNING artifact whose identity has been dismissed for an older
    // completed alias must remain visible.
    let predicate = format!("({})", active_predicate_sql());
    if include_hidden {
        format!("WHERE {predicate} ORDER BY timestamp DESC")
    } else {
        format!("WHERE hidden = 0 AND {predicate} ORDER BY timestamp DESC")
    }
}

fn dismissed_filter_sql() -> &'static str {
    // ``raw_suffix = ''`` is the sentinel for "any artifact suffix sharing
    // this (agent_type, cl_name) prefix"; otherwise the suffix must equal
    // the artifact timestamp.
    " AND NOT EXISTS (
        SELECT 1 FROM dismissed_agents d
        WHERE d.agent_type = agent_artifacts.agent_type
          AND d.cl_name = agent_artifacts.cl_name
          AND (d.raw_suffix = '' OR d.raw_suffix = agent_artifacts.timestamp)
    )"
}

fn completed_where(include_hidden: bool, exclude_dismissed: bool) -> String {
    let mut sql = String::new();
    if include_hidden {
        sql.push_str("WHERE ");
    } else {
        sql.push_str("WHERE hidden = 0 AND ");
    }
    sql.push_str(completed_predicate_sql());
    if exclude_dismissed {
        sql.push_str(dismissed_filter_sql());
    }
    sql.push_str(" ORDER BY COALESCE(finished_at, 0) DESC, timestamp DESC");
    sql
}

fn visible_where(include_hidden: bool, exclude_dismissed: bool) -> String {
    let mut clauses: Vec<String> = Vec::new();
    if !include_hidden {
        clauses.push("hidden = 0".to_string());
    }
    if exclude_dismissed {
        // Active rows always pass; completed rows must not match a
        // dismissed-agent identity.
        clauses.push(format!(
            "({active} OR ({completed}{filter}))",
            active = active_predicate_sql(),
            completed = completed_predicate_sql(),
            filter = dismissed_filter_sql(),
        ));
    }
    let mut sql = String::new();
    if !clauses.is_empty() {
        sql.push_str("WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    sql.push_str(
        " ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC",
    );
    sql
}

fn completed_predicate_sql() -> &'static str {
    "(has_done_marker = 1
      OR (
          has_workflow_state = 1
          AND COALESCE(workflow_status, '') IN
              ('completed', 'failed', 'cancelled', 'noop')
      ))"
}

fn active_predicate_sql() -> &'static str {
    "has_running_marker = 1
     OR has_waiting_marker = 1
     OR has_pending_question_marker = 1
     OR has_agent_meta_active_signal = 1
     OR (
         has_workflow_state = 1
         AND COALESCE(workflow_status, '') NOT IN
             ('completed', 'failed', 'cancelled', 'noop')
     )"
}

#[derive(Default)]
struct RecordSummary {
    status: String,
    agent_type: String,
    cl_name: Option<String>,
    agent_name: Option<String>,
    model: Option<String>,
    llm_provider: Option<String>,
    started_at: Option<String>,
    finished_at: Option<f64>,
    workflow_status: Option<String>,
    has_agent_meta_active_signal: bool,
    hidden: bool,
    parent_timestamp: Option<String>,
    step_index: Option<i64>,
    step_name: Option<String>,
    retry_of_timestamp: Option<String>,
    retried_as_timestamp: Option<String>,
    retry_chain_root_timestamp: Option<String>,
    retry_attempt: Option<i64>,
}

impl RecordSummary {
    fn from_record(record: &AgentArtifactRecordWire) -> Self {
        let meta = record.agent_meta.as_ref();
        let done = record.done.as_ref();
        let running = record.running.as_ref();
        let waiting = record.waiting.as_ref();
        let workflow_state = record.workflow_state.as_ref();
        let first_step = record.prompt_steps.first();

        let workflow_status = workflow_state.map(|w| w.status.clone());
        let has_agent_meta_active_signal =
            meta.is_some_and(agent_meta_has_active_signal);
        let status = if waiting.is_some() {
            "waiting"
        } else if let Some(workflow_status) = workflow_status.as_deref() {
            workflow_status
        } else if record.has_done_marker {
            "done"
        } else if meta
            .and_then(|m| {
                m.run_started_at.as_ref().or(m.wait_completed_at.as_ref())
            })
            .is_some()
        {
            "running"
        } else {
            "starting"
        }
        .to_string();

        Self {
            status,
            agent_type: if workflow_state.is_some() {
                "workflow".to_string()
            } else {
                "run".to_string()
            },
            cl_name: done
                .and_then(|d| d.cl_name.clone())
                .or_else(|| running.and_then(|r| r.cl_name.clone()))
                .or_else(|| workflow_state.and_then(|w| w.cl_name.clone())),
            agent_name: meta
                .and_then(|m| m.name.clone())
                .or_else(|| done.and_then(|d| d.name.clone()))
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            model: meta
                .and_then(|m| m.model.clone())
                .or_else(|| done.and_then(|d| d.model.clone()))
                .or_else(|| running.and_then(|r| r.model.clone()))
                .or_else(|| first_step.and_then(|s| s.model.clone())),
            llm_provider: meta
                .and_then(|m| m.llm_provider.clone())
                .or_else(|| done.and_then(|d| d.llm_provider.clone()))
                .or_else(|| running.and_then(|r| r.llm_provider.clone()))
                .or_else(|| first_step.and_then(|s| s.llm_provider.clone())),
            started_at: meta
                .and_then(|m| m.run_started_at.clone())
                .or_else(|| workflow_state.and_then(|w| w.start_time.clone())),
            finished_at: done.and_then(|d| d.finished_at),
            workflow_status,
            has_agent_meta_active_signal,
            hidden: meta.map(|m| m.hidden).unwrap_or(false)
                || done.map(|d| d.hidden).unwrap_or(false)
                || workflow_state.map(|w| w.hidden).unwrap_or(false)
                || workflow_state.map(|w| w.is_anonymous).unwrap_or(false),
            parent_timestamp: meta.and_then(|m| m.parent_timestamp.clone()),
            step_index: first_step.and_then(|s| s.step_index),
            step_name: first_step.map(|s| s.step_name.clone()),
            retry_of_timestamp: meta.and_then(|m| m.retry_of_timestamp.clone()),
            retried_as_timestamp: meta
                .and_then(|m| m.retried_as_timestamp.clone())
                .or_else(|| done.and_then(|d| d.retried_as_timestamp.clone())),
            retry_chain_root_timestamp: meta
                .and_then(|m| m.retry_chain_root_timestamp.clone())
                .or_else(|| {
                    done.and_then(|d| d.retry_chain_root_timestamp.clone())
                }),
            retry_attempt: meta.and_then(|m| m.retry_attempt),
        }
    }
}

fn agent_meta_has_active_signal(meta: &AgentMetaWire) -> bool {
    meta.pid.is_some()
        || meta.run_started_at.is_some()
        || meta.wait_completed_at.is_some()
        || !meta.wait_for.is_empty()
        || meta.wait_duration.is_some()
        || meta.wait_until.is_some()
        || !meta.plan_submitted_at.is_empty()
        || meta.epic_started_at.is_some()
        || !meta.feedback_submitted_at.is_empty()
        || !meta.questions_submitted_at.is_empty()
        || !meta.retry_started_at.is_empty()
}

#[derive(Default)]
struct MarkerSignatures {
    agent_meta: Option<String>,
    done: Option<String>,
    running: Option<String>,
    waiting: Option<String>,
    workflow_state: Option<String>,
    plan_path: Option<String>,
    prompt_steps: Option<String>,
}

impl MarkerSignatures {
    fn from_artifact_dir(artifact_dir: &str) -> Self {
        let dir = PathBuf::from(artifact_dir);
        let mut sigs = Self {
            agent_meta: marker_signature(&dir.join("agent_meta.json")),
            done: marker_signature(&dir.join("done.json")),
            running: marker_signature(&dir.join("running.json")),
            waiting: marker_signature(&dir.join("waiting.json")),
            workflow_state: marker_signature(&dir.join("workflow_state.json")),
            plan_path: marker_signature(&dir.join("plan_path.json")),
            prompt_steps: None,
        };

        let mut step_sigs: Vec<String> = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str())
                else {
                    continue;
                };
                if name.starts_with("prompt_step_")
                    && name.ends_with(".json")
                    && path.is_file()
                {
                    if let Some(sig) = marker_signature(&path) {
                        step_sigs.push(format!("{name}:{sig}"));
                    }
                }
            }
        }
        step_sigs.sort();
        if !step_sigs.is_empty() {
            sigs.prompt_steps = Some(step_sigs.join("|"));
        }
        sigs
    }
}

fn marker_signature(path: &Path) -> Option<String> {
    if !MARKER_FILES
        .iter()
        .any(|name| path.file_name().and_then(|n| n.to_str()) == Some(*name))
        && !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("prompt_step_"))
    {
        return None;
    }
    let meta = fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(format!(
        "{}:{}:{}",
        meta.len(),
        duration.as_secs(),
        duration.subsec_nanos()
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn write_json(path: &Path, payload: serde_json::Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
    }

    fn artifact(root: &Path, ts: &str) -> PathBuf {
        root.join("proj").join("artifacts").join("ace-run").join(ts)
    }

    #[test]
    fn rebuild_indexes_scanner_equivalent_records() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let first = artifact(&projects, "20260504101010");
        let second = artifact(&projects, "20260504111111");
        write_json(
            &first.join("agent_meta.json"),
            json!({"name": "active", "pid": 123, "model": "gpt"}),
        );
        write_json(
            &second.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1777900000.0,
                "name": "done",
                "cl_name": "cl_alpha"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        let update = rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 2);

        let indexed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: true,
                recent_completed_limit: Some(10),
                include_hidden: false,
                include_dismissed: true,
                parent_timestamps: Vec::new(),
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let source = scan_agent_artifacts(
            &projects,
            AgentArtifactScanOptionsWire::default(),
        );
        assert_eq!(indexed.records, source.records);
    }

    #[test]
    fn upsert_and_delete_one_artifact_row() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260504121212");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "active", "pid": 123}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        let update = upsert_agent_artifact_index_row(
            &index,
            &projects,
            &artifact_dir,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 1);

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].timestamp, "20260504121212");

        let deleted =
            delete_agent_artifact_index_row(&index, &artifact_dir).unwrap();
        assert_eq!(deleted.rows_deleted, 1);
        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn wait_completed_records_are_indexed_as_running() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260513120000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active",
                "pid": 123,
                "wait_completed_at": "2026-05-13T16:00:00Z",
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let conn = Connection::open(&index).unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "running");
    }

    /// Build a small index with one active row, one completed dismissed row,
    /// and one older non-dismissed completed row sharing the active row's
    /// ``cl_name``. Phase 2 visibility-aware tests build on this fixture.
    fn build_inbox_fixture(
        projects: &Path,
        index: &Path,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let active = artifact(projects, "20260601120000");
        write_json(
            &active.join("agent_meta.json"),
            json!({"name": "active", "pid": 7, "cl_name": "cl_alpha"}),
        );
        write_json(
            &active.join("running.json"),
            json!({"pid": 7, "cl_name": "cl_alpha"}),
        );

        let dismissed_done = artifact(projects, "20260601100000");
        write_json(
            &dismissed_done.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782000000.0,
                "name": "old",
                "cl_name": "cl_alpha",
            }),
        );

        let other_done = artifact(projects, "20260601110000");
        write_json(
            &other_done.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782010000.0,
                "name": "untouched",
                "cl_name": "cl_beta",
            }),
        );

        rebuild_agent_artifact_index(
            index,
            projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        (active, dismissed_done, other_done)
    }

    fn inbox_query() -> AgentArtifactIndexQueryWire {
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            recent_completed_limit: None,
            include_hidden: false,
            include_dismissed: false,
            parent_timestamps: Vec::new(),
        }
    }

    #[test]
    fn index_query_can_strip_prompt_steps_until_parent_expansion() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let workflow = projects
            .join("proj")
            .join("artifacts")
            .join("workflow-build")
            .join("20260601130400");
        write_json(
            &workflow.join("workflow_state.json"),
            json!({
                "workflow_name": "build",
                "cl_name": "cl_workflow",
                "status": "completed",
            }),
        );
        write_json(
            &workflow.join("prompt_step_001_plan.json"),
            json!({
                "workflow_name": "build",
                "step_name": "plan",
                "status": "completed",
                "step_type": "agent"
            }),
        );

        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let collapsed = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire {
                include_prompt_step_markers: false,
                ..AgentArtifactScanOptionsWire::default()
            },
        )
        .unwrap();
        assert_eq!(collapsed.records.len(), 1);
        assert!(collapsed.records[0].prompt_steps.is_empty());
        assert_eq!(collapsed.stats.prompt_step_markers_parsed, 0);

        let expanded = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: false,
                include_full_history: false,
                recent_completed_limit: None,
                include_hidden: true,
                include_dismissed: true,
                parent_timestamps: vec!["20260601130400".to_string()],
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(expanded.records.len(), 1);
        assert_eq!(expanded.records[0].prompt_steps.len(), 1);
        assert_eq!(expanded.stats.prompt_step_markers_parsed, 1);
    }

    #[test]
    fn schema_version_meta_is_bumped_to_phase_three() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        open_index(&index).unwrap();
        let conn = Connection::open(&index).unwrap();
        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
        const _: () = assert!(AGENT_ARTIFACT_INDEX_SCHEMA_VERSION >= 3);

        // dismissed_agents sidecar exists and starts empty.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dismissed_agents", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn inbox_query_excludes_dismissed_completed_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (active, dismissed_done, other_done) =
            build_inbox_fixture(&projects, &index);

        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_alpha".to_string(),
                raw_suffix: Some("20260601100000".to_string()),
                dismissed_at: Some("2026-06-01T11:00:00Z".to_string()),
            },
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        assert!(
            dirs.contains(&active.to_string_lossy().as_ref()),
            "active row must always be visible"
        );
        assert!(
            dirs.contains(&other_done.to_string_lossy().as_ref()),
            "non-dismissed completed row must remain visible"
        );
        assert!(
            !dirs.contains(&dismissed_done.to_string_lossy().as_ref()),
            "dismissed completed row must be filtered from the inbox"
        );
    }

    #[test]
    fn inbox_query_excludes_stale_sparse_no_done_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let stale = artifact(&projects, "20260601130000");
        write_json(
            &stale.join("agent_meta.json"),
            json!({"name": "stale_sparse", "cl_name": "cl_alpha"}),
        );
        write_json(
            &stale.join("prompt_step_000_code.json"),
            json!({
                "workflow_name": "stale",
                "step_name": "code",
                "status": "completed",
            }),
        );
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(
            snapshot.records.is_empty(),
            "no-done rows without active signals must not be inbox-active"
        );
    }

    #[test]
    fn inbox_query_keeps_explicit_active_signal_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let running = artifact(&projects, "20260601130000");
        write_json(&running.join("running.json"), json!({"cl_name": "cl_run"}));

        let waiting = artifact(&projects, "20260601130100");
        write_json(
            &waiting.join("waiting.json"),
            json!({"waiting_for": ["upstream"]}),
        );

        let pending = artifact(&projects, "20260601130200");
        write_json(
            &pending.join("pending_question.json"),
            json!({"session_id": "s1"}),
        );

        let meta_active = artifact(&projects, "20260601130300");
        write_json(
            &meta_active.join("agent_meta.json"),
            json!({"name": "meta", "run_started_at": "2026-06-01T13:03:00Z"}),
        );

        let workflow = projects
            .join("proj")
            .join("artifacts")
            .join("workflow-build")
            .join("20260601130400");
        write_json(
            &workflow.join("workflow_state.json"),
            json!({
                "workflow_name": "build",
                "cl_name": "cl_workflow",
                "status": "waiting_hitl",
            }),
        );

        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let timestamps: std::collections::BTreeSet<&str> = snapshot
            .records
            .iter()
            .map(|record| record.timestamp.as_str())
            .collect();
        assert_eq!(
            timestamps,
            [
                "20260601130000",
                "20260601130100",
                "20260601130200",
                "20260601130300",
                "20260601130400",
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn completed_phase_rows_remain_visible_when_not_dismissed() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let phase = artifact(&projects, "20260601130500");
        write_json(
            &phase.join("agent_meta.json"),
            json!({"name": "phase", "parent_timestamp": "20260601120000"}),
        );
        write_json(
            &phase.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782011000.0,
                "name": "phase",
                "cl_name": "sase-3s.2",
            }),
        );
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(snapshot
            .records
            .iter()
            .any(|record| record.timestamp == "20260601130500"));
    }

    #[test]
    fn dismissed_filter_uses_agent_type_not_only_cl_name() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let done = artifact(&projects, "20260601130600");
        write_json(
            &done.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782012000.0,
                "name": "done",
                "cl_name": "shared",
            }),
        );
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "workflow".to_string(),
                cl_name: "shared".to_string(),
                raw_suffix: Some("20260601130600".to_string()),
                dismissed_at: None,
            },
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| record.timestamp == "20260601130600"),
            "a workflow dismissal must not hide a run agent with the same CL"
        );
    }

    #[test]
    fn inbox_query_keeps_old_non_dismissed_completed_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (_active, dismissed_done, _other_done) =
            build_inbox_fixture(&projects, &index);

        // No dismissals registered: every completed row stays visible.
        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        assert!(dirs.contains(&dismissed_done.to_string_lossy().as_ref()));
    }

    #[test]
    fn running_alias_of_dismissed_completion_stays_visible() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (active, _dismissed_done, _other_done) =
            build_inbox_fixture(&projects, &index);

        // A whole-identity dismissal (empty raw_suffix) would still leave the
        // active RUNNING row visible because the active branch never consults
        // dismissal state.
        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_alpha".to_string(),
                raw_suffix: None,
                dismissed_at: None,
            },
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        assert!(
            dirs.contains(&active.to_string_lossy().as_ref()),
            "running alias must stay visible even when identity is dismissed"
        );
    }

    #[test]
    fn hidden_rows_remain_excluded_unless_requested() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let hidden = artifact(&projects, "20260602000000");
        write_json(
            &hidden.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782020000.0,
                "name": "hidden",
                "cl_name": "cl_hidden",
                "hidden": true,
            }),
        );
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(snapshot.records.is_empty());

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_hidden: true,
                ..inbox_query()
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
    }

    #[test]
    fn replace_dismissed_agent_visibility_is_atomic() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (_active, dismissed_done, _other_done) =
            build_inbox_fixture(&projects, &index);

        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_alpha".to_string(),
                raw_suffix: Some("20260601100000".to_string()),
                dismissed_at: None,
            },
        )
        .unwrap();

        // Replace with a different identity. The old row must disappear.
        let update = replace_dismissed_agent_visibility(
            &index,
            vec![DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_beta".to_string(),
                raw_suffix: Some("20260601110000".to_string()),
                dismissed_at: None,
            }],
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 1);
        assert_eq!(update.rows_deleted, 1);

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        // The previously dismissed cl_alpha completion is back; the newly
        // dismissed cl_beta completion is gone.
        assert!(dirs.contains(&dismissed_done.to_string_lossy().as_ref()));
        assert!(snapshot
            .records
            .iter()
            .all(|r| r.timestamp != "20260601110000"));
    }

    #[test]
    fn delete_dismissed_agent_visibility_revives_completion() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (_active, dismissed_done, _other_done) =
            build_inbox_fixture(&projects, &index);

        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_alpha".to_string(),
                raw_suffix: Some("20260601100000".to_string()),
                dismissed_at: None,
            },
        )
        .unwrap();

        // Confirm dismissed first.
        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(snapshot.records.iter().all(|r| {
            r.artifact_dir != dismissed_done.to_string_lossy().as_ref()
        }));

        let update = delete_dismissed_agent_visibility(
            &index,
            "run",
            "cl_alpha",
            Some("20260601100000"),
        )
        .unwrap();
        assert_eq!(update.rows_deleted, 1);

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            inbox_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        assert!(dirs.contains(&dismissed_done.to_string_lossy().as_ref()));
    }

    #[test]
    fn include_dismissed_default_is_backward_compatible() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let (_active, dismissed_done, _other_done) =
            build_inbox_fixture(&projects, &index);

        upsert_dismissed_agent_visibility(
            &index,
            DismissedAgentIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "cl_alpha".to_string(),
                raw_suffix: Some("20260601100000".to_string()),
                dismissed_at: None,
            },
        )
        .unwrap();

        // Default query has include_dismissed = true, so the dismissed
        // completion is still returned. Phase 1 callers see no change.
        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let dirs: Vec<&str> = snapshot
            .records
            .iter()
            .map(|r| r.artifact_dir.as_str())
            .collect();
        assert!(dirs.contains(&dismissed_done.to_string_lossy().as_ref()));
    }
}
