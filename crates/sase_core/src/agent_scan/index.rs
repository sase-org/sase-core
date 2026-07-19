//! SQLite materialized view for agent artifact summaries.
//!
//! The artifact tree remains the source of truth. This module stores one
//! row per artifact directory with denormalized query fields and the
//! scanner's canonical `AgentArtifactRecordWire` JSON payload so indexed
//! queries can return loader-equivalent records without walking every
//! historical timestamp directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_launch::list_workspace_claims_from_content;

use super::scanner::{
    project_allowed_by_filter, project_filter_for_scan,
    scan_agent_artifact_dir, scan_agent_artifacts,
};
use super::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire, DoneMarkerWire,
    AGENT_SCAN_WIRE_SCHEMA_VERSION,
};

pub const AGENT_ARTIFACT_INDEX_SCHEMA_VERSION: u32 = 14;

const MARKER_FILES: &[&str] = &[
    "agent_meta.json",
    "done.json",
    "running.json",
    "waiting.json",
    "pending_question.json",
    "workflow_state.json",
    "plan_path.json",
];

const TERMINAL_WORKFLOW_STATUSES: &[&str] =
    &["completed", "failed", "cancelled", "noop"];
const MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS: usize = 128;
const MAX_RELATED_ARTIFACT_QUERY_ITERATIONS: usize = 32;
const ABANDONED_DONE_OUTCOME: &str = "abandoned";
const DEFAULT_INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

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
    pub active_limit: Option<u32>,
    #[serde(default)]
    pub recent_completed_limit: Option<u32>,
    #[serde(default)]
    pub include_hidden: bool,
}

impl Default for AgentArtifactIndexQueryWire {
    fn default() -> Self {
        Self {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(200),
            include_hidden: false,
        }
    }
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

/// Lightweight status for the persistent artifact index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexStatusWire {
    pub schema_version: u32,
    pub index_path: String,
    pub agent_artifacts_rows: u64,
    pub dismissed_agents_rows: u64,
    pub agent_artifact_aliases_rows: u64,
}

/// Rebuild the index from the canonical artifact tree.
pub fn rebuild_agent_artifact_index(
    index_path: &Path,
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index_for_rebuild(index_path)?;
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
    delete_agent_artifact_index_row_with_busy_timeout(
        index_path,
        artifact_dir,
        DEFAULT_INDEX_BUSY_TIMEOUT,
    )
}

/// Delete one artifact row with a caller-supplied SQLite contention window.
pub fn delete_agent_artifact_index_row_with_busy_timeout(
    index_path: &Path,
    artifact_dir: &Path,
    busy_timeout: Duration,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let conn = open_index_with_busy_timeout(index_path, busy_timeout)?;
    let artifact_dir =
        resolve_index_artifact_dir(&conn, &artifact_dir.to_string_lossy())?;
    let deleted = conn
        .execute(
            "DELETE FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.as_str()],
        )
        .map_err(|e| e.to_string())? as u64;
    let _ = conn.execute(
        "DELETE FROM agent_artifact_aliases WHERE artifact_dir = ?1 OR alias_path = ?1",
        [artifact_dir.as_str()],
    );

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: 0,
        rows_deleted: deleted,
        rows_skipped: 0,
    })
}

/// Terminalize stale, unclaimed active rows that no longer have live markers.
///
/// This is background index maintenance, not a hot-query repair path. It keeps
/// abandoned no-marker runs out of the active tier while preserving rows that
/// still have a running marker, waiting/question marker, workflow state, or a
/// live workspace claim.
pub fn terminalize_stale_active_agent_artifact_index_rows(
    index_path: &Path,
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
    stale_after_seconds: u64,
    max_rows: Option<u32>,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let repaired = repair_abandoned_agent_artifact_index_rows(&mut conn)?;
    let candidates = select_terminalization_candidates(&conn, max_rows)?;
    let stale_after = Duration::from_secs(stale_after_seconds);
    let mut rows_indexed = repaired;
    let mut rows_skipped = 0u64;

    for row in candidates {
        match terminalize_stale_candidate(&conn, &row, &options, stale_after)? {
            TerminalizationOutcome::Terminalized => rows_indexed += 1,
            TerminalizationOutcome::Skipped => rows_skipped += 1,
        }
    }

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        rows_indexed,
        rows_deleted: 0,
        rows_skipped,
    })
}

fn repair_abandoned_agent_artifact_index_rows(
    conn: &mut Connection,
) -> Result<u64, String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let pattern = format!("%\"outcome\":\"{ABANDONED_DONE_OUTCOME}\"%");
    let updates: Vec<(String, AgentArtifactRecordWire)> = {
        let mut stmt = tx
            .prepare(
                "SELECT projects_root, hidden, cl_name, record_json \
                 FROM agent_artifacts \
                 WHERE has_done_marker = 1 \
                   AND record_json LIKE ?1 \
                   AND ( \
                       hidden = 0 \
                       OR record_json LIKE '%\"done\":{%\"hidden\":false%' \
                       OR ( \
                           (cl_name IS NULL OR cl_name = '' OR cl_name = 'unknown') \
                           AND record_json LIKE '%\"agent_meta\":{%\"cl_name\":\"%' \
                       ) \
                   )",
            )
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([pattern]).map_err(|e| e.to_string())?;
        let mut updates = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let projects_root: String =
                row.get(0).map_err(|e| e.to_string())?;
            let row_hidden: i64 = row.get(1).map_err(|e| e.to_string())?;
            let row_cl_name: Option<String> =
                row.get(2).map_err(|e| e.to_string())?;
            let record_json: String = row.get(3).map_err(|e| e.to_string())?;
            let Ok(mut record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
            else {
                continue;
            };
            if !record
                .done
                .as_ref()
                .and_then(|done| done.outcome.as_deref())
                .is_some_and(|outcome| outcome == ABANDONED_DONE_OUTCOME)
            {
                continue;
            }
            let meta_cl_name = record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.cl_name.clone())
                .filter(|name| !name.is_empty());
            let mut changed = row_hidden == 0
                || (meta_cl_name.is_some()
                    && cl_name_is_unknownish(row_cl_name.as_deref()));
            if let Some(done) = record.done.as_mut() {
                if !done.hidden {
                    done.hidden = true;
                    changed = true;
                }
                if let Some(cl_name) = meta_cl_name {
                    if cl_name_is_unknownish(done.cl_name.as_deref()) {
                        done.cl_name = Some(cl_name);
                        changed = true;
                    }
                }
            }
            if changed {
                updates.push((projects_root, record));
            }
        }
        updates
    };

    let repaired = updates.len() as u64;
    for (projects_root, record) in updates {
        upsert_record(&tx, Path::new(&projects_root), &record)?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(repaired)
}

pub(crate) fn cl_name_is_unknownish(cl_name: Option<&str>) -> bool {
    cl_name
        .map(|name| name.is_empty() || name == "unknown")
        .unwrap_or(true)
}

/// Replace the dismissed identity table used by normal index visibility.
pub fn replace_agent_artifact_index_dismissed_agents(
    index_path: &Path,
    dismissed: &[AgentCleanupIdentityWire],
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let deleted = tx
        .execute("DELETE FROM dismissed_agents", [])
        .map_err(|e| e.to_string())? as u64;
    for identity in dismissed {
        tx.execute(
            r#"
            INSERT OR REPLACE INTO dismissed_agents (
                agent_type, cl_name, raw_suffix
            ) VALUES (?1, ?2, ?3)
            "#,
            params![
                identity.agent_type,
                identity.cl_name,
                identity.raw_suffix,
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: dismissed.len() as u64,
        rows_deleted: deleted,
        rows_skipped: 0,
    })
}

/// Read one artifact-index metadata value.
pub fn read_agent_artifact_index_meta(
    index_path: &Path,
    key: &str,
) -> Result<Option<String>, String> {
    let conn = open_index(index_path)?;
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
        row.get::<_, String>(0)
    })
    .optional()
    .map_err(|e| e.to_string())
}

/// Write one artifact-index metadata value.
pub fn write_agent_artifact_index_meta(
    index_path: &Path,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let conn = open_index(index_path)?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        params![key, value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Return lightweight row counts for the artifact index.
pub fn agent_artifact_index_status(
    index_path: &Path,
) -> Result<AgentArtifactIndexStatusWire, String> {
    let conn = open_index(index_path)?;
    Ok(AgentArtifactIndexStatusWire {
        schema_version: read_index_schema_version(&conn)?,
        index_path: index_path.to_string_lossy().into_owned(),
        agent_artifacts_rows: count_table_rows(&conn, "agent_artifacts")?,
        dismissed_agents_rows: count_table_rows(&conn, "dismissed_agents")?,
        agent_artifact_aliases_rows: count_table_rows(
            &conn,
            "agent_artifact_aliases",
        )?,
    })
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
    let project_filter = project_filter_for_scan(projects_root, &options);
    repair_stale_rows_for_query(
        &conn,
        &query,
        &options,
        project_filter.as_ref(),
    )?;

    if query.include_active {
        select_records(
            &conn,
            SelectRecordsQuery {
                where_sql: active_where(
                    query.include_hidden,
                    project_filter.as_ref(),
                ),
                limit: query.active_limit,
                selection: RecordSelection::Active,
                include_hidden: query.include_hidden,
            },
            &mut stats,
            &mut by_dir,
            &options,
            project_filter.as_ref(),
        )?;
    }

    if query.include_recent_completed {
        select_records(
            &conn,
            SelectRecordsQuery {
                where_sql: completed_where(
                    query.include_hidden,
                    project_filter.as_ref(),
                ),
                limit: query.recent_completed_limit,
                selection: RecordSelection::Completed,
                include_hidden: query.include_hidden,
            },
            &mut stats,
            &mut by_dir,
            &options,
            project_filter.as_ref(),
        )?;
    }

    if query.include_full_history {
        select_records(
            &conn,
            SelectRecordsQuery {
                where_sql: visible_where(
                    query.include_hidden,
                    project_filter.as_ref(),
                ),
                limit: None,
                selection: RecordSelection::Visible,
                include_hidden: query.include_hidden,
            },
            &mut stats,
            &mut by_dir,
            &options,
            project_filter.as_ref(),
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

/// Return artifact directories related to one logical agent lineage.
///
/// The query is scoped to the indexed current artifact's project/workflow
/// parent, then follows direct timestamp pointers in the materialized index
/// (`parent_timestamp`, retry back/forward pointers, and retry-chain root).
/// This keeps tools-panel lookups proportional to the lineage size instead
/// of the number of historical sibling artifact directories.
pub fn query_related_agent_artifact_dirs(
    index_path: &Path,
    artifact_dir: &Path,
    seed_timestamps: &[String],
) -> Result<Vec<String>, String> {
    let conn = open_index(index_path)?;
    let current_path =
        resolve_index_artifact_dir(&conn, &artifact_dir.to_string_lossy())?;
    let Some(current) =
        select_lineage_row_by_artifact_dir(&conn, &current_path)?
    else {
        return Ok(Vec::new());
    };

    let mut timestamps: BTreeSet<String> = BTreeSet::new();
    for timestamp in seed_timestamps {
        insert_lineage_timestamp(&mut timestamps, timestamp);
    }
    insert_lineage_timestamp(&mut timestamps, &current.timestamp);
    current.add_related_timestamps(&mut timestamps);

    let mut by_dir: BTreeMap<String, IndexedLineageRow> = BTreeMap::new();
    by_dir.insert(current.artifact_dir.clone(), current.clone());

    for _ in 0..MAX_RELATED_ARTIFACT_QUERY_ITERATIONS {
        let rows = select_lineage_rows(
            &conn,
            &current.project_name,
            &current.workflow_dir_name,
            &timestamps,
        )?;
        let mut changed = false;
        for row in rows {
            changed |= row.add_related_timestamps(&mut timestamps);
            if !by_dir.contains_key(&row.artifact_dir) {
                changed = true;
            }
            by_dir.insert(row.artifact_dir.clone(), row);
        }
        if !changed {
            break;
        }
    }

    let mut rows: Vec<IndexedLineageRow> = by_dir.into_values().collect();
    rows.sort_by(|a, b| {
        (a.timestamp.as_str(), a.artifact_dir.as_str())
            .cmp(&(b.timestamp.as_str(), b.artifact_dir.as_str()))
    });

    let mut dirs: Vec<String> =
        rows.into_iter().map(|row| row.artifact_dir).collect();
    if let Some(index) = dirs.iter().position(|path| path == &current_path) {
        let current = dirs.remove(index);
        dirs.insert(0, current);
    }
    Ok(dirs)
}

fn open_index(index_path: &Path) -> Result<Connection, String> {
    open_index_with_busy_timeout(index_path, DEFAULT_INDEX_BUSY_TIMEOUT)
}

fn open_index_with_busy_timeout(
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
            agent_family TEXT,
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
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent_family
            ON agent_artifacts(agent_family, timestamp);
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
        CREATE INDEX IF NOT EXISTS idx_dismissed_agents_suffix
            ON dismissed_agents(raw_suffix, cl_name, agent_type);
        CREATE TABLE IF NOT EXISTS agent_artifact_aliases (
            alias_path TEXT PRIMARY KEY,
            artifact_dir TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_agent_artifact_aliases_artifact_dir
            ON agent_artifact_aliases(artifact_dir);
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
    if prior_version.map_or(true, |v| v < 3) {
        ensure_agent_artifacts_column(&conn, "pending_question_sig", "TEXT")?;
    }
    if prior_version.map_or(true, |v| v < 4) {
        ensure_agent_artifacts_column(&conn, "workflow_name", "TEXT")?;
        ensure_agent_artifacts_column(&conn, "agent_family", "TEXT")?;
    }
    if prior_version.map_or(true, |v| v < 5) {
        migrate_record_json_refresh_v5(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 6) {
        migrate_record_json_refresh_v6(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 7) {
        migrate_record_json_refresh_v7(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 8) {
        migrate_record_json_refresh_v8(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 9) {
        migrate_record_json_refresh_v9(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 10) {
        migrate_record_json_refresh_v10(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 11) {
        ensure_agent_artifacts_column(&conn, "agent_clan", "TEXT")?;
        migrate_record_json_refresh_v11(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 12) {
        migrate_record_json_refresh_v12(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 13) {
        migrate_record_json_refresh_v13(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 14) {
        migrate_record_json_refresh_v14(&mut conn)?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent_clan \
         ON agent_artifacts(agent_clan, timestamp);",
    )
    .map_err(|e| e.to_string())?;

    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
        [AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string()],
    )
    .map_err(|e| e.to_string())?;
    Ok(conn)
}

fn read_index_schema_version(conn: &Connection) -> Result<u32, String> {
    let raw: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    raw.parse::<u32>().map_err(|e| e.to_string())
}

fn resolve_index_artifact_dir(
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

fn count_table_rows(conn: &Connection, table: &str) -> Result<u64, String> {
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(|e| e.to_string())?;
    u64::try_from(count).map_err(|e| e.to_string())
}

fn ensure_agent_artifacts_column(
    conn: &Connection,
    column: &str,
    column_type: &str,
) -> Result<(), String> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(agent_artifacts)")
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let existing: String = row.get(1).map_err(|e| e.to_string())?;
        if existing == column {
            return Ok(());
        }
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

fn open_index_for_rebuild(index_path: &Path) -> Result<Connection, String> {
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

fn is_sqlite_index_corruption_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database disk image is malformed")
        || lower.contains("file is not a database")
        || lower.contains("not a database")
        || lower.contains("malformed database schema")
        || lower.contains("unsupported file format")
}

fn replace_unusable_index_file(index_path: &Path) -> Result<(), String> {
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

fn corrupt_index_quarantine_path(index_path: &Path) -> PathBuf {
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

fn sqlite_sidecar_path(index_path: &Path, suffix: &str) -> PathBuf {
    let mut raw = index_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

/// One-shot v1 → v2 migration: recompute `hidden` for previously-indexed
/// rows that the old projection marked hidden purely because the workflow
/// was anonymous (`is_anonymous = true`). Idempotent; safe to run on an
/// already-migrated index (no rows will change because `is_anonymous` no
/// longer participates in `RecordSummary::from_record`).
fn migrate_recompute_hidden_v2(conn: &mut Connection) -> Result<(), String> {
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
            let Ok(record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
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
fn migrate_record_json_refresh_v5(conn: &mut Connection) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v6 adds `agent_meta.reasoning_effort` and
/// `prompt_steps[*].reasoning_effort` inside `record_json` so the ACE TUI can
/// render the resolved effort uniformly across providers.
///
/// There is no DDL to apply; callers that need existing rows refreshed run a
/// full rebuild so each row is reserialized from source marker files.
fn migrate_record_json_refresh_v6(conn: &mut Connection) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v7 is a reserved `record_json` refresh migration.
///
/// There is no DDL to apply; callers that need existing rows refreshed run a
/// full rebuild so each row is reserialized from source marker files.
fn migrate_record_json_refresh_v7(conn: &mut Connection) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v8 adds `agent_meta.plan_committed` inside `record_json`. The Python
/// lifecycle checks the stored version before opening the Rust index and
/// performs a source rebuild so existing rows receive the new projection.
fn migrate_record_json_refresh_v8(conn: &mut Connection) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v9 adds `agent_meta.output_path` inside `record_json` so failed workflow
/// rows can expose their runner log without re-reading marker files.
fn migrate_record_json_refresh_v9(conn: &mut Connection) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v10 adds `agent_meta.agent_family_parallel` inside `record_json` so
/// indexed consumers can distinguish parallel members from serial children.
fn migrate_record_json_refresh_v10(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v11 adds the denormalized `agent_clan` projection and refreshes
/// `record_json` with `agent_meta.agent_clan`.
fn migrate_record_json_refresh_v11(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v12 refreshes `record_json` with `agent_meta.agent_clan_generation` and
/// `agent_meta.clan_tribe` for clan-level tribe resolution.
fn migrate_record_json_refresh_v12(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v13 refreshes ``record_json`` so agent metadata serializes the canonical
/// ``tribe`` field.  Startup detects the old version without opening this
/// index and schedules the source rebuild off the UI thread.
fn migrate_record_json_refresh_v13(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v14 refreshes `record_json` with `agent_meta.clan_summary` for clan-level
/// summary resolution.
fn migrate_record_json_refresh_v14(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
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
            workflow_dir_name, workflow_name, agent_clan, agent_family, timestamp,
            status, agent_type, cl_name,
            agent_name, model, llm_provider, started_at, finished_at,
            has_done_marker, has_running_marker, has_waiting_marker,
            has_workflow_state, workflow_status, hidden, parent_timestamp,
            step_index, step_name, retry_of_timestamp, retried_as_timestamp,
            retry_chain_root_timestamp, retry_attempt, agent_meta_sig, done_sig,
            running_sig, waiting_sig, pending_question_sig,
            workflow_state_sig, plan_path_sig, prompt_steps_sig, record_json,
            indexed_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT(artifact_dir) DO UPDATE SET
            projects_root = excluded.projects_root,
            project_name = excluded.project_name,
            project_dir = excluded.project_dir,
            project_file = excluded.project_file,
            workflow_dir_name = excluded.workflow_dir_name,
            workflow_name = excluded.workflow_name,
            agent_clan = excluded.agent_clan,
            agent_family = excluded.agent_family,
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
            pending_question_sig = excluded.pending_question_sig,
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
            summary.workflow_name,
            summary.agent_clan,
            summary.agent_family,
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
            signatures.pending_question,
            signatures.workflow_state,
            signatures.plan_path,
            signatures.prompt_steps,
            record_json,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn repair_stale_rows_for_query(
    conn: &Connection,
    query: &AgentArtifactIndexQueryWire,
    options: &AgentArtifactScanOptionsWire,
    project_filter: Option<&BTreeSet<String>>,
) -> Result<(), String> {
    let mut clauses: Vec<&str> = Vec::new();
    if !query.include_hidden {
        clauses.push("hidden = 1");
    }
    if query.include_recent_completed && !query.include_active {
        clauses.push(
            "(has_done_marker = 0
              OR workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop'))",
        );
    }
    if clauses.is_empty() {
        return Ok(());
    }

    let where_sql = add_project_filter_to_where(
        format!("WHERE {}", clauses.join(" OR ")),
        project_filter,
    );
    refresh_stale_rows(conn, &where_sql, options)
}

fn select_terminalization_candidates(
    conn: &Connection,
    max_rows: Option<u32>,
) -> Result<Vec<PendingRow>, String> {
    let mut sql = String::from(
        "SELECT artifact_dir, projects_root, record_json, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig FROM agent_artifacts \
         WHERE has_done_marker = 0 \
           AND has_running_marker = 0 \
           AND has_waiting_marker = 0 \
           AND has_workflow_state = 0 \
           AND pending_question_sig IS NULL \
         ORDER BY timestamp ASC, artifact_dir ASC",
    );
    if max_rows.is_some() {
        sql.push_str(" LIMIT ?1");
    }

    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = if let Some(limit) = max_rows {
        stmt.query([limit]).map_err(|e| e.to_string())?
    } else {
        stmt.query([]).map_err(|e| e.to_string())?
    };
    let mut candidates = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        candidates.push(pending_row_from_sql(row)?);
    }
    Ok(candidates)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalizationOutcome {
    Terminalized,
    Skipped,
}

fn terminalize_stale_candidate(
    conn: &Connection,
    row: &PendingRow,
    options: &AgentArtifactScanOptionsWire,
    stale_after: Duration,
) -> Result<TerminalizationOutcome, String> {
    let current = MarkerSignatures::from_artifact_dir(&row.artifact_dir);
    let projects_root = PathBuf::from(&row.row_projects_root);
    let record = if row.stored == current {
        match serde_json::from_str::<AgentArtifactRecordWire>(&row.record_json)
        {
            Ok(record) => record,
            Err(_) => return Ok(TerminalizationOutcome::Skipped),
        }
    } else {
        let artifact_dir = PathBuf::from(&row.artifact_dir);
        match scan_agent_artifact_dir(&projects_root, &artifact_dir, options) {
            Some(refreshed) => {
                let _ = upsert_record(conn, &projects_root, &refreshed);
                refreshed
            }
            None => return Ok(TerminalizationOutcome::Skipped),
        }
    };

    if !record_is_terminalization_candidate(&record) {
        return Ok(TerminalizationOutcome::Skipped);
    }
    if !artifact_dir_is_stale(&record.artifact_dir, stale_after) {
        return Ok(TerminalizationOutcome::Skipped);
    }
    if record_has_live_workspace_claim(&record)? {
        return Ok(TerminalizationOutcome::Skipped);
    }

    let terminalized = terminalized_abandoned_record(record);
    upsert_record(conn, &projects_root, &terminalized)?;
    Ok(TerminalizationOutcome::Terminalized)
}

fn record_is_terminalization_candidate(
    record: &AgentArtifactRecordWire,
) -> bool {
    !record.has_done_marker
        && record.done.is_none()
        && record.running.is_none()
        && record.waiting.is_none()
        && record.pending_question.is_none()
        && record.workflow_state.is_none()
}

fn artifact_dir_is_stale(artifact_dir: &str, stale_after: Duration) -> bool {
    let Some(latest) = artifact_dir_latest_modified(artifact_dir) else {
        return false;
    };
    SystemTime::now()
        .duration_since(latest)
        .map(|age| age >= stale_after)
        .unwrap_or(false)
}

fn artifact_dir_latest_modified(artifact_dir: &str) -> Option<SystemTime> {
    let dir = Path::new(artifact_dir);
    let mut latest = fs::metadata(dir).and_then(|m| m.modified()).ok();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let modified = entry.metadata().and_then(|m| m.modified()).ok();
            latest = max_system_time(latest, modified);
        }
    }
    latest
}

fn max_system_time(
    left: Option<SystemTime>,
    right: Option<SystemTime>,
) -> Option<SystemTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn record_has_live_workspace_claim(
    record: &AgentArtifactRecordWire,
) -> Result<bool, String> {
    let project_file = Path::new(&record.project_file);
    let content = match fs::read_to_string(project_file) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(_) => return Ok(true),
    };
    let claims = list_workspace_claims_from_content(&content);
    if claims.is_empty() {
        return Ok(false);
    }

    let summary = RecordSummary::from_record(record);
    let workspace_num = record_workspace_num(record);
    for claim in claims {
        if claim.artifacts_timestamp.as_deref()
            == Some(record.timestamp.as_str())
        {
            return Ok(true);
        }
        if workspace_num.is_some_and(|num| num == claim.workspace_num) {
            return Ok(true);
        }
        if claim.workflow == record.workflow_dir_name
            && claim.cl_name.as_deref() == summary.cl_name.as_deref()
            && summary.cl_name.is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn record_workspace_num(record: &AgentArtifactRecordWire) -> Option<u32> {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .or_else(|| record.done.as_ref().and_then(|done| done.workspace_num))
        .and_then(|num| u32::try_from(num).ok())
}

fn terminalized_abandoned_record(
    mut record: AgentArtifactRecordWire,
) -> AgentArtifactRecordWire {
    let summary = RecordSummary::from_record(&record);
    let meta = record.agent_meta.as_ref();
    record.running = None;
    record.waiting = None;
    record.pending_question = None;
    record.has_done_marker = true;
    record.done = Some(DoneMarkerWire {
        outcome: Some(ABANDONED_DONE_OUTCOME.to_string()),
        finished_at: current_unix_timestamp_secs(),
        cl_name: summary
            .cl_name
            .clone()
            .or_else(|| Some("unknown".to_string())),
        project_file: Some(record.project_file.clone()),
        workspace_num: meta.and_then(|m| m.workspace_num),
        workspace_dir: meta.and_then(|m| m.workspace_dir.clone()),
        pid: meta.and_then(|m| m.pid),
        model: summary.model.clone(),
        llm_provider: summary.llm_provider.clone(),
        vcs_provider: meta.and_then(|m| m.vcs_provider.clone()),
        name: summary.agent_name.clone(),
        hidden: true,
        ..DoneMarkerWire::default()
    });
    record
}

fn current_unix_timestamp_secs() -> Option<f64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

fn refresh_stale_rows(
    conn: &Connection,
    where_sql: &str,
    options: &AgentArtifactScanOptionsWire,
) -> Result<(), String> {
    let sql = format!(
        "SELECT artifact_dir, projects_root, record_json, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig FROM agent_artifacts {where_sql}"
    );
    let mut pending: Vec<PendingRow> = Vec::new();
    {
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            pending.push(pending_row_from_sql(row)?);
        }
    }

    for row in pending {
        let current = MarkerSignatures::from_artifact_dir(&row.artifact_dir);
        if row.stored == current {
            continue;
        }
        let projects_root = PathBuf::from(&row.row_projects_root);
        let artifact_dir = PathBuf::from(&row.artifact_dir);
        if let Some(refreshed) =
            scan_agent_artifact_dir(&projects_root, &artifact_dir, options)
        {
            let _ = upsert_record(conn, &projects_root, &refreshed);
        }
    }
    Ok(())
}

fn select_records(
    conn: &Connection,
    query: SelectRecordsQuery,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
    options: &AgentArtifactScanOptionsWire,
    project_filter: Option<&BTreeSet<String>>,
) -> Result<(), String> {
    let mut sql = format!(
        "SELECT artifact_dir, projects_root, record_json, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig \
         FROM agent_artifacts {}",
        query.where_sql
    );
    if query.limit.is_some() {
        sql.push_str(" LIMIT ?1");
    }

    let mut pending: Vec<PendingRow> = Vec::new();
    {
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = if let Some(limit) = query.limit {
            stmt.query([limit]).map_err(|e| e.to_string())?
        } else {
            stmt.query([]).map_err(|e| e.to_string())?
        };

        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            if by_dir.contains_key(&artifact_dir) {
                continue;
            }
            pending.push(pending_row_from_sql_with_artifact_dir(
                row,
                artifact_dir,
            )?);
        }
    }

    for row in pending {
        let current = MarkerSignatures::from_artifact_dir(&row.artifact_dir);
        let record = if row.stored == current {
            match serde_json::from_str::<AgentArtifactRecordWire>(
                &row.record_json,
            ) {
                Ok(record) => record,
                Err(_) => {
                    stats.json_decode_errors += 1;
                    continue;
                }
            }
        } else {
            let projects_root = PathBuf::from(&row.row_projects_root);
            let artifact_dir = PathBuf::from(&row.artifact_dir);
            match scan_agent_artifact_dir(
                &projects_root,
                &artifact_dir,
                options,
            ) {
                Some(refreshed) => {
                    // Best-effort: persist the refreshed record so the next
                    // query sees fresh data without re-doing the rescan. A
                    // single INSERT ... ON CONFLICT is atomic in SQLite, so
                    // concurrent readers see either the old or new row but
                    // never a torn write. Upsert failure is non-fatal — we
                    // still return the refreshed record to the caller.
                    let _ = upsert_record(conn, &projects_root, &refreshed);
                    refreshed
                }
                None => match serde_json::from_str::<AgentArtifactRecordWire>(
                    &row.record_json,
                ) {
                    Ok(record) => record,
                    Err(_) => {
                        stats.json_decode_errors += 1;
                        continue;
                    }
                },
            }
        };
        if !project_allowed_by_filter(&record.project_name, project_filter) {
            continue;
        }
        if record_matches_selection(
            conn,
            &record,
            query.selection,
            query.include_hidden,
        )? {
            by_dir.insert(row.artifact_dir, record);
        }
    }
    Ok(())
}

fn pending_row_from_sql(row: &rusqlite::Row<'_>) -> Result<PendingRow, String> {
    let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
    pending_row_from_sql_with_artifact_dir(row, artifact_dir)
}

fn pending_row_from_sql_with_artifact_dir(
    row: &rusqlite::Row<'_>,
    artifact_dir: String,
) -> Result<PendingRow, String> {
    let row_projects_root: String = row.get(1).map_err(|e| e.to_string())?;
    let record_json: String = row.get(2).map_err(|e| e.to_string())?;
    let stored = MarkerSignatures {
        agent_meta: row.get(3).map_err(|e| e.to_string())?,
        done: row.get(4).map_err(|e| e.to_string())?,
        running: row.get(5).map_err(|e| e.to_string())?,
        waiting: row.get(6).map_err(|e| e.to_string())?,
        pending_question: row.get(7).map_err(|e| e.to_string())?,
        workflow_state: row.get(8).map_err(|e| e.to_string())?,
        plan_path: row.get(9).map_err(|e| e.to_string())?,
        prompt_steps: row.get(10).map_err(|e| e.to_string())?,
    };
    Ok(PendingRow {
        artifact_dir,
        row_projects_root,
        record_json,
        stored,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordSelection {
    Active,
    Completed,
    Visible,
}

struct SelectRecordsQuery {
    where_sql: String,
    limit: Option<u32>,
    selection: RecordSelection,
    include_hidden: bool,
}

#[derive(Debug, Clone)]
struct IndexedLineageRow {
    artifact_dir: String,
    project_name: String,
    workflow_dir_name: String,
    timestamp: String,
    parent_timestamp: Option<String>,
    retry_of_timestamp: Option<String>,
    retried_as_timestamp: Option<String>,
    retry_chain_root_timestamp: Option<String>,
}

impl IndexedLineageRow {
    fn add_related_timestamps(
        &self,
        timestamps: &mut BTreeSet<String>,
    ) -> bool {
        let mut changed = insert_lineage_timestamp(timestamps, &self.timestamp);
        for value in [
            self.parent_timestamp.as_deref(),
            self.retry_of_timestamp.as_deref(),
            self.retried_as_timestamp.as_deref(),
            self.retry_chain_root_timestamp.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            changed |= insert_lineage_timestamp(timestamps, value);
        }
        changed
    }
}

fn insert_lineage_timestamp(
    timestamps: &mut BTreeSet<String>,
    value: &str,
) -> bool {
    if value.is_empty()
        || timestamps.len() >= MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS
    {
        return false;
    }
    timestamps.insert(value.to_string())
}

fn select_lineage_row_by_artifact_dir(
    conn: &Connection,
    artifact_dir: &str,
) -> Result<Option<IndexedLineageRow>, String> {
    conn.query_row(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               parent_timestamp, retry_of_timestamp, retried_as_timestamp,
               retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE artifact_dir = ?1
        "#,
        [artifact_dir],
        lineage_row_from_sql,
    )
    .optional()
    .map_err(|e| e.to_string())
}

fn select_lineage_rows(
    conn: &Connection,
    project_name: &str,
    workflow_dir_name: &str,
    timestamps: &BTreeSet<String>,
) -> Result<Vec<IndexedLineageRow>, String> {
    if timestamps.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = placeholders(timestamps.len());
    let sql = format!(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               parent_timestamp, retry_of_timestamp, retried_as_timestamp,
               retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE project_name = ?
          AND workflow_dir_name = ?
          AND (
              timestamp IN ({placeholders})
              OR parent_timestamp IN ({placeholders})
              OR retry_of_timestamp IN ({placeholders})
              OR retried_as_timestamp IN ({placeholders})
              OR retry_chain_root_timestamp IN ({placeholders})
          )
        ORDER BY timestamp ASC, artifact_dir ASC
        "#
    );
    let mut values: Vec<String> = Vec::with_capacity(2 + timestamps.len() * 5);
    values.push(project_name.to_string());
    values.push(workflow_dir_name.to_string());
    for _ in 0..5 {
        values.extend(timestamps.iter().cloned());
    }

    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(lineage_row_from_sql(row).map_err(|e| e.to_string())?);
    }
    Ok(result)
}

fn placeholders(len: usize) -> String {
    std::iter::repeat("?")
        .take(len)
        .collect::<Vec<_>>()
        .join(", ")
}

fn lineage_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<IndexedLineageRow> {
    Ok(IndexedLineageRow {
        artifact_dir: row.get(0)?,
        project_name: row.get(1)?,
        workflow_dir_name: row.get(2)?,
        timestamp: row.get(3)?,
        parent_timestamp: row.get(4)?,
        retry_of_timestamp: row.get(5)?,
        retried_as_timestamp: row.get(6)?,
        retry_chain_root_timestamp: row.get(7)?,
    })
}

fn record_matches_selection(
    conn: &Connection,
    record: &AgentArtifactRecordWire,
    selection: RecordSelection,
    include_hidden: bool,
) -> Result<bool, String> {
    let summary = RecordSummary::from_record(record);
    if !include_hidden {
        if summary.hidden {
            return Ok(false);
        }
        if record_is_dismissed(conn, record, &summary)? {
            return Ok(false);
        }
    }

    Ok(match selection {
        RecordSelection::Active => record_is_active(record),
        RecordSelection::Completed => record_is_completed(record),
        RecordSelection::Visible => true,
    })
}

fn record_is_active(record: &AgentArtifactRecordWire) -> bool {
    !record.has_done_marker
        || record.workflow_state.as_ref().is_some_and(|workflow| {
            !is_terminal_workflow_status(&workflow.status)
        })
}

fn record_is_completed(record: &AgentArtifactRecordWire) -> bool {
    record.has_done_marker
        || record.workflow_state.as_ref().is_some_and(|workflow| {
            is_terminal_workflow_status(&workflow.status)
        })
}

fn is_terminal_workflow_status(status: &str) -> bool {
    TERMINAL_WORKFLOW_STATUSES.contains(&status)
}

fn record_is_dismissed(
    conn: &Connection,
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
) -> Result<bool, String> {
    let workflow_terminal = record
        .workflow_state
        .as_ref()
        .is_some_and(|workflow| is_terminal_workflow_status(&workflow.status));
    let inert_without_markers = record.running.is_none()
        && record.waiting.is_none()
        && record.workflow_state.is_none()
        && !record.has_done_marker;
    let terminal_or_inert =
        record.has_done_marker || workflow_terminal || inert_without_markers;
    let dismissed_agent_type = if summary.agent_type == "workflow" {
        "workflow"
    } else {
        "run"
    };
    let mut stmt = conn
        .prepare(
            r#"
            SELECT 1 FROM dismissed_agents dismissed
            WHERE dismissed.raw_suffix = ?1
              AND (
                  ?2 = 1
                  OR (
                      dismissed.agent_type = ?3
                      AND (
                          dismissed.cl_name = ?4
                          OR dismissed.cl_name = 'unknown'
                          OR ?4 IS NULL
                      )
                  )
              )
            LIMIT 1
            "#,
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params![
            record.timestamp.as_str(),
            terminal_or_inert as i64,
            dismissed_agent_type,
            summary.cl_name.as_deref(),
        ])
        .map_err(|e| e.to_string())?;
    Ok(rows.next().map_err(|e| e.to_string())?.is_some())
}

struct PendingRow {
    artifact_dir: String,
    row_projects_root: String,
    record_json: String,
    stored: MarkerSignatures,
}

fn active_where(
    include_hidden: bool,
    project_filter: Option<&BTreeSet<String>>,
) -> String {
    let where_sql = if include_hidden {
        format!(
            "WHERE has_done_marker = 0
         OR workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
         ORDER BY {}, timestamp DESC",
            active_priority_sql()
        )
    } else {
        format!(
            "WHERE hidden = 0 AND (
            has_done_marker = 0
            OR workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
         )
         AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
         ORDER BY {}, timestamp DESC",
            active_priority_sql()
        )
    };
    add_project_filter_to_where(where_sql, project_filter)
}

fn active_priority_sql() -> &'static str {
    "(has_running_marker = 1
       OR has_waiting_marker = 1
       OR pending_question_sig IS NOT NULL
       OR (
           has_workflow_state = 1
           AND workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
       )) DESC"
}

fn completed_where(
    include_hidden: bool,
    project_filter: Option<&BTreeSet<String>>,
) -> String {
    let where_sql = if include_hidden {
        "WHERE has_done_marker = 1
         OR workflow_status IN ('completed', 'failed', 'cancelled', 'noop')
         ORDER BY COALESCE(finished_at, 0) DESC, timestamp DESC"
            .to_string()
    } else {
        format!(
            "WHERE hidden = 0
         AND (
             has_done_marker = 1
             OR workflow_status IN ('completed', 'failed', 'cancelled', 'noop')
         )
         AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
         ORDER BY COALESCE(finished_at, 0) DESC, timestamp DESC"
        )
    };
    add_project_filter_to_where(where_sql, project_filter)
}

fn visible_where(
    include_hidden: bool,
    project_filter: Option<&BTreeSet<String>>,
) -> String {
    let where_sql = if include_hidden {
        "ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC"
            .to_string()
    } else {
        format!(
            "WHERE hidden = 0
         AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
         ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC"
        )
    };
    add_project_filter_to_where(where_sql, project_filter)
}

fn add_project_filter_to_where(
    where_sql: String,
    project_filter: Option<&BTreeSet<String>>,
) -> String {
    let Some(projects) = project_filter else {
        return where_sql;
    };
    let condition = if projects.is_empty() {
        "0 = 1".to_string()
    } else {
        let names = projects
            .iter()
            .map(|name| format!("'{}'", name.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        format!("project_name IN ({names})")
    };
    let upper = where_sql.to_ascii_uppercase();
    let order_index = upper.find("ORDER BY");
    let (prefix, order_by) = match order_index {
        Some(index) => (&where_sql[..index], &where_sql[index..]),
        None => (where_sql.as_str(), ""),
    };
    let trimmed_prefix = prefix.trim_end();
    if trimmed_prefix.is_empty() {
        return format!("WHERE {condition} {order_by}");
    }
    let leading_trimmed = trimmed_prefix.trim_start();
    if leading_trimmed.starts_with("WHERE") {
        let existing = leading_trimmed.trim_start_matches("WHERE").trim();
        format!("WHERE ({existing}) AND {condition} {order_by}")
    } else {
        format!("{trimmed_prefix} WHERE {condition} {order_by}")
    }
}

const DISMISSED_NORMAL_VISIBILITY_FILTER: &str = r#"NOT EXISTS (
             SELECT 1 FROM dismissed_agents dismissed
             WHERE dismissed.raw_suffix = agent_artifacts.timestamp
               AND (
                   (
                       agent_artifacts.has_done_marker = 1
                       OR agent_artifacts.workflow_status IN (
                           'completed', 'failed', 'cancelled', 'noop'
                       )
                       OR (
                           agent_artifacts.has_running_marker = 0
                           AND agent_artifacts.has_waiting_marker = 0
                           AND agent_artifacts.has_workflow_state = 0
                           AND agent_artifacts.has_done_marker = 0
                       )
                   )
                   OR (
                       dismissed.agent_type =
                           CASE agent_artifacts.agent_type
                               WHEN 'workflow' THEN 'workflow'
                               ELSE 'run'
                           END
                       AND (
                           dismissed.cl_name = agent_artifacts.cl_name
                           OR dismissed.cl_name = 'unknown'
                           OR agent_artifacts.cl_name IS NULL
                       )
                   )
               )
         )"#;

#[derive(Default)]
struct RecordSummary {
    status: String,
    agent_type: String,
    cl_name: Option<String>,
    agent_name: Option<String>,
    workflow_name: Option<String>,
    agent_clan: Option<String>,
    agent_family: Option<String>,
    model: Option<String>,
    llm_provider: Option<String>,
    started_at: Option<String>,
    finished_at: Option<f64>,
    workflow_status: Option<String>,
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
                "agent".to_string()
            },
            cl_name: done
                .and_then(|d| d.cl_name.clone())
                .or_else(|| running.and_then(|r| r.cl_name.clone()))
                .or_else(|| workflow_state.and_then(|w| w.cl_name.clone()))
                .or_else(|| meta.and_then(|m| m.cl_name.clone())),
            agent_name: meta
                .and_then(|m| m.name.clone())
                .or_else(|| done.and_then(|d| d.name.clone()))
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            workflow_name: meta
                .and_then(|m| m.workflow_name.clone())
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            agent_clan: meta.and_then(|m| m.agent_clan.clone()),
            agent_family: meta.and_then(|m| m.agent_family.clone()),
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
            hidden: meta.map(|m| m.hidden).unwrap_or(false)
                || done.map(|d| d.hidden).unwrap_or(false)
                || workflow_state.map(|w| w.hidden).unwrap_or(false),
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

#[derive(Default, PartialEq, Eq)]
struct MarkerSignatures {
    agent_meta: Option<String>,
    done: Option<String>,
    running: Option<String>,
    waiting: Option<String>,
    pending_question: Option<String>,
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
            pending_question: marker_signature(
                &dir.join("pending_question.json"),
            ),
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
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
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
    fn rebuild_replaces_corrupt_existing_index() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521143000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "active", "pid": 123}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        fs::write(&index, b"this is not a sqlite database").unwrap();
        fs::write(sqlite_sidecar_path(&index, "-wal"), b"stale wal").unwrap();
        fs::write(sqlite_sidecar_path(&index, "-shm"), b"stale shm").unwrap();

        let update = rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 1);

        let conn = Connection::open(&index).unwrap();
        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
        let quarantined: Vec<PathBuf> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with("agent_artifact_index.sqlite.corrupt-")
                    && !name.ends_with("-wal")
                    && !name.ends_with("-shm")
            })
            .collect();
        assert_eq!(quarantined.len(), 1);

        let indexed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(indexed.records.len(), 1);
        assert_eq!(indexed.records[0].timestamp, "20260521143000");
    }

    #[test]
    fn replace_unusable_index_file_renames_sidecars() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        fs::write(&index, b"this is not a sqlite database").unwrap();
        fs::write(sqlite_sidecar_path(&index, "-wal"), b"stale wal").unwrap();
        fs::write(sqlite_sidecar_path(&index, "-shm"), b"stale shm").unwrap();

        replace_unusable_index_file(&index).unwrap();

        let quarantined: Vec<PathBuf> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with("agent_artifact_index.sqlite.corrupt-")
                    && !name.ends_with("-wal")
                    && !name.ends_with("-shm")
            })
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(
            fs::read(&quarantined[0]).unwrap(),
            b"this is not a sqlite database"
        );
        assert_eq!(
            fs::read(sqlite_sidecar_path(&quarantined[0], "-wal")).unwrap(),
            b"stale wal"
        );
        assert_eq!(
            fs::read(sqlite_sidecar_path(&quarantined[0], "-shm")).unwrap(),
            b"stale shm"
        );
        assert!(!index.exists());
        assert!(!sqlite_sidecar_path(&index, "-wal").exists());
        assert!(!sqlite_sidecar_path(&index, "-shm").exists());
    }

    #[test]
    fn query_keeps_corrupt_existing_index_strict() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        fs::write(&index, b"this is not a sqlite database").unwrap();

        let err = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap_err();

        assert!(is_sqlite_index_corruption_error(&err), "{err}");
        assert_eq!(fs::read(&index).unwrap(), b"this is not a sqlite database");
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
    fn bounded_artifact_index_delete_skips_locked_database() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260504121212");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "active", "pid": 123}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        upsert_agent_artifact_index_row(
            &index,
            &projects,
            &artifact_dir,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let blocker = Connection::open(&index).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let result = delete_agent_artifact_index_row_with_busy_timeout(
            &index,
            &artifact_dir,
            Duration::from_millis(10),
        );
        blocker.execute_batch("ROLLBACK").unwrap();

        assert!(result.is_err());
        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
    }

    #[test]
    fn related_artifact_dirs_follow_retry_and_parent_lineage() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let root = artifact(&projects, "20260504120000");
        let followup = artifact(&projects, "20260504120500");
        let retry = artifact(&projects, "20260504121000");
        let retry2 = artifact(&projects, "20260504121500");
        let unrelated = artifact(&projects, "20260504122000");
        write_json(
            &root.join("agent_meta.json"),
            json!({
                "name": "root",
                "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
                "retried_as_timestamp": retry.file_name().unwrap().to_string_lossy(),
            }),
        );
        write_json(
            &followup.join("agent_meta.json"),
            json!({
                "name": "followup",
                "parent_timestamp": root.file_name().unwrap().to_string_lossy(),
            }),
        );
        write_json(
            &retry.join("agent_meta.json"),
            json!({
                "name": "retry",
                "retry_of_timestamp": root.file_name().unwrap().to_string_lossy(),
                "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
                "retried_as_timestamp": retry2.file_name().unwrap().to_string_lossy(),
            }),
        );
        write_json(
            &retry2.join("agent_meta.json"),
            json!({
                "name": "retry2",
                "retry_of_timestamp": retry.file_name().unwrap().to_string_lossy(),
                "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
            }),
        );
        write_json(
            &unrelated.join("agent_meta.json"),
            json!({"name": "unrelated", "parent_timestamp": "other-root"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let root_related =
            query_related_agent_artifact_dirs(&index, &root, &[]).unwrap();
        assert_eq!(
            timestamps_from_artifact_dirs(&root_related),
            vec![
                "20260504120000",
                "20260504120500",
                "20260504121000",
                "20260504121500",
            ]
        );

        let retry_related =
            query_related_agent_artifact_dirs(&index, &retry, &[]).unwrap();
        assert_eq!(
            timestamps_from_artifact_dirs(&retry_related),
            vec![
                "20260504121000",
                "20260504120000",
                "20260504120500",
                "20260504121500",
            ]
        );
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

    fn timestamps_from_artifact_dirs(paths: &[String]) -> Vec<&str> {
        paths
            .iter()
            .map(|path| Path::new(path).file_name().unwrap().to_str().unwrap())
            .collect()
    }

    #[test]
    fn tier1_active_query_is_bounded_to_newest_incomplete_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for index in 0..5 {
            let artifact_dir =
                artifact(&projects, &format!("2026051312000{index}"));
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": format!("stale-{index}")}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: Some(2),
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let timestamps: Vec<&str> = snapshot
            .records
            .iter()
            .map(|record| record.timestamp.as_str())
            .collect();
        assert_eq!(timestamps, vec!["20260513120003", "20260513120004"]);
    }

    #[test]
    fn recent_completed_limit_does_not_bound_active_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for index in 0..3 {
            let artifact_dir =
                artifact(&projects, &format!("2026051313000{index}"));
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": format!("active-{index}")}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(1),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(snapshot.records.len(), 3);
    }

    #[test]
    fn active_limit_prioritizes_waiting_rows_over_newer_stale_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for index in 0..5 {
            let artifact_dir =
                artifact(&projects, &format!("2026051315000{index}"));
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": format!("stale-{index}")}),
            );
        }
        for timestamp in ["20260513140000", "20260513140001"] {
            let artifact_dir = artifact(&projects, timestamp);
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": format!("waiting-{timestamp}")}),
            );
            write_json(
                &artifact_dir.join("waiting.json"),
                json!({"waiting_for": ["review"]}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: Some(2),
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let timestamps: Vec<&str> = snapshot
            .records
            .iter()
            .map(|record| record.timestamp.as_str())
            .collect();
        assert_eq!(timestamps, vec!["20260513140000", "20260513140001"]);
    }

    #[test]
    fn index_query_wire_round_trips_active_limit() {
        let query: AgentArtifactIndexQueryWire =
            serde_json::from_value(json!({
                "include_active": true,
                "include_recent_completed": false,
                "include_full_history": false,
                "active_limit": 7,
                "recent_completed_limit": 11,
                "include_hidden": true,
            }))
            .unwrap();

        assert_eq!(query.active_limit, Some(7));
        let payload = serde_json::to_value(&query).unwrap();
        assert_eq!(payload["active_limit"], json!(7));

        let legacy: AgentArtifactIndexQueryWire =
            serde_json::from_value(json!({
                "include_active": true,
                "include_recent_completed": true,
                "include_full_history": false,
                "recent_completed_limit": 5,
                "include_hidden": false,
            }))
            .unwrap();
        assert_eq!(legacy.active_limit, None);
    }

    #[test]
    fn active_query_excludes_dismissed_identity_after_rebuild() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260514120000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "dismissed-active", "pid": 123}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        replace_agent_artifact_index_dismissed_agents(
            &index,
            &[AgentCleanupIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "unknown".to_string(),
                raw_suffix: Some("20260514120000".to_string()),
            }],
        )
        .unwrap();
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let visible = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(visible.records.is_empty());
    }

    #[test]
    fn stale_dismissed_suffixes_do_not_consume_active_limit() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let mut dismissed = Vec::new();
        for index in 0..1_000 {
            let timestamp = format!("20260515{index:06}");
            let artifact_dir = artifact(&projects, &timestamp);
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({
                    "name": format!("stale-dismissed-{index}"),
                    "cl_name": "current_shape",
                }),
            );
            dismissed.push(AgentCleanupIdentityWire {
                agent_type: "workflow".to_string(),
                cl_name: "historical_shape".to_string(),
                raw_suffix: Some(timestamp),
            });
        }
        for timestamp in ["20260514000001", "20260514000002"] {
            write_json(
                &artifact(&projects, timestamp).join("agent_meta.json"),
                json!({"name": format!("visible-{timestamp}")}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        replace_agent_artifact_index_dismissed_agents(&index, &dismissed)
            .unwrap();
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let visible = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: Some(5),
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let timestamps: Vec<&str> = visible
            .records
            .iter()
            .map(|record| record.timestamp.as_str())
            .collect();
        assert_eq!(timestamps, vec!["20260514000001", "20260514000002"]);
    }

    #[test]
    fn hidden_inclusive_full_history_can_inspect_dismissed_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260514123000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "dismissed-active", "pid": 123}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        replace_agent_artifact_index_dismissed_agents(
            &index,
            &[AgentCleanupIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "unknown".to_string(),
                raw_suffix: Some("20260514123000".to_string()),
            }],
        )
        .unwrap();

        let visible = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: false,
                include_full_history: true,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(visible.records.is_empty());

        let all = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: false,
                include_full_history: true,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: true,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(all.records.len(), 1);
        assert_eq!(all.records[0].timestamp, "20260514123000");
    }

    #[test]
    fn recent_completed_rows_remain_visible_when_not_dismissed() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260514130000");
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1777900100.0,
                "name": "done-visible",
                "cl_name": "cl_visible"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].timestamp, "20260514130000");
    }

    #[test]
    fn terminal_workflow_state_rows_are_recent_completed_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260514140000");
        write_json(
            &artifact_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "wf",
                "status": "failed",
                "cl_name": "cl_failed",
                "start_time": "2026-05-14T14:00:00Z",
                "steps": []
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].timestamp, "20260514140000");
    }

    #[test]
    fn anonymous_appears_as_agent_workflow_is_not_hidden() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521100533");
        write_json(
            &artifact_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "tmp_260521_104058",
                "status": "completed",
                "appears_as_agent": true,
                "is_anonymous": true,
                "hidden": false,
                "start_time": "2026-05-21T10:05:33Z",
                "steps": []
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "tmp_260521_104058",
                "cl_name": "cl_anon",
                "hidden": false
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].timestamp, "20260521100533");
    }

    #[test]
    fn explicit_workflow_state_hidden_is_still_filtered() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521100600");
        write_json(
            &artifact_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "tmp_260521_104100",
                "status": "completed",
                "appears_as_agent": true,
                "is_anonymous": true,
                "hidden": true,
                "start_time": "2026-05-21T10:06:00Z",
                "steps": []
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "tmp_260521_104100",
                "cl_name": "cl_hidden"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn migration_recomputes_hidden_for_v1_indexes() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        // Visible-but-anonymous: would have been wrongly hidden by v1.
        let anon_dir = artifact(&projects, "20260521110000");
        write_json(
            &anon_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "tmp_anon",
                "status": "completed",
                "appears_as_agent": true,
                "is_anonymous": true,
                "hidden": false,
                "steps": []
            }),
        );
        write_json(
            &anon_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "tmp_anon",
                "cl_name": "cl_anon"
            }),
        );
        // Truly hidden: workflow_state.hidden = true. Must stay hidden.
        let hidden_dir = artifact(&projects, "20260521110001");
        write_json(
            &hidden_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "wf_hidden",
                "status": "completed",
                "hidden": true,
                "steps": []
            }),
        );
        write_json(
            &hidden_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "wf_hidden",
                "cl_name": "cl_hidden"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        // Force the index back to the v1 state: schema_version=1 in meta,
        // and the anonymous row's hidden bit flipped to 1 (matching what
        // the buggy v1 projection would have written).
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) \
                 VALUES ('schema_version', '1')",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE agent_artifacts SET hidden = 1 WHERE artifact_dir = ?1",
                [anon_dir.to_string_lossy().as_ref()],
            )
            .unwrap();
        }

        // Re-opening must run the migration and clear the spurious hidden
        // bit on the anonymous row, while leaving the explicit-hidden row
        // untouched.
        let _conn = open_index(&index).unwrap();
        let conn = Connection::open(&index).unwrap();
        let anon_hidden: i64 = conn
            .query_row(
                "SELECT hidden FROM agent_artifacts WHERE artifact_dir = ?1",
                [anon_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(anon_hidden, 0);
        let hidden_hidden: i64 = conn
            .query_row(
                "SELECT hidden FROM agent_artifacts WHERE artifact_dir = ?1",
                [hidden_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hidden_hidden, 1);
        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
    }

    #[test]
    fn terminalize_stale_active_rows_hides_abandoned_record() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521160000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "abandoned", "cl_name": "cl_abandoned"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let update = terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            0,
            None,
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 1);

        let active = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(active.records.is_empty());

        let recent = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(recent.records.is_empty());

        let full_history = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: false,
                include_full_history: true,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(full_history.records.is_empty());

        let hidden_completed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: true,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(hidden_completed.records.len(), 1);
        assert!(hidden_completed.records[0].has_done_marker);
        assert_eq!(
            hidden_completed.records[0]
                .done
                .as_ref()
                .and_then(|done| done.outcome.as_deref()),
            Some("abandoned")
        );
        assert_eq!(
            hidden_completed.records[0]
                .done
                .as_ref()
                .and_then(|done| done.cl_name.as_deref()),
            Some("cl_abandoned")
        );
        assert!(hidden_completed.records[0]
            .done
            .as_ref()
            .is_some_and(|done| done.hidden));
    }

    #[test]
    fn terminalize_repairs_visible_abandoned_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521160030");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "repair-abandoned", "cl_name": "cl_repaired"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            0,
            None,
        )
        .unwrap();

        {
            let conn = Connection::open(&index).unwrap();
            let record_json: String = conn
                .query_row(
                    "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
                    [artifact_dir.to_string_lossy().as_ref()],
                    |row| row.get(0),
                )
                .unwrap();
            let mut record: AgentArtifactRecordWire =
                serde_json::from_str(&record_json).unwrap();
            let done = record.done.as_mut().unwrap();
            done.hidden = false;
            done.cl_name = Some("unknown".to_string());
            let corrupted = serde_json::to_string(&record).unwrap();
            conn.execute(
                "UPDATE agent_artifacts \
                 SET hidden = 0, cl_name = 'unknown', record_json = ?1 \
                 WHERE artifact_dir = ?2",
                params![corrupted, artifact_dir.to_string_lossy().as_ref()],
            )
            .unwrap();
        }

        let update = terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            0,
            None,
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 1);

        let visible_recent = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(visible_recent.records.is_empty());

        let hidden_recent = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: true,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(hidden_recent.records.len(), 1);
        let done = hidden_recent.records[0].done.as_ref().unwrap();
        assert_eq!(done.cl_name.as_deref(), Some("cl_repaired"));
        assert!(done.hidden);

        let conn = Connection::open(&index).unwrap();
        let (hidden, cl_name): (i64, String) = conn
            .query_row(
                "SELECT hidden, cl_name FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(hidden, 1);
        assert_eq!(cl_name, "cl_repaired");
    }

    #[test]
    fn terminalize_stale_active_rows_skips_fresh_missing_marker_race() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521160100");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "fresh"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let update = terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            24 * 60 * 60,
            None,
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 0);
        assert_eq!(update.rows_skipped, 1);

        let active = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(active.records.len(), 1);
    }

    #[test]
    fn terminalize_stale_active_rows_revalidates_new_running_marker() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521160200");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "became-running"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        write_json(
            &artifact_dir.join("running.json"),
            json!({"pid": 1234, "cl_name": "cl"}),
        );

        let update = terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            0,
            None,
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 0);
        assert_eq!(update.rows_skipped, 1);

        let active = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(active.records.len(), 1);
        assert!(active.records[0].running.is_some());
    }

    #[test]
    fn terminalize_stale_active_rows_skips_workspace_claim() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521160300");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "claimed",
                "workspace_num": 2,
            }),
        );
        fs::write(
            projects.join("proj").join("proj.sase"),
            "NAME: proj\nRUNNING:\n  #2 | 1234 | ace-run | cl | 20260521160300\n",
        )
        .unwrap();

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let update = terminalize_stale_active_agent_artifact_index_rows(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
            0,
            None,
        )
        .unwrap();
        assert_eq!(update.rows_indexed, 0);
        assert_eq!(update.rows_skipped, 1);

        let active = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(active.records.len(), 1);
    }

    fn default_query() -> AgentArtifactIndexQueryWire {
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(200),
            include_hidden: false,
        }
    }

    #[test]
    fn query_self_heals_appended_plan_submitted_at() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521150000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active",
                "run_started_at": "2026-05-21T15:00:00Z",
                "plan_submitted_at": [],
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let initial = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(initial.records.len(), 1);
        assert!(initial.records[0]
            .agent_meta
            .as_ref()
            .unwrap()
            .plan_submitted_at
            .is_empty());

        // Mid-run mutation: state-transition path writes a new plan
        // timestamp directly to agent_meta.json without calling upsert.
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active",
                "run_started_at": "2026-05-21T15:00:00Z",
                "plan_submitted_at": ["2026-05-21T15:05:00Z"],
            }),
        );

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(refreshed.records.len(), 1);
        let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
        assert_eq!(meta.plan_submitted_at, vec!["2026-05-21T15:05:00Z"]);

        // And the stored row was refreshed so a follow-up direct read of
        // the record_json reflects the new data.
        let stored_json: String = Connection::open(&index)
            .unwrap()
            .query_row(
                "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stored_json.contains("2026-05-21T15:05:00Z"));
    }

    #[test]
    fn query_self_heals_appended_feedback_submitted_at() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521151500");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active",
                "run_started_at": "2026-05-21T15:15:00Z",
                "feedback_submitted_at": [],
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active",
                "run_started_at": "2026-05-21T15:15:00Z",
                "feedback_submitted_at": ["2026-05-21T15:20:00Z"],
            }),
        );

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
        assert_eq!(meta.feedback_submitted_at, vec!["2026-05-21T15:20:00Z"]);
    }

    #[test]
    fn query_self_heals_newly_added_run_started_at() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521152000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "starting"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let initial = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(initial.records[0]
            .agent_meta
            .as_ref()
            .unwrap()
            .run_started_at
            .is_none());

        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "starting",
                "run_started_at": "2026-05-21T15:21:00Z",
            }),
        );

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
        assert_eq!(
            meta.run_started_at.as_deref(),
            Some("2026-05-21T15:21:00Z")
        );
    }

    #[test]
    fn query_self_heals_running_to_done_transition() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521153000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "active", "run_started_at": "2026-05-21T15:30:00Z"}),
        );
        write_json(
            &artifact_dir.join("running.json"),
            json!({"pid": 1234, "cl_name": "cl"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        // Simulate done: remove running.json and write done.json without
        // calling upsert.
        fs::remove_file(artifact_dir.join("running.json")).unwrap();
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "active",
                "cl_name": "cl",
            }),
        );

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(refreshed.records.len(), 1);
        assert!(refreshed.records[0].has_done_marker);
        assert!(refreshed.records[0].running.is_none());
    }

    #[test]
    fn query_self_heals_hidden_to_visible_before_visible_filter() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521153100");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "hidden-then-visible",
                "run_started_at": "2026-05-21T15:31:00Z",
                "hidden": true,
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let hidden = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(hidden.records.is_empty());

        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "hidden-then-visible",
                "run_started_at": "2026-05-21T15:31:00Z",
                "hidden": false,
            }),
        );

        let visible = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(visible.records.len(), 1);
        assert_eq!(visible.records[0].timestamp, "20260521153100");
    }

    #[test]
    fn query_self_heals_waiting_deletion_to_running() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521153200");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "wait-then-run",
                "run_started_at": "2026-05-21T15:32:00Z",
            }),
        );
        write_json(
            &artifact_dir.join("waiting.json"),
            json!({"waiting_for": ["parent"]}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        fs::remove_file(artifact_dir.join("waiting.json")).unwrap();

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(refreshed.records.len(), 1);
        assert!(refreshed.records[0].waiting.is_none());

        let status: String = Connection::open(&index)
            .unwrap()
            .query_row(
                "SELECT status FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn query_self_heals_pending_question_creation_and_deletion() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521153300");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "question-agent",
                "run_started_at": "2026-05-21T15:33:00Z",
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        write_json(
            &artifact_dir.join("pending_question.json"),
            json!({
                "session_id": "question-session",
                "request_path": "/tmp/question_request.json",
            }),
        );
        let with_question = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(
            with_question.records[0]
                .pending_question
                .as_ref()
                .and_then(|marker| marker.session_id.as_deref()),
            Some("question-session")
        );

        fs::remove_file(artifact_dir.join("pending_question.json")).unwrap();
        let without_question = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: false,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(without_question.records[0].pending_question.is_none());
    }

    #[test]
    fn query_self_heals_done_creation_before_completed_filter() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521153400");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "active-then-done",
                "run_started_at": "2026-05-21T15:34:00Z",
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "active-then-done",
                "cl_name": "cl_completed",
            }),
        );

        let completed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(completed.records.len(), 1);
        assert_eq!(completed.records[0].timestamp, "20260521153400");
        assert!(completed.records[0].has_done_marker);
    }

    #[test]
    fn query_skips_rescan_when_signatures_match() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260521154000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "active", "run_started_at": "2026-05-21T15:40:00Z"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        // Inject a sentinel into the stored record_json without touching the
        // on-disk marker files. Signatures still match what is on disk, so a
        // correct query path must skip revalidation and return the sentinel
        // unchanged. If rescan ran unconditionally, the sentinel would be
        // overwritten by the real on-disk value.
        let sentinel_name = "sentinel-skip-rescan-marker";
        {
            let conn = Connection::open(&index).unwrap();
            let mut record_json: String = conn
                .query_row(
                    "SELECT record_json FROM agent_artifacts \
                     WHERE artifact_dir = ?1",
                    [artifact_dir.to_string_lossy().as_ref()],
                    |row| row.get(0),
                )
                .unwrap();
            let mut record: AgentArtifactRecordWire =
                serde_json::from_str(&record_json).unwrap();
            if let Some(meta) = record.agent_meta.as_mut() {
                meta.name = Some(sentinel_name.to_string());
            }
            record_json = serde_json::to_string(&record).unwrap();
            conn.execute(
                "UPDATE agent_artifacts SET record_json = ?1 \
                 WHERE artifact_dir = ?2",
                params![record_json, artifact_dir.to_string_lossy().as_ref(),],
            )
            .unwrap();
        }

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            default_query(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        let returned_name = snapshot.records[0]
            .agent_meta
            .as_ref()
            .and_then(|m| m.name.as_deref());
        assert_eq!(returned_name, Some(sentinel_name));
    }
}
