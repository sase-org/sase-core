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

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use super::scanner::{scan_agent_artifact_dir, scan_agent_artifacts};
use super::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire,
    AGENT_SCAN_WIRE_SCHEMA_VERSION,
};

pub const AGENT_ARTIFACT_INDEX_SCHEMA_VERSION: u32 = 1;

const MARKER_FILES: &[&str] = &[
    "agent_meta.json",
    "done.json",
    "running.json",
    "waiting.json",
    "workflow_state.json",
    "plan_path.json",
];

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
}

impl Default for AgentArtifactIndexQueryWire {
    fn default() -> Self {
        Self {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
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

    if query.include_active {
        select_records(
            &conn,
            active_where(query.include_hidden),
            None,
            &mut stats,
            &mut by_dir,
        )?;
    }

    if query.include_recent_completed {
        select_records(
            &conn,
            completed_where(query.include_hidden),
            query.recent_completed_limit,
            &mut stats,
            &mut by_dir,
        )?;
    }

    if query.include_full_history {
        select_records(
            &conn,
            visible_where(query.include_hidden),
            None,
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
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_active
            ON agent_artifacts(hidden, has_done_marker, workflow_status, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_recent_completed
            ON agent_artifacts(hidden, has_done_marker, finished_at, timestamp);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_cl_name
            ON agent_artifacts(cl_name);
        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_project_workflow
            ON agent_artifacts(project_name, workflow_dir_name, timestamp);
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
            has_workflow_state, workflow_status, hidden, parent_timestamp,
            step_index, step_name, retry_of_timestamp, retried_as_timestamp,
            retry_chain_root_timestamp, retry_attempt, agent_meta_sig, done_sig,
            running_sig, waiting_sig, workflow_state_sig, plan_path_sig,
            prompt_steps_sig, record_json, indexed_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, CURRENT_TIMESTAMP
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
            Ok(record) => {
                by_dir.insert(artifact_dir, record);
            }
            Err(_) => {
                stats.json_decode_errors += 1;
            }
        }
    }
    Ok(())
}

fn active_where(include_hidden: bool) -> &'static str {
    if include_hidden {
        "WHERE has_done_marker = 0
         OR workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
         ORDER BY timestamp DESC"
    } else {
        "WHERE hidden = 0 AND (
            has_done_marker = 0
            OR workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
         )
         ORDER BY timestamp DESC"
    }
}

fn completed_where(include_hidden: bool) -> &'static str {
    if include_hidden {
        "WHERE has_done_marker = 1
         ORDER BY COALESCE(finished_at, 0) DESC, timestamp DESC"
    } else {
        "WHERE hidden = 0 AND has_done_marker = 1
         ORDER BY COALESCE(finished_at, 0) DESC, timestamp DESC"
    }
}

fn visible_where(include_hidden: bool) -> &'static str {
    if include_hidden {
        "ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC"
    } else {
        "WHERE hidden = 0
         ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC"
    }
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
        } else if meta.and_then(|m| m.run_started_at.as_ref()).is_some() {
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
            hidden: meta.map(|m| m.hidden).unwrap_or(false)
                || done.map(|d| d.hidden).unwrap_or(false)
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
}
