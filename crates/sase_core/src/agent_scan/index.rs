//! SQLite materialized view for agent artifact summaries.
//!
//! The artifact tree remains the source of truth. This module stores one
//! row per artifact directory with denormalized query fields and the
//! scanner's canonical `AgentArtifactRecordWire` JSON payload so indexed
//! queries can return loader-equivalent records without walking every
//! historical timestamp directory.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{
    params, params_from_iter, Connection, OpenFlags, OptionalExtension,
};
use serde::{Deserialize, Serialize};

use crate::agent_clan_tribe::ClanTribeMemberWire;
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_launch::list_workspace_claims_from_content;
use crate::agent_runtime::{
    is_real_monitor_member_record, parse_runtime_timestamp,
};

use super::context::{
    clan_key_from_meta, represented_clan_keys, resolve_clan_context,
};
use super::scanner::{
    project_allowed_by_filter, project_filter_for_scan,
    scan_agent_artifact_dir, scan_agent_artifacts,
};
use super::wire::{
    AgentArtifactIndexWindowWire, AgentArtifactRecordShapeWire,
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire, AgentMetaWire,
    AgentOutputVariableHistoryQueryWire, AgentOutputVariableHistoryWire,
    AgentOutputVariableKeyGroupWire, AgentOutputVariableLimitWire,
    AgentOutputVariableOccurrenceWire, AgentOutputVariableValueGroupWire,
    DoneMarkerWire, OutputVariableValue, UsedXPromptWire,
    AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION,
    AGENT_SCAN_WIRE_SCHEMA_VERSION,
};

pub const AGENT_ARTIFACT_INDEX_SCHEMA_VERSION: u32 = 24;

/// Newest hidden terminal rows kept hot in the materialized SQLite view.
///
/// The artifact tree remains authoritative for older hidden terminal payloads;
/// rebuilding the index from source artifacts restores evicted history.
pub const DEFAULT_HIDDEN_TERMINAL_HOT_ROWS: u32 = 4096;

/// Schema version for indexed model-alias history queries.
pub const AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_alias_history_limit() -> u32 {
    10
}

fn default_alias_history_prompt_snippet_bytes() -> u32 {
    240
}

fn default_alias_history_freshness() -> AgentArtifactIndexFreshnessWire {
    AgentArtifactIndexFreshnessWire::Cached
}

/// Query knobs for bounded per-alias agent history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAliasHistoryQueryWire {
    /// Bare alias names to report on. Empty is an error.
    pub aliases: Vec<String>,
    /// Maximum runs returned per alias. Zero means unlimited.
    #[serde(default = "default_alias_history_limit")]
    pub limit_per_alias: u32,
    #[serde(default)]
    pub include_hidden: bool,
    /// Exact ProjectSpec keys. Empty means every project.
    #[serde(default)]
    pub projects: Vec<String>,
    /// Bounded read budget per returned run. Zero skips prompt reads.
    #[serde(default = "default_alias_history_prompt_snippet_bytes")]
    pub prompt_snippet_bytes: u32,
    /// Cached by default; `revalidate` refreshes matching artifact rows.
    #[serde(default = "default_alias_history_freshness")]
    pub freshness: AgentArtifactIndexFreshnessWire,
}

/// Effective limit and truncation metadata for one alias group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAliasHistoryLimitWire {
    pub limit: u32,
    pub total_count: u64,
    pub returned_count: u64,
    pub truncated: bool,
}

/// One indexed run that used a requested model alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasRunWire {
    pub artifact_dir: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub model_alias_origin: Option<String>,
    #[serde(default)]
    pub model_alias_trail: Vec<String>,
    pub alias_position: u32,
    pub status: String,
    #[serde(default)]
    pub workflow_status: Option<String>,
    pub has_done_marker: bool,
    pub hidden: bool,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<f64>,
    #[serde(default)]
    pub retry_attempt: Option<i64>,
    #[serde(default)]
    pub bead_id: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub prompt_snippet: Option<String>,
    #[serde(default)]
    pub used_xprompts: Vec<UsedXPromptWire>,
}

/// History group for one requested alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasHistoryGroupWire {
    pub alias: String,
    pub runs_limit: AgentAliasHistoryLimitWire,
    pub runs: Vec<AgentAliasRunWire>,
}

/// Bounded alias history returned by the artifact index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasHistoryWire {
    pub schema_version: u32,
    pub index_path: String,
    pub query: AgentAliasHistoryQueryWire,
    pub groups: Vec<AgentAliasHistoryGroupWire>,
}

const MARKER_FILES: &[&str] = &[
    "agent_meta.json",
    "done.json",
    "running.json",
    "waiting.json",
    "pending_question.json",
    "workflow_state.json",
    "plan_path.json",
    "xprompts.json",
];

const TERMINAL_WORKFLOW_STATUSES: &[&str] =
    &["completed", "failed", "cancelled", "noop"];
const MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS: usize = 128;
const MAX_RELATED_ARTIFACT_QUERY_ITERATIONS: usize = 32;
const ABANDONED_DONE_OUTCOME: &str = "abandoned";
const DEFAULT_INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Freshness policy for persistent artifact index queries.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactIndexFreshnessWire {
    #[default]
    Revalidate,
    Cached,
}

/// Scalar fields that can be tested before `record_json` is decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactCandidateFieldWire {
    Project,
    Cl,
    Model,
    Provider,
    Type,
}

/// Exact candidate filter compiled by Python from the agent-query AST.
///
/// This is intentionally not a user-facing query parser. Callers only send
/// boolean combinations of atoms whose parity against Python Agent evaluation
/// has been proven for indexed scalar columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentArtifactCandidateFilterWire {
    All {
        #[serde(default)]
        filters: Vec<AgentArtifactCandidateFilterWire>,
    },
    Any {
        #[serde(default)]
        filters: Vec<AgentArtifactCandidateFilterWire>,
    },
    Not {
        filter: Box<AgentArtifactCandidateFilterWire>,
    },
    Contains {
        field: AgentArtifactCandidateFieldWire,
        value: String,
    },
    Equals {
        field: AgentArtifactCandidateFieldWire,
        value: String,
    },
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
    pub active_limit: Option<u32>,
    #[serde(default)]
    pub recent_completed_limit: Option<u32>,
    #[serde(default)]
    pub include_hidden: bool,
    #[serde(default)]
    pub freshness: AgentArtifactIndexFreshnessWire,
    /// Restrict results to real monitor family members
    /// (`agent_meta.agent_family_role == "monitor"` and a non-empty
    /// `agent_meta.family_shell.id` on a `"monitor"`-kind shell).
    #[serde(default)]
    pub only_monitors: bool,
    #[serde(default)]
    pub record_shape: AgentArtifactRecordShapeWire,
    #[serde(default)]
    pub window_limit: Option<u32>,
    #[serde(default)]
    pub candidate_filter: Option<AgentArtifactCandidateFilterWire>,
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
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
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
    #[serde(default)]
    pub hidden_terminal_rows_retained: u64,
    #[serde(default)]
    pub hidden_terminal_rows_pruned: u64,
}

/// Lightweight status for the persistent artifact index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexStatusWire {
    pub schema_version: u32,
    pub index_path: String,
    pub agent_artifacts_rows: u64,
    pub dismissed_agents_rows: u64,
    pub agent_artifact_aliases_rows: u64,
    pub agent_output_variables_rows: u64,
    pub agent_artifact_model_aliases_rows: u64,
    #[serde(default)]
    pub hidden_terminal_retention_limit: u64,
    #[serde(default)]
    pub hidden_terminal_rows_retained: u64,
    #[serde(default)]
    pub hidden_terminal_rows_prunable: u64,
    /// Free pages left behind by deletes; never reclaimed without a VACUUM.
    #[serde(default)]
    pub freelist_pages: u64,
    #[serde(default)]
    pub freelist_bytes: u64,
    #[serde(default)]
    pub file_size_bytes: u64,
}

/// Outcome of one `VACUUM` compaction pass over the artifact index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexVacuumWire {
    pub index_path: String,
    pub freelist_pages_before: u64,
    pub freelist_pages_after: u64,
    pub file_size_bytes_before: u64,
    pub file_size_bytes_after: u64,
    pub bytes_reclaimed: u64,
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
    let retention = enforce_hidden_terminal_retention(
        &mut conn,
        DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
    )?;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        rows_indexed,
        rows_deleted: 0,
        rows_skipped: 0,
        hidden_terminal_rows_retained: retention.retained_rows,
        hidden_terminal_rows_pruned: retention.pruned_rows(),
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
            hidden_terminal_rows_retained: 0,
            hidden_terminal_rows_pruned: 0,
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
        hidden_terminal_rows_retained: 0,
        hidden_terminal_rows_pruned: 0,
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
    let _ = conn.execute(
        "DELETE FROM agent_output_variables WHERE artifact_dir = ?1",
        [artifact_dir.as_str()],
    );
    let _ = conn.execute(
        "DELETE FROM agent_artifact_model_aliases WHERE artifact_dir = ?1",
        [artifact_dir.as_str()],
    );
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
        hidden_terminal_rows_retained: 0,
        hidden_terminal_rows_pruned: 0,
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
    let retention = enforce_hidden_terminal_retention(
        &mut conn,
        DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
    )?;

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        rows_indexed,
        rows_deleted: 0,
        rows_skipped,
        hidden_terminal_rows_retained: retention.retained_rows,
        hidden_terminal_rows_pruned: retention.pruned_rows(),
    })
}

/// Prune old hidden terminal rows from the hot SQLite materialized view.
///
/// This never mutates artifact directories. Evicted payloads remain recoverable
/// by rebuilding the index from the canonical artifact tree.
pub fn prune_hidden_terminal_agent_artifact_index_rows(
    index_path: &Path,
    hot_rows: Option<u32>,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut conn = open_index(index_path)?;
    let retention = enforce_hidden_terminal_retention(
        &mut conn,
        hot_rows.unwrap_or(DEFAULT_HIDDEN_TERMINAL_HOT_ROWS),
    )?;
    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed: 0,
        rows_deleted: retention.pruned_rows(),
        rows_skipped: 0,
        hidden_terminal_rows_retained: retention.retained_rows,
        hidden_terminal_rows_pruned: retention.pruned_rows(),
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HiddenTerminalRetentionPlan {
    retained_rows: u64,
    prunable_rows: u64,
    pruned_dirs: Vec<String>,
}

impl HiddenTerminalRetentionPlan {
    fn pruned_rows(&self) -> u64 {
        self.pruned_dirs.len() as u64
    }
}

fn enforce_hidden_terminal_retention(
    conn: &mut Connection,
    hot_rows: u32,
) -> Result<HiddenTerminalRetentionPlan, String> {
    let plan = plan_hidden_terminal_retention(conn, hot_rows)?;
    if plan.pruned_dirs.is_empty() {
        return Ok(plan);
    }
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    delete_agent_artifact_projection_rows(&tx, &plan.pruned_dirs)?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(HiddenTerminalRetentionPlan {
        retained_rows: plan.retained_rows,
        prunable_rows: 0,
        pruned_dirs: plan.pruned_dirs,
    })
}

fn plan_hidden_terminal_retention(
    conn: &Connection,
    hot_rows: u32,
) -> Result<HiddenTerminalRetentionPlan, String> {
    let rows = select_hidden_terminal_rows(conn)?;
    let referenced = select_lineage_reference_keys(conn)?;
    let mut plan = HiddenTerminalRetentionPlan::default();
    let hot_rows = hot_rows as usize;

    for (index, row) in rows.into_iter().enumerate() {
        if index < hot_rows || row.is_context_anchor(&referenced) {
            plan.retained_rows += 1;
        } else {
            plan.prunable_rows += 1;
            plan.pruned_dirs.push(row.artifact_dir);
        }
    }
    Ok(plan)
}

#[derive(Debug, Clone)]
struct HiddenTerminalRow {
    artifact_dir: String,
    project_name: String,
    workflow_dir_name: String,
    timestamp: String,
    parent_timestamp: Option<String>,
    retry_of_timestamp: Option<String>,
    retried_as_timestamp: Option<String>,
    retry_chain_root_timestamp: Option<String>,
    clan_tribe: Option<String>,
    clan_summary: Option<String>,
}

impl HiddenTerminalRow {
    fn is_context_anchor(
        &self,
        referenced: &BTreeSet<LineageReferenceKey>,
    ) -> bool {
        self.has_lineage_pointer()
            || self.has_clan_context()
            || referenced.contains(&LineageReferenceKey {
                project_name: self.project_name.clone(),
                workflow_dir_name: self.workflow_dir_name.clone(),
                timestamp: self.timestamp.clone(),
            })
    }

    fn has_lineage_pointer(&self) -> bool {
        [
            self.parent_timestamp.as_deref(),
            self.retry_of_timestamp.as_deref(),
            self.retried_as_timestamp.as_deref(),
            self.retry_chain_root_timestamp.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.trim().is_empty())
    }

    fn has_clan_context(&self) -> bool {
        [self.clan_tribe.as_deref(), self.clan_summary.as_deref()]
            .into_iter()
            .flatten()
            .any(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LineageReferenceKey {
    project_name: String,
    workflow_dir_name: String,
    timestamp: String,
}

fn select_hidden_terminal_rows(
    conn: &Connection,
) -> Result<Vec<HiddenTerminalRow>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT artifact_dir, project_name, workflow_dir_name, timestamp, \
                    parent_timestamp, retry_of_timestamp, \
                    retried_as_timestamp, retry_chain_root_timestamp, \
                    clan_tribe, clan_summary \
             FROM agent_artifacts \
             WHERE hidden = 1 \
               AND has_done_marker = 1 \
               AND (workflow_status IS NULL \
                    OR workflow_status IN ('completed', 'failed', 'cancelled', 'noop')) \
             ORDER BY timestamp DESC, artifact_dir DESC",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(HiddenTerminalRow {
            artifact_dir: row.get(0).map_err(|e| e.to_string())?,
            project_name: row.get(1).map_err(|e| e.to_string())?,
            workflow_dir_name: row.get(2).map_err(|e| e.to_string())?,
            timestamp: row.get(3).map_err(|e| e.to_string())?,
            parent_timestamp: row.get(4).map_err(|e| e.to_string())?,
            retry_of_timestamp: row.get(5).map_err(|e| e.to_string())?,
            retried_as_timestamp: row.get(6).map_err(|e| e.to_string())?,
            retry_chain_root_timestamp: row
                .get(7)
                .map_err(|e| e.to_string())?,
            clan_tribe: row.get(8).map_err(|e| e.to_string())?,
            clan_summary: row.get(9).map_err(|e| e.to_string())?,
        });
    }
    Ok(result)
}

fn select_lineage_reference_keys(
    conn: &Connection,
) -> Result<BTreeSet<LineageReferenceKey>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT project_name, workflow_dir_name, parent_timestamp, \
                    retry_of_timestamp, retried_as_timestamp, \
                    retry_chain_root_timestamp \
             FROM agent_artifacts",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut result = BTreeSet::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let project_name: String = row.get(0).map_err(|e| e.to_string())?;
        let workflow_dir_name: String =
            row.get(1).map_err(|e| e.to_string())?;
        for column in 2..=5 {
            let timestamp: Option<String> =
                row.get(column).map_err(|e| e.to_string())?;
            let Some(timestamp) = timestamp else {
                continue;
            };
            if timestamp.trim().is_empty() {
                continue;
            }
            result.insert(LineageReferenceKey {
                project_name: project_name.clone(),
                workflow_dir_name: workflow_dir_name.clone(),
                timestamp,
            });
        }
    }
    Ok(result)
}

fn delete_agent_artifact_projection_rows(
    conn: &Connection,
    artifact_dirs: &[String],
) -> Result<(), String> {
    const DELETE_BATCH_SIZE: usize = 500;
    for chunk in artifact_dirs.chunks(DELETE_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        conn.execute(
            &format!(
                "DELETE FROM agent_output_variables \
                 WHERE artifact_dir IN ({placeholders})"
            ),
            params_from_iter(chunk.iter()),
        )
        .map_err(|e| e.to_string())?;
        conn.execute(
            &format!(
                "DELETE FROM agent_artifact_model_aliases \
                 WHERE artifact_dir IN ({placeholders})"
            ),
            params_from_iter(chunk.iter()),
        )
        .map_err(|e| e.to_string())?;

        let mut alias_values: Vec<&str> =
            chunk.iter().map(String::as_str).collect();
        alias_values.extend(chunk.iter().map(String::as_str));
        conn.execute(
            &format!(
                "DELETE FROM agent_artifact_aliases \
                 WHERE artifact_dir IN ({placeholders}) \
                    OR alias_path IN ({placeholders})"
            ),
            params_from_iter(alias_values),
        )
        .map_err(|e| e.to_string())?;
        conn.execute(
            &format!(
                "DELETE FROM agent_artifacts \
                 WHERE artifact_dir IN ({placeholders})"
            ),
            params_from_iter(chunk.iter()),
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn repair_abandoned_agent_artifact_index_rows(
    conn: &mut Connection,
) -> Result<u64, String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let updates: Vec<(String, AgentArtifactRecordWire)> = {
        let mut stmt = tx
            .prepare(
                "SELECT projects_root, hidden, cl_name, record_json \
                 FROM agent_artifacts \
                 WHERE has_done_marker = 1 \
                   AND done_outcome = ?1",
            )
            .map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query([ABANDONED_DONE_OUTCOME])
            .map_err(|e| e.to_string())?;
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
        hidden_terminal_rows_retained: 0,
        hidden_terminal_rows_pruned: 0,
    })
}

/// Read one artifact-index metadata value.
pub fn read_agent_artifact_index_meta(
    index_path: &Path,
    key: &str,
) -> Result<Option<String>, String> {
    let conn = open_index_read_only(index_path)?;
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

/// Load full indexed artifact records by canonical or alias artifact dir.
pub fn load_agent_artifact_records(
    index_path: &Path,
    artifact_dirs: &[String],
) -> Result<Vec<AgentArtifactRecordWire>, String> {
    if artifact_dirs.is_empty() {
        return Ok(Vec::new());
    }

    let conn = open_index_read_only(index_path)?;
    let mut resolved_dirs = Vec::new();
    let mut unique_dirs = BTreeSet::new();
    for artifact_dir in artifact_dirs {
        let resolved = resolve_index_artifact_dir(&conn, artifact_dir)?;
        resolved_dirs.push(resolved.clone());
        unique_dirs.insert(resolved);
    }

    let unique_dirs: Vec<String> = unique_dirs.into_iter().collect();
    let mut records_by_dir = BTreeMap::new();
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    for chunk in unique_dirs.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT artifact_dir, record_json FROM agent_artifacts \
             WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let Ok(mut record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
            else {
                continue;
            };
            record.record_shape = AgentArtifactRecordShapeWire::Full;
            records_by_dir.insert(artifact_dir, record);
        }
    }

    Ok(resolved_dirs
        .into_iter()
        .filter_map(|dir| records_by_dir.get(&dir).cloned())
        .collect())
}

/// Return `(page_count, freelist_count, page_size)` for *conn*.
fn index_page_stats(conn: &Connection) -> Result<(u64, u64, u64), String> {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    let freelist_count: i64 = conn
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    Ok((
        page_count.max(0) as u64,
        freelist_count.max(0) as u64,
        page_size.max(0) as u64,
    ))
}

/// Return lightweight row counts for the artifact index.
pub fn agent_artifact_index_status(
    index_path: &Path,
) -> Result<AgentArtifactIndexStatusWire, String> {
    let conn = open_index_read_only(index_path)?;
    let retention = plan_hidden_terminal_retention(
        &conn,
        DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
    )?;
    let (page_count, freelist_count, page_size) = index_page_stats(&conn)?;
    Ok(AgentArtifactIndexStatusWire {
        schema_version: read_index_schema_version(&conn)?,
        index_path: index_path.to_string_lossy().into_owned(),
        agent_artifacts_rows: count_table_rows(&conn, "agent_artifacts")?,
        dismissed_agents_rows: count_table_rows(&conn, "dismissed_agents")?,
        agent_artifact_aliases_rows: count_table_rows(
            &conn,
            "agent_artifact_aliases",
        )?,
        agent_output_variables_rows: count_table_rows(
            &conn,
            "agent_output_variables",
        )?,
        agent_artifact_model_aliases_rows: count_table_rows(
            &conn,
            "agent_artifact_model_aliases",
        )?,
        hidden_terminal_retention_limit: DEFAULT_HIDDEN_TERMINAL_HOT_ROWS
            as u64,
        hidden_terminal_rows_retained: retention.retained_rows,
        hidden_terminal_rows_prunable: retention.prunable_rows,
        freelist_pages: freelist_count,
        freelist_bytes: freelist_count * page_size,
        file_size_bytes: page_count * page_size,
    })
}

/// Reclaim freelist pages left behind by deletes via `VACUUM`.
///
/// `VACUUM` rebuilds the database file into a fresh copy with no free
/// pages; it does not remove or alter any row. Tooling only: nothing in
/// this codebase calls this automatically, so running it against a live
/// index is always an explicit, user-initiated action.
pub fn vacuum_agent_artifact_index(
    index_path: &Path,
) -> Result<AgentArtifactIndexVacuumWire, String> {
    let conn = open_index(index_path)?;
    let (page_count_before, freelist_count_before, page_size) =
        index_page_stats(&conn)?;
    conn.execute_batch("VACUUM;").map_err(|e| e.to_string())?;
    let (page_count_after, freelist_count_after, _) = index_page_stats(&conn)?;
    let file_size_bytes_before = page_count_before * page_size;
    let file_size_bytes_after = page_count_after * page_size;
    Ok(AgentArtifactIndexVacuumWire {
        index_path: index_path.to_string_lossy().into_owned(),
        freelist_pages_before: freelist_count_before,
        freelist_pages_after: freelist_count_after,
        file_size_bytes_before,
        file_size_bytes_after,
        bytes_reclaimed: file_size_bytes_before
            .saturating_sub(file_size_bytes_after),
    })
}

/// Query indexed rows and return scanner-shaped records.
pub fn query_agent_artifact_index(
    index_path: &Path,
    projects_root: &Path,
    query: AgentArtifactIndexQueryWire,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactScanWire, String> {
    // Revalidate may write repaired rows back through repair_stale_rows_for_query;
    // Cached never writes, so it can use the cheaper read-only open.
    let conn = if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate
    {
        open_index(index_path)?
    } else {
        open_index_read_only(index_path)?
    };
    let mut stats = AgentArtifactScanStatsWire::default();
    let mut by_dir: BTreeMap<String, AgentArtifactRecordWire> = BTreeMap::new();
    let project_filter = project_filter_for_scan(projects_root, &options);
    if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate {
        repair_stale_rows_for_query(
            &conn,
            &query,
            &options,
            project_filter.as_ref(),
        )?;
    }

    let index_window = if should_use_windowed_candidate_query(&query) {
        Some(select_windowed_records(
            &conn,
            &query,
            &mut stats,
            &mut by_dir,
            project_filter.as_ref(),
        )?)
    } else {
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
                    freshness: query.freshness,
                    only_monitors: query.only_monitors,
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
                    freshness: query.freshness,
                    only_monitors: query.only_monitors,
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
                    freshness: query.freshness,
                    only_monitors: query.only_monitors,
                },
                &mut stats,
                &mut by_dir,
                &options,
                project_filter.as_ref(),
            )?;
        }
        None
    };

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
    if query.record_shape == AgentArtifactRecordShapeWire::List {
        for record in &mut records {
            project_record_for_list(record);
        }
    }
    let clan_context = select_clan_context(&conn, &records)?;

    Ok(AgentArtifactScanWire {
        schema_version: AGENT_SCAN_WIRE_SCHEMA_VERSION,
        projects_root: projects_root.to_string_lossy().into_owned(),
        options,
        stats,
        index_window,
        records,
        clan_context,
    })
}

fn project_record_for_list(record: &mut AgentArtifactRecordWire) {
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.linked_repos.clear();
    }
    if let Some(done) = record.done.as_mut() {
        if let Some(output) = done.step_output.as_mut() {
            project_output_for_list(output);
        }
    }
    for step in &mut record.prompt_steps {
        if let Some(output) = step.output.as_mut() {
            project_output_for_list(output);
        }
    }
    if let Some(workflow_state) = record.workflow_state.as_mut() {
        for step in &mut workflow_state.steps {
            if let Some(output) = step.output.as_mut() {
                project_output_for_list(output);
            }
        }
    }
    record.record_shape = AgentArtifactRecordShapeWire::List;
}

fn project_output_for_list(
    output: &mut serde_json::Map<String, serde_json::Value>,
) {
    output.remove("_raw");
    output.remove("_data");
}

/// Query grouped output-variable history from the persistent artifact index.
pub fn query_agent_output_variable_history(
    index_path: &Path,
    query: AgentOutputVariableHistoryQueryWire,
) -> Result<AgentOutputVariableHistoryWire, String> {
    if !query.values.is_empty() && !query.value_json.is_empty() {
        return Err(
            "values and value_json filters are mutually exclusive".to_string()
        );
    }

    let conn = open_index_read_only(index_path)?;
    let exact_value_json = query
        .value_json
        .iter()
        .map(canonical_output_variable_json)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let rows =
        select_output_variable_occurrences(&conn, &query, &exact_value_json)?;
    let mut occurrences = Vec::new();
    for row in rows {
        let occurrence = row.into_occurrence()?;
        if !output_variable_occurrence_matches_filters(&occurrence, &query) {
            continue;
        }
        occurrences.push(occurrence);
    }
    occurrences.sort_by(compare_output_variable_occurrences_newest);

    let mut keys: BTreeMap<String, OutputVariableKeyAccumulator> =
        BTreeMap::new();
    for occurrence in occurrences {
        keys.entry(occurrence.key.clone())
            .or_insert_with(|| {
                OutputVariableKeyAccumulator::new(occurrence.key.clone())
            })
            .push(occurrence);
    }

    let mut key_groups: Vec<AgentOutputVariableKeyGroupWire> = keys
        .into_values()
        .map(|accumulator| {
            accumulator.into_wire(query.value_limit, query.reverse)
        })
        .collect();
    sort_output_variable_key_groups(&mut key_groups, query.reverse);

    let total_key_count = key_groups.len() as u64;
    let returned_key_count =
        truncate_to_limit(&mut key_groups, query.key_limit) as u64;
    let requested_key_limit = query.key_limit;

    Ok(AgentOutputVariableHistoryWire {
        schema_version: AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        query,
        keys_limit: AgentOutputVariableLimitWire {
            limit: requested_key_limit,
            total_count: total_key_count,
            returned_count: returned_key_count,
            truncated: returned_key_count < total_key_count,
        },
        groups: key_groups,
    })
}

/// Query bounded per-alias agent history from the persistent artifact index.
pub fn query_agent_alias_history(
    index_path: &Path,
    query: AgentAliasHistoryQueryWire,
) -> Result<AgentAliasHistoryWire, String> {
    if query.aliases.is_empty() {
        return Err("aliases must be a non-empty list".to_string());
    }

    // Revalidate may write refreshed rows back via refresh_alias_history_candidates;
    // Cached never writes, so it can use the cheaper read-only open.
    let conn = if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate
    {
        open_index(index_path)?
    } else {
        open_index_read_only(index_path)?
    };
    if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate {
        refresh_alias_history_candidates(
            &conn,
            &query.aliases,
            &query.projects,
        )?;
    }

    let mut groups = Vec::with_capacity(query.aliases.len());
    for alias in &query.aliases {
        groups.push(select_alias_history_group(&conn, alias, &query)?);
    }

    Ok(AgentAliasHistoryWire {
        schema_version: AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        query,
        groups,
    })
}

fn refresh_alias_history_candidates(
    conn: &Connection,
    aliases: &[String],
    projects: &[String],
) -> Result<(), String> {
    if aliases.is_empty() {
        return Ok(());
    }
    let mut clauses = vec![format!(
        "artifact_dir IN (SELECT artifact_dir \
         FROM agent_artifact_model_aliases WHERE alias IN ({}))",
        placeholders(aliases.len())
    )];
    let mut values: Vec<String> = aliases.to_vec();
    if !projects.is_empty() {
        clauses.push(format!(
            "project_name IN ({})",
            placeholders(projects.len())
        ));
        values.extend(projects.iter().cloned());
    }
    let sql = format!(
        "SELECT artifact_dir, projects_root, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig, xprompts_sig FROM agent_artifacts \
         WHERE {}",
        clauses.join(" AND ")
    );
    let mut pending: Vec<PendingRefreshRow> = Vec::new();
    {
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(values.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            pending.push(pending_refresh_row_from_sql(row)?);
        }
    }

    let options = AgentArtifactScanOptionsWire::default();
    for row in pending {
        let current = MarkerSignatures::from_artifact_dir(&row.artifact_dir);
        if row.stored == current {
            continue;
        }
        let projects_root = PathBuf::from(&row.row_projects_root);
        let artifact_dir = PathBuf::from(&row.artifact_dir);
        if let Some(refreshed) =
            scan_agent_artifact_dir(&projects_root, &artifact_dir, &options)
        {
            let _ = upsert_record(conn, &projects_root, &refreshed);
        }
    }
    Ok(())
}

fn select_alias_history_group(
    conn: &Connection,
    alias: &str,
    query: &AgentAliasHistoryQueryWire,
) -> Result<AgentAliasHistoryGroupWire, String> {
    let (where_sql, values) = alias_history_where_clause(alias, query);
    let count_sql = format!(
        "SELECT COUNT(*) FROM agent_artifact_model_aliases ma \
         INNER JOIN agent_artifacts a ON a.artifact_dir = ma.artifact_dir \
         {where_sql}"
    );
    let total_count: i64 = conn
        .query_row(&count_sql, params_from_iter(values.iter()), |row| {
            row.get(0)
        })
        .map_err(|e| e.to_string())?;
    let total_count = u64::try_from(total_count).map_err(|e| e.to_string())?;

    let mut select_sql = format!(
        "SELECT a.artifact_dir, a.project_name, a.workflow_dir_name, \
                a.timestamp, a.agent_name, a.workflow_name, a.model, \
                a.llm_provider, a.status, a.workflow_status, \
                a.has_done_marker, a.hidden, a.started_at, a.finished_at, \
                a.retry_attempt, a.cl_name, ma.position, a.record_json \
         FROM agent_artifact_model_aliases ma \
         INNER JOIN agent_artifacts a ON a.artifact_dir = ma.artifact_dir \
         {where_sql} \
         ORDER BY a.timestamp DESC, a.artifact_dir DESC"
    );
    let mut select_values = values.clone();
    if query.limit_per_alias > 0 {
        select_sql.push_str(" LIMIT ?");
        select_values.push(query.limit_per_alias.to_string());
    }

    let mut stmt = conn.prepare(&select_sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(select_values.iter()))
        .map_err(|e| e.to_string())?;
    let mut runs = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        if let Ok(Some(run)) =
            alias_run_from_sql_row(row, query.prompt_snippet_bytes)
        {
            runs.push(run);
        }
    }

    let returned_count = runs.len() as u64;
    Ok(AgentAliasHistoryGroupWire {
        alias: alias.to_string(),
        runs_limit: AgentAliasHistoryLimitWire {
            limit: query.limit_per_alias,
            total_count,
            returned_count,
            truncated: returned_count < total_count,
        },
        runs,
    })
}

fn alias_history_where_clause(
    alias: &str,
    query: &AgentAliasHistoryQueryWire,
) -> (String, Vec<String>) {
    let mut clauses = vec!["ma.alias = ?".to_string()];
    let mut values = vec![alias.to_string()];
    if !query.include_hidden {
        clauses.push("a.hidden = 0".to_string());
    }
    if !query.projects.is_empty() {
        clauses.push(format!(
            "a.project_name IN ({})",
            placeholders(query.projects.len())
        ));
        values.extend(query.projects.iter().cloned());
    }
    (format!("WHERE {}", clauses.join(" AND ")), values)
}

fn alias_run_from_sql_row(
    row: &rusqlite::Row<'_>,
    prompt_snippet_bytes: u32,
) -> Result<Option<AgentAliasRunWire>, String> {
    let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
    let project_name: String = row.get(1).map_err(|e| e.to_string())?;
    let workflow_dir_name: String = row.get(2).map_err(|e| e.to_string())?;
    let timestamp: String = row.get(3).map_err(|e| e.to_string())?;
    let agent_name: Option<String> = row.get(4).map_err(|e| e.to_string())?;
    let workflow_name: Option<String> =
        row.get(5).map_err(|e| e.to_string())?;
    let model: Option<String> = row.get(6).map_err(|e| e.to_string())?;
    let llm_provider: Option<String> = row.get(7).map_err(|e| e.to_string())?;
    let status: String = row.get(8).map_err(|e| e.to_string())?;
    let workflow_status: Option<String> =
        row.get(9).map_err(|e| e.to_string())?;
    let has_done_marker =
        row.get::<_, i64>(10).map_err(|e| e.to_string())? != 0;
    let hidden = row.get::<_, i64>(11).map_err(|e| e.to_string())? != 0;
    let started_at: Option<String> = row.get(12).map_err(|e| e.to_string())?;
    let finished_at: Option<f64> = row.get(13).map_err(|e| e.to_string())?;
    let retry_attempt: Option<i64> = row.get(14).map_err(|e| e.to_string())?;
    let cl_name: Option<String> = row.get(15).map_err(|e| e.to_string())?;
    let alias_position =
        u32::try_from(row.get::<_, i64>(16).map_err(|e| e.to_string())?)
            .unwrap_or(0);
    let record_json: String = row.get(17).map_err(|e| e.to_string())?;
    let Ok(record) =
        serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
    else {
        return Ok(None);
    };
    let meta = record.agent_meta.as_ref();
    let trail = meta.map(effective_model_alias_trail).unwrap_or_default();
    Ok(Some(AgentAliasRunWire {
        artifact_dir: artifact_dir.clone(),
        project_name,
        workflow_dir_name,
        timestamp,
        agent_name,
        workflow_name,
        model,
        llm_provider,
        reasoning_effort: meta.and_then(|m| m.reasoning_effort.clone()),
        model_alias: meta.and_then(|m| m.model_alias.clone()),
        model_alias_origin: meta.and_then(|m| m.model_alias_origin.clone()),
        model_alias_trail: trail,
        alias_position,
        status,
        workflow_status,
        has_done_marker,
        hidden,
        started_at,
        finished_at,
        retry_attempt,
        bead_id: meta.and_then(|m| m.bead_id.clone()),
        cl_name,
        workspace_num: meta.and_then(|m| m.workspace_num),
        prompt_snippet: read_alias_history_prompt_snippet(
            &artifact_dir,
            prompt_snippet_bytes,
        ),
        used_xprompts: record.used_xprompts,
    }))
}

const RAW_PROMPT_FILE: &str = "raw_xprompt.md";
const ALIAS_HISTORY_PROMPT_SNIPPET_ELLIPSIS: &str = "...";

fn effective_model_alias_trail(meta: &AgentMetaWire) -> Vec<String> {
    let trail: Vec<String> = meta
        .model_alias_trail
        .iter()
        .map(|alias| alias.trim())
        .filter(|alias| !alias.is_empty())
        .map(ToString::to_string)
        .collect();
    if !trail.is_empty() {
        return trail;
    }
    meta.model_alias
        .as_deref()
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .map(|alias| vec![alias.to_string()])
        .unwrap_or_default()
}

fn read_alias_history_prompt_snippet(
    artifact_dir: &str,
    max_bytes: u32,
) -> Option<String> {
    if max_bytes == 0 {
        return None;
    }
    let path = Path::new(artifact_dir).join(RAW_PROMPT_FILE);
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(_) => return None,
    };
    let reader = BufReader::new(file);
    let mut body = String::new();
    let mut skipping = true;
    let read_cap = (max_bytes as usize)
        .saturating_mul(4)
        .max(max_bytes as usize);
    for line in reader.lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => return None,
        };
        if skipping {
            if is_leading_prompt_prefix_line(&line) {
                continue;
            }
            skipping = false;
        }
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&line);
        if body.len() >= read_cap {
            break;
        }
    }
    Some(truncate_prompt_snippet(
        &collapse_prompt_whitespace(&body),
        max_bytes as usize,
    ))
}

fn is_leading_prompt_prefix_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty() || trimmed.starts_with('%') || trimmed.starts_with('#')
}

fn collapse_prompt_whitespace(text: &str) -> String {
    let mut out = String::new();
    let mut prev_ws = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !prev_ws && !out.is_empty() {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(ch);
            prev_ws = false;
        }
    }
    out
}

fn truncate_prompt_snippet(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let ellipsis = ALIAS_HISTORY_PROMPT_SNIPPET_ELLIPSIS;
    let budget = max_bytes.saturating_sub(ellipsis.len());
    let mut out = String::new();
    let mut bytes = 0usize;
    for ch in text.chars() {
        let next = bytes + ch.len_utf8();
        if next > budget {
            break;
        }
        out.push(ch);
        bytes = next;
    }
    out.push_str(ellipsis);
    out
}

/// Load indexed output-variable occurrences for selector resolution.
pub(crate) fn load_output_variable_occurrences(
    index_path: &Path,
    projects: &[String],
    include_hidden: bool,
) -> Result<Vec<AgentOutputVariableOccurrenceWire>, String> {
    let query = AgentOutputVariableHistoryQueryWire {
        projects: projects.to_vec(),
        include_hidden,
        key_limit: 0,
        value_limit: 0,
        ..AgentOutputVariableHistoryQueryWire::default()
    };
    let conn = open_index_read_only(index_path)?;
    let rows =
        select_output_variable_occurrences(&conn, &query, &BTreeSet::new())?;
    let mut occurrences = Vec::new();
    for row in rows {
        occurrences.push(row.into_occurrence()?);
    }
    occurrences.sort_by(compare_output_variable_occurrences_newest);
    Ok(occurrences)
}

fn select_output_variable_occurrences(
    conn: &Connection,
    query: &AgentOutputVariableHistoryQueryWire,
    exact_value_json: &BTreeSet<String>,
) -> Result<Vec<IndexedOutputVariableOccurrence>, String> {
    let mut clauses: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    if !query.include_hidden {
        clauses.push("hidden = 0".to_string());
    }
    if !query.projects.is_empty() {
        clauses.push(format!(
            "project_name IN ({})",
            placeholders(query.projects.len())
        ));
        values.extend(query.projects.iter().cloned());
    }
    if let Some(since) = query.since_timestamp.as_ref() {
        clauses.push("timestamp >= ?".to_string());
        values.push(since.clone());
    }
    if let Some(until) = query.until_timestamp.as_ref() {
        clauses.push("timestamp <= ?".to_string());
        values.push(until.clone());
    }
    if !exact_value_json.is_empty() {
        clauses.push(format!(
            "value_json IN ({})",
            placeholders(exact_value_json.len())
        ));
        values.extend(exact_value_json.iter().cloned());
    }

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let sql = format!(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_name, cl_name, variable_key, value_json,
               hidden
        FROM agent_output_variables
        {where_sql}
        ORDER BY timestamp DESC, project_name ASC, artifact_dir ASC,
                 variable_key ASC
        "#
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(IndexedOutputVariableOccurrence {
            artifact_dir: row.get(0).map_err(|e| e.to_string())?,
            project_name: row.get(1).map_err(|e| e.to_string())?,
            workflow_dir_name: row.get(2).map_err(|e| e.to_string())?,
            timestamp: row.get(3).map_err(|e| e.to_string())?,
            agent_name: row.get(4).map_err(|e| e.to_string())?,
            cl_name: row.get(5).map_err(|e| e.to_string())?,
            key: row.get(6).map_err(|e| e.to_string())?,
            value_json: row.get(7).map_err(|e| e.to_string())?,
            hidden: row.get::<_, i64>(8).map_err(|e| e.to_string())? != 0,
        });
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct IndexedOutputVariableOccurrence {
    artifact_dir: String,
    project_name: String,
    workflow_dir_name: String,
    timestamp: String,
    agent_name: Option<String>,
    cl_name: Option<String>,
    key: String,
    value_json: String,
    hidden: bool,
}

impl IndexedOutputVariableOccurrence {
    fn into_occurrence(
        self,
    ) -> Result<AgentOutputVariableOccurrenceWire, String> {
        let value =
            serde_json::from_str::<OutputVariableValue>(&self.value_json)
                .map_err(|e| {
                    format!(
                        "invalid indexed output-variable JSON for {}:{}: {e}",
                        self.artifact_dir, self.key
                    )
                })?;
        Ok(AgentOutputVariableOccurrenceWire {
            artifact_dir: self.artifact_dir,
            project_name: self.project_name,
            workflow_dir_name: self.workflow_dir_name,
            timestamp: self.timestamp,
            agent_name: self.agent_name,
            cl_name: self.cl_name,
            key: self.key,
            value,
            value_json: self.value_json,
            hidden: self.hidden,
        })
    }
}

fn output_variable_occurrence_matches_filters(
    occurrence: &AgentOutputVariableOccurrenceWire,
    query: &AgentOutputVariableHistoryQueryWire,
) -> bool {
    if !query.agents.is_empty()
        && !query.agents.iter().any(|pattern| {
            occurrence
                .agent_name
                .as_deref()
                .is_some_and(|agent| agent_pattern_matches(pattern, agent))
        })
    {
        return false;
    }
    if !query.keys.is_empty()
        && !query
            .keys
            .iter()
            .any(|pattern| glob_matches(pattern, &occurrence.key))
    {
        return false;
    }
    if !query.values.is_empty()
        && !query.values.iter().any(|needle| {
            output_variable_value_contains(&occurrence.value, needle)
        })
    {
        return false;
    }
    true
}

fn output_variable_value_contains(
    value: &OutputVariableValue,
    needle: &str,
) -> bool {
    let needle = needle.to_lowercase();
    if needle.is_empty() {
        return true;
    }
    canonical_output_variable_json(value)
        .map(|json| json.to_lowercase().contains(&needle))
        .unwrap_or(false)
        || output_variable_scalar_text(value)
            .map(|text| text.to_lowercase().contains(&needle))
            .unwrap_or(false)
}

fn agent_pattern_matches(pattern: &str, agent_name: &str) -> bool {
    if let Some(hood) = pattern.strip_suffix(".*") {
        if agent_name == hood
            || agent_name
                .strip_prefix(hood)
                .is_some_and(|suffix| suffix.starts_with('.'))
        {
            return true;
        }
    }
    glob_matches(pattern, agent_name)
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut p, mut v) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut star_value = 0usize;
    while v < value.len() {
        if p < pattern.len() && pattern[p] == value[v] {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            star_value = v;
        } else if let Some(star_index) = star {
            p = star_index + 1;
            star_value += 1;
            v = star_value;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

#[derive(Debug)]
struct OutputVariableKeyAccumulator {
    key: String,
    occurrences: Vec<AgentOutputVariableOccurrenceWire>,
    values: BTreeMap<String, OutputVariableValueAccumulator>,
}

impl OutputVariableKeyAccumulator {
    fn new(key: String) -> Self {
        Self {
            key,
            occurrences: Vec::new(),
            values: BTreeMap::new(),
        }
    }

    fn push(&mut self, occurrence: AgentOutputVariableOccurrenceWire) {
        self.values
            .entry(occurrence.value_json.clone())
            .or_insert_with(|| {
                OutputVariableValueAccumulator::new(
                    occurrence.value.clone(),
                    occurrence.value_json.clone(),
                )
            })
            .push(occurrence.clone());
        self.occurrences.push(occurrence);
    }

    fn into_wire(
        self,
        value_limit: u32,
        reverse: bool,
    ) -> AgentOutputVariableKeyGroupWire {
        let occurrence_count = self.occurrences.len() as u64;
        let mut values: Vec<AgentOutputVariableValueGroupWire> = self
            .values
            .into_values()
            .map(OutputVariableValueAccumulator::into_wire)
            .collect();
        sort_output_variable_value_groups(&mut values, reverse);
        let total_value_count = values.len() as u64;
        let returned_value_count =
            truncate_to_limit(&mut values, value_limit) as u64;
        AgentOutputVariableKeyGroupWire {
            key: self.key,
            occurrence_count,
            distinct_value_count: total_value_count,
            values_limit: AgentOutputVariableLimitWire {
                limit: value_limit,
                total_count: total_value_count,
                returned_count: returned_value_count,
                truncated: returned_value_count < total_value_count,
            },
            values,
        }
    }
}

#[derive(Debug)]
struct OutputVariableValueAccumulator {
    value: OutputVariableValue,
    value_json: String,
    occurrences: Vec<AgentOutputVariableOccurrenceWire>,
    agent_latest: BTreeMap<String, String>,
    projects: BTreeSet<String>,
}

impl OutputVariableValueAccumulator {
    fn new(value: OutputVariableValue, value_json: String) -> Self {
        Self {
            value,
            value_json,
            occurrences: Vec::new(),
            agent_latest: BTreeMap::new(),
            projects: BTreeSet::new(),
        }
    }

    fn push(&mut self, occurrence: AgentOutputVariableOccurrenceWire) {
        if let Some(agent_name) = occurrence.agent_name.as_ref() {
            let entry = self.agent_latest.entry(agent_name.clone());
            entry
                .and_modify(|timestamp| {
                    if occurrence.timestamp > *timestamp {
                        *timestamp = occurrence.timestamp.clone();
                    }
                })
                .or_insert_with(|| occurrence.timestamp.clone());
        }
        self.projects.insert(occurrence.project_name.clone());
        self.occurrences.push(occurrence);
    }

    fn into_wire(mut self) -> AgentOutputVariableValueGroupWire {
        self.occurrences
            .sort_by(compare_output_variable_occurrences_newest);
        let newest = self.occurrences.first().cloned().unwrap_or_else(|| {
            AgentOutputVariableOccurrenceWire {
                artifact_dir: String::new(),
                project_name: String::new(),
                workflow_dir_name: String::new(),
                timestamp: String::new(),
                agent_name: None,
                cl_name: None,
                key: String::new(),
                value: self.value.clone(),
                value_json: self.value_json.clone(),
                hidden: false,
            }
        });
        let first_seen_timestamp = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.timestamp.as_str())
            .min()
            .unwrap_or("")
            .to_string();
        let last_seen_timestamp = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.timestamp.as_str())
            .max()
            .unwrap_or("")
            .to_string();
        let mut agents: Vec<(String, String)> =
            self.agent_latest.into_iter().collect();
        agents.sort_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let agents: Vec<String> =
            agents.into_iter().map(|(agent, _)| agent).collect();
        let agent_count = agents.len() as u64;

        AgentOutputVariableValueGroupWire {
            value: self.value,
            value_json: self.value_json,
            occurrence_count: self.occurrences.len() as u64,
            agent_count,
            agents,
            projects: self.projects.into_iter().collect(),
            first_seen_timestamp,
            last_seen_timestamp,
            newest,
        }
    }
}

fn truncate_to_limit<T>(items: &mut Vec<T>, limit: u32) -> usize {
    if limit > 0 && items.len() > limit as usize {
        items.truncate(limit as usize);
    }
    items.len()
}

fn sort_output_variable_key_groups(
    groups: &mut [AgentOutputVariableKeyGroupWire],
    reverse: bool,
) {
    groups.sort_by(|left, right| {
        let ordering = compare_output_variable_value_groups_by_seen(
            representative_value_group_for_key(left, reverse),
            representative_value_group_for_key(right, reverse),
            reverse,
        );
        ordering.then_with(|| left.key.cmp(&right.key))
    });
    for group in groups {
        sort_output_variable_value_groups(&mut group.values, reverse);
    }
}

fn representative_value_group_for_key(
    group: &AgentOutputVariableKeyGroupWire,
    reverse: bool,
) -> Option<&AgentOutputVariableValueGroupWire> {
    group.values.iter().min_by(|left, right| {
        compare_output_variable_value_groups_by_seen(
            Some(*left),
            Some(*right),
            reverse,
        )
    })
}

fn sort_output_variable_value_groups(
    groups: &mut [AgentOutputVariableValueGroupWire],
    reverse: bool,
) {
    groups.sort_by(|left, right| {
        compare_output_variable_value_groups_by_seen(
            Some(left),
            Some(right),
            reverse,
        )
        .then_with(|| left.value_json.cmp(&right.value_json))
    });
}

fn compare_output_variable_value_groups_by_seen(
    left: Option<&AgentOutputVariableValueGroupWire>,
    right: Option<&AgentOutputVariableValueGroupWire>,
    reverse: bool,
) -> Ordering {
    let Some(left) = left else {
        return Ordering::Greater;
    };
    let Some(right) = right else {
        return Ordering::Less;
    };
    if reverse {
        left.first_seen_timestamp
            .cmp(&right.first_seen_timestamp)
            .then_with(|| {
                left.newest.project_name.cmp(&right.newest.project_name)
            })
            .then_with(|| {
                left.newest.artifact_dir.cmp(&right.newest.artifact_dir)
            })
    } else {
        right
            .last_seen_timestamp
            .cmp(&left.last_seen_timestamp)
            .then_with(|| {
                left.newest.project_name.cmp(&right.newest.project_name)
            })
            .then_with(|| {
                left.newest.artifact_dir.cmp(&right.newest.artifact_dir)
            })
    }
}

pub(crate) fn compare_output_variable_occurrences_newest(
    left: &AgentOutputVariableOccurrenceWire,
    right: &AgentOutputVariableOccurrenceWire,
) -> Ordering {
    right
        .timestamp
        .cmp(&left.timestamp)
        .then_with(|| left.project_name.cmp(&right.project_name))
        .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
        .then_with(|| left.key.cmp(&right.key))
}

fn select_clan_context(
    conn: &Connection,
    records: &[AgentArtifactRecordWire],
) -> Result<Vec<super::wire::AgentClanContextWire>, String> {
    let keys = represented_clan_keys(records);
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut members = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT agent_clan, agent_clan_generation, clan_tribe, \
                    clan_summary, timestamp, artifact_dir \
             FROM agent_artifacts \
             WHERE agent_clan = ?1 \
               AND (agent_clan_generation = ?2 \
                    OR (?2 IS NULL AND agent_clan_generation IS NULL)) \
               AND (NULLIF(TRIM(clan_tribe), '') IS NOT NULL \
                    OR NULLIF(TRIM(clan_summary), '') IS NOT NULL)",
        )
        .map_err(|error| error.to_string())?;
    for (agent_clan, agent_clan_generation) in &keys {
        let rows = stmt
            .query_map(params![agent_clan, agent_clan_generation], |row| {
                Ok(ClanTribeMemberWire {
                    agent_clan: row.get(0)?,
                    agent_clan_generation: row.get(1)?,
                    clan_tribe: row.get(2)?,
                    clan_summary: row.get(3)?,
                    launch_timestamp: row.get(4)?,
                    identity: row.get(5)?,
                })
            })
            .map_err(|error| error.to_string())?;
        for row in rows {
            members.push(row.map_err(|error| error.to_string())?);
        }
    }
    Ok(resolve_clan_context(keys, members))
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
    let conn = open_index_read_only(index_path)?;
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
            agent_clan_generation TEXT,
            clan_tribe TEXT,
            clan_summary TEXT,
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
            done_outcome TEXT,
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
    if prior_version.map_or(true, |v| v < 15) {
        ensure_agent_artifacts_column(&conn, "agent_clan_generation", "TEXT")?;
        ensure_agent_artifacts_column(&conn, "clan_tribe", "TEXT")?;
        ensure_agent_artifacts_column(&conn, "clan_summary", "TEXT")?;
        migrate_clan_context_projection_v15(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 16) {
        migrate_record_json_refresh_v16(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 18) {
        migrate_record_json_refresh_v18(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 19) {
        ensure_agent_artifacts_column(&conn, "xprompts_sig", "TEXT")?;
        migrate_record_json_refresh_v19(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 20) {
        migrate_record_json_refresh_v20(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 21) {
        migrate_output_variable_projection_v21(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 22) {
        ensure_agent_artifacts_column(&conn, "model_alias_origin", "TEXT")?;
        migrate_model_alias_projection_v22(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 23) {
        migrate_record_json_refresh_v23(&mut conn)?;
    }
    if prior_version.map_or(true, |v| v < 24) {
        ensure_agent_artifacts_column(&conn, "done_outcome", "TEXT")?;
        migrate_done_outcome_projection_v24(&mut conn)?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent_clan \
         ON agent_artifacts(agent_clan, timestamp); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_done_outcome \
         ON agent_artifacts(done_outcome); \
         CREATE INDEX IF NOT EXISTS idx_agent_artifacts_clan_context \
         ON agent_artifacts(agent_clan, agent_clan_generation, timestamp);",
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
fn open_index_read_only(index_path: &Path) -> Result<Connection, String> {
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

/// v15 denormalizes generation-scoped clan declarations so bounded index
/// queries can resolve semantic context without parsing historical row JSON.
/// The Python lifecycle rebuilds older indexes from source after detecting
/// this schema bump, populating the new columns for existing records.
fn migrate_clan_context_projection_v15(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v16 refreshes `record_json` with `agent_meta.epic_plan_ref` so indexed
/// snapshot consumers retain the phase's parent-epic relationship after the
/// phase-authored plan replaces `sdd_plan_path`.
fn migrate_record_json_refresh_v16(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v18 refreshes `record_json` with `agent_meta.wait_priority` so indexed
/// snapshot consumers retain authored runner-slot priority without requiring
/// a live `waiting.json` marker.
fn migrate_record_json_refresh_v18(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v19 adds the launch-boundary `used_xprompts` projection to `record_json`
/// and signs `xprompts.json` so late writes refresh cached rows.
///
/// The Python lifecycle rebuilds older indexes from source after detecting
/// this schema bump, populating the projection for historical records.
fn migrate_record_json_refresh_v19(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v20 adds `agent_meta.model_alias` and `prompt_steps[*].model_alias` to
/// `record_json` so the ACE `Model:` field can render launch-time alias
/// provenance.
fn migrate_record_json_refresh_v20(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v22 adds a regenerable child projection for launch-time model aliases.
///
/// The migration is a pure `record_json` re-projection: it performs no
/// filesystem reads and skips malformed legacy payloads so index open
/// cannot fail on a single bad row.
fn migrate_model_alias_projection_v22(
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
            let Ok(record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
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
fn migrate_record_json_refresh_v23(
    conn: &mut Connection,
) -> Result<(), String> {
    conn.execute_batch("").map_err(|e| e.to_string())
}

/// v24 adds the scalar `done_outcome` projection for abandoned-row repair.
fn migrate_done_outcome_projection_v24(
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
            let Ok(record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
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

/// v21 adds a regenerable child projection for indexed output variables.
fn migrate_output_variable_projection_v21(
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
            let Ok(record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
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

fn upsert_record(
    conn: &Connection,
    projects_root: &Path,
    record: &AgentArtifactRecordWire,
) -> Result<(), String> {
    let summary = RecordSummary::from_record(record);
    let signatures = MarkerSignatures::from_artifact_dir(&record.artifact_dir);
    let done_outcome = record
        .done
        .as_ref()
        .and_then(|done| done.outcome.as_deref());
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
            workflow_state_sig, plan_path_sig, prompt_steps_sig, xprompts_sig,
            agent_clan_generation, clan_tribe, clan_summary, record_json,
            model_alias_origin, done_outcome, indexed_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, CURRENT_TIMESTAMP
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
            xprompts_sig = excluded.xprompts_sig,
            agent_clan_generation = excluded.agent_clan_generation,
            clan_tribe = excluded.clan_tribe,
            clan_summary = excluded.clan_summary,
            record_json = excluded.record_json,
            model_alias_origin = excluded.model_alias_origin,
            done_outcome = excluded.done_outcome,
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
            signatures.xprompts,
            summary.agent_clan_generation,
            summary.clan_tribe,
            summary.clan_summary,
            record_json,
            summary.model_alias_origin,
            done_outcome,
        ],
    )
    .map_err(|e| e.to_string())?;
    upsert_output_variables_for_record(conn, projects_root, record)?;
    upsert_model_aliases_for_record(conn, projects_root, record)?;
    Ok(())
}

fn upsert_model_aliases_for_record(
    conn: &Connection,
    _projects_root: &Path,
    record: &AgentArtifactRecordWire,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM agent_artifact_model_aliases WHERE artifact_dir = ?1",
        [record.artifact_dir.as_str()],
    )
    .map_err(|e| e.to_string())?;

    let Some(meta) = record.agent_meta.as_ref() else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for (position, alias) in
        effective_model_alias_trail(meta).into_iter().enumerate()
    {
        if !seen.insert(alias.clone()) {
            continue;
        }
        conn.execute(
            r#"
            INSERT INTO agent_artifact_model_aliases (
                artifact_dir, alias, position
            ) VALUES (?1, ?2, ?3)
            "#,
            params![
                record.artifact_dir.as_str(),
                alias.as_str(),
                position as i64,
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn upsert_output_variables_for_record(
    conn: &Connection,
    projects_root: &Path,
    record: &AgentArtifactRecordWire,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM agent_output_variables WHERE artifact_dir = ?1",
        [record.artifact_dir.as_str()],
    )
    .map_err(|e| e.to_string())?;

    let Some(meta) = record.agent_meta.as_ref() else {
        return Ok(());
    };
    if meta.output_variables.is_empty() {
        return Ok(());
    }

    let summary = RecordSummary::from_record(record);
    let projects_root = projects_root.to_string_lossy().into_owned();
    for (key, value) in &meta.output_variables {
        let value_json = canonical_output_variable_json(value)?;
        let value_scalar_text = output_variable_scalar_text(value);
        conn.execute(
            r#"
            INSERT INTO agent_output_variables (
                artifact_dir, variable_key, value_json, value_scalar_text,
                projects_root, project_name, project_dir, project_file,
                workflow_dir_name, timestamp, agent_name, cl_name, hidden,
                has_done_marker, finished_at, status, agent_type, indexed_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, ?16, ?17, CURRENT_TIMESTAMP
            )
            "#,
            params![
                record.artifact_dir.as_str(),
                key.as_str(),
                value_json.as_str(),
                value_scalar_text.as_deref(),
                projects_root.as_str(),
                record.project_name.as_str(),
                record.project_dir.as_str(),
                record.project_file.as_str(),
                record.workflow_dir_name.as_str(),
                record.timestamp.as_str(),
                summary.agent_name.as_deref(),
                summary.cl_name.as_deref(),
                summary.hidden as i64,
                record.has_done_marker as i64,
                summary.finished_at,
                summary.status.as_str(),
                summary.agent_type.as_str(),
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(crate) fn canonical_output_variable_json(
    value: &OutputVariableValue,
) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}

fn output_variable_scalar_text(value: &OutputVariableValue) -> Option<String> {
    match value {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
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
         prompt_steps_sig, xprompts_sig FROM agent_artifacts \
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
    let Some(latest_modified) =
        artifact_dir_latest_modified(&record.artifact_dir)
    else {
        return Ok(TerminalizationOutcome::Skipped);
    };
    if !artifact_dir_is_stale(latest_modified, stale_after) {
        return Ok(TerminalizationOutcome::Skipped);
    }
    if record_has_live_workspace_claim(&record)? {
        return Ok(TerminalizationOutcome::Skipped);
    }

    let terminalized =
        terminalized_abandoned_record(record, Some(latest_modified));
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

fn artifact_dir_is_stale(latest: SystemTime, stale_after: Duration) -> bool {
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
    latest_modified: Option<SystemTime>,
) -> AgentArtifactRecordWire {
    let summary = RecordSummary::from_record(&record);
    let meta = record.agent_meta.as_ref();
    let finished_at = meta
        .and_then(|value| value.stopped_at.as_deref())
        .and_then(parse_runtime_timestamp)
        .or_else(|| {
            latest_modified.and_then(system_time_to_unix_timestamp_secs)
        });
    record.running = None;
    record.waiting = None;
    record.pending_question = None;
    record.has_done_marker = true;
    record.done = Some(DoneMarkerWire {
        outcome: Some(ABANDONED_DONE_OUTCOME.to_string()),
        finished_at,
        finished_at_estimated: true,
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

fn system_time_to_unix_timestamp_secs(value: SystemTime) -> Option<f64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

fn refresh_stale_rows(
    conn: &Connection,
    where_sql: &str,
    options: &AgentArtifactScanOptionsWire,
) -> Result<(), String> {
    let sql = refresh_stale_rows_sql(where_sql);
    let mut pending: Vec<PendingRefreshRow> = Vec::new();
    {
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            pending.push(pending_refresh_row_from_sql(row)?);
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

fn refresh_stale_rows_sql(where_sql: &str) -> String {
    format!(
        "SELECT artifact_dir, projects_root, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig, xprompts_sig FROM agent_artifacts {where_sql}"
    )
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
         prompt_steps_sig, xprompts_sig \
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
        let record = match query.freshness {
            AgentArtifactIndexFreshnessWire::Cached => {
                match serde_json::from_str::<AgentArtifactRecordWire>(
                    &row.record_json,
                ) {
                    Ok(record) => record,
                    Err(_) => {
                        stats.json_decode_errors += 1;
                        continue;
                    }
                }
            }
            AgentArtifactIndexFreshnessWire::Revalidate => {
                let current =
                    MarkerSignatures::from_artifact_dir(&row.artifact_dir);
                if row.stored == current {
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
                            // Best-effort: persist the refreshed record so the
                            // next query sees fresh data without re-doing the
                            // rescan. A single INSERT ... ON CONFLICT is
                            // atomic in SQLite, so concurrent readers see
                            // either the old or new row but never a torn
                            // write. Upsert failure is non-fatal — we still
                            // return the refreshed record to the caller.
                            let _ =
                                upsert_record(conn, &projects_root, &refreshed);
                            refreshed
                        }
                        None => match serde_json::from_str::<
                            AgentArtifactRecordWire,
                        >(&row.record_json)
                        {
                            Ok(record) => record,
                            Err(_) => {
                                stats.json_decode_errors += 1;
                                continue;
                            }
                        },
                    }
                }
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
            query.only_monitors,
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
        xprompts: row.get(11).map_err(|e| e.to_string())?,
    };
    Ok(PendingRow {
        artifact_dir,
        row_projects_root,
        record_json,
        stored,
    })
}

fn pending_refresh_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> Result<PendingRefreshRow, String> {
    let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
    let row_projects_root: String = row.get(1).map_err(|e| e.to_string())?;
    let stored = MarkerSignatures {
        agent_meta: row.get(2).map_err(|e| e.to_string())?,
        done: row.get(3).map_err(|e| e.to_string())?,
        running: row.get(4).map_err(|e| e.to_string())?,
        waiting: row.get(5).map_err(|e| e.to_string())?,
        pending_question: row.get(6).map_err(|e| e.to_string())?,
        workflow_state: row.get(7).map_err(|e| e.to_string())?,
        plan_path: row.get(8).map_err(|e| e.to_string())?,
        prompt_steps: row.get(9).map_err(|e| e.to_string())?,
        xprompts: row.get(10).map_err(|e| e.to_string())?,
    };
    Ok(PendingRefreshRow {
        artifact_dir,
        row_projects_root,
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
    freshness: AgentArtifactIndexFreshnessWire,
    only_monitors: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateSelection {
    Active,
    Completed,
}

#[derive(Debug, Clone)]
struct IndexedCandidateRow {
    artifact_dir: String,
    project_name: String,
    agent_type: String,
    cl_name: Option<String>,
    model: Option<String>,
    llm_provider: Option<String>,
    selection: CandidateSelection,
}

impl IndexedCandidateRow {
    fn scalar_values(
        &self,
        field: AgentArtifactCandidateFieldWire,
    ) -> Vec<&str> {
        match field {
            AgentArtifactCandidateFieldWire::Project => {
                vec![self.project_name.as_str()]
            }
            AgentArtifactCandidateFieldWire::Cl => {
                self.cl_name.as_deref().into_iter().collect()
            }
            AgentArtifactCandidateFieldWire::Model => {
                self.model.as_deref().into_iter().collect()
            }
            AgentArtifactCandidateFieldWire::Provider => {
                self.llm_provider.as_deref().into_iter().collect()
            }
            AgentArtifactCandidateFieldWire::Type => {
                vec![self.agent_type.as_str()]
            }
        }
    }
}

fn should_use_windowed_candidate_query(
    query: &AgentArtifactIndexQueryWire,
) -> bool {
    query.window_limit.is_some()
        && query.freshness == AgentArtifactIndexFreshnessWire::Cached
        && query.include_active
        && query.include_recent_completed
        && !query.include_full_history
        && !query.include_hidden
        && !query.only_monitors
}

fn select_windowed_records(
    conn: &Connection,
    query: &AgentArtifactIndexQueryWire,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
    project_filter: Option<&BTreeSet<String>>,
) -> Result<AgentArtifactIndexWindowWire, String> {
    let requested_limit = query.window_limit.unwrap_or(1).max(1);
    let active_rows = select_candidate_rows(
        conn,
        active_where(query.include_hidden, project_filter),
        CandidateSelection::Active,
    )?;
    let mut active_candidates = Vec::new();
    let mut active_dirs = BTreeSet::new();
    for row in active_rows {
        if candidate_matches_query_filter(&row, query.candidate_filter.as_ref())
        {
            active_dirs.insert(row.artifact_dir.clone());
            active_candidates.push(row);
        }
    }

    let completed_rows = select_candidate_rows(
        conn,
        completed_window_where(query.include_hidden, project_filter),
        CandidateSelection::Completed,
    )?;
    let mut completed_candidates = Vec::new();
    for row in completed_rows {
        if active_dirs.contains(&row.artifact_dir) {
            continue;
        }
        if candidate_matches_query_filter(&row, query.candidate_filter.as_ref())
        {
            completed_candidates.push(row);
        }
    }

    let completed_budget =
        requested_limit.saturating_sub(active_candidates.len() as u32) as usize;
    let mut selected = active_candidates.clone();
    selected
        .extend(completed_candidates.iter().take(completed_budget).cloned());
    let selected_candidate_count = selected.len() as u64;
    let has_more =
        active_candidates.len() + completed_candidates.len() > selected.len();
    select_records_for_windowed_candidates(
        conn,
        selected,
        stats,
        by_dir,
        query.include_hidden,
    )?;

    let returned_record_count = by_dir.len() as u64;
    Ok(AgentArtifactIndexWindowWire {
        requested_limit: Some(requested_limit),
        selected_candidate_count,
        returned_record_count,
        active_candidate_count: active_candidates.len() as u64,
        completed_candidate_count: completed_candidates.len() as u64,
        has_more,
        truncated: has_more,
    })
}

fn select_candidate_rows(
    conn: &Connection,
    where_sql: String,
    selection: CandidateSelection,
) -> Result<Vec<IndexedCandidateRow>, String> {
    let sql = format!(
        "SELECT artifact_dir, project_name, agent_type, cl_name, model, llm_provider \
         FROM agent_artifacts {where_sql}"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(IndexedCandidateRow {
            artifact_dir: row.get(0).map_err(|e| e.to_string())?,
            project_name: row.get(1).map_err(|e| e.to_string())?,
            agent_type: row.get(2).map_err(|e| e.to_string())?,
            cl_name: row.get(3).map_err(|e| e.to_string())?,
            model: row.get(4).map_err(|e| e.to_string())?,
            llm_provider: row.get(5).map_err(|e| e.to_string())?,
            selection,
        });
    }
    Ok(result)
}

fn select_records_for_windowed_candidates(
    conn: &Connection,
    candidates: Vec<IndexedCandidateRow>,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
    include_hidden: bool,
) -> Result<(), String> {
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    let mut selected_by_dir = BTreeMap::new();
    for candidate in candidates {
        selected_by_dir
            .entry(candidate.artifact_dir.clone())
            .or_insert(candidate);
    }
    let artifact_dirs: Vec<String> = selected_by_dir.keys().cloned().collect();
    for chunk in artifact_dirs.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT artifact_dir, record_json FROM agent_artifacts \
             WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let Some(candidate) = selected_by_dir.get(&artifact_dir) else {
                continue;
            };
            let record_json: String = row.get(1).map_err(|e| e.to_string())?;
            let Ok(record) =
                serde_json::from_str::<AgentArtifactRecordWire>(&record_json)
            else {
                stats.json_decode_errors += 1;
                continue;
            };
            let selection = match candidate.selection {
                CandidateSelection::Active => RecordSelection::Active,
                CandidateSelection::Completed => RecordSelection::Completed,
            };
            if record_matches_selection(
                conn,
                &record,
                selection,
                include_hidden,
                false,
            )? {
                by_dir.insert(artifact_dir, record);
            }
        }
    }
    Ok(())
}

fn candidate_matches_query_filter(
    row: &IndexedCandidateRow,
    filter: Option<&AgentArtifactCandidateFilterWire>,
) -> bool {
    filter
        .map(|filter| candidate_filter_matches(row, filter))
        .unwrap_or(true)
}

fn candidate_filter_matches(
    row: &IndexedCandidateRow,
    filter: &AgentArtifactCandidateFilterWire,
) -> bool {
    match filter {
        AgentArtifactCandidateFilterWire::All { filters } => filters
            .iter()
            .all(|filter| candidate_filter_matches(row, filter)),
        AgentArtifactCandidateFilterWire::Any { filters } => filters
            .iter()
            .any(|filter| candidate_filter_matches(row, filter)),
        AgentArtifactCandidateFilterWire::Not { filter } => {
            !candidate_filter_matches(row, filter)
        }
        AgentArtifactCandidateFilterWire::Contains { field, value } => row
            .scalar_values(*field)
            .into_iter()
            .any(|candidate| contains_case_insensitive(candidate, value)),
        AgentArtifactCandidateFilterWire::Equals { field, value } => row
            .scalar_values(*field)
            .into_iter()
            .any(|candidate| scalar_equals(candidate, value)),
    }
}

fn contains_case_insensitive(candidate: &str, value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    candidate.to_lowercase().contains(&value.to_lowercase())
}

fn scalar_equals(candidate: &str, value: &str) -> bool {
    let candidate = candidate.to_lowercase();
    let value = value.to_lowercase();
    if candidate == value {
        return true;
    }
    candidate == "agent" && (value == "run" || value == "running")
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
    only_monitors: bool,
) -> Result<bool, String> {
    if only_monitors && !record_is_monitor(record) {
        return Ok(false);
    }
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

fn record_is_monitor(record: &AgentArtifactRecordWire) -> bool {
    is_real_monitor_member_record(record)
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

struct PendingRefreshRow {
    artifact_dir: String,
    row_projects_root: String,
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

fn completed_window_where(
    include_hidden: bool,
    project_filter: Option<&BTreeSet<String>>,
) -> String {
    let where_sql = if include_hidden {
        "WHERE has_done_marker = 1
         OR workflow_status IN ('completed', 'failed', 'cancelled', 'noop')
         ORDER BY timestamp DESC, artifact_dir DESC"
            .to_string()
    } else {
        format!(
            "WHERE hidden = 0
         AND (
             has_done_marker = 1
             OR workflow_status IN ('completed', 'failed', 'cancelled', 'noop')
         )
         AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
         ORDER BY timestamp DESC, artifact_dir DESC"
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
    agent_clan_generation: Option<String>,
    clan_tribe: Option<String>,
    clan_summary: Option<String>,
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
    model_alias_origin: Option<String>,
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

        let clan_key = meta.and_then(clan_key_from_meta);
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
            agent_clan: clan_key.as_ref().map(|(clan, _)| clan.clone()),
            agent_clan_generation: clan_key
                .as_ref()
                .and_then(|(_, generation)| generation.clone()),
            clan_tribe: meta.and_then(|m| m.clan_tribe.clone()),
            clan_summary: meta.and_then(|m| m.clan_summary.clone()),
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
            model_alias_origin: meta.and_then(|m| m.model_alias_origin.clone()),
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
    xprompts: Option<String>,
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
            xprompts: marker_signature(&dir.join("xprompts.json")),
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
    use std::collections::BTreeMap;
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
        artifact_for_project(root, "proj", ts)
    }

    fn artifact_for_project(root: &Path, project: &str, ts: &str) -> PathBuf {
        root.join(project)
            .join("artifacts")
            .join("ace-run")
            .join(ts)
    }

    fn windowed_index_query(limit: u32) -> AgentArtifactIndexQueryWire {
        AgentArtifactIndexQueryWire {
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            record_shape: AgentArtifactRecordShapeWire::List,
            window_limit: Some(limit),
            ..AgentArtifactIndexQueryWire::default()
        }
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
    fn windowed_query_decodes_only_selected_candidates() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let older = artifact(&projects, "20260827090000");
        let newer = artifact(&projects, "20260827100000");
        for (artifact_dir, name) in [(&older, "older"), (&newer, "newer")] {
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": name, "model": "gpt-5"}),
            );
            write_json(
                &artifact_dir.join("done.json"),
                json!({"outcome": "completed", "name": name}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        Connection::open(&index)
            .unwrap()
            .execute(
                "UPDATE agent_artifacts SET record_json = ?1 WHERE artifact_dir = ?2",
                params!["{not valid json", older.to_string_lossy().as_ref()],
            )
            .unwrap();

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            windowed_index_query(1),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].timestamp, "20260827100000");
        assert_eq!(snapshot.stats.json_decode_errors, 0);
        let window = snapshot.index_window.unwrap();
        assert_eq!(window.requested_limit, Some(1));
        assert_eq!(window.selected_candidate_count, 1);
        assert_eq!(window.returned_record_count, 1);
        assert_eq!(window.active_candidate_count, 0);
        assert_eq!(window.completed_candidate_count, 2);
        assert!(window.has_more);
        assert!(window.truncated);
    }

    #[test]
    fn windowed_query_preserves_active_rows_and_fills_with_completed() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for ts in ["20260827090000", "20260827090100", "20260827090200"] {
            write_json(
                &artifact(&projects, ts).join("agent_meta.json"),
                json!({"name": format!("active-{ts}")}),
            );
        }
        for ts in ["20260827090300", "20260827090400"] {
            let artifact_dir = artifact(&projects, ts);
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({"name": format!("done-{ts}")}),
            );
            write_json(
                &artifact_dir.join("done.json"),
                json!({"outcome": "completed", "name": format!("done-{ts}")}),
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
            windowed_index_query(4),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let timestamps: BTreeSet<&str> = snapshot
            .records
            .iter()
            .map(|record| record.timestamp.as_str())
            .collect();

        assert_eq!(timestamps.len(), 4);
        assert!(timestamps.contains("20260827090000"));
        assert!(timestamps.contains("20260827090100"));
        assert!(timestamps.contains("20260827090200"));
        assert!(timestamps.contains("20260827090400"));
        assert!(!timestamps.contains("20260827090300"));
        let window = snapshot.index_window.unwrap();
        assert_eq!(window.selected_candidate_count, 4);
        assert_eq!(window.returned_record_count, 4);
        assert_eq!(window.active_candidate_count, 3);
        assert_eq!(window.completed_candidate_count, 2);
        assert!(window.has_more);
    }

    #[test]
    fn windowed_query_applies_safe_candidate_filter() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let keep = artifact_for_project(&projects, "proj", "20260827101000");
        let wrong_model =
            artifact_for_project(&projects, "proj", "20260827101100");
        let wrong_project =
            artifact_for_project(&projects, "other", "20260827101200");
        for (artifact_dir, name, cl_name, model, provider) in [
            (&keep, "keep", "target-cl", "claude-opus-4", "anthropic"),
            (
                &wrong_model,
                "wrong-model",
                "target-cl",
                "gpt-5",
                "anthropic",
            ),
            (
                &wrong_project,
                "wrong-project",
                "target-cl",
                "claude-opus-4",
                "anthropic",
            ),
        ] {
            write_json(
                &artifact_dir.join("agent_meta.json"),
                json!({
                    "name": name,
                    "cl_name": cl_name,
                    "model": model,
                    "llm_provider": provider,
                }),
            );
            write_json(
                &artifact_dir.join("done.json"),
                json!({"outcome": "completed", "name": name, "cl_name": cl_name}),
            );
        }

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let mut query = windowed_index_query(10);
        query.candidate_filter = Some(AgentArtifactCandidateFilterWire::All {
            filters: vec![
                AgentArtifactCandidateFilterWire::Contains {
                    field: AgentArtifactCandidateFieldWire::Model,
                    value: "opus".to_string(),
                },
                AgentArtifactCandidateFilterWire::Any {
                    filters: vec![
                        AgentArtifactCandidateFilterWire::Equals {
                            field: AgentArtifactCandidateFieldWire::Provider,
                            value: "anthropic".to_string(),
                        },
                        AgentArtifactCandidateFilterWire::Contains {
                            field: AgentArtifactCandidateFieldWire::Cl,
                            value: "target".to_string(),
                        },
                    ],
                },
                AgentArtifactCandidateFilterWire::Not {
                    filter: Box::new(
                        AgentArtifactCandidateFilterWire::Contains {
                            field: AgentArtifactCandidateFieldWire::Project,
                            value: "other".to_string(),
                        },
                    ),
                },
            ],
        });

        let snapshot = query_agent_artifact_index(
            &index,
            &projects,
            query,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].artifact_dir, keep.to_string_lossy());
        let window = snapshot.index_window.unwrap();
        assert_eq!(window.selected_candidate_count, 1);
        assert_eq!(window.completed_candidate_count, 1);
        assert!(!window.has_more);
    }

    #[test]
    fn only_monitors_filters_to_monitor_family_role() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let plain_agent = artifact(&projects, "20260812170000");
        let monitor_member = artifact(&projects, "20260812170100");
        write_json(
            &plain_agent.join("agent_meta.json"),
            json!({"name": "acme--0"}),
        );
        write_json(
            &monitor_member.join("agent_meta.json"),
            json!({
                "name": "acme--mon",
                "agent_family": "acme",
                "agent_family_role": "monitor",
                "monitor_id": "m4kq",
                "monitor_command": "sleep 60",
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let indexed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: true,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(indexed.records.len(), 1);
        assert_eq!(
            indexed.records[0]
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.family_shell.as_ref())
                .and_then(|shell| shell.id.as_deref()),
            Some("m4kq")
        );
    }

    #[test]
    fn list_record_shape_projects_only_heavy_leaves() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260827110000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "projected",
                "cl_name": "cl_projected",
                "linked_repos": [
                    {"name": "core", "workspace_dir": "/tmp/core"}
                ]
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "projected",
                "cl_name": "cl_projected",
                "step_output": {
                    "_raw": "done raw",
                    "_data": {"body": "done data"},
                    "meta_commit_message": "keep",
                    "meta_commits": [{"sha": "abc123"}]
                }
            }),
        );
        write_json(
            &artifact_dir.join("workflow_state.json"),
            json!({
                "workflow_name": "projected",
                "status": "completed",
                "steps": [
                    {
                        "name": "build",
                        "status": "completed",
                        "output": {
                            "_raw": "workflow raw",
                            "_data": "workflow data",
                            "meta_workflow": "keep"
                        }
                    }
                ]
            }),
        );
        write_json(
            &artifact_dir.join("prompt_step_001.json"),
            json!({
                "workflow_name": "projected",
                "step_name": "build",
                "step_type": "exec",
                "status": "completed",
                "output": {
                    "_raw": "prompt raw",
                    "_data": "prompt data",
                    "meta_prompt": "keep"
                }
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let full = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: true,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Cached,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let list = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                record_shape: AgentArtifactRecordShapeWire::List,
                ..AgentArtifactIndexQueryWire::default()
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(full.records.len(), 1);
        assert_eq!(list.records.len(), 1);
        let full_record = &full.records[0];
        let list_record = &list.records[0];
        assert_eq!(
            full_record.record_shape,
            AgentArtifactRecordShapeWire::Full
        );
        assert_eq!(
            list_record.record_shape,
            AgentArtifactRecordShapeWire::List
        );
        assert!(serde_json::to_value(full_record)
            .unwrap()
            .get("record_shape")
            .is_none());
        assert_eq!(
            serde_json::to_value(list_record).unwrap()["record_shape"],
            json!("list")
        );

        let mut expected = full_record.clone();
        project_record_for_list(&mut expected);
        assert_eq!(list_record, &expected);
        assert_eq!(
            list_record.prompt_steps.len(),
            full_record.prompt_steps.len()
        );
        assert_eq!(
            list_record.workflow_state.as_ref().unwrap().steps.len(),
            full_record.workflow_state.as_ref().unwrap().steps.len()
        );
        assert_eq!(
            list_record
                .done
                .as_ref()
                .unwrap()
                .step_output
                .as_ref()
                .unwrap()
                .get("meta_commit_message"),
            Some(&json!("keep"))
        );
        assert!(list_record
            .done
            .as_ref()
            .unwrap()
            .step_output
            .as_ref()
            .unwrap()
            .get("_raw")
            .is_none());
        assert!(list_record
            .agent_meta
            .as_ref()
            .unwrap()
            .linked_repos
            .is_empty());

        let stored_json: String = Connection::open(&index)
            .unwrap()
            .query_row(
                "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        let stored_value: serde_json::Value =
            serde_json::from_str(&stored_json).unwrap();
        assert!(stored_value.get("record_shape").is_none());
        let stored_record: AgentArtifactRecordWire =
            serde_json::from_str(&stored_json).unwrap();
        assert_eq!(serde_json::to_string(&stored_record).unwrap(), stored_json);
    }

    #[test]
    fn load_agent_artifact_records_returns_full_records_for_dirs_and_aliases() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260827111500");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": "hydrate",
                "linked_repos": [{"name": "core"}]
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1779999999.0,
                "name": "hydrate",
                "step_output": {"_raw": "full body", "meta_key": "keep"}
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let alias = "/tmp/alias/hydrate";
        Connection::open(&index)
            .unwrap()
            .execute(
                "INSERT INTO agent_artifact_aliases(alias_path, artifact_dir) \
                 VALUES (?1, ?2)",
                params![alias, artifact_dir.to_string_lossy().as_ref()],
            )
            .unwrap();

        let projected = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(10),
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Cached,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::List,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(projected.records.len(), 1);
        assert!(projected.records[0]
            .done
            .as_ref()
            .unwrap()
            .step_output
            .as_ref()
            .unwrap()
            .get("_raw")
            .is_none());

        let loaded = load_agent_artifact_records(
            &index,
            &[
                artifact_dir.to_string_lossy().into_owned(),
                alias.to_string(),
                "/tmp/missing".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(loaded.len(), 2);
        for record in loaded {
            assert_eq!(record.record_shape, AgentArtifactRecordShapeWire::Full);
            assert_eq!(record.artifact_dir, artifact_dir.to_string_lossy());
            assert_eq!(
                record
                    .done
                    .as_ref()
                    .unwrap()
                    .step_output
                    .as_ref()
                    .unwrap()
                    .get("_raw"),
                Some(&json!("full body"))
            );
            assert_eq!(
                record.agent_meta.as_ref().unwrap().linked_repos.len(),
                1
            );
        }
    }

    #[test]
    fn refresh_stale_rows_signature_query_does_not_select_record_json() {
        let sql = refresh_stale_rows_sql("WHERE hidden = 1");
        assert!(sql.contains("agent_meta_sig"));
        assert!(sql.contains("prompt_steps_sig"));
        assert!(!sql.contains("record_json"));
    }

    #[test]
    fn output_variable_history_filters_groups_and_truncates() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let build_root =
            artifact_for_project(&projects, "proj", "20260814101010");
        let build_worker =
            artifact_for_project(&projects, "proj", "20260814111111");
        let deploy = artifact_for_project(&projects, "other", "20260814121212");
        write_json(
            &build_root.join("agent_meta.json"),
            json!({
                "name": "build",
                "cl_name": "proj",
                "output_variables": {
                    "status": "ok",
                    "count": 1,
                    "report": {"z": 2, "a": 1}
                }
            }),
        );
        write_json(
            &build_worker.join("agent_meta.json"),
            json!({
                "name": "build.worker",
                "hidden": true,
                "output_variables": {
                    "status": "ok",
                    "count": 1.0
                }
            }),
        );
        write_json(
            &deploy.join("agent_meta.json"),
            json!({
                "name": "deploy",
                "output_variables": {
                    "status": "failed",
                    "result": "Snowman ☃"
                }
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let status_history = query_agent_output_variable_history(
            &index,
            AgentOutputVariableHistoryQueryWire {
                keys: vec!["status".to_string()],
                value_limit: 1,
                ..AgentOutputVariableHistoryQueryWire::default()
            },
        )
        .unwrap();

        assert_eq!(status_history.schema_version, 1);
        assert_eq!(status_history.groups.len(), 1);
        assert_eq!(status_history.keys_limit.total_count, 1);
        let status_group = &status_history.groups[0];
        assert_eq!(status_group.key, "status");
        assert_eq!(status_group.occurrence_count, 2);
        assert_eq!(status_group.distinct_value_count, 2);
        assert!(status_group.values_limit.truncated);
        assert_eq!(status_group.values[0].value, json!("failed"));
        assert_eq!(status_group.values[0].agents, vec!["deploy"]);

        let oldest_first_status = query_agent_output_variable_history(
            &index,
            AgentOutputVariableHistoryQueryWire {
                keys: vec!["status".to_string()],
                reverse: true,
                value_limit: 0,
                ..AgentOutputVariableHistoryQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(oldest_first_status.groups[0].values[0].value, json!("ok"));

        let build_counts = query_agent_output_variable_history(
            &index,
            AgentOutputVariableHistoryQueryWire {
                agents: vec!["build.*".to_string()],
                keys: vec!["count".to_string()],
                include_hidden: true,
                value_limit: 0,
                ..AgentOutputVariableHistoryQueryWire::default()
            },
        )
        .unwrap();
        let count_values = &build_counts.groups[0].values;
        assert_eq!(build_counts.groups[0].occurrence_count, 2);
        assert_eq!(count_values.len(), 2);
        assert_eq!(count_values[0].value_json, "1.0");
        assert_eq!(count_values[0].agents, vec!["build.worker"]);
        assert_eq!(count_values[1].value_json, "1");
        assert_eq!(count_values[1].agents, vec!["build"]);

        let unicode_result = query_agent_output_variable_history(
            &index,
            AgentOutputVariableHistoryQueryWire {
                values: vec!["snowman".to_string()],
                projects: vec!["other".to_string()],
                since_timestamp: Some("20260814000000".to_string()),
                until_timestamp: Some("20260814235959".to_string()),
                ..AgentOutputVariableHistoryQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(unicode_result.groups.len(), 1);
        assert_eq!(unicode_result.groups[0].key, "result");
        assert_eq!(
            unicode_result.groups[0].values[0].value,
            json!("Snowman ☃")
        );
    }

    #[test]
    fn output_variable_projection_backfills_replaces_and_deletes_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260814131313");
        let meta_path = artifact_dir.join("agent_meta.json");
        write_json(
            &meta_path,
            json!({
                "name": "writer",
                "output_variables": {
                    "status": "old",
                    "drop_me": true
                }
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_output_variables_rows,
            2
        );

        write_json(
            &meta_path,
            json!({
                "name": "writer",
                "output_variables": {
                    "status": "new"
                }
            }),
        );
        upsert_agent_artifact_index_row(
            &index,
            &projects,
            &artifact_dir,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let refreshed = query_agent_output_variable_history(
            &index,
            AgentOutputVariableHistoryQueryWire {
                value_limit: 0,
                key_limit: 0,
                ..AgentOutputVariableHistoryQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(refreshed.groups.len(), 1);
        assert_eq!(refreshed.groups[0].key, "status");
        assert_eq!(refreshed.groups[0].values[0].value, json!("new"));

        {
            let conn = Connection::open(&index).unwrap();
            conn.execute("DELETE FROM agent_output_variables", [])
                .unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '20')",
                [],
            )
            .unwrap();
        }
        let backfilled = agent_artifact_index_status(&index).unwrap();
        assert_eq!(
            backfilled.schema_version,
            AGENT_ARTIFACT_INDEX_SCHEMA_VERSION
        );
        assert_eq!(backfilled.agent_output_variables_rows, 1);

        delete_agent_artifact_index_row(&index, &artifact_dir).unwrap();
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_output_variables_rows,
            0
        );
    }

    fn write_text(path: &Path, body: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }

    fn count_sql(index: &Path, sql: &str) -> i64 {
        Connection::open(index)
            .unwrap()
            .query_row(sql, [], |row| row.get(0))
            .unwrap()
    }

    fn alias_query(aliases: &[&str]) -> AgentAliasHistoryQueryWire {
        AgentAliasHistoryQueryWire {
            aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
            limit_per_alias: 10,
            include_hidden: false,
            projects: Vec::new(),
            prompt_snippet_bytes: 240,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
        }
    }

    #[test]
    fn alias_history_preserves_request_order_and_empty_groups() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let older = artifact(&projects, "20260816010101");
        let newer = artifact(&projects, "20260816020202");
        write_json(
            &older.join("agent_meta.json"),
            json!({
                "name": "older",
                "model_alias": "coder",
                "model_alias_trail": ["coder", "large"],
                "model_alias_origin": "directive"
            }),
        );
        write_json(
            &newer.join("agent_meta.json"),
            json!({
                "name": "newer",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "default_model"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let history = query_agent_alias_history(
            &index,
            alias_query(&["missing", "large", "coder"]),
        )
        .unwrap();
        assert_eq!(history.schema_version, 1);
        assert_eq!(history.groups.len(), 3);
        assert_eq!(history.groups[0].alias, "missing");
        assert!(history.groups[0].runs.is_empty());
        assert_eq!(history.groups[0].runs_limit.total_count, 0);
        assert_eq!(history.groups[1].alias, "large");
        assert_eq!(history.groups[1].runs.len(), 2);
        assert_eq!(
            history.groups[1].runs[0].agent_name.as_deref(),
            Some("newer")
        );
        assert_eq!(
            history.groups[1].runs[1].agent_name.as_deref(),
            Some("older")
        );
        assert_eq!(history.groups[1].runs[1].alias_position, 1);
        assert_eq!(history.groups[2].alias, "coder");
        assert_eq!(history.groups[2].runs.len(), 1);
        assert_eq!(history.groups[2].runs[0].alias_position, 0);
    }

    #[test]
    fn alias_history_truncates_newest_first_and_reports_counts() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for (ts, name) in [
            ("20260816010101", "one"),
            ("20260816020202", "two"),
            ("20260816030303", "three"),
        ] {
            let dir = artifact(&projects, ts);
            write_json(
                &dir.join("agent_meta.json"),
                json!({
                    "name": name,
                    "model_alias": "large",
                    "model_alias_trail": ["large"],
                    "model_alias_origin": "directive"
                }),
            );
        }
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let mut query = alias_query(&["large"]);
        query.limit_per_alias = 2;
        let history = query_agent_alias_history(&index, query).unwrap();
        let group = &history.groups[0];
        assert_eq!(group.runs_limit.limit, 2);
        assert_eq!(group.runs_limit.total_count, 3);
        assert_eq!(group.runs_limit.returned_count, 2);
        assert!(group.runs_limit.truncated);
        assert_eq!(group.runs[0].agent_name.as_deref(), Some("three"));
        assert_eq!(group.runs[1].agent_name.as_deref(), Some("two"));

        let mut unlimited = alias_query(&["large"]);
        unlimited.limit_per_alias = 0;
        let all = query_agent_alias_history(&index, unlimited).unwrap();
        assert_eq!(all.groups[0].runs.len(), 3);
        assert!(!all.groups[0].runs_limit.truncated);
    }

    #[test]
    fn alias_history_filters_hidden_and_project_keys() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let visible = artifact_for_project(&projects, "proj", "20260816040404");
        let hidden = artifact_for_project(&projects, "proj", "20260816050505");
        let other = artifact_for_project(&projects, "other", "20260816060606");
        write_json(
            &visible.join("agent_meta.json"),
            json!({
                "name": "visible",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        write_json(
            &hidden.join("agent_meta.json"),
            json!({
                "name": "hidden",
                "hidden": true,
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        write_json(
            &other.join("agent_meta.json"),
            json!({
                "name": "other",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let default =
            query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
        assert_eq!(default.groups[0].runs.len(), 2);

        let mut hidden_query = alias_query(&["large"]);
        hidden_query.include_hidden = true;
        hidden_query.projects = vec!["proj".to_string()];
        let filtered = query_agent_alias_history(&index, hidden_query).unwrap();
        assert_eq!(filtered.groups[0].runs.len(), 2);
        assert!(filtered.groups[0]
            .runs
            .iter()
            .all(|run| run.project_name == "proj"));
        assert!(filtered.groups[0]
            .runs
            .iter()
            .any(|run| run.agent_name.as_deref() == Some("hidden")));
    }

    #[test]
    fn alias_history_falls_back_to_legacy_first_hop() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260816070707");
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": "legacy",
                "model_alias": "large"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let history =
            query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
        assert_eq!(history.groups[0].runs.len(), 1);
        assert_eq!(history.groups[0].runs[0].alias_position, 0);
        assert_eq!(
            history.groups[0].runs[0].model_alias.as_deref(),
            Some("large")
        );
        assert_eq!(
            history.groups[0].runs[0].model_alias_trail,
            vec!["large".to_string()]
        );
        assert_eq!(history.groups[0].runs[0].model_alias_origin, None);
    }

    #[test]
    fn alias_history_projection_replaces_and_deletes_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260816080808");
        let meta_path = dir.join("agent_meta.json");
        write_json(
            &meta_path,
            json!({
                "name": "writer",
                "model_alias": "coder",
                "model_alias_trail": ["coder", "large"],
                "model_alias_origin": "directive"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_artifact_model_aliases_rows,
            2
        );

        write_json(
            &meta_path,
            json!({
                "name": "writer",
                "model_alias": "medium",
                "model_alias_trail": ["medium"],
                "model_alias_origin": "default_model"
            }),
        );
        upsert_agent_artifact_index_row(
            &index,
            &projects,
            &dir,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let after_replace = query_agent_alias_history(
            &index,
            alias_query(&["coder", "medium", "large"]),
        )
        .unwrap();
        assert!(after_replace.groups[0].runs.is_empty());
        assert_eq!(after_replace.groups[1].runs.len(), 1);
        assert!(after_replace.groups[2].runs.is_empty());
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_artifact_model_aliases_rows,
            1
        );

        delete_agent_artifact_index_row(&index, &dir).unwrap();
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_artifact_model_aliases_rows,
            0
        );
    }

    #[test]
    fn schema_v21_upgrade_backfills_model_alias_projection() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260816090909");
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": "legacy",
                "model_alias": "large",
                "model_alias_origin": "directive",
                "model_alias_trail": ["coder", "large"]
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute("DELETE FROM agent_artifact_model_aliases", [])
                .unwrap();
            conn.execute(
                "INSERT INTO agent_artifacts (
                    artifact_dir, projects_root, project_name, project_dir,
                    project_file, workflow_dir_name, timestamp, status,
                    agent_type, has_done_marker, has_running_marker,
                    has_waiting_marker, has_workflow_state, hidden,
                    record_json
                ) VALUES (
                    'malformed', 'root', 'proj', 'dir', 'file', 'ace-run',
                    '20260816000000', 'done', 'agent', 1, 0, 0, 0, 0,
                    '{not-json'
                )",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) \
                 VALUES ('schema_version', '21')",
                [],
            )
            .unwrap();
        }

        let status = agent_artifact_index_status(&index).unwrap();
        assert_eq!(status.schema_version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION);
        assert_eq!(status.agent_artifact_model_aliases_rows, 2);
        let history =
            query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
        assert_eq!(history.groups[0].runs.len(), 1);
        assert_eq!(history.groups[0].runs[0].alias_position, 1);
    }

    #[test]
    fn alias_history_prompt_snippets_strip_collapse_and_truncate() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let readable = artifact(&projects, "20260816101010");
        let directives = artifact(&projects, "20260816111111");
        let missing = artifact(&projects, "20260816121212");
        write_json(
            &readable.join("agent_meta.json"),
            json!({
                "name": "readable",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        write_text(
            &readable.join("raw_xprompt.md"),
            "%model:@large\n#gh:sase\n\nRefactor   the\nworkspace ☃ module\n",
        );
        write_json(
            &directives.join("agent_meta.json"),
            json!({
                "name": "directives",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        write_text(
            &directives.join("raw_xprompt.md"),
            "%model:@large\n#gh:sase\n\n",
        );
        write_json(
            &missing.join("agent_meta.json"),
            json!({
                "name": "missing",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let history =
            query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
        let by_name: BTreeMap<_, _> = history.groups[0]
            .runs
            .iter()
            .map(|run| (run.agent_name.clone().unwrap(), run.clone()))
            .collect();
        assert_eq!(
            by_name["readable"].prompt_snippet.as_deref(),
            Some("Refactor the workspace ☃ module")
        );
        assert_eq!(by_name["directives"].prompt_snippet.as_deref(), Some(""));
        assert_eq!(by_name["missing"].prompt_snippet, None);

        let mut short = alias_query(&["large"]);
        short.prompt_snippet_bytes = 12;
        let truncated = query_agent_alias_history(&index, short).unwrap();
        let readable_snip = truncated.groups[0]
            .runs
            .iter()
            .find(|run| run.agent_name.as_deref() == Some("readable"))
            .unwrap()
            .prompt_snippet
            .as_deref()
            .unwrap();
        assert!(readable_snip.ends_with("..."));
        assert!(readable_snip.is_char_boundary(readable_snip.len()));
        assert!(readable_snip.len() <= 12);
        assert!(!readable_snip.contains("☃") || readable_snip.ends_with("..."));

        let mut skipped = alias_query(&["large"]);
        skipped.prompt_snippet_bytes = 0;
        let no_reads = query_agent_alias_history(&index, skipped).unwrap();
        assert!(no_reads.groups[0]
            .runs
            .iter()
            .all(|run| run.prompt_snippet.is_none()));
    }

    #[test]
    fn alias_history_rejects_empty_aliases() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        drop(open_index(&index).unwrap());
        let err =
            query_agent_alias_history(&index, alias_query(&[])).unwrap_err();
        assert!(err.contains("non-empty"), "{err}");
    }

    #[test]
    fn alias_history_revalidate_refreshes_candidate_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260816131313");
        let meta_path = dir.join("agent_meta.json");
        write_json(
            &meta_path,
            json!({
                "name": "before",
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
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
            &meta_path,
            json!({
                "name": "after",
                "model_alias": "coder",
                "model_alias_trail": ["coder", "large"],
                "model_alias_origin": "directive"
            }),
        );
        let cached =
            query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
        assert_eq!(
            cached.groups[0].runs[0].agent_name.as_deref(),
            Some("before")
        );
        assert_eq!(cached.groups[0].runs[0].alias_position, 0);

        let mut revalidate = alias_query(&["large"]);
        revalidate.freshness = AgentArtifactIndexFreshnessWire::Revalidate;
        let fresh = query_agent_alias_history(&index, revalidate).unwrap();
        assert_eq!(
            fresh.groups[0].runs[0].agent_name.as_deref(),
            Some("after")
        );
        assert_eq!(fresh.groups[0].runs[0].alias_position, 1);
        assert_eq!(
            fresh.groups[0].runs[0].model_alias_trail,
            vec!["coder".to_string(), "large".to_string()]
        );
    }

    #[test]
    fn prompt_snippet_truncation_stays_on_utf8_char_boundary() {
        let truncated = truncate_prompt_snippet("ab☃cd", 5);
        assert!(truncated.ends_with("..."));
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.len() <= 5);
        assert_eq!(truncated, "ab...");
    }

    #[test]
    fn alias_history_status_counts_projection_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260816141414");
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": "counted",
                "model_alias": "coder",
                "model_alias_trail": ["coder", "large"],
                "model_alias_origin": "directive"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let status = agent_artifact_index_status(&index).unwrap();
        assert_eq!(status.schema_version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION);
        assert_eq!(status.agent_artifact_model_aliases_rows, 2);
    }

    #[test]
    fn read_only_open_falls_back_when_index_missing() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        assert!(!index.exists());

        let conn = open_index_read_only(&index).unwrap();
        assert_eq!(
            read_index_schema_version(&conn).unwrap(),
            AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        );
    }

    #[test]
    fn read_only_open_cannot_write() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        drop(open_index(&index).unwrap());

        let conn = open_index_read_only(&index).unwrap();
        let result = conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('probe', 'x')",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn status_reports_freelist_and_file_size() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260827090000");
        write_json(&dir.join("agent_meta.json"), json!({"name": "sized"}));
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let status = agent_artifact_index_status(&index).unwrap();
        assert!(status.file_size_bytes > 0);
        assert_eq!(
            status.file_size_bytes,
            std::fs::metadata(&index).unwrap().len(),
        );
    }

    #[test]
    fn vacuum_reclaims_freelist_pages_and_preserves_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let index = tmp.path().join("agent_artifact_index.sqlite");
        let padding = "x".repeat(4096);
        let mut dirs = Vec::new();
        for n in 0..50 {
            let dir = artifact(&projects, &format!("202608270900{n:02}"));
            write_json(
                &dir.join("agent_meta.json"),
                json!({"name": format!("agent-{n}-{padding}")}),
            );
            dirs.push(dir);
        }
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        // Remove all but one artifact dir from disk, then rebuild so the
        // index reconciles away the missing rows, leaving real freelist
        // pages behind for VACUUM to reclaim.
        for dir in &dirs[1..] {
            fs::remove_dir_all(dir).unwrap();
        }
        let reconciled = rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(reconciled.rows_indexed, 1);

        let before = agent_artifact_index_status(&index).unwrap();
        assert!(before.freelist_pages > 0);

        let update = vacuum_agent_artifact_index(&index).unwrap();

        assert_eq!(update.freelist_pages_before, before.freelist_pages);
        assert_eq!(update.freelist_pages_after, 0);
        assert!(update.file_size_bytes_after < update.file_size_bytes_before);
        assert_eq!(
            update.bytes_reclaimed,
            update.file_size_bytes_before - update.file_size_bytes_after,
        );

        let after = agent_artifact_index_status(&index).unwrap();
        assert_eq!(after.freelist_pages, 0);
        // VACUUM never removes or alters surviving rows.
        assert_eq!(after.agent_artifacts_rows, before.agent_artifacts_rows);
        assert_eq!(after.agent_artifacts_rows, 1);
    }

    #[test]
    fn late_xprompts_file_refreshes_cached_record() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260729121000");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "late-xprompt-user"}),
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
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(initial.records.len(), 1);
        assert!(initial.records[0].used_xprompts.is_empty());

        write_json(
            &artifact_dir.join("xprompts.json"),
            json!([
                {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
                {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
                {"name": "research_swarm", "kind": "swarm", "tags": ["research"]}
            ]),
        );

        let refreshed = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire::default(),
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(refreshed.records.len(), 1);
        assert_eq!(refreshed.records[0].used_xprompts.len(), 2);
        assert_eq!(refreshed.records[0].used_xprompts[0].name, "gh");
        assert_eq!(refreshed.records[0].used_xprompts[0].references, 2);
        assert_eq!(
            refreshed.records[0].used_xprompts[1].name,
            "research_swarm"
        );
        assert_eq!(refreshed.records[0].used_xprompts[1].kind, "swarm");
        assert_eq!(refreshed.records[0].used_xprompts[1].references, 1);

        let conn = Connection::open(&index).unwrap();
        let (signature, record_json): (Option<String>, String) = conn
            .query_row(
                "SELECT xprompts_sig, record_json FROM agent_artifacts \
                 WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(signature.is_some());
        let stored: AgentArtifactRecordWire =
            serde_json::from_str(&record_json).unwrap();
        assert_eq!(stored.used_xprompts, refreshed.records[0].used_xprompts);
    }

    #[test]
    fn cached_query_returns_rebuilt_records_without_revalidation() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let first = artifact(&projects, "20260729122000");
        let second = artifact(&projects, "20260729122100");
        write_json(
            &first.join("agent_meta.json"),
            json!({"name": "active", "pid": 123, "model": "gpt"}),
        );
        write_json(
            &second.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1816856460.0,
                "name": "done",
                "cl_name": "cl_alpha"
            }),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let cached = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_full_history: true,
                recent_completed_limit: None,
                include_hidden: true,
                freshness: AgentArtifactIndexFreshnessWire::Cached,
                ..AgentArtifactIndexQueryWire::default()
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let source = scan_agent_artifacts(
            &projects,
            AgentArtifactScanOptionsWire::default(),
        );
        assert_eq!(cached.records, source.records);
    }

    #[test]
    fn cached_query_does_not_refresh_stale_marker_rows() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260729122500");
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": "late-xprompt-user"}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        write_json(
            &artifact_dir.join("xprompts.json"),
            json!([{"name": "gh", "kind": "workflow", "tags": ["vcs"]}]),
        );

        let cached = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                freshness: AgentArtifactIndexFreshnessWire::Cached,
                ..AgentArtifactIndexQueryWire::default()
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(cached.records.len(), 1);
        assert!(cached.records[0].used_xprompts.is_empty());

        let conn = Connection::open(&index).unwrap();
        let (signature, record_json): (Option<String>, String) = conn
            .query_row(
                "SELECT xprompts_sig, record_json FROM agent_artifacts \
                 WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(signature.is_none());
        let stored: AgentArtifactRecordWire =
            serde_json::from_str(&record_json).unwrap();
        assert!(stored.used_xprompts.is_empty());
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
    fn hidden_terminal_retention_bounds_rebuild_and_preserves_anchors() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for index in 0..4_700 {
            let dir = artifact(&projects, &format!("20260817{index:06}"));
            write_json(
                &dir.join("agent_meta.json"),
                json!({"name": format!("hidden-{index}"), "hidden": true}),
            );
            write_json(
                &dir.join("done.json"),
                json!({"outcome": "completed", "hidden": true}),
            );
        }

        let pruned_dir = artifact(&projects, "20260817000000");
        write_json(
            &pruned_dir.join("agent_meta.json"),
            json!({
                "name": "old-projected",
                "hidden": true,
                "output_variables": {"status": "old"},
                "model_alias": "large",
                "model_alias_trail": ["large"]
            }),
        );
        write_json(
            &pruned_dir.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );

        let visible = artifact(&projects, "20200101000000");
        write_json(
            &visible.join("done.json"),
            json!({"outcome": "completed", "hidden": false}),
        );
        let active_hidden = artifact(&projects, "20200101000001");
        write_json(
            &active_hidden.join("agent_meta.json"),
            json!({"name": "active-hidden", "hidden": true}),
        );
        let parent = artifact(&projects, "20200101000002");
        write_json(
            &parent.join("agent_meta.json"),
            json!({"name": "hidden-parent", "hidden": true}),
        );
        write_json(
            &parent.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );
        let visible_child = artifact(&projects, "20260918000000");
        write_json(
            &visible_child.join("agent_meta.json"),
            json!({
                "name": "visible-child",
                "parent_timestamp": "20200101000002"
            }),
        );
        write_json(
            &visible_child.join("done.json"),
            json!({"outcome": "completed", "hidden": false}),
        );
        let hidden_lineage = artifact(&projects, "20200101000003");
        write_json(
            &hidden_lineage.join("agent_meta.json"),
            json!({
                "name": "hidden-lineage",
                "hidden": true,
                "parent_timestamp": "20200101000000"
            }),
        );
        write_json(
            &hidden_lineage.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );
        let clan_source = artifact(&projects, "20200101000004");
        write_json(
            &clan_source.join("agent_meta.json"),
            json!({
                "name": "clan-source",
                "hidden": true,
                "agent_clan": "ship",
                "agent_clan_generation": "gen-1",
                "clan_tribe": "release"
            }),
        );
        write_json(
            &clan_source.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        let update = rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert!(update.hidden_terminal_rows_pruned >= 600);
        assert!(update.hidden_terminal_rows_retained >= 4_096);
        let status = agent_artifact_index_status(&index).unwrap();
        assert_eq!(status.hidden_terminal_rows_prunable, 0);
        assert_eq!(
            count_sql(
                &index,
                "SELECT COUNT(*) FROM agent_artifacts \
                 WHERE artifact_dir LIKE '%20260817000000'"
            ),
            0
        );
        for timestamp in [
            "20200101000000",
            "20200101000001",
            "20200101000002",
            "20200101000003",
            "20200101000004",
        ] {
            assert_eq!(
                count_sql(
                    &index,
                    &format!(
                        "SELECT COUNT(*) FROM agent_artifacts \
                         WHERE timestamp = '{timestamp}'"
                    ),
                ),
                1,
                "{timestamp}"
            );
        }
        assert_eq!(
            count_sql(
                &index,
                "SELECT COUNT(*) FROM agent_output_variables \
                 WHERE artifact_dir LIKE '%20260817000000'"
            ),
            0
        );
        assert_eq!(
            count_sql(
                &index,
                "SELECT COUNT(*) FROM agent_artifact_model_aliases \
                 WHERE artifact_dir LIKE '%20260817000000'"
            ),
            0
        );

        let related =
            query_related_agent_artifact_dirs(&index, &visible_child, &[])
                .unwrap();
        assert!(related
            .iter()
            .any(|path| path == parent.to_string_lossy().as_ref()));
    }

    #[test]
    fn hidden_terminal_retention_prunes_dependents_and_is_idempotent() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let dir = artifact(&projects, "20260818000000");
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": "hidden",
                "hidden": true,
                "output_variables": {"status": "old"},
                "model_alias": "large",
                "model_alias_trail": ["large"]
            }),
        );
        write_json(
            &dir.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        upsert_agent_artifact_index_row(
            &index,
            &projects,
            &dir,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute(
                "INSERT INTO agent_artifact_aliases(alias_path, artifact_dir) \
                 VALUES (?1, ?2)",
                params![
                    "/legacy/artifacts/20260818000000",
                    dir.to_string_lossy().as_ref(),
                ],
            )
            .unwrap();
        }

        let pruned =
            prune_hidden_terminal_agent_artifact_index_rows(&index, Some(0))
                .unwrap();
        assert_eq!(pruned.hidden_terminal_rows_pruned, 1);
        assert_eq!(pruned.hidden_terminal_rows_retained, 0);
        assert_eq!(
            agent_artifact_index_status(&index)
                .unwrap()
                .agent_artifacts_rows,
            0
        );
        assert_eq!(
            count_sql(&index, "SELECT COUNT(*) FROM agent_artifact_aliases"),
            0
        );
        assert_eq!(
            count_sql(&index, "SELECT COUNT(*) FROM agent_output_variables"),
            0
        );
        assert_eq!(
            count_sql(
                &index,
                "SELECT COUNT(*) FROM agent_artifact_model_aliases"
            ),
            0
        );

        let second =
            prune_hidden_terminal_agent_artifact_index_rows(&index, Some(0))
                .unwrap();
        assert_eq!(second.hidden_terminal_rows_pruned, 0);
        assert_eq!(second.hidden_terminal_rows_retained, 0);
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                "freshness": "cached",
            }))
            .unwrap();

        assert_eq!(query.active_limit, Some(7));
        assert_eq!(query.freshness, AgentArtifactIndexFreshnessWire::Cached);
        let payload = serde_json::to_value(&query).unwrap();
        assert_eq!(payload["active_limit"], json!(7));
        assert_eq!(payload["freshness"], json!("cached"));

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
        assert_eq!(
            legacy.freshness,
            AgentArtifactIndexFreshnessWire::Revalidate
        );
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(all.records.len(), 1);
        assert_eq!(all.records[0].timestamp, "20260514123000");
    }

    #[test]
    fn bounded_query_retains_dismissed_clan_declaration_as_context() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let declarer =
            artifact_for_project(&projects, "declarations", "20260701000000");
        write_json(
            &declarer.join("agent_meta.json"),
            json!({
                "name": "toobig-0.declarer",
                "cl_name": "cl_declarer",
                "agent_clan": "toobig-0",
                "agent_clan_generation": "generation-1",
                "clan_tribe": "chop",
                "clan_summary": "Chop generation"
            }),
        );
        write_json(
            &declarer.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782864000.0,
                "name": "toobig-0.declarer",
                "cl_name": "cl_declarer"
            }),
        );

        for (timestamp, name) in [
            ("20260701000001", "toobig-0.joiner-a"),
            ("20260701000002", "toobig-0.joiner-b"),
        ] {
            let joiner = artifact(&projects, timestamp);
            write_json(
                &joiner.join("agent_meta.json"),
                json!({
                    "name": name,
                    "agent_clan": "toobig-0",
                    "agent_clan_generation": "generation-1"
                }),
            );
            write_json(
                &joiner.join("waiting.json"),
                json!({"waiting_for": ["predecessor"]}),
            );
        }
        for (offset, timestamp) in
            ["20260702000000", "20260703000000", "20260704000000"]
                .into_iter()
                .enumerate()
        {
            let completed = artifact(&projects, timestamp);
            write_json(
                &completed.join("done.json"),
                json!({
                    "outcome": "completed",
                    "finished_at": 1782950400.0 + offset as f64,
                    "name": format!("recent-{offset}"),
                    "cl_name": format!("cl_recent_{offset}")
                }),
            );
        }

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
                cl_name: "cl_declarer".to_string(),
                raw_suffix: Some("20260701000000".to_string()),
            }],
        )
        .unwrap();

        let bounded = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: true,
                include_recent_completed: true,
                include_full_history: false,
                active_limit: None,
                recent_completed_limit: Some(1),
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire {
                only_projects: vec!["proj".to_string()],
                ..AgentArtifactScanOptionsWire::default()
            },
        )
        .unwrap();
        assert_eq!(bounded.records.len(), 3);
        assert!(!bounded
            .records
            .iter()
            .any(|record| record.timestamp == "20260701000000"));
        let context = bounded
            .clan_context
            .iter()
            .find(|context| context.agent_clan == "toobig-0")
            .unwrap();
        assert_eq!(
            context.agent_clan_generation.as_deref(),
            Some("generation-1")
        );
        assert_eq!(context.clan_tribe.as_deref(), Some("chop"));
        assert_eq!(context.clan_summary.as_deref(), Some("Chop generation"));
        assert_eq!(
            context.clan_tribe_source_launch_timestamp.as_deref(),
            Some("20260701000000")
        );

        let visible_history = query_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactIndexQueryWire {
                include_active: false,
                include_recent_completed: false,
                include_full_history: true,
                active_limit: None,
                recent_completed_limit: None,
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert!(!visible_history
            .records
            .iter()
            .any(|record| record.timestamp == "20260701000000"));
        assert_eq!(
            visible_history.clan_context[0].clan_tribe.as_deref(),
            Some("chop")
        );
    }

    #[test]
    fn indexed_clan_context_honors_latest_declarations_and_generations() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        for (timestamp, generation, tribe, summary) in [
            ("20260710000000", "g1", "alpha", Some("g1 summary")),
            ("20260710000001", "g1", "beta", None),
            ("20260710000002", "g2", "other", Some("g2 summary")),
        ] {
            let declaration = artifact(&projects, timestamp);
            write_json(
                &declaration.join("agent_meta.json"),
                json!({
                    "name": format!("declaration-{timestamp}"),
                    "agent_clan": "shared",
                    "agent_clan_generation": generation,
                    "clan_tribe": tribe,
                    "clan_summary": summary
                }),
            );
            write_json(
                &declaration.join("done.json"),
                json!({"outcome": "completed", "name": "declaration"}),
            );
        }
        for (timestamp, generation) in [
            ("20260710000003", "g1"),
            ("20260710000004", "g2"),
            ("20260710000005", "g3"),
        ] {
            let joiner = artifact(&projects, timestamp);
            write_json(
                &joiner.join("agent_meta.json"),
                json!({
                    "name": format!("joiner-{generation}"),
                    "agent_clan": "shared",
                    "agent_clan_generation": generation
                }),
            );
            write_json(&joiner.join("waiting.json"), json!({}));
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
                recent_completed_limit: None,
                include_hidden: false,
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
            },
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(snapshot.records.len(), 3);
        let context = |generation: &str| {
            snapshot
                .clan_context
                .iter()
                .find(|context| {
                    context.agent_clan_generation.as_deref() == Some(generation)
                })
                .unwrap()
        };
        assert_eq!(context("g1").clan_tribe.as_deref(), Some("beta"));
        assert_eq!(context("g1").clan_summary.as_deref(), Some("g1 summary"));
        assert_eq!(context("g2").clan_tribe.as_deref(), Some("other"));
        assert_eq!(context("g2").clan_summary.as_deref(), Some("g2 summary"));
        assert_eq!(context("g3").clan_tribe, None);
        assert_eq!(context("g3").clan_summary, None);
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
    fn schema_v18_upgrade_adds_xprompts_signature_column() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        drop(open_index(&index).unwrap());
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute_batch(
                "ALTER TABLE agent_artifacts DROP COLUMN xprompts_sig;
                 INSERT OR REPLACE INTO meta(key, value)
                 VALUES ('schema_version', '18');",
            )
            .unwrap();
        }

        drop(open_index(&index).unwrap());

        let conn = Connection::open(&index).unwrap();
        let columns = {
            let mut stmt =
                conn.prepare("PRAGMA table_info(agent_artifacts)").unwrap();
            let rows =
                stmt.query_map([], |row| row.get::<_, String>(1)).unwrap();
            rows.collect::<Result<Vec<_>, _>>().unwrap()
        };
        assert!(columns.iter().any(|column| column == "xprompts_sig"));
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
    fn schema_v19_upgrade_refreshes_record_json_for_model_aliases() {
        let tmp = tempdir().unwrap();
        let index = tmp.path().join("agent_artifact_index.sqlite");
        drop(open_index(&index).unwrap());
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value)
                 VALUES ('schema_version', '19')",
                [],
            )
            .unwrap();
        }

        drop(open_index(&index).unwrap());

        let conn = Connection::open(&index).unwrap();
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
    fn schema_v24_upgrade_backfills_done_outcome_projection() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let artifact_dir = artifact(&projects, "20260827113000");
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "abandoned",
                "finished_at": 1779999999.0,
                "name": "abandoned"
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        {
            let conn = Connection::open(&index).unwrap();
            conn.execute_batch(
                "DROP INDEX IF EXISTS idx_agent_artifacts_done_outcome;
                 ALTER TABLE agent_artifacts DROP COLUMN done_outcome;
                 INSERT OR REPLACE INTO meta(key, value)
                 VALUES ('schema_version', '23');",
            )
            .unwrap();
        }

        drop(open_index(&index).unwrap());

        let conn = Connection::open(&index).unwrap();
        let outcome: Option<String> = conn
            .query_row(
                "SELECT done_outcome FROM agent_artifacts \
                 WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(outcome.as_deref(), Some("abandoned"));
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
        assert!(hidden_completed.records[0]
            .done
            .as_ref()
            .is_some_and(|done| done.finished_at.is_some()));
        assert!(hidden_completed.records[0]
            .done
            .as_ref()
            .is_some_and(|done| done.finished_at_estimated));
    }

    #[test]
    fn abandoned_terminalization_prefers_stopped_at_then_directory_mtime() {
        let record = |stopped_at: &str| {
            serde_json::from_value::<AgentArtifactRecordWire>(json!({
                "project_name": "proj",
                "project_dir": "/tmp/proj",
                "project_file": "/tmp/proj/proj.sase",
                "workflow_dir_name": "ace-run",
                "artifact_dir": "/tmp/proj/artifacts/ace-run/record",
                "timestamp": "record",
                "agent_meta": {
                    "name": "abandoned",
                    "stopped_at": stopped_at
                }
            }))
            .unwrap()
        };
        let latest = UNIX_EPOCH + Duration::from_secs(999);

        let stopped =
            terminalized_abandoned_record(record("123.5"), Some(latest));
        let done = stopped.done.as_ref().unwrap();
        assert_eq!(done.finished_at, Some(123.5));
        assert!(done.finished_at_estimated);

        let fallback =
            terminalized_abandoned_record(record("not-a-time"), Some(latest));
        let done = fallback.done.as_ref().unwrap();
        assert_eq!(done.finished_at, Some(999.0));
        assert!(done.finished_at_estimated);
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
                freshness: AgentArtifactIndexFreshnessWire::Revalidate,
                only_monitors: false,
                record_shape: AgentArtifactRecordShapeWire::Full,
                window_limit: None,
                candidate_filter: None,
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
