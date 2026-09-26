use super::alias_history::effective_model_alias_trail;
use super::index_wire::{
    AgentArtifactIndexUpdateWire, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
    DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
};
use super::output_variables::{
    canonical_output_variable_json, output_variable_scalar_text,
};
use super::placeholders;
use super::record_summary::{MarkerSignatures, RecordSummary};
use super::refresh::{
    select_terminalization_candidates, terminalize_stale_candidate,
    TerminalizationOutcome,
};
use super::storage::{
    open_index, open_index_for_rebuild, open_index_with_busy_timeout,
    resolve_index_artifact_dir, DEFAULT_INDEX_BUSY_TIMEOUT,
    GATE_TURN_INDEX_COLUMN,
};
use crate::agent_scan::scanner::{
    scan_agent_artifact_dir, scan_agent_artifacts,
};
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};
use rusqlite::{params, params_from_iter, Connection};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

pub(super) const ABANDONED_DONE_OUTCOME: &str = "abandoned";

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
pub(super) struct HiddenTerminalRetentionPlan {
    pub(super) retained_rows: u64,
    pub(super) prunable_rows: u64,
    pub(super) pruned_dirs: Vec<String>,
}

impl HiddenTerminalRetentionPlan {
    pub(super) fn pruned_rows(&self) -> u64 {
        self.pruned_dirs.len() as u64
    }
}

pub(super) fn enforce_hidden_terminal_retention(
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

pub(super) fn plan_hidden_terminal_retention(
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
pub(super) struct HiddenTerminalRow {
    pub(super) artifact_dir: String,
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) timestamp: String,
    pub(super) parent_timestamp: Option<String>,
    pub(super) retry_of_timestamp: Option<String>,
    pub(super) retried_as_timestamp: Option<String>,
    pub(super) retry_chain_root_timestamp: Option<String>,
    pub(super) clan_tribe: Option<String>,
    pub(super) clan_summary: Option<String>,
}

impl HiddenTerminalRow {
    pub(super) fn is_context_anchor(
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

    pub(super) fn has_lineage_pointer(&self) -> bool {
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

    pub(super) fn has_clan_context(&self) -> bool {
        [self.clan_tribe.as_deref(), self.clan_summary.as_deref()]
            .into_iter()
            .flatten()
            .any(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct LineageReferenceKey {
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) timestamp: String,
}

pub(super) fn select_hidden_terminal_rows(
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

pub(super) fn select_lineage_reference_keys(
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

pub(super) fn delete_agent_artifact_projection_rows(
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

pub(super) fn repair_abandoned_agent_artifact_index_rows(
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
                decode_agent_artifact_record_json(&record_json)
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

pub(super) fn upsert_record(
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
        &format!(
            r#"
        INSERT INTO agent_artifacts (
            artifact_dir, projects_root, project_name, project_dir, project_file,
            workflow_dir_name, workflow_name, agent_clan, agent_session, timestamp,
            status, agent_type, cl_name,
            agent_name, model, llm_provider, started_at, finished_at,
            has_done_marker, has_running_marker, has_waiting_marker,
            has_workflow_state, workflow_status, hidden, parent_timestamp,
            step_index, step_name, retry_of_timestamp, retried_as_timestamp,
            retry_chain_root_timestamp, retry_attempt, agent_meta_sig, done_sig,
            running_sig, waiting_sig, pending_question_sig,
            workflow_state_sig, plan_path_sig, prompt_steps_sig, xprompts_sig,
            agent_clan_generation, clan_tribe, clan_summary, record_json,
            model_alias_origin, done_outcome, source_machine,
            imported_owner_machine, {GATE_TURN_INDEX_COLUMN}, indexed_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48, ?49, CURRENT_TIMESTAMP
        )
        ON CONFLICT(artifact_dir) DO UPDATE SET
            projects_root = excluded.projects_root,
            project_name = excluded.project_name,
            project_dir = excluded.project_dir,
            project_file = excluded.project_file,
            workflow_dir_name = excluded.workflow_dir_name,
            workflow_name = excluded.workflow_name,
            agent_clan = excluded.agent_clan,
            agent_session = excluded.agent_session,
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
            source_machine = excluded.source_machine,
            imported_owner_machine = excluded.imported_owner_machine,
            {GATE_TURN_INDEX_COLUMN} = excluded.{GATE_TURN_INDEX_COLUMN},
            indexed_at = CURRENT_TIMESTAMP
        "#,
        ),
        params![
            record.artifact_dir,
            projects_root.to_string_lossy().as_ref(),
            record.project_name,
            record.project_dir,
            record.project_file,
            record.workflow_dir_name,
            summary.workflow_name,
            summary.agent_clan,
            summary.agent_session,
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
            summary.source_machine,
            summary.imported_owner_machine,
            summary.gate_turn_id,
        ],
    )
    .map_err(|e| e.to_string())?;
    upsert_output_variables_for_record(conn, projects_root, record)?;
    upsert_model_aliases_for_record(conn, projects_root, record)?;
    Ok(())
}

pub(super) fn upsert_model_aliases_for_record(
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

pub(super) fn upsert_output_variables_for_record(
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
