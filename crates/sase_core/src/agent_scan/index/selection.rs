use super::candidates::{
    candidate_filter_uses_machine, candidate_matches_query_filter,
    expand_machine_tree_relatives, push_machine_value, select_candidate_rows,
    select_candidate_rows_for_dirs,
};
use super::dismissal::record_is_dismissed;
use super::index_wire::{
    AgentArtifactCandidateFieldWire, AgentArtifactCandidateFilterWire,
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
};
use super::maintenance::{
    delete_agent_artifact_projection_rows, upsert_record,
};
use super::placeholders;
use super::record_summary::{MarkerSignatures, RecordSummary};
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_runtime::is_real_monitor_member_record;
use crate::agent_scan::context::ClanGenerationKey;
use crate::agent_scan::scanner::{
    project_allowed_by_filter, scan_agent_artifact_dir,
};
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
use rusqlite::{params_from_iter, Connection};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

pub(super) const TERMINAL_WORKFLOW_STATUSES: &[&str] =
    &["completed", "failed", "cancelled", "noop"];

pub(super) fn select_records(
    conn: &Connection,
    query: SelectRecordsQuery,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
    options: &AgentArtifactScanOptionsWire,
    project_filter: Option<&BTreeSet<String>>,
    clan_keys: Option<&mut BTreeSet<ClanGenerationKey>>,
) -> Result<(), String> {
    let pending =
        if query.candidate_filter.is_some() || query.agents_list_projection {
            select_pending_rows_for_candidate_filter(
                conn, &query, by_dir, clan_keys,
            )?
        } else {
            select_pending_rows_for_query(conn, &query, by_dir)?
        };

    let mut missing = Vec::new();
    for row in pending {
        let record = match query.freshness {
            AgentArtifactIndexFreshnessWire::Cached => {
                match decode_agent_artifact_record_json(&row.record_json) {
                    Ok(record) => {
                        stats.record_json_decoded += 1;
                        record
                    }
                    Err(_) => {
                        stats.json_decode_errors += 1;
                        continue;
                    }
                }
            }
            AgentArtifactIndexFreshnessWire::Revalidate => {
                stats.marker_signatures_checked += 1;
                let current =
                    MarkerSignatures::from_artifact_dir(&row.artifact_dir);
                if row.stored == current {
                    match decode_agent_artifact_record_json(&row.record_json) {
                        Ok(record) => {
                            stats.record_json_decoded += 1;
                            record
                        }
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
                            stats.rows_repaired += 1;
                            refreshed
                        }
                        None => {
                            missing.push(row.artifact_dir.clone());
                            continue;
                        }
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
    if !missing.is_empty() {
        stats.rows_removed += missing.len() as u64;
        delete_agent_artifact_projection_rows(conn, &missing)?;
    }
    Ok(())
}

pub(super) fn select_pending_rows_for_query(
    conn: &Connection,
    query: &SelectRecordsQuery,
    by_dir: &BTreeMap<String, AgentArtifactRecordWire>,
) -> Result<Vec<PendingRow>, String> {
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

    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = if let Some(limit) = query.limit {
        stmt.query([limit]).map_err(|e| e.to_string())?
    } else {
        stmt.query([]).map_err(|e| e.to_string())?
    };

    let mut pending = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
        if by_dir.contains_key(&artifact_dir) {
            continue;
        }
        pending
            .push(pending_row_from_sql_with_artifact_dir(row, artifact_dir)?);
    }
    Ok(pending)
}

pub(super) fn select_pending_rows_for_candidate_filter(
    conn: &Connection,
    query: &SelectRecordsQuery,
    by_dir: &BTreeMap<String, AgentArtifactRecordWire>,
    mut clan_keys: Option<&mut BTreeSet<ClanGenerationKey>>,
) -> Result<Vec<PendingRow>, String> {
    if query.candidate_filter.is_none() && !query.agents_list_projection {
        return select_pending_rows_for_query(conn, query, by_dir);
    }
    let candidates = select_candidate_rows(
        conn,
        query.where_sql.clone(),
        CandidateSelection::from_record_selection(query.selection),
    )?;

    let mut artifact_dirs = Vec::new();
    for row in candidates {
        if by_dir.contains_key(&row.artifact_dir) {
            continue;
        }
        if !candidate_matches_query_filter(
            &row,
            query.candidate_filter.as_ref(),
        ) {
            continue;
        }
        collect_candidate_clan_key(&row, clan_keys.as_deref_mut());
        if !candidate_kept_for_hydration(&row, query.agents_list_projection) {
            continue;
        }
        artifact_dirs.push(row.artifact_dir);
        if query
            .limit
            .is_some_and(|limit| artifact_dirs.len() >= limit as usize)
        {
            break;
        }
    }
    if query
        .candidate_filter
        .as_ref()
        .is_some_and(candidate_filter_uses_machine)
    {
        let expanded = expand_machine_tree_relatives(conn, &artifact_dirs)?;
        let extras: Vec<String> = expanded
            .into_iter()
            .filter(|dir| !artifact_dirs.iter().any(|selected| selected == dir))
            .collect();
        let extra_rows = select_candidate_rows_for_dirs(
            conn,
            &extras,
            CandidateSelection::from_record_selection(query.selection),
        )?;
        for row in extra_rows {
            collect_candidate_clan_key(&row, clan_keys.as_deref_mut());
            if candidate_kept_for_hydration(&row, query.agents_list_projection)
            {
                artifact_dirs.push(row.artifact_dir);
            }
        }
    }

    select_pending_rows_by_artifact_dirs(conn, &artifact_dirs)
}

pub(super) fn select_pending_rows_by_artifact_dirs(
    conn: &Connection,
    artifact_dirs: &[String],
) -> Result<Vec<PendingRow>, String> {
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    if artifact_dirs.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows_by_dir = BTreeMap::new();
    for chunk in artifact_dirs.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT artifact_dir, projects_root, record_json, \
             agent_meta_sig, done_sig, running_sig, waiting_sig, \
             pending_question_sig, workflow_state_sig, plan_path_sig, \
             prompt_steps_sig, xprompts_sig \
             FROM agent_artifacts WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            rows_by_dir.insert(
                artifact_dir.clone(),
                pending_row_from_sql_with_artifact_dir(row, artifact_dir)?,
            );
        }
    }

    Ok(artifact_dirs
        .iter()
        .filter_map(|artifact_dir| rows_by_dir.remove(artifact_dir))
        .collect())
}

pub(super) fn pending_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> Result<PendingRow, String> {
    let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
    pending_row_from_sql_with_artifact_dir(row, artifact_dir)
}

pub(super) fn pending_row_from_sql_with_artifact_dir(
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

pub(super) fn pending_refresh_row_from_sql(
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
pub(super) enum RecordSelection {
    Active,
    Completed,
    Visible,
}

pub(super) struct SelectRecordsQuery {
    pub(super) where_sql: String,
    pub(super) limit: Option<u32>,
    pub(super) selection: RecordSelection,
    pub(super) include_hidden: bool,
    pub(super) freshness: AgentArtifactIndexFreshnessWire,
    pub(super) only_monitors: bool,
    pub(super) candidate_filter: Option<AgentArtifactCandidateFilterWire>,
    pub(super) agents_list_projection: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateSelection {
    Active,
    Completed,
    Visible,
}

impl CandidateSelection {
    pub(super) fn from_record_selection(selection: RecordSelection) -> Self {
        match selection {
            RecordSelection::Active => Self::Active,
            RecordSelection::Completed => Self::Completed,
            RecordSelection::Visible => Self::Visible,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct IndexedCandidateRow {
    pub(super) artifact_dir: String,
    pub(super) project_name: String,
    pub(super) agent_type: String,
    pub(super) cl_name: Option<String>,
    pub(super) model: Option<String>,
    pub(super) llm_provider: Option<String>,
    pub(super) source_machine: Option<String>,
    pub(super) imported_owner_machine: Option<String>,
    pub(super) workflow_dir_name: String,
    pub(super) has_done_marker: bool,
    pub(super) has_running_marker: bool,
    pub(super) has_workflow_state: bool,
    pub(super) agent_clan: Option<String>,
    pub(super) agent_clan_generation: Option<String>,
    pub(super) selection: CandidateSelection,
}

impl IndexedCandidateRow {
    pub(super) fn scalar_values(
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
            AgentArtifactCandidateFieldWire::Machine => {
                let mut values = Vec::new();
                push_machine_value(&mut values, "here");
                if let Some(machine) = self.source_machine.as_deref() {
                    push_machine_value(&mut values, machine);
                }
                if let Some(machine) = self.imported_owner_machine.as_deref() {
                    push_machine_value(&mut values, machine);
                }
                values
            }
            AgentArtifactCandidateFieldWire::Type => {
                vec![self.agent_type.as_str()]
            }
        }
    }
}

pub(super) fn projection_clan_keys_sink<'a>(
    query: &AgentArtifactIndexQueryWire,
    keys: &'a mut BTreeSet<ClanGenerationKey>,
) -> Option<&'a mut BTreeSet<ClanGenerationKey>> {
    query.agents_list_projection.then_some(keys)
}

pub(super) fn workflow_dir_supports_done_loader(name: &str) -> bool {
    DONE_WORKFLOW_DIR_NAMES.contains(&name)
        || DONE_WORKFLOW_DIR_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

pub(super) fn workflow_dir_supports_workflow_loader(name: &str) -> bool {
    WORKFLOW_STATE_DIR_NAMES.contains(&name)
        || WORKFLOW_STATE_DIR_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

pub(super) fn candidate_is_loader_projectable(
    row: &IndexedCandidateRow,
) -> bool {
    if row.has_done_marker
        && workflow_dir_supports_done_loader(&row.workflow_dir_name)
    {
        return true;
    }
    if row.has_running_marker
        && row.project_name == "home"
        && row.workflow_dir_name == "ace-run"
    {
        return true;
    }
    row.has_workflow_state
        && workflow_dir_supports_workflow_loader(&row.workflow_dir_name)
}

pub(super) fn candidate_kept_for_hydration(
    row: &IndexedCandidateRow,
    agents_list_projection: bool,
) -> bool {
    !agents_list_projection || candidate_is_loader_projectable(row)
}

pub(super) fn clan_key_from_candidate(
    row: &IndexedCandidateRow,
) -> Option<ClanGenerationKey> {
    let clan = row.agent_clan.as_deref()?.trim();
    if clan.is_empty() {
        return None;
    }
    let generation = row
        .agent_clan_generation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    Some((clan.to_string(), generation))
}

pub(super) fn collect_candidate_clan_key(
    row: &IndexedCandidateRow,
    keys: Option<&mut BTreeSet<ClanGenerationKey>>,
) {
    let Some(keys) = keys else {
        return;
    };
    if let Some(key) = clan_key_from_candidate(row) {
        keys.insert(key);
    }
}

pub(super) fn record_matches_selection(
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

pub(super) fn record_is_monitor(record: &AgentArtifactRecordWire) -> bool {
    is_real_monitor_member_record(record)
}

pub(super) fn record_is_active(record: &AgentArtifactRecordWire) -> bool {
    !record.has_done_marker
        || record.workflow_state.as_ref().is_some_and(|workflow| {
            !is_terminal_workflow_status(&workflow.status)
        })
}

pub(super) fn record_is_completed(record: &AgentArtifactRecordWire) -> bool {
    record.has_done_marker
        || record.workflow_state.as_ref().is_some_and(|workflow| {
            is_terminal_workflow_status(&workflow.status)
        })
}

pub(super) fn is_terminal_workflow_status(status: &str) -> bool {
    TERMINAL_WORKFLOW_STATUSES.contains(&status)
}

pub(super) fn record_is_definitively_dead_for_dismissal_backfill(
    record: &AgentArtifactRecordWire,
) -> bool {
    if record.waiting.is_some() || record.pending_question.is_some() {
        return false;
    }
    match record_liveness_for_dismissal_backfill(record) {
        DismissalBackfillLiveness::Alive
        | DismissalBackfillLiveness::Unknown => return false,
        DismissalBackfillLiveness::Dead
        | DismissalBackfillLiveness::NotProcess => return true,
        DismissalBackfillLiveness::NoCurrentProcessEvidence => {}
    }
    match record.workflow_state.as_ref() {
        Some(workflow) if is_terminal_workflow_status(&workflow.status) => true,
        _ => record.has_done_marker || record.done.is_some(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DismissalBackfillLiveness {
    Alive,
    Dead,
    NotProcess,
    Unknown,
    NoCurrentProcessEvidence,
}

pub(super) fn record_liveness_for_dismissal_backfill(
    record: &AgentArtifactRecordWire,
) -> DismissalBackfillLiveness {
    let pid = record
        .running
        .as_ref()
        .and_then(|running| running.pid)
        .or_else(|| record.agent_meta.as_ref().and_then(|meta| meta.pid))
        .or_else(|| {
            record
                .workflow_state
                .as_ref()
                .and_then(|workflow| workflow.pid)
        });
    let Some(pid) = pid else {
        if record.running.is_some()
            || record.workflow_state.as_ref().is_some_and(|workflow| {
                !is_terminal_workflow_status(&workflow.status)
            })
        {
            return DismissalBackfillLiveness::Unknown;
        }
        return DismissalBackfillLiveness::NoCurrentProcessEvidence;
    };
    if pid <= 0 {
        return DismissalBackfillLiveness::NotProcess;
    }
    if process_is_alive_for_dismissal_backfill(pid) {
        DismissalBackfillLiveness::Alive
    } else {
        DismissalBackfillLiveness::Dead
    }
}

pub(super) fn process_is_alive_for_dismissal_backfill(pid: i64) -> bool {
    #[cfg(unix)]
    {
        let pid = match libc::pid_t::try_from(pid) {
            Ok(pid) => pid,
            Err(_) => return false,
        };
        unsafe { libc::kill(pid, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

pub(super) fn dismissed_identity_for_record(
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
) -> AgentCleanupIdentityWire {
    AgentCleanupIdentityWire {
        agent_type: if summary.agent_type == "workflow" {
            "workflow".to_string()
        } else {
            "run".to_string()
        },
        cl_name: summary
            .cl_name
            .clone()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "unknown".to_string()),
        raw_suffix: Some(record.timestamp.clone()),
    }
}

pub(super) struct PendingRow {
    pub(super) artifact_dir: String,
    pub(super) row_projects_root: String,
    pub(super) record_json: String,
    pub(super) stored: MarkerSignatures,
}

pub(super) struct PendingRefreshRow {
    pub(super) artifact_dir: String,
    pub(super) row_projects_root: String,
    pub(super) stored: MarkerSignatures,
}

pub(super) fn active_where(
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

pub(super) fn active_priority_sql() -> &'static str {
    "(has_running_marker = 1
       OR has_waiting_marker = 1
       OR pending_question_sig IS NOT NULL
       OR (
           has_workflow_state = 1
           AND workflow_status NOT IN ('completed', 'failed', 'cancelled', 'noop')
       )) DESC"
}

pub(super) fn completed_where(
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

pub(super) fn completed_window_where(
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

pub(super) fn visible_where(
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

pub(super) fn add_project_filter_to_where(
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

pub(super) const DISMISSED_NORMAL_VISIBILITY_FILTER: &str = r#"NOT EXISTS (
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
