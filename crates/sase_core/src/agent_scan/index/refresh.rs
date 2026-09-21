use super::index_wire::AgentArtifactIndexQueryWire;
use super::maintenance::{
    delete_agent_artifact_projection_rows, upsert_record,
    ABANDONED_DONE_OUTCOME,
};
use super::placeholders;
use super::record_summary::{MarkerSignatures, RecordSummary};
use super::selection::{
    add_project_filter_to_where, pending_refresh_row_from_sql,
    pending_row_from_sql, PendingRefreshRow, PendingRow,
};
use crate::agent_launch::list_workspace_claims_from_content;
use crate::agent_runtime::parse_runtime_timestamp;
use crate::agent_scan::scanner::{
    list_agent_artifact_dirs, scan_agent_artifact_dir,
};
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire, DoneMarkerWire,
};
use chrono::{TimeZone, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const SOURCE_RECONCILE_ROOT_META_KEY: &str =
    "source_reconcile_projects_root";
pub(super) const SOURCE_RECONCILE_OK_META_KEY: &str = "source_reconcile_ok";

pub(super) fn discovery_scan_options(
    options: &AgentArtifactScanOptionsWire,
) -> AgentArtifactScanOptionsWire {
    let mut discovery = options.clone();
    discovery.only_projects.clear();
    discovery.max_records = None;
    discovery.not_before_timestamp = None;
    discovery.newest_first = false;
    discovery.capacity_only = false;
    discovery
}

pub(super) fn projects_root_key(projects_root: &Path) -> String {
    projects_root.to_string_lossy().into_owned()
}

pub(super) fn source_reconcile_watermark_valid(
    conn: &Connection,
    projects_root: &Path,
) -> Result<bool, String> {
    let ok: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [SOURCE_RECONCILE_OK_META_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if ok.as_deref() != Some("1") {
        return Ok(false);
    }
    let stored_root: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [SOURCE_RECONCILE_ROOT_META_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(stored_root.as_deref()
        == Some(projects_root_key(projects_root).as_str()))
}

pub(super) fn stamp_source_reconcile_watermark(
    conn: &Connection,
    projects_root: &Path,
) -> Result<(), String> {
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        params![
            SOURCE_RECONCILE_ROOT_META_KEY,
            projects_root_key(projects_root)
        ],
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        params![SOURCE_RECONCILE_OK_META_KEY, "1"],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) fn parse_indexed_at(raw: &str) -> Option<SystemTime> {
    let naive =
        chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S").ok()?;
    let datetime = Utc.from_utc_datetime(&naive);
    let secs = u64::try_from(datetime.timestamp()).ok()?;
    Some(UNIX_EPOCH + Duration::new(secs, datetime.timestamp_subsec_nanos()))
}

pub(super) fn artifact_dir_is_dirty(
    artifact_dir: &str,
    indexed_at: &str,
) -> bool {
    let Some(indexed_at) = parse_indexed_at(indexed_at) else {
        return true;
    };
    let Ok(mtime) = fs::metadata(artifact_dir).and_then(|meta| meta.modified())
    else {
        return true;
    };
    mtime > indexed_at
}

pub(super) fn indexed_artifact_rows(
    conn: &Connection,
    projects_root: &Path,
) -> Result<Vec<(String, String)>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT artifact_dir, indexed_at FROM agent_artifacts \
             WHERE projects_root = ?1",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query([projects_root_key(projects_root)])
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push((
            row.get(0).map_err(|e| e.to_string())?,
            row.get(1).map_err(|e| e.to_string())?,
        ));
    }
    Ok(result)
}

pub(super) fn reconcile_source_directories(
    conn: &Connection,
    projects_root: &Path,
    options: &AgentArtifactScanOptionsWire,
    stats: &mut AgentArtifactScanStatsWire,
) -> Result<(), String> {
    let discovery_options = discovery_scan_options(options);
    let source_dirs: BTreeSet<String> =
        list_agent_artifact_dirs(projects_root, &discovery_options)
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
    let indexed_rows = indexed_artifact_rows(conn, projects_root)?;
    let indexed_dirs: BTreeSet<String> =
        indexed_rows.iter().map(|(dir, _)| dir.clone()).collect();

    let missing: Vec<String> =
        indexed_dirs.difference(&source_dirs).cloned().collect();
    if !missing.is_empty() {
        stats.rows_removed += missing.len() as u64;
        delete_agent_artifact_projection_rows(conn, &missing)?;
    }

    let mut dirty_dirs = Vec::new();
    for (artifact_dir, indexed_at) in &indexed_rows {
        if missing.iter().any(|dir| dir == artifact_dir) {
            continue;
        }
        if artifact_dir_is_dirty(artifact_dir, indexed_at) {
            dirty_dirs.push(artifact_dir.clone());
        }
    }
    if !dirty_dirs.is_empty() {
        let placeholders = placeholders(dirty_dirs.len());
        let where_sql = format!("WHERE artifact_dir IN ({placeholders})");
        let pending = {
            let mut stmt = conn
                .prepare(&refresh_stale_rows_sql(&where_sql))
                .map_err(|e| e.to_string())?;
            let mut rows = stmt
                .query(params_from_iter(dirty_dirs.iter()))
                .map_err(|e| e.to_string())?;
            let mut pending = Vec::new();
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                pending.push(pending_refresh_row_from_sql(row)?);
            }
            pending
        };
        for row in pending {
            stats.marker_signatures_checked += 1;
            let current =
                MarkerSignatures::from_artifact_dir(&row.artifact_dir);
            if row.stored == current {
                continue;
            }
            let row_projects_root = PathBuf::from(&row.row_projects_root);
            let artifact_dir = PathBuf::from(&row.artifact_dir);
            if let Some(refreshed) = scan_agent_artifact_dir(
                &row_projects_root,
                &artifact_dir,
                options,
            ) {
                let _ = upsert_record(conn, &row_projects_root, &refreshed);
                stats.rows_repaired += 1;
            }
        }
    }

    for artifact_dir in source_dirs.difference(&indexed_dirs) {
        let path = PathBuf::from(artifact_dir);
        if let Some(record) =
            scan_agent_artifact_dir(projects_root, &path, options)
        {
            upsert_record(conn, projects_root, &record)?;
            stats.rows_discovered += 1;
        }
    }
    Ok(())
}

pub(super) fn repair_stale_rows_for_query(
    conn: &Connection,
    query: &AgentArtifactIndexQueryWire,
    options: &AgentArtifactScanOptionsWire,
    project_filter: Option<&BTreeSet<String>>,
    stats: &mut AgentArtifactScanStatsWire,
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
    refresh_stale_rows(conn, &where_sql, options, stats)
}

pub(super) fn select_terminalization_candidates(
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
pub(super) enum TerminalizationOutcome {
    Terminalized,
    Skipped,
}

pub(super) fn terminalize_stale_candidate(
    conn: &Connection,
    row: &PendingRow,
    options: &AgentArtifactScanOptionsWire,
    stale_after: Duration,
) -> Result<TerminalizationOutcome, String> {
    let current = MarkerSignatures::from_artifact_dir(&row.artifact_dir);
    let projects_root = PathBuf::from(&row.row_projects_root);
    let record = if row.stored == current {
        match decode_agent_artifact_record_json(&row.record_json) {
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

pub(super) fn record_is_terminalization_candidate(
    record: &AgentArtifactRecordWire,
) -> bool {
    !record.has_done_marker
        && record.done.is_none()
        && record.running.is_none()
        && record.waiting.is_none()
        && record.pending_question.is_none()
        && record.workflow_state.is_none()
}

pub(super) fn artifact_dir_is_stale(
    latest: SystemTime,
    stale_after: Duration,
) -> bool {
    SystemTime::now()
        .duration_since(latest)
        .map(|age| age >= stale_after)
        .unwrap_or(false)
}

pub(super) fn artifact_dir_latest_modified(
    artifact_dir: &str,
) -> Option<SystemTime> {
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

pub(super) fn max_system_time(
    left: Option<SystemTime>,
    right: Option<SystemTime>,
) -> Option<SystemTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn record_has_live_workspace_claim(
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

pub(super) fn record_workspace_num(
    record: &AgentArtifactRecordWire,
) -> Option<u32> {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .or_else(|| record.done.as_ref().and_then(|done| done.workspace_num))
        .and_then(|num| u32::try_from(num).ok())
}

pub(super) fn terminalized_abandoned_record(
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

pub(super) fn system_time_to_unix_timestamp_secs(
    value: SystemTime,
) -> Option<f64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

pub(super) fn refresh_stale_rows(
    conn: &Connection,
    where_sql: &str,
    options: &AgentArtifactScanOptionsWire,
    stats: &mut AgentArtifactScanStatsWire,
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

    let mut missing = Vec::new();
    for row in pending {
        stats.marker_signatures_checked += 1;
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
            stats.rows_repaired += 1;
        } else {
            missing.push(row.artifact_dir);
        }
    }
    if !missing.is_empty() {
        stats.rows_removed += missing.len() as u64;
        delete_agent_artifact_projection_rows(conn, &missing)?;
    }
    Ok(())
}

pub(super) fn refresh_stale_rows_sql(where_sql: &str) -> String {
    format!(
        "SELECT artifact_dir, projects_root, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig, xprompts_sig FROM agent_artifacts {where_sql}"
    )
}
