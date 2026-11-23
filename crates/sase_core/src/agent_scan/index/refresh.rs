use super::index_wire::AgentArtifactIndexQueryWire;
use super::maintenance::{
    delete_agent_artifact_projection_rows, upsert_record,
};
use super::placeholders;
use super::record_summary::MarkerSignatures;
use super::selection::{
    add_project_filter_to_where, pending_refresh_row_from_sql,
    PendingRefreshRow,
};
use crate::agent_scan::scanner::{
    list_agent_artifact_dirs, project_filter_for_scan, scan_agent_artifact_dir,
};
use crate::agent_scan::wire::{
    is_supported_workflow_dir, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
};
use chrono::{TimeZone, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const SOURCE_RECONCILE_ROOT_META_KEY: &str =
    "source_reconcile_projects_root";
pub(super) const SOURCE_RECONCILE_OK_META_KEY: &str = "source_reconcile_ok";

pub(super) fn discovery_scan_options(
    options: &AgentArtifactScanOptionsWire,
) -> AgentArtifactScanOptionsWire {
    let mut discovery = options.clone();
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
    // Every full-history Revalidate pass used to rewrite these rows
    // unconditionally, committing a write transaction on each pass even
    // when nothing changed. Write only the keys whose value would
    // change so an unchanged pass commits nothing.
    let root_value = projects_root_key(projects_root);
    let stored_root: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [SOURCE_RECONCILE_ROOT_META_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if stored_root.as_deref() != Some(root_value.as_str()) {
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
            params![SOURCE_RECONCILE_ROOT_META_KEY, root_value],
        )
        .map_err(|e| e.to_string())?;
    }
    let stored_ok: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [SOURCE_RECONCILE_OK_META_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if stored_ok.as_deref() != Some("1") {
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
            params![SOURCE_RECONCILE_OK_META_KEY, "1"],
        )
        .map_err(|e| e.to_string())?;
    }
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
    project_filter: Option<&BTreeSet<String>>,
    workflow_scope: &[String],
) -> Result<Vec<(String, String, String)>, String> {
    if project_filter.is_some_and(BTreeSet::is_empty) {
        return Ok(Vec::new());
    }
    let mut clauses = vec!["projects_root = ?".to_string()];
    let mut values = vec![projects_root_key(projects_root)];
    if let Some(projects) = project_filter {
        let project_placeholders = placeholders(projects.len());
        clauses.push(format!("project_name IN ({project_placeholders})"));
        values.extend(projects.iter().cloned());
    }
    if !workflow_scope.is_empty() {
        let workflow_placeholders = placeholders(workflow_scope.len());
        clauses.push(format!("workflow_dir_name IN ({workflow_placeholders})"));
        values.extend(workflow_scope.iter().cloned());
    }
    let sql = format!(
        "SELECT artifact_dir, indexed_at, workflow_dir_name \
         FROM agent_artifacts WHERE {}",
        clauses.join(" AND ")
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push((
            row.get(0).map_err(|e| e.to_string())?,
            row.get(1).map_err(|e| e.to_string())?,
            row.get(2).map_err(|e| e.to_string())?,
        ));
    }
    Ok(result)
}

/// Whether an indexed row's workflow directory was covered by a
/// discovery walk built from `discovery_options`.
///
/// `discovery_scan_options` keeps project and workflow scope. A scoped walk
/// (the stranded query's one project and `("ace-run",)`, for example)
/// visits only that scope, so rows outside it must stay out of the missing
/// diff and dirty-row pass.
fn workflow_in_reconcile_scope(
    workflow_dir_name: &str,
    discovery_options: &AgentArtifactScanOptionsWire,
) -> bool {
    if discovery_options.only_workflow_dirs.is_empty() {
        is_supported_workflow_dir(workflow_dir_name)
    } else {
        discovery_options
            .only_workflow_dirs
            .iter()
            .any(|name| name == workflow_dir_name)
    }
}

pub(super) fn reconcile_source_directories(
    conn: &Connection,
    projects_root: &Path,
    options: &AgentArtifactScanOptionsWire,
    stats: &mut AgentArtifactScanStatsWire,
) -> Result<(), String> {
    let discovery_options = discovery_scan_options(options);
    let project_filter =
        project_filter_for_scan(projects_root, &discovery_options);
    let source_dirs: BTreeSet<String> =
        list_agent_artifact_dirs(projects_root, &discovery_options)
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
    let indexed_rows = indexed_artifact_rows(
        conn,
        projects_root,
        project_filter.as_ref(),
        &discovery_options.only_workflow_dirs,
    )?;
    let indexed_dirs: BTreeSet<String> =
        indexed_rows.iter().map(|(dir, _, _)| dir.clone()).collect();

    // Diff only rows inside the walked workflow scope. The discovery walk
    // keeps `only_workflow_dirs` (see `discovery_scan_options`), so a
    // scoped query must not delete rows it never visited.
    let scoped_indexed_dirs: BTreeSet<String> = indexed_rows
        .iter()
        .filter(|(_, _, workflow_dir_name)| {
            workflow_in_reconcile_scope(workflow_dir_name, &discovery_options)
        })
        .map(|(dir, _, _)| dir.clone())
        .collect();
    let missing: BTreeSet<String> = scoped_indexed_dirs
        .difference(&source_dirs)
        .cloned()
        .collect();
    if !missing.is_empty() {
        stats.rows_removed += missing.len() as u64;
    }

    // Scan phase: all filesystem and signature work happens outside any
    // transaction. The write phase below applies the collected repairs
    // and discoveries inside one transaction per pass.
    let mut repairs: Vec<(PathBuf, AgentArtifactRecordWire)> = Vec::new();
    let mut dirty_dirs = Vec::new();
    for (artifact_dir, indexed_at, workflow_dir_name) in &indexed_rows {
        if !workflow_in_reconcile_scope(workflow_dir_name, &discovery_options) {
            continue;
        }
        if missing.contains(artifact_dir) {
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
                repairs.push((row_projects_root, refreshed));
                stats.rows_repaired += 1;
            }
        }
    }

    let mut discoveries: Vec<AgentArtifactRecordWire> = Vec::new();
    for artifact_dir in source_dirs.difference(&indexed_dirs) {
        let path = PathBuf::from(artifact_dir);
        if let Some(record) =
            scan_agent_artifact_dir(projects_root, &path, options)
        {
            discoveries.push(record);
            stats.rows_discovered += 1;
        }
    }

    // Write phase: one transaction per pass. Repairs stay best-effort
    // (a failed repair must not abort the batch), while a failed
    // discovery upsert still propagates with `?` as before.
    if missing.is_empty() && repairs.is_empty() && discoveries.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    if !missing.is_empty() {
        let missing_dirs: Vec<String> = missing.iter().cloned().collect();
        delete_agent_artifact_projection_rows(&tx, &missing_dirs)?;
    }
    for (row_projects_root, refreshed) in &repairs {
        let _ = upsert_record(&tx, row_projects_root, refreshed);
    }
    for record in &discoveries {
        upsert_record(&tx, projects_root, record)?;
    }
    tx.commit().map_err(|e| e.to_string())?;
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

    // Scan phase: all filesystem and signature work happens outside
    // any transaction. The write phase below applies the collected
    // repairs and removals inside one transaction per pass.
    let mut repairs: Vec<(PathBuf, AgentArtifactRecordWire)> = Vec::new();
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
            repairs.push((projects_root, refreshed));
            stats.rows_repaired += 1;
        } else {
            missing.push(row.artifact_dir);
        }
    }
    if repairs.is_empty() && missing.is_empty() {
        return Ok(());
    }
    // Write phase: one transaction per pass. Repairs stay best-effort
    // (a failed repair must not abort the batch), as before.
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    for (projects_root, refreshed) in &repairs {
        let _ = upsert_record(&tx, projects_root, refreshed);
    }
    if !missing.is_empty() {
        stats.rows_removed += missing.len() as u64;
        delete_agent_artifact_projection_rows(&tx, &missing)?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) fn refresh_stale_rows_sql(where_sql: &str) -> String {
    format!(
        "SELECT artifact_dir, projects_root, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig, macros_sig FROM agent_artifacts {where_sql}"
    )
}
