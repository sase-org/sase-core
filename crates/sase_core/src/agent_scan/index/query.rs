use super::candidates::{
    select_windowed_records, should_use_windowed_candidate_query,
};
use super::index_wire::{
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
    AgentArtifactIndexStatusWire, AgentArtifactIndexVacuumWire,
    DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
};
use super::maintenance::plan_hidden_terminal_retention;
use super::output_variables::{
    select_clan_context, select_clan_context_for_keys,
};
use super::refresh::{
    reconcile_source_directories, repair_stale_rows_for_query,
    source_reconcile_watermark_valid, stamp_source_reconcile_watermark,
};
use super::selection::{
    active_where, completed_where, projection_clan_keys_sink, select_records,
    visible_where, RecordSelection, SelectRecordsQuery,
};
use super::storage::{
    count_table_rows, open_index, open_index_read_only,
    read_index_schema_version, resolve_index_artifact_dir,
};
use super::{placeholders, record_gate_shell_lookup_records_decoded};
use crate::agent_scan::context::represented_clan_keys;
use crate::agent_scan::scanner::project_filter_for_scan;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactIndexCompletenessWire,
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AGENT_SCAN_WIRE_SCHEMA_VERSION,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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
                decode_agent_artifact_record_json(&record_json)
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

/// Return the newest real gate-shell member matching `gate_id`, if any.
///
/// Uses the indexed `gate_shell_id` column for a single-row `WHERE` lookup
/// instead of decoding every historical record, the cost that made the
/// previous full-history scan take seconds on a long-lived host. Only rows
/// projected from a genuine gate-shell member carry a `gate_shell_id`
/// (see [`gate_shell_id_from_record`]), so a later descendant that merely
/// inherited the gate id can never shadow the owning shell here.
///
/// `project_name` of `None` searches every project, the same unscoped
/// sweep the historical Python lookup performed for the reclaim chop.
/// Ties (which should not occur for a durable gate id, but are possible
/// for a replayed/duplicated bundle) resolve to the newest row by
/// `timestamp`, then `artifact_dir`, mirroring the prior newest-first sort.
pub fn find_gate_shell_by_gate_id(
    index_path: &Path,
    project_name: Option<&str>,
    gate_id: &str,
) -> Result<Option<AgentArtifactRecordWire>, String> {
    let conn = open_index_read_only(index_path)?;
    let record_json: Option<String> = match project_name {
        Some(project) => conn
            .query_row(
                "SELECT record_json FROM agent_artifacts \
                 WHERE gate_shell_id = ?1 AND project_name = ?2 \
                 ORDER BY timestamp DESC, artifact_dir DESC LIMIT 1",
                params![gate_id, project],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?,
        None => conn
            .query_row(
                "SELECT record_json FROM agent_artifacts \
                 WHERE gate_shell_id = ?1 \
                 ORDER BY timestamp DESC, artifact_dir DESC LIMIT 1",
                params![gate_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?,
    };
    record_gate_shell_lookup_records_decoded(record_json.is_some() as u64);
    record_json
        .map(|json| decode_agent_artifact_record_json(&json))
        .transpose()
}

/// Return `(page_count, freelist_count, page_size)` for *conn*.
pub(super) fn index_page_stats(
    conn: &Connection,
) -> Result<(u64, u64, u64), String> {
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
    // Revalidate may write repaired or newly discovered rows; Cached never
    // writes, so it can use the cheaper read-only open.
    let conn = if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate
    {
        open_index(index_path)?
    } else {
        open_index_read_only(index_path)?
    };
    let mut stats = AgentArtifactScanStatsWire::default();
    let mut by_dir: BTreeMap<String, AgentArtifactRecordWire> = BTreeMap::new();
    let project_filter = project_filter_for_scan(projects_root, &options);
    let mut source_reconciled = false;
    if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate {
        if query.include_full_history {
            reconcile_source_directories(
                &conn,
                projects_root,
                &options,
                &mut stats,
            )?;
            stamp_source_reconcile_watermark(&conn, projects_root)?;
            source_reconciled = true;
        }
        repair_stale_rows_for_query(
            &conn,
            &query,
            &options,
            project_filter.as_ref(),
            &mut stats,
        )?;
    } else if query.include_full_history {
        source_reconciled =
            source_reconcile_watermark_valid(&conn, projects_root)?;
    }

    let mut projection_clan_keys = BTreeSet::new();
    let index_window = if should_use_windowed_candidate_query(&query) {
        Some(select_windowed_records(
            &conn,
            &query,
            &mut stats,
            &mut by_dir,
            project_filter.as_ref(),
            projection_clan_keys_sink(&query, &mut projection_clan_keys),
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
                    candidate_filter: query.candidate_filter.clone(),
                    agents_list_projection: query.agents_list_projection,
                },
                &mut stats,
                &mut by_dir,
                &options,
                project_filter.as_ref(),
                projection_clan_keys_sink(&query, &mut projection_clan_keys),
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
                    candidate_filter: query.candidate_filter.clone(),
                    agents_list_projection: query.agents_list_projection,
                },
                &mut stats,
                &mut by_dir,
                &options,
                project_filter.as_ref(),
                projection_clan_keys_sink(&query, &mut projection_clan_keys),
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
                    candidate_filter: query.candidate_filter.clone(),
                    agents_list_projection: query.agents_list_projection,
                },
                &mut stats,
                &mut by_dir,
                &options,
                project_filter.as_ref(),
                projection_clan_keys_sink(&query, &mut projection_clan_keys),
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
    let mut clan_context = if query.agents_list_projection {
        projection_clan_keys.extend(represented_clan_keys(&records));
        select_clan_context_for_keys(&conn, projection_clan_keys)?
    } else {
        select_clan_context(&conn, &records)?
    };
    if let Some(records_dir) = options
        .clan_records_dir
        .as_deref()
        .map(str::trim)
        .filter(|dir| !dir.is_empty())
    {
        crate::agent_clan_record::apply_clan_records_to_context(
            Path::new(records_dir),
            &mut clan_context,
        );
    }
    let index_completeness = Some(AgentArtifactIndexCompletenessWire {
        complete_history: query.include_full_history && source_reconciled,
        source_reconciled,
        rows_discovered: stats.rows_discovered,
        rows_removed: stats.rows_removed,
        marker_signatures_checked: stats.marker_signatures_checked,
        rows_repaired: stats.rows_repaired,
        record_json_decoded: stats.record_json_decoded,
    });

    Ok(AgentArtifactScanWire {
        schema_version: AGENT_SCAN_WIRE_SCHEMA_VERSION,
        projects_root: projects_root.to_string_lossy().into_owned(),
        options,
        stats,
        index_window,
        records,
        clan_context,
        index_completeness,
    })
}

pub(super) fn project_record_for_list(record: &mut AgentArtifactRecordWire) {
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

pub(super) fn project_output_for_list(
    output: &mut serde_json::Map<String, serde_json::Value>,
) {
    output.remove("_raw");
    output.remove("_data");
}
