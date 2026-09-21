use super::index_wire::{
    AgentArtifactCandidateFieldWire, AgentArtifactCandidateFilterWire,
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
};
use super::placeholders;
use super::selection::{
    active_where, candidate_kept_for_hydration, collect_candidate_clan_key,
    completed_window_where, record_matches_selection, CandidateSelection,
    IndexedCandidateRow, RecordSelection,
};
use crate::agent_scan::context::ClanGenerationKey;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactIndexWindowWire,
    AgentArtifactRecordWire, AgentArtifactScanStatsWire,
};
use rusqlite::{params_from_iter, Connection};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const CANDIDATE_ROW_COLUMNS: &str =
    "artifact_dir, project_name, agent_type, \
     cl_name, model, llm_provider, source_machine, imported_owner_machine, \
     workflow_dir_name, has_done_marker, has_running_marker, \
     has_workflow_state, agent_clan, agent_clan_generation";

pub(super) fn indexed_candidate_row_from_sql(
    row: &rusqlite::Row<'_>,
    selection: CandidateSelection,
) -> Result<IndexedCandidateRow, String> {
    Ok(IndexedCandidateRow {
        artifact_dir: row.get(0).map_err(|e| e.to_string())?,
        project_name: row.get(1).map_err(|e| e.to_string())?,
        agent_type: row.get(2).map_err(|e| e.to_string())?,
        cl_name: row.get(3).map_err(|e| e.to_string())?,
        model: row.get(4).map_err(|e| e.to_string())?,
        llm_provider: row.get(5).map_err(|e| e.to_string())?,
        source_machine: row.get(6).map_err(|e| e.to_string())?,
        imported_owner_machine: row.get(7).map_err(|e| e.to_string())?,
        workflow_dir_name: row.get(8).map_err(|e| e.to_string())?,
        has_done_marker: row.get::<_, i64>(9).map_err(|e| e.to_string())? != 0,
        has_running_marker: row.get::<_, i64>(10).map_err(|e| e.to_string())?
            != 0,
        has_workflow_state: row.get::<_, i64>(11).map_err(|e| e.to_string())?
            != 0,
        agent_clan: row.get(12).map_err(|e| e.to_string())?,
        agent_clan_generation: row.get(13).map_err(|e| e.to_string())?,
        selection,
    })
}

pub(super) fn should_use_windowed_candidate_query(
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

/// Select every matching active candidate plus a newest-first completed
/// prefix of `window_limit` candidates.
///
/// The window bounds the completed tier. `active_limit` on the query
/// bounds the unwindowed active-tier path. Active rows are never
/// truncated to satisfy `window_limit`. `has_more` and `truncated` mean
/// completed candidates were truncated.
pub(super) fn select_windowed_records(
    conn: &Connection,
    query: &AgentArtifactIndexQueryWire,
    stats: &mut AgentArtifactScanStatsWire,
    by_dir: &mut BTreeMap<String, AgentArtifactRecordWire>,
    project_filter: Option<&BTreeSet<String>>,
    mut clan_keys: Option<&mut BTreeSet<ClanGenerationKey>>,
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
        active_dirs.insert(row.artifact_dir.clone());
        active_candidates.push(row);
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
        if !candidate_matches_query_filter(
            &row,
            query.candidate_filter.as_ref(),
        ) {
            continue;
        }
        if !candidate_kept_for_hydration(&row, query.agents_list_projection) {
            continue;
        }
        completed_candidates.push(row);
    }

    let completed_budget = requested_limit as usize;
    let mut selected = active_candidates.clone();
    selected
        .extend(completed_candidates.iter().take(completed_budget).cloned());
    for row in selected.iter().skip(active_candidates.len()) {
        collect_candidate_clan_key(row, clan_keys.as_deref_mut());
    }
    if query
        .candidate_filter
        .as_ref()
        .is_some_and(candidate_filter_uses_machine)
    {
        let selected_dirs: Vec<String> = selected
            .iter()
            .map(|row| row.artifact_dir.clone())
            .collect();
        let expanded_dirs =
            expand_machine_tree_relatives(conn, &selected_dirs)?;
        let extras: Vec<String> = expanded_dirs
            .into_iter()
            .filter(|dir| !selected.iter().any(|row| &row.artifact_dir == dir))
            .collect();
        let extra_rows = select_candidate_rows_for_dirs(
            conn,
            &extras,
            CandidateSelection::Visible,
        )?;
        for row in extra_rows {
            collect_candidate_clan_key(&row, clan_keys.as_deref_mut());
            if candidate_kept_for_hydration(&row, query.agents_list_projection)
            {
                selected.push(row);
            }
        }
    }
    let selected_candidate_count = selected.len() as u64;
    let has_more = completed_candidates.len() > completed_budget;
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

pub(super) fn select_candidate_rows(
    conn: &Connection,
    where_sql: String,
    selection: CandidateSelection,
) -> Result<Vec<IndexedCandidateRow>, String> {
    let sql = format!(
        "SELECT {CANDIDATE_ROW_COLUMNS} FROM agent_artifacts {where_sql}"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(indexed_candidate_row_from_sql(row, selection)?);
    }
    Ok(result)
}

pub(super) fn select_candidate_rows_for_dirs(
    conn: &Connection,
    artifact_dirs: &[String],
    selection: CandidateSelection,
) -> Result<Vec<IndexedCandidateRow>, String> {
    if artifact_dirs.is_empty() {
        return Ok(Vec::new());
    }
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    let mut result = Vec::new();
    for chunk in artifact_dirs.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT {CANDIDATE_ROW_COLUMNS} FROM agent_artifacts \
             WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            result.push(indexed_candidate_row_from_sql(row, selection)?);
        }
    }
    Ok(result)
}

pub(super) fn expand_machine_tree_relatives(
    conn: &Connection,
    selected_dirs: &[String],
) -> Result<Vec<String>, String> {
    let mut dirs: BTreeSet<String> = selected_dirs.iter().cloned().collect();
    if dirs.is_empty() {
        return Ok(Vec::new());
    }
    for _ in 0..8 {
        let keys = select_candidate_tree_keys(
            conn,
            &dirs.iter().cloned().collect::<Vec<_>>(),
        )?;
        let extra = select_related_tree_dirs(conn, &keys)?;
        let before = dirs.len();
        dirs.extend(extra);
        if dirs.len() == before {
            break;
        }
    }
    let mut ordered = Vec::with_capacity(dirs.len());
    let mut seen = BTreeSet::new();
    for dir in selected_dirs {
        if seen.insert(dir.clone()) {
            ordered.push(dir.clone());
        }
    }
    for dir in dirs {
        if seen.insert(dir.clone()) {
            ordered.push(dir);
        }
    }
    Ok(ordered)
}

#[derive(Debug, Clone)]
pub(super) struct CandidateTreeKeys {
    pub(super) timestamp: String,
    pub(super) agent_family: Option<String>,
    pub(super) agent_clan: Option<String>,
    pub(super) parent_timestamp: Option<String>,
}

pub(super) fn select_candidate_tree_keys(
    conn: &Connection,
    artifact_dirs: &[String],
) -> Result<Vec<CandidateTreeKeys>, String> {
    if artifact_dirs.is_empty() {
        return Ok(Vec::new());
    }
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    let mut result = Vec::new();
    for chunk in artifact_dirs.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT timestamp, agent_family, agent_clan, parent_timestamp \
             FROM agent_artifacts WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            result.push(CandidateTreeKeys {
                timestamp: row.get(0).map_err(|e| e.to_string())?,
                agent_family: row.get(1).map_err(|e| e.to_string())?,
                agent_clan: row.get(2).map_err(|e| e.to_string())?,
                parent_timestamp: row.get(3).map_err(|e| e.to_string())?,
            });
        }
    }
    Ok(result)
}

pub(super) fn select_related_tree_dirs(
    conn: &Connection,
    keys: &[CandidateTreeKeys],
) -> Result<Vec<String>, String> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut families = BTreeSet::new();
    let mut clans = BTreeSet::new();
    let mut timestamps = BTreeSet::new();
    for key in keys {
        if let Some(family) = key
            .agent_family
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            families.insert(family.to_string());
        }
        if let Some(clan) =
            key.agent_clan.as_deref().filter(|value| !value.is_empty())
        {
            clans.insert(clan.to_string());
        }
        timestamps.insert(key.timestamp.clone());
        if let Some(parent) = key
            .parent_timestamp
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            timestamps.insert(parent.to_string());
        }
    }

    let mut dirs = BTreeSet::new();
    if !families.is_empty() {
        dirs.extend(select_dirs_in_column(
            conn,
            "agent_family",
            &families.into_iter().collect::<Vec<_>>(),
        )?);
    }
    if !clans.is_empty() {
        dirs.extend(select_dirs_in_column(
            conn,
            "agent_clan",
            &clans.into_iter().collect::<Vec<_>>(),
        )?);
    }
    if !timestamps.is_empty() {
        let stamps: Vec<String> = timestamps.into_iter().collect();
        dirs.extend(select_dirs_in_column(conn, "timestamp", &stamps)?);
        dirs.extend(select_dirs_in_column(conn, "parent_timestamp", &stamps)?);
    }
    Ok(dirs.into_iter().collect())
}

pub(super) fn select_dirs_in_column(
    conn: &Connection,
    column: &'static str,
    values: &[String],
) -> Result<Vec<String>, String> {
    match column {
        "agent_family" | "agent_clan" | "timestamp" | "parent_timestamp" => {}
        _ => return Err(format!("unsupported tree column {column}")),
    }
    if values.is_empty() {
        return Ok(Vec::new());
    }
    const LOAD_RECORDS_BATCH_SIZE: usize = 500;
    let mut dirs = Vec::new();
    for chunk in values.chunks(LOAD_RECORDS_BATCH_SIZE) {
        let placeholders = placeholders(chunk.len());
        let sql = format!(
            "SELECT artifact_dir FROM agent_artifacts WHERE {column} IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            dirs.push(row.get(0).map_err(|e| e.to_string())?);
        }
    }
    Ok(dirs)
}

pub(super) fn select_records_for_windowed_candidates(
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
            let Ok(record) = decode_agent_artifact_record_json(&record_json)
            else {
                stats.json_decode_errors += 1;
                continue;
            };
            stats.record_json_decoded += 1;
            let selection = match candidate.selection {
                CandidateSelection::Active => RecordSelection::Active,
                CandidateSelection::Completed => RecordSelection::Completed,
                CandidateSelection::Visible => RecordSelection::Visible,
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

pub(super) fn candidate_matches_query_filter(
    row: &IndexedCandidateRow,
    filter: Option<&AgentArtifactCandidateFilterWire>,
) -> bool {
    filter
        .map(|filter| candidate_filter_matches(row, filter))
        .unwrap_or(true)
}

pub(super) fn candidate_filter_matches(
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

pub(super) fn push_machine_value<'a>(
    values: &mut Vec<&'a str>,
    machine: &'a str,
) {
    let trimmed = machine.trim();
    if trimmed.is_empty() {
        return;
    }
    if values
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(trimmed))
    {
        return;
    }
    values.push(trimmed);
}

pub(super) fn candidate_filter_uses_machine(
    filter: &AgentArtifactCandidateFilterWire,
) -> bool {
    match filter {
        AgentArtifactCandidateFilterWire::All { filters }
        | AgentArtifactCandidateFilterWire::Any { filters } => {
            filters.iter().any(candidate_filter_uses_machine)
        }
        AgentArtifactCandidateFilterWire::Not { filter } => {
            candidate_filter_uses_machine(filter)
        }
        AgentArtifactCandidateFilterWire::Contains { field, .. }
        | AgentArtifactCandidateFilterWire::Equals { field, .. } => {
            *field == AgentArtifactCandidateFieldWire::Machine
        }
    }
}

pub(super) fn contains_case_insensitive(candidate: &str, value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    candidate.to_lowercase().contains(&value.to_lowercase())
}

pub(super) fn scalar_equals(candidate: &str, value: &str) -> bool {
    let candidate = candidate.to_lowercase();
    let value = value.to_lowercase();
    if candidate == value {
        return true;
    }
    candidate == "agent" && (value == "run" || value == "running")
}
