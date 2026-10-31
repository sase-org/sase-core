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
     has_workflow_state, agent_clan, agent_clan_generation, agent_session, \
     agent_name";

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
        agent_session: row.get(14).map_err(|e| e.to_string())?,
        agent_name: row.get(15).map_err(|e| e.to_string())?,
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
    let (pushdown_predicate, pushdown_params) =
        session_name_pushdown_predicate(query.candidate_filter.as_ref())
            .unzip();
    let pushdown_params = pushdown_params.unwrap_or_default();
    let pushdown_where = |where_sql: String| match &pushdown_predicate {
        Some(predicate) => {
            and_pushdown_predicate_to_where(where_sql, predicate)
        }
        None => where_sql,
    };
    let active_rows = select_candidate_rows(
        conn,
        pushdown_where(active_where(query.include_hidden, project_filter)),
        &pushdown_params,
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
        pushdown_where(completed_window_where(
            query.include_hidden,
            project_filter,
        )),
        &pushdown_params,
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
    params: &[String],
    selection: CandidateSelection,
) -> Result<Vec<IndexedCandidateRow>, String> {
    let sql = format!(
        "SELECT {CANDIDATE_ROW_COLUMNS} FROM agent_artifacts {where_sql}"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(|e| e.to_string())?;
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
    pub(super) agent_session: Option<String>,
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
            "SELECT timestamp, agent_session, agent_clan, parent_timestamp \
             FROM agent_artifacts WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            result.push(CandidateTreeKeys {
                timestamp: row.get(0).map_err(|e| e.to_string())?,
                agent_session: row.get(1).map_err(|e| e.to_string())?,
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
    let mut agent_sessions = BTreeSet::new();
    let mut clans = BTreeSet::new();
    let mut timestamps = BTreeSet::new();
    for key in keys {
        if let Some(agent_session) = key
            .agent_session
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            agent_sessions.insert(agent_session.to_string());
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
    if !agent_sessions.is_empty() {
        dirs.extend(select_dirs_in_column(
            conn,
            super::AGENT_SESSION_INDEX_COLUMN,
            &agent_sessions.into_iter().collect::<Vec<_>>(),
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
        super::AGENT_SESSION_INDEX_COLUMN
        | "agent_clan"
        | "timestamp"
        | "parent_timestamp" => {}
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

/// Indexed column backing one pushable session/name `Equals` atom.
fn pushdown_column_for_field(
    field: AgentArtifactCandidateFieldWire,
) -> Option<&'static str> {
    match field {
        AgentArtifactCandidateFieldWire::AgentSession => Some("agent_session"),
        AgentArtifactCandidateFieldWire::AgentName => Some("agent_name"),
        _ => None,
    }
}

/// Translate one `Equals` on `agent_session`/`agent_name` into a SQL
/// comparison, or return `None` when it must stay in Rust.
///
/// Rust equality lowercases both sides with full Unicode `to_lowercase`
/// (see `scalar_equals`), while SQLite `NOCASE` folds ASCII only. Agent
/// names and sessions are validated ASCII slugs at creation
/// (`validate_simple_segment` in `agent_identity::identity` accepts only
/// ASCII alphanumerics plus `-` and `_` per dot segment), so for an ASCII
/// filter value the two comparisons agree. Non-ASCII values skip pushdown
/// and fall through to the Rust post-filter.
///
/// The `run`/`running` alias in `scalar_equals` (a stored `agent` value
/// matches those filter values) has no SQL equivalent, so those values
/// skip pushdown as well.
fn pushdown_equals_term(
    field: AgentArtifactCandidateFieldWire,
    value: &str,
) -> Option<(String, String)> {
    let column = pushdown_column_for_field(field)?;
    if !value.is_ascii() {
        return None;
    }
    if value.eq_ignore_ascii_case("run")
        || value.eq_ignore_ascii_case("running")
    {
        return None;
    }
    Some((format!("{column} COLLATE NOCASE = ?"), value.to_string()))
}

/// Exact translation: `Some` iff the SQL predicate accepts exactly the
/// rows the Rust matcher accepts. Every other field, `Not`, `Contains`,
/// non-ASCII values, and the `run`/`running` alias are untranslatable.
fn pushdown_exact_term(
    filter: &AgentArtifactCandidateFilterWire,
) -> Option<(String, Vec<String>)> {
    match filter {
        AgentArtifactCandidateFilterWire::Equals { field, value } => {
            let (predicate, param) = pushdown_equals_term(*field, value)?;
            Some((format!("({predicate})"), vec![param]))
        }
        AgentArtifactCandidateFilterWire::Any { filters } => {
            if filters.is_empty() {
                return None;
            }
            let mut parts = Vec::new();
            let mut params = Vec::new();
            for member in filters {
                let (sql, mut member_params) = pushdown_exact_term(member)?;
                parts.push(sql);
                params.append(&mut member_params);
            }
            Some((format!("({})", parts.join(" OR ")), params))
        }
        AgentArtifactCandidateFilterWire::All { filters } => {
            if filters.is_empty() {
                return None;
            }
            let mut parts = Vec::new();
            let mut params = Vec::new();
            for member in filters {
                let (sql, mut member_params) = pushdown_exact_term(member)?;
                parts.push(sql);
                params.append(&mut member_params);
            }
            Some((format!("({})", parts.join(" AND ")), params))
        }
        AgentArtifactCandidateFilterWire::Not { .. }
        | AgentArtifactCandidateFilterWire::Contains { .. } => None,
    }
}

/// Translate a top-level candidate filter into a SQL predicate that
/// selects a superset of what the Rust post-filter accepts.
///
/// An `Equals` on `agent_session`/`agent_name`, an `any` of such terms,
/// and such terms inside a top-level `all` are pushed down. Unpushable
/// conjuncts of a top-level `all` stay in the Rust post-filter, which
/// remains the exact backstop; a partially translatable `any` pushes
/// nothing, since dropping one arm would select a subset. Every other
/// field is never pushed.
pub(super) fn session_name_pushdown_predicate(
    filter: Option<&AgentArtifactCandidateFilterWire>,
) -> Option<(String, Vec<String>)> {
    let filter = filter?;
    if let Some(exact) = pushdown_exact_term(filter) {
        return Some(exact);
    }
    if let AgentArtifactCandidateFilterWire::All { filters } = filter {
        let mut parts = Vec::new();
        let mut params = Vec::new();
        for member in filters {
            if let Some((sql, mut member_params)) = pushdown_exact_term(member)
            {
                parts.push(sql);
                params.append(&mut member_params);
            }
        }
        if parts.is_empty() {
            return None;
        }
        return Some((format!("({})", parts.join(" AND ")), params));
    }
    None
}

/// Conjoin a session/name pushdown predicate onto a
/// `WHERE ... [ORDER BY ...]` fragment, mirroring
/// `add_project_filter_to_where`. A fragment without a `WHERE` (the
/// unfiltered full-history scan) gains one.
pub(super) fn and_pushdown_predicate_to_where(
    where_sql: String,
    predicate: &str,
) -> String {
    let upper = where_sql.to_ascii_uppercase();
    let order_index = upper.find("ORDER BY");
    let (prefix, order_by) = match order_index {
        Some(index) => (&where_sql[..index], &where_sql[index..]),
        None => (where_sql.as_str(), ""),
    };
    let trimmed_prefix = prefix.trim_end();
    if trimmed_prefix.is_empty() {
        return format!("WHERE ({predicate}) {order_by}");
    }
    let leading_trimmed = trimmed_prefix.trim_start();
    if leading_trimmed.starts_with("WHERE") {
        let existing = leading_trimmed.trim_start_matches("WHERE").trim();
        format!("WHERE ({existing}) AND ({predicate}) {order_by}")
    } else {
        format!("{trimmed_prefix} WHERE ({predicate}) {order_by}")
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
