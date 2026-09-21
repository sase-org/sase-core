use super::index_wire::{
    AgentAliasHistoryGroupWire, AgentAliasHistoryLimitWire,
    AgentAliasHistoryQueryWire, AgentAliasHistoryWire, AgentAliasRunWire,
    AgentArtifactIndexFreshnessWire, AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION,
};
use super::maintenance::upsert_record;
use super::placeholders;
use super::record_summary::MarkerSignatures;
use super::selection::{pending_refresh_row_from_sql, PendingRefreshRow};
use super::storage::{open_index, open_index_read_only};
use crate::agent_scan::scanner::scan_agent_artifact_dir;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactScanOptionsWire,
    AgentMetaWire,
};
use rusqlite::{params_from_iter, Connection};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

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

pub(super) fn refresh_alias_history_candidates(
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

pub(super) fn select_alias_history_group(
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

pub(super) fn alias_history_where_clause(
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

pub(super) fn alias_run_from_sql_row(
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
    let Ok(record) = decode_agent_artifact_record_json(&record_json) else {
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

pub(super) const RAW_PROMPT_FILE: &str = "raw_xprompt.md";
pub(super) const ALIAS_HISTORY_PROMPT_SNIPPET_ELLIPSIS: &str = "...";

pub(super) fn effective_model_alias_trail(meta: &AgentMetaWire) -> Vec<String> {
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

pub(super) fn read_alias_history_prompt_snippet(
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

pub(super) fn is_leading_prompt_prefix_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty() || trimmed.starts_with('%') || trimmed.starts_with('#')
}

pub(super) fn collapse_prompt_whitespace(text: &str) -> String {
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

pub(super) fn truncate_prompt_snippet(text: &str, max_bytes: usize) -> String {
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
