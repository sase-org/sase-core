//! Canonical backend operations for dismissed-agent archives.
//!
//! The Python CLI/TUI still owns presentation and Agent object conversion, but
//! stable archive query, facet, visibility, and verification wire contracts live
//! here so other frontends can use the same backend surface.

pub mod wire;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, OptionalExtension, Row};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::agent_identity::AgentOwnerIdentity;

pub use wire::{
    AgentArchiveCapabilitiesWire, AgentArchiveCapabilityFactsWire,
    AgentArchiveCapabilityValidationRequestWire, AgentArchiveFacetCountWire,
    AgentArchiveFacetCountsWire, AgentArchiveFacetRequestWire,
    AgentArchiveKeyWire, AgentArchiveLifecycleFailureWire,
    AgentArchivePurgeReportWire, AgentArchiveQueryPageWire,
    AgentArchiveQueryRequestWire, AgentArchiveReviveMarkReportWire,
    AgentArchiveReviveMarkRequestWire, AgentArchiveScrubReportWire,
    AgentArchiveSummaryWire, AgentArchiveVerifyReportWire,
    AgentArchiveVisibilityWire, AGENT_ARCHIVE_WIRE_SCHEMA_VERSION,
};

const INDEX_FILENAME: &str = "index.sqlite";
const MAX_SOURCE_RUN_ID_BYTES: usize = 128;

pub fn validate_agent_archive_key(
    key: AgentArchiveKeyWire,
) -> Result<AgentArchiveKeyWire, String> {
    AgentOwnerIdentity::new(&key.source_username, &key.source_machine)
        .map_err(|e| e.to_string())?;
    validate_source_run_id(&key.source_run_id)?;
    Ok(key)
}

pub fn validate_agent_archive_visibility(
    visibility: AgentArchiveVisibilityWire,
) -> Result<AgentArchiveVisibilityWire, String> {
    match visibility.visibility.as_str() {
        "hidden" | "visible" | "pinned" => Ok(visibility),
        other => Err(format!(
            "unsupported archive visibility {other:?}; expected hidden, visible, or pinned"
        )),
    }
}

pub fn validate_agent_archive_capabilities(
    request: AgentArchiveCapabilityValidationRequestWire,
) -> Result<AgentArchiveCapabilitiesWire, String> {
    let derived = derive_agent_archive_capabilities(request.facts);
    if let Some(asserted) = request.asserted {
        if asserted.historically_viewable != derived.historically_viewable {
            return Err("asserted historically_viewable does not match persisted inputs"
                .to_string());
        }
        if asserted.durably_revivable != derived.durably_revivable {
            return Err(
                "asserted durably_revivable does not match persisted inputs"
                    .to_string(),
            );
        }
        if asserted.restartable != derived.restartable {
            return Err("asserted restartable does not match persisted inputs"
                .to_string());
        }
        if asserted.missing_requirements != derived.missing_requirements {
            return Err(
                "asserted missing_requirements does not match persisted inputs"
                    .to_string(),
            );
        }
    }
    Ok(derived)
}

pub fn derive_agent_archive_capabilities(
    facts: AgentArchiveCapabilityFactsWire,
) -> AgentArchiveCapabilitiesWire {
    let historically_viewable = facts.has_metadata && facts.has_state;
    let durably_revivable = historically_viewable
        && facts.has_commits
        && facts.loader_reconstructible;
    let restartable = durably_revivable
        && facts.has_prompt
        && facts.has_model
        && facts.has_llm_provider
        && facts.has_reasoning_effort;

    let mut missing = BTreeSet::new();
    if !facts.has_commits {
        missing.insert("commits".to_string());
    }
    if !facts.has_llm_provider {
        missing.insert("llm_provider".to_string());
    }
    if !facts.loader_reconstructible {
        missing.insert("loader_reconstructible_archive".to_string());
    }
    if !facts.has_metadata {
        missing.insert("metadata".to_string());
    }
    if !facts.has_model {
        missing.insert("model".to_string());
    }
    if !facts.has_prompt {
        missing.insert("prompt".to_string());
    }
    if !facts.has_reasoning_effort {
        missing.insert("reasoning_effort".to_string());
    }
    if !facts.has_state {
        missing.insert("state".to_string());
    }

    AgentArchiveCapabilitiesWire {
        historically_viewable,
        durably_revivable,
        restartable,
        missing_requirements: missing.into_iter().collect(),
    }
}

pub fn query_agent_archive(
    root: &Path,
    request: AgentArchiveQueryRequestWire,
) -> Result<AgentArchiveQueryPageWire, String> {
    let Some(conn) = open_existing_index(root)? else {
        return Ok(AgentArchiveQueryPageWire {
            results: Vec::new(),
            next_cursor: None,
        });
    };
    let offset = request.cursor.unwrap_or(0).max(0);
    let limit = request.limit.max(0);
    let sql = format!(
        "SELECT s.* FROM dismissed_bundle_summaries s WHERE {} \
         ORDER BY COALESCE(s.dismissed_at, s.start_time, s.raw_suffix) DESC, \
         s.filename ASC LIMIT ? OFFSET ?",
        request.where_sql
    );
    let mut params = json_params_to_sql(request.params)?;
    params.push(SqlValue::Integer(limit + 1));
    params.push(SqlValue::Integer(offset));
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params_from_iter(params.iter()), summary_from_row)
        .map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(|e| e.to_string())?);
    }
    let next_cursor = if (results.len() as i64) > limit {
        results.truncate(limit as usize);
        Some(offset + limit)
    } else {
        None
    };
    Ok(AgentArchiveQueryPageWire {
        results,
        next_cursor,
    })
}

pub fn agent_archive_facet_counts(
    root: &Path,
    request: AgentArchiveFacetRequestWire,
) -> Result<AgentArchiveFacetCountsWire, String> {
    let Some(conn) = open_existing_index(root)? else {
        return Ok(AgentArchiveFacetCountsWire {
            facet: request.facet,
            counts: Vec::new(),
        });
    };
    let column = facet_column(&request.facet)?;
    let sql = format!(
        "SELECT COALESCE(s.{column}, '') AS value, COUNT(*) AS count \
         FROM dismissed_bundle_summaries s WHERE {} \
         GROUP BY s.{column} ORDER BY count DESC, value ASC LIMIT ?",
        request.where_sql
    );
    let mut params = json_params_to_sql(request.params)?;
    params.push(SqlValue::Integer(request.limit.max(0)));
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params_from_iter(params.iter()), |row| {
            Ok(AgentArchiveFacetCountWire {
                value: row.get::<_, String>("value")?,
                count: row.get::<_, i64>("count")?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut counts = Vec::new();
    for row in rows {
        counts.push(row.map_err(|e| e.to_string())?);
    }
    Ok(AgentArchiveFacetCountsWire {
        facet: request.facet,
        counts,
    })
}

pub fn mark_agent_archive_bundles_revived(
    root: &Path,
    request: AgentArchiveReviveMarkRequestWire,
) -> AgentArchiveReviveMarkReportWire {
    let mut changed = 0;
    let mut failed = Vec::new();
    let matched = (request.bundle_paths.len() + request.keys.len()) as i64;
    let conn = match open_existing_index(root) {
        Ok(Some(conn)) => Some(conn),
        Ok(None) => None,
        Err(error) => {
            return AgentArchiveReviveMarkReportWire {
                ok: false,
                matched,
                changed: 0,
                failed: vec![AgentArchiveLifecycleFailureWire {
                    bundle_path: "<index>".to_string(),
                    error,
                }],
            };
        }
    };
    let Some(conn) = conn else {
        return AgentArchiveReviveMarkReportWire {
            ok: false,
            matched,
            changed: 0,
            failed: request
                .bundle_paths
                .iter()
                .map(|bundle_path| AgentArchiveLifecycleFailureWire {
                    bundle_path: bundle_path.clone(),
                    error: "archive index is not available".to_string(),
                })
                .chain(request.keys.iter().map(|key| {
                    AgentArchiveLifecycleFailureWire {
                        bundle_path: key_label(key),
                        error: "archive index is not available".to_string(),
                    }
                }))
                .collect(),
        };
    };
    for bundle_path in &request.bundle_paths {
        match mark_bundle_path_visible(&conn, bundle_path, &request.revived_at)
        {
            Ok(true) => changed += 1,
            Ok(false) => failed.push(AgentArchiveLifecycleFailureWire {
                bundle_path: bundle_path.clone(),
                error: "bundle is not indexed".to_string(),
            }),
            Err(error) => failed.push(AgentArchiveLifecycleFailureWire {
                bundle_path: bundle_path.clone(),
                error,
            }),
        }
    }
    for key in &request.keys {
        match validate_agent_archive_key(key.clone())
            .and_then(|key| mark_key_visible(&conn, &key, &request.revived_at))
        {
            Ok(true) => changed += 1,
            Ok(false) => failed.push(AgentArchiveLifecycleFailureWire {
                bundle_path: key_label(key),
                error: "archive key is not indexed".to_string(),
            }),
            Err(error) => failed.push(AgentArchiveLifecycleFailureWire {
                bundle_path: key_label(key),
                error,
            }),
        }
    }
    AgentArchiveReviveMarkReportWire {
        ok: failed.is_empty(),
        matched,
        changed,
        failed,
    }
}

pub fn verify_agent_archive_index(root: &Path) -> AgentArchiveVerifyReportWire {
    let mut indexed_paths = BTreeSet::new();
    let mut stale_rows = 0;
    let mut payload_hash_mismatches = 0;
    let conn = match open_existing_index(root) {
        Ok(conn) => conn,
        Err(_) => {
            stale_rows = 1;
            None
        }
    };
    if let Some(conn) = &conn {
        match conn.prepare(
            "SELECT bundle_path, mtime_ns, size_bytes \
             FROM dismissed_bundle_summaries",
        ) {
            Ok(mut stmt) => {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>("bundle_path")?,
                        row.get::<_, i64>("mtime_ns")?,
                        row.get::<_, i64>("size_bytes")?,
                    ))
                });
                match rows {
                    Ok(rows) => {
                        for row in rows {
                            let Ok((bundle_path, mtime_ns, size_bytes)) = row
                            else {
                                stale_rows += 1;
                                continue;
                            };
                            indexed_paths.insert(bundle_path.clone());
                            let path = Path::new(&bundle_path);
                            match file_signature(path) {
                                Ok((actual_mtime, actual_size)) => {
                                    if actual_mtime != mtime_ns
                                        || actual_size != size_bytes
                                    {
                                        stale_rows += 1;
                                    }
                                }
                                Err(_) => {
                                    stale_rows += 1;
                                    continue;
                                }
                            }
                            if let Ok(bundle) = read_bundle(path) {
                                if let Some(expected) = bundle
                                    .get("archive_payload_sha256")
                                    .and_then(JsonValue::as_str)
                                {
                                    if !expected.is_empty()
                                        && expected
                                            != archive_payload_hash(&bundle)
                                    {
                                        payload_hash_mismatches += 1;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => stale_rows += 1,
                }
            }
            Err(_) => stale_rows += 1,
        }
    }

    let mut valid_paths = BTreeSet::new();
    let mut valid_search_paths = BTreeSet::new();
    let mut corrupt_bundles = 0;
    for path in iter_bundle_paths(root) {
        match read_bundle(&path) {
            Ok(bundle) => {
                let path_string = path_to_string(&path);
                valid_paths.insert(path_string.clone());
                if bundle
                    .get("archive_search_text")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|value| !value.is_empty())
                {
                    valid_search_paths.insert(path_string);
                }
            }
            Err(_) => corrupt_bundles += 1,
        }
    }

    let mut fts_paths = BTreeSet::new();
    if let Some(conn) = &conn {
        match conn
            .prepare("SELECT bundle_path FROM dismissed_bundle_search_fts")
        {
            Ok(mut stmt) => match stmt
                .query_map([], |row| row.get::<_, String>("bundle_path"))
            {
                Ok(rows) => {
                    for path in rows.flatten() {
                        fts_paths.insert(path);
                    }
                }
                Err(_) => stale_rows += 1,
            },
            Err(_) => stale_rows += 1,
        }
    }

    let missing_rows = valid_paths.difference(&indexed_paths).count() as i64;
    let fts_missing_rows = valid_search_paths
        .intersection(&indexed_paths)
        .filter(|path| !fts_paths.contains(*path))
        .count() as i64;
    let fts_orphan_rows = fts_paths.difference(&indexed_paths).count() as i64;
    let ok = stale_rows == 0
        && missing_rows == 0
        && fts_missing_rows == 0
        && fts_orphan_rows == 0
        && payload_hash_mismatches == 0;
    AgentArchiveVerifyReportWire {
        ok,
        indexed_rows: indexed_paths.len() as i64,
        valid_bundles: valid_paths.len() as i64,
        corrupt_bundles,
        stale_rows,
        missing_rows,
        fts_missing_rows,
        fts_orphan_rows,
        payload_hash_mismatches,
        orphan_visibility_rows: 0,
        orphan_revision_rows: 0,
    }
}

fn open_existing_index(root: &Path) -> Result<Option<Connection>, String> {
    let index_path = root.join(INDEX_FILENAME);
    if !index_path.is_file() {
        return Ok(None);
    }
    let conn = Connection::open(index_path).map_err(|e| e.to_string())?;
    conn.pragma_update(None, "busy_timeout", 30000)
        .map_err(|e| e.to_string())?;
    Ok(Some(conn))
}

fn facet_column(facet: &str) -> Result<&'static str, String> {
    match facet {
        "status" => Ok("status"),
        "project" => Ok("project_name"),
        "model" => Ok("model"),
        "runtime" => Ok("runtime"),
        other => Err(format!(
            "Unsupported archive facet {other:?}; expected one of model, project, runtime, status"
        )),
    }
}

fn json_params_to_sql(params: Vec<JsonValue>) -> Result<Vec<SqlValue>, String> {
    params
        .into_iter()
        .map(|value| match value {
            JsonValue::Null => Ok(SqlValue::Null),
            JsonValue::Bool(value) => Ok(SqlValue::Integer(i64::from(value))),
            JsonValue::Number(value) => {
                if let Some(int_value) = value.as_i64() {
                    Ok(SqlValue::Integer(int_value))
                } else if let Some(float_value) = value.as_f64() {
                    Ok(SqlValue::Real(float_value))
                } else {
                    Err("unsupported numeric archive query parameter"
                        .to_string())
                }
            }
            JsonValue::String(value) => Ok(SqlValue::Text(value)),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                Err("archive query parameters must be scalar".to_string())
            }
        })
        .collect()
}

fn summary_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<AgentArchiveSummaryWire> {
    let raw_suffix: String = row.get("raw_suffix")?;
    Ok(AgentArchiveSummaryWire {
        agent_id: row_string_or(row, "agent_id", &raw_suffix)?,
        raw_suffix,
        bundle_path: row.get("bundle_path")?,
        source_username: row_optional_string(row, "source_username")?,
        source_machine: row_optional_string(row, "source_machine")?,
        source_run_id: row_optional_string(row, "source_run_id")?,
        archive_visibility: row_string_or(row, "archive_visibility", "hidden")?,
        historically_viewable: row_bool_or(row, "historically_viewable", true)?,
        durably_revivable: row_bool_or(row, "durably_revivable", true)?,
        restartable: row_bool_or(row, "restartable", false)?,
        missing_requirements: row_json_string_list_or_empty(
            row,
            "missing_requirements",
        )?,
        cl_name: row.get("cl_name")?,
        agent_name: row.get("agent_name")?,
        status: row.get("status")?,
        start_time: row.get("start_time")?,
        dismissed_at: row_optional_string(row, "dismissed_at")?,
        revived_at: row_optional_string(row, "revived_at")?,
        project_name: row_optional_string(row, "project_name")?,
        model: row.get("model")?,
        runtime: row_optional_string(row, "runtime")?,
        llm_provider: row.get("llm_provider")?,
        step_index: row.get("step_index")?,
        step_name: row.get("step_name")?,
        step_type: row_optional_string(row, "step_type")?,
        retry_attempt: row.get("retry_attempt")?,
        is_workflow_child: row.get::<_, i64>("is_workflow_child")? != 0,
    })
}

fn read_bundle(path: &Path) -> Result<JsonValue, String> {
    let data = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let value: JsonValue =
        serde_json::from_str(&data).map_err(|e| e.to_string())?;
    if !value.is_object() {
        return Err("bundle JSON must be an object".to_string());
    }
    Ok(value)
}

fn iter_bundle_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return paths;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && is_shard_dir(&path) {
            if let Ok(children) = fs::read_dir(&path) {
                for child in children.flatten() {
                    let child_path = child.path();
                    if child_path.is_file()
                        && child_path.extension().and_then(|s| s.to_str())
                            == Some("json")
                    {
                        paths.push(child_path);
                    } else if child_path.is_dir() {
                        let bundle = child_path.join("bundle.json");
                        if bundle.is_file() {
                            paths.push(bundle);
                        }
                    }
                }
            }
        } else if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("json")
        {
            paths.push(path);
        }
    }
    paths
}

fn is_shard_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| {
            name.len() == 6 && name.as_bytes().iter().all(u8::is_ascii_digit)
        })
}

fn file_signature(path: &Path) -> Result<(i64, i64), String> {
    let metadata = fs::metadata(path).map_err(|e| e.to_string())?;
    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0);
    Ok((mtime_ns, metadata.len() as i64))
}

fn archive_payload_hash(bundle: &JsonValue) -> String {
    let mut payload = bundle.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("archive_payload_sha256");
    }
    let encoded = serde_json::to_vec(&payload).unwrap_or_default();
    hex::encode(Sha256::digest(&encoded))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn validate_source_run_id(run_id: &str) -> Result<(), String> {
    let valid = !run_id.is_empty()
        && run_id.len() <= MAX_SOURCE_RUN_ID_BYTES
        && run_id != "."
        && run_id != ".."
        && !run_id.contains("..")
        && run_id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':')
        });
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid source_run_id {run_id:?}; expected 1..={MAX_SOURCE_RUN_ID_BYTES} path-safe ASCII letters, digits, '-', '_', '.', or ':' without '.."
        ))
    }
}

fn key_label(key: &AgentArchiveKeyWire) -> String {
    format!(
        "{}.{}@{}",
        key.source_username, key.source_machine, key.source_run_id
    )
}

fn mark_bundle_path_visible(
    conn: &Connection,
    bundle_path: &str,
    revived_at: &str,
) -> Result<bool, String> {
    let row = conn
        .query_row(
            "SELECT source_username, source_machine, source_run_id \
             FROM dismissed_bundle_summaries WHERE bundle_path = ?",
            [bundle_path],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let Some((source_username, source_machine, source_run_id)) = row else {
        return Ok(false);
    };
    let mut updated =
        update_summary_by_bundle_path(conn, bundle_path, revived_at)?;
    if let (Some(source_username), Some(source_machine), Some(source_run_id)) =
        (source_username, source_machine, source_run_id)
    {
        let key = validate_agent_archive_key(AgentArchiveKeyWire {
            source_username,
            source_machine,
            source_run_id,
        })?;
        updated |= upsert_projection_visibility(conn, &key, revived_at)?;
    }
    Ok(updated)
}

fn mark_key_visible(
    conn: &Connection,
    key: &AgentArchiveKeyWire,
    revived_at: &str,
) -> Result<bool, String> {
    let mut updated = update_summary_by_key(conn, key, revived_at)?;
    updated |= upsert_projection_visibility(conn, key, revived_at)?;
    Ok(updated)
}

fn update_summary_by_bundle_path(
    conn: &Connection,
    bundle_path: &str,
    revived_at: &str,
) -> Result<bool, String> {
    update_summary_visibility(
        conn,
        "bundle_path = ?",
        vec![SqlValue::Text(bundle_path.to_string())],
        revived_at,
    )
}

fn update_summary_by_key(
    conn: &Connection,
    key: &AgentArchiveKeyWire,
    revived_at: &str,
) -> Result<bool, String> {
    update_summary_visibility(
        conn,
        "source_username = ? AND source_machine = ? AND source_run_id = ?",
        vec![
            SqlValue::Text(key.source_username.clone()),
            SqlValue::Text(key.source_machine.clone()),
            SqlValue::Text(key.source_run_id.clone()),
        ],
        revived_at,
    )
}

fn update_summary_visibility(
    conn: &Connection,
    where_sql: &str,
    mut params: Vec<SqlValue>,
    revived_at: &str,
) -> Result<bool, String> {
    let mut assignments = Vec::new();
    if column_exists(conn, "dismissed_bundle_summaries", "archive_visibility")?
    {
        assignments.push("archive_visibility = 'visible'".to_string());
    }
    if column_exists(conn, "dismissed_bundle_summaries", "revived_at")? {
        assignments.push("revived_at = ?".to_string());
        params.insert(0, SqlValue::Text(revived_at.to_string()));
    }
    if column_exists(conn, "dismissed_bundle_summaries", "times_revived")? {
        assignments
            .push("times_revived = COALESCE(times_revived, 0) + 1".to_string());
    }
    if assignments.is_empty() {
        return Ok(false);
    }
    let sql = format!(
        "UPDATE dismissed_bundle_summaries SET {} WHERE {where_sql}",
        assignments.join(", ")
    );
    let rows = conn
        .execute(&sql, params_from_iter(params.iter()))
        .map_err(|e| e.to_string())?;
    Ok(rows > 0)
}

fn upsert_projection_visibility(
    conn: &Connection,
    key: &AgentArchiveKeyWire,
    revived_at: &str,
) -> Result<bool, String> {
    if !table_exists(conn, "archive_visibility_projection")? {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO archive_visibility_projection (
            source_username, source_machine, source_run_id, visibility,
            revived_at, times_revived, updated_at
         ) VALUES (?1, ?2, ?3, 'visible', ?4, 1, ?4)
         ON CONFLICT(source_username, source_machine, source_run_id) DO UPDATE SET
            visibility='visible',
            revived_at=excluded.revived_at,
            updated_at=excluded.updated_at,
            times_revived=COALESCE(times_revived, 0) + 1",
        (
            &key.source_username,
            &key.source_machine,
            &key.source_run_id,
            revived_at,
        ),
    )
    .map_err(|e| e.to_string())?;
    Ok(true)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, String> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    Ok(count > 0)
}

fn column_exists(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, String> {
    let sql = format!("PRAGMA table_info({})", quote_identifier(table));
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let name: String = row.get("name").map_err(|e| e.to_string())?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn row_has_column(row: &Row<'_>, column: &str) -> bool {
    row.as_ref().column_index(column).is_ok()
}

fn row_optional_string(
    row: &Row<'_>,
    column: &str,
) -> rusqlite::Result<Option<String>> {
    if row_has_column(row, column) {
        row.get(column)
    } else {
        Ok(None)
    }
}

fn row_string_or(
    row: &Row<'_>,
    column: &str,
    default: &str,
) -> rusqlite::Result<String> {
    Ok(
        row_optional_string(row, column)?
            .unwrap_or_else(|| default.to_string()),
    )
}

fn row_bool_or(
    row: &Row<'_>,
    column: &str,
    default: bool,
) -> rusqlite::Result<bool> {
    if row_has_column(row, column) {
        Ok(row.get::<_, i64>(column)? != 0)
    } else {
        Ok(default)
    }
}

fn row_json_string_list_or_empty(
    row: &Row<'_>,
    column: &str,
) -> rusqlite::Result<Vec<String>> {
    let Some(raw) = row_optional_string(row, column)? else {
        return Ok(Vec::new());
    };
    Ok(serde_json::from_str::<Vec<String>>(&raw).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn query_agent_archive_returns_paged_summary_rows() {
        let tmp = TempDir::new().unwrap();
        let conn = create_index(tmp.path());
        conn.execute(
            "INSERT INTO dismissed_bundle_summaries (
                bundle_path, agent_id, raw_suffix, shard, filename,
                archive_revision, bundle_schema_version, agent_type, cl_name,
                status, dismissed_at, is_workflow_child, retry_attempt,
                mtime_ns, size_bytes
             ) VALUES (?1, ?2, ?3, '202605', ?4, 1, 2, 'run', ?5,
                ?6, ?7, 0, 0, 1, 2)",
            params![
                "/tmp/a.json",
                "agent-a",
                "20260512120000",
                "a.json",
                "cl_a",
                "FAILED",
                "2026-05-12T12:00:00",
            ],
        )
        .unwrap();

        let page = query_agent_archive(
            tmp.path(),
            AgentArchiveQueryRequestWire {
                where_sql: "s.status = ?".to_string(),
                params: vec![json!("FAILED")],
                limit: 1,
                cursor: None,
            },
        )
        .unwrap();

        assert_eq!(page.results.len(), 1);
        assert_eq!(page.results[0].cl_name, "cl_a");
        assert_eq!(page.next_cursor, None);
    }

    #[test]
    fn mark_agent_archive_bundles_revived_updates_projection_without_bundle_mutation(
    ) {
        let tmp = TempDir::new().unwrap();
        let bundle_path = tmp
            .path()
            .join("202605")
            .join("agent.1")
            .join("bundle.json");
        fs::create_dir_all(bundle_path.parent().unwrap()).unwrap();
        fs::write(
            &bundle_path,
            r#"{"raw_suffix":"20260512120000","times_revived":1}"#,
        )
        .unwrap();
        let conn = create_index(tmp.path());
        conn.execute(
            "INSERT INTO dismissed_bundle_summaries (
                bundle_path, agent_id, raw_suffix, shard, filename,
                archive_revision, bundle_schema_version, agent_type, cl_name,
                status, source_username, source_machine, source_run_id,
                archive_visibility, historically_viewable, durably_revivable,
                restartable, missing_requirements, is_workflow_child,
                retry_attempt, mtime_ns, size_bytes
             ) VALUES (?1, 'agent', '20260512120000', '202605',
                'agent.1/bundle.json', 1, 2, 'run', 'cl', 'DONE',
                'alice', 'athena', 'run-1', 'hidden', 1, 1, 1, '[]',
                0, 0, 1, 2)",
            params![path_to_string(&bundle_path)],
        )
        .unwrap();

        let report = mark_agent_archive_bundles_revived(
            tmp.path(),
            AgentArchiveReviveMarkRequestWire {
                bundle_paths: vec![path_to_string(&bundle_path)],
                keys: Vec::new(),
                revived_at: "2026-05-12T13:00:00".to_string(),
            },
        );

        assert!(report.ok);
        assert_eq!(report.changed, 1);
        let updated = read_bundle(&bundle_path).unwrap();
        assert_eq!(
            updated,
            json!({"raw_suffix":"20260512120000","times_revived":1})
        );
        let revived_at: String = conn
            .query_row(
                "SELECT revived_at FROM dismissed_bundle_summaries WHERE bundle_path = ?",
                params![path_to_string(&bundle_path)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revived_at, "2026-05-12T13:00:00");
        let projection: (String, i64) = conn
            .query_row(
                "SELECT visibility, times_revived FROM archive_visibility_projection \
                 WHERE source_username = 'alice' AND source_machine = 'athena' \
                   AND source_run_id = 'run-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(projection, ("visible".to_string(), 1));
    }

    #[test]
    fn validates_capabilities_against_persisted_inputs() {
        let full = AgentArchiveCapabilityFactsWire {
            has_metadata: true,
            has_state: true,
            has_commits: true,
            loader_reconstructible: true,
            has_prompt: true,
            has_model: true,
            has_llm_provider: true,
            has_reasoning_effort: true,
        };
        let capabilities = validate_agent_archive_capabilities(
            AgentArchiveCapabilityValidationRequestWire {
                facts: full.clone(),
                asserted: None,
            },
        )
        .unwrap();
        assert!(capabilities.historically_viewable);
        assert!(capabilities.durably_revivable);
        assert!(capabilities.restartable);
        assert!(capabilities.missing_requirements.is_empty());

        let missing_prompt = AgentArchiveCapabilityFactsWire {
            has_prompt: false,
            ..full
        };
        let capabilities = validate_agent_archive_capabilities(
            AgentArchiveCapabilityValidationRequestWire {
                facts: missing_prompt.clone(),
                asserted: None,
            },
        )
        .unwrap();
        assert!(capabilities.durably_revivable);
        assert!(!capabilities.restartable);
        assert_eq!(capabilities.missing_requirements, vec!["prompt"]);

        let error = validate_agent_archive_capabilities(
            AgentArchiveCapabilityValidationRequestWire {
                facts: missing_prompt,
                asserted: Some(AgentArchiveCapabilitiesWire {
                    historically_viewable: true,
                    durably_revivable: true,
                    restartable: true,
                    missing_requirements: Vec::new(),
                }),
            },
        )
        .unwrap_err();
        assert!(error.contains("restartable"));
    }

    #[test]
    fn validates_archive_key_and_visibility() {
        assert!(validate_agent_archive_key(AgentArchiveKeyWire {
            source_username: "alice".to_string(),
            source_machine: "athena".to_string(),
            source_run_id: "run-1".to_string(),
        })
        .is_ok());
        assert!(validate_agent_archive_key(AgentArchiveKeyWire {
            source_username: "Alice".to_string(),
            source_machine: "athena".to_string(),
            source_run_id: "run-1".to_string(),
        })
        .is_err());
        assert!(validate_agent_archive_visibility(
            AgentArchiveVisibilityWire {
                visibility: "pinned".to_string()
            }
        )
        .is_ok());
        assert!(validate_agent_archive_visibility(
            AgentArchiveVisibilityWire {
                visibility: "archived".to_string()
            }
        )
        .is_err());
    }

    fn create_index(root: &Path) -> Connection {
        fs::create_dir_all(root).unwrap();
        let conn = Connection::open(root.join(INDEX_FILENAME)).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE dismissed_bundle_summaries (
                bundle_path TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                raw_suffix TEXT NOT NULL,
                shard TEXT NOT NULL,
                filename TEXT NOT NULL,
                archive_revision INTEGER NOT NULL DEFAULT 1,
                bundle_schema_version INTEGER NOT NULL DEFAULT 0,
                agent_type TEXT NOT NULL,
                source_username TEXT,
                source_machine TEXT,
                source_run_id TEXT,
                archive_visibility TEXT NOT NULL DEFAULT 'hidden',
                historically_viewable INTEGER NOT NULL DEFAULT 1,
                durably_revivable INTEGER NOT NULL DEFAULT 1,
                restartable INTEGER NOT NULL DEFAULT 0,
                missing_requirements TEXT NOT NULL DEFAULT '[]',
                cl_name TEXT NOT NULL,
                agent_name TEXT,
                status TEXT NOT NULL,
                start_time TEXT,
                stop_time TEXT,
                dismissed_at TEXT,
                revived_at TEXT,
                times_revived INTEGER NOT NULL DEFAULT 0,
                project_file TEXT,
                project_name TEXT,
                model TEXT,
                llm_provider TEXT,
                runtime TEXT,
                vcs_provider TEXT,
                workflow TEXT,
                is_workflow_child INTEGER NOT NULL,
                parent_timestamp TEXT,
                step_index INTEGER,
                step_name TEXT,
                step_type TEXT,
                retry_of_timestamp TEXT,
                retried_as_timestamp TEXT,
                retry_chain_root_timestamp TEXT,
                retry_attempt INTEGER NOT NULL DEFAULT 0,
                meta_changespec TEXT,
                cost_usd_micros INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                error_message_excerpt TEXT,
                mtime_ns INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL
            );
            CREATE VIRTUAL TABLE dismissed_bundle_search_fts
            USING fts5(bundle_path UNINDEXED, archive_search_text);
            CREATE TABLE archive_visibility_projection (
                source_username TEXT NOT NULL,
                source_machine TEXT NOT NULL,
                source_run_id TEXT NOT NULL,
                visibility TEXT NOT NULL,
                dismissed_at TEXT,
                revived_at TEXT,
                pinned_at TEXT,
                times_revived INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                PRIMARY KEY(source_username, source_machine, source_run_id)
            );
            ",
        )
        .unwrap();
        conn
    }
}
