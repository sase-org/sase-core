//! Canonical backend operations for dismissed-agent archives.
//!
//! The Python CLI/TUI still owns presentation and Agent object conversion, but
//! stable archive query, facet, visibility, and verification wire contracts live
//! here so other frontends can use the same backend surface.

pub mod wire;

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, Row};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

pub use wire::{
    AgentArchiveFacetCountWire, AgentArchiveFacetCountsWire,
    AgentArchiveFacetRequestWire, AgentArchiveLifecycleFailureWire,
    AgentArchivePurgeReportWire, AgentArchiveQueryPageWire,
    AgentArchiveQueryRequestWire, AgentArchiveReviveMarkReportWire,
    AgentArchiveReviveMarkRequestWire, AgentArchiveScrubReportWire,
    AgentArchiveSummaryWire, AgentArchiveVerifyReportWire,
    AGENT_ARCHIVE_WIRE_SCHEMA_VERSION,
};

const INDEX_FILENAME: &str = "index.sqlite";

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
    let conn = open_existing_index(root).ok().flatten();
    for bundle_path in &request.bundle_paths {
        match mark_one_bundle_revived(
            Path::new(bundle_path),
            &request.revived_at,
        ) {
            Ok(times_revived) => {
                changed += 1;
                if let Some(conn) = &conn {
                    let _ = conn.execute(
                        "UPDATE dismissed_bundle_summaries \
                         SET revived_at = ?, times_revived = ? \
                         WHERE bundle_path = ?",
                        (&request.revived_at, times_revived, bundle_path),
                    );
                }
            }
            Err(error) => failed.push(AgentArchiveLifecycleFailureWire {
                bundle_path: bundle_path.clone(),
                error,
            }),
        }
    }
    AgentArchiveReviveMarkReportWire {
        ok: failed.is_empty(),
        matched: request.bundle_paths.len() as i64,
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
    Ok(AgentArchiveSummaryWire {
        agent_id: row.get("agent_id")?,
        raw_suffix: row.get("raw_suffix")?,
        bundle_path: row.get("bundle_path")?,
        cl_name: row.get("cl_name")?,
        agent_name: row.get("agent_name")?,
        status: row.get("status")?,
        start_time: row.get("start_time")?,
        dismissed_at: row.get("dismissed_at")?,
        revived_at: row.get("revived_at")?,
        project_name: row.get("project_name")?,
        model: row.get("model")?,
        runtime: row.get("runtime")?,
        llm_provider: row.get("llm_provider")?,
        step_index: row.get("step_index")?,
        step_name: row.get("step_name")?,
        step_type: row.get("step_type")?,
        retry_attempt: row.get("retry_attempt")?,
        is_workflow_child: row.get::<_, i64>("is_workflow_child")? != 0,
    })
}

fn mark_one_bundle_revived(
    path: &Path,
    revived_at: &str,
) -> Result<i64, String> {
    let mut bundle = read_bundle(path)?;
    let times_revived = bundle
        .get("times_revived")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .unwrap_or(0)
        .max(0)
        + 1;
    let Some(object) = bundle.as_object_mut() else {
        return Err("bundle JSON must be an object".to_string());
    };
    object.insert(
        "revived_at".to_string(),
        JsonValue::String(revived_at.to_string()),
    );
    object.insert(
        "times_revived".to_string(),
        JsonValue::Number(times_revived.into()),
    );
    write_json_file_atomic(path, &bundle)?;
    Ok(times_revived)
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

fn write_json_file_atomic(
    path: &Path,
    value: &JsonValue,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_file_name(format!(
        ".{}.tmp.{}.{}",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("bundle"),
        std::process::id(),
        nonce
    ));
    let payload = serde_json::to_string_pretty(value)
        .map_err(|e| format!("failed to serialize archive bundle: {e}"))?;
    {
        let mut file = File::create(&tmp_path).map_err(|e| e.to_string())?;
        file.write_all(payload.as_bytes())
            .map_err(|e| e.to_string())?;
        file.write_all(b"\n").map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        e.to_string()
    })?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
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
    fn mark_agent_archive_bundles_revived_updates_bundle_and_index() {
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
                status, is_workflow_child, retry_attempt, mtime_ns, size_bytes
             ) VALUES (?1, 'agent', '20260512120000', '202605',
                'agent.1/bundle.json', 1, 2, 'run', 'cl', 'DONE',
                0, 0, 1, 2)",
            params![path_to_string(&bundle_path)],
        )
        .unwrap();

        let report = mark_agent_archive_bundles_revived(
            tmp.path(),
            AgentArchiveReviveMarkRequestWire {
                bundle_paths: vec![path_to_string(&bundle_path)],
                revived_at: "2026-05-12T13:00:00".to_string(),
            },
        );

        assert!(report.ok);
        assert_eq!(report.changed, 1);
        let updated = read_bundle(&bundle_path).unwrap();
        assert_eq!(updated["times_revived"], json!(2));
        let revived_at: String = conn
            .query_row(
                "SELECT revived_at FROM dismissed_bundle_summaries WHERE bundle_path = ?",
                params![path_to_string(&bundle_path)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revived_at, "2026-05-12T13:00:00");
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
            ",
        )
        .unwrap();
        conn
    }
}
