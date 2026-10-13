//! Rust-owned dismissed-bundle archive index store (`<root>/index.sqlite`).
//!
//! This module ports `src/sase/ace/dismissed_bundle_index/` (schema version
//! 3) from Python into `sase_core` so the dismissed-bundle index is opened by
//! exactly one SQLite library per process. The DDL below is intentionally
//! identical to the Python DDL, so indexes written by Python open without a
//! schema drop.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use chrono::NaiveDateTime;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::agent_archive::wire::{
    DismissedBundleIndexIdentityWire, DismissedBundleIndexProgressWire,
    DismissedBundleIndexQueryWire, DismissedBundleIndexRebuildReportWire,
    DismissedBundleIndexSignatureWire, DismissedBundleSummaryWire,
};
use crate::agent_archive::{
    validate_agent_archive_key, validate_agent_archive_visibility,
    AgentArchiveCapabilitiesWire, AgentArchiveCapabilityFactsWire,
    AgentArchiveKeyWire, AgentArchiveVisibilityWire,
};

pub const ARCHIVE_STORE_SCHEMA_VERSION: i64 = 3;
pub const ARCHIVE_STORE_INDEX_FILENAME: &str = "index.sqlite";

const META_DDL: &str = "
            CREATE TABLE IF NOT EXISTS dismissed_bundle_index_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            ";

const SUMMARIES_DDL: &str = "
        CREATE TABLE IF NOT EXISTS dismissed_bundle_summaries (
            bundle_path TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            raw_suffix TEXT NOT NULL,
            shard TEXT NOT NULL,
            filename TEXT NOT NULL,
            archive_revision INTEGER NOT NULL DEFAULT 1,
            bundle_schema_version INTEGER NOT NULL DEFAULT 0,
            source_username TEXT,
            source_machine TEXT,
            source_run_id TEXT,
            archive_visibility TEXT NOT NULL DEFAULT 'hidden',
            archive_payload_sha256 TEXT,
            historically_viewable INTEGER NOT NULL DEFAULT 1,
            durably_revivable INTEGER NOT NULL DEFAULT 1,
            restartable INTEGER NOT NULL DEFAULT 0,
            missing_requirements TEXT NOT NULL DEFAULT '[]',
            agent_type TEXT NOT NULL,
            cl_name TEXT NOT NULL,
            agent_name TEXT,
            status TEXT NOT NULL,
            start_time TEXT,
            stop_time TEXT,
            dismissed_at TEXT,
            agent_session TEXT,
            agent_session_role TEXT,
            agent_clan TEXT,
            agent_clan_generation TEXT,
            agent_tab TEXT,
            tribe TEXT,
            clan_tribe TEXT,
            revived_at TEXT,
            times_revived INTEGER NOT NULL DEFAULT 0,
            project_file TEXT,
            project_name TEXT,
            model TEXT,
            runtime TEXT,
            llm_provider TEXT,
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
            meta_patch TEXT,
            mtime_ns INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL
        )
        ";

const PROJECTION_DDL: &str = "
        CREATE TABLE IF NOT EXISTS archive_visibility_projection (
            source_username TEXT NOT NULL,
            source_machine TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            visibility TEXT NOT NULL CHECK (
                visibility IN ('hidden', 'visible', 'pinned')
            ),
            dismissed_at TEXT,
            revived_at TEXT,
            pinned_at TEXT,
            times_revived INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY(source_username, source_machine, source_run_id)
        )
        ";

const FTS_DDL: &str = "
        CREATE VIRTUAL TABLE IF NOT EXISTS dismissed_bundle_search_fts
        USING fts5(bundle_path UNINDEXED, archive_search_text)
        ";

const INDEX_DDLS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_dismissed_bundle_raw_suffix \
     ON dismissed_bundle_summaries(raw_suffix)",
    "CREATE INDEX IF NOT EXISTS idx_dismissed_bundle_cl \
     ON dismissed_bundle_summaries(cl_name, meta_patch)",
    "CREATE INDEX IF NOT EXISTS idx_dismissed_bundle_project \
     ON dismissed_bundle_summaries(project_file)",
    "CREATE INDEX IF NOT EXISTS idx_dismissed_bundle_archive_key \
     ON dismissed_bundle_summaries(source_username, source_machine, \
     source_run_id)",
    "CREATE INDEX IF NOT EXISTS idx_dismissed_bundle_visibility \
     ON dismissed_bundle_summaries(archive_visibility)",
];

/// Python `_schema_compatible` required columns.
const REQUIRED_SUMMARY_COLUMNS: &[&str] = &[
    "bundle_path",
    "agent_id",
    "raw_suffix",
    "source_username",
    "source_machine",
    "source_run_id",
    "archive_visibility",
    "historically_viewable",
    "durably_revivable",
    "restartable",
    "missing_requirements",
    "agent_session",
    "agent_session_role",
    "agent_clan",
    "agent_clan_generation",
    "agent_tab",
    "tribe",
    "clan_tribe",
    "mtime_ns",
    "size_bytes",
];

const REQUIRED_PROJECTION_COLUMNS: &[&str] = &[
    "source_username",
    "source_machine",
    "source_run_id",
    "visibility",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ArchiveStoreError {
    #[error("archive store I/O error: {0}")]
    Io(String),
    #[error("archive store SQLite error: {0}")]
    Sqlite(String),
    #[error("invalid bundle: {0}")]
    Bundle(String),
}

impl From<rusqlite::Error> for ArchiveStoreError {
    fn from(error: rusqlite::Error) -> Self {
        ArchiveStoreError::Sqlite(error.to_string())
    }
}

pub fn index_path_for_root(root: &Path) -> PathBuf {
    root.join(ARCHIVE_STORE_INDEX_FILENAME)
}

/// Open `<root>/index.sqlite` for writes, creating the root, enabling WAL,
/// and ensuring the v3 schema. Mirrors Python `_connect(root)`.
pub fn open_archive_store_writable(
    root: &Path,
) -> Result<Connection, ArchiveStoreError> {
    fs::create_dir_all(root)
        .map_err(|e| ArchiveStoreError::Io(e.to_string()))?;
    let conn = Connection::open(index_path_for_root(root))?;
    conn.pragma_update(None, "busy_timeout", 30_000)?;
    set_wal_mode(&conn)?;
    conn.execute_batch("PRAGMA synchronous = NORMAL")?;
    ensure_schema(&conn)?;
    Ok(conn)
}

/// Set WAL journal mode, polling while other connections hold the database.
///
/// `PRAGMA journal_mode` does not honor the busy timeout, so concurrent
/// writers opening fresh connections would otherwise fail immediately with
/// `database is locked` instead of waiting their turn.
fn set_wal_mode(conn: &Connection) -> Result<(), ArchiveStoreError> {
    for _ in 0..600 {
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(_) => {}
            Err(error) if is_busy_error(&error) => {}
            Err(error) => return Err(ArchiveStoreError::from(error)),
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    Err(ArchiveStoreError::Sqlite(
        "timed out setting WAL journal mode: database is locked".to_string(),
    ))
}

fn is_busy_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::DatabaseBusy
    )
}

/// Open an existing `<root>/index.sqlite` read-only. Returns `None` when the
/// file is absent. Never migrates, matching the Python `create=False` path.
pub fn open_archive_store_read_only(
    root: &Path,
) -> Result<Option<Connection>, ArchiveStoreError> {
    let index_path = index_path_for_root(root);
    if !index_path.is_file() {
        return Ok(None);
    }
    let conn = Connection::open_with_flags(
        &index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    conn.pragma_update(None, "busy_timeout", 30_000)?;
    Ok(Some(conn))
}

/// Open an existing `<root>/index.sqlite` read-write without migrating.
/// Returns `None` when the file is absent.
pub fn open_existing_store_writable(
    root: &Path,
) -> Result<Option<Connection>, ArchiveStoreError> {
    let index_path = index_path_for_root(root);
    if !index_path.is_file() {
        return Ok(None);
    }
    let conn = Connection::open(&index_path)?;
    conn.pragma_update(None, "busy_timeout", 30_000)?;
    Ok(Some(conn))
}

fn ensure_schema(conn: &Connection) -> Result<(), ArchiveStoreError> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = ensure_schema_inner(conn);
    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn ensure_schema_inner(conn: &Connection) -> Result<(), ArchiveStoreError> {
    conn.execute_batch(META_DDL)?;
    let existing_version = schema_version_from_row(
        conn.query_row(
            "SELECT value FROM dismissed_bundle_index_meta \
             WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok(),
    );
    if existing_version.is_some()
        && (existing_version != Some(ARCHIVE_STORE_SCHEMA_VERSION)
            || !schema_compatible(conn)?)
    {
        drop_index_tables(conn)?;
        conn.execute_batch("DELETE FROM dismissed_bundle_index_meta")?;
        create_schema(conn)?;
    } else {
        create_schema(conn)?;
    }
    conn.execute(
        "INSERT OR REPLACE INTO dismissed_bundle_index_meta(key, value) \
         VALUES ('schema_version', ?)",
        [ARCHIVE_STORE_SCHEMA_VERSION.to_string()],
    )?;
    Ok(())
}

fn schema_version_from_row(row: Option<String>) -> Option<i64> {
    row.map(|value| value.parse::<i64>().unwrap_or(-1))
}

fn drop_index_tables(conn: &Connection) -> Result<(), ArchiveStoreError> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
           AND name != 'dismissed_bundle_index_meta'",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    for name in names {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {}",
            quote_identifier(&name)
        ))?;
    }
    Ok(())
}

fn schema_compatible(conn: &Connection) -> Result<bool, ArchiveStoreError> {
    let summary_columns = table_columns(conn, "dismissed_bundle_summaries")?;
    let projection_columns =
        table_columns(conn, "archive_visibility_projection")?;
    Ok(REQUIRED_SUMMARY_COLUMNS
        .iter()
        .all(|c| summary_columns.contains(*c))
        && REQUIRED_PROJECTION_COLUMNS
            .iter()
            .all(|c| projection_columns.contains(*c)))
}

fn table_columns(
    conn: &Connection,
    table: &str,
) -> Result<HashSet<String>, ArchiveStoreError> {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&sql)?;
    let mut columns = HashSet::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        columns.insert(row.get::<_, String>(1)?);
    }
    Ok(columns)
}

fn create_schema(conn: &Connection) -> Result<(), ArchiveStoreError> {
    conn.execute_batch(SUMMARIES_DDL)?;
    conn.execute_batch(PROJECTION_DDL)?;
    conn.execute_batch(FTS_DDL)?;
    for ddl in INDEX_DDLS {
        conn.execute_batch(ddl)?;
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn file_signature(path: &Path) -> Result<(i64, i64), ArchiveStoreError> {
    let metadata =
        fs::metadata(path).map_err(|e| ArchiveStoreError::Io(e.to_string()))?;
    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0);
    Ok((mtime_ns, metadata.len() as i64))
}

fn read_bundle(path: &Path) -> Result<JsonValue, ArchiveStoreError> {
    let data = fs::read_to_string(path)
        .map_err(|e| ArchiveStoreError::Io(e.to_string()))?;
    let value: JsonValue = serde_json::from_str(&data)
        .map_err(|e| ArchiveStoreError::Bundle(e.to_string()))?;
    if !value.is_object() {
        return Err(ArchiveStoreError::Bundle(
            "bundle JSON must be an object".to_string(),
        ));
    }
    Ok(value)
}

/// Mirror Python `_bundle_io.iter_bundle_paths`, including the
/// `(shard, name)` descending sort.
fn iter_bundle_paths(root: &Path) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return results;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && is_shard_dir_name(&path) {
            let Ok(children) = fs::read_dir(&path) else {
                continue;
            };
            for child in children.flatten() {
                let child_path = child.path();
                if child_path.is_file()
                    && child_path.extension().and_then(|s| s.to_str())
                        == Some("json")
                {
                    results.push(child_path);
                }
            }
        } else if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("json")
        {
            results.push(path);
        }
    }
    results.sort_by(|left, right| {
        let left_key = (
            path_shard(root, left),
            left.file_name().map(|s| s.to_owned()),
        );
        let right_key = (
            path_shard(root, right),
            right.file_name().map(|s| s.to_owned()),
        );
        right_key.cmp(&left_key)
    });
    results
}

fn is_shard_dir_name(path: &Path) -> bool {
    path.file_name()
        .and_then(|v| v.to_str())
        .is_some_and(|name| {
            name.len() == 6 && name.as_bytes().iter().all(u8::is_ascii_digit)
        })
}

fn path_shard(root: &Path, path: &Path) -> String {
    let parent = path
        .parent()
        .map(|p| {
            p.strip_prefix(root)
                .map(Path::to_path_buf)
                .unwrap_or_else(|_| p.to_path_buf())
        })
        .unwrap_or_default();
    if let Some(first) = parent.components().next() {
        if let Some(name) = first.as_os_str().to_str() {
            let is_shard = name.len() == 6
                && name.as_bytes().iter().all(u8::is_ascii_digit);
            if is_shard {
                return name.to_string();
            }
        }
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(parse_filename_timestamp)
        .map(|ts| ts.format("%Y%m").to_string())
        .unwrap_or_default()
}

/// Mirror `sase.core.paths.parse_filename_timestamp`.
fn parse_filename_timestamp(filename: &str) -> Option<NaiveDateTime> {
    if let Some(ts) = parse_filename_timestamp_suffix(filename) {
        return Some(ts);
    }
    parse_filename_timestamp_prefix(filename)
}

/// Suffix form `-YYmmdd_HHMMSS`, optionally followed by one dotted extension.
///
/// Like Python `re.search`, only the first shape match is tried: when it is
/// not a valid date the result is `None` instead of scanning further.
fn parse_filename_timestamp_suffix(filename: &str) -> Option<NaiveDateTime> {
    let mut start = 0;
    while let Some(rel) = filename[start..].find('-').map(|i| i + start) {
        let rest = &filename[rel + 1..];
        if rest.len() >= 13
            && rest.as_bytes()[..6].iter().all(u8::is_ascii_digit)
            && rest.as_bytes()[6] == b'_'
            && rest.as_bytes()[7..13].iter().all(u8::is_ascii_digit)
        {
            let tail = &rest[13..];
            let tail_ok = tail.is_empty()
                || (tail.starts_with('.')
                    && !tail[1..].is_empty()
                    && tail[1..]
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '_'));
            if tail_ok {
                return NaiveDateTime::parse_from_str(
                    &rest[..13],
                    "%y%m%d_%H%M%S",
                )
                .ok();
            }
        }
        start = rel + 1;
    }
    None
}

/// Prefix form `YYYYmmddHHMMSS`, followed by `__`, `.`, or end of name.
fn parse_filename_timestamp_prefix(filename: &str) -> Option<NaiveDateTime> {
    if filename.len() >= 14
        && filename.as_bytes()[..14].iter().all(u8::is_ascii_digit)
    {
        let tail = &filename[14..];
        if tail.is_empty() || tail.starts_with("__") || tail.starts_with('.') {
            return NaiveDateTime::parse_from_str(
                &filename[..14],
                "%Y%m%d%H%M%S",
            )
            .ok();
        }
    }
    None
}

/// Canonical payload hash identical to Python's `_archive_payload_hash`:
/// the bundle minus `archive_payload_sha256`, dumped with sorted keys and
/// compact separators, then hex SHA-256.
pub fn archive_payload_hash_canonical(bundle: &JsonValue) -> String {
    let mut payload = bundle.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("archive_payload_sha256");
    }
    let canonical = canonical_json(&payload);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(&encoded))
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (key, val) in map {
                sorted.insert(key.clone(), canonical_json(val));
            }
            JsonValue::Object(sorted.into_iter().collect())
        }
        JsonValue::Array(items) => {
            JsonValue::Array(items.iter().map(canonical_json).collect())
        }
        other => other.clone(),
    }
}

fn bundle_object(
    bundle: &JsonValue,
) -> Result<&serde_json::Map<String, JsonValue>, ArchiveStoreError> {
    bundle.as_object().ok_or_else(|| {
        ArchiveStoreError::Bundle("bundle JSON must be an object".to_string())
    })
}

fn required_str(
    bundle: &serde_json::Map<String, JsonValue>,
    key: &str,
) -> Result<String, ArchiveStoreError> {
    match bundle.get(key) {
        Some(JsonValue::String(value)) if !value.is_empty() => {
            Ok(value.clone())
        }
        _ => Err(ArchiveStoreError::Bundle(format!("bundle missing {key}"))),
    }
}

fn string_or_default(value: Option<&JsonValue>, default: &str) -> String {
    match value {
        Some(JsonValue::String(text)) if !text.is_empty() => text.clone(),
        _ => default.to_string(),
    }
}

/// Mirror Python `optional_str`: `None` stays `None`, strings pass through,
/// anything else is stringified.
fn optional_str(value: Option<&JsonValue>) -> Option<String> {
    match value {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::String(text)) => Some(text.clone()),
        Some(JsonValue::Bool(flag)) => Some(if *flag {
            "True".to_string()
        } else {
            "False".to_string()
        }),
        Some(JsonValue::Number(number)) => Some(number.to_string()),
        Some(other) => {
            serde_json::to_string(other).ok().filter(|s| !s.is_empty())
        }
    }
}

/// Mirror Python `optional_int`: JSON integers pass through (bools included,
/// as in Python), numeric strings parse, everything else is `None`.
fn optional_int(value: Option<&JsonValue>) -> Option<i64> {
    match value {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Bool(flag)) => Some(i64::from(*flag)),
        Some(JsonValue::Number(number)) => number.as_i64(),
        Some(JsonValue::String(text)) => text.parse::<i64>().ok(),
        Some(_) => None,
    }
}

fn non_empty_text(value: Option<&JsonValue>) -> Option<String> {
    match value {
        Some(JsonValue::String(text)) if !text.is_empty() => Some(text.clone()),
        _ => None,
    }
}

fn archive_key_from_bundle(
    bundle: &serde_json::Map<String, JsonValue>,
) -> Result<Option<AgentArchiveKeyWire>, ArchiveStoreError> {
    if let (Some(username), Some(machine), Some(run_id)) = (
        non_empty_text(bundle.get("source_username")),
        non_empty_text(bundle.get("source_machine")),
        non_empty_text(bundle.get("source_run_id")),
    ) {
        return validate_key(&username, &machine, &run_id).map(Some);
    }
    if let Some(JsonValue::Object(owner)) = bundle.get("imported_source_owner")
    {
        let username = non_empty_text(owner.get("username"));
        let machine = non_empty_text(owner.get("machine_name"));
        let mut run_id = non_empty_text(bundle.get("imported_source_run_id"));
        if run_id.is_none() {
            if let Some(JsonValue::Object(step_output)) =
                bundle.get("step_output")
            {
                run_id =
                    non_empty_text(step_output.get("imported_source_run_id"));
            }
        }
        if let (Some(username), Some(machine), Some(run_id)) =
            (username, machine, run_id)
        {
            return validate_key(&username, &machine, &run_id).map(Some);
        }
    }
    Ok(None)
}

fn validate_key(
    username: &str,
    machine: &str,
    run_id: &str,
) -> Result<AgentArchiveKeyWire, ArchiveStoreError> {
    validate_agent_archive_key(AgentArchiveKeyWire {
        source_username: username.to_string(),
        source_machine: machine.to_string(),
        source_run_id: run_id.to_string(),
    })
    .map_err(ArchiveStoreError::Bundle)
}

fn capabilities_from_bundle(
    bundle: &serde_json::Map<String, JsonValue>,
) -> Result<AgentArchiveCapabilitiesWire, ArchiveStoreError> {
    let asserted = match bundle.get("archive_capabilities") {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Object(_)) => {
            let value = JsonValue::Object(
                bundle["archive_capabilities"]
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
            );
            Some(capabilities_from_wire_value(&value)?)
        }
        Some(_) => None,
    };
    let facts = AgentArchiveCapabilityFactsWire {
        has_metadata: non_empty_text(bundle.get("raw_suffix")).is_some()
            && (non_empty_text(bundle.get("agent_name")).is_some()
                || non_empty_text(bundle.get("cl_name")).is_some()
                || non_empty_text(bundle.get("patch_name")).is_some()),
        has_state: non_empty_text(bundle.get("status")).is_some()
            && (non_empty_text(bundle.get("start_time")).is_some()
                || non_empty_text(bundle.get("stop_time")).is_some()
                || non_empty_text(bundle.get("raw_suffix")).is_some()),
        has_commits: true,
        loader_reconstructible: non_empty_text(bundle.get("raw_suffix"))
            .is_some(),
        has_prompt: ["raw_xprompt", "raw_prompt", "prompt"]
            .iter()
            .any(|key| non_empty_text(bundle.get(*key)).is_some()),
        has_model: non_empty_text(bundle.get("model")).is_some(),
        has_llm_provider: non_empty_text(bundle.get("llm_provider")).is_some(),
        has_reasoning_effort: non_empty_text(bundle.get("reasoning_effort"))
            .is_some(),
    };
    crate::agent_archive::validate_agent_archive_capabilities(
        crate::agent_archive::AgentArchiveCapabilityValidationRequestWire {
            facts,
            asserted,
        },
    )
    .map_err(ArchiveStoreError::Bundle)
}

fn capabilities_from_wire_value(
    value: &JsonValue,
) -> Result<AgentArchiveCapabilitiesWire, ArchiveStoreError> {
    serde_json::from_value::<AgentArchiveCapabilitiesWire>(value.clone())
        .map_err(|e| {
            ArchiveStoreError::Bundle(format!(
                "archive capability wire is invalid: {e}"
            ))
        })
        .and_then(|caps| {
            if value.get("missing_requirements").is_some_and(|reqs| {
                !reqs.is_array()
                    || reqs.as_array().is_some_and(|items| {
                        items.iter().any(|item| !item.is_string())
                    })
            }) {
                return Err(ArchiveStoreError::Bundle(
                    "archive capability missing_requirements must be strings"
                        .to_string(),
                ));
            }
            Ok(caps)
        })
}

fn is_workflow_child(bundle: &serde_json::Map<String, JsonValue>) -> bool {
    if let Some(JsonValue::Bool(explicit)) = bundle.get("is_workflow_child") {
        return *explicit;
    }
    !matches!(bundle.get("parent_workflow"), None | Some(JsonValue::Null))
        || !matches!(
            bundle.get("parent_timestamp"),
            None | Some(JsonValue::Null)
        )
}

fn meta_patch(bundle: &serde_json::Map<String, JsonValue>) -> Option<String> {
    let JsonValue::Object(step_output) = bundle.get("step_output")? else {
        return None;
    };
    if let Some(JsonValue::String(patch)) = step_output.get("meta_patch") {
        if !patch.trim().is_empty() {
            return Some(patch.trim().to_string());
        }
    }
    if let Some(JsonValue::String(spec)) = step_output.get("meta_changespec") {
        if !spec.trim().is_empty() {
            return Some(spec.trim().to_string());
        }
    }
    if let Some(JsonValue::String(new_cl)) = step_output.get("meta_new_cl") {
        let value = new_cl.trim();
        if !value.is_empty() {
            if let Some(index) = value.rfind(" (") {
                if index > 0 {
                    let head = value[..index].trim();
                    if !head.is_empty() {
                        return Some(head.to_string());
                    }
                }
            }
            return Some(value.to_string());
        }
    }
    None
}

fn project_name_from_file(project_file: Option<&str>) -> Option<String> {
    let file = project_file.filter(|s| !s.is_empty())?;
    let expanded = file.strip_prefix("~/").unwrap_or(file);
    let expanded = if file == "~" { "" } else { expanded };
    let parent = Path::new(expanded).parent()?;
    let name = parent.file_name()?.to_str()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn missing_requirements_from_row(value: Option<&str>) -> Vec<String> {
    let Some(raw) = value.filter(|s| !s.is_empty()) else {
        return Vec::new();
    };
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

/// Mirror Python `summary_from_bundle`.
fn summary_from_bundle(
    root: &Path,
    path: &Path,
    bundle: &JsonValue,
) -> Result<(DismissedBundleSummaryWire, (i64, i64)), ArchiveStoreError> {
    let map = bundle_object(bundle)?;
    let raw_suffix = required_str(map, "raw_suffix")?;
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let shard = path_shard(root, path);
    let project_file = optional_str(map.get("project_file"));
    let llm_provider = optional_str(map.get("llm_provider"));
    let key = archive_key_from_bundle(map)?;
    let capabilities = capabilities_from_bundle(map)?;
    let has_archive_payload = matches!(
        map.get("archive_schema_version"),
        Some(JsonValue::Number(_))
    ) || matches!(
        map.get("archive_payload_sha256"),
        Some(JsonValue::String(_))
    ) || matches!(
        map.get("archive_capabilities"),
        Some(JsonValue::Object(_))
    );
    let visibility_default = if has_archive_payload {
        optional_str(map.get("archive_visibility"))
    } else {
        None
    };
    let visibility =
        validate_agent_archive_visibility(AgentArchiveVisibilityWire {
            visibility: visibility_default
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "hidden".to_string()),
        })
        .map_err(ArchiveStoreError::Bundle)?
        .visibility;
    let (source_username, source_machine, source_run_id, agent_id) = match &key
    {
        Some(key) => (
            Some(key.source_username.clone()),
            Some(key.source_machine.clone()),
            Some(key.source_run_id.clone()),
            format!(
                "{}.{}@{}",
                key.source_username, key.source_machine, key.source_run_id
            ),
        ),
        None => (None, None, None, format!("legacy:{raw_suffix}")),
    };
    let stored_hash = optional_str(map.get("archive_payload_sha256"))
        .filter(|s| !s.is_empty());
    let archive_payload_sha256 =
        stored_hash.or_else(|| Some(archive_payload_hash_canonical(bundle)));
    let workflow = optional_str(map.get("workflow"));
    let signature = file_signature(path)?;
    Ok((
        DismissedBundleSummaryWire {
            raw_suffix,
            bundle_path: path.to_string_lossy().into_owned(),
            shard,
            filename,
            agent_id,
            source_username,
            source_machine,
            source_run_id,
            archive_visibility: visibility,
            archive_payload_sha256,
            historically_viewable: capabilities.historically_viewable,
            durably_revivable: capabilities.durably_revivable,
            restartable: capabilities.restartable,
            missing_requirements: capabilities.missing_requirements,
            agent_type: string_or_default(map.get("agent_type"), "run"),
            cl_name: string_or_default(map.get("cl_name"), "unknown"),
            agent_name: optional_str(map.get("agent_name")),
            status: string_or_default(map.get("status"), "DONE"),
            start_time: optional_str(map.get("start_time")),
            stop_time: optional_str(map.get("stop_time")),
            dismissed_at: optional_str(map.get("dismissed_at")),
            revived_at: optional_str(map.get("revived_at")),
            times_revived: optional_int(map.get("times_revived")).unwrap_or(0),
            project_file: project_file.clone(),
            project_name: project_name_from_file(project_file.as_deref()),
            model: optional_str(map.get("model")),
            runtime: workflow.clone(),
            llm_provider,
            vcs_provider: optional_str(map.get("vcs_provider")),
            workflow,
            is_workflow_child: is_workflow_child(map),
            parent_timestamp: optional_str(map.get("parent_timestamp")),
            step_index: optional_int(map.get("step_index")),
            step_name: optional_str(map.get("step_name")),
            step_type: optional_str(map.get("step_type")),
            retry_of_timestamp: optional_str(map.get("retry_of_timestamp")),
            retried_as_timestamp: optional_str(map.get("retried_as_timestamp")),
            retry_chain_root_timestamp: optional_str(
                map.get("retry_chain_root_timestamp"),
            ),
            retry_attempt: optional_int(map.get("retry_attempt")).unwrap_or(0),
            meta_patch: meta_patch(map),
            agent_session: optional_str(map.get("agent_session")),
            agent_session_role: optional_str(map.get("agent_session_role")),
            agent_clan: optional_str(map.get("agent_clan")),
            agent_clan_generation: optional_str(
                map.get("agent_clan_generation"),
            ),
            agent_tab: optional_str(map.get("agent_tab")),
            tribe: optional_str(map.get("tribe")),
            clan_tribe: optional_str(map.get("clan_tribe")),
        },
        signature,
    ))
}

fn with_immediate<T>(
    conn: &Connection,
    work: impl FnOnce(&Connection) -> Result<T, ArchiveStoreError>,
) -> Result<T, ArchiveStoreError> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    match work(conn) {
        Ok(value) => {
            conn.execute_batch("COMMIT")?;
            Ok(value)
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Insert or update one bundle summary row, mirroring
/// `upsert_bundle_summary(root, path, bundle)`.
pub fn upsert_bundle_summary(
    root: &Path,
    path: &Path,
    bundle: &JsonValue,
) -> Result<(), ArchiveStoreError> {
    let (summary, (mtime_ns, size_bytes)) =
        summary_from_bundle(root, path, bundle)?;
    let conn = open_archive_store_writable(root)?;
    with_immediate(&conn, |conn| {
        upsert_summary_row(conn, &summary, mtime_ns, size_bytes)
    })
}

fn upsert_summary_row(
    conn: &Connection,
    summary: &DismissedBundleSummaryWire,
    mtime_ns: i64,
    size_bytes: i64,
) -> Result<(), ArchiveStoreError> {
    conn.execute(
        "INSERT INTO dismissed_bundle_summaries (
            bundle_path, agent_id, raw_suffix, shard, filename,
            source_username, source_machine, source_run_id, archive_visibility,
            archive_payload_sha256, historically_viewable, durably_revivable,
            restartable, missing_requirements, agent_type, cl_name, agent_name,
            status, start_time, stop_time, dismissed_at, revived_at,
            agent_session, agent_session_role, agent_clan,
            agent_clan_generation, agent_tab, tribe, clan_tribe,
            times_revived, project_file, project_name, model, runtime,
            llm_provider, vcs_provider, workflow, is_workflow_child,
            parent_timestamp, step_index, step_name, step_type,
            retry_of_timestamp, retried_as_timestamp, retry_chain_root_timestamp,
            retry_attempt, meta_changespec, meta_patch, mtime_ns, size_bytes
        )
        VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48, ?49, ?50
        )
        ON CONFLICT(bundle_path) DO UPDATE SET
            agent_id=excluded.agent_id,
            raw_suffix=excluded.raw_suffix,
            shard=excluded.shard,
            filename=excluded.filename,
            source_username=excluded.source_username,
            source_machine=excluded.source_machine,
            source_run_id=excluded.source_run_id,
            archive_visibility=excluded.archive_visibility,
            archive_payload_sha256=excluded.archive_payload_sha256,
            historically_viewable=excluded.historically_viewable,
            durably_revivable=excluded.durably_revivable,
            restartable=excluded.restartable,
            missing_requirements=excluded.missing_requirements,
            agent_type=excluded.agent_type,
            cl_name=excluded.cl_name,
            agent_name=excluded.agent_name,
            status=excluded.status,
            start_time=excluded.start_time,
            stop_time=excluded.stop_time,
            dismissed_at=excluded.dismissed_at,
            agent_session=excluded.agent_session,
            agent_session_role=excluded.agent_session_role,
            agent_clan=excluded.agent_clan,
            agent_clan_generation=excluded.agent_clan_generation,
            agent_tab=excluded.agent_tab,
            tribe=excluded.tribe,
            clan_tribe=excluded.clan_tribe,
            revived_at=excluded.revived_at,
            times_revived=excluded.times_revived,
            project_file=excluded.project_file,
            project_name=excluded.project_name,
            model=excluded.model,
            runtime=excluded.runtime,
            llm_provider=excluded.llm_provider,
            vcs_provider=excluded.vcs_provider,
            workflow=excluded.workflow,
            is_workflow_child=excluded.is_workflow_child,
            parent_timestamp=excluded.parent_timestamp,
            step_index=excluded.step_index,
            step_name=excluded.step_name,
            step_type=excluded.step_type,
            retry_of_timestamp=excluded.retry_of_timestamp,
            retried_as_timestamp=excluded.retried_as_timestamp,
            retry_chain_root_timestamp=excluded.retry_chain_root_timestamp,
            retry_attempt=excluded.retry_attempt,
            meta_changespec=excluded.meta_changespec,
            meta_patch=excluded.meta_patch,
            mtime_ns=excluded.mtime_ns,
            size_bytes=excluded.size_bytes",
        rusqlite::params![
            summary.bundle_path,
            summary.agent_id,
            summary.raw_suffix,
            summary.shard,
            summary.filename,
            summary.source_username,
            summary.source_machine,
            summary.source_run_id,
            summary.archive_visibility,
            summary.archive_payload_sha256,
            i64::from(summary.historically_viewable),
            i64::from(summary.durably_revivable),
            i64::from(summary.restartable),
            serde_json::to_string(&summary.missing_requirements)
                .unwrap_or_else(|_| "[]".to_string()),
            summary.agent_type,
            summary.cl_name,
            summary.agent_name,
            summary.status,
            summary.start_time,
            summary.stop_time,
            summary.dismissed_at,
            summary.revived_at,
            summary.agent_session,
            summary.agent_session_role,
            summary.agent_clan,
            summary.agent_clan_generation,
            summary.agent_tab,
            summary.tribe,
            summary.clan_tribe,
            summary.times_revived,
            summary.project_file,
            summary.project_name,
            summary.model,
            summary.runtime,
            summary.llm_provider,
            summary.vcs_provider,
            summary.workflow,
            i64::from(summary.is_workflow_child),
            summary.parent_timestamp,
            summary.step_index,
            summary.step_name,
            summary.step_type,
            summary.retry_of_timestamp,
            summary.retried_as_timestamp,
            summary.retry_chain_root_timestamp,
            summary.retry_attempt,
            summary.meta_patch,
            summary.meta_patch,
            mtime_ns,
            size_bytes,
        ],
    )?;
    if summary
        .source_username
        .as_deref()
        .is_some_and(|s| !s.is_empty())
        && summary
            .source_machine
            .as_deref()
            .is_some_and(|s| !s.is_empty())
        && summary
            .source_run_id
            .as_deref()
            .is_some_and(|s| !s.is_empty())
    {
        conn.execute(
            "INSERT INTO archive_visibility_projection (
                source_username, source_machine, source_run_id, visibility,
                dismissed_at, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(source_username, source_machine, source_run_id)
            DO NOTHING",
            rusqlite::params![
                summary.source_username,
                summary.source_machine,
                summary.source_run_id,
                summary.archive_visibility,
                summary.dismissed_at,
            ],
        )?;
    }
    Ok(())
}

/// Delete all rows for the given raw suffixes. Empty input is a no-op.
pub fn delete_bundle_summaries_for_suffixes(
    root: &Path,
    suffixes: &[String],
) -> Result<(), ArchiveStoreError> {
    if suffixes.is_empty() {
        return Ok(());
    }
    let conn = open_archive_store_writable(root)?;
    with_immediate(&conn, |conn| {
        for suffix in suffixes {
            conn.execute(
                "DELETE FROM dismissed_bundle_summaries WHERE raw_suffix = ?",
                [suffix],
            )?;
        }
        Ok(())
    })
}

const SUMMARY_SELECT: &str = "SELECT s.*,
        COALESCE(v.visibility, s.archive_visibility)
            AS effective_archive_visibility,
        COALESCE(v.revived_at, s.revived_at) AS effective_revived_at,
        COALESCE(v.times_revived, s.times_revived)
            AS effective_times_revived
        FROM dismissed_bundle_summaries s
        LEFT JOIN archive_visibility_projection v
        ON v.source_username = s.source_username
        AND v.source_machine = s.source_machine
        AND v.source_run_id = s.source_run_id";

/// Query summaries with the Python `query_summaries` filters and ordering.
/// Returns `None` when no usable index exists, matching Python.
pub fn query_summaries(
    root: &Path,
    request: DismissedBundleIndexQueryWire,
) -> Result<Option<Vec<DismissedBundleSummaryWire>>, ArchiveStoreError> {
    if !index_path_for_root(root).is_file() {
        return Ok(None);
    }
    if request.suffixes.as_ref().is_some_and(Vec::is_empty) {
        return Ok(Some(Vec::new()));
    }
    let conn = match open_archive_store_writable(root) {
        Ok(conn) => conn,
        Err(_) => return Ok(None),
    };
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    if let Some(suffixes) = &request.suffixes {
        let mut ordered = suffixes.clone();
        ordered.sort();
        let placeholders =
            ordered.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        clauses.push(format!("s.raw_suffix IN ({placeholders})"));
        for suffix in &ordered {
            params.push(rusqlite::types::Value::Text(suffix.clone()));
        }
    }
    if let Some(cl_name) = &request.cl_name {
        clauses.push("(s.cl_name = ? OR s.meta_patch = ?)".to_string());
        params.push(rusqlite::types::Value::Text(cl_name.clone()));
        params.push(rusqlite::types::Value::Text(cl_name.clone()));
    }
    if let Some(project_name) = &request.project_name {
        clauses.push("s.project_file LIKE ?".to_string());
        params
            .push(rusqlite::types::Value::Text(format!("%/{project_name}/%")));
    }
    if request.top_level_only {
        clauses.push("s.is_workflow_child = 0".to_string());
    }
    if let Some(visibility) = &request.visibility {
        clauses.push(
            "COALESCE(v.visibility, s.archive_visibility) = ?".to_string(),
        );
        params.push(rusqlite::types::Value::Text(visibility.clone()));
    }
    let mut sql = SUMMARY_SELECT.to_string();
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    sql.push_str(
        " ORDER BY COALESCE(s.start_time, s.raw_suffix) DESC, s.filename ASC",
    );
    if let Some(limit) = request.limit {
        sql.push_str(" LIMIT ?");
        params.push(rusqlite::types::Value::Integer(limit.max(0)));
    }
    if let Some(offset) = request.offset {
        if request.limit.is_none() {
            sql.push_str(" LIMIT -1");
        }
        sql.push_str(" OFFSET ?");
        params.push(rusqlite::types::Value::Integer(offset.max(0)));
    }
    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(_) => return Ok(None),
    };
    let rows = match stmt.query_map(params_from_iter(params.iter()), |row| {
        summary_wire_from_row(row)
    }) {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    let mut out = Vec::new();
    for row in rows {
        match row {
            Ok(summary) => out.push(summary),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// Return indexed bundle paths for *suffixes*, or `None` when unusable.
pub fn query_bundle_paths_by_suffixes(
    root: &Path,
    suffixes: Vec<String>,
) -> Result<Option<Vec<String>>, ArchiveStoreError> {
    let summaries = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            suffixes: Some(suffixes),
            visibility: Some("hidden".to_string()),
            ..Default::default()
        },
    )?;
    Ok(summaries
        .map(|rows| rows.into_iter().map(|row| row.bundle_path).collect()))
}

fn summary_wire_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DismissedBundleSummaryWire> {
    let column_names: HashSet<&str> =
        row.as_ref().column_names().into_iter().collect();
    let get_opt = |column: &str| -> rusqlite::Result<Option<String>> {
        if column_names.contains(column) {
            row.get(column)
        } else {
            Ok(None)
        }
    };
    let get_text = |column: &str, default: &str| -> rusqlite::Result<String> {
        Ok(get_opt(column)?.unwrap_or_else(|| default.to_string()))
    };
    let archive_visibility = get_opt("effective_archive_visibility")?
        .filter(|s| !s.is_empty())
        .or(get_opt("archive_visibility")?)
        .unwrap_or_else(|| "hidden".to_string());
    let revived_at =
        get_opt("effective_revived_at")?.or(get_opt("revived_at")?);
    let times_revived = row
        .get::<_, Option<i64>>("effective_times_revived")
        .ok()
        .flatten()
        .or(row.get::<_, Option<i64>>("times_revived")?)
        .unwrap_or(0);
    Ok(DismissedBundleSummaryWire {
        raw_suffix: row.get("raw_suffix")?,
        bundle_path: row.get("bundle_path")?,
        shard: get_text("shard", "")?,
        filename: get_text("filename", "")?,
        agent_id: get_text("agent_id", "")?,
        source_username: get_opt("source_username")?,
        source_machine: get_opt("source_machine")?,
        source_run_id: get_opt("source_run_id")?,
        archive_visibility,
        archive_payload_sha256: get_opt("archive_payload_sha256")?,
        historically_viewable: row
            .get::<_, Option<i64>>("historically_viewable")?
            .unwrap_or(1)
            != 0,
        durably_revivable: row
            .get::<_, Option<i64>>("durably_revivable")?
            .unwrap_or(1)
            != 0,
        restartable: row.get::<_, Option<i64>>("restartable")?.unwrap_or(0)
            != 0,
        missing_requirements: missing_requirements_from_row(
            get_opt("missing_requirements")?.as_deref(),
        ),
        agent_type: get_text("agent_type", "run")?,
        cl_name: get_text("cl_name", "unknown")?,
        agent_name: get_opt("agent_name")?,
        status: get_text("status", "DONE")?,
        start_time: get_opt("start_time")?,
        stop_time: get_opt("stop_time")?,
        dismissed_at: get_opt("dismissed_at")?,
        revived_at,
        times_revived,
        project_file: get_opt("project_file")?,
        project_name: get_opt("project_name")?,
        model: get_opt("model")?,
        runtime: get_opt("runtime")?,
        llm_provider: get_opt("llm_provider")?,
        vcs_provider: get_opt("vcs_provider")?,
        workflow: get_opt("workflow")?,
        is_workflow_child: row
            .get::<_, Option<i64>>("is_workflow_child")?
            .unwrap_or(0)
            != 0,
        parent_timestamp: get_opt("parent_timestamp")?,
        step_index: row.get::<_, Option<i64>>("step_index")?,
        step_name: get_opt("step_name")?,
        step_type: get_opt("step_type")?,
        retry_of_timestamp: get_opt("retry_of_timestamp")?,
        retried_as_timestamp: get_opt("retried_as_timestamp")?,
        retry_chain_root_timestamp: get_opt("retry_chain_root_timestamp")?,
        retry_attempt: row.get::<_, Option<i64>>("retry_attempt")?.unwrap_or(0),
        meta_patch: get_opt("meta_patch")?,
        agent_session: get_opt("agent_session")?,
        agent_session_role: get_opt("agent_session_role")?,
        agent_clan: get_opt("agent_clan")?,
        agent_clan_generation: get_opt("agent_clan_generation")?,
        agent_tab: get_opt("agent_tab")?,
        tribe: get_opt("tribe")?,
        clan_tribe: get_opt("clan_tribe")?,
    })
}

/// Set local visibility for all indexed rows matching *suffixes*,
/// returning the changed count. Mirrors `_schema.set_visibility_for_suffixes`.
pub fn set_visibility_for_suffixes(
    root: &Path,
    suffixes: &[String],
    visibility: &str,
    revived_at: Option<&str>,
    dismissed_at: Option<&str>,
    pinned_at: Option<&str>,
) -> Result<i64, ArchiveStoreError> {
    if suffixes.is_empty() {
        return Ok(0);
    }
    let conn = open_archive_store_writable(root)?;
    with_immediate(&conn, |conn| {
        set_visibility_for_suffixes_inner(
            conn,
            suffixes,
            visibility,
            revived_at,
            dismissed_at,
            pinned_at,
        )
    })
}

/// One `(source_username, source_machine, source_run_id, bundle_path)` row.
type VisibilitySourceRow =
    (Option<String>, Option<String>, Option<String>, String);

fn set_visibility_for_suffixes_inner(
    conn: &Connection,
    suffixes: &[String],
    visibility: &str,
    revived_at: Option<&str>,
    dismissed_at: Option<&str>,
    pinned_at: Option<&str>,
) -> Result<i64, ArchiveStoreError> {
    let mut ordered = suffixes.to_vec();
    ordered.sort();
    let placeholders =
        ordered.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT DISTINCT source_username, source_machine, source_run_id, \
         bundle_path FROM dismissed_bundle_summaries \
         WHERE raw_suffix IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows: Vec<VisibilitySourceRow> = stmt
        .query_map(params_from_iter(ordered.iter()), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    let mut changed = 0;
    for (username, machine, run_id, bundle_path) in rows {
        let full_key: Option<(String, String, String)> =
            match (username, machine, run_id) {
                (Some(username), Some(machine), Some(run_id))
                    if !username.is_empty()
                        && !machine.is_empty()
                        && !run_id.is_empty() =>
                {
                    Some((username, machine, run_id))
                }
                _ => None,
            };
        if let Some((username, machine, run_id)) = full_key {
            conn.execute(
                "INSERT INTO archive_visibility_projection (
                    source_username, source_machine, source_run_id, visibility,
                    dismissed_at, revived_at, pinned_at, times_revived,
                    updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                    COALESCE(?6, ?5, ?7))
                ON CONFLICT(source_username, source_machine, source_run_id)
                DO UPDATE SET
                    visibility=excluded.visibility,
                    dismissed_at=COALESCE(excluded.dismissed_at, dismissed_at),
                    revived_at=COALESCE(excluded.revived_at, revived_at),
                    pinned_at=COALESCE(excluded.pinned_at, pinned_at),
                    updated_at=COALESCE(
                        excluded.updated_at,
                        archive_visibility_projection.updated_at
                    ),
                    times_revived=archive_visibility_projection.times_revived
                        + excluded.times_revived",
                rusqlite::params![
                    username,
                    machine,
                    run_id,
                    visibility,
                    dismissed_at,
                    revived_at,
                    pinned_at,
                    i64::from(revived_at.is_some()),
                ],
            )?;
        }
        conn.execute(
            "UPDATE dismissed_bundle_summaries
             SET archive_visibility = ?1,
                 revived_at = COALESCE(?2, revived_at),
                 dismissed_at = COALESCE(?3, dismissed_at),
                 times_revived = times_revived + ?4
             WHERE bundle_path = ?5",
            rusqlite::params![
                visibility,
                revived_at,
                dismissed_at,
                i64::from(revived_at.is_some()),
                bundle_path,
            ],
        )?;
        changed += 1;
    }
    Ok(changed)
}

/// Distinct `(agent_type, cl_name, raw_suffix)` identities for hidden rows,
/// or `None` when no usable index exists.
pub fn query_summary_identities(
    root: &Path,
) -> Result<Option<Vec<DismissedBundleIndexIdentityWire>>, ArchiveStoreError> {
    if !index_path_for_root(root).is_file() {
        return Ok(None);
    }
    let conn = match open_archive_store_writable(root) {
        Ok(conn) => conn,
        Err(_) => return Ok(None),
    };
    let mut stmt = match conn.prepare(
        "SELECT DISTINCT agent_type,
            CASE WHEN s.cl_name = '' THEN 'unknown' ELSE s.cl_name END,
            NULLIF(s.raw_suffix, '')
         FROM dismissed_bundle_summaries s
         LEFT JOIN archive_visibility_projection v
         ON v.source_username = s.source_username
         AND v.source_machine = s.source_machine
         AND v.source_run_id = s.source_run_id
         WHERE COALESCE(v.visibility, s.archive_visibility) = 'hidden'",
    ) {
        Ok(stmt) => stmt,
        Err(_) => return Ok(None),
    };
    let rows = match stmt.query_map([], |row| {
        Ok(DismissedBundleIndexIdentityWire {
            agent_type: row.get(0)?,
            cl_name: row.get(1)?,
            raw_suffix: row.get(2)?,
        })
    }) {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    let mut out = Vec::new();
    for row in rows {
        match row {
            Ok(identity) => out.push(identity),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// Cheap `(schema_version, mtime_ns, size, row_count)` signature, or `None`.
pub fn index_signature(
    root: &Path,
) -> Option<DismissedBundleIndexSignatureWire> {
    let db_path = index_path_for_root(root);
    if !db_path.is_file() {
        return None;
    }
    let stat = fs::metadata(&db_path).ok()?;
    let mtime_ns = stat
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i64)?;
    let conn = open_archive_store_read_only(root).ok()??;
    let version_row: Option<String> = conn
        .query_row(
            "SELECT value FROM dismissed_bundle_index_meta \
             WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .ok();
    let version = version_row
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(0);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dismissed_bundle_summaries",
            [],
            |row| row.get(0),
        )
        .ok()?;
    Some(DismissedBundleIndexSignatureWire {
        schema_version: version,
        mtime_ns,
        size_bytes: stat.len() as i64,
        row_count: count,
    })
}

fn rebuild_progress_map() -> &'static Mutex<HashMap<String, i64>> {
    static MAP: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rebuild_root_key(root: &Path) -> String {
    fs::canonicalize(root)
        .unwrap_or_else(|_| root.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn set_rebuild_progress(key: &str, indexed_rows: i64) {
    if let Ok(mut map) = rebuild_progress_map().lock() {
        map.insert(key.to_string(), indexed_rows);
    }
}

fn clear_rebuild_progress(key: &str) {
    if let Ok(mut map) = rebuild_progress_map().lock() {
        map.remove(key);
    }
}

struct RebuildProgressGuard {
    key: String,
}

impl RebuildProgressGuard {
    fn new(root: &Path) -> Self {
        let key = rebuild_root_key(root);
        set_rebuild_progress(&key, 0);
        Self { key }
    }
}

impl Drop for RebuildProgressGuard {
    fn drop(&mut self) {
        clear_rebuild_progress(&self.key);
    }
}

/// Cheap in-process view of an active rebuild, without touching SQLite.
pub fn rebuild_progress(root: &Path) -> DismissedBundleIndexProgressWire {
    let key = rebuild_root_key(root);
    let indexed_rows = rebuild_progress_map()
        .lock()
        .ok()
        .and_then(|map| map.get(&key).copied());
    DismissedBundleIndexProgressWire {
        rebuilding: indexed_rows.is_some(),
        indexed_rows: indexed_rows.unwrap_or(0),
    }
}

fn is_corruption_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            code.code == rusqlite::ErrorCode::NotADatabase
                || code.code == rusqlite::ErrorCode::DatabaseCorrupt
                || message
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains("not a database")
                || message
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains("malformed")
        }
        _ => {
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("not a database")
                || error.to_string().to_ascii_lowercase().contains("malformed")
        }
    }
}

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn file_identity(_metadata: &fs::Metadata) -> Option<(u64, u64)> {
    None
}

/// Rename a corrupt index (plus `-wal`/`-shm` siblings) aside, but only when
/// the file on disk is still the file that failed to open.
fn quarantine_corrupt_index(
    index_path: &Path,
) -> Result<PathBuf, ArchiveStoreError> {
    use chrono::Utc;
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let file_name = index_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| ARCHIVE_STORE_INDEX_FILENAME.to_string());
    let mut quarantined =
        index_path.with_file_name(format!("{file_name}.corrupt-{stamp}"));
    let mut serial = 2;
    while quarantined.exists() {
        quarantined = index_path
            .with_file_name(format!("{file_name}.corrupt-{stamp}-{serial}"));
        serial += 1;
    }
    fs::rename(index_path, &quarantined)
        .map_err(|e| ArchiveStoreError::Io(e.to_string()))?;
    for suffix in ["-wal", "-shm"] {
        let sidecar =
            PathBuf::from(format!("{}{suffix}", index_path.display()));
        if sidecar.is_file() {
            let target =
                PathBuf::from(format!("{}{suffix}", quarantined.display()));
            let _ = fs::rename(&sidecar, &target);
        }
    }
    Ok(quarantined)
}

/// Rebuild the entire index from bundle files, quarantining a corrupt
/// existing file first. Returns `{indexed_rows, skipped_corrupt}`.
pub fn rebuild_index(
    root: &Path,
) -> Result<DismissedBundleIndexRebuildReportWire, ArchiveStoreError> {
    let _progress = RebuildProgressGuard::new(root);
    fs::create_dir_all(root)
        .map_err(|e| ArchiveStoreError::Io(e.to_string()))?;
    let index_path = index_path_for_root(root);
    if index_path.is_file() {
        let before = fs::metadata(&index_path)
            .ok()
            .and_then(|m| file_identity(&m));
        let probe = (|| -> Result<(), rusqlite::Error> {
            let conn = Connection::open_with_flags(
                &index_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            conn.pragma_update(None, "busy_timeout", 30_000)?;
            conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| {
                row.get::<_, i64>(0)
            })?;
            Ok(())
        })();
        match probe {
            Ok(()) => {}
            Err(error) if is_corruption_error(&error) => {
                let still_same = fs::metadata(&index_path)
                    .ok()
                    .and_then(|m| file_identity(&m))
                    .map(|after| Some(after) == before)
                    .unwrap_or(false);
                if still_same || before.is_none() {
                    quarantine_corrupt_index(&index_path)?;
                }
            }
            Err(error) => return Err(ArchiveStoreError::from(error)),
        }
    }
    let paths = iter_bundle_paths(root);
    let conn = open_archive_store_writable(root)?;
    let indexed = with_immediate(&conn, |conn| {
        conn.execute_batch("DELETE FROM dismissed_bundle_summaries")?;
        conn.execute_batch("DELETE FROM dismissed_bundle_search_fts")?;
        let mut indexed: i64 = 0;
        let key = rebuild_root_key(root);
        for path in &paths {
            let parsed = read_bundle(path).ok().and_then(|bundle| {
                summary_from_bundle(root, path, &bundle).ok()
            });
            if let Some((summary, (mtime_ns, size_bytes))) = parsed {
                upsert_summary_row(conn, &summary, mtime_ns, size_bytes)?;
                indexed += 1;
            }
            set_rebuild_progress(&key, indexed);
        }
        Ok(indexed)
    })?;
    Ok(DismissedBundleIndexRebuildReportWire {
        indexed_rows: indexed,
        skipped_corrupt: paths.len() as i64 - indexed,
    })
}

#[cfg(test)]
mod tests;
