//! Shared streamed audit-query facility, initially for memory reads.
//!
//! Streams a JSONL audit log and returns only rows that match an ordered
//! set of exact member selectors (artifact directory and optional agent
//! name), preserving the Python TUI matching contract:
//!
//! - Artifact-directory precedence: an event carrying a directory never
//!   falls back to a name when that directory mismatches. A missing
//!   directory permits an exact name match.
//! - First-member attribution: the earliest member in request order wins and
//!   its ordinal is returned so Python can apply role labels.
//! - Context duplicate policy: context queries de-duplicate event IDs after
//!   matching, retaining the first matched source occurrence; ordinary
//!   single-agent queries retain existing duplicate behavior.
//! - Final ordering is stable timestamp-descending; the cap applies after
//!   matching and duplicate handling, never to a raw log tail.
//!
//! Unrelated decoded rows are dropped as they are scanned. The artifact-read
//! phase reuses this traversal and selection contract with its own row
//! validation.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const MEMORY_AUDIT_QUERY_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Error)]
pub enum AuditQueryError {
    #[error("audit log I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    InvalidWire(String),
}

/// One exact member selector in priority order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditMemberWire {
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
}

/// Targeted memory-audit query: ordered members, result cap, duplicate policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryAuditQueryWire {
    #[serde(default)]
    pub members: Vec<AuditMemberWire>,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub deduplicate: bool,
}

/// Identity of the log file actually read, including inode so an atomic
/// replacement with equal size and mtime cannot reuse a stale answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditSourceIdentityWire {
    pub missing: bool,
    pub size: u64,
    pub mtime_ns: u64,
    pub dev: u64,
    pub ino: u64,
}

impl AuditSourceIdentityWire {
    pub(crate) fn missing() -> Self {
        Self {
            missing: true,
            size: 0,
            mtime_ns: 0,
            dev: 0,
            ino: 0,
        }
    }
}

/// One validated memory-read event in wire form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReadWire {
    #[serde(default)]
    pub schema_version: u64,
    pub id: String,
    pub timestamp: String,
    pub project: String,
    pub cwd: String,
    pub canonical_path: String,
    pub resolved_path: String,
    pub agent_name: String,
    pub agent_source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts_dir: Option<String>,
    pub reason: String,
    pub byte_count: i64,
    pub frontmatter_stripped: bool,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub selectors: Vec<String>,
    #[serde(default)]
    pub resolved_targets: Vec<String>,
    #[serde(default)]
    pub included_targets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<i64>,
    #[serde(default)]
    pub scope_origin: Vec<[String; 2]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_oid: Option<String>,
    #[serde(default)]
    pub included_blob_oids: Vec<[String; 2]>,
}

fn default_kind() -> String {
    "note".to_string()
}

/// One matched row plus the ordinal of the member that claimed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryAuditRowWire {
    pub event: MemoryReadWire,
    pub member_index: usize,
}

/// Matching memory rows plus the source identity they were read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryAuditQueryResultWire {
    pub rows: Vec<MemoryAuditRowWire>,
    pub source: AuditSourceIdentityWire,
}

/// Stream the log and return matching rows for the ordered members.
///
/// File order is scanned; final rows are stable timestamp-descending and
/// capped after matching and duplicate handling. Unsupported, malformed,
/// and unmatched rows are skipped without retaining decoded objects.
/// Missing logs return an empty success distinguished by `source.missing`.
pub fn query_memory_audit(
    log_path: &Path,
    query: &MemoryAuditQueryWire,
) -> Result<MemoryAuditQueryResultWire, AuditQueryError> {
    let normalized_members: Vec<(Option<String>, Option<String>)> = query
        .members
        .iter()
        .map(|member| {
            (
                normalize_audit_dir(member.artifacts_dir.as_deref()),
                nonempty_opt(member.agent_name.clone()),
            )
        })
        .collect();
    if normalized_members.is_empty() {
        let source = source_identity_for(log_path)?;
        return Ok(MemoryAuditQueryResultWire {
            rows: Vec::new(),
            source,
        });
    }
    let file = match File::open(log_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MemoryAuditQueryResultWire {
                rows: Vec::new(),
                source: AuditSourceIdentityWire::missing(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let source = source_identity(&file.metadata()?);
    let reader = BufReader::new(file);
    let mut matched: Vec<MemoryAuditRowWire> = Vec::new();
    let mut seen_ids: HashSet<String> = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(event) = memory_row_from_line(trimmed) else {
            continue;
        };
        if query.deduplicate && seen_ids.contains(&event.id) {
            continue;
        }
        let Some(member_index) =
            match_audit_member(&normalized_members, &event)
        else {
            continue;
        };
        if query.deduplicate {
            seen_ids.insert(event.id.clone());
        }
        matched.push(MemoryAuditRowWire {
            event,
            member_index,
        });
    }
    matched.sort_by(|a, b| b.event.timestamp.cmp(&a.event.timestamp));
    if query.limit > 0 && matched.len() > query.limit {
        matched.truncate(query.limit);
    }
    Ok(MemoryAuditQueryResultWire {
        rows: matched,
        source,
    })
}

/// Match Python `match_event_label` precedence without labels.
///
/// An event carrying a directory matches only on exact normalized directory
/// equality against members in order; it never falls back to a name. An
/// event without a directory matches only on exact agent-name equality.
pub fn match_audit_member(
    members: &[(Option<String>, Option<String>)],
    event: &MemoryReadWire,
) -> Option<usize> {
    let normalized_event = normalize_audit_dir(event.artifacts_dir.as_deref());
    if let Some(event_dir) = normalized_event {
        for (index, (member_dir, _)) in members.iter().enumerate() {
            if let Some(member_dir) = member_dir {
                if *member_dir == event_dir {
                    return Some(index);
                }
            }
        }
        return None;
    }
    if event.agent_name.is_empty() {
        return None;
    }
    for (index, (_, member_name)) in members.iter().enumerate() {
        if let Some(member_name) = member_name {
            if *member_name == event.agent_name {
                return Some(index);
            }
        }
    }
    None
}

fn memory_row_from_line(line: &str) -> Option<MemoryReadWire> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    let envelope = value.as_object()?;
    memory_row_from_mapping(envelope)
}

fn memory_row_from_mapping(
    data: &serde_json::Map<String, Value>,
) -> Option<MemoryReadWire> {
    let schema_version = json_u64(data.get("schema_version")?)?;
    if !matches!(schema_version, 1..=3) {
        return None;
    }
    let id = json_string(data.get("id")?)?;
    let timestamp = json_string(data.get("timestamp")?)?;
    let project = json_string(data.get("project")?)?;
    let cwd = json_string(data.get("cwd")?)?;
    let canonical_path = json_string(data.get("canonical_path")?)?;
    let resolved_path = json_string(data.get("resolved_path")?)?;
    let agent_name = json_string(data.get("agent_name")?)?;
    let agent_source = json_string(data.get("agent_source")?)?;
    let reason = json_string(data.get("reason")?)?;
    let byte_count = json_i64(data.get("byte_count")?)?;
    let frontmatter_stripped = data.get("frontmatter_stripped")?.as_bool()?;
    let artifacts_dir = match data.get("artifacts_dir") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(_) => return None,
    };
    let kind = match data.get("kind") {
        None | Some(Value::Null) => "note".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(_) => return None,
    };
    if !matches!(kind.as_str(), "note" | "web" | "strand") {
        return None;
    }
    let selectors = string_list_or_default(
        data.get("selectors"),
        Some(vec![canonical_path.clone()]),
    )?;
    let resolved_targets = string_list_or_default(
        data.get("resolved_targets"),
        Some(vec![canonical_path.clone()]),
    )?;
    let included_targets =
        string_list_or_default(data.get("included_targets"), Some(Vec::new()))?;
    let depth = match data.get("depth") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => number.as_i64().or_else(|| {
            number.as_u64().and_then(|value| i64::try_from(value).ok())
        }),
        Some(_) => return None,
    };
    let scope_origin = pair_list_or_default(data.get("scope_origin"))?;
    let blob_oid = match data.get("blob_oid") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(_) => return None,
    };
    let included_blob_oids =
        pair_list_or_default(data.get("included_blob_oids"))?;
    Some(MemoryReadWire {
        schema_version,
        id,
        timestamp,
        project,
        cwd,
        canonical_path,
        resolved_path,
        agent_name,
        agent_source,
        artifacts_dir,
        reason,
        byte_count,
        frontmatter_stripped,
        kind,
        selectors,
        resolved_targets,
        included_targets,
        depth,
        scope_origin,
        blob_oid,
        included_blob_oids,
    })
}

fn json_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        _ => None,
    }
}

fn json_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64().or_else(|| {
            number
                .as_i64()
                .and_then(|signed| u64::try_from(signed).ok())
        }),
        _ => None,
    }
}

fn json_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64().or_else(|| {
            number.as_u64().and_then(|value| i64::try_from(value).ok())
        }),
        _ => None,
    }
}

fn string_list_or_default(
    value: Option<&Value>,
    default: Option<Vec<String>>,
) -> Option<Vec<String>> {
    match value {
        None | Some(Value::Null) => Some(default.unwrap_or_default()),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(text) => out.push(text.clone()),
                    _ => return None,
                }
            }
            Some(out)
        }
        Some(_) => None,
    }
}

fn pair_list_or_default(value: Option<&Value>) -> Option<Vec<[String; 2]>> {
    match value {
        None | Some(Value::Null) => Some(Vec::new()),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Array(pair) if pair.len() == 2 => {
                        match (&pair[0], &pair[1]) {
                            (Value::String(first), Value::String(second)) => {
                                out.push([first.clone(), second.clone()])
                            }
                            _ => return None,
                        }
                    }
                    _ => return None,
                }
            }
            Some(out)
        }
        Some(_) => None,
    }
}

fn nonempty_opt(value: Option<String>) -> Option<String> {
    match value {
        Some(text) if !text.is_empty() => Some(text),
        _ => None,
    }
}

/// Normalize an artifact directory like Python `normalized_dirs.normalize_dir`.
///
/// Empty values map to `None`. Existing paths canonicalize (resolving
/// symlinks); missing tails fall back to canonicalizing the nearest
/// existing ancestor and lexically cleaning the remainder. String-prefix
/// matching is never used; only the final exact comparison matches.
pub fn normalize_audit_dir(value: Option<&str>) -> Option<String> {
    let raw = value?;
    if raw.is_empty() {
        return None;
    }
    let path = Path::new(raw);
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Some(canonical.to_string_lossy().into_owned());
    }
    // Collect the missing tail leaf-first, then join it onto the nearest
    // existing ancestor once one is found.
    let mut remainder: Vec<String> = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        if remainder.len() > 512 {
            return Some(lexical_clean(path));
        }
        match current.file_name() {
            Some(name) => {
                remainder.push(name.to_string_lossy().into_owned());
                match current.parent() {
                    Some(parent) if !parent.as_os_str().is_empty() => {
                        current = parent.to_path_buf();
                    }
                    _ => return Some(lexical_clean(path)),
                }
            }
            None => return Some(lexical_clean(path)),
        }
        if std::fs::metadata(&current).is_ok() {
            match std::fs::canonicalize(&current) {
                Ok(canonical) => {
                    let mut joined = canonical;
                    for part in remainder.iter().rev() {
                        joined.push(part);
                    }
                    return Some(lexical_clean(&joined));
                }
                Err(_) => return Some(lexical_clean(path)),
            }
        }
    }
}

fn lexical_clean(path: &Path) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut is_absolute = false;
    let mut prefix: Option<String> = None;
    for component in path.components() {
        match component {
            Component::Prefix(prefix_component) => {
                prefix = Some(
                    prefix_component.as_os_str().to_string_lossy().into_owned(),
                );
            }
            Component::RootDir => {
                is_absolute = true;
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() && !is_absolute {
                    parts.push("..".to_string());
                }
            }
            Component::Normal(part) => {
                parts.push(part.to_string_lossy().into_owned());
            }
        }
    }
    let mut out = String::new();
    if let Some(prefix) = prefix {
        out.push_str(&prefix);
    }
    if is_absolute {
        out.push('/');
    }
    out.push_str(&parts.join("/"));
    if out.is_empty() {
        return ".".to_string();
    }
    out
}

pub(crate) fn source_identity_for(
    log_path: &Path,
) -> Result<AuditSourceIdentityWire, AuditQueryError> {
    match std::fs::metadata(log_path) {
        Ok(meta) => Ok(source_identity(&meta)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(AuditSourceIdentityWire::missing())
        }
        Err(error) => Err(error.into()),
    }
}

fn source_identity(meta: &std::fs::Metadata) -> AuditSourceIdentityWire {
    AuditSourceIdentityWire {
        missing: false,
        size: meta.len(),
        mtime_ns: metadata_mtime_ns(meta),
        dev: metadata_dev(meta),
        ino: metadata_ino(meta),
    }
}

fn metadata_mtime_ns(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let secs = u64::try_from(meta.mtime()).unwrap_or(0);
        let nsec = u64::try_from(meta.mtime_nsec()).unwrap_or(0);
        secs.saturating_mul(1_000_000_000).saturating_add(nsec)
    }
    #[cfg(not(unix))]
    {
        meta.modified()
            .ok()
            .and_then(|stamp| stamp.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0)
    }
}

fn metadata_dev(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.dev()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        0
    }
}

fn metadata_ino(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        0
    }
}

#[allow(dead_code)]
fn audit_query_error_for_tests(message: &str) -> AuditQueryError {
    AuditQueryError::InvalidWire(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn write_lines(dir: &Path, name: &str, lines: &[Value]) -> PathBuf {
        let path = dir.join(name);
        let mut content = lines
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        content.push('\n');
        fs::write(&path, content).unwrap();
        path
    }

    fn memory_event(
        id: &str,
        timestamp: &str,
        artifacts_dir: Option<&str>,
        agent_name: &str,
    ) -> Value {
        let mut event = json!({
            "schema_version": 3,
            "id": id,
            "timestamp": timestamp,
            "project": "sase",
            "cwd": "/tmp/demo",
            "canonical_path": "foo.md",
            "resolved_path": "/tmp/demo/memory/foo.md",
            "agent_name": agent_name,
            "agent_source": "SASE_AGENT_NAME",
            "artifacts_dir": artifacts_dir,
            "reason": "Need foo",
            "byte_count": 42,
            "frontmatter_stripped": false,
            "kind": "note",
            "selectors": ["foo.md"],
            "resolved_targets": ["foo.md"],
            "included_targets": [],
        });
        if artifacts_dir.is_none() {
            event["artifacts_dir"] = Value::Null;
        }
        event
    }

    fn query_for(
        dirs: &[&str],
        names: &[&str],
        limit: usize,
        deduplicate: bool,
    ) -> MemoryAuditQueryWire {
        assert_eq!(dirs.len(), names.len());
        MemoryAuditQueryWire {
            members: dirs
                .iter()
                .zip(names.iter())
                .map(|(dir, name)| AuditMemberWire {
                    artifacts_dir: if dir.is_empty() {
                        None
                    } else {
                        Some((*dir).to_string())
                    },
                    agent_name: if name.is_empty() {
                        None
                    } else {
                        Some((*name).to_string())
                    },
                })
                .collect(),
            limit,
            deduplicate,
        }
    }

    #[test]
    fn dir_precedence_blocks_name_fallback() {
        let temp = tempdir().unwrap();
        let log = write_lines(
            temp.path(),
            "memory_reads.jsonl",
            &[
                memory_event(
                    "match-dir",
                    "2026-05-24T10:00:00+00:00",
                    Some("/runs/target"),
                    "other",
                ),
                memory_event(
                    "wrong-dir-same-name",
                    "2026-05-24T10:01:00+00:00",
                    Some("/runs/other"),
                    "alpha",
                ),
                memory_event(
                    "no-dir-same-name",
                    "2026-05-24T10:02:00+00:00",
                    None,
                    "alpha",
                ),
            ],
        );
        let result = query_memory_audit(
            &log,
            &query_for(&["/runs/target"], &["alpha"], 50, false),
        )
        .unwrap();
        let ids: Vec<&str> = result
            .rows
            .iter()
            .map(|row| row.event.id.as_str())
            .collect();
        assert_eq!(ids, ["no-dir-same-name", "match-dir"]);
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.member_index)
                .collect::<Vec<_>>(),
            [0, 0]
        );
    }

    #[test]
    fn context_dedup_keeps_first_source_occurrence_then_sorts() {
        let temp = tempdir().unwrap();
        let log = write_lines(
            temp.path(),
            "memory_reads.jsonl",
            &[
                memory_event(
                    "dup",
                    "2026-05-24T10:00:00+00:00",
                    Some("/runs/a"),
                    "alpha",
                ),
                memory_event(
                    "dup",
                    "2026-05-24T10:05:00+00:00",
                    Some("/runs/b"),
                    "beta",
                ),
                memory_event(
                    "solo",
                    "2026-05-24T10:03:00+00:00",
                    Some("/runs/b"),
                    "beta",
                ),
            ],
        );
        let context = query_memory_audit(
            &log,
            &query_for(&["/runs/a", "/runs/b"], &["alpha", "beta"], 50, true),
        )
        .unwrap();
        assert_eq!(
            context
                .rows
                .iter()
                .map(|row| (row.event.id.as_str(), row.member_index))
                .collect::<Vec<_>>(),
            [("solo", 1), ("dup", 0)]
        );
        let single = query_memory_audit(
            &log,
            &query_for(&["/runs/a", "/runs/b"], &["alpha", "beta"], 50, false),
        )
        .unwrap();
        assert_eq!(single.rows.len(), 3);
    }

    #[test]
    fn cap_applies_after_matching_not_to_raw_tail() {
        let temp = tempdir().unwrap();
        let mut lines = Vec::new();
        for index in 0..10 {
            lines.push(memory_event(
                "unrelated",
                &format!("2026-05-24T11:{index:02}:00+00:00"),
                Some("/runs/other"),
                "other",
            ));
        }
        lines.push(memory_event(
            "old-match",
            "2026-05-24T09:00:00+00:00",
            Some("/runs/target"),
            "alpha",
        ));
        let log = write_lines(temp.path(), "memory_reads.jsonl", &lines);
        let result = query_memory_audit(
            &log,
            &query_for(&["/runs/target"], &["alpha"], 50, false),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].event.id, "old-match");
    }

    #[test]
    fn malformed_and_wrong_schema_rows_are_skipped() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("memory_reads.jsonl");
        let mut content = String::new();
        content.push_str(
            &memory_event(
                "good",
                "2026-05-24T10:00:00+00:00",
                Some("/runs/target"),
                "alpha",
            )
            .to_string(),
        );
        content.push('\n');
        content.push_str("{malformed\n");
        content.push_str(
            &json!({"schema_version": 99, "id": "future"}).to_string(),
        );
        content.push('\n');
        content.push_str(
            &json!({"schema_version": 3, "id": "missing-fields"}).to_string(),
        );
        content.push('\n');
        fs::write(&path, content).unwrap();
        let result = query_memory_audit(
            &path,
            &query_for(&["/runs/target"], &["alpha"], 50, false),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].event.id, "good");
    }

    #[test]
    fn missing_log_returns_missing_identity() {
        let temp = tempdir().unwrap();
        let missing = temp.path().join("absent.jsonl");
        let result = query_memory_audit(
            &missing,
            &query_for(&["/runs/target"], &["alpha"], 50, false),
        )
        .unwrap();
        assert!(result.rows.is_empty());
        assert!(result.source.missing);
    }

    #[test]
    fn v1_row_upgrades_with_note_defaults() {
        let temp = tempdir().unwrap();
        let log = write_lines(
            temp.path(),
            "memory_reads.jsonl",
            &[json!({
                "schema_version": 1,
                "id": "v1",
                "timestamp": "2026-05-23T12:00:00+00:00",
                "project": "sase",
                "cwd": "/tmp/demo",
                "canonical_path": "foo.md",
                "resolved_path": "/tmp/demo/memory/foo.md",
                "agent_name": "alpha",
                "agent_source": "SASE_AGENT_NAME",
                "artifacts_dir": "/runs/target",
                "reason": "Need foo",
                "byte_count": 42,
                "frontmatter_stripped": false
            })],
        );
        let result = query_memory_audit(
            &log,
            &query_for(&["/runs/target"], &["alpha"], 50, false),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].event.kind, "note");
        assert_eq!(result.rows[0].event.selectors, vec!["foo.md"]);
    }

    #[test]
    fn wire_match_helper_mirrors_python_precedence() {
        let members = vec![
            (Some("/runs/a".to_string()), Some("alpha".to_string())),
            (Some("/runs/b".to_string()), Some("beta".to_string())),
        ];
        let mut event = memory_row_from_mapping(
            memory_event(
                "x",
                "2026-05-24T10:00:00+00:00",
                Some("/runs/b"),
                "alpha",
            )
            .as_object()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(match_audit_member(&members, &event), Some(1));
        event.artifacts_dir = None;
        event.agent_name = "alpha".to_string();
        assert_eq!(match_audit_member(&members, &event), Some(0));
        event.agent_name = "beta".to_string();
        assert_eq!(match_audit_member(&members, &event), Some(1));
    }
}
