//! Targeted association queries over the artifact-file JSONL index.
//!
//! Streams supported envelopes and returns only rows that match Python
//! `matches_association()`: exact `agent_artifacts_dir`, or a non-null
//! equal `project` and `raw_timestamp`. Unrelated decoded rows are dropped
//! as they are scanned. A plain agent-name filter is not equivalent.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    artifact_file_index_schema_supported, ArtifactFileQueryError,
    ArtifactFileWire,
};

pub const ARTIFACT_FILE_ASSOCIATION_QUERY_WIRE_SCHEMA_VERSION: u64 = 1;

const LISTING_KINDS: &[&str] =
    &["chat", "plan", "image", "markdown", "pdf", "file"];

/// Association selector matching Python `ArtifactFileAssociation` listing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileAssociationQueryWire {
    pub agent_artifacts_dir: String,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub raw_timestamp: Option<String>,
    #[serde(default)]
    pub explicit_only: bool,
}

/// Identity of the index file actually read, including inode so an atomic
/// replacement with equal size and mtime cannot reuse a stale answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileSourceIdentityWire {
    pub missing: bool,
    pub size: u64,
    pub mtime_ns: u64,
    pub dev: u64,
    pub ino: u64,
}

impl ArtifactFileSourceIdentityWire {
    fn missing() -> Self {
        Self {
            missing: true,
            size: 0,
            mtime_ns: 0,
            dev: 0,
            ino: 0,
        }
    }
}

/// Matching listing rows plus the source identity they were read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileAssociationQueryResultWire {
    pub rows: Vec<ArtifactFileWire>,
    pub source: ArtifactFileSourceIdentityWire,
}

/// Stream the index and return listing-compatible rows for one association.
///
/// File order is preserved. Unsupported, malformed, and unmatched envelopes
/// are skipped without retaining decoded objects. Missing indexes return an
/// empty success distinguished from I/O failure by `source.missing`.
pub fn query_artifact_file_associations(
    index_path: &Path,
    query: &ArtifactFileAssociationQueryWire,
) -> Result<ArtifactFileAssociationQueryResultWire, ArtifactFileQueryError> {
    let file = match File::open(index_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ArtifactFileAssociationQueryResultWire {
                rows: Vec::new(),
                source: ArtifactFileSourceIdentityWire::missing(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let source = source_identity(&file.metadata()?);
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let Some(row) = listing_row_from_line(line.trim()) else {
            continue;
        };
        if !artifact_file_matches_association(&row, query) {
            continue;
        }
        if query.explicit_only && !row.explicit {
            continue;
        }
        rows.push(row);
    }
    Ok(ArtifactFileAssociationQueryResultWire { rows, source })
}

/// Match Python `artifact_file_helpers.matches_association()`.
pub fn artifact_file_matches_association(
    row: &ArtifactFileWire,
    query: &ArtifactFileAssociationQueryWire,
) -> bool {
    if row.agent_artifacts_dir.as_deref()
        == Some(query.agent_artifacts_dir.as_str())
    {
        return true;
    }
    row.project.is_some()
        && row.project == query.project
        && row.raw_timestamp.is_some()
        && row.raw_timestamp == query.raw_timestamp
}

fn listing_row_from_line(line: &str) -> Option<ArtifactFileWire> {
    if line.is_empty() {
        return None;
    }
    let value = serde_json::from_str::<Value>(line).ok()?;
    let envelope = value.as_object()?;
    let schema_version =
        json_int(envelope.get("schema_version").unwrap_or(&Value::Null))?;
    if !artifact_file_index_schema_supported(schema_version) {
        return None;
    }
    let artifact = envelope.get("artifact")?.as_object()?;
    listing_row_from_artifact(schema_version, artifact)
}

fn listing_row_from_artifact(
    schema_version: u64,
    artifact: &serde_json::Map<String, Value>,
) -> Option<ArtifactFileWire> {
    let id = required_python_str(artifact, "id")?;
    let label = required_python_str(artifact, "label")?;
    let kind = required_python_str(artifact, "kind")?;
    if !LISTING_KINDS.contains(&kind.as_str()) {
        return None;
    }
    let path = optional_str(artifact.get("path"));
    let vcs_repo = optional_str(artifact.get("vcs_repo"));
    let vcs_sha = optional_str(artifact.get("vcs_sha"));
    let vcs_relpath = optional_str(artifact.get("vcs_relpath"));
    let has_path = path.as_ref().is_some_and(|value| !value.is_empty());
    let has_vcs = nonempty_opt(&vcs_repo)
        && nonempty_opt(&vcs_sha)
        && nonempty_opt(&vcs_relpath);
    if !has_path && !has_vcs {
        return None;
    }
    Some(ArtifactFileWire {
        schema_version,
        id,
        label: Some(label),
        kind: Some(kind),
        path,
        vcs_repo,
        vcs_sha,
        vcs_relpath,
        source_path: optional_str(artifact.get("source_path")),
        workspace_dir: optional_str(artifact.get("workspace_dir")),
        created_at: optional_str(artifact.get("created_at")),
        agent_artifacts_dir: optional_str(artifact.get("agent_artifacts_dir")),
        project: optional_str(artifact.get("project")),
        workflow: optional_str(artifact.get("workflow")),
        raw_timestamp: optional_str(artifact.get("raw_timestamp")),
        agent_name: optional_str(artifact.get("agent_name")),
        explicit: python_bool(artifact.get("explicit")),
        sha256: optional_str(artifact.get("sha256")),
        size_bytes: optional_u64(artifact.get("size_bytes")),
        mime_type: optional_str(artifact.get("mime_type")),
    })
}

fn required_python_str(
    artifact: &serde_json::Map<String, Value>,
    key: &str,
) -> Option<String> {
    artifact.get(key).map(python_str)
}

fn python_str(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(flag) => {
            if *flag {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn optional_str(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) => Some(text.clone()),
        _ => None,
    }
}

fn optional_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        _ => None,
    }
}

fn json_int(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64().or_else(|| {
            number.as_f64().and_then(|float| {
                if float.is_finite() && float >= 0.0 && float.fract() == 0.0 {
                    Some(float as u64)
                } else {
                    None
                }
            })
        }),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn python_bool(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => {
            number.as_f64().is_some_and(|value| value != 0.0)
        }
        Some(Value::String(text)) => !text.is_empty(),
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::Object(map)) => !map.is_empty(),
    }
}

fn nonempty_opt(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|text| !text.is_empty())
}

fn source_identity(meta: &std::fs::Metadata) -> ArtifactFileSourceIdentityWire {
    ArtifactFileSourceIdentityWire {
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

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn write_index(lines: &[Value]) -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let mut content = lines
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        content.push_str("\n{malformed\n");
        fs::write(&index, content).unwrap();
        (temp, index)
    }

    fn listing_row(
        id: &str,
        dir: &str,
        project: Option<&str>,
        timestamp: Option<&str>,
    ) -> Value {
        json!({
            "schema_version": 2,
            "artifact": {
                "id": id,
                "label": format!("Label {id}"),
                "kind": "markdown",
                "path": format!("/stored/{id}.md"),
                "agent_artifacts_dir": dir,
                "project": project,
                "raw_timestamp": timestamp,
                "agent_name": "agent.one",
                "explicit": true
            }
        })
    }

    fn query(
        dir: &str,
        project: Option<&str>,
        timestamp: Option<&str>,
    ) -> ArtifactFileAssociationQueryWire {
        ArtifactFileAssociationQueryWire {
            agent_artifacts_dir: dir.to_string(),
            project: project.map(str::to_string),
            raw_timestamp: timestamp.map(str::to_string),
            explicit_only: false,
        }
    }

    fn ids(result: &ArtifactFileAssociationQueryResultWire) -> Vec<&str> {
        result.rows.iter().map(|row| row.id.as_str()).collect()
    }

    #[test]
    fn exact_directory_or_project_timestamp_match_in_file_order() {
        let (temp, index) = write_index(&[
            listing_row(
                "unrelated",
                "/runs/other",
                Some("other"),
                Some("20260501000000"),
            ),
            listing_row(
                "exact-dir",
                "/runs/target",
                Some("sase"),
                Some("20260507123456"),
            ),
            listing_row(
                "fallback",
                "/runs/relocated",
                Some("sase"),
                Some("20260507123456"),
            ),
            listing_row(
                "same-name-other-run",
                "/runs/other-two",
                Some("sase"),
                Some("20260508000000"),
            ),
        ]);
        let result = query_artifact_file_associations(
            &index,
            &query("/runs/target", Some("sase"), Some("20260507123456")),
        )
        .unwrap();
        assert_eq!(ids(&result), ["exact-dir", "fallback"]);
        assert!(!result.source.missing);
        assert!(result.source.ino > 0 || cfg!(not(unix)));
        drop(temp);
    }

    #[test]
    fn null_project_or_timestamp_does_not_fallback() {
        let (temp, index) = write_index(&[
            listing_row(
                "null-project",
                "/runs/other",
                None,
                Some("20260507123456"),
            ),
            listing_row("null-ts", "/runs/other", Some("sase"), None),
        ]);
        let result = query_artifact_file_associations(
            &index,
            &query("/runs/target", Some("sase"), Some("20260507123456")),
        )
        .unwrap();
        assert!(result.rows.is_empty());
        drop(temp);
    }

    #[test]
    fn agent_name_filter_is_not_equivalent() {
        let mut named = listing_row(
            "named",
            "/runs/other",
            Some("other"),
            Some("20260101000000"),
        );
        named["artifact"]["agent_name"] = json!("agent.one");
        let (temp, index) = write_index(&[named]);
        let result = query_artifact_file_associations(
            &index,
            &query("/runs/target", Some("sase"), Some("20260507123456")),
        )
        .unwrap();
        assert!(result.rows.is_empty());
        drop(temp);
    }

    #[test]
    fn listing_parser_matches_python_tolerance() {
        let vcs = json!({
            "schema_version": 2,
            "artifact": {
                "id": "vcs",
                "label": "Versioned",
                "kind": "image",
                "path": null,
                "vcs_repo": "sase",
                "vcs_sha": "0123456789abcdef0123456789abcdef01234567",
                "vcs_relpath": "docs/image.png",
                "agent_artifacts_dir": "/runs/target",
                "project": "sase",
                "raw_timestamp": "20260507123456"
            }
        });
        let schema1 = json!({
            "schema_version": 1,
            "artifact": {
                "id": "v1",
                "label": "Old",
                "kind": "file",
                "path": "/stored/v1.bin",
                "agent_artifacts_dir": "/runs/target"
            }
        });
        let (temp, index) = write_index(&[
            json!({"schema_version": 3, "artifact": {
                "id": "future", "label": "x", "kind": "file",
                "path": "/x", "agent_artifacts_dir": "/runs/target"
            }}),
            json!({"schema_version": 2, "artifact": {
                "id": "no-label", "kind": "file", "path": "/x",
                "agent_artifacts_dir": "/runs/target"
            }}),
            json!({"schema_version": 2, "artifact": {
                "id": "bad-kind", "label": "x", "kind": "video",
                "path": "/x", "agent_artifacts_dir": "/runs/target"
            }}),
            json!({"schema_version": 2, "artifact": "not-an-object"}),
            json!([1, 2, 3]),
            vcs,
            schema1,
        ]);
        let result = query_artifact_file_associations(
            &index,
            &query("/runs/target", Some("sase"), Some("20260507123456")),
        )
        .unwrap();
        assert_eq!(ids(&result), ["vcs", "v1"]);
        assert!(result.rows[0].path.is_none());
        assert_eq!(result.rows[0].schema_version, 2);
        assert_eq!(result.rows[1].schema_version, 1);
        drop(temp);
    }

    #[test]
    fn explicit_only_drops_default_rows_during_scan() {
        let mut default_row = listing_row(
            "default",
            "/runs/target",
            Some("sase"),
            Some("20260507123456"),
        );
        default_row["artifact"]["explicit"] = json!(false);
        let (temp, index) = write_index(&[
            default_row,
            listing_row(
                "explicit",
                "/runs/target",
                Some("sase"),
                Some("20260507123456"),
            ),
        ]);
        let mut selector =
            query("/runs/target", Some("sase"), Some("20260507123456"));
        selector.explicit_only = true;
        let result =
            query_artifact_file_associations(&index, &selector).unwrap();
        assert_eq!(ids(&result), ["explicit"]);
        drop(temp);
    }

    #[test]
    fn fifty_thousand_unrelated_rows_return_only_matches() {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let mut content = String::new();
        let unrelated = concat!(
            r#"{"schema_version":2,"artifact":{"id":"u","label":"U","kind":"file","path":"/u","agent_artifacts_dir":"/other","project":"other","raw_timestamp":"1"}}"#,
            "\n"
        );
        for _ in 0..50_000 {
            content.push_str(unrelated);
        }
        content.push_str(
            r#"{"schema_version":2,"artifact":{"id":"keep-a","label":"A","kind":"file","path":"/a","agent_artifacts_dir":"/runs/target","project":"sase","raw_timestamp":"20260507123456"}}"#,
        );
        content.push('\n');
        content.push_str(
            r#"{"schema_version":2,"artifact":{"id":"keep-b","label":"B","kind":"image","path":null,"vcs_repo":"sase","vcs_sha":"0123456789abcdef0123456789abcdef01234567","vcs_relpath":"docs/b.png","agent_artifacts_dir":"/runs/moved","project":"sase","raw_timestamp":"20260507123456"}}"#,
        );
        content.push('\n');
        fs::write(&index, content).unwrap();
        let result = query_artifact_file_associations(
            &index,
            &query("/runs/target", Some("sase"), Some("20260507123456")),
        )
        .unwrap();
        assert_eq!(ids(&result), ["keep-a", "keep-b"]);
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn missing_index_is_empty_success() {
        let temp = tempdir().unwrap();
        let result = query_artifact_file_associations(
            &temp.path().join("missing.jsonl"),
            &query("/runs/target", None, None),
        )
        .unwrap();
        assert!(result.rows.is_empty());
        assert!(result.source.missing);
    }
}
