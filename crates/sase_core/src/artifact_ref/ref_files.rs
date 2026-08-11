//! Durable `(logical_path, sha256)` index rows for artifact file references.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::artifact_object_store::artifact_object_relpath;

use super::ArtifactRefError;

pub const ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefFileVersionRowWire {
    pub schema_version: u64,
    pub logical_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authored_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    pub sha256: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    pub first_seen_at: String,
    pub origin: String,
    pub object_relpath: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_repo: Option<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub projects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefFileVersionWire {
    pub schema_version: u64,
    pub sha256: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    pub first_seen_at: String,
    pub object_relpath: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authored_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_repo: Option<String>,
    pub agents: Vec<String>,
    pub projects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefLogicalFileWire {
    pub schema_version: u64,
    pub logical_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_name: Option<String>,
    pub origin: String,
    pub versions: Vec<ArtifactRefFileVersionWire>,
}

pub fn parse_artifact_ref_file_index(
    bytes: &[u8],
) -> Vec<ArtifactRefFileVersionRowWire> {
    let mut rows = Vec::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        let line_number = index + 1;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<ArtifactRefFileVersionRowWire>(line) {
            Ok(row)
                if row.schema_version
                    == ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION =>
            {
                rows.push(row);
            }
            Ok(row) => eprintln!(
                "artifact reference file index line {line_number}: unsupported schema_version {}",
                row.schema_version
            ),
            Err(error) => eprintln!(
                "artifact reference file index line {line_number}: {error}"
            ),
        }
    }
    rows
}

pub fn render_artifact_ref_file_row(
    row: &ArtifactRefFileVersionRowWire,
) -> Result<String, serde_json::Error> {
    validate_artifact_ref_file_row(row).map_err(|error| {
        <serde_json::Error as serde::ser::Error>::custom(error.to_string())
    })?;
    serde_json::to_string(row)
}

pub fn validate_artifact_ref_file_row(
    row: &ArtifactRefFileVersionRowWire,
) -> Result<(), ArtifactRefError> {
    if row.schema_version != ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION {
        return Err(ArtifactRefError::validation(format!(
            "unsupported artifact reference file index schema_version {}",
            row.schema_version
        )));
    }
    if row.logical_path.is_empty() {
        return Err(ArtifactRefError::validation(
            "artifact reference file logical_path must not be empty",
        ));
    }
    if !matches!(row.origin.as_str(), "ref" | "created" | "capture") {
        return Err(ArtifactRefError::validation(
            "artifact reference file origin must be ref, created, or capture",
        ));
    }
    let expected = artifact_object_relpath(&row.sha256)?;
    if row.object_relpath != expected {
        return Err(ArtifactRefError::validation(
            "artifact reference file object_relpath must match sha256",
        ));
    }
    Ok(())
}

pub fn fold_artifact_ref_files(
    rows: &[ArtifactRefFileVersionRowWire],
) -> Vec<ArtifactRefLogicalFileWire> {
    let mut ordered = rows.to_vec();
    ordered.sort_by(|left, right| {
        (
            left.logical_path.as_str(),
            left.first_seen_at.as_str(),
            left.sha256.as_str(),
        )
            .cmp(&(
                right.logical_path.as_str(),
                right.first_seen_at.as_str(),
                right.sha256.as_str(),
            ))
    });

    let mut logicals = BTreeMap::<String, LogicalAccum>::new();
    for row in ordered {
        if let Err(error) = validate_artifact_ref_file_row(&row) {
            eprintln!(
                "artifact reference file index fold skipped invalid row: {error}"
            );
            continue;
        }
        let logical =
            logicals.entry(row.logical_path.clone()).or_insert_with(|| {
                LogicalAccum {
                    logical_path: row.logical_path.clone(),
                    root_name: row.root_name.clone(),
                    origin: row.origin.clone(),
                    versions: BTreeMap::new(),
                }
            });
        if logical.root_name.is_none() {
            logical.root_name = row.root_name.clone();
        }
        let version = logical
            .versions
            .entry(row.sha256.clone())
            .or_insert_with(|| VersionAccum {
                sha256: row.sha256.clone(),
                size_bytes: row.size_bytes,
                mime_type: row.mime_type.clone(),
                first_seen_at: row.first_seen_at.clone(),
                object_relpath: row.object_relpath.clone(),
                authored_path: row.authored_path.clone(),
                artifact_id: row.artifact_id.clone(),
                sidecar_repo: row.sidecar_repo.clone(),
                agents: BTreeSet::new(),
                projects: BTreeSet::new(),
            });
        if row.first_seen_at < version.first_seen_at {
            version.first_seen_at = row.first_seen_at.clone();
            version.authored_path = row.authored_path.clone();
            version.artifact_id = row.artifact_id.clone();
            version.sidecar_repo = row.sidecar_repo.clone();
        }
        if version.mime_type.is_none() {
            version.mime_type = row.mime_type.clone();
        }
        if version.artifact_id.is_none() {
            version.artifact_id = row.artifact_id.clone();
        }
        if version.sidecar_repo.is_none() {
            version.sidecar_repo = row.sidecar_repo.clone();
        }
        version.agents.extend(row.agents);
        version.projects.extend(row.projects);
    }

    logicals
        .into_values()
        .map(LogicalAccum::into_wire)
        .collect()
}

#[derive(Debug)]
struct LogicalAccum {
    logical_path: String,
    root_name: Option<String>,
    origin: String,
    versions: BTreeMap<String, VersionAccum>,
}

impl LogicalAccum {
    fn into_wire(self) -> ArtifactRefLogicalFileWire {
        let mut versions = self
            .versions
            .into_values()
            .map(VersionAccum::into_wire)
            .collect::<Vec<_>>();
        versions.sort_by(|left, right| {
            (left.first_seen_at.as_str(), left.sha256.as_str())
                .cmp(&(right.first_seen_at.as_str(), right.sha256.as_str()))
        });
        ArtifactRefLogicalFileWire {
            schema_version: ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION,
            logical_path: self.logical_path,
            root_name: self.root_name,
            origin: self.origin,
            versions,
        }
    }
}

#[derive(Debug)]
struct VersionAccum {
    sha256: String,
    size_bytes: u64,
    mime_type: Option<String>,
    first_seen_at: String,
    object_relpath: String,
    authored_path: Option<String>,
    artifact_id: Option<String>,
    sidecar_repo: Option<String>,
    agents: BTreeSet<String>,
    projects: BTreeSet<String>,
}

impl VersionAccum {
    fn into_wire(self) -> ArtifactRefFileVersionWire {
        ArtifactRefFileVersionWire {
            schema_version: ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION,
            sha256: self.sha256,
            size_bytes: self.size_bytes,
            mime_type: self.mime_type,
            first_seen_at: self.first_seen_at,
            object_relpath: self.object_relpath,
            authored_path: self.authored_path,
            artifact_id: self.artifact_id,
            sidecar_repo: self.sidecar_repo,
            agents: self.agents.into_iter().collect(),
            projects: self.projects.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        logical_path: &str,
        sha256: &str,
        first_seen_at: &str,
    ) -> ArtifactRefFileVersionRowWire {
        ArtifactRefFileVersionRowWire {
            schema_version: ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION,
            logical_path: logical_path.to_string(),
            root_name: Some("bob".to_string()),
            authored_path: Some("~/bob/gtd.md".to_string()),
            artifact_id: None,
            sha256: sha256.to_string(),
            size_bytes: 5,
            mime_type: Some("text/markdown".to_string()),
            first_seen_at: first_seen_at.to_string(),
            origin: "ref".to_string(),
            object_relpath: artifact_object_relpath(sha256).unwrap(),
            sidecar_repo: Some("sase--agents".to_string()),
            agents: vec!["agent.one".to_string()],
            projects: vec!["sase".to_string()],
        }
    }

    #[test]
    fn row_round_trips_and_parse_skips_bad_and_future_lines() {
        let expected =
            row("bob:gtd.md", &"a".repeat(64), "2026-08-01T00:00:00Z");
        validate_artifact_ref_file_row(&expected).unwrap();
        let rendered = render_artifact_ref_file_row(&expected).unwrap();
        let manifest =
            format!("{rendered}\nnot json\n{{\"schema_version\":99}}\n\n");
        assert_eq!(
            parse_artifact_ref_file_index(manifest.as_bytes()),
            vec![expected]
        );
    }

    #[test]
    fn validation_rejects_bad_identity_fields() {
        let mut invalid =
            row("bob:gtd.md", &"a".repeat(64), "2026-08-01T00:00:00Z");
        invalid.logical_path = String::new();
        assert!(validate_artifact_ref_file_row(&invalid).is_err());

        let mut invalid =
            row("bob:gtd.md", &"a".repeat(64), "2026-08-01T00:00:00Z");
        invalid.origin = "other".to_string();
        assert!(validate_artifact_ref_file_row(&invalid).is_err());

        let mut invalid =
            row("bob:gtd.md", &"a".repeat(64), "2026-08-01T00:00:00Z");
        invalid.object_relpath =
            "files/objects/sha256/ff/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        assert!(validate_artifact_ref_file_row(&invalid).is_err());
    }

    #[test]
    fn fold_deduplicates_versions_and_unions_provenance() {
        let first = row("bob:gtd.md", &"b".repeat(64), "2026-08-02T00:00:00Z");
        let mut repeat =
            row("bob:gtd.md", &"b".repeat(64), "2026-08-03T00:00:00Z");
        repeat.agents = vec!["agent.two".to_string()];
        repeat.projects = vec!["other".to_string()];
        let older = row("bob:gtd.md", &"a".repeat(64), "2026-08-01T00:00:00Z");

        let folded = fold_artifact_ref_files(&[first, repeat, older]);

        assert_eq!(folded.len(), 1);
        assert_eq!(folded[0].logical_path, "bob:gtd.md");
        assert_eq!(folded[0].versions.len(), 2);
        assert_eq!(folded[0].versions[0].sha256, "a".repeat(64));
        assert_eq!(folded[0].versions[1].sha256, "b".repeat(64));
        assert_eq!(
            folded[0].versions[1].agents,
            ["agent.one".to_string(), "agent.two".to_string()]
        );
        assert_eq!(
            folded[0].versions[1].projects,
            ["other".to_string(), "sase".to_string()]
        );
    }
}
