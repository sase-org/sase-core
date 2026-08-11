//! Normalized artifact-entry and per-occurrence resolution wire types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::validate_kind;
use super::wire::{ArtifactRefError, ArtifactRefSpanWire};

pub const ARTIFACT_REF_ENTRY_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactEntryOriginWire {
    PromptRef,
    AgentArtifact,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEntryWire {
    pub schema_version: u64,
    pub stable_id: String,
    pub ref_kind: String,
    pub canonical_argument: String,
    pub display_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_relative_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_path: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    pub origin: ArtifactEntryOriginWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedArtifactRefWire {
    pub schema_version: u64,
    pub raw_ref: String,
    pub canonical_ref: String,
    pub occurrence_span: ArtifactRefSpanWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<ArtifactEntryWire>,
    pub prompt_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_file: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

/// Validate one normalized artifact entry.
pub fn validate_artifact_entry(
    entry: &ArtifactEntryWire,
) -> Result<(), ArtifactRefError> {
    if entry.stable_id.is_empty() {
        return Err(ArtifactRefError::validation(
            "artifact entry stable_id must not be empty",
        ));
    }
    validate_kind(&entry.ref_kind)?;
    if let Some(revision) = &entry.captured_revision {
        if revision.len() != 40 || !is_lowercase_hex(revision) {
            return Err(ArtifactRefError::validation(
                "artifact entry captured_revision must be 40 lowercase hexadecimal characters",
            ));
        }
    }
    if let Some(digest) = &entry.captured_digest {
        if digest.len() != 64 || !is_lowercase_hex(digest) {
            return Err(ArtifactRefError::validation(
                "artifact entry captured_digest must be 64 lowercase hexadecimal characters",
            ));
        }
    }
    for key in entry.properties.keys() {
        if !is_property_key(key) {
            return Err(ArtifactRefError::validation(format!(
                "artifact entry property key '{key}' must match [a-z][a-z0-9_]*"
            )));
        }
    }
    Ok(())
}

fn is_lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_property_key(key: &str) -> bool {
    let mut bytes = key.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> ArtifactEntryWire {
        ArtifactEntryWire {
            schema_version: ARTIFACT_REF_ENTRY_WIRE_SCHEMA_VERSION,
            stable_id: "research:notes/x.md".to_string(),
            ref_kind: "research".to_string(),
            canonical_argument: "notes/x.md".to_string(),
            display_label: "notes/x.md".to_string(),
            project_display_name: Some("sase".to_string()),
            repository: None,
            repo_relative_path: None,
            captured_revision: Some(
                "0123456789abcdef0123456789abcdef01234567".to_string(),
            ),
            captured_digest: Some("a".repeat(64)),
            logical_path: None,
            properties: BTreeMap::from([(
                "status".to_string(),
                "final".to_string(),
            )]),
            origin: ArtifactEntryOriginWire::PromptRef,
        }
    }

    #[test]
    fn valid_entry_passes() {
        validate_artifact_entry(&entry()).unwrap();
    }

    #[test]
    fn rejects_empty_stable_id_and_bad_kind() {
        let mut invalid = entry();
        invalid.stable_id = String::new();
        assert!(validate_artifact_entry(&invalid).is_err());

        let mut invalid = entry();
        invalid.ref_kind = "Bad Kind".to_string();
        assert!(validate_artifact_entry(&invalid).is_err());
    }

    #[test]
    fn rejects_malformed_revision_digest_and_property_keys() {
        let mut invalid = entry();
        invalid.captured_revision = Some("short".to_string());
        assert!(validate_artifact_entry(&invalid).is_err());

        let mut invalid = entry();
        invalid.captured_digest = Some("A".repeat(64));
        assert!(validate_artifact_entry(&invalid).is_err());

        let mut invalid = entry();
        invalid.properties =
            BTreeMap::from([("Bad-Key".to_string(), "x".to_string())]);
        assert!(validate_artifact_entry(&invalid).is_err());
    }
}
