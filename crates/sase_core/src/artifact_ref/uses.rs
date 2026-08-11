//! Immutable per-occurrence artifact-reference use records and their JSONL
//! manifest.
//!
//! Modelled on `prompt_artifact.rs`'s tolerant parser: a malformed or
//! future-versioned line is skipped and reported rather than failing the
//! whole file, since the manifest is append-only and one corrupt append must
//! not make every other row inaccessible.

use serde::{Deserialize, Serialize};

use super::validate_kind;
use super::wire::ArtifactRefError;

pub const ARTIFACT_REF_USE_WIRE_SCHEMA_VERSION: u64 = 1;

/// One immutable record of an artifact reference being used in a prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefUseRecordWire {
    pub schema_version: u64,
    pub recorded_at: String,
    pub agent_name: String,
    pub raw_ref: String,
    pub canonical_ref: String,
    pub ref_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_id: Option<String>,
    pub prompt_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_file: Option<String>,
}

/// Parse a JSONL manifest, skipping malformed or unsupported rows.
pub fn parse_artifact_ref_use_manifest(
    bytes: &[u8],
) -> Vec<ArtifactRefUseRecordWire> {
    let mut records = Vec::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        let line_number = index + 1;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<ArtifactRefUseRecordWire>(line) {
            Ok(record)
                if record.schema_version
                    == ARTIFACT_REF_USE_WIRE_SCHEMA_VERSION =>
            {
                records.push(record);
            }
            Ok(record) => eprintln!(
                "artifact reference use manifest line {line_number}: unsupported schema_version {}",
                record.schema_version
            ),
            Err(error) => eprintln!(
                "artifact reference use manifest line {line_number}: {error}"
            ),
        }
    }
    records
}

/// Render one compact JSON manifest row without a trailing newline.
pub fn render_artifact_ref_use_record(
    record: &ArtifactRefUseRecordWire,
) -> Result<String, serde_json::Error> {
    if record.schema_version != ARTIFACT_REF_USE_WIRE_SCHEMA_VERSION {
        return Err(<serde_json::Error as serde::ser::Error>::custom(format!(
            "unsupported artifact reference use manifest schema_version {}",
            record.schema_version
        )));
    }
    serde_json::to_string(record)
}

/// Validate one use record's required, non-empty, well-shaped fields.
pub fn validate_artifact_ref_use_record(
    record: &ArtifactRefUseRecordWire,
) -> Result<(), ArtifactRefError> {
    if record.agent_name.is_empty() {
        return Err(ArtifactRefError::validation(
            "artifact reference use record agent_name must not be empty",
        ));
    }
    if record.raw_ref.is_empty() {
        return Err(ArtifactRefError::validation(
            "artifact reference use record raw_ref must not be empty",
        ));
    }
    if record.canonical_ref.is_empty() {
        return Err(ArtifactRefError::validation(
            "artifact reference use record canonical_ref must not be empty",
        ));
    }
    validate_kind(&record.ref_kind)?;
    if let Some(stable_id) = &record.stable_id {
        if stable_id.is_empty() {
            return Err(ArtifactRefError::validation(
                "artifact reference use record stable_id must not be empty when present",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(raw_ref: &str) -> ArtifactRefUseRecordWire {
        ArtifactRefUseRecordWire {
            schema_version: ARTIFACT_REF_USE_WIRE_SCHEMA_VERSION,
            recorded_at: "2026-08-01T14:22:03Z".to_string(),
            agent_name: "bbugyi200.athena.sase-js.1".to_string(),
            raw_ref: raw_ref.to_string(),
            canonical_ref: raw_ref.to_string(),
            ref_kind: "bead".to_string(),
            stable_id: Some("bead:sase-js.1".to_string()),
            prompt_text: format!("@{raw_ref}"),
            publication_target: None,
            captured_file: None,
        }
    }

    #[test]
    fn valid_record_passes() {
        validate_artifact_ref_use_record(&record("bead:sase-js.1")).unwrap();
    }

    #[test]
    fn rejects_empty_required_fields_and_bad_kind() {
        let mut invalid = record("bead:sase-js.1");
        invalid.agent_name = String::new();
        assert!(validate_artifact_ref_use_record(&invalid).is_err());

        let mut invalid = record("bead:sase-js.1");
        invalid.raw_ref = String::new();
        assert!(validate_artifact_ref_use_record(&invalid).is_err());

        let mut invalid = record("bead:sase-js.1");
        invalid.ref_kind = "Bad Kind".to_string();
        assert!(validate_artifact_ref_use_record(&invalid).is_err());

        let mut invalid = record("bead:sase-js.1");
        invalid.stable_id = Some(String::new());
        assert!(validate_artifact_ref_use_record(&invalid).is_err());
    }

    #[test]
    fn manifest_round_trip_skips_bad_and_future_lines() {
        let expected = record("bead:sase-js.1");
        let rendered = render_artifact_ref_use_record(&expected).unwrap();
        let manifest =
            format!("{rendered}\nnot json\n{{\"schema_version\":99}}\n\n");
        assert_eq!(
            parse_artifact_ref_use_manifest(manifest.as_bytes()),
            vec![expected]
        );
    }

    #[test]
    fn manifest_parse_tolerates_unknown_fields() {
        let rendered =
            render_artifact_ref_use_record(&record("bead:sase-js.1")).unwrap();
        let with_unknown =
            rendered.replacen('{', "{\"future_field\":{\"nested\":true},", 1);
        assert_eq!(
            parse_artifact_ref_use_manifest(with_unknown.as_bytes()).len(),
            1
        );
    }
}
