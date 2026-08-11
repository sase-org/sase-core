//! Wire types and validation for one document-provider's ref specification.
//!
//! Spec merge (the `use:` plus inline layering config callers do before a
//! spec reaches this module) stays in Python, per the Rust/Python backend
//! boundary; this module only validates an already-assembled spec and gives
//! it a stable content digest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::filter::ArtifactPathFilter;
use super::validate_artifact_ref_expansion_format;
use super::wire::ArtifactRefError;

pub const ARTIFACT_REF_PROVIDER_SPEC_WIRE_SCHEMA_VERSION: u64 = 1;

const RESERVED_KINDS: &[&str] = &["stitch", "patch", "bead", "agent", "file"];
const PROPERTY_TYPES: &[&str] = &[
    "string",
    "enum",
    "boolean",
    "integer",
    "number",
    "date",
    "datetime",
    "string_list",
];
const PUBLICATION_LINKS: &[&str] = &["vcs_permalink", "agents_object", "none"];
const PUBLICATION_REFERENCED_BY: &[&str] = &["markdown_table", "none"];
const PROPERTY_SOURCES: &[&str] = &["markdown_frontmatter", "provider"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefProviderSpecWire {
    pub schema_version: u64,
    pub provider: String,
    #[serde(rename = "ref")]
    pub reference: ArtifactRefProviderRefSpecWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefProviderRefSpecWire {
    pub kind: String,
    pub expansion_format: String,
    #[serde(default)]
    pub properties: BTreeMap<String, ArtifactRefPropertySpecWire>,
    #[serde(default)]
    pub detail: ArtifactRefDetailSpecWire,
    #[serde(default)]
    pub identity: ArtifactRefIdentitySpecWire,
    #[serde(default)]
    pub inventory: ArtifactRefInventorySpecWire,
    pub publication: ArtifactRefPublicationSpecWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefPropertySpecWire {
    #[serde(rename = "type")]
    pub property_type: String,
    #[serde(default)]
    pub values: Vec<String>,
    pub source: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefDetailSpecWire {
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefIdentitySpecWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefInventorySpecWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub globs: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefPublicationSpecWire {
    pub link: String,
    pub referenced_by: String,
}

/// Validate one assembled provider spec.
pub fn validate_artifact_ref_provider_spec(
    spec: &ArtifactRefProviderSpecWire,
) -> Result<(), ArtifactRefError> {
    if spec.schema_version != ARTIFACT_REF_PROVIDER_SPEC_WIRE_SCHEMA_VERSION {
        return Err(ArtifactRefError::validation(format!(
            "unsupported artifact reference provider spec schema_version {}; expected {}",
            spec.schema_version, ARTIFACT_REF_PROVIDER_SPEC_WIRE_SCHEMA_VERSION
        )));
    }
    validate_identifier(&spec.provider, "provider id")?;
    validate_identifier(&spec.reference.kind, "ref kind")?;
    if RESERVED_KINDS.contains(&spec.reference.kind.as_str()) {
        return Err(ArtifactRefError::validation(format!(
            "ref kind '{}' is reserved and cannot be claimed by a document provider",
            spec.reference.kind
        )));
    }
    validate_artifact_ref_expansion_format(&spec.reference.expansion_format)?;

    for (name, property) in &spec.reference.properties {
        if !PROPERTY_TYPES.contains(&property.property_type.as_str()) {
            return Err(ArtifactRefError::validation(format!(
                "property '{name}' has unsupported type '{}'",
                property.property_type
            )));
        }
        if property.property_type == "enum" && property.values.is_empty() {
            return Err(ArtifactRefError::validation(format!(
                "enum property '{name}' must declare at least one value"
            )));
        }
        if !PROPERTY_SOURCES.contains(&property.source.as_str()) {
            return Err(ArtifactRefError::validation(format!(
                "property '{name}' has unsupported source '{}'",
                property.source
            )));
        }
    }

    for field in &spec.reference.detail.fields {
        if !spec.reference.properties.contains_key(field) {
            return Err(ArtifactRefError::validation(format!(
                "detail field '{field}' does not name a declared property"
            )));
        }
    }
    if let Some(property) = &spec.reference.identity.property {
        if !spec.reference.properties.contains_key(property) {
            return Err(ArtifactRefError::validation(format!(
                "identity property '{property}' does not name a declared property"
            )));
        }
    }

    if let Some(globs) = &spec.reference.inventory.globs {
        ArtifactPathFilter::compile(Some(globs))?;
    }

    if !PUBLICATION_LINKS.contains(&spec.reference.publication.link.as_str()) {
        return Err(ArtifactRefError::validation(format!(
            "publication.link must be one of vcs_permalink, agents_object, none; got '{}'",
            spec.reference.publication.link
        )));
    }
    if !PUBLICATION_REFERENCED_BY
        .contains(&spec.reference.publication.referenced_by.as_str())
    {
        return Err(ArtifactRefError::validation(format!(
            "publication.referenced_by must be markdown_table or none; got '{}'",
            spec.reference.publication.referenced_by
        )));
    }

    Ok(())
}

/// Compute a stable sha256 hex digest over the normalized spec.
pub fn artifact_ref_provider_spec_digest(
    spec: &ArtifactRefProviderSpecWire,
) -> Result<String, ArtifactRefError> {
    let normalized = serde_json::to_vec(spec).map_err(|error| {
        ArtifactRefError::validation(format!(
            "unable to normalize provider spec for digest: {error}"
        ))
    })?;
    Ok(hex::encode(Sha256::digest(&normalized)))
}

fn validate_identifier(
    value: &str,
    label: &str,
) -> Result<(), ArtifactRefError> {
    let mut bytes = value.bytes();
    let valid = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'-')
        });
    if !valid {
        return Err(ArtifactRefError::validation(format!(
            "{label} must match [a-z][a-z0-9_-]*, got '{value}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_spec() -> ArtifactRefProviderSpecWire {
        ArtifactRefProviderSpecWire {
            schema_version: ARTIFACT_REF_PROVIDER_SPEC_WIRE_SCHEMA_VERSION,
            provider: "research".to_string(),
            reference: ArtifactRefProviderRefSpecWire {
                kind: "research".to_string(),
                expansion_format: "{kind}:{argument}".to_string(),
                properties: BTreeMap::from([(
                    "status".to_string(),
                    ArtifactRefPropertySpecWire {
                        property_type: "enum".to_string(),
                        values: vec!["draft".to_string(), "final".to_string()],
                        source: "markdown_frontmatter".to_string(),
                    },
                )]),
                detail: ArtifactRefDetailSpecWire {
                    fields: vec!["status".to_string()],
                },
                identity: ArtifactRefIdentitySpecWire {
                    property: Some("status".to_string()),
                },
                inventory: ArtifactRefInventorySpecWire {
                    globs: Some(vec!["**/*.md".to_string()]),
                },
                publication: ArtifactRefPublicationSpecWire {
                    link: "vcs_permalink".to_string(),
                    referenced_by: "markdown_table".to_string(),
                },
            },
        }
    }

    #[test]
    fn valid_spec_passes_and_digest_is_stable() {
        let spec = valid_spec();
        validate_artifact_ref_provider_spec(&spec).unwrap();
        let first = artifact_ref_provider_spec_digest(&spec).unwrap();
        let second = artifact_ref_provider_spec_digest(&spec).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);

        let mut changed = spec.clone();
        changed.reference.kind = "other".to_string();
        let third = artifact_ref_provider_spec_digest(&changed).unwrap();
        assert_ne!(first, third);
    }

    #[test]
    fn rejects_reserved_kind_and_bad_identifiers() {
        let mut spec = valid_spec();
        spec.reference.kind = "stitch".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.provider = "Research".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }

    #[test]
    fn rejects_bad_expansion_format() {
        let mut spec = valid_spec();
        spec.reference.expansion_format = "{bogus}".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }

    #[test]
    fn rejects_property_type_enum_and_source_issues() {
        let mut spec = valid_spec();
        spec.reference
            .properties
            .get_mut("status")
            .unwrap()
            .property_type = "object".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.reference.properties.get_mut("status").unwrap().values = vec![];
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.reference.properties.get_mut("status").unwrap().source =
            "database".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }

    #[test]
    fn rejects_undeclared_detail_and_identity_properties() {
        let mut spec = valid_spec();
        spec.reference.detail.fields = vec!["missing".to_string()];
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.reference.identity.property = Some("missing".to_string());
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }

    #[test]
    fn rejects_bad_inventory_globs_and_publication_values() {
        let mut spec = valid_spec();
        spec.reference.inventory.globs = Some(vec!["../escape".to_string()]);
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.reference.publication.link = "http".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());

        let mut spec = valid_spec();
        spec.reference.publication.referenced_by = "yaml_list".to_string();
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let mut spec = valid_spec();
        spec.schema_version = 99;
        assert!(validate_artifact_ref_provider_spec(&spec).is_err());
    }
}
