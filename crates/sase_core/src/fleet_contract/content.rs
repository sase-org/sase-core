use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_CAPABILITY_BYTES;
use super::locators::logical_key_unchecked;
use super::locators::LogicalAgentLocatorWire;
use super::validation::reject_path_like;
use super::validation::reject_secretish;
use super::validation::validate_identifier;
use super::validation::validate_key;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::validate_sha256_digest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Closed content-handle kind. Handles are opaque and never expose local
/// paths, PIDs, process groups, raw bearer tokens, or auth headers.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ContentHandleKindWire {
    Transcript,
    Output,
    Diff,
    Log,
    Artifact,
    Question,
}

/// Safe content handle metadata for lazy detail retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentHandleWire {
    pub schema_version: u32,
    pub id: String,
    pub kind: ContentHandleKindWire,
    pub revision: Option<ResourceRevisionWire>,
    pub digest: Option<String>,
    pub byte_len: Option<u64>,
    pub supports_range: bool,
    pub supports_growth: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMetadataWire {
    pub schema_version: u32,
    pub handle_count: u64,
    pub total_byte_len: Option<u64>,
    pub kinds: Vec<ContentHandleKindWire>,
    pub supports_range: bool,
    pub supports_growth: bool,
}

/// Resource/action revision used as a mutation precondition. This is not a
/// feed cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRevisionWire {
    pub schema_version: u32,
    pub logical_key: String,
    pub revision: u64,
}

/// Normalized resource, host, and protocol capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitySetWire {
    pub schema_version: u32,
    pub resource: Vec<String>,
    pub host: Vec<String>,
    pub protocol: Vec<String>,
}

pub fn validate_content_handle(
    handle: &ContentHandleWire,
) -> Result<ContentHandleWire, FleetContractError> {
    handle.validate()?;
    Ok(handle.clone())
}

impl ResourceRevisionWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("resource revision", self.schema_version)?;
        validate_key("resource revision logical_key", &self.logical_key)
    }

    pub(crate) fn validate_for_logical(
        &self,
        logical: &LogicalAgentLocatorWire,
    ) -> Result<(), FleetContractError> {
        self.validate()?;
        let expected = logical_key_unchecked(logical);
        if self.logical_key != expected {
            return Err(FleetContractError::Validation(
                "resource revision belongs to a different logical identity"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl CapabilitySetWire {
    pub(crate) fn normalized(&self) -> Result<Self, FleetContractError> {
        validate_schema("capability set", self.schema_version)?;
        Ok(Self {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            resource: normalize_capabilities("resource", &self.resource)?,
            host: normalize_capabilities("host", &self.host)?,
            protocol: normalize_capabilities("protocol", &self.protocol)?,
        })
    }

    pub(crate) fn content_is_normalized(
        &self,
    ) -> Result<bool, FleetContractError> {
        let normalized = self.normalized()?;
        Ok(self.resource == normalized.resource
            && self.host == normalized.host
            && self.protocol == normalized.protocol)
    }
}

impl ContentHandleWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("content handle", self.schema_version)?;
        validate_reference_id("content handle id", &self.id)?;
        reject_path_like("content handle id", &self.id)?;
        reject_secretish("content handle id", &self.id)?;
        if let Some(revision) = &self.revision {
            revision.validate()?;
        }
        if let Some(digest) = &self.digest {
            validate_sha256_digest("content handle digest", digest)?;
        }
        Ok(())
    }
}

impl ContentMetadataWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("content metadata", self.schema_version)?;
        let mut previous = None;
        for kind in &self.kinds {
            if previous.is_some_and(|prior| prior >= *kind) {
                return Err(FleetContractError::Validation(
                    "content metadata kinds must be sorted and deduplicated"
                        .to_string(),
                ));
            }
            previous = Some(*kind);
        }
        Ok(())
    }
}

fn validate_capability(
    scope: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    validate_identifier(scope, value)?;
    if value.len() > MAX_CAPABILITY_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability exceeds {MAX_CAPABILITY_BYTES} bytes"
        )));
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability must be non-empty"
        )));
    };
    if !first.is_ascii_lowercase() {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability must start with a lowercase ASCII letter"
        )));
    }
    if !chars.all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | '_' | '-')
    }) {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability contains unsupported characters"
        )));
    }
    Ok(())
}

fn normalize_capabilities(
    scope: &str,
    values: &[String],
) -> Result<Vec<String>, FleetContractError> {
    let mut set = BTreeSet::new();
    for value in values {
        validate_capability(scope, value)?;
        set.insert(value.trim().to_string());
    }
    Ok(set.into_iter().collect())
}
