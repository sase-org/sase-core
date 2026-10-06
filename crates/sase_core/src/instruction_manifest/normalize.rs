//! Normalize and validate an instruction manifest v1 value.
//!
//! Parsing uses the wire types (unknown fields rejected); invariants from
//! the E2 plan decision 8 are enforced here. `common_digest` is filled when
//! null and rejected when wrong.

use serde_json::Value as JsonValue;
use thiserror::Error;

use super::wire::{
    ActorWire, CacheWire, DeliveryStatusWire, InstructionManifestWire,
    LifecycleWire, ModeWire, SectionStatusWire,
    INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION,
};
use crate::finalizer::canonical_json_sha256;

/// Errors from [`normalize_instruction_manifest`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InstructionManifestError {
    /// The manifest names a schema version outside `1..=current`.
    #[error("unsupported instruction manifest schema_version {0}")]
    UnsupportedSchema(u32),
    /// Any other invariant failure; the message names the invariant.
    #[error("invalid instruction manifest: {0}")]
    Invalid(String),
}

/// Normalize one manifest JSON value.
///
/// On success the returned value is the same manifest with `common_digest`
/// filled when it was null. Unknown fields, bad enums, and every decision-8
/// invariant produce an error naming the failure.
pub fn normalize_instruction_manifest(
    value: &JsonValue,
) -> Result<JsonValue, InstructionManifestError> {
    let mut manifest: InstructionManifestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            InstructionManifestError::Invalid(format!(
                "manifest is not a valid InstructionManifestWire: {error}"
            ))
        })?;
    if manifest.schema_version == 0
        || manifest.schema_version > INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION
    {
        return Err(InstructionManifestError::UnsupportedSchema(
            manifest.schema_version,
        ));
    }
    validate_facts(&manifest)?;
    validate_bundle(&manifest)?;
    validate_budget(&manifest)?;
    validate_sections(&manifest)?;
    let expected = compute_common_digest(&manifest);
    match manifest.bundle.common_digest.as_deref() {
        None => manifest.bundle.common_digest = Some(expected),
        Some(supplied) => {
            if !is_sha256(supplied) {
                return Err(invalid(format!(
                    "bundle.common_digest {supplied:?} is not 64 lowercase hex"
                )));
            }
            if supplied != expected {
                return Err(invalid(format!(
                    "bundle.common_digest mismatch: supplied {supplied} != computed {expected}"
                )));
            }
        }
    }
    serde_json::to_value(&manifest).map_err(|error| {
        InstructionManifestError::Invalid(format!(
            "unable to re-encode normalized manifest: {error}"
        ))
    })
}

/// Compute `common_digest` for a parsed manifest.
///
/// sha256 of the canonical JSON array `[[id, sha256], …]` over included
/// sections with `provider_specific == false`, in bundle order, using the
/// sorted-key no-trailing-newline canonical form.
pub fn compute_common_digest(manifest: &InstructionManifestWire) -> String {
    let pairs: Vec<[String; 2]> = manifest
        .sections
        .iter()
        .filter(|section| {
            section.status == SectionStatusWire::Included
                && !section.provider_specific
        })
        .map(|section| {
            [
                section.id.clone(),
                section.sha256.clone().unwrap_or_default(),
            ]
        })
        .collect();
    let value = JsonValue::Array(
        pairs
            .into_iter()
            .map(|pair| {
                JsonValue::Array(
                    pair.into_iter().map(JsonValue::String).collect(),
                )
            })
            .collect(),
    );
    canonical_json_sha256(&value)
        .expect("common_digest input is always canonicalizable")
}

fn invalid(message: String) -> InstructionManifestError {
    InstructionManifestError::Invalid(message)
}

fn is_lower_hex(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && is_lower_hex(value)
}

fn is_blob_oid(value: &str) -> bool {
    (value.len() == 40 || value.len() == 64) && is_lower_hex(value)
}

fn is_provider_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_section_id_part(part: &str) -> bool {
    !part.is_empty()
        && part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn section_id_prefix(id: &str) -> Option<&str> {
    id.split('.').next()
}

fn validate_facts(
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    let facts = &manifest.facts;
    if !is_provider_identifier(&facts.provider) {
        return Err(invalid(format!(
            "facts.provider {:?} is not an identifier",
            facts.provider
        )));
    }
    if facts.host.is_empty() {
        return Err(invalid("facts.host must not be empty".to_string()));
    }
    if let Some(project) = facts.project.as_deref() {
        if project.is_empty() {
            return Err(invalid(
                "facts.project must not be empty when present".to_string(),
            ));
        }
    }
    let valid_combo = matches!(
        (facts.actor, facts.mode),
        (ActorWire::SaseRoot, ModeWire::Runtime)
            | (ActorWire::NativeHelper, ModeWire::Runtime)
            | (ActorWire::Interactive, ModeWire::Interactive)
            | (ActorWire::Interactive, ModeWire::Export)
    );
    if !valid_combo {
        return Err(invalid(format!(
            "invalid fact combination: actor {:?} with mode {:?}",
            actor_str(facts.actor),
            mode_str(facts.mode)
        )));
    }
    Ok(())
}

fn actor_str(actor: ActorWire) -> &'static str {
    match actor {
        ActorWire::SaseRoot => "sase_root",
        ActorWire::NativeHelper => "native_helper",
        ActorWire::Interactive => "interactive",
    }
}

fn mode_str(mode: ModeWire) -> &'static str {
    match mode {
        ModeWire::Runtime => "runtime",
        ModeWire::Interactive => "interactive",
        ModeWire::Export => "export",
    }
}

fn validate_bundle(
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    let bundle = &manifest.bundle;
    if !is_sha256(&bundle.sha256) {
        return Err(invalid(format!(
            "bundle.sha256 {:?} is not 64 lowercase hex",
            bundle.sha256
        )));
    }
    if let Some(common) = bundle.common_digest.as_deref() {
        if !is_sha256(common) {
            return Err(invalid(format!(
                "bundle.common_digest {common:?} is not 64 lowercase hex"
            )));
        }
    }
    if manifest.compiler.name.is_empty() {
        return Err(invalid("compiler.name must not be empty".to_string()));
    }
    Ok(())
}

fn validate_budget(
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    for key in manifest.budget.by_layer.keys() {
        if !matches!(
            key.as_str(),
            "frame" | "package" | "plugin" | "home" | "project" | "launch"
        ) {
            return Err(invalid(format!(
                "budget.by_layer key {key:?} is not a layer"
            )));
        }
    }
    Ok(())
}

fn validate_sections(
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    use std::collections::BTreeSet;

    let mut seen = BTreeSet::new();
    for section in &manifest.sections {
        if !seen.insert(section.id.clone()) {
            return Err(invalid(format!(
                "duplicate section id {:?}",
                section.id
            )));
        }
        validate_section_id(section)?;
        validate_section_layer_prefix(section)?;
        validate_section_digests(section)?;
        validate_section_sources(section)?;
        validate_section_status_fields(section)?;
        validate_section_lifecycle(section, manifest)?;
        validate_section_provider_specific(section, manifest)?;
        validate_section_required(section)?;
    }
    validate_offsets(manifest)?;
    Ok(())
}

fn validate_section_id(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    let parts: Vec<&str> = section.id.split('.').collect();
    if parts.len() < 2 {
        return Err(invalid(format!(
            "section id {:?} must have at least two dot parts",
            section.id
        )));
    }
    let prefix = parts[0];
    if !matches!(
        prefix,
        "frame" | "pkg" | "plugin" | "home" | "proj" | "launch"
    ) {
        return Err(invalid(format!(
            "section id {:?} has unknown prefix {prefix:?}",
            section.id
        )));
    }
    for part in parts {
        if !is_section_id_part(part) {
            return Err(invalid(format!(
                "section id {:?} has invalid part {part:?}",
                section.id
            )));
        }
    }
    let _ = section_id_prefix(&section.id);
    Ok(())
}

fn validate_section_layer_prefix(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    use super::wire::LayerWire;
    let prefix = section_id_prefix(&section.id).unwrap_or("");
    let expected = match section.layer {
        LayerWire::Frame => "frame",
        LayerWire::Package => "pkg",
        LayerWire::Plugin => "plugin",
        LayerWire::Home => "home",
        LayerWire::Project => "proj",
        LayerWire::Launch => "launch",
    };
    if prefix != expected {
        return Err(invalid(format!(
            "section id {:?} prefix {prefix:?} does not match layer {expected:?}",
            section.id
        )));
    }
    Ok(())
}

fn validate_section_digests(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    match section.status {
        SectionStatusWire::Included => {
            let Some(sha) = section.sha256.as_deref() else {
                return Err(invalid(format!(
                    "included section {:?} must have sha256",
                    section.id
                )));
            };
            if !is_sha256(sha) {
                return Err(invalid(format!(
                    "section {:?} sha256 {sha:?} is not 64 lowercase hex",
                    section.id
                )));
            }
        }
        SectionStatusWire::Excluded => {
            if let Some(sha) = section.sha256.as_deref() {
                if !is_sha256(sha) {
                    return Err(invalid(format!(
                        "section {:?} sha256 {sha:?} is not 64 lowercase hex",
                        section.id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_section_sources(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    for source in &section.sources {
        if source.path.is_empty() {
            return Err(invalid(format!(
                "section {:?} has empty source path",
                section.id
            )));
        }
        if let Some(sha) = source.sha256.as_deref() {
            if !is_sha256(sha) {
                return Err(invalid(format!(
                    "section {:?} source sha256 {sha:?} is not 64 lowercase hex",
                    section.id
                )));
            }
        }
        if let Some(oid) = source.blob_oid.as_deref() {
            if !is_blob_oid(oid) {
                return Err(invalid(format!(
                    "section {:?} source blob_oid {oid:?} is not 40 or 64 lowercase hex",
                    section.id
                )));
            }
        }
    }
    Ok(())
}

fn validate_section_status_fields(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    match section.status {
        SectionStatusWire::Included => {
            if section.offset.is_none() || section.length.is_none() {
                return Err(invalid(format!(
                    "included section {:?} must have offset and length",
                    section.id
                )));
            }
            if section.reason.is_some() {
                return Err(invalid(format!(
                    "included section {:?} must not have a reason",
                    section.id
                )));
            }
        }
        SectionStatusWire::Excluded => {
            if section.offset.is_some() || section.length.is_some() {
                return Err(invalid(format!(
                    "excluded section {:?} must not have offset or length",
                    section.id
                )));
            }
            if section.reason.is_none() {
                return Err(invalid(format!(
                    "excluded section {:?} must have a reason",
                    section.id
                )));
            }
        }
    }
    Ok(())
}

fn validate_section_lifecycle(
    section: &super::wire::SectionWire,
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    use super::wire::SectionStatusWire;
    if section.status == SectionStatusWire::Excluded {
        return Ok(());
    }
    let facts = &manifest.facts;
    match section.lifecycle {
        LifecycleWire::Neutral => Ok(()),
        LifecycleWire::Root => {
            if facts.actor == ActorWire::SaseRoot
                && facts.mode == ModeWire::Runtime
            {
                Ok(())
            } else {
                Err(invalid(format!(
                    "root section {:?} requires runtime + sase_root",
                    section.id
                )))
            }
        }
        LifecycleWire::Helper => {
            if facts.actor == ActorWire::NativeHelper
                && facts.mode == ModeWire::Runtime
            {
                Ok(())
            } else {
                Err(invalid(format!(
                    "helper section {:?} requires runtime + native_helper",
                    section.id
                )))
            }
        }
    }
}

fn validate_section_provider_specific(
    section: &super::wire::SectionWire,
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    let expected = format!("pkg.provider.{}", manifest.facts.provider);
    if section.provider_specific {
        if section.id != expected {
            return Err(invalid(format!(
                "provider_specific section {:?} must be exactly {expected:?}",
                section.id
            )));
        }
    } else if section.id == expected {
        return Err(invalid(format!(
            "section {expected:?} must set provider_specific",
        )));
    }
    Ok(())
}

fn validate_section_required(
    section: &super::wire::SectionWire,
) -> Result<(), InstructionManifestError> {
    if section.required && section.status == SectionStatusWire::Excluded {
        let allowed = matches!(
            section.reason,
            Some(super::wire::SectionReasonWire::Overlay)
                | Some(super::wire::SectionReasonWire::Mode)
        );
        if !allowed {
            return Err(invalid(format!(
                "required section {:?} must not be excluded except by overlay or mode",
                section.id
            )));
        }
    }
    Ok(())
}

fn validate_offsets(
    manifest: &InstructionManifestWire,
) -> Result<(), InstructionManifestError> {
    let mut cursor: u64 = 0;
    for section in &manifest.sections {
        if section.status != SectionStatusWire::Included {
            continue;
        }
        let offset = section.offset.unwrap_or(u64::MAX);
        let length = section.length.unwrap_or(u64::MAX);
        if offset != cursor {
            return Err(invalid(format!(
                "included section {:?} offset {offset} is not contiguous (expected {cursor})",
                section.id
            )));
        }
        cursor = cursor.saturating_add(length);
    }
    if cursor != manifest.bundle.bytes {
        return Err(invalid(format!(
            "included section lengths sum to {cursor} but bundle.bytes is {}",
            manifest.bundle.bytes
        )));
    }
    let _ = (CacheWire::Hit, DeliveryStatusWire::Shadow);
    Ok(())
}
