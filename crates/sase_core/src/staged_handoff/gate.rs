//! Versioned typed gate payload for staged gate handoffs.
//!
//! The outer staged-handoff wire already permits `kind: gate` with an object
//! payload. This module gives that payload a versioned schema: the normalized
//! gate spec, the frozen auto evaluation, resource snapshot metadata, and the
//! creator identity the publisher needs after the CLI environment is gone.
//!
//! Python assembles the spec and captures resource bytes into private staging
//! storage; Rust validates shape and cross-field consistency, including the
//! workspace-mode/disposition rule: an inherited workspace maps to
//! `successor_workspace: inherit` with `carry_allowed: true`, while a
//! released workspace maps to `none` with `carry_allowed: false`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use super::validate::StagedHandoffError;

/// Wire schema version for the staged gate payload.
pub const GATE_STAGED_PAYLOAD_SCHEMA_VERSION: u32 = 1;

/// Return the staged gate payload schema version.
pub fn gate_staged_payload_schema_version() -> u32 {
    GATE_STAGED_PAYLOAD_SCHEMA_VERSION
}

/// Workspace disposition declared by a staged gate payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateWorkspaceModeWire {
    Inherit,
    Release,
}

/// Snapshot metadata for one source-backed gate resource.
///
/// The bytes live in private staging storage under `staged_relpath`; this
/// record carries only the identity and integrity metadata the publisher
/// needs to materialize the bundle without reopening the CLI working
/// directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateResourceSnapshotWire {
    pub path: String,
    pub role: String,
    pub executable: bool,
    pub sha256: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub staged_relpath: String,
}

/// Frozen automatic-resolution evaluation for a staged gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAutoSnapshotWire {
    pub decision: Map<String, JsonValue>,
    pub policy_block: Map<String, JsonValue>,
    pub auto_execute: bool,
}

/// Creator identity persisted so the publisher can publish without the
/// original CLI environment. Only the fields publication needs are stored,
/// never the whole process environment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateCreatorWire {
    pub project: String,
    pub project_file: String,
    pub artifacts_dir: String,
    pub timestamp: String,
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_num: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub meta: Map<String, JsonValue>,
}

/// Durable staged gate payload nested under a `kind: gate` record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateStagedPayloadWire {
    pub gate_schema_version: u32,
    pub request_id: String,
    pub spec: Map<String, JsonValue>,
    pub auto: GateAutoSnapshotWire,
    pub resources: Vec<GateResourceSnapshotWire>,
    pub creator: GateCreatorWire,
    pub workspace_mode: GateWorkspaceModeWire,
}

/// Roles a staged gate resource snapshot may carry.
const GATE_RESOURCE_ROLES: [&str; 4] =
    ["attachment", "command", "editable", "preview"];

/// Validate and re-encode one staged gate payload JSON value.
pub fn validate_gate_staged_payload(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let payload: GateStagedPayloadWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "payload is not a valid GateStagedPayloadWire: {error}"
            ))
        })?;
    reject_gate_schema(payload.gate_schema_version)?;
    validate_payload(&payload)?;
    encode(&payload)
}

/// Assemble a staged gate payload from its parts, defaulting an absent
/// schema version before validation.
///
/// Python assembles the spec, auto snapshot, resource snapshots, and creator
/// identity; this call stamps the current schema version when the caller
/// omits it, then validates and re-encodes the canonical payload.
pub fn build_gate_staged_payload(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let mut raw = value.clone();
    if let JsonValue::Object(fields) = &mut raw {
        fields
            .entry("gate_schema_version".to_string())
            .or_insert(JsonValue::from(GATE_STAGED_PAYLOAD_SCHEMA_VERSION));
    }
    validate_gate_staged_payload(&raw)
}

/// Return the workspace mode declared by a validated gate payload.
pub fn gate_payload_workspace_mode(
    value: &JsonValue,
) -> Result<GateWorkspaceModeWire, StagedHandoffError> {
    let normalized = validate_gate_staged_payload(value)?;
    let payload: GateStagedPayloadWire =
        serde_json::from_value(normalized).map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "normalized payload is not a valid GateStagedPayloadWire: {error}"
            ))
        })?;
    Ok(payload.workspace_mode)
}

fn reject_gate_schema(schema_version: u32) -> Result<(), StagedHandoffError> {
    if schema_version == 0
        || schema_version > GATE_STAGED_PAYLOAD_SCHEMA_VERSION
    {
        return Err(StagedHandoffError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn validate_payload(
    payload: &GateStagedPayloadWire,
) -> Result<(), StagedHandoffError> {
    if payload.request_id.trim().is_empty() {
        return Err(invalid("payload.request_id must be a non-empty string"));
    }
    match payload.spec.get("request_id") {
        Some(JsonValue::String(spec_request_id))
            if *spec_request_id == payload.request_id =>
        {
            Ok(())
        }
        Some(JsonValue::String(_)) => Err(invalid(
            "payload.spec.request_id must equal payload.request_id",
        )),
        _ => Err(invalid(
            "payload.spec.request_id is required and must be a string",
        )),
    }?;
    validate_spec_resources(&payload.spec, &payload.resources)?;
    for snapshot in &payload.resources {
        validate_snapshot(snapshot)?;
    }
    validate_creator(&payload.creator)?;
    Ok(())
}

fn validate_spec_resources(
    spec: &Map<String, JsonValue>,
    snapshots: &[GateResourceSnapshotWire],
) -> Result<(), StagedHandoffError> {
    let resources = match spec.get("resources") {
        Some(JsonValue::Array(resources)) => resources,
        _ => {
            return Err(invalid(
                "payload.spec.resources is required and must be an array",
            ));
        }
    };
    for (index, resource) in resources.iter().enumerate() {
        let target = format!("payload.spec.resources[{index}]");
        let fields = match resource {
            JsonValue::Object(fields) => fields,
            _ => {
                return Err(invalid(format!("{target} must be an object")));
            }
        };
        let path = match fields.get("path") {
            Some(JsonValue::String(path)) if !path.trim().is_empty() => path,
            _ => {
                return Err(invalid(format!(
                    "{target}.path must be a non-empty string"
                )));
            }
        };
        let executable = match fields.get("executable") {
            None | Some(JsonValue::Null) => false,
            Some(JsonValue::Bool(executable)) => *executable,
            _ => {
                return Err(invalid(format!(
                    "{target}.executable must be a boolean"
                )));
            }
        };
        let snapshot = snapshots.iter().find(|entry| entry.path == *path);
        let has_source =
            matches!(fields.get("source"), Some(JsonValue::String(_)));
        let has_content =
            !matches!(fields.get("content"), None | Some(JsonValue::Null));
        if has_source == has_content {
            return Err(invalid(format!(
                "{target} must declare exactly one of content or source"
            )));
        }
        if has_source {
            let source = fields
                .get("source")
                .and_then(JsonValue::as_str)
                .expect("source is a string");
            let entry = snapshot.ok_or_else(|| {
                invalid(format!(
                    "{target} reads source {source:?} but has no resource snapshot"
                ))
            })?;
            if entry.executable != executable {
                return Err(invalid(format!(
                    "{target} snapshot executable must match the spec resource"
                )));
            }
            if entry.origin.as_deref() != Some(source) {
                return Err(invalid(format!(
                    "{target} snapshot origin must preserve the spec source"
                )));
            }
        } else if snapshot.is_some() {
            return Err(invalid(format!(
                "{target} carries inline content but has a resource snapshot"
            )));
        }
    }
    for snapshot in snapshots {
        if !resources.iter().any(|resource| {
            resource.get("path").is_some_and(|path| {
                path == &JsonValue::from(snapshot.path.as_str())
            })
        }) {
            return Err(invalid(format!(
                "resource snapshot {:?} has no matching spec resource",
                snapshot.path
            )));
        }
    }
    Ok(())
}

fn validate_snapshot(
    snapshot: &GateResourceSnapshotWire,
) -> Result<(), StagedHandoffError> {
    if snapshot.path.trim().is_empty() {
        return Err(invalid(
            "resource snapshot path must be a non-empty string",
        ));
    }
    if !GATE_RESOURCE_ROLES.contains(&snapshot.role.as_str()) {
        return Err(invalid(format!(
            "resource snapshot role {:?} must be attachment, command, editable, or preview",
            snapshot.role
        )));
    }
    if snapshot.sha256.len() != 64
        || !snapshot.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid(format!(
            "resource snapshot {:?} sha256 must be 64 hex digits",
            snapshot.path
        )));
    }
    if snapshot.staged_relpath.trim().is_empty() {
        return Err(invalid(format!(
            "resource snapshot {:?} staged_relpath must be a non-empty string",
            snapshot.path
        )));
    }
    match snapshot.origin.as_deref() {
        None => Ok(()),
        Some(origin) if !origin.trim().is_empty() => Ok(()),
        _ => Err(invalid(format!(
            "resource snapshot {:?} origin must be a non-empty string",
            snapshot.path
        ))),
    }
}

fn validate_creator(
    creator: &GateCreatorWire,
) -> Result<(), StagedHandoffError> {
    for (field, value) in [
        ("project", creator.project.as_str()),
        ("project_file", creator.project_file.as_str()),
        ("artifacts_dir", creator.artifacts_dir.as_str()),
        ("timestamp", creator.timestamp.as_str()),
        ("agent", creator.agent.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(invalid(format!(
                "payload.creator.{field} must be a non-empty string"
            )));
        }
    }
    for (field, value) in [
        ("agent_session", creator.agent_session.as_deref()),
        ("target", creator.target.as_deref()),
        ("cl_name", creator.cl_name.as_deref()),
    ] {
        if value.is_some_and(|text| text.trim().is_empty()) {
            return Err(invalid(format!(
                "payload.creator.{field} must be a non-empty string"
            )));
        }
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> Result<JsonValue, StagedHandoffError> {
    serde_json::to_value(value).map_err(|error| {
        StagedHandoffError::Invalid(format!(
            "unable to re-encode staged gate payload value: {error}"
        ))
    })
}

fn invalid(message: impl Into<String>) -> StagedHandoffError {
    StagedHandoffError::Invalid(message.into())
}
