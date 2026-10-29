//! Versioned staged-monitor launch payload: the frozen record an adopter
//! replays after host finalization.
//!
//! The CLI gathers every fact at stage time and freezes them here so
//! adoption needs neither the exited CLI's environment nor a live
//! `StartIdentity` object. Durable paths and the existing ToolRun launch
//! envelope are referenced, never duplicated: secrets and full repository
//! contents stay where they already live.
//!
//! Historic minimal monitor staged records keep validating through
//! [`super::validate_staged_handoff`]; this payload is validated
//! explicitly at the monitor staging/adoption boundary instead.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use super::validate::StagedHandoffError;
use crate::continuation::prepared_coverage::PreparedReservationWire;

/// Wire schema version for staged-monitor launch payloads.
pub const STAGED_MONITOR_LAUNCH_PAYLOAD_SCHEMA_VERSION: u32 = 1;

/// Monitor continuation protocols the payload may freeze.
const PROTOCOL_LEGACY: &str = "legacy";
const PROTOCOL_RECORDS_V1: &str = "records_v1";

const MAX_COMMAND_PARTS: usize = 256;
const MAX_COMMAND_PART_BYTES: usize = 16 * 1024;
const MAX_LABEL_BYTES: usize = 1024;
const MAX_PARENTS: usize = 32;

/// Return the staged-monitor launch payload wire schema version.
pub fn staged_monitor_launch_payload_schema_version() -> u32 {
    STAGED_MONITOR_LAUNCH_PAYLOAD_SCHEMA_VERSION
}

/// Starter identity frozen at stage time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorStarterWire {
    pub agent_id: String,
    pub run_id: String,
    pub artifacts_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

/// Workspace and claim context frozen at stage time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorWorkspaceWire {
    /// Successor workspace mode; staged monitors always use `inherit`.
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_context: Option<Map<String, JsonValue>>,
}

/// Continuation parents, protocol, and checkpoint frozen at stage time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorContinuationWire {
    #[serde(default)]
    pub parent_node_ids: Vec<String>,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_ref: Option<String>,
}

/// Resource references frozen at stage time.
///
/// ToolRun and prepared-intent state live in their own stores; the payload
/// carries references (run ID, intent ref, reservation) instead of copies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorResourcesWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_intent_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_reservation: Option<PreparedReservationWire>,
}

/// Frozen staged-monitor launch payload, persisted before publication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorLaunchPayloadWire {
    pub schema_version: u32,
    pub staged_id: String,
    pub monitor_id: String,
    pub member_id: String,
    pub request_fingerprint: String,
    pub command: Vec<String>,
    pub execution_argv: Vec<String>,
    pub label: String,
    pub policy_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Map<String, JsonValue>>,
    pub lane: String,
    pub cwd: String,
    pub starter: StagedMonitorStarterWire,
    pub workspace: StagedMonitorWorkspaceWire,
    pub continuation: StagedMonitorContinuationWire,
    pub resources: StagedMonitorResourcesWire,
}

/// Validate and re-encode one staged-monitor launch payload JSON value.
pub fn validate_staged_monitor_launch_payload(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let payload: StagedMonitorLaunchPayloadWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "payload is not a valid StagedMonitorLaunchPayloadWire: {error}"
            ))
        })?;
    if payload.schema_version != STAGED_MONITOR_LAUNCH_PAYLOAD_SCHEMA_VERSION {
        return Err(StagedHandoffError::UnsupportedSchema(
            payload.schema_version,
        ));
    }
    validate_uuid(&payload.staged_id, "staged_id")?;
    validate_identifier(&payload.monitor_id, "monitor_id")?;
    validate_identifier(&payload.member_id, "member_id")?;
    validate_identifier(&payload.request_fingerprint, "request_fingerprint")?;
    validate_argv(&payload.command, "command")?;
    validate_argv(&payload.execution_argv, "execution_argv")?;
    validate_non_empty(&payload.label, "label", MAX_LABEL_BYTES)?;
    validate_sha256_hex(&payload.policy_digest, "policy_digest")?;
    validate_non_empty(&payload.lane, "lane", MAX_LABEL_BYTES)?;
    validate_non_empty(&payload.cwd, "cwd", MAX_LABEL_BYTES)?;
    validate_starter(&payload.starter)?;
    validate_workspace(&payload.workspace)?;
    validate_continuation(&payload.continuation)?;
    validate_resources(&payload.resources)?;
    serde_json::to_value(&payload).map_err(|error| {
        StagedHandoffError::Invalid(format!(
            "unable to re-encode staged monitor launch payload: {error}"
        ))
    })
}

fn validate_starter(
    starter: &StagedMonitorStarterWire,
) -> Result<(), StagedHandoffError> {
    validate_non_empty(&starter.agent_id, "starter.agent_id", MAX_LABEL_BYTES)?;
    validate_non_empty(&starter.run_id, "starter.run_id", MAX_LABEL_BYTES)?;
    validate_non_empty(
        &starter.artifacts_dir,
        "starter.artifacts_dir",
        MAX_LABEL_BYTES,
    )?;
    if let Some(workspace_id) = starter.workspace_id.as_deref() {
        validate_non_empty(
            workspace_id,
            "starter.workspace_id",
            MAX_LABEL_BYTES,
        )?;
    }
    Ok(())
}

fn validate_workspace(
    workspace: &StagedMonitorWorkspaceWire,
) -> Result<(), StagedHandoffError> {
    if workspace.mode != "inherit" {
        return Err(StagedHandoffError::Invalid(format!(
            "workspace.mode must be \"inherit\" for staged monitors: {:?}",
            workspace.mode
        )));
    }
    Ok(())
}

fn validate_continuation(
    continuation: &StagedMonitorContinuationWire,
) -> Result<(), StagedHandoffError> {
    if continuation.protocol != PROTOCOL_LEGACY
        && continuation.protocol != PROTOCOL_RECORDS_V1
    {
        return Err(StagedHandoffError::Invalid(format!(
            "continuation.protocol must be {PROTOCOL_LEGACY:?} or \
             {PROTOCOL_RECORDS_V1:?}: {:?}",
            continuation.protocol
        )));
    }
    if continuation.parent_node_ids.len() > MAX_PARENTS {
        return Err(StagedHandoffError::Invalid(format!(
            "continuation.parent_node_ids has {} entries; maximum is {MAX_PARENTS}",
            continuation.parent_node_ids.len()
        )));
    }
    for (index, parent) in continuation.parent_node_ids.iter().enumerate() {
        validate_identifier(
            parent,
            &format!("continuation.parent_node_ids[{index}]"),
        )?;
    }
    if let Some(checkpoint) = continuation.checkpoint_ref.as_deref() {
        validate_non_empty(
            checkpoint,
            "continuation.checkpoint_ref",
            MAX_LABEL_BYTES,
        )?;
    }
    Ok(())
}

fn validate_resources(
    resources: &StagedMonitorResourcesWire,
) -> Result<(), StagedHandoffError> {
    if let Some(run_id) = resources.tool_run_id.as_deref() {
        validate_identifier(run_id, "resources.tool_run_id")?;
    }
    if let Some(intent_ref) = resources.prepared_intent_ref.as_deref() {
        validate_non_empty(
            intent_ref,
            "resources.prepared_intent_ref",
            MAX_LABEL_BYTES,
        )?;
    }
    if let Some(reservation) = resources.prepared_reservation.as_ref() {
        validate_uuid(
            &reservation.staged_id,
            "resources.prepared_reservation.staged_id",
        )?;
        validate_identifier(
            &reservation.monitor_id,
            "resources.prepared_reservation.monitor_id",
        )?;
        validate_identifier(
            &reservation.request_fingerprint,
            "resources.prepared_reservation.request_fingerprint",
        )?;
        validate_argv(
            &reservation.command,
            "resources.prepared_reservation.command",
        )?;
    }
    Ok(())
}

fn validate_uuid(value: &str, field: &str) -> Result<(), StagedHandoffError> {
    let bytes = value.as_bytes();
    let hyphen_at = [8_usize, 13, 18, 23];
    let valid = bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if hyphen_at.contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if valid {
        Ok(())
    } else {
        Err(StagedHandoffError::Invalid(format!(
            "{field} is not a UUID: {value:?}"
        )))
    }
}

fn validate_identifier(
    value: &str,
    field: &str,
) -> Result<(), StagedHandoffError> {
    validate_non_empty(value, field, MAX_LABEL_BYTES)?;
    if value.chars().any(char::is_whitespace) {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} must not contain whitespace: {value:?}"
        )));
    }
    Ok(())
}

fn validate_non_empty(
    value: &str,
    field: &str,
    max_bytes: usize,
) -> Result<(), StagedHandoffError> {
    if value.trim().is_empty() {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} is too large: {} bytes exceeds {max_bytes}",
            value.len()
        )));
    }
    Ok(())
}

fn validate_argv(
    argv: &[String],
    field: &str,
) -> Result<(), StagedHandoffError> {
    if argv.is_empty() {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} must contain at least one argv part"
        )));
    }
    if argv.len() > MAX_COMMAND_PARTS {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} has {} parts; maximum is {MAX_COMMAND_PARTS}",
            argv.len()
        )));
    }
    for (index, part) in argv.iter().enumerate() {
        if part.is_empty() {
            return Err(StagedHandoffError::Invalid(format!(
                "{field}[{index}] must not be empty"
            )));
        }
        if part.len() > MAX_COMMAND_PART_BYTES {
            return Err(StagedHandoffError::Invalid(format!(
                "{field}[{index}] is too large: {} bytes exceeds {MAX_COMMAND_PART_BYTES}",
                part.len()
            )));
        }
    }
    Ok(())
}

fn validate_sha256_hex(
    value: &str,
    field: &str,
) -> Result<(), StagedHandoffError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(StagedHandoffError::Invalid(format!(
            "{field} must be 64 hex sha256 characters: {value:?}"
        )));
    }
    Ok(())
}
