//! Schema validation, kind invariants, and staging-admission guards.

use chrono::DateTime;
use serde_json::{Map, Value as JsonValue};
use thiserror::Error;

use super::gate::{
    gate_payload_workspace_mode, validate_gate_staged_payload,
    GateWorkspaceModeWire,
};
use super::wire::{
    StageAdmissionRequestWire, StagedHandoffKindWire, StagedHandoffReceiptWire,
    StagedHandoffWire, SuccessorWorkspaceWire, STAGED_HANDOFF_OUTCOME_ADOPTED,
    STAGED_HANDOFF_OUTCOME_NOT_ADOPTED, STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
};

/// Errors from staged-handoff validation and admission.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StagedHandoffError {
    /// The record names a schema version outside `1..=current`.
    #[error("unsupported staged handoff schema_version {0}")]
    UnsupportedSchema(u32),
    /// Any other invariant failure; the message names the invariant.
    #[error("invalid staged handoff: {0}")]
    Invalid(String),
    /// The caller is not an in-agent process.
    #[error("SASE_AGENT is unset")]
    MissingAgent,
    /// The caller has no artifacts directory.
    #[error("SASE_ARTIFACTS_DIR is unset")]
    MissingArtifactsDir,
    /// Staging is refused inside a finalizer-owned recovery turn.
    #[error("{0}")]
    FinalizerOwned(String),
    /// Staging is refused inside a native helper.
    #[error("{0}")]
    NativeHelper(String),
    /// A staged record, reservation, or legacy marker already occupies the turn.
    #[error("pending handoff already exists: {0}")]
    AlreadyExists(String),
    /// Staging after an accepted, current final submission is refused.
    #[error(
        "stage the handoff before the final declaration, then refresh the declaration through /sase_final"
    )]
    AcceptedSubmission,
}

/// Validate and re-encode one staged-handoff JSON value.
pub fn validate_staged_handoff(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let record: StagedHandoffWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "record is not a valid StagedHandoffWire: {error}"
            ))
        })?;
    reject_schema(record.schema_version)?;
    validate_uuid(&record.staged_id, "staged_id")?;
    validate_rfc3339(&record.created_at, "created_at")?;
    validate_kind_invariants(&record)?;
    validate_obligation_ids(&record.dirty_obligations_at_stage)?;
    encode(&record)
}

/// Validate and re-encode one adoption-receipt JSON value.
pub fn validate_staged_handoff_receipt(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let receipt: StagedHandoffReceiptWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "receipt is not a valid StagedHandoffReceiptWire: {error}"
            ))
        })?;
    validate_uuid(&receipt.staged_id, "staged_id")?;
    validate_rfc3339(&receipt.adopted_at, "adopted_at")?;
    validate_outcome(&receipt)?;
    validate_refs(&receipt.refs)?;
    encode(&receipt)
}

/// Return whether *receipt* authorizes adoption of *staged_id*.
///
/// A receipt for another staged ID, a held `handoff_not_adopted` outcome,
/// or a malformed receipt cannot authorize anything. Malformed input is an
/// error, not a silent miss.
pub fn receipt_authorizes(
    receipt: &JsonValue,
    staged_id: &str,
) -> Result<bool, StagedHandoffError> {
    let normalized = validate_staged_handoff_receipt(receipt)?;
    let parsed: StagedHandoffReceiptWire = serde_json::from_value(normalized)
        .map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "normalized receipt is not a valid StagedHandoffReceiptWire: {error}"
            ))
        })?;
    Ok(parsed.outcome == STAGED_HANDOFF_OUTCOME_ADOPTED
        && parsed.staged_id == staged_id)
}

/// Decide whether a host may stage a handoff given its observations.
pub fn evaluate_stage_admission(
    value: &JsonValue,
) -> Result<(), StagedHandoffError> {
    let request: StageAdmissionRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "request is not a valid StageAdmissionRequestWire: {error}"
            ))
        })?;
    reject_schema(request.schema_version)?;
    if !request.is_agent {
        return Err(StagedHandoffError::MissingAgent);
    }
    if !request.artifacts_dir_present {
        return Err(StagedHandoffError::MissingArtifactsDir);
    }
    let command = command_label(&request.command);
    if request.is_finalizer_owned {
        return Err(StagedHandoffError::FinalizerOwned(format!(
            "{command} is refused inside a host finalizer turn: this turn cannot \
end the agent run, so the handoff could never hand off. Finish the finalizer's \
task in this turn, or report the blocker in your response so the host records \
the failure."
        )));
    }
    if request.is_native_helper {
        return Err(StagedHandoffError::NativeHelper(format!(
            "{command} is refused inside a native helper: only the root SASE \
agent stages handoffs."
        )));
    }
    if !request.occupying.is_empty() {
        return Err(StagedHandoffError::AlreadyExists(
            request.occupying.join(", "),
        ));
    }
    if request.has_accepted_current_submission {
        return Err(StagedHandoffError::AcceptedSubmission);
    }
    Ok(())
}

fn reject_schema(schema_version: u32) -> Result<(), StagedHandoffError> {
    if schema_version == 0
        || schema_version > STAGED_HANDOFF_WIRE_SCHEMA_VERSION
    {
        return Err(StagedHandoffError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn validate_kind_invariants(
    record: &StagedHandoffWire,
) -> Result<(), StagedHandoffError> {
    match record.kind {
        StagedHandoffKindWire::Plan => {
            require_workspace(
                record,
                SuccessorWorkspaceWire::None,
                "plan handoffs use successor_workspace none",
            )?;
            if record.carry_allowed {
                return Err(invalid("plan handoffs cannot set carry_allowed"));
            }
            require_payload_string(&record.payload, "plan_file")?;
            require_payload_string(&record.payload, "original_file")?;
        }
        StagedHandoffKindWire::Questions | StagedHandoffKindWire::Pipe => {
            require_workspace(
                record,
                SuccessorWorkspaceWire::Inherit,
                &format!(
                    "{kind} handoffs use successor_workspace inherit",
                    kind = kind_name(record.kind)
                ),
            )?;
            if !record.carry_allowed {
                return Err(invalid(format!(
                    "{kind} handoffs require carry_allowed",
                    kind = kind_name(record.kind)
                )));
            }
        }
        StagedHandoffKindWire::Monitor => {
            require_workspace(
                record,
                SuccessorWorkspaceWire::Inherit,
                "monitor handoffs use successor_workspace inherit",
            )?;
        }
        StagedHandoffKindWire::Gate => {
            validate_gate_record(record)?;
        }
    }
    Ok(())
}

fn validate_gate_record(
    record: &StagedHandoffWire,
) -> Result<(), StagedHandoffError> {
    let payload = JsonValue::Object(record.payload.clone());
    validate_gate_staged_payload(&payload)?;
    match gate_payload_workspace_mode(&payload)? {
        GateWorkspaceModeWire::Inherit => {
            require_workspace(
                record,
                SuccessorWorkspaceWire::Inherit,
                "gate handoffs with workspace_mode inherit require \
                 successor_workspace inherit",
            )?;
            if !record.carry_allowed {
                return Err(invalid(
                    "gate handoffs with workspace_mode inherit require carry_allowed",
                ));
            }
            Ok(())
        }
        GateWorkspaceModeWire::Release => {
            require_workspace(
                record,
                SuccessorWorkspaceWire::None,
                "gate handoffs with workspace_mode release require \
                 successor_workspace none",
            )?;
            if record.carry_allowed {
                return Err(invalid(
                    "gate handoffs with workspace_mode release cannot set carry_allowed",
                ));
            }
            Ok(())
        }
    }
}

fn require_workspace(
    record: &StagedHandoffWire,
    expected: SuccessorWorkspaceWire,
    message: &str,
) -> Result<(), StagedHandoffError> {
    if record.successor_workspace != expected {
        return Err(invalid(message));
    }
    Ok(())
}

fn require_payload_string(
    payload: &Map<String, JsonValue>,
    field: &str,
) -> Result<(), StagedHandoffError> {
    match payload.get(field) {
        Some(JsonValue::String(value)) if !value.trim().is_empty() => Ok(()),
        Some(_) => Err(invalid(format!(
            "payload.{field} must be a non-empty string"
        ))),
        None => Err(invalid(format!("payload.{field} is required"))),
    }
}

fn validate_obligation_ids(ids: &[String]) -> Result<(), StagedHandoffError> {
    for id in ids {
        if id.trim().is_empty() {
            return Err(invalid(
                "dirty_obligations_at_stage entries must be non-empty",
            ));
        }
    }
    Ok(())
}

fn validate_outcome(
    receipt: &StagedHandoffReceiptWire,
) -> Result<(), StagedHandoffError> {
    match receipt.outcome.as_str() {
        STAGED_HANDOFF_OUTCOME_ADOPTED => {
            if receipt
                .reason
                .as_ref()
                .is_some_and(|value| !value.is_empty())
            {
                return Err(invalid(
                    "adopted receipts cannot carry a held-handoff reason",
                ));
            }
            Ok(())
        }
        STAGED_HANDOFF_OUTCOME_NOT_ADOPTED => {
            match receipt.reason.as_deref().map(str::trim) {
                Some(reason) if !reason.is_empty() => Ok(()),
                _ => Err(invalid(
                    "handoff_not_adopted receipts require a specific reason",
                )),
            }
        }
        other => Err(invalid(format!("unknown receipt outcome {other:?}"))),
    }
}

fn validate_refs(
    refs: &Map<String, JsonValue>,
) -> Result<(), StagedHandoffError> {
    for (key, value) in refs {
        if key.trim().is_empty() {
            return Err(invalid("receipt refs keys must be non-empty"));
        }
        match value {
            JsonValue::String(text) if !text.trim().is_empty() => {}
            JsonValue::Null => {}
            _ => {
                return Err(invalid(format!(
                    "receipt refs.{key} must be a non-empty string"
                )));
            }
        }
    }
    Ok(())
}

fn validate_uuid(value: &str, field: &str) -> Result<(), StagedHandoffError> {
    if is_uuid(value) {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not a UUID: {value:?}")))
    }
}

fn validate_rfc3339(
    value: &str,
    field: &str,
) -> Result<(), StagedHandoffError> {
    if DateTime::parse_from_rfc3339(value).is_ok() {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not RFC3339: {value:?}")))
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    let hyphen_at = [8_usize, 13, 18, 23];
    bytes.iter().enumerate().all(|(index, byte)| {
        if hyphen_at.contains(&index) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}

fn command_label(command: &str) -> &str {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        "turn-ending handoffs"
    } else {
        trimmed
    }
}

fn kind_name(kind: StagedHandoffKindWire) -> &'static str {
    match kind {
        StagedHandoffKindWire::Plan => "plan",
        StagedHandoffKindWire::Questions => "questions",
        StagedHandoffKindWire::Pipe => "pipe",
        StagedHandoffKindWire::Gate => "gate",
        StagedHandoffKindWire::Monitor => "monitor",
    }
}

fn encode<T: serde::Serialize>(
    value: &T,
) -> Result<JsonValue, StagedHandoffError> {
    serde_json::to_value(value).map_err(|error| {
        StagedHandoffError::Invalid(format!(
            "unable to re-encode staged handoff value: {error}"
        ))
    })
}

fn invalid(message: impl Into<String>) -> StagedHandoffError {
    StagedHandoffError::Invalid(message.into())
}
