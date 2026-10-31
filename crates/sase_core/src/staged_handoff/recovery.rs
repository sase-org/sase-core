//! Durable recovery identity and replay decisions for staged handoffs.
//!
//! The staged record (`StagedHandoffWire`) proves *what* was staged. This
//! module owns the companion facts a dead runner's successor needs to adopt
//! without the original `SASE_AGENT*` environment, working directory, model
//! defaults, or live `LoopState`: stable origin identity, workspace/project
//! ownership, prompt/continuation references, the selected model/provider,
//! and the already-observed destination identity.
//!
//! Filesystem probing and spawning stay outside these policy functions.
//! Python gathers facts and performs host I/O; Rust validates shape and
//! decides whether the observed evidence permits adoption, delivery-only
//! replay, a retryable hold, or a terminal refusal.
//!
//! Lock ordering: the shared adoption lock is the existing handoff flock
//! (`.sase_handoff.lock`). Acquire it before any gate-lane, monitor-lane,
//! or claim lock, and never while already holding the receipt writer's
//! lock (the writer takes the adoption lock itself; callers that already
//! hold it must use the `_locked` file helpers). Re-read the receipt after
//! acquiring the lock: a matching `adopted` receipt wins, and a malformed
//! or mismatched receipt is a refusal, never missing state.

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::validate::StagedHandoffError;
use super::wire::{
    StagedHandoffKindWire, STAGED_HANDOFF_OUTCOME_ADOPTED,
    STAGED_HANDOFF_OUTCOME_NOT_ADOPTED,
};

/// Wire schema version for the recovery-context companion record.
pub const RECOVERY_CONTEXT_WIRE_SCHEMA_VERSION: u32 = 1;

/// Filename of the shared adoption lock (the existing handoff flock).
pub const RECOVERY_ADOPTION_LOCK_FILENAME: &str = ".sase_handoff.lock";

/// Return the recovery-context wire schema version.
pub fn recovery_context_wire_schema_version() -> u32 {
    RECOVERY_CONTEXT_WIRE_SCHEMA_VERSION
}

/// Return the shared adoption-lock filename.
pub fn recovery_adoption_lock_filename() -> &'static str {
    RECOVERY_ADOPTION_LOCK_FILENAME
}

/// Durable recovery context bound to one staged handoff.
///
/// This companion preserves v1 staged records and receipts: it is stored
/// beside them (`.sase_handoff_recovery.json`) and never rewrites them.
/// Only facts not already recoverable from canonical artifacts are carried
/// here. Callers must not copy unrelated environment or credential values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryContextWire {
    pub schema_version: u32,
    pub staged_id: String,
    pub kind: StagedHandoffKindWire,
    pub origin_agent: String,
    pub origin_artifacts_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_ref: Option<String>,
    pub created_at: String,
}

/// Observed receipt state for one recovery decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryReceiptStateWire {
    Absent,
    Adopted,
    Held,
    Mismatched,
    Malformed,
}

/// Observed runner liveness. Unknown liveness is not death.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryRunnerStateWire {
    Alive,
    Dead,
    Unknown,
}

/// Recovery caller mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryModeWire {
    Automatic,
    Explicit,
}

/// Finalizer settlement proof observed by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverySettlementWire {
    Settled,
    Held,
    Missing,
}

/// Already-observed destination state. Completed effects are reused,
// never relaunched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDestinationWire {
    Absent,
    Pending,
    Owned,
    Terminal,
}

/// Host observations for one adoption-replay decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDecisionRequestWire {
    pub schema_version: u32,
    pub staged_id: String,
    pub receipt: RecoveryReceiptStateWire,
    pub runner: RecoveryRunnerStateWire,
    pub killed_or_discarded: bool,
    pub mode: RecoveryModeWire,
    pub settlement: RecoverySettlementWire,
    pub destination: RecoveryDestinationWire,
}

/// Replay action for the observed evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryActionWire {
    Adopt,
    DeliverOnly,
    Hold,
    Refuse,
}

/// Decision for the observed recovery evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDecisionWire {
    pub schema_version: u32,
    pub action: RecoveryActionWire,
    pub reason: String,
    pub already_adopted: bool,
}

/// Validate and re-encode one recovery-context value.
pub fn validate_recovery_context(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let context: RecoveryContextWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "recovery context is not a valid RecoveryContextWire: {error}"
            ))
        })?;
    reject_context_schema(context.schema_version)?;
    validate_uuid(&context.staged_id, "staged_id")?;
    validate_rfc3339(&context.created_at, "created_at")?;
    if context.origin_agent.trim().is_empty() {
        return Err(invalid("recovery context origin_agent is required"));
    }
    if context.origin_artifacts_dir.trim().is_empty() {
        return Err(invalid(
            "recovery context origin_artifacts_dir is required",
        ));
    }
    for (field, value) in [
        ("workspace_dir", context.workspace_dir.as_deref()),
        ("project", context.project.as_deref()),
        ("model", context.model.as_deref()),
        ("provider", context.provider.as_deref()),
        ("prompt_ref", context.prompt_ref.as_deref()),
    ] {
        if let Some(text) = value {
            if text.trim().is_empty() {
                return Err(invalid(format!(
                    "recovery context {field} must be a non-empty string when present"
                )));
            }
        }
    }
    encode(&context)
}

/// Decide the replay action for the observed recovery evidence.
///
/// Pure policy: no filesystem access, no spawning. Old artifacts with
/// insufficient evidence get a precise hold/refusal; the caller must not
/// manufacture creator or destination identity from its own environment.
pub fn evaluate_recovery_decision(
    value: &JsonValue,
) -> Result<JsonValue, StagedHandoffError> {
    let request: RecoveryDecisionRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            StagedHandoffError::Invalid(format!(
                "request is not a valid RecoveryDecisionRequestWire: {error}"
            ))
        })?;
    reject_context_schema(request.schema_version)?;
    validate_uuid(&request.staged_id, "staged_id")?;
    let decision = decide(&request);
    encode(&decision)
}

/// Return whether *next* may replace *current* for *staged_id*.
///
/// *current* may be `Null` for an absent receipt. A matching `adopted`
/// receipt wins and is idempotent; a racing hold must never overwrite it.
/// A mismatched or malformed receipt is a refusal, not missing state.
pub fn receipt_transition_allowed(
    current: &JsonValue,
    next: &JsonValue,
    staged_id: &str,
) -> Result<bool, StagedHandoffError> {
    validate_uuid(staged_id, "staged_id")?;
    let next_outcome = receipt_outcome(next, "next")?;
    let next_id = receipt_staged_id(next, "next")?;
    if next_id != staged_id {
        return Err(invalid(
            "next receipt staged_id does not match the active stage",
        ));
    }
    if current.is_null() {
        return Ok(next_outcome == STAGED_HANDOFF_OUTCOME_ADOPTED
            || next_outcome == STAGED_HANDOFF_OUTCOME_NOT_ADOPTED);
    }
    let current_outcome = receipt_outcome(current, "current")?;
    let current_id = receipt_staged_id(current, "current")?;
    if current_id != staged_id {
        return Err(invalid(
            "current receipt staged_id does not match the active stage",
        ));
    }
    match (current_outcome, next_outcome) {
        (current, next)
            if current == STAGED_HANDOFF_OUTCOME_ADOPTED
                && next == STAGED_HANDOFF_OUTCOME_ADOPTED =>
        {
            Ok(true)
        }
        (current, _)
            if current == STAGED_HANDOFF_OUTCOME_ADOPTED =>
        {
            Ok(false)
        }
        (current, next)
            if current == STAGED_HANDOFF_OUTCOME_NOT_ADOPTED
                && (next == STAGED_HANDOFF_OUTCOME_ADOPTED
                    || next == STAGED_HANDOFF_OUTCOME_NOT_ADOPTED) =>
        {
            Ok(true)
        }
        _ => Err(invalid("unknown receipt outcome transition")),
    }
}

fn decide(request: &RecoveryDecisionRequestWire) -> RecoveryDecisionWire {
    let decided = |action: RecoveryActionWire,
                   reason: &str,
                   already_adopted: bool| {
        RecoveryDecisionWire {
            schema_version: RECOVERY_CONTEXT_WIRE_SCHEMA_VERSION,
            action,
            reason: reason.to_string(),
            already_adopted,
        }
    };
    match request.receipt {
        RecoveryReceiptStateWire::Malformed
        | RecoveryReceiptStateWire::Mismatched => {
            return decided(
                RecoveryActionWire::Refuse,
                "existing receipt is unusable and cannot authorize adoption",
                false,
            );
        }
        RecoveryReceiptStateWire::Adopted => {
            return decided(
                RecoveryActionWire::Adopt,
                "matching success receipt already owns this adoption",
                true,
            );
        }
        RecoveryReceiptStateWire::Absent | RecoveryReceiptStateWire::Held => {}
    }
    if request.killed_or_discarded {
        return decided(
            RecoveryActionWire::Refuse,
            "explicit kill or discard forbids later activation",
            false,
        );
    }
    if request.settlement != RecoverySettlementWire::Settled {
        return decided(
            RecoveryActionWire::Hold,
            "finalizer settlement proof is missing for this stage",
            false,
        );
    }
    if request.mode == RecoveryModeWire::Automatic
        && request.runner != RecoveryRunnerStateWire::Dead
    {
        return decided(
            RecoveryActionWire::Hold,
            "automatic adoption requires a proven-dead originating runner",
            false,
        );
    }
    if request.runner == RecoveryRunnerStateWire::Unknown {
        return decided(
            RecoveryActionWire::Hold,
            "unknown runner liveness is not death",
            false,
        );
    }
    match request.destination {
        RecoveryDestinationWire::Pending => {
            return decided(
                RecoveryActionWire::DeliverOnly,
                "successor was recorded but delivery is still pending",
                false,
            );
        }
        RecoveryDestinationWire::Owned | RecoveryDestinationWire::Terminal => {
            return decided(
                RecoveryActionWire::Adopt,
                "destination already exists and must be reused, not relaunched",
                false,
            );
        }
        RecoveryDestinationWire::Absent => {}
    }
    decided(
        RecoveryActionWire::Adopt,
        "settlement is proven and no terminal receipt blocks adoption",
        false,
    )
}

fn receipt_outcome<'a>(
    receipt: &'a JsonValue,
    which: &str,
) -> Result<&'a str, StagedHandoffError> {
    receipt
        .as_object()
        .and_then(|object| object.get("outcome"))
        .and_then(JsonValue::as_str)
        .filter(|outcome| {
            *outcome == STAGED_HANDOFF_OUTCOME_ADOPTED
                || *outcome == STAGED_HANDOFF_OUTCOME_NOT_ADOPTED
        })
        .ok_or_else(|| {
            invalid(format!("{which} receipt has an unknown outcome"))
        })
}

fn receipt_staged_id<'a>(
    receipt: &'a JsonValue,
    which: &str,
) -> Result<&'a str, StagedHandoffError> {
    receipt
        .as_object()
        .and_then(|object| object.get("staged_id"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            invalid(format!("{which} receipt has no staged_id"))
        })
}

fn reject_context_schema(schema_version: u32) -> Result<(), StagedHandoffError> {
    if schema_version == 0
        || schema_version > RECOVERY_CONTEXT_WIRE_SCHEMA_VERSION
    {
        return Err(StagedHandoffError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn validate_uuid(value: &str, field: &str) -> Result<(), StagedHandoffError> {
    let bytes = value.as_bytes();
    let hyphen_at = [8_usize, 13, 18, 23];
    let is_uuid = bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if hyphen_at.contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if is_uuid {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not a UUID: {value:?}")))
    }
}

fn validate_rfc3339(value: &str, field: &str) -> Result<(), StagedHandoffError> {
    if DateTime::parse_from_rfc3339(value).is_ok() {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not RFC3339: {value:?}")))
    }
}

fn encode<T: serde::Serialize>(
    value: &T,
) -> Result<JsonValue, StagedHandoffError> {
    serde_json::to_value(value).map_err(|error| {
        StagedHandoffError::Invalid(format!(
            "unable to re-encode recovery value: {error}"
        ))
    })
}

fn invalid(message: impl Into<String>) -> StagedHandoffError {
    StagedHandoffError::Invalid(message.into())
}
