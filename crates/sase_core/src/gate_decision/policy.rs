//! Deterministic gate-decision-acceptance policy.
//!
//! The host supplies the verified request hash, the normalized selection,
//! and input/feedback identity for one gate, plus whatever receipt already
//! exists on disk for it. This module decides whether the submission is a
//! fresh acceptance, an idempotent replay of the same decision, or a
//! conflicting decision that must be rejected before any slow execution
//! work (archive publication, workspace preparation, successor launch)
//! runs. It performs no filesystem, subprocess, or notification I/O.

use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::wire::{
    GateDecisionAcceptanceOutcomeWire, GateDecisionAcceptanceRequestWire,
    GateDecisionError, GateDecisionOutcomeStatusWire, GateDecisionReceiptWire,
    GateLifecycleDecisionWire, GateLifecycleRequestWire,
    GATE_DECISION_CODE_CONFLICT, GATE_DECISION_CODE_INVALID_REQUEST,
    GATE_DECISION_CODE_UNSUPPORTED_SCHEMA, GATE_DECISION_WIRE_SCHEMA_VERSION,
    GATE_LIFECYCLE_CODE_INVALID_RECEIPT, GATE_LIFECYCLE_CODE_INVALID_REQUEST,
    GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED,
    GATE_LIFECYCLE_DISPOSITION_ANSWERED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW,
    GATE_LIFECYCLE_DISPOSITION_PENDING, GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
};

fn validate_schema(actual: u32) -> Result<(), GateDecisionError> {
    if actual == GATE_DECISION_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(GateDecisionError::new(
            GATE_DECISION_CODE_UNSUPPORTED_SCHEMA,
            format!(
                "unsupported gate-decision schema_version {actual}; expected {GATE_DECISION_WIRE_SCHEMA_VERSION}"
            ),
        ))
    }
}

/// Compute the identity fingerprint every semantically relevant input feeds:
/// the verified request hash, the normalized selection, and both input and
/// feedback identity. Two submissions with the same fingerprint are the same
/// decision; source and timestamp never affect it. Selection order matters
/// here -- callers must normalize (e.g. to branch/query order) before
/// calling, exactly as they must before persisting a receipt.
pub fn gate_decision_identity_fingerprint(
    request_hash: &str,
    selected_option_ids: &[String],
    input_identity: &str,
    feedback_identity: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(request_hash.as_bytes());
    hasher.update([0u8]);
    for option_id in selected_option_ids {
        hasher.update(option_id.as_bytes());
        hasher.update(*b",");
    }
    hasher.update([0u8]);
    hasher.update(input_identity.as_bytes());
    hasher.update([0u8]);
    hasher.update(feedback_identity.unwrap_or("").as_bytes());
    hex::encode(hasher.finalize())
}

/// Decide one gate's decision acceptance.
///
/// Returns an [`GateDecisionAcceptanceOutcomeWire`] with `status: accepted`
/// for a fresh decision, `status: replayed` (carrying the original,
/// unmodified receipt) for an identical resubmission against
/// `request.existing_receipt`, or a [`GateDecisionError`] with code
/// [`GATE_DECISION_CODE_CONFLICT`] when `existing_receipt` names a
/// different decision. On conflict the caller must reject the submission
/// promptly, before running any option command, archive, or launch work.
pub fn decide_gate_decision_acceptance(
    request: &GateDecisionAcceptanceRequestWire,
) -> Result<GateDecisionAcceptanceOutcomeWire, GateDecisionError> {
    validate_schema(request.schema_version)?;
    let fingerprint = gate_decision_identity_fingerprint(
        &request.request_hash,
        &request.selected_option_ids,
        &request.input_identity,
        request.feedback_identity.as_deref(),
    );
    if let Some(existing) = &request.existing_receipt {
        if existing.identity_fingerprint == fingerprint {
            return Ok(GateDecisionAcceptanceOutcomeWire {
                status: GateDecisionOutcomeStatusWire::Replayed,
                receipt: existing.clone(),
            });
        }
        return Err(GateDecisionError::new(
            GATE_DECISION_CODE_CONFLICT,
            format!(
                "gate {} already has an accepted decision with a different selection or input; \
                 requested fingerprint {fingerprint} does not match accepted fingerprint {}",
                request.gate_id, existing.identity_fingerprint
            ),
        ));
    }
    let receipt = GateDecisionReceiptWire {
        schema_version: GATE_DECISION_WIRE_SCHEMA_VERSION,
        gate_id: request.gate_id.clone(),
        request_hash: request.request_hash.clone(),
        selected_option_ids: request.selected_option_ids.clone(),
        input_identity: request.input_identity.clone(),
        feedback_identity: request.feedback_identity.clone(),
        source: request.source.clone(),
        accepted_at_unix: request.accepted_at_unix,
        execution_owner: request.execution_owner.clone(),
        identity_fingerprint: fingerprint,
    };
    Ok(GateDecisionAcceptanceOutcomeWire {
        status: GateDecisionOutcomeStatusWire::Accepted,
        receipt,
    })
}

/// Parse a JSON request and decide, rejecting a malformed payload before
/// policy runs.
pub fn decide_gate_decision_acceptance_from_json(
    value: &JsonValue,
) -> Result<GateDecisionAcceptanceOutcomeWire, GateDecisionError> {
    let request: GateDecisionAcceptanceRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            GateDecisionError::new(
                GATE_DECISION_CODE_INVALID_REQUEST,
                format!("invalid gate-decision-acceptance request: {error}"),
            )
        })?;
    decide_gate_decision_acceptance(&request)
}

fn validate_lifecycle_schema(actual: u32) -> Result<(), GateDecisionError> {
    if actual == GATE_LIFECYCLE_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(GateDecisionError::new(
            GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA,
            format!(
                "unsupported gate-lifecycle schema_version {actual}; expected {GATE_LIFECYCLE_WIRE_SCHEMA_VERSION}"
            ),
        ))
    }
}

fn lifecycle_decision(
    disposition: &str,
    reason: &str,
) -> GateLifecycleDecisionWire {
    GateLifecycleDecisionWire {
        schema_version: GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        disposition: disposition.to_string(),
        reason: reason.to_string(),
    }
}

/// Classify one gate's current lifecycle disposition from host-collected
/// evidence.
///
/// Precedence, highest first: a published response; a decision receipt that
/// fails identity verification (reported explicitly, never silently treated
/// as unanswered); a decision receipt that verifies (the gate is accepted,
/// its execution may still be incomplete, and this outranks both the review
/// deadline and the reclaim grace window); a recorded cancellation; then the
/// review deadline and reclaim grace window. This operation performs no
/// filesystem, clock, or process I/O.
pub fn decide_gate_lifecycle(
    request: &GateLifecycleRequestWire,
) -> Result<GateLifecycleDecisionWire, GateDecisionError> {
    validate_lifecycle_schema(request.schema_version)?;

    if request.has_response {
        return Ok(lifecycle_decision(
            GATE_LIFECYCLE_DISPOSITION_ANSWERED,
            "gate has a published response",
        ));
    }
    if request.receipt_unreadable {
        return Err(GateDecisionError::new(
            GATE_LIFECYCLE_CODE_INVALID_RECEIPT,
            format!(
                "gate {} has a decision receipt that could not be read",
                request.gate_id
            ),
        ));
    }
    if let Some(receipt) = &request.receipt {
        if receipt.gate_id != request.gate_id
            || receipt.request_hash != request.request_hash
        {
            return Err(GateDecisionError::new(
                GATE_LIFECYCLE_CODE_INVALID_RECEIPT,
                format!(
                    "gate {} has a decision receipt naming a different gate or request",
                    request.gate_id
                ),
            ));
        }
        return Ok(lifecycle_decision(
            GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED,
            "gate decision is accepted; execution has not published a response yet",
        ));
    }
    if let Some(reason) = &request.cancellation_reason {
        let disposition = match reason.as_str() {
            "timeout" => GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT,
            "grace_expired" => GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST,
            _ => GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED,
        };
        return Ok(lifecycle_decision(
            disposition,
            "gate has a recorded cancellation",
        ));
    }
    let Some(deadline) = request.deadline_unix else {
        return Ok(lifecycle_decision(
            GATE_LIFECYCLE_DISPOSITION_PENDING,
            "gate has no review deadline",
        ));
    };
    if request.now_unix < deadline {
        return Ok(lifecycle_decision(
            GATE_LIFECYCLE_DISPOSITION_PENDING,
            "gate review deadline has not passed",
        ));
    }
    if request.now_unix < deadline + request.grace_seconds {
        return Ok(lifecycle_decision(
            GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW,
            "gate review deadline passed; still inside the reclaim grace window",
        ));
    }
    Ok(lifecycle_decision(
        GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE,
        "gate review deadline and reclaim grace window both passed",
    ))
}

/// Parse a JSON request and classify, rejecting a malformed payload before
/// policy runs.
pub fn decide_gate_lifecycle_from_json(
    value: &JsonValue,
) -> Result<GateLifecycleDecisionWire, GateDecisionError> {
    let request: GateLifecycleRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            GateDecisionError::new(
                GATE_LIFECYCLE_CODE_INVALID_REQUEST,
                format!("invalid gate-lifecycle request: {error}"),
            )
        })?;
    decide_gate_lifecycle(&request)
}
