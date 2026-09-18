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
    GateDecisionError, GateDecisionExecutionClaimOutcomeWire,
    GateDecisionExecutionClaimRequestWire, GateDecisionExecutionFactsWire,
    GateDecisionExecutionOwnerKindWire, GateDecisionExecutionOwnerWire,
    GateDecisionFailureOutcomeWire, GateDecisionFailureStageWire,
    GateDecisionOutcomeStatusWire, GateDecisionOwnerLossWire,
    GateDecisionOwnerSummaryWire, GateDecisionReceiptWire,
    GateLifecycleDecisionWire, GateLifecycleRequestWire,
    GATE_DECISION_CODE_CONFLICT, GATE_DECISION_CODE_INVALID_REQUEST,
    GATE_DECISION_CODE_UNSUPPORTED_SCHEMA, GATE_DECISION_WIRE_SCHEMA_VERSION,
    GATE_LIFECYCLE_CODE_INVALID_RECEIPT, GATE_LIFECYCLE_CODE_INVALID_REQUEST,
    GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_FAILED,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_OWNER_LOST,
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

fn mint_receipt(
    request: &GateDecisionAcceptanceRequestWire,
    fingerprint: String,
) -> GateDecisionReceiptWire {
    GateDecisionReceiptWire {
        schema_version: GATE_DECISION_WIRE_SCHEMA_VERSION,
        gate_id: request.gate_id.clone(),
        request_hash: request.request_hash.clone(),
        selected_option_ids: request.selected_option_ids.clone(),
        input_identity: request.input_identity.clone(),
        feedback_identity: request.feedback_identity.clone(),
        acceptance_id: request.acceptance_id.clone(),
        source: request.source.clone(),
        accepted_at_unix: request.accepted_at_unix,
        execution_owner: request.execution_owner.clone(),
        identity_fingerprint: fingerprint,
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.trim().is_empty())
}

fn require_acceptance_id<'a>(
    value: Option<&'a String>,
    context: &str,
) -> Result<&'a str, GateDecisionError> {
    nonempty(value.map(String::as_str)).ok_or_else(|| {
        GateDecisionError::new(
            GATE_DECISION_CODE_INVALID_REQUEST,
            format!("{context} requires a nonempty acceptance_id"),
        )
    })
}

fn validate_receipt_acceptance_id(
    receipt: &GateDecisionReceiptWire,
    context: &str,
) -> Result<(), GateDecisionError> {
    require_acceptance_id(receipt.acceptance_id.as_ref(), context).map(|_| ())
}

fn validate_failure_outcome(
    failure: &GateDecisionFailureOutcomeWire,
    field_name: &str,
) -> Result<(), GateDecisionError> {
    for (name, value) in [
        ("outcome_id", failure.outcome_id.as_str()),
        (
            "acceptance_id",
            failure.acceptance_id.as_deref().unwrap_or(""),
        ),
        ("attempt_id", failure.attempt_id.as_str()),
        ("code", failure.code.as_str()),
        ("message", failure.message.as_str()),
        ("error_record", failure.error_record.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(GateDecisionError::new(
                GATE_DECISION_CODE_INVALID_REQUEST,
                format!("{field_name}.{name} must be nonempty"),
            ));
        }
    }
    if !failure.at_unix.is_finite() {
        return Err(GateDecisionError::new(
            GATE_DECISION_CODE_INVALID_REQUEST,
            format!("{field_name}.at_unix must be finite"),
        ));
    }
    Ok(())
}

fn validate_execution_facts(
    facts: Option<&GateDecisionExecutionFactsWire>,
) -> Result<(), GateDecisionError> {
    let Some(facts) = facts else {
        return Ok(());
    };
    if let Some(failure) = &facts.current_failure {
        validate_failure_outcome(failure, "execution_facts.current_failure")?;
        if matches!(
            failure.stage,
            GateDecisionFailureStageWire::SideEffects
                | GateDecisionFailureStageWire::FollowUp
        ) {
            return Err(GateDecisionError::new(
                GATE_DECISION_CODE_INVALID_REQUEST,
                "execution_facts.current_failure must be a pre-response failure",
            ));
        }
    }
    if let Some(failure) = &facts.post_response_failure {
        validate_failure_outcome(
            failure,
            "execution_facts.post_response_failure",
        )?;
        if !matches!(
            failure.stage,
            GateDecisionFailureStageWire::SideEffects
                | GateDecisionFailureStageWire::FollowUp
        ) {
            return Err(GateDecisionError::new(
                GATE_DECISION_CODE_INVALID_REQUEST,
                "execution_facts.post_response_failure must be a post-response failure",
            ));
        }
    }
    Ok(())
}

fn failure_matches_receipt(
    receipt: &GateDecisionReceiptWire,
    failure: &GateDecisionFailureOutcomeWire,
) -> bool {
    receipt.acceptance_id.as_ref() == failure.acceptance_id.as_ref()
}

fn current_failure_for_receipt<'a>(
    receipt: &GateDecisionReceiptWire,
    facts: Option<&'a GateDecisionExecutionFactsWire>,
) -> Option<&'a GateDecisionFailureOutcomeWire> {
    let failure = facts.and_then(|facts| facts.current_failure.as_ref())?;
    failure_matches_receipt(receipt, failure).then_some(failure)
}

fn post_response_failure_for_receipt<'a>(
    receipt: &GateDecisionReceiptWire,
    facts: Option<&'a GateDecisionExecutionFactsWire>,
) -> Option<&'a GateDecisionFailureOutcomeWire> {
    let failure =
        facts.and_then(|facts| facts.post_response_failure.as_ref())?;
    failure_matches_receipt(receipt, failure).then_some(failure)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionOwnerLiveness {
    Live,
    Dead,
    Unknown,
}

impl ExecutionOwnerLiveness {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Dead => "dead",
            Self::Unknown => "unknown",
        }
    }
}

fn owner_summary(
    owner: Option<&GateDecisionExecutionOwnerWire>,
) -> Option<GateDecisionOwnerSummaryWire> {
    match owner? {
        GateDecisionExecutionOwnerWire::LegacyProcId(proc_id) => {
            Some(GateDecisionOwnerSummaryWire {
                kind: GateDecisionExecutionOwnerKindWire::Proc,
                proc_id: Some(proc_id.clone()),
                host: None,
                pid: None,
            })
        }
        GateDecisionExecutionOwnerWire::Structured(owner) => {
            Some(GateDecisionOwnerSummaryWire {
                kind: owner.kind,
                proc_id: owner.proc_id.clone(),
                host: owner.host.clone(),
                pid: owner.pid,
            })
        }
    }
}

fn owner_loss(
    receipt: &GateDecisionReceiptWire,
    liveness: ExecutionOwnerLiveness,
) -> Option<GateDecisionOwnerLossWire> {
    if liveness != ExecutionOwnerLiveness::Dead {
        return None;
    }
    let acceptance_id = nonempty(receipt.acceptance_id.as_deref())?;
    Some(GateDecisionOwnerLossWire {
        acceptance_id: acceptance_id.to_string(),
        owner: owner_summary(receipt.execution_owner.as_ref()),
        owner_liveness: liveness.as_str().to_string(),
        reason: "gate decision execution owner is no longer live".to_string(),
    })
}

fn conflict_message(
    request: &GateDecisionAcceptanceRequestWire,
    fingerprint: &str,
    existing: &GateDecisionReceiptWire,
    liveness: ExecutionOwnerLiveness,
) -> String {
    let mut message = format!(
        "gate {} already has an accepted decision with a different selection or input; \
         requested fingerprint {fingerprint} does not match accepted fingerprint {}; \
         existing owner liveness is {}",
        request.gate_id,
        existing.identity_fingerprint,
        liveness.as_str(),
    );
    if let Some(owner) = owner_summary(existing.execution_owner.as_ref()) {
        if let Some(proc_id) = owner.proc_id {
            message.push_str(&format!(", owner proc_id {proc_id}"));
        }
        if let Some(host) = owner.host {
            message.push_str(&format!(", owner host {host}"));
        }
        if let Some(pid) = owner.pid {
            message.push_str(&format!(", owner pid {pid}"));
        }
    }
    message
}

fn execution_owner_liveness(
    receipt: &GateDecisionReceiptWire,
    facts: Option<&GateDecisionExecutionFactsWire>,
) -> ExecutionOwnerLiveness {
    let Some(facts) = facts else {
        return ExecutionOwnerLiveness::Unknown;
    };
    if facts.response_lock_held {
        return ExecutionOwnerLiveness::Live;
    }
    let Some(owner) = receipt.execution_owner.as_ref() else {
        return ExecutionOwnerLiveness::Dead;
    };
    match owner {
        GateDecisionExecutionOwnerWire::LegacyProcId(_) => {
            legacy_proc_owner_liveness(facts)
        }
        GateDecisionExecutionOwnerWire::Structured(owner) => match owner.kind {
            GateDecisionExecutionOwnerKindWire::Proc => {
                legacy_proc_owner_liveness(facts)
            }
            GateDecisionExecutionOwnerKindWire::Process => {
                process_owner_liveness(facts)
            }
        },
    }
}

fn process_owner_liveness(
    facts: &GateDecisionExecutionFactsWire,
) -> ExecutionOwnerLiveness {
    if facts.owner_host_matches == Some(false) {
        return ExecutionOwnerLiveness::Unknown;
    }
    if facts.owner_from_previous_boot == Some(true)
        || facts.owner_pid_running == Some(false)
        || facts.owner_identity_matches == Some(false)
    {
        return ExecutionOwnerLiveness::Dead;
    }
    ExecutionOwnerLiveness::Live
}

fn legacy_proc_owner_liveness(
    facts: &GateDecisionExecutionFactsWire,
) -> ExecutionOwnerLiveness {
    match facts.legacy_proc_status.as_deref() {
        Some("missing") | Some("success") | Some("error") | Some("killed") => {
            ExecutionOwnerLiveness::Dead
        }
        Some("pending") | Some("running") | Some("settling") => {
            match facts.legacy_proc_supervisor_alive {
                Some(true) => ExecutionOwnerLiveness::Live,
                Some(false) => ExecutionOwnerLiveness::Dead,
                None => ExecutionOwnerLiveness::Unknown,
            }
        }
        Some(_) | None => ExecutionOwnerLiveness::Unknown,
    }
}

/// Decide one gate's decision acceptance.
///
/// Returns an [`GateDecisionAcceptanceOutcomeWire`] with `status: accepted`
/// for a fresh decision, `status: replayed` (carrying the original,
/// unmodified receipt) for an identical resubmission against
/// `request.existing_receipt`, `status: superseded` when a conflicting
/// decision is permitted because the previous accepted execution durably
/// failed or its owner is proven dead, or a [`GateDecisionError`] with code
/// [`GATE_DECISION_CODE_CONFLICT`] while the existing owner is live or
/// unknown. On conflict the caller must reject the submission promptly,
/// before running any option command, archive, or launch work.
pub fn decide_gate_decision_acceptance(
    request: &GateDecisionAcceptanceRequestWire,
) -> Result<GateDecisionAcceptanceOutcomeWire, GateDecisionError> {
    validate_schema(request.schema_version)?;
    require_acceptance_id(
        request.acceptance_id.as_ref(),
        "gate-decision acceptance request",
    )?;
    validate_execution_facts(request.execution_facts.as_ref())?;
    let fingerprint = gate_decision_identity_fingerprint(
        &request.request_hash,
        &request.selected_option_ids,
        &request.input_identity,
        request.feedback_identity.as_deref(),
    );
    if let Some(existing) = &request.existing_receipt {
        validate_receipt_acceptance_id(
            existing,
            "existing gate-decision receipt",
        )?;
        if existing.identity_fingerprint == fingerprint {
            return Ok(GateDecisionAcceptanceOutcomeWire {
                status: GateDecisionOutcomeStatusWire::Replayed,
                receipt: existing.clone(),
                superseded_receipt: None,
                owner_lost: false,
                owner_loss: None,
                owner_liveness: None,
                failure: None,
            });
        }
        let facts = request.execution_facts.as_ref();
        let current_failure = current_failure_for_receipt(existing, facts);
        let liveness = execution_owner_liveness(existing, facts);
        if current_failure.is_some() || liveness == ExecutionOwnerLiveness::Dead
        {
            let owner_loss = owner_loss(existing, liveness);
            return Ok(GateDecisionAcceptanceOutcomeWire {
                status: GateDecisionOutcomeStatusWire::Superseded,
                receipt: mint_receipt(request, fingerprint),
                superseded_receipt: Some(existing.clone()),
                owner_lost: owner_loss.is_some(),
                owner_loss,
                owner_liveness: Some(liveness.as_str().to_string()),
                failure: current_failure.cloned(),
            });
        }
        return Err(GateDecisionError::new(
            GATE_DECISION_CODE_CONFLICT,
            conflict_message(request, &fingerprint, existing, liveness),
        )
        .with_owner_diagnostics(
            liveness.as_str(),
            owner_summary(existing.execution_owner.as_ref()),
        ));
    }
    Ok(GateDecisionAcceptanceOutcomeWire {
        status: GateDecisionOutcomeStatusWire::Accepted,
        receipt: mint_receipt(request, fingerprint),
        superseded_receipt: None,
        owner_lost: false,
        owner_loss: None,
        owner_liveness: None,
        failure: None,
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

/// Re-own the receipt that is still current immediately before execution.
///
/// The host calls this while holding the gate's response lock and acceptance
/// lock. If another decision replaced the receipt while the caller was
/// waiting, the acceptance id (when supplied), gate id, or request hash no
/// longer matches and the function returns `gate_decision_conflict`.
pub fn claim_gate_decision_execution(
    request: &GateDecisionExecutionClaimRequestWire,
) -> Result<GateDecisionExecutionClaimOutcomeWire, GateDecisionError> {
    validate_schema(request.schema_version)?;
    require_acceptance_id(
        request.acceptance_id.as_ref(),
        "gate-decision execution claim",
    )?;
    let receipt = &request.receipt;
    validate_receipt_acceptance_id(receipt, "claimed gate-decision receipt")?;
    if receipt.gate_id != request.gate_id
        || receipt.request_hash != request.request_hash
        || request.acceptance_id.as_ref().is_some_and(|expected| {
            receipt.acceptance_id.as_ref() != Some(expected)
        })
    {
        return Err(GateDecisionError::new(
            GATE_DECISION_CODE_CONFLICT,
            format!(
                "gate {} decision was superseded while waiting for execution",
                request.gate_id
            ),
        ));
    }
    let mut claimed = receipt.clone();
    claimed.execution_owner = request.execution_owner.clone();
    Ok(GateDecisionExecutionClaimOutcomeWire { receipt: claimed })
}

pub fn claim_gate_decision_execution_from_json(
    value: &JsonValue,
) -> Result<GateDecisionExecutionClaimOutcomeWire, GateDecisionError> {
    let request: GateDecisionExecutionClaimRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            GateDecisionError::new(
                GATE_DECISION_CODE_INVALID_REQUEST,
                format!(
                    "invalid gate-decision-execution-claim request: {error}"
                ),
            )
        })?;
    claim_gate_decision_execution(&request)
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
    lifecycle_decision_with_evidence(
        disposition,
        reason,
        false,
        false,
        None,
        None,
        None,
    )
}

fn lifecycle_decision_with_permissions(
    disposition: &str,
    reason: &str,
    can_cancel: bool,
    can_supersede: bool,
) -> GateLifecycleDecisionWire {
    lifecycle_decision_with_evidence(
        disposition,
        reason,
        can_cancel,
        can_supersede,
        None,
        None,
        None,
    )
}

fn lifecycle_decision_with_evidence(
    disposition: &str,
    reason: &str,
    cancel_permitted: bool,
    supersede_permitted: bool,
    owner_liveness: Option<String>,
    owner_loss: Option<GateDecisionOwnerLossWire>,
    failure: Option<GateDecisionFailureOutcomeWire>,
) -> GateLifecycleDecisionWire {
    GateLifecycleDecisionWire {
        schema_version: GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        disposition: disposition.to_string(),
        reason: reason.to_string(),
        can_cancel: cancel_permitted,
        can_supersede: supersede_permitted,
        cancel_permitted,
        supersede_permitted,
        owner_liveness,
        owner_loss,
        failure,
    }
}

/// Classify one gate's current lifecycle disposition from host-collected
/// evidence.
///
/// Precedence, highest first: a published response; a decision receipt that
/// fails identity verification (reported explicitly, never silently treated
/// as unanswered); a recorded cancellation; a verified receipt classified as
/// failed, owner-lost, or unfinished; then the review deadline and reclaim
/// grace window. This operation performs no filesystem, clock, or process I/O.
pub fn decide_gate_lifecycle(
    request: &GateLifecycleRequestWire,
) -> Result<GateLifecycleDecisionWire, GateDecisionError> {
    validate_lifecycle_schema(request.schema_version)?;
    validate_execution_facts(request.execution_facts.as_ref())?;

    if request.has_response {
        let failure = request
            .receipt
            .as_ref()
            .and_then(|receipt| {
                if receipt.gate_id == request.gate_id
                    && receipt.request_hash == request.request_hash
                    && nonempty(receipt.acceptance_id.as_deref()).is_some()
                {
                    post_response_failure_for_receipt(
                        receipt,
                        request.execution_facts.as_ref(),
                    )
                } else {
                    None
                }
            })
            .cloned();
        return Ok(lifecycle_decision_with_evidence(
            GATE_LIFECYCLE_DISPOSITION_ANSWERED,
            "gate has a published response",
            false,
            false,
            None,
            None,
            failure,
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
    let verified_receipt = if let Some(receipt) = &request.receipt {
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
        validate_receipt_acceptance_id(receipt, "gate lifecycle receipt")?;
        Some(receipt)
    } else {
        None
    };
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
    if let Some(receipt) = verified_receipt {
        let facts = request.execution_facts.as_ref();
        if let Some(failure) = current_failure_for_receipt(receipt, facts) {
            let liveness = execution_owner_liveness(receipt, facts);
            return Ok(lifecycle_decision_with_evidence(
                GATE_LIFECYCLE_DISPOSITION_ACCEPTED_FAILED,
                "gate decision execution failed after acceptance",
                true,
                true,
                Some(liveness.as_str().to_string()),
                owner_loss(receipt, liveness),
                Some(failure.clone()),
            ));
        }
        let liveness = execution_owner_liveness(receipt, facts);
        if liveness == ExecutionOwnerLiveness::Dead {
            let owner_loss = owner_loss(receipt, liveness);
            return Ok(lifecycle_decision_with_evidence(
                GATE_LIFECYCLE_DISPOSITION_ACCEPTED_OWNER_LOST,
                "gate decision execution owner is no longer live",
                true,
                true,
                Some(liveness.as_str().to_string()),
                owner_loss,
                None,
            ));
        }
        return Ok(lifecycle_decision_with_evidence(
            GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED,
            "gate decision is accepted; execution has not published a response yet",
            false,
            false,
            Some(liveness.as_str().to_string()),
            None,
            None,
        ));
    }
    let Some(deadline) = request.deadline_unix else {
        return Ok(lifecycle_decision_with_permissions(
            GATE_LIFECYCLE_DISPOSITION_PENDING,
            "gate has no review deadline",
            true,
            false,
        ));
    };
    if request.now_unix < deadline {
        return Ok(lifecycle_decision_with_permissions(
            GATE_LIFECYCLE_DISPOSITION_PENDING,
            "gate review deadline has not passed",
            true,
            false,
        ));
    }
    if request.now_unix < deadline + request.grace_seconds {
        return Ok(lifecycle_decision_with_permissions(
            GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW,
            "gate review deadline passed; still inside the reclaim grace window",
            true,
            false,
        ));
    }
    Ok(lifecycle_decision_with_permissions(
        GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE,
        "gate review deadline and reclaim grace window both passed",
        true,
        false,
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
