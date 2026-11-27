//! Shared liveness and settlement policy for staged handoffs whose origin
//! turn may have failed before execution began.

use serde::{Deserialize, Serialize};

use super::validate::{is_uuid, StagedHandoffError};
use super::wire::STAGED_HANDOFF_WIRE_SCHEMA_VERSION;

/// Whether a durable stage record was observed and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveStageEvidenceValidityWire {
    Absent,
    Valid,
    Malformed,
}

/// Receipt outcomes relevant to pending-stage ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveStageReceiptOutcomeWire {
    Adopted,
    Held,
}

/// Observed completion state of the originating turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveStageOriginOutcomeWire {
    Unknown,
    Running,
    Succeeded,
    Failed,
}

/// Whether execution evidence is absent, present, or could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveStageExecutionStateWire {
    Unknown,
    Absent,
    Present,
}

/// Whether explicit recovery is inactive, active, or could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveStageRecoveryStateWire {
    Unknown,
    Inactive,
    Active,
}

/// Typed host observations for one stage's inactive-owner decision.
///
/// Hosts gather these facts from persisted records. This function decides
/// whether those records still represent pending ownership and whether it is
/// safe for the host to settle resources belonging to a never-started stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InactiveStageDecisionRequestWire {
    pub schema_version: u32,
    pub stage_validity: InactiveStageEvidenceValidityWire,
    #[serde(default)]
    pub stage_id: Option<String>,
    pub receipt_validity: InactiveStageEvidenceValidityWire,
    #[serde(default)]
    pub receipt_id: Option<String>,
    #[serde(default)]
    pub receipt_outcome: Option<InactiveStageReceiptOutcomeWire>,
    #[serde(default)]
    pub origin_id: Option<String>,
    pub origin_identity_valid: bool,
    pub origin_outcome: InactiveStageOriginOutcomeWire,
    pub origin_finalizing: bool,
    pub execution_state: InactiveStageExecutionStateWire,
    pub recovery_state: InactiveStageRecoveryStateWire,
    pub successor_delivered: bool,
}

/// Result of the shared inactive-stage decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InactiveStageDecisionWire {
    pub schema_version: u32,
    pub inactive: bool,
    pub pending_owner: bool,
    pub settlement_eligible: bool,
    pub reason: String,
}

/// Decide whether a staged handoff remains a pending owner or can be settled.
///
/// A failed origin alone never authorizes cleanup. The origin identity and
/// stage evidence must be valid, execution and recovery must be positively
/// absent, and neither an adopted receipt nor delivered successor may exist.
/// Unknown observations and malformed or mismatched evidence preserve the
/// pending owner conservatively.
pub fn evaluate_inactive_stage(
    request: InactiveStageDecisionRequestWire,
) -> Result<InactiveStageDecisionWire, StagedHandoffError> {
    if request.schema_version != STAGED_HANDOFF_WIRE_SCHEMA_VERSION {
        return Err(StagedHandoffError::UnsupportedSchema(
            request.schema_version,
        ));
    }

    let decision = |inactive: bool, settlement_eligible: bool, reason: &str| {
        InactiveStageDecisionWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            inactive,
            pending_owner: !inactive,
            settlement_eligible,
            reason: reason.to_string(),
        }
    };

    if (request.stage_validity == InactiveStageEvidenceValidityWire::Absent
        && request.stage_id.is_some())
        || (request.receipt_validity
            == InactiveStageEvidenceValidityWire::Absent
            && (request.receipt_id.is_some()
                || request.receipt_outcome.is_some()))
    {
        return Ok(decision(false, false, "stage_evidence_malformed"));
    }

    let has_evidence = request.stage_validity
        != InactiveStageEvidenceValidityWire::Absent
        || request.receipt_validity
            != InactiveStageEvidenceValidityWire::Absent;
    if !has_evidence {
        return Ok(decision(true, false, "no_stage_evidence"));
    }

    if request.stage_validity == InactiveStageEvidenceValidityWire::Malformed
        || request.receipt_validity
            == InactiveStageEvidenceValidityWire::Malformed
    {
        return Ok(decision(false, false, "stage_evidence_malformed"));
    }

    let stage_id = match request.stage_validity {
        InactiveStageEvidenceValidityWire::Absent => None,
        InactiveStageEvidenceValidityWire::Valid => {
            match request.stage_id.as_deref().filter(|id| is_uuid(id)) {
                Some(id) => Some(id),
                None => {
                    return Ok(decision(false, false, "stage_identity_invalid"))
                }
            }
        }
        InactiveStageEvidenceValidityWire::Malformed => unreachable!(),
    };
    let receipt_id = match request.receipt_validity {
        InactiveStageEvidenceValidityWire::Absent => {
            if request.receipt_id.is_some() || request.receipt_outcome.is_some()
            {
                return Ok(decision(
                    false,
                    false,
                    "receipt_evidence_malformed",
                ));
            }
            None
        }
        InactiveStageEvidenceValidityWire::Valid => {
            match request.receipt_id.as_deref().filter(|id| is_uuid(id)) {
                Some(id) if request.receipt_outcome.is_some() => Some(id),
                Some(_) => {
                    return Ok(decision(
                        false,
                        false,
                        "receipt_outcome_unknown",
                    ))
                }
                None => {
                    return Ok(decision(
                        false,
                        false,
                        "receipt_identity_invalid",
                    ))
                }
            }
        }
        InactiveStageEvidenceValidityWire::Malformed => unreachable!(),
    };

    if let (Some(stage_id), Some(receipt_id)) = (stage_id, receipt_id) {
        if stage_id != receipt_id {
            return Ok(decision(
                false,
                false,
                "stage_receipt_identity_mismatch",
            ));
        }
    }
    if !request.origin_identity_valid
        || request.origin_id.as_deref().is_none_or(str::is_empty)
    {
        return Ok(decision(false, false, "origin_identity_unverified"));
    }

    if request.successor_delivered {
        return Ok(decision(true, false, "successor_delivered"));
    }
    if request.execution_state == InactiveStageExecutionStateWire::Present {
        return Ok(decision(true, false, "execution_present"));
    }
    if request.recovery_state == InactiveStageRecoveryStateWire::Active {
        return Ok(decision(true, false, "recovery_active"));
    }
    if request.execution_state == InactiveStageExecutionStateWire::Unknown {
        return Ok(decision(false, false, "execution_unknown"));
    }
    if request.recovery_state == InactiveStageRecoveryStateWire::Unknown {
        return Ok(decision(false, false, "recovery_unknown"));
    }
    if request.origin_finalizing {
        return Ok(decision(false, false, "origin_finalizing"));
    }
    if request.origin_outcome != InactiveStageOriginOutcomeWire::Failed {
        return Ok(decision(false, false, "origin_not_failed"));
    }
    if request.receipt_outcome == Some(InactiveStageReceiptOutcomeWire::Adopted)
    {
        return Ok(decision(false, false, "receipt_adopted"));
    }

    let settlement_eligible = stage_id.is_some()
        && request.execution_state == InactiveStageExecutionStateWire::Absent;
    let reason = if stage_id.is_some() {
        "origin_failed"
    } else {
        "origin_failed_receipt_only"
    };
    Ok(decision(true, settlement_eligible, reason))
}
