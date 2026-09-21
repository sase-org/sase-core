use crate::queue_directive::{queue_weight_is_valid, DEFAULT_QUEUE_WEIGHT};

use super::capacity_math::weights_equal;
use super::claims::{
    claim_is_reusable, claim_lineage, inherited_lineage_weight,
};
use super::records::{effective_weight, is_user_agent_record, record_index};
use super::wire::{
    blocker, RunnerCapacityCandidateDecisionWire, RunnerCapacityClaimWire,
    RunnerCapacityRecordWire, RunnerCapacityRequestWire,
    RunnerCapacityWaiterWire,
};

pub(super) fn build_candidate_decision(
    candidate: &RunnerCapacityRecordWire,
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
    waiters: &[RunnerCapacityWaiterWire],
    request: &RunnerCapacityRequestWire,
    fail_closed: bool,
) -> RunnerCapacityCandidateDecisionWire {
    let index = record_index(records);
    let lineage = claim_lineage(candidate, &index);
    let requested = effective_weight(candidate);
    let active_claim = claims.iter().find(|claim| {
        claim.owner_key == lineage.owner_key && claim_is_reusable(claim)
    });
    let inherited_result = active_claim
        .map(|claim| Ok(Some(claim.occupied_capacity)))
        .unwrap_or_else(|| inherited_lineage_weight(candidate, &index));
    let inherited_error = inherited_result.as_ref().err().cloned();
    let inherited_weight = inherited_result.ok().flatten();
    let requested_weight =
        requested.as_ref().copied().unwrap_or(DEFAULT_QUEUE_WEIGHT);
    let effective_weight = if candidate.queue_weight_explicit {
        requested_weight
    } else {
        inherited_weight.unwrap_or(requested_weight)
    };
    let mut blockers = Vec::new();
    let explicit_weight_compatibility;
    let decision;
    let eligible;

    if let Err(message) = requested.as_ref() {
        blockers.push(blocker(
            "invalid-request-weight",
            message,
            None,
            None,
            None,
            None,
            None,
        ));
        explicit_weight_compatibility = "invalid".to_string();
        decision = "invalid".to_string();
        eligible = false;
    } else if let Some(message) = inherited_error {
        blockers.push(blocker(
            "invalid-inherited-weight",
            &message,
            None,
            None,
            None,
            None,
            None,
        ));
        explicit_weight_compatibility = "invalid-inherited-lineage".to_string();
        decision = "invalid".to_string();
        eligible = false;
    } else if let Some(claim) = active_claim {
        let active_weight = claim.occupied_capacity;
        if candidate.queue_weight_explicit
            && !weights_equal(requested_weight, active_weight)
        {
            blockers.push(blocker(
                "active-claim-weight-conflict",
                "Serial continuation requested a different explicit weight than its active lineage claim.",
                Some(requested_weight),
                None,
                None,
                None,
                None,
            ));
            explicit_weight_compatibility = "conflict".to_string();
            decision = "invalid".to_string();
            eligible = false;
        } else {
            explicit_weight_compatibility = if candidate.queue_weight_explicit {
                "compatible".to_string()
            } else {
                "inherited-active-claim".to_string()
            };
            decision = "reuse_existing_claim".to_string();
            eligible = true;
        }
    } else {
        let waiter = waiters
            .iter()
            .find(|waiter| waiter.artifact_dir == candidate.artifact_dir);
        if let Some(waiter) = waiter {
            blockers = waiter.blockers.clone();
            eligible = waiter.eligible;
        } else if fail_closed || !is_user_agent_record(candidate) {
            blockers.push(blocker(
                "capacity-snapshot-invalid",
                "Runner capacity cannot admit this candidate from the current snapshot.",
                None,
                None,
                None,
                None,
                None,
            ));
            eligible = false;
        } else {
            eligible = false;
        }
        explicit_weight_compatibility = if candidate.queue_weight_explicit {
            "authored".to_string()
        } else if inherited_weight.is_some() {
            "inherited-released-lineage".to_string()
        } else {
            "default".to_string()
        };
        decision = if eligible {
            "acquire_capacity".to_string()
        } else if blockers.iter().any(|blocker| {
            matches!(
                blocker.code.as_str(),
                "invalid-request-weight"
                    | "invalid-capacity-limit"
                    | "capacity-snapshot-invalid"
                    | "capacity-overflow"
            )
        }) || !queue_weight_is_valid(request.effective_limit)
        {
            "invalid".to_string()
        } else {
            "blocked".to_string()
        };
    }

    RunnerCapacityCandidateDecisionWire {
        artifact_dir: candidate.artifact_dir.clone(),
        decision,
        owner_key: lineage.owner_key,
        claim_kind: lineage.claim_kind,
        lineage_key: lineage.lineage_key,
        requested_weight,
        effective_weight,
        inherited_weight,
        active_claim_weight: active_claim.map(|claim| claim.occupied_capacity),
        explicit_weight_compatibility,
        eligible,
        blockers,
    }
}
