use crate::queue_directive::queue_weight_is_valid;

use super::candidate::build_candidate_decision;
use super::capacity_math::compensated_sum;
use super::claims::{
    active_claim_keys, build_claims, claim_is_reusable, claim_lineage,
    inherited_lineage_weight,
};
use super::records::record_index;
use super::waiters::build_waiters;
use super::wire::{
    diagnostic, RunnerCapacityClaimWire, RunnerCapacityRecordWire,
    RunnerCapacityRequestWire, RunnerCapacitySnapshotWire,
    RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
};

pub fn runner_capacity_snapshot(
    request: &RunnerCapacityRequestWire,
) -> RunnerCapacitySnapshotWire {
    let mut request = request.clone();
    for record in &mut request.records {
        record.normalize_queue_capacity_aliases();
    }
    if let Some(candidate) = &mut request.candidate {
        candidate.normalize_queue_capacity_aliases();
    }
    let request = &request;
    let mut diagnostics = Vec::new();
    let claim_records = records_excluding_candidate(request);
    let (mut claims, invalid_live_claim) =
        build_claims(&claim_records, &mut diagnostics);
    claims.sort_by(|left, right| {
        left.owner_key
            .cmp(&right.owner_key)
            .then_with(|| left.claim_kind.cmp(&right.claim_kind))
    });
    let occupied_lanes =
        claims.iter().map(|claim| claim.occupied_lanes).sum::<u32>();
    let (occupied_capacity, capacity_overflow) = match compensated_sum(
        claims.iter().map(|claim| claim.occupied_capacity),
    ) {
        Some(value) => (value, false),
        None => {
            diagnostics.push(diagnostic(
                "capacity-overflow",
                "Occupied runner capacity overflowed while summing live claims.",
                None,
            ));
            (f64::MAX, true)
        }
    };
    let limit_invalid = !queue_weight_is_valid(request.effective_limit);
    if limit_invalid {
        diagnostics.push(diagnostic(
            "invalid-capacity-limit",
            "The effective runner capacity limit must be positive and finite.",
            None,
        ));
    }
    let fail_closed = invalid_live_claim || capacity_overflow || limit_invalid;
    let waiter_request = request_for_waiters(request, &claim_records, &claims);
    let active_claims = active_claim_keys(&claims);
    let waiters = build_waiters(
        &waiter_request,
        occupied_capacity,
        &claims,
        &active_claims,
        fail_closed,
        &mut diagnostics,
    );
    let first_eligible_artifact_dir = waiters
        .iter()
        .find(|waiter| waiter.eligible)
        .map(|waiter| waiter.artifact_dir.clone());
    let candidate_decision = request.candidate.as_ref().map(|candidate| {
        build_candidate_decision(
            candidate,
            &claim_records,
            &claims,
            &waiters,
            request,
            fail_closed,
        )
    });

    RunnerCapacitySnapshotWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit: request.effective_limit,
        occupied_lanes,
        occupied_capacity,
        claims,
        waiters,
        first_eligible_artifact_dir,
        candidate_decision,
        diagnostics,
    }
}

fn records_excluding_candidate(
    request: &RunnerCapacityRequestWire,
) -> Vec<RunnerCapacityRecordWire> {
    let Some(candidate) = request.candidate.as_ref() else {
        return request.records.clone();
    };
    request
        .records
        .iter()
        .filter(|record| record.artifact_dir != candidate.artifact_dir)
        .cloned()
        .collect()
}

fn request_for_waiters(
    request: &RunnerCapacityRequestWire,
    claim_records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
) -> RunnerCapacityRequestWire {
    let mut records = claim_records.to_vec();
    if let Some(candidate) = request.candidate.as_ref() {
        records.push(candidate_record_with_effective_weight(
            candidate,
            claim_records,
            claims,
        ));
    }
    RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit: request.effective_limit,
        records,
        holds: request.holds.clone(),
        candidate: None,
        now: request.now.clone(),
        deference_seconds_per_step: request.deference_seconds_per_step,
        deference_max_seconds: request.deference_max_seconds,
        feature_flags: request.feature_flags.clone(),
    }
}

fn candidate_record_with_effective_weight(
    candidate: &RunnerCapacityRecordWire,
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
) -> RunnerCapacityRecordWire {
    if candidate.queue_weight_explicit || candidate.queue_weight_invalid {
        return candidate.clone();
    }
    let index = record_index(records);
    let lineage = claim_lineage(candidate, &index);
    let inherited = claims
        .iter()
        .find(|claim| {
            claim.owner_key == lineage.owner_key && claim_is_reusable(claim)
        })
        .map(|claim| Ok(Some(claim.occupied_capacity)))
        .unwrap_or_else(|| inherited_lineage_weight(candidate, &index));
    match inherited {
        Ok(Some(weight)) => {
            let mut record = candidate.clone();
            record.queue_weight = Some(weight);
            record
        }
        Ok(None) => candidate.clone(),
        Err(_) => {
            let mut record = candidate.clone();
            record.queue_weight_invalid = true;
            record
        }
    }
}
