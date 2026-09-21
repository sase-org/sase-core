//! Candidate admission decision tests.

use super::support::*;

#[test]
fn candidate_decision_reuses_active_lineage_or_rejects_reweight() {
    let mut active = running("root", Some(2.0));
    active.agent_family = Some("fam".to_string());
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(1.0));
    successor.agent_family = Some("fam".to_string());
    successor.parent_timestamp = Some("root".to_string());

    let inherited =
        snapshot_with_candidate(2.0, vec![active.clone()], successor.clone());
    let decision = inherited.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "reuse_existing_claim");
    assert_eq!(decision.effective_weight, 2.0);
    assert_eq!(decision.inherited_weight, Some(2.0));
    assert_eq!(
        decision.explicit_weight_compatibility,
        "inherited-active-claim"
    );

    successor.queue_weight = Some(3.0);
    successor.queue_weight_explicit = true;
    let conflict = snapshot_with_candidate(2.0, vec![active], successor);
    let decision = conflict.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "invalid");
    assert_eq!(decision.explicit_weight_compatibility, "conflict");
    assert_eq!(decision.blockers[0].code, "active-claim-weight-conflict");
}

#[test]
fn candidate_decision_excludes_unadmitted_candidate_from_own_claim() {
    let unrelated = running("unrelated", Some(2.0));
    let mut candidate = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    candidate.agent_family = Some("fam".to_string());
    candidate.parent_timestamp = Some("released-parent".to_string());
    candidate.run_started_at = Some("2026-09-10T00:00:01Z".to_string());

    let result = snapshot_with_candidate(
        2.0,
        vec![unrelated, candidate.clone()],
        candidate,
    );
    assert_eq!(result.occupied_lanes, 1);
    assert_eq!(result.occupied_capacity, 2.0);
    let decision = result.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "blocked");
    assert_eq!(decision.blockers[0].code, "insufficient-capacity");
}

#[test]
fn released_lineage_inherits_weight_unless_candidate_authors_new_weight() {
    let mut parent = running("parent", Some(2.0));
    parent.live = false;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", None);
    successor.parent_timestamp = Some("parent".to_string());

    let inherited =
        snapshot_with_candidate(2.0, vec![parent.clone()], successor.clone());
    let decision = inherited.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "acquire_capacity");
    assert_eq!(decision.effective_weight, 2.0);
    assert_eq!(
        decision.explicit_weight_compatibility,
        "inherited-released-lineage"
    );

    successor.queue_weight = Some(0.5);
    successor.queue_weight_explicit = true;
    let authored = snapshot_with_candidate(2.0, vec![parent], successor);
    let decision = authored.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "acquire_capacity");
    assert_eq!(decision.effective_weight, 0.5);
    assert_eq!(decision.explicit_weight_compatibility, "authored");
}

#[test]
fn invalid_released_lineage_weight_blocks_omitted_candidate() {
    let mut parent = running("parent", None);
    parent.live = false;
    parent.queue_weight_invalid = true;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", None);
    successor.parent_timestamp = Some("parent".to_string());

    let result = snapshot_with_candidate(2.0, vec![parent], successor);
    let decision = result.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "invalid");
    assert_eq!(
        decision.explicit_weight_compatibility,
        "invalid-inherited-lineage"
    );
    assert_eq!(decision.blockers[0].code, "invalid-inherited-weight");
    assert_eq!(
        waiter(&result, "successor").blockers[0].code,
        "invalid-request-weight"
    );
}

#[test]
fn candidate_serial_successor_of_live_parallel_member_reuses_parallel_lineage()
{
    let mut parallel = running("parallel", Some(2.0));
    parallel.agent_family = Some("fam".to_string());
    parallel.agent_family_parallel = true;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_family = Some("fam".to_string());
    successor.parent_timestamp = Some("parallel".to_string());

    let decision = snapshot_with_candidate(4.0, vec![parallel], successor)
        .candidate_decision
        .unwrap();
    assert_eq!(decision.decision, "reuse_existing_claim");
    assert_eq!(decision.claim_kind, "parallel_member");
    assert_eq!(decision.owner_key, "proj:fam:parallel:parallel");
    assert_eq!(decision.lineage_key, "fam:parallel:parallel");
    assert_eq!(decision.effective_weight, 2.0);
}

#[test]
fn persisted_owner_keeps_released_parallel_successor_off_unrelated_family_claim(
) {
    let mut serial_branch = running("serial-branch", Some(2.0));
    serial_branch.agent_family = Some("fam".to_string());
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_family = Some("fam".to_string());
    successor.parent_timestamp = Some("parallel-parent".to_string());
    successor.runner_claim_owner_key =
        Some("fam:parallel:parallel-parent".to_string());

    let result = snapshot_with_candidate(4.0, vec![serial_branch], successor);
    assert_eq!(result.occupied_capacity, 2.0);
    let decision = result.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "acquire_capacity");
    assert_eq!(decision.owner_key, "proj:fam:parallel:parallel-parent");
    assert_eq!(decision.lineage_key, "fam:parallel:parallel-parent");
    assert_ne!(decision.owner_key, result.claims[0].owner_key);
}
