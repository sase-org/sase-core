//! Truth table for [`super::decide_workspace_retention`].

use super::{
    decide_workspace_retention, WorkspaceRetentionClaimWire,
    WorkspaceRetentionDirtWire, WorkspaceRetentionRequestWire,
    WorkspaceRetentionSuccessorWire,
    WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_DISPOSITION_RELEASE,
    WORKSPACE_RETENTION_DISPOSITION_RETAIN,
    WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_REASON_CREATOR_RESTORED,
    WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE,
    WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY,
    WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
    WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR,
    WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION,
};

fn claim(pid: u32) -> WorkspaceRetentionClaimWire {
    WorkspaceRetentionClaimWire {
        workspace_num: 17,
        workflow: "ace(run)-260101_000000".to_string(),
        cl_name: "feature".to_string(),
        artifacts_timestamp: "20260101000000".to_string(),
        pid,
    }
}

fn request() -> WorkspaceRetentionRequestWire {
    WorkspaceRetentionRequestWire {
        schema_version: WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION,
        expected_claim: claim(111),
        observed_claim: Some(claim(111)),
        successor: WorkspaceRetentionSuccessorWire::default(),
        creator_restored: false,
        gate_released_workspace: false,
        dirt: WorkspaceRetentionDirtWire::default(),
        probe_failed: false,
    }
}

fn assert_decision(
    input: &WorkspaceRetentionRequestWire,
    disposition: &str,
    reason: &str,
) {
    let decision = decide_workspace_retention(input);
    assert_eq!(
        decision.schema_version,
        WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION
    );
    assert_eq!(decision.disposition, disposition);
    assert_eq!(decision.reason, reason);
}

#[test]
fn same_workspace_successor_releases_without_hold() {
    let mut input = request();
    input.successor = WorkspaceRetentionSuccessorWire {
        confirmed_active: true,
        same_workspace: true,
        inherited_obligations: true,
    };
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RELEASE,
        WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR,
    );
}

#[test]
fn historical_carried_to_alone_does_not_count_as_successor() {
    let mut input = request();
    input.successor = WorkspaceRetentionSuccessorWire {
        confirmed_active: false,
        same_workspace: true,
        inherited_obligations: true,
    };
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    );
}

#[test]
fn different_workspace_successor_retains_original_dirt() {
    let mut input = request();
    input.successor = WorkspaceRetentionSuccessorWire {
        confirmed_active: true,
        same_workspace: false,
        inherited_obligations: true,
    };
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    );
}

#[test]
fn session_owned_dirt_without_successor_retains() {
    let mut input = request();
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    );
}

#[test]
fn linked_dirty_is_session_owned_even_when_primary_is_clean() {
    let mut input = request();
    input.dirt.session_owned = true;
    input.dirt.all_proven_clean = false;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    );
}

#[test]
fn proven_clean_without_successor_releases() {
    let mut input = request();
    input.dirt.all_proven_clean = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RELEASE,
        WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
    );
}

#[test]
fn only_unchanged_foreign_dirt_releases() {
    let mut input = request();
    input.dirt.only_unchanged_foreign = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RELEASE,
        WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
    );
}

#[test]
fn probe_failure_retains_and_is_not_treated_as_clean() {
    let mut input = request();
    input.probe_failed = true;
    input.dirt.all_proven_clean = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    );
}

#[test]
fn missing_baseline_with_custody_retains() {
    let mut input = request();
    input.dirt.missing_baseline = true;
    input.dirt.positive_custody = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY,
    );
}

#[test]
fn stale_owner_pid_is_ownership_moved() {
    let mut input = request();
    input.observed_claim = Some(claim(999));
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
        WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    );
}

#[test]
fn missing_observed_claim_is_ownership_moved() {
    let mut input = request();
    input.observed_claim = None;
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
        WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    );
}

#[test]
fn gate_released_without_matching_claim_does_not_reacquire() {
    let mut input = request();
    input.observed_claim = None;
    input.gate_released_workspace = true;
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
        WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE,
    );
}

#[test]
fn creator_restored_releases_to_live_creator() {
    let mut input = request();
    input.creator_restored = true;
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RELEASE,
        WORKSPACE_RETENTION_REASON_CREATOR_RESTORED,
    );
}

#[test]
fn successor_same_workspace_without_inherited_obligations_does_not_release() {
    let mut input = request();
    input.successor = WorkspaceRetentionSuccessorWire {
        confirmed_active: true,
        same_workspace: true,
        inherited_obligations: false,
    };
    input.dirt.session_owned = true;
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    );
}

#[test]
fn timestamp_mismatch_is_ownership_moved() {
    let mut input = request();
    let mut observed = claim(111);
    observed.artifacts_timestamp = "20260101999999".to_string();
    input.observed_claim = Some(observed);
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
        WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    );
}

#[test]
fn uncertain_leftover_without_clean_proof_retains() {
    let input = request();
    assert_decision(
        &input,
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    );
}
