use serde_json::json;

use super::*;
use crate::finalizer::wire::{
    FinalizerAssignedBeadWire, FinalizerContextWire, FinalizerObligationWire,
    FINALIZER_WIRE_SCHEMA_VERSION,
};

fn request(
    bead: Option<&str>,
    action: Option<BeadActionWire>,
) -> BeadActionRequestWire {
    BeadActionRequestWire {
        schema_version: BEAD_ACTION_WIRE_SCHEMA_VERSION,
        assigned_bead_id: bead.map(ToOwned::to_owned),
        commit_method: BeadCommitMethodWire::CreateCommit,
        repository_scope: BeadRepositoryScopeWire::Primary,
        primary_repository_identified: true,
        bead_action: action,
        bead_status: Some(BeadActionStatusFactWire::InProgress),
        legacy_do_not_close_bead: false,
    }
}

fn context(
    assigned: Option<FinalizerAssignedBeadWire>,
) -> FinalizerContextWire {
    FinalizerContextWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        run_id: "run-1".to_string(),
        agent_id: "agent-1".to_string(),
        turn_nonce: "nonce-1".to_string(),
        plan_digest: "d".repeat(64),
        requirements: Vec::new(),
        obligations: vec![
            FinalizerObligationWire {
                obligation_id: "repo:primary".to_string(),
                kind: "repository".to_string(),
                display_name: Some("primary".to_string()),
                paths: vec![".".to_string()],
                digest: None,
            },
            FinalizerObligationWire {
                obligation_id: "repo:linked".to_string(),
                kind: "repository".to_string(),
                display_name: Some("linked".to_string()),
                paths: vec!["sase/repos/linked".to_string()],
                digest: None,
            },
        ],
        assigned_bead: assigned,
        context_digest: None,
    }
}

fn assigned() -> FinalizerAssignedBeadWire {
    FinalizerAssignedBeadWire {
        bead_id: "sase-zq.1".to_string(),
        primary_repo_obligation_id: Some("repo:primary".to_string()),
    }
}

fn decision(
    repo_id: &str,
    action: Option<BeadActionWire>,
) -> FinalizerBeadDecisionWire {
    FinalizerBeadDecisionWire {
        repo_id: repo_id.to_string(),
        commit_method: BeadCommitMethodWire::CreateCommit,
        bead_action: action,
        bead_id: None,
        bead_status: Some(BeadActionStatusFactWire::InProgress),
        repository_scope: None,
        legacy_do_not_close_bead: false,
    }
}

#[test]
fn omitted_action_with_assigned_bead_is_rejected_for_every_scope() {
    for (method, scope, identified) in [
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Primary,
            true,
        ),
        (
            BeadCommitMethodWire::CreatePullRequest,
            BeadRepositoryScopeWire::Primary,
            true,
        ),
        (
            BeadCommitMethodWire::CreateProposal,
            BeadRepositoryScopeWire::Primary,
            true,
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Linked,
            false,
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::External,
            false,
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Sdd,
            false,
        ),
    ] {
        let mut input = request(Some("sase-zq.1"), None);
        input.commit_method = method;
        input.repository_scope = scope;
        input.primary_repository_identified = identified;
        let error = decide_bead_action(&input).unwrap_err();
        assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_MISSING);
        assert!(error.message.contains("-B keep"));
        assert!(error.message.contains("-B close"));
    }
}

#[test]
fn omitted_action_without_assigned_bead_is_existing_commit_behavior() {
    let decision = decide_bead_action(&request(None, None)).unwrap();
    assert_eq!(
        decision.disposition,
        BeadActionDispositionWire::Unassociated
    );
    assert!(!decision.close_bead);
}

#[test]
fn keep_commits_without_closing_for_every_assigned_context() {
    for (method, scope, identified, status) in [
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Primary,
            true,
            Some(BeadActionStatusFactWire::InProgress),
        ),
        (
            BeadCommitMethodWire::CreatePullRequest,
            BeadRepositoryScopeWire::Primary,
            true,
            Some(BeadActionStatusFactWire::Closed),
        ),
        (
            BeadCommitMethodWire::CreateProposal,
            BeadRepositoryScopeWire::Primary,
            true,
            None,
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Linked,
            false,
            Some(BeadActionStatusFactWire::Unreadable),
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::External,
            false,
            None,
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Sdd,
            false,
            Some(BeadActionStatusFactWire::Other),
        ),
        (
            BeadCommitMethodWire::CreateCommit,
            BeadRepositoryScopeWire::Unknown,
            false,
            None,
        ),
    ] {
        let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Keep));
        input.commit_method = method;
        input.repository_scope = scope;
        input.primary_repository_identified = identified;
        input.bead_status = status;
        let decision = decide_bead_action(&input).unwrap();
        assert_eq!(decision.disposition, BeadActionDispositionWire::Keep);
        assert_eq!(decision.bead_id.as_deref(), Some("sase-zq.1"));
        assert!(!decision.close_bead);
    }
}

#[test]
fn keep_without_assigned_bead_is_a_uniform_noop() {
    let mut input = request(None, Some(BeadActionWire::Keep));
    input.bead_status = None;
    let decision = decide_bead_action(&input).unwrap();
    assert_eq!(
        decision.disposition,
        BeadActionDispositionWire::Unassociated
    );
    assert!(!decision.close_bead);
}

#[test]
fn close_on_primary_commit_or_pr_closes_an_in_progress_bead() {
    for method in [
        BeadCommitMethodWire::CreateCommit,
        BeadCommitMethodWire::CreatePullRequest,
    ] {
        let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
        input.commit_method = method;
        let decision = decide_bead_action(&input).unwrap();
        assert_eq!(decision.disposition, BeadActionDispositionWire::Close);
        assert!(decision.close_bead);
    }
}

#[test]
fn close_on_already_closed_bead_is_idempotent() {
    let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
    input.bead_status = Some(BeadActionStatusFactWire::Closed);
    let decision = decide_bead_action(&input).unwrap();
    assert_eq!(
        decision.disposition,
        BeadActionDispositionWire::CloseIdempotent
    );
    assert!(!decision.close_bead);
}

#[test]
fn close_on_proposal_is_rejected() {
    let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
    input.commit_method = BeadCommitMethodWire::CreateProposal;
    let error = decide_bead_action(&input).unwrap_err();
    assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_CLOSE_ON_PROPOSAL);
}

#[test]
fn close_outside_identified_primary_is_rejected() {
    for (scope, identified) in [
        (BeadRepositoryScopeWire::Linked, false),
        (BeadRepositoryScopeWire::External, false),
        (BeadRepositoryScopeWire::Sdd, false),
        (BeadRepositoryScopeWire::Unknown, false),
        (BeadRepositoryScopeWire::Primary, false),
        (BeadRepositoryScopeWire::Linked, true),
    ] {
        let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
        input.repository_scope = scope;
        input.primary_repository_identified = identified;
        let error = decide_bead_action(&input).unwrap_err();
        assert_eq!(
            error.code,
            super::wire::BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY
        );
    }
}

#[test]
fn close_without_assigned_bead_is_rejected() {
    let error = decide_bead_action(&request(None, Some(BeadActionWire::Close)))
        .unwrap_err();
    assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_CLOSE_WITHOUT_BEAD);
}

#[test]
fn close_requires_readable_in_progress_or_closed_status() {
    for status in [
        None,
        Some(BeadActionStatusFactWire::Unchecked),
        Some(BeadActionStatusFactWire::Unreadable),
    ] {
        let mut input = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
        input.bead_status = status;
        let error = decide_bead_action(&input).unwrap_err();
        assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_UNREADABLE_STATUS);
    }

    let mut other = request(Some("sase-zq.1"), Some(BeadActionWire::Close));
    other.bead_status = Some(BeadActionStatusFactWire::Other);
    let error = decide_bead_action(&other).unwrap_err();
    assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_INELIGIBLE_STATUS);
}

#[test]
fn closed_assigned_bead_still_requires_an_explicit_decision() {
    let mut input = request(Some("sase-zq.1"), None);
    input.bead_status = Some(BeadActionStatusFactWire::Closed);
    let error = decide_bead_action(&input).unwrap_err();
    assert_eq!(error.code, super::wire::BEAD_ACTION_CODE_MISSING);
}

#[test]
fn json_request_rejects_null_and_legacy_fields() {
    let missing = decide_bead_action_from_json(&json!({
        "schema_version": 1,
        "assigned_bead_id": "sase-zq.1",
        "commit_method": "create_commit",
        "repository_scope": "primary",
        "primary_repository_identified": true
    }))
    .unwrap_err();
    assert_eq!(missing.code, super::wire::BEAD_ACTION_CODE_MISSING);

    let null_action = decide_bead_action_from_json(&json!({
        "schema_version": 1,
        "assigned_bead_id": "sase-zq.1",
        "commit_method": "create_commit",
        "repository_scope": "primary",
        "primary_repository_identified": true,
        "bead_action": null
    }))
    .unwrap_err();
    assert_eq!(null_action.code, super::wire::BEAD_ACTION_CODE_INVALID);

    let legacy = decide_bead_action_from_json(&json!({
        "schema_version": 1,
        "commit_method": "create_commit",
        "repository_scope": "primary",
        "do_not_close_bead": true
    }))
    .unwrap_err();
    assert_eq!(legacy.code, super::wire::BEAD_ACTION_CODE_LEGACY);
}

#[test]
fn json_keep_on_primary_commit_round_trips() {
    let decision = decide_bead_action_from_json(&json!({
        "schema_version": 1,
        "assigned_bead_id": "sase-zq.1",
        "commit_method": "create_commit",
        "repository_scope": "primary",
        "primary_repository_identified": true,
        "bead_action": "keep"
    }))
    .unwrap();
    assert_eq!(decision.disposition, BeadActionDispositionWire::Keep);
    assert_eq!(
        serde_json::to_value(&decision).unwrap()["disposition"],
        json!("keep")
    );
}

#[test]
fn finalizer_keep_and_close_follow_repository_identity() {
    let context = context(Some(assigned()));
    let keep_linked = validate_finalizer_bead_decision(
        &context,
        &decision("repo:linked", Some(BeadActionWire::Keep)),
    )
    .unwrap();
    assert_eq!(keep_linked.disposition, BeadActionDispositionWire::Keep);

    let close_primary = validate_finalizer_bead_decision(
        &context,
        &decision("repo:primary", Some(BeadActionWire::Close)),
    )
    .unwrap();
    assert_eq!(close_primary.disposition, BeadActionDispositionWire::Close);

    let close_linked = validate_finalizer_bead_decision(
        &context,
        &decision("repo:linked", Some(BeadActionWire::Close)),
    )
    .unwrap_err();
    assert_eq!(
        close_linked.code,
        super::wire::BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY
    );
}

#[test]
fn failed_primary_lookup_does_not_grant_close() {
    let mut unidentified = assigned();
    unidentified.primary_repo_obligation_id = None;
    let unidentified_context = context(Some(unidentified));
    let error = validate_finalizer_bead_decision(
        &unidentified_context,
        &decision("repo:primary", Some(BeadActionWire::Close)),
    )
    .unwrap_err();
    assert_eq!(
        error.code,
        super::wire::BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY
    );

    let mut stale = assigned();
    stale.primary_repo_obligation_id = Some("repo:missing".to_string());
    let stale_context = context(Some(stale));
    let stale_error = validate_finalizer_bead_decision(
        &stale_context,
        &decision("repo:missing", Some(BeadActionWire::Close)),
    )
    .unwrap_err();
    assert_eq!(
        stale_error.code,
        super::wire::BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY
    );
}

#[test]
fn declaration_for_a_different_assignment_is_rejected() {
    let context = context(Some(assigned()));
    let mut foreign = decision("repo:primary", Some(BeadActionWire::Keep));
    foreign.bead_id = Some("sase-other".to_string());
    let error =
        validate_finalizer_bead_decision(&context, &foreign).unwrap_err();
    assert_eq!(
        error.code,
        super::wire::BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH
    );

    let expected = assigned();
    validate_finalizer_assigned_bead_binding(&context, Some(&expected))
        .unwrap();
    let mut other = expected.clone();
    other.bead_id = "sase-other".to_string();
    let mismatch =
        validate_finalizer_assigned_bead_binding(&context, Some(&other))
            .unwrap_err();
    assert_eq!(
        mismatch.code,
        super::wire::BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH
    );
}

#[test]
fn unassociated_context_allows_omitted_or_keep_and_rejects_close() {
    let context = context(None);
    let omitted = validate_finalizer_bead_decision(
        &context,
        &decision("repo:primary", None),
    )
    .unwrap();
    assert_eq!(omitted.disposition, BeadActionDispositionWire::Unassociated);

    let keep = validate_finalizer_bead_decision(
        &context,
        &decision("repo:primary", Some(BeadActionWire::Keep)),
    )
    .unwrap();
    assert_eq!(keep.disposition, BeadActionDispositionWire::Unassociated);

    let close = validate_finalizer_bead_decision(
        &context,
        &decision("repo:primary", Some(BeadActionWire::Close)),
    )
    .unwrap_err();
    assert_eq!(close.code, super::wire::BEAD_ACTION_CODE_CLOSE_WITHOUT_BEAD);
}
