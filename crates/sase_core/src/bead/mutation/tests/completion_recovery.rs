use super::super::*;
use super::support::dual_tempdir as tempdir;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::show_issue;
use crate::bead::wire::{
    BeadCloseProvenanceWire, BeadReopenCauseWire, BeadResolutionWire,
    BeadTierWire, IssueTypeWire, PhaseSizeWire, StatusWire,
};
use crate::bead::BeadLaunchClaimContextWire;

fn provenance(run_id: &str) -> BeadCloseProvenanceWire {
    BeadCloseProvenanceWire {
        run_id: run_id.to_string(),
        agent_name: format!("{run_id}.agent"),
        artifact_dir: format!("/artifacts/{run_id}"),
    }
}

fn recovery_request(run_id: &str) -> BeadFailedCloseRecoveryRequestWire {
    BeadFailedCloseRecoveryRequestWire {
        run_id: run_id.to_string(),
        failed_at: "2026-01-02T00:00:00Z".to_string(),
        failure_code: "hook_failed".to_string(),
        evidence_locations: vec![format!("/artifacts/{run_id}/failure.json")],
        note: "STITCH FAILED: the host commit hook failed; preserved workspace evidence is available.".to_string(),
        actor: "sase-finalizer".to_string(),
    }
}

fn create_task(beads_dir: &std::path::Path, title: &str) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn close_with_run(
    beads_dir: &std::path::Path,
    ids: &[String],
    run_id: &str,
    resolution: BeadResolutionWire,
    now: &str,
) {
    close_issues_with_note_and_provenance(
        beads_dir,
        ids,
        Some("verified before host finalization".to_string()),
        Some(resolution),
        false,
        None,
        Some(format!("{run_id}.agent")),
        Some(now.to_string()),
        None,
        Some(provenance(run_id)),
    )
    .unwrap();
}

#[test]
fn recovers_all_current_run_closes_once_and_clears_on_a_later_claim() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let first = create_task(&beads_dir, "First");
        let second = create_task(&beads_dir, "Second");
        close_with_run(
            &beads_dir,
            &[first.clone(), second.clone()],
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:01:00Z",
        );

        let recovered =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert!(recovered.changed);
        assert_eq!(recovered.recovered.len(), 2);
        assert!(recovered.already_applied.is_empty());
        assert!(recovered.skipped.is_empty());
        for id in [&first, &second] {
            let issue = show_issue(&beads_dir, id).unwrap();
            assert_eq!(issue.status, StatusWire::Open);
            assert_eq!(issue.close_history.len(), 1);
            assert_eq!(
                issue.close_history[0].reopened_via,
                BeadReopenCauseWire::CompletionFailure
            );
            assert_eq!(
                issue.completion_failure.as_ref().unwrap().run_id,
                "origin-run"
            );
            assert_eq!(
                issue
                    .completion_failure
                    .as_ref()
                    .unwrap()
                    .evidence_locations,
                vec!["/artifacts/origin-run/failure.json"]
            );
            assert_eq!(
                issue
                    .notes
                    .iter()
                    .filter(|note| note.text.starts_with("STITCH FAILED:"))
                    .count(),
                1
            );
        }

        let repeated =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert!(!repeated.changed);
        assert!(repeated.recovered.is_empty());
        assert_eq!(repeated.already_applied.len(), 2);
        assert!(repeated.skipped.is_empty());
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let recovery_event_count = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation
                    == BeadEventOperationWire::CompletionFailureRecovered
            })
            .count();
        assert_eq!(recovery_event_count, 2);

        claim_for_agent_launch(
            &beads_dir,
            &first,
            "successor.agent",
            Some("2026-01-03T00:00:00Z".to_string()),
            &BeadLaunchClaimContextWire::default(),
        )
        .unwrap();
        let claimed = show_issue(&beads_dir, &first).unwrap();
        assert_eq!(claimed.status, StatusWire::InProgress);
        assert!(claimed.completion_failure.is_none());
        assert_eq!(claimed.assignee, "successor.agent");
        assert_reprojection_byte_stable(&beads_dir, "failed-close-recovery");
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn reopens_closed_ancestors_but_never_recovers_non_done_or_newer_closes() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                tier: Some(BeadTierWire::Epic),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:00:01Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&phase.id),
            "child-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:01:00Z",
        );
        close_issues(
            &beads_dir,
            std::slice::from_ref(&epic.id),
            Some("parent done".to_string()),
            Some(BeadResolutionWire::Done),
            false,
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        let recovered =
            recover_failed_closes(&beads_dir, recovery_request("child-run"))
                .unwrap();
        assert_eq!(recovered.recovered.len(), 1);
        assert_eq!(recovered.reopened_ancestor_ids, vec![epic.id.clone()]);
        let reopened_parent = show_issue(&beads_dir, &epic.id).unwrap();
        assert_eq!(reopened_parent.status, StatusWire::Open);
        assert_eq!(reopened_parent.close_history.len(), 1);
        assert!(reopened_parent.completion_failure.is_none());
        let reopened_phase = show_issue(&beads_dir, &phase.id).unwrap();
        assert_eq!(reopened_phase.status, StatusWire::Open);
        assert_eq!(
            reopened_phase.completion_failure.as_ref().unwrap().run_id,
            "child-run"
        );

        let canceled = create_task(&beads_dir, "Canceled close");
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&canceled),
            "old-run",
            BeadResolutionWire::Canceled,
            "2026-01-01T00:03:00Z",
        );
        let newer = create_task(&beads_dir, "Reclosed elsewhere");
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&newer),
            "old-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:04:00Z",
        );
        open_issue(
            &beads_dir,
            &newer,
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap();
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&newer),
            "new-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:06:00Z",
        );
        let unproven = create_task(&beads_dir, "Human close");
        close_issues(
            &beads_dir,
            std::slice::from_ref(&unproven),
            None,
            Some(BeadResolutionWire::Done),
            false,
            Some("2026-01-01T00:07:00Z".to_string()),
        )
        .unwrap();

        let stale =
            recover_failed_closes(&beads_dir, recovery_request("old-run"))
                .unwrap();
        assert!(!stale.changed);
        assert_eq!(stale.skipped.len(), 2);
        assert!(stale.skipped.iter().any(|entry| {
            entry.issue_id == canceled
                && entry.reason.as_deref() == Some("resolution_not_done")
        }));
        assert!(stale.skipped.iter().any(|entry| {
            entry.issue_id == newer
                && entry.reason.as_deref() == Some("close_not_current")
        }));
        assert_eq!(
            show_issue(&beads_dir, &unproven).unwrap().status,
            StatusWire::Closed
        );
        assert_reprojection_byte_stable(&beads_dir, "failed-close-eligibility");
        assert_dual_mode_parity_for_current_mode();
    });
}

fn assert_recovered_episode(
    beads_dir: &std::path::Path,
    id: &str,
    run_id: &str,
) {
    let issue = show_issue(beads_dir, id).unwrap();
    assert_eq!(issue.status, StatusWire::Open);
    assert_eq!(issue.close_history.len(), 1);
    assert_eq!(
        issue.close_history[0].reopened_via,
        BeadReopenCauseWire::CompletionFailure
    );
    assert_eq!(issue.completion_failure.as_ref().unwrap().run_id, run_id);
    assert_eq!(
        issue
            .notes
            .iter()
            .filter(|note| note.text.starts_with("STITCH FAILED:"))
            .count(),
        1
    );
}

#[test]
fn recovers_every_eligible_close_in_a_phase_and_plan_chain() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let plan = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                tier: Some(BeadTierWire::Epic),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(plan.id.clone()),
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:00:01Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&phase.id),
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:01:00Z",
        );
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&plan.id),
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:02:00Z",
        );

        let recovered =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert!(recovered.changed);
        assert_eq!(recovered.recovered.len(), 2);
        assert!(recovered.already_applied.is_empty());
        assert!(recovered.skipped.is_empty());
        assert!(recovered.reopened_ancestor_ids.is_empty());
        assert_recovered_episode(&beads_dir, &phase.id, "origin-run");
        assert_recovered_episode(&beads_dir, &plan.id, "origin-run");

        let repeated =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert!(!repeated.changed);
        assert!(repeated.recovered.is_empty());
        assert_eq!(repeated.already_applied.len(), 2);
        assert!(repeated.skipped.is_empty());
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let recovery_event_count = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation
                    == BeadEventOperationWire::CompletionFailureRecovered
            })
            .count();
        assert_eq!(recovery_event_count, 2);
        let opened_count = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueOpened
            })
            .count();
        assert_eq!(opened_count, 0);
        assert_reprojection_byte_stable(
            &beads_dir,
            "failed-close-recovery-phase-plan",
        );
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn recovers_deeper_ancestor_chain_closed_by_one_run() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                tier: Some(BeadTierWire::Epic),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let inner = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Inner plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:00:01Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(inner.id.clone()),
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:00:02Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&phase.id),
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:01:00Z",
        );
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&inner.id),
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:02:00Z",
        );
        close_with_run(
            &beads_dir,
            std::slice::from_ref(&epic.id),
            "origin-run",
            BeadResolutionWire::Done,
            "2026-01-01T00:03:00Z",
        );

        let recovered =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert_eq!(recovered.recovered.len(), 3);
        assert!(recovered.reopened_ancestor_ids.is_empty());
        assert_recovered_episode(&beads_dir, &phase.id, "origin-run");
        assert_recovered_episode(&beads_dir, &inner.id, "origin-run");
        assert_recovered_episode(&beads_dir, &epic.id, "origin-run");

        let repeated =
            recover_failed_closes(&beads_dir, recovery_request("origin-run"))
                .unwrap();
        assert!(!repeated.changed);
        assert_eq!(repeated.already_applied.len(), 3);
        assert_reprojection_byte_stable(
            &beads_dir,
            "failed-close-recovery-deeper-chain",
        );
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn provenance_validation_rejects_partial_run_identity() {
    let mut close = provenance("origin-run");
    close.artifact_dir = " ".to_string();
    assert!(close
        .validate()
        .unwrap_err()
        .message
        .contains("artifact_dir"));
}
