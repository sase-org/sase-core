use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::StatusWire;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::tempdir;

use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::bead::mutation::store::bead_mutation_holder_path;
use crate::bead::mutation::store::bead_mutation_lock_path;
use crate::bead::mutation::store::lock_bead_mutation_with_timeout;
use crate::bead::wire::BeadError;
use std::time::Duration;
#[test]
fn claim_for_agent_launch_claims_open_and_reassigns_in_progress_issue() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
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
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let claimed_epic = claim_for_agent_launch(
        &beads_dir,
        &epic.id,
        "land-agent",
        Some("2026-01-01T00:01:30Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(claimed_epic.issue_type, IssueTypeWire::Plan);
    assert_eq!(claimed_epic.status, StatusWire::InProgress);
    assert_eq!(claimed_epic.assignee, "land-agent");

    let first = claim_for_agent_launch(
        &beads_dir,
        &phase.id,
        "agent-1",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    let first_issue = first.issue.unwrap();
    assert_eq!(first.operation, "claim_for_agent_launch");
    assert!(first.changed);
    assert_eq!(first.issue_ids, vec![phase.id.clone()]);
    assert_eq!(first_issue.status, StatusWire::InProgress);
    assert_eq!(first_issue.assignee, "agent-1");
    assert_eq!(first_issue.updated_at, "2026-01-01T00:02:00Z");
    assert_reprojection_byte_stable(&beads_dir, "claim");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let claim_event = streams[0].events.last().unwrap();
    assert_eq!(claim_event.operation, BeadEventOperationWire::IssueUpdated);
    assert!(matches!(
        &claim_event.payload,
        BeadEventPayloadWire::IssueUpdated { fields }
            if fields.status == Some(StatusWire::InProgress)
                && fields.assignee.as_deref() == Some("agent-1")
    ));
    let reduced = reduce_event_streams(&streams).unwrap();
    let reduced_phase =
        reduced.iter().find(|issue| issue.id == phase.id).unwrap();
    assert_eq!(reduced_phase.assignee, "agent-1");

    let before_repeated = persisted_claim_state(&beads_dir);
    let repeated = claim_for_agent_launch(
        &beads_dir,
        &phase.id,
        "agent-1",
        Some("2026-01-01T00:02:30Z".to_string()),
    )
    .unwrap();
    assert!(!repeated.changed);
    assert!(repeated.message.is_empty());
    assert_eq!(repeated.issue.unwrap().updated_at, "2026-01-01T00:02:00Z");
    assert_eq!(persisted_claim_state(&beads_dir), before_repeated);

    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let reassigned = claim_for_agent_launch(
        &beads_dir,
        &phase.id,
        "agent-2",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(reassigned.status, StatusWire::InProgress);
    assert_eq!(reassigned.assignee, "agent-2");
    assert_eq!(reassigned.updated_at, "2026-01-01T00:03:00Z");
    let projection =
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(projection.contains(r#""assignee":"agent-2""#));
    assert!(projection.contains(r#""updated_at":"2026-01-01T00:03:00Z""#));
}

#[test]
fn claim_for_agent_launch_rejects_missing_closed_and_blank_requests() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "sase-1",
            "Closed plan",
            "plan",
            None,
            "closed",
            "2026-01-01T00:00:00Z",
        ) + "\n",
    )
    .unwrap();

    let missing =
        claim_for_agent_launch(&beads_dir, "sase-missing", "agent", None)
            .unwrap_err();
    assert_eq!(missing.kind, "not_found");
    assert!(missing.message.contains("sase-missing"));

    let closed = claim_for_agent_launch(&beads_dir, "sase-1", "agent", None)
        .unwrap_err();
    assert_eq!(closed.kind, "closed");
    assert!(closed.message.contains("closed bead"));

    for agent_name in ["", "  \t"] {
        let invalid =
            claim_for_agent_launch(&beads_dir, "sase-1", agent_name, None)
                .unwrap_err();
        assert_eq!(invalid.kind, "validation");
        assert!(invalid.message.contains("cannot be empty or blank"));
    }
}

#[test]
fn claim_for_agent_wait_claims_open_and_is_idempotent_for_same_agent() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();

    let first = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    let issue = first.issue.unwrap();
    assert_eq!(first.operation, "claim_for_agent_wait");
    assert!(first.changed);
    assert_eq!(first.issue_ids, vec![phase_id.clone()]);
    assert_eq!(issue.status, StatusWire::Claimed);
    assert_eq!(issue.assignee, "agent-1");
    assert_eq!(issue.updated_at, "2026-01-01T00:02:00Z");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let claim_event = streams[0].events.last().unwrap();
    assert_eq!(claim_event.operation, BeadEventOperationWire::IssueUpdated);
    assert!(matches!(
        &claim_event.payload,
        BeadEventPayloadWire::IssueUpdated { fields }
            if fields.status == Some(StatusWire::Claimed)
                && fields.assignee.as_deref() == Some("agent-1")
    ));

    let before = persisted_claim_state(&beads_dir);
    let repeated = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(!repeated.changed);
    assert!(repeated.message.is_empty());
    assert_eq!(repeated.issue.unwrap().updated_at, "2026-01-01T00:02:00Z");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn claim_for_agent_wait_declines_other_claims_and_terminal_states_without_writes(
) {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    let before_other_claim = persisted_claim_state(&beads_dir);
    let other_claim = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-2",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(!other_claim.changed);
    assert!(other_claim.message.contains("status is claimed"));
    assert!(other_claim.message.contains("holder is agent-1"));
    assert_eq!(persisted_claim_state(&beads_dir), before_other_claim);

    claim_for_agent_launch(
        &beads_dir,
        &phase_id,
        "agent-2",
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    let before_in_progress = persisted_claim_state(&beads_dir);
    let retained = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-2",
        Some("2026-01-01T00:04:30Z".to_string()),
    )
    .unwrap();
    assert!(!retained.changed);
    assert!(retained.message.is_empty());
    assert_eq!(retained.issue.unwrap().updated_at, "2026-01-01T00:04:00Z");
    assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

    let in_progress = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-3",
        Some("2026-01-01T00:05:00Z".to_string()),
    )
    .unwrap();
    assert!(!in_progress.changed);
    assert!(in_progress.message.contains("status is in_progress"));
    assert!(in_progress.message.contains("holder is agent-2"));
    assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

    close_issues(
        &beads_dir,
        std::slice::from_ref(&phase_id),
        None,
        None,
        false,
        Some("2026-01-01T00:06:00Z".to_string()),
    )
    .unwrap();
    let before_closed = persisted_claim_state(&beads_dir);
    let closed = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-3",
        Some("2026-01-01T00:07:00Z".to_string()),
    )
    .unwrap();
    assert!(!closed.changed);
    assert!(closed.message.contains("status is closed"));
    assert!(closed.message.contains("holder is agent-2"));
    assert_eq!(persisted_claim_state(&beads_dir), before_closed);
}

#[test]
fn release_agent_claim_is_owner_guarded_and_round_trips_to_open() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    let before_wrong_agent = persisted_claim_state(&beads_dir);
    let wrong_agent = release_agent_claim(
        &beads_dir,
        &phase_id,
        "agent-2",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(!wrong_agent.changed);
    assert_eq!(persisted_claim_state(&beads_dir), before_wrong_agent);

    let released = release_agent_claim(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    let released_issue = released.issue.unwrap();
    assert_eq!(released.operation, "release_agent_claim");
    assert!(released.changed);
    assert_eq!(released_issue.status, StatusWire::Open);
    assert!(released_issue.assignee.is_empty());
    assert_eq!(released_issue.updated_at, "2026-01-01T00:04:00Z");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let release_event = streams[0].events.last().unwrap();
    assert!(matches!(
        &release_event.payload,
        BeadEventPayloadWire::IssueUpdated { fields }
            if fields.status == Some(StatusWire::Open)
                && fields.assignee.as_deref() == Some("")
    ));

    let before_open_release = persisted_claim_state(&beads_dir);
    assert!(
        !release_agent_claim(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap()
        .changed
    );
    assert_eq!(persisted_claim_state(&beads_dir), before_open_release);

    let reclaimed = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-2",
        Some("2026-01-01T00:06:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(reclaimed.status, StatusWire::Claimed);
    assert_eq!(reclaimed.assignee, "agent-2");
}

#[test]
fn release_agent_claim_declines_in_progress_and_closed_without_writes() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    claim_for_agent_launch(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    let before_in_progress = persisted_claim_state(&beads_dir);
    assert!(
        !release_agent_claim(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap()
        .changed
    );
    assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

    close_issues(
        &beads_dir,
        std::slice::from_ref(&phase_id),
        None,
        None,
        false,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    let before_closed = persisted_claim_state(&beads_dir);
    assert!(
        !release_agent_claim(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap()
        .changed
    );
    assert_eq!(persisted_claim_state(&beads_dir), before_closed);
}

#[test]
fn agent_claim_mutations_reject_missing_and_blank_requests() {
    let (_temp, beads_dir, _phase_id) = claim_mutation_fixture();

    for mutation in [
        claim_for_agent_wait
            as fn(
                &Path,
                &str,
                &str,
                Option<String>,
            ) -> Result<BeadMutationOutcomeWire, BeadError>,
        release_agent_claim,
    ] {
        let missing =
            mutation(&beads_dir, "sase-missing", "agent", None).unwrap_err();
        assert_eq!(missing.kind, "not_found");
        assert!(missing.message.contains("sase-missing"));

        for agent_name in ["", "  \t"] {
            let invalid =
                mutation(&beads_dir, "sase-missing", agent_name, None)
                    .unwrap_err();
            assert_eq!(invalid.kind, "validation");
            assert!(invalid.message.contains("cannot be empty or blank"));
        }
    }
}

#[test]
fn bead_mutation_lock_contention_times_out() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    let lock_path = bead_mutation_lock_path(&beads_dir);
    let holder = lock_bead_mutation_with_timeout(
        &beads_dir,
        &lock_path,
        Duration::from_secs(1),
        "test_holder",
    )
    .unwrap();

    let started = Instant::now();
    let error = lock_bead_mutation_with_timeout(
        &beads_dir,
        &lock_path,
        Duration::from_millis(50),
        "test_contender",
    )
    .unwrap_err();

    assert_eq!(error.kind, "lock_timeout");
    assert!(error.message.contains("timed out"));
    assert!(error
        .message
        .contains(&format!("pid={}", std::process::id())));
    assert!(error.message.contains("operation=test_holder"));
    assert!(started.elapsed() < Duration::from_secs(1));
    holder.release().unwrap();
    assert!(!bead_mutation_holder_path(&beads_dir).exists());
}

#[test]
fn preclaim_epic_work_plan_updates_once_and_returns_rollback() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let p1 = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "P1".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            assignee: "previous".to_string(),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    update_issue(
        &beads_dir,
        &p1.id,
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            assignee: Some("previous".to_string()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let p2 = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "P2".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let outcome = preclaim_epic_work_plan(
        &beads_dir,
        &epic.id,
        &[
            BeadPreclaimAssignmentWire {
                bead_id: p1.id.clone(),
                agent_name: "agent-1".to_string(),
            },
            BeadPreclaimAssignmentWire {
                bead_id: p2.id.clone(),
                agent_name: "agent-2".to_string(),
            },
        ],
        Some("land-agent".to_string()),
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(outcome.operation, "preclaim_epic_work");
    assert_eq!(
        outcome.issue_ids,
        vec![p1.id.clone(), p2.id.clone(), epic.id.clone()]
    );
    assert_eq!(
        outcome.rollback_preclaims,
        vec![
            BeadPreclaimRollbackWire {
                bead_id: p1.id.clone(),
                status: StatusWire::InProgress,
                assignee: "previous".to_string(),
            },
            BeadPreclaimRollbackWire {
                bead_id: p2.id.clone(),
                status: StatusWire::Open,
                assignee: String::new(),
            },
            BeadPreclaimRollbackWire {
                bead_id: epic.id.clone(),
                status: StatusWire::Open,
                assignee: String::new(),
            },
        ]
    );

    let store = MutableStore::load(&beads_dir).unwrap();
    let updated_epic = store.get_issue(&epic.id).unwrap();
    assert_eq!(updated_epic.status, StatusWire::InProgress);
    assert_eq!(updated_epic.assignee, "land-agent");
    assert_eq!(updated_epic.updated_at, "2026-01-01T00:04:00Z");
    let updated_p1 = store.get_issue(&p1.id).unwrap();
    assert_eq!(updated_p1.status, StatusWire::InProgress);
    assert_eq!(updated_p1.assignee, "agent-1");
    assert_eq!(updated_p1.updated_at, "2026-01-01T00:04:00Z");
    let updated_p2 = store.get_issue(&p2.id).unwrap();
    assert_eq!(updated_p2.status, StatusWire::InProgress);
    assert_eq!(updated_p2.assignee, "agent-2");
}

#[test]
fn preclaim_epic_work_plan_validation_is_all_or_nothing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let p1 = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "P1".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let p2 = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "P2".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    close_issues(
        &beads_dir,
        std::slice::from_ref(&p2.id),
        Some("done".to_string()),
        None,
        false,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();

    let err = preclaim_epic_work_plan(
        &beads_dir,
        &epic.id,
        &[
            BeadPreclaimAssignmentWire {
                bead_id: p1.id.clone(),
                agent_name: "agent-1".to_string(),
            },
            BeadPreclaimAssignmentWire {
                bead_id: p2.id.clone(),
                agent_name: "agent-2".to_string(),
            },
        ],
        None,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap_err();

    assert!(err.message.contains("preclaim target is closed"));
    let store = MutableStore::load(&beads_dir).unwrap();
    let unchanged_p1 = store.get_issue(&p1.id).unwrap();
    assert_eq!(unchanged_p1.status, StatusWire::Open);
    assert_eq!(unchanged_p1.assignee, "");
    assert_eq!(store.get_issue(&p2.id).unwrap().status, StatusWire::Closed);
}
