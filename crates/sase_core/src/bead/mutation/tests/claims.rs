use super::super::*;
use super::support::dual_tempdir as tempdir;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

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
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
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
            &BeadLaunchClaimContextWire::default(),
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
            &BeadLaunchClaimContextWire::default(),
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
            &BeadLaunchClaimContextWire::default(),
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
            &BeadLaunchClaimContextWire::default(),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(reassigned.status, StatusWire::InProgress);
        assert_eq!(reassigned.assignee, "agent-2");
        assert_eq!(reassigned.updated_at, "2026-01-01T00:03:00Z");
        export_jsonl(&beads_dir).unwrap();
        let projection =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(projection.contains(r#""assignee":"agent-2""#));
        assert!(projection.contains(r#""updated_at":"2026-01-01T00:03:00Z""#));
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_launch_rejects_missing_closed_and_blank_requests() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
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

        let missing = claim_for_agent_launch(
            &beads_dir,
            "sase-missing",
            "agent",
            None,
            &BeadLaunchClaimContextWire::default(),
        )
        .unwrap_err();
        assert_eq!(missing.kind, "not_found");
        assert!(missing.message.contains("sase-missing"));

        let closed = claim_for_agent_launch(
            &beads_dir,
            "sase-1",
            "agent",
            None,
            &BeadLaunchClaimContextWire::default(),
        )
        .unwrap_err();
        assert_eq!(closed.kind, "closed");
        assert!(closed.message.contains("closed bead"));

        for agent_name in ["", "  \t"] {
            let invalid = claim_for_agent_launch(
                &beads_dir,
                "sase-1",
                agent_name,
                None,
                &BeadLaunchClaimContextWire::default(),
            )
            .unwrap_err();
            assert_eq!(invalid.kind, "validation");
            assert!(invalid.message.contains("cannot be empty or blank"));
        }
        assert_dual_mode_parity_for_current_mode();
    });
}

fn launch_ctx(
    submitted_at: &str,
    run_id: Option<&str>,
) -> BeadLaunchClaimContextWire {
    BeadLaunchClaimContextWire {
        submitted_at: Some(submitted_at.to_string()),
        run_id: run_id.map(str::to_string),
    }
}

fn closed_phase_under_closed_epic(
    close_reason: &str,
) -> (tempfile::TempDir, PathBuf, IssueWire, IssueWire) {
    let temp = tempdir();
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
    close_issues(
        &beads_dir,
        std::slice::from_ref(&phase.id),
        Some(close_reason.to_string()),
        Some(BeadResolutionWire::Done),
        false,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    close_issues(
        &beads_dir,
        std::slice::from_ref(&epic.id),
        Some("epic done".to_string()),
        Some(BeadResolutionWire::Done),
        false,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    let store = MutableStore::load(&beads_dir).unwrap();
    let phase = store.get_issue(&phase.id).unwrap().clone();
    let epic = store.get_issue(&epic.id).unwrap().clone();
    (temp, beads_dir, phase, epic)
}

fn closed_launch_notes(
    issue: &IssueWire,
) -> Vec<&crate::bead::wire::BeadNoteWire> {
    issue
        .notes
        .iter()
        .filter(|note| note.text.starts_with(CLOSED_BEAD_LAUNCH_NOTE_PREFIX))
        .collect()
}

#[test]
fn closed_bead_launch_note_format_includes_and_drops_run_id() {
    let (_temp, _beads_dir, mut issue, _epic) =
        closed_phase_under_closed_epic("phase done");
    issue.closed_at = Some("2026-10-16T14:02:11Z".to_string());
    issue.resolution = Some(BeadResolutionWire::Done);
    let with_run = closed_bead_launch_note(
        &issue,
        "worker",
        &launch_ctx("2026-10-16T13:58:40Z", Some("20261016_135840")),
    );
    assert_eq!(
        with_run,
        "CLOSED-BEAD LAUNCH: agent worker launched against this closed bead; status left closed.\nLaunch submitted 2026-10-16T13:58:40Z; run 20261016_135840.\nPrior close: 2026-10-16T14:02:11Z · done."
    );
    let without_run = closed_bead_launch_note(
        &issue,
        "worker",
        &launch_ctx("2026-10-16T13:58:40Z", None),
    );
    assert_eq!(
        without_run,
        "CLOSED-BEAD LAUNCH: agent worker launched against this closed bead; status left closed.\nLaunch submitted 2026-10-16T13:58:40Z.\nPrior close: 2026-10-16T14:02:11Z · done."
    );
    issue.closed_at = None;
    issue.resolution = None;
    let unknown = closed_bead_launch_note(
        &issue,
        "worker",
        &launch_ctx("2026-10-16T13:58:40Z", None),
    );
    assert!(unknown.contains("Prior close: unknown · unknown."));
}

#[test]
fn claim_for_agent_launch_attaches_to_bead_closed_before_submission() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, closed_phase, closed_epic) =
            closed_phase_under_closed_epic("phase done");
        let launch =
            launch_ctx("2026-01-01T00:04:00Z", Some("20260101_000400"));
        let outcome = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
            &launch,
        )
        .unwrap();
        let attached = outcome.issue.as_ref().unwrap();
        assert_eq!(outcome.operation, "claim_for_agent_launch");
        assert!(outcome.changed);
        assert_eq!(outcome.noted_ids, vec![closed_phase.id.clone()]);
        assert_eq!(
            outcome.message,
            format!(
                "attached to closed bead {}; status left closed",
                closed_phase.id
            )
        );
        assert_eq!(attached.status, StatusWire::Closed);
        assert_eq!(attached.assignee, closed_phase.assignee);
        assert_eq!(attached.resolution, closed_phase.resolution);
        assert_eq!(attached.close_reason, closed_phase.close_reason);
        assert_eq!(attached.closed_at, closed_phase.closed_at);
        assert_eq!(attached.close_history, closed_phase.close_history);
        assert_eq!(attached.updated_at, "2026-01-01T00:05:00Z");
        let notes = closed_launch_notes(attached);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].author, "agent-1");
        assert_eq!(notes[0].timestamp, "2026-01-01T00:05:00Z");
        assert_eq!(
            notes[0].text,
            closed_bead_launch_note(&closed_phase, "agent-1", &launch)
        );

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let note_event = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .rfind(|event| {
                event.issue_id == closed_phase.id
                    && event.operation == BeadEventOperationWire::NoteAppended
            })
            .unwrap();
        assert_eq!(note_event.actor, "agent-1");
        assert_eq!(note_event.timestamp, "2026-01-01T00:05:00Z");

        let store = MutableStore::load(&beads_dir).unwrap();
        let epic_after = store.get_issue(&closed_epic.id).unwrap().clone();
        assert_eq!(epic_after, closed_epic);

        let reduced = reduces_to_store(&beads_dir);
        let reduced_phase = reduced
            .iter()
            .find(|issue| issue.id == closed_phase.id)
            .unwrap();
        assert_eq!(reduced_phase, attached);
        assert_reprojection_byte_stable(&beads_dir, "closed-bead attach");
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_launch_appends_a_note_on_every_closed_attach() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, closed_phase, _epic) =
            closed_phase_under_closed_epic("phase done");
        let first = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
            &launch_ctx("2026-01-01T00:04:00Z", Some("run-a")),
        )
        .unwrap()
        .issue
        .unwrap();
        let second = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-2",
            Some("2026-01-01T00:06:00Z".to_string()),
            &launch_ctx("2026-01-01T00:05:30Z", Some("run-b")),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(closed_launch_notes(&first).len(), 1);
        assert_eq!(closed_launch_notes(&second).len(), 2);
        assert_eq!(second.status, StatusWire::Closed);
        assert_eq!(second.closed_at, closed_phase.closed_at);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_launch_refuses_close_after_submission_without_writes() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, closed_phase, _epic) =
            closed_phase_under_closed_epic("phase done");
        let before = persisted_claim_state(&beads_dir);
        let error = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
            &launch_ctx("2026-01-01T00:01:30Z", Some("run-queued")),
        )
        .unwrap_err();
        assert_eq!(error.kind, "closed_while_queued");
        assert!(error.message.contains(&closed_phase.id));
        assert!(error.message.contains("2026-01-01T00:02:00Z"));
        assert!(error.message.contains("2026-01-01T00:01:30Z"));
        assert!(error.message.contains("done: phase done"));
        assert!(error.message.contains(
            "relaunch to run it against the closed bead deliberately"
        ));
        assert_eq!(persisted_claim_state(&beads_dir), before);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_launch_attaches_when_close_and_submission_timestamps_match()
{
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, closed_phase, _epic) =
            closed_phase_under_closed_epic("phase done");
        let outcome = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
            &launch_ctx("2026-01-01T00:02:00Z", None),
        )
        .unwrap();
        assert!(outcome.changed);
        let attached = outcome.issue.unwrap();
        assert_eq!(attached.status, StatusWire::Closed);
        assert_eq!(closed_launch_notes(&attached).len(), 1);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_launch_rejects_invalid_submitted_at_before_writing() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, closed_phase, _epic) =
            closed_phase_under_closed_epic("phase done");
        let before = persisted_claim_state(&beads_dir);
        let error = claim_for_agent_launch(
            &beads_dir,
            &closed_phase.id,
            "agent-1",
            Some("2026-01-01T00:05:00Z".to_string()),
            &BeadLaunchClaimContextWire {
                submitted_at: Some("not-rfc3339".to_string()),
                run_id: None,
            },
        )
        .unwrap_err();
        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("submitted_at"));
        assert_eq!(persisted_claim_state(&beads_dir), before);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_wait_claims_open_and_is_idempotent_for_same_agent() {
    run_dual_mode_test(|_mode| {
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
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn claim_for_agent_wait_declines_other_claims_and_terminal_states_without_writes(
) {
    run_dual_mode_test(|_mode| {
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
            &BeadLaunchClaimContextWire::default(),
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
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn release_agent_claim_is_owner_guarded_and_round_trips_to_open() {
    run_dual_mode_test(|_mode| {
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
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn release_agent_claim_declines_in_progress_and_closed_without_writes() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        claim_for_agent_launch(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
            &BeadLaunchClaimContextWire::default(),
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
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn agent_claim_mutations_reject_missing_and_blank_requests() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, _phase_id) = claim_mutation_fixture();

        for mutation in [
            claim_for_agent_wait
                as fn(
                    &Path,
                    &str,
                    &str,
                    Option<String>,
                )
                    -> Result<BeadMutationOutcomeWire, BeadError>,
            release_agent_claim,
        ] {
            let missing = mutation(&beads_dir, "sase-missing", "agent", None)
                .unwrap_err();
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
        assert_dual_mode_parity_for_current_mode();
    });
}

// lock/contention test runs once by design.
#[test]
fn bead_mutation_lock_contention_times_out() {
    let temp = tempdir();
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
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
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
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn preclaim_epic_work_plan_validation_is_all_or_nothing() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
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
        assert_dual_mode_parity_for_current_mode();
    });
}
