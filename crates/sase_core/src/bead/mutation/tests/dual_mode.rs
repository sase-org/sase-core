//! Dual-mode coverage for every mutation family (sase-1h8.13.1.2).
//!
//! Each test below runs the same scenario against a git-backed cached store
//! and a plain replay store through the [`StoreMode`] fixture in
//! `support.rs`, then asserts the two backings converged: identical replayed
//! state on both sides, and cache-equals-replay on the cached side. The
//! already-cached create/note/update paths additionally assert zero full
//! replays; families that are still replay-only pass through their replay
//! fallback until ported, which is expected. Lock and contention tests stay
//! in their own suites and are not duplicated here.

use super::super::*;
use super::support::*;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::path::Path;

fn replay_json(beads_dir: &Path) -> serde_json::Value {
    serde_json::to_value(reduces_to_store(beads_dir)).unwrap()
}

fn dual_stores() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let (cached_temp, cached_beads) = mode_store(StoreMode::Cached);
    let (plain_temp, plain_beads) = mode_store(StoreMode::Replay);
    assert_cached_path_used(&cached_beads, "dual-mode");
    assert!(
        read_model_cache_path_for_store(&plain_beads).is_none(),
        "dual-mode: plain store must stay on replay"
    );
    (cached_temp, cached_beads, plain_temp, plain_beads)
}

/// An epic plan with one phase child and one task, seeded identically on
/// both backings so the same scenario mints the same IDs twice.
fn seed_epic_phase_task(beads_dir: &Path) -> (String, String, String) {
    let epic = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let phase = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            size: Some(PhaseSizeWire::Small),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let task = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (epic.id, phase.id, task.id)
}

#[test]
fn dual_mode_create_matches_replay_without_full_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    store_io_stats::reset();
    let cached_child = create_issue(
        &cached_beads,
        BeadCreateRequestWire {
            title: "Dual plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let cached_child = create_issue(
        &cached_beads,
        BeadCreateRequestWire {
            title: "Dual phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(cached_child.id.clone()),
            size: Some(PhaseSizeWire::Small),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_no_full_replay("create");
    let plain_plan = create_issue(
        &plain_beads,
        BeadCreateRequestWire {
            title: "Dual plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let plain_child = create_issue(
        &plain_beads,
        BeadCreateRequestWire {
            title: "Dual phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(plain_plan.id.clone()),
            size: Some(PhaseSizeWire::Small),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(cached_child.id, plain_child.id);
    assert_eq!(cached_child.title, plain_child.title);
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "create");
}

#[test]
fn dual_mode_note_append_and_update_match_replay_without_full_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    let (_, _, cached_task) = seed_epic_phase_task(&cached_beads);
    let (_, _, plain_task) = seed_epic_phase_task(&plain_beads);
    assert_eq!(cached_task, plain_task);

    store_io_stats::reset();
    append_issue_note(
        &cached_beads,
        &cached_task,
        "dual-mode note",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    update_issue(
        &cached_beads,
        &cached_task,
        BeadUpdateFieldsWire {
            title: Some("Renamed task".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_no_full_replay("note/update");

    append_issue_note(
        &plain_beads,
        &plain_task,
        "dual-mode note",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    update_issue(
        &plain_beads,
        &plain_task,
        BeadUpdateFieldsWire {
            title: Some("Renamed task".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let cached_issues = reduces_to_store(&cached_beads);
    let renamed = cached_issues
        .iter()
        .find(|issue| issue.id == cached_task)
        .unwrap();
    assert_eq!(renamed.title, "Renamed task");
    assert!(note_text(renamed).contains("dual-mode note"));
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "note/update");
}

#[test]
fn dual_mode_note_edit_and_remove_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (_, _, task_id) = seed_epic_phase_task(beads_dir);
        append_issue_note(
            beads_dir,
            &task_id,
            "original note",
            Some("agent".to_string()),
            Some("2026-01-01T00:03:00Z".to_string()),
            None,
        )
        .unwrap();
        let note_id = reduces_to_store(beads_dir)
            .into_iter()
            .find(|issue| issue.id == task_id)
            .unwrap()
            .notes[0]
            .id
            .clone();
        let edited = edit_issue_note(
            beads_dir,
            &task_id,
            &note_id,
            "edited note",
            Some("agent".to_string()),
            Some("2026-01-01T00:04:00Z".to_string()),
            None,
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(edited.notes[0].id, note_id);
        assert!(note_text(&edited).contains("edited note"));
        remove_issue_note(
            beads_dir,
            &task_id,
            &note_id,
            Some("agent".to_string()),
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap();
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "note edit/remove");
}

#[test]
fn dual_mode_close_and_open_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (_, _, task_id) = seed_epic_phase_task(beads_dir);
        let closed = close_issues(
            beads_dir,
            std::slice::from_ref(&task_id),
            Some("done".to_string()),
            Some(BeadResolutionWire::Done),
            false,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(closed.changed);
        let reopened = open_issue(
            beads_dir,
            &task_id,
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(reopened.id, task_id);
        assert_eq!(reopened.status, crate::bead::wire::StatusWire::Open);
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "close/open");
}

#[test]
fn dual_mode_remove_cascade_matches_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    let (cached_epic, _, cached_task) = seed_epic_phase_task(&cached_beads);
    let (plain_epic, _, plain_task) = seed_epic_phase_task(&plain_beads);
    assert_eq!(cached_epic, plain_epic);
    let cached_outcome =
        remove_issues(&cached_beads, std::slice::from_ref(&cached_epic))
            .unwrap();
    let plain_outcome =
        remove_issues(&plain_beads, std::slice::from_ref(&plain_epic)).unwrap();
    assert_eq!(cached_outcome.issue_ids, plain_outcome.issue_ids);
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    // The cascade removes the epic and its phase child; the top-level task
    // is not a descendant, so it survives on both backings.
    let survivors = reduces_to_store(&cached_beads);
    assert_eq!(
        survivors.iter().map(|issue| &issue.id).collect::<Vec<_>>(),
        vec![&cached_task]
    );
    assert_eq!(cached_task, plain_task);
    assert_cache_equals_replay(&cached_beads, "remove cascade");
}

#[test]
fn dual_mode_claims_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (epic_id, phase_id, _) = seed_epic_phase_task(beads_dir);
        let waited = claim_for_agent_wait(
            beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(waited.changed);
        let released = release_agent_claim(
            beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        assert!(released.changed);
        let launched = claim_for_agent_launch(
            beads_dir,
            &epic_id,
            "agent-2",
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(launched.assignee, "agent-2");
        assert_eq!(launched.status, crate::bead::wire::StatusWire::InProgress);
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "claims");
}

#[test]
fn dual_mode_dependencies_and_references_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (_, phase_id, task_id) = seed_epic_phase_task(beads_dir);
        add_dependency(
            beads_dir,
            &task_id,
            &phase_id,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        add_bead_references(
            beads_dir,
            &task_id,
            &["research:202601/dual-mode.md".to_string()],
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        remove_dependencies(
            beads_dir,
            &task_id,
            std::slice::from_ref(&phase_id),
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap();
        remove_bead_references(
            beads_dir,
            &task_id,
            &["research:202601/dual-mode.md".to_string()],
            Some("2026-01-01T00:06:00Z".to_string()),
        )
        .unwrap();
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "dependencies/references");
}

#[test]
fn dual_mode_links_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (epic_id, _, task_id) = seed_epic_phase_task(beads_dir);
        let target = format!("bead:{epic_id}");
        let added = add_bead_link(
            beads_dir,
            &task_id,
            &target,
            "related",
            "shares the dual-mode root cause",
            ArtifactLinkOriginWire::Manual,
            BeadLinkDirectionWire::Out,
            1,
            Some("2026-01-01T00:03:00Z".to_string()),
            Some("0123456789abcdef0123456789abcdef".to_string()),
        )
        .unwrap();
        assert!(added.changed);
        let removed = remove_bead_link(
            beads_dir,
            &task_id,
            &target,
            Some("related"),
            BeadLinkDirectionWire::Out,
            Some("2026-01-01T00:04:00Z".to_string()),
            Some("fedcba9876543210fedcba9876543210".to_string()),
        )
        .unwrap();
        assert!(removed.changed);
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "links");
}

#[test]
fn dual_mode_plus_one_and_snooze_match_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (_, _, task_id) = seed_epic_phase_task(beads_dir);
        snooze_task(
            beads_dir,
            &task_id,
            "2026-01-04T00:00:00Z",
            Some(2),
            "needs the upstream fix first",
            "owner@example.com",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        add_task_plus_one(
            beads_dir,
            &task_id,
            "reporter-one",
            "hit this too",
            &[],
            Some("2026-01-02T00:00:00Z".to_string()),
            None,
            None,
        )
        .unwrap();
        let canceled = cancel_task_snooze(
            beads_dir,
            &task_id,
            "owner@example.com",
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(canceled.status, crate::bead::wire::StatusWire::Ready);
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "plus-one/snooze");
}

#[test]
fn dual_mode_ready_marking_matches_replay() {
    let (_cached_temp, cached_beads, _plain_temp, plain_beads) = dual_stores();
    for beads_dir in [&cached_beads, &plain_beads] {
        let (epic_id, _, _) = seed_epic_phase_task(beads_dir);
        let marked = mark_ready_to_work(
            beads_dir,
            &epic_id,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert!(marked.is_ready_to_work);
        let unmarked = unmark_ready_to_work(
            beads_dir,
            &epic_id,
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert!(!unmarked.is_ready_to_work);
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "ready marking");
}
