//! Lifecycle warm-path coverage: open, close and remove over the
//! mutation view (sase-1h8.13.1.4).
//!
//! Every lifecycle entry point runs warm (zero full replays and snapshot
//! loads, hydration and stream reads bounded by the affected set) and
//! matches replay. A dual-mode test proves the cached and replay backings
//! converge on one lifecycle scenario.

use super::super::*;
use super::support::*;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::read_model::{
    ensure_cache_ready_for_mutation_at, read_model_cache_path_for_store,
};
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::path::Path;

fn warm_store() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    // Seed one row before arming the cache: an empty store has no
    // streams for the readiness sweep to admit.
    let seed = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Seed".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    (temp, beads_dir, seed)
}

fn make_task(beads_dir: &Path, title: &str, now: &str) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some(now.to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn make_plan(beads_dir: &Path, title: &str, now: &str) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some(now.to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn make_phase(
    beads_dir: &Path,
    title: &str,
    parent: &str,
    now: &str,
) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(parent.to_string()),
            size: Some(PhaseSizeWire::Small),
            now: Some(now.to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn close_one(
    beads_dir: &Path,
    issue_id: &str,
    reason: &str,
    now: &str,
) -> BeadMutationOutcomeWire {
    close_issues(
        beads_dir,
        std::slice::from_ref(&issue_id.to_string()),
        Some(reason.to_string()),
        None,
        false,
        Some(now.to_string()),
    )
    .unwrap()
}

fn assert_warm(label: &str) {
    assert_eq!(
        store_io_stats::full_replays(),
        0,
        "{label}: cached path must not replay"
    );
    assert_eq!(
        store_io_stats::snapshot_loads(),
        0,
        "{label}: cached path must not snapshot-load"
    );
}

fn assert_bounded(label: &str, max_rows: u64, max_streams: u64) {
    let rows = store_io_stats::hydrated_rows();
    let streams = store_io_stats::stream_reads();
    assert!(
        rows <= max_rows,
        "{label}: hydrated {rows} rows, bound is {max_rows}"
    );
    assert!(
        streams <= max_streams,
        "{label}: read {streams} streams, bound is {max_streams}"
    );
}

fn replay_json(beads_dir: &Path) -> serde_json::Value {
    serde_json::to_value(reduces_to_store(beads_dir)).unwrap()
}

#[test]
fn lifecycle_open_stays_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let task = make_task(&beads_dir, "Task", "2026-01-01T00:00:00Z");
    close_one(&beads_dir, &task, "done", "2026-01-01T00:01:00Z");

    store_io_stats::reset();
    let reopened =
        open_issue(&beads_dir, &task, Some("2026-01-01T00:02:00Z".to_string()))
            .unwrap()
            .issue
            .unwrap();
    assert_eq!(reopened.id, task);
    assert_eq!(reopened.status, StatusWire::Open);
    // The close is archived into history with the open as its cause.
    assert_eq!(reopened.close_history.len(), 1);
    assert_eq!(
        reopened.close_history[0].close_reason.as_deref(),
        Some("done")
    );
    assert_warm("open");
    assert_bounded("open", 4, 2);
    assert_cache_equals_replay(&beads_dir, "open");
}

#[test]
fn lifecycle_open_reopens_closed_ancestors_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let phase = make_phase(&beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");
    close_one(&beads_dir, &phase, "done", "2026-01-01T00:02:00Z");
    close_one(&beads_dir, &epic, "done", "2026-01-01T00:03:00Z");

    store_io_stats::reset();
    let outcome = open_issue(
        &beads_dir,
        &phase,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    assert_eq!(outcome.reopened_ancestor_ids, vec![epic.clone()]);
    assert_warm("open with ancestors");
    assert_bounded("open with ancestors", 8, 4);
    let issues = reduces_to_store(&beads_dir);
    for issue_id in [&phase, &epic] {
        let reopened =
            issues.iter().find(|issue| &issue.id == issue_id).unwrap();
        assert_eq!(reopened.status, StatusWire::Open, "{issue_id}");
        assert_eq!(reopened.close_history.len(), 1, "{issue_id}");
    }
    assert_cache_equals_replay(&beads_dir, "open with ancestors");
}

#[test]
fn lifecycle_close_single_stays_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let task = make_task(&beads_dir, "Task", "2026-01-01T00:00:00Z");

    store_io_stats::reset();
    let outcome = close_issues(
        &beads_dir,
        std::slice::from_ref(&task),
        Some("verified".to_string()),
        Some(BeadResolutionWire::Done),
        false,
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.closed_ids, vec![task.clone()]);
    assert!(outcome.cascade_closed_ids.is_empty());
    let closed = outcome.issues[0].clone();
    assert_eq!(closed.status, StatusWire::Closed);
    assert_eq!(closed.close_reason.as_deref(), Some("verified"));
    assert_eq!(closed.resolution, Some(BeadResolutionWire::Done));
    assert_warm("close");
    assert_bounded("close", 4, 2);
    assert_cache_equals_replay(&beads_dir, "close");
}

#[test]
fn lifecycle_close_guard_rejects_open_descendant_without_writing_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let phase = make_phase(&beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");
    let before = persisted_stream_files(&beads_dir);

    store_io_stats::reset();
    let error = close_issues(
        &beads_dir,
        std::slice::from_ref(&epic),
        None,
        None,
        false,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap_err();
    assert!(error.message.contains(&format!("cannot close {epic}")));
    assert!(error.message.contains(&phase));
    assert_warm("close guard");
    assert_eq!(
        persisted_stream_files(&beads_dir),
        before,
        "a rejected close writes no events"
    );
    assert_cache_equals_replay(&beads_dir, "close guard");
}

#[test]
fn lifecycle_close_batch_with_note_stays_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let first = make_task(&beads_dir, "First", "2026-01-01T00:00:00Z");
    let second = make_task(&beads_dir, "Second", "2026-01-01T00:01:00Z");

    store_io_stats::reset();
    let outcome = close_issues_with_note(
        &beads_dir,
        &[first.clone(), second.clone()],
        Some("verified together".to_string()),
        None,
        false,
        Some("closing evidence".to_string()),
        Some("closer-agent".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.closed_ids, vec![first.clone(), second.clone()]);
    assert_eq!(outcome.noted_ids.len(), 2);
    assert!(outcome.noted_ids.contains(&first));
    assert!(outcome.noted_ids.contains(&second));
    for issue in &outcome.issues {
        assert_eq!(issue.status, StatusWire::Closed);
        assert!(note_text(issue).contains("closing evidence"));
    }
    assert_warm("batch close with note");
    assert_bounded("batch close with note", 8, 4);
    assert_cache_equals_replay(&beads_dir, "batch close with note");
}

#[test]
fn lifecycle_forced_close_sweeps_descendants_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let phase = make_phase(&beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");

    store_io_stats::reset();
    let outcome = close_issues(
        &beads_dir,
        std::slice::from_ref(&epic),
        Some("Canceled unfinished work".to_string()),
        Some(BeadResolutionWire::Canceled),
        true,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    assert_eq!(outcome.closed_ids, vec![phase.clone(), epic.clone()]);
    assert_eq!(outcome.cascade_closed_ids, vec![phase.clone()]);
    let forced = outcome
        .issues
        .iter()
        .find(|issue| issue.id == phase)
        .unwrap();
    assert_eq!(
        forced.close_reason.as_deref(),
        Some(format!("forced by {epic}: Canceled unfinished work").as_str())
    );
    assert_warm("forced close");
    assert_bounded("forced close", 8, 4);
    assert_cache_equals_replay(&beads_dir, "forced close");
}

#[test]
fn lifecycle_delegated_parent_completion_stays_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let phase = make_phase(&beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");
    let child_epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Child epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            parent_id: Some(phase.clone()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;

    store_io_stats::reset();
    let outcome =
        close_one(&beads_dir, &child_epic, "landed", "2026-01-01T00:03:00Z");
    assert_eq!(outcome.closed_ids, vec![child_epic.clone(), phase.clone()]);
    assert_eq!(outcome.cascade_closed_ids, vec![phase.clone()]);
    assert_warm("delegated completion");
    assert_bounded("delegated completion", 8, 4);
    let issues = reduces_to_store(&beads_dir);
    let parent = issues.iter().find(|issue| issue.id == phase).unwrap();
    assert_eq!(parent.status, StatusWire::Closed);
    assert_eq!(
        parent.close_reason.as_deref(),
        Some("delegated work landed")
    );
    assert_eq!(
        issues.iter().find(|issue| issue.id == epic).unwrap().status,
        StatusWire::Open
    );
    assert_cache_equals_replay(&beads_dir, "delegated completion");
}

#[test]
fn lifecycle_repeat_close_writes_nothing_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let task = make_task(&beads_dir, "Task", "2026-01-01T00:00:00Z");
    close_one(&beads_dir, &task, "verified", "2026-01-01T00:01:00Z");
    let before = persisted_stream_files(&beads_dir);

    store_io_stats::reset();
    let outcome = close_issues(
        &beads_dir,
        std::slice::from_ref(&task),
        None,
        None,
        false,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    assert!(!outcome.changed);
    assert!(outcome.closed_ids.is_empty());
    assert_eq!(outcome.already_closed_ids, vec![task.clone()]);
    assert_eq!(outcome.message, "all requested issues were already closed");
    assert_warm("repeat close");
    assert_eq!(
        persisted_stream_files(&beads_dir),
        before,
        "a repeat close writes no events"
    );
    assert_cache_equals_replay(&beads_dir, "repeat close");
}

#[test]
fn lifecycle_remove_cascade_stays_warm() {
    let (_temp, beads_dir, seed) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let _phase = make_phase(&beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");
    let lone = make_task(&beads_dir, "Lone", "2026-01-01T00:02:00Z");

    store_io_stats::reset();
    let outcome =
        remove_issues(&beads_dir, std::slice::from_ref(&epic)).unwrap();
    assert_eq!(outcome.operation, "rm");
    assert_warm("remove cascade");
    assert_bounded("remove cascade", 8, 4);
    let survivors = reduces_to_store(&beads_dir);
    // The cascade removes the epic and its phase child; the seed and
    // the top-level task are not descendants, so both survive.
    let mut survivor_ids: Vec<&str> =
        survivors.iter().map(|issue| issue.id.as_str()).collect();
    survivor_ids.sort_unstable();
    let mut expected = vec![seed.as_str(), lone.as_str()];
    expected.sort_unstable();
    assert_eq!(survivor_ids, expected);
    assert_cache_equals_replay(&beads_dir, "remove cascade");
}

#[test]
fn lifecycle_remove_cleans_survivor_dependencies_warm() {
    let (_temp, beads_dir, _) = warm_store();
    let first = make_plan(&beads_dir, "First", "2026-01-01T00:00:00Z");
    let second = make_plan(&beads_dir, "Second", "2026-01-01T00:01:00Z");
    add_dependency(
        &beads_dir,
        &second,
        &first,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    store_io_stats::reset();
    remove_issues(&beads_dir, std::slice::from_ref(&first)).unwrap();
    assert_warm("remove dependency cleanup");
    assert_bounded("remove dependency cleanup", 8, 4);
    let survivors = reduces_to_store(&beads_dir);
    // The seed plan plus the dependent survive; the dependent's edge to
    // the removed bead is stripped.
    assert_eq!(survivors.len(), 2);
    let dependent = survivors.iter().find(|issue| issue.id == second).unwrap();
    assert!(
        dependent.dependencies.is_empty(),
        "survivor dependencies must drop the removed bead"
    );
    assert_cache_equals_replay(&beads_dir, "remove dependency cleanup");
}

#[test]
fn lifecycle_remove_max_suffix_is_reused_warm() {
    // Published deletes update allocation: removing the maximum child
    // suffix frees it for the next child, exactly as the replay oracle
    // reuses it.
    let (_temp, beads_dir, _) = warm_store();
    let epic = make_plan(&beads_dir, "Epic", "2026-01-01T00:00:00Z");
    let first = make_phase(&beads_dir, "First", &epic, "2026-01-01T00:01:00Z");
    let max = make_phase(&beads_dir, "Max", &epic, "2026-01-01T00:02:00Z");

    store_io_stats::reset();
    remove_issues(&beads_dir, std::slice::from_ref(&max)).unwrap();
    assert_warm("remove max suffix");
    let reused =
        make_phase(&beads_dir, "Reused", &epic, "2026-01-01T00:03:00Z");
    assert_eq!(
        reused, max,
        "removing the maximum suffix frees it for reuse"
    );
    assert!(!first.is_empty());
    assert_cache_equals_replay(&beads_dir, "remove max suffix");
}

#[test]
fn lifecycle_dual_mode_converges() {
    // One lifecycle scenario on both backings: the cached side runs the
    // warm view path, the plain side replays, and both converge.
    let (_cached_temp, cached_beads) = mode_store(StoreMode::Cached);
    let (_plain_temp, plain_beads) = mode_store(StoreMode::Replay);
    assert_cached_path_used(&cached_beads, "lifecycle dual-mode");
    assert!(
        read_model_cache_path_for_store(&plain_beads).is_none(),
        "lifecycle dual-mode: plain store must stay on replay"
    );
    for beads_dir in [&cached_beads, &plain_beads] {
        let epic = make_plan(beads_dir, "Epic", "2026-01-01T00:00:00Z");
        let phase =
            make_phase(beads_dir, "Phase", &epic, "2026-01-01T00:01:00Z");
        let task = make_task(beads_dir, "Task", "2026-01-01T00:02:00Z");
        close_one(beads_dir, &task, "done", "2026-01-01T00:03:00Z");
        open_issue(beads_dir, &task, Some("2026-01-01T00:04:00Z".to_string()))
            .unwrap();
        close_issues_with_note(
            beads_dir,
            &[task.clone(), phase.clone()],
            Some("shipped".to_string()),
            None,
            false,
            Some("closing evidence".to_string()),
            Some("agent".to_string()),
            Some("2026-01-01T00:05:00Z".to_string()),
            None,
        )
        .unwrap();
        remove_issues(beads_dir, std::slice::from_ref(&task)).unwrap();
        let survivors = reduces_to_store(beads_dir);
        assert_eq!(survivors.len(), 2);
    }

    // The cached side ran every lifecycle op above through the view
    // path; now prove one more close+open stays warm there, then replay
    // the same tail on the plain side so both backings converge again.
    // Counter assertions only cover the cached ops: the plain side
    // replays by design.
    store_io_stats::reset();
    let cached_extra =
        make_task(&cached_beads, "Extra", "2026-01-01T00:06:00Z");
    close_one(&cached_beads, &cached_extra, "done", "2026-01-01T00:07:00Z");
    open_issue(
        &cached_beads,
        &cached_extra,
        Some("2026-01-01T00:08:00Z".to_string()),
    )
    .unwrap();
    assert_warm("lifecycle dual-mode warm tail");
    let plain_extra = make_task(&plain_beads, "Extra", "2026-01-01T00:06:00Z");
    close_one(&plain_beads, &plain_extra, "done", "2026-01-01T00:07:00Z");
    open_issue(
        &plain_beads,
        &plain_extra,
        Some("2026-01-01T00:08:00Z".to_string()),
    )
    .unwrap();
    assert_eq!(cached_extra, plain_extra);
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "lifecycle dual-mode");
}
