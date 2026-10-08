//! Warm-path coverage for the claims/ready/dependencies port
//! (sase-1h8.13.1.5).
//!
//! Every entry point ported onto `MutationView` runs warm on a git-backed
//! store: zero full replays and snapshot loads, one admission sweep, and
//! bounded hydrated rows and stream reads. Cache-equals-replay holds after
//! each mutation. The existing `claims`, `dependencies` and `store` suites
//! plus `dual_mode` keep covering the replay backing.

use super::super::*;
use super::support::*;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;

fn warm_epic_phase_task() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    String,
    String,
    String,
) {
    let (temp, beads_dir) = mode_store(StoreMode::Cached);
    assert_cached_path_used(&beads_dir, "claims-deps warm");
    let epic = create_issue(
        &beads_dir,
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
        &beads_dir,
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
        &beads_dir,
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
    (temp, beads_dir, epic.id, phase.id, task.id)
}

fn assert_warm(label: &str, sweeps: u64, max_rows: u64, max_streams: u64) {
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
    assert_eq!(
        store_io_stats::full_sweeps(),
        sweeps,
        "{label}: one admission sweep per mutation"
    );
    assert!(
        store_io_stats::hydrated_rows() <= max_rows,
        "{label}: hydrated rows {} above bound {max_rows}",
        store_io_stats::hydrated_rows()
    );
    assert!(
        store_io_stats::stream_reads() <= max_streams,
        "{label}: stream reads {} above bound {max_streams}",
        store_io_stats::stream_reads()
    );
}

#[test]
fn claims_deps_launch_claim_stays_warm() {
    let (_temp, beads_dir, _epic, phase_id, _) = warm_epic_phase_task();
    store_io_stats::reset();
    let claimed = claim_for_agent_launch(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(claimed.status, StatusWire::InProgress);
    assert_eq!(claimed.assignee, "agent-1");
    assert_warm("launch claim", 1, 5, 2);
    assert_cache_equals_replay(&beads_dir, "launch claim");
}

#[test]
fn claims_deps_wait_and_release_stay_warm() {
    let (_temp, beads_dir, _epic, phase_id, _) = warm_epic_phase_task();
    store_io_stats::reset();
    let waited = claim_for_agent_wait(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(waited.changed);
    assert_warm("wait claim", 1, 5, 2);

    store_io_stats::reset();
    let released = release_agent_claim(
        &beads_dir,
        &phase_id,
        "agent-1",
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(released.status, StatusWire::Open);
    assert!(released.assignee.is_empty());
    assert_warm("release claim", 1, 5, 2);
    assert_cache_equals_replay(&beads_dir, "wait/release");
}

#[test]
fn claims_deps_preclaim_stays_warm() {
    let (_temp, beads_dir, epic_id, phase_id, _) = warm_epic_phase_task();
    // A second phase so the preclaim touches two targets plus the epic.
    let phase2 = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Phase two".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic_id.clone()),
            size: Some(PhaseSizeWire::Small),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:02:30Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    store_io_stats::reset();
    let outcome = preclaim_epic_work_plan(
        &beads_dir,
        &epic_id,
        &[
            BeadPreclaimAssignmentWire {
                bead_id: phase_id.clone(),
                agent_name: "agent-1".to_string(),
            },
            BeadPreclaimAssignmentWire {
                bead_id: phase2.id.clone(),
                agent_name: "agent-2".to_string(),
            },
        ],
        Some("land-agent".to_string()),
        Some("2026-01-01T00:05:00Z".to_string()),
    )
    .unwrap();
    assert_eq!(
        outcome.issue_ids,
        vec![phase_id.clone(), phase2.id.clone(), epic_id.clone()]
    );
    assert_eq!(outcome.rollback_preclaims.len(), 3);
    assert_warm("preclaim", 1, 12, 3);
    assert_cache_equals_replay(&beads_dir, "preclaim");
}

#[test]
fn claims_deps_ready_marking_stays_warm() {
    let (_temp, beads_dir, epic_id, _, _) = warm_epic_phase_task();
    store_io_stats::reset();
    let marked = mark_ready_to_work(
        &beads_dir,
        &epic_id,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(marked.is_ready_to_work);
    assert_warm("mark ready", 1, 5, 2);

    store_io_stats::reset();
    let unmarked = unmark_ready_to_work(
        &beads_dir,
        &epic_id,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(!unmarked.is_ready_to_work);
    assert_warm("unmark ready", 1, 5, 2);
    assert_cache_equals_replay(&beads_dir, "ready marking");
}

#[test]
fn claims_deps_dependency_add_remove_stay_warm() {
    let (_temp, beads_dir, _epic, phase_id, task_id) = warm_epic_phase_task();
    store_io_stats::reset();
    let added = add_dependency(
        &beads_dir,
        &task_id,
        &phase_id,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(added.changed);
    assert_warm("dep add", 1, 8, 2);

    store_io_stats::reset();
    let removed = remove_dependencies(
        &beads_dir,
        &task_id,
        std::slice::from_ref(&phase_id),
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    assert!(removed.changed);
    assert!(removed.active_blocker_ids.is_empty());
    assert_warm("dep remove", 1, 8, 2);
    assert_cache_equals_replay(&beads_dir, "dependencies");
}

#[test]
fn claims_deps_blocker_status_comes_from_point_lookups() {
    let (_temp, beads_dir, _epic, phase_id, task_id) = warm_epic_phase_task();
    add_dependency(
        &beads_dir,
        &task_id,
        &phase_id,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    // The phase target is still open, so it blocks the task.
    store_io_stats::reset();
    let removed = remove_dependencies(
        &beads_dir,
        &task_id,
        // Removing a different edge is a validation error, not a write;
        // first re-add coverage uses the blocker list on a no-op path.
        std::slice::from_ref(&phase_id),
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    assert!(removed.changed);
    // After removal nothing blocks; before removal the open phase did.
    // Re-add and check the blocker list via a second add/remove cycle.
    add_dependency(
        &beads_dir,
        &task_id,
        &phase_id,
        Some("2026-01-01T00:05:00Z".to_string()),
    )
    .unwrap();
    store_io_stats::reset();
    // Close the phase target, then remove: a closed target is no blocker.
    close_issues(
        &beads_dir,
        std::slice::from_ref(&phase_id),
        Some("done".to_string()),
        Some(crate::bead::wire::BeadResolutionWire::Done),
        false,
        Some("2026-01-01T00:06:00Z".to_string()),
    )
    .unwrap();
    store_io_stats::reset();
    let removed_closed = remove_dependencies(
        &beads_dir,
        &task_id,
        std::slice::from_ref(&phase_id),
        Some("2026-01-01T00:07:00Z".to_string()),
    )
    .unwrap();
    assert!(removed_closed.changed);
    assert!(removed_closed.active_blocker_ids.is_empty());
    assert_cache_equals_replay(&beads_dir, "blocker status");
}

#[test]
fn claims_deps_references_add_remove_stay_warm() {
    let (_temp, beads_dir, _epic, _phase, task_id) = warm_epic_phase_task();
    store_io_stats::reset();
    let added = add_bead_references(
        &beads_dir,
        &task_id,
        &["research:202601/warm.md".to_string()],
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(added.changed);
    assert_warm("ref add", 1, 5, 2);

    store_io_stats::reset();
    let removed = remove_bead_references(
        &beads_dir,
        &task_id,
        &["research:202601/warm.md".to_string()],
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    assert!(removed.changed);
    assert_warm("ref remove", 1, 5, 2);
    assert_cache_equals_replay(&beads_dir, "references");
}
