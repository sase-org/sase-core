//! Links-evidence coverage: links, projections, receipts, +1, snooze and
//! cancel on the mutation view (sase-1h8.13.1.6).
//!
//! Every entry point runs warm on a git-backed store: one admission sweep,
//! zero full replays and snapshot loads, bounded hydration and stream reads,
//! and cache-equals-replay. A dual-mode parity test runs the whole family on
//! cached and replay backings and compares the replayed state.

use super::super::*;
use super::support::*;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::read_model::ensure_cache_ready_at;
use crate::bead::read_model::ensure_cache_ready_for_mutation_at;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::read_model::read_model_verify_cache_at;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use tempfile::tempdir;

fn warm_two_plans() -> (tempfile::TempDir, PathBuf, String, String) {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let left = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Left".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let right = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Right".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    (temp, beads_dir, left.id, right.id)
}

fn warm_task() -> (tempfile::TempDir, PathBuf, String) {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Warm task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    (temp, beads_dir, task.id)
}

fn cache_path_for(beads_dir: &Path) -> PathBuf {
    read_model_cache_path_for_store(beads_dir)
        .expect("git-backed store has a cache path")
}

fn assert_cache_matches_replay(
    beads_dir: &Path,
    cache_path: &Path,
    label: &str,
) {
    let report = read_model_verify_cache_at(beads_dir, cache_path);
    assert!(report.compared, "{label}: {}", report.reason);
    assert!(
        report.matched,
        "{label}: differing {:?}",
        report.differing_ids
    );
}

fn assert_token_only_next_read(beads_dir: &Path, sweeps: u64, label: &str) {
    let cache_path = cache_path_for(beads_dir);
    assert!(ensure_cache_ready_at(beads_dir, &cache_path).unwrap());
    assert_eq!(store_io_stats::full_sweeps(), sweeps, "{label}");
    assert_eq!(store_io_stats::snapshot_loads(), 0, "{label}");
}

/// One warm mutation: a single admission sweep, no snapshot load, no full
/// replay, exactly `sig_rows` changed stream signatures and
/// `published_rows` published rows, with hydration and stream reads bounded
/// by the affected set.
#[allow(clippy::too_many_arguments)]
fn assert_warm(
    label: &str,
    sig_rows: u64,
    published_rows: u64,
    max_hydrated: u64,
    max_streams: u64,
) {
    assert_eq!(store_io_stats::full_sweeps(), 1, "{label}: one admission");
    assert_eq!(store_io_stats::snapshot_loads(), 0, "{label}: no snapshot");
    assert_eq!(store_io_stats::full_replays(), 0, "{label}: no replay");
    assert_eq!(
        store_io_stats::sig_rows_written(),
        sig_rows,
        "{label}: signature rows"
    );
    assert_eq!(
        store_io_stats::published_rows(),
        published_rows,
        "{label}: published rows"
    );
    let hydrated = store_io_stats::hydrated_rows();
    assert!(
        hydrated <= max_hydrated,
        "{label}: hydrated {hydrated} rows, more than {max_hydrated}"
    );
    let streams = store_io_stats::stream_reads();
    assert!(
        streams <= max_streams,
        "{label}: {streams} stream reads, more than {max_streams}"
    );
}

#[test]
fn warm_link_add_and_remove_stay_warm() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    let target = format!("bead:{right_id}");
    store_io_stats::reset();
    let added = add_bead_link(
        &beads_dir,
        &left_id,
        &target,
        "related",
        "shares the warm root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        Some("0123456789abcdef0123456789abcdef".to_string()),
    )
    .unwrap();
    assert!(added.changed);
    assert_eq!(added.issue.as_ref().unwrap().links.len(), 1);
    assert_warm("link add", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "link add next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "link add");
    assert_reprojection_byte_stable(&beads_dir, "link add");

    store_io_stats::reset();
    let removed = remove_bead_link(
        &beads_dir,
        &left_id,
        &target,
        Some("related"),
        BeadLinkDirectionWire::Out,
        Some("2026-01-01T00:03:00Z".to_string()),
        Some("fedcba9876543210fedcba9876543210".to_string()),
    )
    .unwrap();
    assert!(removed.changed);
    assert!(removed.issue.as_ref().unwrap().links.is_empty());
    assert_warm("link remove", 1, 1, 6, 3);
    assert_token_only_next_read(&beads_dir, 1, "link remove next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "link remove");
}

#[test]
fn warm_undirected_link_consolidates_onto_one_holder() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    add_bead_link(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        "shares the warm root cause",
        ArtifactLinkOriginWire::Read,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    // Writing the same undirected edge from the other bead updates the bead
    // that already holds it instead of storing a second copy. `Read` edges
    // accumulate `uses`.
    store_io_stats::reset();
    let consolidated = add_bead_link(
        &beads_dir,
        &right_id,
        &format!("bead:{left_id}"),
        "related",
        "shares the warm root cause from the peer side",
        ArtifactLinkOriginWire::Read,
        BeadLinkDirectionWire::Out,
        2,
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(consolidated.changed);
    assert_eq!(
        consolidated.issue_ids,
        vec![right_id.clone(), left_id.clone()]
    );
    assert_warm("undirected consolidate", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_cache_matches_replay(&beads_dir, &cache_path, "consolidate");
    let issues = reduces_to_store(&beads_dir);
    let left = issues.iter().find(|issue| issue.id == left_id).unwrap();
    let right = issues.iter().find(|issue| issue.id == right_id).unwrap();
    assert_eq!(left.links.len(), 1);
    assert_eq!(left.links[0].uses, 3);
    assert!(right.links.is_empty());
}

#[test]
fn warm_link_receipt_replay_writes_nothing() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    let operation_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    add_bead_link(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        "shares the warm root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        Some(operation_id.clone()),
    )
    .unwrap();
    // The same operation ID is idempotent: no new event, no signature or
    // published rows, but the admission sweep still runs.
    store_io_stats::reset();
    let replayed = add_bead_link(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        "shares the warm root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:03:00Z".to_string()),
        Some(operation_id),
    )
    .unwrap();
    assert!(!replayed.changed);
    assert_warm("link receipt replay", 0, 0, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_cache_matches_replay(&beads_dir, &cache_path, "receipt replay");
}

#[test]
fn warm_link_projections_stay_warm() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    let operation_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    store_io_stats::reset();
    let projected = set_bead_link_projection(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("projected warm edge".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        operation_id.clone(),
    )
    .unwrap();
    assert!(projected.changed);
    assert_warm("link project", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "project next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "project");

    // Same operation ID with the same desired state changes nothing and
    // writes nothing.
    store_io_stats::reset();
    let replayed = set_bead_link_projection(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("projected warm edge".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        Some("2026-01-01T00:03:00Z".to_string()),
        operation_id,
    )
    .unwrap();
    assert!(!replayed.changed);
    assert_warm("projection receipt replay", 0, 0, 6, 3);

    // Absent removes the materialized edge with a LinkRemoved event.
    store_io_stats::reset();
    let removed = set_bead_link_projection(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        BeadLinkDirectionWire::Out,
        false,
        None,
        None,
        0,
        Some("2026-01-01T00:04:00Z".to_string()),
        "cccccccccccccccccccccccccccccccc".to_string(),
    )
    .unwrap();
    assert!(removed.changed);
    assert!(removed.issue.as_ref().unwrap().links.is_empty());
    assert_warm("projection absent", 1, 1, 6, 3);
    assert_cache_matches_replay(&beads_dir, &cache_path, "projection absent");
}

#[test]
fn warm_link_projection_batch_publishes_both_streams() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    let third = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Third".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    // Two holders in two physical streams commit once with two changed
    // signatures and two published rows.
    store_io_stats::reset();
    let batch = set_bead_link_projections(
        &beads_dir,
        &[
            projection_request(
                &left_id,
                &format!("bead:{right_id}"),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(1),
                "left warm edge",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:03:00Z",
            ),
            projection_request(
                &third.id,
                &format!("bead:{right_id}"),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(2),
                "third warm edge",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:03:00Z",
            ),
        ],
    )
    .unwrap();
    assert!(batch.changed);
    assert_eq!(batch.issue_ids.len(), 2);
    assert_warm("projection batch", 2, 2, 10, 5);
    let cache_path = cache_path_for(&beads_dir);
    assert_cache_matches_replay(&beads_dir, &cache_path, "projection batch");
}

#[test]
fn warm_plus_one_records_evidence_and_dedupes() {
    let (_temp, beads_dir, task_id) = warm_task();
    store_io_stats::reset();
    let reported = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &["plan:202610/notes.md".to_string()],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert!(reported.changed);
    let issue = reported.issue.as_ref().unwrap();
    assert_eq!(issue.plus_one_evidence.len(), 1);
    assert_eq!(
        reported.references,
        vec!["plan:202610/notes.md".to_string()]
    );
    assert_warm("plus one", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "plus-one next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "plus one");

    // Repeating the reporter is an exact no-op that writes nothing.
    store_io_stats::reset();
    let duplicate = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &[],
        Some("2026-01-02T01:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert!(!duplicate.changed);
    assert!(!duplicate.message.is_empty());
    assert_warm("plus-one dedupe", 0, 0, 6, 3);
    assert_cache_matches_replay(&beads_dir, &cache_path, "plus-one dedupe");
}

#[test]
fn warm_closed_task_plus_one_promotes_to_ready() {
    let (_temp, beads_dir, task_id) = warm_task();
    close_issues(
        &beads_dir,
        std::slice::from_ref(&task_id),
        Some("stale close".to_string()),
        Some(crate::bead::wire::BeadResolutionWire::Canceled),
        false,
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();
    // The still-replay close leaves the cache stale behind its append; warm
    // the read path first so the measured +1 runs the steady-state admission.
    let cache_path = cache_path_for(&beads_dir);
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    // Without an observation window the +1 reopens the closed task: status
    // promotes to ready, the assignee clears, and close history archives.
    store_io_stats::reset();
    let promoted = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "still reproduces on main",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert!(promoted.changed);
    let issue = promoted.issue.as_ref().unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert!(issue.assignee.is_empty());
    assert_eq!(issue.close_history.len(), 1);
    assert!(!promoted.reopen_withheld);
    assert_warm("plus-one promotion", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_cache_matches_replay(&beads_dir, &cache_path, "promotion");
}

#[test]
fn warm_snooze_wake_and_cancel_stay_warm() {
    let (_temp, beads_dir, task_id) = warm_task();
    store_io_stats::reset();
    let snoozed = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-04T00:00:00Z",
        Some(2),
        "needs the upstream fix first",
        "owner@example.com",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    assert!(snoozed.changed);
    let issue = snoozed.issue.as_ref().unwrap();
    assert_eq!(issue.status, StatusWire::Snoozed);
    assert_eq!(issue.snooze.as_ref().unwrap().plus_one_target, Some(2));
    assert_warm("snooze", 1, 1, 6, 3);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "snooze next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "snooze");

    // First +1 keeps the bead snoozed; the second reaches the target and
    // wakes it with a threshold note.
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    store_io_stats::reset();
    let woken = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-two",
        "same here",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert!(woken.changed);
    let issue = woken.issue.as_ref().unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert!(issue.snooze.is_none());
    assert!(note_text(issue).contains("Reopened by +1 threshold"));
    assert_warm("plus-one wake", 1, 1, 6, 3);
    assert_cache_matches_replay(&beads_dir, &cache_path, "wake");

    // Re-snooze, then cancel back to triage with no record left behind.
    snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-11T00:00:00Z",
        None,
        "still waiting",
        "owner@example.com",
        Some("2026-01-03T01:00:00Z".to_string()),
    )
    .unwrap();
    store_io_stats::reset();
    let canceled = cancel_task_snooze(
        &beads_dir,
        &task_id,
        "owner@example.com",
        Some("2026-01-03T02:00:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(canceled.status, StatusWire::Ready);
    assert!(canceled.snooze.is_none());
    assert_warm("snooze cancel", 1, 1, 6, 3);
    assert_token_only_next_read(&beads_dir, 1, "cancel next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "cancel");
}

#[test]
fn warm_links_evidence_errors_leave_no_bytes() {
    let (_temp, beads_dir, left_id, right_id) = warm_two_plans();
    let (_task_temp, task_beads, task_id) = warm_task();
    let before = reduces_to_store(&beads_dir);

    // Self-target, unknown ID, bad operation ID, and non-task snooze all
    // fail without writing.
    let error = add_bead_link(
        &beads_dir,
        &left_id,
        &format!("bead:{left_id}"),
        "related",
        "self edge",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    let error = add_bead_link(
        &beads_dir,
        "no-such-bead",
        &format!("bead:{right_id}"),
        "related",
        "ghost edge",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind, "not_found");
    let error = add_bead_link(
        &beads_dir,
        &left_id,
        &format!("bead:{right_id}"),
        "related",
        "bad op edge",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        Some("not-hex".to_string()),
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    let error = snooze_task(
        &task_beads,
        &task_id,
        "2026-01-01T00:00:00Z",
        None,
        "backdated",
        "owner@example.com",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    let error = cancel_task_snooze(
        &task_beads,
        &task_id,
        "owner@example.com",
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    let error = add_task_plus_one(
        &task_beads,
        "sase-999",
        "reporter-one",
        "unknown bead",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind, "not_found");

    assert_eq!(reduces_to_store(&beads_dir), before);
    let cache_path = cache_path_for(&beads_dir);
    assert_cache_matches_replay(&beads_dir, &cache_path, "error paths");
}

fn replay_json(beads_dir: &Path) -> serde_json::Value {
    serde_json::to_value(reduces_to_store(beads_dir)).unwrap()
}

/// The whole links-evidence family converges on cached and replay backings:
/// identical replayed state on both sides, and cache-equals-replay on the
/// cached side.
#[test]
fn dual_mode_links_evidence_matches_replay() {
    let (cached_temp, cached_beads) = mode_store(StoreMode::Cached);
    let (plain_temp, plain_beads) = mode_store(StoreMode::Replay);
    let _held = (cached_temp, plain_temp);
    assert_cached_path_used(&cached_beads, "links-evidence parity");
    for beads_dir in [&cached_beads, &plain_beads] {
        let left = create_issue(
            beads_dir,
            BeadCreateRequestWire {
                title: "Left".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let right = create_issue(
            beads_dir,
            BeadCreateRequestWire {
                title: "Right".to_string(),
                issue_type: IssueTypeWire::Plan,
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
        add_bead_link(
            beads_dir,
            &left.id,
            &format!("bead:{}", right.id),
            "related",
            "shares the dual root cause",
            ArtifactLinkOriginWire::Manual,
            BeadLinkDirectionWire::Out,
            1,
            Some("2026-01-01T00:03:00Z".to_string()),
            Some(hex_operation_id(11)),
        )
        .unwrap();
        set_bead_link_projection(
            beads_dir,
            &left.id,
            "plan:202610/dual.md",
            "related",
            BeadLinkDirectionWire::Out,
            true,
            Some("dual projected edge".to_string()),
            Some(ArtifactLinkOriginWire::Manual),
            1,
            Some("2026-01-01T00:04:00Z".to_string()),
            hex_operation_id(12),
        )
        .unwrap();
        remove_bead_link(
            beads_dir,
            &left.id,
            &format!("bead:{}", right.id),
            Some("related"),
            BeadLinkDirectionWire::Out,
            Some("2026-01-01T00:05:00Z".to_string()),
            Some(hex_operation_id(13)),
        )
        .unwrap();
        snooze_task(
            beads_dir,
            &task.id,
            "2026-01-04T00:00:00Z",
            Some(2),
            "needs the upstream fix first",
            "owner@example.com",
            Some("2026-01-01T00:06:00Z".to_string()),
        )
        .unwrap();
        add_task_plus_one(
            beads_dir,
            &task.id,
            "reporter-one",
            "hit this too",
            &[],
            Some("2026-01-02T00:00:00Z".to_string()),
            None,
            None,
        )
        .unwrap();
        add_task_plus_one(
            beads_dir,
            &task.id,
            "reporter-two",
            "same here",
            &[],
            Some("2026-01-03T00:00:00Z".to_string()),
            None,
            None,
        )
        .unwrap();
        cancel_task_snooze(
            beads_dir,
            &task.id,
            "owner@example.com",
            Some("2026-01-03T01:00:00Z".to_string()),
        )
        .unwrap_err();
    }
    assert_eq!(replay_json(&cached_beads), replay_json(&plain_beads));
    assert_cache_equals_replay(&cached_beads, "links-evidence parity");
}
