//! Dual-mode coverage for read-model mutations (sase-1h8.13).
//!
//! Otherwise identical cacheable (git-backed) and replay (non-git) stores
//! run the same mutation scenarios with the same assertions: the cached
//! path must exercise identical behavior, not a weaker suite.

use super::super::*;
use super::support::*;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::jsonl::{
    read_event_store, write_event_store_changed_with_total,
};
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::store::MutableStore;
use crate::bead::mutation::view::MutationView;
use crate::bead::read_model::{
    ensure_cache_ready_at, ensure_cache_ready_for_mutation_at,
    read_model_cache_path_for_store, rebuild_read_model_at,
};
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::collections::BTreeSet;
use std::fs;
use tempfile::tempdir;

fn seed_two_issue_store(beads_dir: &std::path::Path) -> (String, String) {
    let epic = create_issue(
        beads_dir,
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
    let task = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            size: Some(PhaseSizeWire::Small),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (epic.id, task.id)
}

fn shared_note_update_scenario(beads_dir: &std::path::Path, task_id: &str) {
    append_issue_note(
        beads_dir,
        task_id,
        "first note",
        Some("agent".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    update_issue(
        beads_dir,
        task_id,
        BeadUpdateFieldsWire {
            title: Some("Renamed task".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let issues = reduces_to_store(beads_dir);
    let renamed = issues.iter().find(|issue| issue.id == task_id).unwrap();
    assert_eq!(renamed.title, "Renamed task");
    assert!(note_text(renamed).contains("first note"));
}

#[test]
fn dual_mode_note_update_matches() {
    let git_temp = tempdir().unwrap();
    fs::create_dir_all(git_temp.path().join(".git")).unwrap();
    init_store(git_temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let git_beads = git_temp.path().join("beads");
    let (_, git_task) = seed_two_issue_store(&git_beads);
    shared_note_update_scenario(&git_beads, &git_task);

    let plain_temp = tempdir().unwrap();
    init_store(plain_temp.path(), "beads", "sase", "owner@example.com")
        .unwrap();
    let plain_beads = plain_temp.path().join("beads");
    let (_, plain_task) = seed_two_issue_store(&plain_beads);
    shared_note_update_scenario(&plain_beads, &plain_task);

    let git_issues = reduces_to_store(&git_beads);
    let plain_issues = reduces_to_store(&plain_beads);
    let git_value = serde_json::to_value(
        git_issues
            .iter()
            .map(|issue| (issue.title.clone(), note_text(issue)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let plain_value = serde_json::to_value(
        plain_issues
            .iter()
            .map(|issue| (issue.title.clone(), note_text(issue)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(git_value, plain_value);

    let cache_path = read_model_cache_path_for_store(&git_beads).unwrap();
    assert!(ensure_cache_ready_at(&git_beads, &cache_path).unwrap());
    assert!(read_model_cache_path_for_store(&plain_beads).is_none());
}

#[test]
fn mutation_view_cached_matches_replay() {
    let git_temp = tempdir().unwrap();
    fs::create_dir_all(git_temp.path().join(".git")).unwrap();
    init_store(git_temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = git_temp.path().join("beads");
    let (epic_id, task_id) = seed_two_issue_store(&beads_dir);
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    assert!(ensure_cache_ready_at(&beads_dir, &cache_path).unwrap());

    let replay_issues = reduces_to_store(&beads_dir);
    let cached = MutationView::load(&beads_dir, &replay_issues).unwrap();
    assert!(cached.is_cached());

    let replay_value = serde_json::to_value(&replay_issues).unwrap();
    let cached_task = cached.get(&task_id).unwrap();
    let replay_task = replay_issues
        .iter()
        .find(|issue| issue.id == task_id)
        .unwrap();
    assert_eq!(
        serde_json::to_value(&cached_task).unwrap(),
        serde_json::to_value(replay_task).unwrap()
    );
    assert_eq!(cached.resolve(&task_id).unwrap(), task_id);
    let cached_children = cached.children(&epic_id).unwrap();
    assert_eq!(cached_children.len(), 1);
    assert_eq!(cached_children[0].id, task_id);
    let descendants = cached.descendants(&epic_id).unwrap();
    assert_eq!(descendants.len(), 1);
    let ancestors = cached.ancestors(&task_id).unwrap();
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0].id, epic_id);
    let stream_id = cached.stream_id_for_issue(&task_id).unwrap();
    assert_eq!(stream_id, epic_id);
    let next_child = cached.next_child_id(&epic_id).unwrap();
    assert_eq!(next_child, format!("{epic_id}.2"));
    let next_top = cached.next_top_level_counter("sase", 1).unwrap();
    assert!(next_top >= 1);
    let _ = replay_value;
}

#[test]
fn mutation_view_covers_deps_refs_receipts() {
    let git_temp = tempdir().unwrap();
    fs::create_dir_all(git_temp.path().join(".git")).unwrap();
    init_store(git_temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = git_temp.path().join("beads");
    let (epic_id, task_id) = seed_two_issue_store(&beads_dir);
    add_dependency(
        &beads_dir,
        &task_id,
        &epic_id,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            external_ref: Some("ext-1".to_string()),
            now: Some("2026-01-01T00:05:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    add_bead_link(
        &beads_dir,
        &task_id,
        &format!("bead:{epic_id}"),
        "related",
        "shares the read-model root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:06:00Z".to_string()),
        Some("0123456789abcdef0123456789abcdef".to_string()),
    )
    .unwrap();
    let replay_issues = reduces_to_store(&beads_dir);
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let view = MutationView::load(&beads_dir, &replay_issues).unwrap();
    assert!(view.is_cached());

    let targets = view.dependency_targets(&task_id).unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].id, epic_id);
    let dependents = view.reverse_dependents(&epic_id).unwrap();
    assert!(dependents.iter().any(|issue| issue.id == task_id));
    let owner = view.external_ref_owner("ext-1").unwrap().unwrap();
    assert_eq!(owner.id, task_id);
    assert!(view.external_ref_owner("ext-missing").unwrap().is_none());
    assert!(view
        .projection_receipt_seen(
            &task_id,
            "0123456789abcdef0123456789abcdef",
            BeadEventOperationWire::LinkAdded,
            &format!("bead:{epic_id}"),
            "related",
            BeadLinkDirectionWire::Out,
        )
        .unwrap());
    assert!(!view
        .projection_receipt_seen(
            &task_id,
            "ffffffffffffffffffffffffffffffff",
            BeadEventOperationWire::LinkAdded,
            &format!("bead:{epic_id}"),
            "related",
            BeadLinkDirectionWire::Out,
        )
        .unwrap());
}

#[test]
fn mutation_view_overlay_covers_batch_exchange() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let (epic_id, task_id) = seed_two_issue_store(&beads_dir);
    let replay_issues = reduces_to_store(&beads_dir);
    let mut view = MutationView::load(&beads_dir, &replay_issues).unwrap();
    assert!(!view.is_cached());

    let mut first = view.get(&task_id).unwrap();
    first.title = "First wins".to_string();
    view.stage_issue(first);
    assert_eq!(view.get(&task_id).unwrap().title, "First wins");
    let mut second = view.get(&task_id).unwrap();
    second.title = "Second wins".to_string();
    view.stage_issue(second);
    assert_eq!(view.get(&task_id).unwrap().title, "Second wins");
    view.stage_removal(&task_id);
    assert!(view.get(&task_id).is_err());
    assert!(view.children(&epic_id).unwrap().is_empty());
}

#[test]
fn store_io_stats_prove_replay_and_validation() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    seed_two_issue_store(&beads_dir);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let stream_count = streams.len() as u64;
    drop(streams);
    store_io_stats::reset();
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(store_io_stats::loads(), 1);
    assert_eq!(store_io_stats::full_replays(), 1);
    assert_eq!(store_io_stats::stream_reads(), stream_count);
    assert_eq!(store_io_stats::hydrated_rows(), 2);
    store.save().unwrap();
    assert_eq!(store_io_stats::saves(), 1);
    assert_eq!(store_io_stats::validation_runs(), 2);
}

#[test]
fn partial_writer_keeps_full_manifest_count() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    seed_two_issue_store(&beads_dir);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let total = streams.len();
    assert!(total >= 1);
    let first = streams[0].clone();
    let mut changed = BTreeSet::new();
    changed.insert(first.stream_id.clone());
    let lazy_slice = vec![first];
    write_event_store_changed_with_total(
        &beads_dir,
        &lazy_slice,
        &changed,
        total,
    )
    .unwrap();
    let manifest_text =
        fs::read_to_string(beads_dir.join("events/manifest.json")).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_text).unwrap();
    assert_eq!(manifest["stream_count"], total as u64);
}

#[test]
fn mutation_sweep_establishes_fresh_baseline() {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    seed_two_issue_store(&beads_dir);
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    assert!(ensure_cache_ready_at(&beads_dir, &cache_path).unwrap());
}

#[test]
fn validation_rejection_leaves_canonical_bytes_unchanged() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let (_, task_id) = seed_two_issue_store(&beads_dir);
    let before = reduces_to_store(&beads_dir);
    let rejected = update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            status: Some("not-a-status".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    );
    assert!(rejected.is_err());
    let after = reduces_to_store(&beads_dir);
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&after).unwrap()
    );
}
