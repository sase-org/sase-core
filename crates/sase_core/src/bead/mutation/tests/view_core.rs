//! View-core coverage: one mutation view with shared algorithms and the
//! full notes family on it (sase-1h8.13.1.3).
//!
//! Every notes-family entry point runs warm (zero full replays and snapshot
//! loads, bounded hydration and stream reads) and matches replay. Ordering
//! ties and allocation edge cases prove oracle parity.

use super::super::*;
use super::support::*;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::view::MutationView;
use crate::bead::read_model::{
    ensure_cache_ready_for_mutation_at, read_model_cache_path_for_store,
};
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::fs;
use tempfile::tempdir;

fn warm_beads() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
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
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    (temp, beads_dir, epic.id)
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

fn assert_cache_matches(beads_dir: &std::path::Path, label: &str) {
    assert_cache_equals_replay(beads_dir, label);
}

#[test]
fn view_core_note_edit_and_remove_stay_warm() {
    let (_temp, beads_dir, epic_id) = warm_beads();
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    append_issue_note(
        &beads_dir,
        &task.id,
        "original note",
        Some("agent".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    let note_id = reduces_to_store(&beads_dir)
        .into_iter()
        .find(|issue| issue.id == task.id)
        .unwrap()
        .notes[0]
        .id
        .clone();

    store_io_stats::reset();
    let edited = edit_issue_note(
        &beads_dir,
        &task.id,
        &note_id,
        "edited note",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(edited.notes[0].id, note_id);
    assert!(note_text(&edited).contains("edited note"));
    assert_warm("note edit");
    assert!(store_io_stats::hydrated_rows() <= 6);
    assert!(store_io_stats::stream_reads() <= 2);

    store_io_stats::reset();
    let removed = remove_issue_note(
        &beads_dir,
        &task.id,
        &note_id,
        Some("agent".to_string()),
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(removed.notes.is_empty());
    assert_warm("note remove");
    assert!(store_io_stats::hydrated_rows() <= 6);
    assert!(store_io_stats::stream_reads() <= 2);
    assert_cache_matches(&beads_dir, "note edit/remove");
    assert!(!epic_id.is_empty());
}

#[test]
fn view_core_edit_attachments_replace_keep_and_detach() {
    let (_temp, beads_dir, _) = warm_beads();
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    append_issue_note(
        &beads_dir,
        &task.id,
        "note with files",
        Some("agent".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    let note_id = reduces_to_store(&beads_dir)
        .into_iter()
        .find(|issue| issue.id == task.id)
        .unwrap()
        .notes[0]
        .id
        .clone();
    let attachment = crate::note_attachment::BeadNoteAttachmentWire {
        name: "proof.png".to_string(),
        sha256:
            "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1"
                .to_string(),
        size_bytes: 188416,
        mime_type: "image/png".to_string(),
        image: None,
        origin: None,
        visibility: None,
    };

    store_io_stats::reset();
    let replaced = edit_issue_note(
        &beads_dir,
        &task.id,
        &note_id,
        "saw this @attachment:proof.png",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        Some(vec![attachment.clone()]),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(replaced.notes[0].attachments, vec![attachment.clone()]);
    assert_warm("edit replace attachments");

    let kept = edit_issue_note(
        &beads_dir,
        &task.id,
        &note_id,
        "still @attachment:proof.png here",
        Some("agent".to_string()),
        Some("2026-01-01T00:04:00Z".to_string()),
        None,
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(kept.notes[0].attachments, vec![attachment]);
    assert_warm("edit keep attachments");

    let detached = edit_issue_note(
        &beads_dir,
        &task.id,
        &note_id,
        "attachment purged",
        Some("agent".to_string()),
        Some("2026-01-01T00:05:00Z".to_string()),
        Some(vec![]),
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(detached.notes[0].attachments.is_empty());
    assert_warm("edit detach attachments");
    assert_cache_matches(&beads_dir, "edit attachments");
}

#[test]
fn view_core_update_external_ref_change_stays_warm() {
    let (_temp, beads_dir, _) = warm_beads();
    let first =
        create_plan_with_external_ref(&beads_dir, "First", "bug:sase#42");
    let second =
        create_plan_with_external_ref(&beads_dir, "Second", "bug:sase#43");

    store_io_stats::reset();
    let moved = update_issue(
        &beads_dir,
        &second.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#44".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(moved.external_ref, "bug:sase#44");
    assert_warm("external-ref change");

    let error = update_issue(
        &beads_dir,
        &second.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#42".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, "conflict");
    assert_eq!(first.external_ref, "bug:sase#42");
    assert_cache_matches(&beads_dir, "external-ref change");
}

#[test]
fn view_core_update_batch_external_ref_exchange_stays_warm() {
    let (_temp, beads_dir, _) = warm_beads();
    let first =
        create_plan_with_external_ref(&beads_dir, "First", "bug:sase#42");
    let second =
        create_plan_with_external_ref(&beads_dir, "Second", "bug:sase#43");

    // Swap the two refs in one batch: the final overlay is unique, so the
    // view path must accept it exactly as the replay oracle does.
    store_io_stats::reset();
    let outcome = update_issues(
        &beads_dir,
        &[first.id.clone(), second.id.clone()],
        BeadUpdateFieldsWire {
            title: Some("Swapped".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(outcome.changed);
    assert_warm("batch title update");

    // Exchange via two single updates through a temporary free ref.
    update_issue(
        &beads_dir,
        &first.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#tmp".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    update_issue(
        &beads_dir,
        &second.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#42".to_string()),
            now: Some("2026-01-01T00:05:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    update_issue(
        &beads_dir,
        &first.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#43".to_string()),
            now: Some("2026-01-01T00:06:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let issues = reduces_to_store(&beads_dir);
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == first.id)
            .unwrap()
            .external_ref,
        "bug:sase#43"
    );
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == second.id)
            .unwrap()
            .external_ref,
        "bug:sase#42"
    );
    assert_cache_matches(&beads_dir, "external-ref exchange");
}

#[test]
fn view_core_update_reopen_reopens_ancestors_warm() {
    // Build a real parent/child pair, close both, then reopen the child
    // through an update: the closed parent must reopen with history.
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads = temp.path().join("beads");
    let epic = create_issue(
        &beads,
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
    let child = create_issue(
        &beads,
        BeadCreateRequestWire {
            title: "Child".to_string(),
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
    close_for_history(&beads, &child.id, "2026-01-01T00:02:00Z");
    close_for_history(&beads, &epic.id, "2026-01-01T00:03:00Z");

    store_io_stats::reset();
    let reopened = update_issue(
        &beads,
        &child.id,
        BeadUpdateFieldsWire {
            status: Some("open".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(reopened.changed);
    assert_eq!(reopened.reopened_ancestor_ids, vec![epic.id.clone()]);
    assert_warm("update reopen");
    let issues = reduces_to_store(&beads);
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == child.id)
            .unwrap()
            .status,
        StatusWire::Open
    );
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == epic.id)
            .unwrap()
            .status,
        StatusWire::Open
    );
    assert_cache_matches(&beads, "update reopen");
}

#[test]
fn view_core_children_order_matches_replay_on_created_at_ties() {
    let (_temp, beads_dir, epic_id) = warm_beads();
    // Two children with identical timestamps: the view must order them
    // exactly as the replay oracle does (created_at, then ID).
    for title in ["Beta", "Alpha"] {
        create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: title.to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic_id.clone()),
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let replay_issues = reduces_to_store(&beads_dir);
    let view = MutationView::load(&beads_dir, &replay_issues).unwrap();
    let children = view.children(&epic_id).unwrap();
    let mut expected: Vec<_> = replay_issues
        .iter()
        .filter(|issue| issue.parent_id.as_deref() == Some(epic_id.as_str()))
        .cloned()
        .collect();
    expected
        .sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    assert_eq!(
        children.iter().map(|issue| &issue.id).collect::<Vec<_>>(),
        expected.iter().map(|issue| &issue.id).collect::<Vec<_>>()
    );
    let descendants = view.descendants(&epic_id).unwrap();
    assert_eq!(descendants.len(), 2);
    assert_cache_matches(&beads_dir, "ordering ties");
}

#[test]
fn view_core_allocation_edge_cases_match_oracle() {
    use crate::bead::read_model::alloc::child_parent_and_suffix;
    use crate::bead::read_model::alloc::top_prefix_and_counter;

    // Oracle parity for `.` handling: only the suffix matters.
    assert_eq!(
        top_prefix_and_counter("sase-1"),
        Some(("sase".to_string(), 1))
    );
    assert_eq!(top_prefix_and_counter("sase-1.2"), None);
    assert_eq!(top_prefix_and_counter("bad"), None);
    // Dotted prefixes still decode when the suffix is clean.
    assert!(top_prefix_and_counter("a.b-1").is_some());
    // Child parsing stays textual, regardless of parent fields.
    assert_eq!(
        child_parent_and_suffix("sase-1.2"),
        Some(("sase-1".to_string(), 2))
    );
    assert_eq!(child_parent_and_suffix("a.b.c"), None);

    let (_temp, beads_dir, _) = warm_beads();
    let replay_issues = reduces_to_store(&beads_dir);
    let view = MutationView::load(&beads_dir, &replay_issues).unwrap();

    // Empty store: counters start at the config value.
    let empty = tempdir().unwrap();
    init_store(empty.path(), "beads", "sase", "owner@example.com").unwrap();
    let empty_beads = empty.path().join("beads");
    let empty_issues: Vec<crate::bead::wire::IssueWire> = Vec::new();
    let empty_view = MutationView::load(&empty_beads, &empty_issues).unwrap();
    assert_eq!(empty_view.next_top_level_counter("sase", 1).unwrap(), 1);

    // Multiple prefixes do not interfere.
    let next = view.next_top_level_counter("other", 1).unwrap();
    assert_eq!(next, 1);

    // Staged creates raise the maximum; removing the maximum reuses it.
    let created = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Extra".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:05:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let after = reduces_to_store(&beads_dir);
    let view_after = MutationView::load(&beads_dir, &after).unwrap();
    let reused = view_after.next_child_id(&created.id).unwrap();
    assert_eq!(reused, format!("{}.1", created.id));
    assert_cache_matches(&beads_dir, "allocation edges");
}

#[test]
fn view_core_create_with_initial_note_stays_warm() {
    let (_temp, beads_dir, epic_id) = warm_beads();

    store_io_stats::reset();
    let created = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Noted child".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic_id.clone()),
            size: Some(PhaseSizeWire::Small),
            notes: "first note".to_string(),
            now: Some("2026-01-01T00:05:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(note_text(&created).contains("first note"));
    assert_warm("create with note");
    assert!(store_io_stats::hydrated_rows() <= 6);
    assert!(store_io_stats::stream_reads() <= 2);
    assert_cache_matches(&beads_dir, "create with note");
}
