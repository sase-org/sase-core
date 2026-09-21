//! Mutation-command tests, covering
//! `super::super::mutate_commands`: open, update, close, dep, ref, rm.

use super::super::*;
use super::support::*;
use crate::bead::read::read_store_issues;
use crate::bead::wire::{notes_text, DependencyWire, StatusWire};
use std::fs;
use std::path::Path;

#[test]
fn dependency_remove_is_handled_with_a_batch_mutation_summary() {
    let mut source = plan_issue(
        "beads-1",
        "Source",
        "",
        StatusWire::Open,
        "2026-01-01T00:00:00Z",
    );
    source.dependencies = vec![
        DependencyWire {
            issue_id: "beads-1".to_string(),
            depends_on_id: "beads-2".to_string(),
            created_at: "2026-01-01T00:02:00Z".to_string(),
            created_by: "owner@example.com".to_string(),
        },
        DependencyWire {
            issue_id: "beads-1".to_string(),
            depends_on_id: "beads-3".to_string(),
            created_at: "2026-01-01T00:03:00Z".to_string(),
            created_by: "owner@example.com".to_string(),
        },
    ];
    let store = seed_issues(vec![
        source,
        plan_issue(
            "beads-2",
            "First target",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        plan_issue(
            "beads-3",
            "Second target",
            "",
            StatusWire::Open,
            "2026-01-01T00:02:00Z",
        ),
    ]);

    let outcome = execute_search(
        &store.beads_dir,
        &["dep", "rm", "beads-1", "beads-2", "beads-3"],
    );

    assert_eq!(
        outcome.stdout,
        concat!(
            "✗ Removed dependency: beads-1 no longer depends on beads-2\n",
            "✗ Removed dependency: beads-1 no longer depends on beads-3\n",
            "○ beads-1 has no active blockers.\n",
        )
    );
    let summary = outcome.mutation_summary.unwrap();
    assert_eq!(summary.operation, "dep_rm");
    assert_eq!(summary.issue_ids, vec!["beads-1", "beads-2", "beads-3"]);
    assert!(read_store_issues(&store.beads_dir)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == "beads-1")
        .unwrap()
        .dependencies
        .is_empty());
}

#[test]
fn remove_handles_multiple_ids_with_unique_output_and_requested_summary() {
    let mut child = phase_issue(
        "beads-1.1",
        "Child",
        "",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    );
    child.parent_id = Some("beads-1".to_string());
    let store = seed_issues(vec![
        plan_issue(
            "beads-1",
            "Plan",
            "",
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        ),
        child,
        plan_issue(
            "beads-2",
            "Independent",
            "",
            StatusWire::Open,
            "2026-01-01T00:02:00Z",
        ),
    ]);
    let args = vec![
        "rm".to_string(),
        "beads-1".to_string(),
        "beads-1.1".to_string(),
        "beads-2".to_string(),
        "beads-2".to_string(),
    ];

    let outcome = execute_bead_cli(
        &args,
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        concat!(
            "✗ Removed: beads-1.1 — Child\n",
            "✗ Removed: beads-1 — Plan\n",
            "✗ Removed: beads-2 — Independent\n",
        )
    );
    let summary = outcome.mutation_summary.unwrap();
    assert_eq!(summary.operation, "rm");
    assert_eq!(summary.issue_ids, args[1..]);
    assert!(read_store_issues(&store.beads_dir).unwrap().is_empty());
}

#[test]
fn remove_missing_later_id_is_an_atomic_fast_path_error() {
    let store = seed_issues(vec![
        plan_issue(
            "beads-1",
            "First",
            "",
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        ),
        plan_issue(
            "beads-2",
            "Second",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
    ]);
    let projection_before =
        fs::read(store.beads_dir.join("issues.jsonl")).unwrap();

    let outcome = execute_bead_cli(
        &[
            "rm".to_string(),
            "beads-1".to_string(),
            "beads-missing".to_string(),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 1);
    assert_eq!(outcome.stderr, "Error: issue not found: beads-missing\n");
    assert!(outcome.stdout.is_empty());
    assert!(outcome.mutation_summary.is_none());
    assert_eq!(
        fs::read(store.beads_dir.join("issues.jsonl")).unwrap(),
        projection_before
    );
    assert_eq!(
        read_store_issues(&store.beads_dir)
            .unwrap()
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec!["beads-1", "beads-2"]
    );
}

#[test]
fn close_summary_preserves_requested_ids_and_real_prior_status() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Active phase",
        "",
        StatusWire::InProgress,
        "2026-01-01T00:01:00Z",
    )]);
    let outcome = execute_bead_cli(
        &["close".to_string(), "beads-1.1".to_string()],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();
    let summary = outcome.mutation_summary.unwrap();
    assert_eq!(summary.issue_ids, vec!["beads-1.1"]);
    assert_eq!(summary.status_transitions.len(), 1);
    assert_eq!(summary.status_transitions[0].from_status, "in_progress");
    assert_eq!(summary.status_transitions[0].to_status, "closed");
}

#[test]
fn close_fast_path_accepts_note_and_updates_once() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Active phase",
        "",
        StatusWire::InProgress,
        "2026-01-01T00:01:00Z",
    )]);
    let outcome = execute_bead_cli(
        &[
            "close".to_string(),
            "beads-1.1".to_string(),
            "--note".to_string(),
            "verified with cargo test".to_string(),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 0);
    let summary = outcome.mutation_summary.unwrap();
    assert_eq!(summary.operation, "close");
    assert_eq!(summary.issue_ids, vec!["beads-1.1"]);
    let issue = read_store_issues(&store.beads_dir)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == "beads-1.1")
        .unwrap();
    assert_eq!(issue.status, StatusWire::Closed);
    assert!(notes_text(&issue.notes).ends_with("] verified with cargo test"));
    let (_manifest, streams) =
        crate::bead::jsonl::read_event_store(&store.beads_dir).unwrap();
    let operations = streams[0]
        .events
        .iter()
        .rev()
        .take(2)
        .map(|event| event.operation)
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        vec![
            crate::bead::events::BeadEventOperationWire::IssueClosed,
            crate::bead::events::BeadEventOperationWire::NoteAppended,
        ]
    );
}

#[test]
fn update_fast_path_reports_changed_and_unchanged_rows_in_one_commit() {
    let store = seed_issues(vec![
        task_issue(
            "beads-1",
            "First task",
            "",
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        ),
        task_issue(
            "beads-2",
            "Second task",
            "",
            StatusWire::InProgress,
            "2026-01-01T00:01:00Z",
        ),
    ]);
    let outcome = execute_bead_cli(
        &[
            "update".to_string(),
            "beads-1".to_string(),
            "beads-2".to_string(),
            "-s".to_string(),
            "in_progress".to_string(),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        concat!(
            "✓ Updated issue: beads-1 — First task\n",
            "· Unchanged: beads-2 — Second task\n",
        )
    );
    let summary = outcome.mutation_summary.unwrap();
    assert_eq!(summary.operation, "update");
    assert!(summary.changed);
    assert_eq!(summary.issue_ids, vec!["beads-1".to_string()]);

    let issues = read_store_issues(&store.beads_dir).unwrap();
    let first = issues.iter().find(|issue| issue.id == "beads-1").unwrap();
    assert_eq!(first.status, StatusWire::InProgress);
    let second = issues.iter().find(|issue| issue.id == "beads-2").unwrap();
    assert_eq!(second.status, StatusWire::InProgress);
}
