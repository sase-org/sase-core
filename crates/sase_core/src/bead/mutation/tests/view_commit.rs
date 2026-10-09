//! View-commit coverage (`sase-1h8.13.1.9.3`): one staging area, one
//! commit, and one runner for create and the notes family.
//!
//! A replay-backed `commit` on a legacy store materializes the event
//! store and rewrites `issues.jsonl`, exactly as `MutableStore::save`
//! does. A cached-path decline retries the same algorithm on the replay
//! backing with exactly one durable write across both passes.

use super::super::*;
use super::support::*;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::mutation::runner::run_mutation;
use crate::bead::mutation::runner::MutationStep;
use crate::bead::mutation::store::outcome;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::wire::IssueTypeWire;
use std::cell::Cell;
use std::fs;
use tempfile::tempdir;

fn legacy_plan(title: &str) -> BeadCreateRequestWire {
    BeadCreateRequestWire {
        title: title.to_string(),
        issue_type: IssueTypeWire::Plan,
        now: Some("2026-01-01T00:00:00Z".to_string()),
        ..Default::default()
    }
}

#[test]
fn view_commit_replay_commit_on_legacy_store() {
    // A legacy store has config plus `issues.jsonl` and no `events/`
    // dir, so the runner admits no cache and commits on the replay
    // backing.
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    assert!(!beads_dir.join("events").exists());

    let created = create_issue(&beads_dir, legacy_plan("Legacy plan"))
        .unwrap()
        .issue
        .unwrap();

    // The first replay commit materialized the event store and rewrote
    // the `issues.jsonl` projection, exactly as `MutableStore::save`
    // does for a legacy store.
    assert!(beads_dir.join("events").is_dir());
    let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(
        jsonl.contains(&created.id),
        "legacy commit must rewrite issues.jsonl"
    );

    append_issue_note(
        &beads_dir,
        &created.id,
        "legacy note",
        Some("agent".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
        None,
    )
    .unwrap();
    let reduced = reduces_to_store(&beads_dir);
    let reloaded = reduced
        .iter()
        .find(|issue| issue.id == created.id)
        .expect("created issue replays back");
    assert_eq!(reloaded.notes.len(), 1);
    assert_eq!(
        note_text(reloaded),
        "[2026-01-01T00:01:00Z · agent] legacy note"
    );
}

#[test]
fn view_commit_runner_declines_then_replays_once() {
    // A git-backed store admits the cached path, so the first pass runs
    // cached and the retry runs on the owned replay backing.
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    assert_cached_path_used(&beads_dir, "decline-then-replay");
    let issue = create_issue(&beads_dir, legacy_plan("Epic"))
        .unwrap()
        .issue
        .unwrap();

    store_io_stats::reset();
    let declined = Cell::new(false);
    let outcome = run_mutation(&beads_dir, "view_commit_probe", |view| {
        if !declined.get() {
            // Simulate a pre-write cached-path decline (a missing
            // stream file or manifest): no durable write happened, so
            // the runner must retry on the replay backing.
            declined.set(true);
            assert!(
                !view.durably_written(),
                "decline must precede any durable write"
            );
            return Ok(MutationStep::NeedsReplay);
        }
        let mut staged = view.get(&issue.id)?;
        let Some(event_id) = view.stage_event(
            &staged.id,
            BeadEventOperationWire::NoteAppended,
            BeadEventPayloadWire::NoteAppended {
                entry: "retried note".to_string(),
                attachments: Vec::new(),
            },
            "2026-01-01T00:02:00Z",
            "agent",
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        if let Some(note) = crate::bead::wire::BeadNoteWire::from_event(
            &event_id,
            "2026-01-01T00:02:00Z",
            "agent",
            "retried note",
            Vec::new(),
        ) {
            staged.notes.push(note);
        }
        staged.updated_at = "2026-01-01T00:02:00Z".to_string();
        staged.validate()?;
        view.stage_issue(staged.clone());
        let Some(mut rows) = view.commit(&[staged.id.clone()])? else {
            return Ok(MutationStep::NeedsReplay);
        };
        let committed = rows.pop().expect("one retried row");
        let mut result =
            outcome("view_commit_probe", true, vec![committed.id.clone()]);
        result.issue = Some(committed);
        Ok(MutationStep::Done(result))
    })
    .unwrap();
    assert_eq!(outcome.issue_ids, vec![issue.id.clone()]);
    assert!(declined.get(), "the first pass must have declined");
    // Exactly one durable write across both passes: the declined cached
    // pass wrote nothing, and the replay pass saved once.
    assert_eq!(
        store_io_stats::saves(),
        1,
        "decline-then-replay must write exactly once"
    );
    assert_eq!(
        store_io_stats::full_replays(),
        1,
        "the replay backing must be built exactly once"
    );
    // The retried event appended exactly once.
    let reduced = reduces_to_store(&beads_dir);
    let reloaded = reduced
        .iter()
        .find(|row| row.id == issue.id)
        .expect("probed issue replays back");
    assert_eq!(reloaded.notes.len(), 1);
    assert_eq!(
        note_text(reloaded),
        "[2026-01-01T00:02:00Z · agent] retried note"
    );
    assert_cache_equals_replay(&beads_dir, "decline-then-replay");
}
