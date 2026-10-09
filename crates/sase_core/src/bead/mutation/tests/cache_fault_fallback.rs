//! Cache-fault fallback (core-cache-race): a mutation whose cache
//! disappears or is invalidated between admission and its cached reads
//! still succeeds through the replay fallback. The fault never fails
//! the mutation, and the retry never appends twice.

use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::mutation::runner::run_mutation;
use crate::bead::mutation::runner::MutationStep;
use crate::bead::mutation::store::outcome;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::view::MutationView;
use crate::bead::read_model::drop_cache_file;
use crate::bead::read_model::ensure_cache_ready_for_mutation_at;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::cell::Cell;
use std::fs;
use tempfile::tempdir;

fn warm_epic() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    assert_cached_path_used(&beads_dir, "cache-fault-fallback");
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
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap(),
        "cache-fault-fallback: cache warm-up failed"
    );
    assert!(
        cache_path.is_file(),
        "cache-fault-fallback: expected a cache file"
    );
    (temp, beads_dir, epic.id)
}

fn stage_probe_note(
    view: &mut MutationView,
    issue_id: &str,
) -> Result<MutationStep<BeadMutationOutcomeWire>, crate::bead::wire::BeadError>
{
    let mut staged = view.get(issue_id)?;
    let Some(event_id) = view.stage_event(
        &staged.id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: "fallback note".to_string(),
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
        "fallback note",
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
    let committed = rows.pop().expect("one fallback row");
    let mut result =
        outcome("cache_fault_probe", true, vec![committed.id.clone()]);
    result.issue = Some(committed);
    Ok(MutationStep::Done(result))
}

#[test]
fn cache_disappearance_falls_back_to_replay_mid_mutation() {
    let (_temp, beads_dir, epic_id) = warm_epic();
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    assert!(cache_path.is_file());

    store_io_stats::reset();
    let sabotaged = Cell::new(false);
    let outcome = run_mutation(&beads_dir, "cache_fault_probe", |view| {
        if !sabotaged.get() {
            // Pull the cache out from under the admitted view, the way
            // the old unlink race did mid-mutation.
            sabotaged.set(true);
            assert!(
                view.is_cached(),
                "first pass must run on the cached backing"
            );
            fs::remove_file(&cache_path).unwrap();
            let fault = view.get(&epic_id).unwrap_err();
            assert_eq!(
                fault.kind, "cache_fault",
                "a vanished cache must decline, not fail: {fault:?}"
            );
            return Err(fault);
        }
        stage_probe_note(view, &epic_id)
    })
    .unwrap();

    assert_eq!(outcome.issue_ids, vec![epic_id.clone()]);
    assert!(sabotaged.get(), "the first pass must have run");
    // Exactly one durable write across both passes: the faulted cached
    // pass wrote nothing, and the replay pass saved once.
    assert_eq!(
        store_io_stats::saves(),
        1,
        "cache-fault fallback must write exactly once"
    );
    let reduced = reduces_to_store(&beads_dir);
    let reloaded = reduced
        .iter()
        .find(|row| row.id == epic_id)
        .expect("probed issue replays back");
    assert_eq!(reloaded.notes.len(), 1);
    assert_eq!(
        note_text(reloaded),
        "[2026-01-01T00:02:00Z · agent] fallback note"
    );
    assert_cache_equals_replay(&beads_dir, "cache-fault-fallback");
}

#[test]
fn invalidated_cache_declines_cached_reads_and_mutation_succeeds() {
    let (_temp, beads_dir, epic_id) = warm_epic();
    // A second row the admitted view never loads, so the post-
    // invalidation read below reaches SQLite instead of the memo.
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

    // An admitted view reads fine before the invalidation.
    let view = MutationView::load_cached(&beads_dir)
        .unwrap()
        .expect("admitted");
    assert!(view.is_cached());
    view.get(&epic_id).unwrap();

    // Invalidate under the admitted view: its next cached read must
    // decline instead of serving rows a rebuild is about to replace.
    drop_cache_file(&read_model_cache_path_for_store(&beads_dir).unwrap());
    let fault = view.get(&task.id).unwrap_err();
    assert_eq!(
        fault.kind, "cache_fault",
        "an invalidated cache must decline: {fault:?}"
    );
    drop(view);

    // The mutation itself still succeeds, and the cache heals after.
    append_issue_note(
        &beads_dir,
        &epic_id,
        "note after invalidation",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    let reduced = reduces_to_store(&beads_dir);
    let reloaded = reduced
        .iter()
        .find(|row| row.id == epic_id)
        .expect("probed issue replays back");
    assert_eq!(reloaded.notes.len(), 1);
    assert_cache_equals_replay(&beads_dir, "invalidation-fallback");
}
