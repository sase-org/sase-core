//! Direct write-through publication proof (`publish-direct`).
//!
//! Warm create, note append, and update publish their delta straight to
//! the read model inside the mutation flock: exactly one full sweep
//! (admission), zero snapshot loads, zero full replays, signature rows
//! written only for changed streams, and token-only next reads. A
//! backdated append invalidates instead of pairing stale rows with
//! newer signatures, so it takes the synchronous guarded repair with
//! identical outcome telemetry; a concurrent publication loses the
//! content-generation race without writing; a cache fault never fails
//! the mutation and never duplicates the durable event; and an
//! in-place change inside the 60 s window is caught at admission.

use super::super::*;
use super::support::*;
use crate::bead::events::mint_bead_event_id;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventRecordWire;
use crate::bead::events::BEAD_EVENT_SCHEMA_VERSION;
use crate::bead::jsonl::event_streams_dir;
use crate::bead::jsonl::file_inode;
use crate::bead::jsonl::hex_signature;
use crate::bead::jsonl::read_event_stream_file;
use crate::bead::jsonl::StreamWriteSignature;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::view::read_admission_witness;
use crate::bead::read_model::ensure_cache_ready_at;
use crate::bead::read_model::ensure_cache_ready_for_mutation_at;
use crate::bead::read_model::fingerprint_manifest_config;
use crate::bead::read_model::publish_mutation_write;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::read_model::read_model_status;
use crate::bead::read_model::read_model_verify_cache_at;
use crate::bead::read_model::AppendedStream;
use crate::bead::read_model::PublishOutcome;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::fs_sig::mtime_ns;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use tempfile::tempdir;

fn git_store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    (temp, beads_dir)
}

fn seed_plan_with_task(beads_dir: &Path) -> (String, String) {
    let plan = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Publish plan".to_string(),
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
            title: "Indexed task".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(plan.id.clone()),
            size: Some(PhaseSizeWire::Small),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (plan.id, task.id)
}

/// A git-backed store with a warm cache: the seed writes publish
/// through the read model, so the measured mutation starts warm.
fn warm_store() -> (tempfile::TempDir, PathBuf, String, String) {
    let (temp, beads_dir) = git_store();
    let (plan_id, task_id) = seed_plan_with_task(&beads_dir);
    append_issue_note(
        &beads_dir,
        &task_id,
        "warming note",
        Some("agent".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    (temp, beads_dir, plan_id, task_id)
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

fn stream_event_count(beads_dir: &Path, stream_id: &str) -> usize {
    let path = event_streams_dir(beads_dir).join(format!("{stream_id}.jsonl"));
    read_event_stream_file(&path).unwrap().0.events.len()
}

#[test]
fn warm_create_publishes_direct_without_sweep_or_snapshot() {
    let (_temp, beads_dir, _plan_id, _task_id) = warm_store();
    store_io_stats::reset();
    let created = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Direct child".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(created.id.starts_with("sase-"));
    // One admission sweep; no snapshot load, no replay, one changed
    // stream signature, one published row.
    assert_eq!(store_io_stats::full_sweeps(), 1);
    assert_eq!(store_io_stats::snapshot_loads(), 0);
    assert_eq!(store_io_stats::full_replays(), 0);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_eq!(store_io_stats::published_rows(), 1);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "create next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "create");
}

#[test]
fn warm_note_append_publishes_direct_without_sweep_or_snapshot() {
    let (_temp, beads_dir, _plan_id, task_id) = warm_store();
    store_io_stats::reset();
    let outcome = append_issue_note(
        &beads_dir,
        &task_id,
        "direct note",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(note_text(outcome.issue.as_ref().unwrap()).contains("direct"));
    assert_eq!(store_io_stats::full_sweeps(), 1);
    assert_eq!(store_io_stats::snapshot_loads(), 0);
    assert_eq!(store_io_stats::full_replays(), 0);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_eq!(store_io_stats::published_rows(), 1);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "note next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "note");
}

#[test]
fn warm_update_publishes_direct_without_sweep_or_snapshot() {
    let (_temp, beads_dir, _plan_id, task_id) = warm_store();
    store_io_stats::reset();
    update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            title: Some("Renamed direct".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(store_io_stats::full_sweeps(), 1);
    assert_eq!(store_io_stats::snapshot_loads(), 0);
    assert_eq!(store_io_stats::full_replays(), 0);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_eq!(store_io_stats::published_rows(), 1);
    let cache_path = cache_path_for(&beads_dir);
    assert_token_only_next_read(&beads_dir, 1, "update next read");
    assert_cache_matches_replay(&beads_dir, &cache_path, "update");
}

/// Hand-append one note event to a stream file, mimicking the writer:
/// the event is serialized, the file grows by whole lines, and the
/// signature is captured from the stored bytes plus a handle on the
/// final path.
fn hand_append_note(
    beads_dir: &Path,
    stream_id: &str,
    issue_id: &str,
    timestamp: &str,
) -> (BeadEventRecordWire, StreamWriteSignature) {
    let path = event_streams_dir(beads_dir).join(format!("{stream_id}.jsonl"));
    let (stream, _) = read_event_stream_file(&path).unwrap();
    let ordinal = stream.events.len() + 1;
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "hand-appended note".to_string(),
        attachments: Vec::new(),
    };
    let event_id = mint_bead_event_id(
        stream_id,
        ordinal,
        timestamp,
        "tester",
        BeadEventOperationWire::NoteAppended,
        issue_id,
        &payload,
    )
    .unwrap();
    let event = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id,
        timestamp: timestamp.to_string(),
        actor: "tester".to_string(),
        operation: BeadEventOperationWire::NoteAppended,
        issue_id: issue_id.to_string(),
        payload,
    };
    event.validate().unwrap();
    let mut line = serde_json::to_string(&event).unwrap();
    line.push('\n');
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(line.as_bytes())
        .unwrap();
    let bytes = fs::read(&path).unwrap();
    let file = fs::File::open(&path).unwrap();
    let metadata = file.metadata().unwrap();
    let signature = StreamWriteSignature {
        size: metadata.len(),
        mtime_ns: mtime_ns(metadata.modified().ok()),
        inode: file_inode(&metadata),
        byte_len: bytes.len() as u64,
        content_hash: hex_signature(&bytes),
    };
    (event, signature)
}

fn admit(beads_dir: &Path) -> (PathBuf, crate::bead::read_model::CacheWitness) {
    let cache_path = cache_path_for(beads_dir);
    assert!(ensure_cache_ready_for_mutation_at(beads_dir, &cache_path).unwrap());
    let witness = read_admission_witness(&cache_path);
    (cache_path, witness)
}

#[test]
fn backdated_append_takes_synchronous_repair() {
    let (_temp, beads_dir, plan_id, task_id) = warm_store();
    let (cache_path, witness) = admit(&beads_dir);
    let status_before = read_model_status(&beads_dir);
    // A clock-skewed event sorting at or before the stored frontier:
    // the direct commit cannot prove equivalence, so publication runs
    // the same guarded tail-or-rebuild repair the old sweep-based path
    // ran, with identical outcome telemetry.
    let (event, signature) = hand_append_note(
        &beads_dir,
        &plan_id,
        &task_id,
        "2020-01-01T00:00:00Z",
    );
    let fingerprint = fingerprint_manifest_config(&beads_dir).unwrap();
    let appended = vec![AppendedStream {
        stream_id: plan_id.clone(),
        events: vec![event],
        signature,
    }];
    match publish_mutation_write(
        &beads_dir,
        &cache_path,
        &witness,
        &appended,
        &fingerprint,
        &[],
    ) {
        PublishOutcome::Published { .. } => {}
        PublishOutcome::Invalidated { reason } => {
            panic!("backdated append must repair, not invalidate: {reason}")
        }
        PublishOutcome::Skipped => panic!("backdated append must not skip"),
    }
    // Exactly one rebuild refresh: the repaired cache already equals
    // replay, so the next read serves without another refresh.
    let status_after = read_model_status(&beads_dir);
    assert_eq!(
        status_after.generation,
        status_before.generation + 1,
        "repair rebuilds exactly once"
    );
    assert_eq!(status_after.rebuild_count, status_before.rebuild_count + 1);
    assert!(cache_path.is_file());
    assert_cache_matches_replay(&beads_dir, &cache_path, "backdated repair");
    let replayed = reduces_to_store(&beads_dir);
    let task = replayed.iter().find(|issue| issue.id == task_id).unwrap();
    assert!(note_text(task).contains("hand-appended note"));
}

#[test]
fn concurrent_publication_loses_cas_without_stale_pairing() {
    let (_temp, beads_dir, plan_id, task_id) = warm_store();
    let base_events = stream_event_count(&beads_dir, &plan_id);
    let (cache_path, witness) = admit(&beads_dir);
    let (event, signature) = hand_append_note(
        &beads_dir,
        &plan_id,
        &task_id,
        "2026-01-01T00:03:00Z",
    );
    let fingerprint = fingerprint_manifest_config(&beads_dir).unwrap();
    let appended = vec![AppendedStream {
        stream_id: plan_id.clone(),
        events: vec![event],
        signature,
    }];
    // A doctored overlay: the reducer truth must win the outcome.
    let mut doctored = reduces_to_store(&beads_dir)
        .into_iter()
        .find(|issue| issue.id == task_id)
        .unwrap();
    doctored.title = "doctored overlay title".to_string();
    let expected = vec![(task_id.clone(), doctored)];
    let corrected = publish_mutation_write(
        &beads_dir,
        &cache_path,
        &witness,
        &appended,
        &fingerprint,
        &expected,
    )
    .corrections();
    assert_eq!(corrected.len(), 1);
    assert_eq!(corrected[0].0, task_id);
    assert_ne!(corrected[0].1.title, "doctored overlay title");
    // Replaying the same append with the now-stale witness loses the
    // content-generation race and writes nothing.
    let fingerprint = fingerprint_manifest_config(&beads_dir).unwrap();
    match publish_mutation_write(
        &beads_dir,
        &cache_path,
        &witness,
        &appended,
        &fingerprint,
        &expected,
    ) {
        PublishOutcome::Skipped => {}
        PublishOutcome::Published { .. } => {
            panic!("stale witness must not publish twice")
        }
        PublishOutcome::Invalidated { reason } => {
            panic!("stale witness must skip, not invalidate: {reason}")
        }
    }
    // Exactly one durable event; the winner's rows stand unpaired.
    assert_eq!(stream_event_count(&beads_dir, &plan_id), base_events + 1);
    assert_cache_matches_replay(&beads_dir, &cache_path, "cas winner");
}

#[test]
fn cache_fault_never_fails_the_mutation_or_duplicates_events() {
    let (_temp, beads_dir, plan_id, task_id) = warm_store();
    let base_events = stream_event_count(&beads_dir, &plan_id);
    // A foreign file at the cache path: admission heals it instead of
    // failing, and the indexed path still serves the mutation.
    let cache_path = cache_path_for(&beads_dir);
    fs::write(&cache_path, b"not a database").unwrap();
    store_io_stats::reset();
    let outcome = append_issue_note(
        &beads_dir,
        &task_id,
        "fault-tolerant note",
        Some("agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(note_text(outcome.issue.as_ref().unwrap())
        .contains("fault-tolerant note"));
    assert_eq!(
        stream_event_count(&beads_dir, &plan_id),
        base_events + 1,
        "exactly one durable event"
    );
    assert_eq!(store_io_stats::full_replays(), 0);
    assert_cache_matches_replay(&beads_dir, &cache_path, "fault repair");
}

#[test]
fn in_place_change_inside_window_is_caught_at_admission() {
    let (_temp, beads_dir, plan_id, task_id) = warm_store();
    // An unsupported in-place rewrite of published history: the next
    // admission sweep must see it even inside the 60 s token window.
    let path = event_streams_dir(&beads_dir).join(format!("{plan_id}.jsonl"));
    let bytes = fs::read(&path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("Indexed task"));
    fs::write(&path, text.replace("Indexed task", "Edited task")).unwrap();
    store_io_stats::reset();
    update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            title: Some("Post-edit title".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    // Admission sweep plus the post-rebuild guard sweep; no snapshot
    // load on the readiness path.
    assert_eq!(store_io_stats::full_sweeps(), 2);
    assert_eq!(store_io_stats::snapshot_loads(), 0);
    assert_cache_matches_replay(
        &beads_dir,
        &cache_path_for(&beads_dir),
        "edit",
    );
    let replayed = reduces_to_store(&beads_dir);
    let task = replayed.iter().find(|issue| issue.id == task_id).unwrap();
    assert_eq!(task.title, "Post-edit title");
}
