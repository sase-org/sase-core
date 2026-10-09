//! Proof-phase bounded work on a history-shaped fixture
//! (sase-1h8.13.1.7).
//!
//! A store with many closed lineages (an epic plan, three phases, twelve
//! tasks, most of them closed) proves the per-family claim directly: one
//! representative operation from every mutation family runs warm with
//! zero full replays and snapshot loads, exactly one admission sweep,
//! hydrated rows and stream reads bounded by the affected set, signature
//! rows written only for changed streams, a token-only next read, and
//! cache-equals-replay afterwards. Failure paths (descendant-guarded
//! close, invalid batch, old-version cache) keep the same bounds and
//! leave bytes untouched or heal through the replay fallback.

use super::super::*;
use super::support::*;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
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
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use crate::fs_sig::mtime_ns;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// A history-shaped store: one open epic, three phases (two closed),
/// twelve tasks (nine closed). Most rows are untouched closed history;
/// the open beads at the end are the bounded-work targets.
struct HistoryFixture {
    _temp: tempfile::TempDir,
    beads_dir: PathBuf,
    epic: String,
    open_phase: String,
    open_tasks: Vec<String>,
    closed_task: String,
}

fn history_fixture() -> HistoryFixture {
    let (temp, beads_dir) = mode_store(StoreMode::Cached);
    assert_cached_path_used(&beads_dir, "proof history fixture");
    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Proof epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            created_by: Some("proof-agent".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    let mut phases = Vec::new();
    for (index, title) in ["Alpha", "Beta", "Gamma"].iter().enumerate() {
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: format!("Phase {title}"),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.clone()),
                size: Some(PhaseSizeWire::Small),
                created_by: Some("proof-agent".to_string()),
                now: Some(format!("2026-01-01T00:{index:02}:00Z")),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        phases.push(phase);
    }
    let mut tasks = Vec::new();
    for index in 0..12 {
        let task = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: format!("Task {index}"),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                task_type: Some("bug".to_string()),
                created_by: Some("proof-agent".to_string()),
                now: Some(format!("2026-01-01T01:{index:02}:00Z")),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        tasks.push(task);
    }
    // Close two phases and nine tasks: the closed lineages are the
    // history the bounded reads must not touch.
    for (step, id) in phases[0..2].iter().enumerate() {
        close_issues(
            &beads_dir,
            std::slice::from_ref(id),
            Some("proof history".to_string()),
            None,
            false,
            Some(format!("2026-01-02T00:{step:02}:00Z")),
        )
        .unwrap();
    }
    for (step, id) in tasks[0..9].iter().enumerate() {
        close_issues(
            &beads_dir,
            std::slice::from_ref(id),
            Some("proof history".to_string()),
            None,
            false,
            Some(format!("2026-01-02T01:{step:02}:00Z")),
        )
        .unwrap();
    }
    HistoryFixture {
        _temp: temp,
        beads_dir,
        epic,
        open_phase: phases[2].clone(),
        open_tasks: tasks[9..12].to_vec(),
        closed_task: tasks[0].clone(),
    }
}

fn cache_path_for(beads_dir: &Path) -> PathBuf {
    read_model_cache_path_for_store(beads_dir)
        .expect("history fixture is git-backed")
}

/// One admission sweep, no snapshot load, no full replay.
fn assert_bounded(label: &str, max_rows: u64, max_streams: u64) {
    assert_eq!(
        store_io_stats::full_sweeps(),
        1,
        "{label}: one admission sweep"
    );
    assert_eq!(
        store_io_stats::snapshot_loads(),
        0,
        "{label}: cached path must not snapshot-load"
    );
    assert_eq!(
        store_io_stats::full_replays(),
        0,
        "{label}: cached path must not replay"
    );
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

fn assert_cache_matches(beads_dir: &Path, label: &str) {
    let cache_path = cache_path_for(beads_dir);
    let report = read_model_verify_cache_at(beads_dir, &cache_path);
    assert!(report.compared, "{label}: {}", report.reason);
    assert!(
        report.matched,
        "{label}: differing {:?}",
        report.differing_ids
    );
}

fn assert_token_only_next_read(beads_dir: &Path, label: &str) {
    let sweeps = store_io_stats::full_sweeps();
    let cache_path = cache_path_for(beads_dir);
    assert!(ensure_cache_ready_at(beads_dir, &cache_path).unwrap());
    assert_eq!(store_io_stats::full_sweeps(), sweeps, "{label}");
    assert_eq!(store_io_stats::snapshot_loads(), 0, "{label}");
}

#[test]
fn proof_history_close_with_note_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let target = fixture.open_tasks[0].clone();
    store_io_stats::reset();
    let outcome = close_issues_with_note(
        beads_dir,
        std::slice::from_ref(&target),
        Some("proof close".to_string()),
        None,
        false,
        Some("closing evidence".to_string()),
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(outcome.changed);
    assert_bounded("history close with note", 8, 4);
    // Only the closed bead's stream signature is rewritten.
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_token_only_next_read(beads_dir, "history close next read");
    assert_cache_matches(beads_dir, "history close with note");
}

#[test]
fn proof_history_open_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    store_io_stats::reset();
    let reopened = open_issue(
        beads_dir,
        &fixture.closed_task,
        Some("2026-02-01T00:01:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(reopened.id, fixture.closed_task);
    assert_eq!(reopened.status, StatusWire::Open);
    assert_bounded("history open", 8, 4);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_token_only_next_read(beads_dir, "history open next read");
    assert_cache_matches(beads_dir, "history open");
}

#[test]
fn proof_history_guarded_close_fails_without_writing() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    // The epic still owns an open phase: closing it without force must
    // fail exactly as the replay oracle fails it.
    let before = persisted_stream_files(beads_dir);
    store_io_stats::reset();
    let error = close_issues(
        beads_dir,
        std::slice::from_ref(&fixture.epic),
        Some("proof close".to_string()),
        None,
        false,
        Some("2026-02-01T00:02:00Z".to_string()),
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    assert!(
        error.message.contains("descendant(s) are not closed"),
        "unexpected guard text: {}",
        error.message
    );
    assert_eq!(
        persisted_stream_files(beads_dir),
        before,
        "a rejected close writes no bytes"
    );
    assert_cache_matches(beads_dir, "history guarded close");
}

#[test]
fn proof_history_remove_cascade_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    store_io_stats::reset();
    let outcome =
        remove_issues(beads_dir, std::slice::from_ref(&fixture.epic)).unwrap();
    assert_eq!(outcome.operation, "rm");
    assert_bounded("history remove cascade", 24, 8);
    // The cascade removes the epic and its three phases; the twelve
    // tasks are not descendants, so they survive with clean edges.
    let survivors = reduces_to_store(beads_dir);
    assert_eq!(survivors.len(), 12);
    assert_token_only_next_read(beads_dir, "history remove next read");
    assert_cache_matches(beads_dir, "history remove cascade");
}

#[test]
fn proof_history_remove_max_suffix_is_reused() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    // Removing the maximum child suffix frees it for the next child,
    // exactly as the replay oracle reuses it.
    store_io_stats::reset();
    remove_issues(beads_dir, std::slice::from_ref(&fixture.open_phase))
        .unwrap();
    assert_bounded("history remove max suffix", 12, 4);
    let reused = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Reused phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(fixture.epic.clone()),
            size: Some(PhaseSizeWire::Small),
            created_by: Some("proof-agent".to_string()),
            now: Some("2026-02-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    assert_eq!(
        reused, fixture.open_phase,
        "removing the maximum suffix frees it for reuse"
    );
    assert_cache_matches(beads_dir, "history remove max suffix");
}

#[test]
fn proof_history_claim_and_release_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[0].clone();
    store_io_stats::reset();
    let claimed = claim_for_agent_launch(
        beads_dir,
        &task,
        "proof-agent",
        Some("2026-02-01T00:04:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(claimed.assignee, "proof-agent");
    assert_bounded("history claim launch", 8, 4);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    store_io_stats::reset();
    release_agent_claim(
        beads_dir,
        &task,
        "proof-agent",
        Some("2026-02-01T00:05:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history claim release", 8, 4);
    assert_token_only_next_read(beads_dir, "history claim next read");
    assert_cache_matches(beads_dir, "history claim and release");
}

#[test]
fn proof_history_wait_claim_and_preclaim_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[1].clone();
    store_io_stats::reset();
    claim_for_agent_wait(
        beads_dir,
        &task,
        "proof-waiter",
        Some("2026-02-01T00:06:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history claim wait", 8, 4);
    // All-or-nothing preclaim over the epic's open phase.
    let assignments = vec![BeadPreclaimAssignmentWire {
        bead_id: fixture.open_phase.clone(),
        agent_name: "proof-worker".to_string(),
    }];
    store_io_stats::reset();
    let outcome = preclaim_epic_work_plan(
        beads_dir,
        &fixture.epic,
        &assignments,
        None,
        Some("2026-02-01T00:07:00Z".to_string()),
    )
    .unwrap();
    assert!(outcome.changed);
    assert_bounded("history preclaim", 12, 4);
    assert_token_only_next_read(beads_dir, "history preclaim next read");
    assert_cache_matches(beads_dir, "history wait claim and preclaim");
}

#[test]
fn proof_history_ready_marking_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    store_io_stats::reset();
    mark_ready_to_work(
        beads_dir,
        &fixture.epic,
        Some("2026-02-01T00:08:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history mark ready", 8, 4);
    store_io_stats::reset();
    unmark_ready_to_work(
        beads_dir,
        &fixture.epic,
        Some("2026-02-01T00:09:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history unmark ready", 8, 4);
    assert_token_only_next_read(beads_dir, "history ready next read");
    assert_cache_matches(beads_dir, "history ready marking");
}

#[test]
fn proof_history_dependencies_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let (first, second) =
        (fixture.open_tasks[0].clone(), fixture.open_tasks[1].clone());
    store_io_stats::reset();
    add_dependency(
        beads_dir,
        &second,
        &first,
        Some("2026-02-01T00:10:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history dependency add", 8, 4);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    let dependent = reduces_to_store(beads_dir)
        .into_iter()
        .find(|issue| issue.id == second)
        .unwrap();
    assert_eq!(dependent.dependencies.len(), 1);
    assert_eq!(dependent.dependencies[0].depends_on_id, first);
    store_io_stats::reset();
    remove_dependencies(
        beads_dir,
        &second,
        std::slice::from_ref(&first),
        Some("2026-02-01T00:11:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history dependency remove", 8, 4);
    assert_token_only_next_read(beads_dir, "history dependency next read");
    assert_cache_matches(beads_dir, "history dependencies");
}

#[test]
fn proof_history_references_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[2].clone();
    let reference = "artifact:proof-reference".to_string();
    store_io_stats::reset();
    add_bead_references(
        beads_dir,
        &task,
        std::slice::from_ref(&reference),
        Some("2026-02-01T00:12:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history reference add", 8, 4);
    store_io_stats::reset();
    remove_bead_references(
        beads_dir,
        &task,
        std::slice::from_ref(&reference),
        Some("2026-02-01T00:13:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history reference remove", 8, 4);
    assert_token_only_next_read(beads_dir, "history reference next read");
    assert_cache_matches(beads_dir, "history references");
}

#[test]
fn proof_history_links_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[0].clone();
    store_io_stats::reset();
    add_bead_link(
        beads_dir,
        &task,
        "artifact:proof-link",
        "related",
        "proof link",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-02-01T00:14:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_bounded("history link add", 8, 4);
    // A projection batch over the same endpoint stays bounded too.
    let projected = projection_request(
        &task,
        "artifact:proof-link",
        "related",
        BeadLinkDirectionWire::Out,
        true,
        &hex_operation_id(7),
        "proof projection",
        ArtifactLinkOriginWire::Manual,
        2,
        "2026-02-01T00:15:00Z",
    );
    store_io_stats::reset();
    set_bead_link_projections(beads_dir, &[projected]).unwrap();
    assert_bounded("history link projection", 8, 4);
    store_io_stats::reset();
    remove_bead_link(
        beads_dir,
        &task,
        "artifact:proof-link",
        Some("related"),
        BeadLinkDirectionWire::Out,
        Some("2026-02-01T00:16:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_bounded("history link remove", 8, 4);
    assert_token_only_next_read(beads_dir, "history link next read");
    assert_cache_matches(beads_dir, "history links");
}

#[test]
fn proof_history_plus_one_and_snooze_stay_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[1].clone();
    store_io_stats::reset();
    add_task_plus_one(
        beads_dir,
        &task,
        "proof-reporter",
        "proof evidence",
        &[],
        Some("2026-02-01T00:17:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert_bounded("history plus one", 8, 4);
    store_io_stats::reset();
    snooze_task(
        beads_dir,
        &task,
        "2026-03-01T00:00:00Z",
        None,
        "proof snooze",
        "proof-agent",
        Some("2026-02-01T00:18:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history snooze", 8, 4);
    store_io_stats::reset();
    cancel_task_snooze(
        beads_dir,
        &task,
        "proof-agent",
        Some("2026-02-01T00:19:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history snooze cancel", 8, 4);
    assert_token_only_next_read(beads_dir, "history evidence next read");
    assert_cache_matches(beads_dir, "history plus one and snooze");
}

#[test]
fn proof_history_notes_family_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[2].clone();
    store_io_stats::reset();
    let appended = append_issue_note(
        beads_dir,
        &task,
        "proof note",
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:20:00Z".to_string()),
        None,
    )
    .unwrap()
    .issue
    .unwrap();
    assert_bounded("history note append", 8, 4);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    let note_id = appended.notes.first().unwrap().id.clone();
    store_io_stats::reset();
    edit_issue_note(
        beads_dir,
        &task,
        &note_id,
        "proof note edited",
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:21:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_bounded("history note edit", 8, 4);
    // An update batch with an external-ref exchange across two plans.
    let first =
        create_plan_with_external_ref(beads_dir, "First", "bug:sase#42");
    let second =
        create_plan_with_external_ref(beads_dir, "Second", "bug:sase#43");
    store_io_stats::reset();
    update_issues(
        beads_dir,
        &[first.id.clone(), second.id.clone()],
        BeadUpdateFieldsWire {
            title: Some("Swapped".to_string()),
            now: Some("2026-02-01T00:22:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_bounded("history update batch", 8, 4);
    store_io_stats::reset();
    remove_issue_note(
        beads_dir,
        &task,
        &note_id,
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:23:00Z".to_string()),
    )
    .unwrap();
    assert_bounded("history note retract", 8, 4);
    assert_token_only_next_read(beads_dir, "history notes next read");
    assert_cache_matches(beads_dir, "history notes family");
}

#[test]
fn proof_history_write_to_closed_bead_stays_bounded() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    store_io_stats::reset();
    let outcome = append_issue_note(
        beads_dir,
        &fixture.closed_task,
        "note on closed bead",
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:24:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(outcome.changed);
    assert_bounded("history closed-bead note", 8, 4);
    assert_eq!(store_io_stats::sig_rows_written(), 1);
    assert_token_only_next_read(beads_dir, "history closed-bead next read");
    assert_cache_matches(beads_dir, "history write to closed bead");
}

#[test]
fn proof_history_noop_and_invalid_batch_leave_bytes_untouched() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let task = fixture.open_tasks[0].clone();
    let title = reduces_to_store(beads_dir)
        .into_iter()
        .find(|issue| issue.id == task)
        .unwrap()
        .title;
    // A no-op update writes no bytes.
    let before = persisted_stream_files(beads_dir);
    let noop = update_issue(
        beads_dir,
        &task,
        BeadUpdateFieldsWire {
            title: Some(title),
            now: Some("2026-02-01T00:25:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!noop.changed);
    assert_eq!(
        persisted_stream_files(beads_dir),
        before,
        "a no-op update writes no bytes"
    );
    // An invalid batch fails before anything is staged or written.
    let invalid = update_issues(
        beads_dir,
        &[task.clone(), "sase-zzz".to_string()],
        BeadUpdateFieldsWire {
            title: Some("Unreachable".to_string()),
            now: Some("2026-02-01T00:26:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(invalid.kind, "not_found");
    assert_eq!(
        persisted_stream_files(beads_dir),
        before,
        "an invalid batch writes no bytes"
    );
    assert_cache_matches(beads_dir, "history no-op and invalid batch");
}

#[test]
fn proof_history_old_version_cache_falls_back_and_heals() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let cache_path = cache_path_for(beads_dir);
    // Age the cache: the next admission must take the replay fallback
    // instead of serving stale rows.
    {
        let connection = rusqlite::Connection::open(&cache_path).unwrap();
        connection
            .execute(
                "UPDATE meta SET value = '0' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
    }
    let task = fixture.open_tasks[1].clone();
    let outcome = append_issue_note(
        beads_dir,
        &task,
        "old-version fallback note",
        Some("proof-agent".to_string()),
        Some("2026-02-01T00:27:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(note_text(outcome.issue.as_ref().unwrap())
        .contains("old-version fallback note"));
    // The fallback path replays; the next read rebuilds and heals, and
    // the healed cache equals replay.
    let cache_path = cache_path_for(beads_dir);
    assert!(ensure_cache_ready_at(beads_dir, &cache_path).unwrap());
    assert_cache_matches(beads_dir, "history old-version fallback");
    let noted = reduces_to_store(beads_dir)
        .into_iter()
        .find(|issue| issue.id == task)
        .unwrap();
    assert!(note_text(&noted).contains("old-version fallback note"));
}

/// Hand-append one note event to a stream file, mimicking the writer,
/// and capture the writer signature from the stored bytes.
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
        entry: "proof hand-appended note".to_string(),
        attachments: Vec::new(),
    };
    let event_id = mint_bead_event_id(
        stream_id,
        ordinal,
        timestamp,
        "proof-agent",
        BeadEventOperationWire::NoteAppended,
        issue_id,
        &payload,
    )
    .unwrap();
    let event = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id,
        timestamp: timestamp.to_string(),
        actor: "proof-agent".to_string(),
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

fn rewrite_config(beads_dir: &Path, mutate: impl Fn(&mut serde_json::Value)) {
    let path = beads_dir.join("config.json");
    let text = fs::read_to_string(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
    mutate(&mut value);
    fs::write(&path, serde_json::to_string(&value).unwrap()).unwrap();
}

#[test]
fn proof_counter_only_config_change_keeps_tail_open() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let (cache_path, witness) = admit(beads_dir);
    let status_before = read_model_status(beads_dir);
    // Minting bumps the allocation cursor; the fingerprint normalizes
    // it away, so the direct commit still applies through the tail.
    rewrite_config(beads_dir, |config| {
        let next = config["next_counter"].as_u64().unwrap_or(1);
        config["next_counter"] = serde_json::Value::from(next + 41);
    });
    let (event, signature) = hand_append_note(
        beads_dir,
        &fixture.epic,
        &fixture.epic,
        "2026-02-01T00:28:00Z",
    );
    let fingerprint = fingerprint_manifest_config(beads_dir).unwrap();
    let appended = vec![AppendedStream {
        stream_id: fixture.epic.clone(),
        events: vec![event],
        signature,
    }];
    match publish_mutation_write(
        beads_dir,
        &cache_path,
        &witness,
        &appended,
        &fingerprint,
        &[],
    ) {
        PublishOutcome::Published { .. } => {}
        PublishOutcome::Invalidated { reason } => {
            panic!("counter-only change must not invalidate: {reason}")
        }
        PublishOutcome::Skipped => panic!("counter-only change must publish"),
    }
    let status_after = read_model_status(beads_dir);
    assert_eq!(
        status_after.generation, status_before.generation,
        "a counter-only config change keeps the tail open"
    );
    assert_cache_matches(beads_dir, "history counter-only config");
}

#[test]
fn proof_non_counter_config_change_repairs() {
    let fixture = history_fixture();
    let beads_dir = &fixture.beads_dir;
    let (cache_path, witness) = admit(beads_dir);
    let status_before = read_model_status(beads_dir);
    // A real config edit between admission and publication: the direct
    // commit cannot prove equivalence, so publication runs the
    // synchronous guarded repair instead of pairing stale rows.
    rewrite_config(beads_dir, |config| {
        config["owner"] = serde_json::Value::from("proof-new-owner");
    });
    let (event, signature) = hand_append_note(
        beads_dir,
        &fixture.epic,
        &fixture.epic,
        "2026-02-01T00:29:00Z",
    );
    let fingerprint = fingerprint_manifest_config(beads_dir).unwrap();
    let appended = vec![AppendedStream {
        stream_id: fixture.epic.clone(),
        events: vec![event],
        signature,
    }];
    match publish_mutation_write(
        beads_dir,
        &cache_path,
        &witness,
        &appended,
        &fingerprint,
        &[],
    ) {
        PublishOutcome::Published { .. } => {}
        PublishOutcome::Invalidated { reason } => {
            panic!("config change must repair, not invalidate: {reason}")
        }
        PublishOutcome::Skipped => panic!("config change must not skip"),
    }
    let status_after = read_model_status(beads_dir);
    assert_eq!(
        status_after.generation,
        status_before.generation + 1,
        "a non-counter config change rebuilds exactly once"
    );
    assert_eq!(status_after.rebuild_count, status_before.rebuild_count + 1);
    assert_cache_matches(beads_dir, "history non-counter config");
}
