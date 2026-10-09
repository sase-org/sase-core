use super::super::*;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::mutation_status_value;
use crate::bead::wire::notes_text;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::bead::jsonl::export_issues_to_jsonl;
use crate::bead::jsonl::import_issues_from_jsonl;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::read_model::ensure_cache_ready_at;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::read_model::read_model_verify_cache_at;
pub(super) fn note_text(issue: &IssueWire) -> String {
    notes_text(&issue.notes)
}

pub(super) fn task_plus_one_fixture(
    status: StatusWire,
) -> (tempfile::TempDir, PathBuf, String) {
    task_plus_one_fixture_with_assignee(status, "")
}

pub(super) fn task_plus_one_fixture_with_assignee(
    status: StatusWire,
    assignee: &str,
) -> (tempfile::TempDir, PathBuf, String) {
    let temp = dual_tempdir();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Corroborated task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            assignee: assignee.to_string(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    if status == StatusWire::Closed {
        close_issues(
            &beads_dir,
            std::slice::from_ref(&task.id),
            Some("stale close".to_string()),
            Some(BeadResolutionWire::Canceled),
            false,
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap();
    } else if status != StatusWire::Open {
        update_issue(
            &beads_dir,
            &task.id,
            BeadUpdateFieldsWire {
                status: Some(mutation_status_value(&status).to_string()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    (temp, beads_dir, task.id)
}

/// A snoozed task, snoozed at `2026-01-01T00:02:00Z` until `until`.
pub(super) fn snoozed_task_fixture(
    until: &str,
    plus_ones: Option<u32>,
) -> (tempfile::TempDir, PathBuf, String) {
    let (temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);
    snooze_task(
        &beads_dir,
        &task_id,
        until,
        plus_ones,
        " needs the upstream fix first ",
        "bryanbugyi34@gmail.com",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    (temp, beads_dir, task_id)
}

pub(super) fn reduces_to_store(beads_dir: &Path) -> Vec<IssueWire> {
    let (_manifest, streams) = read_event_store(beads_dir).unwrap();
    reduce_event_streams(&streams).unwrap()
}

pub(super) fn assert_reprojection_byte_stable(beads_dir: &Path, label: &str) {
    // projection-off: mutations never rewrite `issues.jsonl`, so the
    // on-demand export must reproduce the replayed state byte for byte,
    // and re-exporting must be a fixed point.
    export_jsonl(beads_dir).unwrap();
    let exported = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    export_jsonl(beads_dir).unwrap();
    let reread = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    assert_eq!(exported, reread, "{label}");
    let reduced = reduces_to_store(beads_dir);
    let replayed = export_issues_to_jsonl(&reduced).unwrap();
    assert_eq!(exported, replayed.as_bytes(), "{label}");
}

pub(super) fn external_ref_store() -> (tempfile::TempDir, PathBuf) {
    let temp = dual_tempdir();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    (temp, beads_dir)
}

pub(super) fn multi_stream_store() -> (tempfile::TempDir, PathBuf, Vec<String>)
{
    let temp = dual_tempdir();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let mut ids = Vec::new();
    for index in 0..3 {
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: format!("Plan {}", index + 1),
                issue_type: IssueTypeWire::Plan,
                now: Some(format!("2026-01-01T00:0{index}:00Z")),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        ids.push(issue.id);
    }
    (temp, beads_dir, ids)
}

pub(super) fn create_plan_with_external_ref(
    beads_dir: &Path,
    title: &str,
    external_ref: &str,
) -> IssueWire {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Plan,
            external_ref: external_ref.to_string(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn projection_request(
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
    present: bool,
    operation_id: &str,
    description: &str,
    origin: ArtifactLinkOriginWire,
    uses: u64,
    now: &str,
) -> BeadLinkProjectionRequestWire {
    BeadLinkProjectionRequestWire {
        issue_id: issue_id.to_string(),
        target_ref: target_ref.to_string(),
        relation: relation.to_string(),
        direction,
        present,
        operation_id: operation_id.to_string(),
        description: Some(description.to_string()),
        origin: Some(origin),
        uses,
        now: Some(now.to_string()),
    }
}

pub(super) fn absent_projection_request(
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
    operation_id: &str,
    now: &str,
) -> BeadLinkProjectionRequestWire {
    BeadLinkProjectionRequestWire {
        issue_id: issue_id.to_string(),
        target_ref: target_ref.to_string(),
        relation: relation.to_string(),
        direction,
        present: false,
        operation_id: operation_id.to_string(),
        now: Some(now.to_string()),
        ..Default::default()
    }
}

pub(super) fn hex_operation_id(n: u64) -> String {
    format!("{n:032x}")
}

pub(super) fn copy_dir(src: &Path, dest: &Path) {
    fs::create_dir_all(dest).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let dest_path = dest.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest_path);
        } else {
            fs::copy(entry.path(), dest_path).unwrap();
        }
    }
}

pub(super) fn two_issue_store() -> (tempfile::TempDir, PathBuf, String, String)
{
    let temp = dual_tempdir();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let first = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Alpha".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let second = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Beta".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:01Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (temp, beads_dir, first.id, second.id)
}

pub(super) fn dependency_mutation_fixture(
) -> (tempfile::TempDir, PathBuf, String, Vec<String>) {
    let temp = dual_tempdir();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let source = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Source".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let first = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First target".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let second = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Second target".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    add_dependency(
        &beads_dir,
        &source.id,
        &first.id,
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    add_dependency(
        &beads_dir,
        &source.id,
        &second.id,
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();

    (temp, beads_dir, source.id, vec![first.id, second.id])
}

/// The issue as the on-demand `issues.jsonl` export projects it, and the
/// same issue as the reducer projects it from the store's event streams.
pub(super) fn projected_and_reduced(
    beads_dir: &Path,
    issue_id: &str,
) -> (IssueWire, IssueWire) {
    export_jsonl(beads_dir).unwrap();
    let projected = import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))
        .unwrap()
        .issues
        .into_iter()
        .find(|issue| issue.id == issue_id)
        .unwrap();
    let (_manifest, streams) = read_event_store(beads_dir).unwrap();
    let reduced = reduce_event_streams(&streams)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == issue_id)
        .unwrap();
    (projected, reduced)
}

pub(super) fn assert_reopen_parity(
    beads_dir: &Path,
    issue_id: &str,
    label: &str,
) {
    let (projected, reduced) = projected_and_reduced(beads_dir, issue_id);
    assert_eq!(projected, reduced, "{label}");
}

/// An epic plan with one phase child and one task, all in one store.
pub(super) fn close_history_fixture(
) -> (tempfile::TempDir, PathBuf, Vec<String>) {
    let temp = dual_tempdir();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
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
            now: Some("2026-01-01T00:00:01Z".to_string()),
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
            now: Some("2026-01-01T00:00:02Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (temp, beads_dir, vec![epic.id, phase.id, task.id])
}

pub(super) fn close_for_history(beads_dir: &Path, issue_id: &str, now: &str) {
    close_issues(
        beads_dir,
        &[issue_id.to_string()],
        Some("Not reproducible on main.".to_string()),
        Some(BeadResolutionWire::Canceled),
        false,
        Some(now.to_string()),
    )
    .unwrap();
}

pub(super) fn issue(
    id: &str,
    title: &str,
    issue_type: &str,
    parent_id: Option<&str>,
    status: &str,
    timestamp: &str,
) -> String {
    let parent = parent_id
        .map_or_else(|| "null".to_string(), |value| format!(r#""{value}""#));
    format!(
        r#"{{"id":"{id}","title":"{title}","status":"{status}","issue_type":"{issue_type}","parent_id":{parent},"owner":"","assignee":"","created_at":"{timestamp}","created_by":"","updated_at":"{timestamp}","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}}"#
    )
}

pub(super) fn closed_issue_fixture(
    resolution: BeadResolutionWire,
    reason: Option<&str>,
) -> (tempfile::TempDir, PathBuf, String) {
    let temp = dual_tempdir();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue_id = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Closed issue".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    close_issues(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        reason.map(str::to_string),
        Some(resolution),
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();
    (temp, beads_dir, issue_id)
}

pub(super) fn claim_mutation_fixture() -> (tempfile::TempDir, PathBuf, String) {
    let temp = dual_tempdir();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

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
    let phase = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    (temp, beads_dir, phase.id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PersistedStreamFile {
    pub(super) bytes: Vec<u8>,
    pub(super) modified: SystemTime,
}

pub(super) fn persisted_stream_files(
    beads_dir: &Path,
) -> BTreeMap<String, PersistedStreamFile> {
    let streams_dir = beads_dir.join("events/streams");
    fs::read_dir(streams_dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            (
                entry.file_name().to_string_lossy().into_owned(),
                PersistedStreamFile {
                    bytes: fs::read(&path).unwrap(),
                    modified: fs::metadata(&path).unwrap().modified().unwrap(),
                },
            )
        })
        .collect()
}

pub(super) fn reordered_event_json_object(line: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(line).unwrap();
    let object = value.as_object().unwrap();
    format!(
        r#"{{"payload":{},"issue_id":{},"operation":{},"actor":{},"timestamp":{},"event_id":{},"schema_version":{}}}"#,
        serde_json::to_string(object.get("payload").unwrap()).unwrap(),
        serde_json::to_string(object.get("issue_id").unwrap()).unwrap(),
        serde_json::to_string(object.get("operation").unwrap()).unwrap(),
        serde_json::to_string(object.get("actor").unwrap()).unwrap(),
        serde_json::to_string(object.get("timestamp").unwrap()).unwrap(),
        serde_json::to_string(object.get("event_id").unwrap()).unwrap(),
        serde_json::to_string(object.get("schema_version").unwrap()).unwrap(),
    )
}

pub(super) fn persisted_claim_state(
    beads_dir: &Path,
) -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
    let issues = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    let streams_dir = beads_dir.join("events/streams");
    let mut streams: Vec<_> = fs::read_dir(streams_dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    streams.sort_by(|left, right| left.0.cmp(&right.0));
    (issues, streams)
}

pub(super) fn batch_remove_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = dual_tempdir();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "Plan",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "First child",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-1.2",
                "Second child",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:02:00Z",
            ),
            issue(
                "sase-2",
                "Independent",
                "plan",
                None,
                "open",
                "2026-01-01T00:03:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();
    (temp, beads_dir)
}

/// Which backing a dual-mode test store uses.
///
/// `Cached` creates the `.git` dir that gives the store a read-model cache
/// path, so mutations run the cached path with its replay fallback.
/// `Replay` leaves the store without a git dir, so every mutation and read
/// replays the event streams. Tests branch on this enum through the helpers
/// below instead of `macro_rules!`, which the workspace forbids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreMode {
    Cached,
    Replay,
}

/// One store for a dual-mode run: `Cached` is git-backed (the read model is
/// used), `Replay` is plain (full replay). Both seed through `init_store`
/// with the same prefix and owner, so identical scenarios mint identical
/// IDs on both backings.
pub(super) fn mode_store(mode: StoreMode) -> (tempfile::TempDir, PathBuf) {
    let temp = dual_tempdir();
    if mode == StoreMode::Cached {
        fs::create_dir_all(temp.path().join(".git")).unwrap();
    }
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    (temp, beads_dir)
}

/// Assert the store's read model equals a full replay.
///
/// Refresh through the normal read path, then compare against a forced
/// replay. On a replay store there is no cache to compare, so this is a
/// no-op there. Stores that never grew an event store (validation-only
/// tests) are also a no-op: there is nothing to compare yet.
pub(super) fn assert_cache_equals_replay(beads_dir: &Path, label: &str) {
    let Some(cache_path) = read_model_cache_path_for_store(beads_dir) else {
        return;
    };
    if !crate::bead::jsonl::event_store_present(beads_dir) {
        return;
    }
    ensure_cache_ready_at(beads_dir, &cache_path).unwrap();
    let report = read_model_verify_cache_at(beads_dir, &cache_path);
    assert!(report.compared, "{label}: {}", report.reason);
    assert!(
        report.matched,
        "{label}: differing {:?}: {}",
        report.differing_ids, report.reason
    );
}

/// Assert the store actually has a read-model cache path (the cached half
/// of a dual-mode run), so a zero-replay assertion cannot pass vacuously on
/// a replay store.
pub(super) fn assert_cached_path_used(beads_dir: &Path, label: &str) {
    assert!(
        read_model_cache_path_for_store(beads_dir).is_some(),
        "{label}: expected a git-backed cached store"
    );
}

/// Assert no full-store replay ran since the last `store_io_stats::reset`.
/// Only the already-cached create/note/update paths may assert this;
/// replay-fallback families still replay by design until ported.
pub(super) fn assert_no_full_replay(label: &str) {
    assert_eq!(
        store_io_stats::full_replays(),
        0,
        "{label}: cached path must not replay"
    );
}

thread_local! {
    static DUAL_TEST_MODE: Cell<Option<StoreMode>> =
        const { Cell::new(None) };
    static DUAL_TRACKED_TEMPS: RefCell<Vec<PathBuf>> =
        const { RefCell::new(Vec::new()) };
}

/// Both backings every suite-mode test runs against, without `macro_rules!`.
///
/// Tests iterate this array through [`run_dual_mode_test`]; the `Cached`
/// half creates the `.git` dir that admits the cached path, while `Replay`
/// leaves the store without one so every mutation replays.
pub(super) fn all_store_modes() -> [StoreMode; 2] {
    [StoreMode::Cached, StoreMode::Replay]
}

/// Mode-aware `tempdir` replacement for the nine dual-mode suites.
///
/// Reads the thread-local mode installed by [`run_dual_mode_test`]. Under
/// `Cached` it pre-creates a `.git` dir at every plausible parent of a
/// beads dir under the temp (`temp/.git` covers `temp/beads`, and
/// `temp/sdd/.git` covers `temp/sdd/beads`), so any layout the test builds
/// is admitted to the cached path. Under `Replay` (or outside a dual test)
/// it creates a plain temp. Every temp is tracked so the cached iteration
/// can assert parity over every store the test built.
pub(super) fn dual_tempdir() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    if DUAL_TEST_MODE.get() == Some(StoreMode::Cached) {
        for rel in [
            ".git",
            "beads/.git",
            "sdd/.git",
            "sdd/beads/.git",
            "sase/.git",
            "sase/sdd/.git",
            "sase/sdd/beads/.git",
            "singleton-root/.git",
            "singleton-root/beads/.git",
        ] {
            fs::create_dir_all(temp.path().join(rel)).unwrap();
        }
    }
    DUAL_TRACKED_TEMPS
        .with(|tracked| tracked.borrow_mut().push(temp.path().to_path_buf()));
    temp
}

fn collect_beads_dirs(dir: &Path, out: &mut Vec<PathBuf>) {
    if dir.join("config.json").is_file() {
        out.push(dir.to_path_buf());
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == ".git" || name == "target")
            {
                continue;
            }
            collect_beads_dirs(&path, out);
        }
    }
}

/// Run `test` once per backing.
///
/// The closure runs first with `Cached` then with `Replay`, each on fresh
/// temp stores. Each wrapped test ends its closure with
/// [`assert_dual_mode_parity_for_current_mode`], which asserts
/// cache-equals-replay over every store the run created while those temps
/// are still alive. While the closure panics the parity check is skipped
/// (the test failure itself is the signal).
pub(super) fn run_dual_mode_test(test: impl Fn(StoreMode)) {
    for mode in all_store_modes() {
        DUAL_TEST_MODE.set(Some(mode));
        DUAL_TRACKED_TEMPS.with(|tracked| tracked.borrow_mut().clear());
        test(mode);
        DUAL_TEST_MODE.set(None);
    }
}

/// Assert cache-equals-replay for the current dual-mode run, if cached.
///
/// Call at the end of every [`run_dual_mode_test`] closure while its temp
/// stores are still alive. On the cached half, every beads dir found under
/// every temp the run created must have a cache path and its read model
/// must equal a full replay. On the replay half this is a no-op. For tests
/// whose operation has a cached path (every family now), the cached run
/// must actually have used it: the cache-path assertion guards against a
/// vacuous replay pass.
pub(super) fn assert_dual_mode_parity_for_current_mode() {
    if DUAL_TEST_MODE.get() != Some(StoreMode::Cached) {
        return;
    }
    let temps = DUAL_TRACKED_TEMPS.with(|tracked| tracked.borrow().clone());
    // Storeless unit tests (pure label/format checks) create no temp;
    // there is nothing to compare, so parity is a no-op for them.
    if temps.is_empty() {
        return;
    }
    let mut beads_dirs = Vec::new();
    for temp in &temps {
        collect_beads_dirs(temp, &mut beads_dirs);
    }
    if beads_dirs.is_empty() {
        return;
    }
    for beads_dir in &beads_dirs {
        assert_cached_path_used(beads_dir, "dual-mode");
        assert_cache_equals_replay(beads_dir, "dual-mode");
    }
}
