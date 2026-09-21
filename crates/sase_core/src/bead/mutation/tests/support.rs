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
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use tempfile::tempdir;

use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::bead::jsonl::import_issues_from_jsonl;
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
    let temp = tempdir().unwrap();
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
    let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    export_jsonl(beads_dir).unwrap();
    let after = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    assert_eq!(before, after, "{label}");
}

pub(super) fn external_ref_store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    (temp, beads_dir)
}

pub(super) fn multi_stream_store() -> (tempfile::TempDir, PathBuf, Vec<String>)
{
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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

/// The issue the mutation wrote to `issues.jsonl`, and the same issue as
/// the reducer projects it from the store's event streams.
pub(super) fn projected_and_reduced(
    beads_dir: &Path,
    issue_id: &str,
) -> (IssueWire, IssueWire) {
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
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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
