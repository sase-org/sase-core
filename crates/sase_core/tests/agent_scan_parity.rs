//! Phase 3B parity gate: build the same synthetic artifact tree the
//! Python `agent_scan_golden::fixture_builder` writes, run the Rust
//! scanner, and assert the same record/stats expectations
//! `sase_100/tests/test_core_agent_scan.py` pins on the Python facade.
//!
//! The fixture is materialized programmatically (rather than copied from
//! Python) so this crate stays self-contained — no Python toolchain is
//! required to run `cargo test`. The timestamp constants and JSON
//! payloads are byte-for-byte equivalent to
//! `sase_100/tests/agent_scan_golden/fixture_builder.py`. If that file
//! changes, mirror the change here.

use std::fs;
use std::path::{Path, PathBuf};

use sase_core::agent_scan::{
    scan_agent_artifacts, AgentArtifactScanOptionsWire,
};
use sase_core::AGENT_SCAN_WIRE_SCHEMA_VERSION;
use serde_json::{json, Value};
use tempfile::tempdir;

const TS_HOME_RUNNING: &str = "20260427100000";
const TS_ACE_RUN_RUNNING: &str = "20260427110000";
const TS_ACE_RUN_DONE: &str = "20260427120000";
const TS_ACE_RUN_FAILED: &str = "20260427130000";
const TS_ACE_RUN_RETRIED_PARENT: &str = "20260427140000";
const TS_ACE_RUN_RETRIED_CHILD: &str = "20260427140500";
const TS_WORKFLOW_ROOT: &str = "20260427150000";
const TS_MENTOR_DONE: &str = "20260427160000";
const TS_WAITING: &str = "20260427170000";
const TS_MALFORMED: &str = "20260427180000";
const TS_PENDING_QUESTION: &str = "20260427190000";

const EXPECTED_TIMESTAMPS: &[&str] = &[
    TS_HOME_RUNNING,
    TS_ACE_RUN_RUNNING,
    TS_ACE_RUN_DONE,
    TS_ACE_RUN_FAILED,
    TS_ACE_RUN_RETRIED_PARENT,
    TS_ACE_RUN_RETRIED_CHILD,
    TS_WORKFLOW_ROOT,
    TS_MENTOR_DONE,
    TS_WAITING,
    TS_MALFORMED,
    TS_PENDING_QUESTION,
];

const EXPECTED_DECODE_ERRORS: u64 = 2;
const EXPECTED_OS_ERRORS: u64 = 0;

fn write_json(path: &Path, payload: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let s = serde_json::to_string_pretty(payload).unwrap();
    fs::write(path, s).unwrap();
}

fn write_text(path: &Path, content: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn build_fixture_tree(root: &Path) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    build_home_running(root);
    build_ace_run_running(root);
    build_ace_run_done(root);
    build_ace_run_failed(root);
    build_ace_run_retried(root);
    build_workflow(root);
    build_mentor_done(root);
    build_waiting(root);
    build_malformed(root);
    build_pending_question(root);
    root.to_path_buf()
}

fn build_home_running(root: &Path) {
    let dir = root
        .join("home")
        .join("artifacts")
        .join("ace-run")
        .join(TS_HOME_RUNNING);
    write_json(
        &dir.join("running.json"),
        &json!({
            "pid": 11111,
            "cl_name": "~",
            "model": "claude-opus-4-7",
            "llm_provider": "claude",
            "vcs_provider": "github",
            "workspace_dir": "/tmp/home-target",
        }),
    );
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "home_runner",
            "pid": 11111,
            "model": "claude-opus-4-7",
            "llm_provider": "claude",
            "vcs_provider": "github",
            "workspace_dir": "/tmp/home-target",
            "approve": true,
            "run_started_at": "2026-04-27T10:00:00Z",
        }),
    );
    write_text(&dir.join("raw_xprompt.md"), "Investigate the failing job\n");
}

fn build_ace_run_running(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_ACE_RUN_RUNNING);
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "running_alpha",
            "workflow_name": "wf_alpha",
            "pid": 22222,
            "model": "claude-sonnet-4-6",
            "llm_provider": "claude",
            "vcs_provider": "github",
            "workspace_dir": "/tmp/workspaces/alpha",
            "approve": false,
            "plan": true,
            "plan_approved": false,
            "wait_for": ["bob", "carol"],
            "wait_duration": 3600.0,
        }),
    );
}

fn build_ace_run_done(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_ACE_RUN_DONE);
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "done_alpha",
            "model": "claude-haiku-4-5-20251001",
            "llm_provider": "claude",
            "vcs_provider": "github",
            "workspace_dir": "/tmp/workspaces/alpha_3",
            "stopped_at": "2026-04-27T12:05:00Z",
        }),
    );
    write_json(
        &dir.join("done.json"),
        &json!({
            "outcome": "completed",
            "finished_at": 1745758800.0,
            "cl_name": "feature_alpha",
            "project_file": "/home/u/.sase/projects/myproj/myproj.sase",
            "workspace_num": 3,
            "workspace_dir": "/tmp/workspaces/alpha_3",
            "model": "claude-haiku-4-5-20251001",
            "llm_provider": "claude",
            "vcs_provider": "github",
            "name": "done_alpha",
            "diff_path": "/tmp/diff_alpha.diff",
            "markdown_pdf_paths": ["/tmp/markdown_pdfs/notes.pdf"],
            "image_paths": ["/tmp/images/alpha.png"],
            "response_path": "/tmp/resp_alpha.md",
            "output_path": "/tmp/out_alpha.log",
        }),
    );
}

fn build_ace_run_failed(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_ACE_RUN_FAILED);
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "failed_beta",
            "model": "claude-sonnet-4-6",
            "llm_provider": "claude",
            "stopped_at": "2026-04-27T13:10:00Z",
        }),
    );
    write_json(
        &dir.join("done.json"),
        &json!({
            "outcome": "failed",
            "finished_at": 1745762400.0,
            "cl_name": "feature_beta",
            "name": "failed_beta",
            "error": "RuntimeError: kaboom",
            "traceback": "Traceback (most recent call last):\n  ...",
        }),
    );
}

fn build_ace_run_retried(root: &Path) {
    let parent = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_ACE_RUN_RETRIED_PARENT);
    write_json(
        &parent.join("agent_meta.json"),
        &json!({
            "name": "retry_root",
            "model": "claude-opus-4-7",
            "llm_provider": "claude",
            "retry_chain_root_timestamp": TS_ACE_RUN_RETRIED_PARENT,
            "retried_as_timestamp": TS_ACE_RUN_RETRIED_CHILD,
            "retry_terminal": true,
            "retry_error_category": "transient",
        }),
    );
    write_json(
        &parent.join("done.json"),
        &json!({
            "outcome": "failed",
            "name": "retry_root",
            "error": "Temporary upstream failure",
            "retried_as_timestamp": TS_ACE_RUN_RETRIED_CHILD,
            "retry_chain_root_timestamp": TS_ACE_RUN_RETRIED_PARENT,
            "retry_error_category": "transient",
        }),
    );

    let child = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_ACE_RUN_RETRIED_CHILD);
    write_json(
        &child.join("agent_meta.json"),
        &json!({
            "name": "retry_root",
            "model": "claude-opus-4-7",
            "llm_provider": "claude",
            "retry_of_timestamp": TS_ACE_RUN_RETRIED_PARENT,
            "retry_attempt": 1,
            "retry_chain_root_timestamp": TS_ACE_RUN_RETRIED_PARENT,
            "pid": 33333,
        }),
    );
}

fn build_workflow(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("workflow-three_phase")
        .join(TS_WORKFLOW_ROOT);
    write_json(
        &dir.join("workflow_state.json"),
        &json!({
            "workflow_name": "three_phase",
            "context": {"cl_name": "feature_workflow"},
            "status": "completed",
            "pid": 44444,
            "appears_as_agent": true,
            "is_anonymous": false,
            "current_step_index": 2,
            "start_time": "2026-04-27T15:00:00",
            "activity": "PDF 2/5 docs/notes.md",
            "pdf_status": {
                "stage": "engine_started",
                "index": 2,
                "total": 5,
                "source_path": "docs/notes.md",
                "engine": "wkhtmltopdf",
            },
            "steps": [
                {
                    "name": "plan",
                    "status": "completed",
                    "output": {"plan_path": "/tmp/plan.md"},
                    "output_types": {"plan_path": "path"},
                },
                {
                    "name": "code",
                    "status": "completed",
                    "output": {"diff_path": "/tmp/diff.diff"},
                    "output_types": {"diff_path": "path"},
                },
                {
                    "name": "review",
                    "status": "completed",
                    "output": {"response_path": "/tmp/review.md"},
                },
            ],
        }),
    );
    write_json(
        &dir.join("plan_path.json"),
        &json!({"plan_path": "/tmp/plan.md"}),
    );
    write_json(
        &dir.join("prompt_step_001_plan.json"),
        &json!({
            "workflow_name": "three_phase",
            "step_name": "plan",
            "step_type": "agent",
            "step_index": 0,
            "total_steps": 3,
            "status": "completed",
            "model": "claude-opus-4-7",
            "llm_provider": "claude",
            "output": {"meta_workspace": "5", "plan_path": "/tmp/plan.md"},
            "output_types": {"plan_path": "path"},
            "artifacts_dir": dir.to_string_lossy(),
        }),
    );
    write_json(
        &dir.join("prompt_step_002_code.json"),
        &json!({
            "workflow_name": "three_phase",
            "step_name": "code",
            "step_type": "agent",
            "step_index": 1,
            "total_steps": 3,
            "status": "completed",
            "hidden": true,
            "diff_path": "/tmp/diff.diff",
            "output": {"diff_path": "/tmp/diff.diff"},
            "output_types": {"diff_path": "path"},
        }),
    );
    write_json(
        &dir.join("prompt_step_000_pre.json"),
        &json!({
            "workflow_name": "three_phase",
            "step_name": "pre",
            "step_type": "python",
            "step_index": -1,
            "total_steps": 3,
            "status": "completed",
            "is_pre_prompt_step": true,
            "embedded_workflow_name": "three_phase",
        }),
    );
}

fn build_mentor_done(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("mentor-bryan")
        .join(TS_MENTOR_DONE);
    write_json(
        &dir.join("done.json"),
        &json!({
            "outcome": "completed",
            "cl_name": "feature_alpha",
            "name": "mentor_review",
            "response_path": "/tmp/mentor.md",
        }),
    );
}

fn build_waiting(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_WAITING);
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "waiter",
            "wait_for": ["upstream"],
            "wait_duration": 600.0,
        }),
    );
    // Intentionally malformed waiting.json; the scanner counts it as a
    // json_decode_error and continues without crashing.
    write_text(&dir.join("waiting.json"), "{ not json");
}

fn build_malformed(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_MALFORMED);
    write_text(&dir.join("agent_meta.json"), "{not json}");
    // No done.json; the scanner picks up the corrupt agent_meta.json only.
}

fn build_pending_question(root: &Path) {
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join(TS_PENDING_QUESTION);
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "asker",
            "pid": 33333,
        }),
    );
    write_json(
        &dir.join("pending_question.json"),
        &json!({
            "session_id": "ses-abc",
            "request_path": "/tmp/user_question/ses-abc/question_request.json",
            "submitted_at": "2026-04-27T19:00:00+00:00",
        }),
    );
}

fn record_by_timestamp<'a>(
    snapshot: &'a sase_core::AgentArtifactScanWire,
    timestamp: &str,
) -> &'a sase_core::AgentArtifactRecordWire {
    let matches: Vec<_> = snapshot
        .records
        .iter()
        .filter(|r| r.timestamp == timestamp)
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one record with ts={timestamp}, got {}",
        matches.len()
    );
    matches[0]
}

#[test]
fn scan_returns_one_record_per_artifact_dir() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());

    let mut timestamps: Vec<&str> = snapshot
        .records
        .iter()
        .map(|r| r.timestamp.as_str())
        .collect();
    timestamps.sort();
    let mut expected: Vec<&str> = EXPECTED_TIMESTAMPS.to_vec();
    expected.sort();
    assert_eq!(timestamps, expected);

    assert_eq!(snapshot.schema_version, AGENT_SCAN_WIRE_SCHEMA_VERSION);
    assert_eq!(snapshot.projects_root, root.to_string_lossy());
}

#[test]
fn records_are_sorted_deterministically() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());

    let keys: Vec<(&str, &str, &str)> = snapshot
        .records
        .iter()
        .map(|r| {
            (
                r.project_name.as_str(),
                r.workflow_dir_name.as_str(),
                r.timestamp.as_str(),
            )
        })
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn stats_count_decode_errors() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());

    assert_eq!(snapshot.stats.json_decode_errors, EXPECTED_DECODE_ERRORS);
    assert_eq!(snapshot.stats.os_errors, EXPECTED_OS_ERRORS);
    assert_eq!(snapshot.stats.projects_visited, 2);
    assert_eq!(
        snapshot.stats.artifact_dirs_visited as usize,
        EXPECTED_TIMESTAMPS.len()
    );
    assert!(snapshot.stats.marker_files_parsed > 0);
    assert_eq!(snapshot.stats.prompt_step_markers_parsed, 3);
}

#[test]
fn running_record_carries_agent_meta() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_ACE_RUN_RUNNING);

    assert_eq!(rec.workflow_dir_name, "ace-run");
    assert_eq!(rec.project_name, "myproj");
    assert!(!rec.has_done_marker);
    assert!(rec.done.is_none());

    let meta = rec.agent_meta.as_ref().unwrap();
    assert_eq!(meta.name.as_deref(), Some("running_alpha"));
    assert_eq!(meta.workflow_name.as_deref(), Some("wf_alpha"));
    assert_eq!(meta.pid, Some(22222));
    assert!(meta.plan);
    assert!(!meta.plan_approved);
    assert_eq!(meta.wait_for, vec!["bob".to_string(), "carol".to_string()]);
    assert_eq!(meta.wait_duration, Some(3600.0));
    assert_eq!(meta.workspace_dir.as_deref(), Some("/tmp/workspaces/alpha"));
}

#[test]
fn running_record_carries_agent_meta_tag() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    write_json(
        &root
            .join("myproj")
            .join("artifacts")
            .join("ace-run")
            .join(TS_ACE_RUN_RUNNING)
            .join("agent_meta.json"),
        &json!({
            "name": "running_alpha",
            "tag": "sase-26",
        }),
    );

    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_ACE_RUN_RUNNING);
    let meta = rec.agent_meta.as_ref().unwrap();

    assert_eq!(meta.tag.as_deref(), Some("sase-26"));
}

#[test]
fn scalar_agent_meta_timestamps_are_normalized_to_lists() {
    let tmp = tempdir().unwrap();
    let root = tmp.path().join("projects");
    let dir = root
        .join("myproj")
        .join("artifacts")
        .join("ace-run")
        .join("20260429131818");
    write_json(
        &dir.join("agent_meta.json"),
        &json!({
            "name": "planner",
            "plan_submitted_at": "2026-04-29T17:20:22.951546+00:00",
            "epic_started_at": "2026-04-29T17:21:22.951546+00:00",
        }),
    );

    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, "20260429131818");
    let meta = rec.agent_meta.as_ref().unwrap();

    assert_eq!(
        meta.plan_submitted_at,
        vec!["2026-04-29T17:20:22.951546+00:00".to_string()]
    );
    assert_eq!(
        meta.epic_started_at.as_deref(),
        Some("2026-04-29T17:21:22.951546+00:00")
    );
}

#[test]
fn done_record_parses_done_marker() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_ACE_RUN_DONE);

    assert!(rec.has_done_marker);
    let done = rec.done.as_ref().unwrap();
    assert_eq!(done.outcome.as_deref(), Some("completed"));
    assert_eq!(done.cl_name.as_deref(), Some("feature_alpha"));
    assert_eq!(done.workspace_num, Some(3));
    assert_eq!(
        done.workspace_dir.as_deref(),
        Some("/tmp/workspaces/alpha_3")
    );
    assert_eq!(done.diff_path.as_deref(), Some("/tmp/diff_alpha.diff"));
    assert_eq!(
        done.markdown_pdf_paths,
        vec!["/tmp/markdown_pdfs/notes.pdf"]
    );
    assert_eq!(done.image_paths, vec!["/tmp/images/alpha.png"]);
    assert_eq!(done.response_path.as_deref(), Some("/tmp/resp_alpha.md"));
    assert_eq!(done.output_path.as_deref(), Some("/tmp/out_alpha.log"));

    let meta = rec.agent_meta.as_ref().unwrap();
    assert_eq!(meta.stopped_at.as_deref(), Some("2026-04-27T12:05:00Z"));
}

#[test]
fn failed_record_carries_error_and_traceback() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_ACE_RUN_FAILED);

    let done = rec.done.as_ref().unwrap();
    assert_eq!(done.outcome.as_deref(), Some("failed"));
    assert_eq!(done.error.as_deref(), Some("RuntimeError: kaboom"));
    let tb = done.traceback.as_deref().unwrap();
    assert!(tb.contains("Traceback"));
}

#[test]
fn retried_records_link_via_lineage_fields() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let parent = record_by_timestamp(&snapshot, TS_ACE_RUN_RETRIED_PARENT);
    let child = record_by_timestamp(&snapshot, TS_ACE_RUN_RETRIED_CHILD);

    let parent_done = parent.done.as_ref().unwrap();
    assert_eq!(
        parent_done.retried_as_timestamp.as_deref(),
        Some(TS_ACE_RUN_RETRIED_CHILD)
    );
    assert_eq!(
        parent_done.retry_chain_root_timestamp.as_deref(),
        Some(TS_ACE_RUN_RETRIED_PARENT)
    );
    assert_eq!(
        parent_done.retry_error_category.as_deref(),
        Some("transient")
    );
    assert!(parent.agent_meta.as_ref().unwrap().retry_terminal);

    let child_meta = child.agent_meta.as_ref().unwrap();
    assert_eq!(
        child_meta.retry_of_timestamp.as_deref(),
        Some(TS_ACE_RUN_RETRIED_PARENT)
    );
    assert_eq!(child_meta.retry_attempt, Some(1));
    assert_eq!(
        child_meta.retry_chain_root_timestamp.as_deref(),
        Some(TS_ACE_RUN_RETRIED_PARENT)
    );
    assert!(!child.has_done_marker);
}

#[test]
fn home_running_record_has_running_marker() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_HOME_RUNNING);

    assert_eq!(rec.project_name, "home");
    assert_eq!(rec.workflow_dir_name, "ace-run");
    let running = rec.running.as_ref().unwrap();
    assert_eq!(running.pid, Some(11111));
    assert_eq!(running.cl_name.as_deref(), Some("~"));
    assert_eq!(running.workspace_dir.as_deref(), Some("/tmp/home-target"));
    assert_eq!(
        rec.raw_prompt_snippet.as_deref(),
        Some("Investigate the failing job")
    );
}

#[test]
fn workflow_root_record_has_state_and_steps() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_WORKFLOW_ROOT);

    assert_eq!(rec.workflow_dir_name, "workflow-three_phase");
    let state = rec.workflow_state.as_ref().unwrap();
    assert_eq!(state.workflow_name, "three_phase");
    assert_eq!(state.cl_name.as_deref(), Some("feature_workflow"));
    assert_eq!(state.status, "completed");
    assert!(state.appears_as_agent);
    assert!(!state.is_anonymous);
    assert_eq!(state.activity.as_deref(), Some("PDF 2/5 docs/notes.md"));
    let pdf_status = state.pdf_status.as_ref().unwrap();
    assert_eq!(
        pdf_status.get("stage").and_then(Value::as_str),
        Some("engine_started")
    );
    assert_eq!(pdf_status.get("index").and_then(Value::as_i64), Some(2));
    assert_eq!(state.steps.len(), 3);
    assert_eq!(state.steps[0].name, "plan");
    assert_eq!(state.steps[0].status, "completed");
    let step0_output = state.steps[0].output.as_ref().unwrap();
    assert_eq!(
        step0_output.get("plan_path").and_then(Value::as_str),
        Some("/tmp/plan.md")
    );
    let step0_types = state.steps[0].output_types.as_ref().unwrap();
    assert_eq!(
        step0_types.get("plan_path").map(String::as_str),
        Some("path")
    );

    let plan_path = rec.plan_path.as_ref().unwrap();
    assert_eq!(plan_path.plan_path.as_deref(), Some("/tmp/plan.md"));

    let names: Vec<&str> = rec
        .prompt_steps
        .iter()
        .map(|s| s.file_name.as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "prompt_step_000_pre.json",
            "prompt_step_001_plan.json",
            "prompt_step_002_code.json",
        ]
    );
    let pre = &rec.prompt_steps[0];
    let plan = &rec.prompt_steps[1];
    let code = &rec.prompt_steps[2];
    assert!(pre.is_pre_prompt_step);
    assert_eq!(pre.embedded_workflow_name.as_deref(), Some("three_phase"));
    let plan_output = plan.output.as_ref().unwrap();
    assert_eq!(
        plan_output.get("meta_workspace").and_then(Value::as_str),
        Some("5")
    );
    assert!(code.hidden);
    assert_eq!(code.diff_path.as_deref(), Some("/tmp/diff.diff"));
}

#[test]
fn mentor_dir_is_walked() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_MENTOR_DONE);

    assert_eq!(rec.workflow_dir_name, "mentor-bryan");
    let done = rec.done.as_ref().unwrap();
    assert_eq!(done.outcome.as_deref(), Some("completed"));
    assert_eq!(done.response_path.as_deref(), Some("/tmp/mentor.md"));
    assert!(rec.agent_meta.is_none());
}

#[test]
fn waiting_marker_decode_error_does_not_crash() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_WAITING);

    assert!(rec.waiting.is_none());
    let meta = rec.agent_meta.as_ref().unwrap();
    assert_eq!(meta.wait_for, vec!["upstream".to_string()]);
    assert_eq!(meta.wait_duration, Some(600.0));
}

#[test]
fn malformed_agent_meta_is_skipped() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_MALFORMED);

    assert!(rec.agent_meta.is_none());
    assert!(!rec.has_done_marker);
    assert!(rec.done.is_none());
}

#[test]
fn only_workflow_dirs_filters_records() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        only_workflow_dirs: vec!["ace-run".to_string()],
        ..Default::default()
    };
    let snapshot = scan_agent_artifacts(&root, options);
    let dirs: std::collections::BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|r| r.workflow_dir_name.as_str())
        .collect();
    assert_eq!(
        dirs,
        ["ace-run"]
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn pending_question_marker_is_surfaced_when_present() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_PENDING_QUESTION);
    let marker = rec
        .pending_question
        .as_ref()
        .expect("pending_question marker should be present");
    assert_eq!(marker.session_id.as_deref(), Some("ses-abc"));
    assert_eq!(
        marker.request_path.as_deref(),
        Some("/tmp/user_question/ses-abc/question_request.json"),
    );
    assert_eq!(
        marker.submitted_at.as_deref(),
        Some("2026-04-27T19:00:00+00:00"),
    );
}

#[test]
fn pending_question_marker_is_absent_when_file_missing() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let rec = record_by_timestamp(&snapshot, TS_ACE_RUN_RUNNING);
    assert!(rec.pending_question.is_none());
}

#[test]
fn disable_prompt_step_markers() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        include_prompt_step_markers: false,
        ..Default::default()
    };
    let snapshot = scan_agent_artifacts(&root, options);
    let rec = record_by_timestamp(&snapshot, TS_WORKFLOW_ROOT);
    assert!(rec.prompt_steps.is_empty());
    assert_eq!(snapshot.stats.prompt_step_markers_parsed, 0);
}

#[test]
fn disable_raw_prompt_snippet() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        include_raw_prompt_snippets: false,
        ..Default::default()
    };
    let snapshot = scan_agent_artifacts(&root, options);
    let rec = record_by_timestamp(&snapshot, TS_HOME_RUNNING);
    assert!(rec.raw_prompt_snippet.is_none());
}

#[test]
fn max_prompt_snippet_bytes_truncates() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        max_prompt_snippet_bytes: 10,
        ..Default::default()
    };
    let snapshot = scan_agent_artifacts(&root, options);
    let rec = record_by_timestamp(&snapshot, TS_HOME_RUNNING);
    assert_eq!(rec.raw_prompt_snippet.as_deref(), Some("Investigat"));
}

#[test]
fn missing_root_returns_empty_snapshot() {
    let tmp = tempdir().unwrap();
    let snapshot = scan_agent_artifacts(
        &tmp.path().join("does_not_exist"),
        AgentArtifactScanOptionsWire::default(),
    );
    assert!(snapshot.records.is_empty());
    assert_eq!(snapshot.stats.projects_visited, 0);
    assert_eq!(snapshot.stats.artifact_dirs_visited, 0);
    assert_eq!(snapshot.stats.json_decode_errors, 0);
}

#[test]
fn options_round_trip_through_snapshot() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        include_prompt_step_markers: false,
        include_raw_prompt_snippets: false,
        max_prompt_snippet_bytes: 42,
        only_workflow_dirs: vec![
            "ace-run".to_string(),
            "workflow-three_phase".to_string(),
        ],
        max_records: Some(3),
        newest_first: true,
        not_before_timestamp: Some("20260427120000".to_string()),
        include_done_markers: false,
        include_workflow_state: false,
        include_waiting: false,
        only_projects: vec!["myproj".to_string()],
    };
    let snapshot = scan_agent_artifacts(&root, options.clone());
    assert_eq!(snapshot.options, options);
}

#[test]
fn bounded_newest_first_limits_completed_without_hiding_incomplete() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        max_records: Some(2),
        newest_first: true,
        ..Default::default()
    };

    let snapshot = scan_agent_artifacts(&root, options);
    let timestamps: Vec<&str> = snapshot
        .records
        .iter()
        .map(|r| r.timestamp.as_str())
        .collect();
    let completed: Vec<&str> = snapshot
        .records
        .iter()
        .filter(|r| r.has_done_marker)
        .map(|r| r.timestamp.as_str())
        .collect();

    assert_eq!(completed, vec![TS_MENTOR_DONE, TS_ACE_RUN_RETRIED_PARENT]);
    assert!(timestamps.contains(&TS_HOME_RUNNING));
    assert!(timestamps.contains(&TS_ACE_RUN_RUNNING));
    assert!(timestamps.contains(&TS_WAITING));
    let mut sorted = timestamps.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(timestamps, sorted);
}

#[test]
fn bounded_not_before_applies_to_completed_rows_only() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        newest_first: true,
        not_before_timestamp: Some(TS_ACE_RUN_RETRIED_PARENT.to_string()),
        ..Default::default()
    };

    let snapshot = scan_agent_artifacts(&root, options);
    let timestamps: Vec<&str> = snapshot
        .records
        .iter()
        .map(|r| r.timestamp.as_str())
        .collect();

    assert!(!timestamps.contains(&TS_ACE_RUN_DONE));
    assert!(!timestamps.contains(&TS_ACE_RUN_FAILED));
    assert!(timestamps.contains(&TS_HOME_RUNNING));
    assert!(timestamps.contains(&TS_ACE_RUN_RUNNING));
    assert!(timestamps.contains(&TS_WAITING));
}

#[test]
fn selective_marker_options_skip_payloads_but_keep_done_presence() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let options = AgentArtifactScanOptionsWire {
        include_done_markers: false,
        include_waiting: false,
        include_workflow_state: false,
        ..Default::default()
    };

    let snapshot = scan_agent_artifacts(&root, options);
    let done_rec = record_by_timestamp(&snapshot, TS_ACE_RUN_DONE);
    assert!(done_rec.has_done_marker);
    assert!(done_rec.done.is_none());

    let workflow_rec = record_by_timestamp(&snapshot, TS_WORKFLOW_ROOT);
    assert!(workflow_rec.workflow_state.is_none());
    let waiting_rec = record_by_timestamp(&snapshot, TS_WAITING);
    assert!(waiting_rec.waiting.is_none());
}

#[test]
fn snapshot_serializes_to_json() {
    let tmp = tempdir().unwrap();
    let root = build_fixture_tree(&tmp.path().join("projects"));
    let snapshot =
        scan_agent_artifacts(&root, AgentArtifactScanOptionsWire::default());
    let payload = serde_json::to_value(&snapshot).unwrap();
    // Sanity-check the top-level shape so a future refactor can't drop a key.
    let obj = payload
        .as_object()
        .expect("snapshot must serialize as JSON object");
    for key in [
        "schema_version",
        "projects_root",
        "options",
        "stats",
        "records",
    ] {
        assert!(obj.contains_key(key), "missing top-level key: {key}");
    }
    let records = obj["records"].as_array().unwrap();
    assert_eq!(records.len(), EXPECTED_TIMESTAMPS.len());
    // Round-trip through JSON without surprises.
    let raw = serde_json::to_string(&payload).unwrap();
    let _: Value = serde_json::from_str(&raw).unwrap();
}
