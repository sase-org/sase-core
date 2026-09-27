//! Detail fixtures: commit, command, and plugin instance content plus
//! multi-run node composition.
//!
//! Every fixture builds real artifact text (plan JSON resolved through
//! `resolve_finalizer_plan`, C2 operation records, C3 steps, live tails,
//! schema-v1 result JSON) and projects it through
//! `project_finalizer_node_view`. The plugin fixture renders with no
//! plugin-specific fields anywhere in the projection.

use serde_json::{json, Value as JsonValue};

use super::super::selection::resolve_finalizer_plan;
use super::super::wire::{
    FinalizerInstanceSpecWire, FinalizerPlanInputWire, FinalizerPlanWire,
    FINALIZER_WIRE_SCHEMA_VERSION,
};
use super::detail::project_finalizer_node_view;
use super::detail_content::collapse_cr_tail;
use super::wire::{
    FinalizerNodeViewRequestWire, FinalizerNodeViewWire, RunViewFileInputWire,
    RunViewInstanceInputWire, RunViewRunInputWire, RunViewRunKindWire,
    RunViewTextInputWire, RUN_VIEW_WIRE_SCHEMA_VERSION,
};

fn spec(
    instance_id: &str,
    provider_ref: &str,
    after: &[&str],
) -> FinalizerInstanceSpecWire {
    serde_json::from_value(json!({
        "schema_version": FINALIZER_WIRE_SCHEMA_VERSION,
        "instance_id": instance_id,
        "provider_ref": provider_ref,
        "after": after,
    }))
    .unwrap()
}

fn resolve(input: &FinalizerPlanInputWire) -> FinalizerPlanWire {
    resolve_finalizer_plan(input).unwrap()
}

fn commit_check_plan() -> FinalizerPlanWire {
    resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![
            spec("commit", "builtin@commit", &[]),
            spec("check", "builtin@command", &["commit"]),
        ],
        defaults: vec!["commit".to_string()],
        required: vec!["check".to_string()],
        selectors: Vec::new(),
    })
}

fn solo_plan(instance_id: &str, provider_ref: &str) -> FinalizerPlanWire {
    resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![spec(instance_id, provider_ref, &[])],
        defaults: vec![instance_id.to_string()],
        required: Vec::new(),
        selectors: Vec::new(),
    })
}

fn text_input(text: &str) -> RunViewTextInputWire {
    RunViewTextInputWire {
        text: Some(text.to_string()),
        size: text.len() as u64,
        too_large: false,
    }
}

fn empty_input() -> RunViewTextInputWire {
    RunViewTextInputWire {
        text: None,
        size: 0,
        too_large: false,
    }
}

fn text_file(name: &str, text: &str) -> RunViewFileInputWire {
    RunViewFileInputWire {
        name: name.to_string(),
        size: text.len() as u64,
        mtime_ns: None,
        text: Some(RunViewTextInputWire {
            text: Some(text.to_string()),
            size: text.len() as u64,
            too_large: false,
        }),
        line_count: None,
        tail: None,
    }
}

fn meta_file(name: &str, size: u64, line_count: u64) -> RunViewFileInputWire {
    RunViewFileInputWire {
        name: name.to_string(),
        size,
        mtime_ns: None,
        text: None,
        line_count: Some(line_count),
        tail: None,
    }
}

fn tail_file(name: &str, size: u64, tail: &str) -> RunViewFileInputWire {
    RunViewFileInputWire {
        name: name.to_string(),
        size,
        mtime_ns: None,
        text: None,
        line_count: None,
        tail: Some(tail.to_string()),
    }
}

fn instance_input(
    instance_id: &str,
    files: Vec<RunViewFileInputWire>,
) -> RunViewInstanceInputWire {
    RunViewInstanceInputWire {
        instance_id: instance_id.to_string(),
        files,
    }
}

fn base_run(plan: &FinalizerPlanWire, number: u64) -> RunViewRunInputWire {
    RunViewRunInputWire {
        run_id: format!("run-{number}"),
        number,
        label: "agent turn".to_string(),
        kind: RunViewRunKindWire::Agent,
        turn_terminal: true,
        runner_live: None,
        agent_meta: empty_input(),
        plan: text_input(&serde_json::to_string(plan).unwrap()),
        authority_plan: empty_input(),
        context: empty_input(),
        submission: empty_input(),
        submission_attempts: empty_input(),
        journal: empty_input(),
        result: empty_input(),
        instances: Vec::new(),
        recovery_files: Vec::new(),
    }
}

fn project_runs(runs: Vec<RunViewRunInputWire>) -> FinalizerNodeViewWire {
    let request = FinalizerNodeViewRequestWire {
        schema_version: RUN_VIEW_WIRE_SCHEMA_VERSION,
        runs,
        tail_lines: 12,
    };
    project_finalizer_node_view(&request).unwrap()
}

fn journal_text(lines: &[JsonValue]) -> String {
    lines
        .iter()
        .map(|line| serde_json::to_string(line).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn journal_line(seq: u32, event: &str, extra: JsonValue) -> JsonValue {
    let mut line = json!({"v": 1, "seq": seq, "t": 1727440000.0 + seq as f64, "event": event});
    for (key, value) in extra.as_object().unwrap() {
        line[key] = value.clone();
    }
    line
}

fn not_triggered_context(plan: &FinalizerPlanWire) -> String {
    json!({
        "schema_version": 2,
        "run_id": "run-1",
        "agent_id": "agent-1",
        "turn_nonce": "nonce-1",
        "plan_digest": plan.plan_digest,
        "requirements": [
            {"instance_id": "commit", "trigger": "always", "submission_required": false},
            {"instance_id": "check", "trigger": "not_triggered", "submission_required": false},
        ],
        "obligations": [],
    })
    .to_string()
}

fn commit_success_run(plan: &FinalizerPlanWire) -> RunViewRunInputWire {
    let sha = "8bb7e5507f4a1c2d9e6f0a3b5c7d8e9f0a1b2c3d";
    let mut run = base_run(plan, 1);
    run.context = text_input(&not_triggered_context(plan));
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "instance_started", json!({"instance_id": "commit"})),
        journal_line(
            3,
            "attempt_started",
            json!({"instance_id": "commit", "attempt": 1, "max_attempts": 1, "t": 1727440010.0}),
        ),
        journal_line(
            4,
            "op_started",
            json!({"instance_id": "commit", "attempt": 1, "op": "stitch-main", "kind": "subprocess", "label": "stitch main"}),
        ),
        journal_line(
            5,
            "op_finished",
            json!({"instance_id": "commit", "attempt": 1, "op": "stitch-main", "returncode": 0, "duration_seconds": 12.3, "timed_out": false}),
        ),
        journal_line(
            6,
            "attempt_finished",
            json!({"instance_id": "commit", "attempt": 1, "status": "success", "t": 1727440025.0}),
        ),
        journal_line(
            7,
            "instance_finished",
            json!({"instance_id": "commit", "status": "success"}),
        ),
        journal_line(
            8,
            "phase_finished",
            json!({"status": "success", "cycles": 1}),
        ),
    ]));
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "success",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "commit",
                    "status": "success",
                    "attempts": [{"attempt": 1, "status": "success"}],
                    "evidence": [
                        {"kind": "commit_sha", "value": sha},
                        {"kind": "result", "value": "stitch main"},
                    ],
                    "diagnostics": [
                        {"code": "stitch_warning", "severity": "warning",
                         "message": "line over 120 chars in main",
                         "instance_id": "commit", "attempt": 1},
                    ],
                },
                {"instance_id": "check", "status": "pending", "attempts": []},
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run.instances = vec![instance_input(
        "commit",
        vec![
            text_file(
                "attempt-1.stitch-main.outcome.json",
                &json!({
                    "returncode": 0,
                    "duration_seconds": 12.3,
                    "timed_out": false,
                    "stdout_truncated": false,
                    "stderr_truncated": false,
                    "argv": ["sase", "stitch", "create"],
                    "message_file": "main.msg",
                })
                .to_string(),
            ),
            meta_file("attempt-1.stitch-main.stdout", 400, 9),
            meta_file("attempt-1.stitch-main.stderr", 120, 3),
            text_file(
                "attempt-1.stitch-main.steps.jsonl",
                &[
                    json!({"v": 1, "t": 1727440011.0, "step": "before hook: just fix", "state": "start"}).to_string(),
                    json!({"v": 1, "t": 1727440012.0, "step": "before hook: just fix", "state": "ok"}).to_string(),
                    json!({"v": 1, "t": 1727440020.0, "step": "long line", "state": "warn", "detail": "line over 120 chars in main"}).to_string(),
                    "not json".to_string(),
                ]
                .join("\n"),
            ),
        ],
    )];
    run
}

#[test]
fn commit_stitch_reports_legacy_record_steps_and_headline() {
    let plan = commit_check_plan();
    let view = project_runs(vec![commit_success_run(&plan)]);
    assert_eq!(view.status, "success");
    let commit = view.runs[0]
        .instances
        .iter()
        .find(|instance| instance.instance_id == "commit")
        .unwrap();
    assert_eq!(commit.status, "success");
    assert_eq!(commit.attempts.len(), 1);
    assert_eq!(commit.attempts[0].attempt, 1);
    assert_eq!(commit.attempts[0].status, "success");
    assert_eq!(commit.attempts[0].started_at, Some(1727440010.0));
    assert_eq!(commit.attempts[0].duration_seconds, Some(15.0));
    // One legacy commit record; the journal op for the same op merges
    // instead of duplicating.
    assert_eq!(commit.operations.len(), 1);
    let operation = &commit.operations[0];
    assert_eq!(operation.op, "stitch-main");
    assert_eq!(operation.kind.as_deref(), Some("subprocess"));
    assert_eq!(operation.attempt, Some(1));
    assert_eq!(operation.returncode, Some(0));
    assert_eq!(operation.duration_seconds, Some(12.3));
    assert!(!operation.timed_out);
    assert_eq!(operation.argv, vec!["sase", "stitch", "create"]);
    assert_eq!(
        operation
            .logs
            .iter()
            .map(|log| (log.kind.clone(), log.name.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                "stdout".to_string(),
                "attempt-1.stitch-main.stdout".to_string()
            ),
            (
                "stderr".to_string(),
                "attempt-1.stitch-main.stderr".to_string()
            ),
        ]
    );
    // Steps parse tolerantly: the malformed line is skipped.
    assert_eq!(operation.steps.len(), 3);
    assert!(operation.steps.iter().any(|step| step.state == "warn"));
    assert!(!operation.steps_truncated);
    // Typed evidence: the SHA wins the headline; the ambiguous commit
    // `result` kind stays text and never wins.
    assert_eq!(commit.evidence.len(), 2);
    let sha = commit
        .evidence
        .iter()
        .find(|item| item.kind == "commit_sha")
        .unwrap();
    assert_eq!(sha.evidence_type.as_deref(), Some("sha"));
    let headline = commit.headline.as_ref().unwrap();
    assert_eq!(headline.kind, "commit_sha");
    assert_eq!(headline.evidence_type.as_deref(), Some("sha"));
    // Warnings count the warn step plus the warning diagnostic in the
    // latest attempt. No failure reason on success.
    assert_eq!(commit.warnings, 2);
    assert_eq!(commit.failure_reason, None);
    assert!(commit
        .diagnostics
        .iter()
        .all(|diagnostic| diagnostic.severity == "warning"));
}

#[test]
fn commit_deferral_reports_typed_reason_and_paths() {
    let plan = solo_plan("commit", "builtin@commit");
    let mut run = base_run(&plan, 1);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "deferred",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "commit",
                    "status": "deferred",
                    "attempts": [{"attempt": 1, "status": "deferred"}],
                    "evidence": [
                        {"kind": "deferred_repo", "value": "main:protected_paths:secrets/token"},
                    ],
                    "deferral": {"reason": "protected_paths", "paths": ["secrets/token"]},
                    "diagnostics": [],
                },
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    let view = project_runs(vec![run]);
    let commit = &view.runs[0].instances[0];
    assert_eq!(commit.status, "deferred");
    let deferral = commit.deferral.as_ref().unwrap();
    assert_eq!(deferral.reason, "protected_paths");
    assert_eq!(deferral.paths, vec!["secrets/token".to_string()]);
    assert_eq!(commit.failure_reason, None);
    assert_eq!(view.attention_instance_id.as_deref(), Some("commit"));
}

fn command_failure_run(plan: &FinalizerPlanWire) -> RunViewRunInputWire {
    let mut run = base_run(plan, 1);
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "instance_started", json!({"instance_id": "check"})),
        journal_line(
            3,
            "attempt_started",
            json!({"instance_id": "check", "attempt": 1, "max_attempts": 2, "t": 1727440030.0}),
        ),
        journal_line(
            4,
            "op_started",
            json!({"instance_id": "check", "attempt": 1, "op": "run", "kind": "subprocess", "label": "just check"}),
        ),
        journal_line(
            5,
            "op_finished",
            json!({"instance_id": "check", "attempt": 1, "op": "run", "returncode": 1, "duration_seconds": 220.0, "timed_out": false}),
        ),
        journal_line(
            6,
            "attempt_finished",
            json!({"instance_id": "check", "attempt": 1, "status": "failed", "code": "command_failed", "t": 1727440250.0}),
        ),
        journal_line(
            7,
            "attempt_started",
            json!({"instance_id": "check", "attempt": 2, "max_attempts": 2, "t": 1727440251.0}),
        ),
        journal_line(
            8,
            "op_started",
            json!({"instance_id": "check", "attempt": 2, "op": "run", "kind": "subprocess", "label": "just check"}),
        ),
        journal_line(
            9,
            "op_finished",
            json!({"instance_id": "check", "attempt": 2, "op": "run", "returncode": 1, "duration_seconds": 221.0, "timed_out": false}),
        ),
        journal_line(
            10,
            "attempt_finished",
            json!({"instance_id": "check", "attempt": 2, "status": "failed", "code": "command_failed", "t": 1727440472.0}),
        ),
        journal_line(
            11,
            "instance_finished",
            json!({"instance_id": "check", "status": "failed"}),
        ),
        journal_line(
            12,
            "phase_finished",
            json!({"status": "failed", "cycles": 1}),
        ),
    ]));
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "failed",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "check",
                    "status": "failed",
                    "attempts": [
                        {"attempt": 1, "status": "failed", "diagnostic_code": "command_failed"},
                        {"attempt": 2, "status": "failed", "diagnostic_code": "command_failed"},
                    ],
                    "evidence": [
                        {"kind": "exit_code", "value": "1"},
                        {"kind": "duration_seconds", "value": "221.000"},
                    ],
                    "diagnostics": [
                        {"code": "command_failed", "severity": "error",
                         "message": "builtin@command 'check' failed on attempt 2",
                         "instance_id": "check", "attempt": 2},
                    ],
                },
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run.instances = vec![instance_input(
        "check",
        vec![
            meta_file("attempt-1.stdout", 900, 20),
            meta_file("attempt-1.stderr", 300, 8),
            text_file(
                "attempt-1.diagnostics.json",
                &json!({"attempt": 1, "returncode": 1, "timed_out": false,
                        "stdout_truncated": false, "stderr_truncated": false})
                .to_string(),
            ),
            meta_file("attempt-2.stdout", 950, 21),
            tail_file(
                "attempt-2.stderr",
                400,
                "FAILED tests/ace/tui/test_final_deck.py::test_receipt\n",
            ),
        ],
    )];
    run
}

#[test]
fn command_failure_reports_attempts_journal_ops_and_reason() {
    let plan = solo_plan("check", "builtin@command");
    let view = project_runs(vec![command_failure_run(&plan)]);
    assert_eq!(view.status, "failed");
    let check = &view.runs[0].instances[0];
    assert_eq!(check.status, "failed");
    assert_eq!(check.attempts.len(), 2);
    assert_eq!(check.attempts[0].attempt, 1);
    assert_eq!(check.attempts[0].status, "failed");
    assert_eq!(check.attempts[0].code.as_deref(), Some("command_failed"));
    assert_eq!(check.attempts[0].duration_seconds, Some(220.0));
    assert_eq!(check.attempts[1].attempt, 2);
    assert_eq!(check.attempts[1].duration_seconds, Some(221.0));
    // No C2 records exist; both operations render from journal op events
    // with conventional log names attached.
    assert_eq!(check.operations.len(), 2);
    assert_eq!(check.operations[0].attempt, Some(1));
    assert_eq!(check.operations[0].label.as_deref(), Some("just check"));
    assert_eq!(check.operations[0].returncode, Some(1));
    assert_eq!(check.operations[0].duration_seconds, Some(220.0));
    assert!(check.operations[0]
        .logs
        .iter()
        .any(|log| log.name == "attempt-1.stdout"));
    assert!(check.operations[1]
        .logs
        .iter()
        .any(|log| log.name == "attempt-2.stderr"));
    // The first error diagnostic wins over the stderr tail.
    assert_eq!(
        check.failure_reason.as_deref(),
        Some("builtin@command 'check' failed on attempt 2")
    );
    // Headline falls through SHA and URL to the exit code.
    let headline = check.headline.as_ref().unwrap();
    assert_eq!(headline.kind, "exit_code");
    assert_eq!(headline.evidence_type.as_deref(), Some("exit_code"));
    assert!(check.protocol_files.is_empty());
    assert_eq!(view.attention_instance_id.as_deref(), Some("check"));
}

#[test]
fn failure_reason_falls_back_to_the_stderr_tail() {
    let plan = solo_plan("check", "builtin@command");
    let mut run = base_run(&plan, 1);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "failed",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "check",
                    "status": "failed",
                    "attempts": [{"attempt": 1, "status": "failed"}],
                    "evidence": [],
                    "diagnostics": [],
                },
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run.instances = vec![instance_input(
        "check",
        vec![tail_file(
            "attempt-1.stderr",
            200,
            "running tests...\n\nFAILED tests/x.py::test_y - assert\n",
        )],
    )];
    let view = project_runs(vec![run]);
    let check = &view.runs[0].instances[0];
    assert_eq!(
        check.failure_reason.as_deref(),
        Some("FAILED tests/x.py::test_y - assert")
    );
}

#[test]
fn plugin_fixture_renders_with_no_plugin_specific_fields() {
    let plan = solo_plan("open-pr", "acme-sase@open-pr");
    let mut run = base_run(&plan, 1);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "success",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "open-pr",
                    "status": "success",
                    "attempts": [{"attempt": 1, "status": "success"}],
                    "evidence": [
                        {"kind": "pr_url", "value": "https://example.com/acme/repo/pull/42"},
                        {"kind": "bead_id", "value": "sase-1b2.3"},
                    ],
                    "diagnostics": [],
                },
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run.instances = vec![instance_input(
        "open-pr",
        vec![
            text_file(
                "attempt-1.execute.outcome.json",
                &json!({
                    "schema_version": 1,
                    "op": "execute",
                    "kind": "subprocess",
                    "label": "execute",
                    "attempt": 1,
                    "argv": ["acme-sase", "open-pr", "--execute"],
                    "started_at": 1727440100.0,
                    "duration_seconds": 3.2,
                    "returncode": 0,
                    "timed_out": false,
                    "stdout_truncated": false,
                    "stderr_truncated": false,
                    "logs": {"stdout": "attempt-1.execute.stdout", "stderr": "attempt-1.execute.stderr"},
                    "steps": "attempt-1.execute.steps.jsonl",
                })
                .to_string(),
            ),
            text_file(
                "attempt-1.verify.outcome.json",
                &json!({
                    "schema_version": 1,
                    "op": "verify",
                    "kind": "subprocess",
                    "label": "verify",
                    "attempt": 1,
                    "duration_seconds": 1.1,
                    "returncode": 0,
                    "timed_out": false,
                })
                .to_string(),
            ),
            text_file(
                "preflight.describe.outcome.json",
                &json!({
                    "schema_version": 1,
                    "op": "describe",
                    "kind": "validation",
                    "label": "describe",
                    "attempt": null,
                    "duration_seconds": 0.4,
                    "returncode": 0,
                    "timed_out": false,
                })
                .to_string(),
            ),
            meta_file("attempt-1.execute.stdout", 300, 6),
            meta_file("attempt-1.execute.stderr", 60, 2),
            meta_file("describe.stdout", 200, 5),
            meta_file("describe.stderr", 10, 1),
            text_file(
                "attempt-1.execute.steps.jsonl",
                &[
                    json!({"v": 1, "t": 1727440101.0, "step": "open pull request", "state": "start"}).to_string(),
                    json!({"v": 1, "t": 1727440103.0, "step": "open pull request", "state": "ok"}).to_string(),
                ]
                .join("\n"),
            ),
            tail_file(
                "attempt-1.execute.live",
                90,
                "uploading 30%\ruploading 90%\r done\nopen https://example.com/acme/repo/pull/42\n",
            ),
        ],
    )];
    let view = project_runs(vec![run]);
    let instance = &view.runs[0].instances[0];
    assert_eq!(instance.status, "success");
    // Preflight (no attempt) sorts first, then attempt order, then op.
    assert_eq!(
        instance
            .operations
            .iter()
            .map(|op| op.op.clone())
            .collect::<Vec<_>>(),
        vec![
            "describe".to_string(),
            "execute".to_string(),
            "verify".to_string()
        ]
    );
    let execute = &instance.operations[1];
    assert_eq!(execute.kind.as_deref(), Some("subprocess"));
    assert_eq!(execute.attempt, Some(1));
    assert_eq!(execute.started_at, Some(1727440100.0));
    assert_eq!(execute.duration_seconds, Some(3.2));
    assert_eq!(execute.argv, vec!["acme-sase", "open-pr", "--execute"]);
    assert_eq!(execute.steps.len(), 2);
    // Carriage-return progress collapses to its last segment.
    assert_eq!(
        execute.live_tail,
        vec![
            " done".to_string(),
            "open https://example.com/acme/repo/pull/42".to_string()
        ]
    );
    let describe = &instance.operations[0];
    assert_eq!(describe.attempt, None);
    assert_eq!(describe.kind.as_deref(), Some("validation"));
    // Typed evidence classifies by convention; the URL beats the bead id.
    assert_eq!(
        instance
            .evidence
            .iter()
            .map(|item| item.evidence_type.clone().unwrap())
            .collect::<Vec<_>>(),
        vec!["url".to_string(), "bead".to_string()]
    );
    let headline = instance.headline.as_ref().unwrap();
    assert_eq!(headline.kind, "pr_url");
    // Protocol envelopes surface as generic file names only.
    assert!(instance
        .protocol_files
        .contains(&"attempt-1.execute.stdout".to_string()));
    assert!(instance
        .protocol_files
        .contains(&"describe.stdout".to_string()));
    assert!(!instance
        .protocol_files
        .iter()
        .any(|name| name.ends_with(".outcome.json")));
    // The generic path holds: provider identity travels only in the
    // provider_ref string, and every evidence type is a convention word.
    assert_eq!(instance.provider_ref.as_deref(), Some("acme-sase@open-pr"));
    assert!(instance.evidence.iter().all(|item| matches!(
        item.evidence_type.as_deref(),
        Some(
            "sha" | "url" | "bead" | "path" | "duration" | "exit_code" | "text"
        )
    )));
}

#[test]
fn superseded_attempt_errors_never_paint_red() {
    let plan = solo_plan("commit", "builtin@commit");
    let mut run = base_run(&plan, 1);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "success",
            "cycles": 1,
            "instances": [
                {
                    "instance_id": "commit",
                    "status": "success",
                    "attempts": [
                        {"attempt": 1, "status": "failed", "diagnostic_code": "stitch_conflict"},
                        {"attempt": 2, "status": "success"},
                    ],
                    "evidence": [
                        {"kind": "commit_sha", "value": "8bb7e5507f4a1c2d9e6f0a3b5c7d8e9f0a1b2c3d"},
                    ],
                    "diagnostics": [
                        {"code": "stitch_conflict", "severity": "error",
                         "message": "stitch failed: conflict", "attempt": 1},
                        {"code": "stitch_conflict", "severity": "error",
                         "message": "stitch failed: conflict", "attempt": 1},
                        {"code": "stitch_warning", "severity": "warning",
                         "message": "trailing whitespace", "attempt": 2},
                    ],
                },
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    let view = project_runs(vec![run]);
    let commit = &view.runs[0].instances[0];
    assert_eq!(commit.status, "success");
    // The duplicated attempt-1 error dedupes to one downgraded entry.
    assert_eq!(commit.diagnostics.len(), 2);
    assert_eq!(commit.diagnostics[0].severity, "superseded");
    assert_eq!(commit.diagnostics[0].attempt, Some(1));
    assert_eq!(commit.diagnostics[1].severity, "warning");
    assert_eq!(commit.warnings, 1);
    assert_eq!(commit.failure_reason, None);
}

fn skipped_run(number: u64) -> RunViewRunInputWire {
    let plan = commit_check_plan();
    let mut run = base_run(&plan, number);
    run.turn_terminal = true;
    run.journal = text_input(&journal_text(&[journal_line(
        1,
        "phase_skipped",
        json!({"reason": "handoff:plan"}),
    )]));
    run
}

fn settled_run(
    plan: &FinalizerPlanWire,
    number: u64,
    status: &str,
) -> RunViewRunInputWire {
    let check_status = if status == "success" {
        "success"
    } else {
        "failed"
    };
    let mut run = base_run(plan, number);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": status,
            "cycles": 1,
            "instances": [
                {"instance_id": "commit", "status": "success",
                 "attempts": [{"attempt": 1, "status": "success"}]},
                {"instance_id": "check", "status": check_status,
                 "attempts": [{"attempt": 1, "status": check_status}]},
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run
}

#[test]
fn three_run_session_supersedes_to_the_final_success() {
    let plan = commit_check_plan();
    // Deliberately out of order: the response ledger sorts by number.
    let view = project_runs(vec![
        settled_run(&plan, 2, "success"),
        settled_run(&plan, 1, "failed"),
        skipped_run(0),
    ]);
    assert_eq!(
        view.runs.iter().map(|run| run.number).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(view.status, "success");
    let check = view
        .instances
        .iter()
        .find(|instance| instance.instance_id == "check")
        .unwrap();
    assert_eq!(check.status, "success");
    assert_eq!(check.appearances.len(), 3);
}

#[test]
fn success_then_failure_pins_the_later_failure() {
    let plan = commit_check_plan();
    let view = project_runs(vec![
        settled_run(&plan, 0, "success"),
        settled_run(&plan, 1, "failed"),
    ]);
    assert_eq!(view.status, "failed");
    assert_eq!(view.attention_instance_id.as_deref(), Some("check"));
}

#[test]
fn collapse_cr_tail_keeps_the_last_segment_of_each_line() {
    assert_eq!(
        collapse_cr_tail("a\rb\rc\nok\n", 12),
        vec!["c".to_string(), "ok".to_string()]
    );
    assert_eq!(
        collapse_cr_tail("line\r\nnext\r\n", 12),
        vec!["line".to_string(), "next".to_string()]
    );
    assert!(collapse_cr_tail("anything\n", 0).is_empty());
    assert_eq!(
        collapse_cr_tail("1\n2\n3\n", 2),
        vec!["2".to_string(), "3".to_string()]
    );
}

#[test]
fn oversized_steps_flag_truncation_and_keep_parsing() {
    let mut text = String::new();
    while text.len() <= 70 * 1024 {
        text.push_str(
            &json!({"v": 1, "step": "tick", "state": "start"}).to_string(),
        );
        text.push('\n');
    }
    let plan = solo_plan("check", "builtin@command");
    let mut run = base_run(&plan, 1);
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "success",
            "cycles": 1,
            "instances": [
                {"instance_id": "check", "status": "success",
                 "attempts": [{"attempt": 1, "status": "success"}]},
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "instance_started", json!({"instance_id": "check"})),
        journal_line(
            3,
            "attempt_started",
            json!({"instance_id": "check", "attempt": 1}),
        ),
        journal_line(
            4,
            "op_started",
            json!({"instance_id": "check", "attempt": 1, "op": "run"}),
        ),
        journal_line(
            5,
            "op_finished",
            json!({"instance_id": "check", "attempt": 1, "op": "run", "returncode": 0}),
        ),
    ]));
    run.instances = vec![instance_input(
        "check",
        vec![text_file("attempt-1.run.steps.jsonl", &text)],
    )];
    let view = project_runs(vec![run]);
    let operation = &view.runs[0].instances[0].operations[0];
    assert!(operation.steps_truncated);
    assert!(!operation.steps.is_empty());
    assert!(operation.steps.iter().all(|step| step.step == "tick"));
}
