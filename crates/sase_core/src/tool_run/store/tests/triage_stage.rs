//! Store-backed stage/settle/show/failures tests.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::tool_run::store::{
    begin, finish, observe, tool_run_failures, triage_settle, triage_show,
    triage_stage,
};
use crate::tool_run::triage::{
    ToolRunFailuresRequestWire, ToolRunTriageSettleRequestWire,
    ToolRunTriageStageInputWire, ToolRunTriageStageRequestWire,
};
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolDirtyPathWire,
    ToolEvidenceCompletenessWire, ToolFingerprintWire, ToolRepoFingerprintWire,
    ToolRunBeginRequestWire, ToolRunFinishRequestWire,
    ToolRunObserveRequestWire, ToolRunStateWire, ToolRunWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::{normalize_tool_definition, ToolRunError};

fn definition() -> ToolDefinitionWire {
    ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: "check".into(),
        argv: vec!["just".into(), "check".into()],
        description: "check".into(),
        stages: crate::tool_run::wire::ToolStagesWire::RunSilent,
        inputs: vec!["Justfile".into()],
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: crate::tool_run::wire::ToolFingerprintSpecWire::default(),
        diagnostics: Vec::new(),
    }
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn fingerprint(head: &str, dirty: Vec<&str>) -> ToolFingerprintWire {
    ToolFingerprintWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project_identity: None,
        definition_digest: None,
        extra_args_digest: None,
        repos: vec![ToolRepoFingerprintWire {
            identity: "sase".to_string(),
            head: Some(head.to_string()),
            index_tree: None,
            dirty_paths: dirty
                .into_iter()
                .map(|path| ToolDirtyPathWire {
                    path: path.to_string(),
                    status: "modified".to_string(),
                    kind: "file".to_string(),
                    mode: None,
                    content_hash: None,
                    incomplete: None,
                })
                .collect(),
            incomplete: None,
        }],
        inputs: Vec::new(),
        env: Default::default(),
        toolchain: Default::default(),
        completeness: ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        },
        diagnostics: Vec::new(),
    }
}

fn begin_named(
    path: &Path,
    run_id: &str,
    workspace: Option<&str>,
    now: i64,
) -> ToolRunWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some(run_id.to_string()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: Some("sase".into()),
            agent: Some(format!("agent-{run_id}")),
            workspace: workspace.map(str::to_string),
            bead: None,
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: None,
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(now),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap()
    .run
}

fn observe_fp(path: &Path, run_id: &str, head: &str, dirty: Vec<&str>) {
    observe(
        path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(fingerprint(head, dirty)),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn finish_failed(
    path: &Path,
    run_id: &str,
    now: i64,
    cause: Option<crate::tool_run::handoff_wire::ToolRunTerminalCauseWire>,
) {
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: Some(1),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(1),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(now),
            terminal_cause: cause,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn mypy_output() -> String {
    "src/foo.py:10:5: error: Bad thing  [attr-defined]\n".to_string()
}

#[test]
fn stage_settle_show_round_trip_with_verdict() {
    let (_temp, path) = store();
    let run = begin_named(&path, "subject-1", Some("ws-a"), 1);
    observe_fp(&path, &run.run_id, "head-3", vec![]);
    finish_failed(
        &path,
        &run.run_id,
        50,
        Some(crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited),
    );
    let staged = triage_stage(
        &path,
        ToolRunTriageStageRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            stage: ToolRunTriageStageInputWire {
                stage_key: "lint (mypy)".to_string(),
                stage_id: Some("stage-1".to_string()),
                output: Some(mypy_output()),
                truncated: false,
                output_path: Some("logs/stage.log".to_string()),
            },
            project_root: None,
            workspace_roots: Vec::new(),
            ancestry: vec!["head-3".to_string()],
            flake_baseline: Vec::new(),
            selection_records: Vec::new(),
            owner_candidates: Vec::new(),
            knobs: Default::default(),
            now_ts: Some(100),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(staged.refused.is_none());
    assert_eq!(staged.items.len(), 1);
    assert!(staged.items[0].label.is_some());
    // Settle with recipe finish still yields undetermined for UNKNOWN.
    let settled = triage_settle(
        &path,
        ToolRunTriageSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            stages: Vec::new(),
            run_output: None,
            run_output_truncated: false,
            project_root: None,
            workspace_roots: Vec::new(),
            ancestry: vec!["head-3".to_string()],
            flake_baseline: Vec::new(),
            selection_records: Vec::new(),
            owner_candidates: Vec::new(),
            knobs: Default::default(),
            continuation_mode: None,
            recipe_finished_ts: Some(150),
            now_ts: Some(151),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(settled.refused.is_none());
    assert!(settled.triaged);
    assert_eq!(settled.failure_kind.as_deref(), Some("verification"));
    // Untouched with no witness and no pass witness -> UNKNOWN.
    assert_eq!(settled.verdict.as_deref(), Some("undetermined"));
    let shown = triage_show(
        &path,
        crate::tool_run::triage::ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run_found);
    assert_eq!(shown.failure_kind.as_deref(), Some("verification"));
    assert_eq!(shown.verdict.as_deref(), Some("undetermined"));
}

#[test]
fn stage_is_idempotent_and_first_writer_wins() {
    let (_temp, path) = store();
    let run = begin_named(&path, "subject-1", Some("ws-a"), 1);
    observe_fp(&path, &run.run_id, "head-3", vec![]);
    finish_failed(
        &path,
        &run.run_id,
        50,
        Some(crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited),
    );
    let request = ToolRunTriageStageRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run.run_id.clone(),
        stage: ToolRunTriageStageInputWire {
            stage_key: "lint (mypy)".to_string(),
            stage_id: Some("stage-1".to_string()),
            output: Some(mypy_output()),
            truncated: false,
            output_path: None,
        },
        project_root: None,
        workspace_roots: Vec::new(),
        ancestry: vec!["head-3".to_string()],
        flake_baseline: Vec::new(),
        selection_records: Vec::new(),
        owner_candidates: Vec::new(),
        knobs: Default::default(),
        now_ts: Some(100),
    };
    let first =
        triage_stage(&path, request.clone(), Duration::from_secs(1)).unwrap();
    let second = triage_stage(&path, request, Duration::from_secs(1)).unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(second.items.len(), 1);
    assert_eq!(
        first.items[0].label.as_ref().unwrap().class,
        second.items[0].label.as_ref().unwrap().class
    );
}

#[test]
fn known_witness_across_runs_in_store() {
    let (_temp, path) = store();
    // Witness in another workspace at an ancestor head.
    let witness = begin_named(&path, "witness-1", Some("ws-b"), 1);
    observe_fp(&path, &witness.run_id, "head-2", vec![]);
    finish_failed(
        &path,
        &witness.run_id,
        50,
        Some(crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited),
    );
    triage_stage(
        &path,
        ToolRunTriageStageRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: witness.run_id.clone(),
            stage: ToolRunTriageStageInputWire {
                stage_key: "lint (mypy)".to_string(),
                stage_id: Some("stage-1".to_string()),
                output: Some(mypy_output()),
                truncated: false,
                output_path: None,
            },
            project_root: None,
            workspace_roots: Vec::new(),
            ancestry: vec!["head-2".to_string()],
            flake_baseline: Vec::new(),
            selection_records: Vec::new(),
            owner_candidates: Vec::new(),
            knobs: Default::default(),
            now_ts: Some(60),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    // Subject at a descendant head.
    let subject = begin_named(&path, "subject-1", Some("ws-a"), 70);
    observe_fp(&path, &subject.run_id, "head-3", vec![]);
    finish_failed(
        &path,
        &subject.run_id,
        80,
        Some(crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited),
    );
    let staged = triage_stage(
        &path,
        ToolRunTriageStageRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: subject.run_id.clone(),
            stage: ToolRunTriageStageInputWire {
                stage_key: "lint (mypy)".to_string(),
                stage_id: Some("stage-1".to_string()),
                output: Some(mypy_output()),
                truncated: false,
                output_path: None,
            },
            project_root: None,
            workspace_roots: Vec::new(),
            ancestry: vec!["head-3".to_string(), "head-2".to_string()],
            flake_baseline: Vec::new(),
            selection_records: Vec::new(),
            owner_candidates: Vec::new(),
            knobs: Default::default(),
            now_ts: Some(90),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        staged.items[0].label.as_ref().unwrap().class.as_str(),
        "known"
    );
}

#[test]
fn control_and_infra_produce_no_labels() {
    // (cause, state, exit, signal) triples compatible with finish validation.
    for (cause, state, exit_code, signal) in [
        (
            crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Timeout,
            ToolRunStateWire::Signaled,
            None,
            Some(15),
        ),
        (
            crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::LaunchFailed,
            ToolRunStateWire::Failed,
            None,
            None,
        ),
    ] {
        let (_temp, path) = store();
        let run = begin_named(&path, "subject-1", Some("ws-a"), 1);
        observe_fp(&path, &run.run_id, "head-3", vec![]);
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run.run_id.clone(),
                event_id: None,
                state,
                exit_code,
                signal,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                child_process_start_identity: None,
                duration_ms: Some(1),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(50),
                terminal_cause: Some(cause),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let staged = triage_stage(
            &path,
            ToolRunTriageStageRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run.run_id.clone(),
                stage: ToolRunTriageStageInputWire {
                    stage_key: "lint (mypy)".to_string(),
                    stage_id: None,
                    output: Some(mypy_output()),
                    truncated: false,
                    output_path: None,
                },
                project_root: None,
                workspace_roots: Vec::new(),
                ancestry: vec!["head-3".to_string()],
                flake_baseline: Vec::new(),
                selection_records: Vec::new(),
                owner_candidates: Vec::new(),
                knobs: Default::default(),
                now_ts: Some(100),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(staged.items[0].label.is_none());
        let shown = triage_show(
            &path,
            crate::tool_run::triage::ToolRunTriageShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run.run_id.clone(),
                owner_kind: None,
                owner_id: None,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(shown.verdict.as_deref(), Some("undetermined"));
    }
}

#[test]
fn failures_aggregation_filters_and_isolation() {
    let (_temp, path) = store();
    for (run_id, workspace, ts) in
        [("r-1", "ws-a", 1000), ("r-2", "ws-b", 1500)]
    {
        let run = begin_named(&path, run_id, Some(workspace), ts - 50);
        observe_fp(&path, &run.run_id, "head-3", vec![]);
        finish_failed(
            &path,
            &run.run_id,
            ts - 10,
            Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
        );
        triage_stage(
            &path,
            ToolRunTriageStageRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run.run_id.clone(),
                stage: ToolRunTriageStageInputWire {
                    stage_key: "lint (mypy)".to_string(),
                    stage_id: None,
                    output: Some(mypy_output()),
                    truncated: false,
                    output_path: None,
                },
                project_root: None,
                workspace_roots: Vec::new(),
                ancestry: vec!["head-3".to_string()],
                flake_baseline: Vec::new(),
                selection_records: Vec::new(),
                owner_candidates: Vec::new(),
                knobs: Default::default(),
                now_ts: Some(ts),
            },
            Duration::from_secs(1),
        )
        .unwrap();
    }
    let groups = tool_run_failures(
        &path,
        ToolRunFailuresRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: Some("sase".to_string()),
            all_projects: false,
            tool: Some("check".to_string()),
            class: None,
            days: 7,
            limit: 50,
            now_ts: Some(2000),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(groups.groups.len(), 1);
    assert_eq!(groups.groups[0].runs, 2);
    assert_eq!(groups.groups[0].agents, 2);
    assert_eq!(groups.groups[0].workspaces, 2);
    assert_eq!(
        groups.groups[0].last_seen_ts,
        groups.groups[0].first_seen_ts.map(|first| first + 500)
    );
    // Linked-project isolation: another project never appears under sase.
    let empty = tool_run_failures(
        &path,
        ToolRunFailuresRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: Some("sase-core".to_string()),
            all_projects: false,
            tool: None,
            class: None,
            days: 7,
            limit: 50,
            now_ts: Some(2000),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(empty.groups.is_empty());
    // Missing tables probe: fresh store reads empty.
    let (fresh_temp, fresh_path) = store();
    let _ = fresh_temp;
    let missing = tool_run_failures(
        &fresh_path,
        ToolRunFailuresRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: None,
            all_projects: true,
            tool: None,
            class: None,
            days: 7,
            limit: 50,
            now_ts: Some(2000),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(missing.groups.is_empty());
}

#[test]
fn show_supports_owner_lookup() {
    let (_temp, path) = store();
    let run = begin_named(&path, "owned-1", Some("ws-a"), 1);
    // Attach an owner via direct update (begin has no owner fields in this
    // helper path beyond kind/id None, so set them explicitly).
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE runs SET owner_kind = 'bead', owner_id = 'sase-1'
             WHERE run_id = ?1",
            [&run.run_id],
        )
        .unwrap();
    }
    observe_fp(&path, &run.run_id, "head-3", vec![]);
    finish_failed(
        &path,
        &run.run_id,
        50,
        Some(crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited),
    );
    let shown = triage_show(
        &path,
        crate::tool_run::triage::ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: String::new(),
            owner_kind: Some("bead".to_string()),
            owner_id: Some("sase-1".to_string()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run_found);
    assert_eq!(shown.run_id, run.run_id);
}

#[test]
fn stage_and_settle_reject_unknown_fields() {
    let error = serde_json::from_value::<
        crate::tool_run::triage::ToolRunTriageStageRequestWire,
    >(serde_json::json!({
        "schema_version": 1, "run_id": "r",
        "stage": {
            "stage_key": "s",
        },
        "bogus": 1,
    }))
    .unwrap_err();
    assert!(error.to_string().contains("bogus"));
    let error = serde_json::from_value::<
        crate::tool_run::triage::ToolRunTriageSettleRequestWire,
    >(serde_json::json!({
        "schema_version": 1, "run_id": "r", "bogus": 1,
    }))
    .unwrap_err();
    assert!(error.to_string().contains("bogus"));
    let error = serde_json::from_value::<ToolRunFailuresRequestWire>(
        serde_json::json!({"schema_version": 1, "bogus": 1}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("bogus"));
    let error: ToolRunError = triage_stage(
        &PathBuf::from("/nonexistent/runs.sqlite"),
        serde_json::from_value::<ToolRunTriageStageRequestWire>(
            serde_json::from_str(include_str!(
                "../../fixtures/triage_stage_request.json"
            ))
            .unwrap(),
        )
        .unwrap(),
        Duration::from_secs(1),
    )
    .unwrap_err();
    let _ = error;
}
