//! Store-backed triage record/show tests.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::tool_run::store::{
    begin, finish, observe, triage_record, triage_show,
};
use crate::tool_run::triage::{
    extract_triage_items, ToolRunTriageClassWire,
    ToolRunTriageContinuationModeWire, ToolRunTriageDecisionKindWire,
    ToolRunTriageDecisionWire, ToolRunTriageExtractRequestWire,
    ToolRunTriageExtractionStatusWire, ToolRunTriageItemWire,
    ToolRunTriageLabelWire, ToolRunTriageRecordRequestWire,
    ToolRunTriageRefusalWire, ToolRunTriageRunFactsWire,
    ToolRunTriageShowRequestWire, ToolRunTriageStageRecordWire,
};
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintWire,
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
        receipt: None,
        diagnostics: Vec::new(),
    }
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn begin_named(path: &Path, run_id: &str, now: i64) -> ToolRunWire {
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
            agent: None,
            workspace: None,
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

fn begin_adhoc(path: &Path, run_id: &str) -> ToolRunWire {
    begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some(run_id.to_string()),
            created_event_id: None,
            running_event_id: None,
            tool_name: None,
            definition: ToolDefinitionWire {
                name: String::new(),
                argv: vec!["adhoc".into()],
                ..definition()
            },
            extra_args: Vec::new(),
            display_argv: vec!["adhoc".into()],
            private_argv: None,
            project: None,
            agent: None,
            workspace: None,
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
            now_ts: Some(1),
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

fn mypy_items(stage_key: &str) -> Vec<ToolRunTriageItemWire> {
    let result = extract_triage_items(ToolRunTriageExtractRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: stage_key.to_string(),
        stage_id: Some("stage-1".to_string()),
        output: Some(
            "src/foo.py:10:5: error: Bad thing  [attr-defined]\n".to_string(),
        ),
        truncated: false,
        project_root: None,
        workspace_roots: Vec::new(),
    })
    .unwrap();
    assert_eq!(result.status, ToolRunTriageExtractionStatusWire::Parsed);
    result.items
}

fn record_request(
    run_id: &str,
    items: Vec<ToolRunTriageItemWire>,
) -> ToolRunTriageRecordRequestWire {
    ToolRunTriageRecordRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        stages: vec![ToolRunTriageStageRecordWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            stage_key: "lint (mypy)".to_string(),
            stage_id: Some("stage-1".to_string()),
            extraction_status: ToolRunTriageExtractionStatusWire::Parsed,
            output_path: Some("logs/stage.log".to_string()),
            decision: None,
            items,
        }],
        run_facts: Some(ToolRunTriageRunFactsWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            continuation_mode: Some(ToolRunTriageContinuationModeWire::Never),
            recipe_finished_ts: Some(10),
            first_continued_exit_code: None,
            continuation_extra_ms: None,
            repeat_of_run_id: None,
            triaged_ts: Some(11),
            diagnostics: vec!["triage note".to_string()],
        }),
        now_ts: Some(12),
    }
}

#[test]
fn record_then_show_round_trip() {
    let (_temp, path) = store();
    let run = begin_named(&path, "triage-1", 1);
    let items = mypy_items("lint (mypy)");
    let recorded = triage_record(
        &path,
        record_request(&run.run_id, items),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(recorded.refused.is_none());
    assert_eq!(recorded.items_inserted, 1);
    assert_eq!(recorded.stages_inserted, 1);
    let shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run_found);
    assert!(shown.triaged);
    assert_eq!(shown.stages.len(), 1);
    assert_eq!(shown.items.len(), 1);
    assert!(shown.items[0].item_id.is_some());
    assert_eq!(shown.run_facts.unwrap().triaged_ts, Some(11));
}

#[test]
fn idempotent_replay_changes_nothing() {
    let (_temp, path) = store();
    let run = begin_named(&path, "triage-1", 1);
    let items = mypy_items("lint (mypy)");
    let first = triage_record(
        &path,
        record_request(&run.run_id, items.clone()),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(first.items_inserted, 1);
    // Second record with changed display/decision/label changes nothing.
    let mut changed = items;
    changed[0].display = "different display".to_string();
    changed[0].label = Some(ToolRunTriageLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        class: ToolRunTriageClassWire::Known,
        touched: Some(true),
        rule_version: 2,
        knobs: serde_json::json!({}),
        evidence: serde_json::json!({}),
        possible_owners: serde_json::json!([]),
        classified_ts: 99,
    });
    let mut request = record_request(&run.run_id, changed);
    request.stages[0].decision = Some(ToolRunTriageDecisionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        mode: ToolRunTriageContinuationModeWire::Never,
        decision: ToolRunTriageDecisionKindWire::Stop,
        reason: "changed".to_string(),
        elapsed_ms: None,
        decided_ts: 50,
    });
    // First record had no label and no decision, so the second writes them.
    let second = triage_record(&path, request, Duration::from_secs(1)).unwrap();
    assert_eq!(second.items_inserted, 0);
    assert_eq!(second.items_existing, 1);
    // Third replay with the same label/decision now keeps everything.
    let items = mypy_items("lint (mypy)");
    let third = triage_record(
        &path,
        record_request(&run.run_id, items),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(third.items_existing, 1);
    let shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id,
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    // Stored display wins over the changed replay.
    assert_ne!(shown.items[0].display, "different display");
}

#[test]
fn label_first_writer_wins() {
    let (_temp, path) = store();
    let run = begin_named(&path, "triage-1", 1);
    let mut items = mypy_items("lint (mypy)");
    items[0].label = Some(ToolRunTriageLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        class: ToolRunTriageClassWire::Known,
        touched: None,
        rule_version: 1,
        knobs: serde_json::json!({}),
        evidence: serde_json::json!({}),
        possible_owners: serde_json::json!([]),
        classified_ts: 10,
    });
    let first = triage_record(
        &path,
        record_request(&run.run_id, items),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(first.labels_written, 1);
    let mut items = mypy_items("lint (mypy)");
    items[0].label = Some(ToolRunTriageLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        class: ToolRunTriageClassWire::Flaky,
        touched: None,
        rule_version: 2,
        knobs: serde_json::json!({}),
        evidence: serde_json::json!({}),
        possible_owners: serde_json::json!([]),
        classified_ts: 20,
    });
    let second = triage_record(
        &path,
        record_request(&run.run_id, items),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(second.labels_written, 0);
    assert_eq!(second.labels_kept, 1);
    let shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id,
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        shown.items[0].label.as_ref().unwrap().class,
        ToolRunTriageClassWire::Known
    );
}

#[test]
fn refusals_for_missing_and_adhoc_runs() {
    let (_temp, path) = store();
    begin_named(&path, "real-1", 1);
    let missing = triage_record(
        &path,
        record_request("does-not-exist", mypy_items("lint (mypy)")),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(missing.refused, Some(ToolRunTriageRefusalWire::RunNotFound));
    assert_eq!(missing.items_inserted, 0);
    let adhoc = begin_adhoc(&path, "adhoc-1");
    let refused = triage_record(
        &path,
        record_request(&adhoc.run_id, mypy_items("lint (mypy)")),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(refused.refused, Some(ToolRunTriageRefusalWire::AdHocRun));
}

#[test]
fn validation_errors() {
    let (_temp, path) = store();
    let run = begin_named(&path, "triage-1", 1);
    // Empty stage key.
    let mut request = record_request(&run.run_id, Vec::new());
    request.stages[0].stage_key = String::new();
    assert!(triage_record(&path, request, Duration::from_secs(1)).is_err());
    // Item stage mismatch.
    let mut items = mypy_items("other");
    items[0].stage_key = "other".to_string();
    let request = record_request(&run.run_id, items);
    assert!(triage_record(&path, request, Duration::from_secs(1)).is_err());
    // Bad signature.
    let mut items = mypy_items("lint (mypy)");
    items[0].signature = "zzz".to_string();
    let request = record_request(&run.run_id, items);
    assert!(triage_record(&path, request, Duration::from_secs(1)).is_err());
    // Absolute locator.
    let mut items = mypy_items("lint (mypy)");
    items[0].locator_paths = vec!["/abs/path.py".to_string()];
    let request = record_request(&run.run_id, items);
    assert!(triage_record(&path, request, Duration::from_secs(1)).is_err());
    // Wrong item_id.
    let mut items = mypy_items("lint (mypy)");
    items[0].item_id = Some("wrong".to_string());
    let request = record_request(&run.run_id, items);
    assert!(triage_record(&path, request, Duration::from_secs(1)).is_err());
}

fn fingerprint_with_repo(identity: &str) -> ToolFingerprintWire {
    ToolFingerprintWire {
        repos: vec![crate::tool_run::wire::ToolRepoFingerprintWire {
            identity: identity.to_string(),
            head: Some("abc".to_string()),
            index_tree: None,
            dirty_paths: Vec::new(),
            incomplete: None,
        }],
        completeness: crate::tool_run::wire::ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        },
        ..ToolFingerprintWire::default()
    }
}

#[test]
fn observe_persists_fingerprint_before() {
    let (_temp, path) = store();
    let run = begin_named(&path, "fp-1", 1);
    let before = fingerprint_with_repo("sase");
    let observed = observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(before.clone()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!observed.replayed);
    assert!(observed.run.fingerprint_before.is_some());
    // Equal resend is a no-op replay.
    let replayed = observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(before),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(replayed.replayed);
    assert!(replayed.diagnostics.is_empty());
    // Different value is kept with a diagnostic.
    let different = observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(fingerprint_with_repo("other")),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(different
        .diagnostics
        .iter()
        .any(|line| line.contains("kept the recorded value")));
    assert_eq!(
        different.run.fingerprint_before.unwrap().repos[0].identity,
        "sase"
    );
}

#[test]
fn finish_keeps_recorded_fingerprint_before() {
    let (_temp, path) = store();
    let run = begin_named(&path, "fp-2", 1);
    let before = fingerprint_with_repo("sase");
    observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            fingerprint_before: Some(before),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id.clone(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(1),
            fingerprint_before: Some(fingerprint_with_repo("other")),
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(2),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(finished
        .diagnostics
        .iter()
        .any(|line| line.contains("kept the recorded value")));
    assert_eq!(
        finished.run.fingerprint_before.unwrap().repos[0].identity,
        "sase"
    );
}

#[test]
fn show_on_missing_store_and_old_store() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing").join("runs.sqlite");
    let shown = triage_show(
        &missing,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "x".to_string(),
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!shown.run_found);
    assert!(shown
        .diagnostics
        .iter()
        .any(|line| line.contains("does not exist")));
    // Store without triage tables: empty with run_found from runs.
    let (_temp, path) = store();
    let run = begin_named(&path, "old-1", 1);
    // Drop triage tables to simulate an old store.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "DROP TABLE IF EXISTS tool_triage_items;
         DROP TABLE IF EXISTS tool_triage_stages;
         DROP TABLE IF EXISTS tool_triage_runs;",
    )
    .unwrap();
    drop(conn);
    let shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run.run_id,
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run_found);
    assert!(!shown.triaged);
    assert!(shown.items.is_empty());
    // Tables still absent afterwards (read never creates).
    let conn = rusqlite::Connection::open(&path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'tool_triage%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn record_creates_missing_triage_tables() {
    let (_temp, path) = store();
    let run = begin_named(&path, "old-1", 1);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "DROP TABLE IF EXISTS tool_triage_items;
         DROP TABLE IF EXISTS tool_triage_stages;
         DROP TABLE IF EXISTS tool_triage_runs;",
    )
    .unwrap();
    drop(conn);
    // A record recreates the tables via SCHEMA_SQL on write open.
    let recorded = triage_record(
        &path,
        record_request(&run.run_id, mypy_items("lint (mypy)")),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(recorded.items_inserted, 1);
}

#[test]
fn unused_imports_are_used() {
    let _ = ToolRunError::invalid("x");
}
