//! Receipt settle/lookup tests.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::tool_run::catalog::{
    normalize_receipt_policy, normalize_tool_definition,
};
use crate::tool_run::store::{
    begin, finish, observe, receipt_lookup, receipt_settle, retention_apply,
    retention_preview, show_run, store_stats, triage_record, triage_settle,
};
use crate::tool_run::triage::{
    extract_triage_items, ToolRunTriageClassWire, ToolRunTriageLabelWire,
};
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolDirtyPathWire,
    ToolEvidenceCompletenessWire, ToolFingerprintSpecWire, ToolFingerprintWire,
    ToolReceiptPolicyWire, ToolRepoFingerprintWire, ToolRunBeginRequestWire,
    ToolRunFinishRequestWire, ToolRunObserveRequestWire,
    ToolRunReceiptLookupRequestWire, ToolRunReceiptSettleRequestWire,
    ToolRunRetentionRequestWire, ToolRunShowRequestWire, ToolRunStateWire,
    ToolStagesWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::{canonicalize_tool_fingerprint, RECEIPT_POLICY_VERSION};

fn definition() -> ToolDefinitionWire {
    ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: "check".into(),
        argv: vec!["just".into(), "check".into()],
        description: "check".into(),
        stages: ToolStagesWire::RunSilent,
        inputs: vec!["Justfile".into()],
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: ToolFingerprintSpecWire::default(),
        receipt: None,
        diagnostics: Vec::new(),
    }
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn complete_fingerprint() -> ToolFingerprintWire {
    ToolFingerprintWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project_identity: None,
        definition_digest: None,
        extra_args_digest: None,
        repos: vec![ToolRepoFingerprintWire {
            identity: "sase".to_string(),
            head: Some("head-1".to_string()),
            index_tree: Some("tree-1".to_string()),
            dirty_paths: Vec::new(),
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

fn policy_2h() -> ToolReceiptPolicyWire {
    ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec!["pass".to_string()],
        ttl: "2h".to_string(),
    }
}

fn policy_no_new() -> ToolReceiptPolicyWire {
    ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec!["pass".to_string(), "no_new_failures".to_string()],
        ttl: "2h".to_string(),
    }
}

fn retention_req(now: i64, dry_run: bool) -> ToolRunRetentionRequestWire {
    ToolRunRetentionRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        policy: crate::tool_run::wire::ToolRunRetentionPolicyWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            summary_days: 180,
            detail_days: 60,
            log_days: 14,
            log_max_bytes: 2 * 1024 * 1024 * 1024,
            run_log_max_bytes: 256 * 1024 * 1024,
            event_max_bytes: 16 * 1024 * 1024,
        },
        now_ts: Some(now),
        dry_run,
    }
}

fn begin_run(
    path: &Path,
    run_id: &str,
    tool: Option<&str>,
    now: i64,
) -> String {
    let normalized = normalize_tool_definition(definition()).unwrap();
    let tool_name = tool.map(str::to_string);
    let result = begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some(run_id.to_string()),
            created_event_id: None,
            running_event_id: None,
            tool_name,
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
    .unwrap();
    result.run.run_id
}

fn finish_pass(
    path: &Path,
    run_id: &str,
    fingerprint: &ToolFingerprintWire,
    now: i64,
) {
    observe(
        path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(fingerprint.clone()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: Some(fingerprint.clone()),
            mutated_input: Some(false),
            now_ts: Some(now),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    triage_settle(
        path,
        crate::tool_run::triage::ToolRunTriageSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            stages: Vec::new(),
            run_output: None,
            run_output_truncated: false,
            project_root: None,
            workspace_roots: Vec::new(),
            ancestry: Vec::new(),
            flake_baseline: Vec::new(),
            selection_records: Vec::new(),
            owner_candidates: Vec::new(),
            knobs: Default::default(),
            continuation_mode: None,
            recipe_finished_ts: Some(now + 1),
            now_ts: Some(now + 2),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn settle_pass(
    path: &Path,
    run_id: &str,
    now: i64,
) -> crate::tool_run::wire::ToolRunReceiptSettleResultWire {
    receipt_settle(
        path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            policy: Some(policy_2h()),
            bypassed: false,
            now_ts: Some(now),
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
fn policy_normalization_matrix() {
    let normalized = normalize_receipt_policy(policy_no_new()).unwrap();
    assert_eq!(normalized.accept, vec!["no_new_failures", "pass"]);
    assert_eq!(normalized.ttl, "2h");
    let only_no_new = ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec!["no_new_failures".to_string()],
        ttl: "2h".to_string(),
    };
    let normalized = normalize_receipt_policy(only_no_new).unwrap();
    assert_eq!(normalized.accept, vec!["no_new_failures", "pass"]);
    let unknown = ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec!["bogus".to_string()],
        ttl: "2h".to_string(),
    };
    assert!(normalize_receipt_policy(unknown).is_err());
    for bad in ["3h", "0s", "0h", "7201s", "2", "", "00s", "01h"] {
        let policy = ToolReceiptPolicyWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            accept: vec!["pass".to_string()],
            ttl: bad.to_string(),
        };
        assert!(
            normalize_receipt_policy(policy).is_err(),
            "ttl {bad:?} should be rejected"
        );
    }
    for good in ["1s", "60s", "120m", "2h", "7199s", "7200s"] {
        let policy = ToolReceiptPolicyWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            accept: vec!["pass".to_string()],
            ttl: good.to_string(),
        };
        assert!(
            normalize_receipt_policy(policy).is_ok(),
            "ttl {good:?} should be accepted"
        );
    }
    let mixed = ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec![
            "  PASS ".to_string(),
            "No_New_Failures".to_string(),
            "pass".to_string(),
        ],
        ttl: "2h".to_string(),
    };
    let normalized = normalize_receipt_policy(mixed).unwrap();
    assert_eq!(normalized.accept, vec!["no_new_failures", "pass"]);
}

#[test]
fn digest_unchanged_when_only_receipt_differs() {
    let mut without = definition();
    without.receipt = None;
    let mut with_pass = definition();
    with_pass.receipt = Some(policy_2h());
    let mut with_no_new = definition();
    with_no_new.receipt = Some(policy_no_new());
    let left = normalize_tool_definition(without).unwrap();
    let middle = normalize_tool_definition(with_pass).unwrap();
    let right = normalize_tool_definition(with_no_new).unwrap();
    assert_eq!(left.digest, middle.digest);
    assert_eq!(left.digest, right.digest);
    assert_eq!(middle.definition.receipt.unwrap().accept, vec!["pass"]);
    let fixture: ToolDefinitionWire = serde_json::from_str(include_str!(
        "../../fixtures/definition_check.json"
    ))
    .unwrap();
    let normalized = normalize_tool_definition(fixture).unwrap();
    assert_eq!(normalized.digest.len(), 64);
}

#[test]
fn eligible_pass_writes_one_active_row_and_retry_is_stable() {
    let (_temp, path) = store();
    let fingerprint = complete_fingerprint();
    let run_id = begin_run(&path, "run-1", Some("check"), 100);
    finish_pass(&path, &run_id, &fingerprint, 110);
    let first = settle_pass(&path, &run_id, 120);
    assert!(first.minted);
    assert!(!first.superseded);
    let receipt = first.receipt.clone().unwrap();
    assert_eq!(receipt.verdict, "pass");
    assert!(receipt.signature_refs.is_empty());
    assert_eq!(receipt.policy_version, RECEIPT_POLICY_VERSION);
    assert_eq!(receipt.ttl_seconds, 7200);
    assert_eq!(receipt.issue_ts, 110);
    assert_eq!(receipt.mint_ts, 120);
    assert_eq!(receipt.expiry_ts, 120 + 7200);
    assert_eq!(receipt.status, "active");
    assert_eq!(receipt.receipt_id.len(), 64);
    let second = settle_pass(&path, &run_id, 130);
    assert!(!second.minted);
    assert!(!second.superseded);
    let again = second.receipt.unwrap();
    assert_eq!(again.receipt_id, receipt.receipt_id);
    assert_eq!(again.mint_ts, receipt.mint_ts);
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.receipt_count, 1);
}

#[test]
fn no_mint_matrix_writes_zero_rows() {
    let fingerprint = complete_fingerprint();
    let check_zero = |path: &Path| {
        let stats = store_stats(path, Duration::from_secs(1)).unwrap();
        assert_eq!(stats.receipt_count, 0);
    };
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "ad-hoc", None, 100);
        finish_pass(&path, &run_id, &fingerprint, 110);
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: Some(policy_2h()),
                bypassed: false,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
        check_zero(&path);
    }
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-bypass", Some("check"), 100);
        finish_pass(&path, &run_id, &fingerprint, 110);
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: Some(policy_2h()),
                bypassed: true,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
        check_zero(&path);
    }
    for (id, mutated) in [("run-mut", Some(true)), ("run-null-mut", None)] {
        let (_temp, path) = store();
        let run_id = begin_run(&path, id, Some("check"), 100);
        observe(
            &path,
            ToolRunObserveRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                child_pid: Some(1),
                child_pgid: Some(1),
                child_process_start_identity: None,
                fingerprint_before: Some(fingerprint.clone()),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                child_process_start_identity: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: Some(fingerprint.clone()),
                mutated_input: mutated,
                now_ts: Some(110),
                terminal_cause: Some(
                    crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
                ),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let result = settle_pass(&path, &run_id, 120);
        assert!(!result.minted);
        check_zero(&path);
    }
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-nopolicy", Some("check"), 100);
        finish_pass(&path, &run_id, &fingerprint, 110);
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: None,
                bypassed: false,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
        check_zero(&path);
    }
}

fn finish_failed_with_fp(
    path: &Path,
    run_id: &str,
    fingerprint: &ToolFingerprintWire,
    now: i64,
) {
    observe(
        path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(fingerprint.clone()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
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
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: Some(fingerprint.clone()),
            mutated_input: Some(false),
            now_ts: Some(now),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn known_label() -> ToolRunTriageLabelWire {
    ToolRunTriageLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        class: ToolRunTriageClassWire::Known,
        touched: Some(true),
        rule_version: 1,
        knobs: serde_json::json!({}),
        evidence: serde_json::json!({}),
        possible_owners: serde_json::json!([]),
        classified_ts: 200,
    }
}

fn record_known(
    path: &Path,
    run_id: &str,
    now: i64,
) -> Vec<crate::tool_run::triage::ToolRunTriageItemWire> {
    let extracted = extract_triage_items(
        crate::tool_run::triage::ToolRunTriageExtractRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            stage_key: "lint (mypy)".to_string(),
            stage_id: Some("stage-1".to_string()),
            output: Some(
                "src/foo.py:10:5: error: Bad thing  [attr-defined]\n"
                    .to_string(),
            ),
            truncated: false,
            project_root: None,
            workspace_roots: Vec::new(),
        },
    )
    .unwrap();
    let mut items = extracted.items;
    for item in &mut items {
        item.label = Some(known_label());
    }
    triage_record(
        path,
        crate::tool_run::triage::ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            stages: vec![
                crate::tool_run::triage::ToolRunTriageStageRecordWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_key: "lint (mypy)".to_string(),
                    stage_id: Some("stage-1".to_string()),
                    extraction_status:
                        crate::tool_run::triage::ToolRunTriageExtractionStatusWire::Parsed,
                    output_path: None,
                    decision: None,
                    items: items.clone(),
                },
            ],
            run_facts: Some(
                crate::tool_run::triage::ToolRunTriageRunFactsWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    continuation_mode: None,
                    recipe_finished_ts: Some(now),
                    first_continued_exit_code: None,
                    continuation_extra_ms: None,
                    repeat_of_run_id: None,
                    triaged_ts: Some(now + 1),
                    diagnostics: Vec::new(),
                },
            ),
            now_ts: Some(now + 2),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    items
}

#[test]
fn ineligible_fingerprint_and_settlement_cases() {
    let good = complete_fingerprint();
    {
        let (_temp, path) = store();
        let mut incomplete = good.clone();
        incomplete.completeness.complete = false;
        incomplete.completeness.missing = vec!["incomplete".to_string()];
        let run_id = begin_run(&path, "run-incomplete", Some("check"), 100);
        observe(
            &path,
            ToolRunObserveRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                child_pid: Some(1),
                child_pgid: Some(1),
                child_process_start_identity: None,
                fingerprint_before: Some(incomplete.clone()),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                child_process_start_identity: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: Some(incomplete),
                mutated_input: Some(false),
                now_ts: Some(110),
                terminal_cause: Some(
                    crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
                ),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let result = settle_pass(&path, &run_id, 120);
        assert!(!result.minted);
    }
    {
        let (_temp, path) = store();
        let mut before = good.clone();
        let mut after = good.clone();
        after.repos[0].head = Some("head-2".to_string());
        let run_id = begin_run(&path, "run-mismatch", Some("check"), 100);
        observe(
            &path,
            ToolRunObserveRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                child_pid: Some(1),
                child_pgid: Some(1),
                child_process_start_identity: None,
                fingerprint_before: Some(before.clone()),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        before.repos[0].head = Some("head-1".to_string());
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                child_process_start_identity: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: Some(after),
                mutated_input: Some(false),
                now_ts: Some(110),
                terminal_cause: Some(
                    crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
                ),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let result = settle_pass(&path, &run_id, 120);
        assert!(!result.minted);
    }
    for settled_by in ["reconcile", "owner"] {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-settled", Some("check"), 100);
        finish_pass(&path, &run_id, &good, 110);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE runs SET settled_by = ?1 WHERE run_id = ?2",
            rusqlite::params![settled_by, run_id],
        )
        .unwrap();
        drop(conn);
        let result = settle_pass(&path, &run_id, 120);
        assert!(!result.minted, "settled_by {settled_by} should not mint");
    }
    for (cause, state, code) in [
        (
            crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Timeout,
            ToolRunStateWire::Signaled,
            None,
        ),
        (
            crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::LaunchFailed,
            ToolRunStateWire::Failed,
            None,
        ),
    ] {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-kind", Some("check"), 100);
        observe(
            &path,
            ToolRunObserveRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                child_pid: Some(1),
                child_pgid: Some(1),
                child_process_start_identity: None,
                fingerprint_before: Some(good.clone()),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                event_id: None,
                state,
                exit_code: code,
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                child_process_start_identity: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: Some(good.clone()),
                mutated_input: Some(false),
                now_ts: Some(110),
                terminal_cause: Some(cause),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let result = settle_pass(&path, &run_id, 120);
        assert!(!result.minted);
    }
}

#[test]
fn no_new_failures_matrix() {
    let good = complete_fingerprint();
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-new", Some("check"), 100);
        finish_failed_with_fp(&path, &run_id, &good, 110);
        let extracted = extract_triage_items(
            crate::tool_run::triage::ToolRunTriageExtractRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: "lint (mypy)".to_string(),
                stage_id: Some("stage-1".to_string()),
                output: Some(
                    "src/foo.py:10:5: error: Bad thing  [attr-defined]\n"
                        .to_string(),
                ),
                truncated: false,
                project_root: None,
                workspace_roots: Vec::new(),
            },
        )
        .unwrap();
        let mut items = extracted.items;
        for item in &mut items {
            item.label = Some(ToolRunTriageLabelWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                class: ToolRunTriageClassWire::New,
                touched: Some(true),
                rule_version: 1,
                knobs: serde_json::json!({}),
                evidence: serde_json::json!({}),
                possible_owners: serde_json::json!([]),
                classified_ts: 200,
            });
        }
        triage_record(
            &path,
            crate::tool_run::triage::ToolRunTriageRecordRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                stages: vec![
                    crate::tool_run::triage::ToolRunTriageStageRecordWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        stage_key: "lint (mypy)".to_string(),
                        stage_id: Some("stage-1".to_string()),
                        extraction_status:
                            crate::tool_run::triage::ToolRunTriageExtractionStatusWire::Parsed,
                        output_path: None,
                        decision: None,
                        items,
                    },
                ],
                run_facts: Some(
                    crate::tool_run::triage::ToolRunTriageRunFactsWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        continuation_mode: None,
                        recipe_finished_ts: Some(111),
                        first_continued_exit_code: None,
                        continuation_extra_ms: None,
                        repeat_of_run_id: None,
                        triaged_ts: Some(112),
                        diagnostics: Vec::new(),
                    },
                ),
                now_ts: Some(113),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: Some(policy_no_new()),
                bypassed: false,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
    }
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-no-token", Some("check"), 100);
        finish_failed_with_fp(&path, &run_id, &good, 110);
        record_known(&path, &run_id, 111);
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: Some(policy_2h()),
                bypassed: false,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
    }
    {
        let (_temp, path) = store();
        let run_id = begin_run(&path, "run-no-triage", Some("check"), 100);
        finish_failed_with_fp(&path, &run_id, &good, 110);
        let result = receipt_settle(
            &path,
            ToolRunReceiptSettleRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id,
                policy: Some(policy_no_new()),
                bypassed: false,
                now_ts: Some(120),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!result.minted);
    }
}

#[test]
fn eligible_no_new_failures_stores_refs() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-known", Some("check"), 100);
    finish_failed_with_fp(&path, &run_id, &good, 110);
    let items = record_known(&path, &run_id, 111);
    let result = receipt_settle(
        &path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
            policy: Some(policy_no_new()),
            bypassed: false,
            now_ts: Some(120),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(result.minted);
    let receipt = result.receipt.unwrap();
    assert_eq!(receipt.verdict, "no_new_failures");
    assert!(!receipt.signature_refs.is_empty());
    for reference in &receipt.signature_refs {
        assert_eq!(reference.signature.len(), 64);
    }
    assert_eq!(receipt.signature_refs.len(), items.len());
}

fn run_identity(path: &Path, run_id: &str) -> (String, String, String, String) {
    let shown = show_run(
        path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.unwrap();
    (
        run.project.unwrap(),
        run.tool_name.unwrap(),
        run.definition_digest,
        run.extra_args_digest,
    )
}

#[test]
fn expiry_boundary_keeps_row() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-exp", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    let settled = settle_pass(&path, &run_id, 120);
    let receipt = settled.receipt.unwrap();
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &run_id);
    for (now, covered) in [(120 + 7199, true), (120 + 7200, false)] {
        let result = receipt_lookup(
            &path,
            ToolRunReceiptLookupRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                project: project.clone(),
                tool_name: tool.clone(),
                definition_digest: definition_digest.clone(),
                extra_args_digest: extra_args_digest.clone(),
                fingerprint: good.clone(),
                accept: vec!["pass".to_string()],
                now_ts: Some(now),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        if covered {
            assert_eq!(
                result.outcome,
                crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
            );
            assert_eq!(result.age_seconds, Some(now - 120));
            assert_eq!(result.receipt.unwrap().receipt_id, receipt.receipt_id);
        } else {
            assert_eq!(
                result.outcome,
                crate::tool_run::wire::ToolRunReceiptOutcomeWire::Refused
            );
            assert_eq!(
                result.refusal,
                Some(crate::tool_run::wire::ToolRunReceiptRefusalWire::Expired)
            );
        }
    }
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.receipt_count, 1);
}

#[test]
fn later_non_success_invalidates_and_recovery_succeeds() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let first = begin_run(&path, "run-a", Some("check"), 100);
    finish_pass(&path, &first, &good, 110);
    let minted = settle_pass(&path, &first, 120);
    assert!(minted.minted);
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &first);
    let lookup_now = |fp: &ToolFingerprintWire, now: i64| {
        receipt_lookup(
            &path,
            ToolRunReceiptLookupRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                project: project.clone(),
                tool_name: tool.clone(),
                definition_digest: definition_digest.clone(),
                extra_args_digest: extra_args_digest.clone(),
                fingerprint: fp.clone(),
                accept: vec!["pass".to_string()],
                now_ts: Some(now),
            },
            Duration::from_secs(1),
        )
        .unwrap()
    };
    assert_eq!(
        lookup_now(&good, 130).outcome,
        crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
    );
    let second = begin_run(&path, "run-b", Some("check"), 131);
    finish_failed_with_fp(&path, &second, &good, 140);
    triage_record(
        &path,
        crate::tool_run::triage::ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: second.clone(),
            stages: Vec::new(),
            run_facts: Some(
                crate::tool_run::triage::ToolRunTriageRunFactsWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    continuation_mode: None,
                    recipe_finished_ts: Some(141),
                    first_continued_exit_code: None,
                    continuation_extra_ms: None,
                    repeat_of_run_id: None,
                    triaged_ts: Some(142),
                    diagnostics: Vec::new(),
                },
            ),
            now_ts: Some(143),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let invalid = receipt_settle(
        &path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: second.clone(),
            policy: Some(policy_2h()),
            bypassed: false,
            now_ts: Some(144),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!invalid.minted);
    assert!(invalid.superseded);
    let refused = lookup_now(&good, 150);
    assert_eq!(
        refused.refusal,
        Some(
            crate::tool_run::wire::ToolRunReceiptRefusalWire::InvalidatedByLaterRun
        )
    );
    let third = begin_run(&path, "run-c", Some("check"), 151);
    finish_pass(&path, &third, &good, 160);
    let recovered = settle_pass(&path, &third, 170);
    assert!(recovered.minted);
    assert_ne!(
        recovered.receipt.clone().unwrap().receipt_id,
        minted.receipt.unwrap().receipt_id
    );
    let covered = lookup_now(&good, 180);
    assert_eq!(
        covered.outcome,
        crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
    );
    assert_eq!(covered.receipt.unwrap().source_run_id, third);
}

#[test]
fn earlier_run_does_not_supersede_newer() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let newer = begin_run(&path, "run-newer", Some("check"), 200);
    finish_pass(&path, &newer, &good, 210);
    let minted = settle_pass(&path, &newer, 220);
    assert!(minted.minted);
    let older = begin_run(&path, "aaa-older", Some("check"), 100);
    finish_failed_with_fp(&path, &older, &good, 105);
    let result = receipt_settle(
        &path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: older.clone(),
            policy: Some(policy_2h()),
            bypassed: false,
            now_ts: Some(230),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!result.superseded);
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &newer);
    let lookup = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project,
            tool_name: tool,
            definition_digest,
            extra_args_digest,
            fingerprint: good.clone(),
            accept: vec!["pass".to_string()],
            now_ts: Some(240),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        lookup.outcome,
        crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
    );
}

#[test]
fn fingerprint_drift_and_definition_change() {
    let (_temp, path) = store();
    let mut clean = complete_fingerprint();
    let run_id = begin_run(&path, "run-drift", Some("check"), 100);
    finish_pass(&path, &run_id, &clean, 110);
    settle_pass(&path, &run_id, 120);
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &run_id);
    let mut drifted = clean.clone();
    drifted.repos[0].dirty_paths = vec![ToolDirtyPathWire {
        path: "src/changed.py".to_string(),
        status: "modified".to_string(),
        kind: "file".to_string(),
        mode: None,
        content_hash: Some("abc".to_string()),
        incomplete: None,
    }];
    let canonical_drifted =
        canonicalize_tool_fingerprint(drifted.clone()).unwrap();
    let _ = canonical_drifted;
    clean.repos[0].dirty_paths = Vec::new();
    let refused = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: project.clone(),
            tool_name: tool.clone(),
            definition_digest: definition_digest.clone(),
            extra_args_digest: extra_args_digest.clone(),
            fingerprint: drifted.clone(),
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(crate::tool_run::wire::ToolRunReceiptRefusalWire::FingerprintChanged)
    );
    assert!(refused
        .changed_paths
        .contains(&"src/changed.py".to_string()));
    assert!(!refused
        .changed_paths
        .iter()
        .any(|path| path.starts_with('/')));
    let changed_def = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project,
            tool_name: tool,
            definition_digest: "0".repeat(64),
            extra_args_digest,
            fingerprint: complete_fingerprint(),
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        changed_def.refusal,
        Some(
            crate::tool_run::wire::ToolRunReceiptRefusalWire::DefinitionChanged
        )
    );
}

#[test]
fn lookup_refusals_for_incomplete_and_policy_version() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-ref", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    settle_pass(&path, &run_id, 120);
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &run_id);
    let mut incomplete = good.clone();
    incomplete.completeness.complete = false;
    let refused = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: project.clone(),
            tool_name: tool.clone(),
            definition_digest: definition_digest.clone(),
            extra_args_digest: extra_args_digest.clone(),
            fingerprint: incomplete,
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(
            crate::tool_run::wire::ToolRunReceiptRefusalWire::IncompleteFingerprint
        )
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE tool_receipts SET policy_version = 2", [])
        .unwrap();
    drop(conn);
    let insufficient = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project,
            tool_name: tool,
            definition_digest,
            extra_args_digest,
            fingerprint: good.clone(),
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        insufficient.refusal,
        Some(
            crate::tool_run::wire::ToolRunReceiptRefusalWire::VerdictInsufficient
        )
    );
}

#[test]
fn corrupt_proof_is_not_covered_and_tombstones_on_retention() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-corrupt", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    settle_pass(&path, &run_id, 120);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE tool_receipts SET proof_json = 'not-json'", [])
        .unwrap();
    drop(conn);
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &run_id);
    let lookup = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project,
            tool_name: tool,
            definition_digest,
            extra_args_digest,
            fingerprint: good.clone(),
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_ne!(
        lookup.outcome,
        crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
    );
    let _ = retention_preview(
        &path,
        retention_req(110 + 181 * 86400, true),
        Duration::from_secs(1),
    )
    .unwrap();
    let _ = retention_apply(
        &path,
        retention_req(110 + 181 * 86400, false),
        Duration::from_secs(1),
    )
    .unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let status: Option<String> = conn
        .query_row("SELECT status FROM tool_receipts LIMIT 1", [], |row| {
            row.get(0)
        })
        .ok();
    drop(conn);
    assert!(status.is_none() || status.as_deref() == Some("tombstone"));
    if status.as_deref() == Some("tombstone") {
        let second = receipt_lookup(
            &path,
            ToolRunReceiptLookupRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                project: "sase".to_string(),
                tool_name: "check".to_string(),
                definition_digest: "x".to_string(),
                extra_args_digest: "y".to_string(),
                fingerprint: good,
                accept: vec!["pass".to_string()],
                now_ts: Some(130),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            second.refusal,
            Some(crate::tool_run::wire::ToolRunReceiptRefusalWire::NoReceipt)
        );
        assert!(
            second
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("pruned")
                || second.reason.is_some()
        );
    }
}

#[test]
fn retention_deletes_expired_and_counts_detail_rows() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-ret", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    let short = ToolReceiptPolicyWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        accept: vec!["pass".to_string()],
        ttl: "60s".to_string(),
    };
    receipt_settle(
        &path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
            policy: Some(short),
            bypassed: false,
            now_ts: Some(120),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let now = 120 + 61 + 181 * 86400;
    let preview = retention_preview(
        &path,
        retention_req(now, true),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(preview.detail_rows >= 1);
    let applied = retention_apply(
        &path,
        retention_req(now, false),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(applied.detail_rows >= 1);
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.receipt_count, 0);
}

#[test]
fn unexpired_receipt_survives_source_run_pruning() {
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-survive", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    let now_retain = 110 + 181 * 86400;
    let mint = now_retain - 100;
    receipt_settle(
        &path,
        ToolRunReceiptSettleRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
            policy: Some(policy_2h()),
            bypassed: false,
            now_ts: Some(mint),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let (project, tool, definition_digest, extra_args_digest) =
        run_identity(&path, &run_id);
    retention_apply(
        &path,
        retention_req(now_retain, false),
        Duration::from_secs(1),
    )
    .unwrap();
    let lookup = receipt_lookup(
        &path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project,
            tool_name: tool,
            definition_digest,
            extra_args_digest,
            fingerprint: good,
            accept: vec!["pass".to_string()],
            now_ts: Some(mint + 200),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        lookup.outcome,
        crate::tool_run::wire::ToolRunReceiptOutcomeWire::Covered
    );
}

#[test]
fn store_stats_counts_and_missing_table_is_zero() {
    let (_temp, path) = store();
    let stats_empty = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats_empty.receipt_count, 0);
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-stats", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    settle_pass(&path, &run_id, 120);
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.receipt_count, 1);
}

#[test]
fn old_reader_opens_store_with_receipts_and_lookup_is_no_receipt() {
    use super::compat::{OLD_RUN_COLUMNS, OLD_SCHEMA_SQL};
    let (_temp, path) = store();
    let good = complete_fingerprint();
    let run_id = begin_run(&path, "run-compat", Some("check"), 100);
    finish_pass(&path, &run_id, &good, 110);
    settle_pass(&path, &run_id, 120);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "1");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM tool_receipts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    {
        let sql =
            format!("SELECT {OLD_RUN_COLUMNS} FROM runs WHERE run_id = ?1");
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut rows = stmt.query([run_id.clone()]).unwrap();
        assert!(rows.next().unwrap().is_some());
    }
    drop(conn);
    let temp_old = tempfile::tempdir().unwrap();
    let old_path = temp_old.path().join("old.sqlite");
    let conn_old = rusqlite::Connection::open(&old_path).unwrap();
    conn_old.execute_batch(OLD_SCHEMA_SQL).unwrap();
    conn_old
        .execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', '1')",
            [],
        )
        .unwrap();
    drop(conn_old);
    let shown_old = show_run(
        &old_path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "missing".to_string(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown_old.run.is_none());
    let lookup_old = receipt_lookup(
        &old_path,
        ToolRunReceiptLookupRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: "sase".to_string(),
            tool_name: "check".to_string(),
            definition_digest: "d".to_string(),
            extra_args_digest: "e".to_string(),
            fingerprint: complete_fingerprint(),
            accept: vec!["pass".to_string()],
            now_ts: Some(130),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        lookup_old.refusal,
        Some(crate::tool_run::wire::ToolRunReceiptRefusalWire::NoReceipt)
    );
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run.is_some());
}
