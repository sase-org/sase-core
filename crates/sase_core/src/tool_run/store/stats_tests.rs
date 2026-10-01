use super::super::catalog::normalize_tool_definition;
use super::super::demand_wire::{
    ToolRunDemandContextWire, ToolRunRecordDemandRequestWire,
    ToolRunResourceUsageWire,
};
use super::super::handoff_wire::ToolRunTerminalCauseWire;
use super::super::stats::{ToolRunStatsRequestWire, STATS_MAX_DAYS};
use super::super::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolEvidenceCompletenessWire,
    ToolFingerprintSpecWire, ToolFingerprintWire, ToolLoadSampleWire,
    ToolRunAppendRequestWire, ToolRunBeginRequestWire, ToolRunEventKindWire,
    ToolRunEventWire, ToolRunFinishRequestWire, ToolRunStateWire,
    ToolStageWire, ToolStagesWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::{append_event, finish, record_demand, tool_run_stats_report};
use crate::tool_run::begin;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::tempdir;

const NOW: i64 = 1_750_000_000;

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
        duration_class: None,
        diagnostics: Vec::new(),
    }
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn begin_request(
    tool_name: Option<&str>,
    project: Option<&str>,
    now: i64,
) -> ToolRunBeginRequestWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    ToolRunBeginRequestWire {
        starter: None,
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: None,
        created_event_id: None,
        running_event_id: None,
        tool_name: tool_name.map(str::to_string),
        definition: normalized.definition,
        extra_args: Vec::new(),
        display_argv: vec!["just".into(), "check".into()],
        private_argv: None,
        project: project.map(str::to_string),
        agent: Some("agent-1".into()),
        workspace: None,
        bead: None,
        owner_kind: None,
        owner_id: None,
        parent_run_id: None,
        wrapper_pid: Some(4242),
        boot_id: Some("boot-1".into()),
        process_start_identity: Some("start-1".into()),
        events_path: None,
        log_stdout_path: None,
        log_stderr_path: None,
        now_ts: Some(now),
        commit_running: true,
        launch_mode: None,
        launch: None,
        owner_log_path: None,
    }
}

fn begin_run(path: &Path, tool: Option<&str>, project: Option<&str>) -> String {
    begin(
        path,
        begin_request(tool, project, NOW),
        Duration::from_secs(1),
    )
    .unwrap()
    .run
    .run_id
}

fn finish_state(
    path: &Path,
    run_id: &str,
    state: ToolRunStateWire,
    duration_ms: i64,
    cause: Option<ToolRunTerminalCauseWire>,
) {
    let signal = matches!(state, ToolRunStateWire::Signaled).then_some(9);
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state,
            exit_code: (!matches!(state, ToolRunStateWire::Signaled))
                .then_some(0),
            signal,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(duration_ms),
            fingerprint_before: Some(complete_fingerprint()),
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(NOW + 60),
            terminal_cause: cause,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn complete_fingerprint() -> ToolFingerprintWire {
    ToolFingerprintWire {
        completeness: ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        },
        ..ToolFingerprintWire::default()
    }
}

fn record_usage(path: &Path, run_id: &str) {
    record_demand(
        path,
        ToolRunRecordDemandRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            context: Some(ToolRunDemandContextWire {
                provider: Some("Muse".into()),
                sync_ceiling_seconds: Some(600),
                sync_soft_ceiling_seconds: None,
            }),
            usage: Some(ToolRunResourceUsageWire {
                cpu_user_ms: Some(2000),
                cpu_system_ms: Some(1000),
                max_process_rss_kib: Some(1000),
                peak_tree_rss_kib: Some(8000),
                tree_rss_samples: 3,
                availability: Vec::new(),
            }),
            worker_grants: Vec::new(),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn stats_request(
    project: Option<&str>,
    tool: Option<&str>,
    days: i64,
) -> ToolRunStatsRequestWire {
    ToolRunStatsRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: project.map(str::to_string),
        tool_name: tool.map(str::to_string),
        days,
        now_ts: Some(NOW + 3600),
        utc_offset_seconds: 0,
    }
}

fn stats(
    path: &Path,
    project: Option<&str>,
    tool: Option<&str>,
    days: i64,
) -> super::super::stats::ToolRunStatsResultWire {
    tool_run_stats_report(
        path,
        stats_request(project, tool, days),
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
fn report_over_begin_finish_and_record_demand() {
    let (_temp, path) = store();
    let ok = begin_run(&path, Some("check"), Some("sase"));
    finish_state(&path, &ok, ToolRunStateWire::Succeeded, 60_000, None);
    record_usage(&path, &ok);
    // A legacy-window signaled run with an agent is a ceiling kill.
    let killed = begin_run(&path, Some("check"), Some("sase"));
    finish_state(
        &path,
        &killed,
        ToolRunStateWire::Signaled,
        540_000,
        Some(ToolRunTerminalCauseWire::Signal),
    );
    // Another project stays out of the filtered report.
    let other = begin_run(&path, Some("check"), Some("other"));
    finish_state(&path, &other, ToolRunStateWire::Succeeded, 60_000, None);
    // Ad-hoc runs count outside the groups.
    let adhoc = begin_run(&path, None, Some("sase"));
    finish_state(&path, &adhoc, ToolRunStateWire::Succeeded, 60_000, None);

    let result = stats(&path, Some("sase"), None, 7);
    assert_eq!(result.project.as_deref(), Some("sase"));
    assert_eq!(result.runs_scanned, 3);
    assert!(!result.runs_truncated);
    assert_eq!(result.adhoc_runs, 1);
    assert_eq!(result.tools.len(), 1);
    let group = &result.tools[0];
    assert_eq!(group.tool_name, "check");
    assert_eq!(group.runs, 2);
    assert_eq!(group.outcomes.succeeded, 1);
    assert_eq!(group.outcomes.signaled, 1);
    assert_eq!(group.outcomes.censored, 1);
    assert_eq!(group.duration.count, 1);
    assert_eq!(group.waste.killed_at_ceiling.runs, 1);
    assert_eq!(group.demand.runs_with_usage, 1);
    assert_eq!(group.demand.runs_with_context, 1);
    // Both finished runs share one complete fingerprint: one repeat.
    assert_eq!(group.repeats.repeat_runs, 1);
    assert!(result.thresholds.max_runs > 0);

    let unfiltered = stats(&path, None, None, 7);
    assert_eq!(unfiltered.runs_scanned, 4);
    assert_eq!(unfiltered.tools.len(), 2);
}

#[test]
fn unmigrated_store_without_demand_column_still_loads() {
    let (_temp, path) = store();
    let run_id = begin_run(&path, Some("check"), Some("sase"));
    finish_state(&path, &run_id, ToolRunStateWire::Succeeded, 60_000, None);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("ALTER TABLE runs DROP COLUMN demand_json", [])
        .unwrap();
    drop(conn);
    let result = stats(&path, Some("sase"), None, 7);
    assert_eq!(result.runs_scanned, 1);
    assert_eq!(result.tools.len(), 1);
    assert_eq!(result.tools[0].demand.runs_with_usage, 0);
}

#[test]
fn days_outside_range_is_invalid() {
    let (_temp, path) = store();
    for days in [0, -3, STATS_MAX_DAYS + 1] {
        let error = tool_run_stats_report(
            &path,
            stats_request(None, None, days),
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(error, ToolRunError::Invalid { .. }), "days {days}");
    }
    // The bounds themselves load (the store is missing, so empty).
    for days in [1, STATS_MAX_DAYS] {
        let result = tool_run_stats_report(
            &path,
            stats_request(None, None, days),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(result.window.days, days);
    }
}

#[test]
fn missing_store_returns_empty_report() {
    let (_temp, path) = store();
    let result = stats(&path, None, None, 7);
    assert_eq!(result.runs_scanned, 0);
    assert!(result.tools.is_empty());
    assert_eq!(result.diagnostics, ["tool run store does not exist"]);
    assert!(result.thresholds.max_runs > 0);
}

fn append_stage_finished(
    path: &Path,
    run_id: &str,
    event_id: &str,
    started_ms: i64,
    elapsed_ms: i64,
) {
    append_event(
        path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: event_id.to_string(),
                run_id: run_id.to_string(),
                attempt: 1,
                kind: ToolRunEventKindWire::StageFinished,
                created_ts: (started_ms + elapsed_ms).div_euclid(1000),
                stage: Some(ToolStageWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_id: format!("{event_id}-stage"),
                    run_id: run_id.to_string(),
                    attempt: 1,
                    description: "lint".to_string(),
                    started_ts: Some(started_ms),
                    finished_ts: Some(started_ms + elapsed_ms),
                    elapsed_ms: Some(elapsed_ms),
                    exit_code: Some(0),
                    output_bytes: Some(100),
                    incomplete: false,
                    diagnostics: Vec::new(),
                }),
                sample: None,
                exit_code: None,
                signal: None,
                reason: None,
                diagnostics: Vec::new(),
            },
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn append_sample(path: &Path, run_id: &str, event_id: &str) {
    append_event(
        path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: event_id.to_string(),
                run_id: run_id.to_string(),
                attempt: 1,
                kind: ToolRunEventKindWire::Sample,
                created_ts: NOW + 100,
                stage: None,
                sample: Some(ToolLoadSampleWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    sample_id: format!("{event_id}-sample"),
                    run_id: run_id.to_string(),
                    attempt: 1,
                    observed_ts: NOW + 100,
                    elapsed_ms: None,
                    loadavg_1: Some(4.0),
                    loadavg_5: None,
                    loadavg_15: None,
                    logical_cpus: Some(2),
                    psi_cpu_some: Some(3.0),
                    psi_memory_some: Some(12.0),
                    psi_io_some: Some(1.0),
                    host_identity: None,
                    availability: Vec::new(),
                    diagnostics: Vec::new(),
                }),
                exit_code: None,
                signal: None,
                reason: None,
                diagnostics: Vec::new(),
            },
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

#[test]
fn report_includes_stages_backtest_and_pressure() {
    let (_temp, path) = store();
    // Twenty-one flat runs: the last run's whole-run backtest and the
    // last lint instance's stage backtest each see twenty priors.
    for index in 0..21 {
        let run_id = begin_run(&path, Some("check"), Some("sase"));
        append_stage_finished(
            &path,
            &run_id,
            &format!("stage-{index}"),
            (NOW + index as i64) * 1000,
            5000,
        );
        append_sample(&path, &run_id, &format!("sample-{index}"));
        finish_state(&path, &run_id, ToolRunStateWire::Succeeded, 60_000, None);
    }
    let result = stats(&path, Some("sase"), None, 7);
    assert_eq!(result.tools.len(), 1);
    let group = &result.tools[0];
    assert_eq!(group.stages.len(), 1);
    assert_eq!(group.stages[0].description, "lint");
    assert_eq!(group.stages[0].runs, 21);
    assert_eq!(group.stages[0].ok, 21);
    assert_eq!(group.stages[0].failed, 0);
    assert_eq!(group.stages[0].incomplete, 0);
    assert_eq!(group.stages[0].p50_ms, Some(5000));
    assert_eq!(group.stages[0].p90_ms, Some(5000));
    assert_eq!(group.stages[0].p90_over_p50, Some(1.0));
    assert_eq!(group.backtest.predictions, 1);
    assert_eq!(group.backtest.covered, 1);
    assert_eq!(group.backtest.meets_target, Some(true));
    assert_eq!(group.stage_backtests.len(), 1);
    assert_eq!(group.stage_backtests[0].description, "lint");
    assert_eq!(group.stage_backtests[0].backtest.predictions, 1);
    assert_eq!(group.stage_backtests[0].backtest.covered, 1);
    // All samples share one observed timestamp: one busy bucket over
    // the memory PSI threshold.
    assert_eq!(result.pressure.buckets, 1);
    assert_eq!(result.pressure.busy_buckets, 1);
    assert_eq!(result.pressure.buckets_with_psi, 1);
    assert_eq!(result.pressure.memory_over_threshold_share, Some(1.0));
    assert_eq!(result.pressure.busy_memory_over_threshold_share, Some(1.0));
    assert_eq!(result.pressure.cpu_psi_p90, Some(3.0));
    assert_eq!(result.pressure.memory_psi_p90, Some(12.0));
    assert_eq!(result.pressure.load_per_cpu_p90, Some(2.0));
    assert!(!result.stages_truncated);
    assert!(!result.samples_truncated);
    assert!(result.diagnostics.is_empty());
}

#[test]
fn malformed_sample_payload_is_skipped_with_a_diagnostic() {
    let (_temp, path) = store();
    let run_id = begin_run(&path, Some("check"), Some("sase"));
    append_sample(&path, &run_id, "sample-1");
    append_sample(&path, &run_id, "sample-2");
    finish_state(&path, &run_id, ToolRunStateWire::Succeeded, 60_000, None);
    // Corrupt one stored payload: it must not move any bucket.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE samples SET payload_json = 'not json{'
         WHERE event_id = 'sample-1'",
        [],
    )
    .unwrap();
    drop(conn);
    let result = stats(&path, Some("sase"), None, 7);
    assert_eq!(result.pressure.buckets, 1);
    assert_eq!(result.pressure.busy_buckets, 0);
    assert_eq!(result.pressure.buckets_with_psi, 1);
    assert_eq!(result.pressure.cpu_psi_p90, Some(3.0));
    assert_eq!(result.diagnostics, ["skipped 1 unparseable load samples"]);
}

/// Synthetic 10k-run / 60k-sample stats timing. Ignored by default:
/// run explicitly to record the number in the landing note.
#[test]
#[ignore]
fn perf_stats_report_ten_thousand_runs() {
    use super::super::catalog::extra_args_digest;

    let (_temp, path) = store();
    // One API-created run lays out the store; the bulk fixture is
    // direct SQL so the bench spends its wall time in the report,
    // not in 80k validated writes.
    begin_run(&path, Some("check"), Some("sase"));
    let empty_extra = extra_args_digest(&[]).unwrap();
    let payload = r#"{"psi_cpu_some":3.0,"psi_memory_some":12.0,
        "psi_io_some":1.0,"loadavg_1":4.0,"logical_cpus":2}"#
        .replace([' ', '\n'], "");
    let mut conn = rusqlite::Connection::open(&path).unwrap();
    let tx = conn.transaction().unwrap();
    for index in 0..10_000 {
        let run_id = format!("perf-{index}");
        tx.execute(
            "INSERT INTO runs(
                run_id, state, source, executor, tool_name,
                definition_digest, extra_args_digest, display_argv_json,
                project, agent, created_ts, running_ts, settled_ts,
                duration_ms, evidence_json, diagnostics_json
             ) VALUES (
                ?1, 'succeeded', 'native', 'inline', 'check', 'def-1', ?2,
                '[]', 'sase', 'agent-1', ?3, ?3, ?4, 60000, '[]', '[]'
             )",
            rusqlite::params![run_id, empty_extra, NOW, NOW + 60],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO attempts(
                run_id, attempt, state, started_ts, settled_ts, exit_code,
                signal, diagnostics_json
             ) VALUES (?1, 1, 'succeeded', ?2, ?3, 0, NULL, '[]')",
            rusqlite::params![run_id, NOW, NOW + 60],
        )
        .unwrap();
        for sample in 0..6 {
            tx.execute(
                "INSERT INTO samples(
                    sample_id, run_id, attempt, event_id, observed_ts,
                    payload_json
                 ) VALUES (?1, ?2, 1, ?3, ?4, ?5)",
                rusqlite::params![
                    format!("perf-{index}-{sample}"),
                    run_id,
                    format!("perf-ev-{index}-{sample}"),
                    NOW + 100,
                    payload,
                ],
            )
            .unwrap();
        }
        if index % 10 == 0 {
            let started_ms = (NOW + index as i64) * 1000;
            tx.execute(
                "INSERT INTO stages(
                    stage_id, run_id, attempt, event_id, description,
                    started_ts, finished_ts, elapsed_ms, exit_code,
                    output_bytes, incomplete, diagnostics_json
                 ) VALUES (
                    ?1, ?2, 1, ?3, 'lint', ?4, ?5, 5000, 0, 100, 0, '[]'
                 )",
                rusqlite::params![
                    format!("perf-stage-{index}"),
                    run_id,
                    format!("perf-stage-ev-{index}"),
                    started_ms,
                    started_ms + 5000,
                ],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    drop(conn);
    let start = std::time::Instant::now();
    let result = stats(&path, Some("sase"), None, 7);
    let elapsed = start.elapsed();
    println!("stats report over 10k runs: {elapsed:?}");
    assert_eq!(result.runs_scanned, 10_001);
    assert_eq!(result.tools.len(), 1);
    assert_eq!(result.tools[0].duration.count, 10_000);
    assert!(!result.samples_truncated);
    #[cfg(not(debug_assertions))]
    assert!(
        elapsed.as_secs_f64() < 2.0,
        "stats report took {elapsed:?}, over the 2 s budget"
    );
}
