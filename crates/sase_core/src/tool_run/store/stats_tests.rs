use super::super::catalog::normalize_tool_definition;
use super::super::demand_wire::{
    ToolRunDemandContextWire, ToolRunRecordDemandRequestWire,
    ToolRunResourceUsageWire,
};
use super::super::handoff_wire::ToolRunTerminalCauseWire;
use super::super::stats::{ToolRunStatsRequestWire, STATS_MAX_DAYS};
use super::super::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolEvidenceCompletenessWire,
    ToolFingerprintSpecWire, ToolFingerprintWire, ToolRunBeginRequestWire,
    ToolRunFinishRequestWire, ToolRunStateWire, ToolStagesWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::{finish, record_demand, tool_run_stats_report};
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
