use super::report::{
    compute_stats_report, StatsReportScope, StatsRunRow, StatsSampleRow,
    StatsStageRow,
};
use super::wire::*;
use crate::tool_run::catalog::extra_args_digest;
use crate::tool_run::demand_wire::{
    ToolRunDemandContextWire, ToolRunDemandWire, ToolRunResourceUsageWire,
    ToolRunWorkerGrantWire,
};
use crate::tool_run::wire::{
    ToolEvidenceCompletenessWire, ToolFingerprintWire, ToolRunStateWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};

fn empty_extra() -> String {
    extra_args_digest(&[]).unwrap()
}

fn row(run_id: &str) -> StatsRunRow {
    StatsRunRow {
        run_id: run_id.to_string(),
        tool_name: Some("check".into()),
        project: Some("sase".into()),
        agent: Some("agent-1".into()),
        owner_kind: None,
        state: ToolRunStateWire::Succeeded,
        created_ts: 1_000_000,
        running_ts: Some(1_000_000),
        settled_ts: Some(1_000_100),
        duration_ms: Some(100_000),
        definition_digest: "def-1".into(),
        extra_args_digest: empty_extra(),
        terminal_cause: None,
        launch_mode: None,
        has_starter: false,
        has_join: false,
        demand: None,
        fingerprint_before: None,
    }
}

fn demand(provider: Option<&str>, ceiling: Option<u64>) -> ToolRunDemandWire {
    ToolRunDemandWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        context: Some(ToolRunDemandContextWire {
            provider: provider.map(str::to_string),
            sync_ceiling_seconds: ceiling,
            sync_soft_ceiling_seconds: None,
        }),
        usage: None,
        worker_grants: Vec::new(),
        diagnostics: Vec::new(),
    }
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

fn scope(
    since: i64,
    now: i64,
    offset: i32,
    truncated: bool,
) -> StatsReportScope {
    StatsReportScope {
        project: Some("sase".into()),
        tool_name: None,
        days: 7,
        since_ts: since,
        now_ts: now,
        utc_offset_seconds: offset,
        runs_truncated: truncated,
        stages_truncated: false,
        samples_truncated: false,
    }
}

fn report(rows: &[StatsRunRow]) -> ToolRunStatsResultWire {
    report_full(rows, &[], &[], &[], &[])
}

fn report_full(
    rows: &[StatsRunRow],
    priors: &[StatsRunRow],
    stages: &[StatsStageRow],
    prior_stages: &[StatsStageRow],
    samples: &[StatsSampleRow],
) -> ToolRunStatsResultWire {
    let now = 1_000_000 + 7 * 86_400;
    compute_stats_report(
        rows,
        priors,
        stages,
        prior_stages,
        samples,
        &scope(now - 7 * 86_400, now, 0, false),
    )
    .unwrap()
}

fn group(result: ToolRunStatsResultWire) -> ToolRunStatsToolWire {
    assert_eq!(result.tools.len(), 1);
    result.tools.into_iter().next().unwrap()
}

fn route_runs(group: &ToolRunStatsToolWire, route: &str) -> usize {
    group
        .routes
        .iter()
        .find(|entry| entry.route == route)
        .map(|entry| entry.runs)
        .unwrap_or(usize::MAX)
}

#[test]
fn percentile_edges_use_nearest_rank() {
    let mut rows = Vec::new();
    for index in 0..10 {
        let mut row = row(&format!("run-{index}"));
        row.duration_ms = Some((index as i64 + 1) * 1000);
        rows.push(row);
    }
    let group = group(report(&rows));
    assert_eq!(group.duration.count, 10);
    assert_eq!(group.duration.p10_ms, Some(1000));
    assert_eq!(group.duration.p50_ms, Some(5000));
    assert_eq!(group.duration.p90_ms, Some(9000));
    assert_eq!(group.duration.max_ms, Some(10_000));
    assert_eq!(group.duration.succeeded.count, 10);
    assert_eq!(group.duration.failed.count, 0);
    assert_eq!(group.duration.failed.p50_ms, None);
}

#[test]
fn empty_group_reports_missing_not_zero() {
    let mut row = row("run-1");
    row.state = ToolRunStateWire::Running;
    row.settled_ts = None;
    row.duration_ms = None;
    let group = group(report(&[row]));
    assert_eq!(group.duration.count, 0);
    assert_eq!(group.duration.p50_ms, None);
    assert_eq!(group.outcomes.unsettled, 1);
    assert_eq!(group.outcomes.censored, 0);
}

#[test]
fn bare_cohort_excludes_extra_args() {
    let mut plain = row("run-plain");
    plain.duration_ms = Some(60_000);
    let mut extra = row("run-extra");
    extra.extra_args_digest = "nonempty".to_string();
    extra.duration_ms = Some(60_000);
    let group = group(report(&[plain, extra]));
    assert_eq!(group.duration.count, 1);
    assert_eq!(group.extra_args_runs, 1);
    assert_eq!(group.runs, 2);
}

#[test]
fn every_route_branch() {
    // Join present escalates even on a plain launch.
    let mut joined = row("run-join");
    joined.has_join = true;
    // A handoff starter whose ceiling budgets below the duration
    // escalates: ceiling 600 budgets 510 s, duration is 520 s.
    let mut over_budget = row("run-over");
    over_budget.launch_mode = Some("handoff".into());
    over_budget.has_starter = true;
    over_budget.demand = Some(demand(Some("Muse"), Some(600)));
    over_budget.duration_ms = Some(520_000);
    over_budget.settled_ts = Some(1_000_520);
    // Same ceilings with a shorter duration stay detached.
    let mut within_budget = row("run-within");
    within_budget.launch_mode = Some("handoff".into());
    within_budget.has_starter = true;
    within_budget.demand = Some(demand(Some("Muse"), Some(600)));
    within_budget.duration_ms = Some(500_000);
    within_budget.settled_ts = Some(1_000_500);
    // A handoff reservation without a starter is a plain handoff.
    let mut handoff = row("run-handoff");
    handoff.launch_mode = Some("handoff".into());
    // An owner without handoff parts is owned.
    let mut owned = row("run-owned");
    owned.owner_kind = Some("monitor".into());
    let plain = row("run-plain");
    let group = group(report(&[
        joined,
        over_budget,
        within_budget,
        handoff,
        owned,
        plain,
    ]));
    assert_eq!(route_runs(&group, "escalated"), 2);
    assert_eq!(route_runs(&group, "detached"), 1);
    assert_eq!(route_runs(&group, "handoff"), 1);
    assert_eq!(route_runs(&group, "owned"), 1);
    assert_eq!(route_runs(&group, "inline"), 1);
    // Monitor-owned is handoff plus owned plus escalated.
    assert_eq!(group.monitor_owned.runs, 4);
    // Only the 100 s runs finish under 2 m: joined, handoff, owned.
    assert_eq!(group.monitor_owned.under_2m, 3);
    assert_eq!(group.monitor_owned.under_2m_share, Some(0.75));
}

fn kill_row(run_id: &str, duration_ms: i64) -> StatsRunRow {
    let mut row = row(run_id);
    row.state = ToolRunStateWire::Signaled;
    row.duration_ms = Some(duration_ms);
    row.settled_ts = Some(row.created_ts + duration_ms / 1000);
    row
}

#[test]
fn recorded_ceiling_kill_window_edges() {
    // Ceiling 540: the window is 459,000..=600,000 ms.
    let cases = [
        ("low", 458_999, 0),
        ("min", 459_000, 1),
        ("max", 600_000, 1),
        ("high", 600_001, 0),
    ];
    for (name, duration, kills) in cases {
        let mut row = kill_row(name, duration);
        row.demand = Some(demand(Some("Muse"), Some(540)));
        let group = group(report(&[row]));
        assert_eq!(
            group.waste.killed_at_ceiling.runs, kills,
            "duration {duration}"
        );
    }
}

#[test]
fn legacy_kill_window_edges() {
    let cases = [
        ("low", 529_999, 0),
        ("min", 530_000, 1),
        ("max", 550_000, 1),
        ("high", 550_001, 0),
    ];
    for (name, duration, kills) in cases {
        let group = group(report(&[kill_row(name, duration)]));
        assert_eq!(
            group.waste.killed_at_ceiling.runs, kills,
            "duration {duration}"
        );
    }
}

#[test]
fn kill_needs_ceiling_agent_and_inline_route() {
    // Recorded context without a ceiling is never a kill, even inside
    // the legacy window.
    let mut no_ceiling = kill_row("no-ceiling", 540_000);
    no_ceiling.demand = Some(demand(Some("Muse"), None));
    assert_eq!(group(report(&[no_ceiling])).waste.killed_at_ceiling.runs, 0);
    // A human run with no agent is never a kill.
    let mut no_agent = kill_row("no-agent", 540_000);
    no_agent.agent = None;
    assert_eq!(group(report(&[no_agent])).waste.killed_at_ceiling.runs, 0);
    // An owned run inside the legacy window is never a kill.
    let mut owned = kill_row("owned", 540_000);
    owned.owner_kind = Some("monitor".into());
    assert_eq!(group(report(&[owned])).waste.killed_at_ceiling.runs, 0);
    // A failed run inside the legacy window is never a kill.
    let mut failed = kill_row("failed", 540_000);
    failed.state = ToolRunStateWire::Failed;
    failed.terminal_cause = Some("exited".into());
    assert_eq!(group(report(&[failed])).waste.killed_at_ceiling.runs, 0);
    // A non-signal terminal cause is never a kill.
    let mut timeout = kill_row("timeout", 540_000);
    timeout.terminal_cause = Some("timeout".into());
    let timeout_group = group(report(&[timeout]));
    assert_eq!(timeout_group.waste.killed_at_ceiling.runs, 0);
    assert_eq!(timeout_group.waste.timeout.runs, 1);
}

#[test]
fn rerun_window_edges() {
    // Kill ends at created 1000 + settled 2000.
    let mut kill = kill_row("kill", 540_000);
    kill.created_ts = 1000;
    kill.running_ts = Some(1000);
    kill.settled_ts = Some(2000);
    let rerun_at = |id: &str, created: i64| {
        let mut rerun = row(id);
        rerun.created_ts = created;
        rerun.running_ts = Some(created);
        rerun.settled_ts = Some(created + 60);
        rerun.duration_ms = Some(60_000);
        rerun
    };
    // Exactly at end + 1800 counts.
    let hit = group(report(&[kill.clone(), rerun_at("hit", 3800)]));
    assert_eq!(hit.reruns_after_kill, 1);
    // One second past the window does not.
    let miss = group(report(&[kill.clone(), rerun_at("miss", 3801)]));
    assert_eq!(miss.reruns_after_kill, 0);
    // The kill itself is not its own rerun.
    let solo = group(report(&[kill.clone()]));
    assert_eq!(solo.reruns_after_kill, 0);
    // A different agent's run is not a rerun.
    let mut stranger = rerun_at("stranger", 3000);
    stranger.agent = Some("agent-9".into());
    let strange = group(report(&[kill.clone(), stranger]));
    assert_eq!(strange.reruns_after_kill, 0);
}

#[test]
fn unsettled_kill_ends_at_created_plus_duration() {
    // No settled_ts: the end is created + duration seconds.
    let mut kill = kill_row("kill", 540_000);
    kill.created_ts = 1000;
    kill.settled_ts = None;
    let mut rerun = row("rerun");
    rerun.created_ts = 1000 + 540 + 1800;
    rerun.running_ts = Some(rerun.created_ts);
    rerun.settled_ts = Some(rerun.created_ts + 60);
    let hit = group(report(&[kill, rerun]));
    assert_eq!(hit.reruns_after_kill, 1);
}

#[test]
fn waste_categories_are_exclusive() {
    // A ceiling kill is signaled too, but the kill category wins.
    let kill = kill_row("kill", 540_000);
    // A failed timeout, a stopped run, a lost run without a duration,
    // an interrupted run, and a plain signal.
    let mut timeout = row("timeout");
    timeout.state = ToolRunStateWire::Failed;
    timeout.terminal_cause = Some("timeout".into());
    timeout.duration_ms = Some(60_000);
    let mut stopped = row("stopped");
    stopped.state = ToolRunStateWire::Failed;
    stopped.terminal_cause = Some("stop_requested".into());
    stopped.duration_ms = Some(30_000);
    let mut lost = row("lost");
    lost.state = ToolRunStateWire::Lost;
    lost.settled_ts = None;
    lost.duration_ms = None;
    let mut interrupted = row("interrupted");
    interrupted.state = ToolRunStateWire::Interrupted;
    interrupted.duration_ms = Some(10_000);
    let mut signal = row("signal");
    signal.state = ToolRunStateWire::Signaled;
    signal.terminal_cause = Some("signal".into());
    signal.duration_ms = Some(20_000);
    let ok = row("ok");
    let group = group(report(&[
        kill,
        timeout,
        stopped,
        lost,
        interrupted,
        signal,
        ok,
    ]));
    assert_eq!(group.waste.killed_at_ceiling.runs, 1);
    assert_eq!(group.waste.timeout.runs, 1);
    assert_eq!(group.waste.stopped.runs, 1);
    assert_eq!(group.waste.lost.runs, 1);
    assert_eq!(group.waste.lost.runs_without_duration, 1);
    assert_eq!(group.waste.interrupted.runs, 1);
    assert_eq!(group.waste.other_signal.runs, 1);
    let expected =
        (540_000 + 60_000 + 30_000 + 10_000 + 20_000) as f64 / 3_600_000.0;
    assert!((group.waste.total_hours - expected).abs() < 1e-9);
}

#[test]
fn provider_labels() {
    let mut recorded = row("recorded");
    recorded.demand = Some(demand(Some("Muse"), Some(600)));
    let mut missing = row("missing");
    missing.agent = None;
    let mut no_agent = row("no-agent");
    no_agent.agent = None;
    no_agent.demand = Some(demand(None, Some(600)));
    let mut unknown = row("unknown");
    unknown.demand = Some(demand(None, Some(600)));
    let group = group(report(&[recorded, missing, no_agent, unknown]));
    let labels: Vec<(&str, usize)> = group
        .providers
        .iter()
        .map(|entry| (entry.provider.as_str(), entry.runs))
        .collect();
    assert!(labels.contains(&("Muse", 1)));
    assert!(labels.contains(&("unrecorded", 1)));
    assert!(labels.contains(&("no-agent", 1)));
    assert!(labels.contains(&("unknown", 1)));
}

#[test]
fn trend_buckets_empty_days_and_offset() {
    let now = 2 * 86_400 + 5;
    let since = 0;
    let offset = 3600;
    let mut first_day = row("first-day");
    first_day.created_ts = 0;
    let mut second_day = row("second-day");
    second_day.created_ts = 86_400 + 10;
    second_day.state = ToolRunStateWire::Failed;
    let result = compute_stats_report(
        &[first_day, second_day],
        &[],
        &[],
        &[],
        &[],
        &scope(since, now, offset, false),
    )
    .unwrap();
    let trend = group(result).trend;
    assert_eq!(trend.len(), 3);
    assert_eq!(trend[0].day_start_ts, -3600);
    assert_eq!(trend[1].day_start_ts, 82_800);
    assert_eq!(trend[2].day_start_ts, 169_200);
    assert_eq!(trend[0].runs, 1);
    assert_eq!(trend[0].succeeded, 1);
    assert_eq!(trend[0].p50_ms, Some(100_000));
    assert_eq!(trend[1].runs, 1);
    assert_eq!(trend[1].failed, 1);
    assert_eq!(trend[2].runs, 0);
    assert_eq!(trend[2].p50_ms, None);
}

#[test]
fn repeats_after_censored_duplicates_and_unkeyed() {
    let fingerprint = complete_fingerprint();
    // run-1 is censored, so run-2 repeats after a censored run.
    let mut first = row("run-1");
    first.created_ts = 100;
    first.state = ToolRunStateWire::Signaled;
    first.settled_ts = Some(150);
    first.duration_ms = Some(50_000);
    first.fingerprint_before = Some(fingerprint.clone());
    // run-2 starts after run-1 ended: a repeat but no duplicate.
    let mut second = row("run-2");
    second.created_ts = 200;
    second.running_ts = Some(200);
    second.settled_ts = Some(1000);
    second.duration_ms = Some(3_600_000);
    second.fingerprint_before = Some(fingerprint.clone());
    // run-3 starts before run-2 ended: a concurrent duplicate, and not
    // after censored because run-2 succeeded.
    let mut third = row("run-3");
    third.created_ts = 300;
    third.running_ts = Some(250);
    third.settled_ts = Some(1300);
    third.duration_ms = Some(3_600_000);
    third.fingerprint_before = Some(fingerprint.clone());
    // No fingerprint means unkeyed.
    let mut fourth = row("run-4");
    fourth.created_ts = 400;
    let group = group(report(&[first, second, third, fourth]));
    assert_eq!(group.repeats.repeat_runs, 2);
    assert_eq!(group.repeats.repeat_hours, 2.0);
    assert_eq!(group.repeats.after_censored_runs, 1);
    assert_eq!(group.repeats.after_censored_hours, 1.0);
    assert_eq!(group.repeats.duplicate_runs, 1);
    assert_eq!(group.repeats.duplicate_hours, 1.0);
    assert_eq!(group.repeats.unkeyed_runs, 1);
}

#[test]
fn incomplete_fingerprint_is_unkeyed() {
    let mut row = row("run-1");
    row.fingerprint_before = Some(ToolFingerprintWire::default());
    let group = group(report(&[row]));
    assert_eq!(group.repeats.unkeyed_runs, 1);
    assert_eq!(group.repeats.repeat_runs, 0);
}

fn grant(
    id: &str,
    path: &str,
    granted: u32,
    wait_ms: u64,
    escalated: bool,
) -> ToolRunWorkerGrantWire {
    ToolRunWorkerGrantWire {
        grant_id: id.into(),
        source: "pytest".into(),
        observed_ts_ms: 1_759_240_000_000,
        lane: Some("fast".into()),
        path: path.into(),
        requested_floor: 4,
        requested_ceiling: 14,
        granted,
        budget: Some(24),
        wait_ms,
        selected_files: None,
        escalated_from: escalated.then(|| "scoped".to_string()),
    }
}

#[test]
fn demand_aggregates() {
    let mut first = row("run-1");
    first.duration_ms = Some(3000);
    first.demand = Some(ToolRunDemandWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
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
        worker_grants: vec![grant("g1", "lease", 12, 1000, true)],
        diagnostics: Vec::new(),
    });
    let mut second = row("run-2");
    second.duration_ms = Some(6000);
    second.demand = Some(ToolRunDemandWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        context: None,
        usage: Some(ToolRunResourceUsageWire {
            cpu_user_ms: Some(4000),
            cpu_system_ms: Some(2000),
            max_process_rss_kib: Some(3000),
            peak_tree_rss_kib: Some(9000),
            tree_rss_samples: 6,
            availability: Vec::new(),
        }),
        worker_grants: vec![
            grant("g2", "lease", 0, 2000, false),
            grant("g3", "bypass", 8, 0, false),
        ],
        diagnostics: Vec::new(),
    });
    let group = group(report(&[first, second]));
    let demand = &group.demand;
    assert_eq!(demand.runs_with_context, 1);
    assert_eq!(demand.runs_with_usage, 2);
    assert_eq!(demand.cpu_seconds_p50, Some(3.0));
    assert_eq!(demand.cpu_seconds_p90, Some(6.0));
    assert!((demand.cpu_total_hours - 9.0 / 3600.0).abs() < 1e-12);
    // 3 s over 3 s is 1 core; 6 s over 6 s is 1 core.
    assert_eq!(demand.effective_cores_p50, Some(1.0));
    assert_eq!(demand.effective_cores_p90, Some(1.0));
    assert_eq!(demand.max_process_rss_kib_p50, Some(1000));
    assert_eq!(demand.max_process_rss_kib_max, Some(3000));
    assert_eq!(demand.peak_tree_rss_kib_p90, Some(9000));
    assert_eq!(demand.runs_with_grants, 2);
    // Per-run max widths are 12 and 8.
    assert_eq!(demand.grant_width_p50, Some(8));
    assert_eq!(demand.grant_width_p90, Some(12));
    assert_eq!(demand.grant_width_max, Some(12));
    assert_eq!(demand.grant_paths.get("lease"), Some(&2));
    assert_eq!(demand.grant_paths.get("bypass"), Some(&1));
    assert_eq!(demand.token_wait_runs, 2);
    assert_eq!(demand.token_wait_ms_total, 3000);
    assert_eq!(demand.token_wait_ms_max, Some(2000));
    assert_eq!(demand.token_wait_timeouts, 1);
    assert_eq!(demand.escalated_grant_runs, 1);
}

#[test]
fn adhoc_runs_count_outside_groups() {
    let mut adhoc = row("adhoc");
    adhoc.tool_name = None;
    let plain = row("plain");
    let result = report(&[adhoc, plain]);
    assert_eq!(result.adhoc_runs, 1);
    assert_eq!(result.runs_scanned, 2);
    assert_eq!(result.tools.len(), 1);
    assert_eq!(result.tools[0].runs, 1);
}

#[test]
fn truncation_sets_flag_and_diagnostic() {
    let now = 1_000_000 + 7 * 86_400;
    let result = compute_stats_report(
        &[row("run-1")],
        &[],
        &[],
        &[],
        &[],
        &scope(now - 7 * 86_400, now, 0, true),
    )
    .unwrap();
    assert!(result.runs_truncated);
    assert_eq!(result.diagnostics.len(), 1);
    assert!(result.diagnostics[0].contains("truncated"));
}

#[test]
fn thresholds_echo_the_constants() {
    let result = report(&[]);
    assert_eq!(result.thresholds.default_days, STATS_DEFAULT_DAYS);
    assert_eq!(result.thresholds.max_days, STATS_MAX_DAYS);
    assert_eq!(result.thresholds.max_runs, STATS_MAX_RUNS);
    assert_eq!(
        result.thresholds.backtest_lookback_days,
        STATS_BACKTEST_LOOKBACK_DAYS
    );
    assert_eq!(
        result.thresholds.ceiling_kill_min_percent,
        STATS_CEILING_KILL_MIN_PERCENT
    );
    assert_eq!(
        result.thresholds.legacy_ceiling_kill_min_ms,
        STATS_LEGACY_CEILING_KILL_MIN_MS
    );
    assert_eq!(
        result.thresholds.rerun_window_seconds,
        STATS_RERUN_WINDOW_SECONDS
    );
    assert_eq!(result.tools.len(), 0);
    assert_eq!(result.diagnostics.len(), 0);
}

#[allow(clippy::too_many_arguments)]
fn stage_row(
    stage_id: &str,
    run_id: &str,
    description: &str,
    started_ms: Option<i64>,
    elapsed_ms: Option<i64>,
    exit_code: Option<i32>,
    incomplete: bool,
    in_window: bool,
) -> StatsStageRow {
    StatsStageRow {
        stage_id: stage_id.to_string(),
        run_id: run_id.to_string(),
        project: "sase".to_string(),
        tool_name: "check".to_string(),
        description: description.to_string(),
        started_ts: started_ms,
        elapsed_ms,
        exit_code,
        incomplete,
        run_running_ts: Some(1_000_000),
        in_window,
    }
}

fn sample_row(
    run_id: &str,
    observed_ts: i64,
    cpu: Option<f64>,
    memory: Option<f64>,
    io: Option<f64>,
    load_per_cpu: Option<f64>,
) -> StatsSampleRow {
    StatsSampleRow {
        run_id: run_id.to_string(),
        observed_ts,
        psi_cpu_some: cpu,
        psi_memory_some: memory,
        psi_io_some: io,
        load_per_cpu,
    }
}

#[test]
fn stage_distributions_split_ok_failed_and_incomplete() {
    // Four finished lint instances: nearest-rank p50 is the second
    // value, p90 the fourth, and the ratio is exact.
    let mut stages = Vec::new();
    for (index, elapsed) in [1000, 2000, 3000, 4000].iter().enumerate() {
        stages.push(stage_row(
            &format!("lint-{index}"),
            "run-1",
            "lint",
            Some(1_000_000_000 + index as i64),
            Some(*elapsed),
            Some(0),
            false,
            true,
        ));
    }
    // One failed, one incomplete-flagged, one finished without an
    // elapsed time, and one finished without an exit code.
    stages.push(stage_row(
        "test-0",
        "run-1",
        "test",
        Some(1_000_010_000),
        Some(5000),
        Some(2),
        false,
        true,
    ));
    stages.push(stage_row(
        "test-1",
        "run-1",
        "test",
        Some(1_000_011_000),
        Some(6000),
        Some(0),
        true,
        true,
    ));
    stages.push(stage_row(
        "test-2",
        "run-1",
        "test",
        Some(1_000_012_000),
        None,
        None,
        false,
        true,
    ));
    stages.push(stage_row(
        "test-3",
        "run-1",
        "test",
        Some(1_000_013_000),
        Some(7000),
        None,
        false,
        true,
    ));
    let group = group(report_full(&[row("run-1")], &[], &stages, &[], &[]));
    assert_eq!(group.stages.len(), 2);
    // Lint started first, so recipe order puts it first.
    assert_eq!(group.stages[0].description, "lint");
    assert_eq!(group.stages[0].runs, 4);
    assert_eq!(group.stages[0].ok, 4);
    assert_eq!(group.stages[0].failed, 0);
    assert_eq!(group.stages[0].incomplete, 0);
    assert_eq!(group.stages[0].p50_ms, Some(2000));
    assert_eq!(group.stages[0].p90_ms, Some(4000));
    assert_eq!(group.stages[0].p90_over_p50, Some(2.0));
    assert!(
        (group.stages[0].total_hours - 10_000.0 / 3_600_000.0).abs() < 1e-12
    );
    assert_eq!(group.stages[0].median_offset_ms, Some(1));
    let test = &group.stages[1];
    assert_eq!(test.runs, 4);
    assert_eq!(test.ok, 0);
    assert_eq!(test.failed, 1);
    assert_eq!(test.incomplete, 2);
    // Only the two elapsed, finished instances feed p50/p90/hours:
    // the exit-less instance counts in neither ok nor failed.
    assert_eq!(test.p50_ms, Some(5000));
    assert_eq!(test.p90_ms, Some(7000));
    assert!((test.total_hours - 12_000.0 / 3_600_000.0).abs() < 1e-12);
}

#[test]
fn stage_entries_without_offsets_sort_last() {
    let stages = vec![
        stage_row(
            "late",
            "run-1",
            "zzz",
            None,
            Some(1000),
            Some(0),
            false,
            true,
        ),
        stage_row(
            "early",
            "run-1",
            "aaa",
            Some(1_000_000_000),
            Some(1000),
            Some(0),
            false,
            true,
        ),
    ];
    let group = group(report_full(&[row("run-1")], &[], &stages, &[], &[]));
    assert_eq!(group.stages[0].description, "aaa");
    assert_eq!(group.stages[1].description, "zzz");
    assert_eq!(group.stages[1].median_offset_ms, None);
    assert_eq!(group.stages[1].p90_over_p50, Some(1.0));
}

fn bare_run(run_id: &str, order_ts: i64, duration_ms: i64) -> StatsRunRow {
    let mut row = row(run_id);
    row.created_ts = order_ts;
    row.running_ts = Some(order_ts);
    row.settled_ts = Some(order_ts + duration_ms / 1000);
    row.duration_ms = Some(duration_ms);
    row
}

#[test]
fn backtest_counts_predictions_from_lookback_priors() {
    // Twenty flat priors then three identical window runs: every
    // window run predicts, covers, and measures width 1.
    let priors: Vec<StatsRunRow> = (0..20)
        .map(|index| bare_run(&format!("prior-{index}"), 1000 + index, 10_000))
        .collect();
    let window: Vec<StatsRunRow> = (0..3)
        .map(|index| {
            bare_run(&format!("run-{index}"), 1_000_000 + index, 10_000)
        })
        .collect();
    let with_priors = group(report_full(&window, &priors, &[], &[], &[]));
    assert_eq!(with_priors.backtest.predictions, 3);
    assert_eq!(with_priors.backtest.covered, 3);
    assert_eq!(with_priors.backtest.coverage, Some(1.0));
    assert_eq!(with_priors.backtest.median_width, Some(1.0));
    assert_eq!(with_priors.backtest.meets_target, Some(true));
    assert_eq!(
        with_priors.backtest.target_coverage,
        STATS_BACKTEST_TARGET_COVERAGE
    );
    // Without the lookback priors the window alone predicts nothing.
    let solo = group(report_full(&window, &[], &[], &[], &[]));
    assert_eq!(solo.backtest.predictions, 0);
    assert_eq!(solo.backtest.coverage, None);
    assert_eq!(solo.backtest.median_width, None);
    assert_eq!(solo.backtest.meets_target, None);
}

#[test]
fn backtest_misses_and_width_floor() {
    // Twenty priors at 500 ms; the window run at 40 s misses a band
    // whose width divides by the 1 s floor: 500 / 1000, not 500 / 0.
    let priors: Vec<StatsRunRow> = (0..20)
        .map(|index| bare_run(&format!("prior-{index}"), 1000 + index, 500))
        .collect();
    let window = vec![bare_run("run-1", 2_000_000, 40_000)];
    let group = group(report_full(&window, &priors, &[], &[], &[]));
    assert_eq!(group.backtest.predictions, 1);
    assert_eq!(group.backtest.covered, 0);
    assert_eq!(group.backtest.coverage, Some(0.0));
    assert_eq!(group.backtest.median_width, Some(0.5));
    assert_eq!(group.backtest.meets_target, Some(false));
}

#[test]
fn stage_backtests_run_per_description() {
    // Twenty-one flat prior instances then one window instance: the
    // window instance predicts and covers at width 1.
    let mut prior_stages = Vec::new();
    for index in 0..21 {
        prior_stages.push(stage_row(
            &format!("prior-{index}"),
            &format!("prior-run-{index}"),
            "lint",
            Some(1_000_000 + index),
            Some(5000),
            Some(0),
            false,
            false,
        ));
    }
    let stages = vec![stage_row(
        "win-0",
        "run-1",
        "lint",
        Some(2_000_000),
        Some(5000),
        Some(0),
        false,
        true,
    )];
    let group = group(report_full(
        &[row("run-1")],
        &[],
        &stages,
        &prior_stages,
        &[],
    ));
    assert_eq!(group.stage_backtests.len(), 1);
    assert_eq!(group.stage_backtests[0].description, "lint");
    assert_eq!(group.stage_backtests[0].backtest.predictions, 1);
    assert_eq!(group.stage_backtests[0].backtest.covered, 1);
    assert_eq!(group.stage_backtests[0].backtest.meets_target, Some(true));
}

#[test]
fn pressure_buckets_busy_shares_and_p90() {
    let samples = vec![
        sample_row("run-1", 5, Some(3.0), Some(12.0), Some(1.0), Some(2.0)),
        sample_row("run-2", 10, Some(4.0), Some(13.0), None, Some(4.0)),
        sample_row("run-3", 35, None, Some(5.0), None, None),
    ];
    let result = report_full(&[], &[], &[], &[], &samples);
    let pressure = &result.pressure;
    assert_eq!(pressure.buckets, 2);
    assert_eq!(pressure.busy_buckets, 1);
    assert_eq!(pressure.buckets_with_psi, 2);
    assert_eq!(pressure.memory_over_threshold_share, Some(0.5));
    assert_eq!(pressure.busy_memory_over_threshold_share, Some(1.0));
    assert_eq!(pressure.cpu_psi_p90, Some(4.0));
    assert_eq!(pressure.memory_psi_p90, Some(13.0));
    assert_eq!(pressure.io_psi_p90, Some(1.0));
    assert_eq!(pressure.load_per_cpu_p90, Some(4.0));
}

#[test]
fn pressure_empty_samples_leave_shares_missing() {
    let result = report_full(&[], &[], &[], &[], &[]);
    let pressure = &result.pressure;
    assert_eq!(pressure.buckets, 0);
    assert_eq!(pressure.busy_buckets, 0);
    assert_eq!(pressure.memory_over_threshold_share, None);
    assert_eq!(pressure.busy_memory_over_threshold_share, None);
    assert_eq!(pressure.cpu_psi_p90, None);
    assert_eq!(pressure.load_per_cpu_p90, None);
}

#[test]
fn backtest_min_prior_boundary() {
    // Nineteen strictly earlier priors give no prediction; the
    // twentieth gives one.
    let window = vec![bare_run("run-1", 2_000_000, 10_000)];
    let priors_19: Vec<StatsRunRow> = (0..19)
        .map(|index| bare_run(&format!("prior-{index}"), 1000 + index, 10_000))
        .collect();
    assert_eq!(
        group(report_full(&window, &priors_19, &[], &[], &[]))
            .backtest
            .predictions,
        0
    );
    let priors_20: Vec<StatsRunRow> = (0..20)
        .map(|index| bare_run(&format!("prior-{index}"), 1000 + index, 10_000))
        .collect();
    let with_twenty = group(report_full(&window, &priors_20, &[], &[], &[]));
    assert_eq!(with_twenty.backtest.predictions, 1);
    assert_eq!(with_twenty.backtest.covered, 1);
}

#[test]
fn backtest_window_slides_past_sixty_priors() {
    // Seventy priors: ten ancient 1 s outliers, then sixty flat
    // 10 s runs. The window run's band draws on the sixty most
    // recent priors only, so the outliers cannot move it.
    let mut priors = Vec::new();
    for index in 0..10 {
        priors.push(bare_run(&format!("old-{index}"), 1000 + index, 1_000));
    }
    for index in 0..60 {
        priors.push(bare_run(&format!("prior-{index}"), 2000 + index, 10_000));
    }
    let window = vec![bare_run("run-1", 3_000_000, 10_000)];
    let group = group(report_full(&window, &priors, &[], &[], &[]));
    assert_eq!(group.backtest.predictions, 1);
    assert_eq!(group.backtest.covered, 1);
    assert_eq!(group.backtest.median_width, Some(1.0));
}

#[test]
fn pressure_counts_distinct_runs_once_and_ignores_buckets_without_psi() {
    // Three samples from one run in the same 30 s bucket: one
    // bucket, one distinct run, not busy alone.
    let samples = vec![
        sample_row("run-1", 5, Some(3.0), Some(12.0), Some(1.0), Some(2.0)),
        sample_row("run-1", 10, Some(4.0), Some(13.0), Some(2.0), Some(3.0)),
        sample_row("run-1", 20, Some(5.0), Some(14.0), Some(3.0), Some(4.0)),
        // A second bucket with load only and no PSI fields: it
        // counts as a bucket but stays out of the PSI shares and
        // their denominators.
        sample_row("run-2", 35, None, None, None, Some(9.0)),
    ];
    let result = report_full(&[], &[], &[], &[], &samples);
    let pressure = &result.pressure;
    assert_eq!(pressure.buckets, 2);
    assert_eq!(pressure.busy_buckets, 0);
    assert_eq!(pressure.buckets_with_psi, 1);
    assert_eq!(pressure.memory_over_threshold_share, Some(1.0));
    assert_eq!(pressure.busy_memory_over_threshold_share, None);
    assert_eq!(pressure.cpu_psi_p90, Some(5.0));
    assert_eq!(pressure.memory_psi_p90, Some(14.0));
    assert_eq!(pressure.io_psi_p90, Some(3.0));
    assert_eq!(pressure.load_per_cpu_p90, Some(9.0));
}

#[test]
fn bare_cohort_requires_recorded_duration() {
    // A succeeded run with NULL `duration_ms` but valid
    // running/settled stamps stays out of the duration percentiles
    // and the backtest series, while its effective duration still
    // counts for routes.
    let mut fallback = row("fallback");
    fallback.created_ts = 1_000_000;
    fallback.running_ts = Some(1_000_000);
    fallback.settled_ts = Some(1_000_100);
    fallback.duration_ms = None;
    let priors: Vec<StatsRunRow> = (0..20)
        .map(|index| bare_run(&format!("prior-{index}"), 1000 + index, 10_000))
        .collect();
    let window = vec![bare_run("run-1", 2_000_000, 10_000), fallback];
    let group = group(report_full(&window, &priors, &[], &[], &[]));
    assert_eq!(group.duration.count, 1);
    assert_eq!(group.backtest.predictions, 1);
    assert_eq!(route_runs(&group, "inline"), 2);
}
