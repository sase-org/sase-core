//! Pure stats computation over loaded run rows.
//!
//! No SQL here: the store loader maps each row to [`StatsRunRow`] and this
//! module folds rows into the report. Every definition below is owned by
//! Rust; Python renders the result without re-deriving a number.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::super::catalog::extra_args_digest;
use super::super::demand_wire::ToolRunDemandWire;
use super::super::duration::{sync_wait_budget, SyncWaitBudgetRequestWire};
use super::super::fingerprint::canonicalize_tool_fingerprint;
use super::super::wire::{
    ToolFingerprintWire, ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::wire::*;

/// One loaded `runs` row: parsed JSON stays parsed, raw text stays raw.
#[derive(Debug, Clone)]
pub struct StatsRunRow {
    pub run_id: String,
    pub tool_name: Option<String>,
    pub project: Option<String>,
    pub agent: Option<String>,
    pub owner_kind: Option<String>,
    pub state: ToolRunStateWire,
    pub created_ts: i64,
    pub running_ts: Option<i64>,
    pub settled_ts: Option<i64>,
    pub duration_ms: Option<i64>,
    pub definition_digest: String,
    pub extra_args_digest: String,
    pub terminal_cause: Option<String>,
    pub launch_mode: Option<String>,
    pub has_starter: bool,
    pub has_join: bool,
    pub demand: Option<ToolRunDemandWire>,
    pub fingerprint_before: Option<ToolFingerprintWire>,
}

/// One loaded `stages` row, stamped with its run's group keys and
/// window membership. `started_ts` is epoch milliseconds while
/// `run_running_ts` is epoch seconds.
#[derive(Debug, Clone)]
pub struct StatsStageRow {
    pub stage_id: String,
    pub run_id: String,
    pub project: String,
    pub tool_name: String,
    pub description: String,
    pub started_ts: Option<i64>,
    pub elapsed_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub incomplete: bool,
    pub run_running_ts: Option<i64>,
    pub in_window: bool,
}

/// One loaded `samples` row. `observed_ts` is epoch seconds.
#[derive(Debug, Clone)]
pub struct StatsSampleRow {
    pub run_id: String,
    pub observed_ts: i64,
    pub psi_cpu_some: Option<f64>,
    pub psi_memory_some: Option<f64>,
    pub psi_io_some: Option<f64>,
    pub load_per_cpu: Option<f64>,
}

const ROUTES: [&str; 5] =
    ["escalated", "detached", "handoff", "owned", "inline"];

fn is_monitor_route(route: &str) -> bool {
    matches!(route, "escalated" | "handoff" | "owned")
}

fn ms_hours(duration_ms: i64) -> f64 {
    duration_ms as f64 / 3_600_000.0
}

fn percentile_index(count: usize, quantile: f64) -> usize {
    if count == 0 {
        return 0;
    }
    let rank = (quantile * count as f64).ceil() as usize;
    rank.saturating_sub(1).min(count - 1)
}

fn percentile_i64(sorted: &[i64], quantile: f64) -> Option<i64> {
    sorted
        .get(percentile_index(sorted.len(), quantile))
        .copied()
}

fn percentile_u64(sorted: &[u64], quantile: f64) -> Option<u64> {
    sorted
        .get(percentile_index(sorted.len(), quantile))
        .copied()
}

fn percentile_u32(sorted: &[u32], quantile: f64) -> Option<u32> {
    sorted
        .get(percentile_index(sorted.len(), quantile))
        .copied()
}

fn percentile_f64(sorted: &[f64], quantile: f64) -> Option<f64> {
    sorted
        .get(percentile_index(sorted.len(), quantile))
        .copied()
}

fn sort_f64(values: &mut [f64]) {
    values.sort_by(|left, right| {
        left.partial_cmp(right).unwrap_or(Ordering::Equal)
    });
}

/// Effective duration: `duration_ms`, else the settled-minus-running span
/// when both stamps are present and non-negative. Otherwise unknown, and
/// those runs are counted, never guessed.
fn effective_duration_ms(row: &StatsRunRow) -> Option<i64> {
    if let Some(recorded) = row.duration_ms {
        if recorded >= 0 {
            return Some(recorded);
        }
    }
    match (row.settled_ts, row.running_ts) {
        (Some(settled), Some(running)) if settled >= running => {
            Some((settled - running).saturating_mul(1000))
        }
        _ => None,
    }
}

/// Route: the first match wins. A `handoff` launch with a starter whose
/// recorded ceilings budget below the effective duration already
/// escalated, even without a join record yet.
fn route_of(row: &StatsRunRow, effective_ms: Option<i64>) -> &'static str {
    if row.has_join {
        return "escalated";
    }
    let handoff = row.launch_mode.as_deref() == Some("handoff");
    if handoff && row.has_starter {
        if let (Some(demand), Some(duration)) =
            (row.demand.as_ref(), effective_ms)
        {
            if let Some(context) = demand.context.as_ref() {
                let ceilings = context.sync_ceiling_seconds.is_some()
                    || context.sync_soft_ceiling_seconds.is_some();
                if ceilings {
                    let budget = sync_wait_budget(SyncWaitBudgetRequestWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        ceiling_seconds: context.sync_ceiling_seconds,
                        soft_ceiling_seconds: context.sync_soft_ceiling_seconds,
                    });
                    if let Ok(budget) = budget {
                        if let Some(seconds) = budget.budget_seconds {
                            if (seconds as i128) * 1000 < duration as i128 {
                                return "escalated";
                            }
                        }
                    }
                }
            }
        }
        return "detached";
    }
    if handoff {
        return "handoff";
    }
    if row
        .owner_kind
        .as_deref()
        .is_some_and(|kind| !kind.is_empty())
    {
        return "owned";
    }
    "inline"
}

/// Ceiling kill: an inline signaled run with a non-empty agent whose
/// effective duration lands in the recorded-ceiling window, or in the
/// legacy window when no context was ever recorded. Recorded context
/// without a ceiling is never a kill.
fn is_ceiling_kill(row: &StatsRunRow, effective_ms: Option<i64>) -> bool {
    if route_of(row, effective_ms) != "inline" {
        return false;
    }
    if row.state != ToolRunStateWire::Signaled {
        return false;
    }
    if !matches!(row.terminal_cause.as_deref(), None | Some("signal")) {
        return false;
    }
    if row.agent.as_deref().is_none_or(|agent| agent.is_empty()) {
        return false;
    }
    let Some(duration) = effective_ms else {
        return false;
    };
    match row
        .demand
        .as_ref()
        .and_then(|demand| demand.context.as_ref())
    {
        Some(context) => match context.sync_ceiling_seconds {
            Some(ceiling) => {
                let lower = ceiling as i128
                    * 1000
                    * STATS_CEILING_KILL_MIN_PERCENT as i128
                    / 100;
                let upper = (ceiling as i128
                    + STATS_CEILING_KILL_GRACE_SECONDS as i128)
                    * 1000;
                lower <= duration as i128 && duration as i128 <= upper
            }
            None => false,
        },
        None => {
            let duration = duration as i128;
            STATS_LEGACY_CEILING_KILL_MIN_MS as i128 <= duration
                && duration <= STATS_LEGACY_CEILING_KILL_MAX_MS as i128
        }
    }
}

/// Provider label: unrecorded without context, else the recorded
/// provider, else no-agent when no agent either, else unknown.
fn provider_label(row: &StatsRunRow) -> String {
    match row
        .demand
        .as_ref()
        .and_then(|demand| demand.context.as_ref())
    {
        None => "unrecorded".to_string(),
        Some(context) => match context.provider.as_deref() {
            Some(provider) => provider.to_string(),
            None if row.agent.is_none() => "no-agent".to_string(),
            None => "unknown".to_string(),
        },
    }
}

fn outcome_counts(
    rows: &[(&StatsRunRow, Option<i64>)],
) -> ToolRunStatsOutcomesWire {
    let mut outcomes = ToolRunStatsOutcomesWire {
        succeeded: 0,
        failed: 0,
        signaled: 0,
        interrupted: 0,
        lost: 0,
        unsettled: 0,
        censored: 0,
    };
    for (row, _) in rows {
        match row.state {
            ToolRunStateWire::Succeeded => outcomes.succeeded += 1,
            ToolRunStateWire::Failed => outcomes.failed += 1,
            ToolRunStateWire::Signaled => outcomes.signaled += 1,
            ToolRunStateWire::Interrupted => outcomes.interrupted += 1,
            ToolRunStateWire::Lost => outcomes.lost += 1,
            ToolRunStateWire::Created | ToolRunStateWire::Running => {
                outcomes.unsettled += 1;
            }
        }
    }
    outcomes.censored =
        outcomes.signaled + outcomes.interrupted + outcomes.lost;
    outcomes
}

fn duration_summary(durations: &[i64]) -> ToolRunStatsDurationSummaryWire {
    let mut sorted = durations.to_vec();
    sorted.sort_unstable();
    ToolRunStatsDurationSummaryWire {
        count: sorted.len(),
        p10_ms: percentile_i64(&sorted, 0.10),
        p50_ms: percentile_i64(&sorted, 0.50),
        p90_ms: percentile_i64(&sorted, 0.90),
        max_ms: sorted.last().copied(),
    }
}

/// Fold one report entry for a single (project, tool) group.
/// `prior_rows` are the group's lookback rows: they never count in
/// the entry, but the backtest draws its priors from them. `stages`
/// are the group's in-window stage rows; `prior_stages` are the
/// group's lookback stage rows for the per-stage backtests.
#[allow(clippy::too_many_arguments)]
fn tool_entry(
    project: &str,
    tool_name: &str,
    rows: &[(&StatsRunRow, Option<i64>)],
    prior_rows: &[(&StatsRunRow, Option<i64>)],
    stages: &[StatsStageRow],
    prior_stages: &[StatsStageRow],
    empty_extra_args_digest: &str,
    since_ts: i64,
    now_ts: i64,
    utc_offset_seconds: i32,
) -> ToolRunStatsToolWire {
    // Bare-settled cohort: settled state, no extra args, and a
    // recorded `duration_ms`. The settled-minus-running fallback in
    // the effective duration stays for routes, kills, waste, and
    // providers, but never enters this cohort.
    let bare: Vec<(&StatsRunRow, i64)> = rows
        .iter()
        .filter(|(row, _)| {
            matches!(
                row.state,
                ToolRunStateWire::Succeeded | ToolRunStateWire::Failed
            ) && row.extra_args_digest == empty_extra_args_digest
        })
        .filter_map(|(row, _)| row.duration_ms.map(|duration| (*row, duration)))
        .collect();
    let bare_durations: Vec<i64> =
        bare.iter().map(|(_, duration)| *duration).collect();
    let bare_ok: Vec<i64> = bare
        .iter()
        .filter(|(row, _)| row.state == ToolRunStateWire::Succeeded)
        .map(|(_, duration)| *duration)
        .collect();
    let bare_failed: Vec<i64> = bare
        .iter()
        .filter(|(row, _)| row.state == ToolRunStateWire::Failed)
        .map(|(_, duration)| *duration)
        .collect();
    let summary = duration_summary(&bare_durations);
    let duration = ToolRunStatsDurationWire {
        count: summary.count,
        p10_ms: summary.p10_ms,
        p50_ms: summary.p50_ms,
        p90_ms: summary.p90_ms,
        max_ms: summary.max_ms,
        succeeded: duration_summary(&bare_ok),
        failed: duration_summary(&bare_failed),
    };
    let definition_digests: BTreeSet<&str> = bare
        .iter()
        .map(|(row, _)| row.definition_digest.as_str())
        .collect();
    let extra_args_runs = rows
        .iter()
        .filter(|(row, _)| row.extra_args_digest != empty_extra_args_digest)
        .count();
    let outcomes = outcome_counts(rows);
    let mut terminal_causes: BTreeMap<String, usize> = BTreeMap::new();
    for (row, _) in rows {
        let cause = row
            .terminal_cause
            .clone()
            .unwrap_or("unrecorded".to_string());
        *terminal_causes.entry(cause).or_insert(0) += 1;
    }
    let routes = route_section(rows);
    let monitor_owned = monitor_owned_section(&routes);
    let waste = waste_section(rows);
    let reruns_after_kill = reruns_after_kill(rows);
    let providers = provider_section(rows);
    let trend =
        trend_section(rows, &bare, since_ts, now_ts, utc_offset_seconds);
    let repeats = repeats_section(rows, now_ts);
    let demand = demand_section(rows);
    let stage_entries = stage_section(stages);
    let backtest = backtest_section(
        &bare_points(prior_rows, empty_extra_args_digest),
        &bare_points(rows, empty_extra_args_digest),
    );
    let stage_backtests = stage_backtest_section(stages, prior_stages);
    ToolRunStatsToolWire {
        project: project.to_string(),
        tool_name: tool_name.to_string(),
        runs: rows.len(),
        definition_digests: definition_digests.len(),
        extra_args_runs,
        outcomes,
        terminal_causes,
        duration,
        waste,
        reruns_after_kill,
        routes,
        monitor_owned,
        providers,
        trend,
        repeats,
        demand,
        stages: stage_entries,
        backtest,
        stage_backtests,
    }
}

fn route_section(
    rows: &[(&StatsRunRow, Option<i64>)],
) -> Vec<ToolRunStatsRouteWire> {
    let mut entries = Vec::new();
    for route in ROUTES {
        let member: Vec<(&StatsRunRow, Option<i64>)> = rows
            .iter()
            .filter(|(row, effective)| route_of(row, *effective) == route)
            .copied()
            .collect();
        let mut durations: Vec<i64> = member
            .iter()
            .filter_map(|(_, effective)| *effective)
            .collect();
        durations.sort_unstable();
        let settled = member
            .iter()
            .filter(|(row, _)| row.state.is_terminal())
            .count();
        let under_2m = durations
            .iter()
            .filter(|duration| **duration < STATS_SHORT_MONITOR_RUN_MS)
            .count();
        let under_5m = durations
            .iter()
            .filter(|duration| **duration < STATS_MEDIUM_MONITOR_RUN_MS)
            .count();
        let kills = member
            .iter()
            .filter(|(row, effective)| is_ceiling_kill(row, *effective))
            .count();
        entries.push(ToolRunStatsRouteWire {
            route: route.to_string(),
            runs: member.len(),
            settled,
            p50_ms: percentile_i64(&durations, 0.50),
            p90_ms: percentile_i64(&durations, 0.90),
            under_2m,
            under_5m,
            kills,
        });
    }
    entries
}

fn monitor_owned_section(
    routes: &[ToolRunStatsRouteWire],
) -> ToolRunStatsMonitorOwnedWire {
    let mut owned = ToolRunStatsMonitorOwnedWire {
        runs: 0,
        under_2m: 0,
        under_5m: 0,
        under_2m_share: None,
    };
    for entry in routes {
        if is_monitor_route(&entry.route) {
            owned.runs += entry.runs;
            owned.under_2m += entry.under_2m;
            owned.under_5m += entry.under_5m;
        }
    }
    if owned.runs > 0 {
        owned.under_2m_share = Some(owned.under_2m as f64 / owned.runs as f64);
    }
    owned
}

fn waste_section(
    rows: &[(&StatsRunRow, Option<i64>)],
) -> ToolRunStatsWasteWire {
    let mut killed = ToolRunStatsWasteCategoryWire {
        runs: 0,
        hours: 0.0,
        runs_without_duration: 0,
    };
    let mut timeout = killed.clone();
    let mut stopped = killed.clone();
    let mut lost = killed.clone();
    let mut interrupted = killed.clone();
    let mut other_signal = killed.clone();
    // Categories are exclusive; the first match wins.
    for (row, effective) in rows {
        let kill = is_ceiling_kill(row, *effective);
        let category = if kill {
            &mut killed
        } else if row.terminal_cause.as_deref() == Some("timeout") {
            &mut timeout
        } else if row.terminal_cause.as_deref() == Some("stop_requested") {
            &mut stopped
        } else if row.state == ToolRunStateWire::Lost {
            &mut lost
        } else if row.state == ToolRunStateWire::Interrupted {
            &mut interrupted
        } else if row.state == ToolRunStateWire::Signaled {
            &mut other_signal
        } else {
            continue;
        };
        category.runs += 1;
        match effective {
            Some(duration) => category.hours += ms_hours(*duration),
            None => category.runs_without_duration += 1,
        }
    }
    let total_hours = killed.hours
        + timeout.hours
        + stopped.hours
        + lost.hours
        + interrupted.hours
        + other_signal.hours;
    ToolRunStatsWasteWire {
        total_hours,
        killed_at_ceiling: killed,
        timeout,
        stopped,
        lost,
        interrupted,
        other_signal,
    }
}

/// Kills with at least one same-group, same-agent rerun created within
/// the rerun window past the kill end.
fn reruns_after_kill(rows: &[(&StatsRunRow, Option<i64>)]) -> usize {
    let mut count = 0;
    let kill_list: Vec<(&StatsRunRow, String, i64)> = rows
        .iter()
        .filter(|(row, effective)| is_ceiling_kill(row, *effective))
        .filter_map(|(row, effective)| {
            let agent = row.agent.clone()?;
            if agent.is_empty() {
                return None;
            }
            let end = row.settled_ts.unwrap_or_else(|| {
                row.created_ts + effective.unwrap_or(0) / 1000
            });
            Some((*row, agent, end))
        })
        .collect();
    for (kill, agent, end) in &kill_list {
        let found = rows.iter().any(|(row, _)| {
            row.run_id != kill.run_id
                && row.agent.as_deref() == Some(agent.as_str())
                && row.created_ts > kill.created_ts
                && row.created_ts <= end + STATS_RERUN_WINDOW_SECONDS
        });
        if found {
            count += 1;
        }
    }
    count
}

fn provider_section(
    rows: &[(&StatsRunRow, Option<i64>)],
) -> Vec<ToolRunStatsProviderWire> {
    let mut groups: BTreeMap<String, Vec<(&StatsRunRow, Option<i64>)>> =
        BTreeMap::new();
    for (row, effective) in rows {
        groups
            .entry(provider_label(row))
            .or_default()
            .push((*row, *effective));
    }
    let mut entries = Vec::new();
    for (provider, member) in &groups {
        let mut durations: Vec<i64> = member
            .iter()
            .filter_map(|(_, effective)| *effective)
            .collect();
        durations.sort_unstable();
        let mut kills = 0;
        let mut kill_hours = 0.0;
        for (row, effective) in member {
            if is_ceiling_kill(row, *effective) {
                kills += 1;
                if let Some(duration) = effective {
                    kill_hours += ms_hours(*duration);
                }
            }
        }
        let mut routes: BTreeMap<String, usize> = BTreeMap::new();
        for route in ROUTES {
            routes.insert(route.to_string(), 0);
        }
        for (row, effective) in member {
            let route = route_of(row, *effective);
            *routes.entry(route.to_string()).or_insert(0) += 1;
        }
        entries.push(ToolRunStatsProviderWire {
            provider: provider.clone(),
            runs: member.len(),
            outcomes: outcome_counts(member),
            p50_ms: percentile_i64(&durations, 0.50),
            p90_ms: percentile_i64(&durations, 0.90),
            kills,
            kill_hours,
            routes,
        });
    }
    entries.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.provider.cmp(&right.provider))
    });
    entries
}

/// One local-day bucket per day from `since` through `now`, empty days
/// included. The day index is `floor((ts + offset) / 86400)`.
fn trend_section(
    rows: &[(&StatsRunRow, Option<i64>)],
    bare: &[(&StatsRunRow, i64)],
    since_ts: i64,
    now_ts: i64,
    utc_offset_seconds: i32,
) -> Vec<ToolRunStatsTrendBucketWire> {
    const DAY: i64 = 86_400;
    let offset = utc_offset_seconds as i64;
    let first = (since_ts + offset).div_euclid(DAY);
    let last = (now_ts + offset).div_euclid(DAY);
    let count = (last - first + 1).max(1) as usize;
    let mut buckets = Vec::new();
    for index in first..=last {
        buckets.push(ToolRunStatsTrendBucketWire {
            day_start_ts: index * DAY - offset,
            runs: 0,
            succeeded: 0,
            failed: 0,
            censored: 0,
            killed_at_ceiling: 0,
            monitor_owned: 0,
            p50_ms: None,
        });
    }
    let slot = |ts: i64| {
        ((ts + offset).div_euclid(DAY) - first).clamp(0, count as i64 - 1)
            as usize
    };
    let mut bare_by_bucket: Vec<Vec<i64>> = vec![Vec::new(); count];
    for (row, duration) in bare {
        bare_by_bucket[slot(row.created_ts)].push(*duration);
    }
    for (row, effective) in rows {
        let bucket = &mut buckets[slot(row.created_ts)];
        bucket.runs += 1;
        match row.state {
            ToolRunStateWire::Succeeded => bucket.succeeded += 1,
            ToolRunStateWire::Failed => bucket.failed += 1,
            ToolRunStateWire::Signaled
            | ToolRunStateWire::Interrupted
            | ToolRunStateWire::Lost => bucket.censored += 1,
            ToolRunStateWire::Created | ToolRunStateWire::Running => {}
        }
        if is_ceiling_kill(row, *effective) {
            bucket.killed_at_ceiling += 1;
        }
        if is_monitor_route(route_of(row, *effective)) {
            bucket.monitor_owned += 1;
        }
    }
    for (bucket, durations) in buckets.iter_mut().zip(bare_by_bucket.iter_mut())
    {
        durations.sort_unstable();
        bucket.p50_ms = percentile_i64(durations, 0.50);
    }
    buckets
}

/// Exact-state repetition over in-window runs. Only runs whose
/// `fingerprint_before` parses and is complete get a key; the rest count
/// as unkeyed.
fn repeats_section(
    rows: &[(&StatsRunRow, Option<i64>)],
    now_ts: i64,
) -> ToolRunStatsRepeatsWire {
    let mut ordered: Vec<(&StatsRunRow, Option<i64>)> = rows.to_vec();
    ordered.sort_by(|left, right| {
        left.0
            .created_ts
            .cmp(&right.0.created_ts)
            .then_with(|| left.0.run_id.cmp(&right.0.run_id))
    });
    let mut seen: BTreeMap<String, ()> = BTreeMap::new();
    let mut last_censored: BTreeMap<String, bool> = BTreeMap::new();
    let mut latest_end: BTreeMap<String, i64> = BTreeMap::new();
    let mut repeats = ToolRunStatsRepeatsWire {
        repeat_runs: 0,
        repeat_hours: 0.0,
        after_censored_runs: 0,
        after_censored_hours: 0.0,
        duplicate_runs: 0,
        duplicate_hours: 0.0,
        unkeyed_runs: 0,
    };
    for (row, effective) in &ordered {
        let Some(key) = repeat_key(row) else {
            repeats.unkeyed_runs += 1;
            continue;
        };
        let hours = effective.map(ms_hours).unwrap_or(0.0);
        if seen.contains_key(&key) {
            repeats.repeat_runs += 1;
            repeats.repeat_hours += hours;
            if last_censored.get(&key).copied().unwrap_or(false) {
                repeats.after_censored_runs += 1;
                repeats.after_censored_hours += hours;
            }
            let started = row.running_ts.unwrap_or(row.created_ts);
            if let Some(end) = latest_end.get(&key) {
                if started < *end {
                    repeats.duplicate_runs += 1;
                    repeats.duplicate_hours += hours;
                }
            }
        }
        seen.insert(key.clone(), ());
        last_censored.insert(
            key.clone(),
            matches!(
                row.state,
                ToolRunStateWire::Signaled
                    | ToolRunStateWire::Interrupted
                    | ToolRunStateWire::Lost
            ),
        );
        let end = row.settled_ts.unwrap_or(now_ts);
        latest_end
            .entry(key)
            .and_modify(|known| *known = (*known).max(end))
            .or_insert(end);
    }
    repeats
}

fn repeat_key(row: &StatsRunRow) -> Option<String> {
    let fingerprint = row.fingerprint_before.clone()?;
    if !fingerprint.completeness.complete {
        return None;
    }
    let canonical = canonicalize_tool_fingerprint(fingerprint).ok()?;
    Some(format!(
        "{}\x00{}\x00{}\x00{}\x00{}",
        row.project.clone().unwrap_or_default(),
        row.tool_name.clone().unwrap_or_default(),
        row.definition_digest,
        row.extra_args_digest,
        canonical.digest,
    ))
}

fn demand_section(
    rows: &[(&StatsRunRow, Option<i64>)],
) -> ToolRunStatsDemandWire {
    let mut cpu_seconds: Vec<f64> = Vec::new();
    let mut cores: Vec<f64> = Vec::new();
    let mut max_rss: Vec<u64> = Vec::new();
    let mut tree_rss: Vec<u64> = Vec::new();
    let mut grant_widths: Vec<u32> = Vec::new();
    let mut demand = ToolRunStatsDemandWire {
        runs_with_context: 0,
        runs_with_usage: 0,
        cpu_seconds_p50: None,
        cpu_seconds_p90: None,
        cpu_total_hours: 0.0,
        effective_cores_p50: None,
        effective_cores_p90: None,
        max_process_rss_kib_p50: None,
        max_process_rss_kib_p90: None,
        max_process_rss_kib_max: None,
        peak_tree_rss_kib_p50: None,
        peak_tree_rss_kib_p90: None,
        peak_tree_rss_kib_max: None,
        runs_with_grants: 0,
        grant_width_p50: None,
        grant_width_p90: None,
        grant_width_max: None,
        grant_paths: BTreeMap::new(),
        token_wait_runs: 0,
        token_wait_ms_total: 0,
        token_wait_ms_max: None,
        token_wait_timeouts: 0,
        escalated_grant_runs: 0,
    };
    for (row, effective) in rows {
        let Some(record) = row.demand.as_ref() else {
            continue;
        };
        if record.context.is_some() {
            demand.runs_with_context += 1;
        }
        if let Some(usage) = record.usage.as_ref() {
            demand.runs_with_usage += 1;
            let cpu_ms = usage.cpu_user_ms.unwrap_or(0)
                + usage.cpu_system_ms.unwrap_or(0);
            if usage.cpu_user_ms.is_some() || usage.cpu_system_ms.is_some() {
                let seconds = cpu_ms as f64 / 1000.0;
                cpu_seconds.push(seconds);
                demand.cpu_total_hours += seconds / 3600.0;
                if let Some(wall_ms) = effective {
                    if *wall_ms >= 1000 {
                        cores.push(seconds / (*wall_ms as f64 / 1000.0));
                    }
                }
            }
            if let Some(rss) = usage.max_process_rss_kib {
                max_rss.push(rss);
            }
            if let Some(rss) = usage.peak_tree_rss_kib {
                tree_rss.push(rss);
            }
        }
        if record.worker_grants.is_empty() {
            continue;
        }
        demand.runs_with_grants += 1;
        let width =
            record.worker_grants.iter().map(|grant| grant.granted).max();
        if let Some(width) = width {
            grant_widths.push(width);
        }
        let mut run_wait_ms: u64 = 0;
        let mut escalated = false;
        for grant in &record.worker_grants {
            *demand.grant_paths.entry(grant.path.clone()).or_insert(0) += 1;
            run_wait_ms += grant.wait_ms;
            if grant.path == "lease" && grant.granted == 0 {
                demand.token_wait_timeouts += 1;
            }
            if grant.escalated_from.is_some() {
                escalated = true;
            }
        }
        if run_wait_ms > 0 {
            demand.token_wait_runs += 1;
            demand.token_wait_ms_total += run_wait_ms;
            demand.token_wait_ms_max =
                Some(demand.token_wait_ms_max.unwrap_or(0).max(run_wait_ms));
        }
        if escalated {
            demand.escalated_grant_runs += 1;
        }
    }
    sort_f64(&mut cpu_seconds);
    sort_f64(&mut cores);
    max_rss.sort_unstable();
    tree_rss.sort_unstable();
    grant_widths.sort_unstable();
    demand.cpu_seconds_p50 = percentile_f64(&cpu_seconds, 0.50);
    demand.cpu_seconds_p90 = percentile_f64(&cpu_seconds, 0.90);
    demand.effective_cores_p50 = percentile_f64(&cores, 0.50);
    demand.effective_cores_p90 = percentile_f64(&cores, 0.90);
    demand.max_process_rss_kib_p50 = percentile_u64(&max_rss, 0.50);
    demand.max_process_rss_kib_p90 = percentile_u64(&max_rss, 0.90);
    demand.max_process_rss_kib_max = max_rss.last().copied();
    demand.peak_tree_rss_kib_p50 = percentile_u64(&tree_rss, 0.50);
    demand.peak_tree_rss_kib_p90 = percentile_u64(&tree_rss, 0.90);
    demand.peak_tree_rss_kib_max = tree_rss.last().copied();
    demand.grant_width_p50 = percentile_u32(&grant_widths, 0.50);
    demand.grant_width_p90 = percentile_u32(&grant_widths, 0.90);
    demand.grant_width_max = grant_widths.last().copied();
    demand
}

/// Per-stage distributions over a group's in-window stage rows.
/// A finished instance has `incomplete = false` and `elapsed_ms`
/// present. `runs` counts stage instances; `ok` and `failed` split
/// finished instances by exit code, so a finished instance without a
/// recorded exit counts in neither. Entries sort by `median_offset_ms`
/// so stages read in recipe order; undescribed offsets sort last.
fn stage_section(stages: &[StatsStageRow]) -> Vec<ToolRunStatsStageWire> {
    let mut groups: BTreeMap<&str, Vec<&StatsStageRow>> = BTreeMap::new();
    for stage in stages {
        groups
            .entry(stage.description.as_str())
            .or_default()
            .push(stage);
    }
    let mut entries = Vec::new();
    for (description, member) in &groups {
        let mut finished_ms: Vec<i64> = Vec::new();
        let mut offsets: Vec<i64> = Vec::new();
        let mut ok = 0;
        let mut failed = 0;
        let mut incomplete = 0;
        let mut hours = 0.0;
        for stage in member {
            if stage.incomplete || stage.elapsed_ms.is_none() {
                incomplete += 1;
            } else {
                let elapsed = stage.elapsed_ms.unwrap_or(0);
                finished_ms.push(elapsed);
                hours += elapsed as f64 / 3_600_000.0;
                match stage.exit_code {
                    Some(0) => ok += 1,
                    Some(_) => failed += 1,
                    None => {}
                }
            }
            if let (Some(started), Some(running)) =
                (stage.started_ts, stage.run_running_ts)
            {
                offsets
                    .push(started.saturating_sub(running.saturating_mul(1000)));
            }
        }
        finished_ms.sort_unstable();
        offsets.sort_unstable();
        let p50_ms = percentile_i64(&finished_ms, 0.50);
        let p90_ms = percentile_i64(&finished_ms, 0.90);
        let p90_over_p50 = match (p50_ms, p90_ms) {
            (Some(lo), Some(hi)) if lo > 0 => Some(hi as f64 / lo as f64),
            _ => None,
        };
        entries.push(ToolRunStatsStageWire {
            description: (*description).to_string(),
            runs: member.len(),
            ok,
            failed,
            incomplete,
            p50_ms,
            p90_ms,
            p90_over_p50,
            total_hours: hours,
            median_offset_ms: percentile_i64(&offsets, 0.50),
        });
    }
    entries.sort_by(|left, right| {
        let left_key = match left.median_offset_ms {
            Some(offset) => (0, offset),
            None => (1, 0),
        };
        let right_key = match right.median_offset_ms {
            Some(offset) => (0, offset),
            None => (1, 0),
        };
        left_key
            .cmp(&right_key)
            .then_with(|| left.description.cmp(&right.description))
    });
    entries
}

/// Bare-settled series points: (order timestamp, tie-break id,
/// value). The cohort matches the duration summary: settled state,
/// no extra args, recorded `duration_ms`. Whole-run points order by
/// `running_ts` else `created_ts`; stage points order by `started_ts`.
fn bare_points(
    rows: &[(&StatsRunRow, Option<i64>)],
    empty_extra_args_digest: &str,
) -> Vec<(i64, String, i64)> {
    let mut points = Vec::new();
    for (row, _) in rows {
        if !matches!(
            row.state,
            ToolRunStateWire::Succeeded | ToolRunStateWire::Failed
        ) {
            continue;
        }
        if row.extra_args_digest != empty_extra_args_digest {
            continue;
        }
        let Some(duration) = row.duration_ms else {
            continue;
        };
        points.push((
            row.running_ts.unwrap_or(row.created_ts),
            row.run_id.clone(),
            duration,
        ));
    }
    points
}

/// Chronological backtest of the unconditioned empirical-quantile
/// baseline: each in-window point with at least
/// `STATS_BACKTEST_MIN_PRIOR_RUNS` strictly earlier series points is
/// predicted from the `STATS_BACKTEST_PRIOR_RUNS` most recent earlier
/// points (lookback points count) as `p10..=p90`. Covered when the
/// value lands inside; width is `hi / max(lo, floor)`. This is the
/// baseline a future conditioned forecast must beat, not a forecaster.
fn backtest_section(
    prior_points: &[(i64, String, i64)],
    window_points: &[(i64, String, i64)],
) -> ToolRunStatsBacktestWire {
    let mut series: Vec<(i64, &str, i64, bool)> = Vec::new();
    for point in prior_points {
        series.push((point.0, point.1.as_str(), point.2, false));
    }
    for point in window_points {
        series.push((point.0, point.1.as_str(), point.2, true));
    }
    series.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1))
    });
    let mut history: Vec<i64> = Vec::new();
    let mut widths: Vec<f64> = Vec::new();
    let mut predictions = 0;
    let mut covered = 0;
    for (_, _, value, in_window) in &series {
        if *in_window && history.len() >= STATS_BACKTEST_MIN_PRIOR_RUNS {
            let start = history.len().saturating_sub(STATS_BACKTEST_PRIOR_RUNS);
            let mut priors = history[start..].to_vec();
            priors.sort_unstable();
            if let (Some(lo), Some(hi)) =
                (percentile_i64(&priors, 0.10), percentile_i64(&priors, 0.90))
            {
                predictions += 1;
                if lo <= *value && *value <= hi {
                    covered += 1;
                }
                let floor = lo.max(STATS_BACKTEST_WIDTH_FLOOR_MS);
                widths.push(hi as f64 / floor as f64);
            }
        }
        history.push(*value);
    }
    let coverage = if predictions == 0 {
        None
    } else {
        Some(covered as f64 / predictions as f64)
    };
    let mut sorted_widths = widths.clone();
    sort_f64(&mut sorted_widths);
    let median_width = percentile_f64(&sorted_widths, 0.50);
    let meets_target = if predictions == 0 {
        None
    } else {
        Some(
            coverage.unwrap_or(0.0) >= STATS_BACKTEST_TARGET_COVERAGE
                && median_width.unwrap_or(f64::INFINITY)
                    <= STATS_BACKTEST_TARGET_MAX_WIDTH,
        )
    };
    ToolRunStatsBacktestWire {
        predictions,
        covered,
        coverage,
        median_width,
        target_coverage: STATS_BACKTEST_TARGET_COVERAGE,
        target_max_width: STATS_BACKTEST_TARGET_MAX_WIDTH,
        meets_target,
    }
}

/// Per-stage backtests, one per description with at least one
/// finished in-window instance. Finished instances order by
/// (`started_ts`, `stage_id`); instances without `started_ts` sort
/// last.
fn stage_backtest_section(
    stages: &[StatsStageRow],
    prior_stages: &[StatsStageRow],
) -> Vec<ToolRunStatsStageBacktestWire> {
    let mut points: BTreeMap<&str, Vec<(i64, String, i64, bool)>> =
        BTreeMap::new();
    for stage in stages.iter().chain(prior_stages.iter()) {
        if stage.incomplete || stage.elapsed_ms.is_none() {
            continue;
        }
        points.entry(stage.description.as_str()).or_default().push((
            stage.started_ts.unwrap_or(i64::MAX),
            stage.stage_id.clone(),
            stage.elapsed_ms.unwrap_or(0),
            stage.in_window,
        ));
    }
    let mut entries = Vec::new();
    for (description, member) in &points {
        let mut priors = Vec::new();
        let mut window = Vec::new();
        for (started, stage_id, elapsed, in_window) in member {
            if *in_window {
                window.push((*started, stage_id.clone(), *elapsed));
            } else {
                priors.push((*started, stage_id.clone(), *elapsed));
            }
        }
        if window.is_empty() {
            continue;
        }
        entries.push(ToolRunStatsStageBacktestWire {
            description: (*description).to_string(),
            backtest: backtest_section(&priors, &window),
        });
    }
    entries.sort_by(|left, right| left.description.cmp(&right.description));
    entries
}

fn max_present(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(known), Some(seen)) => Some(known.max(seen)),
        (Some(known), None) => Some(known),
        (None, Some(seen)) => Some(seen),
        (None, None) => None,
    }
}

struct PressureBucket<'a> {
    runs: BTreeSet<&'a str>,
    cpu_psi: Option<f64>,
    memory_psi: Option<f64>,
    io_psi: Option<f64>,
    load_per_cpu: Option<f64>,
}

/// Report-level host pressure over samples of in-window runs.
/// Buckets are `floor(observed_ts / STATS_PRESSURE_BUCKET_SECONDS)`
/// seconds wide; a bucket is busy with at least
/// `STATS_PRESSURE_BUSY_MIN_RUNS` distinct runs. A bucket has PSI when
/// any PSI kind was recorded. OOM kills are not observable from the
/// ledger.
fn pressure_section(samples: &[StatsSampleRow]) -> ToolRunStatsPressureWire {
    let mut buckets: BTreeMap<i64, PressureBucket<'_>> = BTreeMap::new();
    for sample in samples {
        let bucket = buckets
            .entry(sample.observed_ts.div_euclid(STATS_PRESSURE_BUCKET_SECONDS))
            .or_insert(PressureBucket {
                runs: BTreeSet::new(),
                cpu_psi: None,
                memory_psi: None,
                io_psi: None,
                load_per_cpu: None,
            });
        bucket.runs.insert(sample.run_id.as_str());
        bucket.cpu_psi = max_present(bucket.cpu_psi, sample.psi_cpu_some);
        bucket.memory_psi =
            max_present(bucket.memory_psi, sample.psi_memory_some);
        bucket.io_psi = max_present(bucket.io_psi, sample.psi_io_some);
        bucket.load_per_cpu =
            max_present(bucket.load_per_cpu, sample.load_per_cpu);
    }
    let mut busy_buckets = 0;
    let mut buckets_with_psi = 0;
    let mut memory_over = 0;
    let mut busy_with_psi = 0;
    let mut busy_memory_over = 0;
    let mut cpu_values: Vec<f64> = Vec::new();
    let mut memory_values: Vec<f64> = Vec::new();
    let mut io_values: Vec<f64> = Vec::new();
    let mut load_values: Vec<f64> = Vec::new();
    for bucket in buckets.values() {
        let busy = bucket.runs.len() >= STATS_PRESSURE_BUSY_MIN_RUNS;
        if busy {
            busy_buckets += 1;
        }
        let has_psi = bucket.cpu_psi.is_some()
            || bucket.memory_psi.is_some()
            || bucket.io_psi.is_some();
        if has_psi {
            buckets_with_psi += 1;
            if busy {
                busy_with_psi += 1;
            }
        }
        let over = bucket
            .memory_psi
            .is_some_and(|psi| psi > STATS_PRESSURE_MEMORY_PSI_THRESHOLD);
        if over {
            memory_over += 1;
            if busy {
                busy_memory_over += 1;
            }
        }
        if let Some(value) = bucket.cpu_psi {
            cpu_values.push(value);
        }
        if let Some(value) = bucket.memory_psi {
            memory_values.push(value);
        }
        if let Some(value) = bucket.io_psi {
            io_values.push(value);
        }
        if let Some(value) = bucket.load_per_cpu {
            load_values.push(value);
        }
    }
    sort_f64(&mut cpu_values);
    sort_f64(&mut memory_values);
    sort_f64(&mut io_values);
    sort_f64(&mut load_values);
    ToolRunStatsPressureWire {
        buckets: buckets.len(),
        busy_buckets,
        buckets_with_psi,
        memory_over_threshold_share: if buckets_with_psi == 0 {
            None
        } else {
            Some(memory_over as f64 / buckets_with_psi as f64)
        },
        busy_memory_over_threshold_share: if busy_with_psi == 0 {
            None
        } else {
            Some(busy_memory_over as f64 / busy_with_psi as f64)
        },
        cpu_psi_p90: percentile_f64(&cpu_values, 0.90),
        memory_psi_p90: percentile_f64(&memory_values, 0.90),
        io_psi_p90: percentile_f64(&io_values, 0.90),
        load_per_cpu_p90: percentile_f64(&load_values, 0.90),
    }
}

/// Report scope for [`compute_stats_report`]: the filters echoed in
/// the result plus the window edges and truncation flags from the
/// loader.
#[derive(Debug, Clone)]
pub struct StatsReportScope {
    pub project: Option<String>,
    pub tool_name: Option<String>,
    pub days: i64,
    pub since_ts: i64,
    pub now_ts: i64,
    pub utc_offset_seconds: i32,
    pub runs_truncated: bool,
    pub stages_truncated: bool,
    pub samples_truncated: bool,
}

/// Fold the loaded window rows into the report. `priors` are lookback
/// run rows, `prior_stages` lookback stage rows: neither counts in any
/// group, but the backtests draw their priors from them. `samples`
/// are sample rows of in-window runs for the report-level pressure
/// section.
pub fn compute_stats_report(
    window: &[StatsRunRow],
    priors: &[StatsRunRow],
    stages: &[StatsStageRow],
    prior_stages: &[StatsStageRow],
    samples: &[StatsSampleRow],
    scope: &StatsReportScope,
) -> Result<ToolRunStatsResultWire, ToolRunError> {
    let empty_extra_args_digest = extra_args_digest(&[])?;
    let adhoc_runs =
        window.iter().filter(|row| row.tool_name.is_none()).count();
    let mut groups: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
    for (index, row) in window.iter().enumerate() {
        let Some(tool) = row.tool_name.as_deref() else {
            continue;
        };
        groups
            .entry((row.project.clone().unwrap_or_default(), tool.to_string()))
            .or_default()
            .push(index);
    }
    let mut prior_groups: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();
    for (index, row) in priors.iter().enumerate() {
        let Some(tool) = row.tool_name.as_deref() else {
            continue;
        };
        prior_groups
            .entry((row.project.clone().unwrap_or_default(), tool.to_string()))
            .or_default()
            .push(index);
    }
    let mut stage_groups: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();
    for (index, stage) in stages.iter().enumerate() {
        stage_groups
            .entry((stage.project.clone(), stage.tool_name.clone()))
            .or_default()
            .push(index);
    }
    let mut prior_stage_groups: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();
    for (index, stage) in prior_stages.iter().enumerate() {
        prior_stage_groups
            .entry((stage.project.clone(), stage.tool_name.clone()))
            .or_default()
            .push(index);
    }
    let mut order: Vec<((String, String), usize)> = groups
        .iter()
        .map(|(key, member)| (key.clone(), member.len()))
        .collect();
    order.sort_by(|left, right| {
        right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
    });
    let with_effective: Vec<Option<i64>> =
        window.iter().map(effective_duration_ms).collect();
    let with_prior_effective: Vec<Option<i64>> =
        priors.iter().map(effective_duration_ms).collect();
    let mut tools = Vec::new();
    for ((project_key, tool_key), _) in &order {
        let key = (project_key.clone(), tool_key.clone());
        let member: Vec<(&StatsRunRow, Option<i64>)> = groups
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|index| (&window[index], with_effective[index]))
            .collect();
        let prior_member: Vec<(&StatsRunRow, Option<i64>)> = prior_groups
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|index| (&priors[index], with_prior_effective[index]))
            .collect();
        let member_stages: Vec<StatsStageRow> = stage_groups
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|index| stages[index].clone())
            .collect();
        let member_prior_stages: Vec<StatsStageRow> = prior_stage_groups
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|index| prior_stages[index].clone())
            .collect();
        tools.push(tool_entry(
            project_key,
            tool_key,
            &member,
            &prior_member,
            &member_stages,
            &member_prior_stages,
            &empty_extra_args_digest,
            scope.since_ts,
            scope.now_ts,
            scope.utc_offset_seconds,
        ));
    }
    let pressure = pressure_section(samples);
    let mut diagnostics = Vec::new();
    if scope.runs_truncated {
        diagnostics.push(format!(
            "stats truncated to {STATS_MAX_RUNS} newest runs; narrowing days or tool keeps the whole window"
        ));
    }
    if scope.stages_truncated {
        diagnostics.push(format!(
            "stats truncated to {STATS_MAX_STAGES} newest stages; narrowing days or tool keeps the whole window"
        ));
    }
    if scope.samples_truncated {
        diagnostics.push(format!(
            "stats truncated to {STATS_MAX_SAMPLES} newest samples; narrowing days or tool keeps the whole window"
        ));
    }
    Ok(ToolRunStatsResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: scope.project.clone(),
        tool_name: scope.tool_name.clone(),
        window: ToolRunStatsWindowWire {
            days: scope.days,
            since_ts: scope.since_ts,
            now_ts: scope.now_ts,
            utc_offset_seconds: scope.utc_offset_seconds,
        },
        runs_scanned: window.len(),
        runs_truncated: scope.runs_truncated,
        stages_truncated: scope.stages_truncated,
        samples_truncated: scope.samples_truncated,
        adhoc_runs,
        tools,
        pressure,
        thresholds: stats_thresholds(),
        diagnostics,
    })
}
