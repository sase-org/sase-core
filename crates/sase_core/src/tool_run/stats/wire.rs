//! Read-only ToolRun stats report wires and thresholds.
//!
//! Rust owns every stats definition, constant, and computation below.
//! Python renders the result and never re-derives a number.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

fn stats_schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn default_stats_days() -> i64 {
    STATS_DEFAULT_DAYS
}

/// Default summary horizon in days.
pub const STATS_DEFAULT_DAYS: i64 = 7;
/// Maximum summary horizon in days.
pub const STATS_MAX_DAYS: i64 = 180;
/// Maximum runs rows loaded for one report, newest first.
pub const STATS_MAX_RUNS: usize = 50_000;
/// Maximum stages rows loaded for one report (detail phase).
pub const STATS_MAX_STAGES: usize = 200_000;
/// Maximum samples rows loaded for one report (detail phase).
pub const STATS_MAX_SAMPLES: usize = 400_000;
/// Backtest lookback horizon in days (detail phase).
pub const STATS_BACKTEST_LOOKBACK_DAYS: i64 = 30;
/// Backtest priors per prediction (detail phase).
pub const STATS_BACKTEST_PRIOR_RUNS: usize = 60;
/// Minimum priors before a backtest prediction (detail phase).
pub const STATS_BACKTEST_MIN_PRIOR_RUNS: usize = 20;
/// Backtest width floor in milliseconds (detail phase).
pub const STATS_BACKTEST_WIDTH_FLOOR_MS: i64 = 1_000;
/// Backtest coverage target (detail phase).
pub const STATS_BACKTEST_TARGET_COVERAGE: f64 = 0.80;
/// Backtest median-width target (detail phase).
pub const STATS_BACKTEST_TARGET_MAX_WIDTH: f64 = 3.0;
/// Recorded-ceiling kill window lower edge, in percent of the ceiling.
pub const STATS_CEILING_KILL_MIN_PERCENT: i64 = 85;
/// Recorded-ceiling kill window grace past the ceiling, in seconds.
pub const STATS_CEILING_KILL_GRACE_SECONDS: i64 = 60;
/// Legacy (context-free) kill window lower edge, in milliseconds.
pub const STATS_LEGACY_CEILING_KILL_MIN_MS: i64 = 530_000;
/// Legacy (context-free) kill window upper edge, in milliseconds.
pub const STATS_LEGACY_CEILING_KILL_MAX_MS: i64 = 550_000;
/// Kill-to-rerun search window, in seconds past the kill end.
pub const STATS_RERUN_WINDOW_SECONDS: i64 = 1_800;
/// Short monitor-owned run edge, in milliseconds.
pub const STATS_SHORT_MONITOR_RUN_MS: i64 = 120_000;
/// Medium monitor-owned run edge, in milliseconds.
pub const STATS_MEDIUM_MONITOR_RUN_MS: i64 = 300_000;
/// Host-pressure bucket width, in seconds (detail phase).
pub const STATS_PRESSURE_BUCKET_SECONDS: i64 = 30;
/// Host-pressure memory PSI threshold (detail phase).
pub const STATS_PRESSURE_MEMORY_PSI_THRESHOLD: f64 = 10.0;
/// Minimum distinct runs in a busy pressure bucket (detail phase).
pub const STATS_PRESSURE_BUSY_MIN_RUNS: usize = 2;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunStatsRequestWire {
    #[serde(default = "stats_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default = "default_stats_days")]
    pub days: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
    #[serde(default)]
    pub utc_offset_seconds: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsWindowWire {
    pub days: i64,
    pub since_ts: i64,
    pub now_ts: i64,
    #[serde(default)]
    pub utc_offset_seconds: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsThresholdsWire {
    #[serde(default)]
    pub default_days: i64,
    #[serde(default)]
    pub max_days: i64,
    #[serde(default)]
    pub max_runs: usize,
    #[serde(default)]
    pub max_stages: usize,
    #[serde(default)]
    pub max_samples: usize,
    #[serde(default)]
    pub backtest_lookback_days: i64,
    #[serde(default)]
    pub backtest_prior_runs: usize,
    #[serde(default)]
    pub backtest_min_prior_runs: usize,
    #[serde(default)]
    pub backtest_width_floor_ms: i64,
    #[serde(default)]
    pub backtest_target_coverage: f64,
    #[serde(default)]
    pub backtest_target_max_width: f64,
    #[serde(default)]
    pub ceiling_kill_min_percent: i64,
    #[serde(default)]
    pub ceiling_kill_grace_seconds: i64,
    #[serde(default)]
    pub legacy_ceiling_kill_min_ms: i64,
    #[serde(default)]
    pub legacy_ceiling_kill_max_ms: i64,
    #[serde(default)]
    pub rerun_window_seconds: i64,
    #[serde(default)]
    pub short_monitor_run_ms: i64,
    #[serde(default)]
    pub medium_monitor_run_ms: i64,
    #[serde(default)]
    pub pressure_bucket_seconds: i64,
    #[serde(default)]
    pub pressure_memory_psi_threshold: f64,
    #[serde(default)]
    pub pressure_busy_min_runs: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStatsOutcomesWire {
    #[serde(default)]
    pub succeeded: usize,
    #[serde(default)]
    pub failed: usize,
    #[serde(default)]
    pub signaled: usize,
    #[serde(default)]
    pub interrupted: usize,
    #[serde(default)]
    pub lost: usize,
    #[serde(default)]
    pub unsettled: usize,
    #[serde(default)]
    pub censored: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStatsDurationSummaryWire {
    #[serde(default)]
    pub count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p10_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStatsDurationWire {
    #[serde(default)]
    pub count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p10_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<i64>,
    #[serde(default)]
    pub succeeded: ToolRunStatsDurationSummaryWire,
    #[serde(default)]
    pub failed: ToolRunStatsDurationSummaryWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsWasteCategoryWire {
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub hours: f64,
    #[serde(default)]
    pub runs_without_duration: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsWasteWire {
    #[serde(default)]
    pub total_hours: f64,
    #[serde(default)]
    pub killed_at_ceiling: ToolRunStatsWasteCategoryWire,
    #[serde(default)]
    pub timeout: ToolRunStatsWasteCategoryWire,
    #[serde(default)]
    pub stopped: ToolRunStatsWasteCategoryWire,
    #[serde(default)]
    pub lost: ToolRunStatsWasteCategoryWire,
    #[serde(default)]
    pub interrupted: ToolRunStatsWasteCategoryWire,
    #[serde(default)]
    pub other_signal: ToolRunStatsWasteCategoryWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStatsRouteWire {
    #[serde(default)]
    pub route: String,
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub settled: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_ms: Option<i64>,
    #[serde(default)]
    pub under_2m: usize,
    #[serde(default)]
    pub under_5m: usize,
    #[serde(default)]
    pub kills: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsMonitorOwnedWire {
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub under_2m: usize,
    #[serde(default)]
    pub under_5m: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub under_2m_share: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsProviderWire {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub outcomes: ToolRunStatsOutcomesWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_ms: Option<i64>,
    #[serde(default)]
    pub kills: usize,
    #[serde(default)]
    pub kill_hours: f64,
    #[serde(default)]
    pub routes: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStatsTrendBucketWire {
    #[serde(default)]
    pub day_start_ts: i64,
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub succeeded: usize,
    #[serde(default)]
    pub failed: usize,
    #[serde(default)]
    pub censored: usize,
    #[serde(default)]
    pub killed_at_ceiling: usize,
    #[serde(default)]
    pub monitor_owned: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsRepeatsWire {
    #[serde(default)]
    pub repeat_runs: usize,
    #[serde(default)]
    pub repeat_hours: f64,
    #[serde(default)]
    pub after_censored_runs: usize,
    #[serde(default)]
    pub after_censored_hours: f64,
    #[serde(default)]
    pub duplicate_runs: usize,
    #[serde(default)]
    pub duplicate_hours: f64,
    #[serde(default)]
    pub unkeyed_runs: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsDemandWire {
    #[serde(default)]
    pub runs_with_context: usize,
    #[serde(default)]
    pub runs_with_usage: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_seconds_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_seconds_p90: Option<f64>,
    #[serde(default)]
    pub cpu_total_hours: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_cores_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_cores_p90: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_process_rss_kib_p50: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_process_rss_kib_p90: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_process_rss_kib_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_tree_rss_kib_p50: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_tree_rss_kib_p90: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_tree_rss_kib_max: Option<u64>,
    #[serde(default)]
    pub runs_with_grants: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_width_p50: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_width_p90: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_width_max: Option<u32>,
    #[serde(default)]
    pub grant_paths: BTreeMap<String, usize>,
    #[serde(default)]
    pub token_wait_runs: usize,
    #[serde(default)]
    pub token_wait_ms_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_wait_ms_max: Option<u64>,
    #[serde(default)]
    pub token_wait_timeouts: usize,
    #[serde(default)]
    pub escalated_grant_runs: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsToolWire {
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub runs: usize,
    #[serde(default)]
    pub definition_digests: usize,
    #[serde(default)]
    pub extra_args_runs: usize,
    #[serde(default)]
    pub outcomes: ToolRunStatsOutcomesWire,
    #[serde(default)]
    pub terminal_causes: BTreeMap<String, usize>,
    #[serde(default)]
    pub duration: ToolRunStatsDurationWire,
    #[serde(default)]
    pub waste: ToolRunStatsWasteWire,
    #[serde(default)]
    pub reruns_after_kill: usize,
    #[serde(default)]
    pub routes: Vec<ToolRunStatsRouteWire>,
    #[serde(default)]
    pub monitor_owned: ToolRunStatsMonitorOwnedWire,
    #[serde(default)]
    pub providers: Vec<ToolRunStatsProviderWire>,
    #[serde(default)]
    pub trend: Vec<ToolRunStatsTrendBucketWire>,
    #[serde(default)]
    pub repeats: ToolRunStatsRepeatsWire,
    #[serde(default)]
    pub demand: ToolRunStatsDemandWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStatsResultWire {
    #[serde(default = "stats_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub window: ToolRunStatsWindowWire,
    #[serde(default)]
    pub runs_scanned: usize,
    #[serde(default)]
    pub runs_truncated: bool,
    #[serde(default)]
    pub adhoc_runs: usize,
    #[serde(default)]
    pub tools: Vec<ToolRunStatsToolWire>,
    pub thresholds: ToolRunStatsThresholdsWire,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

pub fn stats_thresholds() -> ToolRunStatsThresholdsWire {
    ToolRunStatsThresholdsWire {
        default_days: STATS_DEFAULT_DAYS,
        max_days: STATS_MAX_DAYS,
        max_runs: STATS_MAX_RUNS,
        max_stages: STATS_MAX_STAGES,
        max_samples: STATS_MAX_SAMPLES,
        backtest_lookback_days: STATS_BACKTEST_LOOKBACK_DAYS,
        backtest_prior_runs: STATS_BACKTEST_PRIOR_RUNS,
        backtest_min_prior_runs: STATS_BACKTEST_MIN_PRIOR_RUNS,
        backtest_width_floor_ms: STATS_BACKTEST_WIDTH_FLOOR_MS,
        backtest_target_coverage: STATS_BACKTEST_TARGET_COVERAGE,
        backtest_target_max_width: STATS_BACKTEST_TARGET_MAX_WIDTH,
        ceiling_kill_min_percent: STATS_CEILING_KILL_MIN_PERCENT,
        ceiling_kill_grace_seconds: STATS_CEILING_KILL_GRACE_SECONDS,
        legacy_ceiling_kill_min_ms: STATS_LEGACY_CEILING_KILL_MIN_MS,
        legacy_ceiling_kill_max_ms: STATS_LEGACY_CEILING_KILL_MAX_MS,
        rerun_window_seconds: STATS_RERUN_WINDOW_SECONDS,
        short_monitor_run_ms: STATS_SHORT_MONITOR_RUN_MS,
        medium_monitor_run_ms: STATS_MEDIUM_MONITOR_RUN_MS,
        pressure_bucket_seconds: STATS_PRESSURE_BUCKET_SECONDS,
        pressure_memory_psi_threshold: STATS_PRESSURE_MEMORY_PSI_THRESHOLD,
        pressure_busy_min_runs: STATS_PRESSURE_BUSY_MIN_RUNS,
    }
}
