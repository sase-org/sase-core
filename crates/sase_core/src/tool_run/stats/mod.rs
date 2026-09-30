//! Read-only ToolRun stats report: wires plus pure computation.

mod report;
mod wire;

#[cfg(test)]
mod tests;

pub use report::{
    compute_stats_report, StatsReportScope, StatsRunRow, StatsSampleRow,
    StatsStageRow,
};
pub use wire::{
    stats_thresholds, ToolRunStatsBacktestWire, ToolRunStatsDemandWire,
    ToolRunStatsDurationSummaryWire, ToolRunStatsDurationWire,
    ToolRunStatsMonitorOwnedWire, ToolRunStatsOutcomesWire,
    ToolRunStatsPressureWire, ToolRunStatsProviderWire,
    ToolRunStatsRepeatsWire, ToolRunStatsRequestWire, ToolRunStatsResultWire,
    ToolRunStatsRouteWire, ToolRunStatsStageBacktestWire,
    ToolRunStatsStageWire, ToolRunStatsThresholdsWire, ToolRunStatsToolWire,
    ToolRunStatsTrendBucketWire, ToolRunStatsWasteCategoryWire,
    ToolRunStatsWasteWire, ToolRunStatsWindowWire,
    STATS_BACKTEST_LOOKBACK_DAYS, STATS_BACKTEST_MIN_PRIOR_RUNS,
    STATS_BACKTEST_PRIOR_RUNS, STATS_BACKTEST_TARGET_COVERAGE,
    STATS_BACKTEST_TARGET_MAX_WIDTH, STATS_BACKTEST_WIDTH_FLOOR_MS,
    STATS_CEILING_KILL_GRACE_SECONDS, STATS_CEILING_KILL_MIN_PERCENT,
    STATS_DEFAULT_DAYS, STATS_LEGACY_CEILING_KILL_MAX_MS,
    STATS_LEGACY_CEILING_KILL_MIN_MS, STATS_MAX_DAYS, STATS_MAX_RUNS,
    STATS_MAX_SAMPLES, STATS_MAX_STAGES, STATS_MEDIUM_MONITOR_RUN_MS,
    STATS_PRESSURE_BUCKET_SECONDS, STATS_PRESSURE_BUSY_MIN_RUNS,
    STATS_PRESSURE_MEMORY_PSI_THRESHOLD, STATS_RERUN_WINDOW_SECONDS,
    STATS_SHORT_MONITOR_RUN_MS,
};
