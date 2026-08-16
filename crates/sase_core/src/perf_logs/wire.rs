use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const PERF_LOGS_WIRE_SCHEMA_VERSION: u32 = 1;

pub(crate) fn default_max_records_per_source() -> u64 {
    20_000
}

pub(crate) fn default_max_bytes_per_source() -> u64 {
    8 * 1024 * 1024
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PerfLogSourceIdWire {
    Startup,
    Stalls,
    AgentLoads,
    LaunchTiming,
    GitOps,
    ExternalTools,
}

impl PerfLogSourceIdWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Stalls => "stalls",
            Self::AgentLoads => "agent_loads",
            Self::LaunchTiming => "launch_timing",
            Self::GitOps => "git_ops",
            Self::ExternalTools => "external_tools",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfLogSourceWire {
    pub id: PerfLogSourceIdWire,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfLogsQueryWire {
    pub start_ts: i64,
    pub end_ts: i64,
    #[serde(default = "default_max_records_per_source")]
    pub max_records_per_source: u64,
    #[serde(default = "default_max_bytes_per_source")]
    pub max_bytes_per_source: u64,
    #[serde(default)]
    pub sources: Vec<PerfLogSourceWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfLogsSnapshotWire {
    pub schema_version: u32,
    pub start_ts: i64,
    pub end_ts: i64,
    pub startup: PerfStartupSnapshotWire,
    pub stalls: PerfStallsSnapshotWire,
    pub launches: PerfLaunchesSnapshotWire,
    pub agent_loads: PerfAgentLoadsSnapshotWire,
    pub git_ops: PerfGitOpsSnapshotWire,
    pub external_tool_waits: PerfExternalToolWaitsSnapshotWire,
    pub coverage: Vec<PerfLogCoverageWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfLogCoverageWire {
    pub source: PerfLogSourceIdWire,
    pub path: String,
    pub present: bool,
    pub records_scanned: u64,
    pub records_in_window: u64,
    pub earliest_ts: Option<f64>,
    pub latest_ts: Option<f64>,
    pub truncated: bool,
    pub malformed_skipped: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfNumericSummaryWire {
    pub samples: u64,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfStageSummaryWire {
    pub stage: String,
    pub summary: PerfNumericSummaryWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfStartupSnapshotWire {
    pub sessions: u64,
    pub stages: Vec<PerfStageSummaryWire>,
    pub visible_ready_series: Vec<PerfStartupSeriesPointWire>,
    pub slowest_session: Option<PerfStartupSessionWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfStartupSeriesPointWire {
    pub ts: f64,
    pub visible_ready_seconds: f64,
    pub initial_tab: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfStartupSessionWire {
    pub ts: f64,
    pub visible_ready_seconds: f64,
    pub all_surfaces_ready_seconds: Option<f64>,
    pub initial_tab: Option<String>,
    pub source: Option<String>,
    pub tier: Option<String>,
    pub agent_row_count: Option<u64>,
    pub index_row_count: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfStallsSnapshotWire {
    pub events: Vec<PerfStallEventStatsWire>,
    pub top_contexts: Vec<PerfCountWire>,
    pub recovery_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfStallEventStatsWire {
    pub event: String,
    pub count: u64,
    pub worst_seconds: Option<f64>,
    pub median_seconds: Option<f64>,
    pub last_seen_ts: Option<f64>,
    pub suppressed_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfCountWire {
    pub name: String,
    pub count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfLaunchesSnapshotWire {
    pub count: u64,
    pub total_ms: PerfNumericSummaryWire,
    pub slow_stage_count: u64,
    pub worst_stages: Vec<PerfLaunchStageWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfLaunchStageWire {
    pub ts: f64,
    pub operation: Option<String>,
    pub stage: String,
    pub elapsed_ms: f64,
    pub slow_stage: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfAgentLoadsSnapshotWire {
    pub count: u64,
    pub slow_stage_count: u64,
    pub worst_stage_seconds: PerfNumericSummaryWire,
    pub worst_stages: Vec<PerfAgentLoadStageWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfAgentLoadStageWire {
    pub ts: f64,
    pub source: Option<String>,
    pub load_kind: Option<String>,
    pub stage: String,
    pub elapsed_seconds: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfGitOpsSnapshotWire {
    pub count: u64,
    pub duration_ms: PerfNumericSummaryWire,
    pub timeout_count: u64,
    pub suppressed_count: u64,
    pub operations: Vec<PerfCountWire>,
    pub statuses: Vec<PerfCountWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerfExternalToolWaitsSnapshotWire {
    pub count: u64,
    pub elapsed_seconds: PerfNumericSummaryWire,
    pub tools: Vec<PerfCountWire>,
}
