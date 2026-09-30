//! Per-run demand wires: provider/ceiling context, resource usage, and
//! pytest worker grants.
//!
//! The nested wires are lenient (no `deny_unknown_fields`) because they are
//! also the stored shape older readers must keep parsing. Only the top-level
//! request wire is strict.

use serde::{Deserialize, Serialize};

use super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

fn demand_schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

pub const DEMAND_MAX_WORKER_GRANTS: usize = 64;
pub const DEMAND_MAX_DIAGNOSTICS: usize = 16;

/// Provider and ceiling facts captured by the agent-side starter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunDemandContextWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_ceiling_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_soft_ceiling_seconds: Option<u64>,
}

/// CPU and memory facts captured by the executing wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunResourceUsageWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_user_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_system_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_process_rss_kib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_tree_rss_kib: Option<u64>,
    #[serde(default)]
    pub tree_rss_samples: u32,
    /// Typed "unavailable" reasons, never zeros.
    #[serde(default)]
    pub availability: Vec<String>,
}

/// One pytest worker-grant decision reported by the child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunWorkerGrantWire {
    pub grant_id: String,
    pub source: String,
    pub observed_ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<String>,
    pub path: String,
    pub requested_floor: u32,
    pub requested_ceiling: u32,
    pub granted: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<u32>,
    pub wait_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_files: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalated_from: Option<String>,
}

/// The stored per-run demand record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunDemandWire {
    #[serde(default = "demand_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ToolRunDemandContextWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ToolRunResourceUsageWire>,
    #[serde(default)]
    pub worker_grants: Vec<ToolRunWorkerGrantWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

/// Merge-on-write demand request. Allowed in any run state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunRecordDemandRequestWire {
    #[serde(default = "demand_schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ToolRunDemandContextWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ToolRunResourceUsageWire>,
    #[serde(default)]
    pub worker_grants: Vec<ToolRunWorkerGrantWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunRecordDemandResultWire {
    #[serde(default = "demand_schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub demand: ToolRunDemandWire,
    pub replayed: bool,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}
