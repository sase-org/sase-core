//! Serde-friendly wire records for portable AXE runtime status.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version for AXE status requests and snapshots.
pub const AXE_STATUS_SCHEMA_VERSION: u32 = 1;

/// A path-specific structural request failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(deny_unknown_fields)]
#[error("{code} at {path}: {message}")]
pub struct AxeStatusError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl AxeStatusError {
    pub(crate) fn new(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// The operator's last requested lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeDesiredStateValueWire {
    Running,
    Stopped,
}

/// One validated desired-state marker supplied by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeDesiredStateWire {
    pub state: AxeDesiredStateValueWire,
    pub source: String,
    pub timestamp: String,
}

/// One PID source and the host's liveness observation for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeProcessObservationWire {
    pub pid: Option<u32>,
    pub live: Option<bool>,
}

/// Raw lifecycle-lock and PID evidence collected by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeOrchestratorObservationWire {
    pub lifecycle_lock_held: bool,
    pub lock_holder: AxeProcessObservationWire,
    pub orchestrator_pid_file: AxeProcessObservationWire,
    pub legacy_pid_file: AxeProcessObservationWire,
}

/// One structurally valid active maintenance marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeMaintenanceWire {
    pub reason: String,
    pub owner_pid: u32,
    pub started_at: String,
    pub age_seconds: u64,
}

/// Current and configured maximum runner occupancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeRunnerOccupancyWire {
    pub current: u32,
    pub maximum: u32,
}

/// State reported by a lumberjack status document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeLumberjackReportedStateWire {
    Running,
    Stopped,
    Error,
}

/// Raw observations for one configured or state-directory lumberjack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeLumberjackObservationWire {
    pub name: String,
    pub configured: bool,
    pub interval_seconds: Option<u64>,
    pub configured_chops: Vec<String>,
    pub recorded_pid: Option<u32>,
    pub reported_state: Option<AxeLumberjackReportedStateWire>,
    pub process_live: Option<bool>,
    pub started_at: Option<String>,
    pub start_age_seconds: Option<u64>,
    pub heartbeat_at: Option<String>,
    pub heartbeat_age_seconds: Option<u64>,
    pub cycles_run: u64,
    pub errors_encountered: u64,
    pub uptime_seconds: u64,
}

/// Lifecycle operations recorded by the host journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeLifecycleEventKindWire {
    Start,
    Stop,
    Restart,
}

/// Most recent valid lifecycle-journal event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeLifecycleEventWire {
    pub event: AxeLifecycleEventKindWire,
    pub timestamp: String,
    pub source: String,
    pub outcome: String,
    pub success: bool,
    pub reason: Option<String>,
    pub orchestrator_pid: Option<u32>,
    pub age_seconds: Option<u64>,
}

/// A required host-collection failure represented inside a normal snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeStatusCollectionErrorWire {
    pub code: String,
    pub message: String,
}

/// Complete host-collected input to the pure classifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeStatusRequestWire {
    pub schema_version: u32,
    pub generated_at: String,
    pub desired_state: Option<AxeDesiredStateWire>,
    pub orchestrator: AxeOrchestratorObservationWire,
    pub maintenance: Option<AxeMaintenanceWire>,
    pub hook_runners: AxeRunnerOccupancyWire,
    pub agent_runners: AxeRunnerOccupancyWire,
    pub lumberjacks: Vec<AxeLumberjackObservationWire>,
    pub latest_lifecycle_event: Option<AxeLifecycleEventWire>,
    pub collection_error: Option<AxeStatusCollectionErrorWire>,
}

/// Derived orchestrator lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeOrchestratorStateWire {
    Running,
    Stopped,
    Incoherent,
}

/// Whether the lifecycle lock and live PID identities agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeOrchestratorCoherenceWire {
    Coherent,
    Incoherent,
}

/// Raw and derived orchestrator evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeOrchestratorStatusWire {
    pub state: AxeOrchestratorStateWire,
    pub coherence: AxeOrchestratorCoherenceWire,
    pub live_pids: Vec<u32>,
    pub lifecycle_lock_held: bool,
    pub lock_holder: AxeProcessObservationWire,
    pub orchestrator_pid_file: AxeProcessObservationWire,
    pub legacy_pid_file: AxeProcessObservationWire,
}

/// Derived current state for one lumberjack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeLumberjackStateWire {
    Running,
    NotReporting,
    StaleProcess,
    StaleHeartbeat,
    Error,
    Orphaned,
}

/// Raw and derived lumberjack status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeLumberjackStatusWire {
    pub name: String,
    pub state: AxeLumberjackStateWire,
    pub stale_threshold_seconds: Option<u64>,
    pub configured: bool,
    pub interval_seconds: Option<u64>,
    pub configured_chops: Vec<String>,
    pub recorded_pid: Option<u32>,
    pub reported_state: Option<AxeLumberjackReportedStateWire>,
    pub process_live: Option<bool>,
    pub started_at: Option<String>,
    pub start_age_seconds: Option<u64>,
    pub heartbeat_at: Option<String>,
    pub heartbeat_age_seconds: Option<u64>,
    pub cycles_run: u64,
    pub errors_encountered: u64,
    pub uptime_seconds: u64,
}

/// Whole-system AXE lifecycle classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeStatusStateWire {
    Running,
    Maintenance,
    Stopped,
    NotStarted,
    Down,
    Degraded,
    Error,
}

/// Whole-system health classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeStatusHealthWire {
    Healthy,
    Unhealthy,
    Error,
}

/// Severity of one actionable status issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxeStatusIssueSeverityWire {
    Warning,
    Error,
}

/// One stable, actionable status issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeStatusIssueWire {
    pub code: String,
    pub severity: AxeStatusIssueSeverityWire,
    pub subject: Option<String>,
    pub summary: String,
    pub suggested_command: Option<String>,
}

/// Stable version-1 status snapshot consumed by every frontend.
///
/// Field declaration order is the public serialization order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeStatusSnapshotWire {
    pub schema_version: u32,
    pub generated_at: String,
    pub state: AxeStatusStateWire,
    pub health: AxeStatusHealthWire,
    pub summary: String,
    pub exit_code: u8,
    pub desired_state: Option<AxeDesiredStateWire>,
    pub orchestrator: AxeOrchestratorStatusWire,
    pub maintenance: Option<AxeMaintenanceWire>,
    pub hook_runners: AxeRunnerOccupancyWire,
    pub agent_runners: AxeRunnerOccupancyWire,
    pub lumberjacks: Vec<AxeLumberjackStatusWire>,
    pub latest_lifecycle_event: Option<AxeLifecycleEventWire>,
    pub issues: Vec<AxeStatusIssueWire>,
    pub collection_error: Option<AxeStatusCollectionErrorWire>,
}
