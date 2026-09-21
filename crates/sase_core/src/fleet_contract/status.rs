use serde::{Deserialize, Serialize};

/// Row category for logical-agent counting.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetRowKindWire {
    AgentShell,
    ContainerHeader,
    Monitor,
    Gate,
    Proc,
    HistoricalShell,
}

pub(crate) fn default_row_kind() -> FleetRowKindWire {
    FleetRowKindWire::AgentShell
}

/// Normalized family role for viewer folding.
///
/// Independent of `row_kind`: it distinguishes a family root from an
/// ordinary member for `AgentShell` rows, carries `Monitor`/`Gate`/`Proc`
/// straight through from their matching row kinds, and marks any row whose
/// presentation is terminal (genuinely completed, or a demoted dead-active
/// leftover) as `HistoricalShell` so a viewer can render "was running"
/// uniformly once family topology stops mattering.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetFamilyRoleWire {
    Root,
    Member,
    Monitor,
    Gate,
    Proc,
    HistoricalShell,
}

/// Lifecycle status observed in the artifact record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetLifecycleWire {
    Starting,
    Running,
    Waiting,
    Asking,
    Terminal,
    Failed,
    Unknown,
}

/// Owner-resolved process liveness, separate from lifecycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OwnerLivenessWire {
    Alive,
    Dead,
    NotProcess,
    Unknown,
}

/// Current connection health between viewer/controller and owner.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionHealthWire {
    Online,
    Degraded,
    Offline,
    Unknown,
}

/// Viewer observation freshness. This is not owner process liveness.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ObservationFreshnessWire {
    Fresh,
    Aging,
    Stale,
    Unknown,
}

/// Display bucket compatible with the current Python status-bucket semantics.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetStatusBucketWire {
    Stopped,
    Failed,
    Starting,
    Running,
    Queued,
    Waiting,
    Done,
}
