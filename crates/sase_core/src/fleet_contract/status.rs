use serde::{Deserialize, Serialize};

/// Row category for logical-agent counting.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetRowKindWire {
    // legacy sase-shell spelling; flips in contract-flip
    #[serde(rename = "agent_shell", alias = "agent_turn")]
    AgentTurn,
    ContainerHeader,
    Monitor,
    Gate,
    Proc,
    // legacy sase-shell spelling; flips in contract-flip
    #[serde(rename = "historical_shell", alias = "historical_turn")]
    HistoricalTurn,
}

pub(crate) fn default_row_kind() -> FleetRowKindWire {
    FleetRowKindWire::AgentTurn
}

/// Normalized agent session role for viewer folding.
///
/// Independent of `row_kind`: it distinguishes an agent session root from an
/// ordinary member for `AgentTurn` rows, carries `Monitor`/`Gate`/`Proc`
/// straight through from their matching row kinds, and marks any row whose
/// presentation is terminal (genuinely completed, or a demoted dead-active
/// leftover) as `HistoricalTurn` so a viewer can render "was running"
/// uniformly once agent session topology stops mattering.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetAgentSessionRoleWire {
    Root,
    Member,
    Monitor,
    Gate,
    Proc,
    // legacy sase-shell spelling; flips in contract-flip
    #[serde(rename = "historical_shell", alias = "historical_turn")]
    HistoricalTurn,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_kinds_accept_new_spelling_but_emit_legacy() {
        let legacy: FleetRowKindWire =
            serde_json::from_value(serde_json::json!("agent_shell")).unwrap();
        let new: FleetRowKindWire =
            serde_json::from_value(serde_json::json!("agent_turn")).unwrap();
        assert_eq!(legacy, new);
        assert_eq!(legacy, FleetRowKindWire::AgentTurn);
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            serde_json::json!("agent_shell")
        );

        let legacy: FleetRowKindWire =
            serde_json::from_value(serde_json::json!("historical_shell"))
                .unwrap();
        let new: FleetRowKindWire =
            serde_json::from_value(serde_json::json!("historical_turn"))
                .unwrap();
        assert_eq!(legacy, new);
        assert_eq!(legacy, FleetRowKindWire::HistoricalTurn);
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            serde_json::json!("historical_shell")
        );
    }

    #[test]
    fn session_roles_accept_new_spelling_but_emit_legacy() {
        let legacy: FleetAgentSessionRoleWire =
            serde_json::from_value(serde_json::json!("historical_shell"))
                .unwrap();
        let new: FleetAgentSessionRoleWire =
            serde_json::from_value(serde_json::json!("historical_turn"))
                .unwrap();
        assert_eq!(legacy, new);
        assert_eq!(legacy, FleetAgentSessionRoleWire::HistoricalTurn);
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            serde_json::json!("historical_shell")
        );
    }
}
