//! Update-skew agent auto-restart: failure classifier, at-most-once
//! ledger, and episode identity.
//!
//! Shared verdicts that every frontend must render identically are core
//! logic. The host owns subprocess execution, quiescence checks, and the
//! fresh-interpreter probe; this module only classifies observed facts,
//! context, and witnesses into a stable recovery verdict, advances the
//! durable ledger, and derives the episode id.

pub mod catalog;
pub mod classify;
pub mod episode;
pub mod ledger;
pub mod wire;

#[cfg(test)]
mod tests;

pub use catalog::{
    TIER_DATA_FORMAT, TIER_REAL_BUG, TIER_RUST_BINDING, TIER_TORN_PYTHON,
};
pub use classify::{
    classify_agent_failure, classify_phase, fired_witnesses, MODE_ASK,
    MODE_DECLINE, MODE_DEFER, MODE_NOTIFY_POST_PROVIDER, MODE_RELAUNCH,
    PHASE_PLAN_HANDOFF, PHASE_POST_PROVIDER, PHASE_PRE_PROVIDER, PHASE_UNKNOWN,
};
pub use episode::derive_auto_restart_episode;
pub use ledger::{
    advance_auto_restart_ledger, auto_restart_lineage_root,
    auto_restart_recovery_is_in_flight, claim_auto_restart_ledger,
    AutoRestartLedgerError, LEDGER_CLAIMED, LEDGER_DECLINED, LEDGER_DEFERRED,
    LEDGER_EVENT_BEGIN_LAUNCH, LEDGER_EVENT_DECLINE, LEDGER_EVENT_DEFER,
    LEDGER_EVENT_LAUNCHED, LEDGER_EVENT_RECLAIM, LEDGER_EVENT_SETTLED_FAILED,
    LEDGER_EVENT_SETTLED_OK, LEDGER_LAUNCHED, LEDGER_LAUNCHING,
    LEDGER_SETTLED_FAILED, LEDGER_SETTLED_OK, RECOVERY_IN_FLIGHT_STATES,
};
pub use wire::{
    AgentFailureAttributeErrorWire, AgentFailureChainLinkWire,
    AgentFailureFactsWire, AgentFailureFrameWire, AgentFailureImportErrorWire,
    AgentRecoveryWire, AutoRestartContextWire, AutoRestartEpisodeWire,
    AutoRestartFileProofWire, AutoRestartLedgerHistoryWire,
    AutoRestartLedgerRecordWire, AutoRestartManagedRootWire,
    AutoRestartProbeWire, AutoRestartRefreshLogLineWire,
    AutoRestartWitnessesWire, RecoveryVerdictWire,
    AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
};

/// Return the auto-restart wire schema version.
pub fn agent_auto_restart_wire_schema_version() -> u32 {
    AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION
}
