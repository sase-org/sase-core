//! At-most-once ledger state machine.
//!
//! ```text
//! claimed  → deferred | declined | launching
//! deferred → claimed (a later healer pass re-attempts) | declined
//! launching → launched | settled_failed
//! launched → settled_ok | settled_failed
//! ```
//!
//! The claim is recorded before any mutation, and any uncertainty
//! resolves to "attempt spent, notify".

use thiserror::Error;

use super::wire::{
    AutoRestartLedgerRecordWire, AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
};

/// Ledger states.
pub const LEDGER_CLAIMED: &str = "claimed";
pub const LEDGER_DEFERRED: &str = "deferred";
pub const LEDGER_DECLINED: &str = "declined";
pub const LEDGER_LAUNCHING: &str = "launching";
pub const LEDGER_LAUNCHED: &str = "launched";
pub const LEDGER_SETTLED_OK: &str = "settled_ok";
pub const LEDGER_SETTLED_FAILED: &str = "settled_failed";

/// Ledger events accepted by [`advance_auto_restart_ledger`].
pub const LEDGER_EVENT_DEFER: &str = "defer";
pub const LEDGER_EVENT_RECLAIM: &str = "reclaim";
pub const LEDGER_EVENT_DECLINE: &str = "decline";
pub const LEDGER_EVENT_BEGIN_LAUNCH: &str = "begin_launch";
pub const LEDGER_EVENT_LAUNCHED: &str = "launched";
pub const LEDGER_EVENT_SETTLED_OK: &str = "settled_ok";
pub const LEDGER_EVENT_SETTLED_FAILED: &str = "settled_failed";

/// Recovery states that map to the active "restarting" bucket instead
/// of failed: the done-marker `recovery.state` values written while a
/// recovery is still in flight.
pub const RECOVERY_IN_FLIGHT_STATES: &[&str] =
    &["pending", "deferred", "launching"];

/// Return whether a done-marker recovery state renders as restarting.
pub fn auto_restart_recovery_is_in_flight(state: Option<&str>) -> bool {
    state.is_some_and(|state| RECOVERY_IN_FLIGHT_STATES.contains(&state))
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AutoRestartLedgerError {
    #[error("illegal auto-restart ledger transition: {state} + {event}")]
    IllegalTransition { state: String, event: String },
}

/// Advance one ledger record through the transition table.
pub fn advance_auto_restart_ledger(
    record: &AutoRestartLedgerRecordWire,
    event: &str,
) -> Result<AutoRestartLedgerRecordWire, AutoRestartLedgerError> {
    let next = match (record.state.as_str(), event) {
        (LEDGER_CLAIMED, LEDGER_EVENT_DEFER) => LEDGER_DEFERRED,
        (LEDGER_CLAIMED, LEDGER_EVENT_DECLINE) => LEDGER_DECLINED,
        (LEDGER_CLAIMED, LEDGER_EVENT_BEGIN_LAUNCH) => LEDGER_LAUNCHING,
        (LEDGER_DEFERRED, LEDGER_EVENT_RECLAIM) => LEDGER_CLAIMED,
        (LEDGER_DEFERRED, LEDGER_EVENT_DECLINE) => LEDGER_DECLINED,
        (LEDGER_LAUNCHING, LEDGER_EVENT_LAUNCHED) => LEDGER_LAUNCHED,
        (LEDGER_LAUNCHING, LEDGER_EVENT_SETTLED_FAILED) => {
            LEDGER_SETTLED_FAILED
        }
        (LEDGER_LAUNCHED, LEDGER_EVENT_SETTLED_OK) => LEDGER_SETTLED_OK,
        (LEDGER_LAUNCHED, LEDGER_EVENT_SETTLED_FAILED) => LEDGER_SETTLED_FAILED,
        _ => {
            return Err(AutoRestartLedgerError::IllegalTransition {
                state: record.state.clone(),
                event: event.to_string(),
            });
        }
    };
    let mut updated = record.clone();
    updated.state = next.to_string();
    if next == LEDGER_DEFERRED {
        updated.deferrals = updated.deferrals.saturating_add(1);
    }
    updated
        .history
        .push(super::wire::AutoRestartLedgerHistoryWire {
            state: next.to_string(),
            at: None,
            note: Some(format!("event {event}")),
        });
    Ok(updated)
}

/// Create a freshly claimed ledger record for one lineage.
pub fn claim_auto_restart_ledger(
    key: &str,
    lineage_root: &str,
) -> AutoRestartLedgerRecordWire {
    let mut record = AutoRestartLedgerRecordWire {
        schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
        key: key.to_string(),
        lineage_root: lineage_root.to_string(),
        state: LEDGER_CLAIMED.to_string(),
        ..Default::default()
    };
    record
        .history
        .push(super::wire::AutoRestartLedgerHistoryWire {
            state: LEDGER_CLAIMED.to_string(),
            at: None,
            note: Some("claimed".to_string()),
        });
    record
}

/// Derive the lineage root: the first that exists of
/// `agent_meta.auto_restart.lineage_root`, the retry-chain root, then
/// the row's own artifacts timestamp. A manual `,x` carries none of
/// these, so it starts a fresh lineage.
pub fn auto_restart_lineage_root(
    auto_restart_lineage_root: Option<&str>,
    retry_chain_root_timestamp: Option<&str>,
    artifacts_timestamp: &str,
) -> String {
    for candidate in [auto_restart_lineage_root, retry_chain_root_timestamp]
        .into_iter()
        .flatten()
    {
        if !candidate.trim().is_empty() {
            return candidate.to_string();
        }
    }
    artifacts_timestamp.to_string()
}
