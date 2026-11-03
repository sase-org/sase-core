//! Pure Agent Hijack ledger, eligibility, and status policy.

mod eligibility;
mod ledger;
mod status;
pub mod wire;

#[cfg(test)]
mod tests;

pub use eligibility::{agent_hijack_eligibility, AgentHijackEligibilityError};
pub use ledger::{advance_agent_hijack_record, AgentHijackLedgerError};
pub use status::{
    agent_hijack_status_for_done_marker, CONTINUED_STATUS, ESCALATED_STATUS,
    HIJACKED_STATUS, INTERRUPTED_STATUS,
};
pub use wire::*;

/// Return the Agent Hijack wire schema version.
pub fn agent_hijack_wire_schema_version() -> u32 {
    AGENT_HIJACK_WIRE_SCHEMA_VERSION
}
