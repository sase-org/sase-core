//! Agent Hijack ledger, eligibility, status, escalation, and continuation
//! policy.
//!
//! Shared semantics every frontend must match: the pure ledger, eligibility,
//! and status policy plus the never-downgrade rung walk and the successor
//! prompt shape. Python owns file I/O, locks, signals, launches, and the
//! CLI/TUI. Mirrors the `agent_auto_restart` split.

pub mod continuation;
mod eligibility;
pub mod escalation;
mod ledger;
mod status;
pub mod wire;

#[cfg(test)]
mod tests;

pub use continuation::{
    agent_hijack_continuation_prompt, strip_launch_directives,
    AgentHijackContinuationRequestWire, AgentHijackEvidenceWire,
    AGENT_HIJACK_CONTINUATION_WIRE_SCHEMA_VERSION, STRIPPED_LAUNCH_DIRECTIVES,
    STRIPPED_LAUNCH_DIRECTIVE_ALIASES,
};
pub use eligibility::{agent_hijack_eligibility, AgentHijackEligibilityError};
pub use escalation::{
    agent_hijack_escalation_target, normalize_hijack_rung,
    AgentHijackEscalationRequestWire, AgentHijackEscalationResultWire,
    AgentHijackModelFactsWire, AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
};
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
