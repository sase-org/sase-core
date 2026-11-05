//! Agent Hijack ledger, eligibility, status, escalation, continuation,
//! recovery preview, execution validation, and reconciliation policy.
//!
//! Shared semantics every frontend must match: the pure ledger, eligibility,
//! and status policy plus the never-downgrade rung walk, the successor
//! prompt shape, keep-work preview assembly, and abandoned-resolution
//! decisions. Python owns file I/O, locks, signals, launches, and the
//! CLI/TUI. Mirrors the `agent_auto_restart` split.

pub mod continuation;
mod eligibility;
pub mod escalation;
pub mod execution;
mod ledger;
pub mod preview;
pub mod reconcile;
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
pub use execution::{
    hijack_recovery_confirm, hijack_recovery_effective_target,
    hijack_recovery_rollback, hijack_recovery_validate_execution,
    AgentHijackAdmissionDescriptorWire, AgentHijackBootstrapAckWire,
    AgentHijackConfirmationDecisionWire, AgentHijackConfirmationFactsWire,
    AgentHijackEffectiveTargetRequestWire,
    AgentHijackEffectiveTargetResultWire, AgentHijackExecutionDecisionWire,
    AgentHijackExecutionError, AgentHijackExecutionFactsWire,
    AgentHijackRollbackDecisionWire, AgentHijackRollbackFactsWire,
    AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION, HIJACK_CONFIRM_RETAIN,
    HIJACK_CONFIRM_SETTLE, HIJACK_EXECUTION_MODE_DISPATCH,
    HIJACK_EXECUTION_MODE_PREPARE, HIJACK_EXECUTION_PROCEED,
    HIJACK_EXECUTION_REFUSE, HIJACK_ROLLBACK_RETAIN,
    HIJACK_ROLLBACK_RETURN_TO_HELD,
};
pub use ledger::{advance_agent_hijack_record, AgentHijackLedgerError};
pub use preview::{
    hijack_recovery_preview, AgentHijackPreviewError,
    AgentHijackRecoveryActionWire, AgentHijackRecoveryHeaderWire,
    AgentHijackRecoveryPreviewRequestWire, AgentHijackRecoveryPreviewWire,
    AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION,
};
pub use reconcile::{
    hijack_recovery_reconcile, AgentHijackReconcileDecisionWire,
    AgentHijackReconcileError, AgentHijackReconcileFactsWire,
    AgentHijackResolverObservationWire, AgentHijackSuccessorObservationWire,
    AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION,
};
pub use status::{
    agent_hijack_status_for_done_marker, CONTINUED_STATUS, ESCALATED_STATUS,
    HIJACKED_STATUS, INTERRUPTED_STATUS,
};
pub use wire::*;

/// Return the Agent Hijack wire schema version.
pub fn agent_hijack_wire_schema_version() -> u32 {
    AGENT_HIJACK_WIRE_SCHEMA_VERSION
}
