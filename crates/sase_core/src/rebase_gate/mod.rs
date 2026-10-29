//! Pure policy for checking trees produced by rebasing concurrent work.
//!
//! This module owns the wire contract and decisions only. Git, configuration
//! loading, process execution, and persistence remain with the caller.

mod policy;
mod wire;

pub use policy::{
    build_rebase_gate_context, classify_rebase_gate_result,
    merge_rebase_gate_contexts, update_rebase_gate_context,
    validate_rebase_gate_timeout_seconds,
};
pub use wire::{
    RebaseGateChangeKindWire, RebaseGateChangedPathWire,
    RebaseGateClassifyRequestWire, RebaseGateCommandFactsWire,
    RebaseGateConfigWire, RebaseGateContextMergeRequestWire,
    RebaseGateContextUpdateRequestWire, RebaseGateContextUpdateWire,
    RebaseGateContextWire, RebaseGateDecisionWire, RebaseGateError,
    RebaseGateFactsWire, RebaseGateFindingWire, RebaseGateInputIdsWire,
    RebaseGateRebaseOutcomeWire, RebaseGateReportStatusWire,
    RebaseGateReportWire, RebaseGateVerdictWire,
    REBASE_GATE_DEFAULT_TIMEOUT_SECONDS, REBASE_GATE_MAX_TIMEOUT_SECONDS,
    REBASE_GATE_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
