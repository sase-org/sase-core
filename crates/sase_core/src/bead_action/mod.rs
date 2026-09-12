//! Strict bead-action policy for stitch creation and finalizer decisions.
//!
//! Hosts collect assigned-bead identity, canonical commit method, repository
//! scope, the requested action, and the status needed to validate `close`.
//! This module returns a typed disposition or a diagnostic. It performs no
//! filesystem, subprocess, or bead-store I/O.

mod policy;
mod wire;

pub use policy::{
    decide_bead_action, decide_bead_action_from_json,
    validate_finalizer_assigned_bead_binding, validate_finalizer_bead_decision,
    validate_finalizer_bead_decision_from_json,
};
pub use wire::{
    parse_bead_action_field, parse_bead_action_value, BeadActionDecisionWire,
    BeadActionDispositionWire, BeadActionError, BeadActionRequestWire,
    BeadActionStatusFactWire, BeadActionWire, BeadCommitMethodWire,
    BeadRepositoryScopeWire, FinalizerBeadDecisionWire, BEAD_ACTION_USAGE,
    BEAD_ACTION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
