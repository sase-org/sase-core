//! Staged-handoff wire types, schema validation, and admission guards.
//!
//! Hosts gather observations and persist artifacts; this module owns the
//! record/receipt contracts, kind invariants, and the one-pending-handoff
//! admission policy. It is separate from the finalizer context wire.

mod validate;
mod wire;

pub use validate::{
    evaluate_stage_admission, receipt_authorizes, validate_staged_handoff,
    validate_staged_handoff_receipt, StagedHandoffError,
};
pub use wire::{
    staged_handoff_wire_schema_version, StageAdmissionRequestWire,
    StagedHandoffKindWire, StagedHandoffReceiptWire, StagedHandoffWire,
    SuccessorWorkspaceWire, STAGED_HANDOFF_OUTCOME_ADOPTED,
    STAGED_HANDOFF_OUTCOME_NOT_ADOPTED, STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
