//! Durable gate-decision-acceptance policy.
//!
//! A gate's decision -- which option(s) were selected, with what input and
//! feedback -- must be visible immediately once validated and durably
//! recorded, independent of how long its execution (archive publication,
//! workspace preparation, successor launch) takes. This module returns a
//! typed acceptance outcome or a conflict diagnostic. It performs no
//! filesystem, subprocess, or notification I/O: the host persists the
//! returned receipt under its own bounded per-gate lock and dismisses the
//! notification.

mod policy;
mod wire;

pub use policy::{
    claim_gate_decision_execution, claim_gate_decision_execution_from_json,
    decide_gate_decision_acceptance, decide_gate_decision_acceptance_from_json,
    decide_gate_lifecycle, decide_gate_lifecycle_from_json,
    gate_decision_identity_fingerprint,
};
pub use wire::{
    GateDecisionAcceptanceOutcomeWire, GateDecisionAcceptanceRequestWire,
    GateDecisionError, GateDecisionExecutionClaimOutcomeWire,
    GateDecisionExecutionClaimRequestWire, GateDecisionExecutionFactsWire,
    GateDecisionExecutionOwnerKindWire, GateDecisionExecutionOwnerRecordWire,
    GateDecisionExecutionOwnerWire, GateDecisionFailureOutcomeWire,
    GateDecisionFailureStageWire, GateDecisionOutcomeStatusWire,
    GateDecisionOwnerLossWire, GateDecisionOwnerSummaryWire,
    GateDecisionReceiptWire, GateLifecycleDecisionWire,
    GateLifecycleRequestWire, GATE_DECISION_CODE_CONFLICT,
    GATE_DECISION_CODE_INVALID_REQUEST, GATE_DECISION_CODE_UNSUPPORTED_SCHEMA,
    GATE_DECISION_WIRE_SCHEMA_VERSION, GATE_LIFECYCLE_CODE_INVALID_RECEIPT,
    GATE_LIFECYCLE_CODE_INVALID_REQUEST,
    GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_FAILED,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_OWNER_LOST,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED,
    GATE_LIFECYCLE_DISPOSITION_ANSWERED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW,
    GATE_LIFECYCLE_DISPOSITION_PENDING, GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
