//! Staged-handoff wire types, schema validation, and admission guards.
//!
//! Hosts gather observations and persist artifacts; this module owns the
//! record/receipt contracts, kind invariants, and the one-pending-handoff
//! admission policy. It is separate from the finalizer context wire.

pub mod gate;
mod monitor_launch;
mod monitor_lifecycle;
mod validate;
mod wire;

pub use gate::{
    build_gate_staged_payload, gate_payload_workspace_mode,
    gate_staged_payload_schema_version, validate_gate_staged_payload,
    GateAutoSnapshotWire, GateCreatorWire, GateResourceSnapshotWire,
    GateStagedPayloadWire, GateWorkspaceModeWire,
    GATE_STAGED_PAYLOAD_SCHEMA_VERSION,
};
pub use monitor_launch::{
    staged_monitor_launch_payload_schema_version,
    validate_staged_monitor_launch_payload, StagedMonitorContinuationWire,
    StagedMonitorLaunchPayloadWire, StagedMonitorResourcesWire,
    StagedMonitorStarterWire, StagedMonitorWorkspaceWire,
    STAGED_MONITOR_LAUNCH_PAYLOAD_SCHEMA_VERSION,
};
pub use monitor_lifecycle::{
    evaluate_staged_monitor_membership, evaluate_staged_owner_liveness,
    evaluate_staged_self_wait, StagedMonitorMembershipRequestWire,
    StagedMonitorMembershipWire, StagedOwnerLivenessDecisionWire,
    StagedOwnerLivenessRequestWire, StagedSelfWaitDecisionWire,
    StagedSelfWaitRequestWire,
};
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
