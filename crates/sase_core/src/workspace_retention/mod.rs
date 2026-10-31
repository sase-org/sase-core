//! Workspace retention policy: whether a terminal handoff keeps its claim.
//!
//! Python gathers claim identity, successor/creator custody, session-owned
//! dirt, and probe availability. This module classifies those facts into a
//! retain / release / ownership-moved disposition. It performs no I/O.

mod policy;
#[cfg(test)]
mod tests;
mod wire;

pub use policy::decide_workspace_retention;
pub use wire::{
    workspace_retention_wire_schema_version, WorkspaceRetentionClaimWire,
    WorkspaceRetentionDecisionWire, WorkspaceRetentionDirtWire,
    WorkspaceRetentionRequestWire, WorkspaceRetentionSuccessorWire,
    WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_DISPOSITION_RELEASE,
    WORKSPACE_RETENTION_DISPOSITION_RETAIN,
    WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_REASON_CREATOR_RESTORED,
    WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE,
    WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY,
    WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
    WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR,
    WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION,
};
