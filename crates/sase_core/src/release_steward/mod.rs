//! Pure release-steward policy: facts in, typed decision out.
//!
//! `release_steward_decide` is the only entry point. The clock is an
//! explicit request field. This module performs no HTTP, `gh`, sleep, or
//! other side effects: later phases collect the snapshot and execute at
//! most one train action and one escalation operation.

mod cadence;
mod checks;
mod escalation;
mod facts;
mod keys;
mod policy;
mod train;
mod wire;

pub use facts::{
    AssetFactsWire, BlockerLineageWire, CanaryFactsWire, CanaryResultWire,
    CandidateEvalWire, CheckConclusionWire, CheckFactsWire, ClauseWire,
    CoreActionsAccessWire, CoreCutRunWire, CoreEnsureFactsWire, CoreFactsWire,
    CorePrFactsWire, EvidenceRefWire, FactRead, IssueCommentWire,
    IssueFactsWire, JobFactsWire, MasterFactsWire, MergeEventWire,
    PackageInventoryWire, PypiFactsWire, PypiFileWire, ReleaseFactsWire,
    ReleasePrFactsWire, ReleaseStewardSnapshotWire, RepositoryFactsWire,
    RequiredCheckWire, RunStatusWire, TagFactsWire, TrainFactsWire,
    WorkflowFactsWire, WorkflowRunFactsWire,
};
pub use policy::{release_steward_decide, release_steward_decide_from_json};
pub use wire::{
    ActionDispositionWire, CadencePolicyWire, EscalationOpWire,
    EscalationSeverityWire, PrimaryStateWire, ProposedActionWire,
    ProposedEscalationWire, ReleaseStewardError, ReleaseStewardPolicyWire,
    ReleaseStewardRequestWire, ReleaseStewardResultWire, SecondaryFindingWire,
    TrainActionKindWire, RELEASE_STEWARD_CODE_INVALID_REQUEST,
    RELEASE_STEWARD_CODE_UNSUPPORTED_SCHEMA,
    RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
};

/// Return the release-steward request/result wire schema version.
pub fn release_steward_wire_schema_version() -> u32 {
    RELEASE_STEWARD_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests;
