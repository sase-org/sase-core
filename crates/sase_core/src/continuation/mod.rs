//! Versioned continuation contracts for monitor result replay.
//!
//! This module is intentionally pure: it validates wire records and computes
//! replay/evidence/policy/budget decisions without reading files, spawning
//! processes, or mutating monitor state.

pub mod budget;
pub mod evidence;
pub mod policy;
pub mod replay;
pub mod schema;

pub use budget::{
    plan_continuation_budget, ContinuationBudgetDecisionKindWire,
    ContinuationBudgetDecisionWire, ContinuationBudgetReductionCandidateWire,
    ContinuationBudgetReductionKindWire, ContinuationBudgetRequestWire,
    ContinuationBudgetReserveWire,
};
pub use evidence::{
    select_continuation_evidence, ContinuationEvidenceContextKindWire,
    ContinuationEvidenceLimitsWire, ContinuationEvidencePolicyWire,
    ContinuationEvidenceSelectionRequestWire,
    ContinuationEvidenceSelectionWire,
};
pub use policy::{
    resolve_continuation_policy, ContinuationActionWire,
    ContinuationOutcomePolicyWire, ContinuationPolicyBranchWire,
    ContinuationPolicyDecisionWire, ContinuationPolicyResolutionRequestWire,
};
pub use replay::{
    plan_continuation_replay, ContinuationBranchAttributionWire,
    ContinuationParentEdgeWire, ContinuationRenderedComponentSizesWire,
    ContinuationReplayManifestWire, ContinuationReplayPlanRequestWire,
};
pub use schema::{
    validate_agent_delta, validate_continuation_delivery_record,
    validate_continuation_graph, validate_continuation_intent,
    validate_continuation_node, validate_continuation_node_value,
    validate_diagnostic_manifest, validate_launch_requester_continuation,
    validate_monitor_result, AgentDeltaStatusWire, AgentDeltaWire,
    ContinuationAttributionWire, ContinuationByteRangeWire,
    ContinuationCheckpointCoverageWire, ContinuationDeliveryAttemptWire,
    ContinuationDeliveryDispositionWire, ContinuationDeliveryKeyWire,
    ContinuationDeliveryRecordWire, ContinuationError,
    ContinuationExecutionIdentityWire, ContinuationGraphValidationWire,
    ContinuationIntentWire, ContinuationModelRouteWire,
    ContinuationNodeKindWire, ContinuationNodeWire, ContinuationOmissionWire,
    ContinuationPromptSegmentProvenanceWire, ContinuationPromptSegmentWire,
    ContinuationRenderedComponentWire, DiagnosticManifestWire,
    DiagnosticStageStatusWire, DiagnosticStageWire,
    LaunchRequesterContinuationModeWire, LaunchRequesterContinuationWire,
    MonitorOutcomeWire, MonitorResultWire, MonitorTimeoutKindWire,
    RetainedLogMetadataWire, CONTINUATION_WIRE_SCHEMA_VERSION,
};
