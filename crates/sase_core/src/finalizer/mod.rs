//! Shared finalizer protocol wires and deterministic domain rules.
//!
//! The host owns filesystem inspection, subprocess execution, and recovery
//! turns. This module owns the frontend-neutral contract: versioned wire
//! shapes, selector replay, dependency ordering, digest binding, submission
//! coverage, and aggregate outcome classification.

pub mod digest;
pub mod outcome;
pub mod selection;
pub mod submission;
pub mod wire;

pub use digest::{
    canonical_json_bytes, canonical_json_sha256, finalizer_digest_json_value,
    finalizer_digest_serializable,
};
pub use outcome::{
    aggregate_finalizer_outcomes, validate_finalizer_instance_results,
};
pub use selection::{
    authenticate_finalizer_plan, finalizer_instance_spec_digest,
    finalizer_plan_digest, finalizer_provider_spec_digest,
    resolve_finalizer_plan, validate_finalizer_instance_spec,
    validate_finalizer_plan, validate_finalizer_provider_spec,
};
pub use submission::{
    finalizer_context_digest, validate_finalizer_context,
    validate_finalizer_submission,
};
pub use wire::{
    FinalizerAggregateResultWire, FinalizerAggregateStatusWire,
    FinalizerAttemptWire, FinalizerContextWire,
    FinalizerDiagnosticSeverityWire, FinalizerDiagnosticWire,
    FinalizerInstancePolicyWire, FinalizerInstanceResultWire,
    FinalizerInstanceSpecWire, FinalizerInstanceStatusWire,
    FinalizerObligationWire, FinalizerOutcomeEvidenceWire,
    FinalizerPayloadRequirementWire, FinalizerPlanEntryWire,
    FinalizerPlanInputWire, FinalizerPlanWire, FinalizerProviderCapabilityWire,
    FinalizerProviderSpecWire, FinalizerRefusalPolicyWire,
    FinalizerSelectorOpWire, FinalizerSubmissionEnvelopeWire,
    FinalizerSubmissionPayloadWire, FinalizerSubmissionValidationWire,
    FinalizerTriggerKindWire, FINALIZER_WIRE_SCHEMA_VERSION,
};

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FinalizerError {
    #[error("{0}")]
    Validation(String),
}

impl FinalizerError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}
