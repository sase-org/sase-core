//! Pure deterministic retryability classifier for GitHub-adjacent failures.
//!
//! The host owns subprocess execution, sleeping, deadlines, and telemetry.
//! This module only classifies observed `git`/`gh` stdout, stderr, exit
//! status, and operation kind into a stable retryability verdict.

pub mod classify;
pub mod wire;

#[cfg(test)]
mod tests;

pub use classify::{
    classify_failure_retryability, retryability_wire_schema_version,
};
pub use wire::{
    FailureObservationWire, RetryabilityVerdictWire,
    RETRYABILITY_VERDICT_AFTER_DELAY,
    RETRYABILITY_VERDICT_CREDENTIAL_UNCONFIRMED,
    RETRYABILITY_VERDICT_PERMANENT, RETRYABILITY_VERDICT_TRANSIENT,
    RETRYABILITY_WIRE_SCHEMA_VERSION, RETRY_OPERATION_GH, RETRY_OPERATION_GIT,
    RETRY_OPERATION_GIT_CLONE,
};
