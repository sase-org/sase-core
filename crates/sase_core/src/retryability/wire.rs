//! Wire records for deterministic failure retryability classification.

use serde::{Deserialize, Serialize};

pub const RETRYABILITY_WIRE_SCHEMA_VERSION: u32 = 1;

pub const RETRYABILITY_VERDICT_TRANSIENT: &str = "retryable_transient";
pub const RETRYABILITY_VERDICT_AFTER_DELAY: &str = "retryable_after_delay";
pub const RETRYABILITY_VERDICT_PERMANENT: &str = "permanent";

pub const RETRY_OPERATION_GIT: &str = "git";
pub const RETRY_OPERATION_GIT_CLONE: &str = "git_clone";
pub const RETRY_OPERATION_GH: &str = "gh";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureObservationWire {
    pub operation_kind: String,
    pub exit_status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryabilityVerdictWire {
    pub schema_version: u32,
    pub verdict: String,
    pub reason: String,
    pub retryable: bool,
    pub retry_after_seconds: Option<u64>,
}
