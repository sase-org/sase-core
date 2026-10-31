//! Request/decision wires for workspace retention classification.

use serde::{Deserialize, Serialize};

/// Wire schema version for retention request/decision records.
pub const WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Pin the existing claim and keep recovery evidence.
pub const WORKSPACE_RETENTION_DISPOSITION_RETAIN: &str = "retain";
/// Follow the normal release path; do not hold.
pub const WORKSPACE_RETENTION_DISPOSITION_RELEASE: &str = "release";
/// The claim no longer belongs to this owner; do not pin, release, or clear.
pub const WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED: &str =
    "ownership_moved";

/// Confirmed same-workspace successor inherited the obligations.
pub const WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR: &str =
    "same_workspace_successor";
/// Session-owned uncommitted work remains without a same-workspace successor.
pub const WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT: &str =
    "session_owned_dirt";
/// Every relevant repo is clean or only unchanged foreign baseline dirt.
pub const WORKSPACE_RETENTION_REASON_PROVEN_CLEAN: &str = "proven_clean";
/// Dirt or custody evidence exists, but a probe failed.
pub const WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE: &str =
    "evidence_unavailable";
/// Observed claim identity does not match the settling owner.
pub const WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED: &str =
    "claim_ownership_moved";
/// Gate released the workspace and no longer owns a matching claim.
pub const WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE: &str =
    "gate_released_workspace";
/// No baseline, but durable custody still names this checkout.
pub const WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY: &str =
    "missing_baseline_with_custody";
/// The still-live creator remains responsible for the work.
pub const WORKSPACE_RETENTION_REASON_CREATOR_RESTORED: &str =
    "creator_restored";

fn default_schema_version() -> u32 {
    WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION
}

/// Identity of one RUNNING-field claim owner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetentionClaimWire {
    pub workspace_num: u32,
    #[serde(default)]
    pub workflow: String,
    #[serde(default)]
    pub cl_name: String,
    #[serde(default)]
    pub artifacts_timestamp: String,
    #[serde(default)]
    pub pid: u32,
}

/// Confirmed successor custody for the workspace being settled.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetentionSuccessorWire {
    /// Live successor with transfer evidence, not a historical `carried_to`.
    #[serde(default)]
    pub confirmed_active: bool,
    #[serde(default)]
    pub same_workspace: bool,
    #[serde(default)]
    pub inherited_obligations: bool,
}

/// Session-owned dirt and baseline/custody availability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetentionDirtWire {
    /// At least one relevant repo has session-owned uncommitted work.
    #[serde(default)]
    pub session_owned: bool,
    /// Dirty paths exist, but every one still matches the baseline fingerprint.
    #[serde(default)]
    pub only_unchanged_foreign: bool,
    /// Every relevant repo probed clean.
    #[serde(default)]
    pub all_proven_clean: bool,
    /// At least one relevant repo had no baseline snapshot.
    #[serde(default)]
    pub missing_baseline: bool,
    /// Durable custody (opened-repo, origin, or hosting workspace) exists.
    #[serde(default)]
    pub positive_custody: bool,
}

/// Facts Python collected about one claim being relinquished.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetentionRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub expected_claim: WorkspaceRetentionClaimWire,
    #[serde(default)]
    pub observed_claim: Option<WorkspaceRetentionClaimWire>,
    #[serde(default)]
    pub successor: WorkspaceRetentionSuccessorWire,
    #[serde(default)]
    pub creator_restored: bool,
    #[serde(default)]
    pub gate_released_workspace: bool,
    #[serde(default)]
    pub dirt: WorkspaceRetentionDirtWire,
    /// A dirt or snapshot probe failed. Never treat this as clean.
    #[serde(default)]
    pub probe_failed: bool,
}

/// Explicit retain/release/ownership-moved decision with a stable reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetentionDecisionWire {
    pub schema_version: u32,
    pub disposition: String,
    pub reason: String,
}

/// Return the retention wire schema version.
#[must_use]
pub const fn workspace_retention_wire_schema_version() -> u32 {
    WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION
}
