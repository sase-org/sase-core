//! Cross-frontend operational workspace lease ownership decisions.
//!
//! Allocation and claim-transfer stay in [`crate::agent_launch`]. This
//! module only answers whether a workspace number or persisted settlement
//! policy may be used as a machine-owned operational lease, and how to
//! name a failed lease step. Failures never authorize a fallback to the
//! user-owned primary checkout.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical primary checkout identity.
pub const PRIMARY_WORKSPACE_NUM: u32 = 0;
/// Legacy spelling of the primary checkout.
pub const LEGACY_PRIMARY_WORKSPACE_NUM: u32 = 1;
/// First workspace number that may be machine-owned while claimed.
pub const MACHINE_OWNED_MIN_WORKSPACE: u32 = 10;
/// Inclusive upper bound of the unified claim pool.
pub const UNIFIED_MAX_WORKSPACE: u32 = 999;
/// Settlement-policy `kind` for an operational lease.
pub const OPERATIONAL_LEASE_POLICY_KIND: &str = "operational_lease";

/// Why an operational lease could not be used or continued.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OperationalLeaseError {
    #[error("{0}")]
    InvalidWorkspace(String),
    #[error("{0}")]
    InvalidPolicy(String),
}

/// Named lease step that failed. Callers must surface this name and must
/// not fall back to primary `#0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalLeaseFailureKind {
    Allocation,
    Materialization,
    Preparation,
    Transfer,
    Recovery,
}

impl OperationalLeaseFailureKind {
    /// Stable snake_case name used in resumable error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allocation => "allocation",
            Self::Materialization => "materialization",
            Self::Preparation => "preparation",
            Self::Transfer => "transfer",
            Self::Recovery => "recovery",
        }
    }
}

/// Persisted settlement policy that releases one operational lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalLeasePolicyWire {
    pub kind: String,
    pub project_file: String,
    pub workspace_num: u32,
    pub workflow: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub holder: Option<String>,
}

/// Map the legacy primary spelling `#1` onto canonical `#0`.
#[must_use]
pub fn normalize_workspace_num(workspace_num: u32) -> u32 {
    if workspace_num == LEGACY_PRIMARY_WORKSPACE_NUM {
        PRIMARY_WORKSPACE_NUM
    } else {
        workspace_num
    }
}

/// Return whether *workspace_num* is in the machine-owned unified pool.
#[must_use]
pub fn is_operational_lease_workspace(workspace_num: u32) -> bool {
    normalize_workspace_num(workspace_num) >= MACHINE_OWNED_MIN_WORKSPACE
}

/// Unified-pool bounds used when allocating an operational lease.
#[must_use]
pub fn operational_lease_pool_bounds() -> (u32, u32) {
    (MACHINE_OWNED_MIN_WORKSPACE, UNIFIED_MAX_WORKSPACE)
}

/// Accept a workspace number for a machine-owned operational lease.
///
/// Primary `#0`, the legacy `#1` spelling, and reserved `#2`–`#9` are
/// rejected. The returned number is normalized.
pub fn authorize_operational_lease_workspace(
    workspace_num: u32,
) -> Result<u32, OperationalLeaseError> {
    let normalized = normalize_workspace_num(workspace_num);
    if normalized == PRIMARY_WORKSPACE_NUM {
        return Err(OperationalLeaseError::InvalidWorkspace(
            "cannot lease primary workspace #0 \
             (legacy #1 normalizes to the user-owned primary checkout)"
                .to_string(),
        ));
    }
    if normalized < MACHINE_OWNED_MIN_WORKSPACE {
        return Err(OperationalLeaseError::InvalidWorkspace(format!(
            "cannot lease reserved workspace #{normalized}; \
             machine-owned leases start at #{MACHINE_OWNED_MIN_WORKSPACE}"
        )));
    }
    if normalized > UNIFIED_MAX_WORKSPACE {
        return Err(OperationalLeaseError::InvalidWorkspace(format!(
            "cannot lease workspace #{normalized}; \
             the unified claim pool ends at #{UNIFIED_MAX_WORKSPACE}"
        )));
    }
    Ok(normalized)
}

/// Return whether a settlement policy `kind` identifies an operational lease.
#[must_use]
pub fn is_operational_lease_policy_kind(kind: &str) -> bool {
    kind == OPERATIONAL_LEASE_POLICY_KIND
}

/// Validate a persisted operational-lease settlement policy.
///
/// Returns the authorized workspace number when the policy is complete
/// and names a leasable checkout.
pub fn validate_operational_lease_policy(
    policy: &OperationalLeasePolicyWire,
) -> Result<u32, OperationalLeaseError> {
    if !is_operational_lease_policy_kind(&policy.kind) {
        return Err(OperationalLeaseError::InvalidPolicy(format!(
            "settlement policy kind {:?} is not {OPERATIONAL_LEASE_POLICY_KIND}",
            policy.kind
        )));
    }
    if policy.project_file.trim().is_empty() {
        return Err(OperationalLeaseError::InvalidPolicy(
            "operational lease policy is missing project_file".to_string(),
        ));
    }
    if policy.workflow.trim().is_empty() {
        return Err(OperationalLeaseError::InvalidPolicy(
            "operational lease policy is missing workflow".to_string(),
        ));
    }
    authorize_operational_lease_workspace(policy.workspace_num)
}

/// Build a resumable error that names the failed lease step.
///
/// The message never authorizes using the user's primary checkout.
#[must_use]
pub fn operational_lease_failure_message(
    kind: OperationalLeaseFailureKind,
    operation: &str,
    detail: &str,
) -> String {
    let step = kind.as_str();
    let named_operation = if operation.trim().is_empty() {
        step.to_string()
    } else {
        format!("{step} of {operation}")
    };
    format!(
        "operational workspace lease failed during {named_operation}: {detail}; \
         the user-owned primary checkout was left untouched"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(workspace_num: u32) -> OperationalLeasePolicyWire {
        OperationalLeasePolicyWire {
            kind: OPERATIONAL_LEASE_POLICY_KIND.to_string(),
            project_file: "/tmp/demo.sase".to_string(),
            workspace_num,
            workflow: "chop:demo".to_string(),
            cl_name: Some("holder".to_string()),
            holder: Some("holder".to_string()),
        }
    }

    #[test]
    fn normalize_maps_legacy_primary_only() {
        assert_eq!(normalize_workspace_num(0), 0);
        assert_eq!(normalize_workspace_num(1), 0);
        assert_eq!(normalize_workspace_num(9), 9);
        assert_eq!(normalize_workspace_num(10), 10);
    }

    #[test]
    fn authorize_rejects_primary_and_reserved_numbers() {
        assert!(authorize_operational_lease_workspace(0).is_err());
        assert!(authorize_operational_lease_workspace(1).is_err());
        assert!(authorize_operational_lease_workspace(5).is_err());
        assert_eq!(authorize_operational_lease_workspace(10).unwrap(), 10);
        assert_eq!(authorize_operational_lease_workspace(999).unwrap(), 999);
        assert!(authorize_operational_lease_workspace(1000).is_err());
    }

    #[test]
    fn primary_error_names_legacy_spelling() {
        let error = authorize_operational_lease_workspace(1).unwrap_err();
        assert!(error.to_string().contains("legacy #1"));
        assert!(error.to_string().contains("#0"));
    }

    #[test]
    fn pool_bounds_match_unified_claim_range() {
        assert_eq!(
            operational_lease_pool_bounds(),
            (MACHINE_OWNED_MIN_WORKSPACE, UNIFIED_MAX_WORKSPACE)
        );
        assert!(is_operational_lease_workspace(10));
        assert!(!is_operational_lease_workspace(0));
        assert!(!is_operational_lease_workspace(1));
        assert!(!is_operational_lease_workspace(9));
    }

    #[test]
    fn validate_policy_requires_kind_identity_and_leasable_workspace() {
        assert_eq!(validate_operational_lease_policy(&policy(12)).unwrap(), 12);

        let mut bad_kind = policy(12);
        bad_kind.kind = "monitor".to_string();
        assert!(validate_operational_lease_policy(&bad_kind).is_err());

        let mut missing_file = policy(12);
        missing_file.project_file = "  ".to_string();
        assert!(validate_operational_lease_policy(&missing_file).is_err());

        let mut missing_workflow = policy(12);
        missing_workflow.workflow.clear();
        assert!(validate_operational_lease_policy(&missing_workflow).is_err());

        assert!(validate_operational_lease_policy(&policy(0)).is_err());
        assert!(validate_operational_lease_policy(&policy(1)).is_err());
    }

    #[test]
    fn failure_message_names_step_and_forbids_primary_fallback() {
        let message = operational_lease_failure_message(
            OperationalLeaseFailureKind::Allocation,
            "bead-claim-checks",
            "all workspaces are claimed",
        );
        assert!(message.contains("allocation of bead-claim-checks"));
        assert!(message.contains("all workspaces are claimed"));
        assert!(message.contains("primary checkout was left untouched"));
        assert!(!message.to_lowercase().contains("fall back"));
        assert!(!message.contains("workspace #0"));
    }

    #[test]
    fn empty_operation_uses_the_step_name_alone() {
        let message = operational_lease_failure_message(
            OperationalLeaseFailureKind::Transfer,
            "",
            "supervisor pid missing",
        );
        assert!(message.contains("during transfer: supervisor pid missing"));
    }
}
