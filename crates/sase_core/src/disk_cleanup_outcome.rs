//! Shared cleanup-outcome normalization for disk owners.
//!
//! Python still owns discovery, subprocess execution, and filesystem effects.
//! This module folds those owner observations into one operational contract:
//! failed or incomplete observations prevent success, while partial effects
//! and known reclaimed bytes remain visible.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION: u32 = 1;

pub const CLEANUP_OUTCOME_STATUS_SUCCESS: &str = "success";
pub const CLEANUP_OUTCOME_STATUS_FAILED: &str = "failed";
pub const CLEANUP_OUTCOME_STATUS_BLOCKED: &str = "blocked";
pub const CLEANUP_OUTCOME_STATUS_INCOMPLETE: &str = "incomplete";

#[derive(Debug, Error)]
pub enum DiskCleanupOutcomeError {
    #[error(
        "disk cleanup outcome requires schema_version {expected}, got {actual}"
    )]
    SchemaVersion { expected: u32, actual: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskCleanupOutcomeRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub owners: Vec<DiskCleanupOwnerOutcomeWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskCleanupOwnerOutcomeWire {
    pub owner: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub reclaimed_bytes: Option<u64>,
    #[serde(default = "default_true")]
    pub byte_accounting_complete: bool,
    #[serde(default)]
    pub owner_error: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub invalid_result: Option<String>,
    #[serde(default)]
    pub blocked_reason: Option<String>,
    #[serde(default)]
    pub required_observation_unavailable: bool,
    #[serde(default)]
    pub incomplete_reason: Option<String>,
    #[serde(default)]
    pub protective_skip: Option<String>,
    #[serde(default)]
    pub capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCleanupOutcomeProblemWire {
    pub owner: String,
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCleanupOwnerResultWire {
    pub owner: String,
    pub status: String,
    pub changed: bool,
    pub known_reclaimed_bytes: u64,
    pub byte_accounting_complete: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCleanupOutcomeResultWire {
    pub schema_version: u32,
    pub status: String,
    pub success: bool,
    pub changed: bool,
    pub known_reclaimed_bytes: u64,
    pub byte_accounting_complete: bool,
    pub protective_skips: u64,
    pub capped_batches: u64,
    pub owners: Vec<DiskCleanupOwnerResultWire>,
    pub problems: Vec<DiskCleanupOutcomeProblemWire>,
}

pub fn normalize_disk_cleanup_outcome(
    request: &DiskCleanupOutcomeRequestWire,
) -> Result<DiskCleanupOutcomeResultWire, DiskCleanupOutcomeError> {
    if request.schema_version != DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION {
        return Err(DiskCleanupOutcomeError::SchemaVersion {
            expected: DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let mut status = CLEANUP_OUTCOME_STATUS_SUCCESS;
    let mut changed = false;
    let mut known_reclaimed_bytes = 0_u64;
    let mut byte_accounting_complete = true;
    let mut protective_skips = 0_u64;
    let mut capped_batches = 0_u64;
    let mut owners = Vec::with_capacity(request.owners.len());
    let mut problems = Vec::new();

    for owner in &request.owners {
        let owner_known_bytes = owner.reclaimed_bytes.unwrap_or(0);
        let owner_accounting_complete = owner.byte_accounting_complete
            && (!owner.changed || owner.reclaimed_bytes.is_some());
        let owner_problems = owner_problems(owner);
        let owner_status = owner_status(&owner_problems);
        status = aggregate_status(status, owner_status);
        changed |= owner.changed;
        known_reclaimed_bytes =
            known_reclaimed_bytes.saturating_add(owner_known_bytes);
        byte_accounting_complete &= owner_accounting_complete
            && owner_status != CLEANUP_OUTCOME_STATUS_INCOMPLETE;
        protective_skips += u64::from(owner.protective_skip.is_some());
        capped_batches += u64::from(owner.capped);
        owners.push(DiskCleanupOwnerResultWire {
            owner: owner.owner.clone(),
            status: owner_status.to_string(),
            changed: owner.changed,
            known_reclaimed_bytes: owner_known_bytes,
            byte_accounting_complete: owner_accounting_complete,
            reasons: owner_problems
                .iter()
                .map(|problem| problem.kind.clone())
                .collect(),
        });
        problems.extend(owner_problems);
    }

    Ok(DiskCleanupOutcomeResultWire {
        schema_version: DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        success: status == CLEANUP_OUTCOME_STATUS_SUCCESS,
        changed,
        known_reclaimed_bytes,
        byte_accounting_complete,
        protective_skips,
        capped_batches,
        owners,
        problems,
    })
}

fn owner_problems(
    owner: &DiskCleanupOwnerOutcomeWire,
) -> Vec<DiskCleanupOutcomeProblemWire> {
    let mut problems = Vec::new();
    if let Some(detail) = clean_option(&owner.owner_error) {
        problems.push(problem(owner, "owner_error", detail));
    }
    if let Some(exit_code) = owner.exit_code.filter(|code| *code != 0) {
        problems.push(problem(
            owner,
            "nonzero_exit",
            &format!("exit code {exit_code}"),
        ));
    }
    if let Some(detail) = clean_option(&owner.invalid_result) {
        problems.push(problem(owner, "invalid_result", detail));
    }
    if let Some(detail) = clean_option(&owner.blocked_reason) {
        problems.push(problem(owner, "blocked", detail));
    }
    if owner.required && owner.required_observation_unavailable {
        problems.push(problem(
            owner,
            "required_observation_unavailable",
            "required observation unavailable",
        ));
    }
    if let Some(detail) = clean_option(&owner.incomplete_reason) {
        problems.push(problem(owner, "incomplete", detail));
    }
    problems
}

fn owner_status(problems: &[DiskCleanupOutcomeProblemWire]) -> &'static str {
    let mut status = CLEANUP_OUTCOME_STATUS_SUCCESS;
    for problem in problems {
        let next = match problem.kind.as_str() {
            "owner_error" | "nonzero_exit" | "invalid_result" => {
                CLEANUP_OUTCOME_STATUS_FAILED
            }
            "blocked" => CLEANUP_OUTCOME_STATUS_BLOCKED,
            "required_observation_unavailable" | "incomplete" => {
                CLEANUP_OUTCOME_STATUS_INCOMPLETE
            }
            _ => CLEANUP_OUTCOME_STATUS_SUCCESS,
        };
        status = aggregate_status(status, next);
    }
    status
}

fn aggregate_status(current: &'static str, next: &'static str) -> &'static str {
    if current == CLEANUP_OUTCOME_STATUS_FAILED
        || next == CLEANUP_OUTCOME_STATUS_FAILED
    {
        CLEANUP_OUTCOME_STATUS_FAILED
    } else if current == CLEANUP_OUTCOME_STATUS_BLOCKED
        || next == CLEANUP_OUTCOME_STATUS_BLOCKED
    {
        CLEANUP_OUTCOME_STATUS_BLOCKED
    } else if current == CLEANUP_OUTCOME_STATUS_INCOMPLETE
        || next == CLEANUP_OUTCOME_STATUS_INCOMPLETE
    {
        CLEANUP_OUTCOME_STATUS_INCOMPLETE
    } else {
        CLEANUP_OUTCOME_STATUS_SUCCESS
    }
}

fn problem(
    owner: &DiskCleanupOwnerOutcomeWire,
    kind: &str,
    detail: &str,
) -> DiskCleanupOutcomeProblemWire {
    DiskCleanupOutcomeProblemWire {
        owner: owner.owner.clone(),
        kind: kind.to_string(),
        detail: detail.to_string(),
    }
}

fn clean_option(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        owners: Vec<DiskCleanupOwnerOutcomeWire>,
    ) -> DiskCleanupOutcomeRequestWire {
        DiskCleanupOutcomeRequestWire {
            schema_version: DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION,
            owners,
        }
    }

    fn owner(name: &str) -> DiskCleanupOwnerOutcomeWire {
        DiskCleanupOwnerOutcomeWire {
            owner: name.to_string(),
            required: true,
            changed: false,
            reclaimed_bytes: None,
            byte_accounting_complete: true,
            owner_error: None,
            exit_code: Some(0),
            invalid_result: None,
            blocked_reason: None,
            required_observation_unavailable: false,
            incomplete_reason: None,
            protective_skip: None,
            capped: false,
        }
    }

    #[test]
    fn mixed_success_and_failure_preserves_partial_effects() {
        let mut workspace = owner("workspace");
        workspace.changed = true;
        workspace.reclaimed_bytes = Some(4096);
        let mut proc = owner("proc");
        proc.exit_code = Some(1);

        let result =
            normalize_disk_cleanup_outcome(&request(vec![workspace, proc]))
                .unwrap();

        assert_eq!(result.status, CLEANUP_OUTCOME_STATUS_FAILED);
        assert!(!result.success);
        assert!(result.changed);
        assert_eq!(result.known_reclaimed_bytes, 4096);
        assert_eq!(result.problems[0].kind, "nonzero_exit");
    }

    #[test]
    fn unavailable_required_observation_is_incomplete() {
        let mut scratch = owner("scratch");
        scratch.required_observation_unavailable = true;

        let result =
            normalize_disk_cleanup_outcome(&request(vec![scratch])).unwrap();

        assert_eq!(result.status, CLEANUP_OUTCOME_STATUS_INCOMPLETE);
        assert!(!result.success);
        assert_eq!(result.problems[0].kind, "required_observation_unavailable");
    }

    #[test]
    fn protective_skip_and_capped_batch_do_not_prevent_success() {
        let mut owner = owner("workspace");
        owner.protective_skip = Some("active workspace".to_string());
        owner.capped = true;

        let result =
            normalize_disk_cleanup_outcome(&request(vec![owner])).unwrap();

        assert_eq!(result.status, CLEANUP_OUTCOME_STATUS_SUCCESS);
        assert!(result.success);
        assert_eq!(result.protective_skips, 1);
        assert_eq!(result.capped_batches, 1);
        assert!(result.problems.is_empty());
    }

    #[test]
    fn unknown_reclaimed_bytes_are_separate_from_operational_success() {
        let mut owner = owner("proc");
        owner.changed = true;

        let result =
            normalize_disk_cleanup_outcome(&request(vec![owner])).unwrap();

        assert_eq!(result.status, CLEANUP_OUTCOME_STATUS_SUCCESS);
        assert!(result.success);
        assert!(result.changed);
        assert_eq!(result.known_reclaimed_bytes, 0);
        assert!(!result.byte_accounting_complete);
    }
}
