//! Pending commit-checkpoint recovery eligibility.
//!
//! Hosts gather facts about a checkpoint file, accepted repository, and
//! payload. This module decides whether the finalizer must resume, refuse,
//! or ignore that checkpoint before a clean-tree shortcut or a new stitch.

use serde::{Deserialize, Serialize};

pub const PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION: u32 = 1;

pub const PENDING_COMMIT_CHECKPOINT_ACTION_NONE: &str = "none";
pub const PENDING_COMMIT_CHECKPOINT_ACTION_RESUME: &str = "resume";
pub const PENDING_COMMIT_CHECKPOINT_ACTION_FAIL: &str = "fail";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCommitCheckpointRequestWire {
    #[serde(default)]
    pub checkpoint_present: bool,
    #[serde(default)]
    pub malformed: bool,
    #[serde(default)]
    pub unknown_version: bool,
    #[serde(default)]
    pub repository_matches: bool,
    #[serde(default)]
    pub subject_matches: bool,
    #[serde(default)]
    pub payload_matches: bool,
    #[serde(default)]
    pub has_operation_id: bool,
    #[serde(default)]
    pub independent_ownership_evidence: bool,
    #[serde(default)]
    pub dispatch_completed: bool,
    #[serde(default)]
    pub pending_after_hook: bool,
    #[serde(default)]
    pub pending_tracking: bool,
    #[serde(default)]
    pub unpushed: bool,
    #[serde(default)]
    pub commit_sha_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCommitCheckpointDecisionWire {
    pub schema_version: u32,
    pub action: String,
    pub reason: String,
    pub diagnostics: Vec<String>,
}

pub fn decide_pending_commit_checkpoint_recovery(
    request: &PendingCommitCheckpointRequestWire,
) -> PendingCommitCheckpointDecisionWire {
    if !request.checkpoint_present {
        return none("no pending commit checkpoint", vec![]);
    }
    if request.malformed {
        return fail(
            "commit checkpoint is malformed; leaving evidence intact",
            vec!["malformed_checkpoint".to_string()],
        );
    }
    if request.unknown_version {
        return fail(
            "commit checkpoint has an unknown version; automatic recovery refused",
            vec!["unknown_checkpoint_version".to_string()],
        );
    }

    let pending = pending_work(request);
    if !request.repository_matches {
        if pending {
            return fail(
                "pending commit checkpoint is not for an accepted repository; refusing to overwrite it",
                vec!["checkpoint_repository_mismatch".to_string()],
            );
        }
        return none(
            "checkpoint belongs to another repository and is already complete",
            vec![],
        );
    }
    if !request.subject_matches || !request.payload_matches {
        return fail(
            "commit checkpoint does not match the accepted work; automatic recovery refused",
            vec!["checkpoint_payload_mismatch".to_string()],
        );
    }
    if !request.has_operation_id && !request.independent_ownership_evidence {
        return fail(
            "legacy commit checkpoint lacks independently verified ownership evidence",
            vec!["legacy_checkpoint_unproven".to_string()],
        );
    }
    if request.unpushed && !request.commit_sha_present {
        return fail(
            "unpushed commit checkpoint is missing commit proof",
            vec!["missing_commit_proof".to_string()],
        );
    }
    if (request.pending_after_hook || request.pending_tracking)
        && !request.commit_sha_present
        && request.dispatch_completed
    {
        return fail(
            "pending hook/tracking checkpoint is missing commit proof",
            vec!["missing_commit_proof".to_string()],
        );
    }
    if pending {
        return resume(
            "resume the run-owned pending commit checkpoint before a new stitch or clean acceptance",
            diagnostics(request),
        );
    }
    none("commit checkpoint is already complete", vec![])
}

fn pending_work(request: &PendingCommitCheckpointRequestWire) -> bool {
    request.unpushed
        || request.pending_after_hook
        || request.pending_tracking
        || !request.dispatch_completed
}

fn diagnostics(request: &PendingCommitCheckpointRequestWire) -> Vec<String> {
    let mut items = Vec::new();
    if request.unpushed {
        items.push("unpushed".to_string());
    }
    if !request.dispatch_completed {
        items.push("pending_dispatch".to_string());
    }
    if request.pending_after_hook {
        items.push("pending_after_hook".to_string());
    }
    if request.pending_tracking {
        items.push("pending_tracking".to_string());
    }
    items
}

fn none(
    reason: impl Into<String>,
    diagnostics: Vec<String>,
) -> PendingCommitCheckpointDecisionWire {
    PendingCommitCheckpointDecisionWire {
        schema_version: PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION,
        action: PENDING_COMMIT_CHECKPOINT_ACTION_NONE.to_string(),
        reason: reason.into(),
        diagnostics,
    }
}

fn resume(
    reason: impl Into<String>,
    diagnostics: Vec<String>,
) -> PendingCommitCheckpointDecisionWire {
    PendingCommitCheckpointDecisionWire {
        schema_version: PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION,
        action: PENDING_COMMIT_CHECKPOINT_ACTION_RESUME.to_string(),
        reason: reason.into(),
        diagnostics,
    }
}

fn fail(
    reason: impl Into<String>,
    diagnostics: Vec<String>,
) -> PendingCommitCheckpointDecisionWire {
    PendingCommitCheckpointDecisionWire {
        schema_version: PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION,
        action: PENDING_COMMIT_CHECKPOINT_ACTION_FAIL.to_string(),
        reason: reason.into(),
        diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned_pending() -> PendingCommitCheckpointRequestWire {
        PendingCommitCheckpointRequestWire {
            checkpoint_present: true,
            repository_matches: true,
            subject_matches: true,
            payload_matches: true,
            has_operation_id: true,
            independent_ownership_evidence: true,
            dispatch_completed: true,
            pending_after_hook: true,
            commit_sha_present: true,
            ..Default::default()
        }
    }

    #[test]
    fn missing_checkpoint_is_none() {
        let decision = decide_pending_commit_checkpoint_recovery(
            &PendingCommitCheckpointRequestWire::default(),
        );
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_NONE);
    }

    #[test]
    fn pending_after_hook_resumes() {
        let decision =
            decide_pending_commit_checkpoint_recovery(&owned_pending());
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_RESUME);
        assert!(decision
            .diagnostics
            .contains(&"pending_after_hook".to_string()));
    }

    #[test]
    fn unpushed_without_sha_fails() {
        let mut req = owned_pending();
        req.pending_after_hook = false;
        req.unpushed = true;
        req.commit_sha_present = false;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision.reason.contains("commit proof"));
    }

    #[test]
    fn subject_mismatch_fails() {
        let mut req = owned_pending();
        req.subject_matches = false;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
    }

    #[test]
    fn foreign_pending_checkpoint_fails() {
        let mut req = owned_pending();
        req.repository_matches = false;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
    }

    #[test]
    fn complete_checkpoint_is_none() {
        let mut req = owned_pending();
        req.pending_after_hook = false;
        req.dispatch_completed = true;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_NONE);
    }

    #[test]
    fn legacy_without_proof_fails() {
        let mut req = owned_pending();
        req.has_operation_id = false;
        req.independent_ownership_evidence = false;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision.reason.contains("legacy"));
    }

    #[test]
    fn pending_dispatch_without_sha_still_resumes() {
        let mut req = owned_pending();
        req.dispatch_completed = false;
        req.pending_after_hook = false;
        req.commit_sha_present = false;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_RESUME);
    }
}
