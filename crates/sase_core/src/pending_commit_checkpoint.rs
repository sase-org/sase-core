//! Pending commit-checkpoint recovery eligibility.
//!
//! Hosts gather facts about a checkpoint file, accepted repository, and
//! payload. This module decides whether the finalizer must resume, refuse,
//! or ignore that checkpoint before a clean-tree shortcut or a new stitch.

use serde::{Deserialize, Serialize};

pub const PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION: u32 = 2;

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
    pub checkpoint_method: Option<String>,
    #[serde(default)]
    pub accepted_action: Option<String>,
    #[serde(default)]
    pub checkpoint_payload_identity: Option<String>,
    #[serde(default)]
    pub accepted_payload_identity: Option<String>,
    #[serde(default)]
    pub checkpoint_run_id: Option<String>,
    #[serde(default)]
    pub current_run_id: Option<String>,
    #[serde(default)]
    pub checkpoint_agent_id: Option<String>,
    #[serde(default)]
    pub current_agent_id: Option<String>,
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
    let identity = identity_matches(request);
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
    if pending && !identity.current_present {
        return fail(
            "current run and agent identity are required for automatic checkpoint recovery",
            vec!["missing_current_identity".to_string()],
        );
    }
    if pending && identity.foreign_run {
        return fail(
            "pending commit checkpoint belongs to a different run; automatic recovery refused",
            vec!["checkpoint_run_mismatch".to_string()],
        );
    }
    if pending && identity.foreign_agent {
        return fail(
            "pending commit checkpoint belongs to a different agent; automatic recovery refused",
            vec!["checkpoint_agent_mismatch".to_string()],
        );
    }
    if !operation_matches(request) {
        return fail(
            "commit checkpoint operation does not match the accepted finalizer action",
            vec!["checkpoint_operation_mismatch".to_string()],
        );
    }
    if !payload_matches(request) {
        return fail(
            "commit checkpoint does not match the accepted work; automatic recovery refused",
            vec!["checkpoint_payload_mismatch".to_string()],
        );
    }
    if !identity.ownership_proven {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointIdentityDecision {
    current_present: bool,
    foreign_run: bool,
    foreign_agent: bool,
    ownership_proven: bool,
}

fn identity_matches(
    request: &PendingCommitCheckpointRequestWire,
) -> CheckpointIdentityDecision {
    let checkpoint_run_id =
        clean_optional(request.checkpoint_run_id.as_deref());
    let current_run_id = clean_optional(request.current_run_id.as_deref());
    let checkpoint_agent_id =
        clean_optional(request.checkpoint_agent_id.as_deref());
    let current_agent_id = clean_optional(request.current_agent_id.as_deref());
    let current_present =
        current_run_id.is_some() && current_agent_id.is_some();
    let run_matches = checkpoint_run_id
        .zip(current_run_id)
        .map(|(checkpoint, current)| checkpoint == current);
    let agent_matches = checkpoint_agent_id
        .zip(current_agent_id)
        .map(|(checkpoint, current)| checkpoint == current);
    let run_owned = request.has_operation_id
        && run_matches == Some(true)
        && agent_matches == Some(true);
    CheckpointIdentityDecision {
        current_present,
        foreign_run: run_matches == Some(false),
        foreign_agent: agent_matches == Some(false),
        ownership_proven: run_owned || request.independent_ownership_evidence,
    }
}

fn operation_matches(request: &PendingCommitCheckpointRequestWire) -> bool {
    let checkpoint_method =
        clean_optional(request.checkpoint_method.as_deref());
    let accepted_action = clean_optional(request.accepted_action.as_deref());
    match (checkpoint_method, accepted_action) {
        (Some("create_commit"), Some("commit")) => true,
        (None, None) => request.payload_matches,
        _ => false,
    }
}

fn payload_matches(request: &PendingCommitCheckpointRequestWire) -> bool {
    let checkpoint_payload =
        clean_optional(request.checkpoint_payload_identity.as_deref());
    let accepted_payload =
        clean_optional(request.accepted_payload_identity.as_deref());
    match (checkpoint_payload, accepted_payload) {
        (Some(checkpoint), Some(accepted)) => checkpoint == accepted,
        (None, None) => request.subject_matches && request.payload_matches,
        _ => false,
    }
}

fn clean_optional(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|text| !text.is_empty())
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
            checkpoint_method: Some("create_commit".to_string()),
            accepted_action: Some("commit".to_string()),
            checkpoint_payload_identity: Some(
                "fix(final): reconcile commit declaration\n\nbody".to_string(),
            ),
            accepted_payload_identity: Some(
                "fix(final): reconcile commit declaration\n\nbody".to_string(),
            ),
            checkpoint_run_id: Some("run-1".to_string()),
            current_run_id: Some("run-1".to_string()),
            checkpoint_agent_id: Some("agent-1".to_string()),
            current_agent_id: Some("agent-1".to_string()),
            has_operation_id: true,
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
        req.checkpoint_payload_identity = Some("fix(final): old".to_string());
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
    }

    #[test]
    fn same_subject_different_body_fails() {
        let mut req = owned_pending();
        req.checkpoint_payload_identity =
            Some("fix(final): reconcile commit declaration\n\nold".to_string());
        req.accepted_payload_identity =
            Some("fix(final): reconcile commit declaration\n\nnew".to_string());
        req.subject_matches = true;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision
            .diagnostics
            .contains(&"checkpoint_payload_mismatch".to_string()));
    }

    #[test]
    fn foreign_run_fails() {
        let mut req = owned_pending();
        req.checkpoint_run_id = Some("run-2".to_string());
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision
            .diagnostics
            .contains(&"checkpoint_run_mismatch".to_string()));
    }

    #[test]
    fn foreign_agent_fails() {
        let mut req = owned_pending();
        req.checkpoint_agent_id = Some("agent-2".to_string());
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision
            .diagnostics
            .contains(&"checkpoint_agent_mismatch".to_string()));
    }

    #[test]
    fn missing_current_identity_fails() {
        let mut req = owned_pending();
        req.current_run_id = None;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_FAIL);
        assert!(decision
            .diagnostics
            .contains(&"missing_current_identity".to_string()));
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
    fn legacy_with_independent_proof_resumes() {
        let mut req = owned_pending();
        req.has_operation_id = false;
        req.checkpoint_run_id = None;
        req.checkpoint_agent_id = None;
        req.independent_ownership_evidence = true;
        let decision = decide_pending_commit_checkpoint_recovery(&req);
        assert_eq!(decision.action, PENDING_COMMIT_CHECKPOINT_ACTION_RESUME);
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
