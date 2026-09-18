//! Remaining-work selection after a successful conflict-repair handoff.
//!
//! The host supplies already-validated declaration identities, current
//! repository obligation facts, and completed-obligation evidence. This
//! module returns remaining obligation IDs in host order or a typed
//! diagnostic. It performs no filesystem, subprocess, or stitch I/O, and
//! it never consumes execution paths from a declaration.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::selection::{
    validate_digest, validate_list_len, validate_optional_digest,
    validate_required_text,
};
use super::FinalizerError;

pub const REPAIR_HANDOFF_CODE_IDENTITY_MISMATCH: &str =
    "repair_handoff_identity_mismatch";
pub const REPAIR_HANDOFF_CODE_MISSING_HOST_IDENTITY: &str =
    "repair_handoff_missing_host_identity";
pub const REPAIR_HANDOFF_CODE_HOST_IDENTITY_MISMATCH: &str =
    "repair_handoff_host_identity_mismatch";
pub const REPAIR_HANDOFF_CODE_MISSING_DECISION: &str =
    "repair_handoff_missing_decision";
pub const REPAIR_HANDOFF_CODE_STALE_DIGEST: &str =
    "repair_handoff_stale_digest";
pub const REPAIR_HANDOFF_CODE_MISSING_COMPLETED_PROOF: &str =
    "repair_handoff_missing_completed_proof";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemainingCommitWorkRequestWire {
    pub current_run_id: String,
    pub current_agent_id: String,
    pub current_turn_nonce: String,
    pub current_plan_digest: String,
    pub declaration_run_id: String,
    pub declaration_agent_id: String,
    pub declaration_turn_nonce: String,
    pub declaration_plan_digest: String,
    #[serde(default)]
    pub current_obligations: Vec<RemainingCommitObligationFactWire>,
    #[serde(default)]
    pub executed_obligations: Vec<ExecutedCommitObligationFactWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemainingCommitObligationFactWire {
    pub obligation_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_digest: Option<String>,
    pub has_host_identity: bool,
    pub host_identity_matches: bool,
    pub has_valid_decision: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutedCommitObligationFactWire {
    pub obligation_id: String,
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RemainingCommitWorkOutcomeWire {
    Remaining { obligation_ids: Vec<String> },
    Rejected { code: String, message: String },
}

fn rejected(
    code: &'static str,
    message: impl Into<String>,
) -> RemainingCommitWorkOutcomeWire {
    RemainingCommitWorkOutcomeWire::Rejected {
        code: code.to_string(),
        message: message.into(),
    }
}

fn remaining(obligation_ids: Vec<String>) -> RemainingCommitWorkOutcomeWire {
    RemainingCommitWorkOutcomeWire::Remaining { obligation_ids }
}

/// Select remaining repository obligations after conflict repair.
///
/// Completed executed obligations may be omitted from the refreshed
/// snapshot. Dirty obligations are returned in the host-supplied current
/// order. A repair declaration's identities must match the in-flight run,
/// agent, turn, and plan.
pub fn select_remaining_commit_obligations(
    request: &RemainingCommitWorkRequestWire,
) -> RemainingCommitWorkOutcomeWire {
    match select_remaining_commit_obligations_inner(request) {
        Ok(outcome) => outcome,
        Err(error) => {
            rejected(REPAIR_HANDOFF_CODE_IDENTITY_MISMATCH, error.to_string())
        }
    }
}

fn select_remaining_commit_obligations_inner(
    request: &RemainingCommitWorkRequestWire,
) -> Result<RemainingCommitWorkOutcomeWire, FinalizerError> {
    validate_required_text(&request.current_run_id, "current_run_id")?;
    validate_required_text(&request.current_agent_id, "current_agent_id")?;
    validate_required_text(&request.current_turn_nonce, "current_turn_nonce")?;
    validate_digest(&request.current_plan_digest, "current_plan_digest")?;
    validate_required_text(&request.declaration_run_id, "declaration_run_id")?;
    validate_required_text(
        &request.declaration_agent_id,
        "declaration_agent_id",
    )?;
    validate_required_text(
        &request.declaration_turn_nonce,
        "declaration_turn_nonce",
    )?;
    validate_digest(
        &request.declaration_plan_digest,
        "declaration_plan_digest",
    )?;
    validate_list_len(
        request.current_obligations.len(),
        "current_obligations",
    )?;
    validate_list_len(
        request.executed_obligations.len(),
        "executed_obligations",
    )?;

    if request.current_run_id != request.declaration_run_id
        || request.current_agent_id != request.declaration_agent_id
        || request.current_turn_nonce != request.declaration_turn_nonce
        || request.current_plan_digest != request.declaration_plan_digest
    {
        return Ok(rejected(
            REPAIR_HANDOFF_CODE_IDENTITY_MISMATCH,
            "repair declaration is bound to a different run, agent, turn, or plan",
        ));
    }

    let mut completed = BTreeSet::new();
    let mut executed_ids = BTreeSet::new();
    for executed in &request.executed_obligations {
        validate_required_text(
            &executed.obligation_id,
            "executed.obligation_id",
        )?;
        if !executed_ids.insert(executed.obligation_id.as_str()) {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_MISSING_COMPLETED_PROOF,
                format!(
                    "duplicate executed obligation '{}'",
                    executed.obligation_id
                ),
            ));
        }
        if executed.completed {
            completed.insert(executed.obligation_id.as_str());
        }
        if let Some(sha) = &executed.commit_sha {
            validate_required_text(sha, "executed.commit_sha")?;
        }
    }

    let mut seen_current = BTreeSet::new();
    let mut remaining_ids = Vec::new();
    for fact in &request.current_obligations {
        validate_required_text(&fact.obligation_id, "current.obligation_id")?;
        validate_required_text(&fact.kind, "current.kind")?;
        validate_optional_digest(
            fact.current_digest.as_deref(),
            "current_digest",
        )?;
        validate_optional_digest(
            fact.submitted_digest.as_deref(),
            "submitted_digest",
        )?;
        if !seen_current.insert(fact.obligation_id.as_str()) {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_MISSING_DECISION,
                format!(
                    "duplicate current obligation '{}'",
                    fact.obligation_id
                ),
            ));
        }
        if fact.kind != "repository" {
            continue;
        }
        if completed.contains(fact.obligation_id.as_str()) {
            continue;
        }
        if !fact.has_host_identity {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_MISSING_HOST_IDENTITY,
                format!(
                    "missing host identity for repository obligation {}",
                    fact.obligation_id
                ),
            ));
        }
        if !fact.host_identity_matches {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_HOST_IDENTITY_MISMATCH,
                format!(
                    "host identity does not match repository obligation {}",
                    fact.obligation_id
                ),
            ));
        }
        if !fact.has_valid_decision {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_MISSING_DECISION,
                format!(
                    "dirty repository obligation {} has no valid current decision",
                    fact.obligation_id
                ),
            ));
        }
        if let (Some(current), Some(submitted)) =
            (&fact.current_digest, &fact.submitted_digest)
        {
            if current != submitted {
                return Ok(rejected(
                    REPAIR_HANDOFF_CODE_STALE_DIGEST,
                    format!(
                        "repository obligation {} changed after submit",
                        fact.obligation_id
                    ),
                ));
            }
        } else if fact.submitted_digest.is_some()
            && fact.current_digest.is_none()
        {
            return Ok(rejected(
                REPAIR_HANDOFF_CODE_STALE_DIGEST,
                format!(
                    "repository obligation {} changed after submit",
                    fact.obligation_id
                ),
            ));
        }
        remaining_ids.push(fact.obligation_id.clone());
    }

    for executed in &request.executed_obligations {
        if executed.completed {
            continue;
        }
        if seen_current.contains(executed.obligation_id.as_str()) {
            continue;
        }
        return Ok(rejected(
            REPAIR_HANDOFF_CODE_MISSING_COMPLETED_PROOF,
            format!(
                "executed obligation {} was omitted without completion proof",
                executed.obligation_id
            ),
        ));
    }

    Ok(remaining(remaining_ids))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAN: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_PLAN: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OTHER_DIGEST: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn request(
        current: Vec<RemainingCommitObligationFactWire>,
        executed: Vec<ExecutedCommitObligationFactWire>,
    ) -> RemainingCommitWorkRequestWire {
        RemainingCommitWorkRequestWire {
            current_run_id: "run-1".into(),
            current_agent_id: "agent-1".into(),
            current_turn_nonce: "nonce-1".into(),
            current_plan_digest: PLAN.into(),
            declaration_run_id: "run-1".into(),
            declaration_agent_id: "agent-1".into(),
            declaration_turn_nonce: "nonce-1".into(),
            declaration_plan_digest: PLAN.into(),
            current_obligations: current,
            executed_obligations: executed,
        }
    }

    fn repo(
        id: &str,
        digest: Option<&str>,
    ) -> RemainingCommitObligationFactWire {
        RemainingCommitObligationFactWire {
            obligation_id: id.into(),
            kind: "repository".into(),
            current_digest: digest.map(str::to_string),
            submitted_digest: digest.map(str::to_string),
            has_host_identity: true,
            host_identity_matches: true,
            has_valid_decision: true,
        }
    }

    fn completed(id: &str) -> ExecutedCommitObligationFactWire {
        ExecutedCommitObligationFactWire {
            obligation_id: id.into(),
            completed: true,
            commit_sha: Some("a".repeat(40)),
        }
    }

    #[test]
    fn selects_new_and_queued_remaining_in_host_order() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![repo("linked", Some(DIGEST)), repo("research", Some(DIGEST))],
            vec![completed("main")],
        ));
        assert_eq!(
            outcome,
            remaining(vec!["linked".into(), "research".into()])
        );
    }

    #[test]
    fn skips_completed_obligations_still_listed_in_current() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![repo("main", Some(DIGEST)), repo("linked", Some(DIGEST))],
            vec![completed("main")],
        ));
        assert_eq!(outcome, remaining(vec!["linked".into()]));
    }

    #[test]
    fn preserves_completed_obligations_omitted_from_current() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![repo("linked", Some(DIGEST))],
            vec![completed("main")],
        ));
        assert_eq!(outcome, remaining(vec!["linked".into()]));
    }

    #[test]
    fn empty_remaining_when_everything_completed() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![],
            vec![completed("main")],
        ));
        assert_eq!(outcome, remaining(vec![]));
    }

    #[test]
    fn rejects_identity_mismatch() {
        let mut payload = request(vec![repo("linked", Some(DIGEST))], vec![]);
        payload.declaration_turn_nonce = "other-turn".into();
        let outcome = select_remaining_commit_obligations(&payload);
        assert_eq!(
            outcome,
            rejected(
                REPAIR_HANDOFF_CODE_IDENTITY_MISMATCH,
                "repair declaration is bound to a different run, agent, turn, or plan",
            )
        );

        payload = request(vec![repo("linked", Some(DIGEST))], vec![]);
        payload.declaration_plan_digest = OTHER_PLAN.into();
        let outcome = select_remaining_commit_obligations(&payload);
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_IDENTITY_MISMATCH);
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_and_mismatched_host_identity() {
        let mut fact = repo("linked", Some(DIGEST));
        fact.has_host_identity = false;
        let outcome =
            select_remaining_commit_obligations(&request(vec![fact], vec![]));
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_MISSING_HOST_IDENTITY);
            }
            other => panic!("expected rejection, got {other:?}"),
        }

        let mut fact = repo("linked", Some(DIGEST));
        fact.host_identity_matches = false;
        let outcome =
            select_remaining_commit_obligations(&request(vec![fact], vec![]));
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_HOST_IDENTITY_MISMATCH);
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_decision_and_stale_digest() {
        let mut fact = repo("linked", Some(DIGEST));
        fact.has_valid_decision = false;
        let outcome =
            select_remaining_commit_obligations(&request(vec![fact], vec![]));
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, message } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_MISSING_DECISION);
                assert!(message.contains("linked"));
            }
            other => panic!("expected rejection, got {other:?}"),
        }

        let mut fact = repo("linked", Some(DIGEST));
        fact.current_digest = Some(OTHER_DIGEST.into());
        let outcome =
            select_remaining_commit_obligations(&request(vec![fact], vec![]));
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_STALE_DIGEST);
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_incomplete_executed_obligation_omitted_from_current() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![repo("linked", Some(DIGEST))],
            vec![ExecutedCommitObligationFactWire {
                obligation_id: "main".into(),
                completed: false,
                commit_sha: None,
            }],
        ));
        match outcome {
            RemainingCommitWorkOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPAIR_HANDOFF_CODE_MISSING_COMPLETED_PROOF);
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn host_order_is_the_current_obligation_order() {
        let outcome = select_remaining_commit_obligations(&request(
            vec![
                repo("research", Some(DIGEST)),
                repo("linked", Some(DIGEST)),
                repo("external", Some(DIGEST)),
            ],
            vec![],
        ));
        assert_eq!(
            outcome,
            remaining(vec![
                "research".into(),
                "linked".into(),
                "external".into()
            ])
        );
    }
}
