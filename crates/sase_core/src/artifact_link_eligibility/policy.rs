//! Deterministic eligibility policy: given host-verified change evidence,
//! decide whether a run may publish its pending automatic artifact links,
//! and validate that previously recorded release evidence still belongs to
//! the run trying to use it.

use thiserror::Error;

use super::wire::{
    ArtifactLinkChangeRoleWire, ArtifactLinkEligibilityDecisionWire,
    ArtifactLinkEligibilityRequestWire, ArtifactLinkReleaseEvidenceWire,
    ARTIFACT_LINK_ELIGIBILITY_ID_MAX_LEN,
    ARTIFACT_LINK_ELIGIBILITY_LIST_MAX_LEN,
    ARTIFACT_LINK_ELIGIBILITY_TEXT_MAX_CHARS,
    ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{0}")]
pub struct ArtifactLinkEligibilityError(String);

impl ArtifactLinkEligibilityError {
    fn validation(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

fn validate_schema(actual: u64) -> Result<(), ArtifactLinkEligibilityError> {
    if actual == ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ArtifactLinkEligibilityError::validation(format!(
            "unsupported artifact-link eligibility schema_version {actual}; expected {ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION}"
        )))
    }
}

fn validate_required_id(
    value: &str,
    field: &str,
) -> Result<(), ArtifactLinkEligibilityError> {
    let chars = value.chars().count();
    if !value.trim().is_empty() && chars <= ARTIFACT_LINK_ELIGIBILITY_ID_MAX_LEN
    {
        Ok(())
    } else {
        Err(ArtifactLinkEligibilityError::validation(format!(
            "{field} must be nonblank and at most {ARTIFACT_LINK_ELIGIBILITY_ID_MAX_LEN} characters"
        )))
    }
}

fn validate_list_len(
    len: usize,
    field: &str,
) -> Result<(), ArtifactLinkEligibilityError> {
    if len <= ARTIFACT_LINK_ELIGIBILITY_LIST_MAX_LEN {
        Ok(())
    } else {
        Err(ArtifactLinkEligibilityError::validation(format!(
            "{field} has {len} entries; maximum is {ARTIFACT_LINK_ELIGIBILITY_LIST_MAX_LEN}"
        )))
    }
}

/// Decide whether *request* has at least one repository with a real,
/// non-bookkeeping change, and therefore qualifies its run to publish its
/// pending automatic artifact links.
pub fn decide_artifact_link_eligibility(
    request: &ArtifactLinkEligibilityRequestWire,
) -> Result<ArtifactLinkEligibilityDecisionWire, ArtifactLinkEligibilityError> {
    validate_schema(request.schema_version)?;
    validate_required_id(&request.run_id, "run_id")?;
    validate_required_id(&request.agent_id, "agent_id")?;
    validate_list_len(request.repos.len(), "repos")?;
    for repo in &request.repos {
        validate_required_id(&repo.repo_id, "repo_id")?;
        validate_list_len(repo.changed_paths.len(), "changed_paths")?;
        for changed in &repo.changed_paths {
            if changed.path.chars().count()
                > ARTIFACT_LINK_ELIGIBILITY_TEXT_MAX_CHARS
            {
                return Err(ArtifactLinkEligibilityError::validation(
                    "changed path is too long",
                ));
            }
        }
    }

    let qualifying_repo_ids: Vec<String> = request
        .repos
        .iter()
        .filter(|repo| {
            repo.changed_paths
                .iter()
                .any(|changed| changed.role == ArtifactLinkChangeRoleWire::Real)
        })
        .map(|repo| repo.repo_id.clone())
        .collect();

    Ok(ArtifactLinkEligibilityDecisionWire {
        schema_version: ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        agent_id: request.agent_id.clone(),
        eligible: !qualifying_repo_ids.is_empty(),
        qualifying_repo_ids,
    })
}

/// Build the durable release-evidence record for an eligible *decision*.
///
/// Refuses an ineligible decision: evidence is only ever recorded for a run
/// the host has already verified made a qualifying change.
pub fn artifact_link_release_evidence(
    decision: &ArtifactLinkEligibilityDecisionWire,
    recorded_at: &str,
) -> Result<ArtifactLinkReleaseEvidenceWire, ArtifactLinkEligibilityError> {
    validate_schema(decision.schema_version)?;
    if !decision.eligible || decision.qualifying_repo_ids.is_empty() {
        return Err(ArtifactLinkEligibilityError::validation(
            "cannot record release evidence for an ineligible decision",
        ));
    }
    validate_required_id(recorded_at, "recorded_at")?;
    Ok(ArtifactLinkReleaseEvidenceWire {
        schema_version: ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        run_id: decision.run_id.clone(),
        agent_id: decision.agent_id.clone(),
        qualifying_repo_ids: decision.qualifying_repo_ids.clone(),
        recorded_at: recorded_at.to_string(),
    })
}

/// Confirm *evidence* is still bound to the run trying to use it.
///
/// A different run -- even one from the same agent family -- must not
/// borrow another run's release evidence, so both `run_id` and `agent_id`
/// must match exactly.
pub fn validate_artifact_link_release_evidence(
    evidence: &ArtifactLinkReleaseEvidenceWire,
    expected_run_id: &str,
    expected_agent_id: &str,
) -> Result<(), ArtifactLinkEligibilityError> {
    validate_schema(evidence.schema_version)?;
    if evidence.run_id != expected_run_id {
        return Err(ArtifactLinkEligibilityError::validation(
            "release evidence run_id does not match",
        ));
    }
    if evidence.agent_id != expected_agent_id {
        return Err(ArtifactLinkEligibilityError::validation(
            "release evidence agent_id does not match",
        ));
    }
    if evidence.qualifying_repo_ids.is_empty() {
        return Err(ArtifactLinkEligibilityError::validation(
            "release evidence has no qualifying repositories",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::wire::{
        ArtifactLinkChangedPathWire, ArtifactLinkRepoEvidenceWire,
    };
    use super::*;

    fn request(
        repos: Vec<ArtifactLinkRepoEvidenceWire>,
    ) -> ArtifactLinkEligibilityRequestWire {
        ArtifactLinkEligibilityRequestWire {
            schema_version: ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            agent_id: "agent-1".to_string(),
            repos,
        }
    }

    fn bookkeeping_repo(repo_id: &str) -> ArtifactLinkRepoEvidenceWire {
        ArtifactLinkRepoEvidenceWire {
            repo_id: repo_id.to_string(),
            kind: "sdd".to_string(),
            changed_paths: vec![ArtifactLinkChangedPathWire {
                path: "links/plan/foo.md.json".to_string(),
                role: ArtifactLinkChangeRoleWire::Bookkeeping,
            }],
        }
    }

    fn real_repo(repo_id: &str) -> ArtifactLinkRepoEvidenceWire {
        ArtifactLinkRepoEvidenceWire {
            repo_id: repo_id.to_string(),
            kind: "main".to_string(),
            changed_paths: vec![ArtifactLinkChangedPathWire {
                path: "src/lib.rs".to_string(),
                role: ArtifactLinkChangeRoleWire::Real,
            }],
        }
    }

    #[test]
    fn bookkeeping_only_repos_are_ineligible() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![bookkeeping_repo(
                "sdd:plan",
            )]))
            .unwrap();
        assert!(!decision.eligible);
        assert!(decision.qualifying_repo_ids.is_empty());
    }

    #[test]
    fn a_real_change_makes_its_repo_qualify() {
        let decision = decide_artifact_link_eligibility(&request(vec![
            bookkeeping_repo("sdd:plan"),
            real_repo("main"),
        ]))
        .unwrap();
        assert!(decision.eligible);
        assert_eq!(decision.qualifying_repo_ids, vec!["main".to_string()]);
    }

    #[test]
    fn empty_repos_are_ineligible() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![])).unwrap();
        assert!(!decision.eligible);
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let mut req = request(vec![real_repo("main")]);
        req.schema_version = 999;
        let err = decide_artifact_link_eligibility(&req).unwrap_err();
        assert!(err.to_string().contains("schema_version"));
    }

    #[test]
    fn rejects_blank_run_id() {
        let mut req = request(vec![real_repo("main")]);
        req.run_id = "  ".to_string();
        let err = decide_artifact_link_eligibility(&req).unwrap_err();
        assert!(err.to_string().contains("run_id"));
    }

    #[test]
    fn release_evidence_requires_eligible_decision() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![bookkeeping_repo(
                "sdd:plan",
            )]))
            .unwrap();
        let err =
            artifact_link_release_evidence(&decision, "2026-09-08T00:00:00Z")
                .unwrap_err();
        assert!(err.to_string().contains("ineligible"));
    }

    #[test]
    fn release_evidence_round_trips_and_validates_against_the_same_run() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![real_repo("main")]))
                .unwrap();
        let evidence =
            artifact_link_release_evidence(&decision, "2026-09-08T00:00:00Z")
                .unwrap();
        validate_artifact_link_release_evidence(&evidence, "run-1", "agent-1")
            .unwrap();
    }

    #[test]
    fn release_evidence_rejects_a_different_run_id() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![real_repo("main")]))
                .unwrap();
        let evidence =
            artifact_link_release_evidence(&decision, "2026-09-08T00:00:00Z")
                .unwrap();
        let err = validate_artifact_link_release_evidence(
            &evidence, "run-2", "agent-1",
        )
        .unwrap_err();
        assert!(err.to_string().contains("run_id"));
    }

    #[test]
    fn release_evidence_rejects_a_different_agent_in_the_same_family() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![real_repo("main")]))
                .unwrap();
        let evidence =
            artifact_link_release_evidence(&decision, "2026-09-08T00:00:00Z")
                .unwrap();
        let err = validate_artifact_link_release_evidence(
            &evidence, "run-1", "agent-2",
        )
        .unwrap_err();
        assert!(err.to_string().contains("agent_id"));
    }

    #[test]
    fn serde_round_trips_the_decision_wire() {
        let decision =
            decide_artifact_link_eligibility(&request(vec![real_repo("main")]))
                .unwrap();
        let value = serde_json::to_value(&decision).unwrap();
        let restored: ArtifactLinkEligibilityDecisionWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(decision, restored);
    }

    #[test]
    fn serde_rejects_unknown_change_role() {
        let value = serde_json::json!({"path": "x", "role": "make_believe"});
        let err = serde_json::from_value::<ArtifactLinkChangedPathWire>(value)
            .unwrap_err();
        assert!(
            err.to_string().contains("make_believe")
                || err.to_string().contains("unknown")
        );
    }

    #[test]
    fn serde_rejects_unknown_fields() {
        let value = serde_json::json!({
            "schema_version": ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
            "run_id": "run-1",
            "agent_id": "agent-1",
            "repos": [],
            "surprise": true,
        });
        let err =
            serde_json::from_value::<ArtifactLinkEligibilityRequestWire>(value)
                .unwrap_err();
        assert!(
            err.to_string().contains("surprise")
                || err.to_string().contains("unknown")
        );
    }
}
