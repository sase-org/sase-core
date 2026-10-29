//! Prepared-starter coverage: does a sealed intent satisfy the starter's
//! declaration obligation for one staged monitor?
//!
//! A valid prepared intent may stay in `prepared` lifecycle state while a
//! host-owned reservation associates it with a staged ID and its intended
//! monitor. Binding at staging time is forbidden; the outer monitor-staging
//! phase creates the reservation before publishing the staged record and
//! binds at spawn time. This predicate never launches, reserves, or binds —
//! it only authenticates host-observed facts the adapter supplies.
//!
//! Adapter contract for the future monitor-staging path (prepared-bridge
//! phase documents the same contract on `sase-1pj.2`):
//! - Seal the intent with `_prepare_intents.py`; keep it `prepared`.
//! - Persist a host-owned reservation `{staged_id, monitor_id,
//!   request_fingerprint, command}` before publishing the staged record.
//! - At spawn, bind the intent to that monitor via
//!   `continuation_bind_conditional_completion`.
//! - The starter calls this predicate with the validated intent, the staged
//!   identity/kind, the reservation (when the intent is still `prepared`),
//!   the starter identity, the intended monitor identity/command/
//!   fingerprint, live repository observations, the current plan digest, and
//!   the current obligation IDs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::completion::{
    validate_conditional_completion_intent, worktree_fingerprint,
    ConditionalCompletionIntentWire, ConditionalCompletionStatusWire,
    RepositoryObservationWire,
};
use super::schema::{
    validate_command_part, validate_identifier, validate_non_empty_text,
    validate_schema, validate_sha256, ContinuationError,
    CONTINUATION_WIRE_SCHEMA_VERSION, MAX_COMMAND_PARTS, MAX_REF_BYTES,
};

/// Host-owned reservation associating a prepared intent with one staged
/// monitor. Persisted before the staged record is published; binding happens
/// at spawn time, never at staging time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedReservationWire {
    pub staged_id: String,
    pub monitor_id: String,
    pub request_fingerprint: String,
    pub command: Vec<String>,
}

/// Typed request to check prepared-starter coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStarterCoverageRequestWire {
    pub schema_version: u32,
    pub intent: ConditionalCompletionIntentWire,
    pub staged_id: String,
    pub staged_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<PreparedReservationWire>,
    pub starter_run_id: String,
    pub starter_agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starter_workspace_id: Option<String>,
    pub monitor_id: String,
    pub command: Vec<String>,
    pub request_fingerprint: String,
    #[serde(default)]
    pub current_observations: Vec<RepositoryObservationWire>,
    pub current_plan_digest: String,
    #[serde(default)]
    pub current_obligation_ids: Vec<String>,
}

/// Typed prepared-starter coverage decision.
///
/// `eligible` is true only when every check passes. `covered_repository_ids`
/// names the sealed repositories that satisfy current obligations even when
/// the decision is refused (for example partial coverage), so callers never
/// reimplement the coverage computation. `reason` is the first refusal code;
/// `reasons` carries every code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStarterCoverageDecisionWire {
    pub schema_version: u32,
    pub eligible: bool,
    #[serde(default)]
    pub covered_repository_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// Evaluate whether a sealed intent covers the starter's obligation.
pub fn evaluate_prepared_starter_coverage(
    request: PreparedStarterCoverageRequestWire,
) -> Result<PreparedStarterCoverageDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "PreparedStarterCoverageRequestWire",
    )?;
    validate_uuid(&request.staged_id, "staged_id")?;
    validate_non_empty_text(
        &request.staged_kind,
        "staged_kind",
        MAX_REF_BYTES,
    )?;
    validate_non_empty_text(
        &request.starter_run_id,
        "starter_run_id",
        MAX_REF_BYTES,
    )?;
    validate_non_empty_text(
        &request.starter_agent_id,
        "starter_agent_id",
        MAX_REF_BYTES,
    )?;
    if let Some(workspace) = &request.starter_workspace_id {
        validate_non_empty_text(
            workspace,
            "starter_workspace_id",
            MAX_REF_BYTES,
        )?;
    }
    validate_identifier(&request.monitor_id, "monitor_id")?;
    validate_identifier(&request.request_fingerprint, "request_fingerprint")?;
    validate_command(&request.command, "command")?;
    validate_sha256(&request.current_plan_digest, "current_plan_digest")?;
    if let Some(reservation) = &request.reservation {
        validate_uuid(&reservation.staged_id, "reservation.staged_id")?;
        validate_identifier(&reservation.monitor_id, "reservation.monitor_id")?;
        validate_identifier(
            &reservation.request_fingerprint,
            "reservation.request_fingerprint",
        )?;
        validate_command(&reservation.command, "reservation.command")?;
    }

    let mut reasons: Vec<String> = Vec::new();

    if request.staged_kind != "monitor" {
        reasons.push("prepared_requires_monitor_handoff".to_string());
    }

    let intent = match validate_conditional_completion_intent(request.intent) {
        Ok(intent) => Some(intent),
        Err(error) => {
            reasons.push("invalid_prepared_intent".to_string());
            let _ = error;
            None
        }
    };

    // Compute sealed coverage even for malformed intents when possible, so
    // callers still learn which repositories are sealed.
    let sealed_ids: BTreeSet<String> = intent
        .as_ref()
        .map(|intent| {
            intent
                .repository_decisions
                .iter()
                .map(|decision| decision.repo_id.clone())
                .collect()
        })
        .unwrap_or_default();
    let current_ids: BTreeSet<String> =
        request.current_obligation_ids.iter().cloned().collect();
    let mut covered: Vec<String> =
        sealed_ids.intersection(&current_ids).cloned().collect();
    covered.sort();

    if let Some(intent) = &intent {
        match intent.status {
            ConditionalCompletionStatusWire::Consumed => {
                reasons.push("prepared_intent_consumed".to_string());
            }
            ConditionalCompletionStatusWire::Invalidated => {
                reasons.push("prepared_intent_invalidated".to_string());
            }
            ConditionalCompletionStatusWire::Prepared => {
                match &request.reservation {
                    None => {
                        reasons.push(
                            "prepared_intent_requires_reservation".to_string(),
                        );
                    }
                    Some(reservation) => {
                        if reservation.staged_id != request.staged_id {
                            reasons.push(
                                "reservation_staged_id_mismatch".to_string(),
                            );
                        }
                        if reservation.monitor_id != request.monitor_id {
                            reasons.push(
                                "reservation_monitor_mismatch".to_string(),
                            );
                        }
                        if reservation.request_fingerprint
                            != request.request_fingerprint
                        {
                            reasons.push(
                                "reservation_fingerprint_mismatch".to_string(),
                            );
                        }
                        if reservation.command != request.command {
                            reasons.push(
                                "reservation_command_mismatch".to_string(),
                            );
                        }
                    }
                }
            }
            ConditionalCompletionStatusWire::Bound => {
                if let Some(bound_monitor) = &intent.binding.monitor_id {
                    if *bound_monitor != request.monitor_id {
                        reasons.push("bound_monitor_mismatch".to_string());
                    }
                } else {
                    reasons.push("bound_intent_missing_monitor".to_string());
                }
                if let Some(bound_fingerprint) =
                    &intent.binding.request_fingerprint
                {
                    if *bound_fingerprint != request.request_fingerprint {
                        reasons.push("bound_fingerprint_mismatch".to_string());
                    }
                } else {
                    reasons
                        .push("bound_intent_missing_fingerprint".to_string());
                }
                if let Some(bound_command) = &intent.binding.bound_command {
                    if *bound_command != request.command {
                        reasons.push("bound_command_mismatch".to_string());
                    }
                } else {
                    reasons.push("bound_intent_missing_command".to_string());
                }
                if let Some(reservation) = &request.reservation {
                    if reservation.staged_id != request.staged_id {
                        reasons
                            .push("reservation_staged_id_mismatch".to_string());
                    }
                    if reservation.monitor_id != request.monitor_id {
                        reasons
                            .push("reservation_monitor_mismatch".to_string());
                    }
                }
            }
        }

        if intent.seal.creator.run_id != request.starter_run_id {
            reasons.push("prepared_creator_run_mismatch".to_string());
        }
        if intent.seal.creator.agent_name != request.starter_agent_id {
            reasons.push("prepared_creator_agent_mismatch".to_string());
        }
        if let (Some(creator_workspace), Some(starter_workspace)) = (
            intent.seal.creator.workspace_id.as_deref(),
            request.starter_workspace_id.as_deref(),
        ) {
            if creator_workspace != starter_workspace {
                reasons.push("prepared_creator_workspace_mismatch".to_string());
            }
        }
        if request.command != intent.verification.command {
            reasons.push("prepared_command_mismatch".to_string());
        }
        if request.current_plan_digest != intent.seal.plan_digest {
            reasons.push("changed_finalizer_requirements".to_string());
        }
        match worktree_fingerprint(
            &request.current_observations,
            &intent.repository_decisions,
        ) {
            Ok(current) => {
                if current != intent.seal.worktree_fingerprint {
                    reasons.push("stale_observations".to_string());
                }
            }
            Err(_) => {
                reasons.push("unreadable_observations".to_string());
            }
        }
        for obligation_id in &request.current_obligation_ids {
            if !sealed_ids.contains(obligation_id) {
                reasons.push(format!("uncovered_repository:{obligation_id}"));
            }
        }
        if intent.repository_decisions.is_empty() {
            reasons.push("prepared_covers_no_repository".to_string());
        }
    } else {
        // Malformed intent: every current obligation is uncovered.
        for obligation_id in &request.current_obligation_ids {
            reasons.push(format!("uncovered_repository:{obligation_id}"));
        }
    }

    // A raw `prepared_intent_ref` without reservation/binding evidence never
    // satisfies: a prepared intent with no reservation, or a bound intent
    // with no binding fields, already records the precise refusal above.
    let eligible = reasons.is_empty();
    let reason = reasons.first().cloned();
    Ok(PreparedStarterCoverageDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        eligible,
        covered_repository_ids: covered,
        reason,
        reasons,
    })
}

pub(crate) fn validate_uuid(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    if is_uuid(value) {
        Ok(())
    } else {
        Err(ContinuationError::validation(format!(
            "{field} is not a UUID: {value:?}"
        )))
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    let hyphen_at = [8_usize, 13, 18, 23];
    bytes.iter().enumerate().all(|(index, byte)| {
        if hyphen_at.contains(&index) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}

fn validate_command(
    command: &[String],
    field: &str,
) -> Result<(), ContinuationError> {
    if command.is_empty() {
        return Err(ContinuationError::validation(format!(
            "{field} must contain at least one argv part"
        )));
    }
    if command.len() > MAX_COMMAND_PARTS {
        return Err(ContinuationError::validation(format!(
            "{field} has {} parts; maximum is {MAX_COMMAND_PARTS}",
            command.len()
        )));
    }
    for (index, part) in command.iter().enumerate() {
        validate_command_part(part, &format!("{field}[{index}]"))?;
    }
    Ok(())
}

#[allow(dead_code)]
fn _map_order_sentinel() -> BTreeMap<String, String> {
    BTreeMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    use crate::continuation::completion::{
        ConditionalCompletionContextWire,
        ConditionalCompletionPrepareRequestWire,
        ConditionalCompletionStatusWire,
    };
    use crate::continuation::schema::ContinuationExecutionIdentityWire;

    fn observation(repo_id: &str) -> RepositoryObservationWire {
        RepositoryObservationWire {
            repo_id: repo_id.to_string(),
            kind: "repository".to_string(),
            name: repo_id.to_string(),
            head: "head-1".to_string(),
            head_tree: "tree-1".to_string(),
            index_tree: "index-1".to_string(),
            paths: Vec::new(),
            complete: true,
        }
    }

    fn seal_request(
        creator: ContinuationExecutionIdentityWire,
        context: ConditionalCompletionContextWire,
        declaration: Value,
        observations: Vec<RepositoryObservationWire>,
    ) -> ConditionalCompletionPrepareRequestWire {
        ConditionalCompletionPrepareRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            creator,
            context,
            success_message: "done {duration}".to_string(),
            verification_command: vec!["just".to_string(), "check".to_string()],
            declaration,
            observations,
            executors: Vec::new(),
            accept: Default::default(),
            stage_source: None,
            prior_refusals: Vec::new(),
        }
    }

    fn prepared_intent() -> ConditionalCompletionIntentWire {
        let creator = ContinuationExecutionIdentityWire {
            project: "sase".to_string(),
            run_id: "run-1".to_string(),
            agent_name: "agent-1".to_string(),
            machine_name: None,
            workspace_id: Some("ws-1".to_string()),
        };
        let declaration = json!({
            "payloads": [{
                "instance_id": "commit",
                "payload": {"repositories": [{"repo_id": "repo:primary", "action": "commit", "message": "m"}]},
            }],
        });
        let context = ConditionalCompletionContextWire {
            run_id: "run-1".to_string(),
            agent_id: "agent-1".to_string(),
            turn_nonce: "nonce-1".to_string(),
            plan_digest: "a".repeat(64),
            context_digest: "b".repeat(64),
            obligation_ids: vec!["repo:primary".to_string()],
        };
        let observations = vec![observation("repo:primary")];
        let request = seal_request(creator, context, declaration, observations);
        let intent =
            crate::continuation::completion::seal_conditional_completion(
                request,
            )
            .unwrap();
        // Keep the sealed intent in `prepared` state for reservation tests.
        assert_eq!(intent.status, ConditionalCompletionStatusWire::Prepared);
        intent
    }

    fn base_request(
        intent: ConditionalCompletionIntentWire,
    ) -> PreparedStarterCoverageRequestWire {
        PreparedStarterCoverageRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            intent,
            staged_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            staged_kind: "monitor".to_string(),
            reservation: Some(PreparedReservationWire {
                staged_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
                monitor_id: "monitor-1".to_string(),
                request_fingerprint: "fp-1".to_string(),
                command: vec!["just".to_string(), "check".to_string()],
            }),
            starter_run_id: "run-1".to_string(),
            starter_agent_id: "agent-1".to_string(),
            starter_workspace_id: Some("ws-1".to_string()),
            monitor_id: "monitor-1".to_string(),
            command: vec!["just".to_string(), "check".to_string()],
            request_fingerprint: "fp-1".to_string(),
            current_observations: vec![observation("repo:primary")],
            current_plan_digest: "a".repeat(64),
            current_obligation_ids: vec!["repo:primary".to_string()],
        }
    }

    #[test]
    fn prepared_with_matching_reservation_is_eligible() {
        let intent = prepared_intent();
        let decision =
            evaluate_prepared_starter_coverage(base_request(intent)).unwrap();
        assert!(decision.eligible);
        assert_eq!(decision.covered_repository_ids, vec!["repo:primary"]);
    }

    #[test]
    fn prepared_without_reservation_is_refused() {
        let intent = prepared_intent();
        let mut request = base_request(intent);
        request.reservation = None;
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .contains(&"prepared_intent_requires_reservation".to_string()));
    }

    #[test]
    fn wrong_monitor_command_owner_are_refused() {
        let intent = prepared_intent();
        let mut request = base_request(intent);
        request.monitor_id = "monitor-2".to_string();
        let decision =
            evaluate_prepared_starter_coverage(request.clone()).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .contains(&"reservation_monitor_mismatch".to_string()));

        request = base_request(prepared_intent());
        request.command = vec!["just".to_string(), "check-full".to_string()];
        request.reservation.as_mut().unwrap().command =
            vec!["just".to_string(), "check-full".to_string()];
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .contains(&"prepared_command_mismatch".to_string()));

        let mut request = base_request(prepared_intent());
        request.starter_run_id = "run-2".to_string();
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .contains(&"prepared_creator_run_mismatch".to_string()));
    }

    #[test]
    fn consumed_and_invalidated_intents_are_refused() {
        for status in [
            ConditionalCompletionStatusWire::Consumed,
            ConditionalCompletionStatusWire::Invalidated,
        ] {
            let mut intent = prepared_intent();
            intent.status = status;
            // Re-seal is not needed: validation passes for status changes
            // because the seal still matches; the predicate must refuse.
            // Consumed/invalidated intents created by mutating a sealed
            // prepared intent keep a valid seal digest.
            let decision =
                evaluate_prepared_starter_coverage(base_request(intent))
                    .unwrap();
            assert!(!decision.eligible);
        }
    }

    #[test]
    fn non_monitor_kind_and_incomplete_coverage_are_refused() {
        let mut request = base_request(prepared_intent());
        request.staged_kind = "gate".to_string();
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .contains(&"prepared_requires_monitor_handoff".to_string()));

        let mut request = base_request(prepared_intent());
        request.current_obligation_ids =
            vec!["repo:primary".to_string(), "repo:extra".to_string()];
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "uncovered_repository:repo:extra"));
        assert_eq!(decision.covered_repository_ids, vec!["repo:primary"]);
    }

    #[test]
    fn stale_observations_are_refused() {
        let mut request = base_request(prepared_intent());
        let mut stale = observation("repo:primary");
        stale.head = "head-2".to_string();
        // Fingerprint input differs from the sealed observations.
        stale.head_tree = "tree-2".to_string();
        request.current_observations = vec![stale];
        let decision = evaluate_prepared_starter_coverage(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision.reasons.contains(&"stale_observations".to_string()));
    }
}
