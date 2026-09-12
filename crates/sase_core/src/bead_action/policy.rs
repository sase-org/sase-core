//! Deterministic bead-action policy.
//!
//! The host supplies already-collected facts. This module decides whether
//! a stitch may proceed, whether it may close the assigned bead, and
//! whether a finalizer repository decision matches the authenticated
//! assigned-bead context.

use serde_json::{Map, Value as JsonValue};

use super::wire::{
    assigned_bead_id, parse_bead_action_field, BeadActionDecisionWire,
    BeadActionDispositionWire, BeadActionError, BeadActionRequestWire,
    BeadActionStatusFactWire, BeadActionWire, BeadCommitMethodWire,
    BeadRepositoryScopeWire, FinalizerBeadDecisionWire,
    BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH, BEAD_ACTION_CODE_CLOSE_ON_PROPOSAL,
    BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY,
    BEAD_ACTION_CODE_CLOSE_WITHOUT_BEAD, BEAD_ACTION_CODE_INELIGIBLE_STATUS,
    BEAD_ACTION_CODE_INVALID, BEAD_ACTION_CODE_LEGACY,
    BEAD_ACTION_CODE_MISSING, BEAD_ACTION_CODE_UNREADABLE_STATUS,
    BEAD_ACTION_CODE_UNSUPPORTED_SCHEMA, BEAD_ACTION_USAGE,
    BEAD_ACTION_WIRE_SCHEMA_VERSION,
};
use crate::finalizer::wire::{FinalizerAssignedBeadWire, FinalizerContextWire};

fn validate_schema(actual: u64) -> Result<(), BeadActionError> {
    if actual == BEAD_ACTION_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(BeadActionError::new(
            BEAD_ACTION_CODE_UNSUPPORTED_SCHEMA,
            format!(
                "unsupported bead-action schema_version {actual}; expected {BEAD_ACTION_WIRE_SCHEMA_VERSION}"
            ),
        ))
    }
}

fn legacy_present(request: &BeadActionRequestWire) -> bool {
    request.legacy_do_not_close_bead
}

fn assigned_id(request: &BeadActionRequestWire) -> Option<String> {
    assigned_bead_id(request.assigned_bead_id.as_deref())
}

fn allow(
    disposition: BeadActionDispositionWire,
    bead_id: Option<String>,
    close_bead: bool,
    message: impl Into<String>,
) -> BeadActionDecisionWire {
    BeadActionDecisionWire {
        schema_version: BEAD_ACTION_WIRE_SCHEMA_VERSION,
        disposition,
        bead_id,
        close_bead,
        message: message.into(),
    }
}

/// Decide the stitch/commit disposition for one collected fact set.
pub fn decide_bead_action(
    request: &BeadActionRequestWire,
) -> Result<BeadActionDecisionWire, BeadActionError> {
    validate_schema(request.schema_version)?;
    if legacy_present(request) {
        return Err(BeadActionError::new(
            BEAD_ACTION_CODE_LEGACY,
            format!(
                "--do-not-close-bead is no longer accepted; {BEAD_ACTION_USAGE}"
            ),
        ));
    }

    let bead_id = assigned_id(request);
    match (bead_id, request.bead_action) {
        (None, None) => Ok(allow(
            BeadActionDispositionWire::Unassociated,
            None,
            false,
            "no assigned bead; existing commit behavior",
        )),
        (None, Some(BeadActionWire::Keep)) => Ok(allow(
            BeadActionDispositionWire::Unassociated,
            None,
            false,
            "keep is a no-op without an assigned bead",
        )),
        (None, Some(BeadActionWire::Close)) => Err(BeadActionError::new(
            BEAD_ACTION_CODE_CLOSE_WITHOUT_BEAD,
            "there is no assigned bead to close",
        )),
        (Some(_), None) => Err(BeadActionError::new(
            BEAD_ACTION_CODE_MISSING,
            format!(
                "bead_action is required when a bead is assigned; {BEAD_ACTION_USAGE}"
            ),
        )),
        (Some(bead_id), Some(BeadActionWire::Keep)) => Ok(allow(
            BeadActionDispositionWire::Keep,
            Some(bead_id),
            false,
            "commit; leave the assigned bead status unchanged",
        )),
        (Some(bead_id), Some(BeadActionWire::Close)) => {
            decide_close(request, bead_id)
        }
    }
}

fn decide_close(
    request: &BeadActionRequestWire,
    bead_id: String,
) -> Result<BeadActionDecisionWire, BeadActionError> {
    if request.commit_method == BeadCommitMethodWire::CreateProposal {
        return Err(BeadActionError::new(
            BEAD_ACTION_CODE_CLOSE_ON_PROPOSAL,
            format!(
                "proposals cannot complete the assigned bead; {BEAD_ACTION_USAGE}"
            ),
        ));
    }
    if !request.primary_repository_identified
        || request.repository_scope != BeadRepositoryScopeWire::Primary
    {
        return Err(BeadActionError::new(
            BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY,
            "only the owning primary repository may close the assigned bead",
        ));
    }
    match request.bead_status {
        Some(BeadActionStatusFactWire::InProgress) => Ok(allow(
            BeadActionDispositionWire::Close,
            Some(bead_id),
            true,
            "commit, then close the assigned bead",
        )),
        Some(BeadActionStatusFactWire::Closed) => Ok(allow(
            BeadActionDispositionWire::CloseIdempotent,
            Some(bead_id),
            false,
            "commit; the assigned bead is already closed (idempotent)",
        )),
        Some(BeadActionStatusFactWire::Other) => Err(BeadActionError::new(
            BEAD_ACTION_CODE_INELIGIBLE_STATUS,
            "close requires the assigned bead to be in_progress or closed",
        )),
        Some(BeadActionStatusFactWire::Unreadable)
        | Some(BeadActionStatusFactWire::Unchecked)
        | None => Err(BeadActionError::new(
            BEAD_ACTION_CODE_UNREADABLE_STATUS,
            "the assigned bead status could not be read; close is refused",
        )),
    }
}

/// Parse a JSON request, rejecting invalid `bead_action` values before
/// applying policy.
pub fn decide_bead_action_from_json(
    value: &JsonValue,
) -> Result<BeadActionDecisionWire, BeadActionError> {
    let action = parse_bead_action_field(value)?;
    let mut object = json_object(value)?;
    match action {
        Some(action) => {
            object.insert(
                "bead_action".to_string(),
                JsonValue::String(action.as_str().to_string()),
            );
        }
        None => {
            object.remove("bead_action");
        }
    }
    object.remove("do_not_close_bead");
    let request: BeadActionRequestWire =
        serde_json::from_value(JsonValue::Object(object)).map_err(|error| {
            BeadActionError::new(
                BEAD_ACTION_CODE_INVALID,
                format!("invalid bead-action request: {error}"),
            )
        })?;
    decide_bead_action(&request)
}

/// Confirm a previously accepted assigned-bead snapshot still matches the
/// current host-issued context. A different assignment must not reuse the
/// old declaration.
pub fn validate_finalizer_assigned_bead_binding(
    context: &FinalizerContextWire,
    expected: Option<&FinalizerAssignedBeadWire>,
) -> Result<(), BeadActionError> {
    if context.assigned_bead.as_ref() == expected {
        Ok(())
    } else {
        Err(BeadActionError::new(
            BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH,
            "finalizer declaration applies to a different assigned bead",
        ))
    }
}

/// Validate one repository decision against authenticated bead context.
pub fn validate_finalizer_bead_decision(
    context: &FinalizerContextWire,
    decision: &FinalizerBeadDecisionWire,
) -> Result<BeadActionDecisionWire, BeadActionError> {
    if decision.legacy_do_not_close_bead {
        return Err(BeadActionError::new(
            BEAD_ACTION_CODE_LEGACY,
            format!(
                "--do-not-close-bead is no longer accepted; {BEAD_ACTION_USAGE}"
            ),
        ));
    }
    if let Some(declared) = assigned_bead_id(decision.bead_id.as_deref()) {
        match context.assigned_bead.as_ref() {
            Some(assigned) if assigned.bead_id == declared => {}
            _ => {
                return Err(BeadActionError::new(
                    BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH,
                    "finalizer declaration applies to a different assigned bead",
                ));
            }
        }
    }

    let request = bead_action_request_for_decision(context, decision);
    decide_bead_action(&request)
}

/// Parse a JSON repository decision against a JSON context.
pub fn validate_finalizer_bead_decision_from_json(
    context: &JsonValue,
    decision: &JsonValue,
) -> Result<BeadActionDecisionWire, BeadActionError> {
    let action = parse_bead_action_field(decision)?;
    let mut object = json_object(decision)?;
    match action {
        Some(action) => {
            object.insert(
                "bead_action".to_string(),
                JsonValue::String(action.as_str().to_string()),
            );
        }
        None => {
            object.remove("bead_action");
        }
    }
    object.remove("do_not_close_bead");
    let decision: FinalizerBeadDecisionWire =
        serde_json::from_value(JsonValue::Object(object)).map_err(|error| {
            BeadActionError::new(
                BEAD_ACTION_CODE_INVALID,
                format!("invalid finalizer bead decision: {error}"),
            )
        })?;
    let context: FinalizerContextWire = serde_json::from_value(context.clone())
        .map_err(|error| {
            BeadActionError::new(
                BEAD_ACTION_CODE_INVALID,
                format!("invalid finalizer context: {error}"),
            )
        })?;
    validate_finalizer_bead_decision(&context, &decision)
}

fn json_object(
    value: &JsonValue,
) -> Result<Map<String, JsonValue>, BeadActionError> {
    value.as_object().cloned().ok_or_else(|| {
        BeadActionError::new(
            BEAD_ACTION_CODE_INVALID,
            format!(
                "bead-action payload must be an object; {BEAD_ACTION_USAGE}"
            ),
        )
    })
}

fn bead_action_request_for_decision(
    context: &FinalizerContextWire,
    decision: &FinalizerBeadDecisionWire,
) -> BeadActionRequestWire {
    let assigned = context.assigned_bead.as_ref();
    let (repository_scope, primary_repository_identified) = match assigned {
        None => (
            decision
                .repository_scope
                .unwrap_or(BeadRepositoryScopeWire::Primary),
            false,
        ),
        Some(assigned) => {
            let identified = primary_obligation_identified(
                context,
                assigned.primary_repo_obligation_id.as_deref(),
            );
            let is_this_primary = identified
                && assigned.primary_repo_obligation_id.as_deref()
                    == Some(decision.repo_id.as_str());
            if is_this_primary {
                (BeadRepositoryScopeWire::Primary, true)
            } else {
                (
                    decision
                        .repository_scope
                        .unwrap_or(BeadRepositoryScopeWire::Linked),
                    false,
                )
            }
        }
    };
    BeadActionRequestWire {
        schema_version: BEAD_ACTION_WIRE_SCHEMA_VERSION,
        assigned_bead_id: assigned.map(|assigned| assigned.bead_id.clone()),
        commit_method: decision.commit_method,
        repository_scope,
        primary_repository_identified,
        bead_action: decision.bead_action,
        bead_status: decision.bead_status,
        legacy_do_not_close_bead: decision.legacy_do_not_close_bead,
    }
}

fn primary_obligation_identified(
    context: &FinalizerContextWire,
    obligation_id: Option<&str>,
) -> bool {
    let Some(obligation_id) = assigned_bead_id(obligation_id) else {
        return false;
    };
    context.obligations.iter().any(|obligation| {
        obligation.obligation_id == obligation_id
            && obligation.kind == "repository"
    })
}
