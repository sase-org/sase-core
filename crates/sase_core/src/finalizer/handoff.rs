//! Finalizer handoff descriptor and repository-action legality.
//!
//! The host stages handoffs durably (see `staged_handoff`); this module owns
//! the frontend-neutral copy that rides inside `FinalizerContextWire`.
//! `FinalizerHandoffWire` is additive and optional: legacy contexts without a
//! `handoff` field serialize and hash exactly as before.

use serde::{Deserialize, Serialize};

use super::selection::{validate_list_len, validate_required_text};
use super::FinalizerError;

/// Handoff kind vocabulary. Mirrors the staged-handoff kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerHandoffKindWire {
    Plan,
    Questions,
    Pipe,
    Gate,
    Monitor,
}

/// Successor workspace vocabulary. Mirrors the staged-handoff vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerHandoffSuccessorWire {
    Inherit,
    None,
}

/// Optional finalizer handoff descriptor.
///
/// - `kind` names the staged handoff kind.
/// - `staged_ref` is the staged record's UUID (`staged_id`).
/// - `successor_workspace` is `inherit` when the successor keeps this
///   workspace, `none` when the handoff releases it.
/// - `carry_allowed` mirrors the staged record's carry flag.
/// - `prepared_intent_ref` names a sealed conditional-completion intent and
///   applies only to a staged monitor; it is never sufficient on its own —
///   the continuation-domain predicate must authenticate live evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerHandoffWire {
    pub kind: FinalizerHandoffKindWire,
    pub staged_ref: String,
    pub successor_workspace: FinalizerHandoffSuccessorWire,
    pub carry_allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_intent_ref: Option<String>,
}

/// Host request to adjudicate one repository action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryActionCheckRequestWire {
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<FinalizerHandoffWire>,
}

/// Typed repository-action outcome. `commit` is always allowed; `carry`
/// requires a valid inheriting handoff. Unknown actions are rejected so
/// Python never grows a second policy table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RepositoryActionOutcomeWire {
    Allowed { action: String },
    Rejected { code: String, message: String },
}

pub const REPOSITORY_ACTION_CODE_UNKNOWN_ACTION: &str =
    "unknown_repository_action";
pub const REPOSITORY_ACTION_CODE_CARRY_REQUIRES_HANDOFF: &str =
    "carry_requires_handoff";
pub const REPOSITORY_ACTION_CODE_CARRY_NOT_ALLOWED: &str =
    "carry_not_allowed_for_handoff";

fn rejected(
    code: &'static str,
    message: impl Into<String>,
) -> RepositoryActionOutcomeWire {
    RepositoryActionOutcomeWire::Rejected {
        code: code.to_string(),
        message: message.into(),
    }
}

/// Validate one handoff descriptor against the kind/workspace/carry table.
///
/// Rules (mirroring the staged-handoff kind invariants):
/// - `staged_ref` must be a UUID and `prepared_intent_ref`, when present,
///   must be a nonblank reference.
/// - `carry_allowed` requires workspace inheritance: a `none` successor can
///   never carry.
/// - `plan` handoffs never carry and use successor `none`.
/// - `questions` and `pipe` handoffs use successor `inherit` with carry.
/// - `monitor` handoffs use successor `inherit` (carry flag decides whether
///   plain carry is legal; prepared coverage is adjudicated separately).
/// - `gate` with successor `none` cannot carry; `gate` with `inherit` may or
///   may not carry.
/// - A prepared intent applies only to a staged monitor.
pub fn validate_finalizer_handoff(
    handoff: &FinalizerHandoffWire,
) -> Result<(), FinalizerError> {
    validate_staged_ref(&handoff.staged_ref)?;
    if let Some(intent_ref) = &handoff.prepared_intent_ref {
        validate_required_text(intent_ref, "handoff.prepared_intent_ref")?;
    }
    validate_list_len(0, "handoff")?;

    // Kind-specific errors take precedence over the generic workspace/carry
    // and prepared-intent fallbacks below, so a `plan` handoff with carry
    // reports `plan handoffs ...` rather than the generic `none` message.
    match handoff.kind {
        FinalizerHandoffKindWire::Plan => {
            if handoff.successor_workspace
                != FinalizerHandoffSuccessorWire::None
            {
                return Err(FinalizerError::validation(
                    "plan handoffs use successor_workspace none",
                ));
            }
            if handoff.carry_allowed {
                return Err(FinalizerError::validation(
                    "plan handoffs cannot set carry_allowed",
                ));
            }
            if handoff.prepared_intent_ref.is_some() {
                return Err(FinalizerError::validation(
                    "plan handoffs cannot name a prepared intent",
                ));
            }
        }
        FinalizerHandoffKindWire::Questions
        | FinalizerHandoffKindWire::Pipe => {
            if handoff.successor_workspace
                != FinalizerHandoffSuccessorWire::Inherit
            {
                return Err(FinalizerError::validation(format!(
                    "{} handoffs use successor_workspace inherit",
                    kind_name(handoff.kind),
                )));
            }
            if !handoff.carry_allowed {
                return Err(FinalizerError::validation(format!(
                    "{} handoffs require carry_allowed",
                    kind_name(handoff.kind),
                )));
            }
        }
        FinalizerHandoffKindWire::Monitor => {
            if handoff.successor_workspace
                != FinalizerHandoffSuccessorWire::Inherit
            {
                return Err(FinalizerError::validation(
                    "monitor handoffs use successor_workspace inherit",
                ));
            }
        }
        FinalizerHandoffKindWire::Gate => {}
    }
    if handoff.carry_allowed
        && handoff.successor_workspace == FinalizerHandoffSuccessorWire::None
    {
        return Err(FinalizerError::validation(
            "handoff with successor_workspace none cannot set carry_allowed",
        ));
    }
    if handoff.prepared_intent_ref.is_some()
        && handoff.kind != FinalizerHandoffKindWire::Monitor
    {
        return Err(FinalizerError::validation(
            "handoff prepared_intent_ref applies only to a staged monitor",
        ));
    }
    Ok(())
}

/// Reusable repository-action legality check.
///
/// - `commit` is always allowed (dirty or clean; commit-specific guards live
///   with the commit path, not here).
/// - `carry` requires a valid inheriting handoff with `carry_allowed`. Plans
///   never carry; a `none` successor never carries.
/// - Anything else (including `defer`, which has its own typed adjudication)
///   is rejected here so callers cannot smuggle deferrals through carry.
pub fn check_repository_action(
    request: &RepositoryActionCheckRequestWire,
) -> RepositoryActionOutcomeWire {
    match request.action.as_str() {
        "commit" => RepositoryActionOutcomeWire::Allowed {
            action: "commit".to_string(),
        },
        "carry" => match &request.handoff {
            None => rejected(
                REPOSITORY_ACTION_CODE_CARRY_REQUIRES_HANDOFF,
                "carry requires a staged handoff with carry_allowed and workspace inherit",
            ),
            Some(handoff) => {
                if let Err(error) = validate_finalizer_handoff(handoff) {
                    return rejected(
                        REPOSITORY_ACTION_CODE_CARRY_NOT_ALLOWED,
                        error.to_string(),
                    );
                }
                if !handoff.carry_allowed {
                    return rejected(
                        REPOSITORY_ACTION_CODE_CARRY_NOT_ALLOWED,
                        "carry is not allowed for this handoff",
                    );
                }
                if handoff.successor_workspace
                    != FinalizerHandoffSuccessorWire::Inherit
                {
                    return rejected(
                        REPOSITORY_ACTION_CODE_CARRY_NOT_ALLOWED,
                        "carry requires successor_workspace inherit",
                    );
                }
                if handoff.kind == FinalizerHandoffKindWire::Plan {
                    return rejected(
                        REPOSITORY_ACTION_CODE_CARRY_NOT_ALLOWED,
                        "plan handoffs cannot carry",
                    );
                }
                RepositoryActionOutcomeWire::Allowed {
                    action: "carry".to_string(),
                }
            }
        },
        other => rejected(
            REPOSITORY_ACTION_CODE_UNKNOWN_ACTION,
            format!("unknown repository action {other:?}"),
        ),
    }
}

/// Return whether a dirty repository requirement may omit its submission.
///
/// A dirty requirement with `submission_required == false` is legal only
/// with valid carry (inheriting handoff with `carry_allowed`) or prepared
/// coverage (monitor handoff naming a prepared intent; the continuation
/// predicate authenticates the live evidence separately).
pub fn dirty_requirement_may_omit_submission(
    handoff: Option<&FinalizerHandoffWire>,
) -> Result<(), FinalizerError> {
    match handoff {
        None => Err(FinalizerError::validation(
            "dirty repository requirement cannot omit submission without a staged handoff",
        )),
        Some(handoff) => {
            validate_finalizer_handoff(handoff)?;
            if handoff.prepared_intent_ref.is_some() {
                if handoff.kind != FinalizerHandoffKindWire::Monitor {
                    return Err(FinalizerError::validation(
                        "handoff prepared_intent_ref applies only to a staged monitor",
                    ));
                }
                return Ok(());
            }
            match check_repository_action(&RepositoryActionCheckRequestWire {
                action: "carry".to_string(),
                handoff: Some(handoff.clone()),
            }) {
                RepositoryActionOutcomeWire::Allowed { .. } => Ok(()),
                RepositoryActionOutcomeWire::Rejected { message, .. } => {
                    Err(FinalizerError::validation(message))
                }
            }
        }
    }
}

/// Return whether an omitted dirty-repository requirement may still include
/// an explicit payload in the submission.
///
/// Ordinary inheriting carry keeps `commit` available as the default choice.
/// Prepared monitor coverage is different: the starter must leave its sealed
/// declaration for host completion, so naming an intent does not authorize an
/// optional starter payload.
pub fn dirty_requirement_may_accept_optional_submission(
    handoff: Option<&FinalizerHandoffWire>,
) -> bool {
    let Some(handoff) = handoff else {
        return false;
    };
    if handoff.prepared_intent_ref.is_some() {
        return false;
    }
    matches!(
        check_repository_action(&RepositoryActionCheckRequestWire {
            action: "carry".to_string(),
            handoff: Some(handoff.clone()),
        }),
        RepositoryActionOutcomeWire::Allowed { .. }
    )
}

fn kind_name(kind: FinalizerHandoffKindWire) -> &'static str {
    match kind {
        FinalizerHandoffKindWire::Plan => "plan",
        FinalizerHandoffKindWire::Questions => "questions",
        FinalizerHandoffKindWire::Pipe => "pipe",
        FinalizerHandoffKindWire::Gate => "gate",
        FinalizerHandoffKindWire::Monitor => "monitor",
    }
}

fn validate_staged_ref(value: &str) -> Result<(), FinalizerError> {
    if is_uuid(value) {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "handoff.staged_ref is not a UUID: {value:?}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn handoff(
        kind: FinalizerHandoffKindWire,
        successor: FinalizerHandoffSuccessorWire,
        carry: bool,
    ) -> FinalizerHandoffWire {
        FinalizerHandoffWire {
            kind,
            staged_ref: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            successor_workspace: successor,
            carry_allowed: carry,
            prepared_intent_ref: None,
        }
    }

    #[test]
    fn plan_rejects_carry_and_requires_none_successor() {
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Plan,
            FinalizerHandoffSuccessorWire::None,
            false,
        ))
        .is_ok());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Plan,
            FinalizerHandoffSuccessorWire::None,
            true,
        ))
        .is_err());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Plan,
            FinalizerHandoffSuccessorWire::Inherit,
            false,
        ))
        .is_err());
    }

    #[test]
    fn questions_and_pipe_require_inherit_carry() {
        for kind in [
            FinalizerHandoffKindWire::Questions,
            FinalizerHandoffKindWire::Pipe,
        ] {
            assert!(validate_finalizer_handoff(&handoff(
                kind,
                FinalizerHandoffSuccessorWire::Inherit,
                true,
            ))
            .is_ok());
            assert!(validate_finalizer_handoff(&handoff(
                kind,
                FinalizerHandoffSuccessorWire::Inherit,
                false,
            ))
            .is_err());
            assert!(validate_finalizer_handoff(&handoff(
                kind,
                FinalizerHandoffSuccessorWire::None,
                false,
            ))
            .is_err());
        }
    }

    #[test]
    fn monitor_requires_inherit_and_allows_prepared_only_there() {
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Monitor,
            FinalizerHandoffSuccessorWire::Inherit,
            false,
        ))
        .is_ok());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Monitor,
            FinalizerHandoffSuccessorWire::None,
            false,
        ))
        .is_err());
        let mut prepared = handoff(
            FinalizerHandoffKindWire::Monitor,
            FinalizerHandoffSuccessorWire::Inherit,
            false,
        );
        prepared.prepared_intent_ref = Some("intent-1".to_string());
        assert!(validate_finalizer_handoff(&prepared).is_ok());
        let mut misplaced = handoff(
            FinalizerHandoffKindWire::Questions,
            FinalizerHandoffSuccessorWire::Inherit,
            true,
        );
        misplaced.prepared_intent_ref = Some("intent-1".to_string());
        assert!(validate_finalizer_handoff(&misplaced).is_err());
    }

    #[test]
    fn gate_none_cannot_carry_and_carry_requires_inherit() {
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::None,
            false,
        ))
        .is_ok());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::None,
            true,
        ))
        .is_err());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::Inherit,
            true,
        ))
        .is_ok());
        assert!(validate_finalizer_handoff(&handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::Inherit,
            false,
        ))
        .is_ok());
    }

    #[test]
    fn carry_allowed_requires_workspace_inherit() {
        let bad = handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::None,
            true,
        );
        assert!(validate_finalizer_handoff(&bad)
            .unwrap_err()
            .to_string()
            .contains("none cannot set carry_allowed"));
    }

    #[test]
    fn staged_ref_must_be_uuid() {
        let mut bad = handoff(
            FinalizerHandoffKindWire::Gate,
            FinalizerHandoffSuccessorWire::Inherit,
            true,
        );
        bad.staged_ref = "not-a-uuid".to_string();
        assert!(validate_finalizer_handoff(&bad)
            .unwrap_err()
            .to_string()
            .contains("staged_ref is not a UUID"));
    }

    #[test]
    fn check_action_allows_commit_and_gates_carry() {
        let commit =
            check_repository_action(&RepositoryActionCheckRequestWire {
                action: "commit".to_string(),
                handoff: None,
            });
        assert_eq!(
            commit,
            RepositoryActionOutcomeWire::Allowed {
                action: "commit".to_string()
            }
        );
        let missing =
            check_repository_action(&RepositoryActionCheckRequestWire {
                action: "carry".to_string(),
                handoff: None,
            });
        assert!(matches!(
            missing,
            RepositoryActionOutcomeWire::Rejected { .. }
        ));
        let plan_carry =
            check_repository_action(&RepositoryActionCheckRequestWire {
                action: "carry".to_string(),
                handoff: Some(handoff(
                    FinalizerHandoffKindWire::Plan,
                    FinalizerHandoffSuccessorWire::None,
                    false,
                )),
            });
        assert!(matches!(
            plan_carry,
            RepositoryActionOutcomeWire::Rejected { .. }
        ));
        let ok = check_repository_action(&RepositoryActionCheckRequestWire {
            action: "carry".to_string(),
            handoff: Some(handoff(
                FinalizerHandoffKindWire::Questions,
                FinalizerHandoffSuccessorWire::Inherit,
                true,
            )),
        });
        assert_eq!(
            ok,
            RepositoryActionOutcomeWire::Allowed {
                action: "carry".to_string()
            }
        );
        let unknown =
            check_repository_action(&RepositoryActionCheckRequestWire {
                action: "defer".to_string(),
                handoff: None,
            });
        match unknown {
            RepositoryActionOutcomeWire::Rejected { code, .. } => {
                assert_eq!(code, REPOSITORY_ACTION_CODE_UNKNOWN_ACTION);
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn handoff_wire_rejects_unknown_fields() {
        let value = json!({
            "kind": "gate",
            "staged_ref": "123e4567-e89b-12d3-a456-426614174000",
            "successor_workspace": "inherit",
            "carry_allowed": true,
            "bogus": true,
        });
        assert!(serde_json::from_value::<FinalizerHandoffWire>(value).is_err());
    }

    #[test]
    fn absent_optional_prepared_ref_round_trips() {
        let wire = handoff(
            FinalizerHandoffKindWire::Monitor,
            FinalizerHandoffSuccessorWire::Inherit,
            true,
        );
        let value = serde_json::to_value(&wire).unwrap();
        assert!(value.get("prepared_intent_ref").is_none());
        let parsed: FinalizerHandoffWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(parsed, wire);
    }
}
