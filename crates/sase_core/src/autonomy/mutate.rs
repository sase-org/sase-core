//! Revision-checked `mutate_autonomy` and structural `autonomy_inherit`.
//!
//! A human actor (TUI, CLI) may set any selection; an agent actor may
//! only narrow, so for every kind the new value must be the same or
//! `ask`. An explicit `%auto` in a host-composed successor prompt is
//! always agent-authored, so inheritance narrows under agent semantics
//! no matter which actor is recorded.

use super::profiles::{policy_digest, profile_policy};
use super::resolve::{valid_actor_kinds, AutonomyError};
use super::wires::{
    AutonomyInheritResultWire, AutonomyLastWire, AutonomyMutateRequestWire,
    AutonomyMutateResultWire, AutonomyRecordWire, AUTONOMY_PROFILE_MANUAL,
    AUTONOMY_PROFILE_STANDARD, AUTONOMY_SOURCE_INHERITED, AUTONOMY_VALUE_ASK,
};

const POLICY_KINDS: &[&str] = &["plan", "epic", "question"];

/// Missing values fail closed like `ask` when comparing narrowness.
fn narrow_value(value: Option<&str>) -> &str {
    match value {
        None => AUTONOMY_VALUE_ASK,
        Some(text) => text,
    }
}

/// Strip one leading `%auto` (and its `:`) so full prompt spellings and
/// bare value texts share one grammar.
fn normalize_selection(selection: &str) -> &str {
    let mut text = selection.trim();
    if let Some(rest) = text.strip_prefix("%auto") {
        text = rest.trim_start_matches(':');
    }
    text.trim()
}

/// Resolve a mutation selection to `(profile, canonical selection)`.
/// `restore` applies `last`, else `standard`.
fn resolve_target(
    selection: &str,
    last: Option<&AutonomyLastWire>,
) -> Result<(String, String), String> {
    let text = normalize_selection(selection);
    if text == "restore" {
        if let Some(last) = last {
            return Ok((last.profile.clone(), last.selection.clone()));
        }
        return Ok((AUTONOMY_PROFILE_STANDARD.to_string(), String::new()));
    }
    match text {
        "manual" | "off" => {
            Ok((AUTONOMY_PROFILE_MANUAL.to_string(), "manual".to_string()))
        }
        "" | "true" | "+" => {
            Ok((AUTONOMY_PROFILE_STANDARD.to_string(), String::new()))
        }
        "plan" | "tale" => Ok(("tale".to_string(), text.to_string())),
        "epic" => Ok(("epic".to_string(), "epic".to_string())),
        _ => Err(format!("unknown %auto selection '{selection}'")),
    }
}

/// The first kind the target would widen, if any.
fn widened_kind(
    current: &AutonomyRecordWire,
    profile: &str,
) -> Option<(String, String, String)> {
    let target = profile_policy(profile);
    let current_policy = &current.policy;
    for kind in POLICY_KINDS {
        let old = match *kind {
            "plan" => current_policy.gates.plan.as_deref(),
            "epic" => current_policy.gates.epic.as_deref(),
            _ => current_policy.gates.question.as_deref(),
        };
        let new = match *kind {
            "plan" => target.gates.plan.as_deref(),
            "epic" => target.gates.epic.as_deref(),
            _ => target.gates.question.as_deref(),
        };
        let (old, new) = (narrow_value(old), narrow_value(new));
        if new != old && new != AUTONOMY_VALUE_ASK {
            return Some((kind.to_string(), old.to_string(), new.to_string()));
        }
    }
    None
}

fn check_actor_kind(kind: &str) -> Result<(), AutonomyError> {
    if valid_actor_kinds().contains(&kind) {
        Ok(())
    } else {
        Err(AutonomyError::InvalidActorKind(kind.to_string()))
    }
}

fn same_state(
    current: &AutonomyRecordWire,
    profile: &str,
    selection: &str,
    last: &Option<AutonomyLastWire>,
) -> bool {
    current.profile == profile
        && current.selection == selection
        && current.policy == profile_policy(profile)
        && current.last == *last
}

/// Apply a resolved target to `base`: bump the revision, recompute the
/// digest, and stamp the actor.
fn apply_target(
    base: &AutonomyRecordWire,
    profile: &str,
    selection: &str,
    last: Option<AutonomyLastWire>,
    actor: &super::wires::AutonomyActorWire,
    now: &str,
) -> AutonomyRecordWire {
    let policy = profile_policy(profile);
    AutonomyRecordWire {
        schema_version: base.schema_version,
        profile: profile.to_string(),
        selection: selection.to_string(),
        policy: policy.clone(),
        overrides: base.overrides.clone(),
        source: base.source.clone(),
        inherited_from: base.inherited_from.clone(),
        last,
        revision: base.revision + 1,
        digest: policy_digest(&policy),
        updated_at: if now.is_empty() {
            base.updated_at.clone()
        } else {
            Some(now.to_string())
        },
        updated_by: Some(actor.clone()),
    }
}

/// Next `last` when the target profile is `manual`: the previous
/// non-manual `{profile, selection}` is stored.
fn manual_last(base: &AutonomyRecordWire) -> Option<AutonomyLastWire> {
    if base.profile == AUTONOMY_PROFILE_MANUAL {
        base.last.clone()
    } else {
        Some(AutonomyLastWire {
            profile: base.profile.clone(),
            selection: base.selection.clone(),
        })
    }
}

/// Next `last` when the target profile is not `manual`.
fn active_last(profile: &str, selection: &str) -> Option<AutonomyLastWire> {
    Some(AutonomyLastWire {
        profile: profile.to_string(),
        selection: selection.to_string(),
    })
}

/// Apply a `%auto` selection change to a live record with revision and
/// actor checks.
pub fn mutate_autonomy(
    record: &AutonomyRecordWire,
    request: &AutonomyMutateRequestWire,
) -> Result<AutonomyMutateResultWire, AutonomyError> {
    check_actor_kind(&request.actor.kind)?;
    if let Some(expected) = request.expected_revision {
        if expected != record.revision {
            return Ok(AutonomyMutateResultWire {
                status: "stale".to_string(),
                record: record.clone(),
                reason: format!(
                    "expected revision {expected} but the live record is \
                     at revision {}",
                    record.revision
                ),
            });
        }
    }
    let (profile, selection) =
        match resolve_target(&request.selection, record.last.as_ref()) {
            Ok(target) => target,
            Err(reason) => {
                return Ok(AutonomyMutateResultWire {
                    status: "refused".to_string(),
                    record: record.clone(),
                    reason,
                });
            }
        };
    if request.actor.kind == "agent" {
        if let Some((kind, old, new)) = widened_kind(record, &profile) {
            return Ok(AutonomyMutateResultWire {
                status: "refused".to_string(),
                record: record.clone(),
                reason: format!(
                    "agent widening refused: '{}' would widen {kind} \
                     from '{old}' to '{new}'",
                    request.selection,
                ),
            });
        }
    }
    let last = if profile == AUTONOMY_PROFILE_MANUAL {
        manual_last(record)
    } else {
        active_last(&profile, &selection)
    };
    if same_state(record, &profile, &selection, &last) {
        return Ok(AutonomyMutateResultWire {
            status: "unchanged".to_string(),
            record: record.clone(),
            reason: format!(
                "selection '{}' already active at revision {}",
                request.selection, record.revision
            ),
        });
    }
    let next = apply_target(
        record,
        &profile,
        &selection,
        last,
        &request.actor,
        &request.now,
    );
    let revision = next.revision;
    Ok(AutonomyMutateResultWire {
        status: "applied".to_string(),
        record: next,
        reason: format!(
            "selection '{}' applied at revision {revision}",
            request.selection,
        ),
    })
}

/// Seed a host-composed successor from its predecessor's live record:
/// policy, profile, selection, and `last` carry over unchanged with
/// `source: inherited`. An explicit selection narrows under agent
/// semantics; a refused widening keeps the inherited record.
pub fn autonomy_inherit(
    predecessor: &AutonomyRecordWire,
    predecessor_name: &str,
    explicit_selection: Option<&str>,
    actor: &super::wires::AutonomyActorWire,
    now: &str,
) -> Result<AutonomyInheritResultWire, AutonomyError> {
    check_actor_kind(&actor.kind)?;
    let base = AutonomyRecordWire {
        source: AUTONOMY_SOURCE_INHERITED.to_string(),
        inherited_from: if predecessor_name.is_empty() {
            None
        } else {
            Some(predecessor_name.to_string())
        },
        updated_at: if now.is_empty() {
            predecessor.updated_at.clone()
        } else {
            Some(now.to_string())
        },
        updated_by: Some(actor.clone()),
        ..predecessor.clone()
    };
    let from = if predecessor_name.is_empty() {
        "its predecessor".to_string()
    } else {
        predecessor_name.to_string()
    };
    let Some(selection) = explicit_selection else {
        return Ok(AutonomyInheritResultWire {
            status: "inherited".to_string(),
            record: base,
            reason: format!(
                "inherited {} record from {from}",
                predecessor.profile
            ),
        });
    };
    let (profile, canonical) =
        match resolve_target(selection, base.last.as_ref()) {
            Ok(target) => target,
            Err(reason) => {
                return Ok(AutonomyInheritResultWire {
                    status: "refused".to_string(),
                    record: base,
                    reason,
                });
            }
        };
    if let Some((kind, old, new)) = widened_kind(&base, &profile) {
        return Ok(AutonomyInheritResultWire {
            status: "refused".to_string(),
            record: base,
            reason: format!(
                "agent widening refused: '{selection}' would widen \
                 {kind} from '{old}' to '{new}'; kept the inherited record",
            ),
        });
    }
    let last = if profile == AUTONOMY_PROFILE_MANUAL {
        manual_last(&base)
    } else {
        active_last(&profile, &canonical)
    };
    if same_state(&base, &profile, &canonical, &last) {
        let reason = format!(
            "explicit '{selection}' matches the inherited {} record \
             from {from}",
            base.profile,
        );
        return Ok(AutonomyInheritResultWire {
            status: "inherited".to_string(),
            record: base,
            reason,
        });
    }
    let next = apply_target(&base, &profile, &canonical, last, actor, now);
    let revision = next.revision;
    Ok(AutonomyInheritResultWire {
        status: "narrowed".to_string(),
        record: next,
        reason: format!(
            "successor narrows to '{selection}' at revision {revision}",
        ),
    })
}
