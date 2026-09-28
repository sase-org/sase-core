//! Goal action validation.
//!
//! [`plan_goal_action`] turns a requested action into planned events,
//! or a stable snake_case refusal. All trimming, length, one-line,
//! and criteria-count checks live here. Ids come from an injectable
//! [`GoalIdMint`] so tests are deterministic.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::ids::{mint_event_id, mint_goal_id, parse_goal_id};
use super::wire::{
    GoalActorWire, GoalCreatedPayloadWire, GoalCriterionInputWire,
    GoalEditedPayloadWire, GoalEventKindWire, GoalEventPayloadWire,
    GoalEventWire, GoalMergedPayloadWire, GoalOriginWire,
    GoalReopenedPayloadWire, GoalSettledPayloadWire, GoalStateWire,
    GOAL_WIRE_SCHEMA_VERSION,
};

/// Maximum title length in chars.
pub const GOAL_TITLE_MAX: usize = 60;

/// Maximum outcome length in chars.
pub const GOAL_OUTCOME_MAX: usize = 280;

/// Maximum criterion text length in chars.
pub const GOAL_CRITERION_TEXT_MAX: usize = 200;

/// Maximum criteria per goal.
pub const GOAL_CRITERIA_MAX: usize = 10;

/// Maximum note length in chars.
pub const GOAL_NOTE_MAX: usize = 280;

/// Maximum reopen message and drop reason length in chars.
pub const GOAL_MESSAGE_MAX: usize = 280;

/// A typed action refusal with a stable snake_case code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code}: {message}")]
pub struct GoalRefusalWire {
    /// Stable snake_case refusal code.
    #[serde(default)]
    pub code: String,
    /// Human-readable detail.
    #[serde(default)]
    pub message: String,
}

impl GoalRefusalWire {
    /// Build a refusal.
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

/// Source of minted ids for planned events.
pub trait GoalIdMint {
    /// Mint a goal id.
    fn mint_goal_id(&mut self) -> String;
    /// Mint an event id.
    fn mint_event_id(&mut self) -> String;
}

/// Production mint backed by `OsRng` and the monotonic clock guard.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsGoalIdMint;

impl GoalIdMint for OsGoalIdMint {
    fn mint_goal_id(&mut self) -> String {
        mint_goal_id()
    }

    fn mint_event_id(&mut self) -> String {
        mint_event_id()
    }
}

/// Deterministic mint for tests.
#[derive(Debug, Clone, Default)]
pub struct DeterministicGoalIdMint {
    /// Goal ids handed out in order.
    pub goal_ids: Vec<String>,
    /// Event ids handed out in order.
    pub event_ids: Vec<String>,
}

impl DeterministicGoalIdMint {
    /// Build a mint from fixed id sequences.
    pub fn new(goal_ids: Vec<String>, event_ids: Vec<String>) -> Self {
        Self {
            goal_ids,
            event_ids,
        }
    }
}

impl GoalIdMint for DeterministicGoalIdMint {
    fn mint_goal_id(&mut self) -> String {
        if self.goal_ids.is_empty() {
            return "zzzzz".to_string();
        }
        self.goal_ids.remove(0)
    }

    fn mint_event_id(&mut self) -> String {
        if self.event_ids.is_empty() {
            return "0".repeat(26);
        }
        self.event_ids.remove(0)
    }
}

/// A requested goal action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum GoalActionWire {
    /// Create a goal; plans a `created` event.
    New {
        /// Title, at most 60 chars.
        title: String,
        /// Outcome, at most 280 chars on one line.
        outcome: String,
        /// Initial criteria.
        #[serde(default)]
        criteria: Vec<GoalCriterionInputWire>,
        /// Owning project.
        #[serde(default)]
        project: String,
        /// Creation origin.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<GoalOriginWire>,
        /// Creation channel; defaults to `"cli"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        via: Option<String>,
        /// Caller idempotency key; generated when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Edit a goal; plans an `edited` event.
    Edit {
        /// Target goal id.
        goal_id: String,
        /// Replacement title.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Replacement outcome.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<String>,
        /// Criteria to add.
        #[serde(default)]
        criteria_added: Vec<GoalCriterionInputWire>,
        /// Criterion ids to remove.
        #[serde(default)]
        criteria_removed: Vec<String>,
        /// Free note for the timeline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        /// Expected head; refuses `stale_basis` when it moved.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_head: Option<String>,
        /// Caller idempotency key; generated when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Drop a goal; plans `settled{canceled}`.
    Drop {
        /// Target goal id.
        goal_id: String,
        /// Why the goal is dropped.
        why: String,
        /// Expected head; refuses `stale_basis` when it moved.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_head: Option<String>,
        /// Caller idempotency key; generated when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Reopen a settled goal; plans `reopened`.
    Reopen {
        /// Target goal id.
        goal_id: String,
        /// Reopen message, at most 280 chars.
        message: String,
        /// Expected head; refuses `stale_basis` when it moved.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_head: Option<String>,
        /// Caller idempotency key; generated when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Merge one goal into another.
    ///
    /// Plans `settled{merged, into}` on the source plus
    /// `merged{from}` on the target. Both goals must be unsettled
    /// and different.
    Merge {
        /// Source goal id, settled by the merge.
        source_id: String,
        /// Target goal id, kept by the merge.
        target_id: String,
        /// Current target state for validation and basis.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_state: Option<GoalStateWire>,
        /// Why the goals merge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        why: Option<String>,
        /// Expected source head; refuses `stale_basis` on drift.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_head: Option<String>,
        /// Caller idempotency key; generated when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
}

/// Plan the events for an action, or refuse it.
///
/// `state` is the source goal's current state, or `None` for `new`.
/// `now` is the caller's RFC3339 clock reading.
pub fn plan_goal_action(
    state: Option<&GoalStateWire>,
    action: &GoalActionWire,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    match action {
        GoalActionWire::New {
            title,
            outcome,
            criteria,
            project,
            origin,
            via,
            idempotency_key,
        } => plan_new(
            state,
            title,
            outcome,
            criteria,
            project,
            origin.clone(),
            via.clone(),
            idempotency_key.clone(),
            actor,
            now,
            ids,
        ),
        GoalActionWire::Edit {
            goal_id,
            title,
            outcome,
            criteria_added,
            criteria_removed,
            note,
            expected_head,
            idempotency_key,
        } => plan_edit(
            state,
            goal_id,
            title.clone(),
            outcome.clone(),
            criteria_added,
            criteria_removed,
            note.clone(),
            expected_head.clone(),
            idempotency_key.clone(),
            actor,
            now,
            ids,
        ),
        GoalActionWire::Drop {
            goal_id,
            why,
            expected_head,
            idempotency_key,
        } => plan_drop(
            state,
            goal_id,
            why,
            expected_head.clone(),
            idempotency_key.clone(),
            actor,
            now,
            ids,
        ),
        GoalActionWire::Reopen {
            goal_id,
            message,
            expected_head,
            idempotency_key,
        } => plan_reopen(
            state,
            goal_id,
            message,
            expected_head.clone(),
            idempotency_key.clone(),
            actor,
            now,
            ids,
        ),
        GoalActionWire::Merge {
            source_id,
            target_id,
            target_state,
            why,
            expected_head,
            idempotency_key,
        } => plan_merge(
            state,
            source_id,
            target_id,
            target_state.as_ref(),
            why.clone(),
            expected_head.clone(),
            idempotency_key.clone(),
            actor,
            now,
            ids,
        ),
    }
}

fn check_head(
    state: &GoalStateWire,
    expected_head: Option<String>,
) -> Result<(), GoalRefusalWire> {
    if let Some(expected) = expected_head {
        if state.head.as_deref() != Some(expected.as_str()) {
            return Err(GoalRefusalWire::new(
                "stale_basis",
                format!(
                    "head moved: want basis {expected}, have {}",
                    state.head.as_deref().unwrap_or("none"),
                ),
            ));
        }
    }
    Ok(())
}

fn key_or_generated(idempotency_key: Option<String>, event_id: &str) -> String {
    idempotency_key.unwrap_or_else(|| format!("auto:{event_id}"))
}

#[allow(clippy::too_many_arguments)]
fn envelope(
    goal_id: &str,
    event_id: String,
    kind: GoalEventKindWire,
    basis: Option<String>,
    idempotency_key: String,
    actor: &GoalActorWire,
    now: &str,
    payload: serde_json::Value,
) -> GoalEventWire {
    GoalEventWire {
        schema_version: GOAL_WIRE_SCHEMA_VERSION,
        event_id,
        goal_id: goal_id.to_string(),
        kind,
        at: now.to_string(),
        actor: actor.clone(),
        basis,
        idempotency_key,
        payload,
    }
}

fn to_payload<T>(value: &T) -> serde_json::Value
where
    T: Serialize,
{
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

fn trimmed(value: &str) -> String {
    value.trim().to_string()
}

fn is_one_line(value: &str) -> bool {
    !value.contains('\n') && !value.contains('\r')
}

fn check_title(title: &str) -> Result<String, GoalRefusalWire> {
    let title = trimmed(title);
    if title.is_empty() {
        return Err(GoalRefusalWire::new(
            "title_empty",
            "title is empty after trimming",
        ));
    }
    if title.chars().count() > GOAL_TITLE_MAX {
        return Err(GoalRefusalWire::new(
            "title_too_long",
            format!(
                "title is {} chars; want at most {GOAL_TITLE_MAX}",
                title.chars().count(),
            ),
        ));
    }
    if !is_one_line(&title) {
        return Err(GoalRefusalWire::new(
            "title_must_be_one_line",
            "title must fit on one line",
        ));
    }
    Ok(title)
}

fn check_outcome(outcome: &str) -> Result<String, GoalRefusalWire> {
    let outcome = trimmed(outcome);
    if outcome.is_empty() {
        return Err(GoalRefusalWire::new(
            "outcome_empty",
            "outcome is empty after trimming",
        ));
    }
    if outcome.chars().count() > GOAL_OUTCOME_MAX {
        return Err(GoalRefusalWire::new(
            "outcome_too_long",
            format!(
                "outcome is {} chars; want at most {GOAL_OUTCOME_MAX}",
                outcome.chars().count(),
            ),
        ));
    }
    if !is_one_line(&outcome) {
        return Err(GoalRefusalWire::new(
            "outcome_must_be_one_line",
            "outcome must fit on one line",
        ));
    }
    Ok(outcome)
}

fn check_criteria(
    criteria: &[GoalCriterionInputWire],
    existing: usize,
) -> Result<Vec<GoalCriterionInputWire>, GoalRefusalWire> {
    if criteria.len() + existing > GOAL_CRITERIA_MAX {
        return Err(GoalRefusalWire::new(
            "too_many_criteria",
            format!(
                "goal would hold {} criteria; \
                 want at most {GOAL_CRITERIA_MAX}",
                criteria.len() + existing,
            ),
        ));
    }
    let mut cleaned = Vec::with_capacity(criteria.len());
    for input in criteria {
        let text = trimmed(&input.text);
        if text.is_empty() {
            return Err(GoalRefusalWire::new(
                "criterion_required",
                "criterion text is required",
            ));
        }
        if text.chars().count() > GOAL_CRITERION_TEXT_MAX {
            return Err(GoalRefusalWire::new(
                "criterion_too_long",
                format!(
                    "criterion is {} chars; \
                     want at most {GOAL_CRITERION_TEXT_MAX}",
                    text.chars().count(),
                ),
            ));
        }
        cleaned.push(GoalCriterionInputWire {
            text,
            source: input.source,
        });
    }
    Ok(cleaned)
}

fn check_message(message: &str) -> Result<String, GoalRefusalWire> {
    let message = trimmed(message);
    if message.is_empty() {
        return Err(GoalRefusalWire::new(
            "message_required",
            "message is required",
        ));
    }
    if message.chars().count() > GOAL_MESSAGE_MAX {
        return Err(GoalRefusalWire::new(
            "message_too_long",
            format!(
                "message is {} chars; want at most {GOAL_MESSAGE_MAX}",
                message.chars().count(),
            ),
        ));
    }
    Ok(message)
}

#[allow(clippy::too_many_arguments)]
fn plan_new(
    state: Option<&GoalStateWire>,
    title: &str,
    outcome: &str,
    criteria: &[GoalCriterionInputWire],
    project: &str,
    origin: Option<GoalOriginWire>,
    via: Option<String>,
    idempotency_key: Option<String>,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    if state.is_some() {
        return Err(GoalRefusalWire::new(
            "goal_already_exists",
            "new refuses a goal that already has state",
        ));
    }
    let title = check_title(title)?;
    let outcome = check_outcome(outcome)?;
    let criteria = check_criteria(criteria, 0)?;
    let project = trimmed(project);
    if project.is_empty() {
        return Err(GoalRefusalWire::new(
            "project_required",
            "project is required",
        ));
    }
    let goal_id = ids.mint_goal_id();
    let event_id = ids.mint_event_id();
    let key = key_or_generated(idempotency_key, &event_id);
    let mut origin = origin.unwrap_or(GoalOriginWire {
        kind: "cli".to_string(),
        principal: actor.principal.clone(),
        machine: String::new(),
        at: now.to_string(),
        via: via.clone().unwrap_or_else(|| "cli".to_string()),
        agent: actor.agent.clone(),
        unit_prompt_digest: None,
        root_prompt_digest: None,
    });
    if via.is_some() {
        origin.via = via.unwrap_or_else(|| "cli".to_string());
    }
    let payload = GoalCreatedPayloadWire {
        title,
        outcome,
        criteria,
        origin: Some(origin),
        draft: false,
        project,
    };
    Ok(vec![envelope(
        &goal_id,
        event_id,
        GoalEventKindWire::Created,
        None,
        key,
        actor,
        now,
        to_payload(&payload),
    )])
}

#[allow(clippy::too_many_arguments)]
fn plan_edit(
    state: Option<&GoalStateWire>,
    goal_id: &str,
    title: Option<String>,
    outcome: Option<String>,
    criteria_added: &[GoalCriterionInputWire],
    criteria_removed: &[String],
    note: Option<String>,
    expected_head: Option<String>,
    idempotency_key: Option<String>,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    let Some(state) = state else {
        return Err(GoalRefusalWire::new(
            "goal_not_found",
            format!("goal {goal_id} has no state"),
        ));
    };
    if state.status.is_settled() {
        return Err(GoalRefusalWire::new(
            "already_settled",
            format!("goal {goal_id} is already {}", state.status.as_str()),
        ));
    }
    check_head(state, expected_head)?;
    // Plan against the normalized id so the envelope and the write
    // paths never fork on letter case.
    let goal_id =
        parse_goal_id(goal_id).unwrap_or_else(|_| goal_id.to_string());
    let title = title.map(|value| check_title(&value)).transpose()?;
    let outcome = outcome.map(|value| check_outcome(&value)).transpose()?;
    // Every removed id must name a criterion currently on the goal;
    // the reducer's `unknown_criterion` diagnostic is history repair,
    // not validation.
    for removed in criteria_removed {
        if !state.criteria.iter().any(|known| &known.id == removed) {
            return Err(GoalRefusalWire::new(
                "criterion_not_found",
                format!("criterion {removed} is not on goal {}", state.id,),
            ));
        }
    }
    // The cap counts current criteria, minus valid removals, plus
    // additions.
    let criteria_added = check_criteria(
        criteria_added,
        state.criteria.len().saturating_sub(criteria_removed.len()),
    )?;
    let note = note
        .map(|value| {
            let note = trimmed(&value);
            if note.chars().count() > GOAL_NOTE_MAX {
                return Err(GoalRefusalWire::new(
                    "note_too_long",
                    format!(
                        "note is {} chars; \
                         want at most {GOAL_NOTE_MAX}",
                        note.chars().count(),
                    ),
                ));
            }
            Ok(if note.is_empty() { None } else { Some(note) })
        })
        .transpose()?
        .flatten();
    // An edit whose every supplied field already equals the current
    // value changes nothing.
    let title_same = title.as_deref().is_none_or(|value| value == state.title);
    let outcome_same = outcome
        .as_deref()
        .is_none_or(|value| value == state.outcome);
    if title_same
        && outcome_same
        && criteria_added.is_empty()
        && criteria_removed.is_empty()
        && note.is_none()
    {
        return Err(GoalRefusalWire::new("no_changes", "edit changes nothing"));
    }
    let event_id = ids.mint_event_id();
    let key = key_or_generated(idempotency_key, &event_id);
    let payload = GoalEditedPayloadWire {
        title,
        outcome,
        criteria_added,
        criteria_removed: criteria_removed.to_vec(),
        note,
    };
    Ok(vec![envelope(
        &goal_id,
        event_id.clone(),
        GoalEventKindWire::Edited,
        state.head.clone(),
        key,
        actor,
        now,
        to_payload(&payload),
    )])
}

#[allow(clippy::too_many_arguments)]
fn plan_drop(
    state: Option<&GoalStateWire>,
    goal_id: &str,
    why: &str,
    expected_head: Option<String>,
    idempotency_key: Option<String>,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    let Some(state) = state else {
        return Err(GoalRefusalWire::new(
            "goal_not_found",
            format!("goal {goal_id} has no state"),
        ));
    };
    if state.status.is_settled() {
        return Err(GoalRefusalWire::new(
            "already_settled",
            format!("goal {goal_id} is already {}", state.status.as_str()),
        ));
    }
    check_head(state, expected_head)?;
    let goal_id =
        parse_goal_id(goal_id).unwrap_or_else(|_| goal_id.to_string());
    let why = trimmed(why);
    if why.is_empty() {
        return Err(GoalRefusalWire::new(
            "why_required",
            "drop needs a reason",
        ));
    }
    if why.chars().count() > GOAL_NOTE_MAX {
        return Err(GoalRefusalWire::new(
            "why_too_long",
            format!(
                "reason is {} chars; want at most {GOAL_NOTE_MAX}",
                why.chars().count(),
            ),
        ));
    }
    let event_id = ids.mint_event_id();
    let key = key_or_generated(idempotency_key, &event_id);
    let payload = GoalSettledPayloadWire {
        flavor: Some(super::wire::GoalSettleFlavorWire::Canceled),
        into: None,
        note: Some(why),
    };
    Ok(vec![envelope(
        &goal_id,
        event_id.clone(),
        GoalEventKindWire::Settled,
        state.head.clone(),
        key,
        actor,
        now,
        to_payload(&payload),
    )])
}

#[allow(clippy::too_many_arguments)]
fn plan_reopen(
    state: Option<&GoalStateWire>,
    goal_id: &str,
    message: &str,
    expected_head: Option<String>,
    idempotency_key: Option<String>,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    let Some(state) = state else {
        return Err(GoalRefusalWire::new(
            "goal_not_found",
            format!("goal {goal_id} has no state"),
        ));
    };
    if state.status.is_unsettled() {
        return Err(GoalRefusalWire::new(
            "not_settled",
            format!(
                "goal {goal_id} is {}; reopen needs a settled goal",
                state.status.as_str()
            ),
        ));
    }
    check_head(state, expected_head)?;
    let goal_id =
        parse_goal_id(goal_id).unwrap_or_else(|_| goal_id.to_string());
    let message = check_message(message)?;
    let event_id = ids.mint_event_id();
    let key = key_or_generated(idempotency_key, &event_id);
    let payload = GoalReopenedPayloadWire { message };
    Ok(vec![envelope(
        &goal_id,
        event_id.clone(),
        GoalEventKindWire::Reopened,
        state.head.clone(),
        key,
        actor,
        now,
        to_payload(&payload),
    )])
}

#[allow(clippy::too_many_arguments)]
fn plan_merge(
    state: Option<&GoalStateWire>,
    source_id: &str,
    target_id: &str,
    target_state: Option<&GoalStateWire>,
    why: Option<String>,
    expected_head: Option<String>,
    idempotency_key: Option<String>,
    actor: &GoalActorWire,
    now: &str,
    ids: &mut dyn GoalIdMint,
) -> Result<Vec<GoalEventWire>, GoalRefusalWire> {
    // Compare normalized ids so `ABCDE` into `abcde` is still itself.
    let source_id =
        parse_goal_id(source_id).unwrap_or_else(|_| source_id.to_string());
    let target_id =
        parse_goal_id(target_id).unwrap_or_else(|_| target_id.to_string());
    if source_id == target_id {
        return Err(GoalRefusalWire::new(
            "merge_into_self",
            "a goal cannot merge into itself",
        ));
    }
    let Some(state) = state else {
        return Err(GoalRefusalWire::new(
            "goal_not_found",
            format!("source goal {source_id} has no state"),
        ));
    };
    if state.status.is_settled() {
        return Err(GoalRefusalWire::new(
            "already_settled",
            format!(
                "source goal {source_id} is already {}",
                state.status.as_str()
            ),
        ));
    }
    check_head(state, expected_head)?;
    let Some(target) = target_state else {
        return Err(GoalRefusalWire::new(
            "target_not_found",
            format!("target goal {target_id} has no state"),
        ));
    };
    if target.status.is_settled() {
        return Err(GoalRefusalWire::new(
            "target_settled",
            format!(
                "target goal {target_id} is already {}",
                target.status.as_str()
            ),
        ));
    }
    let why = why
        .map(|value| {
            let why = trimmed(&value);
            if why.chars().count() > GOAL_NOTE_MAX {
                return Err(GoalRefusalWire::new(
                    "why_too_long",
                    format!(
                        "reason is {} chars; \
                         want at most {GOAL_NOTE_MAX}",
                        why.chars().count(),
                    ),
                ));
            }
            Ok(if why.is_empty() { None } else { Some(why) })
        })
        .transpose()?
        .flatten();
    let settle_id = ids.mint_event_id();
    let record_id = ids.mint_event_id();
    let settle_key = key_or_generated(idempotency_key.clone(), &settle_id);
    let record_key = key_or_generated(idempotency_key, &record_id);
    let settle = GoalSettledPayloadWire {
        flavor: Some(super::wire::GoalSettleFlavorWire::Merged),
        into: Some(target_id.to_string()),
        note: None,
    };
    let record = GoalMergedPayloadWire {
        from: source_id.to_string(),
        why,
    };
    Ok(vec![
        envelope(
            &source_id,
            settle_id.clone(),
            GoalEventKindWire::Settled,
            state.head.clone(),
            settle_key,
            actor,
            now,
            to_payload(&settle),
        ),
        envelope(
            &target_id,
            record_id.clone(),
            GoalEventKindWire::Merged,
            target.head.clone(),
            record_key,
            actor,
            now,
            to_payload(&record),
        ),
    ])
}

/// Serialize a planned payload back to JSON.
pub fn payload_to_json(payload: &GoalEventPayloadWire) -> serde_json::Value {
    match payload {
        GoalEventPayloadWire::Created(inner) => to_payload(inner),
        GoalEventPayloadWire::Named(inner) => to_payload(inner),
        GoalEventPayloadWire::Edited(inner) => to_payload(inner),
        GoalEventPayloadWire::Adopted(inner) => to_payload(inner),
        GoalEventPayloadWire::AgentAttached(inner) => to_payload(inner),
        GoalEventPayloadWire::Progress(inner) => to_payload(inner),
        GoalEventPayloadWire::PlanAttached(inner) => to_payload(inner),
        GoalEventPayloadWire::Claimed(inner) => to_payload(inner),
        GoalEventPayloadWire::ClaimRetracted(inner) => to_payload(inner),
        GoalEventPayloadWire::Settled(inner) => to_payload(inner),
        GoalEventPayloadWire::Reopened(inner) => to_payload(inner),
        GoalEventPayloadWire::Merged(inner) => to_payload(inner),
    }
}
