//! Goal ledger appends: locked, marker-superset-ordered event writes.
//!
//! [`goal_ledger_append`] takes the caller-supplied lock, reduces the
//! target goal(s), plans the action, and writes in marker-superset order:
//! markers for goals that become unsettled are created _before_ the event
//! write, and markers for goals that settle are removed _after_ it. A
//! crash at any point leaves `live/` as a superset of the unsettled set,
//! which readers tolerate and `doctor --repair` converges.
//!
//! A test-only fault hook (`fault_after_event_write`) stops between the
//! event write and the marker step. It is never reachable from the CLI;
//! `acceptance` uses it for crash tests.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::super::actions::{
    GoalActionWire, GoalIdMint, GoalRefusalWire, OsGoalIdMint,
};
use super::super::ids::{mint_event_id, parse_goal_id};
use super::super::reduce::reduce_goal_events;
use super::super::wire::{GoalActorWire, GoalEventWire, GoalStateWire};
use super::layout::{
    goal_events_dir, goal_ledger_init, goal_marker_path, goal_now_rfc3339,
    read_goal_store, GoalLedgerError,
};
use crate::fs_sig::write_json_atomic;
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
};

/// Default ledger-lock timeout in seconds.
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(10);

/// Environment override for the ledger-lock timeout.
const LOCK_TIMEOUT_ENV: &str = "SASE_GOAL_LEDGER_LOCK_TIMEOUT";

/// Outcome status when events were planned and written.
pub const GOAL_APPEND_APPLIED: &str = "applied";
/// Outcome status when validation refused the action.
pub const GOAL_APPEND_REFUSED: &str = "refused";
/// Outcome status when `expected_head` drifted on a non-commutative action.
pub const GOAL_APPEND_STALE_BASIS: &str = "stale_basis";

/// Request to append one action's events to the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerAppendRequestWire {
    /// The action to plan and write.
    pub action: GoalActionWire,
    /// The actor credited with the events.
    #[serde(default)]
    pub actor: GoalActorWire,
    /// Expected head when the action variant carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_head: Option<String>,
    /// Idempotency key when the action variant carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Lock file path. Defaults to a `<root>.lock` sibling, outside the
    /// tracked tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_path: Option<String>,
    /// RFC3339 clock reading. Defaults to now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now: Option<String>,
    /// Test-only goal-id mint override for `new`. Never from the CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_goal_id: Option<String>,
    /// Test-only crash hook: stop after the event write, before the
    /// marker step. Never from the CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault_after_event_write: Option<bool>,
}

/// Outcome of [`goal_ledger_append`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerAppendOutcomeWire {
    /// `applied`, `refused`, or `stale_basis`.
    pub status: String,
    /// Stable refusal code when not applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable detail when not applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Planned events (empty unless applied).
    #[serde(default)]
    pub events: Vec<GoalEventWire>,
    /// Ledger-relative paths created by this call.
    #[serde(default)]
    pub created_paths: Vec<String>,
    /// Ledger-relative paths removed by this call.
    #[serde(default)]
    pub removed_paths: Vec<String>,
    /// Post-apply states of the touched goals (current states on
    /// `stale_basis`).
    #[serde(default)]
    pub states: Vec<GoalStateWire>,
}

/// Mint that honors a test-only goal-id override.
struct AppendMint {
    goal_override: Option<String>,
}

impl GoalIdMint for AppendMint {
    fn mint_goal_id(&mut self) -> String {
        if let Some(id) = self.goal_override.take() {
            return id;
        }
        OsGoalIdMint.mint_goal_id()
    }

    fn mint_event_id(&mut self) -> String {
        mint_event_id()
    }
}

/// Default lock path: a `<root>.lock` sibling outside the tracked tree.
fn default_lock_path(root: &Path) -> PathBuf {
    let filename = root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("goals");
    root.with_file_name(format!("{filename}.lock"))
}

fn lock_error(error: crate::store_lock::StoreLockError) -> GoalLedgerError {
    GoalLedgerError::Lock(error.to_string())
}

/// Goal ids this action reads or writes, before planning.
fn action_goal_ids(action: &GoalActionWire) -> Vec<String> {
    match action {
        GoalActionWire::New { .. } => Vec::new(),
        GoalActionWire::Edit { goal_id, .. }
        | GoalActionWire::Drop { goal_id, .. }
        | GoalActionWire::Reopen { goal_id, .. } => vec![goal_id.clone()],
        GoalActionWire::Merge {
            source_id,
            target_id,
            ..
        } => vec![source_id.clone(), target_id.clone()],
    }
}

/// Fill variant-level `expected_head` / `idempotency_key` from the request.
fn fill_action_defaults(
    action: &mut GoalActionWire,
    request: &GoalLedgerAppendRequestWire,
) {
    let fill_head = |slot: &mut Option<String>| {
        if slot.is_none() {
            *slot = request.expected_head.clone();
        }
    };
    let fill_key = |slot: &mut Option<String>| {
        if slot.is_none() {
            *slot = request.idempotency_key.clone();
        }
    };
    match action {
        GoalActionWire::New {
            idempotency_key, ..
        } => {
            fill_key(idempotency_key);
        }
        GoalActionWire::Edit {
            expected_head,
            idempotency_key,
            ..
        }
        | GoalActionWire::Drop {
            expected_head,
            idempotency_key,
            ..
        }
        | GoalActionWire::Reopen {
            expected_head,
            idempotency_key,
            ..
        } => {
            fill_head(expected_head);
            fill_key(idempotency_key);
        }
        GoalActionWire::Merge {
            expected_head,
            idempotency_key,
            ..
        } => {
            fill_head(expected_head);
            fill_key(idempotency_key);
        }
    }
}

/// Clear variant-level `expected_head` for the commutative re-plan retry.
fn clear_expected_head(action: &mut GoalActionWire) {
    match action {
        GoalActionWire::New { .. } => {}
        GoalActionWire::Edit { expected_head, .. }
        | GoalActionWire::Drop { expected_head, .. }
        | GoalActionWire::Reopen { expected_head, .. }
        | GoalActionWire::Merge { expected_head, .. } => {
            *expected_head = None;
        }
    }
}

/// Commutative content edits (criteria additions and notes) proceed across
/// a moved head; everything else reports `stale_basis`.
fn is_commutative_edit(action: &GoalActionWire) -> bool {
    match action {
        GoalActionWire::Edit {
            title,
            outcome,
            criteria_removed,
            ..
        } => {
            title.is_none() && outcome.is_none() && criteria_removed.is_empty()
        }
        _ => false,
    }
}

/// Load and reduce one goal. Missing event directories reduce to an empty
/// state so `new` can plan against `None` and readers report unknown ids.
fn load_goal_state(
    root: &Path,
    goal_id: &str,
    probe: Option<&mut super::probe::GoalLedgerProbeCountsWire>,
) -> Result<(GoalStateWire, Vec<GoalEventWire>), GoalLedgerError> {
    let events = super::read::read_goal_events(root, goal_id, probe)?;
    let state = reduce_goal_events(goal_id, &events);
    Ok((state, events))
}

fn refused_outcome(refusal: &GoalRefusalWire) -> GoalLedgerAppendOutcomeWire {
    GoalLedgerAppendOutcomeWire {
        status: GOAL_APPEND_REFUSED.to_string(),
        code: Some(refusal.code.clone()),
        message: Some(refusal.message.clone()),
        events: Vec::new(),
        created_paths: Vec::new(),
        removed_paths: Vec::new(),
        states: Vec::new(),
    }
}

/// Append one action's events to the ledger.
///
/// Returns `{events, created_paths, removed_paths, states}` on success so
/// Python can commit exactly those paths.
pub fn goal_ledger_append(
    root: &Path,
    request: &GoalLedgerAppendRequestWire,
) -> Result<GoalLedgerAppendOutcomeWire, GoalLedgerError> {
    read_goal_store(root)?;
    let now = request.now.clone().unwrap_or_else(goal_now_rfc3339);
    let lock_path = request
        .lock_path
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| default_lock_path(root));
    let holder_path = holder_path_for(&lock_path);
    let _lock = acquire_store_lock(
        &lock_path,
        &holder_path,
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        "goal_ledger_append",
    )
    .map_err(lock_error)?;

    let mut action = request.action.clone();
    fill_action_defaults(&mut action, request);

    // Normalize and validate the touched ids up front.
    let mut touched: Vec<String> = Vec::new();
    for raw in action_goal_ids(&action) {
        let parsed = parse_goal_id(&raw)
            .map_err(|error| GoalLedgerError::InvalidId(error.to_string()))?;
        if !touched.contains(&parsed) {
            touched.push(parsed);
        }
    }
    if let GoalActionWire::New { .. } = &action {
        // The id is minted during planning below.
    }
    // For `new`, pre-validate the mint override when present.
    if let Some(override_id) = request.new_goal_id.as_ref() {
        parse_goal_id(override_id)
            .map_err(|error| GoalLedgerError::InvalidId(error.to_string()))?;
    }

    // Load current states. `merge` also refreshes the embedded target
    // state so planning sees this lock's view of both goals.
    let mut states: Vec<GoalStateWire> = Vec::new();
    let mut event_sets: Vec<Vec<GoalEventWire>> = Vec::new();
    for goal_id in &touched {
        let (state, events) = load_goal_state(root, goal_id, None)?;
        states.push(state);
        event_sets.push(events);
    }
    let source_state = |touched: &[String],
                        states: &[GoalStateWire],
                        want: &str|
     -> Option<GoalStateWire> {
        touched
            .iter()
            .position(|id| id == want)
            .and_then(|idx| states.get(idx).cloned())
    };
    if let GoalActionWire::Merge { target_id, .. } = &action {
        let parsed = parse_goal_id(target_id)
            .map_err(|error| GoalLedgerError::InvalidId(error.to_string()))?;
        if let Some(target_state) = source_state(&touched, &states, &parsed) {
            if let GoalActionWire::Merge {
                target_state: slot, ..
            } = &mut action
            {
                *slot = Some(target_state);
            }
        }
    }

    // Plan. On a stale head, commutative content edits re-plan without
    // the expectation; anything else reports `stale_basis`.
    let mut mint = AppendMint {
        goal_override: request.new_goal_id.clone(),
    };
    let source_for_plan: Option<GoalStateWire> = match &action {
        GoalActionWire::New { .. } => None,
        GoalActionWire::Edit { goal_id, .. }
        | GoalActionWire::Drop { goal_id, .. }
        | GoalActionWire::Reopen { goal_id, .. } => {
            let parsed = parse_goal_id(goal_id).map_err(|error| {
                GoalLedgerError::InvalidId(error.to_string())
            })?;
            source_state(&touched, &states, &parsed)
        }
        GoalActionWire::Merge { source_id, .. } => {
            let parsed = parse_goal_id(source_id).map_err(|error| {
                GoalLedgerError::InvalidId(error.to_string())
            })?;
            source_state(&touched, &states, &parsed)
        }
    };
    // An action on a goal with no events plans against an empty state;
    // `plan_goal_action` refuses or plans as its rules dictate.
    let empty;
    let plan_state = match source_for_plan.as_ref() {
        Some(state) => state,
        None => {
            empty = match &action {
                GoalActionWire::New { .. } => GoalStateWire::empty(""),
                _ => {
                    let raw = action_goal_ids(&action)
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    let parsed = parse_goal_id(&raw).unwrap_or_default();
                    GoalStateWire::empty(&parsed)
                }
            };
            &empty
        }
    };
    let plan_state_opt = match &action {
        GoalActionWire::New { .. } => None,
        _ => Some(plan_state),
    };
    let mut planned = match super::super::actions::plan_goal_action(
        plan_state_opt,
        &action,
        &request.actor,
        &now,
        &mut mint,
    ) {
        Ok(events) => events,
        Err(refusal) if refusal.code == "stale_basis" => {
            if is_commutative_edit(&action) {
                clear_expected_head(&mut action);
                match super::super::actions::plan_goal_action(
                    plan_state_opt,
                    &action,
                    &request.actor,
                    &now,
                    &mut mint,
                ) {
                    Ok(events) => events,
                    Err(retry) => {
                        let mut outcome = refused_outcome(&retry);
                        outcome.states = states;
                        return Ok(outcome);
                    }
                }
            } else {
                return Ok(GoalLedgerAppendOutcomeWire {
                    status: GOAL_APPEND_STALE_BASIS.to_string(),
                    code: Some(refusal.code.clone()),
                    message: Some(refusal.message.clone()),
                    events: Vec::new(),
                    created_paths: Vec::new(),
                    removed_paths: Vec::new(),
                    states,
                });
            }
        }
        Err(refusal) => {
            let mut outcome = refused_outcome(&refusal);
            outcome.states = states;
            return Ok(outcome);
        }
    };
    if planned.is_empty() {
        let refusal =
            GoalRefusalWire::new("no_events", "the action planned no events");
        let mut outcome = refused_outcome(&refusal);
        outcome.states = states;
        return Ok(outcome);
    }

    // The set of goals these events touch.
    let mut event_goals: BTreeSet<String> = BTreeSet::new();
    for event in &planned {
        event_goals.insert(event.goal_id.clone());
    }

    // Marker-superset pre-step: goals that become unsettled get their
    // marker before the event write. `created` and `reopened` events
    // unsettle, so pre-create markers for their goals.
    let mut created_paths: Vec<String> = Vec::new();
    for event in &planned {
        let unsettles = matches!(
            event.kind,
            super::super::wire::GoalEventKindWire::Created
                | super::super::wire::GoalEventKindWire::Reopened
                | super::super::wire::GoalEventKindWire::Named
        );
        if unsettles {
            let marker = goal_marker_path(root, &event.goal_id);
            if !marker.exists() {
                fs::write(&marker, b"").map_err(|error| {
                    GoalLedgerError::Io(format!(
                        "failed to write marker {}: {error}",
                        marker.display()
                    ))
                })?;
                created_paths.push(relative_display(root, &marker));
            }
        }
    }

    // Write the events atomically: temp-file + fsync + rename per file.
    for event in &mut planned {
        let dir = goal_events_dir(root, &event.goal_id);
        fs::create_dir_all(&dir).map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to create {}: {error}",
                dir.display()
            ))
        })?;
        let path = dir.join(format!("{}.json", event.event_id));
        if path.exists() {
            // Redelivery of the same event file is a no-op: the reducer
            // dedupes by event id and idempotency key.
            continue;
        }
        write_json_atomic(&path, &serde_json::to_value(event)?).map_err(
            |error| {
                GoalLedgerError::Io(format!(
                    "failed to write {}: {error}",
                    path.display()
                ))
            },
        )?;
        created_paths.push(relative_display(root, &path));
    }

    // Test-only fault hook: stop before the marker step.
    if request.fault_after_event_write.unwrap_or(false) {
        let mut post_states = Vec::new();
        for goal_id in &event_goals {
            let (state, _) = load_goal_state(root, goal_id, None)?;
            post_states.push(state);
        }
        return Ok(GoalLedgerAppendOutcomeWire {
            status: GOAL_APPEND_APPLIED.to_string(),
            code: None,
            message: Some("fault_after_event_write".to_string()),
            events: planned,
            created_paths,
            removed_paths: Vec::new(),
            states: post_states,
        });
    }

    // Marker-superset post-step: reconcile markers for the touched goals
    // only. Unsettled goals keep (or gain) a marker; settled goals lose
    // theirs. Extras elsewhere are left for `doctor --repair`.
    let mut removed_paths: Vec<String> = Vec::new();
    let mut post_states = Vec::new();
    for goal_id in &event_goals {
        let (state, _) = load_goal_state(root, goal_id, None)?;
        let marker = goal_marker_path(root, goal_id);
        if state.status.is_unsettled() {
            if !marker.exists() {
                fs::write(&marker, b"").map_err(|error| {
                    GoalLedgerError::Io(format!(
                        "failed to write marker {}: {error}",
                        marker.display()
                    ))
                })?;
                created_paths.push(relative_display(root, &marker));
            }
        } else if marker.exists() {
            fs::remove_file(&marker).map_err(|error| {
                GoalLedgerError::Io(format!(
                    "failed to remove marker {}: {error}",
                    marker.display()
                ))
            })?;
            removed_paths.push(relative_display(root, &marker));
        }
        post_states.push(state);
    }

    created_paths.sort();
    created_paths.dedup();
    removed_paths.sort();
    removed_paths.dedup();
    Ok(GoalLedgerAppendOutcomeWire {
        status: GOAL_APPEND_APPLIED.to_string(),
        code: None,
        message: None,
        events: planned,
        created_paths,
        removed_paths,
        states: post_states,
    })
}

/// Ledger-relative display path for commit pathspecs.
fn relative_display(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) => relative.display().to_string(),
        Err(_) => path.display().to_string(),
    }
}

/// Initialize a ledger root. Thin wrapper so bindings import one module.
pub fn goal_ledger_init_root(
    root: &Path,
) -> Result<super::layout::GoalLedgerInitWire, GoalLedgerError> {
    goal_ledger_init(root)
}
