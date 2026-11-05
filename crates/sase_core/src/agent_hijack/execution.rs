//! Reservation-aware execution validation and confirmation policy.
//!
//! The public preview menu deliberately disables keep-work actions on
//! resolving records and clears their row targets, so execution cannot
//! revalidate through the menu: it validates the winning reservation
//! directly. Held request preparation checks the effective pinned target
//! against current availability; resolving dispatch additionally proves the
//! caller owns the exact reservation (resolver pid plus process identity
//! token) and that the stored attempt still names the pinned target.
//!
//! This module also consolidates the shared target precedence previously
//! embedded in Python (`recover_target.py`): an explicit model override wins,
//! otherwise an edited prompt's pre-extracted `%model` value wins, otherwise
//! the predecessor's stalled concrete target is kept. Provider inheritance
//! and effort retention normalize bare values; the directive parser itself
//! stays in Python and its already-extracted `%model` string is the input.
//!
//! Confirmation and safe-rollback decisions are pure observations over proven
//! facts. Missing evidence is uncertain, never successful, and neither
//! decision imposes the reconciliation 60-second stale-resolver delay: the
//! normal transaction completes while its own resolver is live.
//!
//! Python owns file I/O, locks, signals, launches, and admission; this module
//! owns the policy. Mirrors the `agent_auto_restart` split.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wire::*;
use crate::effort::split_model_effort;

/// Wire schema version for every execution/confirmation/rollback type here.
pub const AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Execution decision outcomes.
pub const HIJACK_EXECUTION_PROCEED: &str = "proceed";
pub const HIJACK_EXECUTION_REFUSE: &str = "refuse";

/// Execution decision modes: held preparation versus resolving dispatch.
pub const HIJACK_EXECUTION_MODE_PREPARE: &str = "prepare";
pub const HIJACK_EXECUTION_MODE_DISPATCH: &str = "dispatch";

/// Confirmation decision outcomes.
pub const HIJACK_CONFIRM_SETTLE: &str = "settle";
pub const HIJACK_CONFIRM_RETAIN: &str = "retain";

/// Rollback decision outcomes.
pub const HIJACK_ROLLBACK_RETURN_TO_HELD: &str = "return_to_held";
pub const HIJACK_ROLLBACK_RETAIN: &str = "retain";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackExecutionError {
    #[error("unsupported hijack recovery execution wire schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("unsupported recovery action {0:?}")]
    UnsupportedAction(String),
    #[error("recovery action is unavailable: {0}")]
    ActionUnavailable(String),
    #[error("predecessor model is unknown: {0}")]
    UnknownModel(String),
    #[error("invalid recovery model target ({origin}): {message}")]
    InvalidModelTarget { origin: String, message: String },
    #[error("incompatible options for {action}: {message}")]
    IncompatibleOptions { action: String, message: String },
    #[error("missing recovery prompt: {0}")]
    MissingPrompt(String),
}

/// Effective-target request: shared precedence without a new parser.
///
/// `prompt_model` is the already-extracted `%model` value from the edited
/// Instruct prompt (effort already merged by the directive parser), not raw
/// prompt text. `continue_target` and `escalate_target` are the previewed
/// row targets; the stalled concrete target is the predecessor's pinned
/// `provider/model[@effort]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackEffectiveTargetRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub stalled_concrete: String,
    #[serde(default)]
    pub continue_target: Option<String>,
    #[serde(default)]
    pub escalate_target: Option<String>,
    #[serde(default)]
    pub explicit_model_override: Option<String>,
    #[serde(default)]
    pub prompt_model: Option<String>,
    #[serde(default)]
    pub predecessor_provider: Option<String>,
}

/// Effective-target result with the pinned concrete target.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackEffectiveTargetResultWire {
    pub schema_version: u32,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub concrete_target: String,
    #[serde(default)]
    pub model_changed: bool,
}

/// Execution validation facts gathered at the dispatch site.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackExecutionFactsWire {
    pub schema_version: u32,
    /// Pid observed by the caller attempting dispatch.
    #[serde(default)]
    pub observed_resolver_pid: Option<i64>,
    /// Whether that pid is live right now.
    #[serde(default)]
    pub resolver_pid_live: bool,
    /// Whether the live pid's token matches the recorded resolver token.
    #[serde(default)]
    pub resolver_identity_matches: bool,
    /// Whether the pinned target is still offered by current availability.
    #[serde(default)]
    pub pinned_available: bool,
    #[serde(default)]
    pub pinned_unavailable_reason: Option<String>,
    /// Freshly computed current target; must equal the pinned target.
    #[serde(default)]
    pub current_target: Option<String>,
    /// Observed provider disablement; absent means none observed.
    #[serde(default)]
    pub provider_disabled: bool,
    /// Observed model unavailability; absent means none observed.
    #[serde(default)]
    pub model_unavailable: bool,
}

/// Execution validation decision. Held preparation never checks resolver
/// ownership; resolving dispatch refuses anyone but the exact reservation
/// owner. No menu state is consulted or mutated.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackExecutionDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Confirmation facts: every flag must be proven true to settle.
///
/// Python proves identity by comparing the observed successor's launch
/// identity against the recorded reservation, model by comparing the
/// observed effective concrete target, workspace and lineage by comparing
/// the observed successor facts, and the ack by reading the versioned
/// bootstrap ack bound to the launch identity. A still-queued or
/// still-dispatching admission request retains regardless of the rest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackConfirmationFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub launch_identity_matches: bool,
    #[serde(default)]
    pub model_matches: bool,
    #[serde(default)]
    pub workspace_matches: bool,
    #[serde(default)]
    pub lineage_matches: bool,
    #[serde(default)]
    pub ack_bound: bool,
    #[serde(default)]
    pub ack_matches: bool,
    /// The admission request is still queued or dispatching.
    #[serde(default)]
    pub request_in_flight: bool,
    /// Any successor was observed at all (diagnostics only).
    #[serde(default)]
    pub successor_observed: bool,
}

/// Confirmation decision: settle only on fully proven evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackConfirmationDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub successor_name: Option<String>,
    #[serde(default)]
    pub launch_identity: Option<String>,
}

/// Safe-rollback facts: returning to held requires a verified-dead child,
/// restored custody, and a request that cannot dispatch.
///
/// `child_state` is `absent`, `dead`, `live`, or `uncertain`. `custody` is
/// `restored`, `foreign`, or `uncertain`. `request_state` is `none`,
/// `canceled`, `definite_prelaunch_failure`, `dispatching`, `queued`, or
/// `uncertain`. Unknown values retain.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRollbackFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub child_state: String,
    #[serde(default)]
    pub custody: String,
    #[serde(default)]
    pub request_state: String,
}

/// Safe-rollback decision: only verified death plus restored custody plus a
/// non-dispatchable request permits a fresh attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRollbackDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub successor_name: Option<String>,
    #[serde(default)]
    pub launch_identity: Option<String>,
}

/// Minimal versioned bootstrap ack bound to the reserved launch identity.
///
/// Persisted by the successor only after all required inheritance effects
/// finish and before provider execution. A resolver must read this evidence;
/// it must never manufacture a successful child ack from incomplete
/// parent-side observations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackBootstrapAckWire {
    pub schema_version: u32,
    #[serde(default)]
    pub launch_identity: String,
    #[serde(default)]
    pub successor_name: String,
    #[serde(default)]
    pub process_token: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: String,
    #[serde(default)]
    pub lineage: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub effort: String,
    #[serde(default)]
    pub concrete_target: String,
    #[serde(default)]
    pub goal_verified: bool,
    #[serde(default)]
    pub bead_verified: bool,
    #[serde(default)]
    pub scope_verified: bool,
    #[serde(default)]
    pub baselines_verified: bool,
    #[serde(default)]
    pub inventory_verified: bool,
}

/// Minimal durable admission descriptor for the recovery attempt.
///
/// One validated descriptor per CAS reservation: the prepared prompt digest
/// and exact target, the reserved member/attach plan, the fixed project and
/// workspace pair, the expected held owner, lineage, and the attempt
/// identities. Descriptor storage and ledger ordering must be repairable
/// across interruption without a parallel queue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackAdmissionDescriptorWire {
    pub schema_version: u32,
    #[serde(default)]
    pub hijack_id: String,
    #[serde(default)]
    pub launch_identity: String,
    #[serde(default)]
    pub admission_request_id: Option<String>,
    #[serde(default)]
    pub admission_identity: Option<String>,
    #[serde(default)]
    pub concrete_target: String,
    #[serde(default)]
    pub reserved_successor_name: String,
    #[serde(default)]
    pub reserved_member: String,
    #[serde(default)]
    pub attach_plan: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: String,
    #[serde(default)]
    pub expected_owner_pid: Option<i64>,
    #[serde(default)]
    pub lineage: String,
    #[serde(default)]
    pub resolver_pid: Option<i64>,
    #[serde(default)]
    pub resolver_process_identity_token: Option<String>,
    #[serde(default)]
    pub prompt_digest: String,
    #[serde(default)]
    pub attempt_revision: u64,
}

/// Resolve the effective concrete target under the shared precedence.
///
/// Explicit model override wins, otherwise the edited prompt's pre-extracted
/// `%model` value wins, otherwise the stalled concrete target is kept.
/// Bare `model[@effort]` values inherit the predecessor provider and missing
/// effort is retained from the stalled target. Pool aliases (a leading `@`)
/// stay unresolved and refuse as non-concrete.
pub fn hijack_recovery_effective_target(
    request: &AgentHijackEffectiveTargetRequestWire,
) -> Result<AgentHijackEffectiveTargetResultWire, AgentHijackExecutionError> {
    if request.schema_version != AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            request.schema_version,
        ));
    }
    let action = request.action.trim().to_owned();
    if !matches!(action.as_str(), "escalate" | "continue" | "instruct") {
        return Err(AgentHijackExecutionError::UnsupportedAction(
            request.action.clone(),
        ));
    }
    let stalled_raw = request.stalled_concrete.trim();
    if stalled_raw.is_empty() {
        return Err(AgentHijackExecutionError::UnknownModel(
            "the predecessor's concrete model is unknown".to_owned(),
        ));
    }
    let stalled = require_concrete_target(stalled_raw, "predecessor record")?;
    let (_, stalled_effort) = split_model_effort(&stalled);
    let provider = request.predecessor_provider.as_deref().unwrap_or("");

    let concrete = match action.as_str() {
        "continue" => {
            reject_override_options(
                &action,
                request.explicit_model_override.as_deref(),
                request.prompt_model.as_deref(),
                "Continue takes no model override or replacement prompt",
            )?;
            let pinned = nonempty(request.continue_target.as_deref())
                .unwrap_or(stalled.as_str());
            require_concrete_target(pinned, "continue preview")?
        }
        "escalate" => {
            reject_override_options(
                &action,
                request.explicit_model_override.as_deref(),
                request.prompt_model.as_deref(),
                "Escalate takes no model override or replacement prompt",
            )?;
            let pinned = nonempty(request.escalate_target.as_deref())
                .ok_or_else(|| {
                    AgentHijackExecutionError::ActionUnavailable(
                        "Escalate has no pinned target".to_owned(),
                    )
                })?;
            require_concrete_target(pinned, "escalate preview")?
        }
        _ => {
            if let Some(override_value) =
                nonempty(request.explicit_model_override.as_deref())
            {
                let normalized = normalize_target_candidate(
                    override_value,
                    provider,
                    stalled_effort,
                );
                require_concrete_target(&normalized, "explicit model override")?
            } else if let Some(prompt_value) =
                nonempty(request.prompt_model.as_deref())
            {
                let normalized = normalize_target_candidate(
                    prompt_value,
                    provider,
                    stalled_effort,
                );
                require_concrete_target(&normalized, "instruct prompt %model")?
            } else {
                stalled.clone()
            }
        }
    };

    Ok(AgentHijackEffectiveTargetResultWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        model_changed: concrete != stalled,
        action,
        concrete_target: concrete,
    })
}

/// Validate the winning reservation without reopening the public menu.
///
/// Held records check the pinned target against current availability only.
/// Resolving records additionally require the caller to own the exact
/// reservation and the stored attempt to still name the pinned target.
#[allow(clippy::too_many_lines)]
pub fn hijack_recovery_validate_execution(
    record: &AgentHijackRecordWire,
    pinned_target: &str,
    facts: &AgentHijackExecutionFactsWire,
) -> Result<AgentHijackExecutionDecisionWire, AgentHijackExecutionError> {
    if facts.schema_version != AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            facts.schema_version,
        ));
    }
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            record.schema_version,
        ));
    }
    let pinned = require_concrete_target(pinned_target, "pinned target")?;

    if record.state == HIJACK_HELD {
        return Ok(validate_availability(
            HIJACK_EXECUTION_MODE_PREPARE,
            &pinned,
            facts,
        ));
    }
    if record.state != HIJACK_RESOLVING {
        return Ok(refuse(
            "",
            "not_held",
            &format!(
                "recovery execution requires a held or resolving record (state {})",
                record.state
            ),
        ));
    }

    if !resolver_owns_attempt(record, facts) {
        return Ok(refuse(
            HIJACK_EXECUTION_MODE_DISPATCH,
            "resolver_mismatch",
            "only the owner of the exact reservation can dispatch",
        ));
    }
    if let Some(stored) = nonempty(record.concrete_target_model.as_deref()) {
        if stored != pinned {
            return Ok(refuse(
                HIJACK_EXECUTION_MODE_DISPATCH,
                "target_changed",
                &format!(
                    "the stored attempt target changed ({stored:?} != {pinned:?}); refresh the preview and retry"
                ),
            ));
        }
    }
    Ok(validate_availability(
        HIJACK_EXECUTION_MODE_DISPATCH,
        &pinned,
        facts,
    ))
}

/// Decide whether the observed successor proves settlement.
///
/// Settles only when launch identity, effective model, workspace, lineage,
/// and the versioned bootstrap ack all match the reservation and no
/// admission request is still queued or dispatching. Anything missing or
/// mismatched retains; a name or caller-supplied goal alone never settles.
pub fn hijack_recovery_confirm(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackConfirmationFactsWire,
) -> Result<AgentHijackConfirmationDecisionWire, AgentHijackExecutionError> {
    if facts.schema_version != AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            facts.schema_version,
        ));
    }
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            record.schema_version,
        ));
    }
    if record.state != HIJACK_RESOLVING {
        return Ok(confirm_retain(
            record,
            "not_resolving",
            "confirmation only applies to resolving hijack records",
        ));
    }
    if nonempty(record.launch_identity.as_deref()).is_none() {
        return Ok(confirm_retain(
            record,
            "missing_launch_identity",
            "the reservation recorded no launch identity; confirmation cannot settle",
        ));
    }
    if facts.request_in_flight {
        return Ok(confirm_retain(
            record,
            "request_in_flight",
            "the admission request is still queued or dispatching; custody is not confirmed",
        ));
    }
    if !facts.launch_identity_matches {
        return Ok(confirm_retain(
            record,
            "identity_mismatch",
            "the observed successor identity does not match the reservation",
        ));
    }
    if !facts.model_matches {
        return Ok(confirm_retain(
            record,
            "model_mismatch",
            "the observed effective model does not match the pinned target",
        ));
    }
    if !facts.workspace_matches {
        return Ok(confirm_retain(
            record,
            "workspace_mismatch",
            "the observed workspace does not match the held reservation",
        ));
    }
    if !facts.lineage_matches {
        return Ok(confirm_retain(
            record,
            "lineage_mismatch",
            "the observed lineage does not match the reservation",
        ));
    }
    if !facts.ack_bound {
        return Ok(confirm_retain(
            record,
            "ack_missing",
            "no versioned bootstrap ack bound to the launch identity was observed",
        ));
    }
    if !facts.ack_matches {
        return Ok(confirm_retain(
            record,
            "ack_mismatch",
            "the bootstrap ack evidence does not match the reservation",
        ));
    }
    Ok(AgentHijackConfirmationDecisionWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        outcome: HIJACK_CONFIRM_SETTLE.to_owned(),
        code: Some("proven".to_owned()),
        message: Some(
            "identity, model, workspace, lineage, and bootstrap ack all match the reservation"
                .to_owned(),
        ),
        successor_name: record.reserved_successor_name.clone(),
        launch_identity: record.launch_identity.clone(),
    })
}

/// Decide whether the record may safely return to held.
///
/// Verified-dead (or never-launched absent) children plus restored custody
/// plus a canceled or definitely-failed request permit a fresh attempt. A
/// live, surviving, or unverifiable child, a foreign or uncertain custody, or
/// a queued, dispatching, or otherwise uncertain request retains resolving
/// with a diagnostic.
pub fn hijack_recovery_rollback(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackRollbackFactsWire,
) -> Result<AgentHijackRollbackDecisionWire, AgentHijackExecutionError> {
    if facts.schema_version != AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            facts.schema_version,
        ));
    }
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackExecutionError::UnsupportedSchema(
            record.schema_version,
        ));
    }
    if record.state != HIJACK_RESOLVING {
        return Ok(rollback_retain(
            record,
            "not_resolving",
            "rollback only applies to resolving hijack records",
        ));
    }
    match facts.request_state.trim() {
        "none" | "canceled" | "definite_prelaunch_failure" => {}
        _ => {
            return Ok(rollback_retain(
                record,
                "request_uncertain",
                "the admission request may still dispatch; retaining resolving",
            ));
        }
    }
    match facts.child_state.trim() {
        "absent" | "dead" => {}
        "live" => {
            return Ok(rollback_retain(
                record,
                "child_live",
                "the successor process is still live; a best-effort signal never permits a fresh attempt",
            ));
        }
        _ => {
            return Ok(rollback_retain(
                record,
                "child_uncertain",
                "successor death is unverified (a failed kill, recycled pid, or surviving descendant retains resolving)",
            ));
        }
    }
    match facts.custody.trim() {
        "restored" => {}
        "foreign" => {
            return Ok(rollback_retain(
                record,
                "custody_foreign",
                "workspace custody is foreign; only restored custody permits a fresh attempt",
            ));
        }
        _ => {
            return Ok(rollback_retain(
                record,
                "custody_uncertain",
                "workspace custody restoration is unproven; retaining resolving",
            ));
        }
    }
    Ok(AgentHijackRollbackDecisionWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        outcome: HIJACK_ROLLBACK_RETURN_TO_HELD.to_owned(),
        code: Some("safe".to_owned()),
        message: Some(
            "successor death is verified and custody is restored; a fresh attempt is permitted"
                .to_owned(),
        ),
        successor_name: record.reserved_successor_name.clone(),
        launch_identity: None,
    })
}

fn validate_availability(
    mode: &str,
    pinned: &str,
    facts: &AgentHijackExecutionFactsWire,
) -> AgentHijackExecutionDecisionWire {
    if facts.provider_disabled {
        return refuse(
            mode,
            "provider_disabled",
            "the pinned provider is disabled",
        );
    }
    if facts.model_unavailable {
        return refuse(
            mode,
            "target_unavailable",
            "the pinned target is no longer available",
        );
    }
    if !facts.pinned_available {
        let reason = nonempty(facts.pinned_unavailable_reason.as_deref())
            .unwrap_or("the pinned target is no longer available");
        return refuse(mode, "target_unavailable", reason);
    }
    match nonempty(facts.current_target.as_deref()) {
        None => refuse(
            mode,
            "target_uncertain",
            "current availability produced no target; refusing rather than launching blind",
        ),
        Some(current) if current != pinned => refuse(
            mode,
            "target_changed",
            &format!(
                "the pinned target changed ({current:?} != {pinned:?}); refresh the preview and retry"
            ),
        ),
        Some(_) => AgentHijackExecutionDecisionWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            outcome: HIJACK_EXECUTION_PROCEED.to_owned(),
            mode: mode.to_owned(),
            code: Some(if mode == HIJACK_EXECUTION_MODE_DISPATCH {
                "dispatch_ready"
            } else {
                "prepared"
            }
            .to_owned()),
            message: Some(
                "the pinned target is still available for the reservation owner"
                    .to_owned(),
            ),
        },
    }
}

fn resolver_owns_attempt(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackExecutionFactsWire,
) -> bool {
    let Some(expected_pid) = record.resolver_pid.filter(|pid| *pid > 0) else {
        return false;
    };
    if !facts.resolver_pid_live || !facts.resolver_identity_matches {
        return false;
    }
    if facts.observed_resolver_pid != Some(expected_pid) {
        return false;
    }
    nonempty(record.resolver_process_identity_token.as_deref()).is_some()
}

fn reject_override_options(
    action: &str,
    explicit_override: Option<&str>,
    prompt_model: Option<&str>,
    message: &str,
) -> Result<(), AgentHijackExecutionError> {
    if nonempty(explicit_override).is_some() || nonempty(prompt_model).is_some()
    {
        return Err(AgentHijackExecutionError::IncompatibleOptions {
            action: action.to_owned(),
            message: message.to_owned(),
        });
    }
    Ok(())
}

/// Normalize a bare `model[@effort]` candidate toward concrete form.
///
/// A value without a provider inherits the predecessor provider, mirroring
/// how the stalled target is pinned. A value without effort retains the
/// stalled effort so explicit and prompt overrides keep the effort the
/// predecessor ran with.
fn normalize_target_candidate(
    value: &str,
    predecessor_provider: &str,
    stalled_effort: Option<&str>,
) -> String {
    let trimmed = value.trim().to_owned();
    let (base, effort) = split_model_effort(trimmed.as_str());
    let mut normalized =
        if base.contains('/') || predecessor_provider.trim().is_empty() {
            base.to_owned()
        } else {
            format!("{}/{}", predecessor_provider.trim(), base)
        };
    let effort = effort.or(stalled_effort);
    if let Some(level) = effort {
        normalized.push('@');
        normalized.push_str(level);
    }
    normalized
}

/// Validate a `provider/model[@effort]` concrete target.
fn require_concrete_target(
    value: &str,
    source: &str,
) -> Result<String, AgentHijackExecutionError> {
    let text = value.trim();
    if text.is_empty() {
        return Err(invalid_target(source, "the target is empty"));
    }
    let (base, _) = split_model_effort(text);
    let invalid = |message: &str| invalid_target(source, message);
    match base.split_once('/') {
        Some((provider, model))
            if !provider.is_empty()
                && !model.is_empty()
                && !provider.starts_with('@')
                && !model.starts_with('@') =>
        {
            Ok(text.to_owned())
        }
        _ => Err(invalid(&format!(
            "expected concrete provider/model, got {text:?}"
        ))),
    }
}

fn invalid_target(origin: &str, message: &str) -> AgentHijackExecutionError {
    AgentHijackExecutionError::InvalidModelTarget {
        origin: origin.to_owned(),
        message: message.to_owned(),
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn refuse(
    mode: &str,
    code: &str,
    message: &str,
) -> AgentHijackExecutionDecisionWire {
    AgentHijackExecutionDecisionWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        outcome: HIJACK_EXECUTION_REFUSE.to_owned(),
        mode: mode.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
    }
}

fn confirm_retain(
    record: &AgentHijackRecordWire,
    code: &str,
    message: &str,
) -> AgentHijackConfirmationDecisionWire {
    AgentHijackConfirmationDecisionWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        outcome: HIJACK_CONFIRM_RETAIN.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
        successor_name: record.reserved_successor_name.clone(),
        launch_identity: record.launch_identity.clone(),
    }
}

fn rollback_retain(
    record: &AgentHijackRecordWire,
    code: &str,
    message: &str,
) -> AgentHijackRollbackDecisionWire {
    AgentHijackRollbackDecisionWire {
        schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
        outcome: HIJACK_ROLLBACK_RETAIN.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
        successor_name: record.reserved_successor_name.clone(),
        launch_identity: record.launch_identity.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target_request(
        action: &str,
        stalled: &str,
    ) -> AgentHijackEffectiveTargetRequestWire {
        AgentHijackEffectiveTargetRequestWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            action: action.to_owned(),
            stalled_concrete: stalled.to_owned(),
            ..Default::default()
        }
    }

    fn held_record() -> AgentHijackRecordWire {
        AgentHijackRecordWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            agent_name: "agent.1".to_owned(),
            state: HIJACK_HELD.to_owned(),
            ..Default::default()
        }
    }

    fn resolving_record() -> AgentHijackRecordWire {
        AgentHijackRecordWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            agent_name: "agent.1".to_owned(),
            state: HIJACK_RESOLVING.to_owned(),
            concrete_target_model: Some("claude/opus@high".to_owned()),
            resolver_pid: Some(99),
            resolver_process_identity_token: Some("boot:12345".to_owned()),
            launch_identity: Some("launch-1".to_owned()),
            reserved_successor_name: Some("agent.2".to_owned()),
            ..Default::default()
        }
    }

    fn available_facts(target: &str) -> AgentHijackExecutionFactsWire {
        AgentHijackExecutionFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            observed_resolver_pid: Some(99),
            resolver_pid_live: true,
            resolver_identity_matches: true,
            pinned_available: true,
            current_target: Some(target.to_owned()),
            ..Default::default()
        }
    }

    fn proven_confirmation_facts() -> AgentHijackConfirmationFactsWire {
        AgentHijackConfirmationFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            launch_identity_matches: true,
            model_matches: true,
            workspace_matches: true,
            lineage_matches: true,
            ack_bound: true,
            ack_matches: true,
            request_in_flight: false,
            successor_observed: true,
        }
    }

    fn safe_rollback_facts() -> AgentHijackRollbackFactsWire {
        AgentHijackRollbackFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            child_state: "dead".to_owned(),
            custody: "restored".to_owned(),
            request_state: "canceled".to_owned(),
        }
    }

    #[test]
    fn continue_keeps_stalled_target() {
        let mut request = target_request("continue", "claude/opus@high");
        request.continue_target = Some("claude/opus@high".to_owned());
        let result = hijack_recovery_effective_target(&request).unwrap();
        assert_eq!(result.concrete_target, "claude/opus@high");
        assert!(!result.model_changed);
    }

    #[test]
    fn escalate_pins_previewed_target() {
        let mut request = target_request("escalate", "claude/opus@high");
        request.escalate_target = Some("claude/opus@xhigh".to_owned());
        let result = hijack_recovery_effective_target(&request).unwrap();
        assert_eq!(result.concrete_target, "claude/opus@xhigh");
        assert!(result.model_changed);
    }

    #[test]
    fn escalate_without_pinned_target_is_unavailable() {
        let request = target_request("escalate", "claude/opus@high");
        let error = hijack_recovery_effective_target(&request).unwrap_err();
        assert!(matches!(
            error,
            AgentHijackExecutionError::ActionUnavailable(_)
        ));
    }

    #[test]
    fn continue_rejects_overrides() {
        let mut request = target_request("continue", "claude/opus@high");
        request.explicit_model_override = Some("codex/gpt@xhigh".to_owned());
        let error = hijack_recovery_effective_target(&request).unwrap_err();
        assert!(matches!(
            error,
            AgentHijackExecutionError::IncompatibleOptions { .. }
        ));
    }

    #[test]
    fn explicit_override_wins_and_retains_effort() {
        let mut request = target_request("instruct", "claude/opus@high");
        request.explicit_model_override = Some("codex/gpt-x".to_owned());
        let result = hijack_recovery_effective_target(&request).unwrap();
        assert_eq!(result.concrete_target, "codex/gpt-x@high");
        assert!(result.model_changed);
    }

    #[test]
    fn prompt_model_wins_with_provider_inheritance() {
        let mut request = target_request("instruct", "claude/opus@high");
        request.predecessor_provider = Some("claude".to_owned());
        request.prompt_model = Some("opus".to_owned());
        let result = hijack_recovery_effective_target(&request).unwrap();
        assert_eq!(result.concrete_target, "claude/opus@high");
        assert!(!result.model_changed);
    }

    #[test]
    fn instruct_without_override_keeps_stalled() {
        let request = target_request("instruct", "claude/opus@high");
        let result = hijack_recovery_effective_target(&request).unwrap();
        assert_eq!(result.concrete_target, "claude/opus@high");
        assert!(!result.model_changed);
    }

    #[test]
    fn pool_alias_refuses_as_non_concrete() {
        let mut request = target_request("instruct", "claude/opus@high");
        request.explicit_model_override = Some("@xlarge".to_owned());
        let error = hijack_recovery_effective_target(&request).unwrap_err();
        assert!(matches!(
            error,
            AgentHijackExecutionError::InvalidModelTarget { .. }
        ));
    }

    #[test]
    fn unknown_action_and_schema_refuse() {
        let request = target_request("restart", "claude/opus@high");
        assert!(matches!(
            hijack_recovery_effective_target(&request).unwrap_err(),
            AgentHijackExecutionError::UnsupportedAction(_)
        ));
        let mut bad = target_request("continue", "claude/opus@high");
        bad.schema_version = 999;
        assert!(matches!(
            hijack_recovery_effective_target(&bad).unwrap_err(),
            AgentHijackExecutionError::UnsupportedSchema(999)
        ));
    }

    #[test]
    fn held_preparation_ignores_resolver_but_checks_target() {
        let decision = hijack_recovery_validate_execution(
            &held_record(),
            "claude/opus@high",
            &available_facts("claude/opus@high"),
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_EXECUTION_PROCEED);
        assert_eq!(decision.mode, HIJACK_EXECUTION_MODE_PREPARE);
    }

    #[test]
    fn dispatch_requires_exact_reservation_owner() {
        let mut facts = available_facts("claude/opus@high");
        facts.observed_resolver_pid = Some(7);
        let decision = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &facts,
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_EXECUTION_REFUSE);
        assert_eq!(decision.code.as_deref(), Some("resolver_mismatch"));
    }

    #[test]
    fn dispatch_refuses_changed_and_unavailable_targets() {
        let changed = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &available_facts("claude/opus@xhigh"),
        )
        .unwrap();
        assert_eq!(changed.code.as_deref(), Some("target_changed"));

        let mut missing = available_facts("claude/opus@high");
        missing.pinned_available = false;
        let refused = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &missing,
        )
        .unwrap();
        assert_eq!(refused.code.as_deref(), Some("target_unavailable"));

        let mut disabled = available_facts("claude/opus@high");
        disabled.provider_disabled = true;
        let refused = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &disabled,
        )
        .unwrap();
        assert_eq!(refused.code.as_deref(), Some("provider_disabled"));
    }

    #[test]
    fn dispatch_checks_stored_attempt_and_proceeds() {
        let mut record = resolving_record();
        record.concrete_target_model = Some("claude/opus@xhigh".to_owned());
        let decision = hijack_recovery_validate_execution(
            &record,
            "claude/opus@high",
            &available_facts("claude/opus@high"),
        )
        .unwrap();
        assert_eq!(decision.code.as_deref(), Some("target_changed"));

        let decision = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &available_facts("claude/opus@high"),
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_EXECUTION_PROCEED);
        assert_eq!(decision.mode, HIJACK_EXECUTION_MODE_DISPATCH);
    }

    #[test]
    fn confirmation_settles_only_on_proven_evidence() {
        let decision = hijack_recovery_confirm(
            &resolving_record(),
            &proven_confirmation_facts(),
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_CONFIRM_SETTLE);

        for mutate in [
            "identity",
            "model",
            "workspace",
            "lineage",
            "ack",
            "in_flight",
        ] {
            let mut facts = proven_confirmation_facts();
            match mutate {
                "identity" => facts.launch_identity_matches = false,
                "model" => facts.model_matches = false,
                "workspace" => facts.workspace_matches = false,
                "lineage" => facts.lineage_matches = false,
                "ack" => facts.ack_bound = false,
                _ => facts.request_in_flight = true,
            }
            let decision =
                hijack_recovery_confirm(&resolving_record(), &facts).unwrap();
            assert_eq!(
                decision.outcome, HIJACK_CONFIRM_RETAIN,
                "mismatch {mutate} must never settle"
            );
        }
    }

    #[test]
    fn confirmation_never_settles_without_launch_identity() {
        let mut record = resolving_record();
        record.launch_identity = None;
        let decision =
            hijack_recovery_confirm(&record, &proven_confirmation_facts())
                .unwrap();
        assert_eq!(decision.outcome, HIJACK_CONFIRM_RETAIN);
        assert_eq!(decision.code.as_deref(), Some("missing_launch_identity"));
    }

    #[test]
    fn rollback_permits_only_verified_death_with_restored_custody() {
        let decision = hijack_recovery_rollback(
            &resolving_record(),
            &safe_rollback_facts(),
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_ROLLBACK_RETURN_TO_HELD);

        let mut live = safe_rollback_facts();
        live.child_state = "live".to_owned();
        let decision =
            hijack_recovery_rollback(&resolving_record(), &live).unwrap();
        assert_eq!(decision.outcome, HIJACK_ROLLBACK_RETAIN);
        assert_eq!(decision.code.as_deref(), Some("child_live"));

        let mut uncertain = safe_rollback_facts();
        uncertain.child_state = "uncertain".to_owned();
        let decision =
            hijack_recovery_rollback(&resolving_record(), &uncertain).unwrap();
        assert_eq!(decision.code.as_deref(), Some("child_uncertain"));

        let mut foreign = safe_rollback_facts();
        foreign.custody = "foreign".to_owned();
        let decision =
            hijack_recovery_rollback(&resolving_record(), &foreign).unwrap();
        assert_eq!(decision.code.as_deref(), Some("custody_foreign"));

        let mut racing = safe_rollback_facts();
        racing.request_state = "dispatching".to_owned();
        let decision =
            hijack_recovery_rollback(&resolving_record(), &racing).unwrap();
        assert_eq!(decision.code.as_deref(), Some("request_uncertain"));
    }

    #[test]
    fn missing_evidence_is_uncertain_never_successful() {
        let empty = AgentHijackConfirmationFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            ..Default::default()
        };
        let decision =
            hijack_recovery_confirm(&resolving_record(), &empty).unwrap();
        assert_eq!(decision.outcome, HIJACK_CONFIRM_RETAIN);

        let empty = AgentHijackRollbackFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            ..Default::default()
        };
        let decision =
            hijack_recovery_rollback(&resolving_record(), &empty).unwrap();
        assert_eq!(decision.outcome, HIJACK_ROLLBACK_RETAIN);

        let empty = AgentHijackExecutionFactsWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            observed_resolver_pid: Some(99),
            resolver_pid_live: true,
            resolver_identity_matches: true,
            ..Default::default()
        };
        let decision = hijack_recovery_validate_execution(
            &resolving_record(),
            "claude/opus@high",
            &empty,
        )
        .unwrap();
        assert_eq!(decision.outcome, HIJACK_EXECUTION_REFUSE);
    }

    #[test]
    fn ack_and_descriptor_wires_round_trip() {
        let ack = AgentHijackBootstrapAckWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            launch_identity: "launch-1".to_owned(),
            successor_name: "agent.2".to_owned(),
            process_token: "boot:77".to_owned(),
            project: "sase".to_owned(),
            workspace_num: Some(4),
            workspace_dir: "/tmp/ws4".to_owned(),
            lineage: "fork".to_owned(),
            provider: "claude".to_owned(),
            model: "opus".to_owned(),
            effort: "high".to_owned(),
            concrete_target: "claude/opus@high".to_owned(),
            goal_verified: true,
            bead_verified: true,
            scope_verified: true,
            baselines_verified: true,
            inventory_verified: true,
        };
        let json = serde_json::to_value(&ack).unwrap();
        let back: AgentHijackBootstrapAckWire =
            serde_json::from_value(json).unwrap();
        assert_eq!(back, ack);

        let descriptor = AgentHijackAdmissionDescriptorWire {
            schema_version: AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            launch_identity: "launch-1".to_owned(),
            concrete_target: "claude/opus@high".to_owned(),
            reserved_successor_name: "agent.2".to_owned(),
            project: "sase".to_owned(),
            workspace_num: Some(4),
            workspace_dir: "/tmp/ws4".to_owned(),
            lineage: "fork".to_owned(),
            prompt_digest: "digest".to_owned(),
            ..Default::default()
        };
        let json = serde_json::to_value(&descriptor).unwrap();
        let back: AgentHijackAdmissionDescriptorWire =
            serde_json::from_value(json).unwrap();
        assert_eq!(back, descriptor);
    }
}
