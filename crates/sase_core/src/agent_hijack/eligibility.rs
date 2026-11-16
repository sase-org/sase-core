use super::wire::{
    AgentHijackEligibilityDecisionV2Wire, AgentHijackEligibilityDecisionWire,
    AgentHijackEligibilityFactsV2Wire, AgentHijackEligibilityFactsWire,
    AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
    AGENT_HIJACK_WIRE_SCHEMA_VERSION, HIJACK_ADAPTER_LLM,
    HIJACK_ADAPTER_PROCESSLESS, HIJACK_DECISION_IMMEDIATE,
    HIJACK_DECISION_LATCHED, HIJACK_DECISION_REDIRECT, HIJACK_DECISION_REFUSED,
    HIJACK_OCCUPANT_GATE, HIJACK_OCCUPANT_LLM, HIJACK_OCCUPANT_MONITOR,
    HIJACK_OCCUPANT_NONE, HIJACK_OCCUPANT_QUEUE, HIJACK_OCCUPANT_WAIT,
    HIJACK_OCCUPANT_WORKFLOW_STEP, HIJACK_REDIRECT_COPY_REMOTE_COMMAND,
    HIJACK_REDIRECT_GATE_REVIEW, HIJACK_REDIRECT_KILL_AND_EDIT,
    HIJACK_REDIRECT_OPEN_RECOVERY, HIJACK_REDIRECT_SELECT_MEMBER,
    HIJACK_REDIRECT_SHOW_ACTIVE_HIJACK,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackEligibilityError {
    #[error("unsupported agent hijack wire schema_version {0}")]
    UnsupportedSchema(u32),
}

/// Decide whether the observed local agent row can be hijacked.
pub fn agent_hijack_eligibility(
    facts: &AgentHijackEligibilityFactsWire,
) -> Result<AgentHijackEligibilityDecisionWire, AgentHijackEligibilityError> {
    if facts.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackEligibilityError::UnsupportedSchema(
            facts.schema_version,
        ));
    }

    let phase = facts
        .lifecycle_phase
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    let refusal = if matches!(
        phase.as_str(),
        "declaring" | "executing" | "finalizing"
    ) {
        Some(("finalizing", "finalizing; try again in a moment"))
    } else if facts.is_monitor_turn {
        Some(("monitor_turn", "monitor turns cannot be hijacked"))
    } else if facts.is_gate_turn {
        Some(("gate_turn", "gate turns cannot be hijacked"))
    } else if facts.is_named_proc {
        Some(("named_proc", "named processes cannot be hijacked"))
    } else if facts.is_home_mode {
        Some(("home_mode", "home-mode turns cannot be hijacked"))
    } else if facts.is_remote {
        Some((
            "remote_row",
            "hijack this agent on its owning host with `sase agent hijack NAME`",
        ))
    } else if facts.is_clan_container {
        Some(("clan_container", "clan containers cannot be hijacked"))
    } else if facts.is_bulk {
        Some(("bulk_mark", "bulk-marked rows cannot be hijacked"))
    } else if !facts.is_local || !facts.is_llm_agent {
        Some((
            "not_local_llm_agent",
            "only local LLM agent turns can be hijacked",
        ))
    } else if !facts.has_hijack_capability {
        Some((
            "missing_capability",
            "launched before hijack support; use x",
        ))
    } else if facts.workspace_num.is_none_or(|number| number <= 0) {
        Some(("unnumbered_workspace", "agent has no numbered workspace"))
    } else if !facts.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_uppercase().as_str(),
            "RUNNING" | "STARTING" | "RETRYING"
        )
    }) {
        Some((
            "ineligible_status",
            "agent is not RUNNING, STARTING, or RETRYING",
        ))
    } else if facts.pid.is_none_or(|pid| pid <= 0) {
        Some(("missing_pid", "agent process is not running"))
    } else {
        None
    };

    Ok(match refusal {
        Some((code, message)) => AgentHijackEligibilityDecisionWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            eligible: false,
            code: Some(code.to_string()),
            message: Some(message.to_string()),
        },
        None => AgentHijackEligibilityDecisionWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            eligible: true,
            ..Default::default()
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackDecideError {
    #[error("unsupported agent hijack eligibility wire schema_version {0}")]
    UnsupportedSchema(u32),
}

fn refused(
    code: &str,
    message: &str,
    next_action: &str,
) -> AgentHijackEligibilityDecisionV2Wire {
    AgentHijackEligibilityDecisionV2Wire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        mode: HIJACK_DECISION_REFUSED.to_string(),
        eligible: false,
        code: Some(code.to_string()),
        message: Some(message.to_string()),
        next_action: Some(next_action.to_string()),
        redirect_op: None,
        adapter: None,
        boundary: None,
    }
}

fn redirect(
    code: &str,
    message: &str,
    next_action: &str,
    redirect_op: &str,
) -> AgentHijackEligibilityDecisionV2Wire {
    AgentHijackEligibilityDecisionV2Wire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        mode: HIJACK_DECISION_REDIRECT.to_string(),
        eligible: false,
        code: Some(code.to_string()),
        message: Some(message.to_string()),
        next_action: Some(next_action.to_string()),
        redirect_op: Some(redirect_op.to_string()),
        adapter: None,
        boundary: None,
    }
}

fn immediate(adapter: &str) -> AgentHijackEligibilityDecisionV2Wire {
    AgentHijackEligibilityDecisionV2Wire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        mode: HIJACK_DECISION_IMMEDIATE.to_string(),
        eligible: true,
        code: None,
        message: None,
        next_action: None,
        redirect_op: None,
        adapter: Some(adapter.to_string()),
        boundary: None,
    }
}

fn latched(
    boundary: &str,
    code: &str,
    message: &str,
    next_action: &str,
) -> AgentHijackEligibilityDecisionV2Wire {
    AgentHijackEligibilityDecisionV2Wire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        mode: HIJACK_DECISION_LATCHED.to_string(),
        eligible: true,
        code: Some(code.to_string()),
        message: Some(message.to_string()),
        next_action: Some(next_action.to_string()),
        redirect_op: None,
        adapter: None,
        boundary: Some(boundary.to_string()),
    }
}

fn pre_provider_phase(facts: &AgentHijackEligibilityFactsV2Wire) -> bool {
    let phase = facts
        .turn_phase
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(phase.as_str(), "booting" | "waiting" | "preparing")
        || (phase.is_empty()
            && facts.status.as_deref().is_some_and(|status| {
                matches!(
                    status.to_ascii_uppercase().as_str(),
                    "WAITING" | "QUEUED"
                )
            }))
}

fn pre_provider_status(facts: &AgentHijackEligibilityFactsV2Wire) -> bool {
    facts.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_uppercase().as_str(),
            "WAITING"
                | "QUEUED"
                | "STARTING"
                | "LOST"
                | "DONE"
                | "STOPPED"
                | "FAILED"
        )
    })
}

fn pre_provider_live_decision(
    facts: &AgentHijackEligibilityFactsV2Wire,
) -> AgentHijackEligibilityDecisionV2Wire {
    if facts.process_alive {
        if !facts.has_hijack_capability {
            return refused(
                "missing_capability",
                "runner did not record hijack support",
                "use x, or wait for its next turn",
            );
        }
        if facts.pid.is_none_or(|pid| pid <= 0) {
            return refused(
                "missing_pid",
                "parked runner process is not identified",
                "refresh the agent row, then retry",
            );
        }
        immediate(HIJACK_ADAPTER_LLM)
    } else {
        // A dead pre-provider runner has no process to signal. The claim,
        // rather than its stale PID, is the custody identity for this stop.
        immediate(HIJACK_ADAPTER_PROCESSLESS)
    }
}

/// Decide a hijack request against the v2 facts wire.
///
/// `WAITING`, `QUEUED`, and booting turns use their live runner when it is
/// alive, or a processless, claim-owned stop after it has died. Rows without
/// a numbered claim are sent to the existing kill-and-edit flow.
pub fn agent_hijack_decide(
    facts: &AgentHijackEligibilityFactsV2Wire,
) -> Result<AgentHijackEligibilityDecisionV2Wire, AgentHijackDecideError> {
    if facts.schema_version != AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackDecideError::UnsupportedSchema(
            facts.schema_version,
        ));
    }

    if facts.is_remote {
        return Ok(redirect(
            "remote_row",
            "hijack this agent on its owning host with `sase agent hijack NAME`",
            "run `sase agent hijack NAME` on the owning host",
            HIJACK_REDIRECT_COPY_REMOTE_COMMAND,
        ));
    }
    if facts.is_bulk {
        return Ok(refused(
            "bulk_mark",
            "bulk-marked rows cannot be hijacked",
            "clear the bulk mark and hijack one row",
        ));
    }
    if facts.is_named_proc {
        return Ok(refused(
            "named_proc",
            "named processes cannot be hijacked",
            "use x to stop the named process",
        ));
    }
    if facts.is_home_mode {
        return Ok(refused(
            "home_mode",
            "home-mode turns cannot be hijacked",
            "run the agent in a workspace session to hijack it",
        ));
    }
    if facts.is_clan_container {
        return Ok(redirect(
            "clan_container",
            "clan containers cannot be hijacked; select a member",
            "select the live member row and hijack it",
            HIJACK_REDIRECT_SELECT_MEMBER,
        ));
    }
    if !facts.is_local {
        return Ok(refused(
            "not_local_llm_agent",
            "only local LLM agent turns can be hijacked",
            "hijack the turn on the host that runs it",
        ));
    }
    if facts
        .done_outcome
        .as_deref()
        .is_some_and(|outcome| outcome.eq_ignore_ascii_case("hijacked"))
    {
        return Ok(redirect(
            "already_hijacked",
            "already hijacked; open recovery to Continue or Escalate",
            "open the recovery panel",
            HIJACK_REDIRECT_OPEN_RECOVERY,
        ));
    }
    if facts.active_hijack_state.as_deref().is_some_and(|state| {
        !state.trim().is_empty() && is_active_hijack(state)
    }) {
        return Ok(redirect(
            "active_hijack",
            "a hijack is already requested, stopping, or latched for this target",
            "show the active hijack operation",
            HIJACK_REDIRECT_SHOW_ACTIVE_HIJACK,
        ));
    }
    // A staged-but-not-adopted agent handoff latches even when a done
    // marker already exists: the pending staged payload distinguishes it
    // from a fully completed turn. Locality, capability, numbered-custody,
    // and successor-attachability still refuse; liveness refuses keep their
    // own codes so the pending handoff stays visible.
    if is_staged_pending(facts) {
        if !facts.successor_attachable {
            return Ok(refused(
                "no_session",
                "no session to attach a hijack successor to",
                "fork or retry from the session instead",
            ));
        }
        if !facts.has_hijack_capability {
            return Ok(refused(
                "missing_capability",
                "launched before hijack support; use x",
                "use x, or wait for its next turn",
            ));
        }
        if !facts.has_numbered_claim
            || facts.workspace_num.is_none_or(|number| number <= 0)
        {
            return Ok(refused(
                "unnumbered_workspace",
                "agent has no numbered workspace",
                "wait for a workspace claim, then hijack",
            ));
        }
        if !facts.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_uppercase().as_str(),
                "RUNNING" | "STARTING" | "RETRYING"
            )
        }) {
            return Ok(refused(
                "ineligible_status",
                "agent is not RUNNING, STARTING, or RETRYING",
                "wait for the turn to run, then hijack",
            ));
        }
        if facts.pid.is_none_or(|pid| pid <= 0) || !facts.process_alive {
            return Ok(refused(
                "missing_pid",
                "agent process is not running",
                "wait for the runner's next turn",
            ));
        }
        return Ok(latched(
            "staged_adoption",
            "staged_pending",
            "staged handoff pending adoption; hijack latches and applies before adoption",
            "request the hijack now; it applies before the staged handoff is adopted",
        ));
    }
    if facts
        .done_outcome
        .as_deref()
        .is_some_and(|outcome| !outcome.trim().is_empty())
    {
        return Ok(refused(
            "already_completed",
            "agent turn already completed",
            "fork or retry the completed turn",
        ));
    }

    match facts.occupying_kind.as_str() {
        HIJACK_OCCUPANT_MONITOR => {
            return Ok(refused(
                "monitor_turn",
                "monitor turns cannot be hijacked yet; stop it with x or wait for its follow-up",
                "stop it with x or wait for its follow-up",
            ));
        }
        HIJACK_OCCUPANT_GATE => {
            if facts
                .gate_state
                .as_deref()
                .is_some_and(|state| state.eq_ignore_ascii_case("pending"))
            {
                return Ok(redirect(
                    "gate_pending",
                    "Awaiting your decision — Enter to review",
                    "press Enter to review the gate",
                    HIJACK_REDIRECT_GATE_REVIEW,
                ));
            }
            return Ok(refused(
                "gate_executing",
                "gate is executing its selected option; try again when it settles",
                "wait for the gate to settle",
            ));
        }
        HIJACK_OCCUPANT_WAIT | HIJACK_OCCUPANT_QUEUE => {
            if !facts.has_numbered_claim
                || facts.workspace_num.is_none_or(|number| number <= 0)
            {
                return Ok(redirect(
                    "nothing_to_keep",
                    "waiting turn has no numbered workspace to keep",
                    "open kill-and-edit to preserve the prompt and relaunch",
                    HIJACK_REDIRECT_KILL_AND_EDIT,
                ));
            }
            if !facts.successor_attachable {
                return Ok(refused(
                    "no_session",
                    "no session to attach a hijack successor to",
                    "fork or retry from the session instead",
                ));
            }
            if !pre_provider_phase(facts) {
                return Ok(refused(
                    "ineligible_status",
                    "parked turn has already crossed its provider boundary",
                    "wait for the turn to settle, then use recovery",
                ));
            }
            return Ok(pre_provider_live_decision(facts));
        }
        HIJACK_OCCUPANT_WORKFLOW_STEP => {
            return Ok(refused(
                "workflow_step",
                "multi-step workflow steps cannot be hijacked",
                "select the running member row and hijack it",
            ));
        }
        HIJACK_OCCUPANT_LLM | HIJACK_OCCUPANT_NONE => {}
        _ => {
            return Ok(refused(
                "unknown_occupant",
                "occupying turn kind is not recognized",
                "wait for the turn to settle, then hijack its successor",
            ));
        }
    }

    if facts.occupying_kind == HIJACK_OCCUPANT_NONE {
        return Ok(refused(
            "nothing_to_keep",
            "session has no occupying turn",
            "fork or retry the session to start a new turn",
        ));
    }
    if facts.occupying_kind == HIJACK_OCCUPANT_LLM
        && pre_provider_status(facts)
        && pre_provider_phase(facts)
    {
        if !facts.has_numbered_claim
            || facts.workspace_num.is_none_or(|number| number <= 0)
        {
            return Ok(refused(
                "unnumbered_workspace",
                "booting turn has no numbered workspace",
                "wait for a workspace claim, then hijack",
            ));
        }
        if !facts.successor_attachable {
            return Ok(refused(
                "no_session",
                "no session to attach a hijack successor to",
                "fork or retry from the session instead",
            ));
        }
        return Ok(pre_provider_live_decision(facts));
    }
    if !facts.successor_attachable {
        return Ok(refused(
            "no_session",
            "no session to attach a hijack successor to",
            "fork or retry from the session instead",
        ));
    }
    if !facts.has_hijack_capability {
        return Ok(refused(
            "missing_capability",
            "launched before hijack support; use x",
            "use x, or wait for its next turn",
        ));
    }
    if !facts.has_numbered_claim
        || facts.workspace_num.is_none_or(|number| number <= 0)
    {
        return Ok(refused(
            "unnumbered_workspace",
            "agent has no numbered workspace",
            "wait for a workspace claim, then hijack",
        ));
    }
    if !facts.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_uppercase().as_str(),
            "RUNNING" | "STARTING" | "RETRYING"
        )
    }) {
        return Ok(refused(
            "ineligible_status",
            "agent is not RUNNING, STARTING, or RETRYING",
            "wait for the turn to run, then hijack",
        ));
    }
    if facts.pid.is_none_or(|pid| pid <= 0) || !facts.process_alive {
        return Ok(refused(
            "missing_pid",
            "agent process is not running",
            "wait for the runner's next turn",
        ));
    }

    if is_finalizing(facts) {
        return Ok(latched(
            "finalizer",
            "finalizing",
            "finalizing; hijack latches and applies after the hand-off settles",
            "request the hijack now; it applies after the finalizer settles",
        ));
    }

    Ok(immediate(HIJACK_ADAPTER_LLM))
}

/// True while a ledger record for this target still needs the workspace.
fn is_active_hijack(state: &str) -> bool {
    !matches!(
        state.to_ascii_lowercase().as_str(),
        "resolved"
            | "dismissed"
            | "expired"
            | "stop_failed"
            | "finished_first"
            | "cancelled"
    )
}

/// True while the occupying turn is finalizing: the runner lifecycle reached
/// `provider_done`/`finalizing`/`handoff`, or the finalizer is
/// `declaring`/`executing`. Finalizing turns latch; the hijack applies at
/// the next safe hand-off once locality, capability, numbered-custody, and
/// successor-attachability checks pass.
fn is_staged_pending(facts: &AgentHijackEligibilityFactsV2Wire) -> bool {
    facts.has_staged_handoff && facts.occupying_kind == HIJACK_OCCUPANT_LLM
}

/// True while the occupying turn is finalizing: the runner lifecycle reached
/// `provider_done`/`finalizing`/`handoff`, or the finalizer is
/// `declaring`/`executing`.
fn is_finalizing(facts: &AgentHijackEligibilityFactsV2Wire) -> bool {
    let turn = facts
        .turn_phase
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    if matches!(turn.as_str(), "provider_done" | "finalizing" | "handoff") {
        return true;
    }
    let finalizer = facts
        .finalizer_phase
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(finalizer.as_str(), "declaring" | "executing")
}
