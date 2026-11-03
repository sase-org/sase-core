use super::wire::{
    AgentHijackEligibilityDecisionWire, AgentHijackEligibilityFactsWire,
    AGENT_HIJACK_WIRE_SCHEMA_VERSION,
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
