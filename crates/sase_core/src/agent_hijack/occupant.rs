//! Session occupant selection for Agent Hijack.
//!
//! A hijack targets the session's occupying turn, not the row that was
//! clicked. This pure function applies the four-step rule over member facts:
//! the claim holder first, then the newest in-flight member, then the
//! earliest waiting member, otherwise none. Python resolves names, session
//! names, and session containers to a member roster and calls this to pick
//! the occupant; `--explain` prints the returned reason.

use super::wire::{
    AgentHijackOccupantFactsWire, AgentHijackOccupantSelectionRequestWire,
    AgentHijackOccupantSelectionWire,
    AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackOccupantError {
    #[error("unsupported occupant selection wire schema_version {0}")]
    UnsupportedSchema(u32),
}

/// Select the session's occupying member from its roster.
///
/// Ranks every unsettled member: claim holders first (newest wins ties),
/// then in-flight members (newest wins), then waiting members (earliest
/// wins). Settled members are never selected while a live member exists;
/// with no live member the selection is empty.
pub fn agent_hijack_select_occupant(
    request: &AgentHijackOccupantSelectionRequestWire,
) -> Result<AgentHijackOccupantSelectionWire, AgentHijackOccupantError> {
    if request.schema_version != AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackOccupantError::UnsupportedSchema(
            request.schema_version,
        ));
    }

    let live: Vec<(usize, &AgentHijackOccupantFactsWire)> = request
        .members
        .iter()
        .enumerate()
        .filter(|(_, member)| !member.settled)
        .collect();

    let holders: Vec<(usize, &AgentHijackOccupantFactsWire)> = live
        .iter()
        .copied()
        .filter(|(_, member)| member.holds_claim)
        .collect();
    if let Some((index, member)) = pick_newest(holders) {
        return Ok(select(
            index,
            format!("{} holds the workspace claim", display_name(member)),
        ));
    }

    let in_flight: Vec<(usize, &AgentHijackOccupantFactsWire)> = live
        .iter()
        .copied()
        .filter(|(_, member)| member.in_flight)
        .collect();
    if let Some((index, member)) = pick_newest(in_flight) {
        return Ok(select(
            index,
            format!("{} is the newest in-flight member", display_name(member)),
        ));
    }

    let waiting: Vec<(usize, &AgentHijackOccupantFactsWire)> = live
        .iter()
        .copied()
        .filter(|(_, member)| member.waiting)
        .collect();
    if let Some((index, member)) = pick_earliest(waiting) {
        return Ok(select(
            index,
            format!("{} is the earliest waiting member", display_name(member)),
        ));
    }

    Ok(AgentHijackOccupantSelectionWire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        selected_index: None,
        reason: if request.members.is_empty() {
            "session has no members".to_string()
        } else {
            "session has no live member".to_string()
        },
    })
}

fn pick_newest(
    candidates: Vec<(usize, &AgentHijackOccupantFactsWire)>,
) -> Option<(usize, &AgentHijackOccupantFactsWire)> {
    candidates
        .into_iter()
        .max_by_key(|(_, member)| member.order)
}

fn pick_earliest(
    candidates: Vec<(usize, &AgentHijackOccupantFactsWire)>,
) -> Option<(usize, &AgentHijackOccupantFactsWire)> {
    candidates
        .into_iter()
        .min_by_key(|(_, member)| member.order)
}

fn select(index: usize, reason: String) -> AgentHijackOccupantSelectionWire {
    AgentHijackOccupantSelectionWire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        selected_index: Some(index),
        reason,
    }
}

fn display_name(member: &AgentHijackOccupantFactsWire) -> &str {
    if member.name.is_empty() {
        "the unnamed member"
    } else {
        member.name.as_str()
    }
}
