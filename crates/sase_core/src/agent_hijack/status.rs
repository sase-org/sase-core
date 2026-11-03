use crate::agent_auto_restart::AgentRecoveryWire;

pub const HIJACKED_STATUS: &str = "HIJACKED";
pub const ESCALATED_STATUS: &str = "ESCALATED";
pub const CONTINUED_STATUS: &str = "CONTINUED";
pub const INTERRUPTED_STATUS: &str = "INTERRUPTED";

/// Derive the display/archive status from the durable hijack done marker.
/// Unknown recovery states stay parked as HIJACKED instead of falling through
/// to a generic DONE or FAILED interpretation.
pub fn agent_hijack_status_for_done_marker(
    outcome: Option<&str>,
    recovery: Option<&AgentRecoveryWire>,
) -> Option<&'static str> {
    if !outcome.is_some_and(|value| value.eq_ignore_ascii_case("hijacked")) {
        return None;
    }
    match recovery.and_then(|value| value.state.as_deref()) {
        Some("resolved")
            if recovery
                .is_some_and(|value| value.model_changed == Some(true)) =>
        {
            Some(ESCALATED_STATUS)
        }
        Some("resolved") => Some(CONTINUED_STATUS),
        Some("dismissed") => Some(INTERRUPTED_STATUS),
        Some("pending" | "expired") | None | Some(_) => Some(HIJACKED_STATUS),
    }
}
