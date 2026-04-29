//! Status constants and validation helpers.
//!
//! Mirrors `sase_100/src/sase/status_state_machine/constants.py`. Workspace
//! suffix stripping is duplicated here (separate from `get_base_status` in
//! `query::matchers`) because the Python source is also separate and the
//! semantics differ: this helper does **not** trim the result, matching
//! `remove_workspace_suffix(status)` in `constants.py` line 13.

use std::sync::OnceLock;

use regex::Regex;

/// All valid STATUS values for ChangeSpecs. Mirrors
/// `VALID_STATUSES` in `constants.py`.
pub const VALID_STATUSES: &[&str] = &[
    "WIP",
    "Draft",
    "Ready",
    "Mailed",
    "Submitted",
    "Reverted",
    "Archived",
];

/// Statuses that move a ChangeSpec to the archive file. Mirrors
/// `ARCHIVE_STATUSES` in `constants.py`.
pub const ARCHIVE_STATUSES: &[&str] = &["Submitted", "Archived", "Reverted"];

/// Allowed next statuses for each current status. The empty slice marks a
/// terminal status.
fn valid_transitions(from: &str) -> &'static [&'static str] {
    match from {
        "WIP" => &["Draft", "Ready"],
        "Draft" => &["Ready"],
        "Ready" => &["Mailed", "Draft"],
        "Mailed" => &["Submitted"],
        "Submitted" | "Reverted" | "Archived" => &[],
        _ => &[],
    }
}

/// Public read-only accessor used by error-message formatting in
/// [`crate::status::planner`]. Returns the empty slice for unknown source
/// statuses, matching `VALID_TRANSITIONS.get(from_status, [])` in
/// `transitions.py`.
pub fn valid_transitions_from(from: &str) -> &'static [&'static str] {
    valid_transitions(from)
}

/// Workspace-suffix regex `r" \([a-zA-Z0-9_-]+_\d+\)$"`.
fn workspace_suffix_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r" \([a-zA-Z0-9_-]+_\d+\)$").unwrap())
}

/// Legacy READY TO MAIL suffix regex `r" - \(!\: READY TO MAIL\)$"`.
fn legacy_ready_to_mail_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r" - \(!: READY TO MAIL\)$").unwrap())
}

/// Strip the workspace suffix and the legacy READY TO MAIL suffix from a
/// STATUS value. Mirrors `remove_workspace_suffix` in `constants.py`.
///
/// The result is **not** trimmed — matches the Python implementation
/// byte-for-byte.
pub fn remove_workspace_suffix(status: &str) -> String {
    let stripped = workspace_suffix_re().replace(status, "");
    let stripped = legacy_ready_to_mail_re().replace(&stripped, "");
    stripped.into_owned()
}

/// Check if a status transition is valid. Mirrors `is_valid_transition` in
/// `constants.py`. Workspace suffixes are stripped before validation.
pub fn is_valid_transition(from_status: &str, to_status: &str) -> bool {
    let from_base = remove_workspace_suffix(from_status);
    let to_base = remove_workspace_suffix(to_status);

    if !VALID_STATUSES.contains(&from_base.as_str()) {
        return false;
    }
    if !VALID_STATUSES.contains(&to_base.as_str()) {
        return false;
    }
    valid_transitions(&from_base).contains(&to_base.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_suffix_stripped_from_status() {
        assert_eq!(remove_workspace_suffix("Ready (proj_2)"), "Ready");
        assert_eq!(remove_workspace_suffix("Ready (some-project_12)"), "Ready");
    }

    #[test]
    fn legacy_ready_to_mail_suffix_stripped() {
        assert_eq!(
            remove_workspace_suffix("Ready - (!: READY TO MAIL)"),
            "Ready"
        );
    }

    #[test]
    fn unmodified_status_passes_through() {
        assert_eq!(remove_workspace_suffix("Mailed"), "Mailed");
    }

    #[test]
    fn valid_transitions_allow_known_pairs() {
        assert!(is_valid_transition("WIP", "Draft"));
        assert!(is_valid_transition("WIP", "Ready"));
        assert!(is_valid_transition("Draft", "Ready"));
        assert!(is_valid_transition("Ready", "Mailed"));
        assert!(is_valid_transition("Ready", "Draft"));
        assert!(is_valid_transition("Mailed", "Submitted"));
    }

    #[test]
    fn terminal_statuses_have_no_outgoing_transitions() {
        assert!(!is_valid_transition("Submitted", "Mailed"));
        assert!(!is_valid_transition("Reverted", "WIP"));
        assert!(!is_valid_transition("Archived", "WIP"));
    }

    #[test]
    fn unknown_statuses_rejected() {
        assert!(!is_valid_transition("Bogus", "Mailed"));
        assert!(!is_valid_transition("Ready", "Bogus"));
    }

    #[test]
    fn workspace_suffix_does_not_block_validation() {
        assert!(is_valid_transition("Ready (proj_2)", "Mailed"));
        assert!(is_valid_transition("Ready - (!: READY TO MAIL)", "Mailed"));
    }
}
