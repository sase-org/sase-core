//! Machine agent hood canonicalization helpers.
//!
//! A *machine agent hood* is a top-level agent hood segment derived from a
//! machine's configured `machine_name`. Qualifying an agent name prepends
//! `<machine_name>.` so agent names become globally unique across machines;
//! stripping removes the local machine's hood for display. Every helper here
//! is idempotent and reuses the hood boundary semantics already proven in
//! `axe_chop::decision`: a name belongs to hood `X` iff it equals `X` or
//! starts with `X.`.

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum MachineNameError {
    #[error("machine name must not be empty")]
    Empty,

    #[error(
        "invalid machine name '{name}': must contain only lowercase letters (a-z) and underscores"
    )]
    InvalidCharacters { name: String },
}

/// Validate a machine name: non-empty and matching `^[a-z_]+$`.
pub fn validate_machine_name(name: &str) -> Result<(), MachineNameError> {
    if name.is_empty() {
        return Err(MachineNameError::Empty);
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
        return Err(MachineNameError::InvalidCharacters {
            name: name.to_string(),
        });
    }
    Ok(())
}

/// True when `name` belongs to the hood `hood` — it equals `hood` or is a
/// dotted descendant (`hood.<...>`). Mirrors the boundary matcher in
/// `axe_chop::decision`.
fn name_in_hood(name: &str, hood: &str) -> bool {
    name == hood
        || name
            .strip_prefix(hood)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

/// Prepend `<machine_name>.` to `name` unless it is already qualified with
/// that hood. Idempotent: qualifying an already-qualified name is a no-op.
pub fn qualify_machine_agent_name(name: &str, machine_name: &str) -> String {
    let prefix = format!("{machine_name}.");
    if name.starts_with(&prefix) {
        name.to_string()
    } else {
        format!("{prefix}{name}")
    }
}

/// Remove a leading `<machine_name>.` from `name` if present. The inverse of
/// [`qualify_machine_agent_name`] and equally idempotent. Never strips
/// mid-name segments and never strips when the remainder would be empty.
pub fn strip_machine_agent_name(name: &str, machine_name: &str) -> String {
    let prefix = format!("{machine_name}.");
    match name.strip_prefix(&prefix) {
        Some(remainder) if !remainder.is_empty() => remainder.to_string(),
        _ => name.to_string(),
    }
}

/// Return the leading hood segment of `name` when it names one of
/// `known_machines`. Used by import/display to classify foreign agents.
pub fn machine_hood_of(
    name: &str,
    known_machines: &[String],
) -> Option<String> {
    known_machines
        .iter()
        .find(|machine| name_in_hood(name, machine))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_lowercase_and_underscores() {
        assert!(validate_machine_name("athena").is_ok());
        assert!(validate_machine_name("my_machine").is_ok());
        assert!(validate_machine_name("_").is_ok());
    }

    #[test]
    fn validate_rejects_empty_uppercase_digits_and_dots() {
        assert_eq!(validate_machine_name(""), Err(MachineNameError::Empty));
        assert!(matches!(
            validate_machine_name("Athena"),
            Err(MachineNameError::InvalidCharacters { .. })
        ));
        assert!(matches!(
            validate_machine_name("machine1"),
            Err(MachineNameError::InvalidCharacters { .. })
        ));
        assert!(matches!(
            validate_machine_name("a.b"),
            Err(MachineNameError::InvalidCharacters { .. })
        ));
        assert!(matches!(
            validate_machine_name("with-dash"),
            Err(MachineNameError::InvalidCharacters { .. })
        ));
    }

    #[test]
    fn qualify_prepends_when_missing() {
        assert_eq!(qualify_machine_agent_name("foo", "athena"), "athena.foo");
        assert_eq!(
            qualify_machine_agent_name("chop.x.y", "athena"),
            "athena.chop.x.y"
        );
    }

    #[test]
    fn qualify_is_idempotent() {
        let once = qualify_machine_agent_name("foo", "athena");
        assert_eq!(qualify_machine_agent_name(&once, "athena"), once);
    }

    #[test]
    fn qualify_preserves_family_names() {
        assert_eq!(
            qualify_machine_agent_name("foo--code", "athena"),
            "athena.foo--code"
        );
        // Already-qualified family name is untouched.
        assert_eq!(
            qualify_machine_agent_name("athena.foo--code", "athena"),
            "athena.foo--code"
        );
    }

    #[test]
    fn qualify_does_not_confuse_prefix_of_another_machine() {
        // `athenax.foo` is NOT in the `athena` hood, so it gets qualified.
        assert_eq!(
            qualify_machine_agent_name("athenax.foo", "athena"),
            "athena.athenax.foo"
        );
    }

    #[test]
    fn strip_removes_leading_hood() {
        assert_eq!(strip_machine_agent_name("athena.foo", "athena"), "foo");
        assert_eq!(
            strip_machine_agent_name("athena.chop.x.y", "athena"),
            "chop.x.y"
        );
        assert_eq!(
            strip_machine_agent_name("athena.foo--code", "athena"),
            "foo--code"
        );
    }

    #[test]
    fn strip_leaves_unqualified_and_foreign_names() {
        assert_eq!(strip_machine_agent_name("foo", "athena"), "foo");
        assert_eq!(strip_machine_agent_name("zeus.bar", "athena"), "zeus.bar");
    }

    #[test]
    fn strip_never_produces_empty_remainder() {
        // A name that is exactly `<machine>.` must stay as-is rather than
        // collapsing to an empty string.
        assert_eq!(strip_machine_agent_name("athena.", "athena"), "athena.");
    }

    #[test]
    fn strip_is_idempotent() {
        let once = strip_machine_agent_name("athena.foo", "athena");
        assert_eq!(strip_machine_agent_name(&once, "athena"), once);
    }

    #[test]
    fn qualify_and_strip_round_trip() {
        let machine = "athena";
        for name in ["foo", "foo--code", "chop.x.y"] {
            let qualified = qualify_machine_agent_name(name, machine);
            assert_eq!(strip_machine_agent_name(&qualified, machine), name);
        }
    }

    #[test]
    fn machine_hood_of_classifies_known_machines() {
        let known = vec!["athena".to_string(), "zeus".to_string()];
        assert_eq!(
            machine_hood_of("zeus.bar", &known),
            Some("zeus".to_string())
        );
        assert_eq!(
            machine_hood_of("athena.foo--code", &known),
            Some("athena".to_string())
        );
        // The bare hood name itself is classified.
        assert_eq!(
            machine_hood_of("athena", &known),
            Some("athena".to_string())
        );
    }

    #[test]
    fn machine_hood_of_returns_none_for_unknown_or_partial() {
        let known = vec!["athena".to_string()];
        assert_eq!(machine_hood_of("zeus.bar", &known), None);
        assert_eq!(machine_hood_of("foo", &known), None);
        // `athenax` shares a prefix but is not the `athena` hood.
        assert_eq!(machine_hood_of("athenax.foo", &known), None);
    }
}
