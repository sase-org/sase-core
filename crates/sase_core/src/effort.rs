//! Canonical reasoning-effort vocabulary shared across the editor and
//! agent-launch layers. Mirrors `src/sase/xprompt/effort.py` in the sase repo
//! so the directive grammar, `@effort` split rule, and completion candidates
//! stay in parity with the Python xprompt parser.
//!
//! The public surface spells it `effort`; the threaded/stored field is named
//! `reasoning_effort` everywhere else. Spelling is validated against this
//! vocabulary — *which* levels a given provider actually honors is decided in
//! the provider adapter (Python), not here.

/// Canonical vocabulary, ordered from least to most effort for human-readable
/// messages. Membership checks use [`is_valid_effort`].
pub const EFFORT_LEVELS_ORDERED: &[&str] =
    &["none", "minimal", "low", "medium", "high", "xhigh", "max"];

/// Return true when `level` is a canonical reasoning-effort level.
pub fn is_valid_effort(level: &str) -> bool {
    EFFORT_LEVELS_ORDERED.contains(&level)
}

/// Split a trailing `@<effort>` token off a model string.
///
/// Only a trailing `@<level>` whose `<level>` is a known effort level is split
/// off; any other `@` — or an unknown trailing token — is left in place so
/// model ids that legitimately contain `@` survive untouched. Returns
/// `(clean_model, effort)` where `effort` is `None` when no known-effort suffix
/// is present.
///
/// Callers must bypass this split for backtick-literal model values
/// (`` %model:`literal@id` ``), which intentionally preserve `@`.
pub fn split_model_effort(model: &str) -> (&str, Option<&str>) {
    match model.rfind('@') {
        Some(at) if at > 0 => {
            let candidate = &model[at + 1..];
            if is_valid_effort(candidate) {
                (&model[..at], Some(candidate))
            } else {
                (model, None)
            }
        }
        _ => (model, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_canonical_vocabulary() {
        for level in EFFORT_LEVELS_ORDERED {
            assert!(is_valid_effort(level), "{level} should be valid");
        }
        assert!(!is_valid_effort("xlow"));
        assert!(!is_valid_effort(""));
        assert!(!is_valid_effort("HIGH"));
    }

    #[test]
    fn splits_trailing_known_effort() {
        assert_eq!(split_model_effort("opus@xhigh"), ("opus", Some("xhigh")));
        assert_eq!(
            split_model_effort("codex/gpt-5.5@low"),
            ("codex/gpt-5.5", Some("low"))
        );
    }

    #[test]
    fn leaves_unknown_or_internal_at_intact() {
        // Unknown trailing token is not an effort level.
        assert_eq!(split_model_effort("opus@v2"), ("opus@v2", None));
        // A leading `@` (position 0) is never split.
        assert_eq!(split_model_effort("@max"), ("@max", None));
        // No `@` at all.
        assert_eq!(split_model_effort("opus"), ("opus", None));
        // Only the trailing `@` token is considered.
        assert_eq!(
            split_model_effort("foo@bar/baz@high"),
            ("foo@bar/baz", Some("high"))
        );
    }
}
