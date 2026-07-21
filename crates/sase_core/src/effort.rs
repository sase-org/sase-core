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

/// Provenance of the resolved reasoning-effort value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveEffortSource {
    Explicit,
    Alias,
    TemporaryOverride,
    Configured,
    ProviderDefault,
}

/// Canonical effective-effort resolution shared by every frontend.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveEffortResolutionWire {
    pub level: Option<String>,
    pub source: EffectiveEffortSource,
    pub explicit: bool,
}

/// Resolve launch effort with the domain's single precedence definition.
pub fn resolve_effective_effort(
    explicit_effort: Option<&str>,
    alias_effort: Option<&str>,
    temporary_effort: Option<&str>,
    configured_effort: Option<&str>,
) -> EffectiveEffortResolutionWire {
    let candidates = [
        (explicit_effort, EffectiveEffortSource::Explicit, true),
        (alias_effort, EffectiveEffortSource::Alias, false),
        (
            temporary_effort,
            EffectiveEffortSource::TemporaryOverride,
            false,
        ),
        (configured_effort, EffectiveEffortSource::Configured, false),
    ];
    for (candidate, source, explicit) in candidates {
        if let Some(level) = candidate.filter(|value| is_valid_effort(value)) {
            return EffectiveEffortResolutionWire {
                level: Some(level.to_string()),
                source,
                explicit,
            };
        }
    }
    EffectiveEffortResolutionWire {
        level: None,
        source: EffectiveEffortSource::ProviderDefault,
        explicit: false,
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
            split_model_effort("codex/gpt-5.6-sol@low"),
            ("codex/gpt-5.6-sol", Some("low"))
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

    #[test]
    fn resolves_effective_effort_precedence_and_source() {
        let cases = [
            (
                (Some("max"), Some("high"), Some("medium"), Some("low")),
                Some("max"),
                EffectiveEffortSource::Explicit,
                true,
            ),
            (
                (None, Some("high"), Some("medium"), Some("low")),
                Some("high"),
                EffectiveEffortSource::Alias,
                false,
            ),
            (
                (None, None, Some("medium"), Some("low")),
                Some("medium"),
                EffectiveEffortSource::TemporaryOverride,
                false,
            ),
            (
                (None, None, None, Some("low")),
                Some("low"),
                EffectiveEffortSource::Configured,
                false,
            ),
            (
                (None, None, None, None),
                None,
                EffectiveEffortSource::ProviderDefault,
                false,
            ),
        ];
        for (inputs, level, source, explicit) in cases {
            let resolved = resolve_effective_effort(
                inputs.0, inputs.1, inputs.2, inputs.3,
            );
            assert_eq!(resolved.level.as_deref(), level);
            assert_eq!(resolved.source, source);
            assert_eq!(resolved.explicit, explicit);
        }
    }

    #[test]
    fn invalid_candidates_are_skipped() {
        let resolved = resolve_effective_effort(
            Some("turbo"),
            Some("HIGH"),
            Some("medium"),
            Some("low"),
        );
        assert_eq!(resolved.level.as_deref(), Some("medium"));
        assert_eq!(resolved.source, EffectiveEffortSource::TemporaryOverride);
    }
}
