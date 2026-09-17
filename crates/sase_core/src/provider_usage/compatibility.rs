use super::{
    validation, Result, UsageApplicabilityWire, UsageWindowObservationWire,
};
use std::collections::BTreeMap;

pub(super) const CLAUDE_FABLE_CANONICAL_KEY: &str = "weekly:claude-fable-5";
const CLAUDE_FABLE_ALIAS_KEY: &str = "window:seven-day-overage-included";
const CLAUDE_FABLE_MODEL_ID: &str = "claude-fable-5";
const CLAUDE_FABLE_LABEL: &str = "Claude weekly Fable";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UsageIdentityOrigin {
    Alias,
    Canonical,
    Other,
}

struct WindowCandidate {
    window: UsageWindowObservationWire,
    origin: UsageIdentityOrigin,
}

pub(super) fn normalize_observation_windows(
    provider: &str,
    windows: Vec<UsageWindowObservationWire>,
) -> Result<Vec<UsageWindowObservationWire>> {
    let mut positions: BTreeMap<String, usize> = BTreeMap::new();
    let mut candidates: Vec<WindowCandidate> =
        Vec::with_capacity(windows.len());
    for mut window in windows {
        let origin = normalize_window_identity(provider, &mut window);
        let key = window.key.clone();
        let candidate = WindowCandidate { window, origin };
        if let Some(index) = positions.get(&key).copied() {
            let existing = &candidates[index];
            if !collapsible_claude_fable_pair(
                provider,
                existing.origin,
                candidate.origin,
            ) {
                return Err(validation(format!(
                    "provider {provider} has duplicate window key {:?}",
                    key
                )));
            }
            if observation_candidate_wins(&candidate, existing) {
                candidates[index] = candidate;
            }
        } else {
            positions.insert(key, candidates.len());
            candidates.push(candidate);
        }
    }
    Ok(candidates
        .into_iter()
        .map(|candidate| candidate.window)
        .collect())
}

pub(super) fn normalize_window_identity(
    provider: &str,
    window: &mut UsageWindowObservationWire,
) -> UsageIdentityOrigin {
    let origin = window_identity_origin(provider, &window.key);
    if origin == UsageIdentityOrigin::Alias {
        window.key = CLAUDE_FABLE_CANONICAL_KEY.to_string();
        window.label = CLAUDE_FABLE_LABEL.to_string();
        window.applicability = UsageApplicabilityWire::Models {
            model_ids: vec![CLAUDE_FABLE_MODEL_ID.to_string()],
        };
    }
    origin
}

pub(super) fn window_identity_origin(
    provider: &str,
    key: &str,
) -> UsageIdentityOrigin {
    if provider != "claude" {
        return UsageIdentityOrigin::Other;
    }
    match key {
        CLAUDE_FABLE_ALIAS_KEY => UsageIdentityOrigin::Alias,
        CLAUDE_FABLE_CANONICAL_KEY => UsageIdentityOrigin::Canonical,
        _ => UsageIdentityOrigin::Other,
    }
}

pub(super) fn is_obsolete_alias_tombstone(provider: &str, key: &str) -> bool {
    window_identity_origin(provider, key) == UsageIdentityOrigin::Alias
}

pub(super) fn is_claude_fable_identity(origin: UsageIdentityOrigin) -> bool {
    matches!(
        origin,
        UsageIdentityOrigin::Alias | UsageIdentityOrigin::Canonical
    )
}

fn collapsible_claude_fable_pair(
    provider: &str,
    left: UsageIdentityOrigin,
    right: UsageIdentityOrigin,
) -> bool {
    provider == "claude"
        && is_claude_fable_identity(left)
        && is_claude_fable_identity(right)
        && (left == UsageIdentityOrigin::Alias
            || right == UsageIdentityOrigin::Alias)
}

fn observation_candidate_wins(
    candidate: &WindowCandidate,
    existing: &WindowCandidate,
) -> bool {
    candidate.window.observed_at > existing.window.observed_at
        || (candidate.window.observed_at == existing.window.observed_at
            && candidate.origin == UsageIdentityOrigin::Canonical
            && existing.origin != UsageIdentityOrigin::Canonical)
}
