//! Routing classifier over [`ModelValiditySnapshot`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::macro_input_types::suggest_closest;

/// Snapshot of the routing world a model value is classified against.
///
/// Serialized under the model catalog file's `routing` object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelValiditySnapshot {
    pub schema_version: u64,
    pub providers: Vec<String>,
    pub models: BTreeMap<String, String>,
    pub aliases: Vec<String>,
    pub effort_levels: Vec<String>,
}

/// Wire request for the `classify_model_value` binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifyModelValueRequestWire {
    pub name: String,
    pub value: String,
    pub snapshot: ModelValiditySnapshot,
}

/// Wire result for `classify_model_value`.
///
/// A rejected value is a successful classification with `ok: false`, not an
/// error. `kind` is `alias`, `provider_model`, or `known_model` when
/// accepted. `provider` is the token prefix for `provider/rest` and the map
/// value for a known model. An alias has no provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifyModelValueResultWire {
    pub ok: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default)]
    pub suggestions: Vec<String>,
}

/// Malformed snapshot error. Rejected values are not errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelValidityError {
    #[error("invalid model validity snapshot: {0}")]
    InvalidSnapshot(String),
}

/// Classify `value` for argument `name` against `snapshot`.
///
/// Applies the contract steps in order: peel a known trailing effort level,
/// `@alias`, bare-alias rejection, `provider/rest`, known model, then the
/// effort-suffix message replacement for already-rejected values.
pub fn classify_model_value(
    name: &str,
    value: &str,
    snapshot: &ModelValiditySnapshot,
) -> Result<ClassifyModelValueResultWire, ModelValidityError> {
    validate_snapshot(snapshot)?;
    let (body, peeled_effort) = split_trailing_effort(value, snapshot);
    match classify_body(body, snapshot) {
        Accept::Yes { kind, provider } => Ok(ClassifyModelValueResultWire {
            ok: true,
            message: String::new(),
            kind: Some(kind.to_string()),
            provider,
            suggestions: Vec::new(),
        }),
        Accept::No { reason } => {
            let (message, suggestions) = rejected_message(
                name,
                value,
                body,
                peeled_effort,
                reason,
                snapshot,
            );
            Ok(ClassifyModelValueResultWire {
                ok: false,
                message,
                kind: None,
                provider: None,
                suggestions,
            })
        }
    }
}

fn validate_snapshot(
    snapshot: &ModelValiditySnapshot,
) -> Result<(), ModelValidityError> {
    if snapshot.schema_version != 1 {
        return Err(ModelValidityError::InvalidSnapshot(format!(
            "unsupported schema_version {}, expected 1",
            snapshot.schema_version
        )));
    }
    Ok(())
}

enum Accept {
    Yes {
        kind: &'static str,
        provider: Option<String>,
    },
    No {
        reason: RejectReason,
    },
}

enum RejectReason {
    AliasMiss,
    BareAlias,
    ProviderMiss { provider: String, rest: String },
    BareMiss,
}

fn split_trailing_effort<'a>(
    value: &'a str,
    snapshot: &ModelValiditySnapshot,
) -> (&'a str, Option<&'a str>) {
    match value.rfind('@') {
        Some(at) if at > 0 => {
            let candidate = &value[at + 1..];
            if snapshot
                .effort_levels
                .iter()
                .any(|level| level == candidate)
            {
                (&value[..at], Some(candidate))
            } else {
                (value, None)
            }
        }
        _ => (value, None),
    }
}

fn classify_body(body: &str, snapshot: &ModelValiditySnapshot) -> Accept {
    if let Some(name) = body.strip_prefix('@') {
        if snapshot.aliases.iter().any(|alias| alias == name) {
            return Accept::Yes {
                kind: "alias",
                provider: None,
            };
        }
        return Accept::No {
            reason: RejectReason::AliasMiss,
        };
    }
    if snapshot.aliases.iter().any(|alias| alias == body) {
        return Accept::No {
            reason: RejectReason::BareAlias,
        };
    }
    if let Some((provider, rest)) = body.split_once('/') {
        if snapshot.providers.iter().any(|name| name == provider) {
            return Accept::Yes {
                kind: "provider_model",
                provider: Some(provider.to_string()),
            };
        }
        return Accept::No {
            reason: RejectReason::ProviderMiss {
                provider: provider.to_string(),
                rest: rest.to_string(),
            },
        };
    }
    if let Some(provider) = snapshot.models.get(body) {
        return Accept::Yes {
            kind: "known_model",
            provider: Some(provider.clone()),
        };
    }
    Accept::No {
        reason: RejectReason::BareMiss,
    }
}

fn would_accept(body: &str, snapshot: &ModelValiditySnapshot) -> bool {
    matches!(classify_body(body, snapshot), Accept::Yes { .. })
}

fn rejected_message(
    arg_name: &str,
    original_value: &str,
    body: &str,
    peeled_effort: Option<&str>,
    reason: RejectReason,
    snapshot: &ModelValiditySnapshot,
) -> (String, Vec<String>) {
    // Step 6 only replaces the message of an already-rejected value. When
    // the value has a trailing `@suffix` whose suffix is not an effort
    // level and the body before that `@` would itself be accepted, the
    // message says the suffix is not an effort level.
    if peeled_effort.is_none() {
        if let Some((before, suffix)) =
            split_non_effort_suffix(original_value, snapshot)
        {
            if would_accept(before, snapshot) {
                let levels = snapshot.effort_levels.join(", ");
                let message = format!(
                    "Argument `{arg_name}` expects a model, got \
                     `{original_value}`: `{suffix}` is not an effort level \
                     ({levels})"
                );
                return (message, Vec::new());
            }
        }
    }
    match reason {
        RejectReason::BareAlias => {
            let mut suggestions =
                suggest_closest(body, snapshot.aliases.iter());
            suggestions = suggestions
                .into_iter()
                .map(|name| format!("@{name}"))
                .collect();
            if let Some(peeled) = peeled_effort {
                suggestions = suggestions
                    .into_iter()
                    .map(|suggestion| format!("{suggestion}@{peeled}"))
                    .collect();
            }
            let message = format!(
                "Argument `{arg_name}` expects a model, got \
                 `{original_value}`: model aliases need `@`{}",
                did_you_mean(&suggestions)
            );
            (message, suggestions)
        }
        RejectReason::AliasMiss => {
            let candidates: Vec<String> = snapshot
                .aliases
                .iter()
                .map(|name| format!("@{name}"))
                .collect();
            let mut suggestions = suggest_closest(body, candidates.iter());
            if let Some(peeled) = peeled_effort {
                suggestions = suggestions
                    .into_iter()
                    .map(|suggestion| format!("{suggestion}@{peeled}"))
                    .collect();
            }
            let message = format!(
                "Argument `{arg_name}` expects a model, got \
                 `{original_value}`: not a known model, `@alias`, or \
                 `provider/model`{}",
                did_you_mean(&suggestions)
            );
            (message, suggestions)
        }
        RejectReason::ProviderMiss { provider, rest } => {
            let mut suggestions: Vec<String> =
                suggest_closest(&provider, snapshot.providers.iter())
                    .into_iter()
                    .map(|name| format!("{name}/{rest}"))
                    .collect();
            if let Some(peeled) = peeled_effort {
                suggestions = suggestions
                    .into_iter()
                    .map(|suggestion| format!("{suggestion}@{peeled}"))
                    .collect();
            }
            let message = format!(
                "Argument `{arg_name}` expects a model, got \
                 `{original_value}`: provider `{provider}` is not installed{}",
                did_you_mean(&suggestions)
            );
            (message, suggestions)
        }
        RejectReason::BareMiss => {
            let model_keys: Vec<&String> = snapshot.models.keys().collect();
            let mut suggestions = suggest_closest(body, model_keys.iter());
            if let Some(peeled) = peeled_effort {
                suggestions = suggestions
                    .into_iter()
                    .map(|suggestion| format!("{suggestion}@{peeled}"))
                    .collect();
            }
            let message = format!(
                "Argument `{arg_name}` expects a model, got \
                 `{original_value}`: not a known model, `@alias`, or \
                 `provider/model`{}",
                did_you_mean(&suggestions)
            );
            (message, suggestions)
        }
    }
}

fn split_non_effort_suffix<'a>(
    value: &'a str,
    snapshot: &ModelValiditySnapshot,
) -> Option<(&'a str, &'a str)> {
    let at = value.rfind('@')?;
    if at == 0 {
        return None;
    }
    let suffix = &value[at + 1..];
    if suffix.is_empty()
        || snapshot.effort_levels.iter().any(|level| level == suffix)
    {
        return None;
    }
    Some((&value[..at], suffix))
}

fn did_you_mean(suggestions: &[String]) -> String {
    crate::macro_input_types::did_you_mean_suffix(suggestions)
}

#[allow(dead_code)]
fn _sorted_unique(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            out.push(value.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    pub(crate) fn fixture_snapshot() -> ModelValiditySnapshot {
        ModelValiditySnapshot {
            schema_version: 1,
            providers: vec![
                "claude".to_string(),
                "codex".to_string(),
                "fakey".to_string(),
            ],
            models: BTreeMap::from([
                ("opus".to_string(), "claude".to_string()),
                ("sonnet".to_string(), "claude".to_string()),
                ("gpt-5.6-sol".to_string(), "codex".to_string()),
                ("fakey-large".to_string(), "fakey".to_string()),
            ]),
            aliases: vec!["large".to_string(), "default".to_string()],
            effort_levels: vec![
                "none".to_string(),
                "minimal".to_string(),
                "low".to_string(),
                "medium".to_string(),
                "high".to_string(),
                "xhigh".to_string(),
                "max".to_string(),
            ],
        }
    }

    #[test]
    fn accepts_the_corpus() {
        let snapshot = fixture_snapshot();
        for (value, kind, provider) in [
            ("@large", "alias", None),
            ("claude/opus@xhigh", "provider_model", Some("claude")),
            ("codex/new-model", "provider_model", Some("codex")),
            ("fakey-large", "known_model", Some("fakey")),
            ("fakey/fakey-large", "provider_model", Some("fakey")),
        ] {
            let result =
                classify_model_value("claude_model", value, &snapshot).unwrap();
            assert!(result.ok, "{value}: {}", result.message);
            assert_eq!(result.kind.as_deref(), Some(kind), "{value}");
            assert_eq!(result.provider.as_deref(), provider, "{value}");
        }
    }

    #[test]
    fn rejects_with_canonical_messages() {
        let snapshot = fixture_snapshot();
        let result =
            classify_model_value("claude_model", "opsu", &snapshot).unwrap();
        assert!(!result.ok);
        assert_eq!(
            result.message,
            "Argument `claude_model` expects a model, got `opsu`: not a \
             known model, `@alias`, or `provider/model`; did you mean `opus`?"
        );
        assert_eq!(result.suggestions, vec!["opus".to_string()]);

        let result =
            classify_model_value("claude_model", "large", &snapshot).unwrap();
        assert_eq!(
            result.message,
            "Argument `claude_model` expects a model, got `large`: model \
             aliases need `@`; did you mean `@large`?"
        );

        let result =
            classify_model_value("claude_model", "@lareg", &snapshot).unwrap();
        assert_eq!(
            result.message,
            "Argument `claude_model` expects a model, got `@lareg`: not a \
             known model, `@alias`, or `provider/model`; did you mean \
             `@large`?"
        );

        let result =
            classify_model_value("claude_model", "cluade/opus", &snapshot)
                .unwrap();
        assert_eq!(
            result.message,
            "Argument `claude_model` expects a model, got `cluade/opus`: \
             provider `cluade` is not installed; did you mean \
             `claude/opus`?"
        );

        let result =
            classify_model_value("claude_model", "opus@turbo", &snapshot)
                .unwrap();
        assert_eq!(
            result.message,
            "Argument `claude_model` expects a model, got `opus@turbo`: \
             `turbo` is not an effort level (none, minimal, low, medium, \
             high, xhigh, max)"
        );
        assert!(result.suggestions.is_empty());
    }

    #[test]
    fn effort_step6_keeps_generic_and_provider_messages() {
        let snapshot = fixture_snapshot();
        let generic =
            classify_model_value("claude_model", "notaname@turbo", &snapshot)
                .unwrap();
        assert!(generic.message.contains("not a known model"));

        let provider = classify_model_value(
            "claude_model",
            "cluade/opus@turbo",
            &snapshot,
        )
        .unwrap();
        assert!(provider
            .message
            .contains("provider `cluade` is not installed"));

        let open = classify_model_value(
            "claude_model",
            "claude/opus@turbo",
            &snapshot,
        )
        .unwrap();
        assert!(open.ok);
    }

    #[test]
    fn malformed_snapshot_is_an_error() {
        let mut snapshot = fixture_snapshot();
        snapshot.schema_version = 2;
        let error = classify_model_value("claude_model", "opus", &snapshot)
            .unwrap_err();
        assert!(error.to_string().contains("schema_version"));
    }
}
