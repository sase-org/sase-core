//! Recorded model identities used by concrete agent turns.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// The kind of row represented by one projected session turn fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentModelTurnKind {
    Agent,
    Gate,
    Monitor,
    Other,
}

/// Recorded model facts for one row in the caller's causal session projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentModelTurnWire {
    pub kind: AgentModelTurnKind,
    pub provider: Option<String>,
    pub model: Option<String>,
}

/// One distinct recorded provider/model identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize)]
pub struct AgentModelIdentityWire {
    pub provider: Option<String>,
    pub model: String,
}

/// Request to summarize ordered concrete session turn facts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentModelSummaryRequestWire {
    pub turns: Vec<AgentModelTurnWire>,
}

/// Stable, first-use ordered model summary for a loaded session projection.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentModelSummaryWire {
    pub models: Vec<AgentModelIdentityWire>,
    pub latest: Option<AgentModelIdentityWire>,
    pub unknown_turn_count: usize,
}

/// Summarize recorded models without consulting aliases or provider defaults.
pub fn summarize_agent_models(
    request: AgentModelSummaryRequestWire,
) -> AgentModelSummaryWire {
    let mut summary = AgentModelSummaryWire::default();
    let mut seen = HashSet::new();

    for turn in request.turns {
        if turn.kind != AgentModelTurnKind::Agent {
            continue;
        }

        let Some(identity) = recorded_identity(turn.provider, turn.model)
        else {
            summary.unknown_turn_count += 1;
            continue;
        };

        if seen.insert(identity.clone()) {
            summary.models.push(identity.clone());
        }
        summary.latest = Some(identity);
    }

    summary
}

fn recorded_identity(
    provider: Option<String>,
    model: Option<String>,
) -> Option<AgentModelIdentityWire> {
    let mut model = model?.trim().to_owned();
    if model.is_empty() || model.starts_with('@') {
        return None;
    }

    let provider = provider
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());

    let provider = match provider {
        Some(provider) => {
            let prefix = format!("{provider}/");
            if let Some(rest) = model.strip_prefix(&prefix) {
                model = rest.trim().to_owned();
            }
            Some(provider)
        }
        None => {
            // Older records sometimes store the explicit provider/model pair
            // in the model field. With no provider fact, this syntax is the
            // only permitted source for recovering the provider identity. A
            // plain model remains known with an unknown provider.
            if let Some((prefix, rest)) = model.split_once('/') {
                let provider = prefix.trim().to_owned();
                let normalized = rest.trim().to_owned();
                if provider.is_empty()
                    || normalized.is_empty()
                    || normalized.starts_with('@')
                {
                    return None;
                }
                model = normalized;
                Some(provider)
            } else {
                None
            }
        }
    };

    if model.is_empty() || model.starts_with('@') {
        return None;
    }

    Some(AgentModelIdentityWire { provider, model })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(
        kind: AgentModelTurnKind,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> AgentModelTurnWire {
        AgentModelTurnWire {
            kind,
            provider: provider.map(str::to_owned),
            model: model.map(str::to_owned),
        }
    }

    #[test]
    fn keeps_first_use_order_and_tracks_latest_repeat() {
        let summary = summarize_agent_models(AgentModelSummaryRequestWire {
            turns: vec![
                turn(AgentModelTurnKind::Agent, Some("codex"), Some("gpt-5")),
                turn(AgentModelTurnKind::Agent, Some("claude"), Some("opus")),
                turn(AgentModelTurnKind::Agent, Some("codex"), Some("gpt-5")),
            ],
        });

        assert_eq!(
            summary.models,
            vec![
                AgentModelIdentityWire {
                    provider: Some("codex".to_owned()),
                    model: "gpt-5".to_owned(),
                },
                AgentModelIdentityWire {
                    provider: Some("claude".to_owned()),
                    model: "opus".to_owned(),
                },
            ]
        );
        assert_eq!(summary.latest, summary.models.first().cloned());
    }

    #[test]
    fn normalizes_recorded_identity_without_resolving_aliases() {
        let summary = summarize_agent_models(AgentModelSummaryRequestWire {
            turns: vec![
                turn(
                    AgentModelTurnKind::Agent,
                    Some(" claude "),
                    Some(" claude/opus "),
                ),
                turn(AgentModelTurnKind::Agent, None, Some("claude/opus")),
                turn(
                    AgentModelTurnKind::Agent,
                    Some("codex"),
                    Some("vendor/model-v2"),
                ),
                turn(AgentModelTurnKind::Agent, None, Some("legacy-model")),
                turn(AgentModelTurnKind::Agent, None, Some("@current-default")),
                turn(AgentModelTurnKind::Agent, None, Some("   ")),
                turn(AgentModelTurnKind::Agent, None, None),
            ],
        });

        assert_eq!(summary.models.len(), 3);
        assert_eq!(summary.models[0].provider.as_deref(), Some("claude"));
        assert_eq!(summary.models[0].model, "opus");
        assert_eq!(summary.models[1].model, "vendor/model-v2");
        assert_eq!(summary.models[2].provider, None);
        assert_eq!(summary.models[2].model, "legacy-model");
        assert_eq!(summary.unknown_turn_count, 3);
    }

    #[test]
    fn providers_are_part_of_the_identity_and_non_agent_rows_are_ignored() {
        let summary = summarize_agent_models(AgentModelSummaryRequestWire {
            turns: vec![
                turn(AgentModelTurnKind::Agent, Some("codex"), Some("same")),
                turn(AgentModelTurnKind::Agent, Some("claude"), Some("same")),
                turn(
                    AgentModelTurnKind::Monitor,
                    Some("codex"),
                    Some("ignored"),
                ),
                turn(AgentModelTurnKind::Gate, None, Some("ignored")),
                turn(AgentModelTurnKind::Other, None, Some("ignored")),
            ],
        });

        assert_eq!(summary.models.len(), 2);
        assert_eq!(summary.unknown_turn_count, 0);
    }

    #[test]
    fn empty_input_has_no_fabricated_default() {
        assert_eq!(
            summarize_agent_models(AgentModelSummaryRequestWire {
                turns: vec![]
            }),
            AgentModelSummaryWire::default()
        );
    }
}
