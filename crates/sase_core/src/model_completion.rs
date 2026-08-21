use serde::{Deserialize, Serialize};

pub const MODEL_COMPLETION_ENTRY_WIRE_FIELDS: &[&str] = &[
    "value",
    "display",
    "description",
    "kind",
    "provider",
    "aliases",
    "alias_kind",
    "target_provider",
    "target_model",
    "target_effort",
    "provenance",
    "reference",
    "reference_effort",
    "selector_mode",
    "pool_available",
    "pool_total",
    "config_source",
    "bucket",
    "advisory_label",
    "advisory_severity",
    "provider_model_count",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelCompletionEntryWire {
    pub value: String,
    pub display: String,
    pub description: String,
    pub kind: String,
    pub provider: String,
    pub aliases: Vec<String>,
    pub alias_kind: String,
    pub target_provider: String,
    pub target_model: String,
    pub target_effort: String,
    pub provenance: String,
    pub reference: String,
    pub reference_effort: String,
    pub selector_mode: String,
    pub pool_available: u64,
    pub pool_total: u64,
    pub config_source: String,
    pub bucket: String,
    pub advisory_label: String,
    pub advisory_severity: String,
    pub provider_model_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompletionCandidateWire {
    pub entry: ModelCompletionEntryWire,
    pub filter_text: String,
}

pub fn filter_model_completion_entries(
    entries: &[ModelCompletionEntryWire],
    partial: &str,
) -> Vec<ModelCompletionEntryWire> {
    filter_model_completion_candidates(entries, partial)
        .into_iter()
        .map(|candidate| candidate.entry)
        .collect()
}

pub fn filter_model_completion_candidates(
    entries: &[ModelCompletionEntryWire],
    partial: &str,
) -> Vec<ModelCompletionCandidateWire> {
    let needle = partial.to_lowercase();
    if needle.is_empty() {
        return entries
            .iter()
            .cloned()
            .map(|entry| ModelCompletionCandidateWire {
                filter_text: entry.value.clone(),
                entry,
            })
            .collect();
    }

    if needle.starts_with('@') {
        return entries
            .iter()
            .filter_map(|entry| {
                model_alias_entry_filter_text(entry, &needle).map(
                    |filter_text| ModelCompletionCandidateWire {
                        entry: entry.clone(),
                        filter_text,
                    },
                )
            })
            .collect();
    }

    if let Some((provider, remainder)) = model_provider_scope(entries, &needle)
    {
        return entries
            .iter()
            .filter_map(|entry| {
                scoped_model_entry(entry, &provider, &remainder)
            })
            .collect();
    }

    entries
        .iter()
        .filter_map(|entry| {
            model_entry_filter_text(entry, &needle).map(|filter_text| {
                ModelCompletionCandidateWire {
                    entry: entry.clone(),
                    filter_text,
                }
            })
        })
        .collect()
}

fn model_alias_entry_filter_text(
    entry: &ModelCompletionEntryWire,
    needle: &str,
) -> Option<String> {
    if !is_model_alias_kind(&entry.kind) {
        return None;
    }
    if starts_with_folded(&entry.value, needle) {
        return Some(entry.value.clone());
    }
    entry.aliases.iter().find_map(|alias| {
        let normalized = format!("@{}", alias.trim_start_matches('@'));
        starts_with_folded(&normalized, needle).then_some(normalized)
    })
}

fn model_entry_filter_text(
    entry: &ModelCompletionEntryWire,
    needle: &str,
) -> Option<String> {
    if needle.is_empty() || starts_with_folded(&entry.value, needle) {
        return Some(entry.value.clone());
    }
    entry
        .aliases
        .iter()
        .find(|alias| starts_with_folded(alias, needle))
        .cloned()
}

fn model_provider_scope(
    entries: &[ModelCompletionEntryWire],
    needle: &str,
) -> Option<(String, String)> {
    let (head, remainder) = needle.split_once('/')?;
    if head.is_empty() {
        return None;
    }
    let provider_value = format!("{head}/");
    entries
        .iter()
        .find(|entry| {
            entry.kind == "provider"
                && entry.value.to_lowercase() == provider_value
        })
        .map(|entry| {
            let provider = if entry.provider.is_empty() {
                head.to_string()
            } else {
                entry.provider.clone()
            };
            (provider, remainder.to_string())
        })
}

fn scoped_model_entry(
    entry: &ModelCompletionEntryWire,
    provider: &str,
    remainder: &str,
) -> Option<ModelCompletionCandidateWire> {
    if entry.kind != "model" || !entry.provider.eq_ignore_ascii_case(provider) {
        return None;
    }
    let filter_suffix = model_entry_filter_text(entry, remainder)?;
    let prefix = format!("{provider}/");
    let mut qualified = entry.clone();
    qualified.value = format!("{prefix}{}", entry.value);
    qualified.display = format!("{prefix}{}", entry.display);
    Some(ModelCompletionCandidateWire {
        entry: qualified,
        filter_text: format!("{prefix}{filter_suffix}"),
    })
}

fn is_model_alias_kind(kind: &str) -> bool {
    matches!(kind, "implicit_alias" | "user_alias")
}

fn starts_with_folded(value: &str, folded_needle: &str) -> bool {
    value.to_lowercase().starts_with(folded_needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filters_values_aliases_and_provider_scopes_in_catalog_order() {
        let entries = sample_entries();

        assert_eq!(
            values(filter_model_completion_entries(&entries, "GPT")),
            vec!["gpt-5.6-sol"]
        );
        assert_eq!(
            values(filter_model_completion_entries(&entries, "fa")),
            vec!["claude-fable-5"]
        );
        assert_eq!(
            values(filter_model_completion_entries(&entries, "@")),
            vec!["@default", "@scout"]
        );
        assert_eq!(
            values(filter_model_completion_entries(&entries, "claude/")),
            vec!["claude/claude-fable-5", "claude/opus"]
        );
        assert_eq!(
            values(filter_model_completion_entries(&entries, "Claude/")),
            vec!["claude/claude-fable-5", "claude/opus"]
        );
        assert_eq!(
            values(filter_model_completion_entries(
                &entries,
                "opencode/anthropic/"
            )),
            vec!["opencode/anthropic/claude-sonnet-4-5"]
        );
        assert_eq!(
            values(filter_model_completion_entries(&entries, "anthropic/")),
            vec!["anthropic/claude-sonnet-4-5"]
        );
    }

    #[test]
    fn scoped_candidates_preserve_filter_text() {
        let entries = sample_entries();
        let candidates =
            filter_model_completion_candidates(&entries, "claude/fa");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entry.value, "claude/claude-fable-5");
        assert_eq!(candidates[0].filter_text, "claude/fable");
    }

    #[test]
    fn old_catalog_rows_deserialize_with_additive_defaults() {
        let entry: ModelCompletionEntryWire =
            serde_json::from_value(json!({"value": "model"})).unwrap();
        assert_eq!(entry.value, "model");
        assert_eq!(entry.aliases, Vec::<String>::new());
        assert_eq!(entry.provider_model_count, 0);
    }

    fn values(entries: Vec<ModelCompletionEntryWire>) -> Vec<String> {
        entries.into_iter().map(|entry| entry.value).collect()
    }

    fn sample_entries() -> Vec<ModelCompletionEntryWire> {
        vec![
            model("claude-fable-5", "claude", &["fable"]),
            model("opus", "claude", &[]),
            model("gpt-5.6-sol", "codex", &["gpt56sol"]),
            model("anthropic/claude-sonnet-4-5", "opencode", &[]),
            alias("@default", "implicit_alias", "default"),
            alias("@scout", "user_alias", "scout"),
            provider("claude/", "claude", 2),
            provider("opencode/", "opencode", 1),
        ]
    }

    fn model(
        value: &str,
        provider: &str,
        aliases: &[&str],
    ) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            kind: "model".to_string(),
            provider: provider.to_string(),
            aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
            ..ModelCompletionEntryWire::default()
        }
    }

    fn alias(value: &str, kind: &str, alias: &str) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            kind: kind.to_string(),
            aliases: vec![alias.to_string()],
            ..ModelCompletionEntryWire::default()
        }
    }

    fn provider(
        value: &str,
        provider: &str,
        model_count: u64,
    ) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            kind: "provider".to_string(),
            provider: provider.to_string(),
            provider_model_count: model_count,
            ..ModelCompletionEntryWire::default()
        }
    }
}
