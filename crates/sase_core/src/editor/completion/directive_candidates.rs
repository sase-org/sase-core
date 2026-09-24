//! Directive clause and value candidates, including machine, finalizer, and
//! model vocabularies. Aggregates the assist-candidate builders above.

use super::assist_candidates::{
    build_agent_completion_candidates, build_hold_completion_candidates,
    build_hood_completion_candidates, build_identity_target_candidates,
    build_queue_completion_candidates,
    build_wait_completion_candidates_for_form_with_flags, shared_extension,
};
use crate::editor::directive::{
    build_bead_completion_candidates, build_directive_static_value_candidates,
    build_filtered_directive_keyword_candidates,
    directive_is_hidden_from_name_completion_with_flags,
    directive_metadata_with_flags, wait_queue_keyword_retired,
};
use crate::editor::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, DirectiveClauseKind, DirectiveCompletionInventories,
    DirectiveFinalizerEntry, DirectiveModelEntry, DirectiveSyntaxForm,
    DirectiveValueRole, EditorRange, EditorTextEdit,
};

pub fn build_directive_clause_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
) -> CompletionList {
    let token = context
        .token
        .as_ref()
        .map(|token| token.text.as_str())
        .unwrap_or_default();
    let replacement = Some(context.replacement_range);
    match context.kind {
        CompletionContextKind::DirectiveName => {
            return crate::editor::directive::build_directive_completion_candidates_with_flags(
                token,
                &inventories.enabled_feature_flags,
            );
        }
        CompletionContextKind::DirectiveArgumentKeyword => {
            if let Some(metadata) =
                context.directive_name.as_deref().and_then(|name| {
                    directive_metadata_with_flags(
                        name,
                        &inventories.enabled_feature_flags,
                    )
                })
            {
                let mut list = build_filtered_directive_keyword_candidates(
                    metadata,
                    token,
                    context.selected_keywords(),
                    replacement,
                    &inventories.enabled_feature_flags,
                );
                if metadata.dynamic_keyword_role
                    == Some(DirectiveValueRole::ModelAliasKey)
                {
                    list.candidates.extend(model_alias_key_candidates(
                        token,
                        inventories,
                        context.selected_keywords(),
                        &context.selected_values,
                        replacement,
                    ));
                }
                return list;
            }
        }
        CompletionContextKind::DirectiveArgumentValue => {
            return build_directive_value_candidates(
                context,
                inventories,
                token,
                replacement,
            );
        }
        CompletionContextKind::DirectiveArgument => {}
        _ => {
            return CompletionList {
                candidates: Vec::new(),
                shared_extension: String::new(),
            };
        }
    }

    let Some(name) = context.directive_name.as_deref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    if directive_is_hidden_from_name_completion_with_flags(
        name,
        &inventories.enabled_feature_flags,
    ) {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    }
    if name == "wait" {
        return build_wait_completion_candidates_for_form_with_flags(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
            context
                .syntax_form()
                .unwrap_or(DirectiveSyntaxForm::Parenthesized),
            &inventories.enabled_feature_flags,
        );
    }
    if name == "hold" {
        return build_hold_completion_candidates(
            context,
            inventories,
            token,
            replacement,
        );
    }
    if name == "queue" {
        return build_queue_completion_candidates(
            context,
            inventories,
            token,
            replacement,
        );
    }
    if name == "model" {
        let mut candidates = model_value_candidates(
            token,
            inventories,
            context.active_keyword(),
            replacement,
        );
        if context.syntax_form() == Some(DirectiveSyntaxForm::Parenthesized)
            && context.clause_kind() == Some(DirectiveClauseKind::Positional)
        {
            candidates.extend(model_alias_key_candidates(
                token,
                inventories,
                context.selected_keywords(),
                &context.selected_values,
                replacement,
            ));
        }
        return CompletionList {
            shared_extension: shared_extension(&candidates, token),
            candidates,
        };
    }
    if context.value_role() == Some(DirectiveValueRole::FinalizerInstance) {
        return finalizer_value_candidates(token, inventories, replacement);
    }
    let Some(metadata) =
        directive_metadata_with_flags(name, &inventories.enabled_feature_flags)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    if metadata.positional_role == Some(DirectiveValueRole::Machine) {
        return machine_value_candidates(token, inventories, replacement);
    }
    // Keyword names are offered only in `DirectiveArgumentKeyword` (and the
    // wait positional mix above). Clan/id positional slots stay free-form.
    build_directive_static_value_candidates(
        metadata.positional_suggestions,
        token,
        replacement,
    )
}
fn build_directive_value_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
    token: &str,
    replacement: Option<EditorRange>,
) -> CompletionList {
    if context.directive_name.as_deref().is_some_and(|name| {
        directive_is_hidden_from_name_completion_with_flags(
            name,
            &inventories.enabled_feature_flags,
        )
    }) {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    }
    if context.directive_name.as_deref() == Some("wait")
        && wait_queue_keyword_retired(
            context.active_keyword().unwrap_or_default(),
            &inventories.enabled_feature_flags,
        )
    {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    }
    match context.value_role() {
        Some(DirectiveValueRole::Bead) => build_bead_completion_candidates(
            &inventories.beads,
            token,
            &context.selected_values,
            &inventories.excluded_bead_ids,
            replacement,
        ),
        Some(DirectiveValueRole::Agent) => build_agent_completion_candidates(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Hood) => build_hood_completion_candidates(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Clan) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "clan",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Session) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "family",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Tribe) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "tribe",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Model) => CompletionList {
            candidates: model_value_candidates(
                token,
                inventories,
                context.active_keyword(),
                replacement,
            ),
            shared_extension: String::new(),
        },
        Some(DirectiveValueRole::FinalizerInstance) => {
            finalizer_value_candidates(token, inventories, replacement)
        }
        Some(DirectiveValueRole::Machine) => {
            machine_value_candidates(token, inventories, replacement)
        }
        _ => {
            let Some(metadata) =
                context.directive_name.as_deref().and_then(|name| {
                    directive_metadata_with_flags(
                        name,
                        &inventories.enabled_feature_flags,
                    )
                })
            else {
                return CompletionList {
                    candidates: Vec::new(),
                    shared_extension: String::new(),
                };
            };
            let values = context
                .active_keyword()
                .and_then(|name| {
                    metadata
                        .keywords
                        .iter()
                        .find(|keyword| keyword.name == name)
                        .map(|keyword| keyword.suggested_values)
                })
                .unwrap_or(metadata.positional_suggestions);
            build_directive_static_value_candidates(values, token, replacement)
        }
    }
}
fn machine_value_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    replacement: Option<EditorRange>,
) -> CompletionList {
    let partial = token.to_lowercase();
    let candidates = inventories
        .machines
        .iter()
        .filter(|entry| {
            entry.alias.to_lowercase().starts_with(&partial)
                || entry.display.to_lowercase().starts_with(&partial)
        })
        .map(|entry| {
            let display = if entry.display.is_empty() {
                entry.alias.clone()
            } else {
                entry.display.clone()
            };
            let detail = [
                entry.status.as_str(),
                entry.provider_ref.as_str(),
                entry.installation_id.as_str(),
            ]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" · ");
            CompletionCandidate {
                display,
                insertion: entry.alias.clone(),
                detail: (!detail.is_empty()).then_some(detail),
                documentation: (!entry.documentation.is_empty())
                    .then(|| entry.documentation.clone()),
                is_dir: false,
                name: entry.alias.clone(),
                replacement: replacement.map(|range| EditorTextEdit {
                    range,
                    new_text: entry.alias.clone(),
                }),
                additional_edits: Vec::new(),
                kind: "machine".to_string(),
                project: String::new(),
                status: entry.status.clone(),
            }
        })
        .collect::<Vec<_>>();
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
const FINALIZER_KIND_ADD: &str = "finalizer";
const FINALIZER_KIND_REMOVE: &str = "finalizer_remove";
const FINALIZER_KIND_CLEAR: &str = "finalizer_clear";
fn finalizer_value_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    replacement: Option<EditorRange>,
) -> CompletionList {
    let removing = token.starts_with('!');
    let query = token.strip_prefix('!').unwrap_or(token).to_lowercase();
    let has_required =
        inventories.finalizers.iter().any(|entry| entry.required);
    let mut entries: Vec<&DirectiveFinalizerEntry> =
        inventories.finalizers.iter().collect();
    entries.sort_by(|left, right| {
        finalizer_policy_rank(left)
            .cmp(&finalizer_policy_rank(right))
            .then_with(|| {
                left.value.to_lowercase().cmp(&right.value.to_lowercase())
            })
    });

    let mut candidates = Vec::new();
    for entry in entries {
        if removing && entry.required {
            continue;
        }
        if !entry.value.to_lowercase().starts_with(&query) {
            continue;
        }
        let insertion = if removing {
            format!("!{}", entry.value)
        } else {
            entry.value.clone()
        };
        let kind = if removing {
            FINALIZER_KIND_REMOVE
        } else {
            FINALIZER_KIND_ADD
        };
        candidates.push(finalizer_candidate(
            &insertion,
            kind,
            finalizer_policy_state(entry),
            finalizer_provider(entry),
            &finalizer_markdown_documentation(entry, removing),
            replacement,
        ));
    }
    if !removing && !has_required && "none".starts_with(&query) {
        candidates.push(finalizer_candidate(
            "none",
            FINALIZER_KIND_CLEAR,
            "clear",
            "",
            "Clear the configured finalizer selection for this launch",
            replacement,
        ));
    }
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
fn finalizer_policy_rank(entry: &DirectiveFinalizerEntry) -> u8 {
    if entry.required {
        0
    } else if entry.is_default {
        1
    } else {
        2
    }
}
fn finalizer_policy_state(entry: &DirectiveFinalizerEntry) -> &'static str {
    if entry.required {
        "required"
    } else if entry.is_default {
        "default"
    } else {
        "optional"
    }
}
fn finalizer_provider(entry: &DirectiveFinalizerEntry) -> &str {
    if !entry.provider_ref.is_empty() {
        &entry.provider_ref
    } else {
        &entry.detail
    }
}
fn finalizer_markdown_documentation(
    entry: &DirectiveFinalizerEntry,
    removing: bool,
) -> String {
    let mut sections = Vec::new();
    if removing {
        sections.push(format!(
            "Remove `{}` from the launch selection.",
            entry.value
        ));
    } else if !entry.documentation.is_empty() {
        sections.push(entry.documentation.clone());
    }
    let provider = finalizer_provider(entry);
    if !provider.is_empty() {
        sections.push(format!("Provider: `{provider}`"));
    }
    if !entry.after.is_empty() {
        sections.push(format!("Depends on: `{}`", entry.after.join("`, `")));
    }
    if let Some(attempts) = entry.max_attempts {
        let noun = if attempts == 1 { "attempt" } else { "attempts" };
        sections.push(format!("Retry policy: {attempts} {noun}"));
    }
    sections.join("\n\n")
}
fn finalizer_candidate(
    insertion: &str,
    kind: &str,
    status: &str,
    detail: &str,
    documentation: &str,
    replacement: Option<EditorRange>,
) -> CompletionCandidate {
    CompletionCandidate {
        display: insertion.to_string(),
        insertion: insertion.to_string(),
        detail: Some(detail.to_string()).filter(|value| !value.is_empty()),
        documentation: Some(documentation.to_string())
            .filter(|value| !value.is_empty()),
        is_dir: false,
        name: insertion.to_string(),
        replacement: replacement.map(|range| EditorTextEdit {
            range,
            new_text: insertion.to_string(),
        }),
        additional_edits: Vec::new(),
        kind: kind.to_string(),
        project: String::new(),
        status: status.to_string(),
    }
}
fn model_value_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    active_keyword: Option<&str>,
    replacement: Option<EditorRange>,
) -> Vec<CompletionCandidate> {
    let partial = token.to_lowercase();
    let self_ref = active_keyword.map(str::to_lowercase);
    inventories
        .models
        .iter()
        .filter(|entry| {
            if self_ref
                .as_ref()
                .is_some_and(|name| model_entry_is_self_ref(entry, name))
            {
                return false;
            }
            entry.value.to_lowercase().starts_with(&partial)
                || entry.display.to_lowercase().starts_with(&partial)
        })
        .map(|entry| {
            let display = if entry.display.is_empty() {
                entry.value.clone()
            } else {
                entry.display.clone()
            };
            CompletionCandidate {
                display,
                insertion: entry.value.clone(),
                detail: (!entry.detail.is_empty())
                    .then(|| entry.detail.clone()),
                documentation: (!entry.documentation.is_empty())
                    .then(|| entry.documentation.clone()),
                is_dir: false,
                name: entry.value.clone(),
                replacement: replacement.map(|range| EditorTextEdit {
                    range,
                    new_text: entry.value.clone(),
                }),
                additional_edits: Vec::new(),
                kind: "model".to_string(),
                project: String::new(),
                status: String::new(),
            }
        })
        .collect()
}
fn model_entry_is_self_ref(entry: &DirectiveModelEntry, keyword: &str) -> bool {
    let keyword = keyword.trim_start_matches('@');
    let value = entry.value.trim_start_matches('@').to_lowercase();
    if value == keyword {
        return true;
    }
    entry
        .display
        .trim_start_matches('@')
        .eq_ignore_ascii_case(keyword)
}
fn model_alias_key_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    selected_keywords: &[String],
    selected_values: &[String],
    replacement: Option<EditorRange>,
) -> Vec<CompletionCandidate> {
    let partial = token.to_lowercase();
    let selected = selected_keywords
        .iter()
        .chain(selected_values.iter())
        .map(|value| {
            value
                .split_once('=')
                .map(|(name, _)| name.trim())
                .unwrap_or(value.as_str())
                .trim_start_matches('@')
                .to_lowercase()
        })
        .collect::<Vec<_>>();
    inventories
        .model_alias_keys
        .iter()
        .filter(|entry| {
            let name = entry.name.to_lowercase();
            !selected.iter().any(|value| value == &name)
                && (name.starts_with(&partial)
                    || format!("{}=", name).starts_with(&partial))
        })
        .map(|entry| {
            let insertion = format!("{}=", entry.name);
            CompletionCandidate {
                display: insertion.clone(),
                insertion: insertion.clone(),
                detail: None,
                documentation: (!entry.documentation.is_empty())
                    .then(|| entry.documentation.clone()),
                is_dir: false,
                name: insertion.clone(),
                replacement: replacement.map(|range| EditorTextEdit {
                    range,
                    new_text: insertion,
                }),
                additional_edits: Vec::new(),
                kind: "keyword".to_string(),
                project: String::new(),
                status: String::new(),
            }
        })
        .collect()
}
