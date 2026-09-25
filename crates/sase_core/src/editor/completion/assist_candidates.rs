//! Catalog-backed candidates: xprompt, agent, wait/hold/queue/identity/hood,
//! and snippet completions, plus the shared item-rendering helpers they all
//! use.

use crate::agent_identity::agent_name_ancestors;
use crate::editor::directive::{
    build_directive_static_value_candidates,
    build_filtered_directive_keyword_candidates, directive_allows_keywords,
    directive_metadata, directive_metadata_with_flags,
};
use crate::editor::wire::{
    AgentCompletionEntry, CompletionCandidate, CompletionContext,
    CompletionList, DirectiveCompletionInventories, DirectiveSyntaxForm,
    EditorRange, EditorTextEdit, XpromptAssistEntry, XpromptInputHint,
};
use crate::{EditorSnippetEntryWire, EditorXpromptCatalogEntryWire};
use std::collections::{BTreeMap, BTreeSet};

pub fn assist_entries_from_catalog(
    entries: &[EditorXpromptCatalogEntryWire],
) -> Vec<XpromptAssistEntry> {
    entries
        .iter()
        .map(|entry| {
            let reference_prefix =
                entry.reference_prefix.as_deref().unwrap_or("#").to_string();
            let insertion = entry
                .insertion
                .clone()
                .unwrap_or_else(|| format!("{reference_prefix}{}", entry.name));
            XpromptAssistEntry {
                name: entry.name.clone(),
                display_label: entry.display_label.clone(),
                insertion,
                reference_prefix,
                kind: entry.kind.clone(),
                source_bucket: entry.source_bucket.clone(),
                project: entry.project.clone(),
                tags: entry.tags.clone(),
                input_signature: entry.input_signature.clone(),
                inputs: entry
                    .inputs
                    .iter()
                    .map(|input| XpromptInputHint {
                        name: input.name.clone(),
                        r#type: input.r#type.clone(),
                        description: input.description.clone(),
                        required: input.required,
                        default_display: input.default_display.clone(),
                        position: input.position,
                        repeatable: input.repeatable,
                    })
                    .collect(),
                content_preview: entry.content_preview.clone(),
                description: entry.description.clone(),
                source_path_display: entry.source_path_display.clone(),
                definition_path: entry.definition_path.clone(),
                definition_range: entry.definition_range,
                is_skill: entry.is_skill,
                skill_name: entry.skill_name.clone(),
                memory_type: entry.memory_type,
            }
        })
        .collect()
}
pub fn build_xprompt_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[XpromptAssistEntry],
) -> CompletionList {
    let slash_skill = token.starts_with('/');
    let standalone_only = token.starts_with("#!");
    let partial = if slash_skill {
        token.strip_prefix('/').unwrap_or_default()
    } else if standalone_only {
        token.strip_prefix("#!").unwrap_or_default()
    } else {
        token.strip_prefix('#').unwrap_or(token)
    };
    let partial_lower = partial.to_lowercase();
    let mut candidates = Vec::new();

    for entry in entries {
        // Slash completion is keyed on the provider skill name (`/foo`) while
        // `#` completion is keyed on the xprompt reference (`#skill/foo`).
        let match_name = if slash_skill {
            let Some(skill_name) = entry.skill_name.as_deref() else {
                continue;
            };
            skill_name
        } else {
            entry.name.as_str()
        };
        if slash_skill && !entry.is_skill {
            continue;
        }
        if standalone_only && entry.reference_prefix != "#!" {
            continue;
        }
        if !match_name.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        let insertion = if slash_skill {
            format!("/{match_name}")
        } else {
            entry.insertion.clone()
        };
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: entry
                .input_signature
                .clone()
                .or_else(|| entry.kind.clone()),
            documentation: entry
                .description
                .clone()
                .or_else(|| entry.content_preview.clone()),
            is_dir: false,
            name: match_name.to_string(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, partial),
        candidates,
    }
}
pub fn build_xprompt_arg_name_candidates(
    entry: &XpromptAssistEntry,
    used_arg_names: &BTreeSet<String>,
    token: &str,
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let partial = token.to_lowercase();
    let mut candidates = Vec::new();
    for input in &entry.inputs {
        if used_arg_names.contains(&input.name) {
            continue;
        }
        if !input.name.to_lowercase().starts_with(&partial) {
            continue;
        }
        let insertion = format!("{}=", input.name);
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: Some(input_label(input)),
            documentation: input_documentation(input),
            is_dir: false,
            name: input.name.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}
pub fn build_agent_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
) -> CompletionList {
    build_agent_completion_candidates_filtered(
        token,
        replacement_range,
        entries,
        selected_values,
        &[],
        false,
    )
}
fn build_agent_completion_candidates_filtered(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
    excluded_kinds: &[&str],
    prioritize_waiting: bool,
) -> CompletionList {
    if token.contains('=') {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    }

    let partial = token.to_lowercase();
    let selected = selected_values
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    let mut ordered_entries = entries.iter().collect::<Vec<_>>();
    ordered_entries.sort_by_key(|entry| {
        (
            if prioritize_waiting {
                agent_status_rank(&entry.status)
            } else {
                0
            },
            agent_kind_rank(agent_entry_kind(entry)),
        )
    });
    for entry in ordered_entries {
        let kind = agent_entry_kind(entry);
        if excluded_kinds.contains(&kind) {
            continue;
        }
        let insertion = entry.name.trim();
        if insertion.is_empty()
            || selected.contains(insertion)
            || !seen.insert(insertion.to_string())
            || !agent_entry_matches(kind, insertion, &partial)
        {
            continue;
        }
        let filter_name = if kind == "tribe" && !partial.starts_with('@') {
            insertion.strip_prefix('@').unwrap_or(insertion).to_string()
        } else {
            insertion.to_string()
        };
        let detail = agent_entry_detail(entry, kind);
        candidates.push(CompletionCandidate {
            display: insertion.to_string(),
            insertion: insertion.to_string(),
            detail,
            documentation: (!entry.documentation.is_empty())
                .then(|| entry.documentation.clone()),
            is_dir: false,
            name: filter_name,
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion.to_string(),
            }),
            additional_edits: Vec::new(),
            kind: kind.to_string(),
            project: entry.project.clone(),
            status: entry.status.clone(),
        });
    }
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
pub fn build_wait_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
) -> CompletionList {
    build_wait_completion_candidates_for_form(
        token,
        replacement_range,
        entries,
        selected_values,
        DirectiveSyntaxForm::Parenthesized,
    )
}
pub fn build_wait_completion_candidates_for_form(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
    syntax_form: DirectiveSyntaxForm,
) -> CompletionList {
    build_wait_completion_candidates_for_form_with_flags(
        token,
        replacement_range,
        entries,
        selected_values,
        syntax_form,
        &[],
    )
}
pub fn build_wait_completion_candidates_for_form_with_flags(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
    syntax_form: DirectiveSyntaxForm,
    enabled_feature_flags: &[String],
) -> CompletionList {
    let mut candidates = Vec::new();
    let wait = directive_metadata("wait");
    if !token.contains('=')
        && wait.is_some_and(|metadata| {
            directive_allows_keywords(metadata, syntax_form)
        })
    {
        let selected_keywords: Vec<String> = selected_values
            .iter()
            .filter(|value| value.contains('='))
            .cloned()
            .collect();
        if let Some(metadata) = wait {
            candidates.extend(
                build_filtered_directive_keyword_candidates(
                    metadata,
                    token,
                    &selected_keywords,
                    replacement_range,
                    enabled_feature_flags,
                )
                .candidates,
            );
        }
    }
    candidates.extend(
        build_agent_completion_candidates_filtered(
            token,
            replacement_range,
            entries,
            selected_values,
            &["proc"],
            false,
        )
        .candidates,
    );
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
pub(crate) fn build_hold_completion_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
    token: &str,
    replacement: Option<EditorRange>,
) -> CompletionList {
    let Some(metadata) = directive_metadata_with_flags(
        "hold",
        &inventories.enabled_feature_flags,
    ) else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let syntax_form = context
        .syntax_form()
        .unwrap_or(DirectiveSyntaxForm::Parenthesized);
    let mut candidates = Vec::new();
    if !token.contains('=') && directive_allows_keywords(metadata, syntax_form)
    {
        candidates.extend(
            build_filtered_directive_keyword_candidates(
                metadata,
                token,
                context.selected_keywords(),
                replacement,
                &inventories.enabled_feature_flags,
            )
            .candidates,
        );
    }
    candidates.extend(
        build_directive_static_value_candidates(
            metadata.positional_suggestions,
            token,
            replacement,
        )
        .candidates
        .into_iter()
        .filter(|candidate| {
            !context
                .selected_values
                .iter()
                .any(|selected| selected == &candidate.insertion)
        }),
    );
    candidates.extend(
        build_agent_completion_candidates_filtered(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
            &["hood"],
            true,
        )
        .candidates,
    );
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
pub fn build_identity_target_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    required_kind: &str,
    selected_values: &[String],
) -> CompletionList {
    let filtered: Vec<AgentCompletionEntry> = entries
        .iter()
        .filter(|entry| agent_entry_kind(entry) == required_kind)
        .cloned()
        .collect();
    build_agent_completion_candidates(
        token,
        replacement_range,
        &filtered,
        selected_values,
    )
}
pub(crate) fn build_queue_completion_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
    token: &str,
    replacement: Option<EditorRange>,
) -> CompletionList {
    let Some(metadata) = directive_metadata_with_flags(
        "queue",
        &inventories.enabled_feature_flags,
    ) else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let syntax_form = context
        .syntax_form()
        .unwrap_or(DirectiveSyntaxForm::Parenthesized);
    let mut selected_keywords = context.selected_keywords().to_vec();
    if queue_has_positional_capacity(&context.selected_values) {
        selected_keywords.push("capacity".to_string());
    }
    let mut candidates = Vec::new();
    if !token.contains('=') && directive_allows_keywords(metadata, syntax_form)
    {
        candidates.extend(
            build_filtered_directive_keyword_candidates(
                metadata,
                token,
                &selected_keywords,
                replacement,
                &inventories.enabled_feature_flags,
            )
            .candidates,
        );
    }
    if !queue_has_capacity_assignment(
        &context.selected_values,
        &selected_keywords,
    ) {
        candidates.extend(
            build_directive_static_value_candidates(
                metadata.positional_suggestions,
                token,
                replacement,
            )
            .candidates,
        );
    }
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
fn queue_has_positional_capacity(selected_values: &[String]) -> bool {
    selected_values
        .iter()
        .any(|value| !value.contains('=') && !value.trim().is_empty())
}
fn queue_has_capacity_assignment(
    selected_values: &[String],
    selected_keywords: &[String],
) -> bool {
    queue_has_positional_capacity(selected_values)
        || selected_keywords
            .iter()
            .any(|keyword| keyword.eq_ignore_ascii_case("capacity"))
}
fn agent_entry_kind(entry: &AgentCompletionEntry) -> &str {
    match entry.kind.as_str() {
        "family" | "session" => "session",
        "clan" => "clan",
        "hood" => "hood",
        "tribe" => "tribe",
        "proc" => "proc",
        _ => "agent",
    }
}
fn agent_kind_rank(kind: &str) -> u8 {
    match kind {
        "keyword" => 0,
        "hood" => 1,
        "tribe" => 2,
        "clan" => 3,
        "family" | "session" => 4,
        "agent" => 5,
        "proc" => 6,
        _ => 7,
    }
}
fn agent_status_rank(status: &str) -> u8 {
    match status.to_ascii_uppercase().as_str() {
        "WAITING" => 0,
        "QUEUED" => 1,
        _ => 2,
    }
}
fn agent_entry_matches(kind: &str, insertion: &str, partial: &str) -> bool {
    let insertion = insertion.to_lowercase();
    if kind != "tribe" {
        return insertion.starts_with(partial);
    }
    let bare = insertion.strip_prefix('@').unwrap_or(&insertion);
    insertion.starts_with(partial) || bare.starts_with(partial)
}
fn agent_entry_detail(
    entry: &AgentCompletionEntry,
    kind: &str,
) -> Option<String> {
    if !entry.detail.is_empty() {
        return Some(entry.detail.clone());
    }
    if kind != "agent" && kind != "proc" {
        return (entry.member_count > 0).then(|| {
            let suffix = if entry.member_count == 1 {
                "member"
            } else {
                "members"
            };
            format!("{kind} · {} {suffix}", entry.member_count)
        });
    }
    match (entry.status.is_empty(), entry.project.is_empty()) {
        (false, false) => Some(format!("{} · {}", entry.status, entry.project)),
        (false, true) => Some(entry.status.clone()),
        (true, false) => Some(entry.project.clone()),
        (true, true) => None,
    }
}
pub(crate) fn build_hood_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
) -> CompletionList {
    let mut hood_entries = Vec::new();
    let mut explicit = BTreeSet::new();
    let mut member_counts = BTreeMap::<String, usize>::new();
    for entry in entries {
        if agent_entry_kind(entry) == "hood" {
            explicit.insert(entry.name.clone());
            hood_entries.push(entry.clone());
            continue;
        }
        if agent_entry_kind(entry) == "tribe" {
            continue;
        }
        for hood in agent_name_ancestors(&entry.name).unwrap_or_default() {
            *member_counts.entry(hood).or_insert(0) += 1;
        }
    }
    for (hood, count) in member_counts {
        if explicit.contains(&hood) {
            continue;
        }
        hood_entries.push(AgentCompletionEntry {
            name: hood,
            status: String::new(),
            project: String::new(),
            kind: "hood".to_string(),
            member_count: count,
            detail: String::new(),
            documentation: String::new(),
        });
    }
    build_identity_target_candidates(
        token,
        replacement_range,
        &hood_entries,
        "hood",
        selected_values,
    )
}
pub fn build_snippet_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[EditorSnippetEntryWire],
) -> CompletionList {
    let partial_lower = token.to_lowercase();
    let mut candidates = Vec::new();
    for entry in entries {
        if !entry.trigger.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        candidates.push(CompletionCandidate {
            display: entry.trigger.clone(),
            insertion: entry.template.clone(),
            detail: Some(entry.source.clone()),
            documentation: snippet_documentation(entry),
            is_dir: false,
            name: entry.trigger.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: entry.template.clone(),
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}
fn input_label(input: &XpromptInputHint) -> String {
    let suffix = if input.required { "" } else { "?" };
    let repeatable = if input.repeatable { "…" } else { "" };
    format!("{}{repeatable}{suffix}: {}", input.name, input.r#type)
}
fn input_documentation(input: &XpromptInputHint) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = input
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(default) = &input.default_display {
        parts.push(format!("default: {default}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}
fn snippet_documentation(entry: &EditorSnippetEntryWire) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = entry
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(source_path) = entry
        .source_path_display
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("Source: {source_path}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}
pub(crate) fn shared_extension(
    candidates: &[CompletionCandidate],
    partial: &str,
) -> String {
    if candidates.len() <= 1 {
        return String::new();
    }
    let mut prefix = candidates[0].name.clone();
    for candidate in &candidates[1..] {
        prefix = common_prefix(&prefix, &candidate.name);
    }
    if prefix.len() > partial.len() {
        prefix[partial.len()..].to_string()
    } else {
        String::new()
    }
}
fn common_prefix(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_ch), (_, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if left_ch != right_ch {
            break;
        }
        end = left_idx + left_ch.len_utf8();
    }
    left[..end].to_string()
}
