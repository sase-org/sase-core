use super::catalogs::load_model_catalog;
use super::*;

pub(super) fn is_directive_argument_context(
    context: &sase_core::CompletionContext,
) -> bool {
    matches!(
        context.kind,
        CompletionContextKind::DirectiveArgument
            | CompletionContextKind::DirectiveArgumentKeyword
            | CompletionContextKind::DirectiveArgumentValue
    )
}

pub(super) fn needs_host_catalog(
    context: &sase_core::CompletionContext,
) -> bool {
    needs_agent_entries(context) || needs_bead_entries(context)
}

pub(super) fn needs_machine_entries(
    context: &sase_core::CompletionContext,
) -> bool {
    context.directive_name.as_deref() == Some("dispatch")
        || context.value_role() == Some(DirectiveValueRole::Machine)
}

pub(super) fn is_finalizer_value_context(
    context: &sase_core::CompletionContext,
) -> bool {
    context.directive_name.as_deref() == Some("final")
        && matches!(
            context.kind,
            CompletionContextKind::DirectiveArgument
                | CompletionContextKind::DirectiveArgumentValue
        )
}

pub(super) fn needs_agent_entries(
    context: &sase_core::CompletionContext,
) -> bool {
    if context.directive_name.as_deref() == Some("wait")
        && context.kind == CompletionContextKind::DirectiveArgument
    {
        return true;
    }
    if context.directive_name.as_deref() == Some("hold")
        && context.kind == CompletionContextKind::DirectiveArgument
    {
        return true;
    }
    matches!(
        context.value_role(),
        Some(
            DirectiveValueRole::Agent
                | DirectiveValueRole::Clan
                | DirectiveValueRole::Session
                | DirectiveValueRole::Hood
                | DirectiveValueRole::Tribe
        )
    )
}

pub(super) fn needs_bead_entries(
    context: &sase_core::CompletionContext,
) -> bool {
    context.value_role() == Some(DirectiveValueRole::Bead)
}

pub(super) fn needs_model_alias_keys(
    context: &sase_core::CompletionContext,
) -> bool {
    context.directive_name.as_deref() == Some("model")
        && context.syntax_form() == Some(DirectiveSyntaxForm::Parenthesized)
        && context.kind != CompletionContextKind::DirectiveArgumentValue
}

pub(super) fn is_rich_model_value_context(
    context: &sase_core::CompletionContext,
) -> bool {
    context.directive_name.as_deref() == Some("model")
        && (context.kind == CompletionContextKind::DirectiveArgument
            && context.clause_kind() == Some(DirectiveClauseKind::Positional)
            || context.value_role() == Some(DirectiveValueRole::Model))
}

pub(super) fn model_insertion_is_self_ref(
    insertion: &str,
    keyword: &str,
) -> bool {
    let keyword = keyword.trim_start_matches('@');
    insertion
        .trim_start_matches('@')
        .eq_ignore_ascii_case(keyword)
}

pub(super) fn model_alias_keys_from_catalog(
    path: Option<&Path>,
) -> Vec<DirectiveModelAliasKey> {
    let mut keys = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in load_model_catalog(path) {
        if !is_model_alias_kind(&entry.kind) {
            continue;
        }
        let names = if entry.aliases.is_empty() {
            vec![entry.value.trim_start_matches('@').to_string()]
        } else {
            entry.aliases.clone()
        };
        for name in names {
            let name = name.trim_start_matches('@').to_string();
            if name.is_empty() || !seen.insert(name.to_lowercase()) {
                continue;
            }
            keys.push(DirectiveModelAliasKey {
                name,
                documentation: if entry.description.is_empty() {
                    "model alias override".to_string()
                } else {
                    entry.description.clone()
                },
            });
        }
    }
    keys.sort_by(|left, right| left.name.cmp(&right.name));
    keys
}

pub(super) fn xprompt_snippet_items(
    list: CompletionList,
    entries: &[XpromptAssistEntry],
    replacement_range: sase_core::EditorRange,
    append_text_arg_space: bool,
) -> Vec<CompletionItem> {
    list.candidates
        .into_iter()
        .filter_map(|candidate| {
            let entry =
                entries.iter().find(|entry| entry.name == candidate.name)?;
            Some(snippet_completion_item(
                candidate.display,
                xprompt_completion_skeleton(entry, append_text_arg_space),
                candidate.detail,
                candidate.documentation,
                replacement_range,
            ))
        })
        .collect()
}

pub(super) fn xprompt_completion_skeleton(
    entry: &XpromptAssistEntry,
    append_text_arg_space: bool,
) -> String {
    let required = entry
        .inputs
        .iter()
        .filter(|input| input.required)
        .collect::<Vec<_>>();
    match required.as_slice() {
        [] => format!("{} ", entry.insertion),
        // The free-form double-colon shorthand is `:: ` followed by text. An
        // end-of-line completion appends that space so the user lands one
        // keystroke from typing the body; an inline completion keeps `::` so the
        // following text supplies the single delimiter.
        [input] if input.r#type == "text" => {
            if append_text_arg_space {
                format!("{}:: ", entry.insertion)
            } else {
                format!("{}::", entry.insertion)
            }
        }
        [_] => format!("{}:", entry.insertion),
        _ => format!("{}($0)", entry.insertion),
    }
}

/// Whether `range`'s end sits at the end of its line (no trailing text), so the
/// required-text `::` skeleton may be widened to `:: `. Compared in UTF-16 units
/// to match [`EditorPosition::character`].
pub(super) fn replacement_ends_line(
    document: &DocumentSnapshot,
    range: EditorRange,
) -> bool {
    document
        .line_text(range.end.line)
        .map(|line| {
            line.chars().map(char::len_utf16).sum::<usize>()
                == range.end.character as usize
        })
        .unwrap_or(false)
}

pub(super) fn directive_snippet_items(
    token: Option<&str>,
    replacement_range: sase_core::EditorRange,
    enabled_feature_flags: &[String],
    snippet_support: bool,
) -> Vec<CompletionItem> {
    let partial = token
        .unwrap_or_default()
        .strip_prefix('%')
        .unwrap_or_default();
    editor_directive_contract_with_flags(enabled_feature_flags)
        .into_iter()
        .filter(|directive| !directive.recipes.is_empty())
        .filter(|directive| directive.takes_argument)
        .filter(|directive| {
            !editor_directive_is_hidden_from_name_completion_with_flags(
                &directive.name,
                enabled_feature_flags,
            )
        })
        .filter(|directive| {
            directive.name.starts_with(partial)
                || directive
                    .alias
                    .as_deref()
                    .is_some_and(|alias| alias.starts_with(partial))
        })
        .flat_map(|directive| {
            directive.recipes.into_iter().map(move |recipe| {
                if snippet_support {
                    snippet_completion_item(
                        recipe.label,
                        recipe.insert_text,
                        Some(recipe.detail),
                        Some(recipe.documentation),
                        replacement_range,
                    )
                } else {
                    directive_plain_completion_item(recipe, replacement_range)
                }
            })
        })
        .collect()
}

pub(super) fn directive_plain_completion_item(
    recipe: sase_core::DirectiveSnippetRecipeContract,
    replacement_range: sase_core::EditorRange,
) -> CompletionItem {
    CompletionItem {
        label: recipe.label,
        label_details: Some(lsp_types::CompletionItemLabelDetails {
            detail: Some(" template".to_string()),
            description: None,
        }),
        kind: Some(lsp_types::CompletionItemKind::TEXT),
        documentation: Some(lsp_types::Documentation::MarkupContent(
            lsp_types::MarkupContent {
                kind: lsp_types::MarkupKind::Markdown,
                value: recipe.documentation,
            },
        )),
        text_edit: Some(lsp_types::CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(replacement_range),
            new_text: recipe.plain_text,
        })),
        ..Default::default()
    }
}

pub(super) fn sase_snippet_items(
    list: CompletionList,
    replacement_range: sase_core::EditorRange,
) -> Vec<CompletionItem> {
    list.candidates
        .into_iter()
        .map(|candidate| {
            sase_snippet_completion_item(
                candidate.display,
                candidate.insertion,
                candidate.detail,
                candidate.documentation,
                replacement_range,
            )
        })
        .collect()
}

pub(super) fn bool_completion_list() -> CompletionList {
    CompletionList {
        candidates: ["false", "true"]
            .into_iter()
            .map(|value| CompletionCandidate {
                display: value.to_string(),
                insertion: value.to_string(),
                detail: None,
                documentation: None,
                is_dir: false,
                name: value.to_string(),
                replacement: None,
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            })
            .collect(),
        shared_extension: String::new(),
    }
}

pub(super) fn empty_completion_list() -> CompletionList {
    CompletionList {
        candidates: Vec::new(),
        shared_extension: String::new(),
    }
}

pub(super) fn empty_completion_response() -> CompletionResponse {
    CompletionResponse::Array(Vec::new())
}

pub(super) fn ranked_vcs_repo_entries(
    entries: &[VcsRepoEntry],
    query: &str,
) -> Vec<VcsRepoEntry> {
    let query = query.to_lowercase();
    let mut ranked = entries.to_vec();
    ranked.sort_by(|left, right| {
        vcs_repo_name_matches_query(right, &query)
            .cmp(&vcs_repo_name_matches_query(left, &query))
            .then_with(|| compare_vcs_repo_pushed_at(left, right))
            .then_with(|| {
                left.name.to_lowercase().cmp(&right.name.to_lowercase())
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    ranked
}

pub(super) fn vcs_repo_name_matches_query(
    entry: &VcsRepoEntry,
    query: &str,
) -> bool {
    query.is_empty() || entry.name.to_lowercase().starts_with(query)
}

pub(super) fn compare_vcs_repo_pushed_at(
    left: &VcsRepoEntry,
    right: &VcsRepoEntry,
) -> Ordering {
    match (left.pushed_at.as_deref(), right.pushed_at.as_deref()) {
        (Some(left), Some(right)) => right.cmp(left),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub(super) fn model_completion_list(
    partial: &str,
    path: Option<&Path>,
) -> CompletionList {
    let entries = load_model_catalog(path);
    let mut candidates = Vec::new();
    for candidate in filter_model_completion_candidates(&entries, partial) {
        candidates.push(model_completion_candidate(
            candidate.entry,
            candidate.filter_text,
        ));
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

/// Build the `=alias` shortcut completion response for a detected equals
/// context: filter the model catalog to effective alias rows through the
/// shared Rust filter, validate and plan each candidate's edit through the
/// shared edit planner, and hand both to [`model_alias_shortcut_completion_response`]
/// for LSP rendering. Returns the (possibly empty) shortcut response
/// unconditionally; the caller never falls through to unrelated completion
/// once an equals context is detected.
pub(super) fn model_alias_shortcut_completion(
    text: &str,
    position: EditorPosition,
    context: &ModelAliasShortcutContextWire,
    path: Option<&Path>,
) -> CompletionResponse {
    let entries = load_model_catalog(path);
    let candidates =
        editor_filter_model_alias_shortcut_entries(&entries, &context.query)
            .into_iter()
            .filter_map(|entry| {
                let edit = editor_plan_model_alias_shortcut_edit(
                    text,
                    position,
                    &entries,
                    &entry.value,
                )?;
                let filter_text = entry.value.clone();
                Some((model_completion_candidate(entry, filter_text), edit))
            })
            .collect();
    model_alias_shortcut_completion_response(candidates, context)
}

/// Build the `==model` shortcut completion response for a detected double-marker
/// context. The complete catalog is filtered through the shared model shortcut
/// filter so provider-scoped queries can use provider rows even though only
/// concrete model rows are displayed.
pub(super) fn model_shortcut_completion(
    text: &str,
    position: EditorPosition,
    context: &ModelShortcutContextWire,
    path: Option<&Path>,
) -> CompletionResponse {
    let entries = load_model_catalog(path);
    let candidates =
        editor_filter_explicit_model_shortcut_entries(&entries, &context.query)
            .into_iter()
            .filter_map(|entry| {
                let edit = editor_model_shortcut_edit(
                    text,
                    position,
                    &entries,
                    &entry.value,
                )?;
                let filter_text = entry.value.clone();
                Some((model_completion_candidate(entry, filter_text), edit))
            })
            .collect();
    model_shortcut_completion_response(candidates, context)
}

pub(super) fn model_completion_candidate(
    entry: ModelCompletionEntryWire,
    filter_text: String,
) -> CompletionCandidate {
    let display = if entry.display.is_empty() {
        entry.value.clone()
    } else {
        entry.display.clone()
    };
    let detail = model_completion_detail(&entry);
    let documentation = model_completion_documentation(&entry);
    CompletionCandidate {
        display,
        insertion: entry.value.clone(),
        detail,
        documentation,
        is_dir: entry.kind == "provider",
        name: filter_text,
        replacement: None,
        additional_edits: Vec::new(),
        kind: entry.kind,
        project: String::new(),
        status: entry.alias_kind,
    }
}

pub(super) fn is_model_alias_kind(kind: &str) -> bool {
    matches!(kind, "implicit_alias" | "user_alias")
}

pub(super) fn model_completion_detail(
    entry: &ModelCompletionEntryWire,
) -> Option<String> {
    if !is_model_alias_kind(&entry.kind) {
        return (!entry.provider.is_empty()).then(|| entry.provider.clone());
    }

    let mut target = match (
        entry.target_provider.is_empty(),
        entry.target_model.is_empty(),
    ) {
        (false, false) => format!(
            "{}({})",
            entry.target_provider.to_uppercase(),
            entry.target_model
        ),
        (true, false) => entry.target_model.clone(),
        (false, true) => entry.target_provider.to_uppercase(),
        (true, true) => String::new(),
    };
    if !target.is_empty() && !entry.target_effort.is_empty() {
        target.push_str(" @ ");
        target.push_str(&entry.target_effort);
    }
    if !target.is_empty() {
        return Some(target);
    }

    // Additive v1 compatibility: an older catalog has none of the structured
    // target fields, so retain its legacy provider/description detail.
    let legacy_parts: Vec<&str> = [
        (!entry.provider.is_empty()).then_some(entry.provider.as_str()),
        (!entry.description.is_empty()).then_some(entry.description.as_str()),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!legacy_parts.is_empty()).then(|| legacy_parts.join("  "))
}

pub(super) fn model_completion_documentation(
    entry: &ModelCompletionEntryWire,
) -> Option<String> {
    let mut sections = Vec::new();
    if !entry.description.is_empty() {
        sections.push(entry.description.clone());
    }
    if !entry.provenance.is_empty() {
        let mut provenance = entry.provenance.clone();
        if !entry.reference.is_empty() {
            provenance.push_str(" → @");
            provenance.push_str(entry.reference.trim_start_matches('@'));
            if !entry.reference_effort.is_empty() {
                provenance.push_str(" @ ");
                provenance.push_str(&entry.reference_effort);
            }
        }
        sections.push(format!("**Provenance:** {provenance}"));
    }
    if !entry.config_source.is_empty() {
        sections.push(format!(
            "**Config:** `llm_provider.model_aliases.{}.{}`",
            entry.config_source,
            entry.value.trim_start_matches('@')
        ));
    }
    if !entry.bucket.is_empty() {
        sections.push(format!("**Bucket:** `{}`", entry.bucket));
    }
    if entry.selector_mode == "round_robin" {
        sections.push(format!(
            "**Pool:** {}/{} available",
            entry.pool_available, entry.pool_total
        ));
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}
