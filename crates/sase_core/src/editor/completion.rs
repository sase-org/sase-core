use std::collections::BTreeSet;

use regex::Regex;
use std::sync::OnceLock;

use crate::{EditorSnippetEntryWire, EditorXpromptCatalogEntryWire};

use super::directive::canonical_directive_name;
use super::token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_xprompt_like_token, vcs_project_trigger_token,
    DocumentSnapshot,
};
use super::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorRange, EditorTextEdit, TokenInfo,
    VcsNamespaceEntry, VcsProjectEntry, VcsRefTrigger, VcsRepoEntry,
    VcsRepoTrigger, XpromptAssistEntry, XpromptInputHint,
};

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
                    })
                    .collect(),
                content_preview: entry.content_preview.clone(),
                description: entry.description.clone(),
                source_path_display: entry.source_path_display.clone(),
                definition_path: entry.definition_path.clone(),
                definition_range: entry.definition_range,
                is_skill: entry.is_skill,
            }
        })
        .collect()
}

pub fn classify_completion_context(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    classify_completion_context_with_workflows(document, position, entries, &[])
}

pub fn classify_completion_context_with_workflows(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    if let Some(context) = detect_vcs_repo_context_at_position(
        document,
        position,
        known_workflow_names,
    ) {
        return Some(context);
    }
    if let Some(context) = detect_vcs_ref_context_at_position(
        document,
        position,
        known_workflow_names,
    ) {
        return Some(context);
    }
    if let Some(context) =
        detect_xprompt_arg_completion_at_position(document, position, entries)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_directive_context_at_position(document, position)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_vcs_project_context_at_position(document, position)
    {
        return Some(context);
    }

    let token = extract_token_at_position(document, position);
    match token {
        None => {
            let byte = document.position_to_byte_offset(position)?;
            if byte > 0 && document.text()[..byte].ends_with('+') {
                return None;
            }
            Some(CompletionContext {
                kind: CompletionContextKind::FileHistory,
                token: None,
                active_xprompt: None,
                active_input: None,
                directive_name: None,
                vcs_repo: None,
                vcs_ref: None,
                replacement_range: document.byte_range_to_range(byte, byte)?,
            })
        }
        Some(token) if is_xprompt_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::Xprompt, token))
        }
        Some(token) if is_slash_skill_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::SlashSkill, token))
        }
        Some(token) if is_path_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::FilePath, token))
        }
        Some(token) if is_snippet_trigger_token(&token.text) => Some(
            context_for_token(CompletionContextKind::SnippetTrigger, token),
        ),
        _ => None,
    }
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
        if slash_skill && !entry.is_skill {
            continue;
        }
        if standalone_only && entry.reference_prefix != "#!" {
            continue;
        }
        if !entry.name.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        let insertion = if slash_skill {
            format!("/{}", entry.name)
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
            name: entry.name.clone(),
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

// --- vcs_repo (`#gh:owner/`) completion -----------------------------------

pub fn build_vcs_repo_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsRepoEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_repo.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let candidates = entries
        .iter()
        .map(|entry| {
            let new_text = vcs_repo_replacement_text(
                document.text(),
                trigger,
                &entry.r#ref,
            );
            CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.r#ref.clone(),
                detail: (!entry.visibility.is_empty())
                    .then(|| entry.visibility.clone()),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: "repo".to_string(),
                project: trigger.namespace.clone(),
                status: entry.visibility.clone(),
            }
        })
        .collect();

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn apply_vcs_repo_selection(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let replacement = vcs_repo_replacement_text(text, trigger, selected_ref);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}

fn vcs_repo_replacement_text(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}

pub fn detect_vcs_repo_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    if separator == "(" && text[ref_start..cursor].contains(')') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    let slash_offset = ref_before_cursor.rfind('/')?;
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains("://") {
        return None;
    }

    let namespace = &ref_before_cursor[..slash_offset];
    if namespace.is_empty()
        || namespace.starts_with('~')
        || namespace.starts_with('.')
    {
        return None;
    }

    let query_start = ref_start + slash_offset + 1;
    let trigger = VcsRepoTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        namespace: namespace.to_string(),
        query: ref_before_cursor[slash_offset + 1..].to_string(),
        namespace_span: (ref_start, ref_start + slash_offset),
        query_span: (query_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRepo,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        vcs_repo: Some(trigger),
        vcs_ref: None,
        replacement_range,
    })
}

fn find_vcs_repo_ref_end(
    text: &str,
    cursor: usize,
    separator: &str,
) -> Option<(usize, usize)> {
    let mut end = cursor;
    if separator == "(" {
        while end < text.len() {
            let ch = text[end..].chars().next()?;
            if ch.is_whitespace() || ch == ')' {
                break;
            }
            end += ch.len_utf8();
        }
        let token_end = if end < text.len()
            && text[end..].chars().next().is_some_and(|ch| ch == ')')
        {
            end + 1
        } else {
            end
        };
        return Some((end, token_end));
    }

    while end < text.len() {
        let ch = text[end..].chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    Some((end, end))
}

// --- vcs_ref (`#gh:` / `#gh(` root-ref completion) ------------------------

pub fn build_vcs_ref_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsProjectEntry],
    namespaces: &[VcsNamespaceEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_ref.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let query = trigger.query.to_lowercase();
    let mut candidates = Vec::new();
    for include_changespecs in [false, true] {
        for entry in entries {
            let is_changespec = entry.kind == "changespec";
            if is_changespec != include_changespecs
                || entry.vcs_prefix != trigger.workflow
                || !vcs_ref_project_matches(entry, &query)
            {
                continue;
            }

            let new_text = vcs_ref_replacement_text(
                document.text(),
                trigger,
                &entry.name,
                false,
            );
            candidates.push(CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.name.clone(),
                detail: Some(format!(
                    "{} · {}",
                    entry.provider_display, entry.display_tag
                )),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: entry.kind.clone(),
                project: entry.project.clone(),
                status: entry.status.clone(),
            });
        }
    }

    for namespace in namespaces {
        if !prefix_matches(&namespace.name, &query) {
            continue;
        }
        let insertion = vcs_ref_namespace_insertion(&namespace.name);
        let new_text = vcs_ref_replacement_text(
            document.text(),
            trigger,
            &insertion,
            true,
        );
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion,
            detail: (!namespace.description.is_empty())
                .then(|| namespace.description.clone()),
            documentation: None,
            is_dir: true,
            name: namespace.name.clone(),
            replacement: Some(EditorTextEdit {
                range: replacement_range,
                new_text,
            }),
            additional_edits: Vec::new(),
            kind: "namespace".to_string(),
            project: trigger.workflow.clone(),
            status: if namespace.kind_label.is_empty() {
                "org".to_string()
            } else {
                namespace.kind_label.clone()
            },
        });
    }

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn apply_vcs_ref_selection(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let replacement =
        vcs_ref_replacement_text(text, trigger, selected_ref, chain);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}

fn vcs_ref_replacement_text(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let selected_ref = if chain {
        vcs_ref_namespace_insertion(selected_ref)
    } else {
        selected_ref.to_string()
    };
    if chain {
        return selected_ref;
    }

    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}

fn vcs_ref_namespace_insertion(name: &str) -> String {
    format!("{}/", name.trim_end_matches('/'))
}

fn vcs_ref_project_matches(entry: &VcsProjectEntry, query: &str) -> bool {
    prefix_matches(&entry.name, query)
        || entry
            .aliases
            .iter()
            .any(|alias| prefix_matches(alias, query))
}

fn prefix_matches(value: &str, query_lower: &str) -> bool {
    query_lower.is_empty() || value.to_lowercase().starts_with(query_lower)
}

pub fn detect_vcs_ref_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    if ref_before_cursor.contains(')') || ref_before_cursor.contains('/') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains('/')
        || full_ref.contains("://")
        || full_ref.starts_with(['~', '.'])
        || full_ref.contains(')')
    {
        return None;
    }

    let trigger = VcsRefTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        query: ref_before_cursor.to_string(),
        query_span: (ref_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRef,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        vcs_repo: None,
        vcs_ref: Some(trigger),
        replacement_range,
    })
}

fn previous_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    text.get(..byte_idx)?
        .char_indices()
        .last()
        .map(|(idx, _)| idx)
}

// --- vcs_project (`#+`) completion ----------------------------------------

/// Build `vcs_project` completion candidates for a `#+query` or BOF `+query`
/// trigger token.
///
/// Each candidate expands the selected project into the prompt via the
/// canonical VCS-tag expansion algorithm (see [`apply_vcs_project_selection`]),
/// represented as a primary edit that consumes the trigger span plus
/// `additional_edits` that prepend/replace the VCS workflow tag at the start of
/// the document. When those edits would overlap they are merged into a single
/// primary edit. The output is byte-for-byte identical to the Python
/// `apply_vcs_project_selection` for the shared golden test vectors.
pub fn build_vcs_project_completion_candidates(
    token: &TokenInfo,
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[VcsProjectEntry],
    known_workflow_names: &[String],
) -> CompletionList {
    let text = document.text();
    let t0 = token.byte_start;
    let t1 = token.byte_end;
    let cursor = document
        .position_to_byte_offset(position)
        .unwrap_or(t1)
        .clamp(t0, t1);
    // Filter query is the text after the trigger prefix up to the cursor (empty
    // for a bare `#+`/`+`), matching the Python `find_vcs_project_trigger`. The
    // prefix is `#+` (offset 2) for a hash-plus token, or `+` (offset 1) for a
    // BOF bare-plus token.
    let prefix_len = if token.text.starts_with("#+") { 2 } else { 1 };
    let query = text
        .get(t0 + prefix_len..cursor)
        .unwrap_or("")
        .to_lowercase();

    let replace_re = vcs_replace_regex(known_workflow_names);
    let mut candidates = Vec::new();
    for entry in entries {
        let matches_query = query.is_empty()
            || entry.name.to_lowercase().starts_with(&query)
            || entry
                .aliases
                .iter()
                .any(|alias| alias.to_lowercase().starts_with(&query));
        if !matches_query {
            continue;
        }

        let edits = vcs_project_byte_edits(
            text,
            t0,
            t1,
            &entry.display_tag,
            &replace_re,
        );
        let Some(primary) = byte_edit_to_text_edit(document, &edits.primary)
        else {
            continue;
        };
        let additional: Option<Vec<EditorTextEdit>> = edits
            .additional
            .iter()
            .map(|edit| byte_edit_to_text_edit(document, edit))
            .collect();
        let Some(additional) = additional else {
            continue;
        };

        candidates.push(CompletionCandidate {
            display: entry.name.clone(),
            insertion: entry.display_tag.clone(),
            detail: Some(format!(
                "{} · {}",
                entry.provider_display, entry.display_tag
            )),
            documentation: (!entry.description.is_empty())
                .then(|| entry.description.clone()),
            is_dir: false,
            name: entry.name.clone(),
            replacement: Some(primary),
            additional_edits: additional,
            kind: entry.kind.clone(),
            project: entry.project.clone(),
            status: entry.status.clone(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

/// Apply a selected project's VCS tag to `text`, returning the new full text.
///
/// This is the canonical expansion algorithm (the cross-language parity
/// contract). It mirrors the Python `apply_vcs_project_selection`: remove the
/// `[t0, t1)` trigger token, collapse one adjacent space, then either replace
/// every line-start VCS workflow tag with `display_tag` or -- when none exist --
/// prepend `display_tag` after any leading frontmatter / whitespace /
/// `%directive` tokens.
pub fn apply_vcs_project_selection(
    text: &str,
    t0: usize,
    t1: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> String {
    let (d0, d1) = strip_trigger_region(text, t0, t1);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let tag_with_space = format!("{display_tag} ");

    if replace_re.is_match(&base) {
        return replace_re
            .replace_all(&base, |caps: &regex::Captures| {
                let prefix = caps.get(1).map_or("", |m| m.as_str());
                format!("{prefix}{tag_with_space}")
            })
            .into_owned();
    }

    let offset = vcs_prepend_offset(&base);
    format!("{}{}{}", &base[..offset], tag_with_space, &base[offset..])
}

fn detect_vcs_project_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let token = vcs_project_trigger_token(document, position)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsProject,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        vcs_repo: None,
        vcs_ref: None,
    })
}

/// A single byte-range edit: replace `text[start..end]` with `new_text`.
struct VcsByteEdit {
    start: usize,
    end: usize,
    new_text: String,
}

struct VcsProjectByteEdits {
    primary: VcsByteEdit,
    additional: Vec<VcsByteEdit>,
}

/// Compute the primary + additional edits for one project selection.
///
/// Edits are expressed in original-document byte coordinates and are guaranteed
/// not to overlap (overlapping cases are either merged into the primary edit or,
/// defensively, collapsed into a single full-document replacement).
fn vcs_project_byte_edits(
    text: &str,
    trigger_start: usize,
    trigger_end: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> VcsProjectByteEdits {
    let (d0, d1) = strip_trigger_region(text, trigger_start, trigger_end);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let gap = d1 - d0;
    // Map a `base` byte offset back to original-document coordinates.
    let to_original = |p: usize| if p <= d0 { p } else { p + gap };
    let tag_with_space = format!("{display_tag} ");

    let tag_matches: Vec<(usize, usize, usize)> = replace_re
        .captures_iter(&base)
        .map(|caps| {
            let whole = caps.get(0).expect("group 0 always present");
            let prefix_len = caps.get(1).map_or(0, |m| m.len());
            (whole.start(), whole.end(), prefix_len)
        })
        .collect();

    let (primary, additional) = if tag_matches.is_empty() {
        // Prepend branch: insert the tag at the frontmatter/directive-aware
        // offset.
        let insert_at = to_original(vcs_prepend_offset(&base));
        if insert_at == d0 {
            // The prepend point coincides with the trigger-deletion start;
            // merge into one edit (the deleted region collapses to the tag).
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: tag_with_space,
                },
                Vec::new(),
            )
        } else {
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: String::new(),
                },
                vec![VcsByteEdit {
                    start: insert_at,
                    end: insert_at,
                    new_text: tag_with_space,
                }],
            )
        }
    } else {
        // Replace branch: rewrite every line-start tag, preserving any leading
        // `%directive` prefix captured in group 1.
        let additional = tag_matches
            .into_iter()
            .map(|(match_start, match_end, prefix_len)| {
                let prefix =
                    base[match_start..match_start + prefix_len].to_string();
                VcsByteEdit {
                    start: to_original(match_start),
                    end: to_original(match_end),
                    new_text: format!("{prefix}{tag_with_space}"),
                }
            })
            .collect();
        (
            VcsByteEdit {
                start: d0,
                end: d1,
                new_text: String::new(),
            },
            additional,
        )
    };

    // Defensive guard: if the edits would overlap (no realistic input produces
    // this, but LSP forbids overlapping ranges), fall back to a single
    // full-document replacement with the canonical result.
    if vcs_edits_conflict(&primary, &additional) {
        let canonical = apply_vcs_project_selection(
            text,
            trigger_start,
            trigger_end,
            display_tag,
            replace_re,
        );
        return VcsProjectByteEdits {
            primary: VcsByteEdit {
                start: 0,
                end: text.len(),
                new_text: canonical,
            },
            additional: Vec::new(),
        };
    }

    VcsProjectByteEdits {
        primary,
        additional,
    }
}

fn vcs_edits_conflict(
    primary: &VcsByteEdit,
    additional: &[VcsByteEdit],
) -> bool {
    let mut spans: Vec<(usize, usize)> =
        std::iter::once((primary.start, primary.end))
            .chain(additional.iter().map(|edit| (edit.start, edit.end)))
            .collect();
    spans.sort_by_key(|&(start, end)| (start, end));
    spans.windows(2).any(|pair| pair[1].0 < pair[0].1)
}

fn byte_edit_to_text_edit(
    document: &DocumentSnapshot,
    edit: &VcsByteEdit,
) -> Option<EditorTextEdit> {
    Some(EditorTextEdit {
        range: document.byte_range_to_range(edit.start, edit.end)?,
        new_text: edit.new_text.clone(),
    })
}

/// Remove the `[t0, t1)` trigger span, collapsing one adjacent space, and
/// return the resulting deletion region `[d0, d1)`. Mirrors the Python
/// `_strip_trigger_token`.
fn strip_trigger_region(text: &str, t0: usize, t1: usize) -> (usize, usize) {
    let before = &text[..t0];
    let after = &text[t1..];
    let before_space = before.ends_with(' ');
    let after_space = after.starts_with(' ');

    if before_space && after_space {
        // Token sat between two spaces; drop the following one.
        (t0, t1 + 1)
    } else if before_space
        && (after.is_empty() || after.starts_with(['\r', '\n']))
    {
        // A trailing space would be orphaned at end of line/prompt.
        (t0 - 1, t1)
    } else if after_space
        && (before.is_empty() || before.ends_with(['\r', '\n']))
    {
        // A leading space would be orphaned at start of line/prompt.
        (t0, t1 + 1)
    } else {
        (t0, t1)
    }
}

/// Where a leading VCS workflow tag should be inserted: after any leading YAML
/// frontmatter block, leading horizontal whitespace, and leading `%directive`
/// tokens.
/// Mirrors the Python `find_vcs_workflow_tag_prepend_offset`.
fn vcs_prepend_offset(text: &str) -> usize {
    let frontmatter_len = frontmatter_block_len(text);
    let body = &text[frontmatter_len..];
    let leading_ws = body
        .char_indices()
        .find(|(_, ch)| !ch.is_whitespace() || matches!(ch, '\n' | '\r'))
        .map_or(body.len(), |(idx, _)| idx);
    let after_ws = &body[leading_ws..];
    let directive_len = directive_prefix_regex()
        .find(after_ws)
        .map_or(0, |m| m.end());
    frontmatter_len + leading_ws + directive_len
}

/// Byte length of a leading YAML frontmatter block (`---` ... `---`), or 0 when
/// `text` does not begin with one. Mirrors the Python `_split_frontmatter_block`.
fn frontmatter_block_len(text: &str) -> usize {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let Some(first) = lines.first() else {
        return 0;
    };
    if first.trim() != "---" {
        return 0;
    }
    let mut consumed = first.len();
    for line in &lines[1..] {
        consumed += line.len();
        if line.trim() == "---" {
            return consumed;
        }
    }
    0
}

fn directive_prefix_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(?:%\S+[\s]+)+").unwrap())
}

/// Build the multiline pattern matching VCS workflow tags at the start of any
/// line, mirroring the Python `_get_vcs_replace_pattern`. Group 1 captures any
/// leading `%directive` prefix to preserve it during replacement.
fn vcs_replace_regex(known_workflow_names: &[String]) -> Regex {
    let mut names: Vec<&str> =
        known_workflow_names.iter().map(String::as_str).collect();
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    // The boundary after a tag is whitespace OR end-of-input. `\s` is tried
    // first, so any actual whitespace (including a newline) is consumed and
    // replaced exactly as before; `$` only wins at true EOF, letting a
    // line-start tag with no trailing whitespace (e.g. `#gh:sase` alone) still
    // be replaced rather than treated as absent.
    let pattern = format!(
        r"(?m)^((?:%\S+[\s]+)*)#(?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|)(?:\s|$)"
    );
    Regex::new(&pattern).expect("valid vcs replace pattern")
}

pub fn named_args_skeleton(entry: &XpromptAssistEntry) -> String {
    let required: Vec<_> =
        entry.inputs.iter().filter(|input| input.required).collect();
    if required.is_empty() {
        return entry.insertion.clone();
    }
    let args = required
        .iter()
        .enumerate()
        .map(|(idx, input)| format!("{}=${}", input.name, idx + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args})$0", entry.insertion)
}

pub fn colon_args_skeleton(entry: &XpromptAssistEntry) -> String {
    format!("{}:$0", entry.insertion)
}

fn detect_xprompt_arg_completion_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let text = document.text();
    let prefix = text.get(..cursor)?;
    let captures = xprompt_ref_re().captures_iter(prefix);
    for caps in captures {
        let whole = caps.get(0)?;
        let name = caps.name("name")?.as_str().replace("__", "/");
        let entry = entries.iter().find(|entry| entry.name == name)?;
        if entry.inputs.is_empty() {
            continue;
        }
        let base_end = whole.end();
        let suffix = text.get(base_end..cursor)?;
        if suffix.starts_with(':') {
            let (kind, active_input, token_start) =
                colon_arg_context(entry, base_end, suffix)?;
            return arg_context(
                document,
                kind,
                cursor,
                token_start,
                entry,
                active_input,
                BTreeSet::new(),
            );
        }
        if suffix.starts_with('(') {
            let (kind, active_input, token_start, used) =
                paren_arg_context(entry, base_end, suffix)?;
            return arg_context(
                document,
                kind,
                cursor,
                token_start,
                entry,
                active_input,
                used,
            );
        }
    }
    None
}

fn colon_arg_context(
    entry: &XpromptAssistEntry,
    base_end: usize,
    suffix: &str,
) -> Option<(CompletionContextKind, XpromptInputHint, usize)> {
    let value = suffix.strip_prefix(':')?;
    if value.chars().any(char::is_whitespace)
        || value.contains('+')
        || value.contains('(')
        || value.contains(')')
    {
        return None;
    }
    let index = value.matches(',').count().min(entry.inputs.len() - 1);
    let active_input = entry.inputs.get(index)?.clone();
    let token_start =
        base_end + 1 + value.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    Some((
        completion_kind_for_input(&active_input),
        active_input,
        token_start,
    ))
}

fn paren_arg_context(
    entry: &XpromptAssistEntry,
    base_end: usize,
    suffix: &str,
) -> Option<(
    CompletionContextKind,
    XpromptInputHint,
    usize,
    BTreeSet<String>,
)> {
    let body = suffix.strip_prefix('(')?;
    if body.contains(')') {
        return None;
    }
    let clause_start = body.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let clause = &body[clause_start..];
    let stripped = clause.trim_start();
    let leading_ws = clause.len() - stripped.len();
    let value_start = base_end + 1 + clause_start + leading_ws;
    let used = used_named_arg_names(&body[..clause_start]);

    if !stripped.contains('=') {
        if stripped.chars().any(char::is_whitespace) {
            return None;
        }
        let placeholder = XpromptInputHint {
            name: String::new(),
            r#type: String::new(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
        };
        return Some((
            CompletionContextKind::XpromptArgumentName,
            placeholder,
            value_start,
            used,
        ));
    }

    let (name_part, value_part) = stripped.split_once('=')?;
    let name = name_part.trim();
    let active_input = entry
        .inputs
        .iter()
        .find(|input| input.name == name)?
        .clone();
    let value_leading_ws = value_part.len() - value_part.trim_start().len();
    let token_start = value_start + name_part.len() + 1 + value_leading_ws;
    Some((
        completion_kind_for_input(&active_input),
        active_input,
        token_start,
        used,
    ))
}

fn arg_context(
    document: &DocumentSnapshot,
    kind: CompletionContextKind,
    cursor: usize,
    token_start: usize,
    entry: &XpromptAssistEntry,
    active_input: XpromptInputHint,
    _used: BTreeSet<String>,
) -> Option<CompletionContext> {
    let range = document.byte_range_to_range(token_start, cursor)?;
    Some(CompletionContext {
        kind,
        token: Some(TokenInfo {
            text: document.text().get(token_start..cursor)?.to_string(),
            range,
            byte_start: token_start,
            byte_end: cursor,
        }),
        active_xprompt: Some(entry.name.clone()),
        active_input: (!active_input.name.is_empty())
            .then_some(active_input.name),
        directive_name: None,
        vcs_repo: None,
        vcs_ref: None,
        replacement_range: range,
    })
}

fn detect_directive_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let line = document.line_text(position.line)?;
    let line_start = document.position_to_byte_offset(EditorPosition {
        line: position.line,
        character: 0,
    })?;
    let cursor_in_line = cursor.checked_sub(line_start)?;
    let before = line.get(..cursor_in_line)?;

    if let Some((start, token)) = directive_name_token(before) {
        let byte_start = line_start + start;
        let range = document.byte_range_to_range(byte_start, cursor)?;
        return Some(CompletionContext {
            kind: CompletionContextKind::DirectiveName,
            token: Some(TokenInfo {
                text: token.to_string(),
                range,
                byte_start,
                byte_end: cursor,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: None,
            vcs_repo: None,
            vcs_ref: None,
            replacement_range: range,
        });
    }

    if let Some((name, arg_start)) = directive_arg_token(before) {
        // A non-leading `@<effort>` suffix on a `%model` value completes from
        // the effort vocabulary, mirroring the Python `%model:<model>@<effort>`
        // split. A leading `@` is the model-alias marker and stays in model
        // completion context. The replacement range covers only the token after
        // a suffix `@`.
        let (directive_name, arg_start) = match name {
            "model" => match before[arg_start..].rfind('@') {
                Some(rel_at) if rel_at > 0 => {
                    ("effort", arg_start + rel_at + 1)
                }
                None => (name, arg_start),
                _ => (name, arg_start),
            },
            _ => (name, arg_start),
        };
        let byte_start = line_start + arg_start;
        let range = document.byte_range_to_range(byte_start, cursor)?;
        return Some(CompletionContext {
            kind: CompletionContextKind::DirectiveArgument,
            token: Some(TokenInfo {
                text: before[arg_start..].to_string(),
                range,
                byte_start,
                byte_end: cursor,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: Some(directive_name.to_string()),
            vcs_repo: None,
            vcs_ref: None,
            replacement_range: range,
        });
    }
    None
}

fn directive_name_token(before: &str) -> Option<(usize, &str)> {
    let start = before.rfind('%')?;
    let token = &before[start..];
    if token == "%("
        || token[1..]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some((start, token));
    }
    None
}

fn directive_arg_token(before: &str) -> Option<(&'static str, usize)> {
    let percent = before.rfind('%')?;
    let directive = &before[percent + 1..];
    let split = directive.find([':', '('])?;
    let name = &directive[..split];
    // Return the canonical directive name so aliases (e.g. `%e` -> `effort`,
    // `%m` -> `model`) classify under the same argument context as their full
    // spelling, matching the Python `extract_directive_arg_token_around_cursor`.
    let canonical = canonical_directive_name(name)?;
    let sep_idx = percent + 1 + split;
    if directive.as_bytes().get(split) == Some(&b'(') && directive.contains(')')
    {
        return None;
    }
    Some((canonical, sep_idx + 1))
}

fn context_for_token(
    kind: CompletionContextKind,
    token: TokenInfo,
) -> CompletionContext {
    CompletionContext {
        kind,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        vcs_repo: None,
        vcs_ref: None,
    }
}

fn completion_kind_for_input(
    input: &XpromptInputHint,
) -> CompletionContextKind {
    match input.r#type.as_str() {
        "path" => CompletionContextKind::XpromptArgumentPath,
        "bool" => CompletionContextKind::XpromptArgumentValue,
        _ => CompletionContextKind::XpromptArgumentTypeHint,
    }
}

fn input_label(input: &XpromptInputHint) -> String {
    let suffix = if input.required { "" } else { "?" };
    format!("{}{suffix}: {}", input.name, input.r#type)
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

fn used_named_arg_names(body_prefix: &str) -> BTreeSet<String> {
    body_prefix
        .split(',')
        .filter_map(|clause| {
            clause.split_once('=').map(|(name, _)| name.trim())
        })
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

fn shared_extension(
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

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::editor::directive::directive_argument_candidates;
    use crate::effort::EFFORT_LEVELS_ORDERED;
    use crate::{EditorXpromptCatalogEntryWire, MobileXpromptInputWire};

    fn pos(character: u32) -> EditorPosition {
        EditorPosition { line: 0, character }
    }

    fn entries() -> Vec<XpromptAssistEntry> {
        assist_entries_from_catalog(&[
            EditorXpromptCatalogEntryWire {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: Some("#review".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("xprompt".to_string()),
                description: Some("Review code".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: vec![],
                input_signature: Some("(path: path, deep?: bool)".to_string()),
                inputs: vec![
                    MobileXpromptInputWire {
                        name: "path".to_string(),
                        r#type: "path".to_string(),
                        description: Some("Path to review".to_string()),
                        required: true,
                        default_display: None,
                        position: 0,
                    },
                    MobileXpromptInputWire {
                        name: "deep".to_string(),
                        r#type: "bool".to_string(),
                        description: Some("Run a deeper pass".to_string()),
                        required: false,
                        default_display: Some("false".to_string()),
                        position: 1,
                    },
                ],
                is_skill: false,
                content_preview: Some("review body".to_string()),
                source_path_display: Some("xprompts/review.md".to_string()),
                definition_path: Some("/tmp/xprompts/review.md".to_string()),
                definition_range: None,
            },
            EditorXpromptCatalogEntryWire {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: Some("#!run".to_string()),
                reference_prefix: Some("#!".to_string()),
                kind: Some("workflow".to_string()),
                description: None,
                source_bucket: "project".to_string(),
                project: None,
                tags: vec![],
                input_signature: None,
                inputs: vec![],
                is_skill: true,
                content_preview: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            },
        ])
    }

    #[test]
    fn classifies_primary_completion_modes() {
        let catalog = entries();
        for (text, col, kind) in [
            ("#re", 3, CompletionContextKind::Xprompt),
            ("/ru", 3, CompletionContextKind::SlashSkill),
            ("./sr", 4, CompletionContextKind::FilePath),
            ("", 0, CompletionContextKind::FileHistory),
            ("%mo", 3, CompletionContextKind::DirectiveName),
            ("%model:", 7, CompletionContextKind::DirectiveArgument),
            ("foo", 3, CompletionContextKind::SnippetTrigger),
            ("foo_1", 5, CompletionContextKind::SnippetTrigger),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
        }
    }

    #[test]
    fn effort_and_auto_directive_arguments_classify_with_candidates() {
        let catalog = entries();
        let cases: Vec<(&str, u32, &str, &str, Vec<&str>)> = vec![
            ("%effort:", 8, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
            (
                "%effort:xh",
                10,
                "effort",
                "xh",
                EFFORT_LEVELS_ORDERED.to_vec(),
            ),
            // The `%e` alias classifies under the canonical `effort` context.
            ("%e:", 3, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
            ("%e:xh", 5, "effort", "xh", EFFORT_LEVELS_ORDERED.to_vec()),
            ("%auto:", 6, "auto", "", vec!["plan", "tale", "epic"]),
            ("%auto:t", 7, "auto", "t", vec!["plan", "tale", "epic"]),
        ];

        for (text, col, directive_name, token, expected_values) in cases {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
            assert_eq!(context.directive_name.as_deref(), Some(directive_name));
            assert_eq!(context.token.as_ref().unwrap().text, token);

            let candidates =
                directive_argument_candidates(directive_name).candidates;
            let values: Vec<&str> =
                candidates.iter().map(|c| c.insertion.as_str()).collect();
            assert_eq!(values, expected_values, "{text}");
        }
    }

    #[test]
    fn builds_catalog_completions_with_marker_filters() {
        let catalog = entries();
        let inline = build_xprompt_completion_candidates("#r", None, &catalog);
        assert_eq!(
            inline
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["#review", "#!run"]
        );

        let standalone =
            build_xprompt_completion_candidates("#!r", None, &catalog);
        assert_eq!(standalone.candidates[0].insertion, "#!run");

        let skill = build_xprompt_completion_candidates("/r", None, &catalog);
        assert_eq!(skill.candidates[0].insertion, "/run");
    }

    #[test]
    fn snippet_context_does_not_steal_higher_priority_tokens() {
        let catalog = entries();
        for text in ["#foo", "/foo", "@foo", "%model", "./foo", ""] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(
                &doc,
                pos(text.len() as u32),
                &catalog,
            );
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::SnippetTrigger),
                "{text}"
            );
        }
    }

    #[test]
    fn builds_snippet_completions_by_case_insensitive_prefix() {
        let list = build_snippet_completion_candidates(
            "fo",
            None,
            &[
                snippet_entry("Foo", "body $1$0", "ace.snippets"),
                snippet_entry("bar", "bar", "xprompt"),
            ],
        );

        assert_eq!(list.candidates.len(), 1);
        assert_eq!(list.candidates[0].display, "Foo");
        assert_eq!(list.candidates[0].insertion, "body $1$0");
        assert_eq!(list.candidates[0].detail.as_deref(), Some("ace.snippets"));
    }

    fn snippet_entry(
        trigger: &str,
        template: &str,
        source: &str,
    ) -> EditorSnippetEntryWire {
        EditorSnippetEntryWire {
            trigger: trigger.to_string(),
            template: template.to_string(),
            source: source.to_string(),
            xprompt_name: None,
            description: None,
            source_path_display: None,
        }
    }

    #[test]
    fn detects_narrow_argument_contexts() {
        let catalog = entries();
        for (text, col, kind, active_input) in [
            (
                "#review:",
                8,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(path=",
                13,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(de",
                10,
                CompletionContextKind::XpromptArgumentName,
                None,
            ),
            (
                "#review!!:",
                10,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
            assert_eq!(context.active_input.as_deref(), active_input);
        }

        let doc = DocumentSnapshot::new("#ns__foo(arg=");
        let ns_entry = XpromptAssistEntry {
            name: "ns/foo".to_string(),
            display_label: "ns/foo".to_string(),
            insertion: "#ns/foo".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "project".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: vec![XpromptInputHint {
                name: "arg".to_string(),
                r#type: "word".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
            }],
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
        };
        assert!(
            classify_completion_context(&doc, pos(13), &[ns_entry]).is_some()
        );
    }

    #[test]
    fn model_at_suffix_completes_effort_vocabulary() {
        let catalog = entries();

        // Right after the `@`, the context targets the effort vocabulary.
        let doc = DocumentSnapshot::new("%model:opus@");
        let context =
            classify_completion_context(&doc, pos(12), &catalog).unwrap();
        assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(context.directive_name.as_deref(), Some("effort"));
        assert_eq!(context.token.as_ref().unwrap().text, "");

        // A partially-typed level keeps the effort context; the token after the
        // `@` is what the editor filters the effort vocabulary against.
        let doc = DocumentSnapshot::new("%model:opus@xh");
        let context =
            classify_completion_context(&doc, pos(14), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("effort"));
        assert_eq!(context.token.as_ref().unwrap().text, "xh");

        // Before the `@`, it is still the model argument.
        let doc = DocumentSnapshot::new("%model:opus");
        let context =
            classify_completion_context(&doc, pos(11), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));

        // A leading `@` is the alias marker, not an effort suffix.
        let doc = DocumentSnapshot::new("%model:@oth");
        let context =
            classify_completion_context(&doc, pos(11), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));
        assert_eq!(context.token.as_ref().unwrap().text, "@oth");
    }

    #[test]
    fn builds_argument_name_completions() {
        let catalog = entries();
        let list = build_xprompt_arg_name_candidates(
            &catalog[0],
            &BTreeSet::from(["path".to_string()]),
            "d",
            None,
        );
        assert_eq!(list.candidates[0].insertion, "deep=");
        assert_eq!(
            list.candidates[0].documentation.as_deref(),
            Some("Run a deeper pass\n\ndefault: false")
        );
    }

    // --- vcs_repo (`#gh:owner/`) completion -------------------------------

    const VCS_REPO_CURSOR: &str = "<CURSOR>";

    fn workflow_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn gh_entry() -> XpromptAssistEntry {
        XpromptAssistEntry {
            name: "gh".to_string(),
            display_label: "gh".to_string(),
            insertion: "#gh".to_string(),
            reference_prefix: "#".to_string(),
            kind: Some("workflow".to_string()),
            source_bucket: "builtin".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: Some("(gh_ref: word)".to_string()),
            inputs: vec![XpromptInputHint {
                name: "gh_ref".to_string(),
                r#type: "word".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
            }],
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
        }
    }

    fn repo_entry(name: &str, full_ref: &str) -> VcsRepoEntry {
        VcsRepoEntry {
            name: name.to_string(),
            r#ref: full_ref.to_string(),
            description: format!("{name} repo"),
            visibility: "public".to_string(),
            is_fork: false,
            is_archived: false,
            pushed_at: None,
        }
    }

    fn vcs_repo_context(
        text: &str,
        cursor: usize,
        names: &[&str],
    ) -> CompletionContext {
        let doc = DocumentSnapshot::new(text);
        let position = doc.byte_offset_to_position(cursor).unwrap();
        detect_vcs_repo_context_at_position(
            &doc,
            position,
            &workflow_names(names),
        )
        .unwrap_or_else(|| panic!("expected repo context for {text:?}"))
    }

    fn apply_text_edit(text: &str, edit: &EditorTextEdit) -> String {
        let doc = DocumentSnapshot::new(text);
        let start = doc.position_to_byte_offset(edit.range.start).unwrap();
        let end = doc.position_to_byte_offset(edit.range.end).unwrap();
        format!("{}{}{}", &text[..start], edit.new_text, &text[end..])
    }

    #[test]
    fn vcs_repo_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `VCS_REPO_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
        let cases = [
            (
                "#gh:bbugyi200/<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "#gh:bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "Fix #gh:bbugyi200/sa<CURSOR> now",
                vec!["gh"],
                "bbugyi200/sase",
                "Fix #gh:bbugyi200/sase now",
            ),
            (
                "#gh!!:bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh!!:bbugyi200/sase ",
            ),
            (
                "#gh(bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh(bbugyi200/sase)",
            ),
            (
                "#gh(bbugyi200/sa<CURSOR>) next",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh(bbugyi200/sase) next",
            ),
            (
                "#gh??(bbugyi200/<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh??(bbugyi200/sase)",
            ),
            (
                "#gl:group/sub/re<CURSOR>",
                vec!["gl"],
                "group/sub/repo",
                "#gl:group/sub/repo ",
            ),
            (
                "#gh:bbugyi200/s<CURSOR>asex",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "#gh:bbugyi200/sa<CURSOR>\n",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase \n",
            ),
        ];

        for (marked, names, selected_ref, expected) in cases {
            let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
            let text = marked.replace(VCS_REPO_CURSOR, "");
            let context = vcs_repo_context(&text, cursor, &names);
            let trigger = context.vcs_repo.as_ref().unwrap();
            assert_eq!(
                apply_vcs_repo_selection(&text, trigger, selected_ref),
                expected,
                "{marked}"
            );
        }
    }

    #[test]
    fn detects_vcs_repo_colon_spans() {
        let context = vcs_repo_context("#gh:bbugyi200/sa", 16, &["gh"]);
        let trigger = context.vcs_repo.as_ref().unwrap();

        assert_eq!(context.kind, CompletionContextKind::VcsRepo);
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, ":");
        assert_eq!(trigger.namespace, "bbugyi200");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 16));
        assert_eq!(trigger.namespace_span, (4, 13));
        assert_eq!(trigger.query_span, (14, 16));
        assert_eq!(context.replacement_range.start, pos(4));
        assert_eq!(context.replacement_range.end, pos(16));
    }

    #[test]
    fn detects_vcs_repo_paren_hitl_and_nested_namespaces() {
        let context = vcs_repo_context("#gh??(bbugyi200/sa", 18, &["gh"]);
        let trigger = context.vcs_repo.as_ref().unwrap();
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, "(");
        assert_eq!(trigger.namespace, "bbugyi200");
        assert_eq!(trigger.query, "sa");

        let context = vcs_repo_context("#gl:group/subgroup/sa", 21, &["gl"]);
        let trigger = context.vcs_repo.as_ref().unwrap();
        assert_eq!(trigger.namespace, "group/subgroup");
        assert_eq!(trigger.query, "sa");
    }

    #[test]
    fn classifies_vcs_repo_then_vcs_ref_before_xprompt_argument_hints() {
        let catalog = vec![gh_entry()];
        let names = workflow_names(&["gh"]);

        let doc = DocumentSnapshot::new("#gh:bbugyi200/");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(14),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRepo);

        let doc = DocumentSnapshot::new("#gh:bbugyi200");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(13),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);
        assert_eq!(context.active_xprompt.as_deref(), None);

        let doc = DocumentSnapshot::new("#foo:bbugyi200/");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(15),
            &[],
            &names,
        );
        assert_ne!(
            context.map(|context| context.kind),
            Some(CompletionContextKind::VcsRepo)
        );
    }

    #[test]
    fn vcs_repo_trigger_negatives() {
        for prompt in [
            "#gh:bbugyi200",
            "#gh:/sa",
            "#gh:~/sa",
            "#gh:./sa",
            "#gh:https://github.com/bbugyi200/sase",
            "#gh_bbugyi200/sase",
            "word#gh:bbugyi200/sa",
            "#foo:bbugyi200/sa",
            "#gh(bbugyi200/sa)",
        ] {
            let doc = DocumentSnapshot::new(prompt);
            let context = detect_vcs_repo_context_at_position(
                &doc,
                doc.byte_offset_to_position(prompt.len()).unwrap(),
                &workflow_names(&["gh"]),
            );
            assert!(context.is_none(), "{prompt}");
        }
    }

    #[test]
    fn vcs_repo_builder_replaces_only_the_ref_value() {
        let marked = "Fix #gh:bbugyi200/s<CURSOR>asex";
        let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
        let text = marked.replace(VCS_REPO_CURSOR, "");
        let doc = DocumentSnapshot::new(text.clone());
        let context = vcs_repo_context(&text, cursor, &["gh"]);
        let list = build_vcs_repo_completion_candidates(
            &doc,
            &context,
            &[repo_entry("sase", "bbugyi200/sase")],
        );

        assert_eq!(list.candidates.len(), 1);
        let edit = list.candidates[0].replacement.as_ref().unwrap();
        assert_eq!(edit.new_text, "bbugyi200/sase ");
        assert_eq!(apply_text_edit(&text, edit), "Fix #gh:bbugyi200/sase ");
        assert!(list.candidates[0].additional_edits.is_empty());
    }

    // --- vcs_ref (`#gh:` / `#gh(` root-ref completion) ---------------------

    const VCS_REF_CURSOR: &str = "<CURSOR>";

    fn vcs_ref_context(
        text: &str,
        cursor: usize,
        names: &[&str],
    ) -> CompletionContext {
        let doc = DocumentSnapshot::new(text);
        let position = doc.byte_offset_to_position(cursor).unwrap();
        detect_vcs_ref_context_at_position(
            &doc,
            position,
            &workflow_names(names),
        )
        .unwrap_or_else(|| panic!("expected ref context for {text:?}"))
    }

    fn namespace_entry(name: &str, description: &str) -> VcsNamespaceEntry {
        VcsNamespaceEntry {
            name: name.to_string(),
            description: description.to_string(),
            kind_label: "org".to_string(),
        }
    }

    #[test]
    fn vcs_ref_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `VCS_REF_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
        let cases: &[(&str, &[&str], &str, bool, &str)] = &[
            ("#gh:<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
            ("#gh:sa<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
            (
                "Fix #gh:sa<CURSOR> now",
                &["gh"],
                "sase",
                false,
                "Fix #gh:sase now",
            ),
            ("#gh!!:sa<CURSOR>", &["gh"], "sase", false, "#gh!!:sase "),
            ("#gh:s<CURSOR>asex", &["gh"], "sase", false, "#gh:sase "),
            (
                "#git:sa<CURSOR>suffix",
                &["git"],
                "sase",
                false,
                "#git:sase ",
            ),
            ("#gh(s<CURSOR>", &["gh"], "sase", false, "#gh(sase)"),
            (
                "#gh(s<CURSOR>) next",
                &["gh"],
                "sase",
                false,
                "#gh(sase) next",
            ),
            ("#gh??(s<CURSOR>", &["gh"], "sase", false, "#gh??(sase)"),
            ("#gh:<CURSOR>", &["gh"], "sase-org", true, "#gh:sase-org/"),
            ("#gh:<CURSOR>", &["gh"], "sase-org/", true, "#gh:sase-org/"),
            (
                "Fix #gh:sa<CURSOR> now",
                &["gh"],
                "sase-org",
                true,
                "Fix #gh:sase-org/ now",
            ),
            ("#gh(sa<CURSOR>", &["gh"], "sase-org", true, "#gh(sase-org/"),
            (
                "#gh(sa<CURSOR>) next",
                &["gh"],
                "sase-org",
                true,
                "#gh(sase-org/) next",
            ),
        ];

        for (marked, names, selected_ref, chain, expected) in cases {
            let cursor = marked.find(VCS_REF_CURSOR).unwrap();
            let text = marked.replace(VCS_REF_CURSOR, "");
            let context = vcs_ref_context(&text, cursor, names);
            let trigger = context.vcs_ref.as_ref().unwrap();
            assert_eq!(
                apply_vcs_ref_selection(&text, trigger, selected_ref, *chain),
                *expected,
                "{marked}"
            );
        }
    }

    #[test]
    fn vcs_ref_accept_preserves_visible_space_before_document_final_newline() {
        // Neovim documents include a final newline; the editor path treats that
        // as end-of-input so the accepted visible line still gains a space.
        let marked = "#gh:sa<CURSOR>\n";
        let cursor = marked.find(VCS_REF_CURSOR).unwrap();
        let text = marked.replace(VCS_REF_CURSOR, "");
        let context = vcs_ref_context(&text, cursor, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();

        assert_eq!(
            apply_vcs_ref_selection(&text, trigger, "sase", false),
            "#gh:sase \n",
        );
    }

    #[test]
    fn detects_vcs_ref_colon_spans() {
        let context = vcs_ref_context("#gh:sa", 6, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();

        assert_eq!(context.kind, CompletionContextKind::VcsRef);
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, ":");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 6));
        assert_eq!(trigger.query_span, (4, 6));
        assert_eq!(context.replacement_range.start, pos(4));
        assert_eq!(context.replacement_range.end, pos(6));

        let context = vcs_ref_context("#gh:", 4, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();
        assert_eq!(trigger.query, "");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 4));
    }

    #[test]
    fn detects_vcs_ref_paren_hitl() {
        let context = vcs_ref_context("#gh??(sa", 8, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, "(");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (6, 8));
    }

    #[test]
    fn classifies_vcs_repo_then_vcs_ref_then_xprompt_args() {
        let catalog = vec![gh_entry()];
        let names = workflow_names(&["gh"]);

        let doc = DocumentSnapshot::new("#gh:owner/repo");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(14),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRepo);

        let doc = DocumentSnapshot::new("#gh:");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(4),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);

        let doc = DocumentSnapshot::new("#gh:sa");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(6),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);

        let doc = DocumentSnapshot::new("#gh:~/x");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(7),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(
            context.kind,
            CompletionContextKind::XpromptArgumentTypeHint
        );
    }

    #[test]
    fn vcs_ref_trigger_negatives() {
        for prompt in [
            "#gh:/sa",
            "#gh:~/x",
            "#gh:./x",
            "#gh:https://github.com/bbugyi200/sase",
            "#gh:owner/repo",
            "#gh_bbugyi200",
            "word#gh:sa",
            "#foo:sa",
            "#gh(sa)",
            "#gh:123)",
        ] {
            let doc = DocumentSnapshot::new(prompt);
            let context = detect_vcs_ref_context_at_position(
                &doc,
                doc.byte_offset_to_position(prompt.len()).unwrap(),
                &workflow_names(&["gh"]),
            );
            assert!(context.is_none(), "{prompt}");
        }
    }

    #[test]
    fn vcs_ref_builder_groups_rows_and_replaces_root_ref() {
        let text = "#gh:";
        let doc = DocumentSnapshot::new(text);
        let context = vcs_ref_context(text, text.len(), &["gh"]);
        let list = build_vcs_ref_completion_candidates(
            &doc,
            &context,
            &[
                changespec_entry("ship-completion", "sase", "Ready"),
                project_entry("sase", "gh"),
                project_entry("bob", "git"),
            ],
            &[namespace_entry("sase-org", "2 active projects")],
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "ship-completion", "sase-org"]
        );
        assert_eq!(list.candidates[0].kind, "project");
        assert_eq!(list.candidates[1].kind, "changespec");
        assert_eq!(list.candidates[1].project, "sase");
        assert_eq!(list.candidates[1].status, "Ready");
        assert_eq!(list.candidates[2].display, "sase-org/");
        assert_eq!(list.candidates[2].kind, "namespace");
        assert_eq!(list.candidates[2].status, "org");

        let project_edit = list.candidates[0].replacement.as_ref().unwrap();
        assert_eq!(apply_text_edit(text, project_edit), "#gh:sase ");
        let namespace_edit = list.candidates[2].replacement.as_ref().unwrap();
        assert_eq!(apply_text_edit(text, namespace_edit), "#gh:sase-org/");
    }

    #[test]
    fn vcs_ref_builder_filters_by_query_and_alias() {
        let text = "#gh:sea";
        let doc = DocumentSnapshot::new(text);
        let context = vcs_ref_context(text, text.len(), &["gh"]);
        let mut entry = project_entry("sase", "gh");
        entry.aliases = vec!["seaside".to_string()];
        let list = build_vcs_ref_completion_candidates(
            &doc,
            &context,
            &[entry, project_entry("bob", "gh")],
            &[namespace_entry("sase-org", "")],
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
    }

    // --- vcs_project (`#+`) completion -------------------------------------

    fn vcs_names() -> Vec<String> {
        ["gh", "git", "hg"].iter().map(|s| s.to_string()).collect()
    }

    fn project_entry(name: &str, prefix: &str) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: prefix.to_string(),
            display_tag: format!("#{prefix}:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            kind: "project".to_string(),
            project: name.to_string(),
            status: String::new(),
        }
    }

    fn changespec_entry(
        name: &str,
        project: &str,
        status: &str,
    ) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: "gh".to_string(),
            display_tag: format!("#gh:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            kind: "changespec".to_string(),
            project: project.to_string(),
            status: status.to_string(),
        }
    }

    fn apply_test_edits(text: &str, edits: &VcsProjectByteEdits) -> String {
        let mut all: Vec<&VcsByteEdit> = std::iter::once(&edits.primary)
            .chain(edits.additional.iter())
            .collect();
        all.sort_by_key(|edit| (edit.start, edit.end));
        let mut out = String::new();
        let mut pos = 0;
        for edit in all {
            out.push_str(&text[pos..edit.start]);
            out.push_str(&edit.new_text);
            pos = edit.end;
        }
        out.push_str(&text[pos..]);
        out
    }

    /// Detect the trigger in `marked` (where `‸` is the cursor), then expand
    /// the `#gh:sase` selection both via the canonical transform and via the
    /// applied byte edits. Returns `(canonical, via_edits)`.
    fn expand_via_both(marked: &str) -> (String, String) {
        let cursor_byte = marked.find('‸').expect("cursor marker");
        let text = marked.replacen('‸', "", 1);
        let doc = DocumentSnapshot::new(text.clone());
        let position = doc
            .byte_offset_to_position(cursor_byte)
            .expect("cursor on a char boundary");
        let token =
            vcs_project_trigger_token(&doc, position).expect("a trigger token");
        let re = vcs_replace_regex(&vcs_names());

        let canonical = apply_vcs_project_selection(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        let edits = vcs_project_byte_edits(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        (canonical, apply_test_edits(&text, &edits))
    }

    #[test]
    fn vcs_project_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `_GOLDEN_VECTORS` table. `‸` marks the cursor.
        let cases = [
            ("Describe this repo. #+‸", "#gh:sase Describe this repo."),
            ("#+‸", "#gh:sase "),
            ("#+sa‸", "#gh:sase "),
            ("#+s‸\n", "#gh:sase \n"),
            ("#+s‸\nmore text", "#gh:sase \nmore text"),
            ("#git:foo Fix bug #+‸", "#gh:sase Fix bug"),
            ("#gh!!:foo do X #+‸", "#gh:sase do X"),
            // Existing leading VCS tag at end-of-input (no trailing text): the
            // trigger strip leaves the bare tag at EOF, which must still be
            // replaced -- not doubled. Covers the `#gh:sase #+` regression.
            ("#gh:sase #+‸", "#gh:sase "),
            ("#gh:sase #+foo‸", "#gh:sase "),
            ("#git:foo #+‸", "#gh:sase "),
            ("Fix #+bug‸ here", "#gh:sase Fix here"),
            ("Line one\n#+‸", "#gh:sase Line one\n"),
            (
                "---\nname: x\n---\nBody #+‸",
                "---\nname: x\n---\n#gh:sase Body",
            ),
            ("%model:opus Body #+‸", "%model:opus #gh:sase Body"),
            // Bare `+` at the very beginning of the prompt (offset 0).
            ("+‸", "#gh:sase "),
            ("+sa‸", "#gh:sase "),
            ("+sa‸ Fix", "#gh:sase Fix"),
        ];
        for (marked, expected) in cases {
            let (canonical, via_edits) = expand_via_both(marked);
            assert_eq!(canonical, expected, "canonical: {marked:?}");
            assert_eq!(via_edits, expected, "via edits: {marked:?}");
        }
    }

    #[test]
    fn vcs_prepend_offset_skips_horizontal_whitespace_only() {
        assert_eq!(vcs_prepend_offset("\n"), 0);
        assert_eq!(vcs_prepend_offset("\nmore"), 0);
        assert_eq!(vcs_prepend_offset("  Body"), 2);
        assert_eq!(vcs_prepend_offset("\tBody"), 1);
    }

    #[test]
    fn classifies_vcs_project_trigger() {
        for (text, col) in [
            ("#+", 2),
            ("Fix #+", 6),
            ("#+sa", 4),
            ("2 #+ 2", 4),
            // Bare `+` at byte offset 0.
            ("+", 1),
            ("+sa", 3),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &[]).unwrap();
            assert_eq!(
                context.kind,
                CompletionContextKind::VcsProject,
                "{text}"
            );
        }

        // A bare `+` outside byte offset 0 is not a project trigger.
        for (text, col) in [("Fix +", 5), (" +", 2), ("# +", 3), ("c#+x", 4)] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(&doc, pos(col), &[]);
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::VcsProject),
                "{text}"
            );
        }
    }

    #[test]
    fn bare_trigger_merges_into_single_primary_edit() {
        let doc = DocumentSnapshot::new("#+");
        let context = classify_completion_context(&doc, pos(2), &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            pos(2),
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "sase");
        assert_eq!(candidate.insertion, "#gh:sase");
        // BOF `#+`: the prepend point coincides with the trigger deletion, so
        // the edits merge into one primary edit with no additional edits.
        assert!(candidate.additional_edits.is_empty());
        assert_eq!(
            candidate.replacement.as_ref().unwrap().new_text,
            "#gh:sase "
        );
    }

    #[test]
    fn trailing_trigger_emits_primary_plus_additional_edit() {
        let doc = DocumentSnapshot::new("Describe this repo. #+");
        let cursor = pos(22);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        let candidate = &list.candidates[0];
        // The primary edit consumes the trigger token; the additional edit
        // prepends the tag at the start of the document.
        assert_eq!(candidate.replacement.as_ref().unwrap().new_text, "");
        assert_eq!(candidate.additional_edits.len(), 1);
        assert_eq!(candidate.additional_edits[0].new_text, "#gh:sase ");
        let prepend_range = candidate.additional_edits[0].range;
        assert_eq!(prepend_range.start, prepend_range.end);
    }

    #[test]
    fn vcs_project_candidates_filter_preserves_catalog_order() {
        let doc = DocumentSnapshot::new("#+sa");
        let cursor = pos(4);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn vcs_project_candidates_include_changespec_context() {
        let doc = DocumentSnapshot::new("#+ship");
        let cursor = pos(6);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                changespec_entry("ship-completion", "sase", "Ready"),
            ],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "ship-completion");
        assert_eq!(candidate.insertion, "#gh:ship-completion");
        assert_eq!(candidate.kind, "changespec");
        assert_eq!(candidate.project, "sase");
        assert_eq!(candidate.status, "Ready");
    }

    #[test]
    fn vcs_project_candidates_filter_for_bare_plus_query() {
        // The query for a BOF `+sa` token is `sa` (prefix length 1), so the
        // candidate list filters and sorts exactly as the `#+sa` case does.
        let doc = DocumentSnapshot::new("+sa");
        let cursor = pos(3);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsProject);
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn bare_plus_trigger_merges_into_single_primary_edit() {
        // A BOF `+`: the prepend point coincides with the trigger deletion, so
        // the edits merge into one primary edit with no additional edits.
        let doc = DocumentSnapshot::new("+");
        let context = classify_completion_context(&doc, pos(1), &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            pos(1),
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert!(candidate.additional_edits.is_empty());
        assert_eq!(
            candidate.replacement.as_ref().unwrap().new_text,
            "#gh:sase "
        );
    }

    #[test]
    fn vcs_project_candidates_match_aliases() {
        let doc = DocumentSnapshot::new("#+sea");
        let cursor = pos(5);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let mut entry = project_entry("sase", "gh");
        entry.aliases = vec!["seaside".to_string()];
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[entry, project_entry("bob", "git")],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
    }

    #[test]
    fn vcs_project_edits_never_overlap() {
        // Every golden input must yield non-overlapping edits (LSP requires
        // it); `vcs_edits_conflict` is the guard.
        for marked in [
            "Describe this repo. #+‸",
            "#+‸",
            "#+sa‸",
            "#git:foo Fix bug #+‸",
            // Existing tag at EOF: the replace edit (tag span) and the primary
            // trigger-deletion edit are adjacent and must not overlap.
            "#git:foo #+‸",
            "#gh:sase #+‸",
            "%model:opus Body #+‸",
            "+‸",
            "+sa‸",
            "+sa‸ Fix",
        ] {
            let cursor_byte = marked.find('‸').unwrap();
            let text = marked.replacen('‸', "", 1);
            let doc = DocumentSnapshot::new(text.clone());
            let position = doc.byte_offset_to_position(cursor_byte).unwrap();
            let token = vcs_project_trigger_token(&doc, position).unwrap();
            let re = vcs_replace_regex(&vcs_names());
            let edits = vcs_project_byte_edits(
                &text,
                token.byte_start,
                token.byte_end,
                "#gh:sase",
                &re,
            );
            assert!(
                !vcs_edits_conflict(&edits.primary, &edits.additional),
                "overlapping edits for {marked:?}"
            );
        }
    }
}
