//! Trigger and context detection: the `classify_*` dispatcher plus the
//! xprompt-argument trigger analysis and skeleton helpers behind it.

use super::artifact_ref::detect_artifact_ref_context_at_position;
use super::vcs_candidates::{
    detect_vcs_project_context_at_position, detect_vcs_ref_context_at_position,
    detect_vcs_repo_context_at_position,
};
use crate::editor::directive::detect_directive_context_at_position;
use crate::editor::macro_args::{
    find_top_level_equal, top_level_commas_for_args,
};
use crate::editor::placeholder::detect_placeholder_context_at_position;
use crate::editor::token::{
    extract_token_at_position, is_macro_like_token, is_path_like_token,
    is_slash_skill_like_token, is_snippet_trigger_token, DocumentSnapshot,
};
use crate::editor::wire::{
    CompletionContext, CompletionContextKind, EditorPosition, MacroAssistEntry,
    MacroInputHint, TokenInfo,
};
use crate::ArtifactRefContextWire;
use regex::Regex;
use std::sync::OnceLock;

pub fn classify_completion_context(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
) -> Option<CompletionContext> {
    classify_completion_context_with_workflows(document, position, entries, &[])
}
pub fn classify_completion_context_with_workflows(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    classify_completion_context_with_artifacts_and_workflows(
        document,
        position,
        entries,
        known_workflow_names,
        None,
    )
}
pub fn classify_completion_context_with_artifacts_and_workflows(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
    known_workflow_names: &[String],
    artifact_context: Option<&ArtifactRefContextWire>,
) -> Option<CompletionContext> {
    if let Some(placeholder) =
        detect_placeholder_context_at_position(document, position)
    {
        return Some(CompletionContext {
            kind: CompletionContextKind::Placeholder,
            token: Some(TokenInfo {
                text: placeholder.prefix,
                range: placeholder.prefix_range,
                byte_start: placeholder.prefix_byte_start,
                byte_end: placeholder.cursor_byte,
            }),
            active_macro: None,
            active_input: None,
            directive_name: None,
            selected_values: Vec::new(),
            directive: None,
            vcs_repo: None,
            vcs_ref: None,
            artifact_ref: None,
            replacement_range: placeholder.replacement_range,
        });
    }
    if let Some(context) = artifact_context.and_then(|context| {
        detect_artifact_ref_context_at_position(document, position, context)
    }) {
        return Some(context);
    }
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
                active_macro: None,
                active_input: None,
                directive_name: None,
                selected_values: Vec::new(),
                directive: None,
                vcs_repo: None,
                vcs_ref: None,
                artifact_ref: None,
                replacement_range: document.byte_range_to_range(byte, byte)?,
            })
        }
        Some(token) if is_macro_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::Macro, token))
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
pub fn named_args_skeleton(entry: &MacroAssistEntry) -> String {
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
pub fn colon_args_skeleton(entry: &MacroAssistEntry) -> String {
    format!("{}:$0", entry.insertion)
}
fn detect_xprompt_arg_completion_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let text = document.text();
    let prefix = text.get(..cursor)?;
    let captures = macro_ref_re().captures_iter(prefix);
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
            let target =
                colon_arg_context(entry, text, base_end, cursor, suffix)?;
            return arg_context(document, cursor, entry, target);
        }
        if suffix.starts_with('(') {
            let target =
                paren_arg_context(entry, text, base_end, cursor, suffix)?;
            return arg_context(document, cursor, entry, target);
        }
    }
    None
}
struct MacroArgCompletionTarget {
    kind: CompletionContextKind,
    active_input: MacroInputHint,
    token_start: usize,
    token_end: usize,
    selected_values: Vec<String>,
}
fn colon_arg_context(
    entry: &MacroAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<MacroArgCompletionTarget> {
    let value = suffix.strip_prefix(':')?;
    if value.chars().any(char::is_whitespace)
        || value.contains('+')
        || value.contains('(')
        || value.contains(')')
    {
        return None;
    }
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    // Body extends to whitespace or end; parser spans handle quoted commas.
    // An empty tail value (cursor at clause start) truncates the body so the
    // active clause is still addressable.
    let provisional_end = text[cursor..]
        .find(char::is_whitespace)
        .map(|offset| cursor + offset)
        .unwrap_or(text.len());
    let provisional_commas =
        top_level_commas_for_args(text, body_start, provisional_end);
    let provisional_clause_start = provisional_commas
        .iter()
        .filter(|&&c| c < cursor)
        .map(|&c| c + 1 - body_start)
        .max()
        .unwrap_or(0);
    let body_end = if cursor_in_body == provisional_clause_start {
        cursor
    } else {
        provisional_end
    };
    let body = text.get(body_start..body_end)?;
    // Shared parser spans: top-level commas respect quotes and text blocks.
    let commas = top_level_commas_for_args(text, body_start, body_end);
    let active_clause = commas.iter().filter(|&&c| c < cursor).count();
    let clause_start = commas
        .iter()
        .filter(|&&c| c < cursor)
        .map(|&c| c + 1 - body_start)
        .max()
        .unwrap_or(0);
    let clause_end = commas
        .iter()
        .map(|&c| c - body_start)
        .find(|&end| end >= cursor_in_body)
        .unwrap_or(body.len());
    // Active input is the clause containing the cursor; fall back to the
    // final repeatable input when the position runs past declared inputs.
    let active_input = entry
        .inputs
        .get(active_clause)
        .or_else(|| entry.inputs.last().filter(|input| input.repeatable))?
        .clone();
    let token_start = body_start + clause_start;
    let token_end = body_start + clause_end;
    // Cursor must be inside the active clause's span (or at its empty edge).
    if cursor < token_start || cursor > token_end {
        return None;
    }
    Some(MacroArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end,
        selected_values: selected_positional_values(
            text,
            body_start,
            body_end,
            clause_start,
        ),
    })
}

fn paren_arg_context(
    entry: &MacroAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<MacroArgCompletionTarget> {
    let prefix_body = suffix.strip_prefix('(')?;
    if prefix_body.contains(')') {
        return None;
    }
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    // Unclosed lists match the argument parser: the body runs to EOF
    // so a mid-value cursor still replaces the whole current value
    // (`#deploy(env=staging` at `st|aging` covers `staging`).
    let body_end = find_matching_paren(text, base_end).unwrap_or(text.len());
    if cursor > body_end {
        return None;
    }
    let body = text.get(body_start..body_end)?;
    // Shared parser spans: top-level commas respect quotes and text blocks.
    let commas = top_level_commas_for_args(text, body_start, body_end);
    let commas_before: Vec<usize> =
        commas.iter().copied().filter(|&c| c < cursor).collect();
    let clause_start = commas_before
        .last()
        .map(|&c| c + 1 - body_start)
        .unwrap_or(0);
    let clause_end = commas
        .iter()
        .map(|&c| c - body_start)
        .find(|&end| end >= cursor_in_body)
        .unwrap_or(body.len());
    let clause = body.get(clause_start..clause_end)?;
    let stripped = clause.trim_start();
    let leading_ws = clause.len() - stripped.len();
    let value_start = body_start + clause_start + leading_ws;
    let value_end = trim_end(text, value_start, body_start + clause_end);
    let selected =
        selected_positional_values(text, body_start, body_end, clause_start);

    // Top-level `=` distinguishes named values from positional/name slots,
    // without mistaking quoted `=` for syntax.
    let clause_abs_start = body_start + clause_start;
    let clause_abs_end = body_start + clause_end;
    let equal = find_top_level_equal(text, clause_abs_start, clause_abs_end);
    if equal.is_none() {
        let token = text.get(value_start..cursor)?;
        if token.chars().any(char::is_whitespace) {
            return None;
        }
        // Count positional args before this clause using parser spans:
        // clauses without a top-level `=` that are non-empty.
        let mut positional_index = 0usize;
        let mut prev = 0usize;
        for end in commas
            .iter()
            .map(|&c| c - body_start)
            .chain(std::iter::once(body.len()))
        {
            if prev == clause_start {
                break;
            }
            let prev_clause = body.get(prev..end).unwrap_or("");
            let abs_s = body_start + prev;
            let abs_e = body_start + end;
            if find_top_level_equal(text, abs_s, abs_e).is_none()
                && !prev_clause.trim().is_empty()
            {
                positional_index += 1;
            }
            prev = end + 1;
        }
        if let Some(active_input) = entry
            .inputs
            .get(positional_index)
            .or_else(|| entry.inputs.last().filter(|input| input.repeatable))
            .filter(|input| input.repeatable)
            .cloned()
        {
            // For repeatable inputs, selected includes named values for the
            // same input (`labels=red`) plus other positional values, so
            // `labels=red, ` excludes `red` while keeping the empty tail.
            let selected = selected_for_repeatable(
                entry,
                text,
                body_start,
                body_end,
                clause_start,
                &active_input,
                positional_index,
            );
            return Some(MacroArgCompletionTarget {
                kind: completion_kind_for_input(&active_input),
                active_input,
                token_start: value_start,
                token_end: value_end,
                selected_values: selected,
            });
        }
        let placeholder = MacroInputHint {
            name: String::new(),
            r#type: String::new(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
            repeatable: false,
            choices: Vec::new(),
            named_type: None,
            value_role: None,
        };
        return Some(MacroArgCompletionTarget {
            kind: CompletionContextKind::MacroArgumentName,
            active_input: placeholder,
            token_start: value_start,
            token_end: value_end,
            selected_values: selected,
        });
    }

    let equal = equal?;
    let name_part = text.get(clause_abs_start..equal).unwrap_or("");
    let value_part = text.get(equal + 1..clause_abs_end).unwrap_or("");
    let name = name_part.trim();
    let active_input = entry
        .inputs
        .iter()
        .find(|input| input.name == name)?
        .clone();
    let value_leading_ws = value_part.len() - value_part.trim_start().len();
    let token_start = equal + 1 + value_leading_ws;
    // Named value spans run to the clause end (trimmed); quoted wrappers
    // are included so replacement covers the whole current value.
    if cursor < token_start || cursor > clause_abs_end {
        return None;
    }
    Some(MacroArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end: value_end.max(token_start),
        selected_values: Vec::new(),
    })
}
fn arg_context(
    document: &DocumentSnapshot,
    cursor: usize,
    entry: &MacroAssistEntry,
    target: MacroArgCompletionTarget,
) -> Option<CompletionContext> {
    let token_range =
        document.byte_range_to_range(target.token_start, cursor)?;
    let replacement_range =
        document.byte_range_to_range(target.token_start, target.token_end)?;
    Some(CompletionContext {
        kind: target.kind,
        token: Some(TokenInfo {
            text: document.text().get(target.token_start..cursor)?.to_string(),
            range: token_range,
            byte_start: target.token_start,
            byte_end: cursor,
        }),
        active_macro: Some(entry.name.clone()),
        active_input: (!target.active_input.name.is_empty())
            .then_some(target.active_input.name),
        directive_name: None,
        selected_values: target.selected_values,
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
        replacement_range,
    })
}
fn find_matching_paren(text: &str, open: usize) -> Option<usize> {
    crate::editor::macro_args::find_matching_paren_for_args(text, open)
}
fn trim_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start && text.as_bytes()[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    end
}
fn selected_for_repeatable(
    entry: &MacroAssistEntry,
    text: &str,
    body_start: usize,
    body_end: usize,
    active_clause_start: usize,
    active_input: &MacroInputHint,
    _active_positional_index: usize,
) -> Vec<String> {
    let body = text.get(body_start..body_end).unwrap_or("");
    let commas = top_level_commas_for_args(text, body_start, body_end);
    let mut values = Vec::new();
    let mut prev = 0usize;
    let mut positional_index = 0usize;
    for end in commas
        .iter()
        .map(|&c| c - body_start)
        .chain(std::iter::once(body.len()))
    {
        if prev != active_clause_start {
            let abs_s = body_start + prev;
            let abs_e = body_start + end;
            if let Some(eq) = find_top_level_equal(text, abs_s, abs_e) {
                let name = text.get(abs_s..eq).unwrap_or("").trim();
                if name == active_input.name {
                    let raw = text.get(eq + 1..abs_e).unwrap_or("").trim();
                    if !raw.is_empty() {
                        values.push(decode_selected_value(raw));
                    }
                }
            } else {
                let raw = body.get(prev..end).unwrap_or("").trim();
                if !raw.is_empty() {
                    // Positional clauses map by order; include those that
                    // belong to the same repeatable tail input.
                    let belongs = entry
                        .inputs
                        .get(positional_index)
                        .map(|inp| inp.name == active_input.name)
                        .unwrap_or_else(|| {
                            entry
                                .inputs
                                .last()
                                .filter(|inp| inp.repeatable)
                                .map(|inp| inp.name == active_input.name)
                                .unwrap_or(false)
                        });
                    if belongs {
                        values.push(decode_selected_value(raw));
                    }
                    positional_index += 1;
                    // Skip increment below since we already advanced.
                    prev = end + 1;
                    continue;
                }
                positional_index += 1;
            }
        } else if find_top_level_equal(
            text,
            body_start + prev,
            body_start + end,
        )
        .is_none()
        {
            positional_index += 1;
        }
        prev = end + 1;
    }
    // Keep declaration order but deduplicate? Builder uses set semantics;
    // preserve first-seen order for stable tests.
    let mut seen = std::collections::BTreeSet::new();
    values
        .into_iter()
        .filter(|v| seen.insert(v.clone()))
        .collect()
}

fn selected_positional_values(
    text: &str,
    body_start: usize,
    body_end: usize,
    active_clause_start: usize,
) -> Vec<String> {
    let body = text.get(body_start..body_end).unwrap_or("");
    let commas = top_level_commas_for_args(text, body_start, body_end);
    let mut values = Vec::new();
    let mut prev = 0usize;
    for end in commas
        .iter()
        .map(|&c| c - body_start)
        .chain(std::iter::once(body.len()))
    {
        if prev != active_clause_start {
            let abs_s = body_start + prev;
            let abs_e = body_start + end;
            if find_top_level_equal(text, abs_s, abs_e).is_none() {
                let raw = body.get(prev..end).unwrap_or("").trim();
                if !raw.is_empty() {
                    values.push(decode_selected_value(raw));
                }
            }
        }
        prev = end + 1;
    }
    values
}

fn decode_selected_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        if (bytes[0] == b'"' || bytes[0] == b'\'')
            && bytes[0] == bytes[trimmed.len() - 1]
        {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}
fn context_for_token(
    kind: CompletionContextKind,
    token: TokenInfo,
) -> CompletionContext {
    CompletionContext {
        kind,
        replacement_range: token.range,
        token: Some(token),
        active_macro: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
    }
}
fn completion_kind_for_input(input: &MacroInputHint) -> CompletionContextKind {
    let is_agent = input
        .value_role
        .as_deref()
        .is_some_and(|role| role == "agent")
        || (input.value_role.is_none() && input.r#type == "agent");
    if is_agent {
        return CompletionContextKind::MacroArgumentAgent;
    }
    let is_model = input
        .value_role
        .as_deref()
        .is_some_and(|role| role == "model");
    if is_model {
        return CompletionContextKind::MacroArgumentModel;
    }
    if !input.choices.is_empty() || input.r#type == "bool" {
        return CompletionContextKind::MacroArgumentValue;
    }
    if input.r#type == "path" {
        return CompletionContextKind::MacroArgumentPath;
    }
    CompletionContextKind::MacroArgumentTypeHint
}
fn macro_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"']|[^\x00-\x7F])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}
