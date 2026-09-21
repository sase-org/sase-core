//! Trigger and context detection: the `classify_*` dispatcher plus the
//! xprompt-argument trigger analysis and skeleton helpers behind it.

use super::artifact_ref::detect_artifact_ref_context_at_position;
use super::vcs_candidates::{
    detect_vcs_project_context_at_position, detect_vcs_ref_context_at_position,
    detect_vcs_repo_context_at_position,
};
use crate::editor::directive::detect_directive_context_at_position;
use crate::editor::placeholder::detect_placeholder_context_at_position;
use crate::editor::token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_xprompt_like_token, DocumentSnapshot,
};
use crate::editor::wire::{
    CompletionContext, CompletionContextKind, EditorPosition, TokenInfo,
    XpromptAssistEntry, XpromptInputHint,
};
use crate::ArtifactRefContextWire;
use regex::Regex;
use std::sync::OnceLock;

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
    entries: &[XpromptAssistEntry],
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
            active_xprompt: None,
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
                active_xprompt: None,
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
struct XpromptArgCompletionTarget {
    kind: CompletionContextKind,
    active_input: XpromptInputHint,
    token_start: usize,
    token_end: usize,
    selected_values: Vec<String>,
}
fn colon_arg_context(
    entry: &XpromptAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<XpromptArgCompletionTarget> {
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
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    let clause_start = value.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let body_end = if cursor_in_body == clause_start {
        cursor
    } else {
        text[cursor..]
            .find(char::is_whitespace)
            .map(|offset| cursor + offset)
            .unwrap_or(text.len())
    };
    let body = text.get(body_start..body_end)?;
    let clause_end = body[cursor_in_body..]
        .find(',')
        .map(|offset| cursor_in_body + offset)
        .unwrap_or(body.len());
    let token_start = body_start + clause_start;
    let token_end = body_start + clause_end;
    Some(XpromptArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end,
        selected_values: selected_positional_values(body, clause_start),
    })
}
fn paren_arg_context(
    entry: &XpromptAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<XpromptArgCompletionTarget> {
    let prefix_body = suffix.strip_prefix('(')?;
    if prefix_body.contains(')') {
        return None;
    }
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    let body_end = find_matching_paren(text, base_end).unwrap_or(cursor);
    let body = text.get(body_start..body_end)?;
    let clause_start = prefix_body.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let clause_end = body[cursor_in_body..]
        .find(',')
        .map(|offset| cursor_in_body + offset)
        .unwrap_or(body.len());
    let clause = &body[clause_start..clause_end];
    let stripped = clause.trim_start();
    let leading_ws = clause.len() - stripped.len();
    let value_start = base_end + 1 + clause_start + leading_ws;
    let value_end = trim_end(text, value_start, body_start + clause_end);
    let selected = selected_positional_values(body, clause_start);

    if !stripped.contains('=') {
        let token = text.get(value_start..cursor)?;
        if token.chars().any(char::is_whitespace) {
            return None;
        }
        let positional_index = body[..clause_start]
            .split(',')
            .filter(|clause| !clause.trim().is_empty() && !clause.contains('='))
            .count();
        if let Some(active_input) = entry
            .inputs
            .get(positional_index)
            .or_else(|| entry.inputs.last().filter(|input| input.repeatable))
            .filter(|input| input.repeatable)
            .cloned()
        {
            return Some(XpromptArgCompletionTarget {
                kind: completion_kind_for_input(&active_input),
                active_input,
                token_start: value_start,
                token_end: value_end,
                selected_values: selected,
            });
        }
        let placeholder = XpromptInputHint {
            name: String::new(),
            r#type: String::new(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
            repeatable: false,
        };
        return Some(XpromptArgCompletionTarget {
            kind: CompletionContextKind::XpromptArgumentName,
            active_input: placeholder,
            token_start: value_start,
            token_end: value_end,
            selected_values: selected,
        });
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
    Some(XpromptArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end: value_end,
        selected_values: Vec::new(),
    })
}
fn arg_context(
    document: &DocumentSnapshot,
    cursor: usize,
    entry: &XpromptAssistEntry,
    target: XpromptArgCompletionTarget,
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
        active_xprompt: Some(entry.name.clone()),
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
    if text.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 1usize;
    for (offset, byte) in text.as_bytes()[open + 1..].iter().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + 1 + offset);
                }
            }
            _ => {}
        }
    }
    None
}
fn trim_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start && text.as_bytes()[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    end
}
fn selected_positional_values(
    body: &str,
    active_clause_start: usize,
) -> Vec<String> {
    let mut values = Vec::new();
    let mut clause_start = 0usize;
    for clause in body.split(',') {
        if clause_start != active_clause_start && !clause.contains('=') {
            let value = clause.trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
        clause_start += clause.len() + 1;
    }
    values
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
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
    }
}
fn completion_kind_for_input(
    input: &XpromptInputHint,
) -> CompletionContextKind {
    match input.r#type.as_str() {
        "path" => CompletionContextKind::XpromptArgumentPath,
        "bool" => CompletionContextKind::XpromptArgumentValue,
        "agent" => CompletionContextKind::XpromptArgumentAgent,
        _ => CompletionContextKind::XpromptArgumentTypeHint,
    }
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
