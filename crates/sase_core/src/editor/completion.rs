use std::collections::BTreeSet;

use regex::Regex;
use std::sync::OnceLock;

use crate::EditorXpromptCatalogEntryWire;

use super::directive::canonical_directive_name;
use super::token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_xprompt_like_token, DocumentSnapshot,
};
use super::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorRange, EditorTextEdit, TokenInfo,
    XpromptAssistEntry, XpromptInputHint,
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
                        required: input.required,
                        default_display: input.default_display.clone(),
                        position: input.position,
                    })
                    .collect(),
                content_preview: entry.content_preview.clone(),
                description: entry.description.clone(),
                source_path_display: entry.source_path_display.clone(),
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

    let token = extract_token_at_position(document, position);
    match token {
        None => {
            let byte = document.position_to_byte_offset(position)?;
            Some(CompletionContext {
                kind: CompletionContextKind::FileHistory,
                token: None,
                active_xprompt: None,
                active_input: None,
                directive_name: None,
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
            documentation: input
                .default_display
                .as_ref()
                .map(|default| format!("default: {default}")),
            is_dir: false,
            name: input.name.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
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
            replacement_range: range,
        });
    }

    if let Some((name, arg_start)) = directive_arg_token(before) {
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
            directive_name: Some(name.to_string()),
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

fn directive_arg_token(before: &str) -> Option<(&str, usize)> {
    let percent = before.rfind('%')?;
    let directive = &before[percent + 1..];
    let split = directive.find([':', '('])?;
    let name = &directive[..split];
    canonical_directive_name(name)?;
    let sep_idx = percent + 1 + split;
    if directive.as_bytes().get(split) == Some(&b'(') && directive.contains(')')
    {
        return None;
    }
    Some((name, sep_idx + 1))
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
                        required: true,
                        default_display: None,
                        position: 0,
                    },
                    MobileXpromptInputWire {
                        name: "deep".to_string(),
                        r#type: "bool".to_string(),
                        required: false,
                        default_display: Some("false".to_string()),
                        position: 1,
                    },
                ],
                is_skill: false,
                content_preview: Some("review body".to_string()),
                source_path_display: Some("xprompts/review.md".to_string()),
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
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
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
                required: true,
                default_display: None,
                position: 0,
            }],
            content_preview: None,
            description: None,
            source_path_display: None,
            is_skill: false,
        };
        assert!(
            classify_completion_context(&doc, pos(13), &[ns_entry]).is_some()
        );
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
    }
}
