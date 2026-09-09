use serde::{Deserialize, Serialize};

use crate::model_completion::{
    filter_model_completion_entries, ModelCompletionEntryWire,
};
use crate::prompt_literal_zone_ranges;

use super::directive::detect_directive_context_at_position;
use super::placeholder::detect_placeholder_context_at_position;
use super::token::DocumentSnapshot;
use super::wire::{EditorPosition, EditorRange, EditorTextEdit};

pub const MODEL_ALIAS_SHORTCUT_WIRE_SCHEMA_VERSION: u32 = 1;
pub const MODEL_SHORTCUT_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelShortcutKind {
    Alias,
    Model,
}

/// Detected `*alias` or `**model` shortcut context.
///
/// `caret`, `token_range`, and `replacement_range` use [`EditorPosition`],
/// whose `character` field is an LSP-compatible UTF-16 code-unit column.
/// Internal trigger scans use UTF-8 byte offsets only after converting through
/// [`DocumentSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelShortcutContextWire {
    pub schema_version: u32,
    pub kind: ModelShortcutKind,
    pub query: String,
    pub token: String,
    pub caret: EditorPosition,
    pub token_range: EditorRange,
    pub replacement_range: EditorRange,
}

/// Detected `*alias` model shortcut context.
///
/// `caret`, `token_range`, and `replacement_range` use [`EditorPosition`],
/// whose `character` field is an LSP-compatible UTF-16 code-unit column.
/// Internal trigger scans use UTF-8 byte offsets only after converting through
/// [`DocumentSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAliasShortcutContextWire {
    pub schema_version: u32,
    pub query: String,
    pub token: String,
    pub caret: EditorPosition,
    pub token_range: EditorRange,
    pub replacement_range: EditorRange,
}

/// One validated edit that expands a star shortcut to an inline `%m:` directive.
///
/// `value` is the selected canonical alias (`@alias`) or concrete model value.
/// `replacement` matches `edit.new_text`, including any spacer. The
/// `edit.range` uses the original document's UTF-16 editor positions and may
/// consume one following ASCII space, mirroring [`ModelAliasShortcutEditWire`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelShortcutEditWire {
    pub schema_version: u32,
    pub kind: ModelShortcutKind,
    pub value: String,
    pub replacement: String,
    pub edit: EditorTextEdit,
    pub caret: EditorPosition,
}

/// One validated edit that expands a `*alias` shortcut to `%m:@alias`.
///
/// The `edit.range` uses the original document's UTF-16 editor positions.
/// It usually equals the detected context's `replacement_range`, but when
/// the star token is immediately followed by one ASCII space, the range
/// deliberately extends one character beyond it to consume that space (a
/// space is then reinserted at the end of `edit.new_text`). This keeps the
/// edit self-contained: applying `edit` alone reproduces the same final
/// document a naive whitespace-preserving expansion would, and `caret`
/// (the post-edit document's UTF-16 editor position) is always exactly the
/// position at the end of the applied `edit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAliasShortcutEditWire {
    pub schema_version: u32,
    pub alias: String,
    pub replacement: String,
    pub edit: EditorTextEdit,
    pub caret: EditorPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DetectedShortcut {
    wire: ModelShortcutContextWire,
    start: usize,
    end: usize,
}

pub fn model_shortcut_context(
    text: &str,
    position: EditorPosition,
) -> Option<ModelShortcutContextWire> {
    let document = DocumentSnapshot::new(text);
    detect_model_shortcut_in_document(&document, position)
        .map(|detected| detected.wire)
}

pub fn detect_model_alias_shortcut_context(
    text: &str,
    position: EditorPosition,
) -> Option<ModelAliasShortcutContextWire> {
    model_shortcut_context(text, position).and_then(alias_context_from_shortcut)
}

/// Filter a model catalog down to the effective alias rows a `*query`
/// shortcut may expand to, in canonical catalog order.
///
/// Reuses [`filter_model_completion_entries`] with a synthesized `@query`
/// partial so alias prefix matching (including matching on a row's
/// `aliases`) stays identical to `%model:@` completion, then restricts the
/// result to `implicit_alias`/`user_alias` rows. [`plan_model_alias_shortcut_edit`]
/// uses this same helper to validate a selected alias against the current
/// catalog.
pub fn filter_model_alias_shortcut_entries(
    entries: &[ModelCompletionEntryWire],
    query: &str,
) -> Vec<ModelCompletionEntryWire> {
    let partial = format!("@{query}");
    filter_model_completion_entries(entries, &partial)
        .into_iter()
        .filter(|entry| is_model_alias_kind(&entry.kind))
        .collect()
}

/// Filter a model catalog down to concrete model rows a `**query` shortcut may
/// expand to, in canonical catalog order.
///
/// The complete catalog is passed through [`filter_model_completion_entries`]
/// before provider rows are removed, because provider-scoped queries such as
/// `**codex/gpt` need provider rows to decide the scope.
pub fn filter_explicit_model_shortcut_entries(
    entries: &[ModelCompletionEntryWire],
    query: &str,
) -> Vec<ModelCompletionEntryWire> {
    filter_model_completion_entries(entries, query)
        .into_iter()
        .filter(|entry| entry.kind == "model")
        .collect()
}

pub fn model_shortcut_edit(
    text: &str,
    position: EditorPosition,
    entries: &[ModelCompletionEntryWire],
    selected_value: &str,
) -> Option<ModelShortcutEditWire> {
    let document = DocumentSnapshot::new(text);
    let detected = detect_model_shortcut_in_document(&document, position)?;
    let value = selected_canonical_shortcut_value(
        entries,
        detected.wire.kind,
        &detected.wire.query,
        selected_value,
    )?;
    let replacement = model_directive_replacement(&value);
    let (new_text, edit_text, edit_end, caret_byte) =
        apply_replacement_preview(
            text,
            detected.start,
            detected.end,
            &replacement,
        )?;
    let caret =
        DocumentSnapshot::new(new_text).byte_offset_to_position(caret_byte)?;
    let edit_range = document.byte_range_to_range(detected.start, edit_end)?;

    Some(ModelShortcutEditWire {
        schema_version: MODEL_SHORTCUT_WIRE_SCHEMA_VERSION,
        kind: detected.wire.kind,
        value,
        replacement: edit_text.clone(),
        edit: EditorTextEdit {
            range: edit_range,
            new_text: edit_text,
        },
        caret,
    })
}

pub fn plan_model_alias_shortcut_edit(
    text: &str,
    position: EditorPosition,
    entries: &[ModelCompletionEntryWire],
    selected_alias: &str,
) -> Option<ModelAliasShortcutEditWire> {
    let edit = model_shortcut_edit(text, position, entries, selected_alias)?;
    if edit.kind != ModelShortcutKind::Alias {
        return None;
    }
    Some(ModelAliasShortcutEditWire {
        schema_version: MODEL_ALIAS_SHORTCUT_WIRE_SCHEMA_VERSION,
        alias: edit.value,
        replacement: edit.replacement,
        edit: edit.edit,
        caret: edit.caret,
    })
}

fn detect_model_shortcut_in_document(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<DetectedShortcut> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    if cursor > text.len() || !text.is_char_boundary(cursor) {
        return None;
    }
    let (start, end) = whitespace_token_bounds(text, cursor)?;
    if !text.get(start..)?.starts_with('*')
        || !shortcut_left_boundary(text, start)
        || excluded_position(document, position, start)
    {
        return None;
    }

    let token = text.get(start..end)?;
    let leading_stars = leading_ascii_star_count(token);
    let kind = match leading_stars {
        1 => ModelShortcutKind::Alias,
        2 => ModelShortcutKind::Model,
        _ => return None,
    };
    let trigger_end = start + leading_stars;
    if cursor < trigger_end {
        return None;
    }
    let suffix = text.get(trigger_end..end)?;
    if suffix.contains('*') {
        return None;
    }
    let query = text.get(trigger_end..cursor)?;
    if kind == ModelShortcutKind::Alias && query.starts_with('@') {
        return None;
    }

    let range = document.byte_range_to_range(start, end)?;
    Some(DetectedShortcut {
        wire: ModelShortcutContextWire {
            schema_version: MODEL_SHORTCUT_WIRE_SCHEMA_VERSION,
            kind,
            query: query.to_string(),
            token: token.to_string(),
            caret: position,
            token_range: range,
            replacement_range: range,
        },
        start,
        end,
    })
}

fn alias_context_from_shortcut(
    context: ModelShortcutContextWire,
) -> Option<ModelAliasShortcutContextWire> {
    if context.kind != ModelShortcutKind::Alias {
        return None;
    }
    Some(ModelAliasShortcutContextWire {
        schema_version: MODEL_ALIAS_SHORTCUT_WIRE_SCHEMA_VERSION,
        query: context.query,
        token: context.token,
        caret: context.caret,
        token_range: context.token_range,
        replacement_range: context.replacement_range,
    })
}

fn leading_ascii_star_count(token: &str) -> usize {
    token.bytes().take_while(|byte| *byte == b'*').count()
}

fn selected_canonical_shortcut_value(
    entries: &[ModelCompletionEntryWire],
    kind: ModelShortcutKind,
    query: &str,
    selected_value: &str,
) -> Option<String> {
    match kind {
        ModelShortcutKind::Alias => {
            selected_canonical_alias(entries, query, selected_value)
        }
        ModelShortcutKind::Model => {
            selected_canonical_model(entries, query, selected_value)
        }
    }
}

fn selected_canonical_alias(
    entries: &[ModelCompletionEntryWire],
    query: &str,
    selected_alias: &str,
) -> Option<String> {
    filter_model_alias_shortcut_entries(entries, query)
        .into_iter()
        .find(|entry| entry.value == selected_alias)
        .and_then(|entry| canonical_alias_value(&entry.value))
}

fn canonical_alias_value(value: &str) -> Option<String> {
    let alias = value.strip_prefix('@')?;
    (!alias.is_empty() && !alias.chars().any(char::is_whitespace))
        .then(|| format!("@{alias}"))
}

fn selected_canonical_model(
    entries: &[ModelCompletionEntryWire],
    query: &str,
    selected_value: &str,
) -> Option<String> {
    filter_explicit_model_shortcut_entries(entries, query)
        .into_iter()
        .find(|entry| entry.value == selected_value)
        .and_then(|entry| canonical_model_value(&entry.value))
}

fn canonical_model_value(value: &str) -> Option<String> {
    (!value.is_empty()
        && !value.starts_with('@')
        && !value.chars().any(char::is_whitespace))
    .then(|| value.to_string())
}

fn model_directive_replacement(value: &str) -> String {
    format!("%m:{value}")
}

/// Build the expansion preview, the edit's `new_text`, the edit range's end
/// byte offset, and the post-edit caret byte offset.
///
/// `edit_end` normally equals `end` (the edit touches only the star token),
/// except when the token is immediately followed by one ASCII space: that
/// space is consumed into the edit range and one space is reinserted at the
/// end of `new_text`, so the edit stays self-contained and the caret is
/// always exactly the end of the applied edit.
fn apply_replacement_preview(
    text: &str,
    start: usize,
    end: usize,
    replacement: &str,
) -> Option<(String, String, usize, usize)> {
    let mut edit_text = replacement.to_string();
    let mut edit_end = end;
    let caret_after_edit =
        match text.get(end..).and_then(|tail| tail.chars().next()) {
            Some(' ') => {
                edit_text.push(' ');
                edit_end = end + 1;
                start + edit_text.len()
            }
            Some('\t') => start + replacement.len(),
            Some('\n') | Some('\r') | None => {
                edit_text.push(' ');
                start + edit_text.len()
            }
            Some(_) => start + replacement.len(),
        };

    let mut preview = String::with_capacity(
        text.len() - (edit_end - start) + edit_text.len(),
    );
    preview.push_str(text.get(..start)?);
    preview.push_str(&edit_text);
    preview.push_str(text.get(edit_end..)?);
    Some((preview, edit_text, edit_end, caret_after_edit))
}

fn whitespace_token_bounds(
    text: &str,
    cursor: usize,
) -> Option<(usize, usize)> {
    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text.get(prev..)?.chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    let mut end = cursor;
    while end < text.len() {
        let ch = text.get(end..)?.chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    Some((start, end))
}

fn shortcut_left_boundary(text: &str, start: usize) -> bool {
    start == 0
        || text.as_bytes().get(start - 1).copied() == Some(b' ')
        || text.as_bytes().get(start - 1).copied() == Some(b'\n')
}

fn excluded_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    trigger_start: usize,
) -> bool {
    if position_in_ranges(
        trigger_start,
        &excluded_literal_ranges(document.text()),
    ) {
        return true;
    }
    detect_placeholder_context_at_position(document, position).is_some()
        || detect_directive_context_at_position(document, position).is_some()
}

fn excluded_literal_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = prompt_literal_zone_ranges(text);
    ranges.extend(jinja_tag_ranges(text));
    if let Some(end) = frontmatter_block_len(text) {
        ranges.push((0, end));
    }
    ranges
}

fn jinja_tag_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < text.len() {
        let Some((relative_start, close)) = next_jinja_tag(text, offset) else {
            break;
        };
        let start = offset + relative_start;
        let close_start = start + 2;
        let end = text
            .get(close_start..)
            .and_then(|tail| {
                tail.find(close).map(|index| close_start + index + 2)
            })
            .unwrap_or(text.len());
        ranges.push((start, end));
        offset = end.max(start + 2);
    }
    ranges
}

fn next_jinja_tag(text: &str, offset: usize) -> Option<(usize, &'static str)> {
    let tail = text.get(offset..)?;
    [("{{", "}}"), ("{%", "%}"), ("{#", "#}")]
        .into_iter()
        .filter_map(|(open, close)| tail.find(open).map(|start| (start, close)))
        .min_by_key(|(start, _)| *start)
}

fn frontmatter_block_len(text: &str) -> Option<usize> {
    let mut lines = text.split_inclusive('\n');
    let first = lines.next()?;
    if first.trim() != "---" {
        return None;
    }
    let mut consumed = first.len();
    for line in lines {
        consumed += line.len();
        if line.trim() == "---" {
            return Some(consumed);
        }
    }
    None
}

fn position_in_ranges(pos: usize, ranges: &[(usize, usize)]) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= pos && pos < *end)
}

fn previous_char_boundary(text: &str, mut byte_idx: usize) -> Option<usize> {
    if byte_idx == 0 || byte_idx > text.len() {
        return None;
    }
    byte_idx -= 1;
    while !text.is_char_boundary(byte_idx) {
        byte_idx = byte_idx.checked_sub(1)?;
    }
    Some(byte_idx)
}

fn is_model_alias_kind(kind: &str) -> bool {
    matches!(kind, "implicit_alias" | "user_alias")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(line: u32, character: u32) -> EditorPosition {
        EditorPosition { line, character }
    }

    fn entry(value: &str, kind: &str) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            description: String::new(),
            kind: kind.to_string(),
            provider: String::new(),
            aliases: Vec::new(),
            alias_kind: String::new(),
            target_provider: String::new(),
            target_model: String::new(),
            target_effort: String::new(),
            provenance: String::new(),
            reference: String::new(),
            reference_effort: String::new(),
            selector_mode: String::new(),
            pool_available: 0,
            pool_total: 0,
            config_source: String::new(),
            bucket: String::new(),
            advisory_label: String::new(),
            advisory_severity: String::new(),
            provider_model_count: 0,
        }
    }

    fn model_entry(
        value: &str,
        provider: &str,
        aliases: &[&str],
    ) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            description: String::new(),
            kind: "model".to_string(),
            provider: provider.to_string(),
            aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
            ..ModelCompletionEntryWire::default()
        }
    }

    fn provider_entry(value: &str, provider: &str) -> ModelCompletionEntryWire {
        ModelCompletionEntryWire {
            value: value.to_string(),
            display: value.to_string(),
            description: String::new(),
            kind: "provider".to_string(),
            provider: provider.to_string(),
            ..ModelCompletionEntryWire::default()
        }
    }

    fn context(
        text: &str,
        line: u32,
        character: u32,
    ) -> ModelAliasShortcutContextWire {
        detect_model_alias_shortcut_context(text, pos(line, character)).unwrap()
    }

    fn shortcut_context(
        text: &str,
        line: u32,
        character: u32,
    ) -> ModelShortcutContextWire {
        model_shortcut_context(text, pos(line, character)).unwrap()
    }

    fn plan(
        text: &str,
        line: u32,
        character: u32,
        selected: &str,
    ) -> ModelAliasShortcutEditWire {
        let entries = vec![
            entry("@large", "user_alias"),
            entry("@small", "implicit_alias"),
            entry("claude-sonnet", "model"),
        ];
        plan_model_alias_shortcut_edit(
            text,
            pos(line, character),
            &entries,
            selected,
        )
        .unwrap()
    }

    fn apply(text: &str, edit: &EditorTextEdit) -> String {
        let document = DocumentSnapshot::new(text);
        let start = document.position_to_byte_offset(edit.range.start).unwrap();
        let end = document.position_to_byte_offset(edit.range.end).unwrap();
        let mut out = String::new();
        out.push_str(&text[..start]);
        out.push_str(&edit.new_text);
        out.push_str(&text[end..]);
        out
    }

    #[test]
    fn detects_start_space_and_logical_line_boundaries() {
        assert_eq!(context("*", 0, 1).query, "");
        assert_eq!(context("Explain *la", 0, 11).query, "la");
        assert_eq!(context("one\n*sm", 1, 3).query, "sm");
        assert_eq!(context("one\n  *sm", 1, 5).query, "sm");
    }

    #[test]
    fn detects_model_shortcut_context_without_alias_wrapper_fallback() {
        let model = shortcut_context("Review **gpt", 0, 12);
        assert_eq!(model.kind, ModelShortcutKind::Model);
        assert_eq!(model.query, "gpt");
        assert_eq!(model.token, "**gpt");
        assert_eq!(model.caret, pos(0, 12));
        assert_eq!(model.replacement_range.start, pos(0, 7));
        assert_eq!(model.replacement_range.end, pos(0, 12));
        assert_eq!(
            detect_model_alias_shortcut_context("Review **gpt", pos(0, 12)),
            None
        );

        let alias = shortcut_context("Review *la", 0, 10);
        assert_eq!(alias.kind, ModelShortcutKind::Alias);
        assert_eq!(alias.query, "la");

        let crlf = shortcut_context("one\r\n  **gpt", 1, 7);
        assert_eq!(crlf.kind, ModelShortcutKind::Model);
        assert_eq!(crlf.query, "gpt");
    }

    #[test]
    fn rejects_embedded_escaped_tabs_and_completed_emphasis() {
        for (text, position) in [
            ("a*b", pos(0, 3)),
            ("path/*", pos(0, 6)),
            (r"\*", pos(0, 2)),
            ("one\t*la", pos(0, 7)),
            ("**bold", pos(0, 2)),
            ("*emphasis*", pos(0, 5)),
            ("*@large", pos(0, 7)),
        ] {
            assert_eq!(
                detect_model_alias_shortcut_context(text, position),
                None
            );
        }
    }

    #[test]
    fn rejects_literal_model_star_tokens() {
        for (text, position) in [
            ("**", pos(0, 1)),
            ("***", pos(0, 3)),
            ("**bold**", pos(0, 4)),
            ("**gpt**", pos(0, 5)),
            ("a**b", pos(0, 4)),
            ("path/**", pos(0, 7)),
            (r"\**", pos(0, 3)),
            ("one\t**gpt", pos(0, 9)),
        ] {
            assert_eq!(model_shortcut_context(text, position), None);
        }
    }

    #[test]
    fn plans_full_token_replacement_from_mid_token_caret() {
        let planned = plan("Explain *laX later", 0, 11, "@large");
        assert_eq!(planned.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Explain *laX later", &planned.edit),
            "Explain %m:@large later"
        );
        assert_eq!(planned.caret, pos(0, 18));
    }

    #[test]
    fn preserves_or_adds_whitespace_after_expansion() {
        let at_end = plan("Use *la", 0, 7, "@large");
        assert_eq!(at_end.edit.new_text, "%m:@large ");
        assert_eq!(apply("Use *la", &at_end.edit), "Use %m:@large ");
        assert_eq!(at_end.caret, pos(0, 14));

        let before_one_space = plan("Use *la now", 0, 7, "@large");
        assert_eq!(before_one_space.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use *la now", &before_one_space.edit),
            "Use %m:@large now"
        );
        assert_eq!(before_one_space.caret, pos(0, 14));

        let before_space = plan("Use *la   now", 0, 7, "@large");
        assert_eq!(before_space.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use *la   now", &before_space.edit),
            "Use %m:@large   now"
        );
        assert_eq!(before_space.caret, pos(0, 14));

        let before_tab = plan("Use *la\tnow", 0, 7, "@large");
        assert_eq!(before_tab.edit.new_text, "%m:@large");
        assert_eq!(
            apply("Use *la\tnow", &before_tab.edit),
            "Use %m:@large\tnow"
        );
        assert_eq!(before_tab.caret, pos(0, 13));

        let before_newline = plan("Use *la\nnow", 0, 7, "@large");
        assert_eq!(before_newline.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use *la\nnow", &before_newline.edit),
            "Use %m:@large \nnow"
        );
        assert_eq!(before_newline.caret, pos(0, 14));
    }

    #[test]
    fn rejects_literal_regions_and_frontmatter() {
        for (text, position) in [
            ("`*la`", pos(0, 3)),
            ("```text\n*la\n```", pos(1, 3)),
            (
                "%xprompts_enabled:false\n*la\n%xprompts_enabled:true\n",
                pos(1, 3),
            ),
            ("---\nname: *la\n---\nbody", pos(1, 9)),
            ("%model:*la", pos(0, 10)),
            ("{{*la}}", pos(0, 5)),
            ("{{ *la }}", pos(0, 6)),
            ("{% if *la %}", pos(0, 9)),
            ("{# *la #}", pos(0, 6)),
            ("prefix {{\n  *la\n}} suffix", pos(1, 5)),
            ("{{ *la", pos(0, 6)),
        ] {
            assert_eq!(
                detect_model_alias_shortcut_context(text, position),
                None
            );
        }
    }

    #[test]
    fn validates_selected_alias_against_current_filtered_aliases() {
        let entries = vec![
            entry("@large", "user_alias"),
            entry("@launch", "implicit_alias"),
            entry("@scout", "user_alias"),
            entry("large-model", "model"),
        ];
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use *la",
                pos(0, 7),
                &entries,
                "@scout"
            ),
            None
        );
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use *la",
                pos(0, 7),
                &entries,
                "large-model"
            ),
            None
        );
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use *la",
                pos(0, 7),
                &entries,
                "@launch"
            )
            .unwrap()
            .alias,
            "@launch"
        );
    }

    #[test]
    fn filter_explicit_model_shortcut_entries_keeps_models_only() {
        let entries = vec![
            model_entry("claude-fable-5", "claude", &["fable"]),
            entry("@large", "user_alias"),
            provider_entry("claude/", "claude"),
            model_entry("gpt-5.6-sol", "codex", &["gpt56sol"]),
            provider_entry("codex/", "codex"),
            model_entry("anthropic/claude-sonnet-4-5", "opencode", &[]),
            provider_entry("opencode/", "opencode"),
        ];

        assert_eq!(
            values(filter_explicit_model_shortcut_entries(&entries, "")),
            vec![
                "claude-fable-5",
                "gpt-5.6-sol",
                "anthropic/claude-sonnet-4-5"
            ]
        );
        assert_eq!(
            values(filter_explicit_model_shortcut_entries(&entries, "fa")),
            vec!["claude-fable-5"]
        );
        assert_eq!(
            values(filter_explicit_model_shortcut_entries(
                &entries,
                "claude/fa"
            )),
            vec!["claude/claude-fable-5"]
        );
        assert_eq!(
            values(filter_explicit_model_shortcut_entries(
                &entries,
                "opencode/anthropic/"
            )),
            vec!["opencode/anthropic/claude-sonnet-4-5"]
        );
        assert_eq!(
            values(filter_explicit_model_shortcut_entries(&entries, "@")),
            Vec::<String>::new()
        );
    }

    #[test]
    fn plans_explicit_model_replacement_and_validates_selection() {
        let entries = vec![
            model_entry("claude-fable-5", "claude", &["fable"]),
            model_entry("gpt-5.6-sol", "codex", &["gpt56sol"]),
            entry("@large", "user_alias"),
            provider_entry("claude/", "claude"),
        ];

        let planned = model_shortcut_edit(
            "Use **faX later",
            pos(0, 8),
            &entries,
            "claude-fable-5",
        )
        .unwrap();
        assert_eq!(planned.kind, ModelShortcutKind::Model);
        assert_eq!(planned.value, "claude-fable-5");
        assert_eq!(planned.edit.new_text, "%m:claude-fable-5 ");
        assert_eq!(
            apply("Use **faX later", &planned.edit),
            "Use %m:claude-fable-5 later"
        );
        assert_eq!(planned.caret, pos(0, 22));

        let scoped = model_shortcut_edit(
            "Use **claude/fa",
            pos(0, 15),
            &entries,
            "claude/claude-fable-5",
        )
        .unwrap();
        assert_eq!(scoped.value, "claude/claude-fable-5");
        assert_eq!(scoped.edit.new_text, "%m:claude/claude-fable-5 ");

        assert_eq!(
            model_shortcut_edit(
                "Use **claude/fa",
                pos(0, 15),
                &entries,
                "claude-fable-5",
            ),
            None
        );
        assert_eq!(
            model_shortcut_edit("Use **fa", pos(0, 8), &entries, "@large"),
            None
        );
        assert_eq!(
            model_shortcut_edit(
                "Use **claude",
                pos(0, 12),
                &entries,
                "claude/"
            ),
            None
        );
        let unsafe_entries = vec![model_entry("gpt bad", "codex", &[])];
        assert_eq!(
            model_shortcut_edit(
                "Use **gpt",
                pos(0, 9),
                &unsafe_entries,
                "gpt bad"
            ),
            None
        );
    }

    #[test]
    fn utf16_positions_survive_unicode_and_crlf() {
        let text = "🙂 *la\r\nnext";
        let detected = context(text, 0, 6);
        assert_eq!(detected.replacement_range.start, pos(0, 3));
        assert_eq!(detected.replacement_range.end, pos(0, 6));

        let planned = plan(text, 0, 6, "@large");
        assert_eq!(apply(text, &planned.edit), "🙂 %m:@large \r\nnext");
        assert_eq!(planned.caret, pos(0, 13));
    }

    #[test]
    fn filter_model_alias_shortcut_entries_restricts_to_alias_kinds_in_catalog_order(
    ) {
        let entries = vec![
            entry("@large", "user_alias"),
            entry("@launch", "implicit_alias"),
            entry("large-model", "model"),
            entry("@scout", "user_alias"),
        ];
        assert_eq!(
            filter_model_alias_shortcut_entries(&entries, ""),
            vec![
                entry("@large", "user_alias"),
                entry("@launch", "implicit_alias"),
                entry("@scout", "user_alias"),
            ]
        );
        assert_eq!(
            filter_model_alias_shortcut_entries(&entries, "la"),
            vec![
                entry("@large", "user_alias"),
                entry("@launch", "implicit_alias"),
            ]
        );
        assert_eq!(
            filter_model_alias_shortcut_entries(&entries, "LA"),
            vec![
                entry("@large", "user_alias"),
                entry("@launch", "implicit_alias"),
            ]
        );
        assert_eq!(
            filter_model_alias_shortcut_entries(&entries, "nope"),
            Vec::new()
        );
    }

    /// The [`ModelAliasShortcutEditWire`] doc invariant: `caret` is always
    /// exactly the UTF-16 position at the end of the applied `edit`, for
    /// every trailing-whitespace and Unicode case the planner handles.
    #[test]
    fn planned_caret_always_equals_end_of_applied_edit() {
        for (text, character) in [
            ("Use *la", 7),
            ("Use *la now", 7),
            ("Use *la   now", 7),
            ("Use *la\tnow", 7),
            ("Use *la\nnow", 7),
            ("Explain *laX later", 11),
            ("🙂 *la\r\nnext", 6),
        ] {
            let planned = plan(text, 0, character, "@large");
            assert_eq!(
                planned.caret,
                end_of_edit(&planned.edit),
                "text={text:?}"
            );
        }
    }

    /// `new_text` never contains a newline, so the end of an applied edit is
    /// always on the same line as its start, offset by the UTF-16 length of
    /// `new_text`.
    fn end_of_edit(edit: &EditorTextEdit) -> EditorPosition {
        let utf16_len = edit.new_text.encode_utf16().count() as u32;
        pos(
            edit.range.start.line,
            edit.range.start.character + utf16_len,
        )
    }

    fn values(entries: Vec<ModelCompletionEntryWire>) -> Vec<String> {
        entries.into_iter().map(|entry| entry.value).collect()
    }
}
