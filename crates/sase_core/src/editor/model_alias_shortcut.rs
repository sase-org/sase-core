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

/// One validated edit that expands a `*alias` shortcut to `%m:@alias`.
///
/// The `edit.range` uses the original document's UTF-16 editor positions.
/// `caret` uses the post-edit document's UTF-16 editor position.
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
    wire: ModelAliasShortcutContextWire,
    start: usize,
    end: usize,
}

pub fn detect_model_alias_shortcut_context(
    text: &str,
    position: EditorPosition,
) -> Option<ModelAliasShortcutContextWire> {
    let document = DocumentSnapshot::new(text);
    detect_model_alias_shortcut_in_document(&document, position)
        .map(|detected| detected.wire)
}

pub fn plan_model_alias_shortcut_edit(
    text: &str,
    position: EditorPosition,
    entries: &[ModelCompletionEntryWire],
    selected_alias: &str,
) -> Option<ModelAliasShortcutEditWire> {
    let document = DocumentSnapshot::new(text);
    let detected =
        detect_model_alias_shortcut_in_document(&document, position)?;
    let alias = selected_canonical_alias(
        entries,
        &detected.wire.query,
        selected_alias,
    )?;
    let replacement = model_alias_replacement(&alias);
    let (new_text, edit_text, caret_byte) = apply_replacement_preview(
        text,
        detected.start,
        detected.end,
        &replacement,
    )?;
    let caret =
        DocumentSnapshot::new(new_text).byte_offset_to_position(caret_byte)?;

    Some(ModelAliasShortcutEditWire {
        schema_version: MODEL_ALIAS_SHORTCUT_WIRE_SCHEMA_VERSION,
        alias,
        replacement: edit_text.clone(),
        edit: EditorTextEdit {
            range: detected.wire.replacement_range,
            new_text: edit_text,
        },
        caret,
    })
}

fn detect_model_alias_shortcut_in_document(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<DetectedShortcut> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    if cursor > text.len() || !text.is_char_boundary(cursor) {
        return None;
    }
    let (start, end) = whitespace_token_bounds(text, cursor)?;
    if cursor < start + 1
        || !text.get(start..)?.starts_with('*')
        || !shortcut_left_boundary(text, start)
        || excluded_position(document, position, start)
    {
        return None;
    }

    let token = text.get(start..end)?;
    let query = text.get(start + 1..cursor)?;
    if query.starts_with('@') || token.get(1..)?.contains('*') {
        return None;
    }

    let range = document.byte_range_to_range(start, end)?;
    Some(DetectedShortcut {
        wire: ModelAliasShortcutContextWire {
            schema_version: MODEL_ALIAS_SHORTCUT_WIRE_SCHEMA_VERSION,
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

fn selected_canonical_alias(
    entries: &[ModelCompletionEntryWire],
    query: &str,
    selected_alias: &str,
) -> Option<String> {
    let partial = format!("@{query}");
    filter_model_completion_entries(entries, &partial)
        .into_iter()
        .find(|entry| {
            entry.value == selected_alias && is_model_alias_kind(&entry.kind)
        })
        .and_then(|entry| canonical_alias_value(&entry.value))
}

fn canonical_alias_value(value: &str) -> Option<String> {
    let alias = value.strip_prefix('@')?;
    (!alias.is_empty() && !alias.chars().any(char::is_whitespace))
        .then(|| format!("@{alias}"))
}

fn model_alias_replacement(alias: &str) -> String {
    format!("%m:{alias}")
}

fn apply_replacement_preview(
    text: &str,
    start: usize,
    end: usize,
    replacement: &str,
) -> Option<(String, String, usize)> {
    let mut edit_text = replacement.to_string();
    let caret_after_edit =
        match text.get(end..).and_then(|tail| tail.chars().next()) {
            Some(' ') => start + replacement.len() + 1,
            Some('\t') => start + replacement.len(),
            Some('\n') | Some('\r') | None => {
                edit_text.push(' ');
                start + edit_text.len()
            }
            Some(_) => start + replacement.len(),
        };

    let mut preview =
        String::with_capacity(text.len() - (end - start) + edit_text.len());
    preview.push_str(text.get(..start)?);
    preview.push_str(&edit_text);
    preview.push_str(text.get(end..)?);
    Some((preview, edit_text, caret_after_edit))
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
    if let Some(end) = frontmatter_block_len(text) {
        ranges.push((0, end));
    }
    ranges
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

    fn context(
        text: &str,
        line: u32,
        character: u32,
    ) -> ModelAliasShortcutContextWire {
        detect_model_alias_shortcut_context(text, pos(line, character)).unwrap()
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
    fn plans_full_token_replacement_from_mid_token_caret() {
        let planned = plan("Explain *laX later", 0, 11, "@large");
        assert_eq!(planned.edit.new_text, "%m:@large");
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

        let before_space = plan("Use *la   now", 0, 7, "@large");
        assert_eq!(before_space.edit.new_text, "%m:@large");
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
    fn utf16_positions_survive_unicode_and_crlf() {
        let text = "🙂 *la\r\nnext";
        let detected = context(text, 0, 6);
        assert_eq!(detected.replacement_range.start, pos(0, 3));
        assert_eq!(detected.replacement_range.end, pos(0, 6));

        let planned = plan(text, 0, 6, "@large");
        assert_eq!(apply(text, &planned.edit), "🙂 %m:@large \r\nnext");
        assert_eq!(planned.caret, pos(0, 13));
    }
}
