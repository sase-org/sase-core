use serde::{Deserialize, Serialize};

use crate::agent_launch::directive_occurrences;
use crate::bead::validate_model_value;
use crate::model_completion::{
    filter_model_completion_entries, ModelCompletionEntryWire,
};
use crate::project_tag::{
    orphan_strip_region, ranges_overlap, segment_containing,
    trigger_strip_region,
};

use super::alternation::{
    alternation_body_ranges, position_in_alternation, span_in_alternation,
};
use super::directive::detect_directive_context_at_position;
use super::exclusion::{
    excluded_literal_and_definition_ranges, position_in_ranges,
};
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

const MODEL_SHORTCUT_MARKER: u8 = b'=';

/// Detected `=alias` or `==model` shortcut context.
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

/// Detected `=alias` model shortcut context.
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

/// One validated edit set that expands a model shortcut to an inline `%m:`
/// directive.
///
/// `value` is the selected canonical alias (`@alias`) or concrete model value.
/// `edit` always covers the typed shortcut token: when no eligible standalone
/// `%model`/`%m` directive exists in the shortcut's `---` segment it expands
/// the token in place (then `additional_edits` is empty and `replacement`
/// matches `edit.new_text`, including any spacer); otherwise `edit` deletes
/// the token and `additional_edits[0]` replaces the earliest eligible
/// directive with the selected value while the rest delete the remaining
/// eligible directives. Every `edit.range` uses the original document's UTF-16
/// editor positions, and the set never overlaps. `caret` is the post-edit
/// document's UTF-16 editor position: the end of the applied `edit` in the
/// single-edit case, the end of the destination replacement otherwise.
///
/// `additional_edits` is serde-defaulted and skipped when empty, so responses
/// planned before this field existed deserialize unchanged and old readers
/// that apply only `edit` keep working (they delete the shortcut token and
/// leave any pre-existing directives in place).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelShortcutEditWire {
    pub schema_version: u32,
    pub kind: ModelShortcutKind,
    pub value: String,
    pub replacement: String,
    pub edit: EditorTextEdit,
    pub caret: EditorPosition,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_edits: Vec<EditorTextEdit>,
}

/// One validated edit set that expands a `=alias` shortcut to `%m:@alias`.
///
/// The `edit.range` uses the original document's UTF-16 editor positions.
/// In the single-edit case (no eligible standalone directive elsewhere in the
/// segment) it usually equals the detected context's `replacement_range`, but
/// when the shortcut token is immediately followed by one ASCII space, the
/// range deliberately extends one character beyond it to consume that space (a
/// space is then reinserted at the end of `edit.new_text`). This keeps the
/// edit self-contained: applying `edit` alone reproduces the same final
/// document a naive whitespace-preserving expansion would, and `caret`
/// (the post-edit document's UTF-16 editor position) is always exactly the
/// position at the end of the applied `edit`. With an eligible standalone
/// directive elsewhere in the segment, `edit` deletes the shortcut token and
/// `additional_edits` carries the destination replacement plus the remaining
/// removals, mirroring [`ModelShortcutEditWire`]; `additional_edits` is
/// serde-defaulted and skipped when empty so older responses keep working.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAliasShortcutEditWire {
    pub schema_version: u32,
    pub alias: String,
    pub replacement: String,
    pub edit: EditorTextEdit,
    pub caret: EditorPosition,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_edits: Vec<EditorTextEdit>,
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

/// Filter a model catalog down to the effective alias rows a `=query`
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

/// Filter a model catalog down to concrete model rows a `==query` shortcut may
/// expand to, in canonical catalog order.
///
/// The complete catalog is passed through [`filter_model_completion_entries`]
/// before provider rows are removed, because provider-scoped queries such as
/// `==codex/gpt` need provider rows to decide the scope.
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
    let trigger_end = clamp_trigger_end(text, detected.start, detected.end);
    if let Some(planned) = plan_segment_model_accept(
        text,
        &document,
        &detected,
        trigger_end,
        &replacement,
    ) {
        return Some(ModelShortcutEditWire {
            schema_version: MODEL_SHORTCUT_WIRE_SCHEMA_VERSION,
            kind: detected.wire.kind,
            value,
            replacement: planned.destination_text,
            edit: planned.primary,
            caret: planned.caret,
            additional_edits: planned.additional,
        });
    }
    let (new_text, edit_text, edit_end, caret_byte) =
        apply_replacement_preview(
            text,
            detected.start,
            trigger_end,
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
        additional_edits: Vec::new(),
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
        additional_edits: edit.additional_edits,
    })
}

/// A planned multi-edit acceptance: the trigger deletion plus the
/// destination replacement and the remaining removals, all in
/// original-document coordinates.
struct PlannedSegmentAccept {
    primary: EditorTextEdit,
    additional: Vec<EditorTextEdit>,
    destination_text: String,
    caret: EditorPosition,
}

/// Plan the segment-scoped acceptance for a validated shortcut value.
///
/// When the shortcut's `---` segment holds at least one eligible standalone
/// `%model`/`%m` directive, the earliest becomes the destination: the typed
/// shortcut token is deleted, the destination is replaced with `replacement`
/// at its own position, and every further eligible directive in the segment
/// is removed so the segment keeps exactly one model directive. Returns
/// `None` when the token-local expansion applies instead: the trigger sits
/// inside an alternation body (an independent edit boundary), or no eligible
/// directive exists.
///
/// Eligibility mirrors the launch directive grammar (colon and parenthesized
/// values, `%m` through the canonical alias table) rather than matching
/// arbitrary `%m` text. Directives inside alternation bodies, literal zones
/// (fenced/inline code, disabled regions), frontmatter, Jinja tags, other
/// `---` segments, or overlapping the trigger token are never targets.
/// Clamp the trigger token's edit end so a token glued to an alternation
/// body never deletes body text. Returns `end` unchanged unless a body
/// starts strictly inside `[start, end)`; the boundary itself is ASCII, so
/// the clamped offset stays a character boundary.
fn clamp_trigger_end(text: &str, start: usize, end: usize) -> usize {
    alternation_body_ranges(text)
        .into_iter()
        .filter(|(body_start, _)| *body_start > start && *body_start < end)
        .map(|(body_start, _)| body_start)
        .min()
        .unwrap_or(end)
}

fn plan_segment_model_accept(
    text: &str,
    document: &DocumentSnapshot,
    detected: &DetectedShortcut,
    trigger_end: usize,
    replacement: &str,
) -> Option<PlannedSegmentAccept> {
    if position_in_alternation(text, detected.start) {
        return None;
    }
    let (segment_start, segment_end) = segment_containing(text, detected.start);
    let ignored = excluded_literal_and_definition_ranges(text);
    let mut targets: Vec<(usize, usize)> = directive_occurrences(text)
        .unwrap_or_default()
        .into_iter()
        .filter(|occurrence| occurrence.canonical_name == "model")
        .map(|occurrence| (occurrence.start, occurrence.end))
        .filter(|(start, end)| {
            *start >= segment_start
                && *end <= segment_end
                && !ignored.iter().any(|(low, high)| {
                    ranges_overlap(*start, *end, *low, *high)
                })
                && !span_in_alternation(text, *start, *end)
                && !ranges_overlap(*start, *end, detected.start, detected.end)
        })
        .collect();
    targets.sort_unstable();
    targets.dedup();
    let (dest_start, dest_end) = targets.first().copied()?;

    let (delete_start, delete_end) =
        trigger_strip_region(text, detected.start, trigger_end);
    let (dest_text, dest_end) =
        destination_replacement(text, dest_end, replacement);
    let dest_start_range = (dest_start, dest_end);
    if ranges_overlap(
        dest_start_range.0,
        dest_start_range.1,
        delete_start,
        delete_end,
    ) {
        return None;
    }
    let primary_range =
        document.byte_range_to_range(delete_start, delete_end)?;
    let dest_range = document.byte_range_to_range(dest_start, dest_end)?;
    let mut additional = vec![EditorTextEdit {
        range: dest_range,
        new_text: dest_text.clone(),
    }];
    for (start, end) in targets.into_iter().skip(1) {
        let (mut del_start, mut del_end) =
            orphan_strip_region(text, start, end);
        if span_in_alternation(text, del_start, del_end) {
            continue;
        }
        // Adjacent directives share the single space between them: the
        // destination consumes one following space while the strip eats one
        // neighboring space, so the raw removal can overlap the trigger
        // deletion, the destination, or an earlier removal. Shrink the
        // shared padding instead of dropping the removal, keeping every
        // edit pairwise disjoint. Planned spans only ever cover padding
        // around this directive, never its core, so the core always
        // survives the shrink; anything else fails closed below.
        let mut planned: Vec<(usize, usize)> =
            Vec::with_capacity(additional.len() + 1);
        planned.push((delete_start, delete_end));
        planned.push((dest_start, dest_end));
        let mut ranges_valid = true;
        for edit in additional.iter().skip(1) {
            match byte_range_of(document, &edit.range) {
                Some(span) => planned.push(span),
                None => {
                    ranges_valid = false;
                    break;
                }
            }
        }
        if !ranges_valid {
            continue;
        }
        for (span_start, span_end) in planned {
            if !ranges_overlap(del_start, del_end, span_start, span_end) {
                continue;
            }
            if span_start <= del_start {
                del_start = del_start.max(span_end);
            } else {
                del_end = del_end.min(span_start);
            }
        }
        if del_start >= del_end || del_start > start || del_end < end {
            continue;
        }
        let range = document.byte_range_to_range(del_start, del_end)?;
        additional.push(EditorTextEdit {
            range,
            new_text: String::new(),
        });
    }
    additional[1..].sort_by_key(|edit| {
        byte_range_of(document, &edit.range).unwrap_or((usize::MAX, usize::MAX))
    });

    let mut ordered: Vec<(usize, usize, String, bool)> =
        vec![(delete_start, delete_end, String::new(), false)];
    ordered.push((dest_start, dest_end, dest_text.clone(), true));
    for edit in additional.iter().skip(1) {
        let (start, end) = byte_range_of(document, &edit.range)?;
        ordered.push((start, end, String::new(), false));
    }
    ordered.sort_by_key(|(start, end, _, _)| (*start, *end));
    let mut out = String::with_capacity(text.len() + replacement.len());
    let mut pos = 0;
    let mut caret_byte = delete_start.min(text.len());
    let mut saw_destination = false;
    for (start, end, new_text, is_destination) in &ordered {
        if *start < pos {
            return None;
        }
        out.push_str(text.get(pos..*start).unwrap_or_default());
        if *is_destination && !saw_destination {
            caret_byte = out.len() + new_text.len();
            saw_destination = true;
        }
        out.push_str(new_text);
        pos = (*end).max(pos);
    }
    out.push_str(text.get(pos..).unwrap_or_default());
    if !saw_destination {
        return None;
    }
    let caret =
        DocumentSnapshot::new(&out).byte_offset_to_position(caret_byte)?;

    Some(PlannedSegmentAccept {
        primary: EditorTextEdit {
            range: primary_range,
            new_text: String::new(),
        },
        additional,
        destination_text: dest_text,
        caret,
    })
}

/// The destination's `new_text` and possibly extended end byte offset.
///
/// Mirrors the token-local expansion's trailing-space policy at the
/// destination site: one following ASCII space is consumed into the edit and
/// reinserted, a tab is left alone, and end-of-line/segment appends a spacer
/// so neighbors never join.
fn destination_replacement(
    text: &str,
    dest_end: usize,
    replacement: &str,
) -> (String, usize) {
    match text.get(dest_end..).and_then(|tail| tail.chars().next()) {
        Some(' ') => (format!("{replacement} "), dest_end + 1),
        Some('\t') => (replacement.to_string(), dest_end),
        Some('\n') | Some('\r') | None => (format!("{replacement} "), dest_end),
        Some(_) => (replacement.to_string(), dest_end),
    }
}

fn byte_range_of(
    document: &DocumentSnapshot,
    range: &EditorRange,
) -> Option<(usize, usize)> {
    Some((
        document.position_to_byte_offset(range.start)?,
        document.position_to_byte_offset(range.end)?,
    ))
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
    let token = text.get(start..end)?;
    let leading_markers =
        leading_ascii_marker_count(token, MODEL_SHORTCUT_MARKER);
    if leading_markers == 0
        || !shortcut_left_boundary(text, start)
        || excluded_position(document, position, start)
    {
        return None;
    }

    let kind = match leading_markers {
        1 => ModelShortcutKind::Alias,
        2 => ModelShortcutKind::Model,
        _ => return None,
    };
    let trigger_end = start + leading_markers;
    if cursor < trigger_end {
        return None;
    }
    let suffix = text.get(trigger_end..end)?;
    if suffix.bytes().any(|byte| byte == MODEL_SHORTCUT_MARKER) {
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

fn leading_ascii_marker_count(token: &str, marker: u8) -> usize {
    token.bytes().take_while(|byte| *byte == marker).count()
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
    safe_inline_model_directive_value(alias).then(|| format!("@{alias}"))
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
    (safe_inline_model_directive_value(value) && !value.starts_with('@'))
        .then(|| value.to_string())
}

fn safe_inline_model_directive_value(value: &str) -> bool {
    !value.is_empty()
        && !value.chars().any(char::is_whitespace)
        && validate_model_value(value).is_ok()
}

fn model_directive_replacement(value: &str) -> String {
    format!("%m:{value}")
}

/// Build the expansion preview, the edit's `new_text`, the edit range's end
/// byte offset, and the post-edit caret byte offset.
///
/// `edit_end` normally equals `end` (the edit touches only the shortcut token),
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
        &excluded_literal_and_definition_ranges(document.text()),
    ) {
        return true;
    }
    detect_placeholder_context_at_position(document, position).is_some()
        || directive_value_covers_trigger(document, position)
}

/// Whether the cursor sits inside a directive value token.
///
/// A whitespace-separated shortcut token that merely follows a directive on
/// the same line (`%model:old Use =la`) or inside `%alt(...)` arguments
/// (`%alt(a =la, b)`) is its own token, not a continuation of the directive
/// value, so the directive context there must not suppress the shortcut.
/// The reported context token runs from the value start to the cursor, so a
/// token containing whitespace proves the cursor left the value.
fn directive_value_covers_trigger(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> bool {
    detect_directive_context_at_position(document, position)
        .and_then(|context| context.token)
        .is_some_and(|token| !token.text.chars().any(char::is_whitespace))
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
            provider_display: String::new(),
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
        assert_eq!(context("=", 0, 1).query, "");
        assert_eq!(context("Explain =la", 0, 11).query, "la");
        assert_eq!(context("one\n=sm", 1, 3).query, "sm");
        assert_eq!(context("one\n  =sm", 1, 5).query, "sm");
    }

    #[test]
    fn detects_model_shortcut_context_without_alias_wrapper_fallback() {
        let model = shortcut_context("Review ==gpt", 0, 12);
        assert_eq!(model.kind, ModelShortcutKind::Model);
        assert_eq!(model.query, "gpt");
        assert_eq!(model.token, "==gpt");
        assert_eq!(model.caret, pos(0, 12));
        assert_eq!(model.replacement_range.start, pos(0, 7));
        assert_eq!(model.replacement_range.end, pos(0, 12));
        assert_eq!(
            detect_model_alias_shortcut_context("Review ==gpt", pos(0, 12)),
            None
        );

        let alias = shortcut_context("Review =la", 0, 10);
        assert_eq!(alias.kind, ModelShortcutKind::Alias);
        assert_eq!(alias.query, "la");

        let crlf = shortcut_context("one\r\n  ==gpt", 1, 7);
        assert_eq!(crlf.kind, ModelShortcutKind::Model);
        assert_eq!(crlf.query, "gpt");
    }

    #[test]
    fn rejects_embedded_escaped_tabs_and_completed_equals_pairs() {
        for (text, position) in [
            ("a=b", pos(0, 3)),
            ("path/=", pos(0, 6)),
            (r"\=", pos(0, 2)),
            ("one\t=la", pos(0, 7)),
            ("==bold", pos(0, 2)),
            ("=emphasis=", pos(0, 5)),
            ("=@large", pos(0, 7)),
        ] {
            assert_eq!(
                detect_model_alias_shortcut_context(text, position),
                None
            );
        }
    }

    #[test]
    fn rejects_literal_model_equals_tokens() {
        for (text, position) in [
            ("==", pos(0, 1)),
            ("===", pos(0, 3)),
            ("==bold==", pos(0, 4)),
            ("==gpt==", pos(0, 5)),
            ("a==b", pos(0, 4)),
            ("path/==", pos(0, 7)),
            (r"\==", pos(0, 3)),
            ("one\t==gpt", pos(0, 9)),
        ] {
            assert_eq!(model_shortcut_context(text, position), None);
        }
    }

    #[test]
    fn ignores_legacy_star_shortcut_tokens() {
        for (text, position) in [
            ("*", pos(0, 1)),
            ("*la", pos(0, 3)),
            ("**", pos(0, 2)),
            ("**gpt", pos(0, 5)),
            ("Use *la", pos(0, 7)),
            ("Use **gpt", pos(0, 9)),
        ] {
            assert_eq!(model_shortcut_context(text, position), None);
            assert_eq!(
                detect_model_alias_shortcut_context(text, position),
                None
            );
        }
    }

    #[test]
    fn plans_full_token_replacement_from_mid_token_caret() {
        let planned = plan("Explain =laX later", 0, 11, "@large");
        assert_eq!(planned.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Explain =laX later", &planned.edit),
            "Explain %m:@large later"
        );
        assert_eq!(planned.caret, pos(0, 18));
    }

    #[test]
    fn preserves_or_adds_whitespace_after_expansion() {
        let at_end = plan("Use =la", 0, 7, "@large");
        assert_eq!(at_end.edit.new_text, "%m:@large ");
        assert_eq!(apply("Use =la", &at_end.edit), "Use %m:@large ");
        assert_eq!(at_end.caret, pos(0, 14));

        let before_one_space = plan("Use =la now", 0, 7, "@large");
        assert_eq!(before_one_space.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use =la now", &before_one_space.edit),
            "Use %m:@large now"
        );
        assert_eq!(before_one_space.caret, pos(0, 14));

        let before_space = plan("Use =la   now", 0, 7, "@large");
        assert_eq!(before_space.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use =la   now", &before_space.edit),
            "Use %m:@large   now"
        );
        assert_eq!(before_space.caret, pos(0, 14));

        let before_tab = plan("Use =la\tnow", 0, 7, "@large");
        assert_eq!(before_tab.edit.new_text, "%m:@large");
        assert_eq!(
            apply("Use =la\tnow", &before_tab.edit),
            "Use %m:@large\tnow"
        );
        assert_eq!(before_tab.caret, pos(0, 13));

        let before_newline = plan("Use =la\nnow", 0, 7, "@large");
        assert_eq!(before_newline.edit.new_text, "%m:@large ");
        assert_eq!(
            apply("Use =la\nnow", &before_newline.edit),
            "Use %m:@large \nnow"
        );
        assert_eq!(before_newline.caret, pos(0, 14));
    }

    #[test]
    fn rejects_literal_regions_and_frontmatter() {
        for (text, position) in [
            ("`=la`", pos(0, 3)),
            ("```text\n=la\n```", pos(1, 3)),
            (
                "%xprompts_enabled:false\n=la\n%xprompts_enabled:true\n",
                pos(1, 3),
            ),
            ("---\nname: =la\n---\nbody", pos(1, 9)),
            ("%model:=la", pos(0, 10)),
            ("{{=la}}", pos(0, 5)),
            ("{{ =la }}", pos(0, 6)),
            ("{% if =la %}", pos(0, 9)),
            ("{# =la #}", pos(0, 6)),
            ("prefix {{\n  =la\n}} suffix", pos(1, 5)),
            ("{{ =la", pos(0, 6)),
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
                "Use =la",
                pos(0, 7),
                &entries,
                "@scout"
            ),
            None
        );
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use =la",
                pos(0, 7),
                &entries,
                "large-model"
            ),
            None
        );
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use =la",
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
            "Use ==faX later",
            pos(0, 8),
            &entries,
            "claude-fable-5",
        )
        .unwrap();
        assert_eq!(planned.kind, ModelShortcutKind::Model);
        assert_eq!(planned.value, "claude-fable-5");
        assert_eq!(planned.edit.new_text, "%m:claude-fable-5 ");
        assert_eq!(
            apply("Use ==faX later", &planned.edit),
            "Use %m:claude-fable-5 later"
        );
        assert_eq!(planned.caret, pos(0, 22));

        let scoped = model_shortcut_edit(
            "Use ==claude/fa",
            pos(0, 15),
            &entries,
            "claude/claude-fable-5",
        )
        .unwrap();
        assert_eq!(scoped.value, "claude/claude-fable-5");
        assert_eq!(scoped.edit.new_text, "%m:claude/claude-fable-5 ");

        assert_eq!(
            model_shortcut_edit(
                "Use ==claude/fa",
                pos(0, 15),
                &entries,
                "claude-fable-5",
            ),
            None
        );
        assert_eq!(
            model_shortcut_edit("Use ==fa", pos(0, 8), &entries, "@large"),
            None
        );
        assert_eq!(
            model_shortcut_edit(
                "Use ==claude",
                pos(0, 12),
                &entries,
                "claude/"
            ),
            None
        );
    }

    #[test]
    fn rejects_unsafe_shortcut_values_before_emitting_model_directive() {
        for unsafe_value in [
            "gpt bad",
            "gpt\tbad",
            "gpt\nbad",
            "gpt\0bad",
            "gpt\u{001f}bad",
            "gpt\u{007f}bad",
        ] {
            let unsafe_entries = vec![model_entry(unsafe_value, "codex", &[])];
            assert_eq!(
                values(filter_explicit_model_shortcut_entries(
                    &unsafe_entries,
                    "gpt"
                )),
                vec![unsafe_value],
                "the candidate may match filtering before edit validation"
            );
            assert_eq!(
                model_shortcut_edit(
                    "Use ==gpt",
                    pos(0, 9),
                    &unsafe_entries,
                    unsafe_value
                ),
                None,
                "unsafe selected model value {unsafe_value:?}"
            );
        }

        let alias_entries = vec![entry("@large", "user_alias")];
        let planned = plan_model_alias_shortcut_edit(
            "Use =la",
            pos(0, 7),
            &alias_entries,
            "@large",
        )
        .unwrap();
        assert_eq!(planned.edit.new_text, "%m:@large ");

        let unsafe_alias_entries = vec![entry("@bad\0alias", "user_alias")];
        assert_eq!(
            plan_model_alias_shortcut_edit(
                "Use =bad",
                pos(0, 8),
                &unsafe_alias_entries,
                "@bad\0alias",
            ),
            None
        );
    }

    #[test]
    fn accepts_safe_model_punctuation_and_nested_provider_values() {
        let entries = vec![
            model_entry("gpt-5.6_sol.alpha+preview", "codex", &["gpt56"]),
            model_entry("anthropic/claude-sonnet-4-5", "opencode", &[]),
            provider_entry("opencode/", "opencode"),
        ];

        let punctuation = model_shortcut_edit(
            "Use ==gptX later",
            pos(0, 9),
            &entries,
            "gpt-5.6_sol.alpha+preview",
        )
        .unwrap();
        assert_eq!(punctuation.value, "gpt-5.6_sol.alpha+preview");
        assert_eq!(
            apply("Use ==gptX later", &punctuation.edit),
            "Use %m:gpt-5.6_sol.alpha+preview later"
        );

        let nested = model_shortcut_edit(
            "Use ==opencode/anthropic/",
            pos(0, 25),
            &entries,
            "opencode/anthropic/claude-sonnet-4-5",
        )
        .unwrap();
        assert_eq!(nested.value, "opencode/anthropic/claude-sonnet-4-5");
        assert_eq!(
            nested.edit.new_text,
            "%m:opencode/anthropic/claude-sonnet-4-5 "
        );
    }

    #[test]
    fn utf16_positions_survive_unicode_and_crlf() {
        let text = "🙂 =la\r\nnext";
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
            ("Use =la", 7),
            ("Use =la now", 7),
            ("Use =la   now", 7),
            ("Use =la\tnow", 7),
            ("Use =la\nnow", 7),
            ("Explain =laX later", 11),
            ("🙂 =la\r\nnext", 6),
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

    fn both_entries() -> Vec<ModelCompletionEntryWire> {
        vec![
            entry("@large", "user_alias"),
            entry("@small", "implicit_alias"),
            model_entry("gpt-5.6-sol", "codex", &["gpt56sol"]),
            model_entry("opus", "claude", &[]),
            provider_entry("codex/", "codex"),
        ]
    }

    fn accept(
        text: &str,
        line: u32,
        character: u32,
        selected: &str,
    ) -> ModelShortcutEditWire {
        model_shortcut_edit(
            text,
            pos(line, character),
            &both_entries(),
            selected,
        )
        .unwrap_or_else(|| panic!("expected accept for {text:?}"))
    }

    /// Apply the primary edit plus every additional edit, asserting the set
    /// is pairwise disjoint in original-document bytes.
    fn apply_all(text: &str, planned: &ModelShortcutEditWire) -> String {
        let document = DocumentSnapshot::new(text);
        let mut edits: Vec<(usize, usize, &str)> = vec![(
            byte_offset(&document, planned.edit.range.start),
            byte_offset(&document, planned.edit.range.end),
            planned.edit.new_text.as_str(),
        )];
        for edit in &planned.additional_edits {
            edits.push((
                byte_offset(&document, edit.range.start),
                byte_offset(&document, edit.range.end),
                edit.new_text.as_str(),
            ));
        }
        edits.sort_by_key(|(start, end, _)| (*start, *end));
        for pair in edits.windows(2) {
            assert!(
                pair[0].1 <= pair[1].0,
                "edits overlap in {text:?}: {edits:?}"
            );
        }
        let mut out = String::new();
        let mut cursor = 0;
        for (start, end, new_text) in edits {
            out.push_str(&text[cursor..start]);
            out.push_str(new_text);
            cursor = end;
        }
        out.push_str(&text[cursor..]);
        out
    }

    fn byte_offset(document: &DocumentSnapshot, pos: EditorPosition) -> usize {
        document.position_to_byte_offset(pos).unwrap()
    }

    /// Assert the applied document, the post-edit caret, and the invariant
    /// that the caret sits at the end of the destination replacement (the
    /// multi-edit `replacement`) or, for single edits, at the end of the
    /// applied edit.
    fn assert_accept(
        text: &str,
        line: u32,
        character: u32,
        selected: &str,
        expected_doc: &str,
        expected_caret: EditorPosition,
    ) {
        let planned = accept(text, line, character, selected);
        let applied = apply_all(text, &planned);
        assert_eq!(applied, expected_doc, "accept: {text:?}");
        assert_eq!(planned.caret, expected_caret, "caret: {text:?}");
        let applied_doc = DocumentSnapshot::new(&applied);
        let caret_byte = byte_offset(&applied_doc, planned.caret);
        if planned.additional_edits.is_empty() {
            assert_eq!(
                planned.caret,
                end_of_edit(&planned.edit),
                "single-edit caret: {text:?}"
            );
        } else {
            assert!(
                planned.edit.new_text.is_empty(),
                "multi-edit primary deletes the trigger: {text:?}"
            );
            assert!(
                applied[..caret_byte].ends_with(&planned.replacement),
                "caret ends the destination: {text:?}"
            );
        }
    }

    #[test]
    fn replaces_earliest_directive_before_and_after_trigger() {
        assert_accept(
            "%model:old Use =la",
            0,
            18,
            "@large",
            "%m:@large Use ",
            pos(0, 10),
        );
        assert_accept(
            "Use =la then %m:old",
            0,
            7,
            "@large",
            "Use then %m:@large ",
            pos(0, 19),
        );
        assert_accept(
            "Use =la now %model:old end",
            0,
            7,
            "@large",
            "Use now %m:@large end",
            pos(0, 18),
        );
    }

    #[test]
    fn replaces_explicit_model_and_paren_forms_at_their_position() {
        assert_accept(
            "Use ==gpt then %m:old",
            0,
            9,
            "gpt-5.6-sol",
            "Use then %m:gpt-5.6-sol ",
            pos(0, 24),
        );
        assert_accept(
            "%model(old) Use =la",
            0,
            19,
            "@large",
            "%m:@large Use ",
            pos(0, 10),
        );
        assert_accept(
            "%m(old) Use =la",
            0,
            15,
            "@large",
            "%m:@large Use ",
            pos(0, 10),
        );
        assert_accept(
            "%model:old Use ==op",
            0,
            19,
            "opus",
            "%m:opus Use ",
            pos(0, 8),
        );
    }

    #[test]
    fn removes_further_directives_leaving_one() {
        assert_accept(
            "%m:a one %model:b two =la",
            0,
            25,
            "@large",
            "%m:@large one two ",
            pos(0, 10),
        );
    }

    #[test]
    fn removes_adjacent_trailing_directives_at_end_of_line() {
        // The destination consumes the space after `%m:a` while the strip
        // eats the space before `%m:b`: the shared space shrinks the second
        // removal instead of skipping it.
        assert_accept(
            "=la %m:a %m:b",
            0,
            3,
            "@large",
            "%m:@large ",
            pos(0, 10),
        );
        assert_accept(
            "Use ==op %m:a %m:b",
            0,
            7,
            "opus",
            "Use %m:opus ",
            pos(0, 12),
        );
    }

    #[test]
    fn removes_adjacent_trailing_directives_before_newline() {
        assert_accept(
            "Use =la %m:a %m:b\ntail",
            0,
            7,
            "@large",
            "Use %m:@large \ntail",
            pos(0, 14),
        );
        assert_accept(
            "Use ==op %m:a %m:b\ntail",
            0,
            7,
            "opus",
            "Use %m:opus \ntail",
            pos(0, 12),
        );
    }

    #[test]
    fn removes_trailing_directives_on_their_own_lines() {
        assert_accept(
            "%m:a\n%m:b\nUse =la",
            2,
            7,
            "@large",
            "%m:@large \nUse ",
            pos(0, 10),
        );
        assert_accept(
            "%m:a\n%m:b\nUse ==op",
            2,
            8,
            "opus",
            "%m:opus \nUse ",
            pos(0, 8),
        );
    }

    #[test]
    fn adjacent_cleanup_keeps_protected_branch_targets() {
        // Alternation branches are never targets even when an eligible
        // directive sits directly beside them, and the eligible neighbor
        // is still removed.
        assert_accept(
            "%alt(%m:opus, %m:sonnet) %m:a %m:b =la",
            0,
            38,
            "@large",
            "%alt(%m:opus, %m:sonnet) %m:@large ",
            pos(0, 35),
        );
        assert_accept(
            "%alt(%m:opus, %m:sonnet) %m:a %m:b ==op",
            0,
            39,
            "opus",
            "%alt(%m:opus, %m:sonnet) %m:opus ",
            pos(0, 33),
        );
    }

    #[test]
    fn destination_spacing_mirrors_token_expansion_policy() {
        assert_accept(
            "%model:old\tUse =la",
            0,
            17,
            "@large",
            "%m:@large\tUse ",
            pos(0, 9),
        );
        assert_accept(
            "%model:old\nUse =la",
            1,
            7,
            "@large",
            "%m:@large \nUse ",
            pos(0, 10),
        );
    }

    #[test]
    fn unicode_offsets_use_utf16_for_destination_and_caret() {
        assert_accept(
            "🙂 %model:old =la",
            0,
            17,
            "@large",
            "🙂 %m:@large ",
            pos(0, 13),
        );
    }

    #[test]
    fn directives_in_other_segments_are_not_targets() {
        assert_accept(
            "%model:seg\n---\nUse =la",
            2,
            7,
            "@large",
            "%model:seg\n---\nUse %m:@large ",
            pos(2, 14),
        );
        assert_accept(
            "Use =la\n---\n%m:seg",
            0,
            7,
            "@large",
            "Use %m:@large \n---\n%m:seg",
            pos(0, 14),
        );
    }

    #[test]
    fn alternation_branches_are_never_targets() {
        for text in [
            "%{%m:opus | %m:sonnet} Use =la",
            "%alt(%m:opus, %m:sonnet) Use =la",
            "%(%m:opus, %m:sonnet) Use =la",
            "%{doc=%m:opus | code=%m:sonnet} Use =la",
            "%alt(foo(%m:opus), %m:sonnet) Use =la",
        ] {
            let len = text.encode_utf16().count() as u32;
            let planned = accept(text, 0, len, "@large");
            assert!(
                planned.additional_edits.is_empty(),
                "token-local for {text:?}"
            );
            let applied = apply_all(text, &planned);
            assert!(
                applied.starts_with(&text[..text.find(" Use ").unwrap()]),
                "branches intact for {text:?}"
            );
            assert!(
                applied.ends_with("Use %m:@large "),
                "applied: {applied:?}"
            );
        }
    }

    #[test]
    fn trigger_inside_alternation_stays_local() {
        assert_accept(
            "%{Use =la | other} %model:old",
            0,
            8,
            "@large",
            "%{Use %m:@large | other} %model:old",
            pos(0, 16),
        );
        assert_accept(
            "%alt(a =la, b) %model:old",
            0,
            10,
            "@large",
            "%alt(a %m:@large b) %model:old",
            pos(0, 17),
        );
        assert_accept(
            "%{Use ==gpt | other} %m:old",
            0,
            10,
            "gpt-5.6-sol",
            "%{Use %m:gpt-5.6-sol | other} %m:old",
            pos(0, 21),
        );
    }

    #[test]
    fn branch_tags_and_outside_text_survive_model_accept() {
        assert_accept(
            "%model:old %{+sase | +notes} Use =la",
            0,
            34,
            "@large",
            "%m:@large %{+sase | +notes} Use ",
            pos(0, 10),
        );
    }

    #[test]
    fn literal_zones_are_never_targets() {
        assert_accept(
            "```text\n%model:old\n```\nUse =la",
            3,
            7,
            "@large",
            "```text\n%model:old\n```\nUse %m:@large ",
            pos(3, 14),
        );
        assert_accept(
            "---\ntitle: %model:old\n---\nUse =la",
            3,
            7,
            "@large",
            "---\ntitle: %model:old\n---\nUse %m:@large ",
            pos(3, 14),
        );
        assert_accept(
            "%xprompts_enabled:false\n%model:old\n%xprompts_enabled:true\nUse =la",
            3,
            7,
            "@large",
            "%xprompts_enabled:false\n%model:old\n%xprompts_enabled:true\nUse %m:@large ",
            pos(3, 14),
        );
    }

    #[test]
    fn jinja_open_after_percent_is_an_alternation_not_a_zone() {
        // Without the `%{` carve-out in the Jinja scan, the `{` would read
        // as an unclosed `{%` and swallow the shortcut trigger.
        let planned = accept("%{a | b} Use =la", 0, 16, "@large");
        assert!(planned.additional_edits.is_empty());
        assert_eq!(
            apply_all("%{a | b} Use =la", &planned),
            "%{a | b} Use %m:@large "
        );
    }

    #[test]
    fn trigger_token_overlapping_a_body_keeps_body_text() {
        // The shared alternation grammar still sees `%{` after `(` inside
        // the whitespace token, so the expansion stops at the body start.
        let planned = accept("Use =la(%{x | y})", 0, 7, "@large");
        assert_eq!(
            apply_all("Use =la(%{x | y})", &planned),
            "Use %m:@large%{x | y})"
        );
    }
}
