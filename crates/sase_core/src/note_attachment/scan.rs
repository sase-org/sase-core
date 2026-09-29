//! Scan, compose, and invert note-attachment references.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::extensions::{extension_mime_for, is_tld_like_extension};
use super::names::{is_valid_attachment_name, sanitize_attachment_name};
use super::NOTE_ATTACHMENT_SCAN_WIRE_SCHEMA_VERSION;
use crate::artifact_ref::{
    artifact_ref_kind_catalog, baseline_document_kind_labels,
    has_allowed_left_context, scan_quoted_argument, unescape_quoted_argument,
};
use crate::fenced_code::fenced_block_ranges;
use crate::prompt_literals::inline_code_ranges;

/// Byte-offset span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentSpanWire {
    pub start: usize,
    pub end: usize,
}

/// One unquoted or quoted `@path` reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentPathRefWire {
    pub raw: String,
    pub path: String,
    pub quoted: bool,
    pub span: NoteAttachmentSpanWire,
}

/// Reuse binding for `@attachment:<name>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoteAttachmentReuseBindingWire {
    Roster,
    EarlierPath,
    Unknown,
}

/// One `@attachment:<name>` reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentReuseRefWire {
    pub name: String,
    pub span: NoteAttachmentSpanWire,
    pub binding: NoteAttachmentReuseBindingWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earlier_path_index: Option<u64>,
}

/// One bare-word `@word` mention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentBareWordWire {
    pub word: String,
    pub span: NoteAttachmentSpanWire,
}

/// One syntax diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentDiagnosticWire {
    pub code: String,
    pub message: String,
    pub span: NoteAttachmentSpanWire,
    pub hint: String,
}

/// Full scan result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachmentScanWire {
    pub schema_version: u64,
    pub path_refs: Vec<NoteAttachmentPathRefWire>,
    pub reuse_refs: Vec<NoteAttachmentReuseRefWire>,
    pub escapes: Vec<NoteAttachmentSpanWire>,
    pub bare_words: Vec<NoteAttachmentBareWordWire>,
    pub diagnostics: Vec<NoteAttachmentDiagnosticWire>,
}

/// One `@attachment:<name>` token outside literal zones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAttachmentTokenWire {
    pub name: String,
    pub span: NoteAttachmentSpanWire,
}

/// Compose errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NoteAttachmentError {
    #[error(
        "assigned_names length {actual} does not match path_refs length {expected}"
    )]
    AssignedNameCount { expected: usize, actual: usize },
    #[error("assigned name {name:?} would be changed by sanitization")]
    UnsanitizedAssignedName { name: String },
}

const TRAILING_PUNCTUATION: &[char] =
    &['.', ',', ';', ':', '!', '?', ')', ']', '}', '>'];
const ATTACHMENT_PREFIX: &str = "attachment:";
const MAX_NAME_LEN: usize = 96;

fn span(start: usize, end: usize) -> NoteAttachmentSpanWire {
    NoteAttachmentSpanWire { start, end }
}

fn citation_kinds() -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for label in baseline_document_kind_labels() {
        set.insert((*label).to_string());
    }
    for descriptor in artifact_ref_kind_catalog() {
        set.insert(descriptor.kind);
        set.extend(descriptor.aliases);
    }
    set.insert("research".to_string());
    set.remove("attachment");
    set
}

fn literal_ranges(text: &str) -> Vec<(usize, usize)> {
    let fenced = fenced_block_ranges(text);
    let mut inline = inline_code_ranges(text, &fenced);
    let mut all = fenced;
    all.append(&mut inline);
    all.sort_unstable();
    all
}

fn position_in_literal(position: usize, ranges: &[(usize, usize)]) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= position && position < *end)
}

fn is_name_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

fn is_segment_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'.' | b'_' | b'+' | b'~' | b'-')
}

fn is_bare_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-')
}

fn take_name_run(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    let mut end = from;
    while end < bytes.len() && is_name_char(bytes[end]) {
        end += 1;
    }
    end
}

fn take_token_end(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    let mut end = from;
    while end < bytes.len() {
        let byte = bytes[end];
        if byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\'' | b'`') {
            break;
        }
        // Multi-byte chars: advance by UTF-8 length. Find char length from
        // the string slice to stay on boundaries.
        if byte < 0x80 {
            end += 1;
        } else {
            let rest = &text[end..];
            let length = rest.chars().next().map_or(1, |c| c.len_utf8());
            end += length;
        }
    }
    end
}

fn trim_trailing_punctuation(
    text: &str,
    start: usize,
    mut end: usize,
) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        if !TRAILING_PUNCTUATION.contains(&character) {
            break;
        }
        end -= character.len_utf8();
    }
    end
}

fn basename_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Scan note text for attachment references.
pub fn scan_note_attachment_refs(
    text: &str,
    roster_names: &[String],
) -> NoteAttachmentScanWire {
    let literals = literal_ranges(text);
    let citations = citation_kinds();
    let mut path_refs = Vec::new();
    let mut reuse_refs = Vec::new();
    let mut escapes = Vec::new();
    let mut bare_words = Vec::new();
    let mut diagnostics = Vec::new();
    let roster: BTreeSet<&str> =
        roster_names.iter().map(String::as_str).collect();

    let at_positions: Vec<usize> = text
        .char_indices()
        .filter_map(|(offset, character)| (character == '@').then_some(offset))
        .collect();
    let mut consumed_until = 0usize;

    for start in at_positions {
        if start < consumed_until {
            continue;
        }
        if position_in_literal(start, &literals) {
            continue;
        }
        if !has_allowed_left_context(text, start) {
            continue;
        }
        // 1. Escape.
        if text[start + 1..].starts_with('@') {
            escapes.push(span(start, start + 2));
            consumed_until = start + 2;
            continue;
        }
        // 2. Reuse.
        if text[start + 1..].starts_with(ATTACHMENT_PREFIX) {
            let after_colon = start + 1 + ATTACHMENT_PREFIX.len();
            let name_end = take_name_run(text, after_colon);
            let name = &text[after_colon..name_end];
            let valid = !name.is_empty()
                && name.len() <= MAX_NAME_LEN
                && !(name.starts_with('.') || name.starts_with('-'));
            if !valid {
                diagnostics.push(NoteAttachmentDiagnosticWire {
                    code: "unknown_attachment".to_string(),
                    message: if name.is_empty() {
                        "attachment name is missing".to_string()
                    } else {
                        format!("invalid attachment name {name:?}")
                    },
                    span: span(start, name_end.max(after_colon)),
                    hint: "Attach the file in this note first, or write `@@attachment:NAME` for literal text.".to_string(),
                });
                consumed_until = name_end.max(after_colon);
                continue;
            }
            if text[name_end..].starts_with('/') {
                consumed_until = start + 1;
                continue;
            }
            let binding = if roster.contains(name) {
                NoteAttachmentReuseBindingWire::Roster
            } else if earliest_path_index(&path_refs, name).is_some() {
                NoteAttachmentReuseBindingWire::EarlierPath
            } else {
                NoteAttachmentReuseBindingWire::Unknown
            };
            let earlier_path_index = match binding {
                NoteAttachmentReuseBindingWire::EarlierPath => {
                    earliest_path_index(&path_refs, name)
                        .map(|index| index as u64)
                }
                _ => None,
            };
            // borrow check: compute before push
            let is_unknown = binding == NoteAttachmentReuseBindingWire::Unknown;
            reuse_refs.push(NoteAttachmentReuseRefWire {
                name: name.to_string(),
                span: span(start, name_end),
                binding,
                earlier_path_index,
            });
            if is_unknown {
                diagnostics.push(NoteAttachmentDiagnosticWire {
                    code: "unknown_attachment".to_string(),
                    message: format!("unknown attachment {name:?}"),
                    span: span(start, name_end),
                    hint: "Attach the file in this note first, or write `@@attachment:NAME` for literal text.".to_string(),
                });
            }
            consumed_until = name_end;
            continue;
        }
        // 3. Citation.
        if let Some(kind_end) = citation_kind_end(text, start, &citations) {
            let _ = kind_end;
            consumed_until = start + 1;
            continue;
        }
        // 4. Quoted path.
        if text[start + 1..].starts_with('"') {
            let (close, terminated) = scan_quoted_argument(text, start + 1);
            let content_start = start + 2;
            if !terminated {
                diagnostics.push(NoteAttachmentDiagnosticWire {
                    code: "unterminated_quote".to_string(),
                    message:
                        "quoted attachment path is missing its closing quote"
                            .to_string(),
                    span: span(start, close),
                    hint: "Close the quote, or write `@@` for a literal `@`."
                        .to_string(),
                });
                consumed_until = start + 2;
                continue;
            }
            let quote_end = close + 1;
            if close == content_start {
                diagnostics.push(NoteAttachmentDiagnosticWire {
                    code: "empty_path".to_string(),
                    message: "quoted attachment path is empty".to_string(),
                    span: span(start, quote_end),
                    hint: "Write a path inside the quotes, or remove the reference."
                        .to_string(),
                });
                consumed_until = quote_end;
                continue;
            }
            let path = unescape_quoted_argument(&text[content_start..close]);
            path_refs.push(NoteAttachmentPathRefWire {
                raw: text[start..quote_end].to_string(),
                path,
                quoted: true,
                span: span(start, quote_end),
            });
            consumed_until = quote_end;
            continue;
        }
        // 5. Unquoted token.
        let raw_end = take_token_end(text, start + 1);
        let trimmed_end = trim_trailing_punctuation(text, start + 1, raw_end);
        if trimmed_end <= start + 1 {
            consumed_until = start + 1;
            continue;
        }
        let token = &text[start + 1..trimmed_end];
        if is_path_token(token) {
            path_refs.push(NoteAttachmentPathRefWire {
                raw: text[start..trimmed_end].to_string(),
                path: token.to_string(),
                quoted: false,
                span: span(start, trimmed_end),
            });
            consumed_until = trimmed_end;
            continue;
        }
        let run_end = take_bare_run(token);
        if run_end == 0 {
            consumed_until = start + 1;
            continue;
        }
        let word = &token[..run_end];
        bare_words.push(NoteAttachmentBareWordWire {
            word: word.to_string(),
            span: span(start, start + 1 + run_end),
        });
        consumed_until = start + 1 + run_end;
    }

    NoteAttachmentScanWire {
        schema_version: NOTE_ATTACHMENT_SCAN_WIRE_SCHEMA_VERSION,
        path_refs,
        reuse_refs,
        escapes,
        bare_words,
        diagnostics,
    }
}

fn earliest_path_index(
    path_refs: &[NoteAttachmentPathRefWire],
    name: &str,
) -> Option<usize> {
    path_refs.iter().position(|path_ref| {
        sanitize_attachment_name(basename_of(&path_ref.path)) == name
    })
}

fn citation_kind_end(
    text: &str,
    start: usize,
    citations: &BTreeSet<String>,
) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut end = start + 1;
    while end < bytes.len()
        && (bytes[end].is_ascii_lowercase()
            || bytes[end].is_ascii_digit()
            || matches!(bytes[end], b'_' | b'-'))
    {
        end += 1;
    }
    if end == start + 1 || end >= bytes.len() || bytes[end] != b':' {
        return None;
    }
    // First character must be lowercase.
    if !bytes[start + 1].is_ascii_lowercase() {
        return None;
    }
    let kind = &text[start + 1..end];
    citations.contains(kind).then_some(end + 1)
}

fn is_path_token(token: &str) -> bool {
    if token.starts_with('/')
        || token.starts_with("~/")
        || token.starts_with("./")
        || token.starts_with("../")
    {
        return true;
    }
    if token.contains('/') && segments_match(token) {
        return true;
    }
    if let Some(dot) = token.rfind('.') {
        if dot + 1 < token.len() {
            let ext = &token[dot + 1..];
            if !ext.is_empty()
                && !ext.contains('/')
                && ext.bytes().all(|byte| byte.is_ascii_alphanumeric())
            {
                let lower = ext.to_ascii_lowercase();
                if extension_mime_for(&lower).is_some()
                    && !is_tld_like_extension(&lower)
                    && !token[..dot].is_empty()
                {
                    return true;
                }
            }
        }
    }
    false
}

fn segments_match(token: &str) -> bool {
    for segment in token.split('/') {
        if segment.is_empty() || !segment.bytes().all(is_segment_char) {
            return false;
        }
    }
    true
}

fn take_bare_run(token: &str) -> usize {
    let bytes = token.as_bytes();
    let mut end = 0;
    while end < bytes.len() && is_bare_char(bytes[end]) {
        end += 1;
    }
    end
}

/// Compose stored text from source text and assigned names.
pub fn compose_note_attachment_text(
    text: &str,
    scan: &NoteAttachmentScanWire,
    assigned_names: &[String],
) -> Result<String, NoteAttachmentError> {
    if assigned_names.len() != scan.path_refs.len() {
        return Err(NoteAttachmentError::AssignedNameCount {
            expected: scan.path_refs.len(),
            actual: assigned_names.len(),
        });
    }
    for name in assigned_names {
        if sanitize_attachment_name(name) != *name {
            return Err(NoteAttachmentError::UnsanitizedAssignedName {
                name: name.clone(),
            });
        }
    }
    enum Action {
        Escape,
        Path(usize),
        Retarget(u64),
        Copy,
    }
    let mut spans: Vec<(usize, usize, Action)> = Vec::new();
    for escape in &scan.escapes {
        spans.push((escape.start, escape.end, Action::Escape));
    }
    for (index, path_ref) in scan.path_refs.iter().enumerate() {
        spans.push((
            path_ref.span.start,
            path_ref.span.end,
            Action::Path(index),
        ));
    }
    for reuse_ref in &scan.reuse_refs {
        match reuse_ref.binding {
            NoteAttachmentReuseBindingWire::EarlierPath => {
                let target = reuse_ref.earlier_path_index.unwrap_or(u64::MAX);
                spans.push((
                    reuse_ref.span.start,
                    reuse_ref.span.end,
                    Action::Retarget(target),
                ));
            }
            _ => {
                spans.push((
                    reuse_ref.span.start,
                    reuse_ref.span.end,
                    Action::Copy,
                ));
            }
        }
    }
    spans.sort_by_key(|(start, end, _)| (*start, *end));
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    for (start, end, action) in spans {
        if start < cursor {
            continue;
        }
        if start > text.len() || end > text.len() || start > end {
            continue;
        }
        out.push_str(&text[cursor..start]);
        match action {
            Action::Escape => out.push('@'),
            Action::Path(index) => {
                out.push_str("@attachment:");
                out.push_str(&assigned_names[index]);
            }
            Action::Retarget(target) => {
                if (target as usize) < assigned_names.len() {
                    out.push_str("@attachment:");
                    out.push_str(&assigned_names[target as usize]);
                } else {
                    out.push_str(&text[start..end]);
                }
            }
            Action::Copy => out.push_str(&text[start..end]),
        }
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    Ok(out)
}

/// Invert stored text back into source text.
pub fn note_attachment_source_text(
    stored: &str,
    manifest_names: &[String],
) -> String {
    let scan = scan_note_attachment_refs(stored, manifest_names);
    let mut starts: Vec<usize> = Vec::new();
    for escape in &scan.escapes {
        starts.push(escape.start);
    }
    for path_ref in &scan.path_refs {
        starts.push(path_ref.span.start);
    }
    for reuse_ref in &scan.reuse_refs {
        if reuse_ref.binding == NoteAttachmentReuseBindingWire::Unknown {
            starts.push(reuse_ref.span.start);
        }
    }
    for diagnostic in &scan.diagnostics {
        starts.push(diagnostic.span.start);
    }
    starts.sort_unstable();
    starts.dedup();
    let mut out = String::with_capacity(stored.len() + starts.len());
    let mut cursor = 0usize;
    for start in starts {
        if start > stored.len() || !stored.is_char_boundary(start) {
            continue;
        }
        if start < cursor {
            continue;
        }
        out.push_str(&stored[cursor..start]);
        out.push('@');
        cursor = start;
    }
    out.push_str(&stored[cursor..]);
    out
}

/// Every boundary `@attachment:<name>` outside literal zones.
pub fn stored_attachment_tokens(text: &str) -> Vec<StoredAttachmentTokenWire> {
    let literals = literal_ranges(text);
    let mut tokens = Vec::new();
    for (start, _) in text.char_indices().filter(|(_, c)| *c == '@') {
        if position_in_literal(start, &literals) {
            continue;
        }
        if !has_allowed_left_context(text, start) {
            continue;
        }
        if !text[start + 1..].starts_with(ATTACHMENT_PREFIX) {
            continue;
        }
        let after_colon = start + 1 + ATTACHMENT_PREFIX.len();
        let run_end = take_name_run(text, after_colon);
        let mut name = &text[after_colon..run_end];
        while name.ends_with('.') {
            name = &name[..name.len() - 1];
        }
        if !is_valid_attachment_name(name) {
            continue;
        }
        let end = after_colon + name.len();
        tokens.push(StoredAttachmentTokenWire {
            name: name.to_string(),
            span: span(start, end),
        });
    }
    tokens
}
