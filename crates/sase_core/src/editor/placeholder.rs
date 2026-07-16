use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::token::DocumentSnapshot;
use super::wire::{
    CompletionCandidate, CompletionList, EditorPosition, EditorRange,
    EditorTextEdit,
};

/// Maximum number of Unicode scalar values allowed inside a placeholder.
pub const PLACEHOLDER_MAX_INNER_CHARS: usize = 100;

/// One complete, valid `<placeholder>` span in a document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceholderSpan {
    /// The placeholder's inner text, without angle brackets.
    pub text: String,
    /// The full range, including `<` and `>`.
    pub range: EditorRange,
    /// The inner range, excluding `<` and `>`.
    pub inner_range: EditorRange,
}

/// Cursor context inside an opening `<` on one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceholderContext {
    pub prefix: String,
    pub prefix_range: EditorRange,
    pub replacement_range: EditorRange,
    pub append_closing_bracket: bool,
    pub(crate) opening_byte: usize,
    pub(crate) prefix_byte_start: usize,
    pub(crate) cursor_byte: usize,
}

/// Shared placeholder completion payload used by the TUI and LSP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceholderCompletion {
    pub prefix: String,
    /// Range of the current inner text. An existing closing `>` is excluded.
    pub replacement_range: EditorRange,
    pub append_closing_bracket: bool,
    /// Distinct inner texts in document order, filtered by `prefix`.
    pub candidates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtractedPlaceholder {
    span: PlaceholderSpan,
    opening_byte: usize,
}

/// Extract every complete, valid placeholder from the document.
pub fn extract_placeholder_spans(
    document: &DocumentSnapshot,
) -> Vec<PlaceholderSpan> {
    scan_placeholder_spans(document)
        .into_iter()
        .map(|placeholder| placeholder.span)
        .collect()
}

/// Detect whether `position` is inside an unmatched `<` on the same line.
pub fn detect_placeholder_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<PlaceholderContext> {
    let cursor_byte = document.position_to_byte_offset(position)?;
    let line = document.line_text(position.line)?;
    let line_start = document.position_to_byte_offset(EditorPosition {
        line: position.line,
        character: 0,
    })?;
    let cursor_in_line = cursor_byte.checked_sub(line_start)?;
    let before = line.get(..cursor_in_line)?;
    let opening_in_line = before.rfind('<')?;
    let prefix_byte_start = line_start + opening_in_line + 1;
    let prefix = document.text().get(prefix_byte_start..cursor_byte)?;
    if prefix.contains('>') {
        return None;
    }

    let after = line.get(cursor_in_line..)?;
    let closing_in_after = after
        .char_indices()
        .find(|(_, ch)| matches!(ch, '<' | '>'))
        .and_then(|(index, ch)| (ch == '>').then_some(index));
    let replacement_end = closing_in_after
        .map(|index| cursor_byte + index)
        .unwrap_or(cursor_byte);
    let prefix_range =
        document.byte_range_to_range(prefix_byte_start, cursor_byte)?;
    let replacement_range =
        document.byte_range_to_range(prefix_byte_start, replacement_end)?;

    Some(PlaceholderContext {
        prefix: prefix.to_string(),
        prefix_range,
        replacement_range,
        append_closing_bracket: closing_in_after.is_none(),
        opening_byte: line_start + opening_in_line,
        prefix_byte_start,
        cursor_byte,
    })
}

/// Build ordered placeholder candidates for the cursor context.
pub fn build_placeholder_completion_candidates(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<PlaceholderCompletion> {
    let context = detect_placeholder_context_at_position(document, position)?;
    let prefix_lower = context.prefix.to_lowercase();
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();

    for placeholder in scan_placeholder_spans(document) {
        if placeholder.opening_byte == context.opening_byte {
            continue;
        }
        let text = placeholder.span.text;
        if text.to_lowercase().starts_with(&prefix_lower)
            && seen.insert(text.clone())
        {
            candidates.push(text);
        }
    }

    Some(PlaceholderCompletion {
        prefix: context.prefix,
        replacement_range: context.replacement_range,
        append_closing_bracket: context.append_closing_bracket,
        candidates,
    })
}

impl PlaceholderCompletion {
    /// Convert the shared payload into editor candidates with complete accept
    /// edits. The edit consumes an existing closing bracket (when present) and
    /// writes it back so LSP clients leave the cursor after `>`.
    pub fn into_completion_list(self) -> CompletionList {
        let mut accept_range = self.replacement_range;
        if !self.append_closing_bracket {
            accept_range.end.character += 1;
        }
        let candidates = self
            .candidates
            .into_iter()
            .map(|text| CompletionCandidate {
                display: text.clone(),
                insertion: text.clone(),
                detail: Some("placeholder".to_string()),
                documentation: None,
                is_dir: false,
                name: text.clone(),
                replacement: Some(EditorTextEdit {
                    range: accept_range,
                    new_text: format!("{text}>"),
                }),
                additional_edits: Vec::new(),
                kind: "placeholder".to_string(),
                project: String::new(),
                status: String::new(),
            })
            .collect();
        CompletionList {
            candidates,
            shared_extension: String::new(),
        }
    }
}

fn scan_placeholder_spans(
    document: &DocumentSnapshot,
) -> Vec<ExtractedPlaceholder> {
    let mut placeholders = Vec::new();

    for line_number in 0..document.line_count() {
        let Ok(line_number) = u32::try_from(line_number) else {
            break;
        };
        let Some(line) = document.line_text(line_number) else {
            continue;
        };
        let Some(line_start) =
            document.position_to_byte_offset(EditorPosition {
                line: line_number,
                character: 0,
            })
        else {
            continue;
        };
        let mut opening = None;

        for (index, ch) in line.char_indices() {
            match ch {
                '<' => opening = Some(index),
                '>' => {
                    let Some(opening_in_line) = opening.take() else {
                        continue;
                    };
                    let inner_start = opening_in_line + 1;
                    let Some(inner) = line.get(inner_start..index) else {
                        continue;
                    };
                    if !valid_placeholder_inner(inner) {
                        continue;
                    }
                    let opening_byte = line_start + opening_in_line;
                    let inner_byte_start = line_start + inner_start;
                    let closing_byte = line_start + index;
                    let Some(range) = document
                        .byte_range_to_range(opening_byte, closing_byte + 1)
                    else {
                        continue;
                    };
                    let Some(inner_range) = document
                        .byte_range_to_range(inner_byte_start, closing_byte)
                    else {
                        continue;
                    };
                    placeholders.push(ExtractedPlaceholder {
                        span: PlaceholderSpan {
                            text: inner.to_string(),
                            range,
                            inner_range,
                        },
                        opening_byte,
                    });
                }
                _ => {}
            }
        }
    }

    placeholders
}

fn valid_placeholder_inner(inner: &str) -> bool {
    if inner.is_empty() || inner.chars().count() > PLACEHOLDER_MAX_INNER_CHARS {
        return false;
    }
    let mut chars = inner.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    let last = chars.last().unwrap_or(first);
    !first.is_whitespace() && !last.is_whitespace()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marked_document(marked: &str) -> (DocumentSnapshot, EditorPosition) {
        let cursor = marked.find("<CURSOR>").unwrap();
        let document = DocumentSnapshot::new(marked.replace("<CURSOR>", ""));
        let position = document.byte_offset_to_position(cursor).unwrap();
        (document, position)
    }

    #[test]
    fn extracts_strict_single_line_spans_including_code() {
        let long = "x".repeat(PLACEHOLDER_MAX_INNER_CHARS + 1);
        let text = format!(
            "a < b and c > d\n<> < leading> <trailing >\n`<inline>`\n```\n<code value>\n```\n<{long}>\n<alpha>"
        );
        let document = DocumentSnapshot::new(text);
        let spans = extract_placeholder_spans(&document);
        let values: Vec<&str> =
            spans.iter().map(|span| span.text.as_str()).collect();

        assert_eq!(values, vec!["inline", "code value", "alpha"]);
        assert_eq!(
            spans[0].range.start,
            EditorPosition {
                line: 2,
                character: 1
            }
        );
        assert_eq!(spans[0].inner_range.start.character, 2);
    }

    #[test]
    fn detects_context_with_and_without_a_closing_bracket() {
        let (document, cursor) = marked_document("Use <alp<CURSOR>ha> now");
        let context =
            detect_placeholder_context_at_position(&document, cursor).unwrap();
        assert_eq!(context.prefix, "alp");
        assert_eq!(context.replacement_range.start.character, 5);
        assert_eq!(context.replacement_range.end.character, 10);
        assert!(!context.append_closing_bracket);

        let (document, cursor) = marked_document("Use <alp<CURSOR> now");
        let context =
            detect_placeholder_context_at_position(&document, cursor).unwrap();
        assert_eq!(context.replacement_range.end, cursor);
        assert!(context.append_closing_bracket);
    }

    #[test]
    fn rejects_context_after_an_intervening_closing_bracket() {
        let (document, cursor) = marked_document("<alpha> tail<CURSOR>");
        assert!(
            detect_placeholder_context_at_position(&document, cursor).is_none()
        );

        let (document, cursor) = marked_document("<outer <inn<CURSOR>");
        let context =
            detect_placeholder_context_at_position(&document, cursor).unwrap();
        assert_eq!(context.prefix, "inn");
    }

    #[test]
    fn builds_deduplicated_document_order_candidates() {
        let (document, cursor) = marked_document(
            "<Beta> <alpha> <Beta> <alphabet soup> choose <a<CURSOR>>",
        );
        let completion =
            build_placeholder_completion_candidates(&document, cursor).unwrap();

        assert_eq!(completion.prefix, "a");
        assert_eq!(completion.candidates, vec!["alpha", "alphabet soup"]);
    }

    #[test]
    fn excludes_the_span_under_the_cursor() {
        let (document, cursor) = marked_document("<only<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor).unwrap();
        assert!(completion.candidates.is_empty());
    }

    #[test]
    fn filters_prefix_case_insensitively_and_handles_utf16_ranges() {
        let (document, cursor) =
            marked_document("🙂 <Alpha Value> use <a<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor).unwrap();
        assert_eq!(completion.candidates, vec!["Alpha Value"]);
        assert_eq!(completion.replacement_range.start.character, 22);
        assert_eq!(completion.replacement_range.end.character, 23);
    }

    #[test]
    fn completion_edits_leave_the_cursor_after_a_closing_bracket() {
        let (document, cursor) = marked_document("<the plan> then <<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor).unwrap();
        let list = completion.into_completion_list();
        let edit = list.candidates[0].replacement.as_ref().unwrap();

        assert_eq!(edit.range.start.character, 17);
        assert_eq!(edit.range.end.character, 18);
        assert_eq!(edit.new_text, "the plan>");
    }
}
