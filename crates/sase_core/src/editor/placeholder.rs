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

/// Where a placeholder candidate came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlaceholderCandidateSource {
    /// Extracted from another `<...>` span in the document being edited.
    Prompt,
    /// Supplied by the caller from the durable common-placeholder store.
    Common,
}

/// One placeholder candidate together with the source that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceholderCandidate {
    /// The placeholder's inner text, without angle brackets.
    pub text: String,
    pub source: PlaceholderCandidateSource,
}

/// Shared placeholder completion payload used by the TUI and LSP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceholderCompletion {
    pub prefix: String,
    /// Range of the current inner text. An existing closing `>` is excluded.
    pub replacement_range: EditorRange,
    pub append_closing_bracket: bool,
    /// Distinct inner texts filtered by `prefix`: document-order prompt
    /// candidates first, then caller-order common candidates.
    pub candidates: Vec<PlaceholderCandidate>,
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
///
/// `common` holds caller-supplied placeholders from a durable store, already
/// ranked by the caller. They are appended, in the order given, after every
/// candidate found in the document itself. Prompt-local candidates always
/// precede common ones, and an empty `common` slice reproduces the
/// document-only behaviour exactly.
pub fn build_placeholder_completion_candidates(
    document: &DocumentSnapshot,
    position: EditorPosition,
    common: &[String],
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
            candidates.push(PlaceholderCandidate {
                text,
                source: PlaceholderCandidateSource::Prompt,
            });
        }
    }

    for text in common {
        if text.to_lowercase().starts_with(&prefix_lower)
            && seen.insert(text.clone())
        {
            candidates.push(PlaceholderCandidate {
                text: text.clone(),
                source: PlaceholderCandidateSource::Common,
            });
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
    ///
    /// `detail` distinguishes the two sources while `kind` stays `placeholder`
    /// for both, because `kind` drives client behaviour and must not fork.
    pub fn into_completion_list(self) -> CompletionList {
        let mut accept_range = self.replacement_range;
        if !self.append_closing_bracket {
            accept_range.end.character += 1;
        }
        let candidates = self
            .candidates
            .into_iter()
            .map(|candidate| {
                let detail = match candidate.source {
                    PlaceholderCandidateSource::Prompt => "placeholder",
                    PlaceholderCandidateSource::Common => "saved placeholder",
                };
                let text = candidate.text;
                CompletionCandidate {
                    display: text.clone(),
                    insertion: text.clone(),
                    detail: Some(detail.to_string()),
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
                }
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

    fn common(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn texts(completion: &PlaceholderCompletion) -> Vec<&str> {
        completion
            .candidates
            .iter()
            .map(|candidate| candidate.text.as_str())
            .collect()
    }

    fn sources(
        completion: &PlaceholderCompletion,
    ) -> Vec<PlaceholderCandidateSource> {
        completion
            .candidates
            .iter()
            .map(|candidate| candidate.source)
            .collect()
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
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();

        assert_eq!(completion.prefix, "a");
        assert_eq!(texts(&completion), vec!["alpha", "alphabet soup"]);
        assert_eq!(
            sources(&completion),
            vec![
                PlaceholderCandidateSource::Prompt,
                PlaceholderCandidateSource::Prompt
            ]
        );
    }

    #[test]
    fn excludes_the_span_under_the_cursor() {
        let (document, cursor) = marked_document("<only<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();
        assert!(completion.candidates.is_empty());
    }

    #[test]
    fn filters_prefix_case_insensitively_and_handles_utf16_ranges() {
        let (document, cursor) =
            marked_document("🙂 <Alpha Value> use <a<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();
        assert_eq!(texts(&completion), vec!["Alpha Value"]);
        assert_eq!(completion.replacement_range.start.character, 22);
        assert_eq!(completion.replacement_range.end.character, 23);
    }

    #[test]
    fn appends_common_candidates_after_prompt_candidates_in_caller_order() {
        let (document, cursor) =
            marked_document("<alpha> <alphabet soup> use <a<CURSOR>>");
        let completion = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["anchor", "aperture"]),
        )
        .unwrap();

        assert_eq!(
            texts(&completion),
            vec!["alpha", "alphabet soup", "anchor", "aperture"]
        );
        assert_eq!(
            sources(&completion),
            vec![
                PlaceholderCandidateSource::Prompt,
                PlaceholderCandidateSource::Prompt,
                PlaceholderCandidateSource::Common,
                PlaceholderCandidateSource::Common,
            ]
        );
    }

    #[test]
    fn dedups_common_candidates_against_the_prompt_and_each_other() {
        let (document, cursor) = marked_document("<alpha> use <a<CURSOR>>");
        let completion = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["alpha", "anchor", "anchor", "Alpha"]),
        )
        .unwrap();

        assert_eq!(texts(&completion), vec!["alpha", "anchor", "Alpha"]);
        assert_eq!(
            sources(&completion),
            vec![
                PlaceholderCandidateSource::Prompt,
                PlaceholderCandidateSource::Common,
                PlaceholderCandidateSource::Common,
            ]
        );
    }

    #[test]
    fn filters_common_candidates_with_the_same_prefix_rule() {
        let (document, cursor) = marked_document("use <A<CURSOR>>");
        let completion = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["anchor", "beta", "Apex"]),
        )
        .unwrap();

        assert_eq!(texts(&completion), vec!["anchor", "Apex"]);
    }

    #[test]
    fn an_empty_common_slice_leaves_document_only_output_unchanged() {
        let (document, cursor) =
            marked_document("<alpha> <beta> use <<CURSOR>>");
        let baseline =
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();
        let with_empty = build_placeholder_completion_candidates(
            &document,
            cursor,
            &Vec::new(),
        )
        .unwrap();

        assert_eq!(baseline, with_empty);
        assert_eq!(texts(&baseline), vec!["alpha", "beta"]);
        assert!(baseline
            .candidates
            .iter()
            .all(|candidate| candidate.source
                == PlaceholderCandidateSource::Prompt));
    }

    #[test]
    fn completion_list_details_reflect_the_candidate_source() {
        let (document, cursor) = marked_document("<alpha> use <a<CURSOR>>");
        let list = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["anchor"]),
        )
        .unwrap()
        .into_completion_list();

        let details: Vec<&str> = list
            .candidates
            .iter()
            .map(|candidate| candidate.detail.as_deref().unwrap())
            .collect();
        assert_eq!(details, vec!["placeholder", "saved placeholder"]);
        assert!(list
            .candidates
            .iter()
            .all(|candidate| candidate.kind == "placeholder"));
    }

    #[test]
    fn candidates_serialize_with_a_lowercase_source_tag() {
        let (document, cursor) = marked_document("<alpha> use <a<CURSOR>>");
        let completion = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["anchor"]),
        )
        .unwrap();
        let value = serde_json::to_value(&completion).unwrap();

        assert_eq!(
            value["candidates"],
            serde_json::json!([
                {"text": "alpha", "source": "prompt"},
                {"text": "anchor", "source": "common"},
            ])
        );
    }

    #[test]
    fn completion_edits_leave_the_cursor_after_a_closing_bracket() {
        let (document, cursor) = marked_document("<the plan> then <<CURSOR>>");
        let completion =
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();
        let list = completion.into_completion_list();
        let edit = list.candidates[0].replacement.as_ref().unwrap();

        assert_eq!(edit.range.start.character, 17);
        assert_eq!(edit.range.end.character, 18);
        assert_eq!(edit.new_text, "the plan>");
    }
}
