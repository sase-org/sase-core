use std::collections::{BTreeMap, HashMap, HashSet};

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
    /// Whether the span is active prompt text rather than a literal example.
    pub raw: bool,
    /// The full range, including `<` and `>`.
    pub range: EditorRange,
    /// The inner range, excluding `<` and `>`.
    pub inner_range: EditorRange,
}

/// One unique raw placeholder prepared for an input collection surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPlaceholderField {
    /// The placeholder's inner text, without angle brackets.
    pub text: String,
    /// Number of raw occurrences with this exact inner text.
    pub occurrences: usize,
    /// A one-line snippet around the first raw occurrence.
    pub context: String,
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
    /// Literal-zone spans are included after live spans within this group.
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
    /// Distinct inner texts filtered by `prefix`: live prompt candidates in
    /// document order, literal-zone prompt candidates in document order, then
    /// caller-order common candidates.
    pub candidates: Vec<PlaceholderCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtractedPlaceholder {
    span: PlaceholderSpan,
    opening_byte: usize,
    closing_byte_exclusive: usize,
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

/// Summarize unique raw placeholders in first-occurrence order.
pub fn raw_placeholder_fields(
    text: &str,
    context_width: usize,
) -> Vec<RawPlaceholderField> {
    let document = DocumentSnapshot::new(text);
    let mut fields = Vec::<RawPlaceholderField>::new();
    let mut field_indexes = HashMap::<String, usize>::new();

    for placeholder in scan_placeholder_spans(&document)
        .into_iter()
        .filter(|item| item.span.raw)
    {
        if let Some(index) = field_indexes.get(&placeholder.span.text) {
            fields[*index].occurrences += 1;
            continue;
        }

        let index = fields.len();
        field_indexes.insert(placeholder.span.text.clone(), index);
        fields.push(RawPlaceholderField {
            text: placeholder.span.text,
            occurrences: 1,
            context: placeholder_context(
                text,
                placeholder.opening_byte,
                placeholder.closing_byte_exclusive,
                context_width,
            ),
        });
    }

    fields
}

/// Replace mapped raw placeholders in one left-to-right pass.
pub fn substitute_raw_placeholders(
    text: &str,
    values: &BTreeMap<String, String>,
) -> String {
    if values.is_empty() {
        return text.to_string();
    }

    let document = DocumentSnapshot::new(text);
    let placeholders = scan_placeholder_spans(&document);
    let mut output = String::with_capacity(text.len());
    let mut copied_through = 0;

    for placeholder in placeholders {
        if !placeholder.span.raw {
            continue;
        }
        let Some(value) = values.get(&placeholder.span.text) else {
            continue;
        };
        output.push_str(&text[copied_through..placeholder.opening_byte]);
        output.push_str(value);
        copied_through = placeholder.closing_byte_exclusive;
    }
    output.push_str(&text[copied_through..]);
    output
}

/// Convert placeholder labels into deterministic input argument names.
pub fn placeholder_input_names(texts: Vec<String>) -> Vec<String> {
    let mut used = HashSet::<String>::new();
    let mut names = Vec::with_capacity(texts.len());

    for text in texts {
        let mut slug = String::new();
        let mut separating = false;
        for ch in text.chars().flat_map(char::to_lowercase) {
            if ch.is_alphanumeric() {
                if separating && !slug.is_empty() {
                    slug.push('_');
                }
                slug.push(ch);
                separating = false;
            } else {
                separating = true;
            }
        }

        slug = slug.chars().take(40).collect();
        while slug.ends_with('_') {
            slug.pop();
        }
        if slug.is_empty() {
            slug.push_str("arg");
        } else if slug.chars().next().is_some_and(char::is_numeric) {
            slug = format!("arg_{slug}");
        }

        let base = slug.clone();
        let mut suffix = 2;
        while used.contains(&slug) {
            slug = format!("{base}_{suffix}");
            suffix += 1;
        }
        used.insert(slug.clone());
        names.push(slug);
    }

    names
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
/// candidate found in the document itself. Live prompt spans are ranked in
/// document order before literal-zone spans, which are also ranked in document
/// order. Prompt-local candidates always precede common ones, and an empty
/// `common` slice reproduces the document-only behaviour exactly.
pub fn build_placeholder_completion_candidates(
    document: &DocumentSnapshot,
    position: EditorPosition,
    common: &[String],
) -> Option<PlaceholderCompletion> {
    let context = detect_placeholder_context_at_position(document, position)?;
    let prefix_lower = context.prefix.to_lowercase();
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();

    let placeholders = scan_placeholder_spans(document);
    for raw in [true, false] {
        for placeholder in &placeholders {
            if placeholder.opening_byte == context.opening_byte
                || placeholder.span.raw != raw
            {
                continue;
            }
            let text = &placeholder.span.text;
            if text.to_lowercase().starts_with(&prefix_lower)
                && seen.insert(text.clone())
            {
                candidates.push(PlaceholderCandidate {
                    text: text.clone(),
                    source: PlaceholderCandidateSource::Prompt,
                });
            }
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
    let literal_ranges = normalized_literal_ranges(document.text());

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
                    let closing_byte_exclusive = closing_byte + 1;
                    let Some(range) = document.byte_range_to_range(
                        opening_byte,
                        closing_byte_exclusive,
                    ) else {
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
                            raw: !literal_ranges.iter().any(
                                |(literal_start, literal_end)| {
                                    opening_byte < *literal_end
                                        && *literal_start
                                            < closing_byte_exclusive
                                },
                            ),
                            range,
                            inner_range,
                        },
                        opening_byte,
                        closing_byte_exclusive,
                    });
                }
                _ => {}
            }
        }
    }

    placeholders
}

fn normalized_literal_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges: Vec<(usize, usize)> =
        crate::prompt_literal_zone_ranges(text)
            .into_iter()
            .filter_map(|(start, end)| {
                let end = end.min(text.len());
                (start < end).then_some((start, end))
            })
            .collect();
    ranges.sort_unstable();

    let mut normalized = Vec::<(usize, usize)>::new();
    for (start, end) in ranges {
        if let Some((_, prior_end)) = normalized.last_mut() {
            if start <= *prior_end {
                *prior_end = (*prior_end).max(end);
                continue;
            }
        }
        normalized.push((start, end));
    }
    normalized
}

fn placeholder_context(
    text: &str,
    opening_byte: usize,
    closing_byte_exclusive: usize,
    context_width: usize,
) -> String {
    let line_start = text[..opening_byte].rfind('\n').map_or(0, |i| i + 1);
    let line_end = text[closing_byte_exclusive..]
        .find('\n')
        .map_or(text.len(), |i| closing_byte_exclusive + i);
    let line = &text[line_start..line_end];
    let leading_bytes = line.len() - line.trim_start().len();
    let trimmed = line.trim();
    let placeholder_start =
        opening_byte.saturating_sub(line_start + leading_bytes);
    let placeholder_end =
        closing_byte_exclusive.saturating_sub(line_start + leading_bytes);

    let before: Vec<char> = trimmed[..placeholder_start].chars().collect();
    let placeholder: Vec<char> = trimmed[placeholder_start..placeholder_end]
        .chars()
        .collect();
    let after: Vec<char> = trimmed[placeholder_end..].chars().collect();
    let total = before.len() + placeholder.len() + after.len();
    if total <= context_width {
        return trimmed.to_string();
    }
    if context_width <= placeholder.len() {
        return placeholder.into_iter().collect();
    }

    let mut left_ellipsized = false;
    let mut right_ellipsized = false;
    let mut left_take = 0;
    let mut right_take = 0;
    for _ in 0..3 {
        let marker_width =
            usize::from(left_ellipsized) + usize::from(right_ellipsized);
        let available =
            context_width.saturating_sub(placeholder.len() + marker_width);
        left_take = before.len().min(available / 2);
        right_take = after.len().min(available - left_take);

        let remaining = available - left_take - right_take;
        let extra_left = (before.len() - left_take).min(remaining);
        left_take += extra_left;
        let remaining = remaining - extra_left;
        right_take += (after.len() - right_take).min(remaining);

        let next_left_ellipsized = left_take < before.len();
        let next_right_ellipsized = right_take < after.len();
        if next_left_ellipsized == left_ellipsized
            && next_right_ellipsized == right_ellipsized
        {
            break;
        }
        left_ellipsized = next_left_ellipsized;
        right_ellipsized = next_right_ellipsized;
    }

    let mut context = String::new();
    if left_ellipsized {
        context.push('…');
    }
    context.extend(before[before.len() - left_take..].iter());
    context.extend(placeholder);
    context.extend(after[..right_take].iter());
    if right_ellipsized {
        context.push('…');
    }
    context
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
            spans.iter().map(|span| span.raw).collect::<Vec<_>>(),
            vec![false, false, true]
        );
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
    fn summarizes_exact_raw_fields_with_bounded_context() {
        let text = concat!(
            "  a long prefix before <Foo> and a long suffix after it  \n",
            "`<Foo>` then <foo> and <Foo>"
        );
        let fields = raw_placeholder_fields(text, 40);

        assert_eq!(
            fields
                .iter()
                .map(|field| field.text.as_str())
                .collect::<Vec<_>>(),
            vec!["Foo", "foo"]
        );
        assert_eq!(fields[0].occurrences, 2);
        assert_eq!(fields[1].occurrences, 1);
        assert!(fields[0].context.contains("<Foo>"));
        assert!(fields[0].context.starts_with('…'));
        assert!(fields[0].context.ends_with('…'));
        assert!(fields[0].context.chars().count() <= 40);
        assert_eq!(fields[1].context, "`<Foo>` then <foo> and <Foo>");
    }

    #[test]
    fn substitutes_only_mapped_raw_spans_without_rescanning_values() {
        let text = "🙂 raw <x> and `<x>`\n```\n<x>\n```\n<other> then <x>";
        let values = BTreeMap::from([
            ("x".to_string(), "<y>".to_string()),
            ("y".to_string(), "rescanned".to_string()),
        ]);

        assert_eq!(
            substitute_raw_placeholders(text, &values),
            "🙂 raw <y> and `<x>`\n```\n<x>\n```\n<other> then <y>"
        );
        assert_eq!(substitute_raw_placeholders(text, &BTreeMap::new()), text);
    }

    #[test]
    fn slugs_unicode_names_and_resolves_collisions_in_input_order() {
        let names = placeholder_input_names(
            [
                "the plan", "the-plan", "PR #", "2fa code", "???", "código",
                "foo_2", "foo", "foo",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        );

        assert_eq!(
            names,
            vec![
                "the_plan",
                "the_plan_2",
                "pr",
                "arg_2fa_code",
                "arg",
                "código",
                "foo_2",
                "foo",
                "foo_3",
            ]
        );
        assert_eq!(
            placeholder_input_names(vec!["word ".repeat(20)]),
            vec!["word_word_word_word_word_word_word_word"]
        );
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
    fn completion_candidates_rank_literal_spans_after_live_spans() {
        let (document, cursor) = marked_document(
            "`<alpha>`\n```\n<alpine>\n```\n<apricot> use <a<CURSOR>>",
        );
        let completion =
            build_placeholder_completion_candidates(&document, cursor, &[])
                .unwrap();

        assert_eq!(texts(&completion), vec!["apricot", "alpha", "alpine"]);
        assert!(completion
            .candidates
            .iter()
            .all(|candidate| candidate.source
                == PlaceholderCandidateSource::Prompt));
    }

    #[test]
    fn live_candidate_dedups_literal_and_common_occurrences_at_live_position() {
        let (document, cursor) =
            marked_document("`<alpha>` <anchor> <alpha> use <a<CURSOR>>");
        let completion = build_placeholder_completion_candidates(
            &document,
            cursor,
            &common(&["alpha"]),
        )
        .unwrap();

        assert_eq!(texts(&completion), vec!["anchor", "alpha"]);
        assert_eq!(
            sources(&completion),
            vec![
                PlaceholderCandidateSource::Prompt,
                PlaceholderCandidateSource::Prompt,
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
