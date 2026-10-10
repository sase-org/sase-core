use super::directive::{
    directive_argument_open_colon_at, directive_argument_open_double_colon_at,
};
use super::exclusion::{
    excluded_literal_and_definition_ranges, position_in_ranges,
};
use super::macro_args::{
    macro_argument_list_open_paren_for_close, macro_argument_open_colon_at,
};
use super::token::{is_macro_like_token, DocumentSnapshot};
use super::wire::{EditorPosition, EditorRange, EditorTextEdit};
use serde::{Deserialize, Serialize};

/// Plan deleting an invocation argument colon before a just-typed `(`.
///
/// The caller supplies the pre-insertion document plus the caret position
/// immediately after the candidate colon. The returned edit deletes only that
/// colon; pairing, insertion of `(`, and cursor placement remain frontend
/// concerns.
pub fn plan_argument_colon_to_parentheses_edit(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<EditorTextEdit> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let colon_idx = cursor.checked_sub(1)?;
    if text.as_bytes().get(colon_idx) != Some(&b':') {
        return None;
    }
    if text.as_bytes().get(colon_idx.checked_sub(1)?) == Some(&b':')
        || text.as_bytes().get(colon_idx + 1) == Some(&b':')
    {
        return None;
    }
    if position_in_ranges(
        colon_idx,
        &excluded_literal_and_definition_ranges(text),
    ) {
        return None;
    }
    if !(macro_argument_open_colon_at(text, colon_idx)
        || directive_argument_open_colon_at(text, colon_idx))
    {
        return None;
    }
    Some(EditorTextEdit {
        range: document.byte_range_to_range(colon_idx, colon_idx + 1)?,
        new_text: String::new(),
    })
}

/// Plan moving an invocation double-colon text delimiter after `()`.
///
/// The caller supplies the pre-insertion document plus the caret position
/// where `(` is about to be typed. If the caret sits after an invocation's
/// `::` and zero or more ASCII spaces, the returned edit replaces that
/// delimiter span with `()` followed by the original `::` and spaces.
pub fn plan_argument_double_colon_to_parentheses_edit(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<EditorTextEdit> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut delimiter_end = cursor;
    while delimiter_end > 0
        && text.as_bytes().get(delimiter_end - 1) == Some(&b' ')
    {
        delimiter_end -= 1;
    }
    let first_colon = delimiter_end.checked_sub(2)?;
    if text.as_bytes().get(first_colon..delimiter_end) != Some(b"::") {
        return None;
    }
    if first_colon > 0 && text.as_bytes().get(first_colon - 1) == Some(&b':') {
        return None;
    }
    if text.as_bytes().get(delimiter_end) == Some(&b':') {
        return None;
    }
    if position_in_ranges(
        first_colon,
        &excluded_literal_and_definition_ranges(text),
    ) {
        return None;
    }
    if !(macro_argument_open_colon_at(text, first_colon)
        || directive_argument_open_double_colon_at(text, first_colon))
    {
        return None;
    }
    let delimiter = text.get(first_colon..cursor)?;
    Some(EditorTextEdit {
        range: document.byte_range_to_range(first_colon, cursor)?,
        new_text: format!("(){delimiter}"),
    })
}

/// Plan reopening a macro's closed parenthesized argument list before `(`.
///
/// The caller supplies the pre-insertion document plus the caret position
/// where `(` is about to be typed. The returned edit's range ends at the
/// list's closing `)`; frontends place the caret at
/// `range.start + new_text.len()` (immediately before that `)`), and the
/// typed `(` is never inserted.
pub fn plan_argument_list_continuation_edit(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<EditorTextEdit> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let bytes = text.as_bytes();
    let mut delimiter_end = cursor;
    while delimiter_end > 0 && bytes.get(delimiter_end - 1) == Some(&b' ') {
        delimiter_end -= 1;
    }

    let close_idx = if delimiter_end >= 2
        && bytes.get(delimiter_end - 2..delimiter_end) == Some(b"::")
    {
        if bytes.get(delimiter_end) == Some(&b':') {
            return None;
        }
        delimiter_end.checked_sub(3)?
    } else if delimiter_end == cursor {
        cursor.checked_sub(1)?
    } else {
        return None;
    };
    if bytes.get(close_idx) != Some(&b')') {
        return None;
    }
    let open_idx = macro_argument_list_open_paren_for_close(text, close_idx)?;
    let excluded = excluded_literal_and_definition_ranges(text);
    if position_in_ranges(open_idx, &excluded)
        || position_in_ranges(close_idx, &excluded)
    {
        return None;
    }

    let mut content_end = close_idx;
    while content_end > open_idx + 1
        && bytes
            .get(content_end - 1)
            .is_some_and(u8::is_ascii_whitespace)
    {
        content_end -= 1;
    }
    if content_end == open_idx + 1 || bytes.get(content_end - 1) == Some(&b',')
    {
        return Some(EditorTextEdit {
            range: document.byte_range_to_range(close_idx, close_idx)?,
            new_text: String::new(),
        });
    }

    let trailing_whitespace = text.get(content_end..close_idx)?;
    Some(EditorTextEdit {
        range: document.byte_range_to_range(content_end, close_idx)?,
        new_text: format!(",{trailing_whitespace}"),
    })
}

/// Accepted completion spacer record for consuming a macro space before `(`.
///
/// Frontends establish acceptance (manual completion, soft completion, or
/// selector insertion); core validates the exact reference, single ASCII
/// space, adjacency, bounds, input eligibility, and excluded literal/definition
/// regions. Cached acceptance metadata only: no catalog, filesystem, or
/// provider work on the typing path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroCompletionSpacerWire {
    /// Exact insertion recorded at acceptance (for example `#optional`).
    pub reference_text: String,
    /// Document position of the first reference character at acceptance.
    pub reference_start: EditorPosition,
    /// Document position of the owned trailing space at acceptance.
    pub spacer_start: EditorPosition,
    /// True when the accepted entry has at least one input.
    pub has_optional_inputs: bool,
}

/// Plan deleting a completion-owned macro space before a just-typed `(`.
///
/// The caller supplies the pre-insertion document (still containing the owned
/// space), the caret position immediately after that space, and the typed
/// acceptance record. The returned edit deletes only the spacer; parentheses
/// and caret placement remain frontend concerns. This is a completion edit,
/// not a parser change that permits whitespace between an invocation and its
/// argument list.
pub fn plan_macro_completion_spacer_to_parentheses_edit(
    document: &DocumentSnapshot,
    position: EditorPosition,
    record: &MacroCompletionSpacerWire,
) -> Option<EditorTextEdit> {
    if !record.has_optional_inputs {
        return None;
    }
    if !is_macro_like_token(&record.reference_text) {
        return None;
    }
    if !record.reference_text.starts_with('#') {
        return None;
    }
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let reference_byte =
        document.position_to_byte_offset(record.reference_start)?;
    let spacer_byte = document.position_to_byte_offset(record.spacer_start)?;
    let reference_len = record.reference_text.len();
    if reference_len == 0 || !text.is_char_boundary(reference_byte) {
        return None;
    }
    let reference_end = reference_byte.checked_add(reference_len)?;
    if spacer_byte != reference_end {
        return None;
    }
    if text.get(reference_byte..reference_end)
        != Some(record.reference_text.as_str())
    {
        return None;
    }
    if text.as_bytes().get(spacer_byte) != Some(&b' ') {
        return None;
    }
    if cursor != spacer_byte.checked_add(1)? {
        return None;
    }
    if spacer_byte.checked_add(1)? > text.len() {
        return None;
    }
    if !text.is_char_boundary(spacer_byte + 1) {
        return None;
    }
    if position_in_ranges(
        spacer_byte,
        &excluded_literal_and_definition_ranges(text),
    ) {
        return None;
    }
    Some(EditorTextEdit {
        range: document.byte_range_to_range(spacer_byte, spacer_byte + 1)?,
        new_text: String::new(),
    })
}

/// Remove one owned ASCII space at `spacer_byte` for transition views.
///
/// Returns the normalized document text with exactly that byte removed, or
/// `None` when the byte is not a single ASCII space or not on a char
/// boundary.
pub fn normalize_macro_spacer_transition(
    text: &str,
    spacer_byte: usize,
) -> Option<String> {
    if text.as_bytes().get(spacer_byte) != Some(&b' ') {
        return None;
    }
    if !text.is_char_boundary(spacer_byte)
        || !text.is_char_boundary(spacer_byte + 1)
    {
        return None;
    }
    let mut normalized = String::with_capacity(text.len().saturating_sub(1));
    normalized.push_str(text.get(..spacer_byte)?);
    normalized.push_str(text.get(spacer_byte + 1..)?);
    Some(normalized)
}

/// Map a normalized byte offset back to the actual document.
///
/// The normalized document removed the single owned space at `spacer_byte`,
/// so offsets before it are unchanged and offsets at or after it shift by one.
pub fn map_normalized_byte_offset_to_actual(
    normalized_byte: usize,
    spacer_byte: usize,
) -> usize {
    if normalized_byte < spacer_byte {
        normalized_byte
    } else {
        normalized_byte + 1
    }
}

/// Map a normalized UTF-16 range back to actual-document coordinates.
///
/// Converts the normalized range to byte offsets in `normalized_doc`, shifts
/// each endpoint across the removed spacer, then converts to positions in
/// `actual_doc`. Returns `None` when any conversion fails.
pub fn map_normalized_range_to_actual(
    normalized_doc: &DocumentSnapshot,
    actual_doc: &DocumentSnapshot,
    spacer_byte: usize,
    range: EditorRange,
) -> Option<EditorRange> {
    let normalized_start =
        normalized_doc.position_to_byte_offset(range.start)?;
    let normalized_end = normalized_doc.position_to_byte_offset(range.end)?;
    if normalized_end < normalized_start {
        return None;
    }
    let actual_start =
        map_normalized_byte_offset_to_actual(normalized_start, spacer_byte);
    let actual_end =
        map_normalized_byte_offset_to_actual(normalized_end, spacer_byte);
    if actual_end < actual_start || actual_end > actual_doc.text().len() {
        return None;
    }
    if !actual_doc.text().is_char_boundary(actual_start)
        || !actual_doc.text().is_char_boundary(actual_end)
    {
        return None;
    }
    actual_doc.byte_range_to_range(actual_start, actual_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position_for_cursor(marked: &str) -> (DocumentSnapshot, EditorPosition) {
        let cursor = marked.find("<cursor>").expect("cursor marker");
        let text = marked.replace("<cursor>", "");
        let document = DocumentSnapshot::new(text);
        let position = document
            .byte_offset_to_position(cursor)
            .expect("cursor position");
        (document, position)
    }

    fn applied(marked: &str) -> Option<String> {
        let (document, position) = position_for_cursor(marked);
        let edit =
            plan_argument_colon_to_parentheses_edit(&document, position)?;
        let start = document.position_to_byte_offset(edit.range.start)?;
        let end = document.position_to_byte_offset(edit.range.end)?;
        Some(format!(
            "{}{}{}",
            &document.text()[..start],
            edit.new_text,
            &document.text()[end..]
        ))
    }

    fn applied_double(marked: &str) -> Option<String> {
        let (document, position) = position_for_cursor(marked);
        let edit = plan_argument_double_colon_to_parentheses_edit(
            &document, position,
        )?;
        let start = document.position_to_byte_offset(edit.range.start)?;
        let end = document.position_to_byte_offset(edit.range.end)?;
        Some(format!(
            "{}{}{}",
            &document.text()[..start],
            edit.new_text,
            &document.text()[end..]
        ))
    }

    fn applied_continuation(marked: &str) -> Option<(String, usize)> {
        let (document, position) = position_for_cursor(marked);
        let edit = plan_argument_list_continuation_edit(&document, position)?;
        let start = document.position_to_byte_offset(edit.range.start)?;
        let end = document.position_to_byte_offset(edit.range.end)?;
        let text = format!(
            "{}{}{}",
            &document.text()[..start],
            edit.new_text,
            &document.text()[end..]
        );
        Some((text, start + edit.new_text.len()))
    }

    #[test]
    fn converts_directive_aliases_and_long_names() {
        for source in [
            "Some prompt here. %q:<cursor>",
            "%queue:<cursor>",
            "%w:<cursor>",
            "%wait:<cursor>",
            "%m:<cursor>",
            "%model:<cursor>",
        ] {
            assert_eq!(applied(source), Some(source.replace(":<cursor>", "")));
        }
    }

    #[test]
    fn converts_macro_reference_forms_without_catalog_io() {
        for source in [
            "#foo:<cursor>",
            "#!foo:<cursor>",
            "#ns/foo:<cursor>",
            "#ns__foo:<cursor>",
            "#foo!!:<cursor>",
            "#foo??:<cursor>",
        ] {
            assert_eq!(applied(source), Some(source.replace(":<cursor>", "")));
        }
    }

    #[test]
    fn handles_multiline_and_utf16_positions() {
        let (document, position) =
            position_for_cursor("é🙂\nText #foo:<cursor>");
        let edit = plan_argument_colon_to_parentheses_edit(&document, position)
            .expect("planned edit");
        assert_eq!(edit.range.start.line, 1);
        assert_eq!(edit.range.start.character, 9);
        assert_eq!(edit.range.end.character, 10);
    }

    #[test]
    fn rejects_non_invocation_and_mid_word_colons() {
        for source in [
            "Note:<cursor>",
            "https://example.test/#foo:<cursor>",
            "12:30<cursor>",
            "%unknown:<cursor>",
            "word%q:<cursor>",
            "word;%q:<cursor>",
            "word#foo:<cursor>",
            r"\#foo:<cursor>",
            r"\%q:<cursor>",
            "%xprompts_enabled:<cursor>",
        ] {
            assert_eq!(applied(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_double_colons_and_existing_arguments() {
        for source in [
            "%q::<cursor>",
            "%q:<cursor>:",
            "#foo::<cursor>",
            "#foo:<cursor>:",
            "%q:1<cursor>",
            "%q:1:<cursor>",
            "%q: <cursor>",
            "%q(foo:<cursor>",
        ] {
            assert_eq!(applied(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_literal_frontmatter_and_jinja_regions() {
        for source in [
            "`#foo:<cursor>`",
            "```\n#foo:<cursor>\n```",
            "%xprompts_enabled:false\n#foo:<cursor>\n%xprompts_enabled:true\n",
            "---\nname: #foo:<cursor>\n---\n#foo:",
            "{{ #foo:<cursor> }}",
            "{% set value = '#foo:<cursor>' %}",
        ] {
            assert_eq!(applied(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_invalid_utf16_positions() {
        let document = DocumentSnapshot::new("🙂 #foo:");
        assert_eq!(
            plan_argument_colon_to_parentheses_edit(
                &document,
                EditorPosition {
                    line: 0,
                    character: 1,
                },
            ),
            None
        );
    }

    #[test]
    fn converts_double_colon_text_invocations() {
        for (source, expected) in [
            ("#foo::<cursor>", "#foo()::"),
            ("#foo:: <cursor>", "#foo():: "),
            ("#foo::   <cursor>", "#foo()::   "),
            ("#foo:: <cursor>body", "#foo():: body"),
            ("#foo:: <cursor>  body", "#foo()::   body"),
            ("#foo::<cursor>\nbody", "#foo()::\nbody"),
            ("#!ns/foo:: <cursor>", "#!ns/foo():: "),
            ("#foo!!:: <cursor>", "#foo!!():: "),
            ("#foo??:: <cursor>", "#foo??():: "),
        ] {
            assert_eq!(applied_double(source), Some(expected.to_string()));
        }
    }

    #[test]
    fn converts_supported_double_colon_directives() {
        for (source, expected) in [
            ("%proc:: <cursor>", "%proc():: "),
            ("%if:: <cursor>", "%if():: "),
            ("%clan:: <cursor>", "%clan():: "),
            ("%c:: <cursor>", "%c():: "),
        ] {
            assert_eq!(applied_double(source), Some(expected.to_string()));
        }
    }

    #[test]
    fn double_colon_handles_multiline_and_utf16_positions() {
        let (document, position) =
            position_for_cursor("é🙂\nText #foo:: <cursor>");
        let edit =
            plan_argument_double_colon_to_parentheses_edit(&document, position)
                .expect("planned edit");
        assert_eq!(edit.range.start.line, 1);
        assert_eq!(edit.range.start.character, 9);
        assert_eq!(edit.range.end.character, 12);
        assert_eq!(edit.new_text, "():: ");
    }

    #[test]
    fn rejects_double_colon_ineligible_directives_and_contexts() {
        for source in [
            "%q:: <cursor>",
            "%model:: <cursor>",
            "%xprompts_enabled:: <cursor>",
            "%unknown:: <cursor>",
            "word%clan:: <cursor>",
            "word#foo:: <cursor>",
            r"\#foo:: <cursor>",
            r"\%clan:: <cursor>",
        ] {
            assert_eq!(applied_double(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_double_colon_non_space_gaps_and_malformed_delimiters() {
        for source in [
            "#foo::<cursor>:",
            "#foo:::<cursor>",
            "#foo:::\u{20}<cursor>",
            "#foo:<cursor>:",
            "#foo::\t<cursor>",
            "#foo::\u{00a0}<cursor>",
            "#foo:: body<cursor>",
            "#foo(a):: <cursor>",
            "#foo:arg:: <cursor>",
            "%proc(a):: <cursor>",
        ] {
            assert_eq!(applied_double(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_double_colon_literal_frontmatter_and_jinja_regions() {
        for source in [
            "`#foo:: <cursor>`",
            "```\n#foo:: <cursor>\n```",
            "%xprompts_enabled:false\n#foo:: <cursor>\n%xprompts_enabled:true\n",
            "---\nname: #foo:: <cursor>\n---\n#foo::",
            "{{ #foo:: <cursor> }}",
            "{% set value = '#foo:: <cursor>' %}",
        ] {
            assert_eq!(applied_double(source), None, "{source}");
        }
    }

    #[test]
    fn rejects_double_colon_invalid_utf16_positions() {
        let document = DocumentSnapshot::new("🙂 #foo:: ");
        assert_eq!(
            plan_argument_double_colon_to_parentheses_edit(
                &document,
                EditorPosition {
                    line: 0,
                    character: 1,
                },
            ),
            None
        );
    }

    #[test]
    fn continues_closed_macro_argument_lists() {
        for (source, expected) in [
            ("#foo(bar=1)<cursor>", "#foo(bar=1,<cursor>)"),
            (
                "#foo(bar=1):: <cursor>Some text.",
                "#foo(bar=1,<cursor>):: Some text.",
            ),
            ("#foo(bar=1)::<cursor>", "#foo(bar=1,<cursor>)::"),
            (
                "#foo(bar=1)::   <cursor>body",
                "#foo(bar=1,<cursor>)::   body",
            ),
            ("#foo()<cursor>", "#foo(<cursor>)"),
            ("#foo(bar=1,)<cursor>", "#foo(bar=1,<cursor>)"),
            ("#foo(bar=1, )<cursor>", "#foo(bar=1, <cursor>)"),
            ("#foo(bar=1 )<cursor>", "#foo(bar=1, <cursor>)"),
            ("#foo(\n  bar=1\n)<cursor>", "#foo(\n  bar=1,\n<cursor>)"),
            (
                "#outer(#inner(a=1)<cursor>)",
                "#outer(#inner(a=1,<cursor>))",
            ),
            (
                "#foo(a=\")\", b=[[x)y]])<cursor>",
                "#foo(a=\")\", b=[[x)y]],<cursor>)",
            ),
        ] {
            let cursor = expected.find("<cursor>").expect("cursor marker");
            let expected_text = expected.replace("<cursor>", "");
            assert_eq!(
                applied_continuation(source),
                Some((expected_text, cursor)),
                "{source}"
            );
        }
    }

    #[test]
    fn continuation_handles_utf16_and_multiline_positions() {
        let (document, position) =
            position_for_cursor("é🙂 #foo(bar=1)<cursor>");
        let edit = plan_argument_list_continuation_edit(&document, position)
            .expect("planned edit");
        assert_eq!(edit.range.start.line, 0);
        assert_eq!(edit.range.start.character, 14);
        assert_eq!(edit.range.end.character, 14);
        assert_eq!(edit.new_text, ",");

        let (document, position) =
            position_for_cursor("é🙂\nText #foo(bar=1)<cursor>");
        let edit = plan_argument_list_continuation_edit(&document, position)
            .expect("planned edit");
        assert_eq!(edit.range.start.line, 1);
        assert_eq!(edit.range.start.character, 15);
        assert_eq!(edit.range.end.character, 15);
        assert_eq!(edit.new_text, ",");
    }

    #[test]
    fn rejects_ineligible_continuation_sources_and_positions() {
        for source in [
            "%q(capacity=1)<cursor>",
            "%wait(ready=true)<cursor>",
            "%proc(a)::<cursor>",
            "%(a,b)<cursor>",
            "%alt(a,b)<cursor>",
            "(note)<cursor>",
            "foo(bar)<cursor>",
            r"\#foo(a)<cursor>",
            "word#foo(a)<cursor>",
            "https://x.test/#foo(a)<cursor>",
            "#foo(a) <cursor>",
            "#foo(a):<cursor>",
            "#foo(a): <cursor>",
            "#foo(a)::\t<cursor>",
            "#foo(a)::\u{00a0}<cursor>",
            "#foo(a)::<cursor>:",
            "#foo(a):::<cursor>",
            "#foo(a):: body<cursor>",
            "#foo(a)x<cursor>",
            "#foo(a<cursor>)",
            "#foo(a<cursor>",
            "`#foo(a)<cursor>`",
            "```\n#foo(a)<cursor>\n```",
            "%xprompts_enabled:false\n#foo(a)<cursor>\n%xprompts_enabled:true\n",
            "---\nname: #foo(a)<cursor>\n---\n#foo(a)",
            "{{ #foo(a)<cursor> }}",
            "{% set value = '#foo(a)<cursor>' %}",
        ] {
            assert_eq!(applied_continuation(source), None, "{source}");
        }

        let document = DocumentSnapshot::new("🙂 #foo(a)");
        assert_eq!(
            plan_argument_list_continuation_edit(
                &document,
                EditorPosition {
                    line: 0,
                    character: 1,
                },
            ),
            None
        );

        let (document, position) = position_for_cursor("#foo(a)::<cursor>");
        assert_eq!(
            plan_argument_double_colon_to_parentheses_edit(&document, position),
            None
        );
    }

    fn spacer_record(
        document: &DocumentSnapshot,
        reference_text: &str,
        reference_byte: usize,
        spacer_byte: usize,
        has_optional_inputs: bool,
    ) -> MacroCompletionSpacerWire {
        MacroCompletionSpacerWire {
            reference_text: reference_text.to_string(),
            reference_start: document
                .byte_offset_to_position(reference_byte)
                .expect("reference position"),
            spacer_start: document
                .byte_offset_to_position(spacer_byte)
                .expect("spacer position"),
            has_optional_inputs,
        }
    }

    fn applied_spacer(
        text: &str,
        reference_text: &str,
        reference_byte: usize,
        spacer_byte: usize,
        cursor_byte: usize,
        has_optional_inputs: bool,
    ) -> Option<String> {
        let document = DocumentSnapshot::new(text);
        let position = document
            .byte_offset_to_position(cursor_byte)
            .expect("cursor position");
        let record = spacer_record(
            &document,
            reference_text,
            reference_byte,
            spacer_byte,
            has_optional_inputs,
        );
        let edit = plan_macro_completion_spacer_to_parentheses_edit(
            &document, position, &record,
        )?;
        let start = document.position_to_byte_offset(edit.range.start)?;
        let end = document.position_to_byte_offset(edit.range.end)?;
        assert_eq!(edit.new_text, "");
        assert_eq!(end - start, 1);
        Some(format!(
            "{}{}{}",
            &document.text()[..start],
            edit.new_text,
            &document.text()[end..]
        ))
    }

    #[test]
    fn spacer_consumes_owned_space_for_input_bearing_entries() {
        assert_eq!(
            applied_spacer("#optional ", "#optional", 0, 9, 10, true),
            Some("#optional".to_string())
        );
        assert_eq!(
            applied_spacer("#!foo ", "#!foo", 0, 5, 6, true),
            Some("#!foo".to_string())
        );
        assert_eq!(
            applied_spacer("#ns/foo ", "#ns/foo", 0, 7, 8, true),
            Some("#ns/foo".to_string())
        );
        assert_eq!(
            applied_spacer("(#optional )", "#optional", 1, 10, 11, true),
            Some("(#optional)".to_string())
        );
    }

    #[test]
    fn spacer_rejects_zero_input_and_unowned_records() {
        assert_eq!(applied_spacer("#plain ", "#plain", 0, 6, 7, false), None);
        assert_eq!(applied_spacer("#plain ", "", 0, 6, 7, true), None);
        assert_eq!(applied_spacer("#plain ", "plain", 0, 6, 7, true), None);
        assert_eq!(applied_spacer("#plain ", "#other", 0, 6, 7, true), None);
    }

    #[test]
    fn spacer_rejects_stale_mismatched_and_malformed() {
        assert_eq!(
            applied_spacer("#optionalx ", "#optional", 0, 9, 11, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional  ", "#optional", 0, 9, 11, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional\t", "#optional", 0, 9, 10, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional\u{00a0}", "#optional", 0, 9, 11, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional ", "#optional", 1, 9, 10, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional ", "#optional", 0, 8, 10, true),
            None
        );
        assert_eq!(
            applied_spacer("#optional ", "#optional", 0, 9, 9, true),
            None
        );
    }

    #[test]
    fn spacer_rejects_excluded_regions() {
        for text in [
            "`#optional `",
            "```\n#optional \n```",
            "{{ #optional }}",
            "{% set value = '#optional ' %}",
        ] {
            let reference = text.find("#optional").expect("reference");
            let spacer = reference + "#optional".len();
            assert_eq!(
                text.as_bytes().get(spacer),
                Some(&b' '),
                "owned spacer for {text:?}"
            );
            let cursor = spacer + 1;
            assert_eq!(
                applied_spacer(
                    text,
                    "#optional",
                    reference,
                    spacer,
                    cursor,
                    true
                ),
                None,
                "{text:?}"
            );
        }
    }

    #[test]
    fn spacer_handles_multiline_astral_and_exact_range() {
        let text = "é🙂\nText #optional ";
        let reference = text.find("#optional").expect("reference");
        let spacer = reference + "#optional".len();
        let document = DocumentSnapshot::new(text);
        let cursor = document
            .byte_offset_to_position(spacer + 1)
            .expect("cursor");
        let record =
            spacer_record(&document, "#optional", reference, spacer, true);
        let edit = plan_macro_completion_spacer_to_parentheses_edit(
            &document, cursor, &record,
        )
        .expect("planned edit");
        assert_eq!(edit.range.start.line, 1);
        assert_eq!(edit.new_text, "");
        let start = document
            .position_to_byte_offset(edit.range.start)
            .expect("start");
        let end = document
            .position_to_byte_offset(edit.range.end)
            .expect("end");
        assert_eq!((start, end), (spacer, spacer + 1));
        assert_eq!(&text[..start], &text[..spacer]);
    }

    #[test]
    fn spacer_rejects_invalid_utf16_positions() {
        let document = DocumentSnapshot::new("🙂 #optional ");
        let record = MacroCompletionSpacerWire {
            reference_text: "#optional".to_string(),
            reference_start: EditorPosition {
                line: 0,
                character: 1,
            },
            spacer_start: document.byte_offset_to_position(12).expect("spacer"),
            has_optional_inputs: true,
        };
        assert_eq!(
            plan_macro_completion_spacer_to_parentheses_edit(
                &document,
                EditorPosition {
                    line: 0,
                    character: 13,
                },
                &record
            ),
            None
        );
    }

    #[test]
    fn spacer_normalization_maps_ranges_back() {
        let actual = "#optional (suffix)";
        let spacer = actual.find(' ').expect("spacer");
        let normalized = normalize_macro_spacer_transition(actual, spacer)
            .expect("normalized");
        assert_eq!(normalized, "#optional(suffix)");
        let actual_doc = DocumentSnapshot::new(actual);
        let normalized_doc = DocumentSnapshot::new(&normalized);
        let range = normalized_doc
            .byte_range_to_range(0, normalized.len())
            .expect("range");
        let mapped = map_normalized_range_to_actual(
            &normalized_doc,
            &actual_doc,
            spacer,
            range,
        )
        .expect("mapped");
        let start = actual_doc
            .position_to_byte_offset(mapped.start)
            .expect("start");
        let end = actual_doc.position_to_byte_offset(mapped.end).expect("end");
        assert_eq!((start, end), (0, actual.len()));
        assert_eq!(
            map_normalized_byte_offset_to_actual(spacer, spacer),
            spacer + 1
        );
        assert_eq!(
            map_normalized_byte_offset_to_actual(spacer - 1, spacer),
            spacer - 1
        );
        assert_eq!(normalize_macro_spacer_transition(actual, 0), None);
    }
}
