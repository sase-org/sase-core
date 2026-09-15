use super::directive::{
    directive_argument_open_colon_at, directive_argument_open_double_colon_at,
};
use super::exclusion::{
    excluded_literal_and_definition_ranges, position_in_ranges,
};
use super::token::DocumentSnapshot;
use super::wire::{EditorPosition, EditorTextEdit};
use super::xprompt_args::xprompt_argument_open_colon_at;

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
    if !(xprompt_argument_open_colon_at(text, colon_idx)
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
    if !(xprompt_argument_open_colon_at(text, first_colon)
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
    fn converts_xprompt_reference_forms_without_catalog_io() {
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
            "%if:: <cursor>",
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
}
