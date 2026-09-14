use super::directive::directive_argument_open_colon_at;
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
}
