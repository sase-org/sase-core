//! `#{name}` snippet variable substitution shared by the TUI and the LSP.
//!
//! Templates keep `#{project}` literal through catalog composition (the
//! composer only scans `#[` calls) and each surface substitutes it at
//! expansion time from state it already has. Only names present in
//! `variables` are replaced; everything else passes through verbatim.

use std::collections::BTreeMap;

/// The snippet variable naming the prompt's target project.
pub const PROJECT_SNIPPET_VARIABLE: &str = "project";

/// Substitute known `#{name}` variables in a snippet template.
///
/// A token is `#{<name>}` where `<name>` matches
/// `[A-Za-z_][A-Za-z0-9_]*`, with no whitespace and no left-boundary
/// requirement, so `foo/#{project}/` works. Only names present in
/// `variables` are replaced; `$` in an inserted value is escaped as `\$`
/// (mirroring `escape_snippet_arg` in `snippet_catalog`) so the tabstop
/// scanner treats values literally. Everything else — unknown names,
/// malformed tokens (`#{`, `#{}`, `#{a b}`), and `#[...]` calls — is left
/// untouched. The input is returned unchanged when the map is empty or
/// the template has no `#{`.
pub fn substitute_snippet_variables(
    template: &str,
    variables: &BTreeMap<String, String>,
) -> String {
    if variables.is_empty() || !template.contains("#{") {
        return template.to_string();
    }
    let bytes = template.as_bytes();
    let mut rendered = String::with_capacity(template.len());
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] == b'#' && bytes.get(cursor + 1) == Some(&b'{') {
            if let Some((name, token_end)) =
                scan_variable_token(&bytes[cursor + 2..])
            {
                if let Some(value) = variables.get(name) {
                    rendered.push_str(&value.replace('$', "\\$"));
                } else {
                    rendered.push_str(&template[cursor..cursor + token_end]);
                }
                cursor += token_end;
                continue;
            }
        }
        // ASCII delimiters are always char boundaries, so advancing the
        // byte cursor past one byte here never splits a code point; the
        // slice pushes below re-emit whole chars.
        let next = template[cursor..]
            .char_indices()
            .nth(1)
            .map(|(offset, _)| cursor + offset)
            .unwrap_or(bytes.len());
        rendered.push_str(&template[cursor..next]);
        cursor = next;
    }
    rendered
}

/// Parse `name}` at the start of `text` (the bytes after `#{`).
/// Returns the name and the byte offset past `}` relative to the `#`.
fn scan_variable_token(text: &[u8]) -> Option<(&str, usize)> {
    let mut end = 0usize;
    while end < text.len()
        && (text[end].is_ascii_alphanumeric() || text[end] == b'_')
    {
        end += 1;
    }
    if end == 0
        || (!text[0].is_ascii_alphabetic() && text[0] != b'_')
        || text.get(end) != Some(&b'}')
    {
        return None;
    }
    // Names are ASCII-only, so this slicing is always on char boundaries.
    let name = std::str::from_utf8(&text[..end]).ok()?;
    Some((name, end + 3))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variables(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn known_variable_is_substituted() {
        let rendered = substitute_snippet_variables(
            "the #{project} epic bead",
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, "the sase epic bead");
    }

    #[test]
    fn unknown_variable_is_left_verbatim() {
        let rendered = substitute_snippet_variables(
            r##"the #{project} "#{x}" bead"##,
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, r##"the sase "#{x}" bead"##);
    }

    #[test]
    fn empty_map_returns_the_template_unchanged() {
        let template = "the #{project}-$1 epic bead";

        assert_eq!(
            substitute_snippet_variables(template, &BTreeMap::new()),
            template
        );
    }

    #[test]
    fn template_without_tokens_returns_unchanged() {
        let template = "the sase-$1 epic bead";

        assert_eq!(
            substitute_snippet_variables(
                template,
                &variables(&[("project", "sase")])
            ),
            template
        );
    }

    #[test]
    fn multiple_occurrences_are_all_substituted() {
        let rendered = substitute_snippet_variables(
            "#{project}/#{project}",
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, "sase/sase");
    }

    #[test]
    fn variable_adjacent_to_a_tabstop_keeps_the_tabstop() {
        let rendered = substitute_snippet_variables(
            "the #{project}-$1 epic bead",
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, "the sase-$1 epic bead");
    }

    #[test]
    fn dollars_in_values_are_escaped_for_the_tabstop_scanner() {
        let rendered = substitute_snippet_variables(
            "the #{project} bead",
            &variables(&[("project", "a$1b")]),
        );

        assert_eq!(rendered, r"the a\$1b bead");
    }

    #[test]
    fn malformed_tokens_are_left_untouched() {
        for template in ["#{", "#{}", "#{a b}", "# {project}", "##{project}"] {
            // `##{project}` has no left-boundary requirement: the second
            // `#` still opens a token.
            let expected = if template == "##{project}" {
                "#sase"
            } else {
                template
            };
            assert_eq!(
                substitute_snippet_variables(
                    template,
                    &variables(&[("project", "sase")])
                ),
                expected,
                "template {template:?}"
            );
        }
    }

    #[test]
    fn utf8_text_around_tokens_is_preserved() {
        let rendered = substitute_snippet_variables(
            "α #{project} β",
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, "α sase β");
    }

    #[test]
    fn snippet_calls_are_untouched() {
        let rendered = substitute_snippet_variables(
            "#[epic] the #{project}-$1 bead",
            &variables(&[("project", "sase")]),
        );

        assert_eq!(rendered, "#[epic] the sase-$1 bead");
    }

    #[test]
    fn names_must_start_with_a_letter_or_underscore() {
        let rendered = substitute_snippet_variables(
            "#{1x} #{_ok}",
            &variables(&[("1x", "bad"), ("_ok", "good")]),
        );

        assert_eq!(rendered, "#{1x} good");
    }
}
