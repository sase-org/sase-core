use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetExpansionPlan {
    pub text: String,
    pub tabstop_offsets: Vec<usize>,
}

pub fn plan_snippet_expansion(
    template: &str,
    line_indent: &str,
    indent_continuation_lines: bool,
) -> SnippetExpansionPlan {
    let mut markers = Vec::<(usize, usize)>::new();
    let mut cleaned_parts = Vec::<String>::new();
    let mut last_end = 0usize;
    let mut cleaned_offset = 0usize;
    let mut seen = BTreeSet::<usize>::new();

    for (marker_start, marker_end, number) in iter_unescaped_tabstops(template)
    {
        let before =
            unescape_literal_dollars(&template[last_end..marker_start]);
        cleaned_offset += before.chars().count();
        cleaned_parts.push(before);
        if seen.insert(number) {
            markers.push((number, cleaned_offset));
        }
        last_end = marker_end;
    }

    cleaned_parts.push(unescape_literal_dollars(&template[last_end..]));
    let mut text = cleaned_parts.concat();

    if indent_continuation_lines
        && !line_indent.is_empty()
        && text.contains('\n')
    {
        let pre_indent = text;
        text = indent_continuation_lines_to(&pre_indent, line_indent);
        let indent_len = line_indent.chars().count();
        markers = markers
            .into_iter()
            .map(|(number, offset)| {
                (
                    number,
                    offset
                        + newlines_before_char_offset(&pre_indent, offset)
                            * indent_len,
                )
            })
            .collect();
    }

    if markers.is_empty() {
        return SnippetExpansionPlan {
            text,
            tabstop_offsets: Vec::new(),
        };
    }

    if !seen.contains(&0) {
        markers.push((0, text.chars().count()));
    }

    markers.sort_by_key(|(number, _)| (*number == 0, *number));

    SnippetExpansionPlan {
        text,
        tabstop_offsets: markers
            .into_iter()
            .map(|(_, offset)| offset)
            .collect(),
    }
}

pub(crate) fn iter_unescaped_tabstops(
    text: &str,
) -> Vec<(usize, usize, usize)> {
    let mut tabstops = Vec::new();
    let bytes = text.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'$' || is_escaped(text, cursor) {
            cursor += 1;
            continue;
        }
        let digit_start = cursor + 1;
        let mut digit_end = digit_start;
        while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
            digit_end += 1;
        }
        if digit_end == digit_start {
            cursor += 1;
            continue;
        }
        if let Ok(number) = text[digit_start..digit_end].parse::<usize>() {
            tabstops.push((cursor, digit_end, number));
        }
        cursor = digit_end;
    }
    tabstops
}

fn unescape_literal_dollars(text: &str) -> String {
    text.replace("\\$", "$")
}

fn indent_continuation_lines_to(text: &str, indent: &str) -> String {
    let mut lines = text.split('\n');
    let mut rendered = lines.next().unwrap_or_default().to_string();
    for line in lines {
        rendered.push('\n');
        rendered.push_str(indent);
        rendered.push_str(line);
    }
    rendered
}

fn newlines_before_char_offset(text: &str, offset: usize) -> usize {
    text.chars()
        .take(offset)
        .filter(|character| *character == '\n')
        .count()
}

fn is_escaped(text: &str, index: usize) -> bool {
    let mut backslashes = 0usize;
    let mut cursor = index;
    let bytes = text.as_bytes();
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        backslashes += 1;
        cursor -= 1;
    }
    backslashes % 2 == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(template: &str) -> SnippetExpansionPlan {
        plan_snippet_expansion(template, "", true)
    }

    #[test]
    fn escaped_dollars_are_literal_and_not_tabstops() {
        let planned = plan(r"\$1 before $1 after \$0 then $0");

        assert_eq!(planned.text, "$1 before  after $0 then ");
        assert_eq!(planned.tabstop_offsets, vec![10, 25]);
    }

    #[test]
    fn repeated_tabstop_numbers_only_create_one_stop() {
        let planned = plan("$1 a $1 b $2 $0");

        assert_eq!(planned.text, " a  b  ");
        assert_eq!(planned.tabstop_offsets, vec![0, 6, 7]);
    }

    #[test]
    fn missing_zero_appends_implicit_final_stop() {
        let planned = plan("a $2 b $1 c");

        assert_eq!(planned.text, "a  b  c");
        assert_eq!(planned.tabstop_offsets, vec![5, 2, 7]);
    }

    #[test]
    fn templates_without_markers_have_no_stops() {
        let planned = plan(r"cost \$5 and $x");

        assert_eq!(planned.text, "cost $5 and $x");
        assert!(planned.tabstop_offsets.is_empty());
    }

    #[test]
    fn multi_digit_tabstops_sort_by_number() {
        let planned = plan("$10 ten $2 two $0");

        assert_eq!(planned.text, " ten  two ");
        assert_eq!(planned.tabstop_offsets, vec![5, 0, 10]);
    }

    #[test]
    fn continuation_lines_are_indented_and_offsets_shift() {
        let planned = plan_snippet_expansion("a\n$1\nb $2", "  ", true);

        assert_eq!(planned.text, "a\n  \n  b ");
        assert_eq!(planned.tabstop_offsets, vec![4, 9, 9]);
    }

    #[test]
    fn continuation_line_indentation_can_be_disabled() {
        let planned = plan_snippet_expansion("a\n$1\nb $2", "  ", false);

        assert_eq!(planned.text, "a\n\nb ");
        assert_eq!(planned.tabstop_offsets, vec![2, 5, 5]);
    }

    #[test]
    fn offsets_are_character_offsets_not_byte_offsets() {
        let planned = plan("α $1 β $0");

        assert_eq!(planned.text, "α  β ");
        assert_eq!(planned.tabstop_offsets, vec![2, 5]);
    }
}
