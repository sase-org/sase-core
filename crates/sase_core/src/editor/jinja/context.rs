//! Completion-slot classification for the identifier token at the cursor.
//!
//! The token is `[A-Za-z0-9_]`; `prefix` is the part before the cursor and
//! the replacement range covers the whole token. `None` (the function
//! returning `None`) means the cursor is outside any tag or inside an inert
//! region; [`JinjaSlot::None`] means the cursor is claimed by Jinja but gets
//! no menu (string literals, digit-led tokens, new-name positions).

use super::scan::{
    char_width, first_word, floor_char_boundary, is_ident_char, is_ident_start,
    jinja_tag_at_cursor, string_literal_ranges, JinjaTagKind,
};

/// Which completion list the cursor position calls for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JinjaSlot {
    /// Any other position: variables, locals, inputs, builtins, globals.
    Variable,
    /// The token follows `<dotted.path>.`.
    Member,
    /// The token follows `|`, or is the second word of `{% filter`.
    Filter,
    /// The token follows `is` or `is not`.
    Test,
    /// The token is the first word of a `{% %}` tag.
    Statement,
    /// Claimed by Jinja with no menu: strings, digit-led tokens, new names.
    None,
}

/// Slot context for the token around the cursor.
///
/// Offsets are UTF-8 bytes into the document; the assist phase converts the
/// token range to an [`crate::editor::wire::EditorRange`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JinjaSlotContext {
    pub slot: JinjaSlot,
    pub namespace: Option<String>,
    pub prefix: String,
    pub token_start: usize,
    pub token_end: usize,
}

/// Classify the completion slot at `cursor`.
///
/// Returns `None` only when the cursor is outside a tag (or inside an inert
/// region); every in-tag position returns `Some`, possibly with
/// [`JinjaSlot::None`].
pub fn jinja_completion_slot(
    text: &str,
    cursor: usize,
) -> Option<JinjaSlotContext> {
    let cursor = floor_char_boundary(text, cursor);
    let tag = jinja_tag_at_cursor(text, cursor)?;
    let content = &text[tag.content_start..tag.content_end];
    let relative = cursor - tag.content_start;

    let (token_start, token_end) = token_bounds(text, cursor);
    let prefix = text[token_start..cursor].to_string();
    let mut context = JinjaSlotContext {
        slot: JinjaSlot::Variable,
        namespace: None,
        prefix,
        token_start,
        token_end,
    };

    if in_string(content, relative) {
        context.slot = JinjaSlot::None;
        return Some(context);
    }
    let token = &text[token_start..token_end];
    if !token.is_empty()
        && token
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit())
    {
        context.slot = JinjaSlot::None;
        return Some(context);
    }

    if tag.kind == JinjaTagKind::Statement
        && is_first_word(content, token_start - tag.content_start)
    {
        context.slot = JinjaSlot::Statement;
        return Some(context);
    }

    if tag.kind == JinjaTagKind::Statement {
        if let Some(slot) = statement_new_name_slot(
            content,
            token_start,
            token_end,
            tag.content_start,
        ) {
            context.slot = slot.slot;
            context.namespace = slot.namespace;
            return Some(context);
        }
    }

    if token_start > 0 && text.as_bytes()[token_start - 1] == b'.' {
        let namespace = dotted_namespace(text, token_start - 1);
        if namespace
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit())
        {
            context.slot = JinjaSlot::None;
        } else {
            context.slot = JinjaSlot::Member;
            context.namespace = if namespace.is_empty() {
                None
            } else {
                Some(namespace)
            };
        }
        return Some(context);
    }

    if previous_non_space(text, token_start) == Some(b'|') {
        context.slot = JinjaSlot::Filter;
        return Some(context);
    }

    if follows_is_test(content, token_start - tag.content_start) {
        context.slot = JinjaSlot::Test;
        return Some(context);
    }

    Some(context)
}

struct StatementSlot {
    slot: JinjaSlot,
    namespace: Option<String>,
}

/// New-name positions and the `{% filter <name>` slot inside statement tags.
///
/// Returns `Some` when the token is decided here (a `None` slot or the
/// filter name); `None` means normal expression flow continues.
fn statement_new_name_slot(
    content: &str,
    token_start: usize,
    token_end: usize,
    content_start: usize,
) -> Option<StatementSlot> {
    let relative_start = token_start - content_start;
    let relative_end = token_end - content_start;
    let (keyword, _, _) = first_word(content)?;
    let none = |slot: JinjaSlot| {
        Some(StatementSlot {
            slot,
            namespace: None,
        })
    };
    match keyword.as_str() {
        "set" => {
            // `{% set <targets> [= ...] %}`: names before the top-level `=`
            // are declarations. A block `{% set x %}` has no `=`.
            match find_top_level_assign(content) {
                Some(equal) if relative_end > equal => None,
                _ => none(JinjaSlot::None),
            }
        }
        "for" => {
            // `{% for <targets> in ... %}`: targets before `in` are new.
            match find_top_level_word(content, "in") {
                Some(into) if relative_end > into => None,
                _ => none(JinjaSlot::None),
            }
        }
        "macro" | "block" => none(JinjaSlot::None),
        "filter" => {
            let words = words_before(content, relative_start);
            if words == ["filter"] {
                none(JinjaSlot::Filter)
            } else {
                None
            }
        }
        "import" | "from" => {
            let words = words_before(content, relative_start);
            if words.last().is_some_and(|word| word == "as") {
                none(JinjaSlot::None)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Byte range of the `[A-Za-z0-9_]` token around `cursor`.
fn token_bounds(text: &str, cursor: usize) -> (usize, usize) {
    let bytes = text.as_bytes();
    let mut start = cursor;
    while start > 0 && bytes.get(start - 1).is_some_and(|b| is_ident_char(*b)) {
        start -= 1;
    }
    let mut end = cursor;
    while end < bytes.len() && is_ident_char(bytes[end]) {
        end += 1;
    }
    (start, end)
}

fn in_string(content: &str, relative: usize) -> bool {
    string_literal_ranges(content)
        .into_iter()
        .any(|(start, end)| start < relative && relative < end)
}

/// Whether the token at `relative_start` is the first word of the tag.
fn is_first_word(content: &str, relative_start: usize) -> bool {
    let mut before = &content[..relative_start];
    before = before.strip_prefix(['-', '+']).unwrap_or(before);
    before.trim().is_empty()
}

/// Dotted path before the `.` ending at `dot` (exclusive), without
/// surrounding dots, e.g. `a.b` for `{{ a.b.c`.
fn dotted_namespace(text: &str, dot: usize) -> String {
    let bytes = text.as_bytes();
    let mut start = dot;
    while start > 0
        && (is_ident_char(bytes[start - 1])
            || bytes[start - 1] == b'.'
            || bytes[start - 1].is_ascii_whitespace())
    {
        start -= 1;
    }
    text[start..dot]
        .chars()
        .filter(|ch| *ch != ' ' && *ch != '\t' && *ch != '\n')
        .collect::<String>()
        .trim_matches('.')
        .to_string()
}

fn previous_non_space(text: &str, token_start: usize) -> Option<u8> {
    text.as_bytes()[..token_start]
        .iter()
        .rev()
        .find(|byte| !byte.is_ascii_whitespace())
        .copied()
}

/// Identifier words before `relative` (exclusive), skipping string literals.
fn words_before(content: &str, relative: usize) -> Vec<String> {
    let head = content.get(..relative).unwrap_or("");
    let strings = string_literal_ranges(head);
    let in_str = |index: usize| {
        strings
            .iter()
            .any(|(start, end)| *start <= index && index < *end)
    };
    let bytes = head.as_bytes();
    let mut words = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if in_str(index) || !is_ident_start(bytes[index]) {
            index += char_width(head, index);
            continue;
        }
        let mut end = index + 1;
        while end < bytes.len() && !in_str(end) && is_ident_char(bytes[end]) {
            end += 1;
        }
        words.push(head[index..end].to_string());
        index = end;
    }
    words
}

/// Whether the token is governed by `is` / `is not` immediately before it.
fn follows_is_test(content: &str, relative_start: usize) -> bool {
    let words = words_before(content, relative_start);
    match words.as_slice() {
        [.., first, second] => {
            second == "is" || (first == "is" && second == "not")
        }
        _ => false,
    }
}

/// Position of the top-level `=` that is not part of `==`, `!=`, `>=`,
/// `<=`, skipping strings and bracketed groups. `None` for block `set`.
pub(crate) fn find_top_level_assign(content: &str) -> Option<usize> {
    let strings = string_literal_ranges(content);
    let bytes = content.as_bytes();
    let mut depth = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        if strings.iter().any(|(s, e)| *s <= index && index < *e) {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b'=' if depth == 0
                && bytes.get(index + 1) != Some(&b'=')
                && !matches!(
                    bytes.get(index.wrapping_sub(1)),
                    Some(b'=') | Some(b'!') | Some(b'<') | Some(b'>')
                ) =>
            {
                return Some(index);
            }
            _ => {}
        }
        index += char_width(content, index);
    }
    None
}

/// Byte offset of the top-level whole word `needle` (e.g. `in`, `import`),
/// skipping strings and bracketed groups.
pub(crate) fn find_top_level_word(
    content: &str,
    needle: &str,
) -> Option<usize> {
    let strings = string_literal_ranges(content);
    let bytes = content.as_bytes();
    let mut depth = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        if strings.iter().any(|(s, e)| *s <= index && index < *e) {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && content[index..].starts_with(needle)
            && (index == 0 || !is_ident_char(bytes[index - 1]))
            && bytes
                .get(index + needle.len())
                .is_none_or(|b| !is_ident_char(*b))
        {
            return Some(index);
        }
        index += char_width(content, index);
    }
    None
}

/// Split `content` at top-level `separator`, skipping strings and bracketed
/// groups. Returns content-relative byte ranges.
pub(crate) fn split_top_level(
    content: &str,
    separator: u8,
) -> Vec<(usize, usize)> {
    let strings = string_literal_ranges(content);
    let bytes = content.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        if strings.iter().any(|(s, e)| *s <= index && index < *e) {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b if b == separator && depth == 0 => {
                parts.push((start, index));
                start = index + 1;
            }
            _ => {}
        }
        index += char_width(content, index);
    }
    parts.push((start, bytes.len()));
    parts
}

/// Leading identifier of `text`, skipping whitespace.
pub(crate) fn first_ident(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    if index < bytes.len() && is_ident_start(bytes[index]) {
        let mut end = index + 1;
        while end < bytes.len() && is_ident_char(bytes[end]) {
            end += 1;
        }
        Some(text[index..end].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_at(text: &str, cursor: usize) -> Option<JinjaSlotContext> {
        jinja_completion_slot(text, cursor)
    }

    fn slot_name(text: &str, cursor: usize) -> Option<JinjaSlot> {
        slot_at(text, cursor).map(|context| context.slot)
    }

    #[test]
    fn whole_slot_matrix() {
        for (text, cursor, slot) in [
            ("{{  }}", 3, JinjaSlot::Variable),
            ("{{ pa }}", 5, JinjaSlot::Variable),
            ("{{ wait. }}", 8, JinjaSlot::Member),
            ("{{ a.b.c }}", 8, JinjaSlot::Member),
            ("{{ x | up }}", 9, JinjaSlot::Filter),
            ("{% filter up %}", 11, JinjaSlot::Filter),
            ("{{ x is }}", 8, JinjaSlot::Test),
            ("{{ x is not }}", 12, JinjaSlot::Test),
            ("{{ history }}", 10, JinjaSlot::Variable),
            ("{%  %}", 3, JinjaSlot::Statement),
            ("{%if x%}", 3, JinjaSlot::Statement),
            ("{{ \"a\" }}", 4, JinjaSlot::None),
            ("{{ 3 }}", 4, JinjaSlot::None),
            ("{% set x = 1 %}", 7, JinjaSlot::None),
            ("{% for x in y %}", 8, JinjaSlot::None),
            ("{% macro m(a) %}", 10, JinjaSlot::None),
            ("{% macro m(a) %}", 12, JinjaSlot::None),
            ("{% block b %}", 9, JinjaSlot::None),
            ("{% import x as y %}", 16, JinjaSlot::None),
            ("%{a | b}", 4, JinjaSlot::Variable),
        ] {
            // `%{a | b}` is alternation, not a tag: the engine stays silent.
            if text == "%{a | b}" {
                assert!(slot_at(text, cursor).is_none(), "{text}");
                continue;
            }
            assert_eq!(slot_name(text, cursor), Some(slot), "{text}");
        }
    }

    #[test]
    fn variable_prefix_and_range_cover_token() {
        let text = "{{ patch_na }}";
        let context = slot_at(text, 11).expect("variable slot");
        assert_eq!(context.slot, JinjaSlot::Variable);
        assert_eq!(context.prefix, "patch_na");
        assert_eq!(&text[context.token_start..context.token_end], "patch_na");
    }

    #[test]
    fn member_namespace_is_full_dotted_path() {
        let text = "{{ a.b.c }}";
        let context = slot_at(text, 8).expect("member slot");
        assert_eq!(context.namespace.as_deref(), Some("a.b"));
        assert_eq!(context.prefix, "c");
    }

    #[test]
    fn set_value_position_is_variable() {
        let text = "{% set x = yz %}";
        assert_eq!(slot_name(text, 13), Some(JinjaSlot::Variable));
    }

    #[test]
    fn for_iterable_position_is_variable() {
        let text = "{% for x in yz %}";
        assert_eq!(slot_name(text, 14), Some(JinjaSlot::Variable));
    }

    #[test]
    fn float_member_reads_as_none() {
        assert_eq!(slot_name("{{ 3.5 }}", 6), Some(JinjaSlot::None));
    }

    #[test]
    fn inert_regions_return_none() {
        assert!(slot_at("```\n{{ x }}\n```", 6).is_none());
        assert!(slot_at("{# {{ x }} #}", 6).is_none());
        assert!(slot_at("---\na: 1\n---\n", 4).is_none());
    }

    #[test]
    fn if_condition_is_variable() {
        assert_eq!(slot_name("{% if xyz %}", 8), Some(JinjaSlot::Variable));
    }
}
