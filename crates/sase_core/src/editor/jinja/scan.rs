//! Jinja tag scanning: find the `{{ }}` / `{% %}` tag at the cursor.
//!
//! The scanner respects every inert region from the plan: fenced code, inline
//! code spans, `%xprompts_enabled:false` zones (all via
//! [`crate::prompt_literal_zone_ranges`]), `{# #}` comments, `{% raw %}`
//! bodies, and the document's leading YAML frontmatter block. The closer
//! search skips quoted string literals, and unclosed openers run to the next
//! opener or to the end of text so `{{ ro` with no closer still completes.

use crate::agent_launch::{
    disabled_region_ranges, launch_inline_literal_ranges,
};
use crate::editor::exclusion::{
    frontmatter_block_len, next_jinja_tag, position_in_ranges,
};
use crate::fenced_block_ranges;

/// A `{{ }}` expression tag or a `{% %}` statement tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JinjaTagKind {
    /// A `{{ ... }}` expression tag.
    Variable,
    /// A `{% ... %}` statement tag.
    Statement,
}

/// The tag surrounding the cursor, as UTF-8 byte offsets into the document.
///
/// `content_start` is just past the opener (`{{`, `{{-`, `{%`, ...).
/// `content_end` is the start of the closer, the start of the next opener
/// for an unclosed tag, or the end of text. `tag_end` extends past the
/// closer when the tag is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JinjaTag {
    pub kind: JinjaTagKind,
    pub open_start: usize,
    pub content_start: usize,
    pub content_end: usize,
    pub tag_end: usize,
    pub closed: bool,
}

/// A closed or unclosed `{% ... %}` tag, for statement-level analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScannedStatementTag {
    pub open_start: usize,
    pub content_start: usize,
    pub content_end: usize,
    pub tag_end: usize,
    pub closed: bool,
}

/// Find the Jinja tag at `cursor`, or `None` outside tags and inert regions.
///
/// Inert positions (literal zones, comments, raw bodies, frontmatter) return
/// `None`, matching the engine contract: no completion surface may claim
/// them.
pub fn jinja_tag_at_cursor(text: &str, cursor: usize) -> Option<JinjaTag> {
    let cursor = floor_offset(text, cursor);
    if in_frontmatter(text, cursor)
        || position_in_ranges(cursor, &jinja_inert_literal_zones(text))
    {
        return None;
    }
    let scanned = scan_document(text);
    if position_in_ranges(cursor, &scanned.inert) {
        return None;
    }
    scanned
        .tags
        .into_iter()
        .find(|tag| tag.open_start + 2 <= cursor && cursor <= tag.content_end)
}

/// All variable and statement tags in the document, in document order.
pub(crate) fn scan_jinja_tags(text: &str) -> Vec<JinjaTag> {
    scan_document(text).tags
}

/// Inert sub-ranges: `{# #}` comments and `{% raw %}` bodies.
pub(crate) fn scan_jinja_inert_ranges(text: &str) -> Vec<(usize, usize)> {
    scan_document(text).inert
}

/// Closed and unclosed `{% ... %}` tags in document order.
pub(crate) fn scan_statement_tags(text: &str) -> Vec<ScannedStatementTag> {
    scan_document(text).statements
}

struct ScannedDocument {
    tags: Vec<JinjaTag>,
    statements: Vec<ScannedStatementTag>,
    inert: Vec<(usize, usize)>,
}

/// Single-pass tag enumeration.
///
/// String-aware closers keep `{{ "}}" }}` as one tag; comment and raw bodies
/// are literal, so they scan naively to their terminator and any opener-like
/// text inside them never becomes a tag.
fn scan_document(text: &str) -> ScannedDocument {
    let mut out = ScannedDocument {
        tags: Vec::new(),
        statements: Vec::new(),
        inert: Vec::new(),
    };
    let mut offset = 0;
    while offset < text.len() {
        let Some((relative, _)) = next_jinja_tag(text, offset) else {
            break;
        };
        let start = offset + relative;
        let opener = &text[start..start + 2];
        if opener == "{#" {
            let end = text
                .get(start + 2..)
                .and_then(|tail| tail.find("#}"))
                .map(|index| start + 2 + index + 2)
                .unwrap_or(text.len());
            out.inert.push((start, end));
            offset = end.max(start + 2);
            continue;
        }
        let kind = if opener == "{{" {
            JinjaTagKind::Variable
        } else {
            JinjaTagKind::Statement
        };
        let close = if kind == JinjaTagKind::Variable {
            "}}"
        } else {
            "%}"
        };
        let closer = find_tag_close(text, start + 2, close);
        let next = next_jinja_tag(text, start + 2)
            .map(|(relative, _)| start + 2 + relative);
        let (content_end, tag_end, closed) = match closer {
            Some(close_start) if next.is_none_or(|next| next > close_start) => {
                (
                    close_start,
                    close_start + closer_len(text, close_start),
                    true,
                )
            }
            _ => {
                let end = next.unwrap_or(text.len());
                (end, end, false)
            }
        };
        let tag = JinjaTag {
            kind,
            open_start: start,
            content_start: start + 2,
            content_end,
            tag_end,
            closed,
        };
        out.tags.push(tag);
        if kind == JinjaTagKind::Statement {
            out.statements.push(ScannedStatementTag {
                open_start: start,
                content_start: start + 2,
                content_end,
                tag_end,
                closed,
            });
            if closed && is_raw_opener(text, start + 2, content_end) {
                if let Some(end) = find_raw_end(text, tag_end) {
                    out.inert.push((tag_end, end));
                    offset = end;
                    continue;
                }
            }
        }
        offset = tag_end.max(start + 2);
    }
    out
}

/// Length of the closer at `close_start`, including a `-`/`+` prefix.
fn closer_len(text: &str, close_start: usize) -> usize {
    let first = text.as_bytes().get(close_start).copied().unwrap_or(0);
    if first == b'-' || first == b'+' {
        3
    } else {
        2
    }
}

/// Find the closer for the tag opened before `from`, skipping quoted string
/// literals. Accepts whitespace-control (`-`/`+`) prefixes.
fn find_tag_close(text: &str, from: usize, close: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = from;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' || byte == b'"' {
            index = skip_quoted(text, index);
            continue;
        }
        let tail = text.get(index..).unwrap_or("");
        if tail.starts_with(close) {
            return Some(index);
        }
        if (byte == b'-' || byte == b'+')
            && text
                .get(index + 1..)
                .is_some_and(|rest| rest.starts_with(close))
        {
            return Some(index);
        }
        index += char_width(text, index);
    }
    None
}

/// Whether a closed statement tag is a `{% raw %}` opener.
fn is_raw_opener(text: &str, content_start: usize, content_end: usize) -> bool {
    let content = text.get(content_start..content_end).unwrap_or("");
    first_word(content).is_some_and(|(word, _, _)| word == "raw")
}

/// Find the end of a raw body: the tag end of the first `{% endraw %}` at or
/// after `from`. The body is literal, so this scan is naive.
fn find_raw_end(text: &str, from: usize) -> Option<usize> {
    let mut offset = from;
    while offset < text.len() {
        let (relative, _) = next_jinja_tag(text, offset)?;
        let start = offset + relative;
        if &text[start..start + 2] != "{%" {
            offset = start + 2;
            continue;
        }
        let close = text
            .get(start + 2..)
            .and_then(|tail| tail.find("%}"))
            .map(|index| start + 2 + index)?;
        let content = text.get(start + 2..close).unwrap_or("");
        if first_word(content).is_some_and(|(word, _, _)| word == "endraw") {
            return Some(close + 2);
        }
        offset = start + 2;
    }
    None
}

/// Skip a quoted string starting at the quote at `quote_at`, returning the
/// offset just past the closing quote (or the end of text). Handles
/// backslash escapes.
pub(crate) fn skip_quoted(text: &str, quote_at: usize) -> usize {
    let bytes = text.as_bytes();
    let quote = bytes[quote_at];
    let mut index = quote_at + 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index += 1;
            if index < bytes.len() {
                index += char_width(text, index);
            }
            continue;
        }
        if bytes[index] == quote {
            return index + 1;
        }
        index += char_width(text, index);
    }
    bytes.len()
}

/// Byte ranges of quoted strings in `content`, including both quotes.
pub(crate) fn string_literal_ranges(content: &str) -> Vec<(usize, usize)> {
    let bytes = content.as_bytes();
    let mut ranges = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' || byte == b'"' {
            let end = skip_quoted(content, index);
            ranges.push((index, end));
            index = end.max(index + 1);
        } else {
            index += char_width(content, index);
        }
    }
    ranges
}

/// UTF-8 width of the character starting at `index` (0 for out of bounds).
pub(crate) fn char_width(text: &str, index: usize) -> usize {
    text[index..]
        .chars()
        .next()
        .map(|ch| ch.len_utf8())
        .unwrap_or(0)
}

/// Clamp `index` into `text` on a character boundary, rounding down.
pub(crate) fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Clamp `cursor` into `text` on a character boundary.
fn floor_offset(text: &str, cursor: usize) -> usize {
    floor_char_boundary(text, cursor)
}

fn in_frontmatter(text: &str, cursor: usize) -> bool {
    frontmatter_block_len(text).is_some_and(|len| cursor < len)
}

/// Literal zones that stay inert for Jinja: fenced code, inline code spans,
/// and `%xprompts_enabled:false` regions.
///
/// Launch `%if`/`%proc` directive-call zones are deliberately excluded:
/// `{%if` with no space is a Jinja statement tag, and inside any tag Jinja
/// owns completion.
fn jinja_inert_literal_zones(text: &str) -> Vec<(usize, usize)> {
    let mut zones = fenced_block_ranges(text);
    zones.extend(disabled_region_ranges(text));
    if text.contains('`') {
        zones.extend(launch_inline_literal_ranges(text));
    }
    zones
}

/// ASCII identifier character: `[A-Za-z0-9_]`.
pub(crate) fn is_ident_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// ASCII identifier start: `[A-Za-z_]`.
pub(crate) fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

/// First whitespace-separated word of statement-tag content, skipping one
/// leading whitespace-control (`-`/`+`) marker. Returns the word and its
/// content-relative byte range.
pub(crate) fn first_word(content: &str) -> Option<(String, usize, usize)> {
    let bytes = content.as_bytes();
    let mut index = 0;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    // One whitespace-control marker may sit directly after the opener.
    if index == 0
        && (bytes.first() == Some(&b'-') || bytes.first() == Some(&b'+'))
    {
        index += 1;
    }
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    let start = index;
    while index < bytes.len() && is_ident_char(bytes[index]) {
        index += 1;
    }
    if start == index {
        return None;
    }
    Some((content[start..index].to_string(), start, index))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag_at(text: &str, cursor: usize) -> Option<JinjaTag> {
        jinja_tag_at_cursor(text, cursor)
    }

    #[test]
    fn unclosed_expression_tag_still_completes() {
        let text = "{{ ro";
        let tag = tag_at(text, 5).expect("unclosed tag completes");
        assert_eq!(tag.kind, JinjaTagKind::Variable);
        assert!(!tag.closed);
        assert_eq!(tag.content_end, text.len());
    }

    #[test]
    fn at_prefixed_expression_tag_completes_as_jinja() {
        let text = "@{{ fi";
        let tag = tag_at(text, 6).expect("@{{ completes as Jinja");
        assert_eq!(tag.kind, JinjaTagKind::Variable);
        assert_eq!(&text[tag.content_start..tag.content_end], " fi");
    }

    #[test]
    fn cursor_after_closer_is_outside_tag() {
        let text = "{{ x }} {{ y";
        assert!(tag_at(text, 7).is_none());
        let tag = tag_at(text, 11).expect("second tag completes");
        assert_eq!(tag.kind, JinjaTagKind::Variable);
    }

    #[test]
    fn whitespace_control_delimiters_form_one_tag() {
        for (text, kind) in [
            ("{{- x -}}", JinjaTagKind::Variable),
            ("{{+ x +}}", JinjaTagKind::Variable),
            ("{%- if x -%}{%- endif -%}", JinjaTagKind::Statement),
        ] {
            let tag = tag_at(text, 4).expect("control delimiters scan");
            assert_eq!(tag.kind, kind, "{text}");
            assert!(tag.closed, "{text}");
        }
    }

    #[test]
    fn statement_tag_without_space_scans() {
        let text = "{%if x%}y";
        let tag = tag_at(text, 3).expect("{%if scans");
        assert_eq!(tag.kind, JinjaTagKind::Statement);
        assert!(tag.closed);
    }

    #[test]
    fn closer_inside_string_does_not_end_tag() {
        let text = r#"{{ "}}" }} "#;
        let tag = tag_at(text, 6).expect("inside tag after string closer");
        assert!(tag.closed);
        assert_eq!(&text[tag.content_start..tag.content_end], r#" "}}" "#);
        assert!(tag_at(text, 11).is_none());
    }

    #[test]
    fn comment_hides_tag_like_text() {
        let text = "{# {{ x }} #} {{ y";
        assert!(tag_at(text, 6).is_none());
        assert!(tag_at(text, 17).is_some());
    }

    #[test]
    fn raw_body_is_inert_but_later_tags_complete() {
        let text = "{% raw %}{{ x }}{% endraw %} {{ y";
        assert!(tag_at(text, 12).is_none());
        assert!(tag_at(text, 34).is_some());
    }

    #[test]
    fn frontmatter_block_is_inert() {
        let text = "---\ninput:\n  a: word\n---\n{{ a";
        assert!(tag_at(text, 10).is_none());
        assert!(tag_at(text, 30).is_some());
    }

    #[test]
    fn fenced_and_inline_code_are_inert() {
        let fenced = "```\n{{ x }}\n```\n{{ y";
        assert!(tag_at(fenced, 6).is_none());
        assert!(tag_at(fenced, 20).is_some());
        let inline = "`{{ x }}` {{ y";
        assert!(tag_at(inline, 3).is_none());
        assert!(tag_at(inline, 14).is_some());
    }

    #[test]
    fn alternation_openers_are_not_jinja_tags() {
        for text in ["%{a | b}", "{%{a | b}", "{%(a,b)", "x %{a} y {{ z"] {
            let tag = tag_at(text, text.find('|').unwrap_or(2));
            assert!(tag.is_none(), "{text}");
        }
        let text = "x %{a} y {{ z";
        assert!(tag_at(text, text.len()).is_some());
    }

    #[test]
    fn multiline_tag_scans() {
        let text = "{{\n  foo |\n  bar\n}}";
        let tag = tag_at(text, 10).expect("multiline tag completes");
        assert_eq!(tag.kind, JinjaTagKind::Variable);
        assert!(tag.closed);
    }

    #[test]
    fn unclosed_opener_runs_to_next_opener() {
        let text = "{{ a {% if x %}y";
        let first = tag_at(text, 4).expect("first tag completes");
        assert!(!first.closed);
        assert_eq!(&text[first.content_start..first.content_end], " a ");
        let second = tag_at(text, 11).expect("second tag completes");
        assert_eq!(second.kind, JinjaTagKind::Statement);
    }
}
