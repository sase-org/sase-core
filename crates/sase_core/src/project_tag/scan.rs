//! Project tag lexer: D1 grammar, literal-zone skipping, anchoring.

use crate::prompt_literal_zone_ranges;

use super::wire::ProjectTagSpanWire;

/// Scan `text` for lexically valid project tags.
///
/// A tag is `+` immediately followed by a name matching
/// `[A-Za-z](?:[A-Za-z0-9_.-]*[A-Za-z0-9_])?`. The `+` must sit at the start
/// of the text or directly after whitespace, `{`, or `|`, and the name must
/// be followed by the end of the text, whitespace, `|`, or `}`. Spans inside
/// fenced code, inline code, `%xprompts_enabled:false … :true` regions, or
/// the leading YAML frontmatter block are inert and skipped.
///
/// Scanning is purely lexical: whether the name resolves to a project is
/// decided by [`crate::project_tag::resolve_project_tag`].
pub fn scan_project_tags(text: &str) -> Vec<ProjectTagSpanWire> {
    let zones = project_tag_literal_zones(text);
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] != b'+' {
            idx += next_boundary(text, idx);
            continue;
        }
        match parse_tag_at(text, idx) {
            Some((name_end, name)) => {
                // `name` is the run with trailing `.`/`-` trimmed; the span
                // ends at the trimmed name while scanning resumes past the
                // full run.
                let end = idx + 1 + name.len();
                if !span_in_zones(idx, end, &zones) {
                    spans.push(ProjectTagSpanWire {
                        start: idx,
                        end,
                        name_start: idx + 1,
                        name,
                        anchored: is_anchored(text, idx),
                    });
                }
                idx = name_end;
            }
            None => {
                idx += next_boundary(text, idx);
            }
        }
    }
    spans
}

/// Whether `name` can be written as a project tag name.
pub fn is_tag_name(name: &str) -> bool {
    let mut chars = name.chars();
    if !chars.next().is_some_and(|ch| ch.is_ascii_alphabetic()) {
        return false;
    }
    if !name.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')
    }) {
        return false;
    }
    !matches!(name.as_bytes().last(), Some(b'.' | b'-'))
}

/// Parse a tag starting at the `+` at byte offset `plus`.
///
/// Returns the one-past-the-name byte offset and the trimmed name, or `None`
/// when the left, shape, or right boundary fails.
fn parse_tag_at(text: &str, plus: usize) -> Option<(usize, String)> {
    if !is_left_boundary(text, plus) {
        return None;
    }
    let rest = &text[plus + 1..];
    let run_len: usize = rest
        .bytes()
        .take_while(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')
        })
        .count();
    let mut name = &rest[..run_len];
    while name.ends_with(['.', '-']) {
        name = &name[..name.len() - 1];
    }
    if !is_tag_name(name) {
        return None;
    }
    let end = plus + 1 + name.len();
    if !is_right_boundary(text, end) {
        return None;
    }
    Some((plus + 1 + run_len, name.to_string()))
}

pub(crate) fn is_left_boundary(text: &str, plus: usize) -> bool {
    if plus == 0 {
        return true;
    }
    text[..plus]
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_whitespace() || ch == '{' || ch == '|')
}

fn is_right_boundary(text: &str, end: usize) -> bool {
    if end >= text.len() {
        return true;
    }
    text[end..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace() || ch == '|' || ch == '}')
}

/// Whether the tag at byte offset `plus` is the first word on its line.
///
/// Leading whitespace and `%directive` tokens before it on that line don't
/// count, matching `extract_vcs_workflow_tag` and Telegram's `%i:a +sase …`.
fn is_anchored(text: &str, plus: usize) -> bool {
    let line_start = text[..plus].rfind('\n').map_or(0, |pos| pos + 1);
    let mut rest = &text[line_start..plus];
    loop {
        rest = rest.trim_start_matches(|ch: char| ch.is_whitespace());
        let Some(stripped) = strip_directive_token(rest) else {
            return rest.is_empty();
        };
        rest = stripped;
    }
}

/// Strip one leading `%directive` token (with optional `+`, `:value`, or
/// `(args)` suffix) from `rest`, or return `None`.
fn strip_directive_token(rest: &str) -> Option<&str> {
    let body = rest.strip_prefix('%')?;
    let mut name_len = 0;
    for ch in body.chars() {
        let is_first = name_len == 0;
        let ok = if is_first {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        if !ok {
            break;
        }
        name_len += ch.len_utf8();
    }
    if name_len == 0 {
        return None;
    }
    let mut rest = &body[name_len..];
    if let Some(tail) = rest.strip_prefix('+') {
        rest = tail;
    } else if rest.starts_with(':') {
        rest = strip_colon_arg(&rest[1..])?;
    } else if rest.starts_with('(') {
        rest = strip_paren_arg(rest)?;
    }
    if rest.is_empty()
        || rest.chars().next().is_some_and(|ch| ch.is_whitespace())
    {
        Some(rest)
    } else {
        None
    }
}

fn strip_colon_arg(rest: &str) -> Option<&str> {
    if let Some(body) = rest.strip_prefix('`') {
        return body.find('`').map(|close| &body[close + 1..]);
    }
    let len: usize = rest
        .chars()
        .take_while(|ch| !ch.is_whitespace())
        .map(char::len_utf8)
        .sum();
    Some(&rest[len..])
}

fn strip_paren_arg(rest: &str) -> Option<&str> {
    debug_assert!(rest.starts_with('('));
    let mut depth = 0i32;
    let mut in_backticks = false;
    for (idx, ch) in rest.char_indices() {
        if ch == '\n' {
            return None;
        }
        if ch == '`' {
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                return Some(&rest[idx + 1..]);
            }
        }
    }
    None
}

/// Literal zones where tags are inert: the shared launch literal zones plus
/// the leading YAML frontmatter block.
pub(crate) fn project_tag_literal_zones(text: &str) -> Vec<(usize, usize)> {
    let mut zones = prompt_literal_zone_ranges(text);
    let frontmatter = leading_frontmatter_len(text);
    if frontmatter > 0 {
        zones.push((0, frontmatter));
    }
    zones
}

/// Byte length of a leading YAML frontmatter block (`---` … `---`), or 0.
fn leading_frontmatter_len(text: &str) -> usize {
    let mut lines = text.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return 0;
    };
    if first.trim_end_matches(['\r', '\n']) != "---" {
        return 0;
    }
    let mut consumed = first.len();
    for line in lines {
        consumed += line.len();
        if line.trim_end_matches(['\r', '\n']) == "---" {
            return consumed;
        }
    }
    0
}

fn span_in_zones(start: usize, end: usize, zones: &[(usize, usize)]) -> bool {
    zones.iter().any(|(s, e)| start < *e && *s < end)
}

fn next_boundary(text: &str, idx: usize) -> usize {
    text[idx..].chars().next().map_or(1, |ch| ch.len_utf8())
}
