//! Prompt scanning substrate: directive occurrences, literal zones,
//! directive-argument parsing, and the shared directive regexes.
//!
//! Everything here is lexical: no launch planning, only positions.
use super::wires::AgentLaunchFanoutPlanError;
use crate::fenced_code::fenced_block_ranges;
use crate::prompt_literals::inline_code_ranges;
use crate::xprompt_text_block::find_text_block_close_for_args;
use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub(crate) struct DirectiveOccurrence {
    pub(crate) canonical_name: String,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) args: Vec<String>,
    pub(crate) is_bare: bool,
    pub(crate) has_paren_form: bool,
    pub(crate) paren_closed: bool,
    pub(crate) has_plus_suffix: bool,
    // True when a single colon argument came from a backtick literal
    // (`` %model:`literal@id` ``). Such values bypass the `@effort` split so any
    // `@` in the model id is preserved, mirroring the Python parser.
    pub(crate) from_backtick_literal: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct XPromptOccurrence {
    pub(crate) name: String,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) has_time_argument: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectiveArg {
    pub(crate) name: Option<String>,
    pub(crate) value: String,
}

/// Which surface form opened an alternative directive. The legacy `%alt(...)`
/// and `%(...)` shorthand use parens with comma-separated branches; the new
/// `%{...}` shorthand uses braces with top-level `|`-separated branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AltDelimiter {
    Paren,
    Brace,
}

impl AltDelimiter {
    pub(crate) fn open(self) -> char {
        match self {
            Self::Paren => '(',
            Self::Brace => '{',
        }
    }

    pub(crate) fn close(self) -> char {
        match self {
            Self::Paren => ')',
            Self::Brace => '}',
        }
    }

    /// Branch separator inside the directive body.
    pub(crate) fn separator(self) -> char {
        match self {
            Self::Paren => ',',
            Self::Brace => '|',
        }
    }

    /// Human-readable directive name used in unclosed-directive errors.
    pub(crate) fn directive_label(self) -> &'static str {
        match self {
            Self::Paren => "%alt",
            Self::Brace => "%{",
        }
    }
}

pub(crate) fn directive_occurrences(
    prompt: &str,
) -> Result<Vec<DirectiveOccurrence>, AgentLaunchFanoutPlanError> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(prompt) {
        let marker = caps.get(2).expect("directive marker group");
        let raw_name = caps.get(3).expect("directive name group").as_str();
        let canonical_name = canonical_directive_name(raw_name).to_string();
        let mut end = marker.end();
        let mut args = Vec::new();
        let mut has_plus_suffix = false;
        let mut from_backtick_literal = false;
        let has_paren_form = caps.get(4).is_some();
        let mut paren_closed = !has_paren_form;
        let colon_arg = caps.get(5);
        let has_plus_form = caps.get(6).is_some();
        let is_bare = !has_paren_form && colon_arg.is_none() && !has_plus_form;

        if has_paren_form {
            let paren_start = marker.end() - 1;
            if let Some(paren_end) = find_matching_paren(prompt, paren_start) {
                args =
                    parse_directive_args(&prompt[paren_start + 1..paren_end]);
                end = paren_end + 1;
                paren_closed = true;
            }
        } else if let Some(colon_arg) = colon_arg {
            from_backtick_literal = colon_arg.as_str().starts_with('`');
            args = vec![unquote_backticks(colon_arg.as_str())];
        } else if has_plus_form {
            has_plus_suffix = true;
            args = vec!["true".to_string()];
        } else {
            args = vec![String::new()];
        }

        out.push(DirectiveOccurrence {
            canonical_name,
            start: marker.start(),
            end,
            args,
            is_bare,
            has_paren_form,
            paren_closed,
            has_plus_suffix,
            from_backtick_literal,
        });
    }
    Ok(out)
}

pub(crate) fn xprompt_occurrences(prompt: &str) -> Vec<XPromptOccurrence> {
    xprompt_reference_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            let name_match = captures.get(3)?;
            let name = name_match.as_str().replace("__", "/");
            let has_time_argument = prompt
                .as_bytes()
                .get(name_match.end())
                .is_some_and(|byte| matches!(byte, b':' | b'('));
            let mut end = marker.end();
            if captures.get(4).is_some() {
                let paren_start = marker.end() - 1;
                if let Some(paren_end) =
                    find_matching_paren(prompt, paren_start)
                {
                    end = paren_end + 1;
                }
            }
            Some(XPromptOccurrence {
                name,
                start: marker.start(),
                end,
                has_time_argument,
            })
        })
        .collect()
}

pub(crate) fn launch_literal_zone_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    ranges.extend(code_directive_call_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

fn code_directive_call_ranges(prompt: &str) -> Vec<(usize, usize)> {
    directive_occurrences(prompt)
        .unwrap_or_default()
        .into_iter()
        .filter(|directive| {
            matches!(directive.canonical_name.as_str(), "if" | "proc")
        })
        .map(|directive| (directive.start, directive.end))
        .collect()
}

pub(crate) fn launch_inline_literal_ranges(
    prompt: &str,
) -> Vec<(usize, usize)> {
    let mut masks = fenced_block_ranges(prompt);
    masks.extend(disabled_region_ranges(prompt));
    masks.extend(
        directive_occurrences(prompt)
            .unwrap_or_default()
            .into_iter()
            .map(|directive| (directive.start, directive.end)),
    );
    masks.extend(
        xprompt_occurrences(prompt)
            .into_iter()
            .map(|reference| (reference.start, reference.end)),
    );
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            masks.push((start, close_end + 1));
        }
    }
    inline_code_ranges(prompt, &masks)
}

pub(crate) fn alt_directive_starts(
    prompt: &str,
) -> Vec<(usize, usize, AltDelimiter)> {
    alt_directive_re()
        .captures_iter(prompt)
        .filter_map(|caps| {
            let marker = caps.get(2)?;
            let open = marker.end() - 1;
            let delimiter = if prompt.as_bytes()[open] == b'{' {
                AltDelimiter::Brace
            } else {
                AltDelimiter::Paren
            };
            Some((marker.start(), open, delimiter))
        })
        .collect()
}

pub(crate) fn alt_inner_ranges(
    prompt: &str,
    ignored_ranges: &[(usize, usize)],
) -> Result<Vec<(usize, usize)>, AgentLaunchFanoutPlanError> {
    let mut ranges = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if position_in_ranges(start, ignored_ranges) {
            continue;
        }
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            ranges.push((open_start + 1, close_end));
        }
    }
    Ok(ranges)
}

fn parse_directive_args(inner: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < inner.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(inner, idx) {
                idx = next;
                continue;
            }
        }
        let ch = inner[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            ',' if depth == 0 => {
                push_arg(&mut args, &inner[start..idx]);
                start = idx + ch_len;
            }
            _ => {}
        }
        idx += ch_len;
    }
    push_arg(&mut args, &inner[start..]);
    args.into_iter().filter(|arg| !arg.is_empty()).collect()
}

pub(crate) fn parse_directive_args_with_names(
    inner: &str,
    separator: char,
) -> Vec<DirectiveArg> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < inner.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(inner, idx) {
                idx = next;
                continue;
            }
        }
        let ch = inner[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            _ if ch == separator && depth == 0 => {
                push_directive_arg(&mut args, &inner[start..idx]);
                start = idx + ch_len;
            }
            _ => {}
        }
        idx += ch_len;
    }
    push_directive_arg(&mut args, &inner[start..]);
    args.into_iter()
        .filter(|arg| !arg.value.is_empty() || arg.name.is_some())
        .collect()
}

fn push_directive_arg(args: &mut Vec<DirectiveArg>, raw: &str) {
    let trimmed = raw.trim();
    let (name, value_raw) = split_named_directive_arg(trimmed);
    let value_trimmed = value_raw.trim();
    let value = unquote_directive_arg_value(value_trimmed);
    args.push(DirectiveArg { name, value });
}

fn push_arg(args: &mut Vec<String>, raw: &str) {
    let trimmed = raw.trim();
    args.push(unquote_directive_arg_value(trimmed));
}

pub(crate) fn split_named_directive_arg(raw: &str) -> (Option<String>, &str) {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < raw.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(raw, idx) {
                idx = next;
                continue;
            }
        }
        let ch = raw[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            '=' if depth == 0 => {
                let name = raw[..idx].trim();
                let value = &raw[idx + ch_len..];
                if !name.is_empty() {
                    return (Some(unquote_backticks(name)), value);
                }
                return (None, raw);
            }
            _ => {}
        }
        idx += ch_len;
    }
    (None, raw)
}

pub(crate) fn unquote_directive_arg_value(trimmed: &str) -> String {
    if trimmed.starts_with("[[")
        && trimmed.ends_with("]]")
        && trimmed.len() >= 4
    {
        trimmed[2..trimmed.len() - 2].to_string()
    } else if ((trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
        && trimmed.len() >= 2
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        unquote_backticks(trimmed)
    }
}
fn unquote_backticks(value: &str) -> String {
    if value.starts_with('`') && value.ends_with('`') && value.len() >= 2 {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

pub(crate) fn find_matching_paren(
    text: &str,
    paren_start: usize,
) -> Option<usize> {
    find_matching_delimiter(text, paren_start, '(', ')')
}

/// Find the index of the delimiter that closes the `open` character at
/// `open_start`, counting only that delimiter pair and ignoring matches inside
/// backtick-quoted spans.
pub(crate) fn find_matching_delimiter(
    text: &str,
    open_start: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = open_start;
    while idx < text.len() {
        if !in_backticks
            && !in_double_quotes
            && text.as_bytes().get(idx..idx + 2) == Some(b"[[")
        {
            idx = find_text_block_close_for_args(text, idx, text.len())? + 2;
            continue;
        }
        let ch = text[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
        idx += ch_len;
    }
    None
}

/// Skip a `[[...]]` argument text block at `idx`, or the rest of `text` when
/// the block is unterminated. Returns `None` when `idx` is not a block opener.
fn skip_directive_text_block(text: &str, idx: usize) -> Option<usize> {
    if text.as_bytes().get(idx..idx + 2) != Some(b"[[") {
        return None;
    }
    Some(
        find_text_block_close_for_args(text, idx, text.len())
            .map(|close| close + 2)
            .unwrap_or(text.len()),
    )
}
pub(crate) fn disabled_region_ranges(text: &str) -> Vec<(usize, usize)> {
    disabled_region_re()
        .find_iter(text)
        .map(|m| (m.start(), m.end()))
        .collect()
}

pub(crate) fn strip_disabled_region_markers(text: &str) -> String {
    disabled_marker_re().replace_all(text, "").to_string()
}

pub(crate) fn position_in_ranges(
    pos: usize,
    ranges: &[(usize, usize)],
) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= pos && pos < *end)
}

/// Canonicalize a directive name for fan-out planning by deferring to the
/// shared editor directive registry. This keeps the planner in lock-step with
/// the advertised directive set, including `%a`→`auto`, instead of maintaining
/// a second alias table that can drift. Unknown names pass through unchanged.
pub(crate) fn canonical_directive_name(name: &str) -> &str {
    crate::editor::directive::canonical_directive_name(name).unwrap_or(name)
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // The colon-arg class includes `@` so a `%model:<model>@<effort>`
        // suffix is captured as one directive value (matching the Python
        // `_DIRECTIVE_PATTERN`); the `@effort` token is split off in
        // `extract_first_model_value` via `split_model_effort`.
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(%([A-Za-z_][A-Za-z0-9_]*)(?:(\()|:(`[^`]*`|[A-Za-z0-9_#/.,()@-]*[A-Za-z0-9_#/,()@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn xprompt_reference_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(#!?([A-Za-z_][A-Za-z0-9_]*(?:/[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?(?:(\()|:(`[^`]*`|\$\([^)]*\)|\{\{[^}]*\}\}|\{[^}]*\}|[A-Za-z0-9_.~,+/@-]*[A-Za-z0-9_~,+/@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn alt_directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[\s\(\[\{"':])(%(?:alt)?\(|%\{)"#).unwrap()
    })
}

fn disabled_region_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ms)^[ \t]*%xprompts_enabled:false[ \t]*\n.*?(?:^[ \t]*|[ \t]+)%xprompts_enabled:true[ \t]*\n?",
        )
        .unwrap()
    })
}

fn disabled_marker_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?m)^[ \t]*%xprompts_enabled:(?:false|true)[ \t]*\n?|[ \t]+%xprompts_enabled:(?:false|true)[ \t]*",
        )
        .unwrap()
    })
}

pub(crate) fn leading_blank_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*\n").unwrap())
}
