use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XpromptArgSyntax {
    None,
    Plus,
    Colon,
    DoubleColonText,
    Parenthesized,
    Malformed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedXpromptCall {
    pub(crate) name: String,
    pub(crate) name_span: (usize, usize),
    pub(crate) args: Vec<ParsedXpromptArg>,
    pub(crate) syntax: XpromptArgSyntax,
    pub(crate) is_open: bool,
    pub(crate) malformed_span: Option<(usize, usize)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedXpromptArg {
    pub(crate) name: Option<ParsedXpromptArgName>,
    pub(crate) value: String,
    pub(crate) value_span: (usize, usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedXpromptArgName {
    pub(crate) value: String,
    pub(crate) span: (usize, usize),
}

pub(crate) fn parse_xprompt_calls(text: &str) -> Vec<ParsedXpromptCall> {
    let mut calls = Vec::new();
    for caps in xprompt_ref_re().captures_iter(text) {
        let Some(name_match) = caps.name("name") else {
            continue;
        };
        let suffix_start = caps
            .name("hitl")
            .map(|hitl| hitl.end())
            .unwrap_or_else(|| name_match.end());
        let name = name_match.as_str().replace("__", "/");
        calls.push(parse_call_suffix(
            text,
            name,
            (name_match.start(), name_match.end()),
            suffix_start,
        ));
    }
    calls
}

fn parse_call_suffix(
    text: &str,
    name: String,
    name_span: (usize, usize),
    suffix_start: usize,
) -> ParsedXpromptCall {
    let mut call = ParsedXpromptCall {
        name,
        name_span,
        args: Vec::new(),
        syntax: XpromptArgSyntax::None,
        is_open: false,
        malformed_span: None,
    };
    let Some(suffix) = text.get(suffix_start..) else {
        return call;
    };
    if suffix.starts_with('+') {
        call.syntax = XpromptArgSyntax::Plus;
        call.args.push(ParsedXpromptArg {
            name: None,
            value: "true".to_string(),
            value_span: (suffix_start, suffix_start + 1),
        });
        return call;
    }
    if suffix.starts_with('(') {
        parse_parenthesized(text, suffix_start, &mut call);
        return call;
    }
    if suffix.starts_with(':') {
        parse_colon(text, suffix_start, &mut call);
    }
    call
}

fn parse_parenthesized(
    text: &str,
    open_idx: usize,
    call: &mut ParsedXpromptCall,
) {
    call.syntax = XpromptArgSyntax::Parenthesized;
    let Some(close_idx) = find_matching_paren_for_args(text, open_idx) else {
        call.is_open = true;
        return;
    };
    parse_arg_body(text, open_idx + 1, close_idx, call);

    let after_close = close_idx + 1;
    let Some(after) = text.get(after_close..) else {
        return;
    };
    if after.starts_with(":: ") {
        let value_start = after_close + 3;
        let value_end = find_double_colon_text_end(text, value_start);
        push_text_arg(text, value_start, value_end, call);
    } else if after.starts_with(": ") {
        let value_start = after_close + 2;
        let value_end = find_shorthand_text_end(text, value_start);
        push_text_arg(text, value_start, value_end, call);
    } else if after.starts_with(':') && after.len() > 1 {
        let value_start = after_close + 1;
        let value_end = token_end(text, value_start);
        push_text_arg(text, value_start, value_end, call);
    }
}

fn parse_colon(text: &str, colon_idx: usize, call: &mut ParsedXpromptCall) {
    let Some(after_colon) = text.get(colon_idx + 1..) else {
        call.is_open = true;
        return;
    };
    if after_colon.is_empty() {
        call.is_open = true;
        return;
    }
    if after_colon.starts_with(": ") {
        call.syntax = XpromptArgSyntax::DoubleColonText;
        let value_start = colon_idx + 3;
        let value_end = find_double_colon_text_end(text, value_start);
        if value_start >= value_end {
            call.is_open = true;
            return;
        }
        push_text_arg(text, value_start, value_end, call);
        return;
    }
    if after_colon.starts_with(' ') {
        call.syntax = XpromptArgSyntax::Colon;
        let value_start = colon_idx + 2;
        let value_end = find_shorthand_text_end(text, value_start);
        if value_start >= value_end {
            call.is_open = true;
            return;
        }
        push_text_arg(text, value_start, value_end, call);
        return;
    }

    call.syntax = XpromptArgSyntax::Colon;
    let value_start = colon_idx + 1;
    let value_end = token_end(text, value_start);
    if value_start >= value_end {
        call.is_open = true;
        return;
    }
    if text[value_start..value_end].contains(['+', '(', ')']) {
        call.syntax = XpromptArgSyntax::Malformed;
        call.malformed_span = Some((value_start, value_end));
        return;
    }
    for (start, end) in split_commas(text, value_start, value_end) {
        push_value_arg(text, start, end, None, call);
    }
}

fn parse_arg_body(
    text: &str,
    body_start: usize,
    body_end: usize,
    call: &mut ParsedXpromptCall,
) {
    for (token_start, token_end) in split_commas(text, body_start, body_end) {
        if let Some(equal_idx) =
            find_top_level_equal(text, token_start, token_end)
        {
            let (name_start, name_end) =
                trim_span(text, token_start, equal_idx);
            let (value_start, value_end) =
                trim_span(text, equal_idx + 1, token_end);
            if name_start >= name_end {
                call.syntax = XpromptArgSyntax::Malformed;
                call.malformed_span = Some((token_start, token_end));
                continue;
            }
            let name = ParsedXpromptArgName {
                value: text[name_start..name_end].to_string(),
                span: (name_start, name_end),
            };
            push_value_arg(text, value_start, value_end, Some(name), call);
        } else {
            push_value_arg(text, token_start, token_end, None, call);
        }
    }
}

fn push_text_arg(
    text: &str,
    value_start: usize,
    value_end: usize,
    call: &mut ParsedXpromptCall,
) {
    let value_end = trim_end_ascii_ws(text, value_start, value_end);
    if value_start >= value_end {
        return;
    }
    call.args.push(ParsedXpromptArg {
        name: None,
        value: text[value_start..value_end].to_string(),
        value_span: (value_start, value_end),
    });
}

fn push_value_arg(
    text: &str,
    start: usize,
    end: usize,
    name: Option<ParsedXpromptArgName>,
    call: &mut ParsedXpromptCall,
) {
    let value = decoded_value(text, start, end);
    call.args.push(ParsedXpromptArg {
        name,
        value,
        value_span: (start, end),
    });
}

fn decoded_value(text: &str, start: usize, end: usize) -> String {
    if start >= end {
        return String::new();
    }
    let value = &text[start..end];
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[value.len() - 1];
        if (first == b'\'' || first == b'"') && first == last {
            return value[1..value.len() - 1].to_string();
        }
    }
    if value.starts_with("[[") && value.ends_with("]]") {
        return value[2..value.len() - 2].to_string();
    }
    value.to_string()
}

fn split_commas(text: &str, start: usize, end: usize) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut token_start = start;
    let mut scan = ArgScanner::default();
    let mut i = start;
    while i < end {
        if scan.step(text, i) {
            i += 2;
            continue;
        }
        if text.as_bytes()[i] == b',' && scan.is_top_level() {
            let trimmed = trim_span(text, token_start, i);
            if trimmed.0 < trimmed.1 {
                spans.push(trimmed);
            }
            token_start = i + 1;
        }
        i += 1;
    }
    let trimmed = trim_span(text, token_start, end);
    if trimmed.0 < trimmed.1 {
        spans.push(trimmed);
    }
    spans
}

fn find_top_level_equal(text: &str, start: usize, end: usize) -> Option<usize> {
    let mut scan = ArgScanner::default();
    let mut i = start;
    while i < end {
        if scan.step(text, i) {
            i += 2;
            continue;
        }
        if text.as_bytes()[i] == b'=' && scan.is_top_level() {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_matching_paren_for_args(text: &str, open_idx: usize) -> Option<usize> {
    if text.as_bytes().get(open_idx) != Some(&b'(') {
        return None;
    }
    let mut scan = ArgScanner::default();
    let mut depth = 1usize;
    let mut i = open_idx + 1;
    while i < text.len() {
        if scan.step(text, i) {
            i += 2;
            continue;
        }
        if scan.is_top_level() {
            match text.as_bytes()[i] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

#[derive(Default)]
struct ArgScanner {
    quote: Option<u8>,
    in_text_block: bool,
}

impl ArgScanner {
    fn is_top_level(&self) -> bool {
        self.quote.is_none() && !self.in_text_block
    }

    fn step(&mut self, text: &str, idx: usize) -> bool {
        let bytes = text.as_bytes();
        if self.quote.is_none()
            && !self.in_text_block
            && bytes.get(idx..idx + 2) == Some(b"[[")
        {
            self.in_text_block = true;
            return true;
        }
        if self.in_text_block && bytes.get(idx..idx + 2) == Some(b"]]") {
            self.in_text_block = false;
            return true;
        }
        if self.in_text_block {
            return false;
        }
        match (self.quote, bytes[idx]) {
            (None, b'"' | b'\'') => self.quote = Some(bytes[idx]),
            (Some(quote), ch) if ch == quote => self.quote = None,
            _ => {}
        }
        false
    }
}

fn token_end(text: &str, start: usize) -> usize {
    text[start..]
        .find(char::is_whitespace)
        .map(|offset| start + offset)
        .unwrap_or(text.len())
}

fn find_shorthand_text_end(text: &str, start: usize) -> usize {
    text[start..]
        .find("\n\n")
        .map(|offset| start + offset)
        .unwrap_or(text.len())
}

fn find_double_colon_text_end(text: &str, start: usize) -> usize {
    let bytes = text.as_bytes();
    let mut idx = start;
    while idx < text.len() {
        if bytes[idx] == b'\n'
            && text
                .get(idx + 1..)
                .is_some_and(starts_with_xprompt_directive)
        {
            return idx;
        }
        idx += 1;
    }
    text.len()
}

fn starts_with_xprompt_directive(text: &str) -> bool {
    let Some(after_hash) = text.strip_prefix('#') else {
        return false;
    };
    let Some((name_end, _)) = scan_name(after_hash) else {
        return false;
    };
    let after_name = &after_hash[name_end..];
    after_name.starts_with('(')
        || after_name.starts_with(": ")
        || after_name.starts_with(":: ")
}

fn scan_name(text: &str) -> Option<(usize, String)> {
    let bytes = text.as_bytes();
    let first = *bytes.first()?;
    if !is_name_start(first) {
        return None;
    }
    let mut idx = 1;
    while idx < bytes.len() {
        if is_name_continue(bytes[idx]) {
            idx += 1;
        } else if bytes[idx] == b'/' {
            let next = *bytes.get(idx + 1)?;
            if !is_name_start(next) {
                break;
            }
            idx += 2;
        } else {
            break;
        }
    }
    Some((idx, text[..idx].to_string()))
}

fn trim_span(text: &str, start: usize, end: usize) -> (usize, usize) {
    let mut trimmed_start = start;
    let mut trimmed_end = end;
    while trimmed_start < trimmed_end
        && text.as_bytes()[trimmed_start].is_ascii_whitespace()
    {
        trimmed_start += 1;
    }
    trimmed_end = trim_end_ascii_ws(text, trimmed_start, trimmed_end);
    (trimmed_start, trimmed_end)
}

fn trim_end_ascii_ws(text: &str, start: usize, end: usize) -> usize {
    let mut trimmed_end = end;
    while start < trimmed_end
        && text.as_bytes()[trimmed_end - 1].is_ascii_whitespace()
    {
        trimmed_end -= 1;
    }
    trimmed_end
}

fn is_name_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_'
}

fn is_name_continue(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?P<hitl>!!|\?\?)?"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(text: &str) -> ParsedXpromptCall {
        let calls = parse_xprompt_calls(text);
        assert_eq!(calls.len(), 1, "{calls:?}");
        calls.into_iter().next().unwrap()
    }

    #[test]
    fn parses_parenthesized_named_and_positional_args() {
        let call = one("#foo(src/main.rs, enabled=true)");
        assert_eq!(call.name, "foo");
        assert_eq!(call.syntax, XpromptArgSyntax::Parenthesized);
        assert_eq!(call.args[0].value, "src/main.rs");
        assert_eq!(call.args[1].name.as_ref().unwrap().value, "enabled");
        assert_eq!(call.args[1].value, "true");
    }

    #[test]
    fn parses_colon_plus_hitl_and_namespaced_forms() {
        assert_eq!(one("#foo+").args[0].value, "true");
        assert_eq!(one("#foo:a,b").args.len(), 2);
        assert_eq!(one("#ns__foo!!(arg=1)").name, "ns/foo");
    }

    #[test]
    fn preserves_commas_and_equals_inside_quotes_and_text_blocks() {
        let call = one("#foo(text=[[a,b=c]], other=\"x,y=z\")");
        assert_eq!(call.args[0].value, "a,b=c");
        assert_eq!(call.args[1].value, "x,y=z");
    }

    #[test]
    fn marks_open_forms_without_parsing_required_args() {
        assert!(one("#foo(").is_open);
        assert!(one("#foo(arg=").is_open);
        assert!(one("#foo:").is_open);
    }
}
