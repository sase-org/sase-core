use regex::Regex;
use serde_yaml::{Mapping, Value};
use std::collections::HashSet;
use std::sync::OnceLock;

use super::directive::canonical_directive_name;
use super::token::DocumentSnapshot;
use super::wire::{
    DiagnosticSeverity, EditorDiagnostic, XpromptAssistEntry, XpromptInputHint,
};
use super::xprompt_args::{
    parse_xprompt_calls, ParsedXpromptArg, XpromptArgSyntax,
};

pub fn analyze_document(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut diagnostics = Vec::new();
    diagnostics.extend(frontmatter_diagnostics(document));
    diagnostics.extend(xprompt_diagnostics(document, entries));
    diagnostics.extend(slash_skill_diagnostics(document, entries));
    diagnostics.extend(directive_diagnostics(document));
    diagnostics.extend(argument_diagnostics(document, entries));
    diagnostics
}

fn frontmatter_diagnostics(
    document: &DocumentSnapshot,
) -> Vec<EditorDiagnostic> {
    let Some(frontmatter) = extract_frontmatter(document.text()) else {
        return Vec::new();
    };
    let Ok(value) = serde_yaml::from_str::<Value>(frontmatter.text) else {
        return Vec::new();
    };
    let invalid_values = invalid_frontmatter_input_type_values(&value);
    if invalid_values.is_empty() {
        return Vec::new();
    }

    let source_scan = scan_frontmatter_input_sources(frontmatter.text);
    let fallback_range = source_scan
        .fallback_range
        .unwrap_or((0, frontmatter.text.len()));
    let mut used_tokens = vec![false; source_scan.tokens.len()];
    let mut out = Vec::new();

    for invalid_value in invalid_values {
        let range = source_scan
            .tokens
            .iter()
            .enumerate()
            .find(|(idx, token)| {
                !used_tokens[*idx] && token.value == invalid_value
            })
            .map(|(idx, token)| {
                used_tokens[idx] = true;
                (token.start, token.end)
            })
            .unwrap_or(fallback_range);
        push_diagnostic(
            document,
            &mut out,
            frontmatter.start + range.0,
            frontmatter.start + range.1,
            "invalid_xprompt_frontmatter_input_type",
            format!(
                "Invalid xprompt input type `{invalid_value}`. Expected one of: {}",
                XPROMPT_INPUT_TYPE_EXPECTED
            ),
        );
    }

    out
}

const XPROMPT_INPUT_TYPE_EXPECTED: &str =
    "word, line, text, path, int/integer, bool/boolean, float";

#[derive(Debug, Clone, Copy)]
struct FrontmatterBlock<'a> {
    text: &'a str,
    start: usize,
}

#[derive(Debug, Clone)]
struct FrontmatterInputTypeToken {
    value: String,
    start: usize,
    end: usize,
}

#[derive(Debug, Default)]
struct FrontmatterInputSourceScan {
    tokens: Vec<FrontmatterInputTypeToken>,
    fallback_range: Option<(usize, usize)>,
}

#[derive(Debug, Clone, Copy)]
struct FrontmatterLine<'a> {
    start: usize,
    text: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowScanMode {
    ShortformDirect,
    TypeOnly,
}

fn extract_frontmatter(text: &str) -> Option<FrontmatterBlock<'_>> {
    let opening_line_end = text.find('\n')?;
    if text[..opening_line_end].trim_end_matches('\r') != "---" {
        return None;
    }

    let frontmatter_start = opening_line_end + 1;
    let mut line_start = frontmatter_start;
    while line_start <= text.len() {
        let line_end = text[line_start..]
            .find('\n')
            .map(|idx| line_start + idx)
            .unwrap_or(text.len());
        if text[line_start..line_end].trim_end_matches('\r') == "---" {
            return Some(FrontmatterBlock {
                text: &text[frontmatter_start..line_start],
                start: frontmatter_start,
            });
        }
        if line_end == text.len() {
            break;
        }
        line_start = line_end + 1;
    }
    None
}

fn invalid_frontmatter_input_type_values(value: &Value) -> Vec<String> {
    let Some(mapping) = value.as_mapping() else {
        return Vec::new();
    };
    let Some(input) = yaml_mapping_get(mapping, "input") else {
        return Vec::new();
    };

    if let Some(inputs) = input.as_mapping() {
        return inputs
            .values()
            .filter_map(invalid_shortform_input_type_value)
            .collect();
    }
    if let Some(inputs) = input.as_sequence() {
        return inputs
            .iter()
            .filter_map(|input| {
                input
                    .as_mapping()
                    .and_then(|mapping| yaml_mapping_get(mapping, "type"))
                    .and_then(invalid_input_type_value)
            })
            .collect();
    }
    Vec::new()
}

fn invalid_shortform_input_type_value(value: &Value) -> Option<String> {
    if let Some(mapping) = value.as_mapping() {
        return yaml_mapping_get(mapping, "type")
            .and_then(invalid_input_type_value);
    }
    invalid_input_type_value(value)
}

fn invalid_input_type_value(value: &Value) -> Option<String> {
    let raw = yaml_scalar_to_string(value)?;
    (!is_known_xprompt_input_type(&raw)).then_some(raw)
}

fn yaml_scalar_to_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        Some(value.to_string())
    } else if let Some(value) = value.as_i64() {
        Some(value.to_string())
    } else if let Some(value) = value.as_bool() {
        Some(value.to_string())
    } else {
        value.as_f64().map(|value| value.to_string())
    }
}

fn yaml_mapping_get<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a Value> {
    mapping.get(Value::String(key.to_string()))
}

fn is_known_xprompt_input_type(raw: &str) -> bool {
    matches!(
        raw.to_ascii_lowercase().as_str(),
        "word"
            | "line"
            | "text"
            | "path"
            | "int"
            | "integer"
            | "bool"
            | "boolean"
            | "float"
    )
}

fn scan_frontmatter_input_sources(
    frontmatter: &str,
) -> FrontmatterInputSourceScan {
    let lines = frontmatter_lines(frontmatter);
    let Some((input_line_idx, input_colon)) = top_level_input_line(&lines)
    else {
        return FrontmatterInputSourceScan::default();
    };
    let input_line = lines[input_line_idx];
    let input_indent = leading_whitespace_len(input_line.text);
    let block_end_idx =
        input_block_end_line(&lines, input_line_idx + 1, input_indent);
    let fallback_end = lines
        .get(block_end_idx)
        .map(|line| line.start)
        .unwrap_or(frontmatter.len());

    let mut scan = FrontmatterInputSourceScan {
        tokens: Vec::new(),
        fallback_range: Some((input_line.start, fallback_end)),
    };

    scan_inline_input_value(input_line, input_colon, &mut scan.tokens);
    scan_block_input_values(
        &lines[input_line_idx + 1..block_end_idx],
        input_indent,
        &mut scan.tokens,
    );

    scan
}

fn frontmatter_lines(text: &str) -> Vec<FrontmatterLine<'_>> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    while start < text.len() {
        let end = text[start..]
            .find('\n')
            .map(|idx| start + idx)
            .unwrap_or(text.len());
        lines.push(FrontmatterLine {
            start,
            text: text[start..end].trim_end_matches('\r'),
        });
        if end == text.len() {
            break;
        }
        start = end + 1;
    }
    lines
}

fn top_level_input_line(
    lines: &[FrontmatterLine<'_>],
) -> Option<(usize, usize)> {
    lines.iter().enumerate().find_map(|(idx, line)| {
        let trimmed = line.text.trim_start();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || leading_whitespace_len(line.text) != 0
        {
            return None;
        }
        let (key, colon) = yaml_line_key_colon(line.text)?;
        (key == "input").then_some((idx, colon))
    })
}

fn input_block_end_line(
    lines: &[FrontmatterLine<'_>],
    start_idx: usize,
    input_indent: usize,
) -> usize {
    lines[start_idx..]
        .iter()
        .position(|line| {
            let trimmed = line.text.trim_start();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && leading_whitespace_len(line.text) <= input_indent
        })
        .map(|offset| start_idx + offset)
        .unwrap_or(lines.len())
}

fn scan_inline_input_value(
    line: FrontmatterLine<'_>,
    colon: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let value_start = colon + 1;
    let Some(rest) = line.text.get(value_start..) else {
        return;
    };
    let trimmed_offset = leading_whitespace_len(rest);
    let trimmed = &rest[trimmed_offset..];
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return;
    }
    let value_offset = line.start + value_start + trimmed_offset;
    if trimmed.starts_with('{') {
        scan_flow_mapping(
            trimmed,
            value_offset,
            0,
            FlowScanMode::ShortformDirect,
            tokens,
        );
    } else if trimmed.starts_with('[') {
        scan_flow_type_fields(trimmed, value_offset, tokens);
    }
}

fn scan_block_input_values(
    lines: &[FrontmatterLine<'_>],
    input_indent: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let Some(child_indent) = lines
        .iter()
        .filter_map(|line| {
            let trimmed = line.text.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let indent = leading_whitespace_len(line.text);
            (indent > input_indent).then_some(indent)
        })
        .min()
    else {
        return;
    };

    for line in lines {
        scan_block_input_line(line, input_indent, child_indent, tokens);
    }
}

fn scan_block_input_line(
    line: &FrontmatterLine<'_>,
    input_indent: usize,
    child_indent: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let trimmed = line.text.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return;
    }
    let indent = leading_whitespace_len(line.text);
    if indent <= input_indent {
        return;
    }

    let content = &line.text[indent..];
    let content_start = line.start + indent;
    if let Some((item_offset, item)) = sequence_item_content(content) {
        scan_sequence_item(item, content_start + item_offset, tokens);
        return;
    }

    let Some((key, colon)) = yaml_line_key_colon(content) else {
        return;
    };
    let value_start = colon + 1;
    let value = &content[value_start..];
    let value_offset = content_start + value_start;

    if key == "type" {
        push_scalar_value_token(value, value_offset, tokens);
    } else if indent == child_indent {
        scan_shortform_child_value(value, value_offset, tokens);
    } else {
        scan_nested_flow_type_fields(value, value_offset, tokens);
    }
}

fn scan_sequence_item(
    item: &str,
    item_start: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let trimmed_offset = leading_whitespace_len(item);
    let trimmed = &item[trimmed_offset..];
    let trimmed_start = item_start + trimmed_offset;
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        scan_flow_type_fields(trimmed, trimmed_start, tokens);
        return;
    }
    let Some((key, colon)) = yaml_line_key_colon(trimmed) else {
        return;
    };
    let value_start = colon + 1;
    let value = &trimmed[value_start..];
    let value_offset = trimmed_start + value_start;
    if key == "type" {
        push_scalar_value_token(value, value_offset, tokens);
    } else {
        scan_nested_flow_type_fields(value, value_offset, tokens);
    }
}

fn scan_shortform_child_value(
    value: &str,
    value_offset: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let trimmed_offset = leading_whitespace_len(value);
    let trimmed = &value[trimmed_offset..];
    let trimmed_offset = value_offset + trimmed_offset;
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        scan_flow_type_fields(trimmed, trimmed_offset, tokens);
    } else {
        push_scalar_value_token(value, value_offset, tokens);
    }
}

fn scan_nested_flow_type_fields(
    value: &str,
    value_offset: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let trimmed_offset = leading_whitespace_len(value);
    let trimmed = &value[trimmed_offset..];
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        scan_flow_type_fields(trimmed, value_offset + trimmed_offset, tokens);
    }
}

fn scan_flow_type_fields(
    text: &str,
    base_offset: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let mut idx = 0usize;
    while idx < text.len() {
        let Some(ch) = text[idx..].chars().next() else {
            break;
        };
        if ch == '"' || ch == '\'' {
            idx = skip_quoted(text, idx).unwrap_or(text.len());
            continue;
        }
        if ch == '{' {
            scan_flow_mapping(
                text,
                base_offset,
                idx,
                FlowScanMode::TypeOnly,
                tokens,
            );
            idx = matching_flow_end(text, idx)
                .map(|end| end + 1)
                .unwrap_or(text.len());
            continue;
        }
        idx += ch.len_utf8();
    }
}

fn scan_flow_mapping(
    text: &str,
    base_offset: usize,
    open_idx: usize,
    mode: FlowScanMode,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let Some(close_idx) = matching_flow_end(text, open_idx) else {
        return;
    };
    let mut idx = open_idx + 1;
    while idx < close_idx {
        idx = skip_flow_separators(text, idx, close_idx);
        if idx >= close_idx {
            break;
        }
        let Some((key, colon)) = flow_key_colon(text, idx, close_idx) else {
            idx = skip_flow_value(text, idx, close_idx);
            continue;
        };
        let value_start = colon + 1;
        let value = &text[value_start..close_idx];
        let value_offset = base_offset + value_start;
        let trimmed_value_offset = leading_whitespace_len(value);
        let trimmed_value = &value[trimmed_value_offset..];
        let trimmed_base = value_offset + trimmed_value_offset;

        if key == "type" {
            push_scalar_value_token(value, value_offset, tokens);
        } else if trimmed_value.starts_with('{')
            || trimmed_value.starts_with('[')
        {
            scan_flow_type_fields(trimmed_value, trimmed_base, tokens);
        } else if mode == FlowScanMode::ShortformDirect {
            push_scalar_value_token(value, value_offset, tokens);
        }

        idx = skip_flow_value(text, value_start, close_idx);
        if text.get(idx..idx + 1) == Some(",") {
            idx += 1;
        }
    }
}

fn yaml_line_key_colon(line: &str) -> Option<(String, usize)> {
    let colon = line.find(':')?;
    let key = line[..colon].trim();
    if key.is_empty() || key.starts_with('#') || key.starts_with('-') {
        return None;
    }
    Some((unquote_yaml_key(key).to_string(), colon))
}

fn flow_key_colon(
    text: &str,
    start: usize,
    limit: usize,
) -> Option<(String, usize)> {
    let key_start = start + leading_whitespace_len(&text[start..limit]);
    if key_start >= limit {
        return None;
    }
    let first = text[key_start..].chars().next()?;
    let (key_end, key) = if first == '"' || first == '\'' {
        let after_quote = skip_quoted(text, key_start)?;
        let value = text
            [key_start + first.len_utf8()..after_quote - first.len_utf8()]
            .to_string();
        (after_quote, value)
    } else {
        let mut end = key_start;
        for (offset, ch) in text[key_start..limit].char_indices() {
            if ch == ':' || ch.is_whitespace() || ch == ',' || ch == '}' {
                break;
            }
            end = key_start + offset + ch.len_utf8();
        }
        if end == key_start {
            return None;
        }
        (end, text[key_start..end].trim().to_string())
    };
    let colon = key_end + leading_whitespace_len(text.get(key_end..limit)?);
    (colon < limit && text.get(colon..colon + 1) == Some(":"))
        .then_some((key, colon))
}

fn sequence_item_content(content: &str) -> Option<(usize, &str)> {
    let after_dash = content.strip_prefix('-')?;
    if after_dash.is_empty() {
        return Some((1, after_dash));
    }
    let whitespace_len = leading_whitespace_len(after_dash);
    if whitespace_len == 0 {
        return None;
    }
    Some((1 + whitespace_len, &after_dash[whitespace_len..]))
}

fn push_scalar_value_token(
    value: &str,
    value_offset: usize,
    tokens: &mut Vec<FrontmatterInputTypeToken>,
) {
    let Some((start, end, value)) = scalar_value_range(value) else {
        return;
    };
    tokens.push(FrontmatterInputTypeToken {
        value,
        start: value_offset + start,
        end: value_offset + end,
    });
}

fn scalar_value_range(value: &str) -> Option<(usize, usize, String)> {
    let start = leading_whitespace_len(value);
    let rest = &value[start..];
    if rest.is_empty() || rest.starts_with('#') {
        return None;
    }
    let first = rest.chars().next()?;
    if first == '"' || first == '\'' {
        let quote_end = skip_quoted(value, start)?;
        let content_start = start + first.len_utf8();
        let content_end = quote_end - first.len_utf8();
        return Some((
            content_start,
            content_end,
            value[content_start..content_end].to_string(),
        ));
    }
    if matches!(first, '{' | '[' | '|' | '>') {
        return None;
    }

    let mut end = value.len();
    for (offset, ch) in value[start..].char_indices() {
        if matches!(ch, ',' | '}' | ']' | '#') || ch == '\r' || ch == '\n' {
            end = start + offset;
            break;
        }
    }
    let trimmed = value[start..end].trim_end();
    if trimmed.is_empty() {
        return None;
    }
    let end = start + trimmed.len();
    Some((start, end, trimmed.to_string()))
}

fn leading_whitespace_len(text: &str) -> usize {
    text.len() - text.trim_start().len()
}

fn unquote_yaml_key(key: &str) -> &str {
    let Some(first) = key.chars().next() else {
        return key;
    };
    if !matches!(first, '"' | '\'') {
        return key;
    }
    key.strip_prefix(first)
        .and_then(|key| key.strip_suffix(first))
        .unwrap_or(key)
}

fn skip_flow_separators(text: &str, start: usize, limit: usize) -> usize {
    let mut idx = start;
    while idx < limit {
        let Some(ch) = text[idx..].chars().next() else {
            break;
        };
        if ch == ',' || ch.is_whitespace() {
            idx += ch.len_utf8();
        } else {
            break;
        }
    }
    idx
}

fn skip_flow_value(text: &str, start: usize, limit: usize) -> usize {
    let mut idx = start + leading_whitespace_len(&text[start..limit]);
    let mut stack: Vec<char> = Vec::new();
    while idx < limit {
        let Some(ch) = text[idx..].chars().next() else {
            break;
        };
        if ch == '"' || ch == '\'' {
            idx = skip_quoted(text, idx).unwrap_or(limit);
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' if stack.last() == Some(&ch) => {
                stack.pop();
            }
            ',' | '}' | ']' if stack.is_empty() => break,
            _ => {}
        }
        idx += ch.len_utf8();
    }
    idx
}

fn matching_flow_end(text: &str, open_idx: usize) -> Option<usize> {
    let open = text[open_idx..].chars().next()?;
    let close = match open {
        '{' => '}',
        '[' => ']',
        _ => return None,
    };
    let mut stack = vec![close];
    let mut idx = open_idx + open.len_utf8();
    while idx < text.len() {
        let ch = text[idx..].chars().next()?;
        if ch == '"' || ch == '\'' {
            idx = skip_quoted(text, idx)?;
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' if stack.last() == Some(&ch) => {
                stack.pop();
                if stack.is_empty() {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += ch.len_utf8();
    }
    None
}

fn skip_quoted(text: &str, quote_start: usize) -> Option<usize> {
    let quote = text[quote_start..].chars().next()?;
    if !matches!(quote, '"' | '\'') {
        return None;
    }
    let mut escaped = false;
    let mut idx = quote_start + quote.len_utf8();
    while idx < text.len() {
        let ch = text[idx..].chars().next()?;
        if quote == '"' && escaped {
            escaped = false;
            idx += ch.len_utf8();
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            idx += ch.len_utf8();
            continue;
        }
        idx += ch.len_utf8();
        if ch == quote {
            return Some(idx);
        }
    }
    None
}

fn xprompt_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in xprompt_ref_re().captures_iter(document.text()) {
        let Some(marker) = caps.name("marker") else {
            continue;
        };
        let Some(name_match) = caps.name("name") else {
            continue;
        };
        let name = name_match.as_str().replace("__", "/");
        let Some(entry) = entries.iter().find(|entry| entry.name == name)
        else {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), name_match.end())
            {
                out.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Warning,
                    code: "unknown_xprompt".to_string(),
                    message: format!("Unknown xprompt `{name}`"),
                });
            }
            continue;
        };
        if entry.reference_prefix != marker.as_str() {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), marker.end())
            {
                out.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Information,
                    code: "canonical_marker_mismatch".to_string(),
                    message: format!(
                        "`{}` is canonical for `{}`",
                        entry.reference_prefix, entry.name
                    ),
                });
            }
        }
    }
    out
}

fn slash_skill_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in slash_skill_re().captures_iter(document.text()) {
        let Some(skill) = caps.name("skill") else {
            continue;
        };
        if entries
            .iter()
            .any(|entry| entry.is_skill && entry.name == skill.as_str())
        {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(skill.start() - 1, skill.end())
        {
            out.push(EditorDiagnostic {
                range,
                severity: DiagnosticSeverity::Warning,
                code: "unknown_slash_skill".to_string(),
                message: format!("Unknown slash skill `/{}`", skill.as_str()),
            });
        }
    }
    out
}

fn directive_diagnostics(document: &DocumentSnapshot) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(document.text()) {
        let Some(name) = caps.name("name") else {
            continue;
        };
        if canonical_directive_name(name.as_str()).is_some() {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(name.start() - 1, name.end())
        {
            out.push(EditorDiagnostic {
                range,
                severity: DiagnosticSeverity::Information,
                code: "unknown_directive".to_string(),
                message: format!("Unknown directive `%{}`", name.as_str()),
            });
        }
    }
    out
}

fn argument_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for call in parse_xprompt_calls(document.text()) {
        let Some(entry) = entries.iter().find(|entry| entry.name == call.name)
        else {
            continue;
        };
        if matches!(call.syntax, XpromptArgSyntax::Malformed) {
            let Some((start, end)) = call.malformed_span else {
                continue;
            };
            push_diagnostic(
                document,
                &mut out,
                start,
                end,
                "malformed_xprompt_argument",
                "Malformed xprompt argument form".to_string(),
            );
            if entry.inputs.is_empty() || call.is_open {
                continue;
            }
        }
        if call.is_open || entry.inputs.is_empty() {
            continue;
        }
        validate_call_args(document, entry, &call, &mut out);
    }
    out
}

fn validate_call_args(
    document: &DocumentSnapshot,
    entry: &XpromptAssistEntry,
    call: &super::xprompt_args::ParsedXpromptCall,
    out: &mut Vec<EditorDiagnostic>,
) {
    let mut supplied_inputs = HashSet::new();
    let mut positional_inputs = HashSet::new();
    let mut seen_named_args = HashSet::new();
    let mut positional_index = 0usize;

    for arg in &call.args {
        if let Some(name) = &arg.name {
            if !seen_named_args.insert(name.value.clone()) {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "duplicate_xprompt_arg",
                    format!("Duplicate xprompt argument `{}`", name.value),
                );
                continue;
            }
            let Some(input) =
                entry.inputs.iter().find(|input| input.name == name.value)
            else {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "unknown_xprompt_arg",
                    format!(
                        "Unknown argument `{}` for xprompt `{}`",
                        name.value, entry.name
                    ),
                );
                continue;
            };
            if positional_inputs.contains(&input.name) {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "conflicting_xprompt_arg",
                    format!(
                        "Argument `{}` was already provided positionally",
                        input.name
                    ),
                );
            }
            supplied_inputs.insert(input.name.clone());
            validate_type(document, entry, input, arg, out);
        } else {
            let Some(input) = entry.inputs.get(positional_index) else {
                push_diagnostic(
                    document,
                    out,
                    arg.value_span.0,
                    arg.value_span.1,
                    "too_many_args",
                    format!(
                        "Too many positional arguments for `{}`",
                        entry.name
                    ),
                );
                positional_index += 1;
                continue;
            };
            positional_inputs.insert(input.name.clone());
            supplied_inputs.insert(input.name.clone());
            validate_type(document, entry, input, arg, out);
            positional_index += 1;
        }
    }

    for input in entry.inputs.iter().filter(|input| input.required) {
        if supplied_inputs.contains(&input.name) {
            continue;
        }
        push_diagnostic(
            document,
            out,
            call.name_span.0,
            call.name_span.1,
            "missing_required_arg",
            format!(
                "Missing required argument `{}` for xprompt `{}`",
                input.name, entry.name
            ),
        );
    }
}

fn validate_type(
    document: &DocumentSnapshot,
    entry: &XpromptAssistEntry,
    input: &XpromptInputHint,
    arg: &ParsedXpromptArg,
    out: &mut Vec<EditorDiagnostic>,
) {
    if arg.value == "null" || value_matches_input_type(&arg.value, input) {
        return;
    }
    push_diagnostic(
        document,
        out,
        arg.value_span.0,
        arg.value_span.1,
        "invalid_xprompt_arg_type",
        format!(
            "Argument `{}` for xprompt `{}` expects {}",
            input.name, entry.name, input.r#type
        ),
    );
}

fn value_matches_input_type(value: &str, input: &XpromptInputHint) -> bool {
    match input.r#type.as_str() {
        "word" | "path" => !value.chars().any(char::is_whitespace),
        "line" => !value.contains('\n'),
        "text" => true,
        "int" | "integer" => value.parse::<i64>().is_ok(),
        "float" => value.parse::<f64>().is_ok(),
        "bool" | "boolean" => matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
        ),
        _ => true,
    }
}

fn push_diagnostic(
    document: &DocumentSnapshot,
    out: &mut Vec<EditorDiagnostic>,
    start: usize,
    end: usize,
    code: &str,
    message: String,
) {
    let Some(range) = document.byte_range_to_range(start, end) else {
        return;
    };
    out.push(EditorDiagnostic {
        range,
        severity: DiagnosticSeverity::Error,
        code: code.to_string(),
        message,
    });
}

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

fn slash_skill_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?m)(?:^|\s)/(?P<skill>[A-Za-z0-9_]+)(?:\s|$)").unwrap()
    })
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?:%(?P<name>[A-Za-z_][A-Za-z0-9_]*))"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Vec<XpromptAssistEntry> {
        vec![
            XpromptAssistEntry {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: "#review".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
            XpromptAssistEntry {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: "#!run".to_string(),
                reference_prefix: "#!".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: true,
            },
            XpromptAssistEntry {
                name: "typed".to_string(),
                display_label: "typed".to_string(),
                insertion: "#typed".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![
                    input("path", "path", true, 0),
                    input("count", "int", true, 1),
                    input("enabled", "bool", false, 2),
                ],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
            XpromptAssistEntry {
                name: "ns/foo".to_string(),
                display_label: "ns/foo".to_string(),
                insertion: "#ns/foo".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![input("arg", "word", true, 0)],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
        ]
    }

    fn input(
        name: &str,
        r#type: &str,
        required: bool,
        position: u32,
    ) -> XpromptInputHint {
        XpromptInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            required,
            default_display: None,
            position,
        }
    }

    fn diagnostic_text(text: &str, diagnostic: &EditorDiagnostic) -> String {
        let doc = DocumentSnapshot::new(text);
        let start =
            doc.position_to_byte_offset(diagnostic.range.start).unwrap();
        let end = doc.position_to_byte_offset(diagnostic.range.end).unwrap();
        text[start..end].to_string()
    }

    #[test]
    fn reports_initial_diagnostics() {
        let doc = DocumentSnapshot::new("#missing #run /missing %wat");
        let diagnostics = analyze_document(&doc, &catalog());
        assert!(diagnostics.iter().any(|d| d.code == "unknown_xprompt"));
        assert!(diagnostics
            .iter()
            .any(|d| d.code == "canonical_marker_mismatch"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_slash_skill"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_directive"));
    }

    #[test]
    fn reports_xprompt_argument_contract_diagnostics() {
        for (text, code) in [
            ("#typed", "missing_required_arg"),
            ("#typed(src/main.rs)", "missing_required_arg"),
            ("#typed(src/main.rs, 3, true, extra)", "too_many_args"),
            (
                "#typed(path=src/main.rs, nope=1, count=3)",
                "unknown_xprompt_arg",
            ),
            ("#typed(path=a, path=b, count=3)", "duplicate_xprompt_arg"),
            (
                "#typed(src/main.rs, path=other, count=3)",
                "conflicting_xprompt_arg",
            ),
            (
                "#typed(path=\"bad value\", count=3)",
                "invalid_xprompt_arg_type",
            ),
            (
                "#typed(path=src/main.rs, count=nope)",
                "invalid_xprompt_arg_type",
            ),
            ("#typed:path+ ", "malformed_xprompt_argument"),
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                diagnostics.iter().any(|d| d.code == code),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn reports_shortform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  name: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn accepts_known_frontmatter_input_type_aliases() {
        let text = "---\ninput:\n  a: word\n  b: line\n  c: text\n  d: path\n  e: int\n  f: integer\n  g: bool\n  h: boolean\n  i: float\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());

        assert!(
            !diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_longform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  - name: foo\n    type: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn reports_flow_style_frontmatter_input_type_diagnostic() {
        let text = "---\ninput: {name: wordd}\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn ignores_malformed_or_missing_frontmatter() {
        for text in ["---\ninput: [\n---\nBody", "input:\n  name: wordd\nBody"]
        {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == "invalid_xprompt_frontmatter_input_type"
                }),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn accepts_valid_argument_forms_and_bool_spellings() {
        for text in [
            "#typed(path=src/main.rs, count=3, enabled=true)",
            "#typed(src/main.rs, 3, yes)",
            "#typed:src/main.rs,3,on",
            "#typed(path=null, count=null)",
            "#ns/foo(arg=hello)",
            "#ns__foo!!(arg=hello)",
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics
                    .iter()
                    .any(|d| d.severity == DiagnosticSeverity::Error),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn incomplete_forms_do_not_emit_required_arg_noise() {
        for text in ["#typed(", "#typed(path=", "#typed:"] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics.iter().any(|d| d.code == "missing_required_arg"),
                "{text}: {diagnostics:?}"
            );
        }
    }
}
