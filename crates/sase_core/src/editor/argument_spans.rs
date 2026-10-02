use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use crate::{
    fenced_block_ranges, inline_code_ranges, prompt_literal_zone_ranges,
};

use super::diagnostics::{
    macro_arg_value_unresolvable, validate_macro_call_args,
    MacroArgValidationKind,
};
use super::directive::{
    canonical_directive_name, directive_allows_keywords, directive_metadata,
};
use super::macro_args::{
    double_colon_payload_start, find_matching_paren_for_args,
    parse_macro_calls, parse_macro_like_call_at, top_level_commas_for_args,
    MacroArgSyntax, ParsedMacroCall,
};
use super::token::DocumentSnapshot;
use super::wire::{
    DirectiveSyntaxForm, MacroArgumentSource, MacroArgumentSpan,
    MacroArgumentSpanRole, MacroArgumentSpanValidity, MacroAssistEntry,
    MacroCallNameSpan,
};

type ValidityByArg = HashMap<usize, MacroArgumentSpanValidity>;

#[derive(Default)]
struct SpanSemantics {
    key_validity: ValidityByArg,
    value_validity: ValidityByArg,
}

/// Return structural xprompt and directive argument spans.
pub fn extract_macro_argument_spans(
    document: &DocumentSnapshot,
) -> Vec<MacroArgumentSpan> {
    extract_macro_argument_spans_inner(document, None)
}

/// Return structural argument spans plus catalog-derived validity.
pub fn extract_macro_argument_spans_with_catalog(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
) -> Vec<MacroArgumentSpan> {
    extract_macro_argument_spans_inner(document, Some(entries))
}

/// Return xprompt invocation and directive name spans.
pub fn extract_macro_call_name_spans(
    document: &DocumentSnapshot,
) -> Vec<MacroCallNameSpan> {
    let text = document.text();
    let literal_ranges = argument_literal_ranges(text);
    let mut spans = Vec::new();

    for call in parse_macro_calls(text) {
        let marker_start = macro_marker_start(text, &call);
        if ranges_intersect_any(
            (marker_start, call.name_span.1),
            &literal_ranges,
        ) {
            continue;
        }
        push_name_span(
            &mut spans,
            call.name_span,
            MacroArgumentSource::Macro,
            call.name,
        );
    }

    for (call, call_name) in directive_calls(text, &literal_ranges) {
        push_name_span(
            &mut spans,
            call.name_span,
            MacroArgumentSource::Directive,
            call_name,
        );
    }

    spans.retain(|span| {
        !ranges_intersect_any((span.start, span.end), &literal_ranges)
    });
    spans.sort_by_key(|span| {
        (
            span.start,
            span.end,
            match span.source {
                MacroArgumentSource::Macro => 0u8,
                MacroArgumentSource::Directive => 1u8,
            },
        )
    });
    spans
}

fn extract_macro_argument_spans_inner(
    document: &DocumentSnapshot,
    entries: Option<&[MacroAssistEntry]>,
) -> Vec<MacroArgumentSpan> {
    let text = document.text();
    let literal_ranges = argument_literal_ranges(text);
    let mut spans = Vec::new();

    for call in parse_macro_calls(text) {
        let marker_start = macro_marker_start(text, &call);
        if ranges_intersect_any(
            (marker_start, call.name_span.1),
            &literal_ranges,
        ) {
            continue;
        }
        let semantics = entries
            .and_then(|entries| {
                (!call.is_open).then(|| {
                    entries.iter().find(|entry| entry.name == call.name)
                })?
            })
            .filter(|entry| !entry.inputs.is_empty())
            .map(|entry| macro_semantics(entry, &call))
            .unwrap_or_default();
        emit_call_spans(
            text,
            &call,
            MacroArgumentSource::Macro,
            call.name.clone(),
            &semantics,
            &mut spans,
        );
    }

    for (call, call_name) in directive_calls(text, &literal_ranges) {
        let semantics = directive_semantics(&call);
        emit_call_spans(
            text,
            &call,
            MacroArgumentSource::Directive,
            call_name,
            &semantics,
            &mut spans,
        );
    }

    spans.retain(|span| {
        !ranges_intersect_any((span.start, span.end), &literal_ranges)
    });
    spans.sort_by_key(|span| (span.start, span.end, role_rank(span.role)));
    spans
}

fn macro_semantics(
    entry: &MacroAssistEntry,
    call: &ParsedMacroCall,
) -> SpanSemantics {
    let mut semantics = SpanSemantics::default();
    for validation in validate_macro_call_args(entry, call) {
        let Some(arg_index) = validation.arg_index else {
            continue;
        };
        match validation.kind {
            MacroArgValidationKind::DuplicateKey => {
                semantics
                    .key_validity
                    .insert(arg_index, MacroArgumentSpanValidity::DuplicateKey);
            }
            MacroArgValidationKind::UnknownKey => {
                semantics
                    .key_validity
                    .insert(arg_index, MacroArgumentSpanValidity::UnknownKey);
            }
            MacroArgValidationKind::TypeMismatch => {
                semantics
                    .value_validity
                    .insert(arg_index, MacroArgumentSpanValidity::TypeMismatch);
            }
            MacroArgValidationKind::TooManyArgs
            | MacroArgValidationKind::MissingRequiredArg => {}
        }
    }
    semantics
}

fn directive_semantics(call: &ParsedMacroCall) -> SpanSemantics {
    let mut semantics = SpanSemantics::default();
    if call.is_open {
        return semantics;
    }
    let Some(canonical) = canonical_directive_name(&call.name) else {
        return semantics;
    };
    let Some(metadata) = directive_metadata(canonical) else {
        return semantics;
    };
    let Some(syntax_form) = directive_syntax_form(call.syntax) else {
        return semantics;
    };
    if !directive_allows_keywords(metadata, syntax_form) {
        return semantics;
    }

    let mut seen = HashSet::new();
    for (arg_index, arg) in call.args.iter().enumerate() {
        let Some(name) = &arg.name else {
            continue;
        };
        let keyword = metadata
            .keywords
            .iter()
            .find(|item| item.name == name.value);
        let known =
            keyword.is_some() || metadata.dynamic_keyword_role.is_some();
        if !known {
            semantics
                .key_validity
                .insert(arg_index, MacroArgumentSpanValidity::UnknownKey);
            continue;
        }
        let repeatable = keyword.is_some_and(|item| item.repeatable);
        if !repeatable && !seen.insert(name.value.clone()) {
            semantics
                .key_validity
                .insert(arg_index, MacroArgumentSpanValidity::DuplicateKey);
        }
    }
    semantics
}

fn emit_call_spans(
    text: &str,
    call: &ParsedMacroCall,
    source: MacroArgumentSource,
    call_name: String,
    semantics: &SpanSemantics,
    out: &mut Vec<MacroArgumentSpan>,
) {
    let suffix_start = call_suffix_start(text, call, source);
    match call.syntax {
        MacroArgSyntax::None => {}
        MacroArgSyntax::Plus => push_span(
            out,
            suffix_start,
            suffix_start + 1,
            MacroArgumentSpanRole::ArgDelimiter,
            MacroArgumentSpanValidity::Ok,
            source,
            &call_name,
        ),
        MacroArgSyntax::Colon => {
            push_colon_delimiter(
                text,
                suffix_start,
                false,
                source,
                &call_name,
                out,
            );
            if text
                .get(suffix_start + 1..suffix_start + 2)
                .is_some_and(|value| value != " ")
            {
                push_commas_between_args(text, call, source, &call_name, out);
            }
        }
        MacroArgSyntax::DoubleColonText => push_colon_delimiter(
            text,
            suffix_start,
            true,
            source,
            &call_name,
            out,
        ),
        MacroArgSyntax::Parenthesized => {
            emit_parenthesized_delimiters(
                text,
                suffix_start,
                source,
                &call_name,
                out,
            );
        }
        MacroArgSyntax::Malformed => {
            if text.as_bytes().get(suffix_start) == Some(&b':') {
                push_colon_delimiter(
                    text,
                    suffix_start,
                    false,
                    source,
                    &call_name,
                    out,
                );
            }
        }
    }
    emit_arg_spans(text, call, source, call_name, semantics, out);
}

fn emit_parenthesized_delimiters(
    text: &str,
    open_idx: usize,
    source: MacroArgumentSource,
    call_name: &str,
    out: &mut Vec<MacroArgumentSpan>,
) {
    if text.as_bytes().get(open_idx) != Some(&b'(') {
        return;
    }
    push_span(
        out,
        open_idx,
        open_idx + 1,
        MacroArgumentSpanRole::ArgDelimiter,
        MacroArgumentSpanValidity::Ok,
        source,
        call_name,
    );
    let close_idx = find_matching_paren_for_args(text, open_idx);
    let body_end = close_idx.unwrap_or(text.len());
    for comma in top_level_commas_for_args(text, open_idx + 1, body_end) {
        push_span(
            out,
            comma,
            comma + 1,
            MacroArgumentSpanRole::ArgDelimiter,
            MacroArgumentSpanValidity::Ok,
            source,
            call_name,
        );
    }
    let Some(close_idx) = close_idx else {
        return;
    };
    push_span(
        out,
        close_idx,
        close_idx + 1,
        MacroArgumentSpanRole::ArgDelimiter,
        MacroArgumentSpanValidity::Ok,
        source,
        call_name,
    );

    let after_close = close_idx + 1;
    if double_colon_payload_start(text, after_close).is_some() {
        push_span(
            out,
            after_close,
            after_close + 2,
            MacroArgumentSpanRole::ArgDelimiter,
            MacroArgumentSpanValidity::Ok,
            source,
            call_name,
        );
    } else if text
        .get(after_close..)
        .is_some_and(|after| after.starts_with(':'))
    {
        push_span(
            out,
            after_close,
            after_close + 1,
            MacroArgumentSpanRole::ArgDelimiter,
            MacroArgumentSpanValidity::Ok,
            source,
            call_name,
        );
    }
}

fn emit_arg_spans(
    text: &str,
    call: &ParsedMacroCall,
    source: MacroArgumentSource,
    call_name: String,
    semantics: &SpanSemantics,
    out: &mut Vec<MacroArgumentSpan>,
) {
    for (index, arg) in call.args.iter().enumerate() {
        if let Some(name) = &arg.name {
            push_span(
                out,
                name.span.0,
                name.span.1,
                MacroArgumentSpanRole::ArgKey,
                semantics
                    .key_validity
                    .get(&index)
                    .copied()
                    .unwrap_or(MacroArgumentSpanValidity::Ok),
                source,
                &call_name,
            );
            if let Some((start, end)) =
                assign_span(text, name.span.1, arg.value_span.0)
            {
                push_span(
                    out,
                    start,
                    end,
                    MacroArgumentSpanRole::ArgAssign,
                    MacroArgumentSpanValidity::Ok,
                    source,
                    &call_name,
                );
            }
        }
        if arg.value_span.0 < arg.value_span.1 {
            let role = value_role(text, arg.value_span, &arg.value);
            let validity = if macro_arg_value_unresolvable(&arg.value) {
                MacroArgumentSpanValidity::Unresolvable
            } else {
                semantics
                    .value_validity
                    .get(&index)
                    .copied()
                    .unwrap_or(MacroArgumentSpanValidity::Ok)
            };
            push_span(
                out,
                arg.value_span.0,
                arg.value_span.1,
                role,
                validity,
                source,
                &call_name,
            );
        }
    }
}

fn push_colon_delimiter(
    text: &str,
    start: usize,
    double_colon: bool,
    source: MacroArgumentSource,
    call_name: &str,
    out: &mut Vec<MacroArgumentSpan>,
) {
    let width = if double_colon
        && text.as_bytes().get(start..start + 2) == Some(b"::")
    {
        2
    } else {
        1
    };
    push_span(
        out,
        start,
        start + width,
        MacroArgumentSpanRole::ArgDelimiter,
        MacroArgumentSpanValidity::Ok,
        source,
        call_name,
    );
}

fn push_commas_between_args(
    text: &str,
    call: &ParsedMacroCall,
    source: MacroArgumentSource,
    call_name: &str,
    out: &mut Vec<MacroArgumentSpan>,
) {
    for pair in call.args.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        let start = left.value_span.1.min(right.value_span.0);
        let end = left.value_span.1.max(right.value_span.0);
        if let Some(comma) = text
            .get(start..end)
            .and_then(|gap| gap.find(',').map(|offset| start + offset))
        {
            push_span(
                out,
                comma,
                comma + 1,
                MacroArgumentSpanRole::ArgDelimiter,
                MacroArgumentSpanValidity::Ok,
                source,
                call_name,
            );
        }
    }
}

fn push_name_span(
    out: &mut Vec<MacroCallNameSpan>,
    span: (usize, usize),
    source: MacroArgumentSource,
    call_name: String,
) {
    if span.0 >= span.1 {
        return;
    }
    out.push(MacroCallNameSpan {
        start: span.0,
        end: span.1,
        source,
        call_name,
    });
}

fn push_span(
    out: &mut Vec<MacroArgumentSpan>,
    start: usize,
    end: usize,
    role: MacroArgumentSpanRole,
    validity: MacroArgumentSpanValidity,
    source: MacroArgumentSource,
    call_name: &str,
) {
    if start >= end {
        return;
    }
    out.push(MacroArgumentSpan {
        start,
        end,
        role,
        validity,
        source,
        call_name: call_name.to_string(),
    });
}

fn directive_calls(
    text: &str,
    literal_ranges: &[(usize, usize)],
) -> Vec<(ParsedMacroCall, String)> {
    let mut calls = Vec::new();
    for caps in directive_re().captures_iter(text) {
        let Some(marker) = caps.name("marker") else {
            continue;
        };
        if ranges_intersect_any((marker.start(), marker.end()), literal_ranges)
        {
            continue;
        }
        let Some(mut call) = parse_directive_call_at(text, marker.start())
        else {
            continue;
        };
        let call_name = canonical_directive_name(&call.name)
            .map(str::to_string)
            .unwrap_or_else(|| call.name.clone());
        call.name = call_name.clone();
        calls.push((call, call_name));
    }
    calls
}

fn parse_directive_call_at(
    text: &str,
    marker_start: usize,
) -> Option<ParsedMacroCall> {
    if text.as_bytes().get(marker_start) != Some(&b'%') {
        return None;
    }
    parse_macro_like_call_at(text, marker_start, 1, false)
}

fn argument_literal_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = prompt_literal_zone_ranges(text);
    let fenced_ranges = fenced_block_ranges(text);
    let inline_ranges = inline_code_ranges(text, &fenced_ranges);
    let protected_literal_ranges = fenced_ranges
        .iter()
        .chain(inline_ranges.iter())
        .copied()
        .collect::<Vec<_>>();

    for call_range in code_directive_call_ranges_for_highlight(text) {
        if ranges_intersect_any(call_range, &protected_literal_ranges) {
            continue;
        }
        ranges = ranges
            .into_iter()
            .flat_map(|range| subtract_range(range, call_range))
            .collect();
    }

    ranges
}

fn code_directive_call_ranges_for_highlight(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    for caps in directive_re().captures_iter(text) {
        let Some(marker) = caps.name("marker") else {
            continue;
        };
        let Some(name) = caps.name("name") else {
            continue;
        };
        let canonical = canonical_directive_name(name.as_str())
            .unwrap_or_else(|| name.as_str());
        if !matches!(canonical, "if" | "proc") {
            continue;
        }
        let Some(call) = parse_directive_call_at(text, marker.start()) else {
            continue;
        };
        ranges.push((marker.start(), directive_call_end(text, &call)));
    }
    ranges
}

fn directive_call_end(text: &str, call: &ParsedMacroCall) -> usize {
    let suffix_start =
        call_suffix_start(text, call, MacroArgumentSource::Directive);
    match call.syntax {
        MacroArgSyntax::None => call.name_span.1,
        MacroArgSyntax::Plus => suffix_start + 1,
        MacroArgSyntax::Parenthesized => {
            find_matching_paren_for_args(text, suffix_start)
                .map(|close| close + 1)
                .unwrap_or_else(|| text.len())
        }
        MacroArgSyntax::Colon | MacroArgSyntax::DoubleColonText => call
            .args
            .last()
            .map(|arg| arg.value_span.1)
            .unwrap_or(suffix_start + 1),
        MacroArgSyntax::Malformed => call
            .malformed_span
            .map(|span| span.1)
            .unwrap_or(call.name_span.1),
    }
}

fn subtract_range(
    range: (usize, usize),
    remove: (usize, usize),
) -> Vec<(usize, usize)> {
    if !ranges_intersect(range, remove) {
        return vec![range];
    }
    let mut pieces = Vec::new();
    if range.0 < remove.0 {
        pieces.push((range.0, remove.0.min(range.1)));
    }
    if remove.1 < range.1 {
        pieces.push((remove.1.max(range.0), range.1));
    }
    pieces
}

fn macro_marker_start(text: &str, call: &ParsedMacroCall) -> usize {
    if call.name_span.0 >= 2
        && text.get(call.name_span.0 - 2..call.name_span.0) == Some("#!")
    {
        call.name_span.0 - 2
    } else {
        call.name_span.0.saturating_sub(1)
    }
}

fn call_suffix_start(
    text: &str,
    call: &ParsedMacroCall,
    source: MacroArgumentSource,
) -> usize {
    let mut start = call.name_span.1;
    if source == MacroArgumentSource::Macro
        && text
            .get(start..start + 2)
            .is_some_and(|suffix| suffix == "!!" || suffix == "??")
    {
        start += 2;
    }
    start
}

fn directive_syntax_form(
    syntax: MacroArgSyntax,
) -> Option<DirectiveSyntaxForm> {
    match syntax {
        MacroArgSyntax::Plus => Some(DirectiveSyntaxForm::Plus),
        MacroArgSyntax::Colon => Some(DirectiveSyntaxForm::Colon),
        MacroArgSyntax::DoubleColonText => {
            Some(DirectiveSyntaxForm::DoubleColon)
        }
        MacroArgSyntax::Parenthesized => {
            Some(DirectiveSyntaxForm::Parenthesized)
        }
        MacroArgSyntax::None | MacroArgSyntax::Malformed => None,
    }
}

fn assign_span(
    text: &str,
    after_key: usize,
    before_value: usize,
) -> Option<(usize, usize)> {
    text.get(after_key..before_value)
        .and_then(|gap| gap.find('=').map(|offset| after_key + offset))
        .map(|start| (start, start + 1))
}

fn value_role(
    text: &str,
    span: (usize, usize),
    decoded_value: &str,
) -> MacroArgumentSpanRole {
    let raw = text.get(span.0..span.1).unwrap_or("").trim();
    if is_string_literal(raw) {
        return MacroArgumentSpanRole::ArgValueString;
    }
    let lower = decoded_value.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
    ) {
        return MacroArgumentSpanRole::ArgValueBool;
    }
    if decoded_value.parse::<i64>().is_ok()
        || decoded_value.parse::<f64>().is_ok()
    {
        return MacroArgumentSpanRole::ArgValueNumber;
    }
    MacroArgumentSpanRole::ArgValue
}

fn is_string_literal(raw: &str) -> bool {
    if raw.starts_with("[[") && raw.ends_with("]]") {
        return true;
    }
    if raw.len() < 2 {
        return false;
    }
    let bytes = raw.as_bytes();
    matches!(
        (bytes.first(), bytes.last()),
        (Some(b'"'), Some(b'"'))
            | (Some(b'\''), Some(b'\''))
            | (Some(b'`'), Some(b'`'))
    )
}

fn ranges_intersect_any(
    span: (usize, usize),
    ranges: &[(usize, usize)],
) -> bool {
    ranges.iter().any(|range| ranges_intersect(span, *range))
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

fn role_rank(role: MacroArgumentSpanRole) -> u8 {
    match role {
        MacroArgumentSpanRole::ArgDelimiter => 0,
        MacroArgumentSpanRole::ArgKey => 1,
        MacroArgumentSpanRole::ArgAssign => 2,
        MacroArgumentSpanRole::ArgValue => 3,
        MacroArgumentSpanRole::ArgValueString => 4,
        MacroArgumentSpanRole::ArgValueNumber => 5,
        MacroArgumentSpanRole::ArgValueBool => 6,
    }
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>%(?P<name>[A-Za-z_][A-Za-z0-9_]*))"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::super::wire::MacroInputHint;
    use super::*;

    fn spans(text: &str) -> Vec<MacroArgumentSpan> {
        extract_macro_argument_spans(&DocumentSnapshot::new(text))
    }

    fn catalog_spans(text: &str) -> Vec<MacroArgumentSpan> {
        extract_macro_argument_spans_with_catalog(
            &DocumentSnapshot::new(text),
            &catalog(),
        )
    }

    fn catalog() -> Vec<MacroAssistEntry> {
        vec![
            entry(
                "typed",
                vec![
                    input("path", "path", true, 0, false),
                    input("count", "int", true, 1, false),
                    input("enabled", "bool", false, 2, false),
                ],
            ),
            entry("merge", vec![input("names", "agent", false, 0, true)]),
        ]
    }

    fn entry(name: &str, inputs: Vec<MacroInputHint>) -> MacroAssistEntry {
        MacroAssistEntry {
            name: name.to_string(),
            display_label: name.to_string(),
            insertion: format!("#{name}"),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "test".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs,
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
        }
    }

    fn input(
        name: &str,
        r#type: &str,
        required: bool,
        position: u32,
        repeatable: bool,
    ) -> MacroInputHint {
        MacroInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            description: None,
            required,
            default_display: None,
            position,
            repeatable,
        }
    }

    fn span_text<'a>(text: &'a str, span: &MacroArgumentSpan) -> &'a str {
        &text[span.start..span.end]
    }

    fn assert_has(
        text: &str,
        spans: &[MacroArgumentSpan],
        role: MacroArgumentSpanRole,
        value: &str,
    ) {
        assert!(
            spans
                .iter()
                .any(|span| span.role == role && span_text(text, span) == value),
            "missing {role:?} {value:?}: {spans:?}"
        );
    }

    #[test]
    fn emits_spans_for_every_xprompt_argument_syntax() {
        let text =
            "#foo+ #foo:one,two\n#foo:: body\n#foo(a=1): tail\n\n#foo():: block";
        let spans = spans(text);

        for delimiter in ["+", ":", ",", "::", "(", ")", ":"] {
            assert_has(
                text,
                &spans,
                MacroArgumentSpanRole::ArgDelimiter,
                delimiter,
            );
        }
        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "a");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgAssign, "=");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgValue, "tail");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgValue, "block");
    }

    #[test]
    fn double_colon_eol_emits_two_byte_delimiter_and_next_line_value() {
        let text = "#foo(a=1)::\none\ntwo";
        let found = spans(text);
        assert_has(text, &found, MacroArgumentSpanRole::ArgDelimiter, "::");
        assert_has(text, &found, MacroArgumentSpanRole::ArgValue, "one\ntwo");
        let gap = text.find("::").unwrap() + 2;
        assert!(
            !found.iter().any(|span| span.start <= gap && gap < span.end),
            "{found:?}"
        );

        let bare = "#foo::\none";
        let bare_spans = spans(bare);
        assert_has(
            bare,
            &bare_spans,
            MacroArgumentSpanRole::ArgDelimiter,
            "::",
        );
        assert_has(bare, &bare_spans, MacroArgumentSpanRole::ArgValue, "one");
    }

    #[test]
    fn emits_name_spans_for_xprompts_directives_and_aliases() {
        let text =
            "#foo #bar:value #baz:: body\n%q(capacity=2)\n```\n#nope %wait\n```";
        let spans = extract_macro_call_name_spans(&DocumentSnapshot::new(text));
        let actual = spans
            .into_iter()
            .map(|span| {
                (
                    text[span.start..span.end].to_string(),
                    span.source,
                    span.call_name,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                (
                    "foo".to_string(),
                    MacroArgumentSource::Macro,
                    "foo".to_string()
                ),
                (
                    "bar".to_string(),
                    MacroArgumentSource::Macro,
                    "bar".to_string()
                ),
                (
                    "baz".to_string(),
                    MacroArgumentSource::Macro,
                    "baz".to_string()
                ),
                (
                    "q".to_string(),
                    MacroArgumentSource::Directive,
                    "queue".to_string()
                ),
            ]
        );
    }

    #[test]
    fn keeps_complex_values_intact() {
        let text = "#foo(query=a=b, quoted=\"a,b(c)\", block=[[a,b(c)]], nested=call(a,b))\n#foo: first line\nsecond line";
        let spans = spans(text);

        assert_has(text, &spans, MacroArgumentSpanRole::ArgValue, "a=b");
        assert_has(
            text,
            &spans,
            MacroArgumentSpanRole::ArgValueString,
            "\"a,b(c)\"",
        );
        assert_has(
            text,
            &spans,
            MacroArgumentSpanRole::ArgValueString,
            "[[a,b(c)]]",
        );
        assert_has(text, &spans, MacroArgumentSpanRole::ArgValue, "call(a,b)");
        assert_has(
            text,
            &spans,
            MacroArgumentSpanRole::ArgValue,
            "first line\nsecond line",
        );
    }

    #[test]
    fn marks_unterminated_calls_structurally() {
        let text = "#foo(key=42, other=true";
        let open_spans = spans(text);

        assert_has(text, &open_spans, MacroArgumentSpanRole::ArgDelimiter, "(");
        assert_has(text, &open_spans, MacroArgumentSpanRole::ArgDelimiter, ",");
        assert_has(text, &open_spans, MacroArgumentSpanRole::ArgKey, "key");
        assert_has(text, &open_spans, MacroArgumentSpanRole::ArgAssign, "=");
        assert_has(
            text,
            &open_spans,
            MacroArgumentSpanRole::ArgValueNumber,
            "42",
        );
        assert_has(text, &open_spans, MacroArgumentSpanRole::ArgKey, "other");
        assert_has(
            text,
            &open_spans,
            MacroArgumentSpanRole::ArgValueBool,
            "true",
        );

        let empty_value = "#foo(key=";
        let empty_spans = spans(empty_value);
        assert_has(
            empty_value,
            &empty_spans,
            MacroArgumentSpanRole::ArgKey,
            "key",
        );
        assert_has(
            empty_value,
            &empty_spans,
            MacroArgumentSpanRole::ArgAssign,
            "=",
        );
        assert!(
            !empty_spans
                .iter()
                .any(|span| span.role == MacroArgumentSpanRole::ArgValue),
            "{empty_spans:?}"
        );
    }

    #[test]
    fn unterminated_calls_preserve_utf8_and_multiline_text_blocks() {
        let text = "é prefix #foo(café=[[one,\ntwo]], other=δ";
        let spans = spans(text);

        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "café");
        assert_has(
            text,
            &spans,
            MacroArgumentSpanRole::ArgValueString,
            "[[one,\ntwo]]",
        );
        assert_has(text, &spans, MacroArgumentSpanRole::ArgDelimiter, ",");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "other");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgValue, "δ");
        for span in spans {
            assert!(
                text.is_char_boundary(span.start)
                    && text.is_char_boundary(span.end),
                "{span:?}"
            );
        }
    }

    #[test]
    fn skips_calls_inside_fenced_blocks() {
        let text = "```\n#foo(a=1)\n```\n#foo(b=2)";
        let spans = spans(text);

        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "b");
        assert!(
            !spans
                .iter()
                .any(|span| span.role == MacroArgumentSpanRole::ArgKey
                    && span_text(text, span) == "a"),
            "{spans:?}"
        );
    }

    #[test]
    fn highlights_static_if_directive_arguments_outside_literals() {
        let text = "%if(should_run=false)";
        let spans = spans(text);
        let names = extract_macro_call_name_spans(&DocumentSnapshot::new(text));

        assert_eq!(
            names
                .iter()
                .map(|span| (
                    &text[span.start..span.end],
                    span.source,
                    span.call_name.as_str(),
                ))
                .collect::<Vec<_>>(),
            vec![("if", MacroArgumentSource::Directive, "if")]
        );
        assert_has(text, &spans, MacroArgumentSpanRole::ArgDelimiter, "(");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "should_run");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgAssign, "=");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgValueBool, "false");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgDelimiter, ")");
    }

    #[test]
    fn still_skips_static_if_directives_inside_code_literals() {
        let text = "```\n%if(should_run=false)\n```\n`%if(should_run=false)`";
        let spans = spans(text);
        let names = extract_macro_call_name_spans(&DocumentSnapshot::new(text));

        assert!(spans.is_empty(), "{spans:?}");
        assert!(names.is_empty(), "{names:?}");
    }

    #[test]
    fn fills_catalog_aware_validity() {
        let text =
            "#typed(path=a, nope=1, path=b, count=nope, enabled={{ flag }})";
        let spans = catalog_spans(text);

        let key_validity = |value| {
            spans
                .iter()
                .find(|span| {
                    span.role == MacroArgumentSpanRole::ArgKey
                        && span_text(text, span) == value
                })
                .map(|span| span.validity)
        };
        assert_eq!(
            key_validity("nope"),
            Some(MacroArgumentSpanValidity::UnknownKey)
        );
        let duplicate_path = spans
            .iter()
            .rfind(|span| {
                span.role == MacroArgumentSpanRole::ArgKey
                    && span_text(text, span) == "path"
            })
            .unwrap();
        assert_eq!(
            duplicate_path.validity,
            MacroArgumentSpanValidity::DuplicateKey
        );
        let mismatch = spans
            .iter()
            .find(|span| {
                span.role == MacroArgumentSpanRole::ArgValue
                    && span_text(text, span) == "nope"
            })
            .unwrap();
        assert_eq!(mismatch.validity, MacroArgumentSpanValidity::TypeMismatch);
        let unresolvable = spans
            .iter()
            .find(|span| span_text(text, span) == "{{ flag }}")
            .unwrap();
        assert_eq!(
            unresolvable.validity,
            MacroArgumentSpanValidity::Unresolvable
        );
    }

    #[test]
    fn closed_calls_keep_validity_but_open_calls_stay_structural() {
        let closed = catalog_spans("#typed(nope=1, count=nope)");
        let closed_unknown = closed
            .iter()
            .find(|span| {
                span_text("#typed(nope=1, count=nope)", span) == "nope"
            })
            .unwrap();
        assert_eq!(
            closed_unknown.validity,
            MacroArgumentSpanValidity::UnknownKey
        );
        let closed_mismatch = closed
            .iter()
            .find(|span| {
                span.role == MacroArgumentSpanRole::ArgValue
                    && span_text("#typed(nope=1, count=nope)", span) == "nope"
            })
            .unwrap();
        assert_eq!(
            closed_mismatch.validity,
            MacroArgumentSpanValidity::TypeMismatch
        );

        let open_text = "#typed(nope=1, count=nope";
        let open = catalog_spans(open_text);
        assert_has(open_text, &open, MacroArgumentSpanRole::ArgKey, "nope");
        assert_has(open_text, &open, MacroArgumentSpanRole::ArgKey, "count");
        assert!(open
            .iter()
            .all(|span| span.validity == MacroArgumentSpanValidity::Ok));
    }

    #[test]
    fn repeatable_tail_uses_shared_positional_binding() {
        let text = "#merge(planner, coder)";
        let spans = catalog_spans(text);

        assert!(spans.iter().all(|span| {
            span_text(text, span) != "coder"
                || span.validity == MacroArgumentSpanValidity::Ok
        }));
    }

    #[test]
    fn emits_directive_argument_spans() {
        let text = "%id(worker, tribe=research, nope=x, tribe=ops)";
        let spans = spans(text);

        assert_has(text, &spans, MacroArgumentSpanRole::ArgKey, "tribe");
        assert_has(text, &spans, MacroArgumentSpanRole::ArgAssign, "=");
        let unknown = spans
            .iter()
            .find(|span| span_text(text, span) == "nope")
            .unwrap();
        assert_eq!(unknown.source, MacroArgumentSource::Directive);
        assert_eq!(unknown.validity, MacroArgumentSpanValidity::UnknownKey);
        let duplicate = spans
            .iter()
            .rfind(|span| span_text(text, span) == "tribe")
            .unwrap();
        assert_eq!(duplicate.validity, MacroArgumentSpanValidity::DuplicateKey);
    }

    #[test]
    fn open_directive_arguments_are_structural_without_invalidity() {
        let text = "%queue(capacity=2, priority=";
        let directive_spans = spans(text);

        assert_has(
            text,
            &directive_spans,
            MacroArgumentSpanRole::ArgDelimiter,
            "(",
        );
        assert_has(
            text,
            &directive_spans,
            MacroArgumentSpanRole::ArgKey,
            "capacity",
        );
        assert_has(
            text,
            &directive_spans,
            MacroArgumentSpanRole::ArgValueNumber,
            "2",
        );
        assert_has(
            text,
            &directive_spans,
            MacroArgumentSpanRole::ArgDelimiter,
            ",",
        );
        assert_has(
            text,
            &directive_spans,
            MacroArgumentSpanRole::ArgKey,
            "priority",
        );
        assert!(directive_spans.iter().all(|span| {
            span.source == MacroArgumentSource::Directive
                && span.validity == MacroArgumentSpanValidity::Ok
        }));

        let unknown = "%id(nope=";
        let unknown_spans = spans(unknown);
        let key = unknown_spans
            .iter()
            .find(|span| span_text(unknown, span) == "nope")
            .unwrap();
        assert_eq!(key.validity, MacroArgumentSpanValidity::Ok);
    }
}
