use super::parse_artifact_ref;
use super::wire::{
    ArtifactRefPromptCandidateWire, ArtifactRefSpanWire,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
};

const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?', ')'];

pub fn scan_artifact_refs(text: &str) -> Vec<ArtifactRefPromptCandidateWire> {
    let mut candidates = Vec::new();
    for (start, character) in text.char_indices() {
        if character != '@' || !has_allowed_left_context(text, start) {
            continue;
        }
        let raw_end = text[start + 1..]
            .char_indices()
            .find_map(|(offset, character)| {
                (character.is_whitespace()
                    || matches!(character, '"' | '\'' | '`'))
                .then_some(start + 1 + offset)
            })
            .unwrap_or(text.len());
        let end = trim_candidate_end(text, start + 1, raw_end);
        if end <= start + 1 {
            continue;
        }

        let reference = &text[start + 1..end];
        let Some(separator_offset) = reference.find(':') else {
            continue;
        };
        let separator = start + 1 + separator_offset;
        let kind_start = start + 1;
        let kind_end = separator;
        let kind = &text[kind_start..kind_end];
        let payload_start = separator + 1;

        if text[payload_start..].starts_with('"') {
            candidates.push(scan_quoted_candidate(
                text,
                start,
                kind_start,
                kind_end,
                separator,
                payload_start,
                kind,
            ));
            continue;
        }

        let fragment_start = if kind == "bug" {
            None
        } else {
            text[payload_start..end]
                .find('#')
                .map(|offset| payload_start + offset)
        };
        let payload_end = fragment_start.unwrap_or(end);

        candidates.push(ArtifactRefPromptCandidateWire {
            schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
            text: text[start..end].to_string(),
            reference: reference.to_string(),
            kind: kind.to_string(),
            well_formed: parse_artifact_ref(reference).is_ok(),
            candidate_span: span(start, end),
            sigil_span: span(start, start + 1),
            kind_span: span(kind_start, kind_end),
            separator_span: span(separator, separator + 1),
            payload_span: span(payload_start, payload_end),
            fragment_span: fragment_start.map(|start| span(start, end)),
            quoted: false,
        });
    }
    candidates
}

/// Build the candidate for an `@kind:"…"` quoted argument.
///
/// `payload_start` points at the opening quote. Trailing-punctuation
/// trimming never applies here — a quoted argument ends at its own closing
/// quote, and an optional fragment right after the quote extends only to the
/// next whitespace/quote/backtick terminator.
#[allow(clippy::too_many_arguments)]
fn scan_quoted_candidate(
    text: &str,
    start: usize,
    kind_start: usize,
    kind_end: usize,
    separator: usize,
    payload_start: usize,
    kind: &str,
) -> ArtifactRefPromptCandidateWire {
    let content_start = payload_start + 1;
    let (close, terminated) = scan_quoted_argument(text, payload_start);

    if !terminated {
        let line_end = close;
        return ArtifactRefPromptCandidateWire {
            schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
            text: text[start..line_end].to_string(),
            reference: format!(
                "{kind}:{}",
                unescape_quoted_argument(&text[content_start..line_end])
            ),
            kind: kind.to_string(),
            well_formed: false,
            candidate_span: span(start, line_end),
            sigil_span: span(start, start + 1),
            kind_span: span(kind_start, kind_end),
            separator_span: span(separator, separator + 1),
            payload_span: span(content_start, line_end),
            fragment_span: None,
            quoted: true,
        };
    }

    let quote_end = close + 1;
    let argument = unescape_quoted_argument(&text[content_start..close]);
    let (end, fragment_start) =
        if kind != "bug" && text[quote_end..].starts_with('#') {
            let raw_fragment_end = text[quote_end..]
                .char_indices()
                .find_map(|(offset, character)| {
                    (character.is_whitespace()
                        || matches!(character, '"' | '\'' | '`'))
                    .then_some(quote_end + offset)
                })
                .unwrap_or(text.len());
            (raw_fragment_end, Some(quote_end))
        } else {
            (quote_end, None)
        };
    let fragment_text =
        fragment_start.map_or("", |fragment_start| &text[fragment_start..end]);
    let reference = format!("{kind}:{argument}{fragment_text}");

    ArtifactRefPromptCandidateWire {
        schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
        text: text[start..end].to_string(),
        well_formed: parse_artifact_ref(&reference).is_ok(),
        reference,
        kind: kind.to_string(),
        candidate_span: span(start, end),
        sigil_span: span(start, start + 1),
        kind_span: span(kind_start, kind_end),
        separator_span: span(separator, separator + 1),
        payload_span: span(content_start, close),
        fragment_span: fragment_start
            .map(|fragment_start| span(fragment_start, end)),
        quoted: true,
    }
}

/// Scan a quoted argument starting at its opening `"`.
///
/// Returns the byte offset of the terminator and whether it is the matching
/// closing quote. When unterminated, the terminator is either an embedded
/// newline (the argument never crosses a line boundary) or the end of text.
fn scan_quoted_argument(text: &str, payload_start: usize) -> (usize, bool) {
    let content_start = payload_start + 1;
    let mut chars = text[content_start..].char_indices().peekable();
    while let Some((offset, character)) = chars.next() {
        match character {
            '\\' => {
                if let Some(&(_, next)) = chars.peek() {
                    if next == '"' || next == '\\' {
                        chars.next();
                    }
                }
            }
            '"' => return (content_start + offset, true),
            '\n' => return (content_start + offset, false),
            _ => {}
        }
    }
    (text.len(), false)
}

/// Undo `\"` and `\\` escapes. Any other backslash is a literal backslash.
fn unescape_quoted_argument(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\\' {
            result.push(character);
            continue;
        }
        match chars.peek() {
            Some('"') | Some('\\') => result.push(chars.next().unwrap()),
            _ => result.push('\\'),
        }
    }
    result
}

/// Render `argument` as a bare or quoted-and-escaped artifact-ref argument.
///
/// Quoting is applied when the argument contains whitespace, a quote
/// character, a backtick, or a trailing character `trim_candidate_end` would
/// otherwise strip from an unquoted candidate.
pub fn quote_artifact_ref_argument(argument: &str) -> String {
    if !argument_needs_quoting(argument) {
        return argument.to_string();
    }
    let mut quoted = String::with_capacity(argument.len() + 2);
    quoted.push('"');
    for character in argument.chars() {
        if character == '"' || character == '\\' {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

fn argument_needs_quoting(argument: &str) -> bool {
    if argument.is_empty() {
        return false;
    }
    if argument.chars().any(|character| {
        character.is_whitespace() || matches!(character, '"' | '\'' | '`')
    }) {
        return true;
    }
    argument
        .chars()
        .next_back()
        .is_some_and(|character| TRAILING_PUNCTUATION.contains(&character))
}

fn has_allowed_left_context(text: &str, start: usize) -> bool {
    start == 0
        || text[..start].chars().next_back().is_some_and(|character| {
            character.is_whitespace() || matches!(character, '"' | '\'' | '`')
        })
}

fn trim_candidate_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        // A lone trailing colon is the kind separator for an incomplete
        // `@kind:` reference, not prose punctuation. Keep it so editor
        // completion and diagnostics can classify the empty payload.
        if character == ':' {
            let candidate = &text[start..end];
            let colon_count = candidate.matches(':').count();
            let kind = candidate.split_once(':').map(|(kind, _)| kind);
            if colon_count == 1 || kind == Some("file") && colon_count == 2 {
                break;
            }
        }
        if !TRAILING_PUNCTUATION.contains(&character) {
            break;
        }
        end -= character.len_utf8();
    }
    end
}

const fn span(start: usize, end: usize) -> ArtifactRefSpanWire {
    ArtifactRefSpanWire { start, end }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn only(text: &str) -> ArtifactRefPromptCandidateWire {
        let mut candidates = scan_artifact_refs(text);
        assert_eq!(candidates.len(), 1, "{text}");
        candidates.remove(0)
    }

    #[test]
    fn quoted_argument_with_spaces_parses_and_round_trips() {
        let candidate = only(r#"@plan:"a b.md""#);
        assert!(candidate.quoted);
        assert_eq!(candidate.text, r#"@plan:"a b.md""#);
        assert_eq!(candidate.reference, "plan:a b.md");
        assert!(candidate.well_formed, "{candidate:?}");
        assert_eq!(
            &candidate.text
                [candidate.payload_span.start..candidate.payload_span.end],
            "a b.md"
        );
    }

    #[test]
    fn quoted_argument_supports_escaped_quote_and_backslash() {
        let escaped_quote = only(r#"@plan:"say \"hi\".md""#);
        assert_eq!(escaped_quote.reference, r#"plan:say "hi".md"#);

        let escaped_backslash = only(r#"@plan:"a\\b.md""#);
        assert_eq!(escaped_backslash.reference, r"plan:a\b.md");

        let literal_backslash = only(r#"@plan:"a\zb.md""#);
        assert_eq!(literal_backslash.reference, r"plan:a\zb.md");
    }

    #[test]
    fn fragment_splits_after_the_closing_quote() {
        let candidate = only(r##"@plan:"a b.md"#L3"##);
        assert_eq!(candidate.reference, "plan:a b.md#L3");
        assert!(candidate.fragment_span.is_some());
        assert_eq!(
            &candidate.text[candidate.fragment_span.unwrap().start
                ..candidate.fragment_span.unwrap().end],
            "#L3"
        );
        assert!(candidate.well_formed);
    }

    #[test]
    fn unterminated_quote_ends_at_the_current_line_never_at_eof() {
        let text = "@plan:\"unterminated\nnext line @plan:ok.md";
        let candidates = scan_artifact_refs(text);
        assert_eq!(candidates.len(), 2);
        assert!(candidates[0].quoted);
        assert!(!candidates[0].well_formed);
        assert_eq!(candidates[0].text, "@plan:\"unterminated");
        assert_eq!(candidates[1].text, "@plan:ok.md");
    }

    #[test]
    fn unterminated_quote_at_true_eof_ends_at_end_of_text() {
        let candidate = only(r#"@plan:"unterminated"#);
        assert!(candidate.quoted);
        assert!(!candidate.well_formed);
        assert_eq!(candidate.text, r#"@plan:"unterminated"#);
    }

    #[test]
    fn quoted_trailing_punctuation_is_not_trimmed() {
        let candidate = only(r#"@plan:"a b.md.""#);
        assert_eq!(candidate.reference, "plan:a b.md.");
        let unquoted = only("@plan:a.md.");
        assert_eq!(unquoted.reference, "plan:a.md");
    }

    #[test]
    fn quote_artifact_ref_argument_round_trips_through_the_scanner() {
        for argument in [
            "plain",
            "has space",
            "trailing.period.",
            "with \"quote\"",
            "with backtick ` here",
        ] {
            let quoted = quote_artifact_ref_argument(argument);
            let text = format!("@plan:{quoted}");
            let candidate = only(&text);
            let (_, unescaped) = candidate.reference.split_once(':').unwrap();
            assert_eq!(
                unescaped, argument,
                "argument={argument} quoted={quoted}"
            );
        }
    }
}
