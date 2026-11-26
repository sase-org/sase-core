//! Conservative copy-candidate detection for question prose.

use std::collections::HashSet;

use serde_json::Value;

use crate::artifact_ref::parse_artifact_ref;
use crate::fenced_code::fenced_block_details;

use super::normalize::normalize_question_copy;
use super::wire::{QuestionCopyCandidateKind, QuestionCopyCandidateWire};

const MAX_DETECTED_CANDIDATES: usize = 5;

#[derive(Debug)]
struct LocatedCandidate {
    start: usize,
    text: String,
    kind: QuestionCopyCandidateKind,
}

/// Find up to five conservative, safe copy candidates in question prose.
///
/// Fenced and filtered inline code take precedence over URLs and artifact
/// references. Results keep their first source occurrence, omit exact declared
/// values, and pass through the same safety rules as declared copy values.
pub fn detect_question_copy_candidates(
    question_text: &str,
    declared_texts: &[String],
) -> Vec<QuestionCopyCandidateWire> {
    let fenced = fenced_block_details(question_text);
    let fenced_ranges: Vec<_> =
        fenced.iter().map(|block| block.block_range).collect();
    let mut located = Vec::new();
    let mut code_ranges = fenced_ranges.clone();

    for block in &fenced {
        let (start, end) = block.content_range;
        let text = question_text[start..end].trim_end_matches('\n');
        located.push(LocatedCandidate {
            start,
            text: text.to_string(),
            kind: QuestionCopyCandidateKind::Code,
        });
    }

    for (start, end, text) in inline_code_spans(question_text, &fenced_ranges) {
        if is_worth_copying_inline_code(text) {
            located.push(LocatedCandidate {
                start,
                text: text.to_string(),
                kind: QuestionCopyCandidateKind::Code,
            });
        }
        code_ranges.push((start.saturating_sub(1), end + 1));
    }

    located.extend(scan_scheme_urls(question_text, &code_ranges));
    located.extend(scan_artifact_refs(question_text, &code_ranges));
    located.sort_by_key(|candidate| candidate.start);

    let mut seen: HashSet<String> = declared_texts.iter().cloned().collect();
    let mut result = Vec::new();
    for candidate in located {
        let Some(text) = validate_candidate(&candidate.text) else {
            continue;
        };
        if !seen.insert(text.clone()) {
            continue;
        }
        result.push(QuestionCopyCandidateWire {
            text,
            kind: candidate.kind,
        });
        if result.len() == MAX_DETECTED_CANDIDATES {
            break;
        }
    }
    result
}

fn inline_code_spans<'a>(
    text: &'a str,
    fenced_ranges: &[(usize, usize)],
) -> Vec<(usize, usize, &'a str)> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if let Some((_, block_end)) = range_containing(fenced_ranges, cursor) {
            cursor = block_end;
            continue;
        }
        if bytes[cursor] != b'`' {
            cursor += 1;
            continue;
        }

        let open_start = cursor;
        while cursor < bytes.len() && bytes[cursor] == b'`' {
            cursor += 1;
        }
        let run_length = cursor - open_start;
        let Some((close_start, close_end)) =
            find_matching_backticks(bytes, cursor, run_length, fenced_ranges)
        else {
            continue;
        };
        spans.push((cursor, close_end, &text[cursor..close_start]));
        cursor = close_end;
    }
    spans
}

fn find_matching_backticks(
    bytes: &[u8],
    mut cursor: usize,
    run_length: usize,
    fenced_ranges: &[(usize, usize)],
) -> Option<(usize, usize)> {
    while cursor < bytes.len() {
        if range_containing(fenced_ranges, cursor).is_some() {
            return None;
        }
        if bytes[cursor] != b'`' {
            cursor += 1;
            continue;
        }
        let run_start = cursor;
        while cursor < bytes.len() && bytes[cursor] == b'`' {
            cursor += 1;
        }
        if cursor - run_start == run_length {
            return Some((run_start, cursor));
        }
    }
    None
}

fn is_worth_copying_inline_code(text: &str) -> bool {
    if text.chars().count() >= 12
        || text.chars().any(char::is_whitespace)
        || text.contains('/')
        || text.contains("://")
        || text.contains('=')
        || text.contains("--")
    {
        return true;
    }
    text.char_indices().any(|(index, character)| {
        character == '.'
            && text[..index]
                .chars()
                .next_back()
                .is_some_and(char::is_alphanumeric)
            && text[index + 1..]
                .chars()
                .next()
                .is_some_and(char::is_alphanumeric)
    })
}

fn scan_scheme_urls(
    text: &str,
    excluded_ranges: &[(usize, usize)],
) -> Vec<LocatedCandidate> {
    let bytes = text.as_bytes();
    let mut candidates = Vec::new();
    for start in 0..bytes.len() {
        if !bytes[start].is_ascii_lowercase()
            || (start > 0
                && (bytes[start - 1].is_ascii_alphanumeric()
                    || bytes[start - 1] == b'_'))
        {
            continue;
        }
        let mut scheme_end = start + 1;
        while scheme_end < bytes.len()
            && (bytes[scheme_end].is_ascii_alphanumeric()
                || matches!(bytes[scheme_end], b'+' | b'.' | b'-'))
        {
            scheme_end += 1;
        }
        if !text[scheme_end..].starts_with("://") {
            continue;
        }
        let end = token_end(text, start);
        let trimmed_end = trim_sentence_punctuation(text, start, end);
        if trimmed_end <= scheme_end + 3
            || overlaps(excluded_ranges, start, trimmed_end)
        {
            continue;
        }
        candidates.push(LocatedCandidate {
            start,
            text: text[start..trimmed_end].to_string(),
            kind: QuestionCopyCandidateKind::Url,
        });
    }
    candidates
}

fn scan_artifact_refs(
    text: &str,
    excluded_ranges: &[(usize, usize)],
) -> Vec<LocatedCandidate> {
    let bytes = text.as_bytes();
    let mut candidates = Vec::new();
    for (colon, byte) in bytes.iter().enumerate() {
        if *byte != b':' || text[colon + 1..].starts_with("//") {
            continue;
        }
        let mut start = colon;
        while start > 0
            && (bytes[start - 1].is_ascii_lowercase()
                || bytes[start - 1].is_ascii_digit()
                || bytes[start - 1] == b'-'
                || bytes[start - 1] == b'_')
        {
            start -= 1;
        }
        if start == colon || !bytes[start].is_ascii_lowercase() {
            continue;
        }
        if start > 0 {
            let previous = bytes[start - 1];
            if previous == b'@' {
                if start > 1
                    && (bytes[start - 2].is_ascii_alphanumeric()
                        || bytes[start - 2] == b'_')
                {
                    continue;
                }
            } else if previous.is_ascii_alphanumeric()
                || previous == b'_'
                || previous == b'-'
            {
                continue;
            }
        }

        let end = token_end(text, start);
        let trimmed_end = trim_sentence_punctuation(text, start, end);
        if trimmed_end <= colon + 1
            || overlaps(excluded_ranges, start, trimmed_end)
        {
            continue;
        }
        let value = &text[start..trimmed_end];
        if parse_artifact_ref(value).is_err() {
            continue;
        }
        candidates.push(LocatedCandidate {
            start,
            text: value.to_string(),
            kind: QuestionCopyCandidateKind::Ref,
        });
    }
    candidates
}

fn token_end(text: &str, start: usize) -> usize {
    text[start..]
        .char_indices()
        .find_map(|(offset, character)| {
            (character.is_whitespace()
                || matches!(character, '"' | '\'' | '`' | '<' | '>'))
            .then_some(start + offset)
        })
        .unwrap_or(text.len())
}

fn trim_sentence_punctuation(
    text: &str,
    start: usize,
    mut end: usize,
) -> usize {
    loop {
        let Some(character) = text[start..end].chars().next_back() else {
            return end;
        };
        let unmatched_close_paren = character == ')'
            && text[start..end].chars().filter(|ch| *ch == ')').count()
                > text[start..end].chars().filter(|ch| *ch == '(').count();
        if matches!(character, '.' | ',' | ';' | ':' | '!' | '?')
            || unmatched_close_paren
        {
            end -= character.len_utf8();
            continue;
        }
        return end;
    }
}

fn range_containing(
    ranges: &[(usize, usize)],
    position: usize,
) -> Option<(usize, usize)> {
    ranges
        .iter()
        .copied()
        .find(|(start, end)| *start <= position && position < *end)
}

fn overlaps(ranges: &[(usize, usize)], start: usize, end: usize) -> bool {
    ranges.iter().any(|(other_start, other_end)| {
        *other_start < end && start < *other_end
    })
}

fn validate_candidate(text: &str) -> Option<String> {
    let input = Value::Array(vec![Value::String(text.to_string())]);
    normalize_question_copy(&input)
        .ok()?
        .into_iter()
        .next()
        .map(|item| item.text)
}
