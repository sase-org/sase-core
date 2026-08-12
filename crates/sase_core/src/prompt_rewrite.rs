//! Shared protected-range prompt link rewriting.

use crate::prompt_literal_zone_ranges;

pub(crate) struct PromptLinkCandidate<'a> {
    pub(crate) record_index: usize,
    pub(crate) token: &'a str,
    pub(crate) target: &'a str,
}

pub(crate) fn rewrite_prompt_links<F>(
    prompt: &str,
    record_count: usize,
    candidates: &[PromptLinkCandidate<'_>],
    mut valid_start: F,
    mut replacement: impl FnMut(usize, &str, &str) -> String,
) -> (String, Vec<bool>)
where
    F: FnMut(&str, usize) -> bool,
{
    if candidates.is_empty() {
        return (prompt.to_string(), vec![false; record_count]);
    }

    let mut protected = prompt_literal_zone_ranges(prompt);
    protected.extend(markdown_link_ranges(prompt));
    let protected = merge_ranges(&protected, prompt.len());
    let mut linked = vec![false; record_count];
    let mut rewritten = String::with_capacity(prompt.len());
    let mut cursor = 0;
    let mut protected_index = 0;

    while cursor < prompt.len() {
        while protected_index < protected.len()
            && protected[protected_index].1 <= cursor
        {
            protected_index += 1;
        }
        if protected_index < protected.len()
            && protected[protected_index].0 <= cursor
        {
            let end = protected[protected_index].1;
            rewritten.push_str(&prompt[cursor..end]);
            cursor = end;
            continue;
        }

        let next_protected = protected
            .get(protected_index)
            .map_or(prompt.len(), |range| range.0);
        let candidate = candidates
            .iter()
            .filter(|candidate| {
                valid_start(prompt, cursor)
                    && prompt[cursor..next_protected]
                        .starts_with(candidate.token)
            })
            .max_by_key(|candidate| candidate.token.len());
        if let Some(candidate) = candidate {
            rewritten.push_str(&replacement(
                candidate.record_index,
                candidate.token,
                candidate.target,
            ));
            linked[candidate.record_index] = true;
            cursor += candidate.token.len();
            continue;
        }

        let character = prompt[cursor..]
            .chars()
            .next()
            .expect("cursor is before prompt end");
        rewritten.push(character);
        cursor += character.len_utf8();
    }

    (rewritten, linked)
}

fn markdown_link_ranges(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'[' || is_escaped(bytes, cursor) {
            cursor += 1;
            continue;
        }
        let Some(label_end) = matching_delimiter(bytes, cursor, b'[', b']')
        else {
            cursor += 1;
            continue;
        };
        let target_start = label_end + 1;
        let target_end = match bytes.get(target_start) {
            Some(b'(') => matching_delimiter(bytes, target_start, b'(', b')'),
            Some(b'[') => matching_delimiter(bytes, target_start, b'[', b']'),
            _ => None,
        };
        if let Some(target_end) = target_end {
            let start = cursor.saturating_sub(usize::from(
                cursor > 0 && bytes[cursor - 1] == b'!',
            ));
            ranges.push((start, target_end + 1));
            cursor = target_end + 1;
        } else {
            cursor = label_end + 1;
        }
    }
    ranges
}

fn matching_delimiter(
    bytes: &[u8],
    start: usize,
    open: u8,
    close: u8,
) -> Option<usize> {
    let mut depth = 0usize;
    for cursor in start..bytes.len() {
        if bytes[cursor] == b'\n' || bytes[cursor] == b'\r' {
            return None;
        }
        if is_escaped(bytes, cursor) {
            continue;
        }
        if bytes[cursor] == open {
            depth += 1;
        } else if bytes[cursor] == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(cursor);
            }
        }
    }
    None
}

fn is_escaped(bytes: &[u8], position: usize) -> bool {
    let mut slashes = 0;
    let mut cursor = position;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        slashes += 1;
        cursor -= 1;
    }
    slashes % 2 == 1
}

fn merge_ranges(
    ranges: &[(usize, usize)],
    text_len: usize,
) -> Vec<(usize, usize)> {
    let mut ranges = ranges
        .iter()
        .filter_map(|(start, end)| {
            let start = (*start).min(text_len);
            let end = (*end).min(text_len);
            (start < end).then_some((start, end))
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut() {
            if range.0 <= last.1 {
                last.1 = last.1.max(range.1);
                continue;
            }
        }
        merged.push(range);
    }
    merged
}
