//! Prompt literal-range primitives shared by launch and editor frontends.

use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
struct BacktickRun {
    start: usize,
    end: usize,
}

impl BacktickRun {
    fn len(self) -> usize {
        self.end - self.start
    }
}

/// Return matched single-line inline-code ranges as UTF-8 byte offsets.
///
/// Every unmasked backtick run may open a span. It closes at the nearest
/// unmasked run of exactly the same length on the same line. Runs overlapping
/// `masked_ranges` cannot open or close a span. The returned ranges are
/// ordered, non-overlapping, and include both delimiter runs.
pub fn inline_code_ranges(
    text: &str,
    masked_ranges: &[(usize, usize)],
) -> Vec<(usize, usize)> {
    if !text.as_bytes().contains(&b'`') {
        return Vec::new();
    }

    let masks = merge_masked_ranges(masked_ranges, text.len());
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut mask_index = 0;
    let mut line_start = 0;

    while line_start < bytes.len() {
        let newline = bytes[line_start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|relative| line_start + relative);
        let mut line_end = newline.unwrap_or(bytes.len());
        if line_end > line_start && bytes[line_end - 1] == b'\r' {
            line_end -= 1;
        }

        let runs = unmasked_backtick_runs(
            bytes,
            line_start,
            line_end,
            &masks,
            &mut mask_index,
        );
        append_line_spans(&runs, &mut spans);

        let Some(newline) = newline else {
            break;
        };
        line_start = newline + 1;
    }

    spans
}

fn unmasked_backtick_runs(
    bytes: &[u8],
    line_start: usize,
    line_end: usize,
    masks: &[(usize, usize)],
    mask_index: &mut usize,
) -> Vec<BacktickRun> {
    let mut runs = Vec::new();
    let mut cursor = line_start;
    while cursor < line_end {
        if bytes[cursor] != b'`' {
            cursor += 1;
            continue;
        }

        let start = cursor;
        cursor += 1;
        while cursor < line_end && bytes[cursor] == b'`' {
            cursor += 1;
        }
        if !range_overlaps_masks(start, cursor, masks, mask_index) {
            runs.push(BacktickRun { start, end: cursor });
        }
    }
    runs
}

fn append_line_spans(runs: &[BacktickRun], spans: &mut Vec<(usize, usize)>) {
    let mut next_same_length = vec![None; runs.len()];
    let mut nearest_by_length = HashMap::new();
    for (index, run) in runs.iter().copied().enumerate().rev() {
        next_same_length[index] = nearest_by_length.get(&run.len()).copied();
        nearest_by_length.insert(run.len(), index);
    }

    let mut index = 0;
    while index < runs.len() {
        if let Some(closer_index) = next_same_length[index] {
            spans.push((runs[index].start, runs[closer_index].end));
            index = closer_index + 1;
        } else {
            index += 1;
        }
    }
}

fn range_overlaps_masks(
    start: usize,
    end: usize,
    masks: &[(usize, usize)],
    mask_index: &mut usize,
) -> bool {
    while *mask_index < masks.len() && masks[*mask_index].1 <= start {
        *mask_index += 1;
    }
    *mask_index < masks.len() && masks[*mask_index].0 < end
}

fn merge_masked_ranges(
    ranges: &[(usize, usize)],
    text_len: usize,
) -> Vec<(usize, usize)> {
    let mut normalized = ranges
        .iter()
        .filter_map(|(start, end)| {
            let start = (*start).min(text_len);
            let end = (*end).min(text_len);
            (end > start).then_some((start, end))
        })
        .collect::<Vec<_>>();
    normalized.sort_unstable();

    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(normalized.len());
    for (start, end) in normalized {
        if let Some(last) = merged.last_mut() {
            if start <= last.1 {
                last.1 = last.1.max(end);
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sources<'a>(text: &'a str, ranges: &[(usize, usize)]) -> Vec<&'a str> {
        ranges
            .iter()
            .map(|(start, end)| &text[*start..*end])
            .collect()
    }

    #[test]
    fn recognizes_punctuation_and_word_adjacent_spans() {
        for (text, expected) in [
            ("`foo`/`bar`", vec!["`foo`", "`bar`"]),
            ("`foo`,`bar`", vec!["`foo`", "`bar`"]),
            ("prefix`value`suffix", vec!["`value`"]),
        ] {
            let ranges = inline_code_ranges(text, &[]);
            assert_eq!(sources(text, &ranges), expected);
        }
    }

    #[test]
    fn matches_equal_runs_while_allowing_shorter_nested_runs() {
        let text = "`one `` nested`` end` and ``two ` inner` end``";
        let ranges = inline_code_ranges(text, &[]);
        assert_eq!(
            sources(text, &ranges),
            vec!["`one `` nested`` end`", "``two ` inner` end``"]
        );
    }

    #[test]
    fn ignores_unmatched_and_multiline_runs() {
        assert!(inline_code_ranges("before `unmatched", &[]).is_empty());
        assert!(inline_code_ranges("`first line\nsecond line`", &[]).is_empty());
    }

    #[test]
    fn keeps_crlf_lines_independent() {
        let text = "`a`\r\n`b`\r\n`not closed";
        assert_eq!(inline_code_ranges(text, &[]), vec![(0, 3), (5, 8)]);
    }

    #[test]
    fn merges_overlapping_and_adjacent_masks_once() {
        let text = "`one` `two` `three`";
        let masks = [(0, 3), (2, 6), (6, 8), (8, 11)];
        let ranges = inline_code_ranges(text, &masks);
        assert_eq!(sources(text, &ranges), vec!["`three`"]);
    }

    #[test]
    fn reports_utf8_byte_offsets() {
        let text = "é`值`/`ß`";
        assert_eq!(inline_code_ranges(text, &[]), vec![(2, 7), (8, 12)]);
    }
}
