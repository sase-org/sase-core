//! Shared alternation scanner: one walk over `%{...}`, `%(...)`, and
//! `%alt(...)` occurrences for launch, diagnostics, bindings, and the LSP.
//!
//! The launch parser owns the alternation grammar: its delimiter regex and
//! matching rules live in [`crate::agent_launch`]. This module reuses
//! exactly those rules so completion accept never drifts behind launch
//! with a second approximate `%alt` pattern. Bodies reported here cover
//! the whole directive span (marker through the matching close
//! delimiter); targets inside them are never selected or deleted by a
//! completion outside the body, and a trigger inside one stays local to
//! its own token.
//!
//! [`scan_alternations`] is the single walk. Separator and branch-name
//! offsets come from the same top-level splitting launch uses
//! ([`split_top_level_arg_ranges`][crate::agent_launch] and the shared
//! `name=value` scan): backticks, double quotes, `[[...]]`, and bracket
//! depth hide structure, while single quotes do not, so `%{don't | do}`
//! splits exactly like launch splits it.

use serde::{Deserialize, Serialize};

use crate::agent_launch::{
    alt_directive_starts, find_matching_delimiter, split_top_level_arg_ranges,
    top_level_eq_offset, AltDelimiter,
};

use super::exclusion::{
    excluded_literal_and_definition_ranges, position_in_ranges,
};

/// Which surface form opened an alternation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlternationFormWire {
    Brace,
    Paren,
}

/// One alternation occurrence in the document.
///
/// Every offset is a UTF-8 byte offset into the scanned text; the Python
/// binding converts them to code-point offsets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlternationScanWire {
    /// `brace` for `%{...}`, `paren` for `%(...)` and `%alt(...)`.
    pub form: AlternationFormWire,
    /// Byte offset of the `%` marker.
    pub marker_start: usize,
    /// Byte offset one past the `{`/`(` opener.
    pub opener_end: usize,
    /// Byte offset of the matching `}`/`)`, or `None` when unclosed.
    /// An unclosed opener carries no separators or branch names: its
    /// marker span is the error span.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close: Option<usize>,
    /// Byte offsets of each top-level branch separator (`|`/`,`).
    #[serde(default)]
    pub separators: Vec<usize>,
    /// Byte spans of each `name=` branch name.
    #[serde(default)]
    pub branch_names: Vec<(usize, usize)>,
    /// Nesting depth: 0 for an outermost alternation, +1 per enclosing
    /// alternation.
    pub depth: usize,
}

/// Scan every alternation opener in `text`, in source order.
///
/// Openers inside literal/definition zones (frontmatter, fenced/inline
/// code, disabled regions, Jinja tags) are inert and skipped, mirroring
/// the launch literal treatment. An unclosed opener outside those zones
/// produces one record with `close` set to `None`.
pub fn scan_alternations(text: &str) -> Vec<AlternationScanWire> {
    let ignored = excluded_literal_and_definition_ranges(text);
    let mut records = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(text) {
        if position_in_ranges(start, &ignored) {
            continue;
        }
        let form = match delimiter {
            AltDelimiter::Brace => AlternationFormWire::Brace,
            AltDelimiter::Paren => AlternationFormWire::Paren,
        };
        let close = find_matching_delimiter(
            text,
            open_start,
            delimiter.open(),
            delimiter.close(),
        );
        let (separators, branch_names) = match close {
            Some(close) => alt_branch_spans(text, open_start, close, delimiter),
            None => (Vec::new(), Vec::new()),
        };
        let depth = records
            .iter()
            .filter(|record: &&AlternationScanWire| {
                record.marker_start <= start && start < record_end(record, text)
            })
            .count();
        records.push(AlternationScanWire {
            form,
            marker_start: start,
            opener_end: open_start + 1,
            close,
            separators,
            branch_names,
            depth,
        });
    }
    records
}

/// End byte offset of a scan record: one past its close delimiter, or
/// the end of the text for an unclosed opener.
fn record_end(record: &AlternationScanWire, text: &str) -> usize {
    record.close.map(|close| close + 1).unwrap_or(text.len())
}

/// Top-level separator offsets and `name=` branch-name spans for the
/// closed alternation whose body is `text[open_start + 1..close]`.
///
/// Branch splitting reuses launch's splitter, so separators hidden by
/// backticks, double quotes, `[[...]]`, or bracket depth stay hidden
/// here too. Offsets are absolute byte offsets into `text`.
fn alt_branch_spans(
    text: &str,
    open_start: usize,
    close: usize,
    delimiter: AltDelimiter,
) -> (Vec<usize>, Vec<(usize, usize)>) {
    let body_start = open_start + 1;
    let inner = &text[body_start..close];
    let (segments, separators) =
        split_top_level_arg_ranges(inner, delimiter.separator());
    let separators = separators
        .iter()
        .map(|offset| body_start + offset)
        .collect();
    let mut branch_names = Vec::new();
    for (start, end) in segments {
        let raw = &inner[start..end];
        let trimmed_start = raw.len() - raw.trim_start().len();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(eq) = top_level_eq_offset(trimmed) else {
            continue;
        };
        let before = &trimmed[..eq];
        let name_start = before.len() - before.trim_start().len();
        let name_end = before.trim_end().len();
        if name_start >= name_end {
            continue;
        }
        let base = body_start + start + trimmed_start;
        branch_names.push((base + name_start, base + name_end));
    }
    (separators, branch_names)
}

/// Byte ranges covering each closed alternation directive in `text`.
///
/// A range starts at the `%` marker and ends one past the matching close
/// delimiter. Markers inside literal/definition zones (frontmatter,
/// fenced/inline code, disabled regions, Jinja tags) are inert and skipped,
/// mirroring the launch literal treatment. Unclosed markers produce no
/// range, exactly like the launch masks.
pub(crate) fn alternation_body_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges: Vec<(usize, usize)> = scan_alternations(text)
        .into_iter()
        .filter_map(|record| {
            record.close.map(|close| (record.marker_start, close + 1))
        })
        .collect();
    ranges.sort_unstable();
    ranges.dedup();
    ranges
}

/// Whether byte offset `pos` sits inside any alternation body in `text`.
pub(crate) fn position_in_alternation(text: &str, pos: usize) -> bool {
    position_in_ranges(pos, &alternation_body_ranges(text))
}

/// Whether the `[start, end)` span touches any alternation body in `text`.
pub(crate) fn span_in_alternation(
    text: &str,
    start: usize,
    end: usize,
) -> bool {
    alternation_body_ranges(text)
        .iter()
        .any(|(body_start, body_end)| start < *body_end && *body_start < end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers_all_three_spellings_through_close_delimiter() {
        assert_eq!(
            alternation_body_ranges("%alt(a, b)"),
            vec![(0, 10)],
            "paren alt"
        );
        assert_eq!(
            alternation_body_ranges("%(a, b)"),
            vec![(0, 7)],
            "paren shorthand"
        );
        assert_eq!(
            alternation_body_ranges("%{a | b}"),
            vec![(0, 8)],
            "brace shorthand"
        );
    }

    #[test]
    fn skips_markers_in_literal_zones_and_unclosed_markers() {
        assert_eq!(
            alternation_body_ranges("`%{a | b}`"),
            Vec::new(),
            "inline code is inert"
        );
        assert_eq!(
            alternation_body_ranges("```text\n%{a | b}\n```"),
            Vec::new(),
            "fenced code is inert"
        );
        assert_eq!(
            alternation_body_ranges("%{a | b"),
            Vec::new(),
            "unclosed marker is inert like the launch masks"
        );
    }

    #[test]
    fn covers_mid_word_and_adjacent_openers() {
        assert_eq!(
            alternation_body_ranges("foo%{a | b}qux"),
            vec![(3, 11)],
            "mid-word brace opener"
        );
        assert_eq!(
            alternation_body_ranges("%{a|b}%{c|d}"),
            vec![(0, 6), (6, 12)],
            "adjacent openers are all found"
        );
        assert_eq!(
            alternation_body_ranges("x%(a,b)"),
            Vec::new(),
            "paren form still needs a boundary"
        );
    }

    #[test]
    fn detects_positions_and_spans() {
        let text = "%{%m:opus | %m:sonnet} Use =la";
        assert!(position_in_alternation(text, 3));
        assert!(!position_in_alternation(text, 24));
        assert!(span_in_alternation(text, 3, 9));
        assert!(!span_in_alternation(text, 24, 27));
    }

    #[test]
    fn scanner_reports_mid_word_wire_record() {
        assert_eq!(
            scan_alternations("foo%{bar | baz}qux"),
            vec![AlternationScanWire {
                form: AlternationFormWire::Brace,
                marker_start: 3,
                opener_end: 5,
                close: Some(14),
                separators: vec![9],
                branch_names: Vec::new(),
                depth: 0,
            }]
        );
    }

    #[test]
    fn scanner_reports_single_branch_without_separator() {
        assert_eq!(
            scan_alternations("word%{s}"),
            vec![AlternationScanWire {
                form: AlternationFormWire::Brace,
                marker_start: 4,
                opener_end: 6,
                close: Some(7),
                separators: Vec::new(),
                branch_names: Vec::new(),
                depth: 0,
            }]
        );
    }

    #[test]
    fn scanner_reports_branch_name_spans() {
        assert_eq!(
            scan_alternations("%{a=x | b=y}"),
            vec![AlternationScanWire {
                form: AlternationFormWire::Brace,
                marker_start: 0,
                opener_end: 2,
                close: Some(11),
                separators: vec![6],
                branch_names: vec![(2, 3), (8, 9)],
                depth: 0,
            }]
        );
    }

    #[test]
    fn scanner_keeps_paren_boundary_rule_with_separators() {
        assert_eq!(
            scan_alternations("x%(a,b)"),
            Vec::new(),
            "glued paren form is not an alternation"
        );
        assert_eq!(
            scan_alternations("x %(a,b)"),
            vec![AlternationScanWire {
                form: AlternationFormWire::Paren,
                marker_start: 2,
                opener_end: 4,
                close: Some(7),
                separators: vec![5],
                branch_names: Vec::new(),
                depth: 0,
            }]
        );
    }

    #[test]
    fn scanner_reports_nested_depth_with_top_level_separators() {
        let records = scan_alternations("a %{x %{p|q} | y} b");
        assert_eq!(records.len(), 2);
        let outer = &records[0];
        let inner = &records[1];
        assert_eq!(outer.marker_start, 2);
        assert_eq!(outer.close, Some(16));
        assert_eq!(outer.separators, vec![13]);
        assert_eq!(outer.depth, 0);
        assert_eq!(inner.marker_start, 6);
        assert_eq!(inner.close, Some(11));
        assert_eq!(inner.separators, vec![9]);
        assert_eq!(inner.depth, 1);
    }

    #[test]
    fn scanner_splits_on_apostrophes_like_launch() {
        let records = scan_alternations("x %{don't | do} it");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].separators, vec![10]);
    }

    #[test]
    fn scanner_hides_separators_in_quotes_and_brackets() {
        let records = scan_alternations("%{\"a|b\" | c}");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].separators, vec![8]);
        let records = scan_alternations("%{a [x|y] | c}");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].separators, vec![10]);
    }

    #[test]
    fn scanner_skips_literal_zones() {
        assert_eq!(scan_alternations("`%{a | b}`"), Vec::new());
        assert_eq!(scan_alternations("```text\n%{a | b}\n```"), Vec::new());
    }

    #[test]
    fn scanner_reports_unclosed_marker_without_close() {
        assert_eq!(
            scan_alternations("foo%{bar"),
            vec![AlternationScanWire {
                form: AlternationFormWire::Brace,
                marker_start: 3,
                opener_end: 5,
                close: None,
                separators: Vec::new(),
                branch_names: Vec::new(),
                depth: 0,
            }]
        );
    }

    #[test]
    fn scanner_uses_byte_offsets_for_non_ascii_text() {
        let prefix = "héllo ".len();
        let records = scan_alternations("héllo %{a | b}");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].marker_start, prefix);
        assert_eq!(records[0].opener_end, prefix + 2);
        assert_eq!(
            records[0].close,
            Some(prefix + 7),
            "byte offsets, not code points"
        );
        assert_eq!(records[0].separators, vec![prefix + 4]);
    }
}
