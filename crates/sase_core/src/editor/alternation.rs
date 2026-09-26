//! Alternation bodies as independent completion-edit boundaries.
//!
//! The launch parser owns the alternation grammar (`%alt(...)`, `%(...)`,
//! `%{...}`): its delimiter regex and matching rules live in
//! [`crate::agent_launch`]. This module reuses exactly those rules so
//! completion accept never drifts behind launch with a second approximate
//! `%alt` pattern. Bodies reported here cover the whole directive span
//! (marker through the matching close delimiter); targets inside them are
//! never selected or deleted by a completion outside the body, and a
//! trigger inside one stays local to its own token.

use crate::agent_launch::{alt_directive_starts, find_matching_delimiter};

use super::exclusion::{
    excluded_literal_and_definition_ranges, position_in_ranges,
};

/// Byte ranges covering each closed alternation directive in `text`.
///
/// A range starts at the `%` marker and ends one past the matching close
/// delimiter. Markers inside literal/definition zones (frontmatter,
/// fenced/inline code, disabled regions, Jinja tags) are inert and skipped,
/// mirroring the launch literal treatment. Unclosed markers produce no
/// range, exactly like the launch masks.
pub(crate) fn alternation_body_ranges(text: &str) -> Vec<(usize, usize)> {
    let ignored = excluded_literal_and_definition_ranges(text);
    let mut ranges = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(text) {
        if position_in_ranges(start, &ignored) {
            continue;
        }
        if let Some(close_end) = find_matching_delimiter(
            text,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            ranges.push((start, close_end + 1));
        }
    }
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
    fn detects_positions_and_spans() {
        let text = "%{%m:opus | %m:sonnet} Use =la";
        assert!(position_in_alternation(text, 3));
        assert!(!position_in_alternation(text, 24));
        assert!(span_in_alternation(text, 3, 9));
        assert!(!span_in_alternation(text, 24, 27));
    }
}
