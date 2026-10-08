//! Human quote matching for memory decision verification.
//!
//! This module implements the `quotes` phase of
//! `plan:202610/core_plan_decisions.md` (outer design, human quote
//! matcher). The Python host classifies which texts are human-authored
//! and supplies them as ordered `{source, ref, text}` records; Rust
//! never authenticates a source or reads memory files.
//!
//! Both sides normalize with NFKC, full non-Turkic Unicode casefold,
//! straight quote/dash equivalents, and collapsed Unicode whitespace.
//! The quote sheds leading/trailing punctuation, needs at least three
//! word tokens, and then must appear as one contiguous complete-word
//! run inside a single supplied text. Texts are never joined, words
//! are never partially matched, tokens are never reordered, and fuzzy
//! overlap never verifies: it only ranks the closest-sentence hint.
//!
//! A lexical match inside a negated sentence stays a match. Intent is
//! not this function's contract.

use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

/// One host-classified human-authored text to search, in host order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionQuoteTextWire {
    pub source: String,
    pub r#ref: String,
    pub text: String,
}

/// The closest-sentence hint returned when nothing verifies.
///
/// `sentence` is the original sentence text with its supplied source
/// identity so the host can point the reviewer at it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionQuoteClosestWire {
    pub source: String,
    pub r#ref: String,
    pub sentence: String,
}

/// The quote match verdict.
///
/// A verified result carries the first matching input record in
/// `matched_source` and no hint. A failure carries no match and, when
/// at least one quote token overlaps a supplied sentence, the
/// best-overlap hint in `closest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionQuoteMatchWire {
    pub verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_source: Option<PlanDecisionQuoteTextWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closest: Option<PlanDecisionQuoteClosestWire>,
}

/// A quote needs at least three word tokens to verify.
pub const QUOTE_MIN_WORDS: usize = 3;

/// Map curly/smart quotes and dashes to their straight equivalents.
///
/// NFKC already folds full-width forms to ASCII, so only the
/// compatibility survivors are listed here.
fn straight_equivalent(c: char) -> char {
    match c {
        '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' | '\u{2032}'
        | '\u{2035}' | '\u{2bc}' => '\'',
        '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' | '\u{2033}'
        | '\u{2036}' | '\u{ab}' | '\u{bb}' => '"',
        '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}'
        | '\u{2015}' | '\u{2212}' | '\u{fe58}' | '\u{fe63}' => '-',
        _ => c,
    }
}

/// Normalize one side of the match: NFKC, straight punctuation,
/// full non-Turkic casefold, collapsed Unicode whitespace.
fn normalize(raw: &str) -> String {
    let nfkc: String = raw.nfkc().collect();
    let straight: String = nfkc.chars().map(straight_equivalent).collect();
    // Full casefold expands (for example `ß` to `ss`); it must run on
    // the NFKC form rather than standing in for it.
    let folded: String = straight.as_str().case_fold().collect();
    folded.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Strip edge punctuation. Internal punctuation such as `.md` and
/// apostrophes survives because only the token edges are trimmed.
fn strip_edges(token: &str) -> &str {
    token.trim_matches(|c: char| !c.is_alphanumeric())
}

/// Split normalized text into owned word tokens, dropping empties
/// left by punctuation-only runs.
fn word_tokens(normalized: &str) -> Vec<String> {
    normalized
        .split(' ')
        .map(strip_edges)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

/// Quote tokens: normalized, edge-stripped, with at least three words.
fn quote_tokens(quote: &str) -> Vec<String> {
    let normalized = normalize(quote);
    word_tokens(strip_edges(&normalized))
}

/// True when `needle` runs contiguously inside `haystack`.
fn contains_run(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Split original text into sentences, keeping original text.
///
/// A boundary is a newline run or a `.!?…` run followed by whitespace
/// or the end of the text. A period inside a word (`tui.md`) never
/// splits because it is followed by a word character. Abbreviations
/// such as `e.g.` do split; this is an advisory heuristic, not a
/// parser.
fn split_sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < text.len() {
        let c = text[index..].chars().next().unwrap_or('\0');
        let next = index + c.len_utf8();
        if c == '\r' || c == '\n' {
            let mut end = next;
            while end < text.len() {
                let d = text[end..].chars().next().unwrap_or('\0');
                if d == '\r' || d == '\n' {
                    end += d.len_utf8();
                } else {
                    break;
                }
            }
            push_sentence(&mut sentences, &text[start..index]);
            start = end;
            index = end;
        } else if matches!(c, '.' | '!' | '?' | '\u{2026}') {
            let mut end = next;
            while end < text.len() {
                let d = text[end..].chars().next().unwrap_or('\0');
                if matches!(d, '.' | '!' | '?' | '\u{2026}') {
                    end += d.len_utf8();
                } else {
                    break;
                }
            }
            let after = text[end..].chars().next();
            if after.is_none_or(|d| d.is_whitespace()) {
                push_sentence(&mut sentences, &text[start..end]);
                start = end;
            }
            index = end;
        } else {
            index = next;
        }
    }
    push_sentence(&mut sentences, &text[start..]);
    sentences
}

fn push_sentence<'a>(sentences: &mut Vec<&'a str>, raw: &'a str) {
    let trimmed = raw.trim();
    if !trimmed.is_empty() {
        sentences.push(trimmed);
    }
}

/// Verify a human quote against ordered human-authored texts.
///
/// Returns the first text (in host order) whose normalized words
/// contain the whole normalized quote as one contiguous run.
/// Otherwise ranks each original sentence by how many quote tokens
/// it contains; ties stay stable by input order, then sentence
/// order. Empty sources and zero token overlap yield no hint.
pub fn plan_decision_quote_match(
    quote: &str,
    texts: &[PlanDecisionQuoteTextWire],
) -> PlanDecisionQuoteMatchWire {
    let quote = quote_tokens(quote);
    if quote.len() >= QUOTE_MIN_WORDS {
        for text in texts {
            let normalized = normalize(&text.text);
            let words = word_tokens(&normalized);
            if contains_run(&words, &quote) {
                return PlanDecisionQuoteMatchWire {
                    verified: true,
                    matched_source: Some(text.clone()),
                    closest: None,
                };
            }
        }
    }
    let mut best: Option<PlanDecisionQuoteClosestWire> = None;
    let mut best_score = 0usize;
    for text in texts {
        for sentence in split_sentences(&text.text) {
            let normalized = normalize(sentence);
            let words = word_tokens(&normalized);
            let score = quote
                .iter()
                .filter(|token| words.iter().any(|word| word == *token))
                .count();
            if score > best_score {
                best_score = score;
                best = Some(PlanDecisionQuoteClosestWire {
                    source: text.source.clone(),
                    r#ref: text.r#ref.clone(),
                    sentence: sentence.to_string(),
                });
            }
        }
    }
    PlanDecisionQuoteMatchWire {
        verified: false,
        matched_source: None,
        closest: best,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(
        source: &str,
        r#ref: &str,
        body: &str,
    ) -> PlanDecisionQuoteTextWire {
        PlanDecisionQuoteTextWire {
            source: source.to_string(),
            r#ref: r#ref.to_string(),
            text: body.to_string(),
        }
    }

    #[test]
    fn contiguous_three_word_quote_verifies_with_identity() {
        let texts = vec![
            text("chat", "m1", "Nothing relevant here at all."),
            text(
                "note",
                "tui.md",
                "Please note the convention in the tui memory.",
            ),
        ];
        let matched = plan_decision_quote_match(
            "note the convention in the tui memory",
            &texts,
        );
        assert!(matched.verified);
        let source = matched.matched_source.unwrap();
        assert_eq!(source.source, "note");
        assert_eq!(source.r#ref, "tui.md");
        assert!(matched.closest.is_none());
    }

    #[test]
    fn two_word_quote_never_verifies_but_still_suggests() {
        let texts = vec![text("note", "tui.md", "Note the convention here.")];
        let matched = plan_decision_quote_match("note the", &texts);
        assert!(!matched.verified);
        assert!(matched.matched_source.is_none());
        let closest = matched.closest.unwrap();
        assert_eq!(closest.sentence, "Note the convention here.");
    }

    #[test]
    fn noncontiguous_halves_do_not_verify() {
        let texts = vec![text(
            "note",
            "tui.md",
            "Please note the rules. Also honor the convention daily.",
        )];
        let matched = plan_decision_quote_match("note the convention", &texts);
        assert!(!matched.verified);
        // Both halves overlap equally; the first sentence wins.
        let closest = matched.closest.unwrap();
        assert_eq!(closest.sentence, "Please note the rules.");
    }

    #[test]
    fn halves_split_across_two_texts_do_not_verify() {
        let texts = vec![
            text("a", "1", "Please note the endless preamble words."),
            text("b", "2", "Some other convention wording here."),
        ];
        // Each half appears alone, but no single text holds the run.
        let matched =
            plan_decision_quote_match("preamble words some other", &texts);
        assert!(!matched.verified);
    }

    #[test]
    fn partial_words_never_match() {
        let texts =
            vec![text("note", "tui.md", "Noteworthy conventions abound.")];
        let matched = plan_decision_quote_match("note the convention", &texts);
        assert!(!matched.verified);
    }

    #[test]
    fn unicode_normalization_matches_across_forms() {
        // Curly quotes, a spaced em dash, a full-width word, sharp-s,
        // and a decomposed accent all fold to the same normalized run.
        let texts = vec![text(
            "note",
            "tui.md",
            "\u{201c}Note the stra\u{df}e \u{2014} \u{ff54}\u{ff55}\u{ff49}.md cafe\u{301} please\u{201d}",
        )];
        let matched = plan_decision_quote_match(
            "note the strasse - tui.md caf\u{e9} please",
            &texts,
        );
        assert!(matched.verified, "{matched:?}");
    }

    #[test]
    fn greek_sigma_folds_across_positions() {
        let texts = vec![text(
            "web",
            "w1",
            "The \u{3a3}\u{3af}\u{3b3}\u{3bc}\u{3b1} holds three words here.",
        )];
        let matched = plan_decision_quote_match(
            "\u{3c3}\u{3af}\u{3b3}\u{3bc}\u{3b1} holds three",
            &texts,
        );
        assert!(matched.verified, "{matched:?}");
    }

    #[test]
    fn negated_sentence_is_still_a_lexical_match() {
        let texts = vec![text(
            "note",
            "tui.md",
            "Do not note the convention in the tui memory ever.",
        )];
        let matched = plan_decision_quote_match(
            "note the convention in the tui memory",
            &texts,
        );
        assert!(matched.verified);
    }

    #[test]
    fn sentences_span_newlines_and_md_names_survive() {
        let texts = vec![text(
            "note",
            "tui.md",
            "See tui.md for details.\nNote the convention\nin the tui memory today.",
        )];
        // The newline-joined run verifies as one whitespace run.
        let matched = plan_decision_quote_match(
            "note the convention in the tui memory today",
            &texts,
        );
        assert!(matched.verified, "{matched:?}");
    }

    #[test]
    fn suggestion_ties_stay_stable_by_input_order() {
        let texts = vec![
            text(
                "b",
                "2",
                "Unrelated filler words here. Keep the convention safe.",
            ),
            text(
                "a",
                "1",
                "Keep the convention safe. Other filler words here.",
            ),
        ];
        let matched = plan_decision_quote_match(
            "missing quote tokens entirely here",
            &texts,
        );
        assert!(!matched.verified);
        let closest = matched.closest.unwrap();
        // Both sentences share one token ("here"); the first text wins.
        assert_eq!(closest.source, "b");
        assert_eq!(closest.sentence, "Unrelated filler words here.");
    }

    #[test]
    fn empty_sources_and_zero_overlap_yield_no_hint() {
        let empty = plan_decision_quote_match("note the convention", &[]);
        assert!(!empty.verified);
        assert!(empty.closest.is_none());

        let texts = vec![text("note", "tui.md", "Zebra xylophone quark.")];
        let missed = plan_decision_quote_match("note the convention", &texts);
        assert!(!missed.verified);
        assert!(missed.closest.is_none());
    }

    #[test]
    fn sentence_splitter_keeps_original_text() {
        let sentences = split_sentences("First.  Second! Third?\nFourth");
        assert_eq!(sentences, vec!["First.", "Second!", "Third?", "Fourth"]);
        // A dot inside a word is not a boundary.
        let sentences = split_sentences("See tui.md for details. Done.");
        assert_eq!(sentences, vec!["See tui.md for details.", "Done."]);
    }
}
