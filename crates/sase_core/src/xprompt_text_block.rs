//! Canonical `[[...]]` argument text-block closing rule.
//!
//! A `[[` opens a text block. It closes at the first `]]` whose next
//! non-whitespace character is an argument terminator — `,`, `)`, `}`, `|`,
//! or the end of the scanned region. A `]]` anywhere else is ordinary content.

/// Argument-region terminators that may follow a real text-block closer.
pub(crate) const TEXT_BLOCK_ARGUMENT_TERMINATORS: &[u8] = b",)}|";

/// Return the byte index of the closing `]]` for a `[[...]]` argument text block.
///
/// `start` must point at the opening `[[`. `end` is the exclusive end of the
/// scanned region and counts as a terminator. Returns `None` when `start` is
/// not a text-block opener or when no `]]` sits in terminator position (the
/// block then runs through `end`, matching unterminated-block behavior).
pub(crate) fn find_text_block_close_for_args(
    text: &str,
    start: usize,
    end: usize,
) -> Option<usize> {
    find_text_block_close_for_args_bytes(text.as_bytes(), start, end)
}

/// Byte-slice form of [`find_text_block_close_for_args`].
pub(crate) fn find_text_block_close_for_args_bytes(
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Option<usize> {
    let end = end.min(bytes.len());
    if start + 1 >= end || bytes.get(start..start + 2) != Some(b"[[") {
        return None;
    }
    let mut search_start = start + 2;
    while search_start + 1 < end {
        if bytes[search_start] == b']' && bytes[search_start + 1] == b']' {
            let close = search_start;
            let mut next = close + 2;
            while next < end && bytes[next].is_ascii_whitespace() {
                next += 1;
            }
            if next >= end
                || TEXT_BLOCK_ARGUMENT_TERMINATORS.contains(&bytes[next])
            {
                return Some(close);
            }
            search_start = close + 1;
        } else {
            search_start += 1;
        }
    }
    None
}

#[cfg(test)]
use serde::Deserialize;

#[cfg(test)]
#[derive(Debug, Deserialize)]
pub(crate) struct XpromptArgsCorpusCase {
    pub id: String,
    pub source: String,
    pub positional: Vec<String>,
    #[serde(default)]
    pub named: std::collections::BTreeMap<String, String>,
}

#[cfg(test)]
#[derive(Debug, Deserialize)]
struct XpromptArgsCorpusFile {
    schema_version: u32,
    cases: Vec<XpromptArgsCorpusCase>,
}

#[cfg(test)]
pub(crate) fn xprompt_args_corpus() -> Vec<XpromptArgsCorpusCase> {
    let file: XpromptArgsCorpusFile = serde_json::from_str(include_str!(
        "../tests/fixtures/xprompt_args_corpus.json"
    ))
    .expect("xprompt args corpus must parse");
    assert_eq!(file.schema_version, 1, "unexpected corpus schema_version");
    assert!(!file.cases.is_empty(), "corpus must not be empty");
    file.cases
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close_in(text: &str) -> Option<usize> {
        find_text_block_close_for_args(text, 0, text.len())
    }

    #[test]
    fn rejects_non_opener() {
        assert_eq!(close_in("]foo]"), None);
        assert_eq!(find_text_block_close_for_args("[[x]]", 1, 5), None);
    }

    #[test]
    fn closes_at_end_of_region() {
        assert_eq!(close_in("[[a [b [c]] d, e]]"), Some(16));
    }

    #[test]
    fn skips_inner_marker_before_comma_terminator() {
        let text = "[[note: use ]] here, and more]]";
        assert_eq!(close_in(text), Some(text.len() - 2));
    }

    #[test]
    fn closes_before_comma_of_next_argument() {
        assert_eq!(close_in("[[a]], [[b]]"), Some(3));
    }

    #[test]
    fn closes_before_paren_brace_or_pipe() {
        assert_eq!(close_in("[[a]])"), Some(3));
        assert_eq!(close_in("[[a]]}"), Some(3));
        assert_eq!(close_in("[[a]]|"), Some(3));
        assert_eq!(close_in("[[a]]  )"), Some(3));
    }

    #[test]
    fn unterminated_block_returns_none() {
        assert_eq!(close_in("[[no closer"), None);
        assert_eq!(close_in("[[inner ]] still going"), None);
    }

    #[test]
    fn overlapping_closer_before_paren() {
        // `[[foo]]]])` closes at the last terminator-position `]]`.
        let text = "[[foo]]]])";
        assert_eq!(close_in(text), Some(7));
    }

    #[test]
    fn shared_corpus_loads() {
        assert!(!xprompt_args_corpus().is_empty());
    }
}
