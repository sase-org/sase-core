//! Branch-callout parsing for Plan Decisions.
//!
//! A callout header is `> [!decision] <id>`, `> [!decision] <id> = <key>`,
//! or `> [!decision] <id> = no` on a blockquote line outside fenced code.
//! The callout span runs to the end of that blockquote. All line numbers are
//! original-document lines (frontmatter included).

use super::wire::PlanDecisionCalloutWire;

/// One parsed callout header before branch validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCallout {
    pub id: String,
    /// The branch token after `=`, if any. Prose after it is ignored.
    pub value: Option<String>,
    /// Original-document line of the header (for diagnostics).
    pub header_line: u64,
    pub start_line: u64,
    pub end_line: u64,
}

/// One apparent memory edit in the body: a `sase memory init` mention or a
/// `sase/memory/` path on a line with an edit verb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEditSignal {
    pub line: u64,
    pub is_init: bool,
    /// The path after `sase/memory/`; absent for `init` mentions.
    pub note: Option<String>,
}

/// Body lines visible outside backtick and tilde fenced code, each with its
/// body-relative line index.
fn visible_lines(body: &str) -> Vec<(usize, &str)> {
    let mut visible = Vec::new();
    let mut fence: Option<char> = None;
    for (index, line) in body.split('\n').enumerate() {
        let trimmed = line.trim_start();
        let is_fence = trimmed.starts_with("```") || trimmed.starts_with("~~~");
        if is_fence {
            let marker = trimmed.chars().next().unwrap_or('`');
            if fence == Some(marker) {
                fence = None;
            } else if fence.is_none() {
                fence = Some(marker);
            }
            continue;
        }
        if fence.is_none() {
            visible.push((index, line));
        }
    }
    visible
}

fn is_blockquote(line: &str) -> bool {
    line.trim_start_matches(' ').starts_with('>')
}

/// Parse every callout header outside fenced code with its blockquote span.
///
/// A header line closes any open span and starts a new one. Malformed headers
/// (missing id, missing branch after `=`) still produce a [`RawCallout`] with
/// an empty id or value so validation can report them.
pub fn parse_decision_callouts(body: &str, body_base: u64) -> Vec<RawCallout> {
    let mut callouts = Vec::new();
    let mut open: Option<RawCallout> = None;
    for (index, line) in visible_lines(body) {
        let document_line = body_base + index as u64;
        if !is_blockquote(line) {
            if let Some(callout) = open.take() {
                callouts.push(callout);
            }
            continue;
        }
        if let Some(header) = parse_callout_header(line) {
            if let Some(callout) = open.take() {
                callouts.push(callout);
            }
            let (id, value) = header;
            open = Some(RawCallout {
                id,
                value,
                header_line: document_line,
                start_line: document_line,
                end_line: document_line,
            });
        } else if let Some(callout) = open.as_mut() {
            callout.end_line = document_line;
        }
    }
    if let Some(callout) = open {
        callouts.push(callout);
    }
    callouts
}

/// Split a blockquote line into `(id, Option<branch-token>)` when it carries
/// the `[!decision]` marker. Returns `None` for ordinary quote lines.
fn parse_callout_header(line: &str) -> Option<(String, Option<String>)> {
    let stripped = line.trim_start_matches(' ');
    let rest = stripped.strip_prefix('>')?;
    let rest = rest.trim_start();
    let marker = "[!decision]";
    if !rest.starts_with(marker) {
        return None;
    }
    let after = rest[marker.len()..].trim_start();
    if after.is_empty() {
        return Some((String::new(), None));
    }
    match after.find('=') {
        None => {
            let id = first_token(after);
            Some((id, None))
        }
        Some(cut) => {
            let id = after[..cut].trim().to_string();
            let value_part = after[cut + 1..].trim();
            if value_part.is_empty() {
                return Some((id, Some(String::new())));
            }
            Some((id, Some(first_token(value_part))))
        }
    }
}

fn first_token(text: &str) -> String {
    text.split_whitespace().next().unwrap_or("").to_string()
}

/// True when `id` appears as a whole word in the body outside fenced code.
pub fn whole_word_mentions_body(body: &str, id: &str) -> bool {
    if id.is_empty() {
        return false;
    }
    visible_lines(body)
        .iter()
        .any(|(_, line)| line_contains_word(line, id))
}

fn is_word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn line_contains_word(line: &str, word: &str) -> bool {
    let bytes = line.as_bytes();
    let needle = word.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }
    bytes
        .windows(needle.len())
        .enumerate()
        .any(|(index, window)| {
            window == needle
                && (index == 0 || !is_word_char(bytes[index - 1]))
                && (index + needle.len() == bytes.len()
                    || !is_word_char(bytes[index + needle.len()]))
        })
}

/// Whole-word, case-insensitive match for one verb or phrase in a line.
fn line_contains_phrase(lower_line: &str, phrase: &str) -> bool {
    let bytes = lower_line.as_bytes();
    let needle = phrase.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }
    bytes
        .windows(needle.len())
        .enumerate()
        .any(|(index, window)| {
            window == needle
                && (index == 0 || !is_word_char(bytes[index - 1]))
                && (index + needle.len() == bytes.len()
                    || !is_word_char(bytes[index + needle.len()]))
        })
}

fn line_has_edit_verb(lower_line: &str) -> bool {
    [
        "add", "edit", "update", "create", "delete", "remove", "rewrite",
    ]
    .iter()
    .any(|verb| line_contains_phrase(lower_line, verb))
}

/// The `;`- or `:`-separated clause holding a byte offset. Negation is
/// clause-scoped, so `Update <path>: freshness never changes` still fires:
/// the disclaimer words belong to another clause.
fn clause_containing(line: &str, offset: usize) -> &str {
    let bytes = line.as_bytes();
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b';' || *byte == b':' {
            if offset < index {
                return line[start..index].trim();
            }
            start = index + 1;
        }
    }
    line[start..].trim()
}

/// Disclaimer phrasing that keeps a `sase/memory/` path clause quiet, e.g.
/// the hand-written "do not edit memory" disclaimers. `sase memory init`
/// mentions always fire: every archived mention is an actionable step.
fn line_is_disclaimer(lower_line: &str) -> bool {
    [
        "do not",
        "don't",
        "dont",
        "never",
        "not",
        "without",
        "avoid",
        "stop",
        "refuse",
        "against",
        "prohibit",
        "forbid",
        "should not",
        "must not",
        "cannot",
        "can't",
        "no memory edits",
        "no other",
    ]
    .iter()
    .any(|phrase| line_contains_phrase(lower_line, phrase))
}

fn is_path_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'/')
}

/// The first `sase/memory/<path>` occurrence that is not source code
/// (`src/sase/memory/...`) or part of a longer token. Returns the path with
/// trailing punctuation stripped plus its byte offset in the line.
fn memory_note_on_line(line: &str) -> Option<(String, usize)> {
    const PREFIX: &str = "sase/memory/";
    let bytes = line.as_bytes();
    let mut search_from = 0;
    while let Some(relative) = line[search_from..]
        .find(PREFIX)
        .map(|index| search_from + index)
    {
        let preceded_ok = relative == 0 || {
            let prev = bytes[relative - 1];
            !prev.is_ascii_alphanumeric() && prev != b'_' && prev != b'.'
        };
        let src_prefixed =
            relative >= 4 && &line[relative - 4..relative] == "src/";
        if preceded_ok && !src_prefixed {
            let mut end = relative + PREFIX.len();
            while end < bytes.len() && is_path_char(bytes[end]) {
                end += 1;
            }
            let mut note = line[relative + PREFIX.len()..end].to_string();
            while matches!(
                note.as_bytes().last(),
                Some(b'.' | b',' | b';' | b':' | b'!' | b'?' | b')' | b']')
            ) {
                note.pop();
            }
            if !note.is_empty() {
                return Some((note, relative));
            }
        }
        search_from = relative + PREFIX.len();
    }
    None
}

/// Collect apparent memory edits: `sase memory init` mentions plus
/// `sase/memory/` paths on lines with an edit verb and no disclaimer.
pub fn uncovered_memory_edits(
    body: &str,
    body_base: u64,
) -> Vec<MemoryEditSignal> {
    let mut signals = Vec::new();
    for (index, line) in visible_lines(body) {
        let document_line = body_base + index as u64;
        if line.contains("sase memory init") {
            signals.push(MemoryEditSignal {
                line: document_line,
                is_init: true,
                note: None,
            });
            continue;
        }
        if let Some((note, offset)) = memory_note_on_line(line) {
            let lower = line.to_lowercase();
            let clause = clause_containing(line, offset).to_lowercase();
            if line_has_edit_verb(&lower) && !line_is_disclaimer(&clause) {
                signals.push(MemoryEditSignal {
                    line: document_line,
                    is_init: false,
                    note: Some(note),
                });
            }
        }
    }
    signals
}

/// Build the validated wire for one fully-validated callout.
pub fn callout_wire(
    id: &str,
    key: Option<&str>,
    branch: &str,
    start_line: u64,
    end_line: u64,
) -> PlanDecisionCalloutWire {
    PlanDecisionCalloutWire {
        id: id.to_string(),
        key: key.map(str::to_string),
        branch: branch.to_string(),
        start_line,
        end_line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `body_base` 8 puts the first body line on document line 8, matching a
    /// five-line tale frontmatter block.
    #[test]
    fn headers_parse_with_branches_and_prose() {
        let body = "> [!decision] tui_note\n\
            > [!decision] grouping = mode because it scales\n\
            > [!decision] tui_note = no thanks, skip it\n";
        let callouts = parse_decision_callouts(body, 8);
        assert_eq!(callouts.len(), 3);
        assert_eq!(callouts[0].id, "tui_note");
        assert_eq!(callouts[0].value, None);
        assert_eq!(callouts[1].id, "grouping");
        assert_eq!(callouts[1].value.as_deref(), Some("mode"));
        assert_eq!(callouts[2].id, "tui_note");
        assert_eq!(callouts[2].value.as_deref(), Some("no"));
    }

    #[test]
    fn missing_id_or_branch_is_still_reported() {
        let body = "> [!decision]\n> [!decision] tui_note =\n";
        let callouts = parse_decision_callouts(body, 8);
        assert_eq!(callouts.len(), 2);
        assert_eq!(callouts[0].id, "");
        assert_eq!(callouts[1].id, "tui_note");
        assert_eq!(callouts[1].value.as_deref(), Some(""));
    }

    #[test]
    fn spans_end_at_the_blockquote_boundary() {
        let body = "# Plan\n\
            > [!decision] tui_note\n\
            > first reason\n\
            > second reason\n\
            \n\
            After the break.\n\
            > [!decision] grouping = mode\n\
            > single line\n";
        let callouts = parse_decision_callouts(body, 8);
        assert_eq!(callouts.len(), 2);
        // Document lines: header at 9, span through 11, blank ends it.
        assert_eq!((callouts[0].start_line, callouts[0].end_line), (9, 11));
        assert_eq!((callouts[1].start_line, callouts[1].end_line), (14, 15));
    }

    #[test]
    fn adjacent_headers_start_adjacent_spans() {
        let body = "> [!decision] first\n> [!decision] second\n> tail\n";
        let callouts = parse_decision_callouts(body, 8);
        assert_eq!(callouts.len(), 2);
        assert_eq!((callouts[0].start_line, callouts[0].end_line), (8, 8));
        assert_eq!((callouts[1].start_line, callouts[1].end_line), (9, 10));
    }

    #[test]
    fn fenced_headers_and_mentions_are_ignored() {
        let body = "```markdown\n\
            > [!decision] fenced_id\n\
            fenced_id\n\
            ```\n\
            ~~~\n\
            > [!decision] tilde_id\n\
            ~~~\n\
            > [!decision] real_id\n";
        let callouts = parse_decision_callouts(body, 8);
        assert_eq!(callouts.len(), 1);
        assert_eq!(callouts[0].id, "real_id");
        assert!(!whole_word_mentions_body(body, "fenced_id"));
        assert!(!whole_word_mentions_body(body, "tilde_id"));
        assert!(whole_word_mentions_body(body, "real_id"));
    }

    #[test]
    fn mentions_need_whole_words() {
        let body = "tui_notes and atui_note stay quiet\ntui_note. matches\n";
        assert!(!whole_word_mentions_body(body, "missing"));
        assert!(!whole_word_mentions_body(body, ""));
        // `tui_notes`/`atui_note` must not count for `tui_note`.
        assert!(!whole_word_mentions_body("tui_notes", "tui_note"));
        assert!(!whole_word_mentions_body("atui_note", "tui_note"));
        assert!(whole_word_mentions_body(body, "tui_note"));
    }

    #[test]
    fn memory_signals_cover_init_paths_disclaimers_and_code() {
        // `sase memory init` mentions fire even beside disclaimer words: every
        // archived mention is an actionable step.
        let signals = uncovered_memory_edits(
            "Run `sase memory init`; never hand-edit generated shims.\n",
            8,
        );
        assert_eq!(signals.len(), 1);
        assert!(signals[0].is_init);

        let signals = uncovered_memory_edits(
            "Add `sase/memory/decisions/note.md`, titled 'Note'.\n",
            8,
        );
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].note.as_deref(), Some("decisions/note.md"));

        // Trailing punctuation is not part of the note.
        let signals = uncovered_memory_edits(
            "Update `sase/memory/tui.md`, then verify.\n",
            8,
        );
        assert_eq!(signals[0].note.as_deref(), Some("tui.md"));

        // Negation is clause-scoped: a disclaimer in another clause does not
        // suppress the edit, and a verb elsewhere on the line still counts.
        let signals = uncovered_memory_edits(
            "Update `sase/memory/glossary/tui.md`: freshness never changes.\n",
            8,
        );
        assert_eq!(signals.len(), 1);
        let signals = uncovered_memory_edits(
            "Update the following: `sase/memory/tui.md`.\n",
            8,
        );
        assert_eq!(signals.len(), 1);

        // Disclaimers, read-only references, source-code paths, fenced
        // examples, and verbs outside the Section 2 list stay quiet.
        for quiet in [
            "Do not edit `sase/memory/tui.md`.\n",
            "Read `sase/memory/cli_rules.md` first.\n",
            "Add `src/sase/memory/history/pager_provider.py`.\n",
            "```\nRun `sase memory init`.\n```\n",
            "Replace `sase/memory/cli_rules.md` body below.\n",
        ] {
            assert_eq!(
                uncovered_memory_edits(quiet, 8),
                Vec::new(),
                "should stay quiet: {quiet:?}"
            );
        }
    }

    #[test]
    fn bare_memory_dir_without_a_path_is_not_a_signal() {
        assert_eq!(
            uncovered_memory_edits("Do not touch `sase/memory/`.\n", 8),
            Vec::new()
        );
    }
}
