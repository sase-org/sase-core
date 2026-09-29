//! Prose tokenizer shared by corpus compile and query.
//!
//! Compile and query share this tokenizer so context extraction always
//! matches training. Words carry a casefolded key plus a display surface;
//! everything else is a boundary. Excluded regions (frontmatter, fenced
//! code, inline code, Jinja, segment separators, pasted blocks) are hard
//! boundaries: their content yields no tokens and a cursor inside an
//! unclosed one blocks prediction.

use std::collections::HashMap;

use crate::fenced_code::fenced_block_ranges;
use crate::prompt_literals::inline_code_ranges;

/// Sequence-start marker. It counts as a context token but is never predicted.
pub const SEQUENCE_START: &str = "<s>";

/// Cursor-blocked reasons returned in `blocked_reason`.
pub const BLOCKED_UNCLOSED_FENCE: &str = "unclosed_fence";
pub const BLOCKED_UNCLOSED_CODE_SPAN: &str = "unclosed_code_span";
pub const BLOCKED_UNCLOSED_JINJA: &str = "unclosed_jinja";
pub const BLOCKED_FRONTMATTER: &str = "frontmatter";
pub const BLOCKED_NO_WORD_CONTEXT: &str = "no_word_context";
pub const BLOCKED_STRUCTURAL_TAIL: &str = "structural_tail";

/// One prose word occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProseToken {
    /// Casefolded key (`’` mapped to `'`).
    pub key: String,
    /// Display surface as typed.
    pub surface: String,
    /// True when the word opens a sequence.
    pub sequence_initial: bool,
}

/// One tokenized sequence: words plus whether it opened with `<s>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProseSequence {
    pub tokens: Vec<ProseToken>,
    pub started: bool,
}

/// Tokenize full prompt text into sequences.
pub fn tokenize_prompt_text(text: &str) -> Vec<ProseSequence> {
    let excluded = excluded_ranges(text);
    tokenize_with_excluded(text, &excluded)
}

/// Outcome of tokenizing the text before the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorContext {
    Blocked {
        reason: &'static str,
    },
    Ready {
        /// Context tokens oldest-first (`<s>` included where sequences start
        /// with it).
        context: Vec<String>,
        /// Display surfaces for the evidence context (`<s>` omitted).
        context_words: Vec<String>,
        /// Surfaces of the current (incomplete) sequence, for the draft
        /// source and continuation.
        sequences: Vec<ProseSequence>,
        /// Byte offset where the current sequence's word run starts.
        sequence_has_word: bool,
    },
}

/// Tokenize `text_before_cursor` for a prediction query.
pub fn tokenize_cursor_text(text: &str) -> CursorContext {
    if cursor_in_frontmatter(text) {
        return CursorContext::Blocked {
            reason: BLOCKED_FRONTMATTER,
        };
    }
    if in_unclosed_fence(text) {
        return CursorContext::Blocked {
            reason: BLOCKED_UNCLOSED_FENCE,
        };
    }
    if in_unclosed_backtick_span(text) {
        return CursorContext::Blocked {
            reason: BLOCKED_UNCLOSED_CODE_SPAN,
        };
    }
    if in_unclosed_jinja(text) {
        return CursorContext::Blocked {
            reason: BLOCKED_UNCLOSED_JINJA,
        };
    }
    let excluded = excluded_ranges(text);
    if cursor_in_excluded(text.len(), &excluded) {
        // Closed excluded region touching the cursor (e.g. cursor right
        // after a code span or Jinja tag with no trailing word) behaves
        // like a structural tail rather than a word context.
        let sequences = tokenize_with_excluded(text, &excluded);
        let Some(last) = sequences.last() else {
            return CursorContext::Blocked {
                reason: BLOCKED_NO_WORD_CONTEXT,
            };
        };
        if last.tokens.is_empty() {
            return CursorContext::Blocked {
                reason: BLOCKED_NO_WORD_CONTEXT,
            };
        }
    }
    let sequences = tokenize_with_excluded(text, &excluded);
    finish_cursor_context(text, sequences)
}

fn finish_cursor_context(
    text: &str,
    sequences: Vec<ProseSequence>,
) -> CursorContext {
    let Some(last) = sequences.last() else {
        return CursorContext::Blocked {
            reason: BLOCKED_NO_WORD_CONTEXT,
        };
    };
    if last.tokens.is_empty() {
        // Either empty input or a structural tail (trailing boundary
        // punctuation, a bare marker, whitespace after a boundary).
        if text.trim().is_empty() {
            return CursorContext::Blocked {
                reason: BLOCKED_NO_WORD_CONTEXT,
            };
        }
        return CursorContext::Blocked {
            reason: BLOCKED_STRUCTURAL_TAIL,
        };
    }
    // A trailing space does not change the context: the last token is
    // still the last word (auto-mode predicts right after a typed space).
    // Boundaries that matter (structural tails, sentence ends, excluded
    // regions) already leave the final sequence empty and blocked above.
    let mut context: Vec<String> = Vec::new();
    if last.started {
        context.push(SEQUENCE_START.to_string());
    }
    let mut context_words: Vec<String> = Vec::new();
    for token in &last.tokens {
        context.push(token.key.clone());
        context_words.push(token.surface.clone());
    }
    CursorContext::Ready {
        context,
        context_words,
        sequence_has_word: true,
        sequences,
    }
}

/// Byte ranges of excluded regions (closed only).
fn excluded_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    if let Some(range) = frontmatter_range(text) {
        ranges.push(range);
    }
    for (start, end) in fenced_block_ranges(text) {
        ranges.push((start, end));
    }
    let masks = ranges.clone();
    for (start, end) in inline_code_ranges(text, &masks) {
        ranges.push((start, end));
    }
    ranges.extend(jinja_ranges(text));
    ranges.extend(pasted_block_ranges(text));
    // Segment separators are line-based; mark the whole line excluded.
    ranges.extend(segment_separator_ranges(text));
    ranges.sort();
    ranges
}

fn cursor_in_excluded(cursor: usize, ranges: &[(usize, usize)]) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start < cursor && cursor <= *end)
}

/// Leading YAML frontmatter range (`---` ... `---`), if present.
fn frontmatter_range(text: &str) -> Option<(usize, usize)> {
    let mut lines = text.split_inclusive('\n');
    let first = lines.next()?;
    if first.trim_end_matches(['\n', '\r']) != "---" {
        return None;
    }
    let mut offset = first.len();
    for line in lines {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        offset += line.len();
        if trimmed == "---" || trimmed == "..." {
            return Some((0, offset));
        }
    }
    // Unclosed frontmatter excludes everything.
    Some((0, text.len()))
}

fn cursor_in_frontmatter(text: &str) -> bool {
    let mut lines = text.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return false;
    };
    if first.trim_end_matches(['\n', '\r']) != "---" {
        return false;
    }
    let mut offset = first.len();
    for line in lines {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        offset += line.len();
        if trimmed == "---" || trimmed == "..." {
            return false;
        }
    }
    // Never closed: the cursor is inside frontmatter.
    let _ = offset;
    true
}

fn in_unclosed_fence(text: &str) -> bool {
    // A fence is unclosed when the last opening fence has no closer.
    let mut open: Option<usize> = None;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            if open.is_none() {
                open = Some(1);
            } else {
                open = None;
            }
        }
    }
    open.is_some()
}

fn in_unclosed_backtick_span(text: &str) -> bool {
    // Count unmasked backticks on the last line; an odd count whose opener
    // has no same-length closer on the same line is unclosed. Approximate
    // with the shared scanner: any trailing run without a partner.
    let last_line = text.rsplit('\n').next().unwrap_or(text);
    let backticks = last_line.bytes().filter(|b| *b == b'`').count();
    if backticks == 0 {
        return false;
    }
    let spans = inline_code_ranges(text, &[]);
    // If the last line holds backticks but no span ends after the last
    // backtick position, the span is unclosed.
    let last_tick = text.rfind('`').unwrap_or(0);
    !spans.iter().any(|(_, end)| *end > last_tick) && backticks % 2 == 1
}

fn jinja_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'{'
            && (bytes[i + 1] == b'{'
                || bytes[i + 1] == b'%'
                || bytes[i + 1] == b'#')
        {
            let closer = match bytes[i + 1] {
                b'{' => "}}",
                b'%' => "%}",
                _ => "#}",
            };
            if let Some(end) = text[i + 2..].find(closer) {
                ranges.push((i, i + 2 + end + 2));
                i += 2 + end + 2;
                continue;
            }
            // Unclosed opener runs to end of text.
            ranges.push((i, text.len()));
            break;
        }
        i += 1;
    }
    ranges
}

fn in_unclosed_jinja(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'{'
            && (bytes[i + 1] == b'{'
                || bytes[i + 1] == b'%'
                || bytes[i + 1] == b'#')
        {
            let closer = match bytes[i + 1] {
                b'{' => "}}",
                b'%' => "%}",
                _ => "#}",
            };
            match text[i + 2..].find(closer) {
                Some(end) => {
                    i += 2 + end + 2;
                    continue;
                }
                None => return true,
            }
        }
        i += 1;
    }
    false
}

/// Lines that are exactly `---` (segment separators).
fn segment_separator_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']).trim();
        if trimmed == "---" && offset != 0 {
            // Offset != 0 keeps the frontmatter opener out; the
            // frontmatter scan already owns it.
            ranges.push((offset, offset + line.len()));
        }
        offset += line.len();
    }
    ranges
}

/// Pasted-block lines: more than 400 tokens, or fewer than half words.
fn pasted_block_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let len = line.len();
        if is_pasted_line(line) {
            ranges.push((offset, offset + len));
        }
        offset += len;
    }
    ranges
}

fn is_pasted_line(line: &str) -> bool {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() > 400 {
        return true;
    }
    if tokens.len() < 8 {
        return false;
    }
    let words = tokens.iter().filter(|t| looks_like_word(t)).count();
    words * 2 < tokens.len()
}

fn looks_like_word(raw: &str) -> bool {
    let cleaned = strip_affixes(raw);
    !cleaned.is_empty()
        && cleaned.chars().any(|ch| ch.is_alphabetic())
        && cleaned.len() <= 32
}

fn tokenize_with_excluded(
    text: &str,
    excluded: &[(usize, usize)],
) -> Vec<ProseSequence> {
    let mut sequences: Vec<ProseSequence> = vec![ProseSequence {
        tokens: Vec::new(),
        started: true,
    }];
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let line_len = line.len();
        let line_start = offset;
        let body = line.strip_suffix('\n').unwrap_or(line);
        let body = body.strip_suffix('\r').unwrap_or(body);
        process_line(body, line_start, excluded, &mut sequences);
        offset += line_len;
    }
    // Drop a single trailing empty sequence; keep empties that mark a
    // structural tail for blocked detection.
    while sequences.len() > 1
        && sequences
            .last()
            .is_some_and(|seq| seq.tokens.is_empty() && !seq.started)
    {
        sequences.pop();
    }
    sequences
}

fn in_ranges(pos: usize, ranges: &[(usize, usize)]) -> bool {
    ranges.iter().any(|(s, e)| *s <= pos && pos < *e)
}

fn process_line(
    body: &str,
    line_start: usize,
    excluded: &[(usize, usize)],
    sequences: &mut Vec<ProseSequence>,
) {
    if body.trim().is_empty() {
        start_sequence(sequences, true);
        return;
    }
    // Line-start markers force a fresh `<s>` sequence.
    let indented = body.trim_start();
    if is_line_marker(indented) {
        start_sequence(sequences, true);
        // Tokenize the remainder after the marker.
        let marker_len = body.len() - indented.len();
        let (_, marker_bytes) = marker_prefix_len(indented);
        let rest_start = marker_len + marker_bytes;
        tokenize_line_words(body, rest_start, line_start, excluded, sequences);
        return;
    }
    tokenize_line_words(body, 0, line_start, excluded, sequences);
}

fn start_sequence(sequences: &mut Vec<ProseSequence>, started: bool) {
    if sequences.last().is_some_and(|last| last.tokens.is_empty()) {
        // Merge into the pending empty sequence, upgrading `<s>` when set.
        if started {
            sequences.last_mut().expect("last").started = true;
        }
        return;
    }
    sequences.push(ProseSequence {
        tokens: Vec::new(),
        started,
    });
}

fn is_line_marker(trimmed: &str) -> bool {
    if trimmed.starts_with("> ") || trimmed == ">" {
        return true;
    }
    if let Some(rest) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("+ "))
    {
        let _ = rest;
        return true;
    }
    if trimmed == "-" || trimmed == "*" || trimmed == "+" {
        return true;
    }
    if trimmed.starts_with("#") {
        let hashes = trimmed.chars().take_while(|c| *c == '#').count();
        if (1..=6).contains(&hashes)
            && trimmed.chars().nth(hashes).is_some_and(|c| c == ' ')
        {
            return true;
        }
    }
    // `1. `, `2) ` ordered markers.
    let digits = trimmed.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits > 0
        && trimmed
            .chars()
            .nth(digits)
            .is_some_and(|c| c == '.' || c == ')')
        && trimmed.chars().nth(digits + 1).is_some_and(|c| c == ' ')
    {
        return true;
    }
    false
}

/// Byte length of the marker prefix (marker chars plus one space).
fn marker_prefix_len(trimmed: &str) -> (&str, usize) {
    if trimmed.starts_with("> ") {
        return (">", 2);
    }
    for marker in ["- ", "* ", "+ "] {
        if trimmed.starts_with(marker) {
            return (marker, 2);
        }
    }
    if trimmed.starts_with("#") {
        let hashes = trimmed.chars().take_while(|c| *c == '#').count();
        return (&trimmed[..hashes], hashes + 1);
    }
    let digits = trimmed.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits > 0 {
        return (&trimmed[..digits + 1], digits + 2);
    }
    ("", 0)
}

fn tokenize_line_words(
    body: &str,
    from: usize,
    line_start: usize,
    excluded: &[(usize, usize)],
    sequences: &mut Vec<ProseSequence>,
) {
    let bytes = body.as_bytes();
    let mut i = from;
    // Skip the marker-adjacent whitespace.
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    while i < bytes.len() {
        // Skip whitespace.
        if bytes[i] == b' ' || bytes[i] == b'\t' {
            i += 1;
            continue;
        }
        let abs = line_start + i;
        // Excluded content is a hard boundary.
        if in_ranges(abs, excluded) {
            // Skip to the end of the excluded run and start a fresh
            // empty-context sequence (code spans do not set `<s>`).
            let end = excluded
                .iter()
                .filter(|(s, e)| *s <= abs && abs < *e)
                .map(|(_, e)| *e)
                .max()
                .unwrap_or(abs + 1);
            let skip = end - line_start;
            i = skip.max(i + 1).min(bytes.len());
            start_sequence(sequences, false);
            continue;
        }
        // Raw token extends to whitespace.
        let mut j = i;
        while j < bytes.len() && bytes[j] != b' ' && bytes[j] != b'\t' {
            j += 1;
        }
        let raw = &body[i..j];
        handle_raw_token(raw, sequences);
        i = j;
    }
}

fn handle_raw_token(raw: &str, sequences: &mut Vec<ProseSequence>) {
    // Split trailing sentence punctuation: `word.`, `word?`, `word!`,
    // `word…`, `word:`, `word;`.
    let (core, boundary) = split_trailing_boundary(raw);
    if is_structural_token(core) || core.is_empty() {
        // Mid-sentence structural token: boundary with empty context.
        if !core.is_empty() {
            start_sequence_if_words(sequences, false);
        }
        apply_sentence_boundary(sequences, boundary);
        return;
    }
    match classify_word(core) {
        Some((key, surface)) => {
            let seq = sequences.last_mut().expect("sequence");
            let initial = seq.tokens.is_empty();
            seq.tokens.push(ProseToken {
                key,
                surface,
                sequence_initial: initial,
            });
            apply_sentence_boundary(sequences, boundary);
        }
        None => {
            start_sequence_if_words(sequences, false);
            apply_sentence_boundary(sequences, boundary);
        }
    }
}

fn start_sequence_if_words(sequences: &mut Vec<ProseSequence>, started: bool) {
    if sequences.last().is_some_and(|seq| !seq.tokens.is_empty()) {
        sequences.push(ProseSequence {
            tokens: Vec::new(),
            started,
        });
    } else if started {
        sequences.last_mut().expect("last").started = true;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrailingBoundary {
    None,
    Sentence,
    Clause,
}

fn split_trailing_boundary(raw: &str) -> (&str, TrailingBoundary) {
    if let Some(stripped) = raw.strip_suffix("…") {
        return (stripped, TrailingBoundary::Sentence);
    }
    let bytes = raw.as_bytes();
    if bytes.is_empty() {
        return (raw, TrailingBoundary::None);
    }
    let last = bytes[bytes.len() - 1] as char;
    match last {
        '.' | '?' | '!' => (
            raw[..raw.len() - 1].trim_end_matches(['.', '?', '!']),
            TrailingBoundary::Sentence,
        ),
        ':' | ';' => (&raw[..raw.len() - 1], TrailingBoundary::Clause),
        ',' => (&raw[..raw.len() - 1], TrailingBoundary::None),
        _ => (raw, TrailingBoundary::None),
    }
}

fn apply_sentence_boundary(
    sequences: &mut Vec<ProseSequence>,
    boundary: TrailingBoundary,
) {
    match boundary {
        TrailingBoundary::None => {}
        TrailingBoundary::Sentence => {
            sequences.push(ProseSequence {
                tokens: Vec::new(),
                started: true,
            });
        }
        TrailingBoundary::Clause => {
            sequences.push(ProseSequence {
                tokens: Vec::new(),
                started: false,
            });
        }
    }
}

/// Structural tokens are boundaries and are dropped.
fn is_structural_token(raw: &str) -> bool {
    if raw.is_empty() {
        return true;
    }
    let first = raw.chars().next().expect("non-empty");
    if matches!(first, '#' | '%' | '@' | '+' | '=' | '$' | '<' | '{') {
        return true;
    }
    if raw.contains('/')
        || raw.contains('\\')
        || raw.contains("://")
        || raw.contains('=')
        || raw.contains('|')
        || raw.contains("](")
        || raw.contains('`')
    {
        return true;
    }
    // Interior dot (not a simple trailing period, already split).
    if raw.contains('.') {
        return true;
    }
    // Placeholder-ish braces/brackets.
    if raw.contains('{') || raw.contains('}') {
        return true;
    }
    false
}

fn strip_affixes(raw: &str) -> &str {
    let mut s = raw;
    loop {
        let next =
            s.strip_prefix(['(', '"', '\'', '“', '‘', '*', '_', '`', '[']);
        match next {
            Some(rest) => s = rest,
            None => break,
        }
    }
    loop {
        let next =
            s.strip_suffix([')', ']', '"', '\'', '”', '’', '*', '_', ',', '`']);
        match next {
            Some(rest) => s = rest,
            None => break,
        }
    }
    s
}

fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric()
}

/// Classify a raw token core as a word, returning (key, surface).
pub fn classify_word(raw: &str) -> Option<(String, String)> {
    let cleaned = strip_affixes(raw);
    if cleaned.is_empty() || cleaned.len() > 32 {
        return None;
    }
    if !cleaned
        .chars()
        .all(|ch| is_word_char(ch) || matches!(ch, '\'' | '’' | '-' | '_'))
    {
        return None;
    }
    if !cleaned.chars().any(|ch| ch.is_alphabetic()) {
        return None;
    }
    if cleaned.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    if is_hash_like(cleaned) || is_secret_like(cleaned) {
        return None;
    }
    let key = cleaned.to_lowercase().replace('’', "'");
    Some((key, cleaned.to_string()))
}

fn is_hex_char(ch: char) -> bool {
    ch.is_ascii_hexdigit()
}

fn is_hash_like(word: &str) -> bool {
    let chars: Vec<char> = word.chars().collect();
    if chars.len() < 7 {
        return false;
    }
    chars
        .iter()
        .all(|c| is_hex_char(*c) || *c == '-' || *c == '_')
        && chars.iter().any(|c| c.is_ascii_digit())
}

const SECRET_PREFIXES: &[&str] =
    &["sk-", "ghp_", "gho_", "xox", "AKIA", "dph_"];

fn is_secret_like(word: &str) -> bool {
    for prefix in SECRET_PREFIXES {
        if word.starts_with(prefix) {
            return true;
        }
    }
    if word.len() >= 20
        && word.chars().any(|c| c.is_alphabetic())
        && word.chars().any(|c| c.is_ascii_digit())
    {
        return true;
    }
    false
}

/// Choose the display surface for a key from recency-weighted votes.
///
/// Prefers the most common casing among non-sequence-initial occurrences;
/// when every occurrence is sequence-initial, lowercases the first letter
/// unless the word is all caps (2+ letters) or has an interior capital.
pub fn canonical_surface(key: &str, votes: &[(String, f64, bool)]) -> String {
    let mut weighted: HashMap<&str, f64> = HashMap::new();
    let mut non_initial_mass = 0.0;
    for (surface, weight, initial) in votes {
        if !initial {
            non_initial_mass += weight;
            *weighted.entry(surface.as_str()).or_default() += weight;
        }
    }
    if non_initial_mass > 0.0 {
        let mut best: Option<(&str, f64)> = None;
        for (surface, mass) in weighted {
            if best.is_none_or(|(_, m)| mass > m) {
                best = Some((surface, mass));
            }
        }
        return best.expect("votes").0.to_string();
    }
    // All sequence-initial: use the heaviest surface with fallback rules.
    let mut best: Option<(&str, f64)> = None;
    for (surface, weight, _) in votes {
        let entry = best.get_or_insert((surface.as_str(), 0.0));
        if *weight > entry.1 {
            best = Some((surface.as_str(), *weight));
        }
    }
    let Some((surface, _)) = best else {
        return key.to_string();
    };
    fallback_surface(surface)
}

fn fallback_surface(surface: &str) -> String {
    let chars: Vec<char> = surface.chars().collect();
    if chars.len() >= 2 && chars.iter().all(|c| c.is_uppercase()) {
        return surface.to_string();
    }
    if chars.len() >= 2
        && chars[1..].iter().any(|c| c.is_uppercase())
        && chars[0].is_uppercase()
    {
        return surface.to_string();
    }
    let mut out = String::with_capacity(surface.len());
    for (i, ch) in chars.iter().enumerate() {
        if i == 0 {
            out.extend(ch.to_lowercase());
        } else {
            out.push(*ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(text: &str) -> Vec<Vec<String>> {
        tokenize_prompt_text(text)
            .iter()
            .map(|seq| seq.tokens.iter().map(|tok| tok.key.clone()).collect())
            .collect()
    }

    #[test]
    fn plain_sentence_tokenizes_to_one_sequence() {
        let seqs = tokenize_prompt_text("Can you help me implement it");
        assert_eq!(seqs.len(), 1);
        assert!(seqs[0].started);
        assert_eq!(
            keys("Can you help me implement it"),
            vec![vec![
                "can".to_string(),
                "you".to_string(),
                "help".to_string(),
                "me".to_string(),
                "implement".to_string(),
                "it".to_string()
            ]]
        );
    }

    #[test]
    fn sentence_punctuation_starts_new_sequence() {
        let seqs = tokenize_prompt_text("Fix it. Then review");
        assert_eq!(seqs.len(), 2);
        assert_eq!(seqs[0].tokens.len(), 2);
        assert!(seqs[1].started);
        assert_eq!(seqs[1].tokens[0].key, "then");
    }

    #[test]
    fn colon_starts_empty_context_sequence() {
        let seqs = tokenize_prompt_text("Note: fix the parser");
        assert!(seqs.len() >= 2);
        let last = seqs.last().expect("last");
        assert!(!last.started);
        assert_eq!(last.tokens[0].key, "fix");
    }

    #[test]
    fn structural_tokens_are_boundaries() {
        let seqs = tokenize_prompt_text("help me #gh:sase implement");
        let all: Vec<String> = seqs
            .iter()
            .flat_map(|s| s.tokens.iter().map(|t| t.key.clone()))
            .collect();
        assert!(!all.iter().any(|k| k.contains('#')));
        assert!(all.contains(&"implement".to_string()));
    }

    #[test]
    fn paths_urls_and_assignments_are_dropped() {
        for raw in [
            "src/sase/x.py",
            "https://example.com",
            "key=value",
            "a|b",
            "`code`",
            "file.md",
        ] {
            assert_eq!(keys(raw), vec![Vec::<String>::new()], "raw={raw}");
        }
    }

    #[test]
    fn secrets_hashes_and_digits_are_dropped() {
        assert!(classify_word("sk-abcdef123456").is_none());
        assert!(classify_word("ghp_abcdef123456").is_none());
        assert!(classify_word("AKIAIOSFODNN7EXAMPLE").is_none());
        assert!(classify_word("deadbeef123").is_none());
        assert!(classify_word("12345").is_none());
        assert!(classify_word("a1b2c3d4e5f6g7h8i9j0").is_none());
        assert!(classify_word("implement").is_some());
        assert!(classify_word("don't").is_some());
        assert!(classify_word("well-known").is_some());
    }

    #[test]
    fn fenced_code_is_excluded() {
        let text = "explain this\n```python\nprint('hi')\n```\nfix the bug";
        let seqs = tokenize_prompt_text(text);
        let all: Vec<String> = seqs
            .iter()
            .flat_map(|s| s.tokens.iter().map(|t| t.key.clone()))
            .collect();
        assert!(!all.contains(&"print".to_string()));
        assert!(all.contains(&"fix".to_string()));
    }

    #[test]
    fn inline_code_is_excluded() {
        let seqs = tokenize_prompt_text("use `stitch create` today");
        let all: Vec<String> = seqs
            .iter()
            .flat_map(|s| s.tokens.iter().map(|t| t.key.clone()))
            .collect();
        assert!(!all.contains(&"stitch".to_string()));
        assert!(all.contains(&"today".to_string()));
    }

    #[test]
    fn jinja_is_excluded() {
        let seqs = tokenize_prompt_text("render {{ name }} now");
        let all: Vec<String> = seqs
            .iter()
            .flat_map(|s| s.tokens.iter().map(|t| t.key.clone()))
            .collect();
        assert!(!all.contains(&"name".to_string()));
        assert!(all.contains(&"now".to_string()));
    }

    #[test]
    fn frontmatter_is_excluded() {
        let text = "---\ntitle: hello\n---\nfix the bug";
        let seqs = tokenize_prompt_text(text);
        let all: Vec<String> = seqs
            .iter()
            .flat_map(|s| s.tokens.iter().map(|t| t.key.clone()))
            .collect();
        assert!(!all.contains(&"title".to_string()));
        assert!(all.contains(&"fix".to_string()));
    }

    #[test]
    fn list_and_heading_markers_start_new_sequences() {
        let seqs = tokenize_prompt_text("fix parser\n- review tests");
        assert!(seqs.len() >= 2);
        assert!(seqs.last().expect("last").started);
    }

    #[test]
    fn cursor_blocked_in_unclosed_fence() {
        assert_eq!(
            tokenize_cursor_text("explain\n```python\nprint"),
            CursorContext::Blocked {
                reason: BLOCKED_UNCLOSED_FENCE
            }
        );
    }

    #[test]
    fn cursor_blocked_in_unclosed_jinja() {
        assert_eq!(
            tokenize_cursor_text("render {{ name"),
            CursorContext::Blocked {
                reason: BLOCKED_UNCLOSED_JINJA
            }
        );
    }

    #[test]
    fn cursor_blocked_with_no_words() {
        assert_eq!(
            tokenize_cursor_text(""),
            CursorContext::Blocked {
                reason: BLOCKED_NO_WORD_CONTEXT
            }
        );
        assert_eq!(
            tokenize_cursor_text("#gh:sase"),
            CursorContext::Blocked {
                reason: BLOCKED_STRUCTURAL_TAIL
            }
        );
    }

    #[test]
    fn cursor_ready_after_word() {
        match tokenize_cursor_text("Can you help me") {
            CursorContext::Ready {
                context,
                context_words,
                ..
            } => {
                assert_eq!(context[0], SEQUENCE_START);
                assert_eq!(context_words, vec!["Can", "you", "help", "me"]);
            }
            CursorContext::Blocked { reason } => {
                panic!("blocked: {reason}")
            }
        }
    }

    #[test]
    fn canonical_surface_prefers_non_initial_casing() {
        let surface = canonical_surface(
            "sase",
            &[
                ("Sase".to_string(), 1.0, true),
                ("sase".to_string(), 3.0, false),
            ],
        );
        assert_eq!(surface, "sase");
    }

    #[test]
    fn canonical_surface_falls_back_for_initial_only() {
        assert_eq!(
            canonical_surface("fix", &[("Fix".to_string(), 1.0, true)]),
            "fix"
        );
        assert_eq!(
            canonical_surface("api", &[("API".to_string(), 1.0, true)]),
            "API"
        );
    }
}
