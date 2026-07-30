use std::char::ToLowercase;
use std::cmp::Ordering;

const SCORE_MATCH: i32 = 16;
const BONUS_BOUNDARY: i32 = 14;
const BONUS_CONSECUTIVE: i32 = 10;
const BONUS_BASENAME: i32 = 10;
const MAX_LEADING_PENALTY: usize = 10;

/// The quality and character ranges of a successful fuzzy match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FuzzyMatch {
    /// Match class: whole-text prefix, basename prefix, substring, or
    /// subsequence.
    pub tier: u8,
    /// Higher scores indicate stronger boundary, consecutive, and basename
    /// matches.
    pub score: i32,
    /// Half-open character ranges into the matched text.
    pub runs: Vec<(u32, u32)>,
}

/// Query characters normalized once for matching against many indexed texts.
#[derive(Clone, Debug)]
pub(crate) struct FuzzyQuery {
    folded_chars: Vec<ToLowercase>,
}

impl FuzzyQuery {
    pub(crate) fn new(query: &str) -> Self {
        Self {
            folded_chars: query.chars().map(char::to_lowercase).collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.folded_chars.is_empty()
    }
}

/// Text metadata reused across repeated fuzzy matches.
#[derive(Clone, Debug)]
pub(crate) struct FuzzyText {
    chars: Vec<char>,
    folded_chars: Vec<ToLowercase>,
    lowercase: String,
    basename_start: usize,
}

impl FuzzyText {
    pub(crate) fn new(text: &str) -> Self {
        let chars = text.chars().collect::<Vec<_>>();
        let folded_chars =
            chars.iter().copied().map(char::to_lowercase).collect();
        let lowercase = text.to_lowercase();
        let basename_start = basename_start(&chars);
        Self {
            chars,
            folded_chars,
            lowercase,
            basename_start,
        }
    }
}

/// Match `query` against `text` case-insensitively.
///
/// The alignment is first found from left to right, tightened from right to
/// left, and retried inside the basename when that produces a better score.
pub fn fuzzy_match(query: &str, text: &str) -> Option<FuzzyMatch> {
    let query = FuzzyQuery::new(query);
    let text = FuzzyText::new(text);
    fuzzy_match_prepared(&query, &text)
}

/// Match a prepared query against text metadata built outside the hot path.
pub(crate) fn fuzzy_match_prepared(
    query: &FuzzyQuery,
    text: &FuzzyText,
) -> Option<FuzzyMatch> {
    if query.is_empty() {
        return Some(FuzzyMatch {
            tier: 0,
            score: 0,
            runs: Vec::new(),
        });
    }

    let mut positions = tight_alignment(
        &query.folded_chars,
        &text.folded_chars,
        0,
        text.chars.len(),
    )?;
    let mut score =
        score_positions(&text.chars, &positions, text.basename_start);

    if positions[0] < text.basename_start {
        if let Some(basename_positions) = tight_alignment(
            &query.folded_chars,
            &text.folded_chars,
            text.basename_start,
            text.chars.len(),
        ) {
            let basename_score = score_positions(
                &text.chars,
                &basename_positions,
                text.basename_start,
            );
            if basename_score > score {
                positions = basename_positions;
                score = basename_score;
            }
        }
    }

    let tier = if starts_with_case_insensitive(
        &text.folded_chars,
        &query.folded_chars,
    ) {
        0
    } else if starts_with_case_insensitive(
        &text.folded_chars[text.basename_start..],
        &query.folded_chars,
    ) {
        1
    } else if contains_case_insensitive(&text.folded_chars, &query.folded_chars)
    {
        2
    } else {
        3
    };

    Some(FuzzyMatch {
        tier,
        score,
        runs: positions_to_runs(&positions),
    })
}

/// Compare fuzzy matches by tier, descending score, text length, and text.
pub fn compare_fuzzy(
    left: (&FuzzyMatch, &str),
    right: (&FuzzyMatch, &str),
) -> Ordering {
    left.0
        .tier
        .cmp(&right.0.tier)
        .then_with(|| right.0.score.cmp(&left.0.score))
        .then_with(|| left.1.chars().count().cmp(&right.1.chars().count()))
        .then_with(|| left.1.to_lowercase().cmp(&right.1.to_lowercase()))
        .then_with(|| left.1.cmp(right.1))
}

/// Compare fuzzy matches using text metadata built outside the hot path.
pub(crate) fn compare_fuzzy_prepared(
    left: (&FuzzyMatch, &str, &FuzzyText),
    right: (&FuzzyMatch, &str, &FuzzyText),
) -> Ordering {
    left.0
        .tier
        .cmp(&right.0.tier)
        .then_with(|| right.0.score.cmp(&left.0.score))
        .then_with(|| left.2.chars.len().cmp(&right.2.chars.len()))
        .then_with(|| left.2.lowercase.cmp(&right.2.lowercase))
        .then_with(|| left.1.cmp(right.1))
}

pub(crate) fn compare_prepared_text(
    left: (&str, &FuzzyText),
    right: (&str, &FuzzyText),
) -> Ordering {
    left.1
        .lowercase
        .cmp(&right.1.lowercase)
        .then_with(|| left.0.cmp(right.0))
}

fn tight_alignment(
    query: &[ToLowercase],
    text: &[ToLowercase],
    start: usize,
    end: usize,
) -> Option<Vec<usize>> {
    let mut query_index = 0;
    let mut earliest_end = None;
    for (text_index, text_char) in text.iter().enumerate().take(end).skip(start)
    {
        if folded_chars_equal(text_char, &query[query_index]) {
            query_index += 1;
            if query_index == query.len() {
                earliest_end = Some(text_index);
                break;
            }
        }
    }

    let earliest_end = earliest_end?;
    let mut positions = vec![0; query.len()];
    let mut query_index = query.len();
    for text_index in (start..=earliest_end).rev() {
        if folded_chars_equal(&text[text_index], &query[query_index - 1]) {
            query_index -= 1;
            positions[query_index] = text_index;
            if query_index == 0 {
                break;
            }
        }
    }
    Some(positions)
}

fn score_positions(
    text: &[char],
    positions: &[usize],
    basename_start: usize,
) -> i32 {
    let mut score = 0;
    let mut previous = None;
    for &position in positions {
        let mut character_score = SCORE_MATCH;
        if is_boundary(text, position) {
            character_score += BONUS_BOUNDARY;
        }
        if previous.is_some_and(|previous| position == previous + 1) {
            character_score += BONUS_CONSECUTIVE;
        }
        if position >= basename_start {
            character_score += BONUS_BASENAME;
        }
        score += character_score;
        previous = Some(position);
    }
    score - positions[0].min(MAX_LEADING_PENALTY) as i32
}

fn is_boundary(text: &[char], position: usize) -> bool {
    if position == 0 {
        return true;
    }
    let previous = text[position - 1];
    if matches!(previous, '/' | '_' | '-' | '.' | ' ') {
        return true;
    }
    let current = text[position];
    (previous.is_lowercase() || previous.is_numeric()) && current.is_uppercase()
}

fn basename_start(text: &[char]) -> usize {
    let core_end = if text.last() == Some(&'/') {
        text.len().saturating_sub(1)
    } else {
        text.len()
    };
    text[..core_end]
        .iter()
        .rposition(|character| *character == '/')
        .map_or(0, |position| position + 1)
}

fn starts_with_case_insensitive(
    text: &[ToLowercase],
    query: &[ToLowercase],
) -> bool {
    text.len() >= query.len()
        && text
            .iter()
            .zip(query)
            .all(|(left, right)| folded_chars_equal(left, right))
}

fn contains_case_insensitive(
    text: &[ToLowercase],
    query: &[ToLowercase],
) -> bool {
    let mut prefix_lengths = vec![0; query.len()];
    let mut matched = 0;
    for index in 1..query.len() {
        while matched > 0 && !folded_chars_equal(&query[index], &query[matched])
        {
            matched = prefix_lengths[matched - 1];
        }
        if folded_chars_equal(&query[index], &query[matched]) {
            matched += 1;
        }
        prefix_lengths[index] = matched;
    }

    matched = 0;
    for character in text {
        while matched > 0 && !folded_chars_equal(character, &query[matched]) {
            matched = prefix_lengths[matched - 1];
        }
        if folded_chars_equal(character, &query[matched]) {
            matched += 1;
            if matched == query.len() {
                return true;
            }
        }
    }
    false
}

fn folded_chars_equal(left: &ToLowercase, right: &ToLowercase) -> bool {
    left.clone().eq(right.clone())
}

fn positions_to_runs(positions: &[usize]) -> Vec<(u32, u32)> {
    let mut runs = Vec::new();
    let mut start = positions[0];
    let mut end = start + 1;
    for &position in &positions[1..] {
        if position == end {
            end += 1;
        } else {
            runs.push((start as u32, end as u32));
            start = position;
            end = position + 1;
        }
    }
    runs.push((start as u32, end as u32));
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_all_match_tiers() {
        assert_eq!(fuzzy_match("src", "src/editor/fuzzy.rs").unwrap().tier, 0);
        assert_eq!(
            fuzzy_match("fuzzy", "src/editor/fuzzy.rs").unwrap().tier,
            1
        );
        assert_eq!(
            fuzzy_match("editor", "src/editor/fuzzy.rs").unwrap().tier,
            2
        );
        assert_eq!(fuzzy_match("sfrs", "src/editor/fuzzy.rs").unwrap().tier, 3);
    }

    #[test]
    fn tightens_site_to_the_contiguous_segment() {
        let text =
            "202607/sase_sites_hub_and_pages/sase_sites_hub_and_pages.md";
        let match_result = fuzzy_match("site", text).unwrap();

        assert_eq!(match_result.tier, 2);
        assert_eq!(slice_runs(text, &match_result.runs), vec!["site"]);
    }

    #[test]
    fn basename_retry_beats_a_cross_directory_alignment() {
        let text = "s/i/te/site.txt";
        let match_result = fuzzy_match("site", text).unwrap();

        assert_eq!(match_result.runs, vec![(7, 11)]);
        assert_eq!(slice_runs(text, &match_result.runs), vec!["site"]);
    }

    #[test]
    fn rewards_separator_and_camel_case_boundaries() {
        assert!(
            fuzzy_match("ab", "a_b").unwrap().score
                > fuzzy_match("ab", "axb").unwrap().score
        );
        assert!(
            fuzzy_match("ab", "aB").unwrap().score
                > fuzzy_match("ab", "ab").unwrap().score
        );
    }

    #[test]
    fn reports_character_ranges_for_non_ascii_text() {
        let text = "café/東京Résumé.md";
        let match_result = fuzzy_match("rés", text).unwrap();

        assert_eq!(match_result.runs, vec![(7, 10)]);
        assert_eq!(slice_runs(text, &match_result.runs), vec!["Rés"]);
    }

    #[test]
    fn empty_query_matches_and_missing_subsequence_does_not() {
        assert_eq!(
            fuzzy_match("", "anything"),
            Some(FuzzyMatch {
                tier: 0,
                score: 0,
                runs: Vec::new(),
            })
        );
        assert_eq!(fuzzy_match("xyz", "anything"), None);
    }

    #[test]
    fn comparator_is_deterministic_under_shuffled_input() {
        let texts = ["Beta", "alpha", "ALPHA", "alphabet", "zalpha"];
        let matches = texts
            .iter()
            .map(|text| fuzzy_match("a", text).unwrap())
            .collect::<Vec<_>>();
        let mut forward = (0..texts.len()).collect::<Vec<_>>();
        let mut reversed = forward.iter().rev().copied().collect::<Vec<_>>();

        let sort = |indexes: &mut Vec<usize>| {
            indexes.sort_by(|&left, &right| {
                compare_fuzzy(
                    (&matches[left], texts[left]),
                    (&matches[right], texts[right]),
                )
            });
        };
        sort(&mut forward);
        sort(&mut reversed);

        assert_eq!(forward, reversed);
        assert_eq!(
            forward
                .iter()
                .map(|&index| texts[index])
                .collect::<Vec<_>>(),
            vec!["ALPHA", "alpha", "alphabet", "zalpha", "Beta"]
        );
    }

    fn slice_runs<'a>(text: &'a str, runs: &[(u32, u32)]) -> Vec<&'a str> {
        let byte_offsets = text
            .char_indices()
            .map(|(offset, _)| offset)
            .chain(std::iter::once(text.len()))
            .collect::<Vec<_>>();
        runs.iter()
            .map(|&(start, end)| {
                &text[byte_offsets[start as usize]..byte_offsets[end as usize]]
            })
            .collect()
    }
}
