//! Shared did-you-mean ranking for types, plugin ids, and enum values.

use std::collections::HashSet;

/// Rank case-insensitive equality and prefix matches first, then optimal
/// string alignment distance at most `max(1, len/3)`, and return at most
/// three unique suggestions in the candidate's original spelling.
pub fn suggest_closest(
    query: &str,
    candidates: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<String> {
    if query.is_empty() {
        return Vec::new();
    }
    let query_lower = query.to_lowercase();
    let query_len = query.chars().count();
    let max_distance = 1.max(query_len / 3);
    let mut ranked = Vec::new();
    let mut seen = HashSet::new();
    for candidate in candidates {
        let original = candidate.as_ref();
        if original.is_empty() || !seen.insert(original.to_string()) {
            continue;
        }
        let candidate_lower = original.to_lowercase();
        let rank = if candidate_lower == query_lower {
            Rank {
                class: 0,
                distance: 0,
            }
        } else if candidate_lower.starts_with(&query_lower) {
            Rank {
                class: 1,
                distance: candidate_lower.chars().count() - query_len,
            }
        } else {
            let distance = osa_distance(&query_lower, &candidate_lower);
            if distance > max_distance {
                continue;
            }
            Rank { class: 2, distance }
        };
        ranked.push((rank, original.to_string()));
    }
    ranked.sort_by(|(left_rank, left_name), (right_rank, right_name)| {
        left_rank
            .class
            .cmp(&right_rank.class)
            .then_with(|| left_rank.distance.cmp(&right_rank.distance))
            .then_with(|| left_name.cmp(right_name))
    });
    ranked.into_iter().take(3).map(|(_, name)| name).collect()
}

/// Format `; did you mean ...?` for one, two, or three suggestions.
pub fn did_you_mean_suffix(suggestions: &[String]) -> String {
    match suggestions {
        [] => String::new(),
        [one] => format!("; did you mean `{one}`?"),
        [one, two] => format!("; did you mean `{one}` or `{two}`?"),
        [one, two, three, ..] => {
            format!("; did you mean `{one}`, `{two}`, or `{three}`?")
        }
    }
}

#[derive(Clone, Copy)]
struct Rank {
    class: u8,
    distance: usize,
}

/// Optimal string alignment (Damerau–Levenshtein with adjacent
/// transpositions, each substring used at most once).
fn osa_distance(left: &str, right: &str) -> usize {
    let a: Vec<char> = left.chars().collect();
    let b: Vec<char> = right.chars().collect();
    let n = a.len();
    let m = b.len();
    if n == 0 {
        return m;
    }
    if m == 0 {
        return n;
    }
    let mut d = vec![vec![0usize; m + 1]; n + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=n {
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[n][m]
}
