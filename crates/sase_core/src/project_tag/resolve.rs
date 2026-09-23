//! Project tag resolution: exact, then casefold, with suggestions.

use super::scan::is_tag_name;
use super::wire::{ProjectTagResolutionWire, ProjectTagTargetWire};

/// Resolve `name` against the tag targets.
///
/// Each target is reachable by its directory key, its display name, or any
/// alias. An exact match wins first; otherwise a case-insensitive
/// (casefold) match wins. More than one matching target is ambiguous — the
/// backend never guesses. No match is unknown, with up to 3 known-tag
/// spellings ranked by edit distance.
pub fn resolve_project_tag(
    name: &str,
    targets: &[ProjectTagTargetWire],
) -> ProjectTagResolutionWire {
    let exact = matching_targets(name, targets, false);
    if exact.len() == 1 {
        return ProjectTagResolutionWire::Resolved {
            target_index: exact[0],
        };
    }
    if exact.len() > 1 {
        return ProjectTagResolutionWire::Ambiguous { candidates: exact };
    }
    let folded = matching_targets(name, targets, true);
    if folded.len() == 1 {
        return ProjectTagResolutionWire::Resolved {
            target_index: folded[0],
        };
    }
    if folded.len() > 1 {
        return ProjectTagResolutionWire::Ambiguous { candidates: folded };
    }
    ProjectTagResolutionWire::Unknown {
        suggestions: suggest_project_tags(name, targets),
    }
}

/// Indices of targets reachable by `name`, exactly or case-insensitively.
fn matching_targets(
    name: &str,
    targets: &[ProjectTagTargetWire],
    fold_case: bool,
) -> Vec<usize> {
    let needle = if fold_case {
        name.to_lowercase()
    } else {
        name.to_string()
    };
    targets
        .iter()
        .enumerate()
        .filter(|(_, target)| {
            std::iter::once(target.key.as_str())
                .chain(std::iter::once(target.name.as_str()))
                .chain(target.aliases.iter().map(String::as_str))
                .any(|candidate| {
                    if fold_case {
                        candidate.to_lowercase() == needle
                    } else {
                        candidate == needle
                    }
                })
        })
        .map(|(index, _)| index)
        .collect()
}

/// Up to 3 known-tag spellings ranked by edit distance against `name`.
///
/// The pool is every target's name, key, and aliases, spelled as `+<name>`
/// when the spelling fits the tag grammar. There is no distance cutoff: the
/// launch policy (anchored-strict vs. forgiving) belongs to the callers.
fn suggest_project_tags(
    name: &str,
    targets: &[ProjectTagTargetWire],
) -> Vec<String> {
    let query = name.to_lowercase();
    // (distance, spelling rank, display). Display names sort before keys
    // before aliases on ties, so suggestions name projects canonically.
    let mut ranked: Vec<(usize, u8, String)> = Vec::new();
    for target in targets {
        for (rank, spelling) in std::iter::once((0u8, &target.name))
            .chain(std::iter::once((1u8, &target.key)))
            .chain(target.aliases.iter().map(|alias| (2u8, alias)))
        {
            let display = if is_tag_name(spelling) {
                format!("+{spelling}")
            } else {
                spelling.clone()
            };
            let distance = levenshtein(&query, &spelling.to_lowercase());
            ranked.push((distance, rank, display));
        }
    }
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    // Same spelling can rank from several targets (name vs. key) at
    // different distances, so equal displays need not sort adjacently.
    // Dedupe fully while keeping the ranked order.
    let mut seen = std::collections::HashSet::new();
    ranked
        .into_iter()
        .filter(|(_, _, display)| seen.insert(display.clone()))
        .take(3)
        .map(|(_, _, display)| display)
        .collect()
}

/// Edit distance over Unicode scalar values.
fn levenshtein(left: &str, right: &str) -> usize {
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    if left.is_empty() {
        return right.len();
    }
    if right.is_empty() {
        return left.len();
    }
    let mut prev: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0; right.len() + 1];
    for (row, left_ch) in left.iter().enumerate() {
        current[0] = row + 1;
        for (col, right_ch) in right.iter().enumerate() {
            let substitution = prev[col] + usize::from(left_ch != right_ch);
            current[col + 1] =
                (prev[col + 1] + 1).min(current[col] + 1).min(substitution);
        }
        std::mem::swap(&mut prev, &mut current);
    }
    prev[right.len()]
}
