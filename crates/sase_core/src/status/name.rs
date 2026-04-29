//! ChangeSpec name helpers needed by the status planner.
//!
//! Mirrors the subset of `sase_100/src/sase/core/changespec.py` that the
//! status state machine consumes:
//!
//! - [`has_suffix`] — `^.+(?:__|_)\d+$` predicate.
//! - [`get_next_suffix_number`] — lowest free `_<N>` slot, also reserving
//!   the legacy `__<N>` form so the two namespaces never collide.
//!
//! `strip_reverted_suffix` already lives in [`crate::query::matchers`] and
//! is re-exported through the crate root, so the planner uses that
//! implementation directly.

use std::collections::HashSet;
use std::sync::OnceLock;

use regex::Regex;

/// Regex `^.+__\d+$` — legacy double-underscore suffix.
fn double_underscore_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^.+__\d+$").unwrap())
}

/// Regex `^.+_\d+$` — current single-underscore suffix.
fn single_underscore_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^.+_\d+$").unwrap())
}

/// Whether *name* carries a `_<N>` (or legacy `__<N>`) reverted suffix.
/// Mirrors `has_suffix` in `changespec.py`.
pub fn has_suffix(name: &str) -> bool {
    double_underscore_re().is_match(name)
        || single_underscore_re().is_match(name)
}

/// Find the lowest positive integer N such that neither `<base>_<N>` nor
/// `<base>__<N>` appears in *existing*.
///
/// Mirrors `get_next_suffix_number` in `changespec.py`. The legacy
/// double-underscore namespace is reserved alongside the current single-
/// underscore one so the two cannot collide on the same N.
pub fn get_next_suffix_number(
    base_name: &str,
    existing: &HashSet<String>,
) -> u32 {
    let mut n: u32 = 1;
    loop {
        let single = format!("{base_name}_{n}");
        let double = format!("{base_name}__{n}");
        if !existing.contains(&single) && !existing.contains(&double) {
            return n;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn has_suffix_recognises_single_and_double_underscore() {
        assert!(has_suffix("proj_feature_1"));
        assert!(has_suffix("proj_feature__7"));
        assert!(!has_suffix("proj_feature"));
        assert!(!has_suffix("proj_feature_"));
        assert!(!has_suffix("proj_feature_x"));
    }

    #[test]
    fn next_suffix_starts_at_one() {
        assert_eq!(get_next_suffix_number("proj_x", &names(&[])), 1);
    }

    #[test]
    fn next_suffix_skips_existing() {
        let existing = names(&["proj_x", "proj_x_1", "proj_x_2"]);
        assert_eq!(get_next_suffix_number("proj_x", &existing), 3);
    }

    #[test]
    fn next_suffix_reserves_legacy_double_underscore_slot() {
        // `proj_x__3` (legacy) must reserve slot 3 so the new
        // `proj_x_3` we'd otherwise pick is skipped.
        let existing = names(&["proj_x", "proj_x_1", "proj_x_2", "proj_x__3"]);
        assert_eq!(get_next_suffix_number("proj_x", &existing), 4);
    }
}
