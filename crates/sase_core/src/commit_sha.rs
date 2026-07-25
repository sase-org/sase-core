//! Pure helpers for comparing abbreviated and full commit SHAs.

const MIN_ABBREVIATED_SHA_LEN: usize = 7;

/// Return whether two hexadecimal SHAs identify the same commit.
///
/// Git abbreviations are accepted only when both inputs contain at least
/// seven hexadecimal digits. Comparison is case-insensitive and symmetric:
/// the shorter input must be a prefix of the longer input.
pub fn commit_shas_equivalent(left: &str, right: &str) -> bool {
    if left.len() < MIN_ABBREVIATED_SHA_LEN
        || right.len() < MIN_ABBREVIATED_SHA_LEN
        || !left.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !right.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }

    let (shorter, longer) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    longer[..shorter.len()].eq_ignore_ascii_case(shorter)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_SHA: &str = "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e";

    #[test]
    fn equivalent_sha_matrix() {
        for (left, right) in [
            (FULL_SHA, FULL_SHA),
            ("d7e06b77b", FULL_SHA),
            (FULL_SHA, "d7e06b77b"),
            ("d7e06b7", FULL_SHA),
            ("D7E06B77B", FULL_SHA),
            ("d7e06b77b", "D7E06B77B42D89ECF4BB1538C6F89C6FE700124E"),
        ] {
            assert!(
                commit_shas_equivalent(left, right),
                "{left:?} should match {right:?}"
            );
        }
    }

    #[test]
    fn rejects_ambiguous_or_invalid_shas() {
        for (left, right) in [
            ("d7e06b", FULL_SHA),
            ("", FULL_SHA),
            ("d7e06bg", FULL_SHA),
            ("d7e06b7", "e7e06b77b42d89ecf4bb1538c6f89c6fe700124e"),
            ("d7e06b77c", "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e"),
        ] {
            assert!(
                !commit_shas_equivalent(left, right),
                "{left:?} should not match {right:?}"
            );
        }
    }
}
