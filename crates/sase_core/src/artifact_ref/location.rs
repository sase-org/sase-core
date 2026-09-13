//! One link-location grammar for colon and GitHub-style line suffixes.
//!
//! Colon form: `:(\d+)(?::(\d+))?(?:-(\d+))?$`
//! Fragment form: `#[Ll](\d+)(?:[Cc](\d+))?(?:-[Ll]?(\d+)(?:[Cc]\d+)?)?$`
//! (the end column is accepted and discarded).

use std::sync::OnceLock;

use regex::Regex;

use super::wire::{
    LinkLocationSplitWire, LinkLocationWire, LINK_LOCATION_WIRE_SCHEMA_VERSION,
};

/// Optional colon-or-fragment suffix used by the document file-path scanner.
pub(super) const FILE_PATH_LOCATION_SUFFIX: &str = concat!(
    r"(?:",
    r"(?::\d+(?::\d+)?(?:-\d+)?)",
    r"|",
    r"(?:#[Ll]\d+(?:[Cc]\d+)?(?:-[Ll]?\d+(?:[Cc]\d+)?)?)",
    r")?",
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GitHubLineLocation {
    pub line: u64,
    pub column: Option<u64>,
    pub end_line: Option<u64>,
}

/// Split a trailing line location off `target`.
///
/// Pure: no I/O. The only allocation is the returned wire.
pub fn split_link_location(target: &str) -> LinkLocationSplitWire {
    if let Some((base, parsed)) = split_fragment_suffix(target) {
        return finish_split(target, base, parsed, false);
    }
    if let Some((base, parsed)) = split_colon_suffix(target) {
        return finish_split(target, base, parsed, true);
    }
    whole(target)
}

fn finish_split(
    target: &str,
    base: &str,
    parsed: GitHubLineLocation,
    colon_form: bool,
) -> LinkLocationSplitWire {
    if colon_form && colon_base_is_not_a_path(base) {
        return whole(target);
    }
    match sanitize_location(parsed) {
        Some(location) => LinkLocationSplitWire {
            schema_version: LINK_LOCATION_WIRE_SCHEMA_VERSION,
            base: base.to_string(),
            location: Some(location),
        },
        None => whole(target),
    }
}

fn whole(target: &str) -> LinkLocationSplitWire {
    LinkLocationSplitWire {
        schema_version: LINK_LOCATION_WIRE_SCHEMA_VERSION,
        base: target.to_string(),
        location: None,
    }
}

fn split_fragment_suffix(target: &str) -> Option<(&str, GitHubLineLocation)> {
    let hash = target.rfind('#')?;
    let parsed = parse_github_line_fragment(&target[hash + 1..])?;
    Some((&target[..hash], parsed))
}

fn split_colon_suffix(target: &str) -> Option<(&str, GitHubLineLocation)> {
    static COLON_RE: OnceLock<Regex> = OnceLock::new();
    let regex = COLON_RE
        .get_or_init(|| Regex::new(r":(\d+)(?::(\d+))?(?:-(\d+))?$").unwrap());
    let caps = regex.captures(target)?;
    let full = caps.get(0)?;
    let line = parse_capture(caps.get(1))?;
    let column = match caps.get(2) {
        Some(group) => Some(parse_capture(Some(group))?),
        None => None,
    };
    let end_line = match caps.get(3) {
        Some(group) => Some(parse_capture(Some(group))?),
        None => None,
    };
    Some((
        &target[..full.start()],
        GitHubLineLocation {
            line,
            column,
            end_line,
        },
    ))
}

fn parse_capture(group: Option<regex::Match<'_>>) -> Option<u64> {
    group?.as_str().parse().ok()
}

fn colon_base_is_not_a_path(base: &str) -> bool {
    let has_path_marker = base.contains('/') || base.contains('.');
    !has_path_marker || ends_with_colon_digits(base)
}

fn ends_with_colon_digits(base: &str) -> bool {
    let digit_count = base.bytes().rev().take_while(u8::is_ascii_digit).count();
    digit_count > 0
        && base.len() > digit_count
        && base.as_bytes()[base.len() - digit_count - 1] == b':'
}

fn sanitize_location(parsed: GitHubLineLocation) -> Option<LinkLocationWire> {
    if parsed.line == 0 || parsed.column == Some(0) {
        return None;
    }
    Some(LinkLocationWire {
        line: parsed.line,
        column: parsed.column,
        end_line: parsed.end_line.filter(|&end| end > parsed.line),
    })
}

/// Parse `L12`, `L12-40`, `L12C5`, `L12C5-L40C2` (`L`/`C` case-insensitive).
///
/// Returns `None` when `fragment` is not this grammar. Zero is allowed so
/// callers can reject it with their own error (parse) or ignore it (split).
pub(super) fn parse_github_line_fragment(
    fragment: &str,
) -> Option<GitHubLineLocation> {
    let rest = strip_lc_prefix(fragment, b'L')?;
    let (line, rest) = take_digits(rest)?;
    let (column, rest) = match strip_lc_prefix(rest, b'C') {
        Some(after_c) => {
            let (column, rest) = take_digits(after_c)?;
            (Some(column), rest)
        }
        None => (None, rest),
    };
    let (end_line, rest) = match rest.strip_prefix('-') {
        Some(after_dash) => {
            let after_l =
                strip_lc_prefix(after_dash, b'L').unwrap_or(after_dash);
            let (end, rest) = take_digits(after_l)?;
            let rest = match strip_lc_prefix(rest, b'C') {
                Some(after_c) => take_digits(after_c)?.1,
                None => rest,
            };
            (Some(end), rest)
        }
        None => (None, rest),
    };
    if !rest.is_empty() {
        return None;
    }
    Some(GitHubLineLocation {
        line,
        column,
        end_line,
    })
}

fn strip_lc_prefix(input: &str, letter: u8) -> Option<&str> {
    let first = *input.as_bytes().first()?;
    if first != letter && first != letter.to_ascii_lowercase() {
        return None;
    }
    Some(&input[1..])
}

fn take_digits(input: &str) -> Option<(u64, &str)> {
    let digit_count = input.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    let (digits, rest) = input.split_at(digit_count);
    Some((digits.parse().ok()?, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(
        line: u64,
        column: Option<u64>,
        end_line: Option<u64>,
    ) -> Option<LinkLocationWire> {
        Some(LinkLocationWire {
            line,
            column,
            end_line,
        })
    }

    fn assert_split(
        target: &str,
        base: &str,
        location: Option<LinkLocationWire>,
    ) {
        let split = split_link_location(target);
        assert_eq!(split.schema_version, LINK_LOCATION_WIRE_SCHEMA_VERSION);
        assert_eq!(split.base, base, "base for {target}");
        assert_eq!(split.location, location, "location for {target}");
    }

    #[test]
    fn supported_forms_split_off_the_location() {
        for (target, base, location) in [
            ("src/app.py:12", "src/app.py", loc(12, None, None)),
            ("src/app.py:12:5", "src/app.py", loc(12, Some(5), None)),
            ("src/app.py:12-40", "src/app.py", loc(12, None, Some(40))),
            (
                "src/app.py:12:5-40",
                "src/app.py",
                loc(12, Some(5), Some(40)),
            ),
            ("src/app.py#L12", "src/app.py", loc(12, None, None)),
            ("src/app.py#L12-L40", "src/app.py", loc(12, None, Some(40))),
            ("src/app.py#L12-40", "src/app.py", loc(12, None, Some(40))),
            ("src/app.py#L12C5", "src/app.py", loc(12, Some(5), None)),
            (
                "src/app.py#L12C5-L40C2",
                "src/app.py",
                loc(12, Some(5), Some(40)),
            ),
            (
                "plan:202609/x.md:12",
                "plan:202609/x.md",
                loc(12, None, None),
            ),
            (
                "@plan:202609/x.md#L3",
                "@plan:202609/x.md",
                loc(3, None, None),
            ),
            (
                "plan:202609/x.md#L3",
                "plan:202609/x.md",
                loc(3, None, None),
            ),
            ("src/app.py#l12c5", "src/app.py", loc(12, Some(5), None)),
            (
                "src/app.py#L12C5-l40c2",
                "src/app.py",
                loc(12, Some(5), Some(40)),
            ),
        ] {
            assert_split(target, base, location);
        }
    }

    #[test]
    fn forms_that_are_not_a_location_stay_whole() {
        for target in [
            "src/app.py",
            "docs/guide.md#usage",
            "bead:sase-uk.1",
            "commit:abc1234",
            "src/app.py:0",
            "src/app.py#L0",
            "src/app.py:12:0",
            "src/app.py#L12C0",
            "plan:12",
            "bug:12345",
            "plan:foo:12",
            "a/b.py:1:2:3",
        ] {
            assert_split(target, target, None);
        }
    }

    #[test]
    fn inverted_or_equal_end_line_is_dropped() {
        assert_split("src/app.py:40-12", "src/app.py", loc(40, None, None));
        assert_split("src/app.py:12-12", "src/app.py", loc(12, None, None));
        assert_split("src/app.py#L40-L12", "src/app.py", loc(40, None, None));
        assert_split("src/app.py#L12-L12", "src/app.py", loc(12, None, None));
    }

    #[test]
    fn github_line_fragment_parser_covers_new_forms() {
        assert_eq!(
            parse_github_line_fragment("L12C5-L40C2"),
            Some(GitHubLineLocation {
                line: 12,
                column: Some(5),
                end_line: Some(40),
            })
        );
        assert_eq!(
            parse_github_line_fragment("L12-40"),
            Some(GitHubLineLocation {
                line: 12,
                column: None,
                end_line: Some(40),
            })
        );
        assert_eq!(
            parse_github_line_fragment("l12c5"),
            Some(GitHubLineLocation {
                line: 12,
                column: Some(5),
                end_line: None,
            })
        );
        assert_eq!(parse_github_line_fragment("page=2"), None);
        assert_eq!(parse_github_line_fragment("usage"), None);
    }

    #[test]
    fn omitted_optional_fields_are_skipped_in_json() {
        let split = split_link_location("src/app.py:12");
        let value = serde_json::to_value(&split).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["base"], "src/app.py");
        assert_eq!(value["location"]["line"], 12);
        assert!(value["location"].get("column").is_none());
        assert!(value["location"].get("end_line").is_none());

        let whole = split_link_location("src/app.py");
        let value = serde_json::to_value(&whole).unwrap();
        assert!(value.get("location").is_none());
    }
}
