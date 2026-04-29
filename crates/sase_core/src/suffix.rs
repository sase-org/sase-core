//! Suffix prefix parsing — mirrors
//! `sase_100/src/sase/ace/changespec/suffix_utils.py` and the
//! `is_entry_ref_suffix` helper from
//! `sase_100/src/sase/ace/display_helpers.py`.

use regex::Regex;
use std::sync::OnceLock;

/// Output of parsing a suffix string into (value, suffix_type).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSuffix {
    pub value: Option<String>,
    pub suffix_type: Option<String>,
}

/// Prefix → suffix_type map, in priority order (longer prefixes first).
/// Mirrors `_PREFIX_MAP` in `suffix_utils.py`.
const PREFIX_MAP: &[(&str, Option<&str>)] = &[
    ("~!:", Some("rejected_proposal")),
    ("~@:", Some("killed_agent")),
    ("~$:", Some("killed_process")),
    ("?$:", Some("pending_dead_process")),
    ("!:", Some("error")),
    ("@:", Some("running_agent")),
    ("$:", Some("running_process")),
    ("%:", Some("summarize_complete")),
    ("^:", Some("metahook_complete")),
    // Legacy "~:" prefix is treated as a plain suffix (no suffix_type).
    ("~:", None),
];

/// Parse suffix prefix markers and return (value, suffix_type).
///
/// Equivalent to `parse_suffix_prefix` in `suffix_utils.py`. Special-cases
/// the standalone markers `@`, `%`, `^` (no colon) to their respective
/// suffix types with an empty value, and the `!: metahook[ | ...]` form
/// which promotes to `metahook_complete`.
pub fn parse_suffix_prefix(suffix_val: Option<&str>) -> ParsedSuffix {
    let Some(s) = suffix_val else {
        return ParsedSuffix {
            value: None,
            suffix_type: None,
        };
    };

    for (prefix, suffix_type) in PREFIX_MAP {
        let Some(remainder_raw) = s.strip_prefix(prefix) else {
            continue;
        };
        if *prefix == "!:" {
            // Python: remainder = suffix_val[len(prefix):].lstrip().
            let remainder = remainder_raw.trim_start_matches([' ', '\t']);
            if remainder == "metahook" {
                return ParsedSuffix {
                    value: Some(String::new()),
                    suffix_type: Some("metahook_complete".to_string()),
                };
            }
            if let Some(after) = remainder.strip_prefix("metahook |") {
                return ParsedSuffix {
                    value: Some(after.trim().to_string()),
                    suffix_type: Some("metahook_complete".to_string()),
                };
            }
        }
        return ParsedSuffix {
            value: Some(remainder_raw.trim().to_string()),
            suffix_type: suffix_type.map(|t| t.to_string()),
        };
    }

    // Standalone single-char markers without the colon.
    if s == "@" {
        return ParsedSuffix {
            value: Some(String::new()),
            suffix_type: Some("running_agent".to_string()),
        };
    }
    if s == "%" {
        return ParsedSuffix {
            value: Some(String::new()),
            suffix_type: Some("summarize_complete".to_string()),
        };
    }
    if s == "^" {
        return ParsedSuffix {
            value: Some(String::new()),
            suffix_type: Some("metahook_complete".to_string()),
        };
    }

    ParsedSuffix {
        value: Some(s.to_string()),
        suffix_type: None,
    }
}

/// Mirrors `is_entry_ref_suffix` in `display_helpers.py`: matches `\d+[a-z]?`.
pub fn is_entry_ref_suffix(suffix: Option<&str>) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^\d+[a-z]?$").unwrap());
    match suffix {
        Some(s) if !s.is_empty() => re.is_match(s),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(value: Option<&str>, kind: Option<&str>) -> ParsedSuffix {
        ParsedSuffix {
            value: value.map(|s| s.to_string()),
            suffix_type: kind.map(|s| s.to_string()),
        }
    }

    #[test]
    fn none_input_returns_none_pair() {
        assert_eq!(parse_suffix_prefix(None), parsed(None, None));
    }

    #[test]
    fn long_prefixes_take_priority_over_short() {
        // "~!:" must match before "!:" / "~:".
        assert_eq!(
            parse_suffix_prefix(Some("~!: rejected msg")),
            parsed(Some("rejected msg"), Some("rejected_proposal"))
        );
        // "~@:" before "@:".
        assert_eq!(
            parse_suffix_prefix(Some("~@: gone")),
            parsed(Some("gone"), Some("killed_agent"))
        );
        // "~$:" before "$:".
        assert_eq!(
            parse_suffix_prefix(Some("~$: gone")),
            parsed(Some("gone"), Some("killed_process"))
        );
        // "?$:" before "$:".
        assert_eq!(
            parse_suffix_prefix(Some("?$: 12345")),
            parsed(Some("12345"), Some("pending_dead_process"))
        );
    }

    #[test]
    fn legacy_tilde_colon_is_plain() {
        assert_eq!(
            parse_suffix_prefix(Some("~: legacy")),
            parsed(Some("legacy"), None)
        );
    }

    #[test]
    fn metahook_promotes_error_to_metahook_complete() {
        assert_eq!(
            parse_suffix_prefix(Some("!: metahook")),
            parsed(Some(""), Some("metahook_complete"))
        );
        assert_eq!(
            parse_suffix_prefix(Some("!: metahook | summary text")),
            parsed(Some("summary text"), Some("metahook_complete"))
        );
        // Anything else with the !: prefix stays an error.
        assert_eq!(
            parse_suffix_prefix(Some("!: other")),
            parsed(Some("other"), Some("error"))
        );
        // Trailing whitespace after `metahook` does NOT match the bare-metahook
        // branch (Python uses lstrip-only) so it falls through to the "error"
        // path that strips both sides.
        assert_eq!(
            parse_suffix_prefix(Some("!: metahook ")),
            parsed(Some("metahook"), Some("error"))
        );
    }

    #[test]
    fn standalone_markers_have_empty_value() {
        assert_eq!(
            parse_suffix_prefix(Some("@")),
            parsed(Some(""), Some("running_agent"))
        );
        assert_eq!(
            parse_suffix_prefix(Some("%")),
            parsed(Some(""), Some("summarize_complete"))
        );
        assert_eq!(
            parse_suffix_prefix(Some("^")),
            parsed(Some(""), Some("metahook_complete"))
        );
    }

    #[test]
    fn unknown_prefix_returns_value_unchanged() {
        assert_eq!(
            parse_suffix_prefix(Some("plain text")),
            parsed(Some("plain text"), None)
        );
        assert_eq!(parse_suffix_prefix(Some("")), parsed(Some(""), None));
    }

    #[test]
    fn entry_ref_recognizes_digit_optional_letter() {
        assert!(is_entry_ref_suffix(Some("1")));
        assert!(is_entry_ref_suffix(Some("12")));
        assert!(is_entry_ref_suffix(Some("1a")));
        assert!(is_entry_ref_suffix(Some("23z")));
        assert!(!is_entry_ref_suffix(Some("a")));
        assert!(!is_entry_ref_suffix(Some("1A")));
        assert!(!is_entry_ref_suffix(Some("1ab")));
        assert!(!is_entry_ref_suffix(Some("")));
        assert!(!is_entry_ref_suffix(None));
    }
}
