//! Port of PyYAML's implicit resolvers for unquoted plain scalars.

use std::sync::OnceLock;

use regex::Regex;

/// Kind PyYAML would assign to a non-string plain scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyyamlScalarKind {
    Boolean,
    Int,
    Float,
    Null,
    Timestamp,
}

impl PyyamlScalarKind {
    pub fn as_article_phrase(self) -> &'static str {
        match self {
            Self::Boolean => "a boolean",
            Self::Int => "an int",
            Self::Float => "a float",
            Self::Null => "null",
            Self::Timestamp => "a timestamp",
        }
    }
}

/// Whether PyYAML's implicit resolver would type `text` as bool, int,
/// float, null, or timestamp rather than a string.
///
/// Patterns are copied from PyYAML `resolver.py` (`yaml_implicit_resolvers`),
/// including the VERBOSE (`re.X`) flag. The caller invokes this only for an
/// unquoted plain scalar.
pub fn pyyaml_plain_scalar_is_non_string(text: &str) -> bool {
    pyyaml_plain_scalar_kind(text).is_some()
}

/// Verbatim quote error for an unquoted plain scalar, when PyYAML would
/// type it as a non-string.
pub fn unquoted_plain_scalar_choice_error(text: &str) -> Option<String> {
    let kind = pyyaml_plain_scalar_kind(text)?;
    Some(format!(
        "choice `{text}` must be quoted (\"{text}\"): YAML reads it as {}",
        kind.as_article_phrase()
    ))
}

pub(crate) fn pyyaml_plain_scalar_kind(text: &str) -> Option<PyyamlScalarKind> {
    // First-character dispatch matches PyYAML's yaml_implicit_resolvers
    // tables; patterns still anchor the whole value.
    if bool_re().is_match(text) {
        return Some(PyyamlScalarKind::Boolean);
    }
    if int_re().is_match(text) {
        return Some(PyyamlScalarKind::Int);
    }
    if float_re().is_match(text) {
        return Some(PyyamlScalarKind::Float);
    }
    if null_re().is_match(text) {
        return Some(PyyamlScalarKind::Null);
    }
    if timestamp_re().is_match(text) {
        return Some(PyyamlScalarKind::Timestamp);
    }
    None
}

fn bool_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?x)^(yes|Yes|YES|no|No|NO
                    |true|True|TRUE|false|False|FALSE
                    |on|On|ON|off|Off|OFF)$",
        )
        .expect("pyyaml bool regex")
    })
}

fn int_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?x)^([-+]?0b[0-1_]+
                    |[-+]?0[0-7_]+
                    |[-+]?(?:0|[1-9][0-9_]*)
                    |[-+]?0x[0-9a-fA-F_]+
                    |[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$",
        )
        .expect("pyyaml int regex")
    })
}

fn float_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?x)^([-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?
                    |\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?
                    |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*
                    |[-+]?\.(?:inf|Inf|INF)
                    |\.(?:nan|NaN|NAN))$",
        )
        .expect("pyyaml float regex")
    })
}

fn null_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // Copied from PyYAML with re.X: whitespace in the pattern is
        // ignored, so the empty alternative matches the empty string.
        Regex::new(r"(?x)^(?: ~ |null|Null|NULL| )$")
            .expect("pyyaml null regex")
    })
}

fn timestamp_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?x)^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]
                    |[0-9][0-9][0-9][0-9] -[0-9][0-9]? -[0-9][0-9]?
                     (?:[Tt]|[ \t]+)[0-9][0-9]?
                     :[0-9][0-9] :[0-9][0-9] (?:\.[0-9]*)?
                     (?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$",
        )
        .expect("pyyaml timestamp regex")
    })
}
