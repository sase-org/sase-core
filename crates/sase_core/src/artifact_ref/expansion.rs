//! Closed placeholder formatter for artifact-reference expansion strings.
//!
//! The formatter never re-scans a substituted value, so a stored value that
//! happens to contain `{kind}` cannot smuggle in another placeholder. It also
//! never touches the filesystem, a process, or the environment: it is a pure
//! function from a format string and a value map to a rendered string.

use std::collections::BTreeMap;

use super::wire::ArtifactRefError;

pub const ARTIFACT_REF_EXPANSION_PLACEHOLDERS: &[&str] = &[
    "kind",
    "argument",
    "canonical_argument",
    "display_label",
    "project",
    "repository",
    "repo_relative_path",
    "checkout_path",
    "sidecar_role",
    "captured_revision",
    "captured_digest",
    "logical_path",
];

enum ExpansionToken<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

/// Validate `format` and return every placeholder it uses, first-seen order.
pub fn validate_artifact_ref_expansion_format(
    format: &str,
) -> Result<Vec<String>, ArtifactRefError> {
    let tokens = tokenize_expansion_format(format)?;
    let mut placeholders: Vec<String> = Vec::new();
    for token in tokens {
        if let ExpansionToken::Placeholder(name) = token {
            if !placeholders.iter().any(|seen| seen == name) {
                placeholders.push(name.to_string());
            }
        }
    }
    Ok(placeholders)
}

/// Render `format`, substituting each placeholder from `values` verbatim.
pub fn render_artifact_ref_expansion(
    format: &str,
    values: &BTreeMap<String, String>,
) -> Result<String, ArtifactRefError> {
    let tokens = tokenize_expansion_format(format)?;
    let mut rendered = String::with_capacity(format.len());
    for token in tokens {
        match token {
            ExpansionToken::Literal(literal) => rendered.push_str(literal),
            ExpansionToken::Placeholder(name) => {
                let Some(value) = values.get(name) else {
                    return Err(ArtifactRefError::validation(format!(
                        "missing value for artifact reference expansion placeholder '{name}'"
                    )));
                };
                // Inserted verbatim and never re-tokenized: a value
                // containing `{kind}` stays literal text.
                rendered.push_str(value);
            }
        }
    }
    Ok(rendered)
}

fn tokenize_expansion_format(
    format: &str,
) -> Result<Vec<ExpansionToken<'_>>, ArtifactRefError> {
    let bytes = format.as_bytes();
    let mut tokens = Vec::new();
    let mut literal_start = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'{' if bytes.get(index + 1) == Some(&b'{') => {
                push_literal(&mut tokens, format, literal_start, index);
                tokens.push(ExpansionToken::Literal("{"));
                index += 2;
                literal_start = index;
            }
            b'}' if bytes.get(index + 1) == Some(&b'}') => {
                push_literal(&mut tokens, format, literal_start, index);
                tokens.push(ExpansionToken::Literal("}"));
                index += 2;
                literal_start = index;
            }
            b'{' => {
                push_literal(&mut tokens, format, literal_start, index);
                let name_start = index + 1;
                let Some(relative_end) = format[name_start..].find('}') else {
                    return Err(ArtifactRefError::validation(format!(
                        "unbalanced '{{' in artifact reference expansion format at byte {index}"
                    )));
                };
                let name_end = name_start + relative_end;
                let name = &format[name_start..name_end];
                if !ARTIFACT_REF_EXPANSION_PLACEHOLDERS.contains(&name) {
                    return Err(ArtifactRefError::validation(format!(
                        "unknown artifact reference expansion placeholder '{name}'"
                    )));
                }
                tokens.push(ExpansionToken::Placeholder(name));
                index = name_end + 1;
                literal_start = index;
            }
            b'}' => {
                return Err(ArtifactRefError::validation(format!(
                    "unbalanced '}}' in artifact reference expansion format at byte {index}"
                )));
            }
            _ => index += 1,
        }
    }
    push_literal(&mut tokens, format, literal_start, bytes.len());
    Ok(tokens)
}

fn push_literal<'a>(
    tokens: &mut Vec<ExpansionToken<'a>>,
    format: &'a str,
    start: usize,
    end: usize,
) {
    if end > start {
        tokens.push(ExpansionToken::Literal(&format[start..end]));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn validate_returns_placeholders_in_first_seen_order_without_duplicates() {
        let placeholders = validate_artifact_ref_expansion_format(
            "{kind}:{argument} ({kind})",
        )
        .unwrap();
        assert_eq!(placeholders, ["kind", "argument"]);
    }

    #[test]
    fn double_braces_escape_to_literal_braces() {
        let rendered = render_artifact_ref_expansion(
            "{{{kind}}}",
            &values(&[("kind", "stitch")]),
        )
        .unwrap();
        assert_eq!(rendered, "{stitch}");
    }

    #[test]
    fn unknown_placeholder_and_unbalanced_braces_are_rejected() {
        assert!(validate_artifact_ref_expansion_format("{bogus}").is_err());
        assert!(validate_artifact_ref_expansion_format("{kind").is_err());
        assert!(validate_artifact_ref_expansion_format("kind}").is_err());
    }

    #[test]
    fn missing_value_is_a_render_error() {
        let error = render_artifact_ref_expansion("{kind}", &BTreeMap::new())
            .unwrap_err();
        assert!(error.message.contains("kind"), "{error}");
    }

    #[test]
    fn substituted_values_are_never_rescanned() {
        let rendered = render_artifact_ref_expansion(
            "{display_label}",
            &values(&[("display_label", "literal {kind} text")]),
        )
        .unwrap();
        assert_eq!(rendered, "literal {kind} text");
    }
}
