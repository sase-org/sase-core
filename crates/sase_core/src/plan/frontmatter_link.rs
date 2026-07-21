//! Shared contract for prompt/plan links stored in SDD YAML frontmatter.
//!
//! New writers store one inline Markdown link so GitHub renders the reference
//! as clickable. Historical frontmatter stored a plain path, which remains a
//! supported read format.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::wire::PlanError;

/// Parsed shape of one SDD prompt/plan frontmatter link value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SddFrontmatterLinkWire {
    /// Canonical `[label](target)` representation.
    Canonical { label: String, target: String },
    /// Historical plain-path representation.
    Legacy { path: String },
    /// Empty or Markdown-like input that cannot be interpreted safely.
    Invalid { reason: String },
}

/// Render one canonical inline Markdown link.
pub fn render_sdd_frontmatter_link(
    label: &str,
    target: &str,
) -> Result<String, PlanError> {
    validate_label(label).map_err(PlanError::validation)?;
    validate_target(target).map_err(PlanError::validation)?;
    Ok(format!("[{label}]({target})"))
}

/// Parse a canonical Markdown link or classify a historical plain path.
pub fn parse_sdd_frontmatter_link(value: &str) -> SddFrontmatterLinkWire {
    if value.trim().is_empty() {
        return invalid("frontmatter link value must not be empty");
    }

    if let Some(rest) = value.strip_prefix('[') {
        let Some(separator) = rest.find("](") else {
            return invalid("malformed Markdown frontmatter link");
        };
        let label = &rest[..separator];
        let target_with_close = &rest[separator + 2..];
        let Some(target) = target_with_close.strip_suffix(')') else {
            return invalid("malformed Markdown frontmatter link");
        };
        if let Err(reason) = validate_label(label) {
            return invalid(reason);
        }
        if let Err(reason) = validate_target(target) {
            return invalid(reason);
        }
        return SddFrontmatterLinkWire::Canonical {
            label: label.to_string(),
            target: target.to_string(),
        };
    }

    if value.contains("](") {
        return invalid("ambiguous Markdown-like frontmatter link");
    }

    SddFrontmatterLinkWire::Legacy {
        path: value.to_string(),
    }
}

/// Project a link value to the stable reference expected by existing models.
pub fn sdd_frontmatter_link_reference(value: &str) -> String {
    match parse_sdd_frontmatter_link(value) {
        SddFrontmatterLinkWire::Canonical { label, .. } => label,
        SddFrontmatterLinkWire::Legacy { path } => path,
        SddFrontmatterLinkWire::Invalid { .. } => value.to_string(),
    }
}

fn invalid(reason: &str) -> SddFrontmatterLinkWire {
    SddFrontmatterLinkWire::Invalid {
        reason: reason.to_string(),
    }
}

fn validate_label(label: &str) -> Result<(), &'static str> {
    if label.is_empty() {
        return Err("frontmatter link label must not be empty");
    }
    if label.trim() != label {
        return Err(
            "frontmatter link label must not have surrounding whitespace",
        );
    }
    if label.contains(['\n', '\r', '[', ']']) {
        return Err(
            "frontmatter link label contains unsupported Markdown syntax",
        );
    }
    Ok(())
}

fn validate_target(target: &str) -> Result<(), &'static str> {
    if target.is_empty() {
        return Err("frontmatter link target must not be empty");
    }
    if target.trim() != target {
        return Err(
            "frontmatter link target must not have surrounding whitespace",
        );
    }
    if target.contains(['\n', '\r', '\\', '(', ')']) {
        return Err("frontmatter link target must be a plain POSIX path");
    }
    if Path::new(target).is_absolute()
        || target.starts_with("//")
        || target.contains("://")
        || looks_like_windows_absolute_path(target)
    {
        return Err("frontmatter link target must be relative");
    }
    Ok(())
}

fn looks_like_windows_absolute_path(target: &str) -> bool {
    let bytes = target.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && bytes[2] == b'/'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_and_parses_canonical_relative_link() {
        let rendered = render_sdd_frontmatter_link(
            "../202607/example.md",
            "../example.md",
        )
        .unwrap();
        assert_eq!(rendered, "[../202607/example.md](../example.md)");
        assert_eq!(
            parse_sdd_frontmatter_link(&rendered),
            SddFrontmatterLinkWire::Canonical {
                label: "../202607/example.md".to_string(),
                target: "../example.md".to_string(),
            }
        );
    }

    #[test]
    fn classifies_legacy_plain_path() {
        assert_eq!(
            parse_sdd_frontmatter_link("202607/prompts/example.md"),
            SddFrontmatterLinkWire::Legacy {
                path: "202607/prompts/example.md".to_string(),
            }
        );
        assert_eq!(
            sdd_frontmatter_link_reference("202607/prompts/example.md"),
            "202607/prompts/example.md"
        );
    }

    #[test]
    fn rejects_empty_absolute_and_unsafe_render_inputs() {
        for (label, target) in [
            ("", "prompt.md"),
            ("prompt", "/prompt.md"),
            ("prompt", "https://example.test/prompt.md"),
            ("prompt", "dir\\prompt.md"),
            ("[prompt]", "prompt.md"),
        ] {
            assert!(render_sdd_frontmatter_link(label, target).is_err());
        }
    }

    #[test]
    fn malformed_markdown_like_values_are_invalid() {
        for value in [
            "",
            "[label]",
            "[label](target.md",
            "[label](/absolute.md)",
            "prefix [label](target.md)",
            "[label](target.md) suffix",
        ] {
            assert!(matches!(
                parse_sdd_frontmatter_link(value),
                SddFrontmatterLinkWire::Invalid { .. }
            ));
        }
    }

    #[test]
    fn canonical_reference_projects_label() {
        assert_eq!(
            sdd_frontmatter_link_reference(
                "[202607/prompts/example.md](prompts/example.md)"
            ),
            "202607/prompts/example.md"
        );
    }
}
