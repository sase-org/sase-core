//! Shared document-level contract for links between SDD artifacts.
//!
//! Canonical links are ordinary Markdown bullets at the top of the document
//! body. Historical `plan` and `prompt` YAML properties remain readable, but
//! new writes use the bullet representation.

use std::path::Path;
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use super::wire::PlanError;

/// The counterpart named by an SDD artifact link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SddArtifactLinkTypeWire {
    Plan,
    Prompt,
}

impl SddArtifactLinkTypeWire {
    pub(crate) fn parse(value: &str) -> Result<Self, PlanError> {
        match value.to_ascii_uppercase().as_str() {
            "PLAN" => Ok(Self::Plan),
            "PROMPT" => Ok(Self::Prompt),
            _ => Err(PlanError::validation(format!(
                "unsupported SDD artifact link type `{value}`; expected `PLAN` or `PROMPT`"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "PLAN",
            Self::Prompt => "PROMPT",
        }
    }

    fn legacy_field(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Prompt => "prompt",
        }
    }
}

/// Representation found in one SDD Markdown document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SddArtifactLinkKindWire {
    Canonical,
    Legacy,
    Mixed,
    Missing,
    Invalid,
}

/// Historical YAML-property representation retained during migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddLegacyArtifactLinkWire {
    pub link_type: SddArtifactLinkTypeWire,
    /// `path` for the original plain path or `markdown` for the later inline
    /// Markdown value.
    pub format: String,
    /// Stable visible path projected through existing list/search models.
    pub reference: String,
    /// Markdown href when `format == "markdown"`; otherwise the plain path.
    pub target: String,
}

/// Parsed document-level artifact-link state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddArtifactDocumentWire {
    pub kind: SddArtifactLinkKindWire,
    pub link_type: Option<SddArtifactLinkTypeWire>,
    pub label: Option<String>,
    pub target: Option<String>,
    pub legacy: Option<SddLegacyArtifactLinkWire>,
    /// Markdown body with a structurally valid artifact-link bullet and its
    /// separator removed.
    pub body: String,
    pub has_frontmatter: bool,
    /// Whether a detected canonical bullet already has the exact top-of-body
    /// placement and one-blank-line layout.
    pub canonical_layout: bool,
    pub reason: Option<String>,
}

impl SddArtifactDocumentWire {
    pub(crate) fn reference_for(
        &self,
        expected: SddArtifactLinkTypeWire,
    ) -> Option<String> {
        if self.link_type != Some(expected) {
            return None;
        }
        match self.kind {
            SddArtifactLinkKindWire::Canonical => self.label.clone(),
            SddArtifactLinkKindWire::Mixed => self
                .mixed_representations_agree()
                .then(|| self.label.clone())
                .flatten(),
            SddArtifactLinkKindWire::Legacy => {
                self.legacy.as_ref().map(|link| link.reference.clone())
            }
            SddArtifactLinkKindWire::Missing
            | SddArtifactLinkKindWire::Invalid => None,
        }
    }

    fn mixed_representations_agree(&self) -> bool {
        let (Some(label), Some(target), Some(legacy)) =
            (&self.label, &self.target, &self.legacy)
        else {
            return false;
        };
        legacy.reference == *label || legacy.target == *target
    }
}

struct DocumentParts<'a> {
    prefix: &'a str,
    frontmatter: Option<&'a str>,
    body: &'a str,
    has_frontmatter: bool,
}

struct CanonicalCandidate {
    link_type: SddArtifactLinkTypeWire,
    label: String,
    target: String,
    start: usize,
    end: usize,
    layout: bool,
}

/// Render the exact canonical artifact-link bullet.
pub fn render_sdd_artifact_link(
    link_type: &str,
    label: &str,
    target: &str,
) -> Result<String, PlanError> {
    let link_type = SddArtifactLinkTypeWire::parse(link_type)?;
    validate_label(label).map_err(PlanError::validation)?;
    validate_target(target).map_err(PlanError::validation)?;
    Ok(format!("- **{}:** [{label}]({target})", link_type.as_str()))
}

/// Parse the canonical bullet and compatible historical YAML properties from
/// one complete SDD Markdown document.
pub fn parse_sdd_artifact_link(document: &str) -> SddArtifactDocumentWire {
    let parts = match split_document(document) {
        Ok(parts) => parts,
        Err(reason) => return invalid_document(document, false, reason),
    };

    let candidates = artifact_candidates(parts.body, parts.has_frontmatter);
    if candidates.len() > 1 {
        return invalid_document(
            parts.body,
            parts.has_frontmatter,
            "multiple SDD artifact-link bullets found",
        );
    }
    if candidates.len() == 1 && candidates[0].1.is_none() {
        return invalid_document(
            parts.body,
            parts.has_frontmatter,
            "malformed SDD artifact-link Markdown",
        );
    }

    let canonical = candidates
        .into_iter()
        .next()
        .and_then(|(_, candidate)| candidate);
    let clean_body = canonical
        .as_ref()
        .map(|candidate| body_without_candidate(parts.body, candidate))
        .unwrap_or_else(|| parts.body.to_string());

    let legacy = match parse_legacy_link(parts.frontmatter) {
        Ok(legacy) => legacy,
        Err(reason) => {
            return invalid_document(
                &clean_body,
                parts.has_frontmatter,
                reason,
            );
        }
    };

    if let (Some(canonical), Some(legacy)) = (&canonical, &legacy) {
        if canonical.link_type != legacy.link_type {
            return invalid_document(
                &clean_body,
                parts.has_frontmatter,
                "canonical and legacy artifact links name different counterpart types",
            );
        }
    }

    let kind = match (&canonical, &legacy) {
        (Some(_), Some(_)) => SddArtifactLinkKindWire::Mixed,
        (Some(_), None) => SddArtifactLinkKindWire::Canonical,
        (None, Some(_)) => SddArtifactLinkKindWire::Legacy,
        (None, None) => SddArtifactLinkKindWire::Missing,
    };
    let link_type = canonical
        .as_ref()
        .map(|link| link.link_type)
        .or_else(|| legacy.as_ref().map(|link| link.link_type));
    SddArtifactDocumentWire {
        kind,
        link_type,
        label: canonical.as_ref().map(|link| link.label.clone()),
        target: canonical.as_ref().map(|link| link.target.clone()),
        legacy,
        body: clean_body,
        has_frontmatter: parts.has_frontmatter,
        canonical_layout: canonical.as_ref().is_some_and(|link| link.layout),
        reason: None,
    }
}

/// Install or replace one canonical artifact-link bullet.
///
/// Structurally ambiguous documents are rejected. When requested, the
/// corresponding historical YAML property is removed without reserializing
/// unrelated frontmatter.
pub fn upsert_sdd_artifact_link(
    document: &str,
    link_type: &str,
    label: &str,
    target: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> Result<String, PlanError> {
    let requested_type = SddArtifactLinkTypeWire::parse(link_type)?;
    let bullet = render_sdd_artifact_link(link_type, label, target)?;
    let parsed = parse_sdd_artifact_link(document);
    if parsed.kind == SddArtifactLinkKindWire::Invalid {
        return Err(PlanError::validation(
            parsed
                .reason
                .unwrap_or_else(|| "invalid SDD artifact link".to_string()),
        ));
    }
    if parsed.kind == SddArtifactLinkKindWire::Mixed
        && !parsed.mixed_representations_agree()
        && !allow_resolved_mixed
    {
        return Err(PlanError::validation(
            "canonical and legacy artifact links conflict; resolve their physical targets before updating",
        ));
    }
    if parsed
        .link_type
        .is_some_and(|existing| existing != requested_type)
    {
        return Err(PlanError::validation(format!(
            "document contains a {} artifact link, expected {}",
            parsed.link_type.unwrap().as_str(),
            requested_type.as_str()
        )));
    }

    let parts = split_document(document).map_err(PlanError::validation)?;
    let mut prefix = parts.prefix.to_string();
    if remove_legacy && parts.has_frontmatter {
        prefix =
            remove_top_level_yaml_key(&prefix, requested_type.legacy_field());
    }
    let body = parsed.body.trim_start_matches(['\r', '\n']);
    if parts.has_frontmatter {
        if !prefix.ends_with('\n') {
            prefix.push('\n');
        }
        Ok(format!("{prefix}\n{bullet}\n\n{body}"))
    } else {
        Ok(format!("{bullet}\n\n{body}"))
    }
}

fn split_document(document: &str) -> Result<DocumentParts<'_>, String> {
    let mut lines = document.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return Ok(DocumentParts {
            prefix: "",
            frontmatter: None,
            body: "",
            has_frontmatter: false,
        });
    };
    if trim_line(first) != "---" {
        return Ok(DocumentParts {
            prefix: "",
            frontmatter: None,
            body: document,
            has_frontmatter: false,
        });
    }

    let mut consumed = first.len();
    let frontmatter_start = consumed;
    for line in lines {
        let line_start = consumed;
        consumed += line.len();
        if trim_line(line) == "---" {
            return Ok(DocumentParts {
                prefix: &document[..consumed],
                frontmatter: Some(&document[frontmatter_start..line_start]),
                body: &document[consumed..],
                has_frontmatter: true,
            });
        }
    }
    Err("frontmatter is missing its closing `---` marker".to_string())
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches('\n').trim_end_matches('\r')
}

fn artifact_candidates(
    body: &str,
    has_frontmatter: bool,
) -> Vec<(String, Option<CanonicalCandidate>)> {
    let mut candidates = Vec::new();
    let mut offset = 0;
    for line_with_end in body.split_inclusive('\n') {
        let line = trim_line(line_with_end);
        if looks_like_artifact_bullet(line) {
            let parsed = parse_canonical_line(line).map(|mut candidate| {
                candidate.start = offset;
                candidate.end = offset + line_with_end.len();
                let expected_start = usize::from(has_frontmatter);
                let after = &body[candidate.end..];
                candidate.layout = candidate.start == expected_start
                    && (!has_frontmatter || body.starts_with('\n'))
                    && has_exact_separator(after);
                candidate
            });
            candidates.push((line.to_string(), parsed));
        }
        offset += line_with_end.len();
    }
    if offset < body.len() {
        let line = &body[offset..];
        if looks_like_artifact_bullet(line) {
            let parsed = parse_canonical_line(line).map(|mut candidate| {
                candidate.start = offset;
                candidate.end = body.len();
                candidate.layout = false;
                candidate
            });
            candidates.push((line.to_string(), parsed));
        }
    }
    candidates
}

fn has_exact_separator(after: &str) -> bool {
    let Some(content) = after.strip_prefix('\n') else {
        return false;
    };
    content.is_empty() || !content.starts_with(['\r', '\n'])
}

fn artifact_link_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"^- \*\*(PLAN|PROMPT):\*\* \[([^\]\r\n]+)\]\(([^)\r\n]+)\)$",
        )
        .expect("artifact link regex must compile")
    })
}

fn looks_like_artifact_bullet(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("- **PLAN") || line.starts_with("- **PROMPT")
}

fn parse_canonical_line(line: &str) -> Option<CanonicalCandidate> {
    let captures = artifact_link_regex().captures(line)?;
    let link_type = SddArtifactLinkTypeWire::parse(&captures[1]).ok()?;
    let label = captures[2].to_string();
    let target = captures[3].to_string();
    validate_label(&label).ok()?;
    validate_target(&target).ok()?;
    Some(CanonicalCandidate {
        link_type,
        label,
        target,
        start: 0,
        end: 0,
        layout: false,
    })
}

fn body_without_candidate(
    body: &str,
    candidate: &CanonicalCandidate,
) -> String {
    if candidate.layout {
        let after = &body[candidate.end..];
        return after.strip_prefix('\n').unwrap_or(after).to_string();
    }
    let before = body[..candidate.start].trim_end_matches(['\r', '\n']);
    let after = body[candidate.end..].trim_start_matches(['\r', '\n']);
    match (before.is_empty(), after.is_empty()) {
        (true, _) => after.to_string(),
        (_, true) => before.to_string(),
        (false, false) => format!("{before}\n\n{after}"),
    }
}

fn parse_legacy_link(
    frontmatter: Option<&str>,
) -> Result<Option<SddLegacyArtifactLinkWire>, String> {
    let Some(frontmatter) = frontmatter else {
        return Ok(None);
    };
    let value = serde_yaml::from_str::<Value>(frontmatter)
        .map_err(|error| format!("invalid YAML frontmatter: {error}"))?;
    let Some(mapping) = value.as_mapping() else {
        return Err("frontmatter must be a YAML mapping".to_string());
    };

    let mut links = Vec::new();
    for link_type in [
        SddArtifactLinkTypeWire::Plan,
        SddArtifactLinkTypeWire::Prompt,
    ] {
        let key = Value::String(link_type.legacy_field().to_string());
        let Some(value) = mapping.get(&key) else {
            continue;
        };
        let Some(value) = value.as_str() else {
            return Err(format!(
                "legacy `{}` artifact link must be a string",
                link_type.legacy_field()
            ));
        };
        links.push(parse_legacy_value(link_type, value)?);
    }
    match links.len() {
        0 => Ok(None),
        1 => Ok(links.pop()),
        _ => {
            Err("multiple contradictory legacy artifact links found"
                .to_string())
        }
    }
}

fn parse_legacy_value(
    link_type: SddArtifactLinkTypeWire,
    value: &str,
) -> Result<SddLegacyArtifactLinkWire, String> {
    if value.trim().is_empty() {
        return Err("legacy artifact link must not be empty".to_string());
    }
    if let Some(rest) = value.strip_prefix('[') {
        let Some(separator) = rest.find("](") else {
            return Err("malformed legacy Markdown artifact link".to_string());
        };
        let label = &rest[..separator];
        let target_with_close = &rest[separator + 2..];
        let Some(target) = target_with_close.strip_suffix(')') else {
            return Err("malformed legacy Markdown artifact link".to_string());
        };
        validate_label(label).map_err(str::to_string)?;
        validate_target(target).map_err(str::to_string)?;
        return Ok(SddLegacyArtifactLinkWire {
            link_type,
            format: "markdown".to_string(),
            reference: label.to_string(),
            target: target.to_string(),
        });
    }
    if value.contains("](") {
        return Err("ambiguous Markdown-like legacy artifact link".to_string());
    }
    Ok(SddLegacyArtifactLinkWire {
        link_type,
        format: "path".to_string(),
        reference: value.to_string(),
        target: value.to_string(),
    })
}

fn remove_top_level_yaml_key(prefix: &str, key: &str) -> String {
    let lines: Vec<&str> = prefix.split_inclusive('\n').collect();
    let Some(start) = lines.iter().position(|line| {
        let line = trim_line(line);
        !line.starts_with([' ', '\t'])
            && line
                .split_once(':')
                .is_some_and(|(candidate, _)| candidate.trim_end() == key)
    }) else {
        return prefix.to_string();
    };
    let mut end = start + 1;
    while end < lines.len() {
        let line = trim_line(lines[end]);
        if line == "---" || (!line.is_empty() && !line.starts_with([' ', '\t']))
        {
            break;
        }
        end += 1;
    }
    lines[..start]
        .iter()
        .chain(lines[end..].iter())
        .copied()
        .collect()
}

fn invalid_document(
    body: &str,
    has_frontmatter: bool,
    reason: impl Into<String>,
) -> SddArtifactDocumentWire {
    SddArtifactDocumentWire {
        kind: SddArtifactLinkKindWire::Invalid,
        link_type: None,
        label: None,
        target: None,
        legacy: None,
        body: body.to_string(),
        has_frontmatter,
        canonical_layout: false,
        reason: Some(reason.into()),
    }
}

fn validate_label(label: &str) -> Result<(), &'static str> {
    if label.is_empty() {
        return Err("artifact link label must not be empty");
    }
    if label.trim() != label {
        return Err("artifact link label must not have surrounding whitespace");
    }
    if label.contains(['\n', '\r', '[', ']']) {
        return Err("artifact link label contains unsupported Markdown syntax");
    }
    Ok(())
}

fn validate_target(target: &str) -> Result<(), &'static str> {
    if target.is_empty() {
        return Err("artifact link target must not be empty");
    }
    if target.trim() != target {
        return Err(
            "artifact link target must not have surrounding whitespace",
        );
    }
    if target.contains(['\n', '\r', '\\', '(', ')']) {
        return Err("artifact link target must be a plain POSIX path");
    }
    if Path::new(target).is_absolute()
        || target.starts_with("//")
        || target.contains("://")
        || looks_like_windows_absolute_path(target)
    {
        return Err("artifact link target must be relative");
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
    fn renders_exact_typed_bullets() {
        assert_eq!(
            render_sdd_artifact_link(
                "PROMPT",
                "202607/prompts/example.md",
                "prompts/example.md"
            )
            .unwrap(),
            "- **PROMPT:** [202607/prompts/example.md](prompts/example.md)"
        );
        assert_eq!(
            render_sdd_artifact_link(
                "PLAN",
                "../202607/example.md",
                "../example.md"
            )
            .unwrap(),
            "- **PLAN:** [../202607/example.md](../example.md)"
        );
    }

    #[test]
    fn parses_and_splits_canonical_frontmatter_document() {
        let document = "---\ntier: tale\n---\n\n- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n\n# Plan\n\nBody.\n";
        let parsed = parse_sdd_artifact_link(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert_eq!(parsed.link_type, Some(SddArtifactLinkTypeWire::Prompt));
        assert_eq!(parsed.label.as_deref(), Some("202607/prompts/example.md"));
        assert_eq!(parsed.target.as_deref(), Some("prompts/example.md"));
        assert!(parsed.canonical_layout);
        assert_eq!(parsed.body, "# Plan\n\nBody.\n");
    }

    #[test]
    fn parses_bare_document_and_legacy_formats() {
        let bare = "- **PLAN:** [../202607/example.md](../example.md)\n\nOriginal prompt.\n";
        let parsed = parse_sdd_artifact_link(bare);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert_eq!(parsed.body, "Original prompt.\n");

        for (value, format, reference, target) in [
            (
                "202607/prompts/example.md",
                "path",
                "202607/prompts/example.md",
                "202607/prompts/example.md",
            ),
            (
                "[202607/prompts/example.md](prompts/example.md)",
                "markdown",
                "202607/prompts/example.md",
                "prompts/example.md",
            ),
        ] {
            let document =
                format!("---\nprompt: '{value}'\ntier: tale\n---\n# Plan\n");
            let parsed = parse_sdd_artifact_link(&document);
            assert_eq!(parsed.kind, SddArtifactLinkKindWire::Legacy);
            let legacy = parsed.legacy.unwrap();
            assert_eq!(legacy.format, format);
            assert_eq!(legacy.reference, reference);
            assert_eq!(legacy.target, target);
        }
    }

    #[test]
    fn identifies_mixed_transition_and_rejects_contradictory_types() {
        let mixed = "---\nprompt: 202607/prompts/example.md\n---\n\n- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n\n# Plan\n";
        assert_eq!(
            parse_sdd_artifact_link(mixed).kind,
            SddArtifactLinkKindWire::Mixed
        );

        let contradictory = mixed.replace("prompt:", "plan:");
        let parsed = parse_sdd_artifact_link(&contradictory);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Invalid);
        assert!(parsed
            .reason
            .unwrap()
            .contains("different counterpart types"));
    }

    #[test]
    fn rejects_malformed_duplicate_and_unsafe_bullets() {
        for document in [
            "- **PLAN:** [label](target.md\n\nBody\n",
            "- **PLAN:** [label](/absolute.md)\n\nBody\n",
            "- **PLAN:** [one](one.md)\n- **PLAN:** [two](two.md)\n",
            "- **PLAN:** [](target.md)\n\nBody\n",
        ] {
            assert_eq!(
                parse_sdd_artifact_link(document).kind,
                SddArtifactLinkKindWire::Invalid,
                "{document:?}"
            );
        }
    }

    #[test]
    fn upsert_preserves_frontmatter_and_body_and_is_idempotent() {
        let document = "---\ntier: tale\nprompt: old/path.md\ngoal: Keep me\n---\n# Plan\n\nBody.\n";
        let updated = upsert_sdd_artifact_link(
            document,
            "PROMPT",
            "202607/prompts/example.md",
            "prompts/example.md",
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            updated,
            "---\ntier: tale\ngoal: Keep me\n---\n\n- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n\n# Plan\n\nBody.\n"
        );
        assert_eq!(
            upsert_sdd_artifact_link(
                &updated,
                "PROMPT",
                "202607/prompts/example.md",
                "prompts/example.md",
                true,
                false,
            )
            .unwrap(),
            updated
        );
    }

    #[test]
    fn upsert_handles_bare_body_and_replaces_existing_link() {
        let updated = upsert_sdd_artifact_link(
            "# Prompt\n",
            "PLAN",
            "../202607/example.md",
            "../example.md",
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            updated,
            "- **PLAN:** [../202607/example.md](../example.md)\n\n# Prompt\n"
        );
        let replaced = upsert_sdd_artifact_link(
            &updated.replace("example", "old"),
            "PLAN",
            "../202607/example.md",
            "../example.md",
            true,
            false,
        )
        .unwrap();
        assert_eq!(replaced, updated);
    }

    #[test]
    fn detects_and_normalizes_noncanonical_spacing() {
        let document =
            "- **PLAN:** [../202607/example.md](../example.md)\n\n\n# Prompt\n";
        let parsed = parse_sdd_artifact_link(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert!(!parsed.canonical_layout);

        let updated = upsert_sdd_artifact_link(
            document,
            "PLAN",
            "../202607/example.md",
            "../example.md",
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            updated,
            "- **PLAN:** [../202607/example.md](../example.md)\n\n# Prompt\n"
        );
    }

    #[test]
    fn upsert_requires_caller_to_resolve_nonidentical_mixed_targets() {
        let document = "---\nplan: legacy/example.md\n---\n\n- **PLAN:** [../202607/example.md](../example.md)\n\n# Prompt\n";
        assert!(upsert_sdd_artifact_link(
            document,
            "PLAN",
            "../202607/example.md",
            "../example.md",
            true,
            false,
        )
        .is_err());
        assert!(upsert_sdd_artifact_link(
            document,
            "PLAN",
            "../202607/example.md",
            "../example.md",
            true,
            true,
        )
        .is_ok());
    }
}
