//! Shared document-level contract for SDD provenance header blocks.
//!
//! Canonical links are ordinary Markdown bullets at the top of the document
//! body. Historical `plan` and `prompt` YAML properties remain readable, but
//! new writes use the bullet representation.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use super::wire::PlanError;

/// Wire schema for the plan-header block contract.
pub const PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION: u64 = 3;

/// Maximum number of visible entries in one list-shaped section.
pub const MAX_RENDERED_PLAN_HEADER_ENTRIES: usize = 50;

/// A section in an SDD provenance header block.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "UPPERCASE")]
pub enum SddPlanHeaderSectionKindWire {
    Plan,
    Prompt,
    Parent,
    Bead,
    Agents,
    Artifacts,
    Commits,
}

impl SddPlanHeaderSectionKindWire {
    pub(crate) fn parse(value: &str) -> Result<Self, PlanError> {
        match value.to_ascii_uppercase().as_str() {
            "PLAN" => Ok(Self::Plan),
            "PROMPT" => Ok(Self::Prompt),
            "PARENT" => Ok(Self::Parent),
            "BEAD" => Ok(Self::Bead),
            "AGENTS" => Ok(Self::Agents),
            "ARTIFACTS" => Ok(Self::Artifacts),
            "COMMITS" => Ok(Self::Commits),
            _ => Err(PlanError::validation(format!(
                "unsupported plan header section `{value}`; expected PLAN, PROMPT, PARENT, BEAD, AGENTS, ARTIFACTS, or COMMITS"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "PLAN",
            Self::Prompt => "PROMPT",
            Self::Parent => "PARENT",
            Self::Bead => "BEAD",
            Self::Agents => "AGENTS",
            Self::Artifacts => "ARTIFACTS",
            Self::Commits => "COMMITS",
        }
    }

    fn legacy_field(self) -> Option<&'static str> {
        match self {
            Self::Plan => Some("plan"),
            Self::Prompt => Some("prompt"),
            Self::Parent => Some("parent"),
            Self::Bead | Self::Agents | Self::Artifacts | Self::Commits => None,
        }
    }

    fn is_link(self) -> bool {
        matches!(self, Self::Plan | Self::Prompt | Self::Parent | Self::Bead)
    }

    fn allows_unlinked_label(self) -> bool {
        self == Self::Bead
    }
}

/// The counterpart named by the legacy single-link API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SddArtifactLinkTypeWire {
    Plan,
    Prompt,
}

impl SddArtifactLinkTypeWire {
    pub(crate) fn parse(value: &str) -> Result<Self, PlanError> {
        let kind = SddPlanHeaderSectionKindWire::parse(value)?;
        Self::try_from(kind)
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

impl From<SddArtifactLinkTypeWire> for SddPlanHeaderSectionKindWire {
    fn from(value: SddArtifactLinkTypeWire) -> Self {
        match value {
            SddArtifactLinkTypeWire::Plan => Self::Plan,
            SddArtifactLinkTypeWire::Prompt => Self::Prompt,
        }
    }
}

impl TryFrom<SddPlanHeaderSectionKindWire> for SddArtifactLinkTypeWire {
    type Error = PlanError;

    fn try_from(
        value: SddPlanHeaderSectionKindWire,
    ) -> Result<Self, Self::Error> {
        match value {
            SddPlanHeaderSectionKindWire::Plan => Ok(Self::Plan),
            SddPlanHeaderSectionKindWire::Prompt => Ok(Self::Prompt),
            _ => Err(PlanError::validation(format!(
                "{} is not supported by the legacy single-link API",
                value.as_str()
            ))),
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

/// One item under a list-shaped plan-header section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddPlanHeaderEntryWire {
    pub label: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub trailing_text: Option<String>,
}

/// One link-shaped or list-shaped plan-header section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddPlanHeaderSectionWire {
    pub kind: SddPlanHeaderSectionKindWire,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub entries: Vec<SddPlanHeaderEntryWire>,
    /// Entries deliberately omitted from the rendered block after the shared
    /// cap. Parsed truncation markers populate this field.
    #[serde(default)]
    pub omitted: usize,
}

/// Parsed document-level plan-header state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddPlanHeaderDocumentWire {
    pub schema_version: u64,
    pub kind: SddArtifactLinkKindWire,
    pub sections: Vec<SddPlanHeaderSectionWire>,
    pub legacy: Option<SddLegacyArtifactLinkWire>,
    /// Markdown body with a structurally valid header block and its separator
    /// removed.
    pub body: String,
    pub has_frontmatter: bool,
    /// Whether the detected block already has exact top-of-body placement and
    /// one-blank-line layout.
    pub canonical_layout: bool,
    pub reason: Option<String>,
}

/// Parsed document-level state returned by the legacy single-link API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SddArtifactDocumentWire {
    pub kind: SddArtifactLinkKindWire,
    pub link_type: Option<SddArtifactLinkTypeWire>,
    pub label: Option<String>,
    pub target: Option<String>,
    pub legacy: Option<SddLegacyArtifactLinkWire>,
    pub body: String,
    pub has_frontmatter: bool,
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
        let Some(legacy) = &self.legacy else {
            return false;
        };
        let (Some(label), Some(target)) = (&self.label, &self.target) else {
            return true;
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

#[derive(Debug)]
struct PhysicalLine<'a> {
    text: &'a str,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct ParsedBlock {
    sections: Vec<SddPlanHeaderSectionWire>,
    start: usize,
    end: usize,
    layout: bool,
}

#[derive(Clone, Copy)]
enum LegacyRemoval {
    None,
    One(SddPlanHeaderSectionKindWire),
    All,
}

/// Render the exact canonical artifact-link bullet.
pub fn render_sdd_artifact_link(
    link_type: &str,
    label: &str,
    target: &str,
) -> Result<String, PlanError> {
    let link_type = SddArtifactLinkTypeWire::parse(link_type)?;
    render_sdd_plan_header_block(&[SddPlanHeaderSectionWire {
        kind: link_type.into(),
        label: Some(label.to_string()),
        target: Some(target.to_string()),
        entries: Vec::new(),
        omitted: 0,
    }])
}

/// Parse the block contract and project the legacy single-link view.
pub fn parse_sdd_artifact_link(document: &str) -> SddArtifactDocumentWire {
    let block = parse_sdd_plan_header_block(document);
    if block.kind == SddArtifactLinkKindWire::Invalid {
        return artifact_invalid(
            block.body,
            block.has_frontmatter,
            block
                .reason
                .unwrap_or_else(|| "invalid SDD plan header block".to_string()),
        );
    }

    let counterpart_sections = block
        .sections
        .iter()
        .filter(|section| {
            matches!(
                section.kind,
                SddPlanHeaderSectionKindWire::Plan
                    | SddPlanHeaderSectionKindWire::Prompt
            )
        })
        .collect::<Vec<_>>();
    if counterpart_sections.len() > 1 {
        return artifact_invalid(
            block.body,
            block.has_frontmatter,
            "multiple SDD artifact-link bullets found",
        );
    }
    let section = counterpart_sections.first().copied();
    let link_type = section
        .map(|section| SddArtifactLinkTypeWire::try_from(section.kind))
        .transpose()
        .ok()
        .flatten()
        .or_else(|| block.legacy.as_ref().map(|link| link.link_type));

    let kind = match (section, block.legacy.as_ref()) {
        (Some(_), Some(_)) => SddArtifactLinkKindWire::Mixed,
        (Some(_), None) => SddArtifactLinkKindWire::Canonical,
        (None, Some(_)) => SddArtifactLinkKindWire::Legacy,
        (None, None) => SddArtifactLinkKindWire::Missing,
    };
    SddArtifactDocumentWire {
        kind,
        link_type,
        label: section.and_then(|section| section.label.clone()),
        target: section.and_then(|section| section.target.clone()),
        legacy: block.legacy,
        body: block.body,
        has_frontmatter: block.has_frontmatter,
        canonical_layout: block.canonical_layout,
        reason: block.reason,
    }
}

/// Parse one complete SDD Markdown document into its header-block state.
pub fn parse_sdd_plan_header_block(
    document: &str,
) -> SddPlanHeaderDocumentWire {
    let parts = match split_document(document) {
        Ok(parts) => parts,
        Err(reason) => return block_invalid(document, false, reason),
    };
    let parsed_block = match parse_block(parts.body, parts.has_frontmatter) {
        Ok(block) => block,
        Err(reason) => {
            return block_invalid(parts.body, parts.has_frontmatter, reason);
        }
    };
    let clean_body = parsed_block
        .as_ref()
        .map(|block| body_without_block(parts.body, block))
        .unwrap_or_else(|| parts.body.to_string());
    let legacy = match parse_legacy_link(parts.frontmatter) {
        Ok(legacy) => legacy,
        Err(reason) => {
            return block_invalid(&clean_body, parts.has_frontmatter, reason);
        }
    };
    let sections = parsed_block
        .as_ref()
        .map(|block| block.sections.clone())
        .unwrap_or_default();

    let counterpart_kinds = sections
        .iter()
        .filter(|section| {
            matches!(
                section.kind,
                SddPlanHeaderSectionKindWire::Plan
                    | SddPlanHeaderSectionKindWire::Prompt
            )
        })
        .map(|section| section.kind)
        .collect::<Vec<_>>();
    if counterpart_kinds.len() > 1 {
        return block_invalid(
            &clean_body,
            parts.has_frontmatter,
            "header block contains both PLAN and PROMPT sections",
        );
    }
    if let (Some(kind), Some(legacy)) =
        (counterpart_kinds.first(), legacy.as_ref())
    {
        if SddPlanHeaderSectionKindWire::from(legacy.link_type) != *kind {
            return block_invalid(
                &clean_body,
                parts.has_frontmatter,
                "canonical and legacy artifact links name different counterpart types",
            );
        }
    }

    let kind = match (parsed_block.as_ref(), legacy.as_ref()) {
        (Some(_), Some(_)) => SddArtifactLinkKindWire::Mixed,
        (Some(_), None) => SddArtifactLinkKindWire::Canonical,
        (None, Some(_)) => SddArtifactLinkKindWire::Legacy,
        (None, None) => SddArtifactLinkKindWire::Missing,
    };
    SddPlanHeaderDocumentWire {
        schema_version: PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION,
        kind,
        sections,
        legacy,
        body: clean_body,
        has_frontmatter: parts.has_frontmatter,
        canonical_layout: parsed_block
            .as_ref()
            .is_some_and(|block| block.layout),
        reason: None,
    }
}

/// Render canonical sections in their fixed order.
pub fn render_sdd_plan_header_block(
    sections: &[SddPlanHeaderSectionWire],
) -> Result<String, PlanError> {
    let sections = normalize_sections(sections)?;
    let mut lines = Vec::new();
    for section in sections {
        if section.kind.is_link() {
            let label = section.label.as_deref().expect("normalized label");
            if let Some(target) = section.target.as_deref() {
                lines.push(format!(
                    "- **{}:** [{}]({target})",
                    section.kind.as_str(),
                    escape_link_label(label)
                ));
            } else {
                lines.push(format!(
                    "- **{}:** {}",
                    section.kind.as_str(),
                    escape_markdown_text(label)
                ));
            }
            continue;
        }

        lines.push(format!("- **{}:**", section.kind.as_str()));
        for entry in &section.entries {
            let label = if entry.target.is_some() {
                escape_link_label(&entry.label)
            } else {
                escape_markdown_text(&entry.label)
            };
            let mut item = if let Some(target) = &entry.target {
                format!("  - [{label}]({target})")
            } else {
                format!("  - {label}")
            };
            if let Some(trailing_text) = &entry.trailing_text {
                item.push_str(" — ");
                item.push_str(&escape_markdown_text(trailing_text));
            }
            lines.push(item);
        }
        if section.omitted > 0 {
            lines.push(format!("  - … and {} more", section.omitted));
        }
    }
    Ok(lines.join("\n"))
}

/// Install or replace one section in a plan-header block.
pub fn upsert_sdd_plan_header_section(
    document: &str,
    section: SddPlanHeaderSectionWire,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> Result<String, PlanError> {
    let parsed = parse_sdd_plan_header_block(document);
    validate_existing_for_update(&parsed, allow_resolved_mixed)?;
    let mut sections = parsed.sections.clone();
    sections.retain(|candidate| candidate.kind != section.kind);
    sections.push(section.clone());
    rewrite_block(
        document,
        &parsed,
        &sections,
        if remove_legacy {
            LegacyRemoval::One(section.kind)
        } else {
            LegacyRemoval::None
        },
    )
}

/// Replace the complete plan-header block.
pub fn replace_sdd_plan_header_block(
    document: &str,
    sections: &[SddPlanHeaderSectionWire],
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> Result<String, PlanError> {
    let parsed = parse_sdd_plan_header_block(document);
    validate_existing_for_update(&parsed, allow_resolved_mixed)?;
    rewrite_block(
        document,
        &parsed,
        sections,
        if remove_legacy {
            LegacyRemoval::All
        } else {
            LegacyRemoval::None
        },
    )
}

/// Remove one section from the plan-header block.
pub fn remove_sdd_plan_header_section(
    document: &str,
    kind: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> Result<String, PlanError> {
    let kind = SddPlanHeaderSectionKindWire::parse(kind)?;
    let parsed = parse_sdd_plan_header_block(document);
    validate_existing_for_update(&parsed, allow_resolved_mixed)?;
    let sections = parsed
        .sections
        .iter()
        .filter(|section| section.kind != kind)
        .cloned()
        .collect::<Vec<_>>();
    rewrite_block(
        document,
        &parsed,
        &sections,
        if remove_legacy {
            LegacyRemoval::One(kind)
        } else {
            LegacyRemoval::None
        },
    )
}

/// Install or replace one canonical artifact-link bullet.
pub fn upsert_sdd_artifact_link(
    document: &str,
    link_type: &str,
    label: &str,
    target: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> Result<String, PlanError> {
    let requested_type = SddArtifactLinkTypeWire::parse(link_type)?;
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
    upsert_sdd_plan_header_section(
        document,
        SddPlanHeaderSectionWire {
            kind: requested_type.into(),
            label: Some(label.to_string()),
            target: Some(target.to_string()),
            entries: Vec::new(),
            omitted: 0,
        },
        remove_legacy,
        allow_resolved_mixed,
    )
}

fn validate_existing_for_update(
    parsed: &SddPlanHeaderDocumentWire,
    allow_resolved_mixed: bool,
) -> Result<(), PlanError> {
    if parsed.kind == SddArtifactLinkKindWire::Invalid {
        return Err(PlanError::validation(
            parsed
                .reason
                .clone()
                .unwrap_or_else(|| "invalid SDD plan header block".to_string()),
        ));
    }
    if parsed.kind == SddArtifactLinkKindWire::Mixed
        && !block_mixed_representations_agree(parsed)
        && !allow_resolved_mixed
    {
        return Err(PlanError::validation(
            "canonical and legacy artifact links conflict; resolve their physical targets before updating",
        ));
    }
    Ok(())
}

fn block_mixed_representations_agree(
    parsed: &SddPlanHeaderDocumentWire,
) -> bool {
    let Some(legacy) = &parsed.legacy else {
        return true;
    };
    let counterpart = parsed.sections.iter().find(|section| {
        section.kind == SddPlanHeaderSectionKindWire::from(legacy.link_type)
    });
    let Some(section) = counterpart else {
        return true;
    };
    {
        section.kind == SddPlanHeaderSectionKindWire::from(legacy.link_type)
            && section.label.as_ref().is_some_and(|label| {
                *label == legacy.reference
                    || section.target.as_ref() == Some(&legacy.target)
            })
    }
}

fn rewrite_block(
    document: &str,
    parsed: &SddPlanHeaderDocumentWire,
    sections: &[SddPlanHeaderSectionWire],
    remove_legacy: LegacyRemoval,
) -> Result<String, PlanError> {
    let normalized = normalize_sections(sections)?;
    if normalized == parsed.sections
        && parsed.canonical_layout
        && !legacy_removal_needed(document, parsed, remove_legacy)
    {
        return Ok(document.to_string());
    }

    let parts = split_document(document).map_err(PlanError::validation)?;
    let mut prefix = parts.prefix.to_string();
    if !matches!(remove_legacy, LegacyRemoval::None) && parts.has_frontmatter {
        match remove_legacy {
            LegacyRemoval::All => {
                prefix = remove_top_level_yaml_key(&prefix, "plan");
                prefix = remove_top_level_yaml_key(&prefix, "prompt");
                prefix = remove_top_level_yaml_key(&prefix, "parent");
            }
            LegacyRemoval::One(kind) => {
                if let Some(field) = kind.legacy_field() {
                    prefix = remove_top_level_yaml_key(&prefix, field);
                }
            }
            LegacyRemoval::None => {}
        }
    }

    let rendered = render_sdd_plan_header_block(&normalized)?;
    let body = parsed.body.trim_start_matches(['\r', '\n']);
    let content = if rendered.is_empty() {
        body.to_string()
    } else if body.is_empty() {
        format!("{rendered}\n")
    } else {
        format!("{rendered}\n\n{body}")
    };
    if parts.has_frontmatter {
        if !prefix.ends_with('\n') {
            prefix.push('\n');
        }
        if content.is_empty() {
            return Ok(prefix);
        }
        Ok(format!("{prefix}\n{content}"))
    } else {
        Ok(content)
    }
}

fn legacy_removal_needed(
    document: &str,
    parsed: &SddPlanHeaderDocumentWire,
    remove_legacy: LegacyRemoval,
) -> bool {
    match remove_legacy {
        LegacyRemoval::None => false,
        LegacyRemoval::All => {
            parsed.legacy.is_some()
                || ["plan", "prompt", "parent"]
                    .iter()
                    .any(|field| has_top_level_yaml_key(document, field))
        }
        LegacyRemoval::One(kind) => {
            parsed.legacy.as_ref().is_some_and(|legacy| {
                kind == SddPlanHeaderSectionKindWire::from(legacy.link_type)
            }) || kind
                .legacy_field()
                .is_some_and(|field| has_top_level_yaml_key(document, field))
        }
    }
}

fn normalize_sections(
    sections: &[SddPlanHeaderSectionWire],
) -> Result<Vec<SddPlanHeaderSectionWire>, PlanError> {
    let mut normalized = BTreeMap::new();
    for section in sections {
        if normalized.contains_key(&section.kind) {
            return Err(PlanError::validation(format!(
                "duplicate {} plan header section",
                section.kind.as_str()
            )));
        }
        let section = normalize_section(section)?;
        if let Some(section) = section {
            normalized.insert(section.kind, section);
        }
    }
    if normalized.contains_key(&SddPlanHeaderSectionKindWire::Plan)
        && normalized.contains_key(&SddPlanHeaderSectionKindWire::Prompt)
    {
        return Err(PlanError::validation(
            "header block cannot contain both PLAN and PROMPT sections",
        ));
    }
    Ok([
        SddPlanHeaderSectionKindWire::Plan,
        SddPlanHeaderSectionKindWire::Prompt,
        SddPlanHeaderSectionKindWire::Parent,
        SddPlanHeaderSectionKindWire::Bead,
        SddPlanHeaderSectionKindWire::Agents,
        SddPlanHeaderSectionKindWire::Artifacts,
        SddPlanHeaderSectionKindWire::Commits,
    ]
    .into_iter()
    .filter_map(|kind| normalized.remove(&kind))
    .collect())
}

fn normalize_section(
    section: &SddPlanHeaderSectionWire,
) -> Result<Option<SddPlanHeaderSectionWire>, PlanError> {
    if section.kind.is_link() {
        if !section.entries.is_empty() || section.omitted != 0 {
            return Err(PlanError::validation(format!(
                "{} is link-shaped and cannot contain entries",
                section.kind.as_str()
            )));
        }
        let label = section.label.as_deref().ok_or_else(|| {
            PlanError::validation(format!(
                "{} requires a label",
                section.kind.as_str()
            ))
        })?;
        validate_label(label).map_err(PlanError::validation)?;
        if let Some(target) = section.target.as_deref() {
            validate_section_target(section.kind, target)
                .map_err(PlanError::validation)?;
        } else if !section.kind.allows_unlinked_label() {
            return Err(PlanError::validation(format!(
                "{} requires a target",
                section.kind.as_str()
            )));
        }
        return Ok(Some(section.clone()));
    }

    if section.label.is_some() || section.target.is_some() {
        return Err(PlanError::validation(format!(
            "{} is list-shaped and cannot have a section label or target",
            section.kind.as_str()
        )));
    }
    if section.entries.is_empty() && section.omitted == 0 {
        return Ok(None);
    }
    let mut entries = Vec::new();
    for entry in &section.entries {
        validate_entry(entry)?;
        entries.push(entry.clone());
    }
    let total_omitted = section.omitted.saturating_add(
        entries
            .len()
            .saturating_sub(MAX_RENDERED_PLAN_HEADER_ENTRIES),
    );
    entries.truncate(MAX_RENDERED_PLAN_HEADER_ENTRIES);
    Ok(Some(SddPlanHeaderSectionWire {
        kind: section.kind,
        label: None,
        target: None,
        entries,
        omitted: total_omitted,
    }))
}

fn validate_entry(entry: &SddPlanHeaderEntryWire) -> Result<(), PlanError> {
    validate_label(&entry.label).map_err(PlanError::validation)?;
    if let Some(target) = &entry.target {
        validate_absolute_or_relative_target(target)
            .map_err(PlanError::validation)?;
    }
    if let Some(trailing_text) = &entry.trailing_text {
        validate_trailing_text(trailing_text).map_err(PlanError::validation)?;
    }
    Ok(())
}

fn parse_block(
    body: &str,
    has_frontmatter: bool,
) -> Result<Option<ParsedBlock>, String> {
    let lines = physical_lines(body);
    let Some(start_index) = first_header_candidate(&lines) else {
        return Ok(None);
    };

    let expected_start = usize::from(has_frontmatter);
    let mut index = start_index;
    let mut sections = Vec::new();
    while index < lines.len() {
        let line = &lines[index];
        if !is_top_level_header_bullet(line.text) {
            break;
        }
        let (kind_text, inline) = parse_header_prefix(line.text)
            .ok_or_else(|| "malformed plan header Markdown".to_string())?;
        let kind =
            SddPlanHeaderSectionKindWire::parse(kind_text).map_err(|_| {
                format!(
                    "unknown `{kind_text}` section inside plan header block"
                )
            })?;
        if sections
            .iter()
            .any(|section: &SddPlanHeaderSectionWire| section.kind == kind)
        {
            return Err(format!(
                "duplicate {} plan header section",
                kind.as_str()
            ));
        }
        index += 1;

        if kind.is_link() {
            let mut logical = inline.to_string();
            while index < lines.len()
                && is_continuation(lines[index].text, 2)
                && !is_sub_bullet(lines[index].text)
            {
                append_continuation(&mut logical, lines[index].text);
                index += 1;
            }
            let logical = logical.trim();
            let (label, target) = if logical.starts_with('[') {
                let (label, target, rest) = parse_markdown_link(logical)
                    .ok_or_else(|| {
                        format!(
                            "malformed {} plan header link Markdown",
                            kind.as_str()
                        )
                    })?;
                if !rest.trim().is_empty() {
                    return Err(format!(
                        "unexpected trailing text in {} plan header section",
                        kind.as_str()
                    ));
                }
                (label, Some(target))
            } else if kind.allows_unlinked_label() {
                (unescape_markdown(logical)?, None)
            } else {
                return Err(format!(
                    "malformed {} plan header link Markdown",
                    kind.as_str()
                ));
            };
            validate_label(&label).map_err(str::to_string)?;
            if let Some(target) = target.as_deref() {
                validate_section_target(kind, target)
                    .map_err(str::to_string)?;
            }
            sections.push(SddPlanHeaderSectionWire {
                kind,
                label: Some(label),
                target,
                entries: Vec::new(),
                omitted: 0,
            });
            continue;
        }

        if !inline.trim().is_empty() {
            return Err(format!(
                "{} section header must not contain inline content",
                kind.as_str()
            ));
        }
        let mut entries = Vec::new();
        let mut omitted = 0;
        while index < lines.len() && is_sub_bullet(lines[index].text) {
            let mut logical = sub_bullet_content(lines[index].text).to_string();
            index += 1;
            while index < lines.len()
                && is_continuation(lines[index].text, 4)
                && !is_sub_bullet(lines[index].text)
            {
                append_continuation(&mut logical, lines[index].text);
                index += 1;
            }
            if let Some(count) = parse_omitted_marker(logical.trim()) {
                if omitted != 0 {
                    return Err(format!(
                        "duplicate truncation marker in {} section",
                        kind.as_str()
                    ));
                }
                omitted = count;
                continue;
            }
            entries.push(parse_entry(logical.trim())?);
        }
        if entries.is_empty() && omitted == 0 {
            return Err(format!(
                "{} plan header section must not be empty",
                kind.as_str()
            ));
        }
        sections.push(SddPlanHeaderSectionWire {
            kind,
            label: None,
            target: None,
            entries,
            omitted,
        });
    }

    let end = if index == 0 { 0 } else { lines[index - 1].end };
    if first_header_candidate(&lines[index..]).is_some() {
        return Err(
            "discontiguous or nested plan header bullets found".to_string()
        );
    }
    let normalized =
        normalize_sections(&sections).map_err(|error| error.to_string())?;
    let start = lines[start_index].start;
    let after = &body[end..];
    let layout = start == expected_start
        && (!has_frontmatter || body.starts_with('\n'))
        && has_exact_separator(after);
    Ok(Some(ParsedBlock {
        sections: normalized,
        start,
        end,
        layout,
    }))
}

fn parse_entry(value: &str) -> Result<SddPlanHeaderEntryWire, String> {
    let (label, target, trailing_text) = if value.starts_with('[') {
        let (label, target, rest) = parse_markdown_link(value)
            .ok_or_else(|| "malformed plan header entry link".to_string())?;
        let trailing_text = if rest.is_empty() {
            None
        } else {
            let rest = rest.strip_prefix(" — ").ok_or_else(|| {
                "malformed plan header entry trailing text".to_string()
            })?;
            Some(unescape_markdown(rest)?)
        };
        (label, Some(target), trailing_text)
    } else {
        let (label, trailing_text) = value
            .split_once(" — ")
            .map_or((value, None), |(label, rest)| (label, Some(rest)));
        (
            unescape_markdown(label)?,
            None,
            trailing_text.map(unescape_markdown).transpose()?,
        )
    };
    let entry = SddPlanHeaderEntryWire {
        label,
        target,
        trailing_text,
    };
    validate_entry(&entry).map_err(|error| error.to_string())?;
    Ok(entry)
}

fn parse_markdown_link(value: &str) -> Option<(String, String, &str)> {
    let content = value.strip_prefix('[')?;
    let label_end = find_unescaped(content, ']')?;
    let label = unescape_markdown(&content[..label_end]).ok()?;
    let after_label = &content[label_end + 1..];
    let target_and_rest = after_label.strip_prefix('(')?;
    let target_end = find_unescaped(target_and_rest, ')')?;
    let target = &target_and_rest[..target_end];
    if target.is_empty() {
        return None;
    }
    Some((
        label,
        target.to_string(),
        &target_and_rest[target_end + 1..],
    ))
}

fn find_unescaped(value: &str, needle: char) -> Option<usize> {
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
        } else if character == needle {
            return Some(index);
        }
    }
    None
}

fn parse_omitted_marker(value: &str) -> Option<usize> {
    let count = value.strip_prefix("… and ")?.strip_suffix(" more")?;
    count.parse().ok().filter(|count| *count > 0)
}

fn physical_lines(body: &str) -> Vec<PhysicalLine<'_>> {
    let mut lines = Vec::new();
    let mut offset = 0;
    for line_with_end in body.split_inclusive('\n') {
        let end = offset + line_with_end.len();
        lines.push(PhysicalLine {
            text: trim_line(line_with_end),
            start: offset,
            end,
        });
        offset = end;
    }
    if offset < body.len() {
        lines.push(PhysicalLine {
            text: &body[offset..],
            start: offset,
            end: body.len(),
        });
    }
    lines
}

fn first_header_candidate(lines: &[PhysicalLine<'_>]) -> Option<usize> {
    let index = lines.iter().position(|line| !line.text.trim().is_empty())?;
    looks_like_known_header_bullet(lines[index].text).then_some(index)
}

fn looks_like_known_header_bullet(line: &str) -> bool {
    let line = line.trim_start();
    parse_header_prefix(line).is_some_and(|(kind, _)| {
        SddPlanHeaderSectionKindWire::parse(kind).is_ok()
    })
}

fn is_top_level_header_bullet(line: &str) -> bool {
    line.starts_with("- **")
}

fn parse_header_prefix(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix("- **")?;
    let (kind, inline) = rest.split_once(":**")?;
    if kind.is_empty() || kind.contains('*') {
        return None;
    }
    Some((kind, inline.trim_start()))
}

fn is_sub_bullet(line: &str) -> bool {
    line.starts_with("  - ")
}

fn sub_bullet_content(line: &str) -> &str {
    line.strip_prefix("  - ").unwrap_or(line)
}

fn is_continuation(line: &str, minimum_indent: usize) -> bool {
    let indent = line
        .chars()
        .take_while(|character| *character == ' ')
        .count();
    indent >= minimum_indent && !line.trim().is_empty()
}

fn append_continuation(logical: &mut String, line: &str) {
    if !logical.is_empty() {
        logical.push(' ');
    }
    logical.push_str(line.trim());
}

fn has_exact_separator(after: &str) -> bool {
    let Some(content) = after.strip_prefix('\n') else {
        return false;
    };
    content.is_empty() || !content.starts_with(['\r', '\n'])
}

fn body_without_block(body: &str, block: &ParsedBlock) -> String {
    if block.layout {
        let after = &body[block.end..];
        return after.strip_prefix('\n').unwrap_or(after).to_string();
    }
    let before = body[..block.start].trim_end_matches(['\r', '\n']);
    let after = body[block.end..].trim_start_matches(['\r', '\n']);
    match (before.is_empty(), after.is_empty()) {
        (true, _) => after.to_string(),
        (_, true) => before.to_string(),
        (false, false) => format!("{before}\n\n{after}"),
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
        validate_relative_target(target).map_err(str::to_string)?;
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

fn has_top_level_yaml_key(document: &str, key: &str) -> bool {
    let Ok(parts) = split_document(document) else {
        return false;
    };
    parts.frontmatter.is_some_and(|frontmatter| {
        frontmatter.lines().any(|line| {
            !line.starts_with([' ', '\t'])
                && line
                    .split_once(':')
                    .is_some_and(|(candidate, _)| candidate.trim_end() == key)
        })
    })
}

fn block_invalid(
    body: impl Into<String>,
    has_frontmatter: bool,
    reason: impl Into<String>,
) -> SddPlanHeaderDocumentWire {
    SddPlanHeaderDocumentWire {
        schema_version: PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION,
        kind: SddArtifactLinkKindWire::Invalid,
        sections: Vec::new(),
        legacy: None,
        body: body.into(),
        has_frontmatter,
        canonical_layout: false,
        reason: Some(reason.into()),
    }
}

fn artifact_invalid(
    body: impl Into<String>,
    has_frontmatter: bool,
    reason: impl Into<String>,
) -> SddArtifactDocumentWire {
    SddArtifactDocumentWire {
        kind: SddArtifactLinkKindWire::Invalid,
        link_type: None,
        label: None,
        target: None,
        legacy: None,
        body: body.into(),
        has_frontmatter,
        canonical_layout: false,
        reason: Some(reason.into()),
    }
}

fn validate_label(label: &str) -> Result<(), &'static str> {
    if label.is_empty() {
        return Err("plan header label must not be empty");
    }
    if label.trim() != label {
        return Err("plan header label must not have surrounding whitespace");
    }
    if label.contains(['\n', '\r']) {
        return Err("plan header label must not contain newlines");
    }
    Ok(())
}

fn validate_trailing_text(value: &str) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("plan header trailing text must not be empty");
    }
    if value.trim() != value {
        return Err(
            "plan header trailing text must not have surrounding whitespace",
        );
    }
    if value.contains(['\n', '\r']) {
        return Err("plan header trailing text must not contain newlines");
    }
    Ok(())
}

fn validate_section_target(
    kind: SddPlanHeaderSectionKindWire,
    target: &str,
) -> Result<(), &'static str> {
    match kind {
        SddPlanHeaderSectionKindWire::Plan
        | SddPlanHeaderSectionKindWire::Prompt => {
            validate_absolute_or_relative_target(target)
        }
        SddPlanHeaderSectionKindWire::Parent => {
            validate_absolute_or_relative_target(target)
        }
        SddPlanHeaderSectionKindWire::Bead => {
            validate_absolute_or_relative_target(target)
        }
        SddPlanHeaderSectionKindWire::Agents
        | SddPlanHeaderSectionKindWire::Artifacts
        | SddPlanHeaderSectionKindWire::Commits => {
            Err("list-shaped section cannot have a target")
        }
    }
}

fn validate_absolute_or_relative_target(
    target: &str,
) -> Result<(), &'static str> {
    validate_target_syntax(target)?;
    if target.starts_with("https://") || target.starts_with("http://") {
        return Ok(());
    }
    validate_relative_target(target)
}

fn validate_relative_target(target: &str) -> Result<(), &'static str> {
    validate_target_syntax(target)?;
    if Path::new(target).is_absolute()
        || target.starts_with("//")
        || target.contains("://")
        || looks_like_windows_absolute_path(target)
    {
        return Err("plan header target must be relative");
    }
    Ok(())
}

fn validate_target_syntax(target: &str) -> Result<(), &'static str> {
    if target.is_empty() {
        return Err("plan header target must not be empty");
    }
    if target.trim() != target {
        return Err("plan header target must not have surrounding whitespace");
    }
    if target.contains(['\n', '\r', '\\', '(', ')', ' ']) {
        return Err("plan header target contains unsupported Markdown syntax");
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

fn escape_link_label(value: &str) -> String {
    escape_matching(value, |character| matches!(character, '\\' | '[' | ']'))
}

fn escape_markdown_text(value: &str) -> String {
    escape_matching(value, |character| {
        matches!(
            character,
            '\\' | '`'
                | '*'
                | '_'
                | '{'
                | '}'
                | '['
                | ']'
                | '<'
                | '>'
                | '#'
                | '!'
                | '|'
        )
    })
}

fn escape_matching(
    value: &str,
    should_escape: impl Fn(char) -> bool,
) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if should_escape(character) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn unescape_markdown(value: &str) -> Result<String, String> {
    let mut unescaped = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        let Some(escaped) = characters.next() else {
            return Err("dangling Markdown escape in plan header".to_string());
        };
        unescaped.push(escaped);
    }
    Ok(unescaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link_section(
        kind: SddPlanHeaderSectionKindWire,
        label: &str,
        target: &str,
    ) -> SddPlanHeaderSectionWire {
        SddPlanHeaderSectionWire {
            kind,
            label: Some(label.to_string()),
            target: Some(target.to_string()),
            entries: Vec::new(),
            omitted: 0,
        }
    }

    fn entry(
        label: &str,
        target: Option<&str>,
        trailing_text: Option<&str>,
    ) -> SddPlanHeaderEntryWire {
        SddPlanHeaderEntryWire {
            label: label.to_string(),
            target: target.map(str::to_string),
            trailing_text: trailing_text.map(str::to_string),
        }
    }

    #[test]
    fn multi_section_round_trip_uses_fixed_order() {
        let sections = vec![
            SddPlanHeaderSectionWire {
                kind: SddPlanHeaderSectionKindWire::Commits,
                label: None,
                target: None,
                entries: vec![entry(
                    "699456a",
                    Some("https://github.com/sase-org/sase/commit/699456a"),
                    Some("fix(xprompt): canonicalize"),
                )],
                omitted: 0,
            },
            link_section(
                SddPlanHeaderSectionKindWire::Prompt,
                "202607/prompts/example.md",
                "prompts/example.md",
            ),
            link_section(
                SddPlanHeaderSectionKindWire::Parent,
                "202607/epic.md",
                "https://github.com/sase-org/sase--plans/blob/main/202607/epic.md",
            ),
            link_section(
                SddPlanHeaderSectionKindWire::Bead,
                "sase-ai.8",
                "https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md",
            ),
        ];
        let rendered = render_sdd_plan_header_block(&sections).unwrap();
        assert!(rendered.starts_with("- **PROMPT:**"));
        let document = format!("{rendered}\n\n# Plan\n");
        let parsed = parse_sdd_plan_header_block(&document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert_eq!(parsed.sections.len(), 4);
        assert_eq!(
            render_sdd_plan_header_block(&parsed.sections).unwrap(),
            rendered
        );
        assert_eq!(parsed.body, "# Plan\n");
    }

    #[test]
    fn plan_and_prompt_sections_accept_absolute_cross_repo_targets() {
        for (kind, label, target) in [
            (
                SddPlanHeaderSectionKindWire::Plan,
                "202608/example.md",
                "https://github.com/sase-org/sase--plans/blob/main/202608/example.md",
            ),
            (
                SddPlanHeaderSectionKindWire::Prompt,
                "prompts/202608/example.md",
                "https://github.com/sase-org/sase--agents/blob/main/prompts/202608/example.md",
            ),
        ] {
            let section = link_section(kind, label, target);
            let rendered =
                render_sdd_plan_header_block(std::slice::from_ref(&section))
                    .unwrap();
            let parsed = parse_sdd_plan_header_block(&format!(
                "{rendered}\n\nDocument body\n"
            ));
            assert_eq!(parsed.sections, vec![section]);
            assert_eq!(parsed.body, "Document body\n");
        }
    }

    #[test]
    fn artifacts_is_list_shaped_and_orders_between_agents_and_commits() {
        let artifacts = SddPlanHeaderSectionWire {
            kind: SddPlanHeaderSectionKindWire::Artifacts,
            label: None,
            target: None,
            entries: vec![entry(
                "diagram.png",
                Some("../../artifacts/202608/abcdef012345-diagram.png"),
                None,
            )],
            omitted: 0,
        };
        let document = "- **PROMPT:** [prompt.md](https://example.com/prompt.md)\n- **AGENTS:**\n  - agent.one\n- **COMMITS:**\n  - abc1234\n\nBody\n";
        let updated = upsert_sdd_plan_header_section(
            document,
            artifacts.clone(),
            false,
            false,
        )
        .unwrap();
        assert_eq!(
            updated,
            "- **PROMPT:** [prompt.md](https://example.com/prompt.md)\n- **AGENTS:**\n  - agent.one\n- **ARTIFACTS:**\n  - [diagram.png](../../artifacts/202608/abcdef012345-diagram.png)\n- **COMMITS:**\n  - abc1234\n\nBody\n"
        );
        let parsed = parse_sdd_plan_header_block(&updated);
        assert_eq!(parsed.sections[2], artifacts);
        assert_eq!(
            render_sdd_plan_header_block(&parsed.sections).unwrap(),
            updated.trim_end_matches("\n\nBody\n")
        );
    }

    #[test]
    fn artifacts_is_compatible_with_either_counterpart_section() {
        let artifacts = SddPlanHeaderSectionWire {
            kind: SddPlanHeaderSectionKindWire::Artifacts,
            label: None,
            target: None,
            entries: vec![entry("artifact", Some("artifact.bin"), None)],
            omitted: 0,
        };
        for counterpart in [
            link_section(
                SddPlanHeaderSectionKindWire::Plan,
                "plan.md",
                "https://example.com/plan.md",
            ),
            link_section(
                SddPlanHeaderSectionKindWire::Prompt,
                "prompt.md",
                "https://example.com/prompt.md",
            ),
        ] {
            assert!(render_sdd_plan_header_block(&[
                counterpart,
                artifacts.clone()
            ])
            .is_ok());
        }
    }

    #[test]
    fn bead_section_supports_linked_unlinked_and_prettier_wrapped_labels() {
        let linked = link_section(
            SddPlanHeaderSectionKindWire::Bead,
            "sase-ai.8",
            "https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md",
        );
        let rendered =
            render_sdd_plan_header_block(std::slice::from_ref(&linked))
                .unwrap();
        assert_eq!(
            rendered,
            "- **BEAD:** [sase-ai.8](https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md)"
        );

        let wrapped = "- **BEAD:**\n  [sase-ai.8](https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md)\n\n# Plan\n";
        let parsed = parse_sdd_plan_header_block(wrapped);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert_eq!(parsed.sections, vec![linked]);

        let unlinked = SddPlanHeaderSectionWire {
            kind: SddPlanHeaderSectionKindWire::Bead,
            label: Some("sase-ai.8".to_string()),
            target: None,
            entries: Vec::new(),
            omitted: 0,
        };
        let rendered =
            render_sdd_plan_header_block(std::slice::from_ref(&unlinked))
                .unwrap();
        assert_eq!(rendered, "- **BEAD:** sase-ai.8");
        let parsed =
            parse_sdd_plan_header_block(&format!("{rendered}\n\n# Plan\n"));
        assert_eq!(parsed.sections, vec![unlinked]);
    }

    #[test]
    fn bead_upsert_keeps_bead_frontmatter() {
        let document =
            "---\ntier: tale\nbead: sase-ai.8\nbead_id: sase-ai\n---\n\n# Plan\n";
        let section = link_section(
            SddPlanHeaderSectionKindWire::Bead,
            "sase-ai",
            "https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/README.md",
        );

        let updated =
            upsert_sdd_plan_header_section(document, section, true, false)
                .unwrap();

        assert!(updated.contains("bead: sase-ai.8"));
        assert!(updated.contains("bead_id: sase-ai"));
        assert!(updated.contains("- **BEAD:** [sase-ai]"));
    }

    #[test]
    fn parses_prettier_wrapped_commit_and_preserves_unchanged_bytes() {
        let document = "- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n- **COMMITS:**\n  - [699456a](https://github.com/sase-org/sase/commit/699456a521e25e0aaa38f4e289db38e71a6488a6) — fix(xprompt):\n    canonicalize workflow project identity\n\n# Plan\n";
        let parsed = parse_sdd_plan_header_block(document);
        let commit = &parsed.sections[1].entries[0];
        assert_eq!(
            commit.trailing_text.as_deref(),
            Some("fix(xprompt): canonicalize workflow project identity")
        );
        assert_eq!(
            replace_sdd_plan_header_block(
                document,
                &parsed.sections,
                false,
                false,
            )
            .unwrap(),
            document
        );
    }

    #[test]
    fn empty_list_section_is_omitted() {
        let rendered = render_sdd_plan_header_block(&[
            link_section(
                SddPlanHeaderSectionKindWire::Prompt,
                "prompt.md",
                "prompt.md",
            ),
            SddPlanHeaderSectionWire {
                kind: SddPlanHeaderSectionKindWire::Agents,
                label: None,
                target: None,
                entries: Vec::new(),
                omitted: 0,
            },
        ])
        .unwrap();
        assert!(!rendered.contains("AGENTS"));
    }

    #[test]
    fn list_cap_is_visible_and_round_trips_logically() {
        let section = SddPlanHeaderSectionWire {
            kind: SddPlanHeaderSectionKindWire::Commits,
            label: None,
            target: None,
            entries: (0..53)
                .map(|index| entry(&format!("{index:07}"), None, None))
                .collect(),
            omitted: 0,
        };
        let rendered =
            render_sdd_plan_header_block(std::slice::from_ref(&section))
                .unwrap();
        assert!(rendered.contains("  - … and 3 more"));
        let parsed =
            parse_sdd_plan_header_block(&format!("{rendered}\n\nBody\n"));
        assert_ne!(
            parsed.kind,
            SddArtifactLinkKindWire::Invalid,
            "{:?}",
            parsed.reason
        );
        assert_eq!(parsed.sections[0].entries.len(), 50);
        assert_eq!(parsed.sections[0].omitted, 3);
        assert_eq!(
            render_sdd_plan_header_block(&parsed.sections).unwrap(),
            rendered
        );
    }

    #[test]
    fn escapes_labels_and_trailing_text() {
        let rendered = render_sdd_plan_header_block(&[
            link_section(
                SddPlanHeaderSectionKindWire::Prompt,
                "a_[b].md",
                "a_b.md",
            ),
            SddPlanHeaderSectionWire {
                kind: SddPlanHeaderSectionKindWire::Commits,
                label: None,
                target: None,
                entries: vec![entry("abc*123", None, Some("fix [parser]"))],
                omitted: 0,
            },
        ])
        .unwrap();
        assert!(rendered.contains(r"[a_\[b\].md]"));
        assert!(rendered.contains(r"abc\*123 — fix \[parser\]"));
        let parsed =
            parse_sdd_plan_header_block(&format!("{rendered}\n\nBody\n"));
        assert_ne!(
            parsed.kind,
            SddArtifactLinkKindWire::Invalid,
            "{:?}: {rendered}",
            parsed.reason
        );
        assert_eq!(parsed.sections[0].label.as_deref(), Some("a_[b].md"));
        assert_eq!(
            parsed.sections[1].entries[0].trailing_text.as_deref(),
            Some("fix [parser]")
        );
    }

    #[test]
    fn back_compat_single_legacy_and_mixed_cases() {
        let canonical = "- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n\n# Plan\n";
        assert_eq!(
            parse_sdd_artifact_link(canonical).kind,
            SddArtifactLinkKindWire::Canonical
        );

        let legacy = "---\nprompt: 202607/prompts/example.md\n---\n# Plan\n";
        assert_eq!(
            parse_sdd_artifact_link(legacy).kind,
            SddArtifactLinkKindWire::Legacy
        );

        let mixed = "---\nprompt: 202607/prompts/example.md\n---\n\n- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n\n# Plan\n";
        let parsed = parse_sdd_artifact_link(mixed);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Mixed);
        assert!(parsed.mixed_representations_agree());

        let contradictory = mixed.replace("prompt:", "plan:");
        assert_eq!(
            parse_sdd_artifact_link(&contradictory).kind,
            SddArtifactLinkKindWire::Invalid
        );
    }

    #[test]
    fn rejects_malformed_duplicate_unknown_and_unterminated_documents() {
        for document in [
            "- **PLAN:** [label](target.md\n\nBody\n",
            "- **PLAN:** [one](one.md)\n- **PLAN:** [two](two.md)\n",
            "- **PROMPT:** [one](one.md)\n- **UNKNOWN:** [two](two.md)\n",
            "---\ntier: tale\n- **PROMPT:** [one](one.md)\n",
        ] {
            assert_eq!(
                parse_sdd_plan_header_block(document).kind,
                SddArtifactLinkKindWire::Invalid,
                "{document:?}"
            );
        }
    }

    #[test]
    fn ordinary_bold_and_nested_body_bullets_are_not_header_sections() {
        let document = "- **PROMPT:** [prompt.md](prompt.md)\n\n# Plan\n\n- **Goal:** Keep this body bullet.\n  - nested body item\n";
        let parsed = parse_sdd_plan_header_block(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert_eq!(
            parsed.body,
            "# Plan\n\n- **Goal:** Keep this body bullet.\n  - nested body item\n"
        );

        let body_only = "# Plan\n\n- **Goal:** Keep this body bullet.\n";
        assert_eq!(
            parse_sdd_plan_header_block(body_only).kind,
            SddArtifactLinkKindWire::Missing
        );
    }

    #[test]
    fn known_header_labels_after_body_content_are_ordinary_bullets() {
        let document = "- **PLAN:** [plan.md](plan.md)\n\n# Plan\n\n- **Artifacts:** Files used by the plan.\n- **Beads:** Work tracked by the plan.\n- **Commits:** Changes made for the plan.\n";
        let parsed = parse_sdd_plan_header_block(document);

        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert!(parsed.canonical_layout);
        assert_eq!(
            parsed.body,
            "# Plan\n\n- **Artifacts:** Files used by the plan.\n- **Beads:** Work tracked by the plan.\n- **Commits:** Changes made for the plan.\n"
        );
    }

    #[test]
    fn fenced_header_example_is_not_a_live_document_header() {
        let body_only = "# Plan\n\n```markdown\n- **PROMPT:** [example](example.md)\n- **AGENTS:**\n  - example.agent\n```\n";
        let parsed = parse_sdd_plan_header_block(body_only);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Missing);
        assert_eq!(parsed.body, body_only);

        let document = "- **PROMPT:** [prompt](prompt.md)\n\n# Plan\n\n```markdown\n- **PROMPT:** [example](example.md)\n```\n";
        let parsed = parse_sdd_plan_header_block(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert!(parsed.canonical_layout);
        assert_eq!(
            parsed.body,
            "# Plan\n\n```markdown\n- **PROMPT:** [example](example.md)\n```\n"
        );
    }

    #[test]
    fn discontiguous_header_groups_before_body_content_are_invalid() {
        let document = "- **PLAN:** [plan.md](plan.md)\n\n- **ARTIFACTS:**\n  - artifact.txt\n\n# Plan\n";
        let parsed = parse_sdd_plan_header_block(document);

        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Invalid);
        assert_eq!(
            parsed.reason.as_deref(),
            Some("discontiguous or nested plan header bullets found")
        );
    }

    #[test]
    fn extra_leading_blank_line_is_recognized_as_noncanonical_layout() {
        let document = "\n- **PROMPT:** [prompt](prompt.md)\n\n# Plan\n";
        let parsed = parse_sdd_plan_header_block(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Canonical);
        assert!(!parsed.canonical_layout);
    }

    #[test]
    fn title_before_header_means_the_bullet_is_body_content() {
        let document = "# Plan\n\n- **PROMPT:** [prompt](prompt.md)\n";
        let parsed = parse_sdd_plan_header_block(document);
        assert_eq!(parsed.kind, SddArtifactLinkKindWire::Missing);
        assert!(!parsed.canonical_layout);
        assert_eq!(parsed.body, document);
    }

    #[test]
    fn legacy_upsert_preserves_frontmatter_body_and_other_sections() {
        let document = "---\ntier: tale\nprompt: old/path.md\ngoal: Keep me\n---\n\n- **AGENTS:**\n  - old.agent\n\n# Plan\n\nBody.\n";
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
            "---\ntier: tale\ngoal: Keep me\n---\n\n- **PROMPT:** [202607/prompts/example.md](prompts/example.md)\n- **AGENTS:**\n  - old.agent\n\n# Plan\n\nBody.\n"
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
    fn parent_upsert_removes_only_legacy_top_level_parent() {
        let document = "---\ntier: tale\nparent: old/epic.md\nmetadata:\n  parent: keep-nested\n---\n\n- **PARENT:** [202607/epic.md](epic.md)\n\n# Plan\n";
        let section = link_section(
            SddPlanHeaderSectionKindWire::Parent,
            "202607/epic.md",
            "epic.md",
        );
        let updated =
            upsert_sdd_plan_header_section(document, section, true, false)
                .unwrap();
        assert_eq!(
            updated,
            "---\ntier: tale\nmetadata:\n  parent: keep-nested\n---\n\n- **PARENT:** [202607/epic.md](epic.md)\n\n# Plan\n"
        );
        let parsed = parse_sdd_plan_header_block(&updated);
        assert_eq!(
            upsert_sdd_plan_header_section(
                &updated,
                parsed.sections[0].clone(),
                true,
                false,
            )
            .unwrap(),
            updated
        );
    }

    #[test]
    fn remove_section_keeps_the_rest_and_canonical_layout() {
        let document = "- **PROMPT:** [prompt](prompt.md)\n- **PARENT:** [parent](parent.md)\n\n# Plan\n";
        let updated =
            remove_sdd_plan_header_section(document, "PARENT", false, false)
                .unwrap();
        assert_eq!(updated, "- **PROMPT:** [prompt](prompt.md)\n\n# Plan\n");
    }
}
