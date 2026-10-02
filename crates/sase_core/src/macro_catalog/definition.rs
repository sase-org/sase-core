use crate::{
    content_layout::{skill_reference_name, split_skill_reference_name},
    EditorXpromptCatalogRequestWire,
};

use super::loader::CatalogLoader;
use super::types::*;
pub fn resolve_xprompt_skill_definition(
    request: &XpromptSkillDefinitionRequestWire,
    options: &XpromptCatalogLoadOptions,
) -> XpromptSkillDefinitionResolutionWire {
    let authored_reference = request.reference.trim().to_string();
    let loader = CatalogLoader::new(options);
    let applicable_project = loader
        .canonical_project(request.project.as_deref())
        .or_else(|| loader.root_project().map(str::to_string));
    let parsed = match parse_skill_lookup_reference(&authored_reference) {
        Some(parsed) => parsed,
        None => {
            return skill_definition_resolution(
                "not_a_skill_candidate",
                authored_reference,
                None,
                None,
                None,
                None,
                Vec::new(),
                Some("not a supported skill reference".to_string()),
            );
        }
    };

    let sources = match parsed.project.as_deref() {
        Some(project) => loader.skill_lookup_sources(Some(project)),
        None => loader.skill_lookup_sources(applicable_project.as_deref()),
    };
    let sources = match sources {
        Ok(sources) => sources,
        Err(error) => {
            return skill_definition_resolution(
                "catalog_load_failure",
                authored_reference,
                parsed.canonical_reference,
                Some(parsed.skill_name),
                parsed.project,
                None,
                Vec::new(),
                Some(error.to_string()),
            );
        }
    };

    if parsed.slash {
        let mut candidates = sources
            .iter()
            .filter(|entry| {
                entry.is_skill
                    && entry.skill_name.as_deref()
                        == Some(parsed.skill_name.as_str())
            })
            .map(|entry| loader.skill_definition_candidate(entry))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.reference.cmp(&right.reference));
        candidates.dedup_by(|left, right| left.reference == right.reference);
        if candidates.len() > 1 {
            let refs = candidates
                .iter()
                .map(|candidate| format!("#{}", candidate.reference))
                .collect::<Vec<_>>()
                .join(", ");
            return skill_definition_resolution(
                "ambiguous",
                authored_reference,
                None,
                Some(parsed.skill_name.clone()),
                None,
                None,
                candidates,
                Some(format!(
                    "/{} matches multiple skill definitions: {refs}",
                    parsed.skill_name
                )),
            );
        }
        let Some(candidate) = candidates.into_iter().next() else {
            return skill_definition_resolution(
                "missing_skill",
                authored_reference,
                None,
                Some(parsed.skill_name.clone()),
                applicable_project,
                None,
                Vec::new(),
                Some(format!("skill /{} not found", parsed.skill_name)),
            );
        };
        return resolution_for_candidate(
            authored_reference,
            parsed.skill_name,
            candidate,
        );
    }

    let canonical_reference = parsed
        .canonical_reference
        .clone()
        .expect("explicit skill references always have a canonical reference");
    let candidate = sources
        .iter()
        .find(|entry| entry.is_skill && entry.name == canonical_reference)
        .map(|entry| loader.skill_definition_candidate(entry));
    let Some(candidate) = candidate else {
        return skill_definition_resolution(
            "missing_skill",
            authored_reference,
            Some(canonical_reference.clone()),
            Some(parsed.skill_name),
            parsed.project,
            None,
            Vec::new(),
            Some(format!("skill #{} not found", canonical_reference)),
        );
    };
    resolution_for_candidate(authored_reference, parsed.skill_name, candidate)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSkillLookupReference {
    pub(super) slash: bool,
    pub(super) canonical_reference: Option<String>,
    pub(super) project: Option<String>,
    pub(super) skill_name: String,
}

fn parse_skill_lookup_reference(
    raw: &str,
) -> Option<ParsedSkillLookupReference> {
    if let Some(skill_name) = slash_skill_name(raw) {
        return Some(ParsedSkillLookupReference {
            slash: true,
            canonical_reference: None,
            project: None,
            skill_name,
        });
    }
    let token = explicit_xprompt_reference_token(raw)?;
    let normalized = token.replace("__", "/");
    let (project, skill_name) = split_skill_reference_name(&normalized)?;
    let project = project.map(str::to_string);
    Some(ParsedSkillLookupReference {
        slash: false,
        canonical_reference: Some(skill_reference_name(
            project.as_deref(),
            skill_name,
        )),
        project,
        skill_name: skill_name.to_string(),
    })
}

fn explicit_xprompt_reference_token(raw: &str) -> Option<&str> {
    let rest = raw.trim().strip_prefix('#')?;
    let end = rest
        .char_indices()
        .find_map(|(offset, character)| {
            (character.is_whitespace()
                || matches!(
                    character,
                    '(' | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                        | '"'
                        | '\''
                        | '`'
                        | ','
                        | ';'
                        | ':'
                        | '!'
                        | '?'
                ))
            .then_some(offset)
        })
        .unwrap_or(rest.len());
    let token = &rest[..end];
    (!token.is_empty()).then_some(token)
}

fn slash_skill_name(raw: &str) -> Option<String> {
    let skill = raw.trim().strip_prefix('/')?;
    if skill.is_empty() || skill.contains('/') {
        return None;
    }
    if !skill
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return None;
    }
    Some(skill.to_string())
}

fn resolution_for_candidate(
    authored_reference: String,
    skill_name: String,
    candidate: XpromptSkillDefinitionCandidateWire,
) -> XpromptSkillDefinitionResolutionWire {
    let Some(definition_path) = candidate.definition_path.clone() else {
        return skill_definition_resolution(
            "missing_source",
            authored_reference,
            Some(candidate.reference.clone()),
            Some(skill_name),
            candidate.project.clone(),
            None,
            vec![candidate.clone()],
            Some(format!(
                "skill #{} has no local source file",
                candidate.reference
            )),
        );
    };
    skill_definition_resolution(
        "success",
        authored_reference,
        Some(candidate.reference.clone()),
        Some(skill_name),
        candidate.project.clone(),
        Some(definition_path),
        vec![candidate],
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn skill_definition_resolution(
    status: &str,
    authored_reference: String,
    canonical_reference: Option<String>,
    skill_name: Option<String>,
    project: Option<String>,
    definition_path: Option<String>,
    candidates: Vec<XpromptSkillDefinitionCandidateWire>,
    diagnostic: Option<String>,
) -> XpromptSkillDefinitionResolutionWire {
    XpromptSkillDefinitionResolutionWire {
        schema_version: XPROMPT_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        authored_reference,
        canonical_reference,
        skill_name,
        project,
        definition_path,
        candidates,
        diagnostic,
    }
}

pub(super) fn filter_structured_sources(
    entries: Vec<StructuredSource>,
    request: &EditorXpromptCatalogRequestWire,
    canonical_project: Option<&str>,
) -> Vec<StructuredSource> {
    let normalized_query =
        request.query.as_ref().map(|query| query.to_lowercase());
    entries
        .into_iter()
        .filter(|entry| {
            if let Some(project) = canonical_project {
                if matches!(entry.project.as_deref(), Some(p) if p != project) {
                    return false;
                }
            }
            if let Some(source) = request.source.as_deref() {
                if entry.bucket != source {
                    return false;
                }
            }
            if let Some(tag) = request.tag.as_deref() {
                if !entry.workflow.tags.contains(tag) {
                    return false;
                }
            }
            if let Some(query) = normalized_query.as_deref() {
                let input_descriptions = entry
                    .workflow
                    .inputs
                    .iter()
                    .filter_map(|input| input.description.as_deref())
                    .collect::<Vec<_>>()
                    .join("\n");
                let local_xprompt_text = entry
                    .workflow
                    .local_xprompts
                    .iter()
                    .flat_map(|xprompt| {
                        xprompt
                            .description
                            .iter()
                            .map(String::as_str)
                            .chain(std::iter::once(xprompt.content.as_str()))
                            .chain(xprompt.inputs.iter().filter_map(|input| {
                                input.description.as_deref()
                            }))
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let haystack = format!(
                    "{}\n{}\n{}\n{}\n{}\n{}",
                    entry.name,
                    entry.description.as_deref().unwrap_or_default(),
                    input_descriptions,
                    local_xprompt_text,
                    entry.content,
                    entry
                        .workflow
                        .tags
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" ")
                )
                .to_lowercase();
                if !haystack.contains(query) {
                    return false;
                }
            }
            true
        })
        .collect()
}
