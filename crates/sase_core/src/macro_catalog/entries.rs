use std::{
    collections::{BTreeMap, BTreeSet},
    env,
};

use crate::{
    content_layout::MEMORY_NAMESPACE_SEGMENT,
    snippet_catalog::{compose_snippet_catalog, is_valid_snippet_trigger},
    EditorSnippetCatalogRequestWire, EditorSnippetCatalogResponseWire,
    EditorSnippetCatalogStatsWire, EditorSnippetEntryWire,
    EditorXpromptCatalogRequestWire, EditorXpromptCatalogResponseWire,
    MobileHelperProjectContextWire, MobileHelperProjectScopeWire,
    MobileHelperResultWire, MobileHelperSkippedWire, MobileHelperStatusWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire,
};

use super::definition::filter_structured_sources;
use super::loader::CatalogLoader;
use super::types::*;
pub fn load_editor_xprompt_catalog(
    request: &EditorXpromptCatalogRequestWire,
    options: &XpromptCatalogLoadOptions,
) -> Result<EditorXpromptCatalogResponseWire, XpromptCatalogLoadError> {
    let root_dir = options.root_dir.clone().or_else(|| env::current_dir().ok());
    let mut options = options.clone();
    options.root_dir = root_dir;
    let loader = CatalogLoader::new(&options);
    let canonical_project =
        loader.canonical_project(request.project.as_deref());
    let entries = filter_structured_sources(
        loader.gather_structured_sources(canonical_project.as_deref())?,
        request,
        canonical_project.as_deref(),
    );
    let total_count = entries.len() as u64;
    let limited = request
        .limit
        .map(|limit| entries.iter().take(limit as usize).collect::<Vec<_>>())
        .unwrap_or_else(|| entries.iter().collect());
    let wire_entries = limited
        .into_iter()
        .map(|entry| structured_entry(entry, &loader))
        .collect::<Vec<_>>();

    Ok(EditorXpromptCatalogResponseWire {
        schema_version: SCHEMA_VERSION,
        result: MobileHelperResultWire {
            status: MobileHelperStatusWire::Success,
            message: Some(format!("loaded {} xprompt(s)", wire_entries.len())),
            warnings: loader.placement_warnings(),
            skipped: Vec::<MobileHelperSkippedWire>::new(),
            partial_failure_count: None,
        },
        context: MobileHelperProjectContextWire {
            project: request.project.clone(),
            scope: if request.project.is_some() {
                MobileHelperProjectScopeWire::Explicit
            } else {
                MobileHelperProjectScopeWire::AllKnown
            },
        },
        stats: MobileXpromptCatalogStatsWire {
            total_count,
            project_count: entries
                .iter()
                .filter_map(|entry| entry.project.as_deref())
                .collect::<BTreeSet<_>>()
                .len() as u64,
            skill_count: entries.iter().filter(|entry| entry.is_skill).count()
                as u64,
            memory_count: entries
                .iter()
                .filter(|entry| entry.memory_type.is_some())
                .count() as u64,
            pdf_requested: request.include_pdf,
        },
        entries: wire_entries,
        catalog_attachment: None,
    })
}

pub fn load_editor_snippet_catalog(
    request: &EditorSnippetCatalogRequestWire,
    options: &XpromptCatalogLoadOptions,
) -> Result<EditorSnippetCatalogResponseWire, XpromptCatalogLoadError> {
    let root_dir = options.root_dir.clone().or_else(|| env::current_dir().ok());
    let mut options = options.clone();
    options.root_dir = root_dir;
    let loader = CatalogLoader::new(&options);
    let mut entries_by_trigger =
        BTreeMap::<String, EditorSnippetEntryWire>::new();

    for xprompt in loader
        .load_all_xprompts(request.project.as_deref())?
        .values()
    {
        let Some(entry) = snippet_entry_from_xprompt(xprompt) else {
            continue;
        };
        entries_by_trigger
            .entry(entry.trigger.clone())
            .or_insert(entry);
    }

    for (trigger, template) in loader.load_user_snippets()? {
        if !is_valid_snippet_trigger(&trigger) {
            continue;
        }
        entries_by_trigger.insert(
            trigger.clone(),
            EditorSnippetEntryWire {
                trigger,
                template,
                source: "user_config".to_string(),
                xprompt_name: None,
                description: None,
                source_path_display: Some("ace.snippets".to_string()),
            },
        );
    }

    let raw_templates = entries_by_trigger
        .iter()
        .map(|(trigger, entry)| (trigger.clone(), entry.template.clone()))
        .collect::<BTreeMap<_, _>>();
    let composed = compose_snippet_catalog(&raw_templates);
    for (alias, source) in &composed.alias_provenance {
        if let Some(source_entry) = entries_by_trigger.get(source).cloned() {
            let mut alias_entry = source_entry;
            alias_entry.trigger = alias.clone();
            entries_by_trigger.insert(alias.clone(), alias_entry);
        }
    }
    for (trigger, template) in composed.templates {
        if let Some(entry) = entries_by_trigger.get_mut(&trigger) {
            entry.template = template;
        }
    }

    let entries = entries_by_trigger.into_values().collect::<Vec<_>>();
    Ok(EditorSnippetCatalogResponseWire {
        schema_version: SCHEMA_VERSION,
        result: MobileHelperResultWire {
            status: MobileHelperStatusWire::Success,
            message: Some(format!("loaded {} snippet(s)", entries.len())),
            warnings: Vec::new(),
            skipped: Vec::<MobileHelperSkippedWire>::new(),
            partial_failure_count: None,
        },
        context: MobileHelperProjectContextWire {
            project: request.project.clone(),
            scope: if request.project.is_some() {
                MobileHelperProjectScopeWire::Explicit
            } else {
                MobileHelperProjectScopeWire::AllKnown
            },
        },
        stats: EditorSnippetCatalogStatsWire {
            total_count: entries.len() as u64,
        },
        entries,
    })
}

pub(super) fn structured_entry(
    entry: &StructuredSource,
    loader: &CatalogLoader,
) -> MobileXpromptCatalogEntryWire {
    let kind = workflow_kind(&entry.workflow);
    let reference_prefix = workflow_reference_prefix(&entry.workflow);
    MobileXpromptCatalogEntryWire {
        name: entry.name.clone(),
        display_label: display_label(&entry.name),
        insertion: Some(format!("{reference_prefix}{}", entry.name)),
        reference_prefix: Some(reference_prefix.to_string()),
        kind: Some(entry_kind_value(entry, kind).to_string()),
        description: entry.description.clone(),
        source_bucket: entry.bucket.clone(),
        project: entry.project.clone(),
        tags: entry.workflow.tags.iter().cloned().collect(),
        input_signature: format_inputs(&entry.workflow.inputs),
        inputs: structured_inputs(&entry.workflow.inputs),
        is_skill: entry.is_skill,
        skill_name: entry.skill_name.clone(),
        memory_type: entry.memory_type,
        content_preview: content_preview(&entry.content),
        source_path_display: loader.source_path_display(entry),
        definition_path: loader.definition_path(entry),
        definition_range: loader.definition_range(entry),
    }
}

fn structured_inputs(inputs: &[CatalogInput]) -> Vec<MobileXpromptInputWire> {
    inputs
        .iter()
        .filter(|input| !input.is_step_input)
        .enumerate()
        .map(|(position, input)| MobileXpromptInputWire {
            name: input.name.clone(),
            r#type: input.type_name.clone(),
            description: input.description.clone(),
            required: input.required,
            default_display: input.default_display.clone(),
            position: position as u32,
            repeatable: input.repeatable,
            choices: input.choices.clone(),
        })
        .collect()
}

fn format_inputs(inputs: &[CatalogInput]) -> Option<String> {
    let rows = inputs
        .iter()
        .filter(|input| !input.is_step_input)
        .map(|input| {
            let optional = if input.required { "" } else { "?" };
            let repeatable = if input.repeatable { "…" } else { "" };
            format!("{}{repeatable}{optional}: {}", input.name, input.type_name)
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        None
    } else {
        Some(format!("({})", rows.join(", ")))
    }
}

fn display_label(name: &str) -> String {
    let label = name.replace(['_', '-'], " ").trim().to_string();
    if label.is_empty() {
        name.to_string()
    } else {
        label
    }
}

fn content_preview(content: &str) -> Option<String> {
    let text = content.trim();
    if text.is_empty() {
        return None;
    }
    let mut iter = text.chars();
    let preview = iter
        .by_ref()
        .take(MAX_CONTENT_PREVIEW_CHARS)
        .collect::<String>();
    if iter.next().is_some() {
        Some(format!("{}...", preview.trim_end()))
    } else {
        Some(preview)
    }
}

fn workflow_kind(workflow: &CatalogWorkflow) -> WorkflowKind {
    let prompt_part_count = workflow
        .steps
        .iter()
        .filter(|step| step.kind == StepKind::PromptPart)
        .count();
    if workflow.steps.len() == 1 && prompt_part_count == 1 {
        WorkflowKind::SimpleXprompt
    } else if prompt_part_count > 0 {
        WorkflowKind::EmbeddableWorkflow
    } else {
        WorkflowKind::StandaloneWorkflow
    }
}

/// User-facing kind for one catalog entry.
///
/// An xprompt memory renders as `memory` rather than as an ordinary xprompt;
/// `source_bucket` still carries provenance.
fn entry_kind_value(
    entry: &StructuredSource,
    kind: WorkflowKind,
) -> &'static str {
    if entry.memory_type.is_some() {
        return MEMORY_NAMESPACE_SEGMENT;
    }
    workflow_kind_value(kind)
}

fn workflow_kind_value(kind: WorkflowKind) -> &'static str {
    match kind {
        WorkflowKind::SimpleXprompt => "xprompt",
        WorkflowKind::EmbeddableWorkflow => "embeddable_workflow",
        WorkflowKind::StandaloneWorkflow => "standalone_workflow",
    }
}

fn workflow_reference_prefix(workflow: &CatalogWorkflow) -> &'static str {
    match workflow_kind(workflow) {
        WorkflowKind::StandaloneWorkflow => "#!",
        _ => "#",
    }
}

pub(super) fn workflow_prompt_part(workflow: &CatalogWorkflow) -> String {
    workflow
        .steps
        .iter()
        .find_map(|step| step.prompt_part.clone())
        .unwrap_or_default()
}

fn snippet_entry_from_xprompt(
    xprompt: &CatalogXprompt,
) -> Option<EditorSnippetEntryWire> {
    let snippet = xprompt.snippet.as_ref()?;
    let trigger = match snippet {
        CatalogSnippet::Enabled => xprompt
            .name
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(xprompt.name.as_str())
            .to_string(),
        CatalogSnippet::Trigger(trigger) => trigger.clone(),
    };
    if !is_valid_snippet_trigger(&trigger) {
        return None;
    }
    let template =
        xprompt_to_snippet_template(&xprompt.content, &xprompt.inputs)?;
    Some(EditorSnippetEntryWire {
        trigger,
        template,
        source: "xprompt".to_string(),
        xprompt_name: Some(xprompt.name.clone()),
        description: xprompt.description.clone(),
        source_path_display: xprompt.source_path.clone(),
    })
}

fn xprompt_to_snippet_template(
    content: &str,
    inputs: &[CatalogInput],
) -> Option<String> {
    if content.contains("{%") || content.contains("{#") {
        return None;
    }

    let mut tabstop = 1usize;
    let mut input_values = BTreeMap::<&str, String>::new();
    for input in inputs.iter().filter(|input| !input.is_step_input) {
        let value = if input.required {
            let value = format!("${tabstop}");
            tabstop += 1;
            value
        } else {
            input.default_snippet_value.clone().unwrap_or_default()
        };
        input_values.insert(input.name.as_str(), value);
    }

    let mut rendered = String::new();
    let mut rest = content;
    while let Some(start) = rest.find("{{") {
        rendered.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let end = after_start.find("}}")?;
        let expr = after_start[..end].trim();
        if expr.is_empty() {
            return None;
        }
        let value = input_values.get(expr)?;
        rendered.push_str(value);
        rest = &after_start[end + 2..];
    }
    rendered.push_str(rest);

    Some(format!("{}$0", replace_legacy_placeholders(&rendered)))
}

fn replace_legacy_placeholders(content: &str) -> String {
    let mut rendered = String::new();
    let mut rest = content;
    while let Some(start) = rest.find('{') {
        rendered.push_str(&rest[..start]);
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('}') else {
            rendered.push_str(&rest[start..]);
            return rendered;
        };
        let placeholder = &after_start[..end];
        if let Some(replacement) = legacy_placeholder_replacement(placeholder) {
            rendered.push_str(&replacement);
        } else {
            rendered.push('{');
            rendered.push_str(placeholder);
            rendered.push('}');
        }
        rest = &after_start[end + 1..];
    }
    rendered.push_str(rest);
    rendered
}

fn legacy_placeholder_replacement(placeholder: &str) -> Option<String> {
    let (number, default) = placeholder
        .split_once(':')
        .map(|(number, default)| (number, Some(default)))
        .unwrap_or((placeholder, None));
    if number.is_empty() || !number.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    Some(
        default
            .map(str::to_string)
            .unwrap_or_else(|| format!("${number}")),
    )
}
