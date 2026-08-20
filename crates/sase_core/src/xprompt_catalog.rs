use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_yaml::Value;
use thiserror::Error;

use crate::{
    content_layout::{
        memory_note_issue, memory_reference_name,
        reserved_memory_namespace_issue, resolve_layout_candidates,
        sase_content_layout, skill_placement_issue, skill_reference_name,
        CompatibleLayoutPathWire, MemorySourceWire, MemoryTierWire,
        MemoryXpromptIssueWire, SkillPlacementIssueWire, SkillSourceWire,
        XpromptSourceWire, MEMORY_NAMESPACE_SEGMENT, MEMORY_README_FILENAME,
        SKILL_DIRECTORY_SEGMENT,
    },
    list_project_records,
    snippet_catalog::{compose_snippet_catalog, is_valid_snippet_trigger},
    DocumentSnapshot, EditorRange, EditorSnippetCatalogRequestWire,
    EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
    EditorSnippetEntryWire, EditorXpromptCatalogRequestWire,
    EditorXpromptCatalogResponseWire, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperSkippedWire, MobileHelperStatusWire, MobileInputChoiceWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire,
};

const MAX_CONTENT_PREVIEW_CHARS: usize = 500;
const SCHEMA_VERSION: u32 = 1;
const SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV: &str = "SASE_XPROMPT_PLUGIN_DIRS_JSON";
const SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON";
const SASE_SKILL_PLUGIN_DIRS_JSON_ENV: &str = "SASE_SKILL_PLUGIN_DIRS_JSON";

/// The packaged Jinja frame that generated `SKILL.md` files are rendered
/// through. It ships beside the bundled skill sources but is a template, not a
/// skill, so scanning must skip it rather than report it as misplaced.
const SKILL_FRAME_TEMPLATE_FILENAME: &str = "SKILL.frame.template.md";

#[derive(Debug, Error)]
pub enum XpromptCatalogLoadError {
    #[error("failed to read xprompt catalog: {0}")]
    Read(String),
    #[error("xprompt catalog layout collision: {0}")]
    LayoutCollision(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XpromptCatalogLoadOptions {
    pub root_dir: Option<PathBuf>,
}

impl XpromptCatalogLoadOptions {
    pub fn new(root_dir: Option<PathBuf>) -> Self {
        Self { root_dir }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogInput {
    name: String,
    type_name: String,
    description: Option<String>,
    required: bool,
    default_display: Option<String>,
    default_snippet_value: Option<String>,
    is_step_input: bool,
    repeatable: bool,
    choices: Vec<MobileInputChoiceWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepKind {
    Agent,
    Bash,
    Python,
    PromptPart,
    Parallel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogStep {
    name: String,
    kind: StepKind,
    prompt_part: Option<String>,
    has_output: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogWorkflow {
    name: String,
    inputs: Vec<CatalogInput>,
    steps: Vec<CatalogStep>,
    local_xprompts: Vec<CatalogXprompt>,
    source_path: Option<String>,
    tags: BTreeSet<String>,
    description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogXprompt {
    name: String,
    content: String,
    inputs: Vec<CatalogInput>,
    local_xprompts: Vec<CatalogXprompt>,
    source_path: Option<String>,
    tags: BTreeSet<String>,
    description: Option<String>,
    is_skill: bool,
    skill_name: Option<String>,
    /// Tier of the SASE memory note this entry was loaded from. A non-null
    /// value is the authoritative marker that the entry is an xprompt memory.
    memory_type: Option<MemoryTierWire>,
    snippet: Option<CatalogSnippet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogSnippet {
    Enabled,
    Trigger(String),
}

#[derive(Debug, Clone, Deserialize)]
struct PluginPathEntry {
    module: String,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StructuredSource {
    name: String,
    workflow: CatalogWorkflow,
    bucket: String,
    project: Option<String>,
    description: Option<String>,
    is_skill: bool,
    skill_name: Option<String>,
    memory_type: Option<MemoryTierWire>,
    content: String,
    definition_section: DefinitionSection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkflowKind {
    SimpleXprompt,
    EmbeddableWorkflow,
    StandaloneWorkflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefinitionSection {
    Xprompts,
    Workflows,
}

impl DefinitionSection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Xprompts => "xprompts",
            Self::Workflows => "workflows",
        }
    }
}

pub fn load_editor_xprompt_catalog(
    request: &EditorXpromptCatalogRequestWire,
    options: &XpromptCatalogLoadOptions,
) -> Result<EditorXpromptCatalogResponseWire, XpromptCatalogLoadError> {
    let root_dir = options.root_dir.clone().or_else(|| env::current_dir().ok());
    let loader = CatalogLoader::new(root_dir);
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
    let loader = CatalogLoader::new(root_dir);
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

fn filter_structured_sources(
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

fn structured_entry(
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

fn workflow_prompt_part(workflow: &CatalogWorkflow) -> String {
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

#[derive(Debug, Clone, Default)]
struct CatalogLoader {
    root_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    package_xprompts_dir: Option<PathBuf>,
    package_skills_dir: Option<PathBuf>,
    default_xprompts_dir: Option<PathBuf>,
    default_config_path: Option<PathBuf>,
    plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    plugin_skill_dirs: BTreeMap<String, PathBuf>,
    plugin_config_paths: BTreeMap<String, PathBuf>,
    known_workspaces: BTreeMap<String, PathBuf>,
    canonical_project_refs: BTreeMap<String, String>,
    /// Definitions dropped by the canonical skill placement rules, recorded so
    /// the catalog can name the offending source and its migration
    /// destination instead of silently losing it.
    skill_issues: RefCell<Vec<SkillPlacementIssueWire>>,
    /// Definitions dropped by the xprompt-memory rules: a reserved `memory/`
    /// reference claimed by an ordinary definition, an unreachable note stem,
    /// or a file in a memory root that is not a valid memory note.
    memory_issues: RefCell<Vec<MemoryXpromptIssueWire>>,
}

impl CatalogLoader {
    fn new(root_dir: Option<PathBuf>) -> Self {
        let home_dir = env::var_os("HOME").map(PathBuf::from);
        let package_root =
            env::var_os("SASE_XPROMPT_PACKAGE_DIR").map(PathBuf::from);
        let package_xprompts_dir = env_path("SASE_XPROMPT_BUILTIN_DIR")
            .or_else(|| {
                package_root.as_ref().map(|root| root.join("xprompts"))
            });
        let package_skills_dir =
            env_path("SASE_SKILL_BUILTIN_DIR").or_else(|| {
                package_root.as_ref().map(|root| {
                    root.join("xprompts").join(SKILL_DIRECTORY_SEGMENT)
                })
            });
        let default_xprompts_dir = env_path("SASE_XPROMPT_DEFAULT_DIR")
            .or_else(|| {
                package_root
                    .as_ref()
                    .map(|root| root.join("default_xprompts"))
            });
        let default_config_path =
            env_path("SASE_DEFAULT_CONFIG_PATH").or_else(|| {
                package_root
                    .as_ref()
                    .map(|root| root.join("default_config.yml"))
            });
        let plugin_xprompt_dirs =
            plugin_path_map_from_env(SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV);
        let plugin_skill_dirs =
            plugin_path_map_from_env(SASE_SKILL_PLUGIN_DIRS_JSON_ENV);
        let plugin_config_paths =
            plugin_path_map_from_env(SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV);
        let known_projects = known_projects(home_dir.as_deref());
        Self {
            root_dir,
            home_dir,
            package_xprompts_dir,
            package_skills_dir,
            default_xprompts_dir,
            default_config_path,
            plugin_xprompt_dirs,
            plugin_skill_dirs,
            plugin_config_paths,
            known_workspaces: known_projects.workspaces,
            canonical_project_refs: known_projects.canonical_refs,
            skill_issues: RefCell::new(Vec::new()),
            memory_issues: RefCell::new(Vec::new()),
        }
    }

    fn canonical_project(&self, project: Option<&str>) -> Option<String> {
        let project = project?.trim();
        if project.is_empty() {
            return None;
        }
        Some(
            self.canonical_project_refs
                .get(project)
                .cloned()
                .unwrap_or_else(|| project.to_string()),
        )
    }

    fn root_project(&self) -> Option<&str> {
        let root = self.root_dir.as_deref()?;
        self.known_workspaces
            .iter()
            .find_map(|(project, workspace)| {
                path_is_under(root, workspace).then_some(project.as_str())
            })
    }

    fn gather_structured_sources(
        &self,
        project: Option<&str>,
    ) -> Result<Vec<StructuredSource>, XpromptCatalogLoadError> {
        let effective_project = project.or_else(|| self.root_project());
        let workflows = self.load_all_workflows(effective_project)?;
        let workflow_names = workflows.keys().cloned().collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::<(String, String)>::new();
        let mut sources = Vec::new();

        for (name, workflow) in workflows {
            let source = workflow.source_path.clone().unwrap_or_default();
            if seen.insert((source, name.clone())) {
                let (bucket, source_project) =
                    self.classify_source(workflow.source_path.as_deref(), None);
                let content = workflow_prompt_part(&workflow);
                sources.push(StructuredSource {
                    name,
                    description: workflow.description.clone(),
                    workflow,
                    bucket,
                    project: source_project,
                    is_skill: false,
                    skill_name: None,
                    memory_type: None,
                    content,
                    definition_section: DefinitionSection::Workflows,
                });
            }
        }

        for (name, xprompt) in self.load_all_xprompts(effective_project)? {
            if workflow_names.contains(&name) {
                continue;
            }
            let source = xprompt.source_path.clone().unwrap_or_default();
            if !seen.insert((source, name.clone())) {
                continue;
            }
            let (bucket, source_project) =
                self.classify_source(xprompt.source_path.as_deref(), None);
            let workflow = xprompt_to_workflow(&xprompt);
            sources.push(StructuredSource {
                name,
                workflow,
                bucket,
                project: source_project,
                description: xprompt.description,
                is_skill: xprompt.is_skill,
                skill_name: xprompt.skill_name,
                memory_type: xprompt.memory_type,
                content: xprompt.content,
                definition_section: DefinitionSection::Xprompts,
            });
        }

        let project_workspaces = match project {
            Some(project) => self
                .known_workspaces
                .get_key_value(project)
                .into_iter()
                .collect::<Vec<_>>(),
            None => self.known_workspaces.iter().collect::<Vec<_>>(),
        };
        for (project_name, workspace) in project_workspaces {
            let mut project_xprompts =
                self.load_project_local_xprompts(project_name, workspace)?;
            project_xprompts.extend(
                self.load_project_file_xprompts(project_name, workspace)?,
            );
            for (name, xprompt) in project_xprompts {
                let source = xprompt.source_path.clone().unwrap_or_default();
                if !seen.insert((source, name.clone())) {
                    continue;
                }
                let workflow = xprompt_to_workflow(&xprompt);
                sources.push(StructuredSource {
                    name,
                    workflow,
                    bucket: "project".to_string(),
                    project: Some(project_name.clone()),
                    description: xprompt.description,
                    is_skill: xprompt.is_skill,
                    skill_name: xprompt.skill_name,
                    memory_type: xprompt.memory_type,
                    content: xprompt.content,
                    definition_section: DefinitionSection::Xprompts,
                });
            }
        }

        sources.sort_by(|a, b| {
            (
                a.bucket.as_str(),
                a.project.as_deref().unwrap_or(""),
                a.name.as_str(),
            )
                .cmp(&(
                    b.bucket.as_str(),
                    b.project.as_deref().unwrap_or(""),
                    b.name.as_str(),
                ))
        });
        Ok(sources)
    }

    fn load_all_xprompts(
        &self,
        project: Option<&str>,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut all = BTreeMap::new();
        if let Some(dir) = &self.package_xprompts_dir {
            all.extend(self.load_xprompts_from_dir(dir, None, false)?);
        }
        if let Some(dir) = &self.default_xprompts_dir {
            all.extend(self.load_xprompts_from_dir(dir, None, false)?);
        }
        all.extend(self.load_plugin_xprompts()?);
        all.extend(self.load_config_xprompts(project)?);
        for source in self
            .xprompt_directory_sources(self.root_dir.as_deref(), project)
            .into_iter()
            .rev()
        {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            all.extend(self.load_xprompts_from_dir(
                path,
                project,
                source.project_namespaced,
            )?);
        }

        // Skills live in their own `skill/` reference namespace, so they can
        // never shadow (or be shadowed by) an ordinary xprompt of the same
        // bare name. Lowest priority first, so the canonical directory
        // sources win.
        if let Some(dir) = &self.package_skills_dir {
            all.extend(self.load_skills_from_dir(dir, None, false)?);
        }
        all.extend(self.load_plugin_skills()?);
        for source in self
            .skill_directory_sources(self.root_dir.as_deref(), project)
            .into_iter()
            .rev()
        {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            all.extend(self.load_skills_from_dir(
                path,
                project,
                source.project_namespaced,
            )?);
        }

        // Xprompt memories own the reserved `memory/` namespace, so they never
        // collide with an ordinary xprompt or a skill. Home first, so the
        // selected project's note shadows a same-stem home note.
        for source in self.memory_sources(project).into_iter().rev() {
            all.extend(self.load_memory_notes(&source)?);
        }

        Ok(all)
    }

    fn load_all_workflows(
        &self,
        project: Option<&str>,
    ) -> Result<BTreeMap<String, CatalogWorkflow>, XpromptCatalogLoadError>
    {
        let mut all = BTreeMap::new();
        if let Some(dir) = &self.package_xprompts_dir {
            all.extend(self.load_workflows_from_dir(dir, None, false)?);
        }
        all.extend(self.load_plugin_workflows()?);
        let sources =
            self.xprompt_directory_sources(self.root_dir.as_deref(), project);
        for scope in ["home_project", "home"] {
            for source in sources.iter().rev().filter(|s| s.scope == scope) {
                let Some(path) = source.path.as_deref().map(Path::new) else {
                    continue;
                };
                all.extend(self.load_workflows_from_dir(
                    path,
                    project,
                    source.project_namespaced,
                )?);
            }
        }
        if let Some(project) = project {
            if let Some(workspace) = self.known_workspaces.get(project) {
                all.extend(
                    self.load_project_file_workflows(project, workspace)?,
                );
            }
        }
        for source in sources.iter().rev().filter(|s| s.scope == "project") {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            all.extend(self.load_workflows_from_dir(
                path,
                project,
                source.project_namespaced,
            )?);
        }
        Ok(all)
    }

    fn xprompt_directory_sources(
        &self,
        project_root: Option<&Path>,
        project: Option<&str>,
    ) -> Vec<XpromptSourceWire> {
        let home_root =
            self.home_dir.as_deref().unwrap_or_else(|| Path::new(""));
        sase_content_layout(project_root, home_root, None, project)
            .xprompt_sources
            .into_iter()
            .filter(|source| {
                matches!(
                    source.scope.as_str(),
                    "project" | "home" | "home_project"
                ) && (self.home_dir.is_some()
                    || !matches!(
                        source.scope.as_str(),
                        "home" | "home_project"
                    ))
            })
            .collect()
    }

    fn skill_directory_sources(
        &self,
        project_root: Option<&Path>,
        project: Option<&str>,
    ) -> Vec<SkillSourceWire> {
        let home_root =
            self.home_dir.as_deref().unwrap_or_else(|| Path::new(""));
        sase_content_layout(project_root, home_root, None, project)
            .skill_sources
            .into_iter()
            .filter(|source| {
                matches!(
                    source.scope.as_str(),
                    "project" | "home" | "home_project"
                ) && (self.home_dir.is_some()
                    || !matches!(
                        source.scope.as_str(),
                        "home" | "home_project"
                    ))
            })
            .collect()
    }

    /// Ordered xprompt-memory sources for the selected project and home.
    ///
    /// The project scope follows the selection rather than the reference name:
    /// an explicitly requested registered project contributes its own
    /// workspace's memory, and no other project's memory is ever mixed in.
    fn memory_sources(&self, project: Option<&str>) -> Vec<MemorySourceWire> {
        let project_root = match project {
            Some(project) if self.root_project() != Some(project) => self
                .known_workspaces
                .get(project)
                .cloned()
                .or_else(|| self.root_dir.clone()),
            _ => self.root_dir.clone(),
        };
        let home_root =
            self.home_dir.as_deref().unwrap_or_else(|| Path::new(""));
        sase_content_layout(project_root.as_deref(), home_root, None, project)
            .memory_sources
            .into_iter()
            .filter(|source| source.scope != "home" || self.home_dir.is_some())
            .collect()
    }

    /// Load one scope's flat memory notes as no-argument xprompt memories.
    ///
    /// Split canonical/legacy memory state stays an error, `README.md` and
    /// nested assets are not catalog entries, and a file that is not a valid
    /// memory note becomes a diagnostic instead of an ordinary xprompt.
    fn load_memory_notes(
        &self,
        source: &MemorySourceWire,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let label = format!("{} memory", source.scope);
        let Some(root) = resolve_compatible_read_path(&source.paths, &label)?
        else {
            return Ok(BTreeMap::new());
        };
        let mut result = BTreeMap::new();
        for path in files_with_extensions(&root, &["md"])? {
            let Some(filename) =
                path.file_name().and_then(|name| name.to_str())
            else {
                continue;
            };
            if filename == MEMORY_README_FILENAME {
                continue;
            }
            let Some(note) = load_memory_note(&path)? else {
                continue;
            };
            let display = path.to_string_lossy();
            let issue = memory_note_issue(
                &display,
                &note.stem,
                note.declared_type.as_deref(),
            );
            if issue.is_some() {
                self.record_memory_issue(issue);
                continue;
            }
            let Some(memory_type) = note
                .declared_type
                .as_deref()
                .and_then(MemoryTierWire::parse)
            else {
                continue;
            };
            let name = memory_reference_name(&note.stem);
            result.insert(
                name.clone(),
                CatalogXprompt {
                    name,
                    content: note.body,
                    inputs: Vec::new(),
                    local_xprompts: Vec::new(),
                    source_path: Some(display.into_owned()),
                    tags: BTreeSet::new(),
                    description: note.description,
                    is_skill: false,
                    skill_name: None,
                    memory_type: Some(memory_type),
                    snippet: None,
                },
            );
        }
        Ok(result)
    }

    /// Canonical skill directory for the scope owning `dir`, used as the
    /// migration destination when a skill declaration turns up in an ordinary
    /// xprompt directory.
    fn skill_destination_for_xprompt_dir(&self, dir: &Path) -> Option<String> {
        let parent = dir.parent()?;
        Some(
            parent
                .join(SKILL_DIRECTORY_SEGMENT)
                .to_string_lossy()
                .into_owned(),
        )
    }

    fn record_skill_issue(&self, issue: Option<SkillPlacementIssueWire>) {
        if let Some(issue) = issue {
            self.skill_issues.borrow_mut().push(issue);
        }
    }

    fn record_memory_issue(&self, issue: Option<MemoryXpromptIssueWire>) {
        if let Some(issue) = issue {
            self.memory_issues.borrow_mut().push(issue);
        }
    }

    /// Drop an ordinary definition that claims a reserved `memory/` reference.
    ///
    /// Load order must never decide whether the colliding definition or the
    /// memory note wins, so the reserved namespace is enforced at every
    /// non-memory load site instead.
    fn reject_reserved_memory_name(&self, source: &str, name: &str) -> bool {
        let issue = reserved_memory_namespace_issue(source, name);
        let rejected = issue.is_some();
        self.record_memory_issue(issue);
        rejected
    }

    /// Migration diagnostics for every definition the placement rules dropped,
    /// so a misplaced source is reported rather than silently missing.
    fn placement_warnings(&self) -> Vec<String> {
        let mut warnings = self
            .skill_issues
            .borrow()
            .iter()
            .map(|issue| issue.message.clone())
            .chain(
                self.memory_issues
                    .borrow()
                    .iter()
                    .map(|issue| issue.message.clone()),
            )
            .collect::<Vec<_>>();
        warnings.sort();
        warnings.dedup();
        warnings
    }

    fn load_xprompts_from_dir(
        &self,
        dir: &Path,
        project: Option<&str>,
        namespace_local: bool,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        let skill_destination = self.skill_destination_for_xprompt_dir(dir);
        for path in files_with_extensions(dir, &["md"])? {
            let Some(mut xprompt) = load_xprompt_from_markdown(&path)? else {
                continue;
            };
            if xprompt.is_skill {
                self.record_skill_issue(skill_placement_issue(
                    &path.to_string_lossy(),
                    false,
                    true,
                    skill_destination.as_deref(),
                ));
                continue;
            }
            if self.reject_reserved_memory_name(
                &path.to_string_lossy(),
                &xprompt.name,
            ) {
                continue;
            }
            if namespace_local {
                if let Some(project) = project {
                    xprompt.name = format!("{project}/{}", xprompt.name);
                }
            }
            result.insert(xprompt.name.clone(), xprompt);
        }
        Ok(result)
    }

    /// Load one canonical skill directory.
    ///
    /// A definition here must declare a truthy `skill` value; everything else
    /// is rejected with a migration diagnostic rather than being loaded as an
    /// ordinary xprompt. Accepted definitions keep their declared name as the
    /// provider skill name and take the namespaced `skill/<name>` xprompt
    /// reference name.
    fn load_skills_from_dir(
        &self,
        dir: &Path,
        project: Option<&str>,
        namespace_local: bool,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        let destination = dir.parent().map(|parent| {
            parent.join("xprompts").to_string_lossy().into_owned()
        });
        for path in files_with_extensions(dir, &["md"])? {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == SKILL_FRAME_TEMPLATE_FILENAME)
            {
                continue;
            }
            let Some(mut xprompt) = load_xprompt_from_markdown(&path)? else {
                continue;
            };
            if !xprompt.is_skill {
                self.record_skill_issue(skill_placement_issue(
                    &path.to_string_lossy(),
                    true,
                    false,
                    destination.as_deref(),
                ));
                continue;
            }
            let namespace = namespace_local.then_some(project).flatten();
            xprompt.skill_name = Some(xprompt.name.clone());
            xprompt.name = skill_reference_name(namespace, &xprompt.name);
            result.insert(xprompt.name.clone(), xprompt);
        }
        Ok(result)
    }

    fn load_workflows_from_dir(
        &self,
        dir: &Path,
        project: Option<&str>,
        namespace_local: bool,
    ) -> Result<BTreeMap<String, CatalogWorkflow>, XpromptCatalogLoadError>
    {
        let mut result = BTreeMap::new();
        for path in files_with_extensions(dir, &["yml", "yaml"])? {
            let Some(mut workflow) = load_workflow_from_yaml_file(&path)?
            else {
                continue;
            };
            if namespace_local {
                if let Some(project) = project {
                    workflow.name = format!("{project}/{}", workflow.name);
                }
            }
            result.insert(workflow.name.clone(), workflow);
        }
        Ok(result)
    }

    fn load_plugin_xprompts(
        &self,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for (module, dir) in &self.plugin_xprompt_dirs {
            for path in files_with_extensions(dir, &["md"])? {
                let Some(mut xprompt) = load_xprompt_from_markdown(&path)?
                else {
                    continue;
                };
                let Some(filename) =
                    path.file_name().and_then(|name| name.to_str())
                else {
                    continue;
                };
                let source = format!("plugin:{module}/{filename}");
                if xprompt.is_skill {
                    self.record_skill_issue(skill_placement_issue(
                        &source,
                        false,
                        true,
                        Some("the plugin's skills/ resource directory"),
                    ));
                    continue;
                }
                if self.reject_reserved_memory_name(&source, &xprompt.name) {
                    continue;
                }
                xprompt.source_path = Some(source);
                result.insert(xprompt.name.clone(), xprompt);
            }
        }
        Ok(result)
    }

    /// Load skills from plugins' sibling `skills/` resource directories.
    fn load_plugin_skills(
        &self,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for (module, dir) in &self.plugin_skill_dirs {
            for path in files_with_extensions(dir, &["md"])? {
                let Some(mut xprompt) = load_xprompt_from_markdown(&path)?
                else {
                    continue;
                };
                let Some(filename) =
                    path.file_name().and_then(|name| name.to_str())
                else {
                    continue;
                };
                let source = format!("plugin:{module}/{filename}");
                if !xprompt.is_skill {
                    self.record_skill_issue(skill_placement_issue(
                        &source,
                        true,
                        false,
                        Some("the plugin's xprompts/ resource directory"),
                    ));
                    continue;
                }
                xprompt.source_path = Some(source);
                xprompt.skill_name = Some(xprompt.name.clone());
                xprompt.name = skill_reference_name(None, &xprompt.name);
                result.insert(xprompt.name.clone(), xprompt);
            }
        }
        Ok(result)
    }

    fn load_plugin_workflows(
        &self,
    ) -> Result<BTreeMap<String, CatalogWorkflow>, XpromptCatalogLoadError>
    {
        let mut result = BTreeMap::new();
        for (module, dir) in &self.plugin_xprompt_dirs {
            for path in files_with_extensions(dir, &["yml", "yaml"])? {
                let Some(mut workflow) = load_workflow_from_yaml_file(&path)?
                else {
                    continue;
                };
                let Some(filename) =
                    path.file_name().and_then(|name| name.to_str())
                else {
                    continue;
                };
                workflow.source_path =
                    Some(format!("plugin:{module}/{filename}"));
                result.insert(workflow.name.clone(), workflow);
            }
        }
        Ok(result)
    }

    /// Config-defined xprompts can never be skills: a skill must be a file in
    /// a canonical skill directory so it has a source to generate from.
    fn reject_config_skill(
        &self,
        xprompt: &CatalogXprompt,
        source: &str,
    ) -> bool {
        if !xprompt.is_skill {
            return false;
        }
        self.record_skill_issue(skill_placement_issue(
            &format!("{source} xprompt `{}`", xprompt.name),
            false,
            true,
            Some("a Markdown file in the scope's sase/skills/ directory"),
        ));
        true
    }

    fn load_config_xprompts(
        &self,
        project: Option<&str>,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for (source, path) in self.config_paths()? {
            let Some(data) = load_yaml_mapping(&path)? else {
                continue;
            };
            let Some(xprompts) = mapping_get(&data, "xprompts") else {
                continue;
            };
            let Some(mapping) = xprompts.as_mapping() else {
                continue;
            };
            for (name, value) in mapping {
                let Some(name) = value_as_string(name) else {
                    continue;
                };
                let Some(mut xprompt) =
                    xprompt_from_config_entry(&name, value, &source)
                else {
                    continue;
                };
                if self.reject_config_skill(&xprompt, &source) {
                    continue;
                }
                if self.reject_reserved_memory_name(
                    &format!("{source} xprompt `{}`", xprompt.name),
                    &xprompt.name,
                ) {
                    continue;
                }
                if source == "local_config" {
                    if let Some(project) = project {
                        xprompt.name = format!("{project}/{}", xprompt.name);
                    }
                }
                result.insert(xprompt.name.clone(), xprompt);
            }
        }
        Ok(result)
    }

    fn config_paths(
        &self,
    ) -> Result<Vec<(String, PathBuf)>, XpromptCatalogLoadError> {
        let mut paths = Vec::new();
        if let Some(path) = &self.default_config_path {
            paths.push(("default_config".to_string(), path.clone()));
        }
        for (module, path) in &self.plugin_config_paths {
            paths.push((format!("plugin_config:{module}"), path.clone()));
        }
        if let Some(home) = &self.home_dir {
            let config_dir = home.join(".config").join("sase");
            paths.push(("config".to_string(), config_dir.join("sase.yml")));
            if let Ok(entries) = fs::read_dir(&config_dir) {
                let mut overlays = entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .map(|name| {
                                name.starts_with("sase_")
                                    && matches!(
                                        path.extension()
                                            .and_then(|ext| ext.to_str()),
                                        Some("yml" | "yaml")
                                    )
                            })
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>();
                overlays.sort();
                for overlay in overlays {
                    let name = overlay
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("overlay")
                        .to_string();
                    paths.push((format!("config_overlay:{name}"), overlay));
                }
            }
        }
        if let Some(root) = &self.root_dir {
            if let Some(path) =
                self.project_config_read_path(root, "project config")?
            {
                paths.push(("local_config".to_string(), path));
            }
        }
        Ok(paths)
    }

    fn load_project_local_xprompts(
        &self,
        project: &str,
        workspace: &Path,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let source = format!("project_local_config:{project}");
        let Some(config_path) = self.project_config_read_path(
            workspace,
            &format!("project config for {project}"),
        )?
        else {
            return Ok(BTreeMap::new());
        };
        let Some(data) = load_yaml_mapping(&config_path)? else {
            return Ok(BTreeMap::new());
        };
        let Some(xprompts) = mapping_get(&data, "xprompts") else {
            return Ok(BTreeMap::new());
        };
        let Some(mapping) = xprompts.as_mapping() else {
            return Ok(BTreeMap::new());
        };
        let mut result = BTreeMap::new();
        for (name, value) in mapping {
            let Some(name) = value_as_string(name) else {
                continue;
            };
            let Some(mut xprompt) =
                xprompt_from_config_entry(&name, value, &source)
            else {
                continue;
            };
            if self.reject_config_skill(&xprompt, &source) {
                continue;
            }
            if self.reject_reserved_memory_name(
                &format!("{source} xprompt `{}`", xprompt.name),
                &xprompt.name,
            ) {
                continue;
            }
            xprompt.name = format!("{project}/{}", xprompt.name);
            result.insert(xprompt.name.clone(), xprompt);
        }
        Ok(result)
    }

    fn load_project_file_xprompts(
        &self,
        project: &str,
        workspace: &Path,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for source in self
            .xprompt_directory_sources(Some(workspace), Some(project))
            .into_iter()
            .rev()
            .filter(|source| source.scope == "project")
        {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            result.extend(self.load_xprompts_from_dir(
                path,
                Some(project),
                true,
            )?);
        }
        for source in self
            .skill_directory_sources(Some(workspace), Some(project))
            .into_iter()
            .rev()
            .filter(|source| source.scope == "project")
        {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            result.extend(self.load_skills_from_dir(
                path,
                Some(project),
                true,
            )?);
        }
        Ok(result)
    }

    fn load_project_file_workflows(
        &self,
        project: &str,
        workspace: &Path,
    ) -> Result<BTreeMap<String, CatalogWorkflow>, XpromptCatalogLoadError>
    {
        let mut result = BTreeMap::new();
        for source in self
            .xprompt_directory_sources(Some(workspace), Some(project))
            .into_iter()
            .rev()
            .filter(|source| source.scope == "project")
        {
            let Some(path) = source.path.as_deref().map(Path::new) else {
                continue;
            };
            result.extend(self.load_workflows_from_dir(
                path,
                Some(project),
                true,
            )?);
        }
        Ok(result)
    }

    fn load_user_snippets(
        &self,
    ) -> Result<BTreeMap<String, String>, XpromptCatalogLoadError> {
        let mut snippets = BTreeMap::new();
        for (_source, path) in self.config_paths()? {
            let Some(data) = load_yaml_mapping(&path)? else {
                continue;
            };
            let Some(ace) = mapping_get(&data, "ace") else {
                continue;
            };
            let Some(ace_mapping) = ace.as_mapping() else {
                continue;
            };
            let Some(raw_snippets) = mapping_get(ace_mapping, "snippets")
            else {
                continue;
            };
            let Some(snippet_mapping) = raw_snippets.as_mapping() else {
                continue;
            };
            for (trigger, template) in snippet_mapping {
                let (Some(trigger), Some(template)) =
                    (value_as_string(trigger), template.as_str())
                else {
                    continue;
                };
                snippets.insert(trigger, template.to_string());
            }
        }
        Ok(snippets)
    }

    fn project_config_read_path(
        &self,
        root: &Path,
        label: &str,
    ) -> Result<Option<PathBuf>, XpromptCatalogLoadError> {
        let home_root =
            self.home_dir.as_deref().unwrap_or_else(|| Path::new(""));
        let layout = sase_content_layout(Some(root), home_root, None, None);
        let config = layout
            .project
            .expect("explicit project root must produce a project layout")
            .config;
        resolve_compatible_read_path(&config, label)
    }

    fn classify_source(
        &self,
        source: Option<&str>,
        explicit_project: Option<&str>,
    ) -> (String, Option<String>) {
        let Some(source) = source else {
            return ("config".to_string(), None);
        };
        if source.starts_with("plugin:") || source.starts_with("plugin_config:")
        {
            return ("plugin".to_string(), None);
        }
        if source == "config" || source.starts_with("config:") {
            return ("config".to_string(), None);
        }
        if let Some(project) = explicit_project {
            return ("project".to_string(), Some(project.to_string()));
        }
        let path = PathBuf::from(source);
        if path.is_absolute() {
            for package_dir in self.package_dirs() {
                if path_is_under(&path, &package_dir) {
                    return ("built-in".to_string(), None);
                }
            }
            for (project, workspace) in &self.known_workspaces {
                if path_is_under(&path, workspace) {
                    return ("project".to_string(), Some(project.clone()));
                }
            }
            if let Some(home) = &self.home_dir {
                if path_is_under(&path, &home.join(".config").join("sase")) {
                    return ("config".to_string(), None);
                }
            }
        }
        ("config".to_string(), None)
    }

    fn source_path_display(&self, entry: &StructuredSource) -> Option<String> {
        let source = entry.workflow.source_path.as_deref()?;
        if source == "config"
            || source.starts_with("config:")
            || source.starts_with("plugin:")
            || source.starts_with("plugin_config:")
        {
            return Some(source.to_string());
        }
        let path = PathBuf::from(source);
        if !path.is_absolute() {
            return Some(source.to_string());
        }
        for (project, workspace) in &self.known_workspaces {
            if entry.project.as_ref().is_some_and(|p| p != project) {
                continue;
            }
            if let Some(rel) = relative_display(&path, workspace) {
                return Some(rel);
            }
        }
        for package_dir in self.package_dirs() {
            if let Some(rel) = relative_display(&path, &package_dir) {
                let name = package_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("xprompts");
                return Some(format!("{name}/{rel}"));
            }
        }
        if let Some(home) = &self.home_dir {
            let config_dir = home.join(".config").join("sase");
            if let Some(rel) = relative_display(&path, &config_dir) {
                return Some(format!("~/.config/sase/{rel}"));
            }
            if let Some(rel) = relative_display(&path, home) {
                return Some(format!("~/{rel}"));
            }
        }
        None
    }

    fn definition_path(&self, entry: &StructuredSource) -> Option<String> {
        let source = entry.workflow.source_path.as_deref()?;
        let path =
            self.source_definition_path(source, entry.project.as_deref())?;
        if !path.is_file() {
            return None;
        }
        path.canonicalize()
            .ok()
            .map(|path| path.to_string_lossy().into_owned())
    }

    fn definition_range(
        &self,
        entry: &StructuredSource,
    ) -> Option<EditorRange> {
        let source = entry.workflow.source_path.as_deref()?;
        if !source_supports_config_definition_range(source) {
            return None;
        }
        let path =
            self.source_definition_path(source, entry.project.as_deref())?;
        let text = fs::read_to_string(path).ok()?;
        for name in definition_key_candidates(&entry.name, source) {
            if let Some(range) = yaml_child_key_range(
                &text,
                entry.definition_section.as_str(),
                &name,
            ) {
                return Some(range);
            }
        }
        None
    }

    fn source_definition_path(
        &self,
        source: &str,
        project: Option<&str>,
    ) -> Option<PathBuf> {
        if let Some(rest) = source.strip_prefix("plugin:") {
            let (module, filename) = rest.split_once('/')?;
            return self
                .plugin_xprompt_dirs
                .get(module)
                .map(|dir| dir.join(filename));
        }
        if let Some(module) = source.strip_prefix("plugin_config:") {
            return self.plugin_config_paths.get(module).cloned();
        }
        if source.starts_with("config:") {
            return None;
        }
        if source == "default_config" {
            return self.default_config_path.clone();
        }
        if source == "local_config" {
            return self.root_dir.as_ref().and_then(|root| {
                self.project_config_read_path(root, "project config")
                    .ok()
                    .flatten()
            });
        }
        if let Some(project) = source.strip_prefix("project_local_config:") {
            return self.known_workspaces.get(project).and_then(|workspace| {
                self.project_config_read_path(
                    workspace,
                    &format!("project config for {project}"),
                )
                .ok()
                .flatten()
            });
        }
        if source == "config" {
            return self.home_dir.as_ref().map(|home| {
                home.join(".config").join("sase").join("sase.yml")
            });
        }
        if let Some(filename) = source.strip_prefix("config_overlay:") {
            return self
                .home_dir
                .as_ref()
                .map(|home| home.join(".config").join("sase").join(filename));
        }

        let path = PathBuf::from(source);
        if path.is_absolute() {
            return Some(path);
        }
        if let Some(project) = project {
            if let Some(workspace) = self.known_workspaces.get(project) {
                let project_path = workspace.join(&path);
                if project_path.is_file() {
                    return Some(project_path);
                }
            }
        }
        self.root_dir.as_ref().map(|root| root.join(path))
    }

    fn package_dirs(&self) -> Vec<PathBuf> {
        [
            self.package_xprompts_dir.clone(),
            self.package_skills_dir.clone(),
            self.default_xprompts_dir.clone(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

fn resolve_compatible_read_path(
    compatible: &CompatibleLayoutPathWire,
    label: &str,
) -> Result<Option<PathBuf>, XpromptCatalogLoadError> {
    let candidates = std::iter::once(&compatible.canonical)
        .chain(compatible.legacy.iter())
        .map(|entry| PathBuf::from(&entry.path))
        .collect::<Vec<_>>();
    let resolution = resolve_layout_candidates(
        compatible.read_policy,
        &candidates
            .iter()
            .map(|path| path.exists())
            .collect::<Vec<_>>(),
    );
    if resolution.collision {
        let rendered = resolution
            .existing_indices
            .iter()
            .map(|index| candidates[*index].to_string_lossy())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(XpromptCatalogLoadError::LayoutCollision(format!(
            "{label} exists in multiple canonical/legacy locations: {rendered}; migrate to the canonical path instead of merging split state"
        )));
    }
    Ok(resolution
        .selected_index
        .map(|index| candidates[index].clone()))
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}

fn plugin_path_map_from_env(name: &str) -> BTreeMap<String, PathBuf> {
    let Some(raw) = env::var_os(name) else {
        return BTreeMap::new();
    };
    let Some(raw) = raw.to_str() else {
        return BTreeMap::new();
    };
    let Ok(entries) = serde_json::from_str::<Vec<PluginPathEntry>>(raw) else {
        return BTreeMap::new();
    };
    entries
        .into_iter()
        .filter(|entry| !entry.module.is_empty())
        .map(|entry| (entry.module, entry.path))
        .collect()
}

#[derive(Debug, Default)]
struct KnownProjects {
    workspaces: BTreeMap<String, PathBuf>,
    canonical_refs: BTreeMap<String, String>,
}

fn known_projects(home: Option<&Path>) -> KnownProjects {
    let Some(home) = home else {
        return KnownProjects::default();
    };
    let projects_dir = home.join(".sase").join("projects");
    let include_states = vec!["enabled".to_string()];
    let Ok(records) =
        list_project_records(&projects_dir, &include_states, false, true)
    else {
        return KnownProjects::default();
    };

    let project_keys = records
        .iter()
        .map(|record| record.project_name.clone())
        .collect::<BTreeSet<_>>();
    let mut ref_targets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut result = KnownProjects::default();
    for record in records {
        let canonical = record
            .display_name
            .unwrap_or_else(|| record.project_name.clone());
        result
            .canonical_refs
            .insert(record.project_name.clone(), canonical.clone());
        ref_targets
            .entry(canonical.clone())
            .or_default()
            .insert(canonical.clone());
        for alias in record.aliases {
            ref_targets
                .entry(alias)
                .or_default()
                .insert(canonical.clone());
        }
        if let Some(workspace) = record.workspace_dir.map(PathBuf::from) {
            if workspace.is_dir() {
                result.workspaces.insert(canonical, workspace);
            }
        }
    }
    for (project_ref, targets) in ref_targets {
        if project_keys.contains(&project_ref) || targets.len() != 1 {
            continue;
        }
        result
            .canonical_refs
            .insert(project_ref, targets.into_iter().next().unwrap());
    }
    result
}

fn files_with_extensions(
    dir: &Path,
    extensions: &[&str],
) -> Result<Vec<PathBuf>, XpromptCatalogLoadError> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| extensions.contains(&ext))
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn load_xprompt_from_markdown(
    path: &Path,
) -> Result<Option<CatalogXprompt>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    let (front_matter, body) = parse_front_matter(&text);
    let name = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "name"))
        .and_then(value_as_string)
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        });
    let Some(name) = name else {
        return Ok(None);
    };
    let inputs = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "input"))
        .map(parse_inputs)
        .unwrap_or_default();
    let tags = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "tags"))
        .map(parse_tags)
        .unwrap_or_default();
    let description = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "description"))
        .and_then(value_as_string);
    let is_skill = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "skill"))
        .map(value_is_truthy)
        .unwrap_or(false);
    let snippet = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "snippet"))
        .and_then(parse_snippet);
    let source_path = path.to_string_lossy().into_owned();
    let local_xprompts = front_matter
        .as_ref()
        .map(|data| parse_local_xprompts(data, &source_path))
        .unwrap_or_default();
    Ok(Some(CatalogXprompt {
        name: name.clone(),
        content: body,
        inputs,
        local_xprompts,
        source_path: Some(source_path),
        tags,
        description,
        is_skill,
        skill_name: None,
        memory_type: None,
        snippet,
    }))
}

/// One file read from a memory root, before the xprompt-memory rules run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadedMemoryNote {
    stem: String,
    declared_type: Option<String>,
    description: Option<String>,
    body: String,
}

/// Read a memory note, stripping its frontmatter from the prompt body.
fn load_memory_note(
    path: &Path,
) -> Result<Option<LoadedMemoryNote>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(None);
    };
    let (front_matter, body) = parse_front_matter(&text);
    let declared_type = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "type"))
        .and_then(value_as_string);
    let description = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "description"))
        .and_then(value_as_string)
        .map(|description| {
            description.split_whitespace().collect::<Vec<_>>().join(" ")
        })
        .filter(|description| !description.is_empty());
    Ok(Some(LoadedMemoryNote {
        stem: stem.to_string(),
        declared_type,
        description,
        body,
    }))
}

fn parse_front_matter(text: &str) -> (Option<serde_yaml::Mapping>, String) {
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("---") {
        return (None, text.to_string());
    }
    let mut yaml_lines = Vec::new();
    let mut body_lines = Vec::new();
    let mut found_end = false;
    for line in lines.by_ref() {
        if line.trim() == "---" {
            found_end = true;
            break;
        }
        yaml_lines.push(line);
    }
    if !found_end {
        return (None, text.to_string());
    }
    body_lines.extend(lines);
    let front_matter = serde_yaml::from_str::<Value>(&yaml_lines.join("\n"))
        .ok()
        .and_then(|value| value.as_mapping().cloned())
        .unwrap_or_default();
    (Some(front_matter), body_lines.join("\n"))
}

fn load_yaml_mapping(
    path: &Path,
) -> Result<Option<serde_yaml::Mapping>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    Ok(serde_yaml::from_str::<Value>(&text)
        .ok()
        .and_then(|value| value.as_mapping().cloned()))
}

fn load_workflow_from_yaml_file(
    path: &Path,
) -> Result<Option<CatalogWorkflow>, XpromptCatalogLoadError> {
    let Some(mapping) = load_yaml_mapping(path)? else {
        return Ok(None);
    };
    let Some(name) = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
    else {
        return Ok(None);
    };
    let workflow =
        workflow_from_mapping(&name, &mapping, &path.to_string_lossy());
    if workflow.steps.is_empty() {
        Ok(None)
    } else {
        Ok(Some(workflow))
    }
}

fn parse_local_xprompts(
    data: &serde_yaml::Mapping,
    source_path: &str,
) -> Vec<CatalogXprompt> {
    mapping_get(data, "xprompts")
        .and_then(Value::as_mapping)
        .map(|xprompts| {
            xprompts
                .iter()
                .filter_map(|(name, value)| {
                    let name = value_as_string(name)?;
                    xprompt_from_config_entry(&name, value, source_path)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn workflow_from_mapping(
    name: &str,
    data: &serde_yaml::Mapping,
    source_path: &str,
) -> CatalogWorkflow {
    let tags = mapping_get(data, "tags")
        .map(parse_tags)
        .unwrap_or_default();
    let description =
        mapping_get(data, "description").and_then(value_as_string);
    let local_xprompts = parse_local_xprompts(data, source_path);
    let mut inputs = mapping_get(data, "input")
        .map(parse_inputs)
        .unwrap_or_default();
    let mut steps = Vec::new();
    if let Some(step_values) =
        mapping_get(data, "steps").and_then(Value::as_sequence)
    {
        for (idx, step_value) in step_values.iter().enumerate() {
            let Some(step_data) = step_value.as_mapping() else {
                continue;
            };
            if let Some(step) = parse_step(step_data, idx) {
                steps.push(step);
            }
        }
    }
    let explicit_input_names = inputs
        .iter()
        .map(|input| input.name.clone())
        .collect::<BTreeSet<_>>();
    for step in &steps {
        if step.has_output && !explicit_input_names.contains(&step.name) {
            inputs.push(CatalogInput {
                name: step.name.clone(),
                type_name: "line".to_string(),
                description: None,
                required: true,
                default_display: None,
                default_snippet_value: None,
                is_step_input: true,
                repeatable: false,
                choices: Vec::new(),
            });
        }
    }
    CatalogWorkflow {
        name: name.to_string(),
        inputs,
        steps,
        local_xprompts,
        source_path: Some(source_path.to_string()),
        tags,
        description,
    }
}

fn parse_step(data: &serde_yaml::Mapping, index: usize) -> Option<CatalogStep> {
    let name = mapping_get(data, "name")
        .and_then(value_as_string)
        .unwrap_or_else(|| format!("step_{index}"));
    let prompt_part =
        mapping_get(data, "prompt_part").and_then(value_as_string);
    let kind = if prompt_part.is_some() {
        StepKind::PromptPart
    } else if mapping_get(data, "agent").is_some()
        || mapping_get(data, "prompt").is_some()
    {
        StepKind::Agent
    } else if mapping_get(data, "bash").is_some() {
        StepKind::Bash
    } else if mapping_get(data, "python").is_some() {
        StepKind::Python
    } else if mapping_get(data, "parallel").is_some() {
        StepKind::Parallel
    } else {
        return None;
    };
    Some(CatalogStep {
        name,
        kind,
        prompt_part,
        has_output: mapping_get(data, "output").is_some(),
    })
}

fn xprompt_from_config_entry(
    name: &str,
    value: &Value,
    source_path: &str,
) -> Option<CatalogXprompt> {
    if let Some(content) = value.as_str() {
        return Some(CatalogXprompt {
            name: name.to_string(),
            content: content.to_string(),
            inputs: Vec::new(),
            local_xprompts: Vec::new(),
            source_path: Some(source_path.to_string()),
            tags: BTreeSet::new(),
            description: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
            snippet: None,
        });
    }
    let data = value.as_mapping()?;
    let content = mapping_get(data, "content").and_then(value_as_string)?;
    Some(CatalogXprompt {
        name: name.to_string(),
        content,
        inputs: mapping_get(data, "input")
            .map(parse_inputs)
            .unwrap_or_default(),
        local_xprompts: Vec::new(),
        source_path: Some(source_path.to_string()),
        tags: mapping_get(data, "tags")
            .map(parse_tags)
            .unwrap_or_default(),
        description: mapping_get(data, "description").and_then(value_as_string),
        is_skill: mapping_get(data, "skill")
            .map(value_is_truthy)
            .unwrap_or(false),
        skill_name: None,
        memory_type: None,
        snippet: mapping_get(data, "snippet").and_then(parse_snippet),
    })
}

fn xprompt_to_workflow(xprompt: &CatalogXprompt) -> CatalogWorkflow {
    CatalogWorkflow {
        name: xprompt.name.clone(),
        inputs: xprompt.inputs.clone(),
        steps: vec![CatalogStep {
            name: "main".to_string(),
            kind: StepKind::PromptPart,
            prompt_part: Some(xprompt.content.clone()),
            has_output: false,
        }],
        local_xprompts: xprompt.local_xprompts.clone(),
        source_path: xprompt.source_path.clone(),
        tags: xprompt.tags.clone(),
        description: xprompt.description.clone(),
    }
}

fn parse_inputs(value: &Value) -> Vec<CatalogInput> {
    if let Some(mapping) = value.as_mapping() {
        return mapping
            .iter()
            .filter_map(|(name, raw)| {
                let name = value_as_string(name)?;
                let (
                    type_name,
                    description,
                    required,
                    default_display,
                    default_snippet_value,
                ) = parse_short_input_value(raw);
                Some(CatalogInput {
                    name,
                    type_name,
                    description,
                    required,
                    default_display,
                    default_snippet_value,
                    is_step_input: false,
                    repeatable: repeatable_input_value(raw),
                    choices: short_input_choices(raw),
                })
            })
            .collect();
    }
    if let Some(sequence) = value.as_sequence() {
        return sequence
            .iter()
            .filter_map(|item| {
                let mapping = item.as_mapping()?;
                let name =
                    mapping_get(mapping, "name").and_then(value_as_string)?;
                let type_name = mapping_get(mapping, "type")
                    .and_then(value_as_string)
                    .map(|raw| parse_input_type(&raw))
                    .unwrap_or_else(|| "line".to_string());
                let default = mapping_get(mapping, "default");
                let description = mapping_get(mapping, "description")
                    .and_then(value_as_string);
                Some(CatalogInput {
                    name,
                    type_name,
                    description,
                    required: default.is_none(),
                    default_display: default.and_then(default_display),
                    default_snippet_value: default.map(snippet_default_value),
                    is_step_input: false,
                    repeatable: mapping_get(mapping, "repeatable")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    choices: mapping_get(mapping, "choices")
                        .map(parse_input_choices)
                        .unwrap_or_default(),
                })
            })
            .collect();
    }
    Vec::new()
}

fn parse_short_input_value(
    value: &Value,
) -> (String, Option<String>, bool, Option<String>, Option<String>) {
    if let Some(mapping) = value.as_mapping() {
        let type_name = mapping_get(mapping, "type")
            .and_then(value_as_string)
            .map(|raw| parse_input_type(&raw))
            .unwrap_or_else(|| "line".to_string());
        let default = mapping_get(mapping, "default");
        let description =
            mapping_get(mapping, "description").and_then(value_as_string);
        (
            type_name,
            description,
            default.is_none(),
            default.and_then(default_display),
            default.map(snippet_default_value),
        )
    } else {
        (
            parse_input_type(
                &value_as_string(value).unwrap_or_else(|| "line".to_string()),
            ),
            None,
            true,
            None,
            None,
        )
    }
}

fn parse_input_type(raw: &str) -> String {
    match raw.to_lowercase().as_str() {
        "word" => "word",
        "agent" => "agent",
        "text" => "text",
        "path" => "path",
        "int" | "integer" => "int",
        "bool" | "boolean" => "bool",
        "float" => "float",
        "enum" => "enum",
        _ => "line",
    }
    .to_string()
}

fn repeatable_input_value(value: &Value) -> bool {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "repeatable"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn short_input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "choices"))
        .map(parse_input_choices)
        .unwrap_or_default()
}

/// Parse a declared `choices` list, matching the shapes
/// `validate_input_choices` accepts: a scalar or a `{value, label}` mapping.
fn parse_input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    let Some(items) = value.as_sequence() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            if let Some(value) = value_as_string(item) {
                return Some(MobileInputChoiceWire { value, label: None });
            }
            let mapping = item.as_mapping()?;
            let value =
                mapping_get(mapping, "value").and_then(value_as_string)?;
            let label = mapping_get(mapping, "label").and_then(value_as_string);
            Some(MobileInputChoiceWire { value, label })
        })
        .collect()
}

fn default_display(value: &Value) -> Option<String> {
    if value.is_null() || value.as_str().is_some() {
        return None;
    }
    if let Some(value) = value.as_bool() {
        return Some(if value { "true" } else { "false" }.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_f64() {
        return Some(value.to_string());
    }
    None
}

fn snippet_default_value(value: &Value) -> String {
    if value.is_null() {
        return String::new();
    }
    value_as_string(value).unwrap_or_default()
}

fn parse_snippet(value: &Value) -> Option<CatalogSnippet> {
    if value.as_bool() == Some(true) {
        return Some(CatalogSnippet::Enabled);
    }
    value
        .as_str()
        .map(|trigger| CatalogSnippet::Trigger(trigger.to_string()))
}

fn parse_tags(value: &Value) -> BTreeSet<String> {
    if let Some(raw) = value.as_str() {
        return raw
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .map(str::to_string)
            .collect();
    }
    value
        .as_sequence()
        .map(|items| {
            items
                .iter()
                .filter_map(value_as_string)
                .map(|tag| tag.trim().to_string())
                .filter(|tag| !tag.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn value_is_truthy(value: &Value) -> bool {
    value.as_bool().unwrap_or_else(|| {
        value
            .as_sequence()
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    })
}

fn mapping_get<'a>(
    mapping: &'a serde_yaml::Mapping,
    key: &str,
) -> Option<&'a Value> {
    mapping.get(Value::String(key.to_string()))
}

fn value_as_string(value: &Value) -> Option<String> {
    if let Some(raw) = value.as_str() {
        Some(raw.to_string())
    } else if let Some(raw) = value.as_i64() {
        Some(raw.to_string())
    } else {
        value.as_bool().map(|raw| raw.to_string())
    }
}

fn path_is_under(path: &Path, base: &Path) -> bool {
    let Ok(path) = path.canonicalize() else {
        return false;
    };
    let Ok(base) = base.canonicalize() else {
        return false;
    };
    path.starts_with(base)
}

fn relative_display(path: &Path, base: &Path) -> Option<String> {
    let path = path.canonicalize().ok()?;
    let base = base.canonicalize().ok()?;
    path.strip_prefix(base)
        .ok()
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
}

fn source_supports_config_definition_range(source: &str) -> bool {
    matches!(source, "default_config" | "local_config" | "config")
        || source.starts_with("plugin_config:")
        || source.starts_with("config_overlay:")
        || source.starts_with("project_local_config:")
}

fn definition_key_candidates(name: &str, source: &str) -> Vec<String> {
    let mut candidates = vec![name.to_string()];
    if let Some(project) = source.strip_prefix("project_local_config:") {
        if let Some(rest) = name.strip_prefix(&format!("{project}/")) {
            candidates.push(rest.to_string());
        }
    }
    if matches!(source, "local_config")
        || source.starts_with("project_local_config:")
    {
        if let Some((_, rest)) = name.split_once('/') {
            candidates.push(rest.to_string());
        }
    }
    candidates.dedup();
    candidates
}

fn yaml_child_key_range(
    text: &str,
    section: &str,
    child_name: &str,
) -> Option<EditorRange> {
    let document = DocumentSnapshot::new(text);
    let mut section_indent = None;
    let mut child_indent = None;
    let mut line_start = 0usize;

    for raw_line in text.split_inclusive('\n') {
        let line = raw_line.trim_end_matches(['\r', '\n']);
        let parsed = parse_yaml_mapping_key(line);
        line_start += raw_line.len();

        let Some(parsed) = parsed else {
            continue;
        };
        if section_indent.is_none() {
            if parsed.indent == 0 && parsed.key == section {
                section_indent = Some(parsed.indent);
            }
            continue;
        }

        let section_indent = section_indent?;
        if parsed.indent <= section_indent {
            break;
        }
        let expected_child_indent = *child_indent.get_or_insert(parsed.indent);
        if parsed.indent != expected_child_indent {
            continue;
        }
        if parsed.key != child_name {
            continue;
        }

        let raw_line_start = line_start - raw_line.len();
        return document.byte_range_to_range(
            raw_line_start + parsed.key_start,
            raw_line_start + parsed.key_end,
        );
    }

    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedYamlKey {
    indent: usize,
    key: String,
    key_start: usize,
    key_end: usize,
}

fn parse_yaml_mapping_key(line: &str) -> Option<ParsedYamlKey> {
    let indent = line.bytes().take_while(|byte| *byte == b' ').count();
    let rest = &line[indent..];
    if rest.is_empty() || rest.starts_with('#') || rest.starts_with('-') {
        return None;
    }
    if rest.starts_with('"') || rest.starts_with('\'') {
        return parse_quoted_yaml_key(line, indent);
    }
    parse_unquoted_yaml_key(line, indent)
}

fn parse_unquoted_yaml_key(line: &str, indent: usize) -> Option<ParsedYamlKey> {
    let rest = &line[indent..];
    let colon = rest.find(':')?;
    let raw_key = &rest[..colon];
    let trimmed_end = raw_key.trim_end().len();
    let key = raw_key[..trimmed_end].trim();
    if key.is_empty() {
        return None;
    }
    let key_start = indent + raw_key[..trimmed_end].find(key)?;
    let key_end = key_start + key.len();
    Some(ParsedYamlKey {
        indent,
        key: key.to_string(),
        key_start,
        key_end,
    })
}

fn parse_quoted_yaml_key(line: &str, indent: usize) -> Option<ParsedYamlKey> {
    let quote = line[indent..].chars().next()?;
    let mut escaped = false;
    let mut key = String::new();
    let mut close_end = None;
    let content_start = indent + quote.len_utf8();
    for (offset, ch) in line[content_start..].char_indices() {
        let absolute = content_start + offset;
        if quote == '"' && escaped {
            key.push(ch);
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if quote == '\'' && ch == '\'' {
            let next = absolute + ch.len_utf8();
            if line[next..].starts_with('\'') {
                key.push('\'');
                close_end = None;
                continue;
            }
        }
        if ch == quote {
            close_end = Some(absolute + ch.len_utf8());
            break;
        }
        key.push(ch);
    }
    let close_end = close_end?;
    if !line[close_end..].trim_start().starts_with(':') {
        return None;
    }
    Some(ParsedYamlKey {
        indent,
        key,
        key_start: indent,
        key_end: close_end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> EditorXpromptCatalogRequestWire {
        EditorXpromptCatalogRequestWire {
            schema_version: 1,
            project: None,
            source: None,
            tag: None,
            query: None,
            include_pdf: false,
            limit: None,
            device_id: None,
        }
    }

    fn definition_line(entry: &MobileXpromptCatalogEntryWire) -> Option<u32> {
        entry.definition_range.map(|range| range.start.line)
    }

    #[test]
    fn yaml_child_key_range_finds_immediate_quoted_children() {
        let text = "xprompts:\n  parent:\n    child: nested\n  \"quoted/key\": body\nworkflows:\n  flow:\n    steps: []\n";

        let range =
            yaml_child_key_range(text, "xprompts", "quoted/key").unwrap();

        assert_eq!(range.start.line, 3);
        assert_eq!(range.start.character, 2);
        assert_eq!(yaml_child_key_range(text, "xprompts", "child"), None);
        assert_eq!(
            yaml_child_key_range(text, "workflows", "flow")
                .unwrap()
                .start
                .line,
            5
        );
    }

    #[test]
    fn loads_markdown_and_workflow_with_canonical_insertions() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        let skills = root.join("sase/skills");
        fs::create_dir_all(&xprompts).unwrap();
        fs::create_dir_all(&skills).unwrap();
        fs::write(
            skills.join("swarm.md"),
            "---\nname: swarm\ninput:\n  target: word\ntags: [mentor]\nskill: true\n---\nfirst\n---\nsecond",
        )
        .unwrap();
        fs::write(
            xprompts.join("ship.yml"),
            "input:\n  target: word\nsteps:\n  - name: run\n    agent: Ship {{ target }}\n",
        )
        .unwrap();

        let response = load_editor_xprompt_catalog(
            &request(),
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let by_name = response
            .entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        // A skill source keeps its `/swarm` provider name but is only
        // reachable inline through the namespaced `#skill/swarm`.
        assert!(!by_name.contains_key("swarm"));
        let swarm = by_name["skill/swarm"];
        assert_eq!(swarm.insertion.as_deref(), Some("#skill/swarm"));
        assert_eq!(swarm.reference_prefix.as_deref(), Some("#"));
        assert_eq!(swarm.kind.as_deref(), Some("xprompt"));
        assert!(swarm.is_skill);
        assert_eq!(swarm.skill_name.as_deref(), Some("swarm"));
        assert_eq!(swarm.input_signature.as_deref(), Some("(target: word)"));
        assert_eq!(by_name["ship"].insertion.as_deref(), Some("#!ship"));
        assert_eq!(
            by_name["ship"].kind.as_deref(),
            Some("standalone_workflow")
        );
    }

    fn write_memory_note(root: &Path, name: &str, contents: &str) {
        let memory = root.join("sase/memory");
        fs::create_dir_all(&memory).unwrap();
        fs::write(memory.join(name), contents).unwrap();
    }

    #[test]
    fn memory_notes_load_as_namespaced_no_argument_xprompt_memories() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("workspace");
        fs::create_dir_all(&home).unwrap();
        write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\nparent: AGENTS.md\ndescription: SASE terms\n---\nGlossary body\n",
        );
        write_memory_note(
            &root,
            "tui_perf.md",
            "---\ntype: long\ndescription: TUI performance\n---\nPerf body\n",
        );
        // Generated documentation and nested assets are not catalog entries.
        write_memory_note(&root, "README.md", "---\ntype: long\n---\nIndex\n");
        fs::create_dir_all(root.join("sase/memory/assets")).unwrap();
        fs::write(
            root.join("sase/memory/assets/nested.md"),
            "---\ntype: long\n---\nNested\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        assert_eq!(
            xprompts.keys().collect::<Vec<_>>(),
            vec!["memory/glossary", "memory/tui_perf"]
        );
        let glossary = &xprompts["memory/glossary"];
        // Frontmatter is stripped, description and tier are preserved, and an
        // xprompt memory takes no arguments.
        assert_eq!(glossary.content, "Glossary body");
        assert_eq!(glossary.description.as_deref(), Some("SASE terms"));
        assert_eq!(glossary.memory_type, Some(MemoryTierWire::Short));
        assert!(glossary.inputs.is_empty());
        assert!(!glossary.is_skill && glossary.skill_name.is_none());
        assert_eq!(
            xprompts["memory/tui_perf"].memory_type,
            Some(MemoryTierWire::Long)
        );
        // The `memory/` prefix is mandatory: there is no bare alias.
        assert!(!xprompts.contains_key("glossary"));
        assert!(loader.placement_warnings().is_empty());
    }

    #[test]
    fn memory_entries_render_as_memory_with_a_navigable_definition() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        write_memory_note(
            root,
            "glossary.md",
            "---\ntype: long\ndescription: SASE terms\n---\nGlossary body\n",
        );

        let response = load_editor_xprompt_catalog(
            &request(),
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let entry = response
            .entries
            .iter()
            .find(|entry| entry.name == "memory/glossary")
            .unwrap();

        assert_eq!(entry.kind.as_deref(), Some("memory"));
        assert_eq!(entry.memory_type, Some(MemoryTierWire::Long));
        assert_eq!(entry.insertion.as_deref(), Some("#memory/glossary"));
        assert_eq!(entry.input_signature, None);
        // A memory entry is never a slash skill.
        assert!(!entry.is_skill && entry.skill_name.is_none());
        // Definition navigation lands on the note itself.
        assert_eq!(
            entry.definition_path.as_deref().map(Path::new),
            Some(
                root.join("sase/memory/glossary.md")
                    .canonicalize()
                    .unwrap()
                    .as_path()
            )
        );
        // The stats projection counts every xprompt memory in the catalog,
        // including whatever the ambient home root contributes.
        assert_eq!(
            response.stats.memory_count,
            response
                .entries
                .iter()
                .filter(|entry| entry.memory_type.is_some())
                .count() as u64
        );
        assert!(response.stats.memory_count >= 1);
    }

    #[test]
    fn project_memory_shadows_home_memory_of_the_same_stem() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("workspace");
        write_memory_note(
            &home,
            "glossary.md",
            "---\ntype: short\n---\nHome\n",
        );
        write_memory_note(
            &home,
            "obsidian.md",
            "---\ntype: long\n---\nHome only\n",
        );
        write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\n---\nProject\n",
        );

        let loader = CatalogLoader {
            root_dir: Some(root),
            home_dir: Some(home),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        assert_eq!(xprompts["memory/glossary"].content, "Project");
        // Home still supplies notes the project does not define.
        assert_eq!(xprompts["memory/obsidian"].content, "Home only");
    }

    #[test]
    fn explicit_project_selection_picks_that_projects_memory_only() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("workspace");
        let other = temp.path().join("other");
        fs::create_dir_all(&home).unwrap();
        write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\n---\nRoot\n",
        );
        write_memory_note(
            &other,
            "glossary.md",
            "---\ntype: short\n---\nOther\n",
        );
        write_memory_note(
            &other,
            "only.md",
            "---\ntype: long\n---\nOther only\n",
        );

        let loader = CatalogLoader {
            root_dir: Some(root),
            home_dir: Some(home),
            known_workspaces: BTreeMap::from([(
                "other".to_string(),
                other.clone(),
            )]),
            ..CatalogLoader::default()
        };

        // Selecting a registered project changes which root supplies
        // `#memory/foo`; the reference name never gains a project prefix.
        let selected = loader.load_all_xprompts(Some("other")).unwrap();
        assert_eq!(selected["memory/glossary"].content, "Other");
        assert!(selected.contains_key("memory/only"));

        // The ambient catalog never mixes another project's memory in.
        let ambient = loader.load_all_xprompts(None).unwrap();
        assert_eq!(ambient["memory/glossary"].content, "Root");
        assert!(!ambient.contains_key("memory/only"));
    }

    #[test]
    fn split_canonical_and_legacy_memory_state_is_a_collision_error() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\n---\nBody\n",
        );
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::write(
            root.join("memory/glossary.md"),
            "---\ntype: short\n---\nLegacy\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root),
            ..CatalogLoader::default()
        };
        let error = loader.load_all_xprompts(None).unwrap_err();

        let XpromptCatalogLoadError::LayoutCollision(message) = error else {
            panic!("expected a memory layout collision");
        };
        assert!(message.contains("project memory"), "{message}");
        assert!(
            message.contains("migrate to the canonical path"),
            "{message}"
        );
    }

    #[test]
    fn invalid_memory_notes_become_diagnostics_instead_of_silent_gaps() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        write_memory_note(
            &root,
            "untyped.md",
            "---\nparent: AGENTS.md\n---\nBody",
        );
        write_memory_note(
            &root,
            "keyworded.md",
            "---\ntype: dynamic\n---\nBody",
        );
        write_memory_note(&root, "bad-stem.md", "---\ntype: long\n---\nBody");
        write_memory_note(&root, "ok.md", "---\ntype: long\n---\nBody");

        let loader = CatalogLoader {
            root_dir: Some(root),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        assert_eq!(xprompts.keys().collect::<Vec<_>>(), vec!["memory/ok"]);
        let warnings = loader.placement_warnings().join("\n");
        assert!(warnings.contains("untyped.md"), "{warnings}");
        assert!(warnings.contains("keyworded.md"), "{warnings}");
        assert!(warnings.contains("#memory/bad-stem"), "{warnings}");
    }

    #[test]
    fn ordinary_definitions_cannot_claim_the_reserved_memory_namespace() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        fs::create_dir_all(root.join("sase/xprompts")).unwrap();
        fs::write(
            root.join("sase/xprompts/imposter.md"),
            "---\nname: memory/glossary\n---\nNot a memory note",
        )
        .unwrap();
        fs::write(
            root.join("sase/sase.yml"),
            "xprompts:\n  memory/tui_perf:\n    content: Also not a memory note\n",
        )
        .unwrap();
        write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\n---\nReal\n",
        );

        let loader = CatalogLoader {
            root_dir: Some(root),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        // Load order never decides the winner: the colliding definitions are
        // rejected outright, and only the real memory note is reachable.
        assert_eq!(
            xprompts.keys().collect::<Vec<_>>(),
            vec!["memory/glossary"]
        );
        assert_eq!(xprompts["memory/glossary"].content, "Real");
        let warnings = loader.placement_warnings().join("\n");
        assert!(warnings.contains("imposter.md"), "{warnings}");
        assert!(warnings.contains("memory/tui_perf"), "{warnings}");
    }

    #[test]
    fn catalog_payloads_without_memory_fields_still_deserialize() {
        // Helper payloads written before xprompt memories existed omit the
        // additive fields entirely.
        let entry: MobileXpromptCatalogEntryWire = serde_json::from_str(
            r##"{"name":"foo","display_label":"foo","insertion":"#foo","reference_prefix":"#","kind":"xprompt","description":null,"source_bucket":"config","project":null,"tags":[],"input_signature":null,"is_skill":false,"content_preview":null,"source_path_display":null}"##,
        )
        .unwrap();
        assert_eq!(entry.memory_type, None);
        let stats: MobileXpromptCatalogStatsWire = serde_json::from_str(
            r#"{"total_count":1,"project_count":0,"skill_count":0,"pdf_requested":false}"#,
        )
        .unwrap();
        assert_eq!(stats.memory_count, 0);
        // A memory entry serializes its tier as the note's `type:` value.
        let rendered = serde_json::to_value(MobileXpromptCatalogEntryWire {
            memory_type: Some(MemoryTierWire::Long),
            ..entry
        })
        .unwrap();
        assert_eq!(rendered["memory_type"], "long");
    }

    #[test]
    fn rejects_misplaced_skill_definitions_in_both_directions() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("workspace");
        fs::create_dir_all(root.join("sase/xprompts")).unwrap();
        fs::create_dir_all(root.join("sase/skills")).unwrap();
        fs::create_dir_all(&home).unwrap();
        // A skill declaration in the ordinary xprompt directory.
        fs::write(
            root.join("sase/xprompts/stale_skill.md"),
            "---\nskill: true\n---\nOld location",
        )
        .unwrap();
        // An ordinary prompt parked in the canonical skill directory.
        fs::write(
            root.join("sase/skills/not_a_skill.md"),
            "---\ndescription: Plain prompt\n---\nBody",
        )
        .unwrap();
        // A config-defined skill, which no longer exists as a concept.
        fs::write(
            root.join("sase/sase.yml"),
            "xprompts:\n  config_skill:\n    content: Body\n    skill: true\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();
        assert!(xprompts.is_empty(), "{:?}", xprompts.keys());

        // Nothing is dropped silently: each rejection names the source and
        // the move it needs.
        let warnings = loader.placement_warnings().join("\n");
        assert!(warnings.contains("stale_skill.md"), "{warnings}");
        assert!(
            warnings
                .contains(root.join("sase/skills").to_string_lossy().as_ref()),
            "{warnings}"
        );
        assert!(warnings.contains("not_a_skill.md"), "{warnings}");
        assert!(
            warnings.contains(
                root.join("sase/xprompts").to_string_lossy().as_ref()
            ),
            "{warnings}"
        );
        assert!(warnings.contains("config_skill"), "{warnings}");
    }

    #[test]
    fn packaged_skill_frame_template_is_not_a_skill_source() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("package");
        let package_skills = package.join("xprompts/skills");
        fs::create_dir_all(&package_skills).unwrap();
        fs::write(
            package_skills.join("sase_plan.md"),
            "---\nskill: true\n---\nPlan body",
        )
        .unwrap();
        // The Jinja frame ships beside the sources and has no frontmatter, so
        // it must be skipped rather than reported as a misplaced definition.
        fs::write(
            package_skills.join(SKILL_FRAME_TEMPLATE_FILENAME),
            "{{ frontmatter }}\n\n{{ body }}\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            package_skills_dir: Some(package_skills),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        assert_eq!(
            xprompts.keys().collect::<Vec<_>>(),
            vec!["skill/sase_plan"]
        );
        assert!(
            loader.placement_warnings().is_empty(),
            "{:?}",
            loader.placement_warnings()
        );
    }

    #[test]
    fn packaged_skills_load_from_nested_xprompts_skills_only() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("package");
        let nested = package.join("xprompts/skills");
        let legacy = package.join("skills");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(&legacy).unwrap();
        fs::write(
            nested.join("sase_plan.md"),
            "---\nskill: true\n---\nPlan body",
        )
        .unwrap();
        fs::write(
            legacy.join("legacy_plan.md"),
            "---\nskill: true\n---\nLegacy body",
        )
        .unwrap();

        let loader = CatalogLoader {
            package_skills_dir: Some(package.join("xprompts/skills")),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(None).unwrap();

        assert!(xprompts.contains_key("skill/sase_plan"));
        assert!(!xprompts.contains_key("skill/legacy_plan"));
        assert!(!xprompts.contains_key("skills/sase_plan"));
    }

    #[test]
    fn home_skills_use_the_skill_namespace_and_project_qualified_form() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("workspace");
        fs::create_dir_all(home.join("sase/skills/app")).unwrap();
        fs::create_dir_all(&root).unwrap();
        fs::write(
            home.join("sase/skills/bob_query.md"),
            "---\nskill: true\n---\nQuery body",
        )
        .unwrap();
        fs::write(
            home.join("sase/skills/app/scoped.md"),
            "---\nskill: true\n---\nScoped body",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root),
            home_dir: Some(home),
            ..CatalogLoader::default()
        };
        let xprompts = loader.load_all_xprompts(Some("app")).unwrap();
        let names = xprompts.keys().cloned().collect::<Vec<_>>();
        assert_eq!(names, vec!["app/skill/scoped", "skill/bob_query"]);
        assert_eq!(
            xprompts["skill/bob_query"].skill_name.as_deref(),
            Some("bob_query")
        );
        assert_eq!(
            xprompts["app/skill/scoped"].skill_name.as_deref(),
            Some("scoped")
        );
        // The bare names never resolve after the cutover.
        assert!(!xprompts.contains_key("bob_query"));
        assert!(!xprompts.contains_key("app/scoped"));
    }

    #[test]
    fn projects_repeatable_agent_input_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("merge.yml"),
            "input:\n  names:\n    type: agent\n    default:\n    repeatable: true\nsteps:\n  - name: main\n    prompt_part: '{{ names }}'\n",
        )
        .unwrap();

        let response = load_editor_xprompt_catalog(
            &request(),
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let entry = response
            .entries
            .iter()
            .find(|entry| entry.name == "merge")
            .unwrap();

        assert_eq!(entry.input_signature.as_deref(), Some("(names…?: agent)"));
        assert_eq!(entry.inputs[0].r#type, "agent");
        assert!(entry.inputs[0].repeatable);
    }

    #[test]
    fn filters_step_inputs_and_formats_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("typed.yml"),
            "input:\n  required_word: word\n  string_default:\n    type: line\n    default: secret\n  null_default:\n    type: text\n    default:\n  count:\n    type: int\n    default: 3\n  enabled:\n    type: bool\n    default: false\nsteps:\n  - name: setup\n    bash: echo hi\n    output: {value: line}\n  - name: main\n    prompt_part: body\n",
        )
        .unwrap();

        let response = load_editor_xprompt_catalog(
            &request(),
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let entry = response
            .entries
            .iter()
            .find(|entry| entry.name == "typed")
            .unwrap();

        assert_eq!(
            entry.input_signature.as_deref(),
            Some(
                "(required_word: word, string_default?: line, null_default?: text, count?: int, enabled?: bool)"
            )
        );
        assert_eq!(
            entry
                .inputs
                .iter()
                .map(|input| (
                    input.name.as_str(),
                    input.r#type.as_str(),
                    input.required,
                    input.default_display.as_deref(),
                    input.position,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("required_word", "word", true, None, 0),
                ("string_default", "line", false, None, 1),
                ("null_default", "text", false, None, 2),
                ("count", "int", false, Some("3"), 3),
                ("enabled", "bool", false, Some("false"), 4),
            ]
        );
    }

    #[test]
    fn parses_xprompt_workflow_and_input_descriptions() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("long.md"),
            "---\ndescription: Long prompt\ninput:\n  - name: prompt\n    type: text\n    description: User request for the prompt.\n---\nBody {{ prompt }}",
        )
        .unwrap();
        fs::write(
            xprompts.join("nested.md"),
            "---\ninput:\n  target:\n    type: word\n    description: Target name to inspect.\n---\nTarget {{ target }}",
        )
        .unwrap();
        fs::write(
            xprompts.join("ship.yml"),
            "description: Ship workflow\ninput:\n  path:\n    type: path\n    description: Source path for workflow.\nxprompts:\n  _helper:\n    description: Local helper summary.\n    input:\n      topic:\n        type: word\n        description: Local topic description.\n    content: Helper {{ topic }}\nsteps:\n  - name: main\n    prompt_part: Ship {{ path }}\n",
        )
        .unwrap();

        let response = load_editor_xprompt_catalog(
            &request(),
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let by_name = response
            .entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_name["long"].description.as_deref(), Some("Long prompt"));
        assert_eq!(
            by_name["long"].inputs[0].description.as_deref(),
            Some("User request for the prompt.")
        );
        assert_eq!(
            by_name["nested"].inputs[0].description.as_deref(),
            Some("Target name to inspect.")
        );
        assert_eq!(
            by_name["ship"].description.as_deref(),
            Some("Ship workflow")
        );
        assert_eq!(
            by_name["ship"].inputs[0].description.as_deref(),
            Some("Source path for workflow.")
        );
        let mut filtered_request = request();
        filtered_request.query = Some("local helper summary".to_string());
        let filtered = load_editor_xprompt_catalog(
            &filtered_request,
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        assert_eq!(
            filtered
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["ship"]
        );
        filtered_request.query = Some("local topic description".to_string());
        let filtered = load_editor_xprompt_catalog(
            &filtered_request,
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        assert_eq!(
            filtered
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["ship"]
        );
    }

    #[test]
    fn parses_markdown_frontmatter_local_xprompts_without_global_entry() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("reads.md"),
            "---\ndescription: Read articles\nxprompts:\n  _article_search_agent:\n    description: Local article helper summary.\n    input:\n      topic:\n        type: word\n        description: Search topic description.\n    content: Search {{ topic }}\n---\n#_article_search_agent(news)\n",
        )
        .unwrap();

        let loader = CatalogLoader::new(Some(root.to_path_buf()));
        let loaded = loader
            .load_xprompts_from_dir(&xprompts, None, false)
            .unwrap();
        assert!(loaded.contains_key("reads"));
        assert!(!loaded.contains_key("_article_search_agent"));

        let workflow = xprompt_to_workflow(loaded.get("reads").unwrap());
        assert_eq!(workflow.local_xprompts.len(), 1);
        let helper = &workflow.local_xprompts[0];
        assert_eq!(helper.name, "_article_search_agent");
        assert_eq!(
            helper.description.as_deref(),
            Some("Local article helper summary.")
        );
        assert_eq!(helper.inputs[0].name, "topic");
        assert_eq!(
            helper.inputs[0].description.as_deref(),
            Some("Search topic description.")
        );

        let mut filtered_request = request();
        filtered_request.query =
            Some("local article helper summary".to_string());
        let filtered = load_editor_xprompt_catalog(
            &filtered_request,
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        assert_eq!(
            filtered
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["reads"]
        );
    }

    #[test]
    fn loads_native_snippet_catalog_with_user_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("review.md"),
            "---\nsnippet: true\ndescription: Review code\ninput:\n  language: word\n  focus:\n    type: line\n    default: correctness\n---\nReview this {{ language }} code for {{ focus }}.\nLegacy {2:done} {3}",
        )
        .unwrap();
        fs::write(
            xprompts.join("skip.md"),
            "---\nsnippet: bad-trigger!\n---\nBody",
        )
        .unwrap();
        fs::write(
            xprompts.join("capital.md"),
            "---\nsnippet: Review\ndescription: Explicit capitalized review\n---\nAuthored capital review",
        )
        .unwrap();
        fs::write(
            root.join("sase/sase.yml"),
            "ace:\n  snippets:\n    review: User review $0\n    plan: Plan $1$0\n",
        )
        .unwrap();

        let response = load_editor_snippet_catalog(
            &EditorSnippetCatalogRequestWire {
                schema_version: 1,
                project: None,
            },
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let by_trigger = response
            .entries
            .iter()
            .map(|entry| (entry.trigger.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert!(response.stats.total_count >= 2);
        assert_eq!(by_trigger["review"].source, "user_config");
        assert_eq!(by_trigger["review"].template, "User review $0");
        assert_eq!(by_trigger["Review"].source, "xprompt");
        assert_eq!(by_trigger["Review"].template, "Authored capital review$0");
        assert_eq!(
            by_trigger["Review"].description.as_deref(),
            Some("Explicit capitalized review")
        );
        assert_eq!(by_trigger["plan"].template, "Plan $1$0");
        assert_eq!(by_trigger["Plan"].template, "Plan $1$0");
        assert_eq!(by_trigger["Plan"].source, "user_config");
        assert!(!by_trigger.contains_key("bad-trigger!"));
    }

    #[test]
    fn converts_native_xprompt_snippet_templates() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("fix.md"),
            "---\nsnippet: fixit\ndescription: Fix a bug\ninput:\n  bug: word\n  area:\n    type: line\n    default: parser\n  empty:\n    type: line\n    default:\n---\nfix {{ bug }} in {{ area }}{{ empty }}. Then {2} or {3:done}.",
        )
        .unwrap();
        fs::write(
            xprompts.join("complex.md"),
            "---\nsnippet: true\n---\n{% if enabled %}skip{% endif %}",
        )
        .unwrap();

        let response = load_editor_snippet_catalog(
            &EditorSnippetCatalogRequestWire {
                schema_version: 1,
                project: None,
            },
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let by_trigger = response
            .entries
            .iter()
            .map(|entry| (entry.trigger.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            by_trigger["fixit"].template,
            "fix $1 in parser. Then $2 or done.$0"
        );
        assert_eq!(by_trigger["fixit"].xprompt_name.as_deref(), Some("fix"));
        assert_eq!(
            by_trigger["Fixit"].template,
            "Fix $1 in parser. Then $2 or done.$0"
        );
        assert_eq!(by_trigger["Fixit"].source, by_trigger["fixit"].source);
        assert_eq!(
            by_trigger["Fixit"].xprompt_name,
            by_trigger["fixit"].xprompt_name
        );
        assert_eq!(
            by_trigger["Fixit"].description,
            by_trigger["fixit"].description
        );
        assert_eq!(
            by_trigger["Fixit"].source_path_display,
            by_trigger["fixit"].source_path_display
        );
        assert_eq!(response.stats.total_count, response.entries.len() as u64);
        assert!(!by_trigger.contains_key("complex"));
    }

    #[test]
    fn native_snippet_catalog_resolves_references_after_user_merge() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("helper.md"),
            "---\nsnippet: true\ninput:\n  topic: word\n---\nHelp {{ topic }}",
        )
        .unwrap();
        fs::write(
            xprompts.join("outer.md"),
            "---\nsnippet: true\ninput:\n  topic: word\n---\n#[user_snip] {{ topic }}",
        )
        .unwrap();
        fs::write(
            root.join("sase/sase.yml"),
            "ace:\n  snippets:\n    user_snip: User $1$0\n    wrap: \"#[helper(World)] $1$0\"\n",
        )
        .unwrap();

        let response = load_editor_snippet_catalog(
            &EditorSnippetCatalogRequestWire {
                schema_version: 1,
                project: None,
            },
            &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
        )
        .unwrap();
        let by_trigger = response
            .entries
            .iter()
            .map(|entry| (entry.trigger.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_trigger["outer"].template, "User $1 $2$0");
        assert_eq!(by_trigger["wrap"].template, "Help World $1$0");
    }

    #[test]
    fn canonical_project_sources_win_with_legacy_read_compatibility() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        fs::create_dir_all(root.join("sase/xprompts")).unwrap();
        fs::create_dir_all(root.join(".xprompts")).unwrap();
        fs::create_dir_all(root.join("xprompts")).unwrap();

        fs::write(root.join("sase/xprompts/shared.md"), "Canonical body")
            .unwrap();
        fs::write(root.join(".xprompts/shared.md"), "Hidden legacy body")
            .unwrap();
        fs::write(root.join("xprompts/shared.md"), "Visible legacy body")
            .unwrap();
        fs::write(root.join(".xprompts/hidden_only.md"), "Hidden only")
            .unwrap();
        fs::write(root.join("xprompts/visible_only.md"), "Visible only")
            .unwrap();
        fs::write(
            root.join("sase/xprompts/flow.yml"),
            "steps:\n  - name: main\n    prompt_part: Canonical workflow\n",
        )
        .unwrap();
        fs::write(
            root.join(".xprompts/flow.yml"),
            "steps:\n  - name: main\n    prompt_part: Legacy workflow\n",
        )
        .unwrap();
        fs::write(
            root.join("sase.yml"),
            "xprompts:\n  legacy_config:\n    content: Legacy config body\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: None,
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::from([(
                "app".to_string(),
                root.clone(),
            )]),
            canonical_project_refs: BTreeMap::from([(
                "app".to_string(),
                "app".to_string(),
            )]),
            ..CatalogLoader::default()
        };

        let entries = loader.gather_structured_sources(Some("app")).unwrap();
        let by_name = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_name["app/shared"].content, "Canonical body");
        assert_eq!(by_name["app/hidden_only"].content, "Hidden only");
        assert_eq!(by_name["app/visible_only"].content, "Visible only");
        assert_eq!(
            workflow_prompt_part(&by_name["app/flow"].workflow),
            "Canonical workflow"
        );
        assert_eq!(
            workflow_prompt_part(&by_name["app/legacy_config"].workflow),
            "Legacy config body"
        );

        let shared = structured_entry(by_name["app/shared"], &loader);
        assert_eq!(
            shared.source_path_display.as_deref(),
            Some("sase/xprompts/shared.md")
        );
        assert_eq!(
            shared.definition_path.as_deref(),
            Some(
                root.join("sase/xprompts/shared.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
    }

    #[test]
    fn project_config_collision_reports_split_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        fs::create_dir_all(root.join("sase")).unwrap();
        fs::write(root.join("sase/sase.yml"), "xprompts: {}\n").unwrap();
        fs::write(root.join("sase.yml"), "xprompts: {}\n").unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root),
            home_dir: None,
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::new(),
            canonical_project_refs: BTreeMap::new(),
            ..CatalogLoader::default()
        };

        let error = loader.gather_structured_sources(None).unwrap_err();
        assert!(matches!(
            error,
            XpromptCatalogLoadError::LayoutCollision(message)
                if message.contains("multiple canonical/legacy")
                    && message.contains("sase/sase.yml")
                    && message.contains("sase.yml")
        ));
    }

    #[test]
    fn parity_fixture_covers_supported_catalog_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let home = temp.path().join("home");
        let package = temp.path().join("package");
        fs::create_dir_all(root.join("sase/xprompts")).unwrap();
        fs::create_dir_all(root.join("sase/skills")).unwrap();
        fs::create_dir_all(home.join("sase/xprompts/app")).unwrap();
        fs::create_dir_all(package.join("xprompts")).unwrap();
        fs::create_dir_all(package.join("xprompts/skills")).unwrap();
        fs::create_dir_all(package.join("default_xprompts")).unwrap();

        fs::write(
            package.join("xprompts/builtin.md"),
            "---\ntags: [mentor]\n---\nBuilt in",
        )
        .unwrap();
        fs::write(
            package.join("xprompts/skills/sase_plan.md"),
            "---\nskill: true\n---\nPlan skill",
        )
        .unwrap();
        fs::write(
            root.join("sase/skills/local_skill.md"),
            "---\nskill: [claude]\n---\nProject skill",
        )
        .unwrap();
        fs::write(
            package.join("default_xprompts/defaulted.md"),
            "---\ndescription: Default prompt\n---\nDefault body",
        )
        .unwrap();
        fs::write(
            package.join("default_config.yml"),
            "xprompts:\n  cfg:\n    content: Config body\n    input:\n      count:\n        type: int\n        default: 2\n",
        )
        .unwrap();
        fs::write(
            root.join("sase/xprompts/local.md"),
            "---\ninput: {target: word}\n---\nLocal body",
        )
        .unwrap();
        fs::write(root.join("sase/xprompts/swarm.md"), "one\n---\ntwo")
            .unwrap();
        fs::write(
            root.join("sase/xprompts/flow.yml"),
            "input: {target: word}\nsteps:\n  - name: run\n    agent: Run {{ target }}\n",
        )
        .unwrap();
        fs::write(
            home.join("sase/xprompts/app/project.md"),
            "---\ndescription: Project prompt\n---\nProject body",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home.clone()),
            package_xprompts_dir: Some(package.join("xprompts")),
            package_skills_dir: Some(package.join("xprompts/skills")),
            default_xprompts_dir: Some(package.join("default_xprompts")),
            default_config_path: Some(package.join("default_config.yml")),
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::from([(
                "app".to_string(),
                root.clone(),
            )]),
            canonical_project_refs: BTreeMap::from([(
                "app".to_string(),
                "app".to_string(),
            )]),
            ..CatalogLoader::default()
        };

        let entries = loader.gather_structured_sources(Some("app")).unwrap();
        let by_name = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_name["builtin"].bucket, "built-in");
        assert!(!by_name["builtin"].is_skill);
        assert_eq!(by_name["skill/sase_plan"].bucket, "built-in");
        assert!(by_name["skill/sase_plan"].is_skill);
        assert_eq!(
            by_name["skill/sase_plan"].skill_name.as_deref(),
            Some("sase_plan")
        );
        // A project skill is namespaced inside its existing project
        // namespace, and a provider list is just as truthy as `true`.
        let project_skill = by_name["app/skill/local_skill"];
        assert!(project_skill.is_skill);
        assert_eq!(project_skill.skill_name.as_deref(), Some("local_skill"));
        assert_eq!(project_skill.project.as_deref(), Some("app"));
        assert_eq!(by_name["defaulted"].bucket, "built-in");
        assert_eq!(by_name["cfg"].bucket, "config");
        assert_eq!(by_name["app/local"].project.as_deref(), Some("app"));
        assert_eq!(by_name["app/project"].bucket, "config");

        let wire_entries = entries
            .iter()
            .map(|entry| structured_entry(entry, &loader))
            .collect::<Vec<_>>();
        let wire_by_name = wire_entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            wire_by_name["app/flow"].kind.as_deref(),
            Some("standalone_workflow")
        );
        assert_eq!(
            wire_by_name["app/flow"].insertion.as_deref(),
            Some("#!app/flow")
        );
        assert_eq!(
            wire_by_name["app/swarm"].insertion.as_deref(),
            Some("#app/swarm")
        );
        assert_eq!(
            wire_by_name["cfg"].input_signature.as_deref(),
            Some("(count?: int)")
        );
        assert_eq!(
            wire_by_name["builtin"].definition_path.as_deref(),
            Some(
                package
                    .join("xprompts/builtin.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["skill/sase_plan"].insertion.as_deref(),
            Some("#skill/sase_plan")
        );
        assert_eq!(
            wire_by_name["app/skill/local_skill"].insertion.as_deref(),
            Some("#app/skill/local_skill")
        );
        assert_eq!(
            wire_by_name["skill/sase_plan"].definition_path.as_deref(),
            Some(
                package
                    .join("xprompts/skills/sase_plan.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["defaulted"].definition_path.as_deref(),
            Some(
                package
                    .join("default_xprompts/defaulted.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["cfg"].definition_path.as_deref(),
            Some(
                package
                    .join("default_config.yml")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(definition_line(wire_by_name["cfg"]), Some(1));
        assert_eq!(
            wire_by_name["app/local"].definition_path.as_deref(),
            Some(
                root.join("sase/xprompts/local.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["app/local"].source_path_display.as_deref(),
            Some("sase/xprompts/local.md")
        );
        assert_eq!(
            wire_by_name["app/project"].source_path_display.as_deref(),
            Some("~/sase/xprompts/app/project.md")
        );
    }

    #[test]
    fn loads_plugin_file_and_config_catalog_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let home = temp.path().join("home");
        let package = temp.path().join("package");
        let plugin_prompts = temp.path().join("plugin").join("xprompts");
        let plugin_config = temp.path().join("plugin_config");
        fs::create_dir_all(root.join("sase")).unwrap();
        fs::create_dir_all(home.join(".config/sase")).unwrap();
        fs::create_dir_all(package.join("xprompts")).unwrap();
        fs::create_dir_all(package.join("default_xprompts")).unwrap();
        fs::create_dir_all(&plugin_prompts).unwrap();
        fs::create_dir_all(&plugin_config).unwrap();

        fs::write(package.join("default_config.yml"), "xprompts: {}\n")
            .unwrap();
        fs::write(
            plugin_prompts.join("plug.md"),
            "---\nname: plug\ndescription: Plugin prompt\n---\nPlugin prompt body",
        )
        .unwrap();
        fs::write(
            plugin_prompts.join("gh.yml"),
            "steps:\n  - name: main\n    prompt_part: GitHub workflow body\n",
        )
        .unwrap();
        fs::write(
            plugin_config.join("default_config.yml"),
            "xprompts:\n  plug_cfg:\n    content: Plugin config body\nworkflows:\n  plug_flow:\n    steps:\n      - name: run\n        prompt_part: Plugin config workflow\n",
        )
        .unwrap();
        fs::write(
            root.join("sase/sase.yml"),
            "xprompts:\n  plug_cfg:\n    content: Local override body\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home),
            package_xprompts_dir: Some(package.join("xprompts")),
            default_xprompts_dir: Some(package.join("default_xprompts")),
            default_config_path: Some(package.join("default_config.yml")),
            plugin_xprompt_dirs: BTreeMap::from([(
                "fake_plugin.prompts".to_string(),
                plugin_prompts.clone(),
            )]),
            plugin_config_paths: BTreeMap::from([(
                "fake_plugin.config".to_string(),
                plugin_config.join("default_config.yml"),
            )]),
            known_workspaces: BTreeMap::new(),
            canonical_project_refs: BTreeMap::new(),
            ..CatalogLoader::default()
        };

        let entries = loader.gather_structured_sources(None).unwrap();
        let by_name = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_name["plug"].bucket, "plugin");
        assert_eq!(by_name["gh"].bucket, "plugin");
        assert!(
            !by_name.contains_key("plug_flow"),
            "config-defined workflows must not appear in the catalog"
        );
        assert_eq!(by_name["plug_cfg"].bucket, "config");
        assert_eq!(
            workflow_prompt_part(&by_name["plug_cfg"].workflow),
            "Local override body"
        );

        let wire_entries = entries
            .iter()
            .map(|entry| structured_entry(entry, &loader))
            .collect::<Vec<_>>();
        let wire_by_name = wire_entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            wire_by_name["plug"].source_path_display.as_deref(),
            Some("plugin:fake_plugin.prompts/plug.md")
        );
        assert_eq!(
            wire_by_name["plug"].definition_path.as_deref(),
            Some(
                plugin_prompts
                    .join("plug.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["plug_cfg"].definition_path.as_deref(),
            Some(
                root.join("sase/sase.yml")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(definition_line(wire_by_name["plug_cfg"]), Some(1));
    }

    #[test]
    fn config_workflows_are_ignored_but_file_backed_project_workflows_load() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let home = temp.path().join("home");
        let project_workspace = temp.path().join("project");
        fs::create_dir_all(root.join("sase")).unwrap();
        fs::create_dir_all(home.join(".config/sase")).unwrap();
        fs::create_dir_all(project_workspace.join("sase/xprompts")).unwrap();

        fs::write(
            root.join("sase/sase.yml"),
            "xprompts:\n  local_xp:\n    content: Local config xprompt body\nworkflows:\n  local_flow:\n    steps:\n      - name: run\n        prompt_part: Local config workflow body\n",
        )
        .unwrap();
        fs::write(
            home.join(".config/sase/sase.yml"),
            "xprompts:\n  user_xp:\n    content: User config xprompt body\nworkflows:\n  user_flow:\n    steps:\n      - name: run\n        prompt_part: User config workflow body\n",
        )
        .unwrap();
        fs::write(
            project_workspace.join("sase/xprompts/file_flow.yml"),
            "steps:\n  - name: run\n    prompt_part: File-backed workflow body\n",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home),
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::from([(
                "app".to_string(),
                project_workspace.clone(),
            )]),
            canonical_project_refs: BTreeMap::from([(
                "app".to_string(),
                "app".to_string(),
            )]),
            ..CatalogLoader::default()
        };

        let entries = loader.gather_structured_sources(Some("app")).unwrap();
        let by_name = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert!(by_name.contains_key("user_xp"));
        assert_eq!(by_name["user_xp"].bucket, "config");
        assert!(
            by_name.contains_key("app/local_xp"),
            "local_config xprompts still namespace under the active project"
        );
        assert!(
            !by_name.contains_key("user_flow"),
            "user config workflows must not appear"
        );
        assert!(
            !by_name.contains_key("local_flow"),
            "local config workflows must not appear"
        );
        assert!(
            !by_name.contains_key("app/local_flow"),
            "namespaced local config workflows must not appear"
        );
        assert!(
            by_name.contains_key("app/file_flow"),
            "file-backed workflows in known project workspaces still load"
        );
        assert_eq!(by_name["app/file_flow"].bucket, "project");
    }

    #[test]
    fn computes_known_project_local_config_definition_range() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("sase/xprompts")).unwrap();
        fs::write(
            workspace.join("sase/sase.yml"),
            "xprompts:\n  project_cfg:\n    content: Project body\n",
        )
        .unwrap();
        fs::write(
            workspace.join("sase/xprompts/project_file.md"),
            "Project file body",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: None,
            home_dir: None,
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::from([(
                "app".to_string(),
                workspace.clone(),
            )]),
            canonical_project_refs: BTreeMap::from([(
                "app".to_string(),
                "app".to_string(),
            )]),
            ..CatalogLoader::default()
        };

        let entries = loader.gather_structured_sources(None).unwrap();
        let wire_entries = entries
            .iter()
            .map(|entry| structured_entry(entry, &loader))
            .collect::<Vec<_>>();
        let entry = wire_entries
            .iter()
            .find(|entry| entry.name == "app/project_cfg")
            .unwrap();
        let file_entry = wire_entries
            .iter()
            .find(|entry| entry.name == "app/project_file")
            .unwrap();

        assert_eq!(
            entry.definition_path.as_deref(),
            Some(
                workspace
                    .join("sase/sase.yml")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(definition_line(entry), Some(1));
        assert_eq!(
            file_entry.source_path_display.as_deref(),
            Some("sase/xprompts/project_file.md")
        );
        assert_eq!(
            file_entry.definition_path.as_deref(),
            Some(
                workspace
                    .join("sase/xprompts/project_file.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
    }

    #[test]
    fn known_projects_use_display_names_aliases_and_gp_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let projects_dir = home.join(".sase").join("projects");

        let canonical_workspace = temp.path().join("canonical_ws");
        let canonical_project_dir = projects_dir.join("gh_org__proj");
        fs::create_dir_all(&canonical_workspace).unwrap();
        fs::create_dir_all(&canonical_project_dir).unwrap();
        fs::write(
            canonical_project_dir.join("gh_org__proj.sase"),
            format!(
                "PROJECT_NAME: proj\nPROJECT_ALIASES: p, project\nWORKSPACE_DIR: {}\n",
                canonical_workspace.display()
            ),
        )
        .unwrap();
        fs::write(
            canonical_project_dir.join("gh_org__proj.gp"),
            "WORKSPACE_DIR: /tmp/should-be-ignored\n",
        )
        .unwrap();

        let legacy_workspace = temp.path().join("legacy_ws");
        let legacy_project_dir = projects_dir.join("legacy");
        fs::create_dir_all(&legacy_workspace).unwrap();
        fs::create_dir_all(&legacy_project_dir).unwrap();
        fs::write(
            legacy_project_dir.join("legacy.gp"),
            format!("WORKSPACE_DIR: {}\n", legacy_workspace.display()),
        )
        .unwrap();

        let archived_project_dir = projects_dir.join("archived");
        fs::create_dir_all(&archived_project_dir).unwrap();
        fs::write(
            archived_project_dir.join("archived-archive.sase"),
            format!("WORKSPACE_DIR: {}\n", temp.path().display()),
        )
        .unwrap();

        let known = known_projects(Some(home.as_path()));

        assert_eq!(
            known.workspaces.get("proj").map(PathBuf::as_path),
            Some(canonical_workspace.as_path()),
        );
        assert_eq!(
            known.workspaces.get("legacy").map(PathBuf::as_path),
            Some(legacy_workspace.as_path()),
        );
        assert!(!known.workspaces.contains_key("gh_org__proj"));
        assert!(!known.workspaces.contains_key("archived"));
        for project_ref in ["gh_org__proj", "proj", "p", "project"] {
            assert_eq!(
                known.canonical_refs.get(project_ref).map(String::as_str),
                Some("proj"),
            );
        }
        assert_eq!(
            known.canonical_refs.get("legacy").map(String::as_str),
            Some("legacy"),
        );
    }

    #[test]
    fn project_catalog_uses_canonical_namespace_and_filter_refs() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let projects_dir = home.join(".sase").join("projects");
        let project_dir = projects_dir.join("gh_org__proj");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&project_dir).unwrap();
        fs::create_dir_all(workspace.join("sase/xprompts")).unwrap();
        fs::write(
            project_dir.join("gh_org__proj.sase"),
            format!(
                "PROJECT_NAME: proj\nPROJECT_ALIASES: shortcut\nWORKSPACE_DIR: {}\n",
                workspace.display()
            ),
        )
        .unwrap();
        fs::write(
            workspace.join("sase/xprompts/thing.md"),
            "Project prompt body",
        )
        .unwrap();

        let known = known_projects(Some(home.as_path()));
        let loader = CatalogLoader {
            root_dir: Some(workspace.clone()),
            home_dir: Some(home),
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: known.workspaces,
            canonical_project_refs: known.canonical_refs,
            ..CatalogLoader::default()
        };
        let entries = loader.gather_structured_sources(None).unwrap();

        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.name == "proj/thing")
                .count(),
            1,
        );
        let entry = entries
            .iter()
            .find(|entry| entry.name == "proj/thing")
            .unwrap();
        assert_eq!(entry.project.as_deref(), Some("proj"));
        assert!(entries
            .iter()
            .all(|entry| entry.name != "gh_org__proj/thing"));

        for project_ref in ["gh_org__proj", "proj", "shortcut"] {
            let mut filtered_request = request();
            filtered_request.project = Some(project_ref.to_string());
            let canonical =
                loader.canonical_project(filtered_request.project.as_deref());
            let filtered = filter_structured_sources(
                entries.clone(),
                &filtered_request,
                canonical.as_deref(),
            );
            assert_eq!(
                filtered
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["proj/thing"],
                "{project_ref}",
            );
        }
    }

    #[test]
    fn pseudo_sources_do_not_get_definition_paths() {
        let temp = tempfile::tempdir().unwrap();
        let loader = CatalogLoader {
            root_dir: Some(temp.path().to_path_buf()),
            home_dir: Some(temp.path().join("home")),
            package_xprompts_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::new(),
            canonical_project_refs: BTreeMap::new(),
            ..CatalogLoader::default()
        };
        let entry = StructuredSource {
            name: "plugin".to_string(),
            workflow: CatalogWorkflow {
                name: "plugin".to_string(),
                inputs: Vec::new(),
                steps: vec![CatalogStep {
                    name: "prompt".to_string(),
                    kind: StepKind::PromptPart,
                    prompt_part: Some("body".to_string()),
                    has_output: false,
                }],
                local_xprompts: Vec::new(),
                source_path: Some("plugin:module/plugin.md".to_string()),
                tags: BTreeSet::new(),
                description: None,
            },
            bucket: "plugin".to_string(),
            project: None,
            description: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
            content: "body".to_string(),
            definition_section: DefinitionSection::Xprompts,
        };

        assert_eq!(structured_entry(&entry, &loader).definition_path, None);
    }
}
