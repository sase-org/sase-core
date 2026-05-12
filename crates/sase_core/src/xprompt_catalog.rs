use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_yaml::Value;
use thiserror::Error;

use crate::{
    DocumentSnapshot, EditorRange, EditorSnippetCatalogRequestWire,
    EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
    EditorSnippetEntryWire, EditorXpromptCatalogRequestWire,
    EditorXpromptCatalogResponseWire, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperSkippedWire, MobileHelperStatusWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire,
};

const MAX_CONTENT_PREVIEW_CHARS: usize = 500;
const SCHEMA_VERSION: u32 = 1;
const SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV: &str = "SASE_XPROMPT_PLUGIN_DIRS_JSON";
const SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON";

#[derive(Debug, Error)]
pub enum XpromptCatalogLoadError {
    #[error("failed to read xprompt catalog: {0}")]
    Read(String),
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
    required: bool,
    default_display: Option<String>,
    default_snippet_value: Option<String>,
    is_step_input: bool,
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
    source_path: Option<String>,
    tags: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogXprompt {
    name: String,
    content: String,
    inputs: Vec<CatalogInput>,
    source_path: Option<String>,
    tags: BTreeSet<String>,
    description: Option<String>,
    is_skill: bool,
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
    let entries = filter_structured_sources(
        loader.gather_structured_sources(request.project.as_deref())?,
        request,
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
        stats: MobileXpromptCatalogStatsWire {
            total_count,
            project_count: entries
                .iter()
                .filter_map(|entry| entry.project.as_deref())
                .collect::<BTreeSet<_>>()
                .len() as u64,
            skill_count: entries.iter().filter(|entry| entry.is_skill).count()
                as u64,
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
) -> Vec<StructuredSource> {
    let normalized_query =
        request.query.as_ref().map(|query| query.to_lowercase());
    entries
        .into_iter()
        .filter(|entry| {
            if let Some(project) = request.project.as_deref() {
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
                let haystack = format!(
                    "{}\n{}\n{}\n{}",
                    entry.name,
                    entry.description.as_deref().unwrap_or_default(),
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
        kind: Some(workflow_kind_value(kind).to_string()),
        description: entry.description.clone(),
        source_bucket: entry.bucket.clone(),
        project: entry.project.clone(),
        tags: entry.workflow.tags.iter().cloned().collect(),
        input_signature: format_inputs(&entry.workflow.inputs),
        inputs: structured_inputs(&entry.workflow.inputs),
        is_skill: entry.is_skill,
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
            required: input.required,
            default_display: input.default_display.clone(),
            position: position as u32,
        })
        .collect()
}

fn format_inputs(inputs: &[CatalogInput]) -> Option<String> {
    let rows = inputs
        .iter()
        .filter(|input| !input.is_step_input)
        .map(|input| {
            let optional = if input.required { "" } else { "?" };
            format!("{}{optional}: {}", input.name, input.type_name)
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
        WorkflowKind::SimpleXprompt
            if content_has_segment_separators(&workflow_prompt_part(
                workflow,
            )) =>
        {
            "#!"
        }
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

fn content_has_segment_separators(content: &str) -> bool {
    let mut fence: Option<&str> = None;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            fence = if fence == Some("```") {
                None
            } else {
                Some("```")
            };
            continue;
        }
        if trimmed.starts_with("~~~") {
            fence = if fence == Some("~~~") {
                None
            } else {
                Some("~~~")
            };
            continue;
        }
        if fence.is_none() && trimmed == "---" {
            return true;
        }
    }
    false
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

fn is_valid_snippet_trigger(trigger: &str) -> bool {
    !trigger.is_empty()
        && trigger
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
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

#[derive(Debug, Clone)]
struct CatalogLoader {
    root_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    package_xprompts_dir: Option<PathBuf>,
    default_xprompts_dir: Option<PathBuf>,
    default_config_path: Option<PathBuf>,
    plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    plugin_config_paths: BTreeMap<String, PathBuf>,
    known_workspaces: BTreeMap<String, PathBuf>,
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
        let plugin_config_paths =
            plugin_path_map_from_env(SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV);
        let known_workspaces = known_project_workspaces(home_dir.as_deref());
        Self {
            root_dir,
            home_dir,
            package_xprompts_dir,
            default_xprompts_dir,
            default_config_path,
            plugin_xprompt_dirs,
            plugin_config_paths,
            known_workspaces,
        }
    }

    fn gather_structured_sources(
        &self,
        project: Option<&str>,
    ) -> Result<Vec<StructuredSource>, XpromptCatalogLoadError> {
        let workflows = self.load_all_workflows(project)?;
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
                    workflow,
                    bucket,
                    project: source_project,
                    description: None,
                    is_skill: false,
                    content,
                    definition_section: DefinitionSection::Workflows,
                });
            }
        }

        for (name, xprompt) in self.load_all_xprompts(project)? {
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
                content: xprompt.content,
                definition_section: DefinitionSection::Xprompts,
            });
        }

        if project.is_none() {
            for (project_name, workspace) in &self.known_workspaces {
                for (name, xprompt) in
                    self.load_project_local_xprompts(project_name, workspace)?
                {
                    let source =
                        xprompt.source_path.clone().unwrap_or_default();
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
                        content: xprompt.content,
                        definition_section: DefinitionSection::Xprompts,
                    });
                }
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
            all.extend(self.load_xprompts_from_dir(
                &dir.join("skills"),
                None,
                false,
            )?);
        }
        if let Some(dir) = &self.default_xprompts_dir {
            all.extend(self.load_xprompts_from_dir(dir, None, false)?);
        }
        all.extend(self.load_plugin_xprompts()?);
        all.extend(self.load_config_xprompts(project)?);
        all.extend(self.load_memory_xprompts()?);
        if let Some(project) = project {
            all.extend(self.load_project_specific_xprompts(project)?);
        }
        for (dir, local) in self.xprompt_search_dirs_low_to_high() {
            all.extend(self.load_xprompts_from_dir(&dir, project, local)?);
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
        if let Some(project) = project {
            all.extend(self.load_project_specific_workflows(project)?);
            if let Some(workspace) = self.known_workspaces.get(project) {
                for xprompt_dir in
                    [workspace.join(".xprompts"), workspace.join("xprompts")]
                {
                    all.extend(self.load_workflows_from_dir(
                        &xprompt_dir,
                        Some(project),
                        true,
                    )?);
                }
            }
        }
        for (dir, local) in self.xprompt_search_dirs_low_to_high() {
            all.extend(self.load_workflows_from_dir(&dir, project, local)?);
        }
        Ok(all)
    }

    fn xprompt_search_dirs_low_to_high(&self) -> Vec<(PathBuf, bool)> {
        let mut dirs = Vec::new();
        if let Some(home) = &self.home_dir {
            dirs.push((home.join("xprompts"), false));
            dirs.push((home.join(".xprompts"), false));
        }
        if let Some(root) = &self.root_dir {
            dirs.push((root.join("xprompts"), true));
            dirs.push((root.join(".xprompts"), true));
        }
        dirs
    }

    fn load_xprompts_from_dir(
        &self,
        dir: &Path,
        project: Option<&str>,
        namespace_local: bool,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for path in files_with_extensions(dir, &["md"])? {
            let Some(mut xprompt) = load_xprompt_from_markdown(&path)? else {
                continue;
            };
            if namespace_local {
                if let Some(project) = project {
                    xprompt.name = format!("{project}/{}", xprompt.name);
                }
            }
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
                xprompt.source_path =
                    Some(format!("plugin:{module}/{filename}"));
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

    fn load_config_xprompts(
        &self,
        project: Option<&str>,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for (source, path) in self.config_paths() {
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

    fn config_paths(&self) -> Vec<(String, PathBuf)> {
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
            paths.push(("local_config".to_string(), root.join("sase.yml")));
        }
        paths
    }

    fn load_project_local_xprompts(
        &self,
        project: &str,
        workspace: &Path,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let source = format!("project_local_config:{project}");
        let Some(data) = load_yaml_mapping(&workspace.join("sase.yml"))? else {
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
            xprompt.name = format!("{project}/{}", xprompt.name);
            result.insert(xprompt.name.clone(), xprompt);
        }
        Ok(result)
    }

    fn load_project_specific_xprompts(
        &self,
        project: &str,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let Some(home) = &self.home_dir else {
            return Ok(BTreeMap::new());
        };
        self.load_xprompts_from_dir(
            &home
                .join(".config")
                .join("sase")
                .join("xprompts")
                .join(project),
            Some(project),
            true,
        )
    }

    fn load_project_specific_workflows(
        &self,
        project: &str,
    ) -> Result<BTreeMap<String, CatalogWorkflow>, XpromptCatalogLoadError>
    {
        let Some(home) = &self.home_dir else {
            return Ok(BTreeMap::new());
        };
        self.load_workflows_from_dir(
            &home
                .join(".config")
                .join("sase")
                .join("xprompts")
                .join(project),
            Some(project),
            true,
        )
    }

    fn load_memory_xprompts(
        &self,
    ) -> Result<BTreeMap<String, CatalogXprompt>, XpromptCatalogLoadError> {
        let mut result = BTreeMap::new();
        for (dir, cwd_relative) in self.memory_search_dirs_low_to_high() {
            for path in files_with_extensions(&dir, &["md"])? {
                let text = match fs::read_to_string(&path) {
                    Ok(text) => text,
                    Err(_) => continue,
                };
                let (front_matter, _) = parse_front_matter(&text);
                let Some(front_matter) = front_matter else {
                    continue;
                };
                if mapping_get(&front_matter, "keywords").is_none() {
                    continue;
                }
                let Some(stem) =
                    path.file_stem().and_then(|stem| stem.to_str())
                else {
                    continue;
                };
                let cat_path = if cwd_relative {
                    self.root_dir
                        .as_ref()
                        .and_then(|root| path.strip_prefix(root).ok())
                        .unwrap_or(path.as_path())
                        .to_string_lossy()
                        .into_owned()
                } else {
                    path.to_string_lossy().into_owned()
                };
                let name = format!("memory/long/{stem}");
                result.insert(
                    name.clone(),
                    CatalogXprompt {
                        name,
                        content: format!("$(cat {cat_path})"),
                        inputs: Vec::new(),
                        source_path: Some(path.to_string_lossy().into_owned()),
                        tags: BTreeSet::from(["memory".to_string()]),
                        description: None,
                        is_skill: false,
                        snippet: None,
                    },
                );
            }
        }
        Ok(result)
    }

    fn load_user_snippets(
        &self,
    ) -> Result<BTreeMap<String, String>, XpromptCatalogLoadError> {
        let mut snippets = BTreeMap::new();
        for (_source, path) in self.config_paths() {
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

    fn memory_search_dirs_low_to_high(&self) -> Vec<(PathBuf, bool)> {
        let mut dirs = Vec::new();
        if let Some(home) = &self.home_dir {
            dirs.push((home.join(".codex").join("memory").join("long"), false));
            dirs.push((
                home.join(".gemini").join("memory").join("long"),
                false,
            ));
            dirs.push((
                home.join(".claude").join("memory").join("long"),
                false,
            ));
        }
        if let Some(root) = &self.root_dir {
            dirs.push((root.join(".codex").join("memory").join("long"), true));
            dirs.push((root.join(".gemini").join("memory").join("long"), true));
            dirs.push((root.join(".claude").join("memory").join("long"), true));
            dirs.push((root.join("memory").join("long"), true));
        }
        dirs
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
            if path.to_string_lossy().contains("memory/long") {
                return ("memory".to_string(), None);
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
            return self.root_dir.as_ref().map(|root| root.join("sase.yml"));
        }
        if let Some(project) = source.strip_prefix("project_local_config:") {
            return self
                .known_workspaces
                .get(project)
                .map(|workspace| workspace.join("sase.yml"));
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
            self.default_xprompts_dir.clone(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
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

fn known_project_workspaces(home: Option<&Path>) -> BTreeMap<String, PathBuf> {
    let Some(home) = home else {
        return BTreeMap::new();
    };
    let projects_dir = home.join(".sase").join("projects");
    let Ok(project_dirs) = fs::read_dir(projects_dir) else {
        return BTreeMap::new();
    };
    let mut result = BTreeMap::new();
    for project_dir in project_dirs.flatten() {
        let Ok(files) = fs::read_dir(project_dir.path()) else {
            continue;
        };
        let mut entries: Vec<(String, PathBuf)> = Vec::new();
        for file in files.flatten() {
            let path = file.path();
            let extension = path.extension().and_then(|ext| ext.to_str());
            if extension != Some("sase") && extension != Some("gp") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
            else {
                continue;
            };
            if stem.ends_with("-archive") {
                continue;
            }
            entries.push((stem.to_string(), path));
        }
        entries.sort_by(|a, b| {
            let prefer_a =
                a.1.extension().and_then(|ext| ext.to_str()) == Some("sase");
            let prefer_b =
                b.1.extension().and_then(|ext| ext.to_str()) == Some("sase");
            prefer_b.cmp(&prefer_a)
        });
        let mut seen = BTreeSet::<String>::new();
        for (project_name, path) in entries {
            if !seen.insert(project_name.clone()) {
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix("WORKSPACE_DIR:") {
                    let workspace = PathBuf::from(rest.trim());
                    if workspace.is_dir() {
                        result.insert(project_name, workspace);
                    }
                    break;
                }
            }
        }
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
    Ok(Some(CatalogXprompt {
        name,
        content: body,
        inputs,
        source_path: Some(path.to_string_lossy().into_owned()),
        tags,
        description,
        is_skill,
        snippet,
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

fn workflow_from_mapping(
    name: &str,
    data: &serde_yaml::Mapping,
    source_path: &str,
) -> CatalogWorkflow {
    let tags = mapping_get(data, "tags")
        .map(parse_tags)
        .unwrap_or_default();
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
                required: true,
                default_display: None,
                default_snippet_value: None,
                is_step_input: true,
            });
        }
    }
    CatalogWorkflow {
        name: name.to_string(),
        inputs,
        steps,
        source_path: Some(source_path.to_string()),
        tags,
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
            source_path: Some(source_path.to_string()),
            tags: BTreeSet::new(),
            description: None,
            is_skill: false,
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
        source_path: Some(source_path.to_string()),
        tags: mapping_get(data, "tags")
            .map(parse_tags)
            .unwrap_or_default(),
        description: mapping_get(data, "description").and_then(value_as_string),
        is_skill: mapping_get(data, "skill")
            .map(value_is_truthy)
            .unwrap_or(false),
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
        source_path: xprompt.source_path.clone(),
        tags: xprompt.tags.clone(),
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
                    required,
                    default_display,
                    default_snippet_value,
                ) = parse_short_input_value(raw);
                Some(CatalogInput {
                    name,
                    type_name,
                    required,
                    default_display,
                    default_snippet_value,
                    is_step_input: false,
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
                Some(CatalogInput {
                    name,
                    type_name,
                    required: default.is_none(),
                    default_display: default.and_then(default_display),
                    default_snippet_value: default.map(snippet_default_value),
                    is_step_input: false,
                })
            })
            .collect();
    }
    Vec::new()
}

fn parse_short_input_value(
    value: &Value,
) -> (String, bool, Option<String>, Option<String>) {
    if let Some(mapping) = value.as_mapping() {
        let type_name = mapping_get(mapping, "type")
            .and_then(value_as_string)
            .map(|raw| parse_input_type(&raw))
            .unwrap_or_else(|| "line".to_string());
        let default = mapping_get(mapping, "default");
        (
            type_name,
            default.is_none(),
            default.and_then(default_display),
            default.map(snippet_default_value),
        )
    } else {
        (
            parse_input_type(
                &value_as_string(value).unwrap_or_else(|| "line".to_string()),
            ),
            true,
            None,
            None,
        )
    }
}

fn parse_input_type(raw: &str) -> String {
    match raw.to_lowercase().as_str() {
        "word" => "word",
        "text" => "text",
        "path" => "path",
        "int" | "integer" => "int",
        "bool" | "boolean" => "bool",
        "float" => "float",
        _ => "line",
    }
    .to_string()
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
        let xprompts = root.join(".xprompts");
        fs::create_dir(&xprompts).unwrap();
        fs::write(
            xprompts.join("swarm.md"),
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

        assert_eq!(by_name["swarm"].insertion.as_deref(), Some("#!swarm"));
        assert_eq!(by_name["swarm"].reference_prefix.as_deref(), Some("#!"));
        assert_eq!(by_name["swarm"].kind.as_deref(), Some("xprompt"));
        assert!(by_name["swarm"].is_skill);
        assert_eq!(
            by_name["swarm"].input_signature.as_deref(),
            Some("(target: word)")
        );
        assert_eq!(by_name["ship"].insertion.as_deref(), Some("#!ship"));
        assert_eq!(
            by_name["ship"].kind.as_deref(),
            Some("standalone_workflow")
        );
    }

    #[test]
    fn filters_step_inputs_and_formats_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("xprompts");
        fs::create_dir(&xprompts).unwrap();
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
    fn loads_native_snippet_catalog_with_user_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join(".xprompts");
        fs::create_dir(&xprompts).unwrap();
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
            root.join("sase.yml"),
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
        assert_eq!(by_trigger["plan"].template, "Plan $1$0");
        assert!(!by_trigger.contains_key("bad-trigger!"));
    }

    #[test]
    fn converts_native_xprompt_snippet_templates() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("xprompts");
        fs::create_dir(&xprompts).unwrap();
        fs::write(
            xprompts.join("fix.md"),
            "---\nsnippet: fixit\ninput:\n  bug: word\n  area:\n    type: line\n    default: parser\n  empty:\n    type: line\n    default:\n---\nFix {{ bug }} in {{ area }}{{ empty }}. Then {2} or {3:done}.",
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
            "Fix $1 in parser. Then $2 or done.$0"
        );
        assert_eq!(by_trigger["fixit"].xprompt_name.as_deref(), Some("fix"));
        assert!(!by_trigger.contains_key("complex"));
    }

    #[test]
    fn parity_fixture_covers_supported_catalog_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let home = temp.path().join("home");
        let package = temp.path().join("package");
        fs::create_dir_all(root.join(".xprompts")).unwrap();
        fs::create_dir_all(root.join("memory/long")).unwrap();
        fs::create_dir_all(home.join(".config/sase/xprompts/app")).unwrap();
        fs::create_dir_all(package.join("xprompts")).unwrap();
        fs::create_dir_all(package.join("xprompts/skills")).unwrap();
        fs::create_dir_all(package.join("default_xprompts")).unwrap();

        fs::write(
            package.join("xprompts/builtin.md"),
            "---\nskill: true\ntags: [mentor]\n---\nBuilt in",
        )
        .unwrap();
        fs::write(
            package.join("xprompts/skills/sase_plan.md"),
            "---\nskill: true\n---\nPlan skill",
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
            root.join(".xprompts/local.md"),
            "---\ninput: {target: word}\n---\nLocal body",
        )
        .unwrap();
        fs::write(root.join(".xprompts/swarm.md"), "one\n---\ntwo").unwrap();
        fs::write(
            root.join(".xprompts/flow.yml"),
            "input: {target: word}\nsteps:\n  - name: run\n    agent: Run {{ target }}\n",
        )
        .unwrap();
        fs::write(
            root.join("memory/long/topic.md"),
            "---\nkeywords: [topic]\n---\nMemory body",
        )
        .unwrap();
        fs::write(
            home.join(".config/sase/xprompts/app/project.md"),
            "---\ndescription: Project prompt\n---\nProject body",
        )
        .unwrap();

        let loader = CatalogLoader {
            root_dir: Some(root.clone()),
            home_dir: Some(home.clone()),
            package_xprompts_dir: Some(package.join("xprompts")),
            default_xprompts_dir: Some(package.join("default_xprompts")),
            default_config_path: Some(package.join("default_config.yml")),
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            known_workspaces: BTreeMap::from([(
                "app".to_string(),
                root.clone(),
            )]),
        };

        let entries = loader.gather_structured_sources(Some("app")).unwrap();
        let by_name = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_name["builtin"].bucket, "built-in");
        assert!(by_name["builtin"].is_skill);
        assert_eq!(by_name["sase_plan"].bucket, "built-in");
        assert!(by_name["sase_plan"].is_skill);
        assert_eq!(by_name["defaulted"].bucket, "built-in");
        assert_eq!(by_name["cfg"].bucket, "config");
        assert_eq!(by_name["memory/long/topic"].bucket, "memory");
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
            Some("#!app/swarm")
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
            wire_by_name["sase_plan"].definition_path.as_deref(),
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
            wire_by_name["memory/long/topic"].definition_path.as_deref(),
            Some(
                root.join("memory/long/topic.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            wire_by_name["app/local"].definition_path.as_deref(),
            Some(
                root.join(".xprompts/local.md")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
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
        fs::create_dir_all(&root).unwrap();
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
            root.join("sase.yml"),
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
                root.join("sase.yml")
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
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(home.join(".config/sase")).unwrap();
        fs::create_dir_all(project_workspace.join("xprompts")).unwrap();

        fs::write(
            root.join("sase.yml"),
            "xprompts:\n  local_xp:\n    content: Local config xprompt body\nworkflows:\n  local_flow:\n    steps:\n      - name: run\n        prompt_part: Local config workflow body\n",
        )
        .unwrap();
        fs::write(
            home.join(".config/sase/sase.yml"),
            "xprompts:\n  user_xp:\n    content: User config xprompt body\nworkflows:\n  user_flow:\n    steps:\n      - name: run\n        prompt_part: User config workflow body\n",
        )
        .unwrap();
        fs::write(
            project_workspace.join("xprompts/file_flow.yml"),
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
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            workspace.join("sase.yml"),
            "xprompts:\n  project_cfg:\n    content: Project body\n",
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

        assert_eq!(
            entry.definition_path.as_deref(),
            Some(
                workspace
                    .join("sase.yml")
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(definition_line(entry), Some(1));
    }

    #[test]
    fn known_project_workspaces_prefers_sase_spec_with_gp_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let projects_dir = home.join(".sase").join("projects");

        let canonical_workspace = temp.path().join("canonical_ws");
        let canonical_project_dir = projects_dir.join("canonical");
        fs::create_dir_all(&canonical_workspace).unwrap();
        fs::create_dir_all(&canonical_project_dir).unwrap();
        fs::write(
            canonical_project_dir.join("canonical.sase"),
            format!("WORKSPACE_DIR: {}\n", canonical_workspace.display()),
        )
        .unwrap();
        fs::write(
            canonical_project_dir.join("canonical.gp"),
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

        let workspaces = known_project_workspaces(Some(home.as_path()));

        assert_eq!(
            workspaces.get("canonical").map(PathBuf::as_path),
            Some(canonical_workspace.as_path()),
        );
        assert_eq!(
            workspaces.get("legacy").map(PathBuf::as_path),
            Some(legacy_workspace.as_path()),
        );
        assert!(!workspaces.contains_key("archived"));
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
                source_path: Some("plugin:module/plugin.md".to_string()),
                tags: BTreeSet::new(),
            },
            bucket: "plugin".to_string(),
            project: None,
            description: None,
            is_skill: false,
            content: "body".to_string(),
            definition_section: DefinitionSection::Xprompts,
        };

        assert_eq!(structured_entry(&entry, &loader).definition_path, None);
    }
}
