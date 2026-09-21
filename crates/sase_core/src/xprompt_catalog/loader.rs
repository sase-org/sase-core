use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
};

use crate::content_layout::{
    memory_note_issue, memory_reference_name, reserved_memory_namespace_issue,
    sase_content_layout, MemorySourceWire, MemoryTierWire,
    MemoryXpromptIssueWire, SkillPlacementIssueWire, SkillSourceWire,
    XpromptSourceWire, MEMORY_README_FILENAME, SKILL_DIRECTORY_SEGMENT,
};

use super::entries::*;
use super::parsing::*;
use super::types::*;
#[derive(Debug, Clone, Default)]
pub(super) struct CatalogLoader {
    pub(super) root_dir: Option<PathBuf>,
    pub(super) home_dir: Option<PathBuf>,
    pub(super) package_xprompts_dir: Option<PathBuf>,
    pub(super) package_skills_dir: Option<PathBuf>,
    pub(super) default_xprompts_dir: Option<PathBuf>,
    pub(super) default_config_path: Option<PathBuf>,
    pub(super) plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    pub(super) plugin_skill_dirs: BTreeMap<String, PathBuf>,
    pub(super) plugin_config_paths: BTreeMap<String, PathBuf>,
    pub(super) known_workspaces: BTreeMap<String, PathBuf>,
    pub(super) canonical_project_refs: BTreeMap<String, String>,
    /// Definitions dropped by the canonical skill placement rules, recorded so
    /// the catalog can name the offending source and its migration
    /// destination instead of silently losing it.
    pub(super) skill_issues: RefCell<Vec<SkillPlacementIssueWire>>,
    /// Definitions dropped by the xprompt-memory rules: a reserved `memory/`
    /// reference claimed by an ordinary definition, an unreachable note stem,
    /// or a file in a memory root that is not a valid memory note.
    pub(super) memory_issues: RefCell<Vec<MemoryXpromptIssueWire>>,
}

impl CatalogLoader {
    pub(super) fn new(options: &XpromptCatalogLoadOptions) -> Self {
        let root_dir = options.root_dir.clone();
        let home_dir = env::var_os("HOME").map(PathBuf::from);
        let package_root =
            env::var_os("SASE_XPROMPT_PACKAGE_DIR").map(PathBuf::from);
        let package_xprompts_dir = options
            .package_xprompts_dir
            .clone()
            .or_else(|| env_path("SASE_XPROMPT_BUILTIN_DIR"))
            .or_else(|| {
                package_root.as_ref().map(|root| root.join("xprompts"))
            });
        let package_skills_dir = options
            .package_skills_dir
            .clone()
            .or_else(|| env_path("SASE_SKILL_BUILTIN_DIR"))
            .or_else(|| {
                package_root.as_ref().map(|root| {
                    root.join("xprompts").join(SKILL_DIRECTORY_SEGMENT)
                })
            });
        let default_xprompts_dir = options
            .default_xprompts_dir
            .clone()
            .or_else(|| env_path("SASE_XPROMPT_DEFAULT_DIR"))
            .or_else(|| {
                package_root
                    .as_ref()
                    .map(|root| root.join("default_xprompts"))
            });
        let default_config_path = options
            .default_config_path
            .clone()
            .or_else(|| env_path("SASE_DEFAULT_CONFIG_PATH"))
            .or_else(|| {
                package_root
                    .as_ref()
                    .map(|root| root.join("default_config.yml"))
            });
        let plugin_xprompt_dirs = if options.plugin_xprompt_dirs.is_empty() {
            plugin_path_map_from_env(SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV)
        } else {
            options.plugin_xprompt_dirs.clone()
        };
        let plugin_skill_dirs = if options.plugin_skill_dirs.is_empty() {
            plugin_path_map_from_env(SASE_SKILL_PLUGIN_DIRS_JSON_ENV)
        } else {
            options.plugin_skill_dirs.clone()
        };
        let plugin_config_paths = if options.plugin_config_paths.is_empty() {
            plugin_path_map_from_env(SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV)
        } else {
            options.plugin_config_paths.clone()
        };
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

    pub(super) fn canonical_project(
        &self,
        project: Option<&str>,
    ) -> Option<String> {
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

    pub(super) fn root_project(&self) -> Option<&str> {
        let root = self.root_dir.as_deref()?;
        self.known_workspaces
            .iter()
            .find_map(|(project, workspace)| {
                path_is_under(root, workspace).then_some(project.as_str())
            })
    }

    pub(super) fn gather_structured_sources(
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

    pub(super) fn skill_lookup_sources(
        &self,
        project: Option<&str>,
    ) -> Result<Vec<StructuredSource>, XpromptCatalogLoadError> {
        if let Some(project) = project {
            return self.gather_structured_sources(Some(project)).map(
                |sources| {
                    sources
                        .into_iter()
                        .filter(|source| source.is_skill)
                        .collect()
                },
            );
        }

        self.load_all_xprompts(None).map(|xprompts| {
            xprompts
                .into_iter()
                .filter_map(|(name, xprompt)| {
                    xprompt.is_skill.then(|| {
                        let (bucket, project) = self.classify_source(
                            xprompt.source_path.as_deref(),
                            None,
                        );
                        let workflow = xprompt_to_workflow(&xprompt);
                        StructuredSource {
                            name,
                            workflow,
                            bucket,
                            project,
                            description: xprompt.description,
                            is_skill: xprompt.is_skill,
                            skill_name: xprompt.skill_name,
                            memory_type: xprompt.memory_type,
                            content: xprompt.content,
                            definition_section: DefinitionSection::Xprompts,
                        }
                    })
                })
                .collect()
        })
    }

    pub(super) fn skill_definition_candidate(
        &self,
        entry: &StructuredSource,
    ) -> XpromptSkillDefinitionCandidateWire {
        XpromptSkillDefinitionCandidateWire {
            reference: entry.name.clone(),
            skill_name: entry.skill_name.clone().unwrap_or_else(|| {
                entry
                    .name
                    .rsplit_once('/')
                    .map_or(entry.name.as_str(), |(_, tail)| tail)
                    .to_string()
            }),
            project: entry.project.clone(),
            definition_path: self.definition_path(entry),
        }
    }

    pub(super) fn load_all_xprompts(
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

    pub(super) fn xprompt_directory_sources(
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

    pub(super) fn skill_directory_sources(
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
    pub(super) fn skill_destination_for_xprompt_dir(
        &self,
        dir: &Path,
    ) -> Option<String> {
        let parent = dir.parent()?;
        Some(
            parent
                .join(SKILL_DIRECTORY_SEGMENT)
                .to_string_lossy()
                .into_owned(),
        )
    }

    pub(super) fn record_skill_issue(
        &self,
        issue: Option<SkillPlacementIssueWire>,
    ) {
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
    pub(super) fn reject_reserved_memory_name(
        &self,
        source: &str,
        name: &str,
    ) -> bool {
        let issue = reserved_memory_namespace_issue(source, name);
        let rejected = issue.is_some();
        self.record_memory_issue(issue);
        rejected
    }

    /// Migration diagnostics for every definition the placement rules dropped,
    /// so a misplaced source is reported rather than silently missing.
    pub(super) fn placement_warnings(&self) -> Vec<String> {
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
}
