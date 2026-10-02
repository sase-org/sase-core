use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    content_layout::{
        sase_content_layout, skill_placement_issue, skill_reference_name,
    },
    EditorRange,
};

use super::loader::CatalogLoader;
use super::parsing::*;
use super::types::*;
impl CatalogLoader {
    pub(super) fn load_xprompts_from_dir(
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
    pub(super) fn load_skills_from_dir(
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

    pub(super) fn load_workflows_from_dir(
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

    pub(super) fn load_plugin_xprompts(
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
    pub(super) fn load_plugin_skills(
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

    pub(super) fn load_plugin_workflows(
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

    pub(super) fn load_config_xprompts(
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

    pub(super) fn load_project_local_xprompts(
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

    pub(super) fn load_project_file_xprompts(
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

    pub(super) fn load_project_file_workflows(
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

    pub(super) fn load_user_snippets(
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

    pub(super) fn classify_source(
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

    pub(super) fn source_path_display(
        &self,
        entry: &StructuredSource,
    ) -> Option<String> {
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

    pub(super) fn definition_path(
        &self,
        entry: &StructuredSource,
    ) -> Option<String> {
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

    pub(super) fn definition_range(
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
            let xprompt = self
                .plugin_xprompt_dirs
                .get(module)
                .map(|dir| dir.join(filename));
            if xprompt.as_ref().is_some_and(|path| path.is_file()) {
                return xprompt;
            }
            return self
                .plugin_skill_dirs
                .get(module)
                .map(|dir| dir.join(filename))
                .or(xprompt);
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
