use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const CONTENT_LAYOUT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutPathRoleWire {
    Canonical,
    Legacy,
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutTrackingWire {
    SourceControlled,
    Generated,
    RuntimeOnly,
    UserConfig,
    PackageOwned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutCollisionPolicyWire {
    Error,
    FirstWins,
}

impl LayoutCollisionPolicyWire {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "error" => Some(Self::Error),
            "first_wins" => Some(Self::FirstWins),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutPathWire {
    pub path: String,
    pub role: LayoutPathRoleWire,
    pub tracking: LayoutTrackingWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibleLayoutPathWire {
    pub canonical: LayoutPathWire,
    pub legacy: Vec<LayoutPathWire>,
    pub write_path: String,
    pub read_policy: LayoutCollisionPolicyWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectContentLayoutWire {
    pub root: String,
    pub namespace_root: LayoutPathWire,
    pub config: CompatibleLayoutPathWire,
    pub xprompts: CompatibleLayoutPathWire,
    pub memory: CompatibleLayoutPathWire,
    pub repos: LayoutPathWire,
    pub memory_readme: LayoutPathWire,
    pub agent_documents: Vec<LayoutPathWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeContentLayoutWire {
    pub root: String,
    pub namespace_root: LayoutPathWire,
    pub xprompts: CompatibleLayoutPathWire,
    pub memory: CompatibleLayoutPathWire,
    pub global_config: LayoutPathWire,
    pub state_root: LayoutPathWire,
    pub memory_readme: LayoutPathWire,
    pub agent_documents: Vec<LayoutPathWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChezmoiContentLayoutWire {
    pub source_root: String,
    pub namespace_root: LayoutPathWire,
    pub xprompts: CompatibleLayoutPathWire,
    pub memory: CompatibleLayoutPathWire,
    pub global_config: LayoutPathWire,
    pub memory_readme: LayoutPathWire,
    pub agent_documents: Vec<LayoutPathWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptSourceWire {
    pub id: String,
    pub priority: u32,
    pub scope: String,
    pub role: LayoutPathRoleWire,
    pub locator: String,
    pub path: Option<String>,
    pub formats: Vec<String>,
    pub steps_path: Option<String>,
    pub project_namespaced: bool,
    pub writable: bool,
    pub collision_group: Option<String>,
    pub collision_policy: Option<LayoutCollisionPolicyWire>,
    pub ordering: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaseContentLayoutWire {
    pub schema_version: u32,
    pub project: Option<ProjectContentLayoutWire>,
    pub home: HomeContentLayoutWire,
    pub chezmoi: Option<ChezmoiContentLayoutWire>,
    pub xprompt_sources: Vec<XpromptSourceWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutCandidateResolutionWire {
    pub selected_index: Option<usize>,
    pub existing_indices: Vec<usize>,
    pub shadowed_indices: Vec<usize>,
    pub collision: bool,
}

pub fn resolve_layout_candidates(
    policy: LayoutCollisionPolicyWire,
    exists: &[bool],
) -> LayoutCandidateResolutionWire {
    let existing_indices = exists
        .iter()
        .enumerate()
        .filter_map(|(index, exists)| exists.then_some(index))
        .collect::<Vec<_>>();
    let collision = policy == LayoutCollisionPolicyWire::Error
        && existing_indices.len() > 1;
    let selected_index = if collision {
        None
    } else {
        existing_indices.first().copied()
    };
    let shadowed_indices = selected_index
        .map(|selected| {
            existing_indices
                .iter()
                .copied()
                .filter(|index| *index != selected)
                .collect()
        })
        .unwrap_or_default();
    LayoutCandidateResolutionWire {
        selected_index,
        existing_indices,
        shadowed_indices,
        collision,
    }
}

pub fn sase_content_layout(
    project_root: Option<&Path>,
    home_root: &Path,
    chezmoi_source_root: Option<&Path>,
    project_name: Option<&str>,
) -> SaseContentLayoutWire {
    let project = project_root.map(project_content_layout);
    let home = home_content_layout(home_root);
    let chezmoi = chezmoi_source_root.map(chezmoi_content_layout);
    let xprompt_sources =
        xprompt_sources(project_root, home_root, project_name);
    SaseContentLayoutWire {
        schema_version: CONTENT_LAYOUT_SCHEMA_VERSION,
        project,
        home,
        chezmoi,
        xprompt_sources,
    }
}

fn project_content_layout(root: &Path) -> ProjectContentLayoutWire {
    let namespace_root = root.join("sase");
    let config = compatible_path(
        namespace_root.join("sase.yml"),
        [root.join("sase.yml")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::Error,
    );
    let xprompts = compatible_path(
        namespace_root.join("xprompts"),
        [root.join(".xprompts"), root.join("xprompts")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::FirstWins,
    );
    let memory = compatible_path(
        namespace_root.join("memory"),
        [root.join("memory")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::Error,
    );
    let memory_readme = namespace_root.join("memory").join("README.md");
    ProjectContentLayoutWire {
        root: path_string(root),
        namespace_root: layout_path(
            namespace_root.clone(),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::SourceControlled,
        ),
        config,
        xprompts,
        memory,
        repos: layout_path(
            namespace_root.join("repos"),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::RuntimeOnly,
        ),
        memory_readme: layout_path(
            memory_readme,
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::Generated,
        ),
        agent_documents: agent_document_paths(root),
    }
}

fn home_content_layout(root: &Path) -> HomeContentLayoutWire {
    let namespace_root = root.join("sase");
    let xprompts = compatible_path(
        namespace_root.join("xprompts"),
        [root.join(".xprompts"), root.join("xprompts")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::FirstWins,
    );
    let memory = compatible_path(
        namespace_root.join("memory"),
        [root.join("memory")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::Error,
    );
    HomeContentLayoutWire {
        root: path_string(root),
        namespace_root: layout_path(
            namespace_root.clone(),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::SourceControlled,
        ),
        xprompts,
        memory,
        global_config: layout_path(
            root.join(".config").join("sase").join("sase.yml"),
            LayoutPathRoleWire::Unchanged,
            LayoutTrackingWire::UserConfig,
        ),
        state_root: layout_path(
            root.join(".sase"),
            LayoutPathRoleWire::Unchanged,
            LayoutTrackingWire::RuntimeOnly,
        ),
        memory_readme: layout_path(
            namespace_root.join("memory").join("README.md"),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::Generated,
        ),
        agent_documents: agent_document_paths(root),
    }
}

fn chezmoi_content_layout(root: &Path) -> ChezmoiContentLayoutWire {
    let namespace_root = root.join("sase");
    let xprompts = compatible_path(
        namespace_root.join("xprompts"),
        [root.join("dot_xprompts"), root.join("xprompts")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::FirstWins,
    );
    let memory = compatible_path(
        namespace_root.join("memory"),
        [root.join("memory")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::Error,
    );
    ChezmoiContentLayoutWire {
        source_root: path_string(root),
        namespace_root: layout_path(
            namespace_root.clone(),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::SourceControlled,
        ),
        xprompts,
        memory,
        global_config: layout_path(
            root.join("dot_config").join("sase").join("sase.yml"),
            LayoutPathRoleWire::Unchanged,
            LayoutTrackingWire::SourceControlled,
        ),
        memory_readme: layout_path(
            namespace_root.join("memory").join("README.md"),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::Generated,
        ),
        agent_documents: agent_document_paths(root),
    }
}

fn xprompt_sources(
    project_root: Option<&Path>,
    home_root: &Path,
    project_name: Option<&str>,
) -> Vec<XpromptSourceWire> {
    let mut sources = Vec::new();
    if let Some(root) = project_root {
        push_directory_source(
            &mut sources,
            "project_canonical",
            "project",
            LayoutPathRoleWire::Canonical,
            root.join("sase").join("xprompts"),
            true,
            true,
        );
        push_directory_source(
            &mut sources,
            "project_legacy_hidden",
            "project",
            LayoutPathRoleWire::Legacy,
            root.join(".xprompts"),
            true,
            false,
        );
        push_directory_source(
            &mut sources,
            "project_legacy_visible",
            "project",
            LayoutPathRoleWire::Legacy,
            root.join("xprompts"),
            true,
            false,
        );
    }

    push_directory_source(
        &mut sources,
        "home_canonical",
        "home",
        LayoutPathRoleWire::Canonical,
        home_root.join("sase").join("xprompts"),
        false,
        true,
    );
    push_directory_source(
        &mut sources,
        "home_legacy_hidden",
        "home",
        LayoutPathRoleWire::Legacy,
        home_root.join(".xprompts"),
        false,
        false,
    );
    push_directory_source(
        &mut sources,
        "home_legacy_visible",
        "home",
        LayoutPathRoleWire::Legacy,
        home_root.join("xprompts"),
        false,
        false,
    );

    if let Some(project_name) = project_name.filter(|name| !name.is_empty()) {
        push_directory_source(
            &mut sources,
            "home_project_canonical",
            "home_project",
            LayoutPathRoleWire::Canonical,
            home_root.join("sase").join("xprompts").join(project_name),
            true,
            true,
        );
        push_directory_source(
            &mut sources,
            "home_project_legacy_config",
            "home_project",
            LayoutPathRoleWire::Legacy,
            home_root
                .join(".config")
                .join("sase")
                .join("xprompts")
                .join(project_name),
            true,
            false,
        );
    }

    if let Some(root) = project_root {
        push_config_source(
            &mut sources,
            "project_config_canonical",
            "project_config",
            LayoutPathRoleWire::Canonical,
            root.join("sase").join("sase.yml"),
            Some("project_config"),
            Some(LayoutCollisionPolicyWire::Error),
            true,
        );
        push_config_source(
            &mut sources,
            "project_config_legacy",
            "project_config",
            LayoutPathRoleWire::Legacy,
            root.join("sase.yml"),
            Some("project_config"),
            Some(LayoutCollisionPolicyWire::Error),
            false,
        );
    }

    push_config_source(
        &mut sources,
        "user_config_overlays",
        "user_config",
        LayoutPathRoleWire::Unchanged,
        home_root.join(".config").join("sase").join("sase_*.yml"),
        None,
        None,
        true,
    );
    if let Some(source) = sources.last_mut() {
        source.ordering = Some("reverse_lexical_first_wins".to_string());
    }
    push_config_source(
        &mut sources,
        "user_config",
        "user_config",
        LayoutPathRoleWire::Unchanged,
        home_root.join(".config").join("sase").join("sase.yml"),
        None,
        None,
        true,
    );
    push_symbolic_source(
        &mut sources,
        "plugin_config",
        "plugin",
        "entrypoint:sase_config/default_config.yml",
        vec!["config"],
    );
    push_symbolic_source(
        &mut sources,
        "package_default_config",
        "package",
        "package:default_config.yml",
        vec!["config"],
    );
    push_symbolic_source(
        &mut sources,
        "plugin_resources",
        "plugin",
        "entrypoint:sase_xprompts/xprompts",
        vec!["md", "yml", "yaml"],
    );
    push_symbolic_source(
        &mut sources,
        "package_defaults",
        "package",
        "package:default_xprompts",
        vec!["md"],
    );
    push_symbolic_source(
        &mut sources,
        "package_internal",
        "package",
        "package:xprompts",
        vec!["md", "yml", "yaml"],
    );
    if let Some(source) = sources.last_mut() {
        source.steps_path = Some("package:xprompts/steps".to_string());
    }

    for (priority, source) in sources.iter_mut().enumerate() {
        source.priority = (priority + 1) as u32;
    }
    sources
}

fn push_directory_source(
    sources: &mut Vec<XpromptSourceWire>,
    id: &str,
    scope: &str,
    role: LayoutPathRoleWire,
    path: PathBuf,
    project_namespaced: bool,
    writable: bool,
) {
    let path = path_string(&path);
    sources.push(XpromptSourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        role,
        locator: path.clone(),
        path: Some(path.clone()),
        formats: strings(&["md", "yml", "yaml"]),
        steps_path: Some(path_string(&PathBuf::from(path).join("steps"))),
        project_namespaced,
        writable,
        collision_group: None,
        collision_policy: Some(LayoutCollisionPolicyWire::FirstWins),
        ordering: Some("first_wins".to_string()),
    });
}

#[allow(clippy::too_many_arguments)]
fn push_config_source(
    sources: &mut Vec<XpromptSourceWire>,
    id: &str,
    scope: &str,
    role: LayoutPathRoleWire,
    path: PathBuf,
    collision_group: Option<&str>,
    collision_policy: Option<LayoutCollisionPolicyWire>,
    writable: bool,
) {
    let path = path_string(&path);
    sources.push(XpromptSourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        role,
        locator: path.clone(),
        path: Some(path),
        formats: vec!["config".to_string()],
        steps_path: None,
        project_namespaced: scope == "project_config",
        writable,
        collision_group: collision_group.map(str::to_string),
        collision_policy,
        ordering: Some("first_wins".to_string()),
    });
}

fn push_symbolic_source(
    sources: &mut Vec<XpromptSourceWire>,
    id: &str,
    scope: &str,
    locator: &str,
    formats: Vec<&str>,
) {
    sources.push(XpromptSourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        role: LayoutPathRoleWire::Unchanged,
        locator: locator.to_string(),
        path: None,
        formats: formats.into_iter().map(str::to_string).collect(),
        steps_path: None,
        project_namespaced: false,
        writable: false,
        collision_group: None,
        collision_policy: Some(LayoutCollisionPolicyWire::FirstWins),
        ordering: Some("first_wins".to_string()),
    });
}

fn compatible_path<const N: usize>(
    canonical: PathBuf,
    legacy: [PathBuf; N],
    tracking: LayoutTrackingWire,
    read_policy: LayoutCollisionPolicyWire,
) -> CompatibleLayoutPathWire {
    let write_path = path_string(&canonical);
    CompatibleLayoutPathWire {
        canonical: layout_path(
            canonical,
            LayoutPathRoleWire::Canonical,
            tracking,
        ),
        legacy: legacy
            .into_iter()
            .map(|path| layout_path(path, LayoutPathRoleWire::Legacy, tracking))
            .collect(),
        write_path,
        read_policy,
    }
}

fn agent_document_paths(root: &Path) -> Vec<LayoutPathWire> {
    [
        "AGENTS.md",
        "CLAUDE.md",
        "GEMINI.md",
        "OPENCODE.md",
        "QWEN.md",
    ]
    .into_iter()
    .map(|filename| {
        layout_path(
            root.join(filename),
            LayoutPathRoleWire::Unchanged,
            LayoutTrackingWire::Generated,
        )
    })
    .collect()
}

fn layout_path(
    path: PathBuf,
    role: LayoutPathRoleWire,
    tracking: LayoutTrackingWire,
) -> LayoutPathWire {
    LayoutPathWire {
        path: path_string(&path),
        role,
        tracking,
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_and_home_paths_keep_runtime_and_generated_content_separate() {
        let layout = sase_content_layout(
            Some(Path::new("/workspace/project")),
            Path::new("/home/alice"),
            Some(Path::new("/dotfiles/home")),
            Some("project"),
        );
        let project = layout.project.unwrap();
        assert_eq!(
            project.config.canonical.path,
            "/workspace/project/sase/sase.yml"
        );
        assert_eq!(
            project.config.legacy[0].path,
            "/workspace/project/sase.yml"
        );
        assert_eq!(
            project.memory.read_policy,
            LayoutCollisionPolicyWire::Error
        );
        assert_eq!(
            project.xprompts.read_policy,
            LayoutCollisionPolicyWire::FirstWins
        );
        assert_eq!(project.repos.tracking, LayoutTrackingWire::RuntimeOnly);
        assert_eq!(
            project.memory_readme.tracking,
            LayoutTrackingWire::Generated
        );
        assert_eq!(
            layout.home.global_config.path,
            "/home/alice/.config/sase/sase.yml"
        );
        assert_eq!(
            layout.chezmoi.unwrap().xprompts.canonical.path,
            "/dotfiles/home/sase/xprompts"
        );
    }

    #[test]
    fn xprompt_priority_covers_canonical_legacy_config_plugin_and_package() {
        let layout = sase_content_layout(
            Some(Path::new("/repo")),
            Path::new("/home/alice"),
            None,
            Some("demo"),
        );
        let ids = layout
            .xprompt_sources
            .iter()
            .map(|source| source.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "project_canonical",
                "project_legacy_hidden",
                "project_legacy_visible",
                "home_canonical",
                "home_legacy_hidden",
                "home_legacy_visible",
                "home_project_canonical",
                "home_project_legacy_config",
                "project_config_canonical",
                "project_config_legacy",
                "user_config_overlays",
                "user_config",
                "plugin_config",
                "package_default_config",
                "plugin_resources",
                "package_defaults",
                "package_internal",
            ]
        );
        assert!(layout
            .xprompt_sources
            .iter()
            .take(8)
            .all(|source| source.formats == strings(&["md", "yml", "yaml"])
                && source.steps_path.is_some()));
        assert_eq!(
            layout.xprompt_sources[8].collision_policy,
            Some(LayoutCollisionPolicyWire::Error)
        );
        assert_eq!(
            layout.xprompt_sources.last().unwrap().steps_path.as_deref(),
            Some("package:xprompts/steps")
        );
    }

    #[test]
    fn collision_policy_is_exclusive_for_config_and_first_wins_for_xprompts() {
        let exclusive = resolve_layout_candidates(
            LayoutCollisionPolicyWire::Error,
            &[true, true],
        );
        assert!(exclusive.collision);
        assert_eq!(exclusive.selected_index, None);
        assert_eq!(exclusive.existing_indices, vec![0, 1]);

        let first_wins = resolve_layout_candidates(
            LayoutCollisionPolicyWire::FirstWins,
            &[true, true, true],
        );
        assert!(!first_wins.collision);
        assert_eq!(first_wins.selected_index, Some(0));
        assert_eq!(first_wins.shadowed_indices, vec![1, 2]);

        let legacy_only = resolve_layout_candidates(
            LayoutCollisionPolicyWire::Error,
            &[false, true],
        );
        assert_eq!(legacy_only.selected_index, Some(1));
    }

    #[test]
    fn missing_project_root_still_returns_complete_home_contract() {
        let layout =
            sase_content_layout(None, Path::new("/home/alice"), None, None);
        assert!(layout.project.is_none());
        assert!(layout
            .xprompt_sources
            .iter()
            .all(|source| !source.id.starts_with("project_")));
        assert_eq!(layout.xprompt_sources[0].id, "home_canonical");
    }
}
