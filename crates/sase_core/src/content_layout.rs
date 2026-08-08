use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const CONTENT_LAYOUT_SCHEMA_VERSION: u32 = 3;

/// Directory name holding canonical xprompt-backed skill sources, and the
/// namespace segment every such source carries in its xprompt reference name.
///
/// A source declaring `name: foo` under a canonical skill directory is
/// referenced as `#skills/foo` (or `#<project>/skills/foo`) while its provider
/// skill name stays `foo`, so `/foo` keeps working.
pub const SKILL_NAMESPACE_SEGMENT: &str = "skills";

/// Namespace segment every xprompt memory carries in its reference name.
///
/// A flat memory note `sase/memory/glossary.md` is referenced as
/// `#memory/glossary`. The prefix is mandatory: there is no bare `#glossary`
/// alias, and the whole `memory/` reference namespace is reserved so an
/// ordinary xprompt, workflow, config entry, plugin, or skill cannot
/// masquerade as an xprompt memory.
pub const MEMORY_NAMESPACE_SEGMENT: &str = "memory";

/// Filename that a memory root holds as generated documentation rather than as
/// a memory note, so catalog scanning must skip it.
pub const MEMORY_README_FILENAME: &str = "README.md";

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
    pub skills: LayoutPathWire,
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
    pub skills: LayoutPathWire,
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
    pub skills: LayoutPathWire,
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

/// One canonical source of xprompt-backed skill definitions.
///
/// Skill sources are source-controlled and first-wins in the listed priority
/// order, exactly like ordinary xprompt directory sources. There are
/// deliberately no legacy skill paths: skills moved to this layout in a single
/// hard cutover, so an old location is a migration diagnostic rather than a
/// readable source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSourceWire {
    pub id: String,
    pub priority: u32,
    pub scope: String,
    pub locator: String,
    pub path: Option<String>,
    pub formats: Vec<String>,
    pub tracking: LayoutTrackingWire,
    pub project_namespaced: bool,
    pub writable: bool,
    pub ordering: Option<String>,
}

/// Tier declared by a memory note's `type:` frontmatter.
///
/// A note that declares neither tier is not a memory note at all, so it is
/// never an xprompt memory either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryTierWire {
    Short,
    Long,
}

impl MemoryTierWire {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "short" => Some(Self::Short),
            "long" => Some(Self::Long),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Long => "long",
        }
    }
}

/// One scope's source of xprompt memories.
///
/// Memory keeps its existing compatible canonical/legacy contract, including
/// the exclusive read policy that makes split canonical/legacy state an error
/// instead of a merge. Sources are ordered project-before-home and are
/// first-wins across scopes, so the selected project's note shadows the
/// same-named home note. There are deliberately no plugin or package memory
/// sources: memory notes are source-controlled, human-reviewed content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySourceWire {
    pub id: String,
    pub priority: u32,
    pub scope: String,
    pub paths: CompatibleLayoutPathWire,
    pub formats: Vec<String>,
    pub tracking: LayoutTrackingWire,
    pub writable: bool,
    pub ordering: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaseContentLayoutWire {
    pub schema_version: u32,
    pub project: Option<ProjectContentLayoutWire>,
    pub home: HomeContentLayoutWire,
    pub chezmoi: Option<ChezmoiContentLayoutWire>,
    pub xprompt_sources: Vec<XpromptSourceWire>,
    pub skill_sources: Vec<SkillSourceWire>,
    pub memory_sources: Vec<MemorySourceWire>,
}

/// Why a definition was rejected by the xprompt-memory rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryXpromptRuleWire {
    /// A definition that is not a memory note claims a name in the reserved
    /// `memory/` reference namespace.
    ReservedNamespace,
    /// A memory note's filename stem cannot appear in an xprompt reference.
    InvalidStem,
    /// A file in a memory root declares no valid `type: short|long`.
    InvalidNoteType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryXpromptIssueWire {
    pub source: String,
    pub rule: MemoryXpromptRuleWire,
    pub message: String,
}

/// Build the canonical xprompt reference name for a memory note.
///
/// The note's flat filename stays its identity: `sase/memory/glossary.md` is
/// always `memory/glossary`, in every scope. The selected project is
/// contextual, so it is never part of the reference name.
pub fn memory_reference_name(stem: &str) -> String {
    format!("{MEMORY_NAMESPACE_SEGMENT}/{stem}")
}

/// Split a canonical memory reference name back into its note stem.
pub fn memory_reference_stem(name: &str) -> Option<&str> {
    let stem = name.strip_prefix(MEMORY_NAMESPACE_SEGMENT)?;
    let stem = stem.strip_prefix('/')?;
    (!stem.is_empty() && !stem.contains('/')).then_some(stem)
}

/// Whether `name` claims a reference in the reserved `memory/` namespace.
pub fn is_reserved_memory_reference(name: &str) -> bool {
    name.starts_with(&format!("{MEMORY_NAMESPACE_SEGMENT}/"))
}

/// Whether a memory note's filename stem can appear in an xprompt reference.
///
/// This is the ordinary xprompt reference-segment grammar, so an unreachable
/// name becomes an actionable diagnostic instead of a silently missing entry.
pub fn is_invokable_memory_stem(stem: &str) -> bool {
    let mut chars = stem.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Reject a non-memory definition that claims a reserved `memory/` reference.
pub fn reserved_memory_namespace_issue(
    source: &str,
    name: &str,
) -> Option<MemoryXpromptIssueWire> {
    is_reserved_memory_reference(name).then(|| MemoryXpromptIssueWire {
        source: source.to_string(),
        rule: MemoryXpromptRuleWire::ReservedNamespace,
        message: format!(
            "{source} claims the reserved xprompt-memory reference \
             `#{name}`; the `{MEMORY_NAMESPACE_SEGMENT}/` namespace only names \
             flat SASE memory notes, so rename this definition"
        ),
    })
}

/// Apply the xprompt-memory note rules to one file found in a memory root.
///
/// `note_type` is the raw `type:` frontmatter value, absent when the file
/// declares none. A rejected file is reported rather than silently skipped.
pub fn memory_note_issue(
    source: &str,
    stem: &str,
    note_type: Option<&str>,
) -> Option<MemoryXpromptIssueWire> {
    if note_type.and_then(MemoryTierWire::parse).is_none() {
        let declared = note_type
            .map(|value| format!("`{value}`"))
            .unwrap_or_else(|| "no `type:` value".to_string());
        return Some(MemoryXpromptIssueWire {
            source: source.to_string(),
            rule: MemoryXpromptRuleWire::InvalidNoteType,
            message: format!(
                "{source} declares {declared}; a SASE memory note must declare \
                 `type: short` or `type: long` to be an xprompt memory"
            ),
        });
    }
    (!is_invokable_memory_stem(stem)).then(|| MemoryXpromptIssueWire {
        source: source.to_string(),
        rule: MemoryXpromptRuleWire::InvalidStem,
        message: format!(
            "{source} cannot be referenced as \
             `#{}`: rename the note so its filename stem starts with a letter \
             or underscore and holds only letters, digits, and underscores",
            memory_reference_name(stem)
        ),
    })
}

/// Why a definition was rejected by the canonical skill placement rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillPlacementRuleWire {
    /// Lives in a canonical skill source but declares no truthy `skill` value.
    MissingSkillField,
    /// Declares a truthy `skill` value from an ordinary xprompt or config
    /// source instead of a canonical skill source.
    SkillOutsideSkillSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPlacementIssueWire {
    pub source: String,
    pub rule: SkillPlacementRuleWire,
    pub message: String,
    pub migrate_to: Option<String>,
}

/// Build the canonical xprompt reference name for a skill source.
///
/// The provider skill name stays `skill_name`; only the xprompt reference is
/// namespaced, so `#skills/foo` (or `#app/skills/foo`) expands what `/foo`
/// invokes.
pub fn skill_reference_name(project: Option<&str>, skill_name: &str) -> String {
    match project.filter(|project| !project.is_empty()) {
        Some(project) => {
            format!("{project}/{SKILL_NAMESPACE_SEGMENT}/{skill_name}")
        }
        None => format!("{SKILL_NAMESPACE_SEGMENT}/{skill_name}"),
    }
}

/// Split a canonical skill reference name back into project and skill name.
pub fn split_skill_reference_name(name: &str) -> Option<(Option<&str>, &str)> {
    let prefix = format!("{SKILL_NAMESPACE_SEGMENT}/");
    if let Some(skill) = name.strip_prefix(&prefix) {
        return (!skill.is_empty() && !skill.contains('/'))
            .then_some((None, skill));
    }
    let (project, rest) = name.split_once('/')?;
    let skill = rest.strip_prefix(&prefix)?;
    (!project.is_empty() && !skill.is_empty() && !skill.contains('/'))
        .then_some((Some(project), skill))
}

/// Apply the two-way skill placement rules to one loaded definition.
///
/// `migrate_to` is the destination the caller should offer for a misplaced
/// source: the scope's canonical skill directory when a skill declaration was
/// found outside one, and the scope's ordinary xprompt directory when a
/// canonical skill source holds a definition that is not a skill.
pub fn skill_placement_issue(
    source: &str,
    in_skill_source: bool,
    declares_skill: bool,
    migrate_to: Option<&str>,
) -> Option<SkillPlacementIssueWire> {
    let destination = |fallback: &str| {
        migrate_to
            .map(str::to_string)
            .unwrap_or_else(|| fallback.to_string())
    };
    match (in_skill_source, declares_skill) {
        (true, false) => Some(SkillPlacementIssueWire {
            source: source.to_string(),
            rule: SkillPlacementRuleWire::MissingSkillField,
            message: format!(
                "{source} is a canonical skill source but declares no truthy \
                 `skill:` value; add `skill: true` (or a provider list), or \
                 move it to {}",
                destination("the scope's sase/xprompts/ directory")
            ),
            migrate_to: migrate_to.map(str::to_string),
        }),
        (false, true) => Some(SkillPlacementIssueWire {
            source: source.to_string(),
            rule: SkillPlacementRuleWire::SkillOutsideSkillSource,
            message: format!(
                "{source} declares `skill:` outside a canonical skill source; \
                 move it to {}",
                destination("the scope's sase/skills/ directory")
            ),
            migrate_to: migrate_to.map(str::to_string),
        }),
        _ => None,
    }
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
    let skill_sources = skill_sources(project_root, home_root, project_name);
    let memory_sources = memory_sources(project_root, home_root);
    SaseContentLayoutWire {
        schema_version: CONTENT_LAYOUT_SCHEMA_VERSION,
        project,
        home,
        chezmoi,
        xprompt_sources,
        skill_sources,
        memory_sources,
    }
}

/// Ordered xprompt-memory sources, project before home.
fn memory_sources(
    project_root: Option<&Path>,
    home_root: &Path,
) -> Vec<MemorySourceWire> {
    let mut sources = Vec::new();
    if let Some(root) = project_root {
        sources.push(memory_source("project_memory", "project", root));
    }
    sources.push(memory_source("home_memory", "home", home_root));
    for (priority, source) in sources.iter_mut().enumerate() {
        source.priority = (priority + 1) as u32;
    }
    sources
}

fn memory_source(id: &str, scope: &str, root: &Path) -> MemorySourceWire {
    MemorySourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        paths: memory_compatible_path(root),
        formats: strings(&["md"]),
        tracking: LayoutTrackingWire::SourceControlled,
        writable: true,
        ordering: Some("first_wins".to_string()),
    }
}

/// The one canonical/legacy memory contract every scope shares.
fn memory_compatible_path(root: &Path) -> CompatibleLayoutPathWire {
    compatible_path(
        root.join("sase").join("memory"),
        [root.join("memory")],
        LayoutTrackingWire::SourceControlled,
        LayoutCollisionPolicyWire::Error,
    )
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
    let memory = memory_compatible_path(root);
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
        skills: skills_layout_path(&namespace_root),
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
    let memory = memory_compatible_path(root);
    HomeContentLayoutWire {
        root: path_string(root),
        namespace_root: layout_path(
            namespace_root.clone(),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::SourceControlled,
        ),
        xprompts,
        skills: skills_layout_path(&namespace_root),
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
    let memory = memory_compatible_path(root);
    ChezmoiContentLayoutWire {
        source_root: path_string(root),
        namespace_root: layout_path(
            namespace_root.clone(),
            LayoutPathRoleWire::Canonical,
            LayoutTrackingWire::SourceControlled,
        ),
        xprompts,
        skills: skills_layout_path(&namespace_root),
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

fn skill_sources(
    project_root: Option<&Path>,
    home_root: &Path,
    project_name: Option<&str>,
) -> Vec<SkillSourceWire> {
    let mut sources = Vec::new();
    if let Some(root) = project_root {
        push_skill_directory_source(
            &mut sources,
            "project_skills",
            "project",
            root.join("sase").join(SKILL_NAMESPACE_SEGMENT),
            true,
        );
    }
    push_skill_directory_source(
        &mut sources,
        "home_skills",
        "home",
        home_root.join("sase").join(SKILL_NAMESPACE_SEGMENT),
        false,
    );
    if let Some(project_name) = project_name.filter(|name| !name.is_empty()) {
        push_skill_directory_source(
            &mut sources,
            "home_project_skills",
            "home_project",
            home_root
                .join("sase")
                .join(SKILL_NAMESPACE_SEGMENT)
                .join(project_name),
            true,
        );
    }
    push_skill_symbolic_source(
        &mut sources,
        "plugin_skills",
        "plugin",
        "entrypoint:sase_xprompts/skills",
    );
    push_skill_symbolic_source(
        &mut sources,
        "package_skills",
        "package",
        "package:skills",
    );

    for (priority, source) in sources.iter_mut().enumerate() {
        source.priority = (priority + 1) as u32;
    }
    sources
}

fn push_skill_directory_source(
    sources: &mut Vec<SkillSourceWire>,
    id: &str,
    scope: &str,
    path: PathBuf,
    project_namespaced: bool,
) {
    let path = path_string(&path);
    sources.push(SkillSourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        locator: path.clone(),
        path: Some(path),
        formats: strings(&["md"]),
        tracking: LayoutTrackingWire::SourceControlled,
        project_namespaced,
        writable: true,
        ordering: Some("first_wins".to_string()),
    });
}

fn push_skill_symbolic_source(
    sources: &mut Vec<SkillSourceWire>,
    id: &str,
    scope: &str,
    locator: &str,
) {
    sources.push(SkillSourceWire {
        id: id.to_string(),
        priority: 0,
        scope: scope.to_string(),
        locator: locator.to_string(),
        path: None,
        formats: strings(&["md"]),
        tracking: LayoutTrackingWire::PackageOwned,
        project_namespaced: false,
        writable: false,
        ordering: Some("first_wins".to_string()),
    });
}

fn skills_layout_path(namespace_root: &Path) -> LayoutPathWire {
    layout_path(
        namespace_root.join(SKILL_NAMESPACE_SEGMENT),
        LayoutPathRoleWire::Canonical,
        LayoutTrackingWire::SourceControlled,
    )
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
    fn skill_directories_are_canonical_in_every_scope() {
        let layout = sase_content_layout(
            Some(Path::new("/workspace/project")),
            Path::new("/home/alice"),
            Some(Path::new("/dotfiles/home")),
            Some("project"),
        );
        let project = layout.project.unwrap();
        assert_eq!(project.skills.path, "/workspace/project/sase/skills");
        assert_eq!(project.skills.role, LayoutPathRoleWire::Canonical);
        assert_eq!(
            project.skills.tracking,
            LayoutTrackingWire::SourceControlled
        );
        assert_eq!(layout.home.skills.path, "/home/alice/sase/skills");
        assert_eq!(
            layout.chezmoi.unwrap().skills.path,
            "/dotfiles/home/sase/skills"
        );
    }

    #[test]
    fn skill_sources_are_ordered_first_wins_with_no_legacy_paths() {
        let layout = sase_content_layout(
            Some(Path::new("/repo")),
            Path::new("/home/alice"),
            None,
            Some("demo"),
        );
        let ids = layout
            .skill_sources
            .iter()
            .map(|source| source.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "project_skills",
                "home_skills",
                "home_project_skills",
                "plugin_skills",
                "package_skills",
            ]
        );
        assert_eq!(
            layout
                .skill_sources
                .iter()
                .map(|source| source.priority)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(
            layout.skill_sources[0].path.as_deref(),
            Some("/repo/sase/skills")
        );
        assert_eq!(
            layout.skill_sources[1].path.as_deref(),
            Some("/home/alice/sase/skills")
        );
        assert_eq!(
            layout.skill_sources[2].path.as_deref(),
            Some("/home/alice/sase/skills/demo")
        );
        assert!(layout.skill_sources[2].project_namespaced);
        assert!(!layout.skill_sources[1].project_namespaced);
        // Package and plugin sources are symbolic and read-only.
        assert_eq!(
            layout.skill_sources[3].locator,
            "entrypoint:sase_xprompts/skills"
        );
        assert_eq!(layout.skill_sources[4].locator, "package:skills");
        assert!(layout.skill_sources[3..]
            .iter()
            .all(|source| source.path.is_none() && !source.writable));
        assert!(layout
            .skill_sources
            .iter()
            .all(|source| source.formats == strings(&["md"])
                && source.ordering.as_deref() == Some("first_wins")));
    }

    #[test]
    fn skill_reference_names_split_provider_name_from_xprompt_reference() {
        assert_eq!(skill_reference_name(None, "foo"), "skills/foo");
        assert_eq!(skill_reference_name(Some("app"), "foo"), "app/skills/foo");
        assert_eq!(
            split_skill_reference_name("skills/foo"),
            Some((None, "foo"))
        );
        assert_eq!(
            split_skill_reference_name("app/skills/foo"),
            Some((Some("app"), "foo"))
        );
        // A bare or oddly shaped reference is not a skill reference.
        assert_eq!(split_skill_reference_name("foo"), None);
        assert_eq!(split_skill_reference_name("app/foo"), None);
        assert_eq!(split_skill_reference_name("skills/"), None);
        assert_eq!(split_skill_reference_name("a/skills/b/c"), None);
    }

    #[test]
    fn skill_placement_issues_name_the_move_in_both_directions() {
        assert_eq!(
            skill_placement_issue("sase/skills/foo.md", true, true, None),
            None
        );
        assert_eq!(
            skill_placement_issue("sase/xprompts/foo.md", false, false, None),
            None
        );

        let orphan =
            skill_placement_issue("sase/skills/foo.md", true, false, None)
                .unwrap();
        assert_eq!(orphan.rule, SkillPlacementRuleWire::MissingSkillField);
        assert!(orphan.message.contains("sase/skills/foo.md"));
        assert!(orphan.message.contains("skill: true"));

        let misplaced = skill_placement_issue(
            "sase/xprompts/foo.md",
            false,
            true,
            Some("/repo/sase/skills"),
        )
        .unwrap();
        assert_eq!(
            misplaced.rule,
            SkillPlacementRuleWire::SkillOutsideSkillSource
        );
        assert_eq!(misplaced.migrate_to.as_deref(), Some("/repo/sase/skills"));
        assert!(misplaced.message.contains("/repo/sase/skills"));
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
        assert_eq!(layout.home.skills.path, "/home/alice/sase/skills");
        assert_eq!(
            layout
                .skill_sources
                .iter()
                .map(|source| source.id.as_str())
                .collect::<Vec<_>>(),
            vec!["home_skills", "plugin_skills", "package_skills"]
        );
        assert_eq!(
            layout
                .memory_sources
                .iter()
                .map(|source| source.id.as_str())
                .collect::<Vec<_>>(),
            vec!["home_memory"]
        );
    }

    #[test]
    fn memory_sources_are_project_before_home_with_exclusive_read_policy() {
        let layout = sase_content_layout(
            Some(Path::new("/repo")),
            Path::new("/home/alice"),
            None,
            Some("demo"),
        );

        let ids = layout
            .memory_sources
            .iter()
            .map(|source| source.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["project_memory", "home_memory"]);
        assert_eq!(
            layout
                .memory_sources
                .iter()
                .map(|source| source.priority)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let project = &layout.memory_sources[0];
        assert_eq!(project.paths.canonical.path, "/repo/sase/memory");
        assert_eq!(project.paths.legacy[0].path, "/repo/memory");
        assert_eq!(project.paths.write_path, "/repo/sase/memory");
        // Split canonical/legacy memory state stays an error, exactly as the
        // memory subsystem already reports it.
        assert_eq!(project.paths.read_policy, LayoutCollisionPolicyWire::Error);
        assert_eq!(project.formats, strings(&["md"]));
        assert_eq!(project.tracking, LayoutTrackingWire::SourceControlled);
        assert_eq!(
            layout.memory_sources[1].paths.canonical.path,
            "/home/alice/sase/memory"
        );
        // The selected project is contextual, so no source is project
        // namespaced and no plugin or package scope contributes memory.
        assert!(layout.memory_sources.iter().all(|source| matches!(
            source.scope.as_str(),
            "project" | "home"
        ) && source
            .ordering
            .as_deref()
            == Some("first_wins")));
    }

    #[test]
    fn memory_references_are_always_the_flat_namespaced_filename() {
        assert_eq!(memory_reference_name("glossary"), "memory/glossary");
        assert_eq!(memory_reference_stem("memory/glossary"), Some("glossary"));
        // There is no bare alias, no historical `memory/long/<name>` form, and
        // no project-qualified memory reference.
        assert_eq!(memory_reference_stem("glossary"), None);
        assert_eq!(memory_reference_stem("memory/long/glossary"), None);
        assert_eq!(memory_reference_stem("app/memory/glossary"), None);
        assert_eq!(memory_reference_stem("memory/"), None);
        assert!(is_reserved_memory_reference("memory/glossary"));
        assert!(!is_reserved_memory_reference("memories/glossary"));
        assert!(is_invokable_memory_stem("_tui_perf2"));
        assert!(!is_invokable_memory_stem("build-and-run"));
        assert!(!is_invokable_memory_stem("2fast"));
        assert!(!is_invokable_memory_stem(""));
    }

    #[test]
    fn memory_rules_reject_reserved_names_bad_stems_and_bad_tiers() {
        let reserved = reserved_memory_namespace_issue(
            "config xprompt `memory/glossary`",
            "memory/glossary",
        )
        .unwrap();
        assert_eq!(reserved.rule, MemoryXpromptRuleWire::ReservedNamespace);
        assert!(
            reserved.message.contains("#memory/glossary"),
            "{reserved:?}"
        );
        assert_eq!(
            reserved_memory_namespace_issue("sase/xprompts/foo.md", "foo"),
            None
        );

        assert_eq!(
            memory_note_issue(
                "sase/memory/glossary.md",
                "glossary",
                Some("short")
            ),
            None
        );
        assert_eq!(
            memory_note_issue("sase/memory/sase.md", "sase", Some("long")),
            None
        );
        let bad_type =
            memory_note_issue("sase/memory/notes.md", "notes", Some("medium"))
                .unwrap();
        assert_eq!(bad_type.rule, MemoryXpromptRuleWire::InvalidNoteType);
        let missing_type =
            memory_note_issue("sase/memory/notes.md", "notes", None).unwrap();
        assert_eq!(missing_type.rule, MemoryXpromptRuleWire::InvalidNoteType);
        let bad_stem =
            memory_note_issue("sase/memory/a-b.md", "a-b", Some("long"))
                .unwrap();
        assert_eq!(bad_stem.rule, MemoryXpromptRuleWire::InvalidStem);
        assert!(bad_stem.message.contains("#memory/a-b"), "{bad_stem:?}");
    }

    #[test]
    fn memory_tier_parses_only_the_two_supported_note_types() {
        assert_eq!(MemoryTierWire::parse("short"), Some(MemoryTierWire::Short));
        assert_eq!(MemoryTierWire::parse(" long "), Some(MemoryTierWire::Long));
        assert_eq!(MemoryTierWire::parse("dynamic"), None);
        assert_eq!(MemoryTierWire::Long.as_str(), "long");
    }
}
