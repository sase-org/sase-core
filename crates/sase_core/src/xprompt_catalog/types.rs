use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{content_layout::MemoryTierWire, MobileInputChoiceWire};

pub(super) const MAX_CONTENT_PREVIEW_CHARS: usize = 500;
pub(super) const SCHEMA_VERSION: u32 = 1;
pub const XPROMPT_SKILL_DEFINITION_WIRE_SCHEMA_VERSION: u64 = 1;
pub(super) const SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_DIRS_JSON";
pub(super) const SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON";
pub(super) const SASE_SKILL_PLUGIN_DIRS_JSON_ENV: &str =
    "SASE_SKILL_PLUGIN_DIRS_JSON";

/// The packaged Jinja frame that generated `SKILL.md` files are rendered
/// through. It ships beside the bundled skill sources but is a template, not a
/// skill, so scanning must skip it rather than report it as misplaced.
pub(super) const SKILL_FRAME_TEMPLATE_FILENAME: &str =
    "SKILL.frame.template.md";

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
    pub package_xprompts_dir: Option<PathBuf>,
    pub package_skills_dir: Option<PathBuf>,
    pub default_xprompts_dir: Option<PathBuf>,
    pub default_config_path: Option<PathBuf>,
    pub plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    pub plugin_skill_dirs: BTreeMap<String, PathBuf>,
    pub plugin_config_paths: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct XpromptCatalogResourcePaths {
    pub package_xprompts_dir: Option<PathBuf>,
    pub package_skills_dir: Option<PathBuf>,
    pub default_xprompts_dir: Option<PathBuf>,
    pub default_config_path: Option<PathBuf>,
    pub plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    pub plugin_skill_dirs: BTreeMap<String, PathBuf>,
    pub plugin_config_paths: BTreeMap<String, PathBuf>,
}

impl XpromptCatalogLoadOptions {
    pub fn new(root_dir: Option<PathBuf>) -> Self {
        Self {
            root_dir,
            package_xprompts_dir: None,
            package_skills_dir: None,
            default_xprompts_dir: None,
            default_config_path: None,
            plugin_xprompt_dirs: BTreeMap::new(),
            plugin_skill_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
        }
    }

    pub fn with_resource_paths(
        mut self,
        resource_paths: XpromptCatalogResourcePaths,
    ) -> Self {
        self.package_xprompts_dir = resource_paths.package_xprompts_dir;
        self.package_skills_dir = resource_paths.package_skills_dir;
        self.default_xprompts_dir = resource_paths.default_xprompts_dir;
        self.default_config_path = resource_paths.default_config_path;
        self.plugin_xprompt_dirs = resource_paths.plugin_xprompt_dirs;
        self.plugin_skill_dirs = resource_paths.plugin_skill_dirs;
        self.plugin_config_paths = resource_paths.plugin_config_paths;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptSkillDefinitionRequestWire {
    #[serde(default = "xprompt_skill_definition_schema_version")]
    pub schema_version: u64,
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptSkillDefinitionCandidateWire {
    pub reference: String,
    pub skill_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptSkillDefinitionResolutionWire {
    pub schema_version: u64,
    pub status: String,
    pub authored_reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_path: Option<String>,
    #[serde(default)]
    pub candidates: Vec<XpromptSkillDefinitionCandidateWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

fn xprompt_skill_definition_schema_version() -> u64 {
    XPROMPT_SKILL_DEFINITION_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogInput {
    pub(super) name: String,
    pub(super) type_name: String,
    pub(super) description: Option<String>,
    pub(super) required: bool,
    pub(super) default_display: Option<String>,
    pub(super) default_snippet_value: Option<String>,
    pub(super) is_step_input: bool,
    pub(super) repeatable: bool,
    pub(super) choices: Vec<MobileInputChoiceWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StepKind {
    Agent,
    Bash,
    Python,
    PromptPart,
    Parallel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogStep {
    pub(super) name: String,
    pub(super) kind: StepKind,
    pub(super) prompt_part: Option<String>,
    pub(super) has_output: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogWorkflow {
    pub(super) name: String,
    pub(super) inputs: Vec<CatalogInput>,
    pub(super) steps: Vec<CatalogStep>,
    pub(super) local_xprompts: Vec<CatalogXprompt>,
    pub(super) source_path: Option<String>,
    pub(super) tags: BTreeSet<String>,
    pub(super) description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogXprompt {
    pub(super) name: String,
    pub(super) content: String,
    pub(super) inputs: Vec<CatalogInput>,
    pub(super) local_xprompts: Vec<CatalogXprompt>,
    pub(super) source_path: Option<String>,
    pub(super) tags: BTreeSet<String>,
    pub(super) description: Option<String>,
    pub(super) is_skill: bool,
    pub(super) skill_name: Option<String>,
    /// Tier of the SASE memory note this entry was loaded from. A non-null
    /// value is the authoritative marker that the entry is an xprompt memory.
    pub(super) memory_type: Option<MemoryTierWire>,
    pub(super) snippet: Option<CatalogSnippet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CatalogSnippet {
    Enabled,
    Trigger(String),
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct PluginPathEntry {
    pub(super) module: String,
    pub(super) path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StructuredSource {
    pub(super) name: String,
    pub(super) workflow: CatalogWorkflow,
    pub(super) bucket: String,
    pub(super) project: Option<String>,
    pub(super) description: Option<String>,
    pub(super) is_skill: bool,
    pub(super) skill_name: Option<String>,
    pub(super) memory_type: Option<MemoryTierWire>,
    pub(super) content: String,
    pub(super) definition_section: DefinitionSection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkflowKind {
    SimpleXprompt,
    EmbeddableWorkflow,
    StandaloneWorkflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DefinitionSection {
    Xprompts,
    Workflows,
}

impl DefinitionSection {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Xprompts => "xprompts",
            Self::Workflows => "workflows",
        }
    }
}
