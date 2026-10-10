use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{content_layout::MemoryTierWire, MobileInputChoiceWire};

pub(super) const MAX_CONTENT_PREVIEW_CHARS: usize = 500;
pub(super) const SCHEMA_VERSION: u32 = 1;
pub const MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION: u64 = 1;
pub(super) const SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON";
pub(super) const SASE_SKILL_PLUGIN_DIRS_JSON_ENV: &str =
    "SASE_SKILL_PLUGIN_DIRS_JSON";
// Canonical macro transport variables, resolved new-first with the
// corresponding `SASE_XPROMPT_*` fallback.
// legacy xprompt spelling (fallback env names)
pub(super) const SASE_MACRO_PACKAGE_DIR_ENV: &str = "SASE_MACRO_PACKAGE_DIR";
pub(super) const SASE_MACRO_BUILTIN_DIR_ENV: &str = "SASE_MACRO_BUILTIN_DIR";
pub(super) const SASE_MACRO_DEFAULT_DIR_ENV: &str = "SASE_MACRO_DEFAULT_DIR";
pub(super) const SASE_MACRO_PLUGIN_DIRS_JSON_ENV: &str =
    "SASE_MACRO_PLUGIN_DIRS_JSON";
pub(super) const SASE_MACRO_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_MACRO_PLUGIN_CONFIG_PATHS_JSON";
pub const SASE_MACRO_PLUGIN_INPUT_TYPES_JSON_ENV: &str =
    "SASE_MACRO_PLUGIN_INPUT_TYPES_JSON";

/// The packaged Jinja frame that generated `SKILL.md` files are rendered
/// through. It ships beside the bundled skill sources but is a template, not a
/// skill, so scanning must skip it rather than report it as misplaced.
pub(super) const SKILL_FRAME_TEMPLATE_FILENAME: &str =
    "SKILL.frame.template.md";

#[derive(Debug, Error)]
pub enum MacroCatalogLoadError {
    #[error("failed to read macro catalog: {0}")]
    Read(String),
    #[error("macro catalog layout collision: {0}")]
    LayoutCollision(String),
    #[error(
        "duplicate macro definition keys `xprompts` and `macros` in {0}; keep only `macros`"
    )]
    DuplicateAuthoredKeys(String),
    #[error(
        "retired authored `xprompts` key in {0}; rename it to `macros` or reload with legacy names accepted"
    )]
    RetiredAuthoredKey(String),
    #[error("malformed authored `macros` section in {0}; expected a mapping")]
    MalformedAuthoredSection(String),
}

fn default_accept_legacy_xprompt_names() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroCatalogLoadOptions {
    pub root_dir: Option<PathBuf>,
    pub package_macros_dir: Option<PathBuf>,
    pub package_skills_dir: Option<PathBuf>,
    pub default_macros_dir: Option<PathBuf>,
    pub default_config_path: Option<PathBuf>,
    pub plugin_macro_dirs: BTreeMap<String, PathBuf>,
    pub plugin_skill_dirs: BTreeMap<String, PathBuf>,
    pub plugin_config_paths: BTreeMap<String, PathBuf>,
    pub plugin_input_type_files:
        Vec<crate::macro_input_types::PluginInputTypeFileRecord>,
    pub accept_legacy_xprompt_names: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MacroCatalogResourcePaths {
    pub package_macros_dir: Option<PathBuf>,
    pub package_skills_dir: Option<PathBuf>,
    pub default_macros_dir: Option<PathBuf>,
    pub default_config_path: Option<PathBuf>,
    pub plugin_macro_dirs: BTreeMap<String, PathBuf>,
    pub plugin_skill_dirs: BTreeMap<String, PathBuf>,
    pub plugin_config_paths: BTreeMap<String, PathBuf>,
}

impl Default for MacroCatalogLoadOptions {
    fn default() -> Self {
        Self::new(None)
    }
}

impl MacroCatalogLoadOptions {
    pub fn new(root_dir: Option<PathBuf>) -> Self {
        Self {
            root_dir,
            package_macros_dir: None,
            package_skills_dir: None,
            default_macros_dir: None,
            default_config_path: None,
            plugin_macro_dirs: BTreeMap::new(),
            plugin_skill_dirs: BTreeMap::new(),
            plugin_config_paths: BTreeMap::new(),
            plugin_input_type_files: Vec::new(),
            accept_legacy_xprompt_names: default_accept_legacy_xprompt_names(),
        }
    }

    pub fn with_resource_paths(
        mut self,
        resource_paths: MacroCatalogResourcePaths,
    ) -> Self {
        self.package_macros_dir = resource_paths.package_macros_dir;
        self.package_skills_dir = resource_paths.package_skills_dir;
        self.default_macros_dir = resource_paths.default_macros_dir;
        self.default_config_path = resource_paths.default_config_path;
        self.plugin_macro_dirs = resource_paths.plugin_macro_dirs;
        self.plugin_skill_dirs = resource_paths.plugin_skill_dirs;
        self.plugin_config_paths = resource_paths.plugin_config_paths;
        self
    }

    /// Retired rollout switch, kept for wire compatibility and ignored:
    /// the loader always accepts retired xprompt sources.
    pub fn with_legacy_policy(mut self, accept: bool) -> Self {
        self.accept_legacy_xprompt_names = accept;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroSkillDefinitionRequestWire {
    #[serde(default = "macro_skill_definition_schema_version")]
    pub schema_version: u64,
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroSkillDefinitionCandidateWire {
    pub reference: String,
    pub skill_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroSkillDefinitionResolutionWire {
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
    pub candidates: Vec<MacroSkillDefinitionCandidateWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

fn macro_skill_definition_schema_version() -> u64 {
    MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION
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
    pub(super) named_type: Option<String>,
    pub(super) value_role: Option<String>,
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
    pub(super) local_macros: Vec<CatalogMacro>,
    pub(super) source_path: Option<String>,
    pub(super) tags: BTreeSet<String>,
    pub(super) description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogMacro {
    pub(super) name: String,
    pub(super) content: String,
    pub(super) inputs: Vec<CatalogInput>,
    pub(super) local_macros: Vec<CatalogMacro>,
    pub(super) source_path: Option<String>,
    pub(super) tags: BTreeSet<String>,
    pub(super) description: Option<String>,
    pub(super) is_skill: bool,
    pub(super) skill_name: Option<String>,
    /// Tier of the SASE memory note this entry was loaded from. A non-null
    /// value is the authoritative marker that the entry is a macro memory.
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
    SimpleMacro,
    EmbeddableWorkflow,
    StandaloneWorkflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DefinitionSection {
    Macros,
    Workflows,
}

impl DefinitionSection {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Macros => "macros",
            Self::Workflows => "workflows",
        }
    }
}
