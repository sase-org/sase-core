//! Xprompt and snippet catalog loading and resolution.
mod definition;
mod entries;
mod loader;
mod loader_sources;
mod parsing;
mod types;

pub use definition::resolve_macro_skill_definition;
pub use entries::{load_editor_macro_catalog, load_editor_snippet_catalog};
pub use types::{
    MacroCatalogLoadError, MacroCatalogLoadOptions, MacroCatalogResourcePaths,
    MacroSkillDefinitionCandidateWire, MacroSkillDefinitionRequestWire,
    MacroSkillDefinitionResolutionWire,
    MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
