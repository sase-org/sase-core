mod definition;
mod entries;
mod loader;
mod loader_sources;
mod parsing;
mod types;

pub use definition::resolve_xprompt_skill_definition;
pub use entries::{load_editor_snippet_catalog, load_editor_xprompt_catalog};
pub use types::{
    XpromptCatalogLoadError, XpromptCatalogLoadOptions,
    XpromptCatalogResourcePaths, XpromptSkillDefinitionCandidateWire,
    XpromptSkillDefinitionRequestWire, XpromptSkillDefinitionResolutionWire,
    XPROMPT_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
