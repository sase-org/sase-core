use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use super::definition::filter_structured_sources;
use super::entries::{structured_entry, workflow_prompt_part};
use super::loader::CatalogLoader;
use super::parsing::{known_projects, macro_to_workflow, yaml_child_key_range};
use super::types::{
    CatalogStep, CatalogWorkflow, DefinitionSection, StepKind,
    StructuredSource, SKILL_FRAME_TEMPLATE_FILENAME,
};
use super::*;
use crate::{
    EditorMacroCatalogRequestWire, EditorSnippetCatalogRequestWire,
    MemoryTierWire, MobileMacroCatalogEntryWire, MobileMacroCatalogStatsWire,
};

mod authored_inputs;
mod catalog_sources;
mod loading;
mod memory;
mod parsing;
mod projects;
mod skill_definition;
mod snippets;

fn request() -> EditorMacroCatalogRequestWire {
    EditorMacroCatalogRequestWire {
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
fn definition_line(entry: &MobileMacroCatalogEntryWire) -> Option<u32> {
    entry.definition_range.map(|range| range.start.line)
}
fn write_memory_note(root: &Path, name: &str, contents: &str) {
    let memory = root.join("sase/memory");
    fs::create_dir_all(&memory).unwrap();
    fs::write(memory.join(name), contents).unwrap();
}
