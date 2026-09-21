//! Shared fixtures for completion tests.

use super::super::*;
use crate::editor::wire::{
    AgentCompletionEntry, EditorPosition, XpromptAssistEntry,
};
use crate::{
    EditorXpromptCatalogEntryWire, MemoryTierWire, MobileXpromptInputWire,
};
pub(super) fn pos(character: u32) -> EditorPosition {
    EditorPosition { line: 0, character }
}
pub(super) fn agent_target(
    name: &str,
    kind: &str,
    member_count: usize,
    detail: &str,
) -> AgentCompletionEntry {
    AgentCompletionEntry {
        name: name.to_string(),
        status: String::new(),
        project: String::new(),
        kind: kind.to_string(),
        member_count,
        detail: detail.to_string(),
        documentation: String::new(),
    }
}
pub(super) fn entries() -> Vec<XpromptAssistEntry> {
    assist_entries_from_catalog(&[
        EditorXpromptCatalogEntryWire {
            name: "review".to_string(),
            display_label: "review".to_string(),
            insertion: Some("#review".to_string()),
            reference_prefix: Some("#".to_string()),
            kind: Some("xprompt".to_string()),
            description: Some("Review code".to_string()),
            source_bucket: "builtin".to_string(),
            project: None,
            tags: vec![],
            input_signature: Some("(path: path, deep?: bool)".to_string()),
            inputs: vec![
                MobileXpromptInputWire {
                    name: "path".to_string(),
                    r#type: "path".to_string(),
                    description: Some("Path to review".to_string()),
                    required: true,
                    default_display: None,
                    position: 0,
                    repeatable: false,
                    choices: Vec::new(),
                },
                MobileXpromptInputWire {
                    name: "deep".to_string(),
                    r#type: "bool".to_string(),
                    description: Some("Run a deeper pass".to_string()),
                    required: false,
                    default_display: Some("false".to_string()),
                    position: 1,
                    repeatable: false,
                    choices: Vec::new(),
                },
            ],
            is_skill: false,
            skill_name: None,
            memory_type: None,
            content_preview: Some("review body".to_string()),
            source_path_display: Some("sase/xprompts/review.md".to_string()),
            definition_path: Some("/tmp/sase/xprompts/review.md".to_string()),
            definition_range: None,
        },
        EditorXpromptCatalogEntryWire {
            name: "run".to_string(),
            display_label: "run".to_string(),
            insertion: Some("#!run".to_string()),
            reference_prefix: Some("#!".to_string()),
            kind: Some("workflow".to_string()),
            description: None,
            source_bucket: "project".to_string(),
            project: None,
            tags: vec![],
            input_signature: None,
            inputs: vec![],
            is_skill: false,
            skill_name: None,
            memory_type: None,
            content_preview: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
        },
        EditorXpromptCatalogEntryWire {
            name: "memory/glossary".to_string(),
            display_label: "memory/glossary".to_string(),
            insertion: Some("#memory/glossary".to_string()),
            reference_prefix: Some("#".to_string()),
            kind: Some("memory".to_string()),
            description: Some("SASE terms".to_string()),
            source_bucket: "project".to_string(),
            project: None,
            tags: vec![],
            input_signature: None,
            inputs: vec![],
            is_skill: false,
            skill_name: None,
            memory_type: Some(MemoryTierWire::Core),
            content_preview: Some("Glossary body".to_string()),
            source_path_display: Some("sase/memory/glossary.md".to_string()),
            definition_path: Some("/tmp/sase/memory/glossary.md".to_string()),
            definition_range: None,
        },
        EditorXpromptCatalogEntryWire {
            name: "skill/plan".to_string(),
            display_label: "skill/plan".to_string(),
            insertion: Some("#skill/plan".to_string()),
            reference_prefix: Some("#".to_string()),
            kind: Some("xprompt".to_string()),
            description: None,
            source_bucket: "builtin".to_string(),
            project: None,
            tags: vec![],
            input_signature: None,
            inputs: vec![],
            is_skill: true,
            skill_name: Some("plan".to_string()),
            memory_type: None,
            content_preview: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
        },
    ])
}
