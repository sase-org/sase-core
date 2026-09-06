//! Temporary offline migration residue classifier.
//!
//! Deletion owner: sase-x7.14.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::MIGRATION_WIRE_SCHEMA_VERSION;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationResidueEntryWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub entry_id: String,
    pub residue_path: String,
    pub canonical_counterpart: String,
    #[serde(default)]
    pub precondition_query: Option<JsonValue>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationResidueFactsWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub residue_exists: bool,
    pub counterpart_exists: bool,
    #[serde(default)]
    pub live_references: Vec<String>,
    #[serde(default)]
    pub archived: bool,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationResidueDecisionWire {
    Archive,
    #[default]
    AlreadyDone,
    RefuseMissingCounterpart,
    RefuseLiveReference,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationResidueClassificationWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub entry_id: String,
    pub residue_path: String,
    pub canonical_counterpart: String,
    pub decision: MigrationResidueDecisionWire,
    pub reason: String,
    #[serde(default)]
    pub live_references: Vec<String>,
}

pub fn classify(
    entry: &MigrationResidueEntryWire,
    facts: &MigrationResidueFactsWire,
) -> MigrationResidueClassificationWire {
    let (decision, reason) = if !facts.residue_exists || facts.archived {
        (
            MigrationResidueDecisionWire::AlreadyDone,
            "residue is already absent or archived".to_string(),
        )
    } else if !facts.counterpart_exists {
        (
            MigrationResidueDecisionWire::RefuseMissingCounterpart,
            "canonical counterpart is missing".to_string(),
        )
    } else if !facts.live_references.is_empty() {
        (
            MigrationResidueDecisionWire::RefuseLiveReference,
            "live records still reference the residue".to_string(),
        )
    } else {
        (
            MigrationResidueDecisionWire::Archive,
            "residue is inert and has a canonical counterpart".to_string(),
        )
    };

    MigrationResidueClassificationWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        entry_id: entry.entry_id.clone(),
        residue_path: entry.residue_path.clone(),
        canonical_counterpart: entry.canonical_counterpart.clone(),
        decision,
        reason,
        live_references: facts.live_references.clone(),
    }
}

pub fn classify_many(
    entries: &[MigrationResidueEntryWire],
    facts_by_entry: &BTreeMap<String, MigrationResidueFactsWire>,
) -> Vec<MigrationResidueClassificationWire> {
    entries
        .iter()
        .map(|entry| {
            let facts = facts_by_entry
                .get(&entry.entry_id)
                .cloned()
                .unwrap_or_default();
            classify(entry, &facts)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> MigrationResidueEntryWire {
        MigrationResidueEntryWire {
            entry_id: "agent-tags".to_string(),
            residue_path: "~/.sase/agent_tags.json".to_string(),
            canonical_counterpart: "~/.sase/agents".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn classifier_archives_only_inert_residue_with_counterpart() {
        let classification = classify(
            &entry(),
            &MigrationResidueFactsWire {
                residue_exists: true,
                counterpart_exists: true,
                ..Default::default()
            },
        );
        assert_eq!(
            classification.decision,
            MigrationResidueDecisionWire::Archive
        );
    }

    #[test]
    fn classifier_refuses_live_references() {
        let classification = classify(
            &entry(),
            &MigrationResidueFactsWire {
                residue_exists: true,
                counterpart_exists: true,
                live_references: vec!["gate:abc".to_string()],
                ..Default::default()
            },
        );
        assert_eq!(
            classification.decision,
            MigrationResidueDecisionWire::RefuseLiveReference
        );
    }
}
