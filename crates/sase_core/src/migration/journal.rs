//! Temporary offline migration journal replay contract.
//!
//! Deletion owner: sase-x7.14.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::manifest::MigrationManifest;
use super::MIGRATION_WIRE_SCHEMA_VERSION;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationJournalStateWire {
    #[default]
    Planned,
    BackedUp,
    Applying,
    Applied,
    Verified,
    Failed,
    Refused,
}

impl MigrationJournalStateWire {
    fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::BackedUp => 1,
            Self::Applying => 2,
            Self::Applied => 3,
            Self::Verified => 4,
            Self::Failed => 5,
            Self::Refused => 5,
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Verified | Self::Failed | Self::Refused)
    }

    fn next_step(self) -> &'static str {
        match self {
            Self::Planned => "backup",
            Self::BackedUp => "apply",
            Self::Applying => "apply",
            Self::Applied => "verify",
            Self::Verified => "complete",
            Self::Failed => "failed",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationJournalRecord {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub step_id: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    #[serde(default)]
    pub recorded_at: Option<String>,
    pub state: MigrationJournalStateWire,
    #[serde(default)]
    pub source_digests: BTreeMap<String, String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub refusal: Option<MigrationRefusalWire>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRefusalWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub reason: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationDigestMismatchWire {
    pub source: String,
    pub expected: String,
    #[serde(default)]
    pub observed: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationResumePlanWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub current_state: MigrationJournalStateWire,
    pub next_step: String,
    pub terminal: bool,
    pub refused: bool,
    #[serde(default)]
    pub refusal_reason: Option<String>,
    #[serde(default)]
    pub digest_mismatches: Vec<MigrationDigestMismatchWire>,
    #[serde(default)]
    pub last_record: Option<MigrationJournalRecord>,
}

pub fn plan_next_step(
    manifest: &MigrationManifest,
    records: &[MigrationJournalRecord],
    observed_source_digests: &BTreeMap<String, String>,
) -> MigrationResumePlanWire {
    let mismatches = digest_mismatches(manifest, observed_source_digests);
    if !mismatches.is_empty() {
        return refused_plan(
            MigrationJournalStateWire::Refused,
            "source digest changed since manifest was written".to_string(),
            mismatches,
            records.last().cloned(),
        );
    }

    let mut current_state = MigrationJournalStateWire::Planned;
    let mut previous_terminal = false;
    for record in records {
        if previous_terminal && record.state != current_state {
            return refused_plan(
                MigrationJournalStateWire::Refused,
                format!(
                    "journal appended {:?} after terminal {:?}",
                    record.state, current_state
                ),
                Vec::new(),
                Some(record.clone()),
            );
        }
        if record.state.rank() < current_state.rank() {
            return refused_plan(
                MigrationJournalStateWire::Refused,
                format!(
                    "journal moved backward from {:?} to {:?}",
                    current_state, record.state
                ),
                Vec::new(),
                Some(record.clone()),
            );
        }
        current_state = record.state;
        previous_terminal = current_state.is_terminal();
    }

    MigrationResumePlanWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        current_state,
        next_step: current_state.next_step().to_string(),
        terminal: current_state.is_terminal(),
        refused: current_state == MigrationJournalStateWire::Refused,
        refusal_reason: records
            .last()
            .and_then(|record| record.refusal.as_ref())
            .map(|refusal| refusal.reason.clone()),
        digest_mismatches: Vec::new(),
        last_record: records.last().cloned(),
    }
}

fn digest_mismatches(
    manifest: &MigrationManifest,
    observed_source_digests: &BTreeMap<String, String>,
) -> Vec<MigrationDigestMismatchWire> {
    manifest
        .expected_source_digests()
        .into_iter()
        .filter_map(|(source, expected)| {
            let observed = observed_source_digests.get(&source).cloned();
            (observed.as_ref() != Some(&expected)).then_some(
                MigrationDigestMismatchWire {
                    source,
                    expected,
                    observed,
                },
            )
        })
        .collect()
}

fn refused_plan(
    current_state: MigrationJournalStateWire,
    refusal_reason: String,
    digest_mismatches: Vec<MigrationDigestMismatchWire>,
    last_record: Option<MigrationJournalRecord>,
) -> MigrationResumePlanWire {
    MigrationResumePlanWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        current_state,
        next_step: "refused".to_string(),
        terminal: true,
        refused: true,
        refusal_reason: Some(refusal_reason),
        digest_mismatches,
        last_record,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replay_advances_to_resume_point() {
        let manifest: MigrationManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "manifest_id": "m1",
            "source_digests": {"root": "abc"}
        }))
        .unwrap();
        let records = vec![
            MigrationJournalRecord {
                state: MigrationJournalStateWire::Planned,
                ..Default::default()
            },
            MigrationJournalRecord {
                state: MigrationJournalStateWire::BackedUp,
                ..Default::default()
            },
        ];
        let plan = plan_next_step(
            &manifest,
            &records,
            &BTreeMap::from([("root".to_string(), "abc".to_string())]),
        );
        assert_eq!(plan.current_state, MigrationJournalStateWire::BackedUp);
        assert_eq!(plan.next_step, "apply");
        assert!(!plan.terminal);
    }

    #[test]
    fn replay_refuses_digest_movement() {
        let manifest: MigrationManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "manifest_id": "m1",
            "source_digests": {"root": "abc"}
        }))
        .unwrap();
        let plan = plan_next_step(
            &manifest,
            &[],
            &BTreeMap::from([("root".to_string(), "changed".to_string())]),
        );
        assert!(plan.refused);
        assert_eq!(plan.digest_mismatches[0].source, "root");
    }

    #[test]
    fn replay_refuses_backward_journal_transition() {
        let manifest: MigrationManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "manifest_id": "m1"
        }))
        .unwrap();
        let records = vec![
            MigrationJournalRecord {
                state: MigrationJournalStateWire::Applying,
                ..Default::default()
            },
            MigrationJournalRecord {
                state: MigrationJournalStateWire::BackedUp,
                ..Default::default()
            },
        ];
        let plan = plan_next_step(&manifest, &records, &BTreeMap::new());
        assert!(plan.refused);
        assert_eq!(plan.next_step, "refused");
    }
}
