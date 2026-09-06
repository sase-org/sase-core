//! Temporary offline migration proc-residue reconciliation.
//!
//! Deletion owner: sase-x7.14.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::MIGRATION_WIRE_SCHEMA_VERSION;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationLegacyProcRowWire {
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub proc_id: Option<String>,
    #[serde(default)]
    pub log_path: Option<String>,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    #[serde(default, flatten)]
    pub fields: BTreeMap<String, JsonValue>,
}

impl MigrationLegacyProcRowWire {
    pub fn canonical_proc_id(&self) -> Option<&str> {
        self.proc_id
            .as_deref()
            .or(self.task_id.as_deref())
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationCanonicalProcRefWire {
    pub proc_id: String,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    #[serde(default)]
    pub log_path: Option<String>,
    #[serde(default, flatten)]
    pub fields: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationProcMatchWire {
    pub proc_id: String,
    pub legacy_index: u64,
    pub legacy_row: MigrationLegacyProcRowWire,
    pub canonical: MigrationCanonicalProcRefWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationProcConflictWire {
    #[serde(default)]
    pub proc_id: Option<String>,
    pub legacy_index: u64,
    pub reason: String,
    #[serde(default)]
    pub legacy_fingerprint: Option<String>,
    #[serde(default)]
    pub canonical_fingerprint: Option<String>,
    #[serde(default)]
    pub legacy_row: Option<MigrationLegacyProcRowWire>,
    #[serde(default)]
    pub canonical: Option<MigrationCanonicalProcRefWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationProcReconcilePlanWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub matched: Vec<MigrationProcMatchWire>,
    #[serde(default)]
    pub unmatched_legacy: Vec<MigrationLegacyProcRowWire>,
    #[serde(default)]
    pub unmatched_canonical: Vec<MigrationCanonicalProcRefWire>,
    #[serde(default)]
    pub conflicting: Vec<MigrationProcConflictWire>,
}

pub fn reconcile_plan(
    legacy_rows: &[MigrationLegacyProcRowWire],
    canonical: &[MigrationCanonicalProcRefWire],
) -> MigrationProcReconcilePlanWire {
    let canonical_counts = canonical.iter().fold(
        BTreeMap::<String, u64>::new(),
        |mut counts, proc_ref| {
            *counts.entry(proc_ref.proc_id.clone()).or_default() += 1;
            counts
        },
    );
    let duplicate_canonical: BTreeSet<_> = canonical_counts
        .iter()
        .filter_map(|(proc_id, count)| (*count > 1).then_some(proc_id.clone()))
        .collect();
    let canonical_by_id: BTreeMap<_, _> = canonical
        .iter()
        .map(|proc_ref| (proc_ref.proc_id.clone(), proc_ref.clone()))
        .collect();

    let legacy_counts = legacy_rows.iter().fold(
        BTreeMap::<String, u64>::new(),
        |mut counts, row| {
            if let Some(proc_id) = row.canonical_proc_id() {
                *counts.entry(proc_id.to_string()).or_default() += 1;
            }
            counts
        },
    );
    let duplicate_legacy: BTreeSet<_> = legacy_counts
        .iter()
        .filter_map(|(proc_id, count)| (*count > 1).then_some(proc_id.clone()))
        .collect();

    let mut matched = Vec::new();
    let mut unmatched_legacy = Vec::new();
    let mut conflicting = Vec::new();
    let mut matched_ids = BTreeSet::new();

    for (index, legacy_row) in legacy_rows.iter().enumerate() {
        let Some(proc_id) = legacy_row.canonical_proc_id().map(str::to_string)
        else {
            unmatched_legacy.push(legacy_row.clone());
            continue;
        };
        let canonical = canonical_by_id.get(&proc_id).cloned();
        if duplicate_legacy.contains(&proc_id) {
            conflicting.push(MigrationProcConflictWire {
                proc_id: Some(proc_id),
                legacy_index: index as u64,
                reason: "duplicate legacy proc id".to_string(),
                legacy_fingerprint: legacy_row.semantic_fingerprint.clone(),
                canonical_fingerprint: canonical
                    .as_ref()
                    .and_then(|proc_ref| proc_ref.semantic_fingerprint.clone()),
                legacy_row: Some(legacy_row.clone()),
                canonical,
            });
            continue;
        }
        if duplicate_canonical.contains(&proc_id) {
            conflicting.push(MigrationProcConflictWire {
                proc_id: Some(proc_id),
                legacy_index: index as u64,
                reason: "duplicate canonical proc id".to_string(),
                legacy_fingerprint: legacy_row.semantic_fingerprint.clone(),
                canonical_fingerprint: canonical
                    .as_ref()
                    .and_then(|proc_ref| proc_ref.semantic_fingerprint.clone()),
                legacy_row: Some(legacy_row.clone()),
                canonical,
            });
            continue;
        }
        let Some(canonical) = canonical else {
            unmatched_legacy.push(legacy_row.clone());
            continue;
        };
        if let (Some(legacy_fingerprint), Some(canonical_fingerprint)) = (
            &legacy_row.semantic_fingerprint,
            &canonical.semantic_fingerprint,
        ) {
            if legacy_fingerprint != canonical_fingerprint {
                conflicting.push(MigrationProcConflictWire {
                    proc_id: Some(proc_id),
                    legacy_index: index as u64,
                    reason: "semantic fingerprint mismatch".to_string(),
                    legacy_fingerprint: Some(legacy_fingerprint.clone()),
                    canonical_fingerprint: Some(canonical_fingerprint.clone()),
                    legacy_row: Some(legacy_row.clone()),
                    canonical: Some(canonical),
                });
                continue;
            }
        }
        matched_ids.insert(proc_id.clone());
        matched.push(MigrationProcMatchWire {
            proc_id,
            legacy_index: index as u64,
            legacy_row: legacy_row.clone(),
            canonical,
        });
    }

    let unmatched_canonical = canonical
        .iter()
        .filter(|proc_ref| !matched_ids.contains(&proc_ref.proc_id))
        .cloned()
        .collect();

    MigrationProcReconcilePlanWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        matched,
        unmatched_legacy,
        unmatched_canonical,
        conflicting,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_matches_task_id_to_canonical_proc_id() {
        let legacy = vec![MigrationLegacyProcRowWire {
            task_id: Some("proc-1".to_string()),
            semantic_fingerprint: Some("same".to_string()),
            ..Default::default()
        }];
        let canonical = vec![MigrationCanonicalProcRefWire {
            proc_id: "proc-1".to_string(),
            semantic_fingerprint: Some("same".to_string()),
            ..Default::default()
        }];

        let plan = reconcile_plan(&legacy, &canonical);
        assert_eq!(plan.matched.len(), 1);
        assert!(plan.unmatched_legacy.is_empty());
        assert!(plan.conflicting.is_empty());
    }

    #[test]
    fn reconcile_reports_missing_and_conflicting_rows() {
        let legacy = vec![
            MigrationLegacyProcRowWire {
                task_id: Some("proc-1".to_string()),
                semantic_fingerprint: Some("old".to_string()),
                ..Default::default()
            },
            MigrationLegacyProcRowWire {
                task_id: Some("missing".to_string()),
                ..Default::default()
            },
        ];
        let canonical = vec![MigrationCanonicalProcRefWire {
            proc_id: "proc-1".to_string(),
            semantic_fingerprint: Some("new".to_string()),
            ..Default::default()
        }];

        let plan = reconcile_plan(&legacy, &canonical);
        assert_eq!(plan.conflicting.len(), 1);
        assert_eq!(plan.unmatched_legacy.len(), 1);
    }
}
