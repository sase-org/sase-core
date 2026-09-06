//! Temporary offline migration manifest wire types.
//!
//! Deletion owner: sase-x7.14.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::MIGRATION_WIRE_SCHEMA_VERSION;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

fn default_intended_action() -> String {
    "dry_run".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationManifest {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub manifest_id: String,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub host_identity: BTreeMap<String, String>,
    #[serde(default)]
    pub kit_revision: Option<String>,
    #[serde(default)]
    pub root_revisions: BTreeMap<String, String>,
    #[serde(default)]
    pub repo_revisions: BTreeMap<String, String>,
    #[serde(default)]
    pub operations: Vec<MigrationOperationEntry>,
    #[serde(default)]
    pub backups: Vec<MigrationBackupRecord>,
    #[serde(default)]
    pub source_paths: Vec<String>,
    #[serde(default)]
    pub destinations: Vec<String>,
    #[serde(default)]
    pub source_digests: BTreeMap<String, String>,
    #[serde(default)]
    pub schema_versions: BTreeMap<String, u32>,
    #[serde(default)]
    pub record_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub semantic_fingerprints: BTreeMap<String, String>,
    #[serde(default)]
    pub detected_conflicts: Vec<MigrationConflictRecord>,
    #[serde(default)]
    pub estimated_space_bytes: Option<u64>,
    #[serde(default)]
    pub backup_location: Option<String>,
    #[serde(default = "default_intended_action")]
    pub intended_action: String,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

impl MigrationManifest {
    pub fn expected_source_digests(&self) -> BTreeMap<String, String> {
        let mut digests = self.source_digests.clone();
        for operation in &self.operations {
            for (source, digest) in &operation.source_digests {
                digests.insert(source.clone(), digest.clone());
            }
        }
        digests
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationOperationEntry {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub operation: String,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub source_paths: Vec<String>,
    #[serde(default)]
    pub destinations: Vec<String>,
    #[serde(default)]
    pub source_digests: BTreeMap<String, String>,
    #[serde(default)]
    pub schema_versions: BTreeMap<String, u32>,
    #[serde(default)]
    pub record_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub semantic_fingerprints: BTreeMap<String, String>,
    #[serde(default)]
    pub detected_conflicts: Vec<MigrationConflictRecord>,
    #[serde(default)]
    pub estimated_space_bytes: Option<u64>,
    #[serde(default)]
    pub backup_required: bool,
    #[serde(default)]
    pub backup_ids: Vec<String>,
    #[serde(default)]
    pub preconditions: Vec<String>,
    #[serde(default)]
    pub verification_query: Option<JsonValue>,
    #[serde(default)]
    pub rollback_unit: Option<String>,
    #[serde(default = "default_intended_action")]
    pub intended_action: String,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationBackupRecord {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub backup_id: String,
    pub root: String,
    pub location: String,
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub checksum_manifest: Option<String>,
    #[serde(default)]
    pub source_digest: Option<String>,
    #[serde(default)]
    pub secondary_location: Option<String>,
    #[serde(default)]
    pub source_size_bytes: Option<u64>,
    #[serde(default)]
    pub stored_size_bytes: Option<u64>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub sqlite_integrity_checks: BTreeMap<String, String>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationConflictRecord {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub path: String,
    pub kind: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub expected_fingerprint: Option<String>,
    #[serde(default)]
    pub observed_fingerprint: Option<String>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, JsonValue>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn manifest_preserves_unknown_extension_fields() {
        let value = json!({
            "schema_version": 1,
            "manifest_id": "m1",
            "operations": [
                {
                    "schema_version": 1,
                    "operation": "state-residue",
                    "x_phase": "kit-driver"
                }
            ],
            "x_host_note": {"source": "fixture"}
        });

        let manifest: MigrationManifest =
            serde_json::from_value(value).unwrap();
        assert_eq!(
            manifest.extensions["x_host_note"],
            json!({"source": "fixture"})
        );
        assert_eq!(manifest.operations[0].extensions["x_phase"], "kit-driver");

        let encoded = serde_json::to_value(manifest).unwrap();
        assert_eq!(encoded["x_host_note"], json!({"source": "fixture"}));
        assert_eq!(encoded["operations"][0]["x_phase"], "kit-driver");
    }

    #[test]
    fn expected_source_digests_include_operation_entries() {
        let manifest: MigrationManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "manifest_id": "m1",
            "source_digests": {"root": "aaa"},
            "operations": [
                {
                    "operation": "import-purge",
                    "source_digests": {"chats": "bbb"}
                }
            ]
        }))
        .unwrap();

        assert_eq!(
            manifest.expected_source_digests(),
            BTreeMap::from([
                ("chats".to_string(), "bbb".to_string()),
                ("root".to_string(), "aaa".to_string())
            ])
        );
    }
}
