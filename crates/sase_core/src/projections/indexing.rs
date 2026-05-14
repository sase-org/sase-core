use serde::{Deserialize, Serialize};

pub const INDEXING_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceChangeOperationWire {
    Upsert,
    Delete,
    Rewrite,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFingerprintWire {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at_unix_millis: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentityWire {
    pub schema_version: u32,
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub source_path: String,
    #[serde(default)]
    pub is_archive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<SourceFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_indexed_event_seq: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceChangeWire {
    pub schema_version: u32,
    pub identity: SourceIdentityWire,
    pub operation: SourceChangeOperationWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingDomainReportWire {
    pub schema_version: u32,
    pub domain: String,
    pub queued_changes: u64,
    pub coalesced_changes: u64,
    pub indexed_sources: u64,
    pub failed_parses: u64,
    pub shadow_diff_counts: ShadowDiffCountsWire,
    pub source_paths: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffCountsWire {
    pub missing: u64,
    pub stale: u64,
    pub extra: u64,
    pub corrupt: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowDiffCategoryWire {
    Missing,
    Stale,
    Extra,
    Corrupt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffRecordWire {
    pub schema_version: u32,
    pub domain: String,
    pub category: ShadowDiffCategoryWire,
    pub source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffReportWire {
    pub schema_version: u32,
    pub domain: String,
    pub records: Vec<ShadowDiffRecordWire>,
    pub counts: ShadowDiffCountsWire,
}

impl Default for SourceFingerprintWire {
    fn default() -> Self {
        Self {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            file_size: None,
            modified_at_unix_millis: None,
            inode: None,
            content_sha256: None,
        }
    }
}

impl SourceIdentityWire {
    pub fn stable_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.domain,
            self.project_id.as_deref().unwrap_or(""),
            self.source_path
        )
    }
}

pub fn source_event_idempotency_key(
    operation: &SourceChangeOperationWire,
    identity: &SourceIdentityWire,
) -> String {
    let fingerprint = identity
        .fingerprint
        .as_ref()
        .and_then(|value| value.content_sha256.as_deref())
        .map(str::to_string)
        .or_else(|| {
            identity.fingerprint.as_ref().and_then(|value| {
                Some(format!(
                    "size={}:mtime={}",
                    value.file_size?, value.modified_at_unix_millis?
                ))
            })
        })
        .unwrap_or_else(|| "unknown".to_string());
    let operation = match operation {
        SourceChangeOperationWire::Upsert => "upsert",
        SourceChangeOperationWire::Delete => "delete",
        SourceChangeOperationWire::Rewrite => "rewrite",
        SourceChangeOperationWire::Reconcile => "reconcile",
    };
    format!(
        "indexer:{operation}:{}:{fingerprint}",
        identity.stable_key()
    )
}
