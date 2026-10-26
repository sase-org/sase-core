//! Wire records for publication recovery, failure accounting, and drain selection.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION: u32 = 1;

pub const PUBLICATION_KIND_RUN: &str = "run";
pub const PUBLICATION_KIND_SESSION: &str = "session";

pub const PUBLICATION_SCOPE_REVISION: &str = "revision";
pub const PUBLICATION_SCOPE_LINKED: &str = "linked";

pub const PROMPT_STATUS_RESTORED: &str = "restored";
pub const PROMPT_STATUS_ALREADY_ARCHIVED: &str = "already_archived";
pub const PROMPT_STATUS_NOT_APPLICABLE: &str = "not_applicable";
pub const PROMPT_STATUS_UNAVAILABLE: &str = "unavailable";
pub const PROMPT_STATUS_FAILED: &str = "failed";

pub const PUBLICATION_LIFECYCLE_ACTIVE: &str = "active";
pub const PUBLICATION_LIFECYCLE_QUARANTINED: &str = "quarantined";
pub const PUBLICATION_LIFECYCLE_RETIRED: &str = "retired";

pub const PUBLICATION_FAILURE_SCOPE_REPOSITORY: &str = "repository";
pub const PUBLICATION_FAILURE_SCOPE_HOOD: &str = "hood";
pub const PUBLICATION_FAILURE_SCOPE_UNKNOWN: &str = "unknown";

pub const PUBLICATION_ERROR_TIMEOUT: &str = "timeout";
pub const PUBLICATION_ERROR_LOCK_BUSY: &str = "lock_busy";
pub const PUBLICATION_ERROR_INDEX_LOCK: &str = "index_lock";
pub const PUBLICATION_ERROR_UNSTAGED_CHANGES: &str = "unstaged_changes";
pub const PUBLICATION_ERROR_REBASE_CONFLICT: &str = "rebase_conflict";
pub const PUBLICATION_ERROR_MANIFEST_MISMATCH: &str = "manifest_mismatch";
pub const PUBLICATION_ERROR_UNKNOWN: &str = "unknown";
pub const PUBLICATION_ERROR_OTHER: &str = "other";

pub const PUBLICATION_ERROR_CLASSES: &[&str] = &[
    PUBLICATION_ERROR_TIMEOUT,
    PUBLICATION_ERROR_LOCK_BUSY,
    PUBLICATION_ERROR_INDEX_LOCK,
    PUBLICATION_ERROR_UNSTAGED_CHANGES,
    PUBLICATION_ERROR_REBASE_CONFLICT,
    PUBLICATION_ERROR_MANIFEST_MISMATCH,
    PUBLICATION_ERROR_UNKNOWN,
    PUBLICATION_ERROR_OTHER,
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublicationRecoveryError {
    #[error("unsupported publication recovery wire schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid publication recovery request: {0}")]
    InvalidRequest(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationLogicalKeyWire {
    pub global_agent: String,
    pub primary_revision: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationRetryRowWire {
    pub global_agent: String,
    pub primary_revision: String,
    pub terminal: bool,
    pub quarantined: bool,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub terminal_reason: Option<String>,
    #[serde(default)]
    pub created_at: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationRetrySelectionRequestWire {
    pub schema_version: u32,
    pub retry_retired: bool,
    pub retry_quarantined: bool,
    #[serde(default)]
    pub rows: Vec<PublicationRetryRowWire>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub error_classes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetrySelectedWire {
    pub global_agent: String,
    pub primary_revision: String,
    pub prior_class: String,
    pub prior_failure: String,
    pub error_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetrySelectionResponseWire {
    pub schema_version: u32,
    pub selected: Vec<PublicationRetrySelectedWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationDrainSelectionRequestWire {
    pub schema_version: u32,
    pub limit: u32,
    #[serde(default)]
    pub retry_retired: bool,
    #[serde(default)]
    pub retry_quarantined: bool,
    #[serde(default)]
    pub rows: Vec<PublicationRetryRowWire>,
    #[serde(default)]
    pub error_classes: Vec<String>,
    #[serde(default)]
    pub preferred_key: Option<PublicationLogicalKeyWire>,
    /// Exact logical-key constraint. When supplied, selection contains only
    /// this key when it is eligible and is otherwise empty: unused capacity
    /// is never filled with another row. Absence preserves historical
    /// default selection.
    #[serde(default)]
    pub exact_key: Option<PublicationLogicalKeyWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationDrainSelectionResponseWire {
    pub schema_version: u32,
    pub active: Vec<PublicationRetrySelectedWire>,
    pub revive: Vec<PublicationRetrySelectedWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationErrorClassifyRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationErrorClassifyResponseWire {
    pub schema_version: u32,
    pub error_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFailureRowWire {
    pub global_agent: String,
    pub primary_revision: String,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub terminal: bool,
    #[serde(default)]
    pub quarantined: bool,
    #[serde(default)]
    pub terminal_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFailureAccountingRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub scope: Option<String>,
    pub error: String,
    #[serde(default)]
    pub format_error: bool,
    #[serde(default)]
    pub attempted_keys: Vec<PublicationLogicalKeyWire>,
    #[serde(default)]
    pub rows: Vec<PublicationFailureRowWire>,
    #[serde(default)]
    pub quarantine_threshold: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFailureChargeWire {
    pub global_agent: String,
    pub primary_revision: String,
    pub attempts: u32,
    pub increment_attempts: bool,
    pub quarantine: bool,
    pub retire: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFailureAccountingResponseWire {
    pub schema_version: u32,
    pub scope: String,
    pub charge: bool,
    pub error_class: String,
    pub charges: Vec<PublicationFailureChargeWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRequestIdentityWire {
    pub global_agent: String,
    pub local_agent: String,
    pub primary_revision: String,
    /// Fulfillment scope. Empty or `"revision"` keeps today's exact SHA match.
    /// `"linked"` fulfills when the required page exists, with no revision
    /// association: linked-repo commits never appear in run `commit_shas`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationPageObservationWire {
    pub path: String,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRunObservationWire {
    pub global_name: String,
    pub local_name: String,
    #[serde(default)]
    pub commit_shas: Vec<String>,
    #[serde(default)]
    pub has_prompt_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationContainerObservationWire {
    pub kind: String,
    pub global_name: String,
    #[serde(default)]
    pub member_global_names: Vec<String>,
    #[serde(default)]
    pub commit_shas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationCompletionRequestWire {
    pub schema_version: u32,
    pub request: PublicationRequestIdentityWire,
    #[serde(default)]
    pub pages: Vec<PublicationPageObservationWire>,
    #[serde(default)]
    pub runs: Vec<PublicationRunObservationWire>,
    #[serde(default)]
    pub containers: Vec<PublicationContainerObservationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationCompletionResponseWire {
    pub schema_version: u32,
    pub kind: String,
    pub required_page: String,
    pub fulfilled: bool,
    pub reason: String,
}

pub const STALE_REPAIR_ACTION_UNCHANGED: &str = "unchanged";
pub const STALE_REPAIR_ACTION_LINKED: &str = "linked";
pub const STALE_REPAIR_ACTION_REPAIRED: &str = "repaired";
pub const STALE_REPAIR_ACTION_UNRESOLVED: &str = "unresolved";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StalePublicationRepairRequestWire {
    pub schema_version: u32,
    pub requested_revision: String,
    /// Fulfillment scope of the row under repair. Empty or `"revision"` keeps
    /// revision-bound matching; `"linked"` already fulfills without revision
    /// association, so the row classifies as unchanged.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
    /// Recorded run commit SHAs for the row's agent, earliest first.
    #[serde(default)]
    pub run_commit_shas: Vec<String>,
    /// Subset of `run_commit_shas` the requested revision is an ancestor of.
    /// Gathered by the host with `git merge-base --is-ancestor`; the core
    /// only matches entries exactly so ordering stays the host's decision.
    #[serde(default)]
    pub ancestor_commit_shas: Vec<String>,
    /// Whether the requested revision resolves to a linked-repo commit.
    #[serde(default)]
    pub linked_revision: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StalePublicationRepairResponseWire {
    pub schema_version: u32,
    pub action: String,
    /// Present only for `repaired`: the earliest recorded run commit that
    /// descends from the requested revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fulfilling_revision: Option<String>,
    /// Present only for `repaired`: the descendant commit that proves the
    /// ancestry claim. Today it always equals `fulfilling_revision`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_commit: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredPromptClassifyRequestWire {
    pub schema_version: u32,
    pub restore_wrote: bool,
    #[serde(default)]
    pub restore_error: Option<String>,
    pub local_source_present: bool,
    pub archive_present: bool,
    #[serde(default)]
    pub prompt_file_in_snapshot: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredPromptClassifyResponseWire {
    pub schema_version: u32,
    pub status: String,
    pub blocks_acknowledgment: bool,
    pub reason: String,
}

pub const REPLAY_PATH_BROWSING: &str = "browsing";
pub const REPLAY_PATH_OWNER_CONTENT: &str = "owner_content";
pub const REPLAY_PATH_ARCHIVE_OBJECT: &str = "archive_object";
pub const REPLAY_PATH_UNKNOWN: &str = "unknown";

pub const REPLAY_BYTES_KEEP_CURRENT: &str = "keep_current";
pub const REPLAY_BYTES_KEEP_PREVIOUS: &str = "keep_previous";
pub const REPLAY_BYTES_REFUSE: &str = "refuse";

pub const REPLAY_APPLY_ACTION_REPORT: &str = "report";
pub const REPLAY_APPLY_ACTION_APPLY: &str = "apply";
pub const REPLAY_APPLY_ACTION_REFUSE: &str = "refuse";
pub const REPLAY_APPLY_ACTION_ALREADY_COMPLETE: &str = "already_complete";

pub const REPLAY_HOOD_ACTION_MERGE: &str = "merge";
pub const REPLAY_HOOD_ACTION_REFUSE: &str = "refuse";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPathClassifyRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPathClassWire {
    pub path: String,
    pub class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPathClassifyResponseWire {
    pub schema_version: u32,
    pub entries: Vec<ReplayPathClassWire>,
    pub browsing: Vec<String>,
    pub owner_content: Vec<String>,
    pub archive_object: Vec<String>,
    pub unknown: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayByteDivergenceRequestWire {
    pub schema_version: u32,
    pub current_hex: String,
    pub previous_hex: String,
    #[serde(default)]
    pub base_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayByteDivergenceResponseWire {
    pub schema_version: u32,
    pub action: String,
    pub retained: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayHoodMergeRequestWire {
    pub schema_version: u32,
    pub local_owner_username: String,
    pub local_owner_machine: String,
    pub local_hood: String,
    pub upstream_owner_username: String,
    pub upstream_owner_machine: String,
    pub upstream_hood: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayHoodMergeResponseWire {
    pub schema_version: u32,
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayApplyDecisionRequestWire {
    pub schema_version: u32,
    pub apply: bool,
    pub observed_head: String,
    pub expected_head: String,
    pub backup_complete: bool,
    pub backup_verified: bool,
    pub unreconcilable: bool,
    pub already_complete: bool,
    pub mutation_started: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayApplyDecisionResponseWire {
    pub schema_version: u32,
    pub action: String,
    pub reason: String,
}
