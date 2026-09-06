//! Transport-free fleet identity, projection, count, cursor, operation, and
//! connection contracts.
//!
//! Version 1 deliberately separates stable identity from every operational
//! label that may change. An installation ID is an opaque per-user origin ID;
//! it is not a hostname, configured `id.machine_name`, provider reference, or
//! display alias. Logical agent and family locators identify durable work
//! threads, while exact instance locators identify one shell/run/attempt and
//! are required for mutations. Lifecycle, owner-resolved process liveness,
//! connection health, and viewer freshness are distinct states because only
//! the owner can resolve local PIDs and content availability. Feed cursors
//! track store replay positions; resource revisions are separate mutation
//! preconditions. Operation keys provide scoped idempotency inside an
//! acceptance window, not indefinite or exactly-once execution.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::agent_scan::{
    AgentArtifactRecordWire, AgentMetaWire, DoneMarkerWire, RunningMarkerWire,
};
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

/// Schema version shared by the fleet contract surface.
pub const FLEET_CONTRACT_SCHEMA_VERSION: u32 = 1;
/// Current persisted installation-identity file schema.
pub const FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION: u32 = 1;
/// File under SASE home that stores this user's opaque installation identity.
pub const FLEET_INSTALLATION_IDENTITY_FILENAME: &str =
    "installation_identity.json";
/// Reject identity state above this byte limit without overwriting it.
pub const FLEET_INSTALLATION_IDENTITY_MAX_BYTES: usize = 16 * 1024;
/// Versioned, recognizable prefix for opaque installation IDs.
pub const FLEET_INSTALLATION_ID_PREFIX: &str = "sase_inst_v1_";
/// The explicit zero cursor generation.
pub const FLEET_INITIAL_CURSOR_GENERATION: &str = "initial";

const LOCK_TIMEOUT_ENV: &str = "SASE_FLEET_IDENTITY_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_LABEL_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 1024;
const MAX_CAPABILITY_BYTES: usize = 80;
const MAX_INTENT_BYTES: usize = 512;
const PAYLOAD_FINGERPRINT_DOMAIN: &[u8] = b"sase-fleet-operation-payload-v1\0";

#[derive(Debug, Error)]
pub enum FleetContractError {
    #[error("{0}")]
    Validation(String),
    #[error(
        "timed out after {waited_ms}ms waiting for {mode} lock {}: holder: {holder}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
        holder: String,
    },
    #[error("fleet contract I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet contract JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

/// Persisted per-user origin identity.
///
/// `installation_id` is opaque identity. It is intentionally unrelated to
/// local network identity, configured machine name, provider reference,
/// endpoint, display alias, username, or any filesystem location.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRecordWire {
    pub schema_version: u32,
    pub installation_id: String,
    pub created_at_unix: f64,
    pub generation: u64,
    pub prior_installation_id: Option<String>,
    pub rotated_at_unix: Option<f64>,
    pub adopted_at_unix: Option<f64>,
    pub reason: Option<String>,
}

/// Outcome of ensuring the persisted installation identity exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityEnsureOutcomeWire {
    pub schema_version: u32,
    pub record: InstallationIdentityRecordWire,
    pub created: bool,
    pub path: String,
}

/// Read-only load result for callers that must not create a missing identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityLoadOutcomeWire {
    pub schema_version: u32,
    pub record: Option<InstallationIdentityRecordWire>,
    pub path: String,
}

/// Fenced request to rotate a known current installation identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRotateRequestWire {
    pub schema_version: u32,
    pub expected_installation_id: String,
    pub reason: String,
    pub rotated_at_unix: Option<f64>,
}

/// Result of a successful explicit identity rotation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRotateOutcomeWire {
    pub schema_version: u32,
    pub old_record: InstallationIdentityRecordWire,
    pub new_record: InstallationIdentityRecordWire,
    pub path: String,
}

/// Fenced request to adopt an existing identity during reinstall recovery.
///
/// `expected_current_installation_id = null` means the caller expects the
/// store to be missing. A non-null value means the caller expects to replace
/// exactly that current identity. Ordinary ensure/load calls never adopt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityMigrateRequestWire {
    pub schema_version: u32,
    pub expected_current_installation_id: Option<String>,
    pub adopted_installation_id: String,
    pub reason: String,
    pub adopted_at_unix: Option<f64>,
}

/// Result of a successful explicit identity adoption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityMigrateOutcomeWire {
    pub schema_version: u32,
    pub prior_record: Option<InstallationIdentityRecordWire>,
    pub new_record: InstallationIdentityRecordWire,
    pub path: String,
}

/// Origin locator: the installation identity only.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct OriginLocatorWire {
    pub schema_version: u32,
    pub installation_id: String,
}

/// Project locator: origin plus a portable project ID, never a checkout path.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ProjectLocatorWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub project_id: String,
}

/// Stable logical agent/family locator.
///
/// Human names and provider metadata remain labels outside this identity.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct LogicalAgentLocatorWire {
    pub schema_version: u32,
    pub project: ProjectLocatorWire,
    pub agent_id: String,
    pub family_id: Option<String>,
}

/// Exact shell/run/attempt locator required for mutation targets.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceLocatorWire {
    pub schema_version: u32,
    pub logical: LogicalAgentLocatorWire,
    pub shell_id: String,
    pub run_id: String,
    pub attempt_id: String,
}

/// Owner-qualified display labels associated with a locator.
///
/// These labels make machine-hood names useful for lookup and display without
/// deriving the origin identity from those names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameRequestWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub owner_username: String,
    pub owner_machine_name: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub owner_label: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

/// Row category for logical-agent counting.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetRowKindWire {
    AgentShell,
    ContainerHeader,
    Monitor,
    Gate,
    Proc,
    HistoricalShell,
}

fn default_row_kind() -> FleetRowKindWire {
    FleetRowKindWire::AgentShell
}

/// Lifecycle status observed in the artifact record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetLifecycleWire {
    Starting,
    Running,
    Waiting,
    Asking,
    Terminal,
    Failed,
    Unknown,
}

/// Owner-resolved process liveness, separate from lifecycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OwnerLivenessWire {
    Alive,
    Dead,
    NotProcess,
    Unknown,
}

/// Current connection health between viewer/controller and owner.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionHealthWire {
    Online,
    Degraded,
    Offline,
    Unknown,
}

/// Viewer observation freshness. This is not owner process liveness.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ObservationFreshnessWire {
    Fresh,
    Aging,
    Stale,
    Unknown,
}

/// Display bucket compatible with the current Python status-bucket semantics.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetStatusBucketWire {
    Stopped,
    Failed,
    Starting,
    Running,
    Queued,
    Waiting,
    Done,
}

/// Closed content-handle kind. Handles are opaque and never expose local
/// paths, PIDs, process groups, raw bearer tokens, or auth headers.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ContentHandleKindWire {
    Transcript,
    Output,
    Diff,
    Log,
    Artifact,
    Question,
}

/// Safe content handle metadata for lazy detail retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentHandleWire {
    pub schema_version: u32,
    pub id: String,
    pub kind: ContentHandleKindWire,
    pub revision: Option<ResourceRevisionWire>,
    pub digest: Option<String>,
    pub byte_len: Option<u64>,
    pub supports_range: bool,
    pub supports_growth: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMetadataWire {
    pub schema_version: u32,
    pub handle_count: u64,
    pub total_byte_len: Option<u64>,
    pub kinds: Vec<ContentHandleKindWire>,
    pub supports_range: bool,
    pub supports_growth: bool,
}

/// Resource/action revision used as a mutation precondition. This is not a
/// feed cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRevisionWire {
    pub schema_version: u32,
    pub logical_key: String,
    pub revision: u64,
}

/// Normalized resource, host, and protocol capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitySetWire {
    pub schema_version: u32,
    pub resource: Vec<String>,
    pub host: Vec<String>,
    pub protocol: Vec<String>,
}

/// Owner facts produced outside this pure projection layer.
///
/// These facts may come from PID checks, content availability checks, and
/// connection probes, but this module never performs those checks itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerResolutionFactsWire {
    pub schema_version: u32,
    pub exact_locator: Option<AgentInstanceLocatorWire>,
    pub row_revision: ResourceRevisionWire,
    pub liveness: OwnerLivenessWire,
    pub connection_health: ConnectionHealthWire,
    pub freshness: ObservationFreshnessWire,
    pub observed_at_unix: f64,
    #[serde(default = "default_row_kind")]
    pub row_kind: FleetRowKindWire,
    #[serde(default)]
    pub current_instance: bool,
    #[serde(default)]
    pub dismissable: bool,
    #[serde(default)]
    pub needs_attention: bool,
    #[serde(default)]
    pub occupied_runner_slot: bool,
    #[serde(default)]
    pub container_projected_concrete_agent: bool,
    pub capabilities: CapabilitySetWire,
    #[serde(default)]
    pub content_handles: Vec<ContentHandleWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanDisplayLabelsWire {
    pub schema_version: u32,
    pub project_label: String,
    pub agent_label: Option<String>,
    pub family_label: Option<String>,
    pub owner_label: Option<String>,
    pub alias: Option<String>,
}

/// Request to build a safe owner-resolved summary from an artifact record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentProjectionRequestWire {
    pub schema_version: u32,
    pub record: AgentArtifactRecordWire,
    pub logical_locator: LogicalAgentLocatorWire,
    pub owner_facts: OwnerResolutionFactsWire,
}

/// Safe row projection for fleet lists.
///
/// It carries locators, labels, provider/model display data, lifecycle,
/// liveness, health, freshness, row/resource revision, capabilities, and
/// bounded content metadata. It never serializes local artifact directories,
/// checkout paths, marker paths, output/response paths, PIDs, process groups,
/// raw credentials, or auth headers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentSummaryWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub exact_locator: Option<AgentInstanceLocatorWire>,
    pub logical_key: String,
    pub exact_key: Option<String>,
    pub row_kind: FleetRowKindWire,
    pub labels: HumanDisplayLabelsWire,
    pub project_name: String,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub status: String,
    pub status_bucket: FleetStatusBucketWire,
    pub intent: Option<String>,
    pub observed_at_unix: f64,
    pub row_revision: ResourceRevisionWire,
    pub lifecycle: FleetLifecycleWire,
    pub liveness: OwnerLivenessWire,
    pub connection_health: ConnectionHealthWire,
    pub freshness: ObservationFreshnessWire,
    pub capabilities: CapabilitySetWire,
    pub content: ContentMetadataWire,
    pub current_instance: bool,
    pub dismissable: bool,
    pub needs_attention: bool,
    pub occupied_runner_slot: bool,
    pub container_projected_concrete_agent: bool,
}

/// Lazy safe detail projection. Content is exposed through opaque handles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentDetailWire {
    pub schema_version: u32,
    pub summary: ResolvedAgentSummaryWire,
    pub content_handles: Vec<ContentHandleWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalAgentCountsRequestWire {
    pub schema_version: u32,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalAgentCountsWire {
    pub schema_version: u32,
    pub basis: FleetCountBasisWire,
    pub logical_agent_total: u64,
    pub running: u64,
    pub waiting: u64,
    pub attention: u64,
    pub occupied_runner_slots: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCountBasisWire {
    pub schema_version: u32,
    pub input_rows: u64,
    pub selected_rows: u64,
    pub max_revision: Option<u64>,
    pub observed_at_unix_max: Option<f64>,
}

/// Source that created a viewer-local follow record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowCreatedByWire {
    Explicit,
    Dispatch,
}

/// Lifecycle state for a durable follow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowStateWire {
    Pending,
    Active,
}

/// Durable viewer-local follow intent.
///
/// The record identity is `(logical_key, created_by)`, where
/// `logical_key` includes origin installation ID, project ID, family ID, and
/// agent ID. Dispatch follows may be prewritten as `pending` before remote
/// admission and activated when the authoritative receipt arrives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowRecordWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub created_by: FollowCreatedByWire,
    pub state: FollowStateWire,
    pub created_at_unix: f64,
    pub updated_at_unix: f64,
    pub activated_at_unix: Option<f64>,
    pub operation_key: Option<ScopedOperationKeyWire>,
}

/// Explicit local unfollow tombstone.
///
/// Tombstones are keyed by logical identity, not by `created_by`, so they can
/// suppress automatic dispatch/reconciliation follows without preventing a
/// later explicit follow action from deliberately clearing them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowTombstoneWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub unfollowed_at_unix: f64,
}

/// Promote a singleton follow to the formed family identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowFamilyPromotionWire {
    pub schema_version: u32,
    pub from: LogicalAgentLocatorWire,
    pub to: LogicalAgentLocatorWire,
}

/// Activate a pending dispatch follow after receipt binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowActivationWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub operation_key: Option<ScopedOperationKeyWire>,
    pub activated_at_unix: f64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowDiagnosticSeverityWire {
    Info,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowDiagnosticWire {
    pub schema_version: u32,
    pub severity: FollowDiagnosticSeverityWire,
    pub code: String,
    pub message: String,
    pub logical_key: Option<String>,
}

/// Normalize durable follow state after local mutations or remote
/// reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowReconciliationRequestWire {
    pub schema_version: u32,
    pub records: Vec<FollowRecordWire>,
    pub tombstones: Vec<FollowTombstoneWire>,
    #[serde(default)]
    pub promotions: Vec<FollowFamilyPromotionWire>,
    #[serde(default)]
    pub activations: Vec<FollowActivationWire>,
    pub now_unix: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowReconciliationWire {
    pub schema_version: u32,
    pub records: Vec<FollowRecordWire>,
    pub tombstones: Vec<FollowTombstoneWire>,
    pub changed: bool,
    pub diagnostics: Vec<FollowDiagnosticWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHostCountInputWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub observed_at_unix: Option<f64>,
    pub freshness: ObservationFreshnessWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetCountsRequestWire {
    pub schema_version: u32,
    pub local_summaries: Vec<ResolvedAgentSummaryWire>,
    pub followed_remote_hosts: Vec<FleetHostCountInputWire>,
    pub fleet_hosts: Vec<FleetHostCountInputWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHostCountWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub partial: bool,
    pub observed_at_unix: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetScopeCountsWire {
    pub schema_version: u32,
    pub counts: FleetLogicalAgentCountsWire,
    pub partial: bool,
    pub observed_at_unix_max: Option<f64>,
    pub unknown_origins: Vec<String>,
    pub host_counts: Vec<FleetHostCountWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetCountsWire {
    pub schema_version: u32,
    pub focus: FleetScopeCountsWire,
    pub fleet: FleetScopeCountsWire,
}

/// Event-feed cursor. This is separate from mutation resource revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCursorWire {
    pub schema_version: u32,
    pub store_generation: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorReplayRequestWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub current_generation: String,
    pub newest_sequence: u64,
    pub oldest_replayable_sequence: u64,
    pub deletion_history_complete: bool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CursorReplayClassificationWire {
    Current,
    Replayable,
    ResyncRequired,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CursorReplayReasonWire {
    AtAuthorityHead,
    BehindWithinReplayWindow,
    InitialCursor,
    GenerationMismatch,
    SequenceAheadOfAuthority,
    ReplayGap,
    IncompleteDeletionHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorReplayDecisionWire {
    pub schema_version: u32,
    pub classification: CursorReplayClassificationWire,
    pub reason: CursorReplayReasonWire,
    pub replay_from_sequence: Option<u64>,
    pub authoritative_generation: String,
    pub authoritative_newest_sequence: u64,
}

/// Idempotency key scoped by authenticated controller identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedOperationKeyWire {
    pub schema_version: u32,
    pub controller_id: String,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadFingerprintWire {
    pub schema_version: u32,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadFingerprintRequestWire {
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationReceiptStateWire {
    Accepted,
    Pending,
    Settled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableOperationRecordWire {
    pub schema_version: u32,
    pub receipt: OperationReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableOperationRecordWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationDecisionKindWire {
    AcceptNew,
    ReturnOriginalReceipt,
    Conflict,
    Expired,
    PreconditionMismatch,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationDecisionReasonWire {
    UnseenInWindow,
    SameScopedKeyAndPayload,
    SameScopedKeyDifferentPayload,
    ExpiredOrTombstonedKey,
    TargetOrRevisionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<OperationReceiptWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetConnectionKindWire {
    Gateway,
    Tunnel,
    Direct,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TlsTrustModeWire {
    SystemRoots,
    PinnedCa,
    PinnedServerName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsTrustSettingsWire {
    pub schema_version: u32,
    pub mode: TlsTrustModeWire,
    pub ca_ref: Option<String>,
    pub server_name_ref: Option<String>,
}

/// Serializable routing plan. Provider choice is metadata and never
/// participates in locator equality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionPlanWire {
    pub schema_version: u32,
    pub provider_ref: String,
    pub endpoint: String,
    pub credential_ref: String,
    pub pinned_installation_id: String,
    pub connection_kind: FleetConnectionKindWire,
    pub tls: TlsTrustSettingsWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDurationRequestWire {
    pub schema_version: u32,
    pub owner_started_at_unix: f64,
    pub owner_stopped_at_unix: Option<f64>,
    pub owner_observed_at_unix: f64,
    pub max_clock_anomaly_seconds: f64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDurationStateWire {
    Running,
    Stopped,
    ClockAnomalyClamped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDurationWire {
    pub schema_version: u32,
    pub elapsed_seconds: f64,
    pub state: RuntimeDurationStateWire,
    pub clamped: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFreshnessRequestWire {
    pub schema_version: u32,
    pub viewer_monotonic_elapsed_seconds: Option<f64>,
    pub fresh_threshold_seconds: f64,
    pub stale_threshold_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFreshnessWire {
    pub schema_version: u32,
    pub freshness: ObservationFreshnessWire,
    pub age_seconds: Option<f64>,
}

enum LoadedIdentity {
    Missing,
    Valid(InstallationIdentityRecordWire),
    Unusable { message: String },
}

enum WritePrecondition<'a> {
    Missing,
    Current(&'a str),
}

pub fn fleet_contract_schema_version() -> u32 {
    FLEET_CONTRACT_SCHEMA_VERSION
}

pub fn installation_identity_path(sase_home: &Path) -> PathBuf {
    sase_home.join(FLEET_INSTALLATION_IDENTITY_FILENAME)
}

pub fn load_installation_identity(
    sase_home: &Path,
) -> Result<InstallationIdentityLoadOutcomeWire, FleetContractError> {
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_load", || {
        let loaded = load_identity_unlocked(&path)?;
        match loaded {
            LoadedIdentity::Missing => {
                Ok(InstallationIdentityLoadOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record: None,
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Valid(record) => {
                Ok(InstallationIdentityLoadOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record: Some(record),
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Unusable { message } => {
                Err(mutation_blocked(&path, &message))
            }
        }
    })
}

pub fn ensure_installation_identity(
    sase_home: &Path,
) -> Result<InstallationIdentityEnsureOutcomeWire, FleetContractError> {
    ensure_installation_identity_with_generator(
        sase_home,
        current_unix_time()?,
        generate_installation_id,
    )
}

pub fn ensure_installation_identity_with_generator(
    sase_home: &Path,
    now_unix: f64,
    mut generator: impl FnMut() -> String,
) -> Result<InstallationIdentityEnsureOutcomeWire, FleetContractError> {
    validate_timestamp("created_at_unix", now_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_ensure", || {
        match load_identity_unlocked(&path)? {
            LoadedIdentity::Valid(record) => {
                Ok(InstallationIdentityEnsureOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record,
                    created: false,
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Unusable { message } => {
                Err(mutation_blocked(&path, &message))
            }
            LoadedIdentity::Missing => {
                let installation_id = generator();
                validate_installation_id(&installation_id)?;
                let record = InstallationIdentityRecordWire {
                    schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
                    installation_id,
                    created_at_unix: now_unix,
                    generation: 1,
                    prior_installation_id: None,
                    rotated_at_unix: None,
                    adopted_at_unix: None,
                    reason: None,
                };
                validate_installation_record(&record)?;
                write_identity_atomic(
                    &path,
                    &record,
                    WritePrecondition::Missing,
                )?;
                Ok(InstallationIdentityEnsureOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record,
                    created: true,
                    path: path.display().to_string(),
                })
            }
        }
    })
}

pub fn rotate_installation_identity(
    sase_home: &Path,
    request: &InstallationIdentityRotateRequestWire,
) -> Result<InstallationIdentityRotateOutcomeWire, FleetContractError> {
    rotate_installation_identity_with_generator(
        sase_home,
        request,
        generate_installation_id,
    )
}

pub fn rotate_installation_identity_with_generator(
    sase_home: &Path,
    request: &InstallationIdentityRotateRequestWire,
    mut generator: impl FnMut() -> String,
) -> Result<InstallationIdentityRotateOutcomeWire, FleetContractError> {
    validate_schema(
        "installation identity rotate request",
        request.schema_version,
    )?;
    validate_installation_id(&request.expected_installation_id)?;
    validate_label("rotation reason", &request.reason, MAX_LABEL_BYTES)?;
    let rotated_at_unix =
        request.rotated_at_unix.map_or_else(current_unix_time, Ok)?;
    validate_timestamp("rotated_at_unix", rotated_at_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_rotate", || {
        let old_record = match load_identity_unlocked(&path)? {
            LoadedIdentity::Valid(record) => record,
            LoadedIdentity::Missing => {
                return Err(FleetContractError::Validation(format!(
                    "cannot rotate missing installation identity at {}",
                    path.display()
                )))
            }
            LoadedIdentity::Unusable { message } => {
                return Err(mutation_blocked(&path, &message))
            }
        };
        if old_record.installation_id != request.expected_installation_id {
            return Err(FleetContractError::Validation(
                "expected_installation_id does not match the current identity"
                    .to_string(),
            ));
        }
        let installation_id = generator();
        validate_installation_id(&installation_id)?;
        if installation_id == old_record.installation_id {
            return Err(FleetContractError::Validation(
                "rotated installation identity must be distinct".to_string(),
            ));
        }
        let new_record = InstallationIdentityRecordWire {
            schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
            installation_id,
            created_at_unix: rotated_at_unix,
            generation: old_record.generation.saturating_add(1),
            prior_installation_id: Some(old_record.installation_id.clone()),
            rotated_at_unix: Some(rotated_at_unix),
            adopted_at_unix: None,
            reason: Some(request.reason.trim().to_string()),
        };
        validate_installation_record(&new_record)?;
        write_identity_atomic(
            &path,
            &new_record,
            WritePrecondition::Current(&old_record.installation_id),
        )?;
        Ok(InstallationIdentityRotateOutcomeWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            old_record,
            new_record,
            path: path.display().to_string(),
        })
    })
}

pub fn migrate_installation_identity(
    sase_home: &Path,
    request: &InstallationIdentityMigrateRequestWire,
) -> Result<InstallationIdentityMigrateOutcomeWire, FleetContractError> {
    validate_schema(
        "installation identity migrate request",
        request.schema_version,
    )?;
    if let Some(expected) = &request.expected_current_installation_id {
        validate_installation_id(expected)?;
    }
    validate_installation_id(&request.adopted_installation_id)?;
    validate_label("migration reason", &request.reason, MAX_LABEL_BYTES)?;
    let adopted_at_unix =
        request.adopted_at_unix.map_or_else(current_unix_time, Ok)?;
    validate_timestamp("adopted_at_unix", adopted_at_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_migrate", || {
        let loaded = load_identity_unlocked(&path)?;
        let (prior_record, precondition, generation) = match loaded {
            LoadedIdentity::Missing => {
                if request.expected_current_installation_id.is_some() {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id was supplied but the identity store is missing"
                            .to_string(),
                    ));
                }
                (None, WritePrecondition::Missing, 1)
            }
            LoadedIdentity::Valid(record) => {
                let Some(expected) = &request.expected_current_installation_id
                else {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id is required when replacing an existing identity"
                            .to_string(),
                    ));
                };
                if expected != &record.installation_id {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id does not match the current identity"
                            .to_string(),
                    ));
                }
                let generation = record.generation.saturating_add(1);
                (
                    Some(record),
                    WritePrecondition::Current(expected.as_str()),
                    generation,
                )
            }
            LoadedIdentity::Unusable { message } => {
                return Err(mutation_blocked(&path, &message))
            }
        };
        if prior_record.as_ref().is_some_and(|record| {
            record.installation_id == request.adopted_installation_id
        }) {
            return Err(FleetContractError::Validation(
                "adopted identity must differ from the current identity"
                    .to_string(),
            ));
        }
        let new_record = InstallationIdentityRecordWire {
            schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
            installation_id: request.adopted_installation_id.clone(),
            created_at_unix: adopted_at_unix,
            generation,
            prior_installation_id: prior_record
                .as_ref()
                .map(|record| record.installation_id.clone()),
            rotated_at_unix: None,
            adopted_at_unix: Some(adopted_at_unix),
            reason: Some(request.reason.trim().to_string()),
        };
        validate_installation_record(&new_record)?;
        write_identity_atomic(&path, &new_record, precondition)?;
        Ok(InstallationIdentityMigrateOutcomeWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            prior_record,
            new_record,
            path: path.display().to_string(),
        })
    })
}

pub fn logical_locator_key(
    locator: &LogicalAgentLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(logical_key_unchecked(locator))
}

pub fn instance_locator_key(
    locator: &AgentInstanceLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(instance_key_unchecked(locator))
}

pub fn associate_owner_display_name(
    request: &OwnerDisplayNameRequestWire,
) -> Result<OwnerDisplayNameWire, FleetContractError> {
    validate_schema("owner display name request", request.schema_version)?;
    request.logical_locator.validate()?;
    validate_identifier("owner_username", &request.owner_username)?;
    validate_identifier("owner_machine_name", &request.owner_machine_name)?;
    validate_label("display_name", &request.display_name, MAX_LABEL_BYTES)?;
    if let Some(alias) = &request.display_alias {
        validate_label("display_alias", alias, MAX_LABEL_BYTES)?;
    }
    let logical_key = logical_key_unchecked(&request.logical_locator);
    Ok(OwnerDisplayNameWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: request.logical_locator.clone(),
        logical_key,
        owner_label: format!(
            "{}.{}",
            request.owner_username.trim(),
            request.owner_machine_name.trim()
        ),
        display_name: request.display_name.trim().to_string(),
        display_alias: request
            .display_alias
            .as_ref()
            .map(|value| value.trim().to_string()),
    })
}

pub fn project_resolved_agent_summary(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<ResolvedAgentSummaryWire, FleetContractError> {
    validate_projection_request(request)?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    let logical_key = logical_key_unchecked(&request.logical_locator);
    let exact_key = facts.exact_locator.as_ref().map(instance_key_unchecked);
    let lifecycle = lifecycle_for_record(&request.record);
    reject_inconsistent_projection(&request.record, &facts, lifecycle)?;
    let status = status_for_record(&request.record);
    let meta = request.record.agent_meta.as_ref();
    let done = request.record.done.as_ref();
    let running = request.record.running.as_ref();
    let family = meta
        .and_then(|value| value.family_shell.as_ref())
        .or_else(|| done.and_then(|value| value.family_shell.as_ref()));
    let labels = HumanDisplayLabelsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project_label: trim_to_limit(
            &request.record.project_name,
            MAX_LABEL_BYTES,
        ),
        agent_label: first_non_empty([
            meta.and_then(|value| value.name.as_deref()),
            done.and_then(|value| value.name.as_deref()),
            family.and_then(|value| value.label.as_deref()),
        ])
        .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        family_label: first_non_empty([
            meta.and_then(|value| value.agent_family.as_deref()),
            family.and_then(|value| value.label.as_deref()),
        ])
        .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        owner_label: None,
        alias: None,
    };
    let summary = ResolvedAgentSummaryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: request.logical_locator.clone(),
        exact_locator: facts.exact_locator.clone(),
        logical_key,
        exact_key,
        row_kind: facts.row_kind,
        labels,
        project_name: trim_to_limit(
            &request.record.project_name,
            MAX_LABEL_BYTES,
        ),
        model: model_for_record(meta, done, running),
        provider: provider_for_record(meta, done, running),
        status,
        status_bucket: bucket_for_lifecycle(lifecycle),
        intent: intent_for_record(&request.record),
        observed_at_unix: facts.observed_at_unix,
        row_revision: facts.row_revision.clone(),
        lifecycle,
        liveness: facts.liveness,
        connection_health: facts.connection_health,
        freshness: facts.freshness,
        capabilities: facts.capabilities.clone(),
        content: content_metadata(&facts.content_handles)?,
        current_instance: facts.current_instance,
        dismissable: facts.dismissable,
        needs_attention: facts.needs_attention
            || lifecycle == FleetLifecycleWire::Asking,
        occupied_runner_slot: facts.occupied_runner_slot,
        container_projected_concrete_agent: facts
            .container_projected_concrete_agent,
    };
    validate_resolved_agent_summary(&summary)
}

pub fn project_resolved_agent_detail(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<ResolvedAgentDetailWire, FleetContractError> {
    let summary = project_resolved_agent_summary(request)?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    Ok(ResolvedAgentDetailWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summary,
        content_handles: facts.content_handles,
    })
}

pub fn validate_resolved_agent_summary(
    summary: &ResolvedAgentSummaryWire,
) -> Result<ResolvedAgentSummaryWire, FleetContractError> {
    validate_schema("resolved agent summary", summary.schema_version)?;
    summary.logical_locator.validate()?;
    if let Some(exact) = &summary.exact_locator {
        exact.validate()?;
        if exact.logical != summary.logical_locator {
            return Err(FleetContractError::Validation(
                "exact locator logical identity does not match summary logical locator"
                    .to_string(),
            ));
        }
    }
    let logical_key = logical_key_unchecked(&summary.logical_locator);
    if summary.logical_key != logical_key {
        return Err(FleetContractError::Validation(
            "summary logical_key does not match logical locator".to_string(),
        ));
    }
    if summary.exact_key
        != summary.exact_locator.as_ref().map(instance_key_unchecked)
    {
        return Err(FleetContractError::Validation(
            "summary exact_key does not match exact locator".to_string(),
        ));
    }
    summary.row_revision.validate()?;
    if summary.row_revision.logical_key != summary.logical_key {
        return Err(FleetContractError::Validation(
            "row revision belongs to a different logical identity".to_string(),
        ));
    }
    validate_timestamp("observed_at_unix", summary.observed_at_unix)?;
    validate_label("project_name", &summary.project_name, MAX_LABEL_BYTES)?;
    validate_label(
        "labels.project_label",
        &summary.labels.project_label,
        MAX_LABEL_BYTES,
    )?;
    for (field, value) in [
        ("model", summary.model.as_deref()),
        ("provider", summary.provider.as_deref()),
        ("intent", summary.intent.as_deref()),
        ("labels.agent_label", summary.labels.agent_label.as_deref()),
        (
            "labels.family_label",
            summary.labels.family_label.as_deref(),
        ),
        ("labels.owner_label", summary.labels.owner_label.as_deref()),
        ("labels.alias", summary.labels.alias.as_deref()),
    ] {
        if let Some(value) = value {
            validate_label(field, value, MAX_INTENT_BYTES)?;
            reject_secretish(field, value)?;
        }
    }
    let normalized = summary.capabilities.normalized()?;
    if normalized != summary.capabilities {
        return Err(FleetContractError::Validation(
            "summary capabilities are not normalized".to_string(),
        ));
    }
    summary.content.validate()?;
    if terminal_lifecycle(summary.lifecycle)
        && summary.liveness == OwnerLivenessWire::Alive
    {
        return Err(FleetContractError::Validation(
            "terminal agent summary cannot have owner liveness alive"
                .to_string(),
        ));
    }
    if summary
        .capabilities
        .resource
        .iter()
        .any(|capability| actionable_capability(capability))
        && summary.exact_locator.is_none()
    {
        return Err(FleetContractError::Validation(
            "actionable resource capability requires an exact instance locator"
                .to_string(),
        ));
    }
    Ok(summary.clone())
}

pub fn count_logical_agents(
    request: &FleetLogicalAgentCountsRequestWire,
) -> Result<FleetLogicalAgentCountsWire, FleetContractError> {
    validate_schema(
        "fleet logical agent counts request",
        request.schema_version,
    )?;
    let mut selected: BTreeMap<String, ResolvedAgentSummaryWire> =
        BTreeMap::new();
    for summary in &request.summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        if summary.row_kind != FleetRowKindWire::AgentShell {
            continue;
        }
        if !summary.current_instance {
            continue;
        }
        match selected.get(&summary.logical_key) {
            Some(existing)
                if existing.row_revision.revision
                    > summary.row_revision.revision => {}
            Some(existing)
                if existing.row_revision.revision
                    == summary.row_revision.revision =>
            {
                if existing.exact_key != summary.exact_key {
                    return Err(FleetContractError::Validation(format!(
                        "ambiguous current instances for logical identity {} at revision {}",
                        summary.logical_key, summary.row_revision.revision
                    )));
                }
            }
            _ => {
                selected.insert(summary.logical_key.clone(), summary);
            }
        }
    }
    let mut running = 0_u64;
    let mut waiting = 0_u64;
    let mut attention = 0_u64;
    let mut occupied = 0_u64;
    let mut max_revision: Option<u64> = None;
    let mut observed_at_unix_max: Option<f64> = None;
    for summary in selected.values() {
        if counts_as_running(summary) {
            running = running.saturating_add(1);
        }
        if summary.status_bucket == FleetStatusBucketWire::Waiting {
            waiting = waiting.saturating_add(1);
        }
        if summary.needs_attention {
            attention = attention.saturating_add(1);
        }
        if summary.occupied_runner_slot {
            occupied = occupied.saturating_add(1);
        }
        max_revision =
            Some(max_revision.map_or(summary.row_revision.revision, |value| {
                value.max(summary.row_revision.revision)
            }));
        observed_at_unix_max = Some(
            observed_at_unix_max.map_or(summary.observed_at_unix, |value| {
                value.max(summary.observed_at_unix)
            }),
        );
    }
    Ok(FleetLogicalAgentCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        basis: FleetCountBasisWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            input_rows: request.summaries.len() as u64,
            selected_rows: selected.len() as u64,
            max_revision,
            observed_at_unix_max,
        },
        logical_agent_total: selected.len() as u64,
        running,
        waiting,
        attention,
        occupied_runner_slots: occupied,
    })
}

pub fn follow_record_key(
    record: &FollowRecordWire,
) -> Result<String, FleetContractError> {
    record.validate()?;
    Ok(follow_record_key_unchecked(
        record.logical_key.as_str(),
        record.created_by,
    ))
}

pub fn reconcile_follow_records(
    request: &FollowReconciliationRequestWire,
) -> Result<FollowReconciliationWire, FleetContractError> {
    validate_schema("follow reconciliation request", request.schema_version)?;
    validate_timestamp("now_unix", request.now_unix)?;
    let input_records = request.records.clone();
    let input_tombstones = request.tombstones.clone();
    let mut diagnostics = Vec::new();
    let mut tombstones = normalize_follow_tombstones(&request.tombstones)?;
    let mut records = normalize_follow_records(&request.records)?;

    for promotion in &request.promotions {
        apply_follow_promotion(
            &mut records,
            &tombstones,
            promotion,
            request.now_unix,
            &mut diagnostics,
        )?;
    }
    for activation in &request.activations {
        apply_follow_activation(
            &mut records,
            &tombstones,
            activation,
            &mut diagnostics,
        )?;
    }

    tombstones = normalize_follow_tombstones(
        &tombstones.into_values().collect::<Vec<_>>(),
    )?;
    let mut filtered_records = BTreeMap::new();
    for record in records.into_values() {
        if follow_tombstone_blocks_record(&tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_tombstone_blocked",
                "explicit unfollow tombstone suppressed automatic follow",
                Some(record.logical_key.clone()),
            )?);
            continue;
        }
        filtered_records.insert(
            follow_record_key_unchecked(
                record.logical_key.as_str(),
                record.created_by,
            ),
            record,
        );
    }

    let records = filtered_records.into_values().collect::<Vec<_>>();
    let tombstones = tombstones.into_values().collect::<Vec<_>>();
    let changed = records != input_records || tombstones != input_tombstones;
    Ok(FollowReconciliationWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        records,
        tombstones,
        changed,
        diagnostics,
    })
}

pub fn count_focus_and_fleet(
    request: &FocusFleetCountsRequestWire,
) -> Result<FocusFleetCountsWire, FleetContractError> {
    validate_schema("focus/fleet counts request", request.schema_version)?;
    let focus = count_scope(
        &request.local_summaries,
        &request.followed_remote_hosts,
        "followed_remote_hosts",
    )?;
    let fleet = count_scope(&[], &request.fleet_hosts, "fleet_hosts")?;
    Ok(FocusFleetCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        focus,
        fleet,
    })
}

pub fn classify_cursor_replay(
    request: &CursorReplayRequestWire,
) -> Result<CursorReplayDecisionWire, FleetContractError> {
    validate_schema("cursor replay request", request.schema_version)?;
    request.cursor.validate()?;
    validate_identifier("current_generation", &request.current_generation)?;
    if request.oldest_replayable_sequence > request.newest_sequence {
        return Err(FleetContractError::Validation(
            "oldest_replayable_sequence cannot exceed newest_sequence"
                .to_string(),
        ));
    }
    let resync = |reason| CursorReplayDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        classification: CursorReplayClassificationWire::ResyncRequired,
        reason,
        replay_from_sequence: None,
        authoritative_generation: request.current_generation.clone(),
        authoritative_newest_sequence: request.newest_sequence,
    };
    if !request.deletion_history_complete {
        return Ok(resync(CursorReplayReasonWire::IncompleteDeletionHistory));
    }
    if request.cursor.store_generation == FLEET_INITIAL_CURSOR_GENERATION
        && request.cursor.sequence == 0
    {
        return Ok(resync(CursorReplayReasonWire::InitialCursor));
    }
    if request.cursor.store_generation != request.current_generation {
        return Ok(resync(CursorReplayReasonWire::GenerationMismatch));
    }
    if request.cursor.sequence > request.newest_sequence {
        return Ok(resync(CursorReplayReasonWire::SequenceAheadOfAuthority));
    }
    if request.cursor.sequence == request.newest_sequence {
        return Ok(CursorReplayDecisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            classification: CursorReplayClassificationWire::Current,
            reason: CursorReplayReasonWire::AtAuthorityHead,
            replay_from_sequence: None,
            authoritative_generation: request.current_generation.clone(),
            authoritative_newest_sequence: request.newest_sequence,
        });
    }
    let replay_from = request.cursor.sequence.saturating_add(1);
    if replay_from < request.oldest_replayable_sequence {
        return Ok(resync(CursorReplayReasonWire::ReplayGap));
    }
    Ok(CursorReplayDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        classification: CursorReplayClassificationWire::Replayable,
        reason: CursorReplayReasonWire::BehindWithinReplayWindow,
        replay_from_sequence: Some(replay_from),
        authoritative_generation: request.current_generation.clone(),
        authoritative_newest_sequence: request.newest_sequence,
    })
}

pub fn operation_payload_fingerprint(
    request: &PayloadFingerprintRequestWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    validate_schema("payload fingerprint request", request.schema_version)?;
    let mut hasher = Sha256::new();
    hasher.update(PAYLOAD_FINGERPRINT_DOMAIN);
    canonical_json_into(&request.payload, &mut hasher)?;
    Ok(PayloadFingerprintWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        sha256: hex::encode(hasher.finalize()),
    })
}

pub fn decide_operation_replay(
    request: &OperationDecisionRequestWire,
) -> Result<OperationDecisionWire, FleetContractError> {
    validate_schema("operation decision request", request.schema_version)?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
    request.target.validate()?;
    request
        .resource_revision
        .validate_for_logical(&request.target.logical)?;
    validate_timestamp("now_unix", request.now_unix)?;
    if !request.acceptance_window_seconds.is_finite()
        || request.acceptance_window_seconds < 0.0
    {
        return Err(FleetContractError::Validation(
            "acceptance_window_seconds must be finite and non-negative"
                .to_string(),
        ));
    }
    let now_ms = timestamp_ms("now_unix", request.now_unix)?;
    let window_ms = duration_ms(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    let expires_at = now_ms.saturating_add(window_ms);
    if let Some(record) = &request.existing_record {
        record.validate()?;
        if record.receipt.key != request.key {
            return Err(FleetContractError::Validation(
                "existing operation record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(operation_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(operation_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.target != request.target
            || record.receipt.resource_revision != request.resource_revision
        {
            return Ok(operation_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(operation_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = OperationReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target: request.target.clone(),
        resource_revision: request.resource_revision.clone(),
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
    };
    Ok(operation_decision(
        OperationDecisionKindWire::AcceptNew,
        OperationDecisionReasonWire::UnseenInWindow,
        Some(receipt),
    ))
}

pub fn validate_connection_plan(
    plan: &ConnectionPlanWire,
) -> Result<ConnectionPlanWire, FleetContractError> {
    validate_schema("connection plan", plan.schema_version)?;
    validate_reference_id("provider_ref", &plan.provider_ref)?;
    validate_reference_id("credential_ref", &plan.credential_ref)?;
    validate_installation_id(&plan.pinned_installation_id)?;
    validate_absolute_https_endpoint(&plan.endpoint)?;
    plan.tls.validate()?;
    Ok(plan.clone())
}

pub fn classify_runtime_duration(
    request: &RuntimeDurationRequestWire,
) -> Result<RuntimeDurationWire, FleetContractError> {
    validate_schema("runtime duration request", request.schema_version)?;
    validate_timestamp("owner_started_at_unix", request.owner_started_at_unix)?;
    validate_timestamp(
        "owner_observed_at_unix",
        request.owner_observed_at_unix,
    )?;
    if let Some(stopped) = request.owner_stopped_at_unix {
        validate_timestamp("owner_stopped_at_unix", stopped)?;
    }
    if !request.max_clock_anomaly_seconds.is_finite()
        || request.max_clock_anomaly_seconds < 0.0
    {
        return Err(FleetContractError::Validation(
            "max_clock_anomaly_seconds must be finite and non-negative"
                .to_string(),
        ));
    }
    let end = request
        .owner_stopped_at_unix
        .unwrap_or(request.owner_observed_at_unix);
    let raw = end - request.owner_started_at_unix;
    let stopped = request.owner_stopped_at_unix.is_some();
    if raw >= 0.0 {
        return Ok(RuntimeDurationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            elapsed_seconds: raw,
            state: if stopped {
                RuntimeDurationStateWire::Stopped
            } else {
                RuntimeDurationStateWire::Running
            },
            clamped: false,
        });
    }
    if raw.abs() <= request.max_clock_anomaly_seconds {
        return Ok(RuntimeDurationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            elapsed_seconds: 0.0,
            state: RuntimeDurationStateWire::ClockAnomalyClamped,
            clamped: true,
        });
    }
    Err(FleetContractError::Validation(
        "owner timestamps are ordered backwards beyond the allowed clock anomaly"
            .to_string(),
    ))
}

pub fn classify_cache_freshness(
    request: &CacheFreshnessRequestWire,
) -> Result<CacheFreshnessWire, FleetContractError> {
    validate_schema("cache freshness request", request.schema_version)?;
    validate_non_negative_seconds(
        "fresh_threshold_seconds",
        request.fresh_threshold_seconds,
    )?;
    validate_non_negative_seconds(
        "stale_threshold_seconds",
        request.stale_threshold_seconds,
    )?;
    if request.fresh_threshold_seconds > request.stale_threshold_seconds {
        return Err(FleetContractError::Validation(
            "fresh_threshold_seconds cannot exceed stale_threshold_seconds"
                .to_string(),
        ));
    }
    let Some(age) = request.viewer_monotonic_elapsed_seconds else {
        return Ok(CacheFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Unknown,
            age_seconds: None,
        });
    };
    validate_non_negative_seconds("viewer_monotonic_elapsed_seconds", age)?;
    let freshness = if age <= request.fresh_threshold_seconds {
        ObservationFreshnessWire::Fresh
    } else if age >= request.stale_threshold_seconds {
        ObservationFreshnessWire::Stale
    } else {
        ObservationFreshnessWire::Aging
    };
    Ok(CacheFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness,
        age_seconds: Some(age),
    })
}

impl OriginLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("origin locator", self.schema_version)?;
        validate_installation_id(&self.installation_id)
    }
}

impl ProjectLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("project locator", self.schema_version)?;
        self.origin.validate()?;
        validate_identifier("project_id", &self.project_id)
    }
}

impl LogicalAgentLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("logical agent locator", self.schema_version)?;
        self.project.validate()?;
        validate_identifier("agent_id", &self.agent_id)?;
        if let Some(family_id) = &self.family_id {
            validate_identifier("family_id", family_id)?;
        }
        Ok(())
    }
}

impl AgentInstanceLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("agent instance locator", self.schema_version)?;
        self.logical.validate()?;
        validate_identifier("shell_id", &self.shell_id)?;
        validate_identifier("run_id", &self.run_id)?;
        validate_identifier("attempt_id", &self.attempt_id)?;
        Ok(())
    }
}

impl ResourceRevisionWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("resource revision", self.schema_version)?;
        validate_key("resource revision logical_key", &self.logical_key)
    }

    fn validate_for_logical(
        &self,
        logical: &LogicalAgentLocatorWire,
    ) -> Result<(), FleetContractError> {
        self.validate()?;
        let expected = logical_key_unchecked(logical);
        if self.logical_key != expected {
            return Err(FleetContractError::Validation(
                "resource revision belongs to a different logical identity"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl CapabilitySetWire {
    fn normalized(&self) -> Result<Self, FleetContractError> {
        validate_schema("capability set", self.schema_version)?;
        Ok(Self {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            resource: normalize_capabilities("resource", &self.resource)?,
            host: normalize_capabilities("host", &self.host)?,
            protocol: normalize_capabilities("protocol", &self.protocol)?,
        })
    }
}

impl ContentHandleWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("content handle", self.schema_version)?;
        validate_reference_id("content handle id", &self.id)?;
        reject_path_like("content handle id", &self.id)?;
        reject_secretish("content handle id", &self.id)?;
        if let Some(revision) = &self.revision {
            revision.validate()?;
        }
        if let Some(digest) = &self.digest {
            validate_sha256_digest("content handle digest", digest)?;
        }
        Ok(())
    }
}

impl ContentMetadataWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("content metadata", self.schema_version)?;
        let mut previous = None;
        for kind in &self.kinds {
            if previous.is_some_and(|prior| prior >= *kind) {
                return Err(FleetContractError::Validation(
                    "content metadata kinds must be sorted and deduplicated"
                        .to_string(),
                ));
            }
            previous = Some(*kind);
        }
        Ok(())
    }
}

impl StoreCursorWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("store cursor", self.schema_version)?;
        validate_identifier("store_generation", &self.store_generation)
    }
}

impl ScopedOperationKeyWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("scoped operation key", self.schema_version)?;
        validate_reference_id("controller_id", &self.controller_id)?;
        validate_reference_id("operation_id", &self.operation_id)
    }
}

impl PayloadFingerprintWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("payload fingerprint", self.schema_version)?;
        validate_sha256_digest("payload fingerprint", &self.sha256)
    }
}

impl OperationReceiptWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("operation receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        self.target.validate()?;
        self.resource_revision
            .validate_for_logical(&self.target.logical)?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "operation receipt expires before it was accepted".to_string(),
            ));
        }
        Ok(())
    }
}

impl DurableOperationRecordWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable operation record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "operation tombstone predates acceptance".to_string(),
            ));
        }
        Ok(())
    }
}

impl FollowRecordWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow record", self.schema_version)?;
        self.logical_locator.validate()?;
        let expected_key = logical_key_unchecked(&self.logical_locator);
        if self.logical_key != expected_key {
            return Err(FleetContractError::Validation(
                "follow record logical_key does not match logical locator"
                    .to_string(),
            ));
        }
        validate_timestamp(
            "follow record created_at_unix",
            self.created_at_unix,
        )?;
        validate_timestamp(
            "follow record updated_at_unix",
            self.updated_at_unix,
        )?;
        if self.updated_at_unix < self.created_at_unix {
            return Err(FleetContractError::Validation(
                "follow record updated_at_unix predates created_at_unix"
                    .to_string(),
            ));
        }
        match (self.state, self.activated_at_unix) {
            (FollowStateWire::Active, Some(value)) => {
                validate_timestamp("follow record activated_at_unix", value)?;
                if value < self.created_at_unix {
                    return Err(FleetContractError::Validation(
                        "follow record activated_at_unix predates created_at_unix"
                            .to_string(),
                    ));
                }
            }
            (FollowStateWire::Active, None) => {
                return Err(FleetContractError::Validation(
                    "active follow record requires activated_at_unix"
                        .to_string(),
                ));
            }
            (FollowStateWire::Pending, Some(_)) => {
                return Err(FleetContractError::Validation(
                    "pending follow record must not include activated_at_unix"
                        .to_string(),
                ));
            }
            (FollowStateWire::Pending, None) => {}
        }
        if let Some(key) = &self.operation_key {
            key.validate()?;
        }
        if self.created_by == FollowCreatedByWire::Dispatch
            && self.operation_key.is_none()
        {
            return Err(FleetContractError::Validation(
                "dispatch follow record requires operation_key".to_string(),
            ));
        }
        Ok(())
    }
}

impl FollowTombstoneWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow tombstone", self.schema_version)?;
        self.logical_locator.validate()?;
        let expected_key = logical_key_unchecked(&self.logical_locator);
        if self.logical_key != expected_key {
            return Err(FleetContractError::Validation(
                "follow tombstone logical_key does not match logical locator"
                    .to_string(),
            ));
        }
        validate_timestamp(
            "follow tombstone unfollowed_at_unix",
            self.unfollowed_at_unix,
        )
    }
}

impl FollowFamilyPromotionWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow family promotion", self.schema_version)?;
        self.from.validate()?;
        self.to.validate()?;
        if self.from.family_id.is_some() {
            return Err(FleetContractError::Validation(
                "follow family promotion source must be a singleton locator"
                    .to_string(),
            ));
        }
        if self.to.family_id.is_none() {
            return Err(FleetContractError::Validation(
                "follow family promotion target must include family_id"
                    .to_string(),
            ));
        }
        if self.from.project != self.to.project
            || self.from.agent_id != self.to.agent_id
        {
            return Err(FleetContractError::Validation(
                "follow family promotion must keep origin, project, and agent_id"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl FollowActivationWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow activation", self.schema_version)?;
        self.logical_locator.validate()?;
        if let Some(key) = &self.operation_key {
            key.validate()?;
        }
        validate_timestamp(
            "follow activation activated_at_unix",
            self.activated_at_unix,
        )
    }
}

impl FleetHostCountInputWire {
    fn validate(&self, label: &str) -> Result<(), FleetContractError> {
        validate_schema(label, self.schema_version)?;
        self.origin.validate()?;
        if let Some(observed) = self.observed_at_unix {
            validate_timestamp("host observed_at_unix", observed)?;
        }
        for summary in &self.summaries {
            let summary = validate_resolved_agent_summary(summary)?;
            if summary.logical_locator.project.origin != self.origin {
                return Err(FleetContractError::Validation(format!(
                    "{label} summary belongs to a different origin"
                )));
            }
        }
        Ok(())
    }
}

impl TlsTrustSettingsWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("tls trust settings", self.schema_version)?;
        match self.mode {
            TlsTrustModeWire::SystemRoots => {
                if self.ca_ref.is_some() || self.server_name_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "system_roots trust mode must not include CA or server-name refs"
                            .to_string(),
                    ));
                }
            }
            TlsTrustModeWire::PinnedCa => {
                let Some(ca_ref) = &self.ca_ref else {
                    return Err(FleetContractError::Validation(
                        "pinned_ca trust mode requires ca_ref".to_string(),
                    ));
                };
                validate_reference_id("ca_ref", ca_ref)?;
                if self.server_name_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "pinned_ca trust mode must not include server_name_ref"
                            .to_string(),
                    ));
                }
            }
            TlsTrustModeWire::PinnedServerName => {
                let Some(server_name_ref) = &self.server_name_ref else {
                    return Err(FleetContractError::Validation(
                        "pinned_server_name trust mode requires server_name_ref"
                            .to_string(),
                    ));
                };
                validate_reference_id("server_name_ref", server_name_ref)?;
                if self.ca_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "pinned_server_name trust mode must not include ca_ref"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_projection_request(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<(), FleetContractError> {
    validate_schema(
        "resolved agent projection request",
        request.schema_version,
    )?;
    request.logical_locator.validate()?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    let logical_key = logical_key_unchecked(&request.logical_locator);
    facts.row_revision.validate()?;
    if facts.row_revision.logical_key != logical_key {
        return Err(FleetContractError::Validation(
            "row revision belongs to a different logical identity".to_string(),
        ));
    }
    if let Some(exact) = &facts.exact_locator {
        if exact.logical != request.logical_locator {
            return Err(FleetContractError::Validation(
                "owner facts exact locator belongs to a different logical identity"
                    .to_string(),
            ));
        }
    }
    validate_label(
        "record.project_name",
        &request.record.project_name,
        MAX_LABEL_BYTES,
    )?;
    validate_label(
        "record.workflow_dir_name",
        &request.record.workflow_dir_name,
        MAX_LABEL_BYTES,
    )?;
    Ok(())
}

fn normalized_owner_facts(
    facts: &OwnerResolutionFactsWire,
) -> Result<OwnerResolutionFactsWire, FleetContractError> {
    validate_schema("owner resolution facts", facts.schema_version)?;
    if let Some(exact) = &facts.exact_locator {
        exact.validate()?;
    }
    facts.row_revision.validate()?;
    validate_timestamp("observed_at_unix", facts.observed_at_unix)?;
    let capabilities = facts.capabilities.normalized()?;
    let mut content_handles = facts.content_handles.clone();
    for handle in &content_handles {
        handle.validate()?;
    }
    content_handles.sort_by(|left, right| {
        (left.kind, left.id.as_str()).cmp(&(right.kind, right.id.as_str()))
    });
    content_handles
        .dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    Ok(OwnerResolutionFactsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        exact_locator: facts.exact_locator.clone(),
        row_revision: facts.row_revision.clone(),
        liveness: facts.liveness,
        connection_health: facts.connection_health,
        freshness: facts.freshness,
        observed_at_unix: facts.observed_at_unix,
        row_kind: facts.row_kind,
        current_instance: facts.current_instance,
        dismissable: facts.dismissable,
        needs_attention: facts.needs_attention,
        occupied_runner_slot: facts.occupied_runner_slot,
        container_projected_concrete_agent: facts
            .container_projected_concrete_agent,
        capabilities,
        content_handles,
    })
}

fn reject_inconsistent_projection(
    record: &AgentArtifactRecordWire,
    facts: &OwnerResolutionFactsWire,
    lifecycle: FleetLifecycleWire,
) -> Result<(), FleetContractError> {
    if terminal_lifecycle(lifecycle)
        && facts.liveness == OwnerLivenessWire::Alive
    {
        return Err(FleetContractError::Validation(
            "owner facts mark a terminal record as live".to_string(),
        ));
    }
    if facts
        .capabilities
        .resource
        .iter()
        .any(|capability| actionable_capability(capability))
        && facts.exact_locator.is_none()
    {
        return Err(FleetContractError::Validation(
            "actionable resource capability requires an exact instance locator"
                .to_string(),
        ));
    }
    if facts
        .capabilities
        .resource
        .iter()
        .any(|capability| content_capability(capability))
        && facts.content_handles.is_empty()
    {
        return Err(FleetContractError::Validation(
            "content resource capability requires an opaque content handle"
                .to_string(),
        ));
    }
    if facts.row_kind == FleetRowKindWire::Proc
        && facts
            .capabilities
            .resource
            .iter()
            .any(|capability| actionable_capability(capability))
    {
        return Err(FleetContractError::Validation(
            "proc rows cannot expose agent mutation capabilities".to_string(),
        ));
    }
    if record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.proc_id.as_ref())
        .is_some()
        && facts.row_kind == FleetRowKindWire::AgentShell
    {
        return Err(FleetContractError::Validation(
            "proc artifact records must not be projected as agent shells"
                .to_string(),
        ));
    }
    Ok(())
}

fn counts_as_running(summary: &ResolvedAgentSummaryWire) -> bool {
    match summary.status_bucket {
        FleetStatusBucketWire::Running => !summary.dismissable,
        FleetStatusBucketWire::Starting => {
            summary.container_projected_concrete_agent
        }
        _ => false,
    }
}

fn lifecycle_for_record(
    record: &AgentArtifactRecordWire,
) -> FleetLifecycleWire {
    if let Some(done) = &record.done {
        if done
            .error
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || done.outcome.as_deref().is_some_and(|value| {
                value.starts_with("failed") || value == "failed"
            })
            || done
                .status_label
                .as_deref()
                .is_some_and(|value| value.starts_with("FAILED"))
        {
            return FleetLifecycleWire::Failed;
        }
        return FleetLifecycleWire::Terminal;
    }
    if record.pending_question.is_some() {
        return FleetLifecycleWire::Asking;
    }
    if record.waiting.is_some() {
        return FleetLifecycleWire::Waiting;
    }
    if workflow_status_is_starting(record) {
        return FleetLifecycleWire::Starting;
    }
    if record.running.is_some() || workflow_status_is_running(record) {
        return FleetLifecycleWire::Running;
    }
    FleetLifecycleWire::Unknown
}

fn terminal_lifecycle(lifecycle: FleetLifecycleWire) -> bool {
    matches!(
        lifecycle,
        FleetLifecycleWire::Terminal | FleetLifecycleWire::Failed
    )
}

fn bucket_for_lifecycle(
    lifecycle: FleetLifecycleWire,
) -> FleetStatusBucketWire {
    match lifecycle {
        FleetLifecycleWire::Starting => FleetStatusBucketWire::Starting,
        FleetLifecycleWire::Running | FleetLifecycleWire::Unknown => {
            FleetStatusBucketWire::Running
        }
        FleetLifecycleWire::Waiting => FleetStatusBucketWire::Waiting,
        FleetLifecycleWire::Asking => FleetStatusBucketWire::Stopped,
        FleetLifecycleWire::Terminal => FleetStatusBucketWire::Done,
        FleetLifecycleWire::Failed => FleetStatusBucketWire::Failed,
    }
}

fn status_for_record(record: &AgentArtifactRecordWire) -> String {
    if let Some(done) = &record.done {
        if let Some(status) = first_non_empty([done.status_label.as_deref()]) {
            return status.to_string();
        }
        if done
            .error
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || done.outcome.as_deref().is_some_and(|value| {
                value.starts_with("failed") || value == "failed"
            })
        {
            return "FAILED".to_string();
        }
        return "DONE".to_string();
    }
    if record.pending_question.is_some() {
        return "QUESTION".to_string();
    }
    if record.waiting.is_some() {
        return "WAITING".to_string();
    }
    if workflow_status_is_starting(record) {
        return "STARTING".to_string();
    }
    if record.running.is_some() || workflow_status_is_running(record) {
        return "RUNNING".to_string();
    }
    "UNKNOWN".to_string()
}

fn workflow_status_is_starting(record: &AgentArtifactRecordWire) -> bool {
    record.workflow_state.as_ref().is_some_and(|state| {
        state.appears_as_agent && state.status.eq_ignore_ascii_case("starting")
    })
}

fn workflow_status_is_running(record: &AgentArtifactRecordWire) -> bool {
    record.workflow_state.as_ref().is_some_and(|state| {
        state.appears_as_agent
            && matches!(
                state.status.to_ascii_lowercase().as_str(),
                "running" | "waiting" | "queued"
            )
    })
}

fn model_for_record(
    meta: Option<&AgentMetaWire>,
    done: Option<&DoneMarkerWire>,
    running: Option<&RunningMarkerWire>,
) -> Option<String> {
    first_non_empty([
        meta.and_then(|value| value.model.as_deref()),
        running.and_then(|value| value.model.as_deref()),
        done.and_then(|value| value.model.as_deref()),
    ])
    .map(|value| trim_to_limit(value, MAX_LABEL_BYTES))
}

fn provider_for_record(
    meta: Option<&AgentMetaWire>,
    done: Option<&DoneMarkerWire>,
    running: Option<&RunningMarkerWire>,
) -> Option<String> {
    first_non_empty([
        meta.and_then(|value| value.llm_provider.as_deref()),
        running.and_then(|value| value.llm_provider.as_deref()),
        done.and_then(|value| value.llm_provider.as_deref()),
    ])
    .map(|value| trim_to_limit(value, MAX_LABEL_BYTES))
}

fn intent_for_record(record: &AgentArtifactRecordWire) -> Option<String> {
    first_non_empty([
        record
            .agent_meta
            .as_ref()
            .and_then(|value| value.plan_action.as_deref()),
        record.raw_prompt_snippet.as_deref(),
    ])
    .map(|value| trim_to_limit(value, MAX_INTENT_BYTES))
}

fn first_non_empty<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn content_metadata(
    handles: &[ContentHandleWire],
) -> Result<ContentMetadataWire, FleetContractError> {
    let mut total: Option<u64> = Some(0);
    let mut kinds = BTreeSet::new();
    let mut supports_range = false;
    let mut supports_growth = false;
    for handle in handles {
        handle.validate()?;
        kinds.insert(handle.kind);
        supports_range |= handle.supports_range;
        supports_growth |= handle.supports_growth;
        match (total, handle.byte_len) {
            (Some(left), Some(right)) => total = left.checked_add(right),
            _ => total = None,
        }
    }
    Ok(ContentMetadataWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        handle_count: handles.len() as u64,
        total_byte_len: total,
        kinds: kinds.into_iter().collect(),
        supports_range,
        supports_growth,
    })
}

fn operation_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<OperationReceiptWire>,
) -> OperationDecisionWire {
    OperationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        decision,
        reason,
        receipt,
    }
}

fn normalize_follow_records(
    records: &[FollowRecordWire],
) -> Result<BTreeMap<String, FollowRecordWire>, FleetContractError> {
    let mut normalized = BTreeMap::new();
    for record in records {
        record.validate()?;
        let key = follow_record_key_unchecked(
            record.logical_key.as_str(),
            record.created_by,
        );
        match normalized.get(&key) {
            Some(existing)
                if follow_record_prefer_existing(existing, record) => {}
            _ => {
                normalized.insert(key, record.clone());
            }
        }
    }
    Ok(normalized)
}

fn normalize_follow_tombstones(
    tombstones: &[FollowTombstoneWire],
) -> Result<BTreeMap<String, FollowTombstoneWire>, FleetContractError> {
    let mut normalized: BTreeMap<String, FollowTombstoneWire> = BTreeMap::new();
    for tombstone in tombstones {
        tombstone.validate()?;
        match normalized.get(&tombstone.logical_key) {
            Some(existing)
                if existing.unfollowed_at_unix
                    >= tombstone.unfollowed_at_unix => {}
            _ => {
                normalized
                    .insert(tombstone.logical_key.clone(), tombstone.clone());
            }
        }
    }
    Ok(normalized)
}

fn follow_record_prefer_existing(
    existing: &FollowRecordWire,
    candidate: &FollowRecordWire,
) -> bool {
    if existing.updated_at_unix != candidate.updated_at_unix {
        return existing.updated_at_unix > candidate.updated_at_unix;
    }
    if existing.state != candidate.state {
        return existing.state == FollowStateWire::Active;
    }
    true
}

fn apply_follow_promotion(
    records: &mut BTreeMap<String, FollowRecordWire>,
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    promotion: &FollowFamilyPromotionWire,
    now_unix: f64,
    diagnostics: &mut Vec<FollowDiagnosticWire>,
) -> Result<(), FleetContractError> {
    promotion.validate()?;
    validate_timestamp("follow promotion now_unix", now_unix)?;
    let from_key = logical_key_unchecked(&promotion.from);
    let to_logical_key = logical_key_unchecked(&promotion.to);
    let candidates =
        [FollowCreatedByWire::Explicit, FollowCreatedByWire::Dispatch];
    for created_by in candidates {
        let record_key = follow_record_key_unchecked(&from_key, created_by);
        let Some(mut record) = records.remove(&record_key) else {
            continue;
        };
        if follow_tombstone_blocks_record(tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_promotion_source_tombstoned",
                "explicit unfollow tombstone suppressed source follow promotion",
                Some(record.logical_key.clone()),
            )?);
            continue;
        }
        record.logical_locator = promotion.to.clone();
        record.logical_key = to_logical_key.clone();
        record.updated_at_unix = record.updated_at_unix.max(now_unix);
        record.validate()?;
        if follow_tombstone_blocks_record(tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_promotion_tombstoned",
                "explicit unfollow tombstone suppressed family promotion",
                Some(to_logical_key.clone()),
            )?);
            continue;
        }
        let to_record_key =
            follow_record_key_unchecked(&to_logical_key, created_by);
        match records.get(&to_record_key) {
            Some(existing)
                if follow_record_prefer_existing(existing, &record) => {}
            _ => {
                records.insert(to_record_key, record);
            }
        }
    }
    Ok(())
}

fn apply_follow_activation(
    records: &mut BTreeMap<String, FollowRecordWire>,
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    activation: &FollowActivationWire,
    diagnostics: &mut Vec<FollowDiagnosticWire>,
) -> Result<(), FleetContractError> {
    activation.validate()?;
    let logical_key = logical_key_unchecked(&activation.logical_locator);
    let record_key = follow_record_key_unchecked(
        &logical_key,
        FollowCreatedByWire::Dispatch,
    );
    let Some(record) = records.get_mut(&record_key) else {
        return Ok(());
    };
    if let Some(expected) = &activation.operation_key {
        if record.operation_key.as_ref() != Some(expected) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Warning,
                "follow_activation_operation_mismatch",
                "dispatch follow activation operation_key did not match record",
                Some(logical_key),
            )?);
            return Ok(());
        }
    }
    record.state = FollowStateWire::Active;
    record.activated_at_unix = Some(activation.activated_at_unix);
    record.updated_at_unix =
        record.updated_at_unix.max(activation.activated_at_unix);
    record.validate()?;
    if follow_tombstone_blocks_record(tombstones, record) {
        diagnostics.push(follow_diagnostic(
            FollowDiagnosticSeverityWire::Info,
            "follow_activation_tombstoned",
            "explicit unfollow tombstone suppressed dispatch follow activation",
            Some(record.logical_key.clone()),
        )?);
    }
    Ok(())
}

fn follow_tombstone_blocks_record(
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    record: &FollowRecordWire,
) -> bool {
    let Some(tombstone) = tombstones.get(&record.logical_key) else {
        return false;
    };
    record.created_by != FollowCreatedByWire::Explicit
        || tombstone.unfollowed_at_unix >= record.updated_at_unix
}

fn follow_diagnostic(
    severity: FollowDiagnosticSeverityWire,
    code: &str,
    message: &str,
    logical_key: Option<String>,
) -> Result<FollowDiagnosticWire, FleetContractError> {
    validate_identifier("follow diagnostic code", code)?;
    validate_label("follow diagnostic message", message, MAX_LABEL_BYTES)?;
    Ok(FollowDiagnosticWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        severity,
        code: code.to_string(),
        message: message.to_string(),
        logical_key,
    })
}

fn count_scope(
    local_summaries: &[ResolvedAgentSummaryWire],
    hosts: &[FleetHostCountInputWire],
    label: &str,
) -> Result<FleetScopeCountsWire, FleetContractError> {
    let mut host_origins = BTreeSet::new();
    let mut summaries = Vec::new();
    summaries.extend_from_slice(local_summaries);
    let mut unknown_origins = Vec::new();
    let mut host_counts = Vec::new();
    let mut observed_at_unix_max = None;
    for summary in local_summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        observed_at_unix_max = max_optional_f64(
            observed_at_unix_max,
            Some(summary.observed_at_unix),
        );
    }
    for host in hosts {
        host.validate(label)?;
        let origin_key = host.origin.installation_id.clone();
        if !host_origins.insert(origin_key.clone()) {
            return Err(FleetContractError::Validation(format!(
                "{label} contains duplicate origin {origin_key}"
            )));
        }
        summaries.extend_from_slice(&host.summaries);
        let counts =
            count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                summaries: host.summaries.clone(),
            })?;
        let partial = host.freshness == ObservationFreshnessWire::Unknown;
        if partial {
            unknown_origins.push(origin_key);
        }
        let host_observed_at = max_optional_f64(
            host.observed_at_unix,
            counts.basis.observed_at_unix_max,
        );
        observed_at_unix_max =
            max_optional_f64(observed_at_unix_max, host_observed_at);
        host_counts.push(FleetHostCountWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: host.origin.clone(),
            counts,
            partial,
            observed_at_unix: host_observed_at,
        });
    }
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries,
    })?;
    observed_at_unix_max = max_optional_f64(
        observed_at_unix_max,
        counts.basis.observed_at_unix_max,
    );
    unknown_origins.sort();
    Ok(FleetScopeCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        counts,
        partial: !unknown_origins.is_empty(),
        observed_at_unix_max,
        unknown_origins,
        host_counts,
    })
}

fn max_optional_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn actionable_capability(value: &str) -> bool {
    matches!(
        value,
        "approve" | "answer" | "kill" | "resume" | "retry" | "stop"
    )
}

fn content_capability(value: &str) -> bool {
    matches!(value, "content.read" | "content.tail" | "content.range")
}

fn logical_key_unchecked(locator: &LogicalAgentLocatorWire) -> String {
    length_key([
        ("origin", locator.project.origin.installation_id.as_str()),
        ("project", locator.project.project_id.as_str()),
        ("family", locator.family_id.as_deref().unwrap_or("")),
        ("agent", locator.agent_id.as_str()),
    ])
}

fn instance_key_unchecked(locator: &AgentInstanceLocatorWire) -> String {
    format!(
        "{}|{}",
        logical_key_unchecked(&locator.logical),
        length_key([
            ("shell", locator.shell_id.as_str()),
            ("run", locator.run_id.as_str()),
            ("attempt", locator.attempt_id.as_str()),
        ])
    )
}

fn follow_record_key_unchecked(
    logical_key: &str,
    created_by: FollowCreatedByWire,
) -> String {
    length_key([
        ("logical", logical_key),
        (
            "created_by",
            match created_by {
                FollowCreatedByWire::Explicit => "explicit",
                FollowCreatedByWire::Dispatch => "dispatch",
            },
        ),
    ])
}

fn length_key<'a>(
    segments: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
    let mut out = String::from("v1");
    for (name, value) in segments {
        out.push('|');
        out.push_str(name);
        out.push(':');
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(value);
    }
    out
}

fn validate_installation_record(
    record: &InstallationIdentityRecordWire,
) -> Result<(), FleetContractError> {
    if record.schema_version != FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION {
        return Err(FleetContractError::Validation(format!(
            "installation identity version {} is not supported (expected {})",
            record.schema_version, FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION
        )));
    }
    validate_installation_id(&record.installation_id)?;
    validate_timestamp("created_at_unix", record.created_at_unix)?;
    if record.generation == 0 {
        return Err(FleetContractError::Validation(
            "installation identity generation must be positive".to_string(),
        ));
    }
    if let Some(prior) = &record.prior_installation_id {
        validate_installation_id(prior)?;
        if prior == &record.installation_id {
            return Err(FleetContractError::Validation(
                "prior_installation_id must differ from installation_id"
                    .to_string(),
            ));
        }
    }
    if let Some(rotated_at) = record.rotated_at_unix {
        validate_timestamp("rotated_at_unix", rotated_at)?;
    }
    if let Some(adopted_at) = record.adopted_at_unix {
        validate_timestamp("adopted_at_unix", adopted_at)?;
    }
    if let Some(reason) = &record.reason {
        validate_label("identity reason", reason, MAX_LABEL_BYTES)?;
    }
    Ok(())
}

fn validate_schema(
    label: &str,
    version: u32,
) -> Result<(), FleetContractError> {
    if version != FLEET_CONTRACT_SCHEMA_VERSION {
        return Err(FleetContractError::Validation(format!(
            "{label} schema_version {version} is not supported (expected {FLEET_CONTRACT_SCHEMA_VERSION})"
        )));
    }
    Ok(())
}

fn validate_installation_id(value: &str) -> Result<(), FleetContractError> {
    if !value.starts_with(FLEET_INSTALLATION_ID_PREFIX) {
        return Err(FleetContractError::Validation(format!(
            "installation_id must start with {FLEET_INSTALLATION_ID_PREFIX:?}"
        )));
    }
    let suffix = &value[FLEET_INSTALLATION_ID_PREFIX.len()..];
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(
            "installation_id must end with 64 lowercase hex characters"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_identifier(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_reference_id(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    validate_identifier(field, value)?;
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'_' | b'-' | b'.' | b':')
    }) {
        return Err(FleetContractError::Validation(format!(
            "{field} must be an opaque reference identifier, not a path or inline secret"
        )));
    }
    reject_secretish(field, value)?;
    Ok(())
}

fn validate_capability(
    scope: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    validate_identifier(scope, value)?;
    if value.len() > MAX_CAPABILITY_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability exceeds {MAX_CAPABILITY_BYTES} bytes"
        )));
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability must be non-empty"
        )));
    };
    if !first.is_ascii_lowercase() {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability must start with a lowercase ASCII letter"
        )));
    }
    if !chars.all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | '_' | '-')
    }) {
        return Err(FleetContractError::Validation(format!(
            "{scope} capability contains unsupported characters"
        )));
    }
    Ok(())
}

fn normalize_capabilities(
    scope: &str,
    values: &[String],
) -> Result<Vec<String>, FleetContractError> {
    let mut set = BTreeSet::new();
    for value in values {
        validate_capability(scope, value)?;
        set.insert(value.trim().to_string());
    }
    Ok(set.into_iter().collect())
}

fn validate_key(field: &str, value: &str) -> Result<(), FleetContractError> {
    if value.is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > MAX_KEY_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_label(
    field: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), FleetContractError> {
    if value.trim().is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {max_bytes} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_timestamp(
    field: &str,
    value: f64,
) -> Result<(), FleetContractError> {
    if !value.is_finite() || value < 0.0 {
        return Err(FleetContractError::Validation(format!(
            "{field} must be finite and non-negative"
        )));
    }
    Ok(())
}

fn validate_non_negative_seconds(
    field: &str,
    value: f64,
) -> Result<(), FleetContractError> {
    if !value.is_finite() || value < 0.0 {
        return Err(FleetContractError::Validation(format!(
            "{field} must be finite and non-negative"
        )));
    }
    Ok(())
}

fn validate_sha256_digest(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must be a lowercase 64-character SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_absolute_https_endpoint(
    value: &str,
) -> Result<(), FleetContractError> {
    validate_label("endpoint", value, MAX_KEY_BYTES)?;
    reject_secretish("endpoint", value)?;
    if value.contains('#') {
        return Err(FleetContractError::Validation(
            "endpoint must not include a URL fragment".to_string(),
        ));
    }
    let Some(rest) = value.strip_prefix("https://") else {
        return Err(FleetContractError::Validation(
            "endpoint must be an absolute https:// URL".to_string(),
        ));
    };
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return Err(FleetContractError::Validation(
            "endpoint must include a host".to_string(),
        ));
    }
    if authority.contains('@') {
        return Err(FleetContractError::Validation(
            "endpoint must not include URL userinfo".to_string(),
        ));
    }
    if authority
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(FleetContractError::Validation(
            "endpoint host must not contain whitespace or control characters"
                .to_string(),
        ));
    }
    Ok(())
}

fn reject_path_like(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    if value.contains('/')
        || value.contains('\\')
        || value.starts_with('.')
        || value.starts_with('~')
        || value.contains("://")
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must be opaque and must not look like a path or URL"
        )));
    }
    Ok(())
}

fn reject_secretish(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    let lowercase = value.to_ascii_lowercase();
    if lowercase.contains("authorization:")
        || lowercase.contains("bearer ")
        || lowercase.contains("token=")
        || lowercase.contains("access_token")
        || lowercase.contains("password=")
        || lowercase.contains("secret=")
        || lowercase.contains("auth_header")
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain inline credentials or auth headers"
        )));
    }
    Ok(())
}

fn trim_to_limit(value: &str, max_bytes: usize) -> String {
    let value = value.trim();
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn timestamp_ms(field: &str, value: f64) -> Result<u64, FleetContractError> {
    validate_timestamp(field, value)?;
    let millis = value * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(FleetContractError::Validation(format!(
            "{field} is too large to represent in milliseconds"
        )));
    }
    Ok(millis.round() as u64)
}

fn duration_ms(field: &str, value: f64) -> Result<u64, FleetContractError> {
    validate_non_negative_seconds(field, value)?;
    let millis = value * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(FleetContractError::Validation(format!(
            "{field} is too large to represent in milliseconds"
        )));
    }
    Ok(millis.round() as u64)
}

fn load_identity_unlocked(
    path: &Path,
) -> Result<LoadedIdentity, FleetContractError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(LoadedIdentity::Missing)
        }
        Err(error) => return Err(io_error(path, error)),
    };
    if bytes.len() > FLEET_INSTALLATION_IDENTITY_MAX_BYTES {
        return Ok(LoadedIdentity::Unusable {
            message: format!(
                "identity file exceeds {FLEET_INSTALLATION_IDENTITY_MAX_BYTES} bytes"
            ),
        });
    }
    let record: InstallationIdentityRecordWire =
        match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(error) => {
                return Ok(LoadedIdentity::Unusable {
                    message: format!(
                        "identity file is not valid JSON: {error}"
                    ),
                })
            }
        };
    match validate_installation_record(&record) {
        Ok(()) => Ok(LoadedIdentity::Valid(record)),
        Err(error) => Ok(LoadedIdentity::Unusable {
            message: error.to_string(),
        }),
    }
}

fn write_identity_atomic(
    path: &Path,
    record: &InstallationIdentityRecordWire,
    precondition: WritePrecondition<'_>,
) -> Result<(), FleetContractError> {
    validate_installation_record(record)?;
    let parent = path.parent().ok_or_else(|| {
        FleetContractError::Validation(format!(
            "installation identity path has no parent: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
    restrict_dir(parent)?;
    reap_stale_temp_siblings(path, SystemTime::now());
    check_write_precondition(path, &precondition)?;
    let mut bytes = serde_json::to_vec_pretty(record).map_err(|source| {
        FleetContractError::Json {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    if bytes.len() > FLEET_INSTALLATION_IDENTITY_MAX_BYTES {
        return Err(FleetContractError::Validation(format!(
            "serialized installation identity exceeds {FLEET_INSTALLATION_IDENTITY_MAX_BYTES} bytes"
        )));
    }
    let tmp_path = temp_path_for(path);
    let write_result = (|| -> Result<(), FleetContractError> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let handle = options
            .open(&tmp_path)
            .map_err(|error| io_error(&tmp_path, error))?;
        let mut writer = BufWriter::new(handle);
        writer
            .write_all(&bytes)
            .map_err(|error| io_error(&tmp_path, error))?;
        writer.flush().map_err(|error| io_error(&tmp_path, error))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| io_error(&tmp_path, error))?;
        check_write_precondition(path, &precondition)?;
        fs::rename(&tmp_path, path).map_err(|error| io_error(path, error))?;
        restrict_file(path)?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn check_write_precondition(
    path: &Path,
    precondition: &WritePrecondition<'_>,
) -> Result<(), FleetContractError> {
    match precondition {
        WritePrecondition::Missing => {
            if path.exists() {
                return Err(FleetContractError::Validation(format!(
                    "identity store at {} was created concurrently and was left unchanged",
                    path.display()
                )));
            }
            Ok(())
        }
        WritePrecondition::Current(expected) => {
            match load_identity_unlocked(path)? {
                LoadedIdentity::Valid(record)
                    if record.installation_id == *expected =>
                {
                    Ok(())
                }
                LoadedIdentity::Valid(_) => {
                    Err(FleetContractError::Validation(
                        "identity store changed before atomic replacement"
                            .to_string(),
                    ))
                }
                LoadedIdentity::Missing => Err(FleetContractError::Validation(
                    "identity store disappeared before atomic replacement"
                        .to_string(),
                )),
                LoadedIdentity::Unusable { message } => {
                    Err(mutation_blocked(path, &message))
                }
            }
        }
    }
}

fn with_identity_lock<T>(
    sase_home: &Path,
    operation_name: &str,
    operation: impl FnOnce() -> Result<T, FleetContractError>,
) -> Result<T, FleetContractError> {
    fs::create_dir_all(sase_home)
        .map_err(|error| io_error(sase_home, error))?;
    restrict_dir(sase_home)?;
    let path = installation_identity_path(sase_home);
    let lock_path = lock_path_for(&path);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        operation_name,
    )
    .map_err(|error| lock_error_to_fleet(error, &lock_path))?;
    let result = operation();
    let unlock = unlock(lock, &lock_path);
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn lock_error_to_fleet(
    error: StoreLockError,
    path: &Path,
) -> FleetContractError {
    match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => FleetContractError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        StoreLockError::Open { source, .. }
        | StoreLockError::Acquire { source, .. } => io_error(path, source),
    }
}

fn unlock(lock: HeldStoreLock, path: &Path) -> Result<(), FleetContractError> {
    lock.release().map_err(|error| io_error(path, error))
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn reap_stale_temp_siblings(path: &Path, now: SystemTime) {
    let Some(parent) = path.parent() else {
        return;
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    let prefix = format!(".{filename}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix)
            || !name.ends_with(".tmp")
            || name.len() <= prefix.len() + ".tmp".len()
        {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age > STALE_TEMP_MAX_AGE {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn generate_installation_id() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("{FLEET_INSTALLATION_ID_PREFIX}{}", hex::encode(bytes))
}

fn current_unix_time() -> Result<f64, FleetContractError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            FleetContractError::Validation(format!(
                "could not read current Unix timestamp: {error}"
            ))
        })
}

fn restrict_dir(path: &Path) -> Result<(), FleetContractError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), FleetContractError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn mutation_blocked(path: &Path, reason: &str) -> FleetContractError {
    FleetContractError::Validation(format!(
        "cannot update installation identity at {}: {reason}. The file was left unchanged; move or repair it, then retry.",
        path.display()
    ))
}

fn io_error(path: &Path, source: io::Error) -> FleetContractError {
    FleetContractError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn canonical_json_into(
    value: &Value,
    out: &mut Sha256,
) -> Result<(), FleetContractError> {
    match value {
        Value::Null => out.update(b"null"),
        Value::Bool(true) => out.update(b"true"),
        Value::Bool(false) => out.update(b"false"),
        Value::Number(number) => out.update(number.to_string().as_bytes()),
        Value::String(text) => {
            let encoded = serde_json::to_string(text).map_err(|source| {
                FleetContractError::Json {
                    path: PathBuf::from("<payload>"),
                    source,
                }
            })?;
            out.update(encoded.as_bytes());
        }
        Value::Array(values) => {
            out.update(b"[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.update(b",");
                }
                canonical_json_into(item, out)?;
            }
            out.update(b"]");
        }
        Value::Object(map) => {
            out.update(b"{");
            let mut sorted = BTreeMap::new();
            for (key, item) in map {
                sorted.insert(key, item);
            }
            for (index, (key, item)) in sorted.iter().enumerate() {
                if index > 0 {
                    out.update(b",");
                }
                let encoded_key =
                    serde_json::to_string(key).map_err(|source| {
                        FleetContractError::Json {
                            path: PathBuf::from("<payload>"),
                            source,
                        }
                    })?;
                out.update(encoded_key.as_bytes());
                out.update(b":");
                canonical_json_into(item, out)?;
            }
            out.update(b"}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_scan::wire::PendingQuestionMarkerWire;
    use crate::agent_scan::{AgentArtifactRecordShapeWire, WaitingMarkerWire};
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    };
    use std::thread;
    use tempfile::tempdir;

    fn id(hex: char) -> String {
        format!(
            "{FLEET_INSTALLATION_ID_PREFIX}{}",
            hex.to_string().repeat(64)
        )
    }

    fn origin(hex: char) -> OriginLocatorWire {
        OriginLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            installation_id: id(hex),
        }
    }

    fn logical(hex: char, agent: &str) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: ProjectLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin(hex),
                project_id: "project-1".to_string(),
            },
            agent_id: agent.to_string(),
            family_id: Some("family-1".to_string()),
        }
    }

    fn exact(hex: char, agent: &str, run: &str) -> AgentInstanceLocatorWire {
        AgentInstanceLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical: logical(hex, agent),
            shell_id: "shell-1".to_string(),
            run_id: run.to_string(),
            attempt_id: "attempt-1".to_string(),
        }
    }

    fn revision(
        locator: &LogicalAgentLocatorWire,
        revision: u64,
    ) -> ResourceRevisionWire {
        ResourceRevisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key: logical_key_unchecked(locator),
            revision,
        }
    }

    fn caps(resource: &[&str]) -> CapabilitySetWire {
        CapabilitySetWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            resource: resource
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            host: Vec::new(),
            protocol: Vec::new(),
        }
    }

    fn handle(locator: &LogicalAgentLocatorWire) -> ContentHandleWire {
        ContentHandleWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            id: "transcript-1".to_string(),
            kind: ContentHandleKindWire::Transcript,
            revision: Some(revision(locator, 1)),
            digest: Some("a".repeat(64)),
            byte_len: Some(42),
            supports_range: true,
            supports_growth: true,
        }
    }

    fn record_running() -> AgentArtifactRecordWire {
        AgentArtifactRecordWire {
            project_name: "SASE".to_string(),
            project_dir: "/tmp/project".to_string(),
            project_file: "/tmp/project.sase".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            artifact_dir: "/tmp/artifacts/20260906120000".to_string(),
            timestamp: "20260906120000".to_string(),
            agent_meta: Some(AgentMetaWire {
                name: Some("athena.worker".to_string()),
                model: Some("gpt-5".to_string()),
                llm_provider: Some("codex".to_string()),
                agent_family: Some("family-1".to_string()),
                ..AgentMetaWire::default()
            }),
            done: None,
            running: Some(RunningMarkerWire {
                pid: Some(1234),
                model: Some("gpt-5".to_string()),
                llm_provider: Some("codex".to_string()),
                workspace_dir: Some("/tmp/workspace".to_string()),
                ..RunningMarkerWire::default()
            }),
            waiting: None,
            pending_question: None,
            workflow_state: None,
            plan_path: None,
            prompt_steps: Vec::new(),
            raw_prompt_snippet: Some("Implement the approved plan".to_string()),
            used_xprompts: Vec::new(),
            has_done_marker: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
        }
    }

    fn projection_request(
        locator: LogicalAgentLocatorWire,
        exact_locator: Option<AgentInstanceLocatorWire>,
        revision_num: u64,
        record: AgentArtifactRecordWire,
    ) -> ResolvedAgentProjectionRequestWire {
        ResolvedAgentProjectionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            record,
            logical_locator: locator.clone(),
            owner_facts: OwnerResolutionFactsWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                exact_locator,
                row_revision: revision(&locator, revision_num),
                liveness: OwnerLivenessWire::Alive,
                connection_health: ConnectionHealthWire::Online,
                freshness: ObservationFreshnessWire::Fresh,
                observed_at_unix: 1000.0,
                row_kind: FleetRowKindWire::AgentShell,
                current_instance: true,
                dismissable: false,
                needs_attention: false,
                occupied_runner_slot: true,
                container_projected_concrete_agent: false,
                capabilities: caps(&["stop"]),
                content_handles: Vec::new(),
            },
        }
    }

    fn singleton(hex: char, agent: &str) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            family_id: None,
            ..logical(hex, agent)
        }
    }

    fn operation_key(id: &str) -> ScopedOperationKeyWire {
        ScopedOperationKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            controller_id: "controller-1".to_string(),
            operation_id: id.to_string(),
        }
    }

    fn follow_record(
        locator: LogicalAgentLocatorWire,
        created_by: FollowCreatedByWire,
        state: FollowStateWire,
        timestamp: f64,
    ) -> FollowRecordWire {
        let logical_key = logical_key_unchecked(&locator);
        FollowRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_locator: locator,
            logical_key,
            created_by,
            state,
            created_at_unix: timestamp,
            updated_at_unix: timestamp,
            activated_at_unix: match state {
                FollowStateWire::Active => Some(timestamp),
                FollowStateWire::Pending => None,
            },
            operation_key: match created_by {
                FollowCreatedByWire::Explicit => None,
                FollowCreatedByWire::Dispatch => Some(operation_key("op-1")),
            },
        }
    }

    fn tombstone(
        locator: LogicalAgentLocatorWire,
        timestamp: f64,
    ) -> FollowTombstoneWire {
        let logical_key = logical_key_unchecked(&locator);
        FollowTombstoneWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_locator: locator,
            logical_key,
            unfollowed_at_unix: timestamp,
        }
    }

    #[test]
    fn installation_identity_creation_read_rotation_and_migration_are_fenced() {
        let temp = tempdir().unwrap();
        let first = ensure_installation_identity_with_generator(
            temp.path(),
            100.0,
            || id('a'),
        )
        .unwrap();
        assert!(first.created);
        assert_eq!(first.record.installation_id, id('a'));
        let second = ensure_installation_identity_with_generator(
            temp.path(),
            101.0,
            || id('b'),
        )
        .unwrap();
        assert!(!second.created);
        assert_eq!(second.record, first.record);
        let load = load_installation_identity(temp.path()).unwrap();
        assert_eq!(load.record, Some(first.record.clone()));

        let bad_rotate = rotate_installation_identity_with_generator(
            temp.path(),
            &InstallationIdentityRotateRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                expected_installation_id: id('b'),
                reason: "test".to_string(),
                rotated_at_unix: Some(200.0),
            },
            || id('c'),
        );
        assert!(bad_rotate.is_err());
        let rotated = rotate_installation_identity_with_generator(
            temp.path(),
            &InstallationIdentityRotateRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                expected_installation_id: id('a'),
                reason: "operator requested".to_string(),
                rotated_at_unix: Some(200.0),
            },
            || id('b'),
        )
        .unwrap();
        assert_eq!(rotated.old_record.installation_id, id('a'));
        assert_eq!(rotated.new_record.installation_id, id('b'));
        assert_eq!(rotated.new_record.prior_installation_id, Some(id('a')));

        let migrated = migrate_installation_identity(
            temp.path(),
            &InstallationIdentityMigrateRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                expected_current_installation_id: Some(id('b')),
                adopted_installation_id: id('c'),
                reason: "clone recovery".to_string(),
                adopted_at_unix: Some(300.0),
            },
        )
        .unwrap();
        assert_eq!(migrated.prior_record.unwrap().installation_id, id('b'));
        assert_eq!(migrated.new_record.installation_id, id('c'));
    }

    #[test]
    fn concurrent_identity_creators_converge_on_one_record() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(2));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                barrier.wait();
                ensure_installation_identity_with_generator(
                    &home,
                    100.0,
                    || {
                        let next = counter.fetch_add(1, Ordering::SeqCst);
                        if next == 0 {
                            id('a')
                        } else {
                            id('b')
                        }
                    },
                )
                .unwrap()
            }));
        }
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.created).count(),
            1
        );
        assert_eq!(
            outcomes[0].record.installation_id,
            outcomes[1].record.installation_id
        );
    }

    #[test]
    fn malformed_oversized_and_future_identity_files_are_left_unchanged() {
        let temp = tempdir().unwrap();
        let path = installation_identity_path(temp.path());
        fs::write(&path, b"{not-json").unwrap();
        let before = fs::read(&path).unwrap();
        assert!(ensure_installation_identity_with_generator(
            temp.path(),
            100.0,
            || id('a')
        )
        .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);

        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "schema_version": FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION + 1,
                "installation_id": id('a'),
                "created_at_unix": 1.0,
                "generation": 1,
                "prior_installation_id": null,
                "rotated_at_unix": null,
                "adopted_at_unix": null,
                "reason": null
            }))
            .unwrap(),
        )
        .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(load_installation_identity(temp.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);

        fs::write(&path, vec![b'x'; FLEET_INSTALLATION_IDENTITY_MAX_BYTES + 1])
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(ensure_installation_identity_with_generator(
            temp.path(),
            100.0,
            || id('b')
        )
        .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[cfg(unix)]
    #[test]
    fn identity_store_uses_private_modes() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        ensure_installation_identity_with_generator(temp.path(), 100.0, || {
            id('a')
        })
        .unwrap();
        let home_mode =
            fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777;
        let file_mode = fs::metadata(installation_identity_path(temp.path()))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(home_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn locator_keys_are_stable_and_names_do_not_become_identity() {
        let left = logical('a', "worker");
        let right = logical('b', "worker");
        assert_ne!(
            logical_locator_key(&left).unwrap(),
            logical_locator_key(&right).unwrap()
        );
        let renamed = LogicalAgentLocatorWire {
            agent_id: "worker".to_string(),
            ..left.clone()
        };
        assert_eq!(
            logical_locator_key(&left).unwrap(),
            logical_locator_key(&renamed).unwrap()
        );
        let display =
            associate_owner_display_name(&OwnerDisplayNameRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                logical_locator: left.clone(),
                owner_username: "bryan".to_string(),
                owner_machine_name: "athena".to_string(),
                display_name: "athena.worker".to_string(),
                display_alias: Some("worker".to_string()),
            })
            .unwrap();
        assert_eq!(display.owner_label, "bryan.athena");
        assert_eq!(display.logical_key, logical_key_unchecked(&left));
        assert!(LogicalAgentLocatorWire {
            agent_id: "bad\nid".to_string(),
            ..left
        }
        .validate()
        .is_err());
    }

    #[test]
    fn projection_outputs_safe_summary_and_detail_without_local_fields() {
        let locator = logical('a', "worker");
        let exact_locator = exact('a', "worker", "run-1");
        let mut request = projection_request(
            locator.clone(),
            Some(exact_locator.clone()),
            1,
            record_running(),
        );
        request.owner_facts.capabilities =
            caps(&["stop", "content.read", "stop"]);
        request.owner_facts.content_handles = vec![handle(&locator)];
        request.owner_facts.connection_health = ConnectionHealthWire::Offline;
        request.owner_facts.freshness = ObservationFreshnessWire::Stale;
        let summary = project_resolved_agent_summary(&request).unwrap();
        assert_eq!(summary.lifecycle, FleetLifecycleWire::Running);
        assert_eq!(summary.liveness, OwnerLivenessWire::Alive);
        assert_eq!(summary.connection_health, ConnectionHealthWire::Offline);
        assert_eq!(summary.freshness, ObservationFreshnessWire::Stale);
        assert_eq!(summary.capabilities.resource, vec!["content.read", "stop"]);
        assert_eq!(summary.content.handle_count, 1);
        let value = serde_json::to_value(&summary).unwrap();
        assert_no_forbidden_local_fields(&value);

        let detail = project_resolved_agent_detail(&request).unwrap();
        assert_eq!(detail.content_handles.len(), 1);
        let detail_value = serde_json::to_value(&detail).unwrap();
        assert_no_forbidden_local_fields(&detail_value);
    }

    #[test]
    fn projection_rejects_inconsistent_owner_facts_and_handles() {
        let locator = logical('a', "worker");
        let mut terminal = record_running();
        terminal.done = Some(DoneMarkerWire {
            outcome: Some("completed".to_string()),
            ..DoneMarkerWire::default()
        });
        terminal.running = None;
        let request = projection_request(
            locator.clone(),
            Some(exact('a', "worker", "run-1")),
            1,
            terminal,
        );
        assert!(project_resolved_agent_summary(&request).is_err());

        let mut missing_exact =
            projection_request(locator.clone(), None, 1, record_running());
        missing_exact.owner_facts.capabilities = caps(&["stop"]);
        assert!(project_resolved_agent_summary(&missing_exact).is_err());

        let mut missing_handle = projection_request(
            locator.clone(),
            Some(exact('a', "worker", "run-1")),
            1,
            record_running(),
        );
        missing_handle.owner_facts.capabilities = caps(&["content.read"]);
        assert!(project_resolved_agent_summary(&missing_handle).is_err());

        let mut bad_handle = projection_request(
            locator.clone(),
            Some(exact('a', "worker", "run-1")),
            1,
            record_running(),
        );
        bad_handle.owner_facts.capabilities = caps(&["content.read"]);
        let mut path_handle = handle(&locator);
        path_handle.id = "../secret".to_string();
        bad_handle.owner_facts.content_handles = vec![path_handle];
        assert!(project_resolved_agent_summary(&bad_handle).is_err());

        let other = logical('b', "worker");
        let mut wrong_revision = projection_request(
            locator,
            Some(exact('a', "worker", "run-1")),
            1,
            record_running(),
        );
        wrong_revision.owner_facts.row_revision = revision(&other, 1);
        assert!(project_resolved_agent_summary(&wrong_revision).is_err());
    }

    #[test]
    fn count_contract_is_order_independent_and_deduplicates_current_instances()
    {
        let locator = logical('a', "worker");
        let exact_one = exact('a', "worker", "run-1");
        let exact_two = exact('a', "worker", "run-2");
        let mut older = project_resolved_agent_summary(&projection_request(
            locator.clone(),
            Some(exact_one),
            1,
            record_running(),
        ))
        .unwrap();
        let newer = project_resolved_agent_summary(&projection_request(
            locator.clone(),
            Some(exact_two),
            2,
            record_running(),
        ))
        .unwrap();
        older.occupied_runner_slot = false;

        let waiter_locator = logical('a', "waiter");
        let mut waiting_record = record_running();
        waiting_record.running = None;
        waiting_record.waiting = Some(WaitingMarkerWire::default());
        let mut waiting = project_resolved_agent_summary(&projection_request(
            waiter_locator,
            Some(exact('a', "waiter", "run-1")),
            1,
            waiting_record,
        ))
        .unwrap();
        waiting.owner_liveness_for_test(OwnerLivenessWire::Unknown);
        waiting.occupied_runner_slot = false;

        let question_locator = logical('a', "question");
        let mut question_record = record_running();
        question_record.running = None;
        question_record.pending_question =
            Some(PendingQuestionMarkerWire::default());
        let mut attention =
            project_resolved_agent_summary(&projection_request(
                question_locator,
                Some(exact('a', "question", "run-1")),
                1,
                question_record,
            ))
            .unwrap();
        attention.occupied_runner_slot = false;

        let mut monitor = newer.clone();
        monitor.row_kind = FleetRowKindWire::Monitor;
        let request = FleetLogicalAgentCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            summaries: vec![
                waiting.clone(),
                monitor.clone(),
                newer.clone(),
                attention.clone(),
                older.clone(),
            ],
        };
        let counts = count_logical_agents(&request).unwrap();
        assert_eq!(counts.logical_agent_total, 3);
        assert_eq!(counts.running, 1);
        assert_eq!(counts.waiting, 1);
        assert_eq!(counts.attention, 1);
        assert_eq!(counts.occupied_runner_slots, 1);

        let reversed = FleetLogicalAgentCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            summaries: vec![older, attention, newer, monitor, waiting],
        };
        assert_eq!(count_logical_agents(&reversed).unwrap(), counts);
    }

    #[test]
    fn count_rejects_equal_revision_competing_current_instances() {
        let locator = logical('a', "worker");
        let one = project_resolved_agent_summary(&projection_request(
            locator.clone(),
            Some(exact('a', "worker", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let two = project_resolved_agent_summary(&projection_request(
            locator,
            Some(exact('a', "worker", "run-2")),
            1,
            record_running(),
        ))
        .unwrap();
        let err = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            summaries: vec![one, two],
        })
        .unwrap_err();
        assert!(err.to_string().contains("ambiguous current instances"));
    }

    #[test]
    fn follow_reconciliation_promotes_singleton_to_family_identity() {
        let from = singleton('a', "worker");
        let to = logical('a', "worker");
        let record = follow_record(
            from.clone(),
            FollowCreatedByWire::Explicit,
            FollowStateWire::Active,
            10.0,
        );

        let reconciled =
            reconcile_follow_records(&FollowReconciliationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![record],
                tombstones: Vec::new(),
                promotions: vec![FollowFamilyPromotionWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    from,
                    to: to.clone(),
                }],
                activations: Vec::new(),
                now_unix: 12.0,
            })
            .unwrap();

        assert!(reconciled.changed);
        assert_eq!(reconciled.records.len(), 1);
        assert_eq!(reconciled.records[0].logical_locator, to);
        assert_eq!(
            reconciled.records[0].logical_key,
            logical_key_unchecked(&to)
        );
        assert_eq!(
            reconciled.records[0].created_by,
            FollowCreatedByWire::Explicit
        );
        assert_eq!(reconciled.records[0].updated_at_unix, 12.0);
    }

    #[test]
    fn follow_tombstones_suppress_dispatch_recreation_and_activation() {
        let locator = logical('a', "worker");
        let record = follow_record(
            locator.clone(),
            FollowCreatedByWire::Dispatch,
            FollowStateWire::Pending,
            10.0,
        );
        let dispatch_tombstone = tombstone(locator.clone(), 11.0);

        let reconciled =
            reconcile_follow_records(&FollowReconciliationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![record],
                tombstones: vec![dispatch_tombstone],
                promotions: Vec::new(),
                activations: vec![FollowActivationWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    logical_locator: locator,
                    operation_key: Some(operation_key("op-1")),
                    activated_at_unix: 12.0,
                }],
                now_unix: 12.0,
            })
            .unwrap();

        assert!(reconciled.records.is_empty());
        assert_eq!(reconciled.tombstones.len(), 1);
        assert!(reconciled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "follow_tombstone_blocked"
                || diagnostic.code == "follow_activation_tombstoned"
        }));

        let singleton = singleton('a', "worker");
        let family = logical('a', "worker");
        let resurrected =
            reconcile_follow_records(&FollowReconciliationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![follow_record(
                    singleton.clone(),
                    FollowCreatedByWire::Dispatch,
                    FollowStateWire::Pending,
                    20.0,
                )],
                tombstones: vec![tombstone(singleton.clone(), 21.0)],
                promotions: vec![FollowFamilyPromotionWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    from: singleton,
                    to: family,
                }],
                activations: Vec::new(),
                now_unix: 22.0,
            })
            .unwrap();
        assert!(resurrected.records.is_empty());
        assert!(resurrected.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "follow_promotion_source_tombstoned"
        }));
    }

    #[test]
    fn focus_and_fleet_counts_stay_separate_and_propagate_unknown_hosts() {
        let local = project_resolved_agent_summary(&projection_request(
            logical('a', "local"),
            Some(exact('a', "local", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let remote_followed =
            project_resolved_agent_summary(&projection_request(
                logical('b', "followed"),
                Some(exact('b', "followed", "run-1")),
                2,
                record_running(),
            ))
            .unwrap();
        let remote_unfollowed =
            project_resolved_agent_summary(&projection_request(
                logical('c', "unfollowed"),
                Some(exact('c', "unfollowed", "run-1")),
                3,
                record_running(),
            ))
            .unwrap();

        let counted = count_focus_and_fleet(&FocusFleetCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            local_summaries: vec![local],
            followed_remote_hosts: vec![FleetHostCountInputWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin('b'),
                summaries: vec![remote_followed.clone()],
                observed_at_unix: Some(2000.0),
                freshness: ObservationFreshnessWire::Fresh,
            }],
            fleet_hosts: vec![
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('b'),
                    summaries: vec![remote_followed],
                    observed_at_unix: Some(2000.0),
                    freshness: ObservationFreshnessWire::Fresh,
                },
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('c'),
                    summaries: vec![remote_unfollowed],
                    observed_at_unix: Some(1500.0),
                    freshness: ObservationFreshnessWire::Aging,
                },
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('d'),
                    summaries: Vec::new(),
                    observed_at_unix: None,
                    freshness: ObservationFreshnessWire::Unknown,
                },
            ],
        })
        .unwrap();

        assert_eq!(counted.focus.counts.running, 2);
        assert_eq!(counted.fleet.counts.running, 2);
        assert!(!counted.focus.partial);
        assert!(counted.fleet.partial);
        assert_eq!(counted.fleet.unknown_origins, vec![id('d')]);
        assert_eq!(counted.fleet.host_counts.len(), 3);
        assert_eq!(counted.fleet.observed_at_unix_max, Some(2000.0));
    }

    #[test]
    fn cursor_replay_classifies_resync_boundaries() {
        let req = CursorReplayRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: StoreCursorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                store_generation: "gen-1".to_string(),
                sequence: 5,
            },
            current_generation: "gen-1".to_string(),
            newest_sequence: 8,
            oldest_replayable_sequence: 6,
            deletion_history_complete: true,
        };
        let replay = classify_cursor_replay(&req).unwrap();
        assert_eq!(
            replay.classification,
            CursorReplayClassificationWire::Replayable
        );
        assert_eq!(replay.replay_from_sequence, Some(6));

        let mut current = req.clone();
        current.cursor.sequence = 8;
        assert_eq!(
            classify_cursor_replay(&current).unwrap().classification,
            CursorReplayClassificationWire::Current
        );
        let mut ahead = req.clone();
        ahead.cursor.sequence = 9;
        assert_eq!(
            classify_cursor_replay(&ahead).unwrap().reason,
            CursorReplayReasonWire::SequenceAheadOfAuthority
        );
        let mut gap = req.clone();
        gap.cursor.sequence = 4;
        assert_eq!(
            classify_cursor_replay(&gap).unwrap().reason,
            CursorReplayReasonWire::ReplayGap
        );
        let mut generation = req.clone();
        generation.cursor.store_generation = "gen-0".to_string();
        assert_eq!(
            classify_cursor_replay(&generation).unwrap().reason,
            CursorReplayReasonWire::GenerationMismatch
        );
        let mut tombstones = req;
        tombstones.deletion_history_complete = false;
        assert_eq!(
            classify_cursor_replay(&tombstones).unwrap().reason,
            CursorReplayReasonWire::IncompleteDeletionHistory
        );
    }

    #[test]
    fn operation_fingerprint_and_replay_decisions_are_scoped() {
        let left =
            operation_payload_fingerprint(&PayloadFingerprintRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                payload: json!({"b": 2, "a": [1, true]}),
            })
            .unwrap();
        let right =
            operation_payload_fingerprint(&PayloadFingerprintRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                payload: json!({"a": [1, true], "b": 2}),
            })
            .unwrap();
        assert_eq!(left, right);

        let target = exact('a', "worker", "run-1");
        let req = OperationDecisionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: ScopedOperationKeyWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                controller_id: "controller-a".to_string(),
                operation_id: "op-1".to_string(),
            },
            payload_fingerprint: left.clone(),
            resource_revision: revision(&target.logical, 1),
            target: target.clone(),
            now_unix: 10.0,
            acceptance_window_seconds: 5.0,
            existing_record: None,
        };
        let accepted = decide_operation_replay(&req).unwrap();
        assert_eq!(accepted.decision, OperationDecisionKindWire::AcceptNew);
        let receipt = accepted.receipt.unwrap();

        let replay = decide_operation_replay(&OperationDecisionRequestWire {
            existing_record: Some(DurableOperationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            }),
            now_unix: 12.0,
            ..req.clone()
        })
        .unwrap();
        assert_eq!(
            replay.decision,
            OperationDecisionKindWire::ReturnOriginalReceipt
        );

        let conflict = decide_operation_replay(&OperationDecisionRequestWire {
            payload_fingerprint: PayloadFingerprintWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                sha256: "b".repeat(64),
            },
            existing_record: Some(DurableOperationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            }),
            now_unix: 12.0,
            ..req.clone()
        })
        .unwrap();
        assert_eq!(conflict.decision, OperationDecisionKindWire::Conflict);

        let expired = decide_operation_replay(&OperationDecisionRequestWire {
            existing_record: Some(DurableOperationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt,
                tombstoned_at_unix_ms: None,
            }),
            now_unix: 16.0,
            ..req
        })
        .unwrap();
        assert_eq!(expired.decision, OperationDecisionKindWire::Expired);
    }

    #[test]
    fn connection_plan_validation_rejects_insecure_or_secret_bearing_data() {
        let plan = ConnectionPlanWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            provider_ref: "provider-a".to_string(),
            endpoint: "https://fleet.example.test/api".to_string(),
            credential_ref: "cred-main".to_string(),
            pinned_installation_id: id('a'),
            connection_kind: FleetConnectionKindWire::Gateway,
            tls: TlsTrustSettingsWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                mode: TlsTrustModeWire::SystemRoots,
                ca_ref: None,
                server_name_ref: None,
            },
        };
        assert_eq!(validate_connection_plan(&plan).unwrap(), plan);
        let mut http = plan.clone();
        http.endpoint = "http://fleet.example.test".to_string();
        assert!(validate_connection_plan(&http).is_err());
        let mut userinfo = plan.clone();
        userinfo.endpoint = "https://user:pass@fleet.example.test".to_string();
        assert!(validate_connection_plan(&userinfo).is_err());
        let mut fragment = plan.clone();
        fragment.endpoint = "https://fleet.example.test/#frag".to_string();
        assert!(validate_connection_plan(&fragment).is_err());
        let mut inline_secret = plan;
        inline_secret.credential_ref = "token=secret".to_string();
        assert!(validate_connection_plan(&inline_secret).is_err());
    }

    #[test]
    fn time_helpers_keep_owner_runtime_and_viewer_freshness_separate() {
        let running = classify_runtime_duration(&RuntimeDurationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            owner_started_at_unix: 10.0,
            owner_stopped_at_unix: None,
            owner_observed_at_unix: 15.5,
            max_clock_anomaly_seconds: 1.0,
        })
        .unwrap();
        assert_eq!(running.elapsed_seconds, 5.5);
        assert_eq!(running.state, RuntimeDurationStateWire::Running);

        let clamped = classify_runtime_duration(&RuntimeDurationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            owner_started_at_unix: 10.0,
            owner_stopped_at_unix: Some(9.5),
            owner_observed_at_unix: 11.0,
            max_clock_anomaly_seconds: 1.0,
        })
        .unwrap();
        assert_eq!(clamped.elapsed_seconds, 0.0);
        assert!(clamped.clamped);

        let fresh = classify_cache_freshness(&CacheFreshnessRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            viewer_monotonic_elapsed_seconds: Some(2.0),
            fresh_threshold_seconds: 3.0,
            stale_threshold_seconds: 10.0,
        })
        .unwrap();
        assert_eq!(fresh.freshness, ObservationFreshnessWire::Fresh);
        let unknown = classify_cache_freshness(&CacheFreshnessRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            viewer_monotonic_elapsed_seconds: None,
            fresh_threshold_seconds: 3.0,
            stale_threshold_seconds: 10.0,
        })
        .unwrap();
        assert_eq!(unknown.freshness, ObservationFreshnessWire::Unknown);
        assert!(classify_cache_freshness(&CacheFreshnessRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            viewer_monotonic_elapsed_seconds: Some(f64::INFINITY),
            fresh_threshold_seconds: 3.0,
            stale_threshold_seconds: 10.0,
        })
        .is_err());
    }

    fn assert_no_forbidden_local_fields(value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    let key_lower = key.to_ascii_lowercase();
                    assert!(!key_lower.contains("path"), "forbidden key {key}");
                    assert!(!key_lower.contains("dir"), "forbidden key {key}");
                    assert!(!key_lower.contains("pid"), "forbidden key {key}");
                    assert!(
                        !key_lower.contains("token"),
                        "forbidden key {key}"
                    );
                    assert!(
                        !key_lower.contains("authorization"),
                        "forbidden key {key}"
                    );
                    assert_no_forbidden_local_fields(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    assert_no_forbidden_local_fields(value);
                }
            }
            Value::String(text) => {
                assert!(!text.contains("/tmp/"), "forbidden local path {text}");
                assert!(
                    !text.contains("Bearer "),
                    "forbidden credential {text}"
                );
            }
            _ => {}
        }
    }

    trait SummaryTestExt {
        fn owner_liveness_for_test(&mut self, liveness: OwnerLivenessWire);
    }

    impl SummaryTestExt for ResolvedAgentSummaryWire {
        fn owner_liveness_for_test(&mut self, liveness: OwnerLivenessWire) {
            self.liveness = liveness;
        }
    }
}
