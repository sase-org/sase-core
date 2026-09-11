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
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::agent_scan::{
    AgentArtifactRecordWire, AgentMetaWire, DoneMarkerWire, RunningMarkerWire,
};
use crate::queue_directive::queue_weight_is_valid;
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

/// Schema version shared by the fleet contract surface.
pub const FLEET_CONTRACT_SCHEMA_VERSION: u32 = 1;
/// Current fleet protocol version advertised by gateways and required by
/// viewers. Discovery compatibility is derived from this constant.
pub const FLEET_PROTOCOL_VERSION: u32 = 1;
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
pub(crate) const MAX_LABEL_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 1024;
const MAX_CAPABILITY_BYTES: usize = 80;
pub(crate) const MAX_INTENT_BYTES: usize = 512;
pub(crate) const MAX_LAUNCH_PROMPT_BYTES: usize = 64 * 1024;
const PAYLOAD_FINGERPRINT_DOMAIN: &[u8] = b"sase-fleet-operation-payload-v1\0";

/// Default catalog page size for fleet reads.
pub const FLEET_READ_DEFAULT_PAGE_ROWS: u32 = 50;
/// Hard cap for one fleet catalog page.
pub const FLEET_READ_MAX_PAGE_ROWS: u32 = 100;
/// Hard cap for one logical-ID batch lookup.
pub const FLEET_READ_MAX_BATCH_IDS: usize = 200;
/// Hard cap for one project-eligibility lookup.
pub const FLEET_READ_MAX_PROJECT_IDS: usize = 200;
/// Hard cap for catalog free-text query values.
pub const FLEET_READ_MAX_QUERY_BYTES: usize = 512;
/// Hard cap for exact string filters.
pub const FLEET_READ_MAX_FILTER_BYTES: usize = 128;
/// Default bytes returned by one content read.
pub const FLEET_READ_DEFAULT_CONTENT_BYTES: u64 = 64 * 1024;
/// Hard cap for one content read.
pub const FLEET_READ_MAX_CONTENT_BYTES: u64 = 256 * 1024;
/// Default replay ring capacity for durable fleet invalidations.
pub const FLEET_READ_DEFAULT_REPLAY_EVENTS: usize = 128;
/// Hard cap for durable fleet invalidation replay.
pub const FLEET_READ_MAX_REPLAY_EVENTS: usize = 512;
/// Versioned, recognizable prefix for opaque catalog snapshot IDs.
pub const FLEET_CATALOG_SNAPSHOT_ID_PREFIX: &str = "catsnap_v1_";

const FLEET_CATALOG_CURSOR_PREFIX: &str = "catcur_v1";

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

/// Normalized family role for viewer folding.
///
/// Independent of `row_kind`: it distinguishes a family root from an
/// ordinary member for `AgentShell` rows, carries `Monitor`/`Gate`/`Proc`
/// straight through from their matching row kinds, and marks any row whose
/// presentation is terminal (genuinely completed, or a demoted dead-active
/// leftover) as `HistoricalShell` so a viewer can render "was running"
/// uniformly once family topology stops mattering.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetFamilyRoleWire {
    Root,
    Member,
    Monitor,
    Gate,
    Proc,
    HistoricalShell,
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
    pub family_role: FleetFamilyRoleWire,
    /// Parent's record identity, when this row is a tracked family member.
    /// `None` for roots and rows with no tracked family lineage.
    pub parent_timestamp: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight_error: Option<String>,
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
pub struct FleetSnapshotFreshnessWire {
    pub schema_version: u32,
    pub freshness: ObservationFreshnessWire,
    pub partial: bool,
    pub refreshed_at_unix: Option<f64>,
    /// Safe diagnostic code or short reason; never a path or backend error.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAuthoritativeSnapshotWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    #[serde(default)]
    pub catalog_scope: FleetCatalogScopeWire,
    pub catalog_snapshot_id: String,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub freshness: FleetSnapshotFreshnessWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetSummaryResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    #[serde(default)]
    pub catalog_scope: FleetCatalogScopeWire,
    pub catalog_snapshot_id: String,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
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
pub enum FleetCatalogScopeWire {
    #[default]
    Presentation,
    History,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogQueryWire {
    pub schema_version: u32,
    /// Served catalog scope. Missing scope is a safe presentation request.
    #[serde(default)]
    pub scope: FleetCatalogScopeWire,
    /// Optional snapshot evidence from a previous page. Continuation cursors
    /// also carry this ID and are authoritative for their own offset.
    #[serde(default)]
    pub snapshot_id: Option<String>,
    /// Opaque offset cursor returned by a previous catalog page.
    pub cursor: Option<String>,
    /// Requested row limit. `None` means [`FLEET_READ_DEFAULT_PAGE_ROWS`].
    pub limit: Option<u32>,
    #[serde(default)]
    pub project_ids: Vec<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub status_buckets: Vec<FleetStatusBucketWire>,
    #[serde(default)]
    pub include_terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogPageSelectionWire {
    pub schema_version: u32,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: String,
    pub rows: Vec<ResolvedAgentSummaryWire>,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogPageWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
    pub page: FleetCatalogPageSelectionWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchRequestWire {
    pub schema_version: u32,
    pub logical_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchEntryWire {
    pub schema_version: u32,
    pub requested_logical_key: String,
    pub summary: Option<ResolvedAgentSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
    pub entries: Vec<FleetLogicalBatchEntryWire>,
}

/// Request to normalize a federation worker read result.
///
/// The payload is transport-independent JSON from the federation worker result
/// object or from a successful IPC response envelope. Host envelopes are
/// normalized independently so a malformed host degrades to diagnostics
/// without discarding other healthy hosts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetFederationNormalizeRequestWire {
    pub schema_version: u32,
    pub response: Value,
}

/// Count request for the current Focus/Fleet bridge over real federation
/// envelopes.
///
/// `followed_response` must be a followed-batch federation response and is
/// counted only from resolved requested summaries. `fleet_response` may carry
/// catalog or summary responses and may use authoritative host counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetFederationCountsRequestWire {
    pub schema_version: u32,
    pub local_summaries: Vec<ResolvedAgentSummaryWire>,
    pub followed_response: Option<Value>,
    pub fleet_response: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetEnvelopeDiagnosticWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub operation: Option<String>,
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogContinuationStateWire {
    Ready,
    Finished,
    ResyncRequired,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogResetReasonWire {
    ScopeMismatch,
    SnapshotMismatch,
    RestartRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogContinuationWire {
    pub schema_version: u32,
    pub snapshot_cursor: Option<StoreCursorWire>,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: String,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogAccumulationActionWire {
    IgnoredOlderRequest,
    Replaced,
    Merged,
    RestartRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationStateWire {
    pub schema_version: u32,
    pub request_generation: u64,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: Option<String>,
    pub rows: Vec<ResolvedAgentSummaryWire>,
    pub snapshot_cursor: Option<StoreCursorWire>,
    pub counts: Option<FleetLogicalAgentCountsWire>,
    pub count_revision: Option<u64>,
    pub freshness: Option<FleetSnapshotFreshnessWire>,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub continuation_state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationRequestWire {
    pub schema_version: u32,
    pub current: Option<FleetCatalogAccumulationStateWire>,
    pub request_generation: u64,
    pub requested_scope: FleetCatalogScopeWire,
    pub requested_snapshot_id: Option<String>,
    pub requested_cursor: Option<String>,
    pub incoming: FleetCatalogPageWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationDecisionWire {
    pub schema_version: u32,
    pub action: FleetCatalogAccumulationActionWire,
    pub state: FleetCatalogAccumulationStateWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetNormalizedHostWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub origin: Option<OriginLocatorWire>,
    pub status: String,
    pub cached: bool,
    pub age_seconds: Option<f64>,
    pub partial: bool,
    pub freshness: FleetSnapshotFreshnessWire,
    pub observed_at_unix: Option<f64>,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    pub count_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_scope: Option<FleetCatalogScopeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_snapshot_id: Option<String>,
    pub catalog: Option<FleetCatalogContinuationWire>,
    pub unresolved_logical_keys: Vec<String>,
    pub diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    pub count_input: Option<FleetHostCountInputWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetNormalizedReadResponseWire {
    pub schema_version: u32,
    pub operation: Option<String>,
    pub configured_host_count: u64,
    pub partial: bool,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    pub hosts: Vec<FleetNormalizedHostWire>,
    pub count_hosts: Vec<FleetHostCountInputWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDetailRequestWire {
    pub schema_version: u32,
    pub logical_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDetailResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub freshness: FleetSnapshotFreshnessWire,
    pub detail: ResolvedAgentDetailWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetContentReadRequestWire {
    pub schema_version: u32,
    pub handle_id: String,
    pub row_revision: ResourceRevisionWire,
    pub offset: u64,
    /// Requested byte limit. `None` means
    /// [`FLEET_READ_DEFAULT_CONTENT_BYTES`].
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetContentReadResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub handle: ContentHandleWire,
    pub offset: u64,
    pub returned_bytes: u64,
    pub total_byte_len: u64,
    pub next_offset: Option<u64>,
    pub eof: bool,
    pub supports_growth: bool,
    pub sha256: String,
    pub data_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityRequestWire {
    pub schema_version: u32,
    /// Project IDs to resolve. Empty means return every bounded project row.
    pub project_ids: Vec<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityWire {
    pub schema_version: u32,
    pub project_id: String,
    pub display_name: Option<String>,
    pub state: String,
    pub eligible: bool,
    pub launchable: bool,
    pub active_claim_count: u32,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityResponseWire {
    pub schema_version: u32,
    pub projects: Vec<FleetProjectEligibilityWire>,
    pub limit: u32,
    pub total_matching_projects: u64,
    pub truncated: bool,
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
    #[serde(default)]
    pub authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    #[serde(default)]
    pub partial: bool,
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetInvalidationKindWire {
    Launched,
    LifecycleChanged,
    AttentionChanged,
    RevisionChanged,
    Deleted,
    ProcessExited,
    SnapshotReplaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetInvalidationEventWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub kind: FleetInvalidationKindWire,
    pub logical_key: Option<String>,
    pub row_revision: Option<ResourceRevisionWire>,
    pub reason: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetResyncReasonWire {
    InitialCursor,
    GenerationMismatch,
    SequenceAheadOfAuthority,
    ReplayGap,
    IncompleteDeletionHistory,
    ReceiverLag,
    RingRolledOver,
    GenerationReplaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetResyncRequiredWire {
    pub schema_version: u32,
    pub reason: FleetResyncReasonWire,
    pub snapshot: FleetAuthoritativeSnapshotWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum FleetEventStreamItemWire {
    Invalidation(FleetInvalidationEventWire),
    ResyncRequired(FleetResyncRequiredWire),
    Heartbeat { cursor: StoreCursorWire },
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
    Failed,
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

/// Portable source project context for remote launch.
///
/// This is deliberately identity/evidence only. It must not carry a checkout
/// path from the source machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchProjectContextWire {
    pub schema_version: u32,
    pub provider_ref: Option<String>,
    pub project_id: String,
    pub revision: Option<String>,
    pub patch_ref: Option<String>,
}

/// Portable reference consumed by a target-side launch.
///
/// V1 accepts only opaque references; local paths are rejected during
/// validation so the target must resolve everything from provider state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetLaunchReferenceKindWire {
    Artifact,
    Patch,
    Url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchReferenceWire {
    pub schema_version: u32,
    pub kind: FleetLaunchReferenceKindWire,
    pub reference: String,
    pub sha256: Option<String>,
}

/// Side-effect-free remote launch intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchIntentWire {
    pub schema_version: u32,
    pub prompt: String,
    pub request_id: Option<String>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: FleetLaunchProjectContextWire,
    pub dry_run: Option<bool>,
    pub follow: bool,
    #[serde(default)]
    pub references: Vec<FleetLaunchReferenceWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub target_installation_id: String,
    pub intent: FleetLaunchIntentWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub acceptance_window_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
    pub logical_locator: Option<LogicalAgentLocatorWire>,
    pub instance_locator: Option<AgentInstanceLocatorWire>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFleetLaunchRecordWire {
    pub schema_version: u32,
    pub receipt: FleetLaunchReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableFleetLaunchRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<FleetLaunchReceiptWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchResponseWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetLaunchReceiptWire,
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
    let (
        queue_weight,
        queue_weight_explicit,
        queue_weight_invalid,
        queue_weight_error,
    ) = queue_weight_for_record(&request.record);
    let family = meta
        .and_then(|value| value.family_shell.as_ref())
        .or_else(|| done.and_then(|value| value.family_shell.as_ref()));
    let parent_timestamp = meta.and_then(|value| {
        first_non_empty([
            value.parent_timestamp.as_deref(),
            value.parent_agent_timestamp.as_deref(),
        ])
        .map(str::to_string)
    });
    let family_role = family_role_for_projection(
        facts.row_kind,
        lifecycle,
        facts.liveness,
        parent_timestamp.is_some(),
    );
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
        family_role,
        parent_timestamp,
        labels,
        project_name: trim_to_limit(
            &request.record.project_name,
            MAX_LABEL_BYTES,
        ),
        model: model_for_record(meta, done, running),
        provider: provider_for_record(meta, done, running),
        status,
        status_bucket: bucket_for_lifecycle(lifecycle, facts.liveness),
        intent: intent_for_record(&request.record),
        observed_at_unix: facts.observed_at_unix,
        row_revision: facts.row_revision.clone(),
        lifecycle,
        liveness: facts.liveness,
        connection_health: facts.connection_health,
        freshness: facts.freshness,
        capabilities: facts.capabilities.clone(),
        content: content_metadata(&facts.content_handles)?,
        queue_weight,
        queue_weight_explicit,
        queue_weight_invalid,
        queue_weight_error,
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

pub fn validate_store_cursor(
    cursor: &StoreCursorWire,
) -> Result<StoreCursorWire, FleetContractError> {
    cursor.validate()?;
    Ok(cursor.clone())
}

pub fn validate_content_handle(
    handle: &ContentHandleWire,
) -> Result<ContentHandleWire, FleetContractError> {
    handle.validate()?;
    Ok(handle.clone())
}

pub fn validate_fleet_snapshot_freshness(
    freshness: &FleetSnapshotFreshnessWire,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    validate_schema("fleet snapshot freshness", freshness.schema_version)?;
    if let Some(refreshed_at) = freshness.refreshed_at_unix {
        validate_timestamp("fleet snapshot refreshed_at_unix", refreshed_at)?;
    }
    if let Some(error) = &freshness.error {
        validate_label(
            "fleet snapshot freshness error",
            error,
            MAX_LABEL_BYTES,
        )?;
        reject_path_like("fleet snapshot freshness error", error)?;
        reject_secretish("fleet snapshot freshness error", error)?;
    }
    Ok(freshness.clone())
}

pub fn validate_fleet_logical_agent_counts(
    counts: &FleetLogicalAgentCountsWire,
    label: &str,
) -> Result<FleetLogicalAgentCountsWire, FleetContractError> {
    validate_schema(label, counts.schema_version)?;
    validate_schema(&format!("{label} basis"), counts.basis.schema_version)?;
    if let Some(observed) = counts.basis.observed_at_unix_max {
        validate_timestamp(&format!("{label} observed_at_unix_max"), observed)?;
    }
    Ok(counts.clone())
}

pub fn validate_fleet_authoritative_snapshot(
    snapshot: &FleetAuthoritativeSnapshotWire,
) -> Result<FleetAuthoritativeSnapshotWire, FleetContractError> {
    validate_schema("fleet authoritative snapshot", snapshot.schema_version)?;
    snapshot.cursor.validate()?;
    validate_fleet_catalog_snapshot_id(&snapshot.catalog_snapshot_id)?;
    validate_fleet_snapshot_freshness(&snapshot.freshness)?;
    validate_fleet_logical_agent_counts(
        &snapshot.counts,
        "fleet authoritative snapshot counts",
    )?;
    for summary in &snapshot.summaries {
        validate_resolved_agent_summary(summary)?;
    }
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: snapshot.summaries.clone(),
    })?;
    if counts != snapshot.counts {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot counts do not match summaries"
                .to_string(),
        ));
    }
    if snapshot.count_revision != fleet_count_revision(&snapshot.counts) {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot count_revision does not match counts"
                .to_string(),
        ));
    }
    let expected_snapshot_id =
        fleet_catalog_snapshot_id(snapshot.catalog_scope, &snapshot.summaries)?;
    if snapshot.catalog_snapshot_id != expected_snapshot_id {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot catalog_snapshot_id does not match summaries"
                .to_string(),
        ));
    }
    Ok(snapshot.clone())
}

pub fn fleet_count_revision(
    counts: &FleetLogicalAgentCountsWire,
) -> Option<u64> {
    counts.basis.max_revision
}

pub fn validate_fleet_catalog_query(
    query: &FleetCatalogQueryWire,
) -> Result<FleetCatalogQueryWire, FleetContractError> {
    validate_schema("fleet catalog query", query.schema_version)?;
    if let Some(snapshot_id) = &query.snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    if let Some(cursor) = &query.cursor {
        parse_catalog_cursor(cursor)?;
    }
    normalize_catalog_limit(query.limit)?;
    validate_identifier_vec(
        "project_ids",
        &query.project_ids,
        FLEET_READ_MAX_PROJECT_IDS,
        FLEET_READ_MAX_FILTER_BYTES,
    )?;
    if let Some(text) = &query.query {
        validate_label(
            "fleet catalog query",
            text,
            FLEET_READ_MAX_QUERY_BYTES,
        )?;
        reject_secretish("fleet catalog query", text)?;
    }
    let mut prior = None;
    for bucket in &query.status_buckets {
        if prior.is_some_and(|value| value >= *bucket) {
            return Err(FleetContractError::Validation(
                "fleet catalog status_buckets must be sorted and deduplicated"
                    .to_string(),
            ));
        }
        prior = Some(*bucket);
    }
    Ok(query.clone())
}

pub fn validate_fleet_catalog_cursor(
    cursor: &str,
) -> Result<String, FleetContractError> {
    parse_catalog_cursor(cursor)?;
    Ok(cursor.to_string())
}

pub fn validate_fleet_catalog_snapshot_id(
    snapshot_id: &str,
) -> Result<String, FleetContractError> {
    validate_reference_id("fleet catalog snapshot_id", snapshot_id)?;
    reject_path_like("fleet catalog snapshot_id", snapshot_id)?;
    let Some(suffix) =
        snapshot_id.strip_prefix(FLEET_CATALOG_SNAPSHOT_ID_PREFIX)
    else {
        return Err(FleetContractError::Validation(
            "fleet catalog snapshot_id is not recognized".to_string(),
        ));
    };
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(
            "fleet catalog snapshot_id must end with 64 lowercase hex characters"
                .to_string(),
        ));
    }
    Ok(snapshot_id.to_string())
}

pub fn fleet_catalog_snapshot_id(
    scope: FleetCatalogScopeWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<String, FleetContractError> {
    let mut rows = summaries
        .iter()
        .map(|summary| {
            let summary = validate_resolved_agent_summary(summary)?;
            Ok::<_, FleetContractError>(catalog_snapshot_summary_value(
                &summary,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| {
        let left_key = (
            left.get("logical_key")
                .and_then(Value::as_str)
                .unwrap_or(""),
            left.get("exact_key").and_then(Value::as_str).unwrap_or(""),
        );
        let right_key = (
            right
                .get("logical_key")
                .and_then(Value::as_str)
                .unwrap_or(""),
            right.get("exact_key").and_then(Value::as_str).unwrap_or(""),
        );
        left_key.cmp(&right_key)
    });
    let payload = json!({
        "domain": "sase-fleet-catalog-snapshot-v1",
        "scope": scope,
        "rows": rows,
    });
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        FleetContractError::Validation(format!(
            "fleet catalog snapshot could not be serialized: {error}"
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!(
        "{}{}",
        FLEET_CATALOG_SNAPSHOT_ID_PREFIX,
        hex::encode(hasher.finalize())
    ))
}

pub fn select_fleet_catalog_page(
    query: &FleetCatalogQueryWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<FleetCatalogPageSelectionWire, FleetContractError> {
    let query = validate_fleet_catalog_query(query)?;
    let limit = normalize_catalog_limit(query.limit)?;
    let snapshot_id = fleet_catalog_snapshot_id(query.scope, summaries)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(parse_catalog_cursor)
        .transpose()?;
    let requested_scope = cursor
        .as_ref()
        .map(|cursor| cursor.scope)
        .unwrap_or(query.scope);
    let requested_snapshot_id = cursor
        .as_ref()
        .map(|cursor| cursor.snapshot_id.as_str())
        .or(query.snapshot_id.as_deref());
    if requested_scope != query.scope {
        return Ok(fleet_catalog_restart_page(
            query.scope,
            snapshot_id,
            limit,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    if requested_snapshot_id.is_some_and(|requested| requested != snapshot_id) {
        return Ok(fleet_catalog_restart_page(
            query.scope,
            snapshot_id,
            limit,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    let start = cursor.map(|cursor| cursor.offset).unwrap_or(0);
    let mut rows = Vec::new();
    for summary in summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        if !summary_matches_catalog_query(&summary, &query)? {
            continue;
        }
        rows.push(summary);
    }
    rows.sort_by(compare_catalog_summaries);
    let total_matching_rows = rows.len() as u64;
    let start = start.min(rows.len());
    let end = start.saturating_add(limit as usize).min(rows.len());
    let has_more = end < rows.len();
    let next_cursor =
        has_more.then(|| format_catalog_cursor(query.scope, &snapshot_id, end));
    Ok(FleetCatalogPageSelectionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: query.scope,
        snapshot_id,
        rows: rows[start..end].to_vec(),
        limit,
        total_matching_rows,
        next_cursor,
        has_more,
        state: if has_more {
            FleetCatalogContinuationStateWire::Ready
        } else {
            FleetCatalogContinuationStateWire::Finished
        },
        reset_reason: None,
    })
}

pub fn accumulate_fleet_catalog_page(
    request: &FleetCatalogAccumulationRequestWire,
) -> Result<FleetCatalogAccumulationDecisionWire, FleetContractError> {
    validate_schema(
        "fleet catalog accumulation request",
        request.schema_version,
    )?;
    if let Some(current) = &request.current {
        validate_fleet_catalog_accumulation_state(current)?;
        if request.request_generation < current.request_generation {
            return Ok(FleetCatalogAccumulationDecisionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                action: FleetCatalogAccumulationActionWire::IgnoredOlderRequest,
                state: current.clone(),
            });
        }
    }
    if let Some(snapshot_id) = &request.requested_snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    let parsed_cursor = request
        .requested_cursor
        .as_deref()
        .map(parse_catalog_cursor)
        .transpose()?;
    if parsed_cursor
        .as_ref()
        .is_some_and(|cursor| cursor.scope != request.requested_scope)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    let incoming = validate_fleet_catalog_page_wire(&request.incoming)?;
    let incoming_page = &incoming.page;
    if incoming_page.scope != request.requested_scope {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    let requested_snapshot_id = parsed_cursor
        .as_ref()
        .map(|cursor| cursor.snapshot_id.as_str())
        .or(request.requested_snapshot_id.as_deref());
    if requested_snapshot_id
        .is_some_and(|snapshot_id| snapshot_id != incoming_page.snapshot_id)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    if incoming_page.state == FleetCatalogContinuationStateWire::ResyncRequired
    {
        return Ok(catalog_accumulation_restart(
            request,
            incoming_page
                .reset_reason
                .unwrap_or(FleetCatalogResetReasonWire::RestartRequired),
        ));
    }

    let is_initial_request = parsed_cursor.is_none();
    if is_initial_request {
        let rows = incoming_page.rows.clone();
        return Ok(FleetCatalogAccumulationDecisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            action: FleetCatalogAccumulationActionWire::Replaced,
            state: catalog_accumulation_state_from_page(
                request.request_generation,
                incoming,
                rows,
            )?,
        });
    }

    let Some(current) = &request.current else {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::RestartRequired,
        ));
    };
    if current.scope != incoming_page.scope
        || current.snapshot_id.as_deref() != Some(&incoming_page.snapshot_id)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    let rows = merge_catalog_rows(&current.rows, &incoming_page.rows)?;
    Ok(FleetCatalogAccumulationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        action: FleetCatalogAccumulationActionWire::Merged,
        state: catalog_accumulation_state_from_page(
            request.request_generation,
            incoming,
            rows,
        )?,
    })
}

fn catalog_accumulation_restart(
    request: &FleetCatalogAccumulationRequestWire,
    reset_reason: FleetCatalogResetReasonWire,
) -> FleetCatalogAccumulationDecisionWire {
    FleetCatalogAccumulationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        action: FleetCatalogAccumulationActionWire::RestartRequired,
        state: FleetCatalogAccumulationStateWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            request_generation: request.request_generation,
            scope: request.requested_scope,
            snapshot_id: None,
            rows: Vec::new(),
            snapshot_cursor: None,
            counts: None,
            count_revision: None,
            freshness: None,
            limit: request
                .incoming
                .page
                .limit
                .clamp(1, FLEET_READ_MAX_PAGE_ROWS),
            total_matching_rows: 0,
            next_cursor: None,
            has_more: false,
            continuation_state:
                FleetCatalogContinuationStateWire::ResyncRequired,
            reset_reason: Some(reset_reason),
        },
    }
}

fn catalog_accumulation_state_from_page(
    request_generation: u64,
    page: FleetCatalogPageWire,
    rows: Vec<ResolvedAgentSummaryWire>,
) -> Result<FleetCatalogAccumulationStateWire, FleetContractError> {
    Ok(FleetCatalogAccumulationStateWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        request_generation,
        scope: page.page.scope,
        snapshot_id: Some(page.page.snapshot_id),
        rows,
        snapshot_cursor: Some(page.cursor),
        counts: Some(page.counts),
        count_revision: page.count_revision,
        freshness: Some(page.freshness),
        limit: page.page.limit,
        total_matching_rows: page.page.total_matching_rows,
        next_cursor: page.page.next_cursor,
        has_more: page.page.has_more,
        continuation_state: page.page.state,
        reset_reason: page.page.reset_reason,
    })
}

fn merge_catalog_rows(
    current: &[ResolvedAgentSummaryWire],
    incoming: &[ResolvedAgentSummaryWire],
) -> Result<Vec<ResolvedAgentSummaryWire>, FleetContractError> {
    let mut rows = BTreeMap::<String, ResolvedAgentSummaryWire>::new();
    for summary in current.iter().chain(incoming) {
        let summary = validate_resolved_agent_summary(summary)?;
        let key = catalog_row_identity(&summary);
        match rows.get(&key) {
            Some(existing)
                if existing.row_revision.revision
                    >= summary.row_revision.revision => {}
            _ => {
                rows.insert(key, summary);
            }
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by(compare_catalog_summaries);
    Ok(rows)
}

fn catalog_row_identity(summary: &ResolvedAgentSummaryWire) -> String {
    format!(
        "{}\0{}",
        summary.logical_key,
        summary.exact_key.as_deref().unwrap_or("")
    )
}

fn validate_fleet_catalog_page_wire(
    page: &FleetCatalogPageWire,
) -> Result<FleetCatalogPageWire, FleetContractError> {
    validate_schema("fleet catalog page", page.schema_version)?;
    page.cursor.validate()?;
    validate_fleet_logical_agent_counts(
        &page.counts,
        "fleet catalog page counts",
    )?;
    if page.count_revision != fleet_count_revision(&page.counts) {
        return Err(FleetContractError::Validation(
            "fleet catalog page count_revision does not match counts"
                .to_string(),
        ));
    }
    validate_fleet_snapshot_freshness(&page.freshness)?;
    validate_fleet_catalog_page_selection(&page.page)?;
    Ok(page.clone())
}

fn validate_fleet_catalog_page_selection(
    page: &FleetCatalogPageSelectionWire,
) -> Result<FleetCatalogPageSelectionWire, FleetContractError> {
    validate_schema("fleet catalog page selection", page.schema_version)?;
    validate_fleet_catalog_snapshot_id(&page.snapshot_id)?;
    normalize_catalog_limit(Some(page.limit))?;
    for row in &page.rows {
        validate_resolved_agent_summary(row)?;
    }
    match page.state {
        FleetCatalogContinuationStateWire::Ready => {
            if !page.has_more || page.next_cursor.is_none() {
                return Err(FleetContractError::Validation(
                    "fleet catalog ready page requires has_more and next_cursor"
                        .to_string(),
                ));
            }
        }
        FleetCatalogContinuationStateWire::Finished => {
            if page.has_more || page.next_cursor.is_some() {
                return Err(FleetContractError::Validation(
                    "fleet catalog finished page must not have a continuation"
                        .to_string(),
                ));
            }
        }
        FleetCatalogContinuationStateWire::ResyncRequired => {
            if page.has_more
                || page.next_cursor.is_some()
                || !page.rows.is_empty()
                || page.reset_reason.is_none()
            {
                return Err(FleetContractError::Validation(
                    "fleet catalog resync page must have no rows or continuation and must carry a reset_reason"
                        .to_string(),
                ));
            }
        }
    }
    if let Some(cursor) = &page.next_cursor {
        let cursor = parse_catalog_cursor(cursor)?;
        if cursor.scope != page.scope || cursor.snapshot_id != page.snapshot_id
        {
            return Err(FleetContractError::Validation(
                "fleet catalog next_cursor does not match page scope and snapshot_id"
                    .to_string(),
            ));
        }
    }
    Ok(page.clone())
}

fn validate_fleet_catalog_accumulation_state(
    state: &FleetCatalogAccumulationStateWire,
) -> Result<FleetCatalogAccumulationStateWire, FleetContractError> {
    validate_schema("fleet catalog accumulation state", state.schema_version)?;
    if let Some(snapshot_id) = &state.snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    if let Some(cursor) = &state.snapshot_cursor {
        cursor.validate()?;
    }
    if let Some(counts) = &state.counts {
        validate_fleet_logical_agent_counts(
            counts,
            "fleet catalog accumulation counts",
        )?;
    }
    if let Some(freshness) = &state.freshness {
        validate_fleet_snapshot_freshness(freshness)?;
    }
    normalize_catalog_limit(Some(state.limit))?;
    for row in &state.rows {
        validate_resolved_agent_summary(row)?;
    }
    if let Some(cursor) = &state.next_cursor {
        let parsed = parse_catalog_cursor(cursor)?;
        if parsed.scope != state.scope
            || state.snapshot_id.as_deref() != Some(parsed.snapshot_id.as_str())
        {
            return Err(FleetContractError::Validation(
                "fleet catalog accumulation next_cursor does not match state"
                    .to_string(),
            ));
        }
    }
    Ok(state.clone())
}

pub fn validate_fleet_logical_batch_request(
    request: &FleetLogicalBatchRequestWire,
) -> Result<FleetLogicalBatchRequestWire, FleetContractError> {
    validate_schema("fleet logical batch request", request.schema_version)?;
    validate_identifier_vec(
        "logical_keys",
        &request.logical_keys,
        FLEET_READ_MAX_BATCH_IDS,
        MAX_KEY_BYTES,
    )?;
    let mut seen = BTreeSet::new();
    for logical_key in &request.logical_keys {
        validate_key("logical_key", logical_key)?;
        if !seen.insert(logical_key) {
            return Err(FleetContractError::Validation(
                "fleet logical batch request contains duplicate logical_key"
                    .to_string(),
            ));
        }
    }
    Ok(request.clone())
}

pub fn select_fleet_logical_batch(
    request: &FleetLogicalBatchRequestWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<Vec<FleetLogicalBatchEntryWire>, FleetContractError> {
    let request = validate_fleet_logical_batch_request(request)?;
    let mut by_key = BTreeMap::new();
    for summary in summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        by_key.insert(summary.logical_key.clone(), summary);
    }
    Ok(request
        .logical_keys
        .iter()
        .map(|logical_key| FleetLogicalBatchEntryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            requested_logical_key: logical_key.clone(),
            summary: by_key.get(logical_key).cloned(),
        })
        .collect())
}

pub fn validate_fleet_detail_request(
    request: &FleetDetailRequestWire,
) -> Result<FleetDetailRequestWire, FleetContractError> {
    validate_schema("fleet detail request", request.schema_version)?;
    validate_key("logical_key", &request.logical_key)?;
    Ok(request.clone())
}

pub fn validate_fleet_content_read_request(
    request: &FleetContentReadRequestWire,
) -> Result<FleetContentReadRequestWire, FleetContractError> {
    validate_schema("fleet content read request", request.schema_version)?;
    validate_reference_id("content handle id", &request.handle_id)?;
    reject_path_like("content handle id", &request.handle_id)?;
    request.row_revision.validate()?;
    normalize_content_read_limit(request.limit)?;
    Ok(request.clone())
}

pub fn fleet_content_read_limit(
    request: &FleetContentReadRequestWire,
) -> Result<u64, FleetContractError> {
    validate_fleet_content_read_request(request)?;
    normalize_content_read_limit(request.limit)
}

pub fn validate_fleet_project_eligibility_request(
    request: &FleetProjectEligibilityRequestWire,
) -> Result<FleetProjectEligibilityRequestWire, FleetContractError> {
    validate_schema(
        "fleet project eligibility request",
        request.schema_version,
    )?;
    validate_identifier_vec(
        "project_ids",
        &request.project_ids,
        FLEET_READ_MAX_PROJECT_IDS,
        FLEET_READ_MAX_FILTER_BYTES,
    )?;
    normalize_project_limit(request.limit)?;
    Ok(request.clone())
}

pub fn fleet_project_eligibility_limit(
    request: &FleetProjectEligibilityRequestWire,
) -> Result<u32, FleetContractError> {
    validate_fleet_project_eligibility_request(request)?;
    normalize_project_limit(request.limit)
}

pub fn validate_fleet_replay_capacity(
    capacity: usize,
) -> Result<usize, FleetContractError> {
    if capacity == 0 {
        return Err(FleetContractError::Validation(
            "fleet replay capacity must be positive".to_string(),
        ));
    }
    if capacity > FLEET_READ_MAX_REPLAY_EVENTS {
        return Err(FleetContractError::Validation(format!(
            "fleet replay capacity exceeds {FLEET_READ_MAX_REPLAY_EVENTS} events"
        )));
    }
    Ok(capacity)
}

pub fn cursor_replay_reason_to_resync_reason(
    reason: CursorReplayReasonWire,
) -> FleetResyncReasonWire {
    match reason {
        CursorReplayReasonWire::InitialCursor => {
            FleetResyncReasonWire::InitialCursor
        }
        CursorReplayReasonWire::GenerationMismatch => {
            FleetResyncReasonWire::GenerationMismatch
        }
        CursorReplayReasonWire::SequenceAheadOfAuthority => {
            FleetResyncReasonWire::SequenceAheadOfAuthority
        }
        CursorReplayReasonWire::ReplayGap => FleetResyncReasonWire::ReplayGap,
        CursorReplayReasonWire::IncompleteDeletionHistory => {
            FleetResyncReasonWire::IncompleteDeletionHistory
        }
        CursorReplayReasonWire::AtAuthorityHead
        | CursorReplayReasonWire::BehindWithinReplayWindow => {
            FleetResyncReasonWire::ReplayGap
        }
    }
}

pub fn validate_fleet_invalidation_event(
    event: &FleetInvalidationEventWire,
) -> Result<FleetInvalidationEventWire, FleetContractError> {
    validate_schema("fleet invalidation event", event.schema_version)?;
    event.cursor.validate()?;
    if let Some(logical_key) = &event.logical_key {
        validate_key("fleet invalidation logical_key", logical_key)?;
    }
    if let Some(row_revision) = &event.row_revision {
        row_revision.validate()?;
        if let Some(logical_key) = &event.logical_key {
            if &row_revision.logical_key != logical_key {
                return Err(FleetContractError::Validation(
                    "fleet invalidation row_revision logical_key does not match event logical_key"
                        .to_string(),
                ));
            }
        }
    }
    validate_label(
        "fleet invalidation reason",
        &event.reason,
        MAX_LABEL_BYTES,
    )?;
    reject_path_like("fleet invalidation reason", &event.reason)?;
    reject_secretish("fleet invalidation reason", &event.reason)?;
    Ok(event.clone())
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
    if let Some(weight) = summary.queue_weight {
        if !queue_weight_is_valid(weight) {
            return Err(FleetContractError::Validation(
                "summary queue_weight must be a positive finite capacity weight"
                    .to_string(),
            ));
        }
    }
    if summary.queue_weight_invalid && summary.queue_weight.is_some() {
        return Err(FleetContractError::Validation(
            "summary queue_weight cannot be present when queue_weight_invalid is true"
                .to_string(),
        ));
    }
    if !summary.queue_weight_invalid && summary.queue_weight_error.is_some() {
        return Err(FleetContractError::Validation(
            "summary queue_weight_error requires queue_weight_invalid"
                .to_string(),
        ));
    }
    if let Some(error) = &summary.queue_weight_error {
        validate_label("queue_weight_error", error, MAX_INTENT_BYTES)?;
        reject_secretish("queue_weight_error", error)?;
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
    let family_role_matches_row_kind = match summary.row_kind {
        FleetRowKindWire::Proc => {
            summary.family_role == FleetFamilyRoleWire::Proc
        }
        FleetRowKindWire::Monitor => {
            summary.family_role == FleetFamilyRoleWire::Monitor
        }
        FleetRowKindWire::Gate => {
            summary.family_role == FleetFamilyRoleWire::Gate
        }
        FleetRowKindWire::AgentShell
        | FleetRowKindWire::ContainerHeader
        | FleetRowKindWire::HistoricalShell => matches!(
            summary.family_role,
            FleetFamilyRoleWire::Root
                | FleetFamilyRoleWire::Member
                | FleetFamilyRoleWire::HistoricalShell
        ),
    };
    if !family_role_matches_row_kind {
        return Err(FleetContractError::Validation(
            "summary family_role is inconsistent with row_kind".to_string(),
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
        false,
    )?;
    let fleet = count_scope(&[], &request.fleet_hosts, "fleet_hosts", true)?;
    Ok(FocusFleetCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        focus,
        fleet,
    })
}

pub fn normalize_fleet_federation_response(
    request: &FleetFederationNormalizeRequestWire,
) -> Result<FleetNormalizedReadResponseWire, FleetContractError> {
    validate_schema(
        "fleet federation normalize request",
        request.schema_version,
    )?;
    normalize_fleet_federation_response_value(&request.response)
}

pub fn count_focus_and_fleet_from_federation(
    request: &FocusFleetFederationCountsRequestWire,
) -> Result<FocusFleetCountsWire, FleetContractError> {
    validate_schema(
        "focus/fleet federation counts request",
        request.schema_version,
    )?;
    for summary in &request.local_summaries {
        validate_resolved_agent_summary(summary)?;
    }
    let followed = match &request.followed_response {
        Some(response) if !response.is_null() => {
            normalize_fleet_federation_response_value(response)?
        }
        _ => empty_normalized_federation_response(None),
    };
    let fleet = match &request.fleet_response {
        Some(response) if !response.is_null() => {
            normalize_fleet_federation_response_value(response)?
        }
        _ => empty_normalized_federation_response(None),
    };
    count_focus_and_fleet(&FocusFleetCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        local_summaries: request.local_summaries.clone(),
        followed_remote_hosts: followed.count_hosts,
        fleet_hosts: fleet.count_hosts,
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

pub fn validate_fleet_launch_intent(
    intent: &FleetLaunchIntentWire,
) -> Result<FleetLaunchIntentWire, FleetContractError> {
    intent.validate()?;
    Ok(intent.clone())
}

pub fn fleet_launch_payload_fingerprint(
    intent: &FleetLaunchIntentWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    intent.validate()?;
    operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: serde_json::to_value(intent).map_err(|source| {
            FleetContractError::Json {
                path: PathBuf::from("<fleet_launch_intent>"),
                source,
            }
        })?,
    })
}

pub fn validate_fleet_launch_request(
    request: &FleetLaunchRequestWire,
) -> Result<FleetLaunchRequestWire, FleetContractError> {
    validate_schema("fleet launch request", request.schema_version)?;
    request.key.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    request.intent.validate()?;
    request.payload_fingerprint.validate()?;
    let expected = fleet_launch_payload_fingerprint(&request.intent)?;
    if request.payload_fingerprint != expected {
        return Err(FleetContractError::Validation(
            "fleet launch payload_fingerprint does not match intent"
                .to_string(),
        ));
    }
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    Ok(request.clone())
}

pub fn decide_fleet_launch_replay(
    request: &FleetLaunchDecisionRequestWire,
) -> Result<FleetLaunchDecisionWire, FleetContractError> {
    validate_schema("fleet launch decision request", request.schema_version)?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    validate_timestamp("now_unix", request.now_unix)?;
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    let now_ms = timestamp_ms("now_unix", request.now_unix)?;
    let expires_at = now_ms.saturating_add(duration_ms(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?);
    if let Some(record) = &request.existing_record {
        record.validate()?;
        if record.receipt.key != request.key {
            return Err(FleetContractError::Validation(
                "existing fleet launch record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.target_installation_id
            != request.target_installation_id
        {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(fleet_launch_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = FleetLaunchReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target_installation_id: request.target_installation_id.clone(),
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
        logical_locator: None,
        instance_locator: None,
        message: None,
    };
    Ok(fleet_launch_decision(
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

    pub(crate) fn validate_for_logical(
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
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("scoped operation key", self.schema_version)?;
        validate_reference_id("controller_id", &self.controller_id)?;
        validate_reference_id("operation_id", &self.operation_id)
    }
}

impl PayloadFingerprintWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
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

impl FleetLaunchProjectContextWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch project context", self.schema_version)?;
        validate_identifier("fleet launch project_id", &self.project_id)?;
        reject_path_like("fleet launch project_id", &self.project_id)?;
        if let Some(provider_ref) = &self.provider_ref {
            validate_reference_id("fleet launch provider_ref", provider_ref)?;
        }
        if let Some(revision) = &self.revision {
            validate_reference_id("fleet launch revision", revision)?;
        }
        if let Some(patch_ref) = &self.patch_ref {
            validate_reference_id("fleet launch patch_ref", patch_ref)?;
        }
        if self.revision.is_none() && self.patch_ref.is_none() {
            return Err(FleetContractError::Validation(
                "fleet launch project context requires revision or patch_ref evidence"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl FleetLaunchReferenceWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch reference", self.schema_version)?;
        validate_reference_id("fleet launch reference", &self.reference)?;
        reject_path_like("fleet launch reference", &self.reference)?;
        if let Some(digest) = &self.sha256 {
            validate_sha256_digest("fleet launch reference sha256", digest)?;
        }
        Ok(())
    }
}

impl FleetLaunchIntentWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch intent", self.schema_version)?;
        if self.prompt.trim().is_empty() {
            return Err(FleetContractError::Validation(
                "fleet launch prompt must be non-empty".to_string(),
            ));
        }
        if self.prompt.len() > MAX_LAUNCH_PROMPT_BYTES {
            return Err(FleetContractError::Validation(format!(
                "fleet launch prompt exceeds {MAX_LAUNCH_PROMPT_BYTES} bytes"
            )));
        }
        if self.prompt.chars().any(|ch| ch == '\0') {
            return Err(FleetContractError::Validation(
                "fleet launch prompt must not contain NUL bytes".to_string(),
            ));
        }
        self.project.validate()?;
        if let Some(request_id) = &self.request_id {
            validate_reference_id("fleet launch request_id", request_id)?;
        }
        if let Some(display_name) = &self.display_name {
            validate_label(
                "fleet launch display_name",
                display_name,
                MAX_LABEL_BYTES,
            )?;
        }
        for (field, value) in [
            ("fleet launch name", self.name.as_ref()),
            ("fleet launch model", self.model.as_ref()),
            ("fleet launch provider", self.provider.as_ref()),
            ("fleet launch runtime", self.runtime.as_ref()),
        ] {
            if let Some(value) = value {
                validate_reference_id(field, value)?;
            }
        }
        if self.references.len() > FLEET_READ_MAX_BATCH_IDS {
            return Err(FleetContractError::Validation(format!(
                "fleet launch references exceed {FLEET_READ_MAX_BATCH_IDS} entries"
            )));
        }
        for reference in &self.references {
            reference.validate()?;
        }
        Ok(())
    }
}

impl FleetLaunchReceiptWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        validate_installation_id(&self.target_installation_id)?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "fleet launch receipt expires before it was accepted"
                    .to_string(),
            ));
        }
        if let Some(logical) = &self.logical_locator {
            logical.validate()?;
            if logical.project.origin.installation_id
                != self.target_installation_id
            {
                return Err(FleetContractError::Validation(
                    "fleet launch logical locator targets a different installation"
                        .to_string(),
                ));
            }
        }
        if let Some(instance) = &self.instance_locator {
            instance.validate()?;
            if instance.logical.project.origin.installation_id
                != self.target_installation_id
            {
                return Err(FleetContractError::Validation(
                    "fleet launch instance locator targets a different installation"
                        .to_string(),
                ));
            }
            if let Some(logical) = &self.logical_locator {
                if instance.logical != *logical {
                    return Err(FleetContractError::Validation(
                        "fleet launch instance locator does not match logical locator"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(message) = &self.message {
            validate_label(
                "fleet launch receipt message",
                message,
                MAX_LABEL_BYTES,
            )?;
            reject_path_like("fleet launch receipt message", message)?;
            reject_secretish("fleet launch receipt message", message)?;
        }
        Ok(())
    }
}

impl DurableFleetLaunchRecordWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable fleet launch record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "fleet launch tombstone predates acceptance".to_string(),
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
    // Require compatible live owner facts rather than trusting the status
    // bucket alone: a bucket that claims Running/Starting can never count
    // toward authoritative running counts when the owner-resolved liveness
    // is definitively Dead or NotProcess.
    let live_compatible = matches!(
        summary.liveness,
        OwnerLivenessWire::Alive | OwnerLivenessWire::Unknown
    );
    if !live_compatible {
        return false;
    }
    match summary.status_bucket {
        FleetStatusBucketWire::Running => !summary.dismissable,
        FleetStatusBucketWire::Starting => {
            summary.container_projected_concrete_agent
        }
        _ => false,
    }
}

/// Whether a row's presentation should be treated as terminal ("was
/// running") for family-role and status-bucket purposes: genuinely terminal
/// lifecycle, or definitively `Dead`/`NotProcess` liveness — unless a
/// waiting/question marker protects it.
fn presentation_is_historical(
    lifecycle: FleetLifecycleWire,
    liveness: OwnerLivenessWire,
) -> bool {
    if matches!(
        lifecycle,
        FleetLifecycleWire::Waiting | FleetLifecycleWire::Asking
    ) {
        return false;
    }
    terminal_lifecycle(lifecycle)
        || matches!(
            liveness,
            OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
        )
}

fn family_role_for_projection(
    row_kind: FleetRowKindWire,
    lifecycle: FleetLifecycleWire,
    liveness: OwnerLivenessWire,
    has_parent: bool,
) -> FleetFamilyRoleWire {
    match row_kind {
        FleetRowKindWire::Proc => FleetFamilyRoleWire::Proc,
        FleetRowKindWire::Monitor => FleetFamilyRoleWire::Monitor,
        FleetRowKindWire::Gate => FleetFamilyRoleWire::Gate,
        FleetRowKindWire::AgentShell
        | FleetRowKindWire::ContainerHeader
        | FleetRowKindWire::HistoricalShell => {
            if presentation_is_historical(lifecycle, liveness) {
                FleetFamilyRoleWire::HistoricalShell
            } else if has_parent {
                FleetFamilyRoleWire::Member
            } else {
                FleetFamilyRoleWire::Root
            }
        }
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
    liveness: OwnerLivenessWire,
) -> FleetStatusBucketWire {
    // A record whose owner-resolved liveness is definitively Dead or
    // NotProcess can never bucket as Running/Starting, no matter what its
    // lifecycle label says: the four-fact model presents it as stopped
    // instead of fabricating an active state.
    let liveness_stops = matches!(
        liveness,
        OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
    );
    match lifecycle {
        FleetLifecycleWire::Starting if liveness_stops => {
            FleetStatusBucketWire::Stopped
        }
        FleetLifecycleWire::Starting => FleetStatusBucketWire::Starting,
        FleetLifecycleWire::Running | FleetLifecycleWire::Unknown
            if liveness_stops =>
        {
            FleetStatusBucketWire::Stopped
        }
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

fn queue_weight_for_record(
    record: &AgentArtifactRecordWire,
) -> (Option<f64>, bool, bool, Option<String>) {
    if let Some(waiting) = &record.waiting {
        if waiting.queue_weight_invalid {
            return (
                None,
                waiting.queue_weight_explicit,
                true,
                waiting.queue_weight_error.clone(),
            );
        }
        if let Some(weight) = waiting.queue_weight {
            if queue_weight_is_valid(weight) {
                return (
                    Some(weight),
                    waiting.queue_weight_explicit,
                    false,
                    None,
                );
            }
            return (None, waiting.queue_weight_explicit, true, None);
        }
    }
    if let Some(meta) = &record.agent_meta {
        if meta.queue_weight_invalid {
            return (
                None,
                meta.queue_weight_explicit,
                true,
                meta.queue_weight_error.clone(),
            );
        }
        if let Some(weight) = meta.queue_weight {
            if queue_weight_is_valid(weight) {
                return (Some(weight), meta.queue_weight_explicit, false, None);
            }
            return (None, meta.queue_weight_explicit, true, None);
        }
    }
    (None, false, false, None)
}

fn intent_for_record(record: &AgentArtifactRecordWire) -> Option<String> {
    let raw = first_non_empty([
        record
            .agent_meta
            .as_ref()
            .and_then(|value| value.plan_action.as_deref()),
        record.raw_prompt_snippet.as_deref(),
    ])?;
    // Owner-produced prompts and plan actions are ordinary free text and
    // routinely span multiple lines; strip control characters (newlines,
    // CR, tabs, ...) before byte-bounding so a normal multiline prompt
    // cannot make the display intent fail `validate_label`'s control
    // character rejection. A value that normalizes to nothing (e.g. only
    // control characters) becomes an omitted label instead of an invalid
    // empty one.
    let bounded =
        trim_to_limit(&replace_control_characters(raw), MAX_INTENT_BYTES);
    if bounded.is_empty() {
        None
    } else {
        Some(bounded)
    }
}

/// Replace control characters (including newline/CR/tab) with a plain space
/// so owner-produced free text is safe to display as a single-line label.
fn replace_control_characters(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
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

fn fleet_launch_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<FleetLaunchReceiptWire>,
) -> FleetLaunchDecisionWire {
    FleetLaunchDecisionWire {
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

fn normalize_fleet_federation_response_value(
    response: &Value,
) -> Result<FleetNormalizedReadResponseWire, FleetContractError> {
    let response = nested_federation_result(response);
    let object = response.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation response must be a JSON object".to_string(),
        )
    })?;
    validate_allowed_fields(
        object,
        "fleet federation response",
        &[
            "schema_version",
            "operation",
            "configured_hosts",
            "configured_host_count",
            "partial",
            "disabled",
            "diagnostics",
            "hosts",
        ],
    )?;
    validate_optional_schema(object, "fleet federation response")?;
    let operation = optional_string_field(
        object,
        "operation",
        "fleet federation operation",
        MAX_IDENTIFIER_BYTES,
    )?;
    if let Some(operation) = &operation {
        validate_reference_id("fleet federation operation", operation)?;
    }
    let disabled = optional_bool_field(object, "disabled")?.unwrap_or(false);
    let host_values = array_field(object, "hosts")?;
    let configured_hosts = optional_u64_field(object, "configured_hosts")?;
    let configured_host_count =
        optional_u64_field(object, "configured_host_count")?;
    if let (Some(left), Some(right)) = (configured_hosts, configured_host_count)
    {
        if left != right {
            return Err(FleetContractError::Validation(
                "configured_hosts and configured_host_count disagree"
                    .to_string(),
            ));
        }
    }
    let configured_host_count = configured_hosts
        .or(configured_host_count)
        .unwrap_or(host_values.len() as u64);
    if configured_host_count < host_values.len() as u64 {
        return Err(FleetContractError::Validation(
            "configured_host_count is smaller than returned hosts".to_string(),
        ));
    }
    let response_partial =
        optional_bool_field(object, "partial")?.unwrap_or(false);
    let mut diagnostics = diagnostics_array(
        object.get("diagnostics"),
        None,
        operation.as_deref(),
    )?;
    let mut hosts = Vec::new();
    let mut summaries = Vec::new();
    let mut count_hosts = Vec::new();
    for (index, value) in host_values.iter().enumerate() {
        let host = match normalize_federation_host_strict(
            value,
            operation.as_deref(),
            disabled,
            index,
        ) {
            Ok(host) => host,
            Err(error) => invalid_federation_host(
                value,
                operation.as_deref(),
                index,
                &error.to_string(),
            )?,
        };
        diagnostics.extend(host.diagnostics.clone());
        summaries.extend(host.summaries.clone());
        if let Some(input) = &host.count_input {
            count_hosts.push(input.clone());
        }
        hosts.push(host);
    }
    Ok(FleetNormalizedReadResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        operation,
        configured_host_count,
        partial: response_partial
            || configured_host_count > hosts.len() as u64
            || hosts.iter().any(|host| host.partial),
        summaries,
        diagnostics,
        hosts,
        count_hosts,
    })
}

fn empty_normalized_federation_response(
    operation: Option<String>,
) -> FleetNormalizedReadResponseWire {
    FleetNormalizedReadResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        operation,
        configured_host_count: 0,
        partial: false,
        summaries: Vec::new(),
        diagnostics: Vec::new(),
        hosts: Vec::new(),
        count_hosts: Vec::new(),
    }
}

fn normalize_federation_host_strict(
    value: &Value,
    operation: Option<&str>,
    disabled: bool,
    index: usize,
) -> Result<FleetNormalizedHostWire, FleetContractError> {
    let object = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(format!(
            "fleet federation hosts[{index}] must be a JSON object"
        ))
    })?;
    validate_allowed_fields(
        object,
        &format!("fleet federation hosts[{index}]"),
        &[
            "schema_version",
            "alias",
            "origin",
            "provider_ref",
            "installation_id",
            "endpoint",
            "status",
            "cached",
            "age_seconds",
            "payload",
            "error",
            "freshness",
            "observed_at",
            "observed_at_unix",
            "diagnostics",
        ],
    )?;
    validate_optional_schema(
        object,
        &format!("fleet federation hosts[{index}]"),
    )?;
    let alias = optional_string_field(
        object,
        "alias",
        "fleet federation host alias",
        MAX_LABEL_BYTES,
    )?;
    let origin = origin_from_host(object)?;
    let status = optional_string_field(
        object,
        "status",
        "fleet federation host status",
        MAX_IDENTIFIER_BYTES,
    )?
    .unwrap_or_else(|| {
        if disabled {
            "disabled".to_string()
        } else {
            "unknown".to_string()
        }
    });
    validate_reference_id("fleet federation host status", &status)?;
    let cached = optional_bool_field(object, "cached")?.unwrap_or(false);
    let age_seconds =
        optional_non_negative_seconds_field(object, "age_seconds")?;
    let mut diagnostics = diagnostics_array(
        object.get("diagnostics"),
        alias.as_deref(),
        operation,
    )?;
    if let Some(error) = diagnostic_from_error_value(
        object.get("error"),
        alias.as_deref(),
        operation,
        "fleet_host_error",
    )? {
        diagnostics.push(error);
    }

    let payload = object
        .get("payload")
        .filter(|payload| !payload.is_null())
        .map(|payload| {
            payload.as_object().ok_or_else(|| {
                FleetContractError::Validation(
                    "fleet federation host payload must be a JSON object"
                        .to_string(),
                )
            })
        })
        .transpose()?;
    let payload_normalization =
        normalize_host_payload(payload, operation, alias.as_deref())?;
    diagnostics.extend(payload_normalization.diagnostics);
    if payload.is_none() && host_status_healthy(&status) {
        diagnostics.push(fleet_envelope_diagnostic(
            alias.as_deref(),
            operation,
            "fleet_payload_missing",
            "error",
            "healthy host did not include a fleet payload",
        )?);
    }
    if !disabled && !host_status_healthy(&status) && diagnostics.is_empty() {
        diagnostics.push(fleet_envelope_diagnostic(
            alias.as_deref(),
            operation,
            "fleet_host_error",
            "warning",
            &format!("host status {status}"),
        )?);
    }
    let freshness = match payload_normalization.freshness {
        Some(freshness) => freshness,
        None => {
            normalized_host_freshness(object, &status, diagnostics.first())?
        }
    };
    if let Some(reason) = &freshness.error {
        if !diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == *reason)
        {
            diagnostics.push(fleet_envelope_diagnostic(
                alias.as_deref(),
                operation,
                if freshness.freshness == ObservationFreshnessWire::Stale {
                    "fleet_host_stale"
                } else {
                    "fleet_host_partial"
                },
                "warning",
                reason,
            )?);
        }
    }
    let observed_at_unix = normalized_host_observed_at(
        object,
        &freshness,
        payload_normalization.authoritative_counts.as_ref(),
        &payload_normalization.summaries,
    )?;
    let partial = payload_normalization.partial
        || freshness.partial
        || !host_status_healthy(&status)
        || (payload.is_none() && host_status_healthy(&status));
    let count_input = origin.clone().map(|origin| FleetHostCountInputWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        origin,
        summaries: payload_normalization.summaries.clone(),
        observed_at_unix,
        freshness: freshness.freshness,
        authoritative_counts: payload_normalization
            .authoritative_counts
            .clone(),
        partial,
    });
    Ok(FleetNormalizedHostWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias,
        origin,
        status,
        cached,
        age_seconds,
        partial,
        freshness,
        observed_at_unix,
        summaries: payload_normalization.summaries,
        authoritative_counts: payload_normalization.authoritative_counts,
        count_revision: payload_normalization.count_revision,
        catalog_scope: payload_normalization.catalog_scope,
        catalog_snapshot_id: payload_normalization.catalog_snapshot_id,
        catalog: payload_normalization.catalog,
        unresolved_logical_keys: payload_normalization.unresolved_logical_keys,
        diagnostics,
        count_input,
    })
}

fn invalid_federation_host(
    value: &Value,
    operation: Option<&str>,
    index: usize,
    reason: &str,
) -> Result<FleetNormalizedHostWire, FleetContractError> {
    let object = value.as_object();
    let alias = object.and_then(safe_alias_from_host);
    let origin = object
        .and_then(|object| origin_from_host(object).ok())
        .flatten();
    let cached = object
        .and_then(|object| optional_bool_field(object, "cached").ok())
        .flatten()
        .unwrap_or(false);
    let age_seconds = object
        .and_then(|object| {
            optional_non_negative_seconds_field(object, "age_seconds").ok()
        })
        .flatten();
    let diagnostics = vec![fleet_envelope_diagnostic(
        alias.as_deref(),
        operation,
        "fleet_envelope_invalid",
        "error",
        &format!("hosts[{index}] could not be normalized: {reason}"),
    )?];
    let freshness = FleetSnapshotFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness: ObservationFreshnessWire::Unknown,
        partial: true,
        refreshed_at_unix: None,
        error: Some("invalid_envelope".to_string()),
    };
    let count_input = origin.clone().map(|origin| FleetHostCountInputWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        origin,
        summaries: Vec::new(),
        observed_at_unix: None,
        freshness: ObservationFreshnessWire::Unknown,
        authoritative_counts: None,
        partial: true,
    });
    Ok(FleetNormalizedHostWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias,
        origin,
        status: "invalid".to_string(),
        cached,
        age_seconds,
        partial: true,
        freshness,
        observed_at_unix: None,
        summaries: Vec::new(),
        authoritative_counts: None,
        count_revision: None,
        catalog_scope: None,
        catalog_snapshot_id: None,
        catalog: None,
        unresolved_logical_keys: Vec::new(),
        diagnostics,
        count_input,
    })
}

struct PayloadNormalization {
    summaries: Vec<ResolvedAgentSummaryWire>,
    authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    count_revision: Option<u64>,
    catalog_scope: Option<FleetCatalogScopeWire>,
    catalog_snapshot_id: Option<String>,
    freshness: Option<FleetSnapshotFreshnessWire>,
    catalog: Option<FleetCatalogContinuationWire>,
    unresolved_logical_keys: Vec<String>,
    diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    partial: bool,
}

fn normalize_host_payload(
    payload: Option<&Map<String, Value>>,
    operation: Option<&str>,
    alias: Option<&str>,
) -> Result<PayloadNormalization, FleetContractError> {
    let Some(payload) = payload else {
        return Ok(PayloadNormalization {
            summaries: Vec::new(),
            authoritative_counts: None,
            count_revision: None,
            catalog_scope: None,
            catalog_snapshot_id: None,
            freshness: None,
            catalog: None,
            unresolved_logical_keys: Vec::new(),
            diagnostics: Vec::new(),
            partial: false,
        });
    };
    validate_allowed_fields(
        payload,
        "fleet federation host payload",
        &[
            "schema_version",
            "cursor",
            "catalog_scope",
            "catalog_snapshot_id",
            "counts",
            "count_revision",
            "freshness",
            "page",
            "entries",
        ],
    )?;
    validate_optional_schema(payload, "fleet federation host payload")?;
    let mut diagnostics = Vec::new();
    let (snapshot_cursor, cursor_partial) = optional_store_cursor_value(
        payload.get("cursor"),
        "fleet federation payload cursor",
        alias,
        operation,
        &mut diagnostics,
    )?;
    let mut catalog_scope = payload
        .get("catalog_scope")
        .map(|value| wire_from_json_value(value, "fleet catalog scope"))
        .transpose()?;
    let mut catalog_snapshot_id = optional_string_field(
        payload,
        "catalog_snapshot_id",
        "fleet catalog snapshot_id",
        MAX_IDENTIFIER_BYTES,
    )?;
    if let Some(snapshot_id) = &catalog_snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    let freshness = payload
        .get("freshness")
        .map(|value| {
            freshness_from_json_value(value, "fleet host payload freshness")
        })
        .transpose()?;
    let has_page = payload.get("page").is_some_and(|value| !value.is_null());
    let has_entries =
        payload.get("entries").is_some_and(|value| !value.is_null());
    if has_page && has_entries {
        return Err(FleetContractError::Validation(
            "fleet payload cannot contain both page and entries".to_string(),
        ));
    }
    let authoritative_counts = if has_entries {
        None
    } else {
        authoritative_counts_from_payload(payload, operation)?
    };
    let count_revision = optional_u64_field(payload, "count_revision")?
        .or_else(|| {
            authoritative_counts.as_ref().and_then(fleet_count_revision)
        });
    let mut summaries = Vec::new();
    let mut unresolved_logical_keys = Vec::new();
    let mut catalog = None;
    let mut partial = cursor_partial;
    if let Some(page) = payload.get("page").filter(|value| !value.is_null()) {
        let parsed = normalized_catalog_page(
            page,
            snapshot_cursor,
            alias,
            operation,
            &mut diagnostics,
        )?;
        summaries = parsed.0;
        catalog_scope = Some(parsed.1.scope);
        catalog_snapshot_id = Some(parsed.1.snapshot_id.clone());
        catalog = Some(parsed.1);
        partial |= catalog.as_ref().is_some_and(|catalog| {
            catalog.state == FleetCatalogContinuationStateWire::ResyncRequired
        });
    } else if let Some(entries) =
        payload.get("entries").filter(|value| !value.is_null())
    {
        let parsed = normalized_followed_entries(entries)?;
        summaries = parsed.0;
        unresolved_logical_keys = parsed.1;
    }
    Ok(PayloadNormalization {
        summaries,
        authoritative_counts,
        count_revision,
        catalog_scope,
        catalog_snapshot_id,
        freshness,
        catalog,
        unresolved_logical_keys,
        diagnostics,
        partial,
    })
}

fn normalized_catalog_page(
    value: &Value,
    snapshot_cursor: Option<StoreCursorWire>,
    alias: Option<&str>,
    operation: Option<&str>,
    diagnostics: &mut Vec<FleetEnvelopeDiagnosticWire>,
) -> Result<
    (Vec<ResolvedAgentSummaryWire>, FleetCatalogContinuationWire),
    FleetContractError,
> {
    let page = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet catalog payload.page must be a JSON object".to_string(),
        )
    })?;
    validate_allowed_fields(
        page,
        "fleet catalog payload.page",
        &[
            "schema_version",
            "scope",
            "snapshot_id",
            "rows",
            "limit",
            "total_matching_rows",
            "next_cursor",
            "has_more",
            "state",
            "reset_reason",
        ],
    )?;
    validate_optional_schema(page, "fleet catalog payload.page")?;
    let scope = page
        .get("scope")
        .map(|value| wire_from_json_value(value, "fleet catalog scope"))
        .transpose()?
        .unwrap_or_default();
    let snapshot_id = optional_string_field(
        page,
        "snapshot_id",
        "fleet catalog snapshot_id",
        MAX_IDENTIFIER_BYTES,
    )?
    .ok_or_else(|| {
        FleetContractError::Validation(
            "fleet catalog snapshot_id is required".to_string(),
        )
    })?;
    validate_fleet_catalog_snapshot_id(&snapshot_id)?;
    let rows = resolved_summaries_array(
        page.get("rows"),
        "fleet catalog payload.page.rows",
    )?;
    let limit_u64 = required_u64_field(page, "limit")?;
    let limit = u32::try_from(limit_u64).map_err(|_| {
        FleetContractError::Validation(
            "fleet catalog payload.page.limit is out of range".to_string(),
        )
    })?;
    normalize_catalog_limit(Some(limit))?;
    let total_matching_rows = required_u64_field(page, "total_matching_rows")?;
    let raw_next_cursor = optional_string_field(
        page,
        "next_cursor",
        "fleet catalog next_cursor",
        MAX_IDENTIFIER_BYTES,
    )?;
    let has_more = required_bool_field(page, "has_more")?;
    let reset_reason = page
        .get("reset_reason")
        .filter(|value| !value.is_null())
        .map(|value| wire_from_json_value(value, "fleet catalog reset_reason"))
        .transpose()?;
    let state = page
        .get("state")
        .map(|value| {
            wire_from_json_value(value, "fleet catalog continuation state")
        })
        .transpose()?
        .unwrap_or(if has_more {
            FleetCatalogContinuationStateWire::Ready
        } else {
            FleetCatalogContinuationStateWire::Finished
        });
    if state == FleetCatalogContinuationStateWire::ResyncRequired {
        if has_more || raw_next_cursor.is_some() || !rows.is_empty() {
            return Err(FleetContractError::Validation(
                "fleet catalog resync page must have no rows or continuation"
                    .to_string(),
            ));
        }
        return Ok((
            rows,
            FleetCatalogContinuationWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                snapshot_cursor,
                scope,
                snapshot_id,
                limit,
                total_matching_rows,
                next_cursor: None,
                has_more,
                state,
                reset_reason: reset_reason
                    .or(Some(FleetCatalogResetReasonWire::RestartRequired)),
            },
        ));
    }
    let mut next_cursor = None;
    match (has_more, raw_next_cursor) {
        (true, Some(cursor)) => {
            let parsed = parse_catalog_cursor(&cursor)?;
            if parsed.scope != scope || parsed.snapshot_id != snapshot_id {
                return Err(FleetContractError::Validation(
                    "fleet catalog next_cursor does not match page scope and snapshot_id"
                        .to_string(),
                ));
            }
            if state != FleetCatalogContinuationStateWire::Ready {
                return Err(FleetContractError::Validation(
                    "fleet catalog page has continuation but state is not ready"
                        .to_string(),
                ));
            }
            next_cursor = Some(cursor);
        }
        (true, None) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_cursor_missing",
                "warning",
                "fleet catalog page has more rows but no next_cursor",
            )?);
            return Err(FleetContractError::Validation(
                "fleet catalog page has more rows but no next_cursor"
                    .to_string(),
            ));
        }
        (false, Some(_)) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_cursor_inconsistent",
                "warning",
                "fleet catalog page returned next_cursor without has_more",
            )?);
            return Err(FleetContractError::Validation(
                "fleet catalog page returned next_cursor without has_more"
                    .to_string(),
            ));
        }
        (false, None) => {}
    }
    if !has_more && state != FleetCatalogContinuationStateWire::Finished {
        return Err(FleetContractError::Validation(
            "fleet catalog page without continuation must be finished"
                .to_string(),
        ));
    }
    if reset_reason.is_some() {
        return Err(FleetContractError::Validation(
            "fleet catalog reset_reason is only valid on resync_required pages"
                .to_string(),
        ));
    }
    Ok((
        rows,
        FleetCatalogContinuationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            snapshot_cursor,
            scope,
            snapshot_id,
            limit,
            total_matching_rows,
            next_cursor,
            has_more,
            state,
            reset_reason: None,
        },
    ))
}

fn normalized_followed_entries(
    value: &Value,
) -> Result<(Vec<ResolvedAgentSummaryWire>, Vec<String>), FleetContractError> {
    let entries = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet followed payload.entries must be a JSON array".to_string(),
        )
    })?;
    let mut summaries = Vec::new();
    let mut unresolved = Vec::new();
    for (index, value) in entries.iter().enumerate() {
        let entry: FleetLogicalBatchEntryWire = wire_from_json_value(
            value,
            &format!("fleet followed payload.entries[{index}]"),
        )?;
        validate_schema("fleet followed payload entry", entry.schema_version)?;
        validate_key(
            "fleet followed payload requested_logical_key",
            &entry.requested_logical_key,
        )?;
        match entry.summary {
            Some(summary) => {
                summaries.push(validate_resolved_agent_summary(&summary)?);
            }
            None => unresolved.push(entry.requested_logical_key),
        }
    }
    Ok((summaries, unresolved))
}

fn authoritative_counts_from_payload(
    payload: &Map<String, Value>,
    operation: Option<&str>,
) -> Result<Option<FleetLogicalAgentCountsWire>, FleetContractError> {
    if operation == Some("followed_batch") || operation == Some("attention") {
        return Ok(None);
    }
    let Some(value) = payload.get("counts") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let counts: FleetLogicalAgentCountsWire =
        wire_from_json_value(value, "fleet host authoritative counts")?;
    validate_fleet_logical_agent_counts(
        &counts,
        "fleet host authoritative counts",
    )
    .map(Some)
}

fn normalized_host_freshness(
    host: &Map<String, Value>,
    status: &str,
    diagnostic: Option<&FleetEnvelopeDiagnosticWire>,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    if let Some(value) = host.get("freshness") {
        return freshness_from_json_value(value, "fleet host freshness");
    }
    let error = if host_status_healthy(status) {
        None
    } else {
        Some(
            diagnostic
                .map(|diagnostic| diagnostic.code.clone())
                .unwrap_or_else(|| status.to_string()),
        )
    };
    Ok(FleetSnapshotFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness: if status == "stale" {
            ObservationFreshnessWire::Stale
        } else if host_status_healthy(status) {
            ObservationFreshnessWire::Fresh
        } else {
            ObservationFreshnessWire::Unknown
        },
        partial: !host_status_healthy(status),
        refreshed_at_unix: None,
        error,
    })
}

fn freshness_from_json_value(
    value: &Value,
    label: &str,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    if value.is_null() {
        return Ok(FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Unknown,
            partial: true,
            refreshed_at_unix: None,
            error: Some("missing_freshness".to_string()),
        });
    }
    if value.is_string() {
        let freshness: ObservationFreshnessWire =
            wire_from_json_value(value, label)?;
        return Ok(FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness,
            partial: freshness == ObservationFreshnessWire::Unknown,
            refreshed_at_unix: None,
            error: None,
        });
    }
    let freshness: FleetSnapshotFreshnessWire =
        wire_from_json_value(value, label)?;
    validate_fleet_snapshot_freshness(&freshness)
}

fn normalized_host_observed_at(
    host: &Map<String, Value>,
    freshness: &FleetSnapshotFreshnessWire,
    authoritative_counts: Option<&FleetLogicalAgentCountsWire>,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<Option<f64>, FleetContractError> {
    let host_observed = optional_f64_value(
        host.get("observed_at_unix")
            .or_else(|| host.get("observed_at")),
        "fleet host observed_at_unix",
    )?;
    let summary_observed =
        summaries.iter().try_fold(None, |observed, summary| {
            validate_resolved_agent_summary(summary)?;
            Ok::<_, FleetContractError>(max_optional_f64(
                observed,
                Some(summary.observed_at_unix),
            ))
        })?;
    Ok([
        host_observed,
        freshness.refreshed_at_unix,
        authoritative_counts
            .and_then(|counts| counts.basis.observed_at_unix_max),
        summary_observed,
    ]
    .into_iter()
    .fold(None, max_optional_f64))
}

fn optional_store_cursor_value(
    value: Option<&Value>,
    label: &str,
    alias: Option<&str>,
    operation: Option<&str>,
    diagnostics: &mut Vec<FleetEnvelopeDiagnosticWire>,
) -> Result<(Option<StoreCursorWire>, bool), FleetContractError> {
    let Some(value) = value else {
        return Ok((None, false));
    };
    if value.is_null() {
        return Ok((None, false));
    }
    let cursor: StoreCursorWire = match wire_from_json_value(value, label) {
        Ok(cursor) => cursor,
        Err(error) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_snapshot_cursor_invalid",
                "warning",
                &error.to_string(),
            )?);
            return Ok((None, true));
        }
    };
    match cursor.validate() {
        Ok(()) => Ok((Some(cursor), false)),
        Err(error) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_snapshot_cursor_invalid",
                "warning",
                &error.to_string(),
            )?);
            Ok((None, true))
        }
    }
}

fn nested_federation_result(response: &Value) -> &Value {
    let Some(object) = response.as_object() else {
        return response;
    };
    let Some(result) = object.get("result") else {
        return response;
    };
    if result.as_object().is_some_and(|result| {
        result.contains_key("hosts") || result.contains_key("operation")
    }) {
        result
    } else {
        response
    }
}

fn origin_from_host(
    object: &Map<String, Value>,
) -> Result<Option<OriginLocatorWire>, FleetContractError> {
    let origin = object
        .get("origin")
        .filter(|value| !value.is_null())
        .map(|value| {
            let origin: OriginLocatorWire =
                wire_from_json_value(value, "fleet federation host origin")?;
            origin.validate()?;
            Ok::<_, FleetContractError>(origin)
        })
        .transpose()?;
    let installation_id = optional_string_field(
        object,
        "installation_id",
        "fleet federation host installation_id",
        MAX_IDENTIFIER_BYTES,
    )?;
    let installation_origin = installation_id
        .map(|installation_id| {
            validate_installation_id(&installation_id)?;
            Ok::<_, FleetContractError>(OriginLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                installation_id,
            })
        })
        .transpose()?;
    if let (Some(origin), Some(installation_origin)) =
        (&origin, &installation_origin)
    {
        if origin != installation_origin {
            return Err(FleetContractError::Validation(
                "fleet federation host origin does not match installation_id"
                    .to_string(),
            ));
        }
    }
    Ok(origin.or(installation_origin))
}

fn safe_alias_from_host(object: &Map<String, Value>) -> Option<String> {
    optional_string_field(
        object,
        "alias",
        "fleet federation host alias",
        MAX_LABEL_BYTES,
    )
    .ok()
    .flatten()
}

fn diagnostic_from_error_value(
    value: Option<&Value>,
    alias: Option<&str>,
    operation: Option<&str>,
    default_code: &str,
) -> Result<Option<FleetEnvelopeDiagnosticWire>, FleetContractError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(text) = value.as_str() {
        return fleet_envelope_diagnostic(
            alias,
            operation,
            default_code,
            "error",
            text,
        )
        .map(Some);
    }
    let object = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation error must be a JSON object or string"
                .to_string(),
        )
    })?;
    validate_allowed_fields(
        object,
        "fleet federation error",
        &["schema_version", "code", "message", "target", "details"],
    )?;
    validate_optional_schema(object, "fleet federation error")?;
    let code = optional_string_field(
        object,
        "code",
        "fleet federation error code",
        MAX_IDENTIFIER_BYTES,
    )?
    .unwrap_or_else(|| default_code.to_string());
    validate_reference_id("fleet federation error code", &code)?;
    let message = optional_string_field(
        object,
        "message",
        "fleet federation error message",
        MAX_LABEL_BYTES,
    )?
    .unwrap_or_else(|| code.clone());
    fleet_envelope_diagnostic(alias, operation, &code, "error", &message)
        .map(Some)
}

fn diagnostics_array(
    value: Option<&Value>,
    alias: Option<&str>,
    operation: Option<&str>,
) -> Result<Vec<FleetEnvelopeDiagnosticWire>, FleetContractError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let values = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation diagnostics must be a JSON array".to_string(),
        )
    })?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut diagnostic: FleetEnvelopeDiagnosticWire =
                wire_from_json_value(
                    value,
                    &format!("fleet federation diagnostics[{index}]"),
                )?;
            if diagnostic.alias.is_none() {
                diagnostic.alias = alias.map(str::to_string);
            }
            if diagnostic.operation.is_none() {
                diagnostic.operation = operation.map(str::to_string);
            }
            validate_fleet_envelope_diagnostic(&diagnostic)
        })
        .collect()
}

fn validate_fleet_envelope_diagnostic(
    diagnostic: &FleetEnvelopeDiagnosticWire,
) -> Result<FleetEnvelopeDiagnosticWire, FleetContractError> {
    validate_schema("fleet envelope diagnostic", diagnostic.schema_version)?;
    if let Some(alias) = &diagnostic.alias {
        validate_label(
            "fleet envelope diagnostic alias",
            alias,
            MAX_LABEL_BYTES,
        )?;
        reject_secretish("fleet envelope diagnostic alias", alias)?;
    }
    if let Some(operation) = &diagnostic.operation {
        validate_reference_id(
            "fleet envelope diagnostic operation",
            operation,
        )?;
    }
    validate_reference_id("fleet envelope diagnostic code", &diagnostic.code)?;
    validate_reference_id(
        "fleet envelope diagnostic severity",
        &diagnostic.severity,
    )?;
    if !matches!(diagnostic.severity.as_str(), "info" | "warning" | "error") {
        return Err(FleetContractError::Validation(
            "fleet envelope diagnostic severity must be info, warning, or error"
                .to_string(),
        ));
    }
    validate_label(
        "fleet envelope diagnostic message",
        &diagnostic.message,
        MAX_LABEL_BYTES,
    )?;
    reject_secretish("fleet envelope diagnostic message", &diagnostic.message)?;
    Ok(diagnostic.clone())
}

fn fleet_envelope_diagnostic(
    alias: Option<&str>,
    operation: Option<&str>,
    code: &str,
    severity: &str,
    message: &str,
) -> Result<FleetEnvelopeDiagnosticWire, FleetContractError> {
    let message = sanitize_diagnostic_message(message);
    let diagnostic = FleetEnvelopeDiagnosticWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias: alias.map(str::to_string),
        operation: operation.map(str::to_string),
        code: code.to_string(),
        severity: severity.to_string(),
        message,
    };
    validate_fleet_envelope_diagnostic(&diagnostic)
}

fn sanitize_diagnostic_message(message: &str) -> String {
    let normalized = replace_control_characters(message);
    let trimmed = normalized.trim();
    let redacted = if trimmed.contains("://")
        || trimmed.contains("Authorization:")
        || trimmed.to_ascii_lowercase().contains("bearer ")
        || reject_secretish("fleet envelope diagnostic message", trimmed)
            .is_err()
    {
        "federation diagnostic redacted"
    } else if trimmed.is_empty() {
        "federation diagnostic omitted"
    } else {
        trimmed
    };
    trim_to_limit(redacted, MAX_LABEL_BYTES)
}

fn host_status_healthy(status: &str) -> bool {
    status == "ok"
}

fn resolved_summaries_array(
    value: Option<&Value>,
    label: &str,
) -> Result<Vec<ResolvedAgentSummaryWire>, FleetContractError> {
    let value = value.ok_or_else(|| {
        FleetContractError::Validation(format!("{label} is required"))
    })?;
    let values = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(format!("{label} must be a JSON array"))
    })?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let summary: ResolvedAgentSummaryWire =
                wire_from_json_value(value, &format!("{label}[{index}]"))?;
            validate_resolved_agent_summary(&summary)
        })
        .collect()
}

fn validate_allowed_fields(
    object: &Map<String, Value>,
    label: &str,
    allowed: &[&str],
) -> Result<(), FleetContractError> {
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(FleetContractError::Validation(format!(
                "{label} contains unknown field {key}"
            )));
        }
    }
    Ok(())
}

fn validate_optional_schema(
    object: &Map<String, Value>,
    label: &str,
) -> Result<(), FleetContractError> {
    let Some(value) = object.get("schema_version") else {
        return Ok(());
    };
    let Some(version) = value.as_u64() else {
        return Err(FleetContractError::Validation(format!(
            "{label} schema_version must be an unsigned integer"
        )));
    };
    let version = u32::try_from(version).map_err(|_| {
        FleetContractError::Validation(format!(
            "{label} schema_version is out of range"
        ))
    })?;
    validate_schema(label, version)
}

fn optional_string_field(
    object: &Map<String, Value>,
    field: &str,
    label: &str,
    max_bytes: usize,
) -> Result<Option<String>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(text) = value.as_str() else {
        return Err(FleetContractError::Validation(format!(
            "{label} must be a string"
        )));
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    validate_label(label, text, max_bytes)?;
    reject_secretish(label, text)?;
    Ok(Some(text.to_string()))
}

fn optional_bool_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<bool>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .ok_or_else(|| {
            FleetContractError::Validation(format!("{field} must be a boolean"))
        })
        .map(Some)
}

fn required_bool_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<bool, FleetContractError> {
    optional_bool_field(object, field)?.ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })
}

fn optional_u64_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .ok_or_else(|| {
            FleetContractError::Validation(format!(
                "{field} must be an unsigned integer"
            ))
        })
        .map(Some)
}

fn required_u64_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<u64, FleetContractError> {
    optional_u64_field(object, field)?.ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })
}

fn optional_non_negative_seconds_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<f64>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(number) = value.as_f64() else {
        return Err(FleetContractError::Validation(format!(
            "{field} must be a finite number"
        )));
    };
    validate_non_negative_seconds(field, number)?;
    Ok(Some(number))
}

fn optional_f64_value(
    value: Option<&Value>,
    label: &str,
) -> Result<Option<f64>, FleetContractError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(number) = value.as_f64() else {
        return Err(FleetContractError::Validation(format!(
            "{label} must be a finite number"
        )));
    };
    validate_timestamp(label, number)?;
    Ok(Some(number))
}

fn array_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Vec<Value>, FleetContractError> {
    let value = object.get(field).ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })?;
    value.as_array().ok_or_else(|| {
        FleetContractError::Validation(format!("{field} must be a JSON array"))
    })
}

fn wire_from_json_value<T: DeserializeOwned>(
    value: &Value,
    label: &str,
) -> Result<T, FleetContractError> {
    serde_json::from_value(value.clone()).map_err(|error| {
        FleetContractError::Validation(format!(
            "{label} is not a valid fleet wire value: {error}"
        ))
    })
}

fn count_scope(
    local_summaries: &[ResolvedAgentSummaryWire],
    hosts: &[FleetHostCountInputWire],
    label: &str,
    allow_authoritative_counts: bool,
) -> Result<FleetScopeCountsWire, FleetContractError> {
    let mut host_origins = BTreeSet::new();
    let mut unknown_origins = Vec::new();
    let mut host_counts = Vec::new();
    let local_counts =
        count_logical_agents(&FleetLogicalAgentCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            summaries: local_summaries.to_vec(),
        })?;
    let mut counts = empty_logical_counts();
    add_logical_counts(&mut counts, &local_counts);
    for host in hosts {
        host.validate(label)?;
        let origin_key = host.origin.installation_id.clone();
        if !host_origins.insert(origin_key.clone()) {
            return Err(FleetContractError::Validation(format!(
                "{label} contains duplicate origin {origin_key}"
            )));
        }
        let counts_for_host = if allow_authoritative_counts {
            match &host.authoritative_counts {
                Some(counts) => validate_fleet_logical_agent_counts(
                    counts,
                    "fleet host authoritative counts",
                )?,
                None => {
                    count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                        summaries: host.summaries.clone(),
                    })?
                }
            }
        } else {
            count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                summaries: host.summaries.clone(),
            })?
        };
        let partial = host.partial
            || matches!(
                host.freshness,
                ObservationFreshnessWire::Stale
                    | ObservationFreshnessWire::Unknown
            );
        if partial {
            unknown_origins.push(origin_key);
        }
        let host_observed_at = max_optional_f64(
            host.observed_at_unix,
            counts_for_host.basis.observed_at_unix_max,
        );
        host_counts.push(FleetHostCountWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: host.origin.clone(),
            counts: counts_for_host.clone(),
            partial,
            observed_at_unix: host_observed_at,
        });
        add_logical_counts(&mut counts, &counts_for_host);
        counts.basis.observed_at_unix_max = max_optional_f64(
            counts.basis.observed_at_unix_max,
            host_observed_at,
        );
    }
    unknown_origins.sort();
    let observed_at_unix_max = counts.basis.observed_at_unix_max;
    Ok(FleetScopeCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        counts,
        partial: !unknown_origins.is_empty(),
        observed_at_unix_max,
        unknown_origins,
        host_counts,
    })
}

fn empty_logical_counts() -> FleetLogicalAgentCountsWire {
    FleetLogicalAgentCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        basis: FleetCountBasisWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            input_rows: 0,
            selected_rows: 0,
            max_revision: None,
            observed_at_unix_max: None,
        },
        logical_agent_total: 0,
        running: 0,
        waiting: 0,
        attention: 0,
        occupied_runner_slots: 0,
    }
}

fn add_logical_counts(
    total: &mut FleetLogicalAgentCountsWire,
    counts: &FleetLogicalAgentCountsWire,
) {
    total.basis.input_rows = total
        .basis
        .input_rows
        .saturating_add(counts.basis.input_rows);
    total.basis.selected_rows = total
        .basis
        .selected_rows
        .saturating_add(counts.basis.selected_rows);
    total.basis.max_revision =
        max_optional_u64(total.basis.max_revision, counts.basis.max_revision);
    total.basis.observed_at_unix_max = max_optional_f64(
        total.basis.observed_at_unix_max,
        counts.basis.observed_at_unix_max,
    );
    total.logical_agent_total = total
        .logical_agent_total
        .saturating_add(counts.logical_agent_total);
    total.running = total.running.saturating_add(counts.running);
    total.waiting = total.waiting.saturating_add(counts.waiting);
    total.attention = total.attention.saturating_add(counts.attention);
    total.occupied_runner_slots = total
        .occupied_runner_slots
        .saturating_add(counts.occupied_runner_slots);
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_optional_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn normalize_catalog_limit(
    limit: Option<u32>,
) -> Result<u32, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_DEFAULT_PAGE_ROWS);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet catalog limit must be positive".to_string(),
        ));
    }
    if limit > FLEET_READ_MAX_PAGE_ROWS {
        return Err(FleetContractError::Validation(format!(
            "fleet catalog limit exceeds {FLEET_READ_MAX_PAGE_ROWS}"
        )));
    }
    Ok(limit)
}

fn normalize_content_read_limit(
    limit: Option<u64>,
) -> Result<u64, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_DEFAULT_CONTENT_BYTES);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet content limit must be positive".to_string(),
        ));
    }
    if limit > FLEET_READ_MAX_CONTENT_BYTES {
        return Err(FleetContractError::Validation(format!(
            "fleet content limit exceeds {FLEET_READ_MAX_CONTENT_BYTES}"
        )));
    }
    Ok(limit)
}

fn normalize_project_limit(
    limit: Option<u32>,
) -> Result<u32, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_MAX_PROJECT_IDS as u32);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet project eligibility limit must be positive".to_string(),
        ));
    }
    if limit as usize > FLEET_READ_MAX_PROJECT_IDS {
        return Err(FleetContractError::Validation(format!(
            "fleet project eligibility limit exceeds {FLEET_READ_MAX_PROJECT_IDS}"
        )));
    }
    Ok(limit)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCatalogCursor {
    scope: FleetCatalogScopeWire,
    snapshot_id: String,
    offset: usize,
}

fn parse_catalog_cursor(
    cursor: &str,
) -> Result<ParsedCatalogCursor, FleetContractError> {
    validate_reference_id("fleet catalog cursor", cursor)?;
    reject_path_like("fleet catalog cursor", cursor)?;
    let Some(body) = cursor.strip_prefix(FLEET_CATALOG_CURSOR_PREFIX) else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is not recognized".to_string(),
        ));
    };
    let Some(body) = body.strip_prefix(':') else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is malformed".to_string(),
        ));
    };
    let mut parts = body.split(':');
    let Some(scope_token) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor scope is missing".to_string(),
        ));
    };
    let Some(snapshot_id) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor snapshot_id is missing".to_string(),
        ));
    };
    let Some(offset) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor offset is missing".to_string(),
        ));
    };
    if parts.next().is_some() {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is malformed".to_string(),
        ));
    }
    let scope = match scope_token {
        "p" => FleetCatalogScopeWire::Presentation,
        "h" => FleetCatalogScopeWire::History,
        _ => {
            return Err(FleetContractError::Validation(
                "fleet catalog cursor scope is not recognized".to_string(),
            ))
        }
    };
    validate_fleet_catalog_snapshot_id(snapshot_id)?;
    if offset.is_empty()
        || !offset.bytes().all(|byte| byte.is_ascii_digit())
        || (offset.len() > 1 && offset.starts_with('0'))
    {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor offset is malformed".to_string(),
        ));
    }
    let offset = offset.parse::<usize>().map_err(|_| {
        FleetContractError::Validation(
            "fleet catalog cursor offset is out of range".to_string(),
        )
    })?;
    Ok(ParsedCatalogCursor {
        scope,
        snapshot_id: snapshot_id.to_string(),
        offset,
    })
}

fn format_catalog_cursor(
    scope: FleetCatalogScopeWire,
    snapshot_id: &str,
    offset: usize,
) -> String {
    let scope = match scope {
        FleetCatalogScopeWire::Presentation => "p",
        FleetCatalogScopeWire::History => "h",
    };
    format!("{FLEET_CATALOG_CURSOR_PREFIX}:{scope}:{snapshot_id}:{offset}")
}

fn fleet_catalog_restart_page(
    scope: FleetCatalogScopeWire,
    snapshot_id: String,
    limit: u32,
    reset_reason: FleetCatalogResetReasonWire,
) -> FleetCatalogPageSelectionWire {
    FleetCatalogPageSelectionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope,
        snapshot_id,
        rows: Vec::new(),
        limit,
        total_matching_rows: 0,
        next_cursor: None,
        has_more: false,
        state: FleetCatalogContinuationStateWire::ResyncRequired,
        reset_reason: Some(reset_reason),
    }
}

fn catalog_snapshot_summary_value(summary: &ResolvedAgentSummaryWire) -> Value {
    let mut value = serde_json::to_value(summary).unwrap_or_else(|_| {
        json!({
            "logical_key": summary.logical_key,
            "exact_key": summary.exact_key,
            "row_revision": summary.row_revision.revision,
        })
    });
    if let Value::Object(object) = &mut value {
        object.remove("observed_at_unix");
        object.remove("freshness");
    }
    value
}

fn validate_identifier_vec(
    field: &str,
    values: &[String],
    max_count: usize,
    max_bytes: usize,
) -> Result<(), FleetContractError> {
    if values.len() > max_count {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {max_count} entries"
        )));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        validate_label(field, value, max_bytes)?;
        reject_secretish(field, value)?;
        if !seen.insert(value.trim()) {
            return Err(FleetContractError::Validation(format!(
                "{field} contains duplicate entry"
            )));
        }
    }
    Ok(())
}

fn summary_matches_catalog_query(
    summary: &ResolvedAgentSummaryWire,
    query: &FleetCatalogQueryWire,
) -> Result<bool, FleetContractError> {
    if !query.project_ids.is_empty()
        && !query.project_ids.iter().any(|project| {
            project == &summary.logical_locator.project.project_id
        })
    {
        return Ok(false);
    }
    if !query.include_terminal && terminal_lifecycle(summary.lifecycle) {
        return Ok(false);
    }
    if !query.status_buckets.is_empty()
        && !query.status_buckets.contains(&summary.status_bucket)
    {
        return Ok(false);
    }
    if let Some(text) = query.query.as_deref().map(str::trim) {
        if text.is_empty() {
            return Ok(true);
        }
        let needle = text.to_ascii_lowercase();
        let haystacks = [
            Some(summary.project_name.as_str()),
            Some(summary.status.as_str()),
            summary.model.as_deref(),
            summary.provider.as_deref(),
            summary.intent.as_deref(),
            Some(summary.labels.project_label.as_str()),
            summary.labels.agent_label.as_deref(),
            summary.labels.family_label.as_deref(),
            summary.labels.owner_label.as_deref(),
            summary.labels.alias.as_deref(),
        ];
        return Ok(haystacks
            .into_iter()
            .flatten()
            .any(|value| value.to_ascii_lowercase().contains(&needle)));
    }
    Ok(true)
}

fn compare_catalog_summaries(
    left: &ResolvedAgentSummaryWire,
    right: &ResolvedAgentSummaryWire,
) -> std::cmp::Ordering {
    let left_rank = catalog_status_rank(left);
    let right_rank = catalog_status_rank(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| left.project_name.cmp(&right.project_name))
        .then_with(|| {
            left.labels
                .agent_label
                .as_deref()
                .unwrap_or("")
                .cmp(right.labels.agent_label.as_deref().unwrap_or(""))
        })
        .then_with(|| {
            left.labels
                .family_label
                .as_deref()
                .unwrap_or("")
                .cmp(right.labels.family_label.as_deref().unwrap_or(""))
        })
        .then_with(|| right.observed_at_unix.total_cmp(&left.observed_at_unix))
        .then_with(|| left.logical_key.cmp(&right.logical_key))
}

fn catalog_status_rank(summary: &ResolvedAgentSummaryWire) -> u8 {
    if summary.needs_attention {
        return 0;
    }
    match summary.status_bucket {
        FleetStatusBucketWire::Running => 1,
        FleetStatusBucketWire::Starting => 2,
        FleetStatusBucketWire::Waiting => 3,
        FleetStatusBucketWire::Queued => 4,
        FleetStatusBucketWire::Failed => 5,
        FleetStatusBucketWire::Stopped => 6,
        FleetStatusBucketWire::Done => 7,
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

pub(crate) fn logical_key_unchecked(
    locator: &LogicalAgentLocatorWire,
) -> String {
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

pub(crate) fn validate_schema(
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

pub(crate) fn validate_installation_id(
    value: &str,
) -> Result<(), FleetContractError> {
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

pub(crate) fn validate_label(
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

pub(crate) fn validate_timestamp(
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

pub(crate) fn validate_non_negative_seconds(
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

pub(crate) fn reject_path_like(
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

pub(crate) fn reject_secretish(
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

pub(crate) fn timestamp_ms(
    field: &str,
    value: f64,
) -> Result<u64, FleetContractError> {
    validate_timestamp(field, value)?;
    let millis = value * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(FleetContractError::Validation(format!(
            "{field} is too large to represent in milliseconds"
        )));
    }
    Ok(millis.round() as u64)
}

pub(crate) fn duration_ms(
    field: &str,
    value: f64,
) -> Result<u64, FleetContractError> {
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

    fn authoritative_counts(
        running: u64,
        total: u64,
        observed_at_unix_max: Option<f64>,
    ) -> FleetLogicalAgentCountsWire {
        FleetLogicalAgentCountsWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            basis: FleetCountBasisWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                input_rows: total,
                selected_rows: total,
                max_revision: Some(total),
                observed_at_unix_max,
            },
            logical_agent_total: total,
            running,
            waiting: 0,
            attention: 0,
            occupied_runner_slots: running,
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

    fn summary_done(
        hex: char,
        agent: &str,
        revision_num: u64,
        observed_at_unix: f64,
    ) -> ResolvedAgentSummaryWire {
        let locator = logical(hex, agent);
        let mut record = record_running();
        record.running = None;
        record.done = Some(DoneMarkerWire {
            outcome: Some("completed".to_string()),
            status_label: Some("DONE".to_string()),
            ..DoneMarkerWire::default()
        });
        record.has_done_marker = true;
        let mut request = projection_request(
            locator,
            Some(exact(hex, agent, "run-1")),
            revision_num,
            record,
        );
        request.owner_facts.liveness = OwnerLivenessWire::Dead;
        request.owner_facts.occupied_runner_slot = false;
        request.owner_facts.capabilities = caps(&[]);
        request.owner_facts.observed_at_unix = observed_at_unix;
        project_resolved_agent_summary(&request).unwrap()
    }

    fn catalog_snapshot_id(
        scope: FleetCatalogScopeWire,
        summaries: &[ResolvedAgentSummaryWire],
    ) -> String {
        fleet_catalog_snapshot_id(scope, summaries).unwrap()
    }

    fn catalog_cursor(
        scope: FleetCatalogScopeWire,
        snapshot_id: &str,
        offset: usize,
    ) -> String {
        let scope = match scope {
            FleetCatalogScopeWire::Presentation => "p",
            FleetCatalogScopeWire::History => "h",
        };
        format!("catcur_v1:{scope}:{snapshot_id}:{offset}")
    }

    fn catalog_response(
        page: FleetCatalogPageSelectionWire,
        count_rows: &[ResolvedAgentSummaryWire],
    ) -> FleetCatalogPageWire {
        let counts =
            count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                summaries: count_rows.to_vec(),
            })
            .unwrap();
        FleetCatalogPageWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: StoreCursorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                store_generation: "gen-test".to_string(),
                sequence: 1,
            },
            counts: counts.clone(),
            count_revision: fleet_count_revision(&counts),
            freshness: FleetSnapshotFreshnessWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                freshness: ObservationFreshnessWire::Fresh,
                partial: false,
                refreshed_at_unix: Some(1000.0),
                error: None,
            },
            page,
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
        let mut record = record_running();
        if let Some(meta) = record.agent_meta.as_mut() {
            meta.queue_weight = Some(0.25);
            meta.queue_weight_explicit = true;
        }
        let mut request = projection_request(
            locator.clone(),
            Some(exact_locator.clone()),
            1,
            record,
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
        assert_eq!(summary.queue_weight, Some(0.25));
        assert!(summary.queue_weight_explicit);
        assert!(!summary.queue_weight_invalid);
        let value = serde_json::to_value(&summary).unwrap();
        assert_no_forbidden_local_fields(&value);

        let detail = project_resolved_agent_detail(&request).unwrap();
        assert_eq!(detail.content_handles.len(), 1);
        let detail_value = serde_json::to_value(&detail).unwrap();
        assert_no_forbidden_local_fields(&detail_value);
    }

    #[test]
    fn projection_prefers_waiting_queue_weight_over_metadata() {
        let locator = logical('a', "weighted");
        let exact_locator = exact('a', "weighted", "run-1");
        let mut record = record_running();
        if let Some(meta) = record.agent_meta.as_mut() {
            meta.queue_weight = Some(2.0);
            meta.queue_weight_explicit = false;
        }
        record.waiting = Some(crate::agent_scan::WaitingMarkerWire {
            queue_weight: Some(0.5),
            queue_weight_explicit: true,
            ..crate::agent_scan::WaitingMarkerWire::default()
        });
        let request =
            projection_request(locator, Some(exact_locator), 1, record);

        let summary = project_resolved_agent_summary(&request).unwrap();

        assert_eq!(summary.queue_weight, Some(0.5));
        assert!(summary.queue_weight_explicit);
        assert!(!summary.queue_weight_invalid);
    }

    #[test]
    fn projection_normalizes_control_characters_in_multiline_raw_prompt_intent()
    {
        let locator = logical('a', "multiline");
        let exact_locator = exact('a', "multiline", "run-1");
        let mut record = record_running();
        record.raw_prompt_snippet = Some(
            "Refactor the widget\nand update the tests\r\nplease\tthanks"
                .to_string(),
        );
        let request =
            projection_request(locator, Some(exact_locator), 1, record);

        let summary = project_resolved_agent_summary(&request).unwrap();

        let intent = summary.intent.as_deref().unwrap();
        assert!(!intent.chars().any(char::is_control));
        assert_eq!(
            intent,
            "Refactor the widget and update the tests  please thanks"
        );
    }

    #[test]
    fn projection_normalizes_control_characters_in_plan_action_intent() {
        let locator = logical('a', "plan-multiline");
        let exact_locator = exact('a', "plan-multiline", "run-1");
        let mut record = record_running();
        if let Some(meta) = record.agent_meta.as_mut() {
            meta.plan_action = Some("Step 1: build\nStep 2: test".to_string());
        }
        let request =
            projection_request(locator, Some(exact_locator), 1, record);

        let summary = project_resolved_agent_summary(&request).unwrap();

        assert_eq!(
            summary.intent.as_deref(),
            Some("Step 1: build Step 2: test")
        );
    }

    #[test]
    fn projection_bounds_multiline_unicode_intent_to_byte_limit() {
        let locator = logical('a', "byte-limit");
        let exact_locator = exact('a', "byte-limit", "run-1");
        let mut record = record_running();
        record.raw_prompt_snippet =
            Some(format!("line one\nline two\n{}", "é".repeat(400)));
        let request =
            projection_request(locator, Some(exact_locator), 1, record);

        let summary = project_resolved_agent_summary(&request).unwrap();

        let intent = summary.intent.unwrap();
        assert!(intent.len() <= MAX_INTENT_BYTES);
        assert!(!intent.chars().any(char::is_control));
        assert!(validate_label("intent", &intent, MAX_INTENT_BYTES).is_ok());
    }

    #[test]
    fn projection_omits_intent_when_normalization_leaves_it_empty() {
        let locator = logical('a', "empty-intent");
        let exact_locator = exact('a', "empty-intent", "run-1");
        let mut record = record_running();
        record.raw_prompt_snippet = Some("\u{1}\u{2}\u{3}".to_string());
        let request =
            projection_request(locator, Some(exact_locator), 1, record);

        let summary = project_resolved_agent_summary(&request).unwrap();

        assert!(summary.intent.is_none());
    }

    #[test]
    fn external_wire_summary_with_raw_control_character_intent_is_rejected() {
        let locator = logical('a', "external");
        let exact_locator = exact('a', "external", "run-1");
        let request = projection_request(
            locator,
            Some(exact_locator),
            1,
            record_running(),
        );
        let mut summary = project_resolved_agent_summary(&request).unwrap();

        // A projected summary already carries a normalized intent; strict
        // external validation (the boundary used by federation imports and
        // any other externally supplied wire payload) must still reject a
        // raw control character regardless of how the value arrived.
        summary.intent = Some("bad\nintent".to_string());
        assert!(validate_resolved_agent_summary(&summary).is_err());
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
    fn family_role_distinguishes_root_member_and_historical_shell() {
        // A live root: no tracked parent.
        let root_request = projection_request(
            logical('a', "root"),
            Some(exact('a', "root", "run-1")),
            1,
            record_running(),
        );
        let root = project_resolved_agent_summary(&root_request).unwrap();
        assert_eq!(root.family_role, FleetFamilyRoleWire::Root);
        assert_eq!(root.parent_timestamp, None);
        assert_eq!(root.status_bucket, FleetStatusBucketWire::Running);

        // A live member: tracked parent_timestamp.
        let mut member_record = record_running();
        member_record.agent_meta.as_mut().unwrap().parent_timestamp =
            Some("20260906110000".to_string());
        let member_request = projection_request(
            logical('a', "member"),
            Some(exact('a', "member", "run-1")),
            1,
            member_record,
        );
        let member = project_resolved_agent_summary(&member_request).unwrap();
        assert_eq!(member.family_role, FleetFamilyRoleWire::Member);
        assert_eq!(member.parent_timestamp, Some("20260906110000".to_string()));

        // A genuinely completed record is a historical shell.
        let done = summary_done('a', "done", 1, 1000.0);
        assert_eq!(done.family_role, FleetFamilyRoleWire::HistoricalShell);

        // A Dead active-tier record (not yet done, not protected) demotes
        // into a historical shell and a stopped bucket, never running.
        let mut demoted_request = projection_request(
            logical('a', "demoted"),
            Some(exact('a', "demoted", "run-1")),
            1,
            record_running(),
        );
        demoted_request.owner_facts.liveness = OwnerLivenessWire::Dead;
        demoted_request.owner_facts.current_instance = false;
        demoted_request.owner_facts.occupied_runner_slot = false;
        demoted_request.owner_facts.capabilities = caps(&[]);
        let demoted = project_resolved_agent_summary(&demoted_request).unwrap();
        assert_eq!(demoted.family_role, FleetFamilyRoleWire::HistoricalShell);
        assert_eq!(demoted.status_bucket, FleetStatusBucketWire::Stopped);
        assert_eq!(demoted.lifecycle, FleetLifecycleWire::Running);

        // A waiting record protected by a marker stays Root/Waiting even if
        // its owner liveness is Dead: a waiting/question marker must never
        // be demoted.
        let mut protected_record = record_running();
        protected_record.running = None;
        protected_record.waiting =
            Some(crate::agent_scan::WaitingMarkerWire::default());
        let mut protected_request = projection_request(
            logical('a', "protected"),
            Some(exact('a', "protected", "run-1")),
            1,
            protected_record,
        );
        protected_request.owner_facts.liveness = OwnerLivenessWire::Dead;
        protected_request.owner_facts.current_instance = false;
        protected_request.owner_facts.occupied_runner_slot = false;
        protected_request.owner_facts.capabilities = caps(&[]);
        let protected =
            project_resolved_agent_summary(&protected_request).unwrap();
        assert_eq!(protected.family_role, FleetFamilyRoleWire::Root);
        assert_eq!(protected.status_bucket, FleetStatusBucketWire::Waiting);
    }

    #[test]
    fn dead_or_not_process_liveness_never_counts_as_running() {
        let mut alive_request = projection_request(
            logical('a', "alive"),
            Some(exact('a', "alive", "run-1")),
            1,
            record_running(),
        );
        alive_request.owner_facts.occupied_runner_slot = false;
        let alive = project_resolved_agent_summary(&alive_request).unwrap();

        let mut dead_request = projection_request(
            logical('a', "dead"),
            Some(exact('a', "dead", "run-1")),
            1,
            record_running(),
        );
        dead_request.owner_facts.liveness = OwnerLivenessWire::Dead;
        dead_request.owner_facts.occupied_runner_slot = false;
        dead_request.owner_facts.capabilities = caps(&[]);
        let dead = project_resolved_agent_summary(&dead_request).unwrap();
        assert_eq!(dead.status_bucket, FleetStatusBucketWire::Stopped);
        // Still counted as a logical agent (it is still a served row) even
        // though liveness alone keeps it out of the running count below:
        // this proves `counts_as_running` gates on liveness directly rather
        // than trusting the status bucket or `current_instance` alone.
        assert!(dead.current_instance);

        let counts =
            count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                summaries: vec![alive.clone(), dead],
            })
            .unwrap();
        assert_eq!(counts.logical_agent_total, 2);
        assert_eq!(counts.running, 1);
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
        monitor.family_role = FleetFamilyRoleWire::Monitor;
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
                authoritative_counts: None,
                partial: false,
            }],
            fleet_hosts: vec![
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('b'),
                    summaries: vec![remote_followed],
                    observed_at_unix: Some(2000.0),
                    freshness: ObservationFreshnessWire::Fresh,
                    authoritative_counts: None,
                    partial: false,
                },
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('c'),
                    summaries: vec![remote_unfollowed],
                    observed_at_unix: Some(1500.0),
                    freshness: ObservationFreshnessWire::Aging,
                    authoritative_counts: None,
                    partial: false,
                },
                FleetHostCountInputWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    origin: origin('d'),
                    summaries: Vec::new(),
                    observed_at_unix: None,
                    freshness: ObservationFreshnessWire::Unknown,
                    authoritative_counts: None,
                    partial: false,
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
    fn federation_catalog_normalization_preserves_authoritative_counts_freshness_and_cursors(
    ) {
        let apollo_done =
            summary_done('b', "followed-done", 7, 1_700_000_000.0);
        let mac_running = project_resolved_agent_summary(&projection_request(
            logical('c', "running"),
            Some(exact('c', "running", "run-1")),
            8,
            record_running(),
        ))
        .unwrap();
        let apollo_counts = authoritative_counts(9, 9, Some(1_800_000_000.0));
        let mac_counts = authoritative_counts(2, 2, Some(1_700_000_001.0));
        let apollo_snapshot_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&apollo_done),
        );
        let mac_snapshot_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&mac_running),
        );
        let response = json!({
            "schema_version": 1,
            "operation": "catalog",
            "configured_hosts": 2,
            "hosts": [
                {
                    "schema_version": 1,
                    "alias": "apollo",
                    "provider_ref": "apollo-provider",
                    "installation_id": id('b'),
                    "endpoint": "https://apollo.example.test",
                    "status": "ok",
                    "cached": false,
                    "age_seconds": null,
                    "payload": {
                        "schema_version": 1,
                        "cursor": {
                            "schema_version": 1,
                            "store_generation": "gen-apollo",
                            "sequence": 12
                        },
                        "catalog_scope": "presentation",
                        "catalog_snapshot_id": apollo_snapshot_id.clone(),
                        "counts": apollo_counts,
                        "count_revision": 99,
                        "freshness": {
                            "schema_version": 1,
                            "freshness": "fresh",
                            "partial": true,
                            "refreshed_at_unix": 1_800_000_000.0,
                            "error": "catalog_partial"
                        },
                        "page": {
                            "schema_version": 1,
                            "scope": "presentation",
                            "snapshot_id": apollo_snapshot_id.clone(),
                            "rows": [apollo_done],
                            "limit": 50,
                            "total_matching_rows": 42,
                            "next_cursor": catalog_cursor(
                                FleetCatalogScopeWire::Presentation,
                                &apollo_snapshot_id,
                                50,
                            ),
                            "has_more": true,
                            "state": "ready"
                        }
                    },
                    "error": null
                },
                {
                    "schema_version": 1,
                    "alias": "mac",
                    "provider_ref": "mac-provider",
                    "installation_id": id('c'),
                    "endpoint": "https://mac.example.test",
                    "status": "ok",
                    "cached": true,
                    "age_seconds": 2.0,
                    "payload": {
                        "schema_version": 1,
                        "cursor": {
                            "schema_version": 1,
                            "store_generation": "gen-mac",
                            "sequence": 3
                        },
                        "catalog_scope": "presentation",
                        "catalog_snapshot_id": mac_snapshot_id.clone(),
                        "counts": mac_counts,
                        "freshness": "fresh",
                        "page": {
                            "schema_version": 1,
                            "scope": "presentation",
                            "snapshot_id": mac_snapshot_id.clone(),
                            "rows": [mac_running],
                            "limit": 20,
                            "total_matching_rows": 25,
                            "next_cursor": catalog_cursor(
                                FleetCatalogScopeWire::Presentation,
                                &mac_snapshot_id,
                                20,
                            ),
                            "has_more": true,
                            "state": "ready"
                        }
                    },
                    "error": null
                }
            ]
        });

        let normalized = normalize_fleet_federation_response(
            &FleetFederationNormalizeRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                response: response.clone(),
            },
        )
        .unwrap();

        assert_eq!(normalized.operation.as_deref(), Some("catalog"));
        assert!(normalized.partial);
        assert_eq!(normalized.summaries.len(), 2);
        assert_eq!(
            normalized.hosts[0]
                .catalog
                .as_ref()
                .unwrap()
                .next_cursor
                .as_deref(),
            Some(
                catalog_cursor(
                    FleetCatalogScopeWire::Presentation,
                    &apollo_snapshot_id,
                    50,
                )
                .as_str()
            )
        );
        assert_eq!(
            normalized.hosts[1]
                .catalog
                .as_ref()
                .unwrap()
                .next_cursor
                .as_deref(),
            Some(
                catalog_cursor(
                    FleetCatalogScopeWire::Presentation,
                    &mac_snapshot_id,
                    20,
                )
                .as_str()
            )
        );
        assert_eq!(
            normalized.hosts[0].catalog_scope,
            Some(FleetCatalogScopeWire::Presentation)
        );
        assert_eq!(
            normalized.hosts[0].catalog_snapshot_id.as_deref(),
            Some(apollo_snapshot_id.as_str())
        );
        assert_eq!(
            normalized.hosts[0]
                .catalog
                .as_ref()
                .unwrap()
                .snapshot_cursor
                .as_ref()
                .unwrap()
                .store_generation,
            "gen-apollo"
        );
        assert_eq!(normalized.hosts[0].observed_at_unix, Some(1_800_000_000.0));
        assert_eq!(
            normalized.hosts[0]
                .authoritative_counts
                .as_ref()
                .unwrap()
                .running,
            9
        );
        assert!(normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "fleet_host_partial"
                && diagnostic.alias.as_deref() == Some("apollo")
        }));

        let counted = count_focus_and_fleet_from_federation(
            &FocusFleetFederationCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                local_summaries: Vec::new(),
                followed_response: None,
                fleet_response: Some(response),
            },
        )
        .unwrap();
        assert_eq!(counted.fleet.counts.running, 11);
        assert_eq!(counted.fleet.counts.occupied_runner_slots, 11);
        assert!(counted.fleet.partial);
        assert_eq!(counted.fleet.unknown_origins, vec![id('b')]);
    }

    #[test]
    fn federation_followed_batch_counts_only_resolved_requested_entries() {
        let local = project_resolved_agent_summary(&projection_request(
            logical('a', "local"),
            Some(exact('a', "local", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let followed_done = summary_done('b', "followed-done", 7, 2_000.0);
        let response = json!({
            "schema_version": 1,
            "operation": "followed_batch",
            "configured_hosts": 1,
            "hosts": [{
                "schema_version": 1,
                "alias": "apollo",
                "provider_ref": "apollo-provider",
                "installation_id": id('b'),
                "endpoint": "https://apollo.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-apollo",
                        "sequence": 12
                    },
                    "counts": authoritative_counts(9, 9, Some(2_000.0)),
                    "freshness": {
                        "schema_version": 1,
                        "freshness": "fresh",
                        "partial": false,
                        "refreshed_at_unix": 2_000.0,
                        "error": null
                    },
                    "entries": [
                        {
                            "schema_version": 1,
                            "requested_logical_key": followed_done.logical_key,
                            "summary": followed_done
                        },
                        {
                            "schema_version": 1,
                            "requested_logical_key": "missing-logical-key",
                            "summary": null
                        }
                    ]
                },
                "error": null
            }]
        });

        let normalized = normalize_fleet_federation_response(
            &FleetFederationNormalizeRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                response: response.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            normalized.hosts[0].unresolved_logical_keys,
            vec!["missing-logical-key".to_string()]
        );
        assert!(normalized.hosts[0].authoritative_counts.is_none());

        let counted = count_focus_and_fleet_from_federation(
            &FocusFleetFederationCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                local_summaries: vec![local],
                followed_response: Some(response),
                fleet_response: None,
            },
        )
        .unwrap();

        assert_eq!(counted.focus.counts.running, 1);
        assert_eq!(counted.focus.host_counts[0].counts.running, 0);
        assert!(!counted.focus.partial);
    }

    #[test]
    fn federation_malformed_host_degrades_without_losing_healthy_hosts() {
        let healthy = project_resolved_agent_summary(&projection_request(
            logical('b', "healthy"),
            Some(exact('b', "healthy", "run-1")),
            2,
            record_running(),
        ))
        .unwrap();
        let healthy_snapshot_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&healthy),
        );
        let response = json!({
            "schema_version": 1,
            "operation": "catalog",
            "configured_hosts": 3,
            "hosts": [
                {
                    "schema_version": 1,
                    "alias": "apollo",
                    "provider_ref": "apollo-provider",
                    "installation_id": id('b'),
                    "endpoint": "https://apollo.example.test",
                    "status": "ok",
                    "cached": false,
                    "age_seconds": null,
                    "payload": {
                        "schema_version": 1,
                        "cursor": {
                            "schema_version": 1,
                            "store_generation": "gen-apollo",
                            "sequence": 12
                        },
                        "catalog_scope": "presentation",
                        "catalog_snapshot_id": healthy_snapshot_id.clone(),
                        "counts": authoritative_counts(1, 1, Some(2_000.0)),
                        "freshness": "fresh",
                        "page": {
                            "schema_version": 1,
                            "scope": "presentation",
                            "snapshot_id": healthy_snapshot_id,
                            "rows": [healthy],
                            "limit": 50,
                            "total_matching_rows": 1,
                            "next_cursor": null,
                            "has_more": false,
                            "state": "finished"
                        }
                    },
                    "error": null
                },
                {
                    "schema_version": 1,
                    "alias": "bad",
                    "provider_ref": "bad-provider",
                    "installation_id": id('c'),
                    "endpoint": "https://bad.example.test",
                    "status": "ok",
                    "cached": false,
                    "age_seconds": null,
                    "payload": {
                        "schema_version": 1,
                        "freshness": "fresh",
                        "page": {
                            "schema_version": 1,
                            "rows": [{"schema_version": 1, "logical_key": "bad"}],
                            "limit": 50,
                            "total_matching_rows": 1,
                            "next_cursor": null,
                            "has_more": false
                        }
                    },
                    "error": null
                },
                {
                    "schema_version": 1,
                    "alias": "offline",
                    "provider_ref": "offline-provider",
                    "installation_id": id('d'),
                    "endpoint": "https://offline.example.test",
                    "status": "timeout",
                    "cached": false,
                    "age_seconds": null,
                    "payload": null,
                    "error": {
                        "schema_version": 1,
                        "code": "timeout",
                        "message": "host did not reply before deadline",
                        "target": "offline",
                        "details": null
                    }
                }
            ]
        });

        let normalized = normalize_fleet_federation_response(
            &FleetFederationNormalizeRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                response: response.clone(),
            },
        )
        .unwrap();

        assert!(normalized.partial);
        assert_eq!(normalized.summaries.len(), 1);
        assert_eq!(normalized.hosts[1].status, "invalid");
        assert!(normalized.hosts[1].partial);
        assert!(normalized.hosts[2].partial);
        assert!(normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "fleet_envelope_invalid"
                && diagnostic.alias.as_deref() == Some("bad")
        }));
        assert!(normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "timeout"
                && diagnostic.alias.as_deref() == Some("offline")
        }));

        let counted = count_focus_and_fleet_from_federation(
            &FocusFleetFederationCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                local_summaries: Vec::new(),
                followed_response: None,
                fleet_response: Some(response),
            },
        )
        .unwrap();
        assert_eq!(counted.fleet.counts.running, 1);
        assert!(counted.fleet.partial);
        assert_eq!(counted.fleet.unknown_origins, vec![id('c'), id('d')]);
    }

    #[test]
    fn federation_malformed_external_catalog_cursor_rejects_host_wire() {
        let row = project_resolved_agent_summary(&projection_request(
            logical('b', "cursor"),
            Some(exact('b', "cursor", "run-1")),
            2,
            record_running(),
        ))
        .unwrap();
        let snapshot_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&row),
        );
        let response = json!({
            "schema_version": 1,
            "operation": "catalog",
            "configured_hosts": 1,
            "hosts": [{
                "schema_version": 1,
                "alias": "apollo",
                "provider_ref": "apollo-provider",
                "installation_id": id('b'),
                "endpoint": "https://apollo.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-apollo",
                        "sequence": 12
                    },
                    "counts": authoritative_counts(1, 1, Some(2_000.0)),
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": snapshot_id,
                        "rows": [row],
                        "limit": 50,
                        "total_matching_rows": 2,
                        "next_cursor": "../other-host",
                        "has_more": true,
                        "state": "ready"
                    }
                },
                "error": null
            }]
        });

        let normalized = normalize_fleet_federation_response(
            &FleetFederationNormalizeRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                response,
            },
        )
        .unwrap();
        assert_eq!(normalized.hosts[0].status, "invalid");
        assert!(normalized.hosts[0].catalog.is_none());
        assert!(normalized.hosts[0].partial);
        assert!(normalized.summaries.is_empty());
        assert!(normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "fleet_envelope_invalid"
                && diagnostic.alias.as_deref() == Some("apollo")
        }));
    }

    #[test]
    fn federation_typed_catalog_resync_preserves_healthy_hosts() {
        let row = project_resolved_agent_summary(&projection_request(
            logical('c', "healthy"),
            Some(exact('c', "healthy", "run-1")),
            3,
            record_running(),
        ))
        .unwrap();
        let restart_snapshot_id =
            catalog_snapshot_id(FleetCatalogScopeWire::History, &[]);
        let healthy_snapshot_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&row),
        );
        let response = json!({
            "schema_version": 1,
            "operation": "catalog",
            "configured_hosts": 2,
            "hosts": [
                {
                    "schema_version": 1,
                    "alias": "history",
                    "provider_ref": "history-provider",
                    "installation_id": id('b'),
                    "endpoint": "https://history.example.test",
                    "status": "ok",
                    "cached": false,
                    "age_seconds": null,
                    "payload": {
                        "schema_version": 1,
                        "cursor": {
                            "schema_version": 1,
                            "store_generation": "gen-history",
                            "sequence": 8
                        },
                        "catalog_scope": "history",
                        "catalog_snapshot_id": restart_snapshot_id.clone(),
                        "counts": authoritative_counts(0, 0, None),
                        "freshness": "fresh",
                        "page": {
                            "schema_version": 1,
                            "scope": "history",
                            "snapshot_id": restart_snapshot_id.clone(),
                            "rows": [],
                            "limit": 50,
                            "total_matching_rows": 0,
                            "next_cursor": null,
                            "has_more": false,
                            "state": "resync_required",
                            "reset_reason": "snapshot_mismatch"
                        }
                    },
                    "error": null
                },
                {
                    "schema_version": 1,
                    "alias": "healthy",
                    "provider_ref": "healthy-provider",
                    "installation_id": id('c'),
                    "endpoint": "https://healthy.example.test",
                    "status": "ok",
                    "cached": false,
                    "age_seconds": null,
                    "payload": {
                        "schema_version": 1,
                        "cursor": {
                            "schema_version": 1,
                            "store_generation": "gen-healthy",
                            "sequence": 9
                        },
                        "catalog_scope": "presentation",
                        "catalog_snapshot_id": healthy_snapshot_id.clone(),
                        "counts": authoritative_counts(1, 1, Some(1_000.0)),
                        "freshness": "fresh",
                        "page": {
                            "schema_version": 1,
                            "scope": "presentation",
                            "snapshot_id": healthy_snapshot_id,
                            "rows": [row],
                            "limit": 50,
                            "total_matching_rows": 1,
                            "next_cursor": null,
                            "has_more": false,
                            "state": "finished"
                        }
                    },
                    "error": null
                }
            ]
        });

        let normalized = normalize_fleet_federation_response(
            &FleetFederationNormalizeRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                response,
            },
        )
        .unwrap();

        assert!(normalized.partial);
        assert_eq!(normalized.summaries.len(), 1);
        let catalog = normalized.hosts[0].catalog.as_ref().unwrap();
        assert_eq!(
            catalog.state,
            FleetCatalogContinuationStateWire::ResyncRequired
        );
        assert_eq!(
            catalog.reset_reason,
            Some(FleetCatalogResetReasonWire::SnapshotMismatch)
        );
        assert_eq!(normalized.hosts[1].status, "ok");
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
        let mut initial = tombstones;
        initial.deletion_history_complete = true;
        initial.cursor = StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: FLEET_INITIAL_CURSOR_GENERATION.to_string(),
            sequence: 0,
        };
        assert_eq!(
            classify_cursor_replay(&initial).unwrap().reason,
            CursorReplayReasonWire::InitialCursor
        );
    }

    #[test]
    fn catalog_query_is_bounded_and_pages_deterministically() {
        let alpha = project_resolved_agent_summary(&projection_request(
            logical('a', "alpha"),
            Some(exact('a', "alpha", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let mut beta_record = record_running();
        beta_record.agent_meta.as_mut().unwrap().name =
            Some("athena.beta".to_string());
        let beta = project_resolved_agent_summary(&projection_request(
            logical('a', "beta"),
            Some(exact('a', "beta", "run-1")),
            2,
            beta_record,
        ))
        .unwrap();
        let query = FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        };
        let first =
            select_fleet_catalog_page(&query, &[beta.clone(), alpha.clone()])
                .unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.total_matching_rows, 2);
        assert!(first.has_more);
        assert_eq!(first.scope, FleetCatalogScopeWire::Presentation);
        assert!(first
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.starts_with("catcur_v1:p:")));
        let second = select_fleet_catalog_page(
            &FleetCatalogQueryWire {
                cursor: first.next_cursor,
                ..query
            },
            &[alpha, beta],
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_ne!(first.rows[0].logical_key, second.rows[0].logical_key);

        assert!(validate_fleet_catalog_query(&FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: Some("../bad".to_string()),
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: false,
        })
        .is_err());
        assert!(validate_fleet_catalog_query(&FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(FLEET_READ_MAX_PAGE_ROWS + 1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: false,
        })
        .is_err());
    }

    #[test]
    fn catalog_snapshot_identity_is_scope_and_stable_content_scoped() {
        let alpha = project_resolved_agent_summary(&projection_request(
            logical('a', "alpha"),
            Some(exact('a', "alpha", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let mut alpha_observed_later = alpha.clone();
        alpha_observed_later.observed_at_unix += 60.0;
        alpha_observed_later.freshness = ObservationFreshnessWire::Stale;
        let same_content_id = catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&alpha),
        );
        assert_eq!(
            same_content_id,
            catalog_snapshot_id(
                FleetCatalogScopeWire::Presentation,
                std::slice::from_ref(&alpha_observed_later),
            )
        );
        assert_ne!(
            same_content_id,
            catalog_snapshot_id(
                FleetCatalogScopeWire::History,
                std::slice::from_ref(&alpha),
            )
        );
        let empty =
            catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[]);
        assert_eq!(
            empty,
            catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[])
        );

        let mut beta_record = record_running();
        beta_record.agent_meta.as_mut().unwrap().name =
            Some("athena.beta".to_string());
        let beta = project_resolved_agent_summary(&projection_request(
            logical('a', "beta"),
            Some(exact('a', "beta", "run-1")),
            1,
            beta_record,
        ))
        .unwrap();
        assert_ne!(
            same_content_id,
            catalog_snapshot_id(
                FleetCatalogScopeWire::Presentation,
                std::slice::from_ref(&beta),
            )
        );

        let mut revised_alpha = alpha.clone();
        revised_alpha.row_revision.revision += 1;
        assert_ne!(
            same_content_id,
            catalog_snapshot_id(
                FleetCatalogScopeWire::Presentation,
                std::slice::from_ref(&revised_alpha),
            )
        );
    }

    #[test]
    fn catalog_cursor_scope_or_snapshot_mismatch_returns_restart_page() {
        let alpha = project_resolved_agent_summary(&projection_request(
            logical('a', "alpha"),
            Some(exact('a', "alpha", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let beta = project_resolved_agent_summary(&projection_request(
            logical('a', "beta"),
            Some(exact('a', "beta", "run-1")),
            2,
            record_running(),
        ))
        .unwrap();
        let gamma = project_resolved_agent_summary(&projection_request(
            logical('a', "gamma"),
            Some(exact('a', "gamma", "run-1")),
            3,
            record_running(),
        ))
        .unwrap();
        let query = FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        };
        let first =
            select_fleet_catalog_page(&query, &[alpha.clone(), beta]).unwrap();
        let stale = select_fleet_catalog_page(
            &FleetCatalogQueryWire {
                cursor: first.next_cursor.clone(),
                ..query.clone()
            },
            &[alpha.clone(), gamma.clone()],
        )
        .unwrap();
        assert!(stale.rows.is_empty());
        assert_eq!(
            stale.state,
            FleetCatalogContinuationStateWire::ResyncRequired
        );
        assert_eq!(
            stale.reset_reason,
            Some(FleetCatalogResetReasonWire::SnapshotMismatch)
        );
        assert!(stale.next_cursor.is_none());

        let cross_scope = select_fleet_catalog_page(
            &FleetCatalogQueryWire {
                scope: FleetCatalogScopeWire::History,
                cursor: first.next_cursor,
                ..query
            },
            &[alpha, gamma],
        )
        .unwrap();
        assert_eq!(
            cross_scope.reset_reason,
            Some(FleetCatalogResetReasonWire::ScopeMismatch)
        );
        assert!(validate_fleet_catalog_cursor("off:1").is_err());
    }

    #[test]
    fn catalog_accumulation_uses_generations_and_snapshot_equality() {
        let alpha = project_resolved_agent_summary(&projection_request(
            logical('a', "alpha"),
            Some(exact('a', "alpha", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let beta = project_resolved_agent_summary(&projection_request(
            logical('a', "beta"),
            Some(exact('a', "beta", "run-1")),
            2,
            record_running(),
        ))
        .unwrap();
        let rows = vec![alpha.clone(), beta.clone()];
        let query = FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        };
        let first_page = select_fleet_catalog_page(&query, &rows).unwrap();
        let first_cursor = first_page.next_cursor.clone();
        let first = accumulate_fleet_catalog_page(
            &FleetCatalogAccumulationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                current: None,
                request_generation: 2,
                requested_scope: FleetCatalogScopeWire::Presentation,
                requested_snapshot_id: None,
                requested_cursor: None,
                incoming: catalog_response(first_page, &rows),
            },
        )
        .unwrap();
        assert_eq!(first.action, FleetCatalogAccumulationActionWire::Replaced);
        assert_eq!(first.state.rows.len(), 1);

        let older = accumulate_fleet_catalog_page(
            &FleetCatalogAccumulationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                current: Some(first.state.clone()),
                request_generation: 1,
                requested_scope: FleetCatalogScopeWire::Presentation,
                requested_snapshot_id: first.state.snapshot_id.clone(),
                requested_cursor: None,
                incoming: catalog_response(
                    select_fleet_catalog_page(&query, &[]).unwrap(),
                    &[],
                ),
            },
        )
        .unwrap();
        assert_eq!(
            older.action,
            FleetCatalogAccumulationActionWire::IgnoredOlderRequest
        );
        assert_eq!(older.state.rows, first.state.rows);

        let second_page = select_fleet_catalog_page(
            &FleetCatalogQueryWire {
                cursor: first_cursor.clone(),
                ..query.clone()
            },
            &rows,
        )
        .unwrap();
        let merged = accumulate_fleet_catalog_page(
            &FleetCatalogAccumulationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                current: Some(first.state.clone()),
                request_generation: 2,
                requested_scope: FleetCatalogScopeWire::Presentation,
                requested_snapshot_id: first.state.snapshot_id.clone(),
                requested_cursor: first_cursor,
                incoming: catalog_response(second_page, &rows),
            },
        )
        .unwrap();
        assert_eq!(merged.action, FleetCatalogAccumulationActionWire::Merged);
        assert_eq!(merged.state.rows.len(), 2);

        let mut revised_alpha = alpha.clone();
        revised_alpha.row_revision.revision += 10;
        let duplicate_page = FleetCatalogPageSelectionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: merged.state.snapshot_id.clone().unwrap(),
            rows: vec![revised_alpha.clone()],
            limit: 1,
            total_matching_rows: 2,
            next_cursor: None,
            has_more: false,
            state: FleetCatalogContinuationStateWire::Finished,
            reset_reason: None,
        };
        let revised = accumulate_fleet_catalog_page(
            &FleetCatalogAccumulationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                current: Some(merged.state.clone()),
                request_generation: 2,
                requested_scope: FleetCatalogScopeWire::Presentation,
                requested_snapshot_id: merged.state.snapshot_id.clone(),
                requested_cursor: Some(catalog_cursor(
                    FleetCatalogScopeWire::Presentation,
                    merged.state.snapshot_id.as_deref().unwrap(),
                    1,
                )),
                incoming: catalog_response(duplicate_page, &rows),
            },
        )
        .unwrap();
        assert_eq!(
            revised
                .state
                .rows
                .iter()
                .find(|row| row.logical_key == revised_alpha.logical_key)
                .unwrap()
                .row_revision
                .revision,
            revised_alpha.row_revision.revision
        );

        let replacement = accumulate_fleet_catalog_page(
            &FleetCatalogAccumulationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                current: Some(revised.state),
                request_generation: 3,
                requested_scope: FleetCatalogScopeWire::Presentation,
                requested_snapshot_id: None,
                requested_cursor: None,
                incoming: catalog_response(
                    select_fleet_catalog_page(&query, &[]).unwrap(),
                    &[],
                ),
            },
        )
        .unwrap();
        assert_eq!(
            replacement.action,
            FleetCatalogAccumulationActionWire::Replaced
        );
        assert!(replacement.state.rows.is_empty());
        assert_eq!(replacement.state.total_matching_rows, 0);
        assert!(replacement.state.next_cursor.is_none());
    }

    #[test]
    fn batch_lookup_preserves_requested_order_and_bounds_ids() {
        let alpha = project_resolved_agent_summary(&projection_request(
            logical('a', "alpha"),
            Some(exact('a', "alpha", "run-1")),
            1,
            record_running(),
        ))
        .unwrap();
        let beta = project_resolved_agent_summary(&projection_request(
            logical('a', "beta"),
            Some(exact('a', "beta", "run-1")),
            2,
            record_running(),
        ))
        .unwrap();
        let request = FleetLogicalBatchRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_keys: vec![
                beta.logical_key.clone(),
                "missing-logical-key".to_string(),
                alpha.logical_key.clone(),
            ],
        };
        let entries = select_fleet_logical_batch(
            &request,
            &[alpha.clone(), beta.clone()],
        )
        .unwrap();
        assert_eq!(
            entries[0].summary.as_ref().unwrap().logical_key,
            beta.logical_key
        );
        assert!(entries[1].summary.is_none());
        assert_eq!(
            entries[2].summary.as_ref().unwrap().logical_key,
            alpha.logical_key
        );

        let mut too_many = request;
        too_many.logical_keys =
            vec!["k".to_string(); FLEET_READ_MAX_BATCH_IDS + 1];
        assert!(validate_fleet_logical_batch_request(&too_many).is_err());
    }

    #[test]
    fn content_and_project_read_requests_are_bounded() {
        let request = FleetContentReadRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            handle_id: "handle-1".to_string(),
            row_revision: revision(&logical('a', "alpha"), 1),
            offset: 0,
            limit: Some(FLEET_READ_DEFAULT_CONTENT_BYTES),
        };
        assert_eq!(
            fleet_content_read_limit(&request).unwrap(),
            FLEET_READ_DEFAULT_CONTENT_BYTES
        );
        assert!(validate_fleet_content_read_request(
            &FleetContentReadRequestWire {
                handle_id: "../secret".to_string(),
                ..request.clone()
            }
        )
        .is_err());
        assert!(validate_fleet_content_read_request(
            &FleetContentReadRequestWire {
                limit: Some(FLEET_READ_MAX_CONTENT_BYTES + 1),
                ..request
            }
        )
        .is_err());

        assert!(validate_fleet_project_eligibility_request(
            &FleetProjectEligibilityRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                project_ids: vec![
                    "project".to_string();
                    FLEET_READ_MAX_PROJECT_IDS + 1
                ],
                limit: None,
            }
        )
        .is_err());
    }

    #[test]
    fn invalidations_validate_cursor_and_revision_identity() {
        let locator = logical('a', "alpha");
        let logical_key = logical_key_unchecked(&locator);
        let event = FleetInvalidationEventWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: StoreCursorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                store_generation: "gen-1".to_string(),
                sequence: 1,
            },
            kind: FleetInvalidationKindWire::RevisionChanged,
            logical_key: Some(logical_key.clone()),
            row_revision: Some(ResourceRevisionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                logical_key,
                revision: 1,
            }),
            reason: "revision_changed".to_string(),
        };
        assert!(validate_fleet_invalidation_event(&event).is_ok());
        assert!(validate_fleet_invalidation_event(
            &FleetInvalidationEventWire {
                reason: "../path".to_string(),
                ..event
            }
        )
        .is_err());
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
    fn fleet_launch_intent_and_replay_are_portable_and_target_pinned() {
        let intent = FleetLaunchIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            prompt: "do the remote work".to_string(),
            request_id: Some("request-1".to_string()),
            display_name: Some("Remote work".to_string()),
            name: Some("worker".to_string()),
            model: Some("gpt-5".to_string()),
            provider: Some("openai".to_string()),
            runtime: None,
            project: FleetLaunchProjectContextWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                provider_ref: Some("provider-a".to_string()),
                project_id: "project-1".to_string(),
                revision: Some("a".repeat(40)),
                patch_ref: None,
            },
            dry_run: Some(false),
            follow: true,
            references: vec![FleetLaunchReferenceWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                kind: FleetLaunchReferenceKindWire::Artifact,
                reference: "artifact:abc123".to_string(),
                sha256: Some("a".repeat(64)),
            }],
        };
        let fingerprint = fleet_launch_payload_fingerprint(&intent).unwrap();
        let request = FleetLaunchRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: operation_key("dispatch-1"),
            target_installation_id: id('a'),
            intent: intent.clone(),
            payload_fingerprint: fingerprint.clone(),
            acceptance_window_seconds: 30.0,
        };
        assert_eq!(validate_fleet_launch_request(&request).unwrap(), request);

        let decision_request = FleetLaunchDecisionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: operation_key("dispatch-1"),
            payload_fingerprint: fingerprint.clone(),
            target_installation_id: id('a'),
            now_unix: 10.0,
            acceptance_window_seconds: 30.0,
            existing_record: None,
        };
        let accepted = decide_fleet_launch_replay(&decision_request).unwrap();
        assert_eq!(accepted.decision, OperationDecisionKindWire::AcceptNew);
        let receipt = accepted.receipt.unwrap();
        assert_eq!(receipt.target_installation_id, id('a'));

        let replay =
            decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
                existing_record: Some(DurableFleetLaunchRecordWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    receipt: receipt.clone(),
                    tombstoned_at_unix_ms: None,
                }),
                now_unix: 11.0,
                ..decision_request.clone()
            })
            .unwrap();
        assert_eq!(
            replay.decision,
            OperationDecisionKindWire::ReturnOriginalReceipt
        );

        let conflict =
            decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
                payload_fingerprint: PayloadFingerprintWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    sha256: "b".repeat(64),
                },
                existing_record: Some(DurableFleetLaunchRecordWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    receipt: receipt.clone(),
                    tombstoned_at_unix_ms: None,
                }),
                now_unix: 11.0,
                ..decision_request.clone()
            })
            .unwrap();
        assert_eq!(conflict.decision, OperationDecisionKindWire::Conflict);

        let mismatch =
            decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
                target_installation_id: id('b'),
                existing_record: Some(DurableFleetLaunchRecordWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    receipt: receipt.clone(),
                    tombstoned_at_unix_ms: None,
                }),
                now_unix: 11.0,
                ..decision_request
            })
            .unwrap();
        assert_eq!(
            mismatch.decision,
            OperationDecisionKindWire::PreconditionMismatch
        );

        let mut path_context = intent;
        path_context.project.revision =
            Some("/tmp/source-checkout".to_string());
        assert!(validate_fleet_launch_intent(&path_context).is_err());

        let bad_receipt = FleetLaunchReceiptWire {
            logical_locator: Some(logical('b', "worker")),
            ..receipt
        };
        assert!(bad_receipt.validate().is_err());
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
