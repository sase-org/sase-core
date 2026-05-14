use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use sase_core::projections::{ShadowDiffCountsWire, ShadowDiffRecordWire};

pub use sase_core::projections::{
    AgentArchiveReadResponseWire, AgentReadDetailRequestWire,
    AgentReadDetailResponseWire, AgentReadListRequestWire,
    AgentReadListResponseWire, BeadReadDetailRequestWire,
    BeadReadDetailResponseWire, BeadReadListRequestWire,
    BeadReadListResponseWire, BeadReadStatsResponseWire,
    CatalogReadListRequestWire, CatalogReadResponseWire,
    ChangeSpecReadDetailRequestWire, ChangeSpecReadDetailResponseWire,
    ChangeSpecReadListRequestWire, ChangeSpecReadListResponseWire,
    LocalDaemonMutationOutcomeWire, MutationActorWire, MutationConflictWire,
    NotificationPendingActionsReadResponseWire,
    NotificationProjectionFacetCountsWire, NotificationReadDetailRequestWire,
    NotificationReadDetailResponseWire, NotificationReadListRequestWire,
    NotificationReadListResponseWire, ProjectionPageRequestWire,
    SourceExportPlanWire, SourceFingerprintWire,
    PROJECTION_READ_WIRE_SCHEMA_VERSION,
};

pub use sase_core::host_bridge::{
    MobileBeadDetailWire, MobileBeadListRequestWire,
    MobileBeadListResponseWire, MobileBeadShowRequestWire,
    MobileBeadShowResponseWire, MobileBeadSummaryWire,
    MobileChangeSpecTagEntryWire, MobileChangeSpecTagListRequestWire,
    MobileChangeSpecTagListResponseWire, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperSkippedWire, MobileHelperStatusWire, MobileUpdateJobStatusWire,
    MobileUpdateJobWire, MobileUpdateStartRequestWire,
    MobileUpdateStartResponseWire, MobileUpdateStatusRequestWire,
    MobileUpdateStatusResponseWire, MobileXpromptCatalogAttachmentWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogRequestWire,
    MobileXpromptCatalogResponseWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire,
};

pub const GATEWAY_WIRE_SCHEMA_VERSION: u32 = 1;
pub const LOCAL_DAEMON_WIRE_SCHEMA_VERSION: u32 = 1;
pub const LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION: u32 = 1;
pub const LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION: u32 = 1;
pub const LOCAL_DAEMON_MAX_PAYLOAD_BYTES: u32 = 1_048_576;
pub const LOCAL_DAEMON_DEFAULT_PAGE_LIMIT: u32 = 100;
pub const LOCAL_DAEMON_MAX_PAGE_LIMIT: u32 = 500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBuildWire {
    pub package_version: String,
    pub git_sha: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBindWire {
    pub address: String,
    pub is_loopback: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponseWire {
    pub schema_version: u32,
    pub status: String,
    pub service: String,
    pub version: String,
    pub build: GatewayBuildWire,
    pub bind: GatewayBindWire,
    pub push: PushGatewayStatusWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushGatewayStatusWire {
    pub provider: String,
    pub enabled: bool,
    pub attempted: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub last_failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecordWire {
    pub schema_version: u32,
    pub device_id: String,
    pub display_name: String,
    pub platform: String,
    pub app_version: Option<String>,
    pub paired_at: Option<String>,
    pub last_seen_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionResponseWire {
    pub schema_version: u32,
    pub device: DeviceRecordWire,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushProviderWire {
    Fcm,
    UnifiedPush,
    Ntfy,
    Test,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushHintCategoryWire {
    Notifications,
    Agents,
    Helpers,
    Update,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscriptionRequestWire {
    pub schema_version: u32,
    pub provider: PushProviderWire,
    pub provider_token: String,
    pub app_instance_id: Option<String>,
    pub device_display_name: Option<String>,
    pub platform: Option<String>,
    pub app_version: Option<String>,
    pub hint_categories: Vec<PushHintCategoryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscriptionRecordWire {
    pub schema_version: u32,
    pub id: String,
    pub device_id: String,
    pub provider: PushProviderWire,
    pub provider_token: String,
    pub app_instance_id: Option<String>,
    pub device_display_name: Option<String>,
    pub platform: Option<String>,
    pub app_version: Option<String>,
    pub hint_categories: Vec<PushHintCategoryWire>,
    pub enabled_at: String,
    pub disabled_at: Option<String>,
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscriptionListResponseWire {
    pub schema_version: u32,
    pub subscriptions: Vec<PushSubscriptionRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscriptionRegisterResponseWire {
    pub schema_version: u32,
    pub subscription: PushSubscriptionRecordWire,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscriptionDeleteResponseWire {
    pub schema_version: u32,
    pub subscription: PushSubscriptionRecordWire,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushHintWire {
    pub schema_version: u32,
    pub id: String,
    pub created_at: String,
    pub category: PushHintCategoryWire,
    pub reason: String,
    pub notification_id: Option<String>,
    pub agent_name: Option<String>,
    pub helper: Option<String>,
    pub job_id: Option<String>,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairStartRequestWire {
    pub schema_version: u32,
    pub host_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairStartResponseWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    pub expires_at: String,
    pub host_label: String,
    pub host_fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingDeviceMetadataWire {
    pub display_name: String,
    pub platform: String,
    pub app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairFinishRequestWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    pub device: PairingDeviceMetadataWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairFinishResponseWire {
    pub schema_version: u32,
    pub device: DeviceRecordWire,
    pub token_type: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCodeWire {
    Unauthorized,
    NotFound,
    InvalidRequest,
    PairingExpired,
    PairingRejected,
    ConflictAlreadyHandled,
    GoneStale,
    AmbiguousPrefix,
    UnsupportedAction,
    AttachmentExpired,
    AgentNotFound,
    AgentNotRunning,
    LaunchFailed,
    InvalidUpload,
    BridgeUnavailable,
    HelperNotFound,
    UpdateAlreadyRunning,
    UpdateJobNotFound,
    PermissionDenied,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiErrorWire {
    pub schema_version: u32,
    pub code: ApiErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
    pub details: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonClientWire {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonRequestEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub client: LocalDaemonClientWire,
    pub payload: LocalDaemonRequestPayloadWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonResponseEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub snapshot_id: Option<String>,
    pub payload: LocalDaemonResponsePayloadWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum LocalDaemonRequestPayloadWire {
    Health {
        include_capabilities: bool,
    },
    Capabilities,
    List(LocalDaemonListRequestWire),
    Read(LocalDaemonReadRequestWire),
    Write(LocalDaemonWriteRequestWire),
    Events(LocalDaemonEventRequestWire),
    Rebuild(LocalDaemonRebuildRequestWire),
    IndexingStatus(LocalDaemonIndexingStatusRequestWire),
    Verify(LocalDaemonIndexingVerifyRequestWire),
    Diff(LocalDaemonIndexingDiffRequestWire),
    Batch {
        requests: Vec<LocalDaemonBatchRequestWire>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum LocalDaemonResponsePayloadWire {
    Health(LocalDaemonHealthResponseWire),
    Capabilities(LocalDaemonCapabilitiesResponseWire),
    List(LocalDaemonListResponseWire),
    Read(Box<LocalDaemonReadResponseWire>),
    Write(LocalDaemonWriteResponseWire),
    Events(LocalDaemonEventBatchWire),
    Rebuild(LocalDaemonRebuildResponseWire),
    IndexingStatus(LocalDaemonIndexingStatusResponseWire),
    Verify(LocalDaemonIndexingVerifyResponseWire),
    Diff(LocalDaemonIndexingDiffResponseWire),
    Batch {
        responses: Vec<LocalDaemonBatchResponseWire>,
    },
    Error(LocalDaemonErrorWire),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonBatchRequestWire {
    pub request_id: String,
    pub payload: LocalDaemonRequestPayloadWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonBatchResponseWire {
    pub request_id: String,
    pub snapshot_id: Option<String>,
    pub payload: LocalDaemonResponsePayloadWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonWriteRequestWire {
    pub schema_version: u32,
    pub surface: String,
    pub project_id: String,
    pub idempotency_key: String,
    pub actor: MutationActorWire,
    #[serde(default)]
    pub payload: JsonValue,
    #[serde(default)]
    pub expected_source_fingerprints: Vec<SourceFingerprintWire>,
    #[serde(default)]
    pub source_exports: Vec<SourceExportPlanWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonWriteResponseWire {
    pub schema_version: u32,
    pub surface: String,
    pub outcome: LocalDaemonMutationOutcomeWire,
    pub fallback: LocalDaemonFallbackWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "surface", content = "data")]
pub enum LocalDaemonReadRequestWire {
    ChangespecList(ChangeSpecReadListRequestWire),
    ChangespecSearch(ChangeSpecReadListRequestWire),
    ChangespecDetail(ChangeSpecReadDetailRequestWire),
    AgentActive(AgentReadListRequestWire),
    AgentRecent(AgentReadListRequestWire),
    AgentArchive(AgentReadListRequestWire),
    AgentSearch(AgentReadListRequestWire),
    AgentDetail(AgentReadDetailRequestWire),
    NotificationList(NotificationReadListRequestWire),
    NotificationDetail(NotificationReadDetailRequestWire),
    NotificationCounts,
    NotificationPendingActions,
    BeadList(BeadReadListRequestWire),
    BeadReady(BeadReadListRequestWire),
    BeadBlocked(BeadReadListRequestWire),
    BeadShow(BeadReadDetailRequestWire),
    BeadStats(BeadReadListRequestWire),
    XpromptCatalog(CatalogReadListRequestWire),
    EditorCatalog(CatalogReadListRequestWire),
    SnippetCatalog(CatalogReadListRequestWire),
    FileHistory(CatalogReadListRequestWire),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "surface", content = "data")]
pub enum LocalDaemonReadResponseWire {
    ChangespecList(ChangeSpecReadListResponseWire),
    ChangespecSearch(ChangeSpecReadListResponseWire),
    ChangespecDetail(Box<ChangeSpecReadDetailResponseWire>),
    AgentActive(AgentReadListResponseWire),
    AgentRecent(AgentReadListResponseWire),
    AgentArchive(AgentArchiveReadResponseWire),
    AgentSearch(AgentReadListResponseWire),
    AgentDetail(AgentReadDetailResponseWire),
    NotificationList(NotificationReadListResponseWire),
    NotificationDetail(NotificationReadDetailResponseWire),
    NotificationCounts(NotificationProjectionFacetCountsWire),
    NotificationPendingActions(NotificationPendingActionsReadResponseWire),
    BeadList(BeadReadListResponseWire),
    BeadReady(BeadReadListResponseWire),
    BeadBlocked(BeadReadListResponseWire),
    BeadShow(BeadReadDetailResponseWire),
    BeadStats(BeadReadStatsResponseWire),
    XpromptCatalog(CatalogReadResponseWire),
    EditorCatalog(CatalogReadResponseWire),
    SnippetCatalog(CatalogReadResponseWire),
    FileHistory(CatalogReadResponseWire),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonRebuildRequestWire {
    #[serde(default)]
    pub storage_reset_only: bool,
    #[serde(default)]
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonRebuildResponseWire {
    pub schema_version: u32,
    pub mode: String,
    pub storage_reset_only: bool,
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub limitation: Option<String>,
    pub report: JsonValue,
    #[serde(default)]
    pub source_exports: JsonValue,
    #[serde(default)]
    pub summaries: Vec<LocalDaemonIndexingSurfaceSummaryWire>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonIndexingSurfaceWire {
    #[default]
    All,
    Changespecs,
    Notifications,
    Agents,
    Beads,
    Catalogs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingStatusRequestWire {
    #[serde(default)]
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingVerifyRequestWire {
    #[serde(default)]
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingDiffRequestWire {
    #[serde(default)]
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub page: LocalDaemonPageRequestWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingStatusResponseWire {
    pub schema_version: u32,
    pub service: JsonValue,
    pub summaries: Vec<LocalDaemonIndexingSurfaceSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingVerifyResponseWire {
    pub schema_version: u32,
    pub ok: bool,
    pub summaries: Vec<LocalDaemonIndexingSurfaceSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingDiffResponseWire {
    pub schema_version: u32,
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub records: Vec<ShadowDiffRecordWire>,
    pub counts: ShadowDiffCountsWire,
    pub next_cursor: Option<String>,
    pub bounded: LocalDaemonPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonIndexingSurfaceSummaryWire {
    pub schema_version: u32,
    pub surface: LocalDaemonIndexingSurfaceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_indexed_event_seq: Option<i64>,
    pub queued_changes: u64,
    pub watcher_active: bool,
    pub diff_counts: ShadowDiffCountsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonHealthResponseWire {
    pub schema_version: u32,
    pub status: LocalDaemonHealthStatusWire,
    pub service: String,
    pub daemon_started: bool,
    pub version: String,
    pub min_client_schema_version: u32,
    pub max_client_schema_version: u32,
    pub fallback: LocalDaemonFallbackWire,
    pub details: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonHealthStatusWire {
    Ok,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonFallbackWire {
    pub available: bool,
    pub reason: Option<LocalDaemonFallbackReasonWire>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonFallbackReasonWire {
    DaemonNotRunning,
    UnsupportedClientVersion,
    RecoveryMode,
    HostAdapterRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonCapabilitiesResponseWire {
    pub schema_version: u32,
    pub contract: String,
    pub contract_version: u32,
    pub min_client_schema_version: u32,
    pub max_client_schema_version: u32,
    pub capabilities: Vec<String>,
    pub max_payload_bytes: u32,
    pub default_page_limit: u32,
    pub max_page_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonListRequestWire {
    pub collection: LocalDaemonCollectionWire,
    pub page: LocalDaemonPageRequestWire,
    pub snapshot_id: Option<String>,
    pub stable_handle: Option<String>,
    pub max_payload_bytes: Option<u32>,
    pub filters: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonCollectionWire {
    Agents,
    Artifacts,
    Beads,
    Changespecs,
    Notifications,
    Workflows,
    Xprompts,
    Indexing,
    Mocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonPageRequestWire {
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonListResponseWire {
    pub schema_version: u32,
    pub collection: LocalDaemonCollectionWire,
    pub snapshot_id: String,
    pub items: Vec<LocalDaemonListItemWire>,
    pub next_cursor: Option<String>,
    pub stable_handle: Option<String>,
    pub bounded: LocalDaemonPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonListItemWire {
    pub handle: String,
    pub schema_version: u32,
    pub summary: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonPayloadBoundWire {
    pub max_payload_bytes: u32,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonEventRequestWire {
    #[serde(default, alias = "after_event_id")]
    pub since_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collections: Vec<LocalDaemonCollectionWire>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub max_events: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonEventBatchWire {
    pub schema_version: u32,
    pub snapshot_id: String,
    pub events: Vec<LocalDaemonEventRecordWire>,
    pub heartbeat: Option<LocalDaemonHeartbeatWire>,
    pub next_event_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonEventRecordWire {
    pub schema_version: u32,
    pub event_id: String,
    pub snapshot_id: String,
    pub created_at: String,
    pub source: LocalDaemonEventSourceWire,
    pub payload: LocalDaemonEventPayloadWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonEventSourceWire {
    Daemon,
    Filesystem,
    HostAdapter,
    Mock,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum LocalDaemonEventPayloadWire {
    Heartbeat {
        sequence: u64,
    },
    Delta {
        collection: LocalDaemonCollectionWire,
        handle: String,
        operation: LocalDaemonDeltaOperationWire,
        fields: JsonValue,
    },
    ResyncRequired {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonDeltaOperationWire {
    Upsert,
    Delete,
    Invalidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDaemonHeartbeatWire {
    pub schema_version: u32,
    pub sequence: u64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonErrorWire {
    pub schema_version: u32,
    pub code: LocalDaemonErrorCodeWire,
    pub message: String,
    pub retryable: bool,
    pub target: Option<String>,
    pub details: Option<JsonValue>,
    pub fallback: LocalDaemonFallbackWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDaemonErrorCodeWire {
    InvalidRequest,
    UnsupportedClientVersion,
    UnsupportedCapability,
    ProjectionDegraded,
    CursorExpired,
    SnapshotExpired,
    PayloadTooLarge,
    ResourceNotFound,
    HostAdapterRequired,
    ConflictStaleSource,
    ExportPendingRepair,
    IdempotencyConflict,
    UnsupportedMutation,
    SourceLockBusy,
    DaemonUnavailable,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum EventPayloadWire {
    Heartbeat {
        sequence: u64,
    },
    Session {
        device_id: String,
    },
    ResyncRequired {
        reason: String,
    },
    NotificationsChanged {
        reason: String,
        notification_id: Option<String>,
    },
    AgentsChanged {
        reason: String,
        agent_name: Option<String>,
        timestamp: Option<String>,
    },
    HelpersChanged {
        reason: String,
        helper: Option<String>,
        job_id: Option<String>,
        timestamp: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecordWire {
    pub schema_version: u32,
    pub id: String,
    pub created_at: String,
    pub payload: EventPayloadWire,
}

pub fn push_hint_from_event(record: &EventRecordWire) -> Option<PushHintWire> {
    let (
        category,
        reason,
        notification_id,
        agent_name,
        helper,
        job_id,
        title,
        body,
    ) = match &record.payload {
        EventPayloadWire::Heartbeat { .. } => return None,
        EventPayloadWire::Session { .. } => (
            PushHintCategoryWire::Session,
            "session".to_string(),
            None,
            None,
            None,
            None,
            "SASE session updated".to_string(),
            "Open SASE to refresh.".to_string(),
        ),
        EventPayloadWire::ResyncRequired { reason } => (
            PushHintCategoryWire::Session,
            reason.clone(),
            None,
            None,
            None,
            None,
            "SASE refresh needed".to_string(),
            "Open SASE to refresh current state.".to_string(),
        ),
        EventPayloadWire::NotificationsChanged {
            reason,
            notification_id,
        } => (
            PushHintCategoryWire::Notifications,
            reason.clone(),
            notification_id.clone(),
            None,
            None,
            None,
            "SASE notification update".to_string(),
            "Open SASE to view current notifications.".to_string(),
        ),
        EventPayloadWire::AgentsChanged {
            reason, agent_name, ..
        } => {
            let safe_agent_name =
                agent_name.as_deref().and_then(safe_push_identifier);
            let body = safe_agent_name
                .as_ref()
                .map(|name| format!("Agent {name} changed."))
                .unwrap_or_else(|| "Agent state changed.".to_string());
            (
                PushHintCategoryWire::Agents,
                reason.clone(),
                None,
                safe_agent_name,
                None,
                None,
                "SASE agent update".to_string(),
                body,
            )
        }
        EventPayloadWire::HelpersChanged {
            reason,
            helper,
            job_id,
            ..
        } => {
            let category = if helper.as_deref() == Some("update")
                || job_id.as_deref().is_some_and(|id| id.starts_with("update"))
            {
                PushHintCategoryWire::Update
            } else {
                PushHintCategoryWire::Helpers
            };
            (
                category,
                reason.clone(),
                None,
                None,
                helper.as_deref().and_then(safe_push_identifier),
                job_id.as_deref().and_then(safe_push_identifier),
                "SASE helper update".to_string(),
                "Open SASE to refresh helper state.".to_string(),
            )
        }
    };
    Some(PushHintWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        id: record.id.clone(),
        created_at: record.created_at.clone(),
        category,
        reason,
        notification_id,
        agent_name,
        helper,
        job_id,
        title,
        body,
    })
}

fn safe_push_identifier(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return None;
    }
    if trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationStateMutationResponseWire {
    pub schema_version: u32,
    pub notification_id: String,
    pub read: bool,
    pub dismissed: bool,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentListRequestWire {
    pub schema_version: u32,
    pub include_recent: bool,
    pub status: Option<String>,
    pub project: Option<String>,
    pub device_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentListResponseWire {
    pub schema_version: u32,
    pub agents: Vec<MobileAgentSummaryWire>,
    pub total_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentSummaryWire {
    pub name: String,
    pub project: Option<String>,
    pub status: String,
    pub pid: Option<u32>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub workspace_number: Option<u32>,
    pub started_at: Option<String>,
    pub duration_seconds: Option<u64>,
    pub prompt_snippet: Option<String>,
    pub has_artifact_dir: bool,
    pub retry_lineage: MobileAgentRetryLineageWire,
    pub actions: MobileAgentActionAffordancesWire,
    pub display: MobileAgentDisplayLabelsWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryLineageWire {
    pub retry_of_timestamp: Option<String>,
    pub retried_as_timestamp: Option<String>,
    pub retry_chain_root_timestamp: Option<String>,
    pub retry_attempt: Option<u32>,
    pub parent_agent_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentActionAffordancesWire {
    pub can_resume: bool,
    pub can_wait: bool,
    pub can_kill: bool,
    pub can_retry: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentDisplayLabelsWire {
    pub title: String,
    pub subtitle: Option<String>,
    pub status_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentResumeOptionsResponseWire {
    pub schema_version: u32,
    pub options: Vec<MobileAgentResumeOptionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentResumeOptionWire {
    pub id: String,
    pub agent_name: String,
    pub kind: MobileAgentResumeOptionKindWire,
    pub label: String,
    pub prompt_text: String,
    pub direct_launch_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileAgentResumeOptionKindWire {
    Resume,
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentTextLaunchRequestWire {
    pub schema_version: u32,
    pub prompt: String,
    pub request_id: Option<String>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: Option<String>,
    pub device_id: Option<String>,
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentImageLaunchRequestWire {
    pub schema_version: u32,
    pub prompt: String,
    pub request_id: Option<String>,
    pub original_filename: String,
    pub content_type: String,
    pub byte_length: u64,
    pub base64_image: String,
    pub device_id: Option<String>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: Option<String>,
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentLaunchResultWire {
    pub schema_version: u32,
    pub primary: Option<MobileAgentLaunchSlotResultWire>,
    pub slots: Vec<MobileAgentLaunchSlotResultWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentLaunchSlotResultWire {
    pub slot_id: String,
    pub name: Option<String>,
    pub status: MobileAgentLaunchSlotStatusWire,
    pub artifact_dir: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileAgentLaunchSlotStatusWire {
    Launched,
    DryRun,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentKillRequestWire {
    pub schema_version: u32,
    pub reason: Option<String>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentKillResultWire {
    pub schema_version: u32,
    pub name: String,
    pub status: String,
    pub pid: Option<u32>,
    pub changed: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryRequestWire {
    pub schema_version: u32,
    pub request_id: Option<String>,
    pub prompt_override: Option<String>,
    pub dry_run: Option<bool>,
    pub kill_source_first: Option<bool>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryResultWire {
    pub schema_version: u32,
    pub source_agent: String,
    pub launch: MobileAgentLaunchResultWire,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample_device() -> DeviceRecordWire {
        DeviceRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device_id: "device_123".to_string(),
            display_name: "Pixel 9".to_string(),
            platform: "android".to_string(),
            app_version: Some("0.1.0".to_string()),
            paired_at: Some("2026-05-06T14:30:00Z".to_string()),
            last_seen_at: None,
            revoked_at: None,
        }
    }

    #[test]
    fn health_wire_json_snapshot() {
        let wire = HealthResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            status: "ok".to_string(),
            service: "sase_gateway".to_string(),
            version: "0.1.1".to_string(),
            build: GatewayBuildWire {
                package_version: "0.1.1".to_string(),
                git_sha: None,
            },
            bind: GatewayBindWire {
                address: "127.0.0.1:7629".to_string(),
                is_loopback: true,
            },
            push: PushGatewayStatusWire {
                provider: "disabled".to_string(),
                enabled: false,
                attempted: 0,
                succeeded: 0,
                failed: 0,
                last_attempt_at: None,
                last_success_at: None,
                last_failure_at: None,
                last_failure: None,
            },
        };

        assert_eq!(
            serde_json::to_value(wire).unwrap(),
            json!({
                "schema_version": 1,
                "status": "ok",
                "service": "sase_gateway",
                "version": "0.1.1",
                "build": {
                    "package_version": "0.1.1",
                    "git_sha": null
                },
                "bind": {
                    "address": "127.0.0.1:7629",
                    "is_loopback": true
                },
                "push": {
                    "provider": "disabled",
                    "enabled": false,
                    "attempted": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "last_attempt_at": null,
                    "last_success_at": null,
                    "last_failure_at": null,
                    "last_failure": null
                }
            })
        );
    }

    #[test]
    fn pairing_wire_json_snapshot() {
        let start = PairStartResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            pairing_id: "pair_123".to_string(),
            code: "123456".to_string(),
            expires_at: "2026-05-06T14:35:00Z".to_string(),
            host_label: "workstation".to_string(),
            host_fingerprint: None,
        };
        assert_eq!(
            serde_json::to_value(start).unwrap(),
            json!({
                "schema_version": 1,
                "pairing_id": "pair_123",
                "code": "123456",
                "expires_at": "2026-05-06T14:35:00Z",
                "host_label": "workstation",
                "host_fingerprint": null
            })
        );

        let finish = PairFinishResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device: sample_device(),
            token_type: "bearer".to_string(),
            token: "sase_device_token".to_string(),
        };
        assert_eq!(
            serde_json::to_value(finish).unwrap(),
            json!({
                "schema_version": 1,
                "device": {
                    "schema_version": 1,
                    "device_id": "device_123",
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0",
                    "paired_at": "2026-05-06T14:30:00Z",
                    "last_seen_at": null,
                    "revoked_at": null
                },
                "token_type": "bearer",
                "token": "sase_device_token"
            })
        );
    }

    #[test]
    fn session_wire_json_snapshot() {
        let session = SessionResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device: sample_device(),
            capabilities: vec![
                "session.read".to_string(),
                "events.read".to_string(),
            ],
        };
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            json!({
                "schema_version": 1,
                "device": {
                    "schema_version": 1,
                    "device_id": "device_123",
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0",
                    "paired_at": "2026-05-06T14:30:00Z",
                    "last_seen_at": null,
                    "revoked_at": null
                },
                "capabilities": ["session.read", "events.read"]
            })
        );
    }

    #[test]
    fn local_daemon_wire_json_snapshot() {
        let request = LocalDaemonRequestEnvelopeWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            request_id: "req_001".to_string(),
            client: LocalDaemonClientWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                name: "sase-cli".to_string(),
                version: "0.1.1".to_string(),
            },
            payload: LocalDaemonRequestPayloadWire::List(
                LocalDaemonListRequestWire {
                    collection: LocalDaemonCollectionWire::Mocked,
                    page: LocalDaemonPageRequestWire {
                        limit: LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
                        cursor: None,
                    },
                    snapshot_id: Some("snap_mock_001".to_string()),
                    stable_handle: Some("mocked:list:contract".to_string()),
                    max_payload_bytes: Some(LOCAL_DAEMON_MAX_PAYLOAD_BYTES),
                    filters: Some(json!({"status": "open"})),
                },
            ),
        };

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "schema_version": 1,
                "request_id": "req_001",
                "client": {
                    "schema_version": 1,
                    "name": "sase-cli",
                    "version": "0.1.1"
                },
                "payload": {
                    "type": "list",
                    "data": {
                        "collection": "mocked",
                        "page": {
                            "limit": 100,
                            "cursor": null
                        },
                        "snapshot_id": "snap_mock_001",
                        "stable_handle": "mocked:list:contract",
                        "max_payload_bytes": 1048576,
                        "filters": {
                            "status": "open"
                        }
                    }
                }
            })
        );

        let error =
            LocalDaemonResponsePayloadWire::Error(LocalDaemonErrorWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                code: LocalDaemonErrorCodeWire::DaemonUnavailable,
                message: "local daemon transport is not implemented"
                    .to_string(),
                retryable: false,
                target: None,
                details: None,
                fallback: LocalDaemonFallbackWire {
                    available: true,
                    reason: Some(
                        LocalDaemonFallbackReasonWire::DaemonNotRunning,
                    ),
                    message: Some(
                        "use direct source-store readers".to_string(),
                    ),
                },
            });

        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({
                "type": "error",
                "data": {
                    "schema_version": 1,
                    "code": "daemon_unavailable",
                    "message": "local daemon transport is not implemented",
                    "retryable": false,
                    "target": null,
                    "details": null,
                    "fallback": {
                        "available": true,
                        "reason": "daemon_not_running",
                        "message": "use direct source-store readers"
                    }
                }
            })
        );
    }

    #[test]
    fn error_wire_json_snapshot() {
        let errors = vec![
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some("authorization".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::ConflictAlreadyHandled,
                message: "action was already handled".to_string(),
                target: Some("abcdef12".to_string()),
                details: Some(json!({"notification_id": "abcdef1234567890"})),
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::GoneStale,
                message: "action request is stale".to_string(),
                target: Some("abcdef12".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AmbiguousPrefix,
                message: "notification prefix matches multiple actions"
                    .to_string(),
                target: Some("abcd".to_string()),
                details: Some(json!({"matches": 2})),
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::UnsupportedAction,
                message: "notification action is not supported by mobile"
                    .to_string(),
                target: Some("JumpToChangeSpec".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AttachmentExpired,
                message: "attachment token is expired".to_string(),
                target: Some("token".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::BridgeUnavailable,
                message: "agent bridge is unavailable".to_string(),
                target: Some("agent_bridge".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::UpdateJobNotFound,
                message: "update job not found".to_string(),
                target: Some("job_123".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PermissionDenied,
                message: "permission denied".to_string(),
                target: Some("agent_bridge:kill-agent".to_string()),
                details: None,
            },
        ];
        assert_eq!(
            serde_json::to_value(errors).unwrap(),
            json!([
                {
                    "schema_version": 1,
                    "code": "unauthorized",
                    "message": "authentication is required for this endpoint",
                    "target": "authorization",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "conflict_already_handled",
                    "message": "action was already handled",
                    "target": "abcdef12",
                    "details": {"notification_id": "abcdef1234567890"}
                },
                {
                    "schema_version": 1,
                    "code": "gone_stale",
                    "message": "action request is stale",
                    "target": "abcdef12",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "ambiguous_prefix",
                    "message": "notification prefix matches multiple actions",
                    "target": "abcd",
                    "details": {"matches": 2}
                },
                {
                    "schema_version": 1,
                    "code": "unsupported_action",
                    "message": "notification action is not supported by mobile",
                    "target": "JumpToChangeSpec",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "attachment_expired",
                    "message": "attachment token is expired",
                    "target": "token",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "bridge_unavailable",
                    "message": "agent bridge is unavailable",
                    "target": "agent_bridge",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "update_job_not_found",
                    "message": "update job not found",
                    "target": "job_123",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "permission_denied",
                    "message": "permission denied",
                    "target": "agent_bridge:kill-agent",
                    "details": null
                }
            ])
        );
    }

    #[test]
    fn event_wire_json_snapshot() {
        let event = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000001".to_string(),
            created_at: "2026-05-06T14:30:00Z".to_string(),
            payload: EventPayloadWire::Heartbeat { sequence: 1 },
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000001",
                "created_at": "2026-05-06T14:30:00Z",
                "payload": {
                    "type": "heartbeat",
                    "data": {
                        "sequence": 1
                    }
                }
            })
        );

        let changed = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000002".to_string(),
            created_at: "2026-05-06T14:31:00Z".to_string(),
            payload: EventPayloadWire::AgentsChanged {
                reason: "launch".to_string(),
                agent_name: Some("mobile-demo".to_string()),
                timestamp: Some("2026-05-06T14:31:00Z".to_string()),
            },
        };
        assert_eq!(
            serde_json::to_value(changed).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000002",
                "created_at": "2026-05-06T14:31:00Z",
                "payload": {
                    "type": "agents_changed",
                    "data": {
                        "reason": "launch",
                        "agent_name": "mobile-demo",
                        "timestamp": "2026-05-06T14:31:00Z"
                    }
                }
            })
        );

        let helper_changed = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000003".to_string(),
            created_at: "2026-05-06T14:32:00Z".to_string(),
            payload: EventPayloadWire::HelpersChanged {
                reason: "update_start".to_string(),
                helper: Some("update".to_string()),
                job_id: Some("job_123".to_string()),
                timestamp: Some("2026-05-06T14:32:00Z".to_string()),
            },
        };
        assert_eq!(
            serde_json::to_value(helper_changed).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000003",
                "created_at": "2026-05-06T14:32:00Z",
                "payload": {
                    "type": "helpers_changed",
                    "data": {
                        "reason": "update_start",
                        "helper": "update",
                        "job_id": "job_123",
                        "timestamp": "2026-05-06T14:32:00Z"
                    }
                }
            })
        );
    }

    #[test]
    fn push_subscription_wire_json_snapshot() {
        let subscription = PushSubscriptionRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "push_123".to_string(),
            device_id: "dev_pixel".to_string(),
            provider: PushProviderWire::Fcm,
            provider_token: "opaque-token".to_string(),
            app_instance_id: Some("app_instance_pixel".to_string()),
            device_display_name: Some("Pixel 9".to_string()),
            platform: Some("android".to_string()),
            app_version: Some("0.1.0".to_string()),
            hint_categories: vec![
                PushHintCategoryWire::Notifications,
                PushHintCategoryWire::Agents,
                PushHintCategoryWire::Update,
            ],
            enabled_at: "2026-05-06T14:30:00Z".to_string(),
            disabled_at: None,
            last_seen_at: Some("2026-05-06T14:31:00Z".to_string()),
        };

        assert_eq!(
            serde_json::to_value(PushSubscriptionListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                subscriptions: vec![subscription.clone()],
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "subscriptions": [{
                    "schema_version": 1,
                    "id": "push_123",
                    "device_id": "dev_pixel",
                    "provider": "fcm",
                    "provider_token": "opaque-token",
                    "app_instance_id": "app_instance_pixel",
                    "device_display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0",
                    "hint_categories": ["notifications", "agents", "update"],
                    "enabled_at": "2026-05-06T14:30:00Z",
                    "disabled_at": null,
                    "last_seen_at": "2026-05-06T14:31:00Z"
                }]
            })
        );
        assert_eq!(
            serde_json::to_value(PushSubscriptionRegisterResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                subscription: subscription.clone(),
                created: false,
            })
            .unwrap()["created"],
            false
        );
        assert_eq!(
            serde_json::to_value(PushSubscriptionDeleteResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                subscription,
                revoked: true,
            })
            .unwrap()["revoked"],
            true
        );
    }

    #[test]
    fn push_hint_mapping_uses_safe_event_projection() {
        let notification_hint = push_hint_from_event(&EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000001".to_string(),
            created_at: "2026-05-06T14:30:00Z".to_string(),
            payload: EventPayloadWire::NotificationsChanged {
                reason: "mark_read".to_string(),
                notification_id: Some("notif_123".to_string()),
            },
        })
        .unwrap();
        assert_eq!(
            serde_json::to_value(notification_hint).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000001",
                "created_at": "2026-05-06T14:30:00Z",
                "category": "notifications",
                "reason": "mark_read",
                "notification_id": "notif_123",
                "agent_name": null,
                "helper": null,
                "job_id": null,
                "title": "SASE notification update",
                "body": "Open SASE to view current notifications."
            })
        );

        let unsafe_agent_hint = push_hint_from_event(&EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000002".to_string(),
            created_at: "2026-05-06T14:31:00Z".to_string(),
            payload: EventPayloadWire::AgentsChanged {
                reason: "launch".to_string(),
                agent_name: Some("agent with prompt text".to_string()),
                timestamp: None,
            },
        })
        .unwrap();
        assert_eq!(unsafe_agent_hint.category, PushHintCategoryWire::Agents);
        assert_eq!(unsafe_agent_hint.agent_name, None);
        assert_eq!(unsafe_agent_hint.body, "Agent state changed.");

        let update_hint = push_hint_from_event(&EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000003".to_string(),
            created_at: "2026-05-06T14:32:00Z".to_string(),
            payload: EventPayloadWire::HelpersChanged {
                reason: "update_start".to_string(),
                helper: Some("update".to_string()),
                job_id: Some("update_job_123".to_string()),
                timestamp: None,
            },
        })
        .unwrap();
        assert_eq!(update_hint.category, PushHintCategoryWire::Update);
        assert_eq!(update_hint.helper.as_deref(), Some("update"));
        assert_eq!(update_hint.job_id.as_deref(), Some("update_job_123"));
    }

    #[test]
    fn mobile_agent_wire_json_snapshot() {
        let summary = MobileAgentSummaryWire {
            name: "mobile-demo".to_string(),
            project: Some("sase".to_string()),
            status: "running".to_string(),
            pid: Some(4242),
            model: Some("gpt-5.5".to_string()),
            provider: Some("codex".to_string()),
            workspace_number: Some(102),
            started_at: Some("2026-05-06T14:30:00Z".to_string()),
            duration_seconds: Some(90),
            prompt_snippet: Some("Implement gateway skeleton".to_string()),
            has_artifact_dir: true,
            retry_lineage: MobileAgentRetryLineageWire {
                retry_of_timestamp: None,
                retried_as_timestamp: None,
                retry_chain_root_timestamp: Some(
                    "2026-05-06T14:30:00Z".to_string(),
                ),
                retry_attempt: Some(0),
                parent_agent_name: None,
            },
            actions: MobileAgentActionAffordancesWire {
                can_resume: true,
                can_wait: true,
                can_kill: true,
                can_retry: false,
            },
            display: MobileAgentDisplayLabelsWire {
                title: "mobile-demo".to_string(),
                subtitle: Some("sase".to_string()),
                status_label: "Running".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(MobileAgentListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                agents: vec![summary],
                total_count: 1,
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "agents": [{
                    "name": "mobile-demo",
                    "project": "sase",
                    "status": "running",
                    "pid": 4242,
                    "model": "gpt-5.5",
                    "provider": "codex",
                    "workspace_number": 102,
                    "started_at": "2026-05-06T14:30:00Z",
                    "duration_seconds": 90,
                    "prompt_snippet": "Implement gateway skeleton",
                    "has_artifact_dir": true,
                    "retry_lineage": {
                        "retry_of_timestamp": null,
                        "retried_as_timestamp": null,
                        "retry_chain_root_timestamp": "2026-05-06T14:30:00Z",
                        "retry_attempt": 0,
                        "parent_agent_name": null
                    },
                    "actions": {
                        "can_resume": true,
                        "can_wait": true,
                        "can_kill": true,
                        "can_retry": false
                    },
                    "display": {
                        "title": "mobile-demo",
                        "subtitle": "sase",
                        "status_label": "Running"
                    }
                }],
                "total_count": 1
            })
        );

        assert_eq!(
            serde_json::to_value(MobileAgentResumeOptionsResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                options: vec![MobileAgentResumeOptionWire {
                    id: "mobile-demo-resume".to_string(),
                    agent_name: "mobile-demo".to_string(),
                    kind: MobileAgentResumeOptionKindWire::Resume,
                    label: "Resume".to_string(),
                    prompt_text: "#resume:mobile-demo".to_string(),
                    direct_launch_supported: true,
                }],
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "options": [{
                    "id": "mobile-demo-resume",
                    "agent_name": "mobile-demo",
                    "kind": "resume",
                    "label": "Resume",
                    "prompt_text": "#resume:mobile-demo",
                    "direct_launch_supported": true
                }]
            })
        );

        let slot = MobileAgentLaunchSlotResultWire {
            slot_id: "0".to_string(),
            name: Some("mobile-demo".to_string()),
            status: MobileAgentLaunchSlotStatusWire::Launched,
            artifact_dir: Some("/tmp/sase/agents/mobile-demo".to_string()),
            message: None,
        };
        assert_eq!(
            serde_json::to_value(MobileAgentLaunchResultWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                primary: Some(slot.clone()),
                slots: vec![slot],
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "primary": {
                    "slot_id": "0",
                    "name": "mobile-demo",
                    "status": "launched",
                    "artifact_dir": "/tmp/sase/agents/mobile-demo",
                    "message": null
                },
                "slots": [{
                    "slot_id": "0",
                    "name": "mobile-demo",
                    "status": "launched",
                    "artifact_dir": "/tmp/sase/agents/mobile-demo",
                    "message": null
                }]
            })
        );

        assert_eq!(
            serde_json::to_value(MobileAgentImageLaunchRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                prompt: "Review this screenshot".to_string(),
                request_id: Some("req-image-1".to_string()),
                original_filename: "screen.png".to_string(),
                content_type: "image/png".to_string(),
                byte_length: 8,
                base64_image: "iVBORw0K".to_string(),
                device_id: Some("device_123".to_string()),
                display_name: None,
                name: Some("mobile-demo".to_string()),
                model: None,
                provider: None,
                runtime: None,
                project: Some("sase".to_string()),
                dry_run: Some(false),
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "prompt": "Review this screenshot",
                "request_id": "req-image-1",
                "original_filename": "screen.png",
                "content_type": "image/png",
                "byte_length": 8,
                "base64_image": "iVBORw0K",
                "device_id": "device_123",
                "display_name": null,
                "name": "mobile-demo",
                "model": null,
                "provider": null,
                "runtime": null,
                "project": "sase",
                "dry_run": false
            })
        );
    }

    #[test]
    fn mobile_helper_wire_json_snapshot() {
        let result = MobileHelperResultWire {
            status: MobileHelperStatusWire::PartialSuccess,
            message: Some("loaded helper records".to_string()),
            warnings: vec!["one project could not be read".to_string()],
            skipped: vec![MobileHelperSkippedWire {
                target: Some("archive.sase".to_string()),
                reason: "terminal changespec".to_string(),
            }],
            partial_failure_count: Some(1),
        };
        let context = MobileHelperProjectContextWire {
            project: Some("sase".to_string()),
            scope: MobileHelperProjectScopeWire::Explicit,
        };

        assert_eq!(
            serde_json::to_value(MobileChangeSpecTagListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                tags: vec![MobileChangeSpecTagEntryWire {
                    tag: "#gh:feature".to_string(),
                    project: Some("sase".to_string()),
                    changespec: "feature".to_string(),
                    title: Some("Feature work".to_string()),
                    status: "WIP".to_string(),
                    workflow: Some("gh".to_string()),
                    source_path_display: Some(
                        "~/.sase/projects/sase.sase".to_string()
                    ),
                }],
                total_count: 1,
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "result": {
                    "status": "partial_success",
                    "message": "loaded helper records",
                    "warnings": ["one project could not be read"],
                    "skipped": [{
                        "target": "archive.sase",
                        "reason": "terminal changespec"
                    }],
                    "partial_failure_count": 1
                },
                "context": {
                    "project": "sase",
                    "scope": "explicit"
                },
                "tags": [{
                    "tag": "#gh:feature",
                    "project": "sase",
                    "changespec": "feature",
                    "title": "Feature work",
                    "status": "WIP",
                    "workflow": "gh",
                    "source_path_display": "~/.sase/projects/sase.sase"
                }],
                "total_count": 1
            })
        );

        assert_eq!(
            serde_json::to_value(MobileXpromptCatalogResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                entries: vec![MobileXpromptCatalogEntryWire {
                    name: "gh".to_string(),
                    display_label: "GitHub workflow".to_string(),
                    insertion: Some("#!gh".to_string()),
                    reference_prefix: Some("#!".to_string()),
                    kind: Some("workflow".to_string()),
                    description: Some(
                        "Create a GitHub workflow prompt".to_string()
                    ),
                    source_bucket: "project".to_string(),
                    project: Some("sase".to_string()),
                    tags: vec!["changespec".to_string()],
                    input_signature: Some("topic".to_string()),
                    inputs: vec![MobileXpromptInputWire {
                        name: "topic".to_string(),
                        r#type: "word".to_string(),
                        required: true,
                        default_display: None,
                        position: 0,
                    }],
                    is_skill: false,
                    content_preview: Some("Use this workflow...".to_string()),
                    source_path_display: Some("xprompts/gh.md".to_string()),
                    definition_path: None,
                    definition_range: None,
                }],
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 1,
                    skill_count: 0,
                    pdf_requested: true,
                },
                catalog_attachment: Some(MobileXpromptCatalogAttachmentWire {
                    display_name: "xprompts.pdf".to_string(),
                    content_type: Some("application/pdf".to_string()),
                    byte_size: Some(1234),
                    path_display: Some("~/.sase/xprompts.pdf".to_string()),
                    generated: true,
                }),
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "result": {
                    "status": "partial_success",
                    "message": "loaded helper records",
                    "warnings": ["one project could not be read"],
                    "skipped": [{
                        "target": "archive.sase",
                        "reason": "terminal changespec"
                    }],
                    "partial_failure_count": 1
                },
                "context": {
                    "project": "sase",
                    "scope": "explicit"
                },
                "entries": [{
                    "name": "gh",
                    "display_label": "GitHub workflow",
                    "insertion": "#!gh",
                    "reference_prefix": "#!",
                    "kind": "workflow",
                    "description": "Create a GitHub workflow prompt",
                    "source_bucket": "project",
                    "project": "sase",
                    "tags": ["changespec"],
                    "input_signature": "topic",
                    "inputs": [{
                        "name": "topic",
                        "type": "word",
                        "required": true,
                        "default_display": null,
                        "position": 0
                    }],
                    "is_skill": false,
                    "content_preview": "Use this workflow...",
                    "source_path_display": "xprompts/gh.md"
                }],
                "stats": {
                    "total_count": 1,
                    "project_count": 1,
                    "skill_count": 0,
                    "pdf_requested": true
                },
                "catalog_attachment": {
                    "display_name": "xprompts.pdf",
                    "content_type": "application/pdf",
                    "byte_size": 1234,
                    "path_display": "~/.sase/xprompts.pdf",
                    "generated": true
                }
            })
        );

        let summary = MobileBeadSummaryWire {
            id: "sase-26.4.1".to_string(),
            title: "Rust helper skeleton".to_string(),
            status: "in_progress".to_string(),
            bead_type: "phase".to_string(),
            tier: None,
            project: Some("sase".to_string()),
            parent_id: Some("sase-26.4".to_string()),
            assignee: Some("sase-26.4.1".to_string()),
            updated_at: Some("2026-05-06T15:08:41Z".to_string()),
            dependency_count: 0,
            block_count: 5,
            child_count: 0,
            plan_path_display: Some("sdd/epics/202605/mobile.md".to_string()),
            changespec_name: None,
            changespec_status: None,
        };
        assert_eq!(
            serde_json::to_value(MobileBeadShowResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                bead: MobileBeadDetailWire {
                    summary: summary.clone(),
                    description: Some("Define route skeletons".to_string()),
                    notes: None,
                    design_path_display: Some(
                        "sdd/epics/202605/mobile.md".to_string()
                    ),
                    dependencies: Vec::new(),
                    blocks: vec!["sase-26.4.2".to_string()],
                    children: Vec::new(),
                    workspace_display: Some("~/projects/sase".to_string()),
                },
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "result": {
                    "status": "partial_success",
                    "message": "loaded helper records",
                    "warnings": ["one project could not be read"],
                    "skipped": [{
                        "target": "archive.sase",
                        "reason": "terminal changespec"
                    }],
                    "partial_failure_count": 1
                },
                "context": {
                    "project": "sase",
                    "scope": "explicit"
                },
                "bead": {
                    "summary": {
                        "id": "sase-26.4.1",
                        "title": "Rust helper skeleton",
                        "status": "in_progress",
                        "bead_type": "phase",
                        "tier": null,
                        "project": "sase",
                        "parent_id": "sase-26.4",
                        "assignee": "sase-26.4.1",
                        "updated_at": "2026-05-06T15:08:41Z",
                        "dependency_count": 0,
                        "block_count": 5,
                        "child_count": 0,
                        "plan_path_display": "sdd/epics/202605/mobile.md",
                        "changespec_name": null,
                        "changespec_status": null
                    },
                    "description": "Define route skeletons",
                    "notes": null,
                    "design_path_display": "sdd/epics/202605/mobile.md",
                    "dependencies": [],
                    "blocks": ["sase-26.4.2"],
                    "children": [],
                    "workspace_display": "~/projects/sase"
                }
            })
        );

        assert_eq!(
            serde_json::to_value(MobileUpdateStartResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result,
                job: MobileUpdateJobWire {
                    job_id: "job_123".to_string(),
                    status: MobileUpdateJobStatusWire::Running,
                    started_at: Some("2026-05-06T15:00:00Z".to_string()),
                    finished_at: None,
                    message: Some("update started".to_string()),
                    log_path_display: Some(
                        "~/.sase/chat_install/job.log".to_string()
                    ),
                    completion_path_display: None,
                },
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "result": {
                    "status": "partial_success",
                    "message": "loaded helper records",
                    "warnings": ["one project could not be read"],
                    "skipped": [{
                        "target": "archive.sase",
                        "reason": "terminal changespec"
                    }],
                    "partial_failure_count": 1
                },
                "job": {
                    "job_id": "job_123",
                    "status": "running",
                    "started_at": "2026-05-06T15:00:00Z",
                    "finished_at": null,
                    "message": "update started",
                    "log_path_display": "~/.sase/chat_install/job.log",
                    "completion_path_display": null
                }
            })
        );
    }

    #[test]
    fn mobile_xprompt_catalog_entry_deserializes_legacy_shape() {
        let entry: MobileXpromptCatalogEntryWire =
            serde_json::from_value(json!({
                "name": "gh",
                "display_label": "GitHub workflow",
                "description": "Create a GitHub workflow prompt",
                "source_bucket": "project",
                "project": "sase",
                "tags": ["changespec"],
                "input_signature": "topic",
                "is_skill": false,
                "content_preview": "Use this workflow...",
                "source_path_display": "xprompts/gh.md"
            }))
            .unwrap();

        assert_eq!(entry.name, "gh");
        assert_eq!(entry.insertion, None);
        assert_eq!(entry.reference_prefix, None);
        assert_eq!(entry.kind, None);
        assert_eq!(entry.inputs, Vec::<MobileXpromptInputWire>::new());
    }
}
