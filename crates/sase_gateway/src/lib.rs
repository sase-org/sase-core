#![recursion_limit = "512"]

//! Local host gateway skeleton for SASE mobile clients.

pub mod cli;
pub mod contract;
pub mod daemon;
pub mod federation_worker;
pub mod fleet_attention;
pub mod fleet_auth;
pub mod fleet_launch;
pub mod fleet_mutations;
pub mod fleet_reads;
pub mod host_bridge;
pub mod push;
pub mod routes;
pub mod server;
pub mod storage;
pub mod wire;

pub use cli::run_gateway_cli;
pub use contract::{
    api_v1_contract_snapshot, fleet_api_v1_contract_snapshot,
    write_api_v1_contract_snapshot, write_fleet_api_v1_contract_snapshot,
    ContractSnapshotError,
};
pub use daemon::{
    default_run_root, default_socket_path, host_identity_from_env,
    mobile_gateway_config, run_daemon, sanitize_host_identity,
    validate_daemon_config, DaemonConfig, DaemonRunError, DaemonRuntime,
    DaemonRuntimePaths, DaemonShutdown, DaemonState,
};
pub use federation_worker::{
    default_federation_socket_path, run_federation_worker,
    run_federation_worker_blocking, run_federation_worker_cli,
    FederationErrorWire, FederationHealthWire, FederationHostConfigWire,
    FederationHostResultWire, FederationIpcRequestEnvelopeWire,
    FederationIpcRequestWire, FederationIpcResponseEnvelopeWire,
    FederationReadResponseWire, FederationWorkerConfig, FederationWorkerError,
    FEDERATION_IPC_SCHEMA_VERSION, FEDERATION_MAX_FRAME_BYTES,
    FEDERATION_WORKER_SERVICE,
};
pub use fleet_attention::{
    FleetAttentionAdmission, FleetAttentionStore, FleetAttentionStoreError,
};
pub use fleet_auth::{
    credential_has_scope, current_unix_time, default_fleet_scopes,
    fleet_capabilities, negotiate_fleet_protocol_version, FleetAuthentication,
    FleetCredentialStore, FleetEnrollmentResult, FleetEnrollmentSuccess,
    FleetStoreError, FLEET_AUTH_DIR, FLEET_AUTH_FILE, FLEET_AUTH_LOCK_FILE,
    FLEET_AUTH_STORE_SCHEMA_VERSION, FLEET_BOOTSTRAP_TTL_SECONDS,
    FLEET_CREDENTIAL_TTL_SECONDS, FLEET_SCOPE_ATTENTION_READ,
    FLEET_SCOPE_ATTENTION_RESOLVE, FLEET_SCOPE_BATCH_READ,
    FLEET_SCOPE_CATALOG_READ, FLEET_SCOPE_CONTENT_READ,
    FLEET_SCOPE_DETAIL_READ, FLEET_SCOPE_EVENTS_READ, FLEET_SCOPE_HELLO,
    FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE, FLEET_SCOPE_PROJECTS_READ,
    FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE, FLEET_SCOPE_SUMMARY_READ,
};
pub use fleet_launch::{
    FleetLaunchAdmission, FleetLaunchStore, FleetLaunchStoreError,
};
pub use fleet_mutations::{
    FleetMutationAdmission, FleetMutationStore, FleetMutationStoreError,
};
pub use fleet_reads::{
    resync_item, FleetEventSubscription, FleetInvalidationHub, FleetReadError,
    FleetReadService,
};
pub use host_bridge::{
    split_command_words, AgentHostBridge, CommandAgentHostBridge,
    CommandHelperHostBridge, DynAgentHostBridge, DynHelperHostBridge,
    DynNotificationHostBridge, HelperHostBridge, HostBridgeError,
    HostFileMetadataWire, LocalJsonlNotificationBridge, NotificationHostBridge,
    StaticAgentHostBridge, StaticHelperHostBridge,
    StaticNotificationHostBridge, UnavailableAgentHostBridge,
    UnavailableHelperHostBridge,
};
pub use push::{
    PushConfig, PushDeliveryAttempt, PushDispatcher, PushProviderMode,
};
pub use routes::{app, app_with_state, default_sase_home, GatewayState};
pub use server::{
    serve, serve_listener_with_state, validate_bind_policy, GatewayConfig,
    GatewayRunError,
};
pub use storage::{AuditLogEntryWire, DeviceTokenStore, StoreError};
pub use wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, FleetAuthoritativeSnapshotWire,
    FleetBootstrapIssueRequestWire, FleetBootstrapIssueResponseWire,
    FleetCatalogAccumulationActionWire, FleetCatalogAccumulationDecisionWire,
    FleetCatalogAccumulationRequestWire, FleetCatalogAccumulationStateWire,
    FleetCatalogContinuationStateWire, FleetCatalogContinuationWire,
    FleetCatalogPageSelectionWire, FleetCatalogPageWire, FleetCatalogQueryWire,
    FleetCatalogResetReasonWire, FleetCatalogScopeWire,
    FleetContentReadRequestWire, FleetContentReadResponseWire,
    FleetControllerMetadataWire, FleetCredentialRecordWire,
    FleetCredentialRevokeRequestWire, FleetCredentialRevokeResponseWire,
    FleetDetailRequestWire, FleetDetailResponseWire,
    FleetEnrollmentRequestWire, FleetEnrollmentResponseWire,
    FleetEventStreamItemWire, FleetHealthWire, FleetHelloResponseWire,
    FleetInvalidationEventWire, FleetInvalidationKindWire,
    FleetLaunchReceiptWire, FleetLaunchRequestWire, FleetLaunchResponseWire,
    FleetLogicalAgentCountsWire, FleetLogicalBatchEntryWire,
    FleetLogicalBatchRequestWire, FleetLogicalBatchResponseWire,
    FleetMutationReceiptWire, FleetMutationRequestWire,
    FleetMutationResponseWire, FleetProjectEligibilityRequestWire,
    FleetProjectEligibilityResponseWire, FleetProjectEligibilityWire,
    FleetQuarantineWire, FleetResyncReasonWire, FleetResyncRequiredWire,
    FleetSnapshotFreshnessWire, FleetSummaryResponseWire,
    FleetTokenRotateRequestWire, FleetTokenRotateResponseWire, GatewayBindWire,
    GatewayBuildWire, HealthResponseWire, MobileAgentActionAffordancesWire,
    MobileAgentDisplayLabelsWire, MobileAgentForkRequestWire,
    MobileAgentForkResultWire, MobileAgentImageLaunchRequestWire,
    MobileAgentKillRequestWire, MobileAgentKillResultWire,
    MobileAgentLaunchResultWire, MobileAgentLaunchSlotResultWire,
    MobileAgentLaunchSlotStatusWire, MobileAgentListRequestWire,
    MobileAgentListResponseWire, MobileAgentResumeOptionKindWire,
    MobileAgentResumeOptionWire, MobileAgentResumeOptionsResponseWire,
    MobileAgentRetryLineageWire, MobileAgentRetryRequestWire,
    MobileAgentRetryResultWire, MobileAgentSummaryWire,
    MobileAgentTextLaunchRequestWire, MobileBeadDetailWire,
    MobileBeadListRequestWire, MobileBeadListResponseWire,
    MobileBeadShowRequestWire, MobileBeadShowResponseWire,
    MobileBeadSummaryWire, MobileChangeSpecTagEntryWire,
    MobileChangeSpecTagListRequestWire, MobileChangeSpecTagListResponseWire,
    MobileHelperProjectContextWire, MobileHelperProjectScopeWire,
    MobileHelperResultWire, MobileHelperSkippedWire, MobileHelperStatusWire,
    MobilePatchTagEntryWire, MobilePatchTagListRequestWire,
    MobilePatchTagListResponseWire, MobileUpdateJobStatusWire,
    MobileUpdateJobWire, MobileUpdateStartRequestWire,
    MobileUpdateStartResponseWire, MobileUpdateStatusRequestWire,
    MobileUpdateStatusResponseWire, MobileXpromptCatalogAttachmentWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogRequestWire,
    MobileXpromptCatalogResponseWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire, NotificationStateMutationResponseWire,
    PairFinishRequestWire, PairFinishResponseWire, PairStartRequestWire,
    PairStartResponseWire, PairingDeviceMetadataWire, PushGatewayStatusWire,
    PushHintCategoryWire, PushHintWire, PushProviderWire,
    PushSubscriptionDeleteResponseWire, PushSubscriptionListResponseWire,
    PushSubscriptionRecordWire, PushSubscriptionRegisterResponseWire,
    PushSubscriptionRequestWire, SessionResponseWire,
    FLEET_API_WIRE_SCHEMA_VERSION, FLEET_PROTOCOL_VERSION,
    GATEWAY_WIRE_SCHEMA_VERSION,
};
