#![recursion_limit = "512"]

//! Local host gateway skeleton for SASE mobile clients.

pub mod contract;
pub mod host_bridge;
pub mod push;
pub mod routes;
pub mod server;
pub mod storage;
pub mod wire;

pub use contract::{
    api_v1_contract_snapshot, local_daemon_contract_snapshot,
    write_api_v1_contract_snapshot, write_local_daemon_contract_snapshot,
    ContractSnapshotError,
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
pub use server::{serve, validate_bind_policy, GatewayConfig, GatewayRunError};
pub use storage::{AuditLogEntryWire, DeviceTokenStore, StoreError};
pub use wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
    LocalDaemonBatchRequestWire, LocalDaemonBatchResponseWire,
    LocalDaemonCapabilitiesResponseWire, LocalDaemonClientWire,
    LocalDaemonCollectionWire, LocalDaemonDeltaOperationWire,
    LocalDaemonErrorCodeWire, LocalDaemonErrorWire, LocalDaemonEventBatchWire,
    LocalDaemonEventPayloadWire, LocalDaemonEventRecordWire,
    LocalDaemonEventRequestWire, LocalDaemonEventSourceWire,
    LocalDaemonFallbackReasonWire, LocalDaemonFallbackWire,
    LocalDaemonHealthResponseWire, LocalDaemonHealthStatusWire,
    LocalDaemonHeartbeatWire, LocalDaemonListItemWire,
    LocalDaemonListRequestWire, LocalDaemonListResponseWire,
    LocalDaemonPageRequestWire, LocalDaemonPayloadBoundWire,
    LocalDaemonRequestEnvelopeWire, LocalDaemonRequestPayloadWire,
    LocalDaemonResponseEnvelopeWire, LocalDaemonResponsePayloadWire,
    MobileAgentActionAffordancesWire, MobileAgentDisplayLabelsWire,
    MobileAgentImageLaunchRequestWire, MobileAgentKillRequestWire,
    MobileAgentKillResultWire, MobileAgentLaunchResultWire,
    MobileAgentLaunchSlotResultWire, MobileAgentLaunchSlotStatusWire,
    MobileAgentListRequestWire, MobileAgentListResponseWire,
    MobileAgentResumeOptionKindWire, MobileAgentResumeOptionWire,
    MobileAgentResumeOptionsResponseWire, MobileAgentRetryLineageWire,
    MobileAgentRetryRequestWire, MobileAgentRetryResultWire,
    MobileAgentSummaryWire, MobileAgentTextLaunchRequestWire,
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
    MobileXpromptInputWire, NotificationStateMutationResponseWire,
    PairFinishRequestWire, PairFinishResponseWire, PairStartRequestWire,
    PairStartResponseWire, PairingDeviceMetadataWire, PushGatewayStatusWire,
    PushHintCategoryWire, PushHintWire, PushProviderWire,
    PushSubscriptionDeleteResponseWire, PushSubscriptionListResponseWire,
    PushSubscriptionRecordWire, PushSubscriptionRegisterResponseWire,
    PushSubscriptionRequestWire, SessionResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION, LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
    LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION, LOCAL_DAEMON_MAX_PAGE_LIMIT,
    LOCAL_DAEMON_MAX_PAYLOAD_BYTES, LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
    LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
};
