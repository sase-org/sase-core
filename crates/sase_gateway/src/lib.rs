#![recursion_limit = "256"]

//! Local host gateway skeleton for SASE mobile clients.

pub mod contract;
pub mod host_bridge;
pub mod routes;
pub mod server;
pub mod storage;
pub mod wire;

pub use contract::{
    api_v1_contract_snapshot, write_api_v1_contract_snapshot,
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
pub use routes::{app, app_with_state, default_sase_home, GatewayState};
pub use server::{serve, validate_bind_policy, GatewayConfig, GatewayRunError};
pub use storage::{AuditLogEntryWire, DeviceTokenStore, StoreError};
pub use wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
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
    NotificationStateMutationResponseWire, PairFinishRequestWire,
    PairFinishResponseWire, PairStartRequestWire, PairStartResponseWire,
    PairingDeviceMetadataWire, SessionResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};
