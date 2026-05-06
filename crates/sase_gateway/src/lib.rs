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
    DynNotificationHostBridge, HostBridgeError, HostFileMetadataWire,
    LocalJsonlNotificationBridge, NotificationHostBridge,
    StaticNotificationHostBridge,
};
pub use routes::{app, app_with_state, default_sase_home, GatewayState};
pub use server::{serve, validate_bind_policy, GatewayConfig, GatewayRunError};
pub use storage::{AuditLogEntryWire, DeviceTokenStore, StoreError};
pub use wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
    NotificationStateMutationResponseWire, PairFinishRequestWire,
    PairFinishResponseWire, PairStartRequestWire, PairStartResponseWire,
    PairingDeviceMetadataWire, SessionResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};
