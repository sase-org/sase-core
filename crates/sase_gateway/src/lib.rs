//! Local host gateway skeleton for SASE mobile clients.

pub mod contract;
pub mod routes;
pub mod server;
pub mod storage;
pub mod wire;

pub use contract::{
    api_v1_contract_snapshot, write_api_v1_contract_snapshot,
    ContractSnapshotError,
};
pub use routes::{app, app_with_state, default_sase_home, GatewayState};
pub use server::{serve, validate_bind_policy, GatewayConfig, GatewayRunError};
pub use storage::{AuditLogEntryWire, DeviceTokenStore, StoreError};
pub use wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
    PairFinishRequestWire, PairFinishResponseWire, PairStartRequestWire,
    PairStartResponseWire, PairingDeviceMetadataWire, SessionResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};
