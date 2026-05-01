pub mod store;
pub mod wire;

pub use store::{
    append_notification, apply_notification_state_update,
    read_notifications_snapshot, rewrite_notifications,
};
pub use wire::{
    NotificationAgentKeyWire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};
