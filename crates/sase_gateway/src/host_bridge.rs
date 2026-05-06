use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use sase_core::notifications::{
    read_notifications_snapshot_with_options, NotificationStoreSnapshotWire,
    NotificationWire,
};
use thiserror::Error;

#[derive(Clone)]
pub struct DynNotificationHostBridge(Arc<dyn NotificationHostBridge>);

impl DynNotificationHostBridge {
    pub fn new(bridge: Arc<dyn NotificationHostBridge>) -> Self {
        Self(bridge)
    }

    pub fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        self.0.list_notifications(include_dismissed)
    }

    pub fn notification_file_metadata(
        &self,
        path: &str,
    ) -> HostFileMetadataWire {
        self.0.notification_file_metadata(path)
    }
}

impl fmt::Debug for DynNotificationHostBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynNotificationHostBridge")
            .finish_non_exhaustive()
    }
}

pub trait NotificationHostBridge: Send + Sync {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError>;

    fn notification_file_metadata(&self, path: &str) -> HostFileMetadataWire {
        let path = expand_home_path(path);
        match std::fs::metadata(path) {
            Ok(metadata) => HostFileMetadataWire {
                byte_size: if metadata.is_file() {
                    Some(metadata.len())
                } else {
                    None
                },
                path_available: true,
            },
            Err(_) => HostFileMetadataWire {
                byte_size: None,
                path_available: false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFileMetadataWire {
    pub byte_size: Option<u64>,
    pub path_available: bool,
}

#[derive(Debug)]
pub struct LocalJsonlNotificationBridge {
    notifications_path: PathBuf,
}

impl LocalJsonlNotificationBridge {
    pub fn new(sase_home: impl AsRef<Path>) -> Self {
        Self {
            notifications_path: sase_home
                .as_ref()
                .join("notifications")
                .join("notifications.jsonl"),
        }
    }
}

impl NotificationHostBridge for LocalJsonlNotificationBridge {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        read_notifications_snapshot_with_options(
            &self.notifications_path,
            include_dismissed,
            true,
        )
        .map_err(HostBridgeError::ReadNotifications)
    }
}

#[derive(Debug)]
pub struct StaticNotificationHostBridge {
    snapshot: NotificationStoreSnapshotWire,
}

impl StaticNotificationHostBridge {
    pub fn new(notifications: Vec<NotificationWire>) -> Self {
        Self {
            snapshot: NotificationStoreSnapshotWire {
                schema_version: sase_core::notifications::NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                notifications,
                counts: Default::default(),
                expired_ids: Vec::new(),
                stats: Default::default(),
            },
        }
    }
}

impl NotificationHostBridge for StaticNotificationHostBridge {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        let mut snapshot = self.snapshot.clone();
        if !include_dismissed {
            snapshot
                .notifications
                .retain(|notification| !notification.dismissed);
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Error)]
pub enum HostBridgeError {
    #[error("failed to read notifications: {0}")]
    ReadNotifications(String),
}

fn expand_home_path(path: &str) -> PathBuf {
    if path == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
