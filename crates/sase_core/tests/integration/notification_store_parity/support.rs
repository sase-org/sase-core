//! Shared builders for the notification store parity tests.
//!
//! Only helpers used by more than one behavior area live here. Helpers
//! used by a single area stay private to that area's file: the contract
//! fixture in `store_io`, the time/JSONL builders in `mute_snooze_expiry`,
//! the settlement builders in `agent_dismissal`, and the plus-one builders
//! in `plus_one_upsert`.

use sase_core::notifications::NotificationWire;
use std::path::{Path, PathBuf};

pub(super) fn store_path(root: &Path) -> PathBuf {
    root.join("notifications").join("notifications.jsonl")
}

pub(super) fn archive_path(root: &Path) -> PathBuf {
    root.join("notifications")
        .join("notifications-archive.jsonl")
}

pub(super) fn notification(id: &str) -> NotificationWire {
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-05-01T01:02:03+00:00".to_string(),
        sender: "test-sender".to_string(),
        ..NotificationWire::default()
    }
}
