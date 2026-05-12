use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const NOTIFICATION_STORE_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationWire {
    pub id: String,
    pub timestamp: String,
    pub sender: String,
    #[serde(default)]
    pub notes: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub action_data: BTreeMap<String, String>,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub dismissed: bool,
    #[serde(default)]
    pub silent: bool,
    #[serde(default)]
    pub muted: bool,
    #[serde(default)]
    pub snooze_until: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationCountsWire {
    pub priority: u64,
    #[serde(default)]
    pub errors: u64,
    pub rest: u64,
    pub muted: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationStoreStatsWire {
    pub total_lines: u64,
    pub blank_lines: u64,
    pub invalid_json_lines: u64,
    pub invalid_record_lines: u64,
    pub loaded_rows: u64,
    pub dismissed_filtered: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationStoreSnapshotWire {
    pub schema_version: u32,
    pub notifications: Vec<NotificationWire>,
    pub counts: NotificationCountsWire,
    pub expired_ids: Vec<String>,
    pub stats: NotificationStoreStatsWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationUpdateOutcomeWire {
    pub schema_version: u32,
    pub matched_count: u64,
    pub changed_count: u64,
    pub appended_count: u64,
    pub rewritten: bool,
    pub notifications: Vec<NotificationWire>,
    pub counts: NotificationCountsWire,
    pub expired_ids: Vec<String>,
    pub stats: NotificationStoreStatsWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAgentKeyWire {
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationStateUpdateWire {
    MarkRead {
        id: String,
    },
    MarkAllRead,
    MarkDismissed {
        id: String,
    },
    MarkManyDismissed {
        ids: Vec<String>,
    },
    MarkMuted {
        id: String,
        muted: bool,
    },
    MarkSnoozed {
        id: String,
        until: String,
    },
    ExpireSnoozes {
        now: String,
    },
    DismissMatchingAgents {
        agents: Vec<NotificationAgentKeyWire>,
    },
    DismissAgentCompletionsMatchingAgents {
        agents: Vec<NotificationAgentKeyWire>,
    },
    DismissAgentCompletions,
    RewriteAll {
        notifications: Vec<NotificationWire>,
    },
}
