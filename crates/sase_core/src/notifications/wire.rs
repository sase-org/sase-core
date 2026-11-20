use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const NOTIFICATION_STORE_WIRE_SCHEMA_VERSION: u32 = 1;

/// Hard cap on a stored plus-one note after trim/collapse.
pub const NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS: usize = 2000;

/// Hard cap on stored plus-one entries per notification row.
pub const NOTIFICATION_PLUS_ONE_MAX_ENTRIES: usize = 500;

/// Default interval in which identical notification +1s coalesce.
pub const NOTIFICATION_PLUS_ONE_COALESCE_WINDOW_SECONDS: u64 = 10 * 60;

/// Live cap on stored plus-one entries for `wait_checks` rows.
pub const NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES: usize = 32;

/// Sender whose rows use the tighter plus-one live cap.
pub const NOTIFICATION_WAIT_CHECKS_SENDER: &str = "wait_checks";

fn u32_is_zero(value: &u32) -> bool {
    *value == 0
}

/// One append-only corroboration entry on a notification row.
///
/// Unlike bead +1 evidence, the same sender may appear repeatedly: a
/// notification +1 is a new occurrence in time, not one-per-reporter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationPlusOneWire {
    pub timestamp: String,
    pub sender: String,
    pub note: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationWire {
    pub id: String,
    pub timestamp: String,
    pub sender: String,
    #[serde(default)]
    pub icon: Option<String>,
    /// Sender-declared `#RRGGBB` accent for the tab this row lands in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
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
    #[serde(default)]
    pub resurfaced_at: Option<String>,
    /// Append-only occurrence notes. Empty on rows that have never been +1'd.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plus_ones: Vec<NotificationPlusOneWire>,
    /// Count of plus-one entries dropped after the stored-entry cap.
    #[serde(default, skip_serializing_if = "u32_is_zero")]
    pub plus_ones_dropped: u32,
    /// Sender-scoped opaque exact-match key used by create-or-plus-one upsert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
}

impl NotificationWire {
    /// Displayed +1 count: stored entries plus any overflow drops.
    pub fn plus_one_count(&self) -> u64 {
        self.plus_ones.len() as u64 + u64::from(self.plus_ones_dropped)
    }
}

/// Request to append one plus-one, by id or `(sender, dedup_key)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationPlusOneRequestWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub sender: String,
    #[serde(default)]
    pub note: String,
    /// Permit appending a +1 to a dismissed row. Defaults to false.
    #[serde(default)]
    pub plus_one_dismissed: bool,
    /// Identical +1s coalesce inside this window. Omitted means 10 minutes;
    /// zero disables coalescing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce_window_seconds: Option<u64>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NotificationPlusOneActionWire {
    Applied,
    Coalesced,
    SkippedDismissed,
    #[default]
    NoMatch,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationPlusOneOutcomeWire {
    pub schema_version: u32,
    pub action: NotificationPlusOneActionWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub plus_one_count: u64,
    pub plus_ones_dropped: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationWire>,
}

/// Create a fully minted row, or +1 the newest `(sender, dedup_key)` match.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationUpsertRequestWire {
    pub notification: NotificationWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plus_one_note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plus_one_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    /// Permit appending a +1 to a dismissed matching row. Defaults to false.
    #[serde(default)]
    pub plus_one_dismissed: bool,
    /// Identical +1s coalesce inside this window. Omitted means 10 minutes;
    /// zero disables coalescing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce_window_seconds: Option<u64>,
    /// When set, action data values and notes over this byte limit are stored
    /// as notification sidecars on the create branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub externalize_limit_bytes: Option<u64>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NotificationUpsertActionWire {
    #[default]
    Created,
    PlusOned,
    Coalesced,
    SkippedDismissed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationUpsertOutcomeWire {
    pub schema_version: u32,
    pub action: NotificationUpsertActionWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub plus_one_count: u64,
    pub plus_ones_dropped: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationCountsWire {
    pub priority: u64,
    #[serde(default)]
    pub errors: u64,
    pub rest: u64,
    pub muted: u64,
}

/// One notification-panel tab and its ordered, single-valued membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationTabWire {
    /// "hitl" | "errors" | "general" | "__muted__" | "__snoozed__" | panel | tag
    pub key: String,
    /// "hitl"|"panel"|"errors"|"general"|"tag"|"muted"|"snoozed"
    pub kind: String,
    pub count: u64,
    /// Minimum activity timestamp in the tab.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_activity_at: Option<String>,
    /// Snoozed tab only: the minimum `snooze_until` in the tab.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_wake_at: Option<String>,
    /// Sender-declared color, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// Sender-declared tab icon, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
}

/// Ordered tabs plus the single tab key owning each classified row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationTabClassificationWire {
    pub schema_version: u32,
    pub tabs: Vec<NotificationTabWire>,
    pub row_tab_keys: BTreeMap<String, String>,
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
    /// Ordered per-tab counts; empty when produced by an older core.
    #[serde(default)]
    pub tabs: Vec<NotificationTabWire>,
    pub expired_ids: Vec<String>,
    #[serde(default)]
    pub next_snooze_deadline: Option<String>,
    pub stats: NotificationStoreStatsWire,
    /// Store generation the rows were observed at; 0 when an older core
    /// omitted it. Additive: the wire schema version stays 1.
    #[serde(default)]
    pub generation: u64,
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
    #[serde(default)]
    pub next_snooze_deadline: Option<String>,
    pub stats: NotificationStoreStatsWire,
}

/// Append one row with optional content-addressed payload storage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAppendRequestWire {
    pub notification: NotificationWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub externalize_limit_bytes: Option<u64>,
}

/// Request for an explicit notification compaction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationCompactionRequestWire {
    /// When set, existing live values over this byte limit are externalized.
    /// Omitted by default so compaction preserves inline payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub externalize_limit_bytes: Option<u64>,
}

/// Counts reported by one explicit notification compaction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationCompactionOutcomeWire {
    pub schema_version: u32,
    pub archived_count: u64,
    pub externalized_count: u64,
    pub broken_files_dropped: u64,
    pub sidecars_deleted: u64,
    pub rewritten: bool,
}

pub fn notification_activity_at(notification: &NotificationWire) -> &str {
    notification
        .resurfaced_at
        .as_deref()
        .unwrap_or(notification.timestamp.as_str())
}

pub fn notification_activity_cursor(
    notification: &NotificationWire,
) -> (&str, &str) {
    (
        notification_activity_at(notification),
        notification.id.as_str(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAgentKeyWire {
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
}

/// Agent keys whose completion and settlement rows one ack dismisses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAckRequestWire {
    #[serde(default)]
    pub agents: Vec<NotificationAgentKeyWire>,
}

/// Ids one [`super::store::ack_agent_completions`] call newly dismissed,
/// plus the store generation after the call.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAckOutcomeWire {
    pub schema_version: u32,
    #[serde(default)]
    pub dismissed_ids: Vec<String>,
    #[serde(default)]
    pub generation: u64,
}

/// One live completion or settlement row in the lean unread index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnreadCompletionIndexRowWire {
    pub id: String,
    pub agent: NotificationAgentKeyWire,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub dismissed: bool,
}

/// Lean unread index: the store generation plus one row per live
/// completion and settlement row, dismissed rows included.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnreadCompletionIndexWire {
    pub schema_version: u32,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub rows: Vec<UnreadCompletionIndexRowWire>,
}

/// Field-scoped reconcile write for one inventory owner (for example the
/// remote-attention reconciler).
///
/// `notifications` carries only the rows this call created, refreshed, or
/// auto-dismissed — never a full-store snapshot. `reversible_dismiss_marker_key`
/// names the `action_data` key whose `"true"` value marks a dismissal the
/// reconciler itself owns (and may therefore resurface or re-apply); `None`
/// disables both transitions and every dismissal stays as it is on disk.
/// `resurface_on_change_key` names an `action_data` key whose value is the
/// row's content revision: when the input row's value differs from the
/// on-disk value, the row takes the input's `read`, `dismissed` and
/// `resurfaced_at` instead of keeping the on-disk state, so a new revision
/// resurfaces even a user dismissal. An equal value leaves dismissals as the
/// marker rules decide; `None` disables the check.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationReconcileRequestWire {
    #[serde(default)]
    pub notifications: Vec<NotificationWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reversible_dismiss_marker_key: Option<String>,
    /// Default `None` and omitted from the wire when unset, so older cores
    /// and callers that never send it keep today's behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resurface_on_change_key: Option<String>,
    /// When true, copy `files` from the input row onto the on-disk row.
    /// Default false so existing callers never move evidence links.
    /// Does not touch timestamps or delivery cursors.
    #[serde(default)]
    pub refresh_files: bool,
    /// When set, action data values and notes over this byte limit are stored
    /// as notification sidecars before the rows are reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub externalize_limit_bytes: Option<u64>,
}

/// Outcome counts for one [`super::store::reconcile_notification_rows`] call.
///
/// Every input row lands in at most one bucket: created, then resurfaced,
/// then dismissed, then updated.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationReconcileOutcomeWire {
    pub schema_version: u32,
    pub created: u64,
    pub updated: u64,
    pub dismissed: u64,
    pub resurfaced: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationStateUpdateWire {
    MarkRead {
        id: String,
    },
    MarkAllRead,
    MarkTabRead {
        tab_key: String,
    },
    MarkDismissed {
        id: String,
    },
    MarkManyDismissed {
        ids: Vec<String>,
    },
    MarkUndismissed {
        id: String,
    },
    MarkManyUndismissed {
        ids: Vec<String>,
    },
    MarkMuted {
        id: String,
        muted: bool,
    },
    MarkManyMuted {
        ids: Vec<String>,
        muted: bool,
    },
    MarkSnoozed {
        id: String,
        until: String,
    },
    MarkManySnoozed {
        ids: Vec<String>,
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
