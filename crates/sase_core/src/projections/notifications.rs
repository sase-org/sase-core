use std::collections::BTreeMap;

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventEnvelopeWire,
    EventSourceWire,
};
use super::replay::ProjectionApplier;
use crate::notifications::{
    mobile_notification_error_from_wire,
    mobile_notification_priority_from_wire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationWire, PendingActionStoreWire,
    PendingActionWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};

pub const NOTIFICATION_PROJECTION_NAME: &str = "notifications";
pub const NOTIFICATION_EVENT_APPENDED: &str = "notification.appended";
pub const NOTIFICATION_EVENT_REWRITTEN: &str = "notification.rewritten";
pub const NOTIFICATION_EVENT_STATE_UPDATED: &str = "notification.state_updated";
pub const PENDING_ACTION_EVENT_REGISTERED: &str =
    "notification.pending_action.registered";
pub const PENDING_ACTION_EVENT_UPDATED: &str =
    "notification.pending_action.updated";
pub const PENDING_ACTION_EVENT_CLEANED_UP: &str =
    "notification.pending_action.cleaned_up";
pub const PENDING_ACTION_EVENT_STORE_REWRITTEN: &str =
    "notification.pending_action.store_rewritten";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotificationProjectionEventContextWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationProjectionEventPayloadWire {
    NotificationAppended {
        notification: NotificationWire,
    },
    NotificationsRewritten {
        notifications: Vec<NotificationWire>,
    },
    NotificationStateUpdated {
        update: NotificationStateUpdateWire,
        notifications: Vec<NotificationWire>,
    },
    PendingActionRegistered {
        action: PendingActionWire,
    },
    PendingActionUpdated {
        action: PendingActionWire,
    },
    PendingActionCleanedUp {
        prefixes: Vec<String>,
    },
    PendingActionStoreRewritten {
        store: PendingActionStoreWire,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationProjectionFacetCountsWire {
    pub active: u64,
    pub dismissed: u64,
    pub read: u64,
    pub unread: u64,
    pub priority: u64,
    pub errors: u64,
    pub rest: u64,
    pub muted: u64,
    pub pending_actions: u64,
    pub stale_pending_actions: u64,
}

pub struct NotificationProjectionApplier;

impl ProjectionApplier for NotificationProjectionApplier {
    fn projection_name(&self) -> &str {
        NOTIFICATION_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_notification_projection_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_notification_projection_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_notification_projection_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    NOTIFICATION_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }
}

pub fn notification_append_event_request(
    context: NotificationProjectionEventContextWire,
    notification: NotificationWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        NOTIFICATION_EVENT_APPENDED,
        NotificationProjectionEventPayloadWire::NotificationAppended {
            notification,
        },
    )
}

pub fn notification_rewrite_event_request(
    context: NotificationProjectionEventContextWire,
    notifications: Vec<NotificationWire>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        NOTIFICATION_EVENT_REWRITTEN,
        NotificationProjectionEventPayloadWire::NotificationsRewritten {
            notifications,
        },
    )
}

pub fn notification_state_update_event_request(
    context: NotificationProjectionEventContextWire,
    update: NotificationStateUpdateWire,
    notifications: Vec<NotificationWire>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        NOTIFICATION_EVENT_STATE_UPDATED,
        NotificationProjectionEventPayloadWire::NotificationStateUpdated {
            update,
            notifications,
        },
    )
}

pub fn pending_action_register_event_request(
    context: NotificationProjectionEventContextWire,
    action: PendingActionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        PENDING_ACTION_EVENT_REGISTERED,
        NotificationProjectionEventPayloadWire::PendingActionRegistered {
            action,
        },
    )
}

pub fn pending_action_update_event_request(
    context: NotificationProjectionEventContextWire,
    action: PendingActionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        PENDING_ACTION_EVENT_UPDATED,
        NotificationProjectionEventPayloadWire::PendingActionUpdated { action },
    )
}

pub fn pending_action_cleanup_event_request(
    context: NotificationProjectionEventContextWire,
    prefixes: Vec<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        PENDING_ACTION_EVENT_CLEANED_UP,
        NotificationProjectionEventPayloadWire::PendingActionCleanedUp {
            prefixes,
        },
    )
}

pub fn pending_action_store_rewrite_event_request(
    context: NotificationProjectionEventContextWire,
    store: PendingActionStoreWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    projection_event_request(
        context,
        PENDING_ACTION_EVENT_STORE_REWRITTEN,
        NotificationProjectionEventPayloadWire::PendingActionStoreRewritten {
            store,
        },
    )
}

pub fn notification_projection_snapshot(
    conn: &Connection,
    include_dismissed: bool,
) -> Result<NotificationStoreSnapshotWire, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT notification_json
        FROM notifications
        WHERE (?1 OR dismissed = 0)
        ORDER BY source_order ASC, id ASC
        "#,
    )?;
    let rows =
        stmt.query_map([include_dismissed], |row| row.get::<_, String>(0))?;
    let mut notifications = Vec::new();
    for row in rows {
        notifications.push(serde_json::from_str(&row?)?);
    }
    let counts = counts_for(&notifications);
    Ok(NotificationStoreSnapshotWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        stats: NotificationStoreStatsWire {
            total_lines: notifications.len() as u64,
            loaded_rows: notifications.len() as u64,
            ..NotificationStoreStatsWire::default()
        },
        notifications,
        counts,
        expired_ids: Vec::new(),
    })
}

pub fn projected_pending_action_store(
    conn: &Connection,
) -> Result<PendingActionStoreWire, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT prefix, pending_action_json
        FROM notification_pending_actions
        ORDER BY prefix ASC
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut actions = BTreeMap::new();
    for row in rows {
        let (prefix, json) = row?;
        actions.insert(prefix, serde_json::from_str(&json)?);
    }
    Ok(PendingActionStoreWire {
        schema_version:
            crate::notifications::PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
        actions,
    })
}

pub fn notification_projection_counts(
    conn: &Connection,
) -> Result<NotificationProjectionFacetCountsWire, ProjectionError> {
    let all = notification_projection_snapshot(conn, true)?.notifications;
    let mut counts = NotificationProjectionFacetCountsWire::default();
    let visible_counts = counts_for(
        &all.iter()
            .filter(|notification| !notification.dismissed)
            .cloned()
            .collect::<Vec<_>>(),
    );
    counts.priority = visible_counts.priority;
    counts.errors = visible_counts.errors;
    counts.rest = visible_counts.rest;
    counts.muted = visible_counts.muted;
    for notification in &all {
        if notification.dismissed {
            counts.dismissed += 1;
        } else {
            counts.active += 1;
        }
        if notification.read {
            counts.read += 1;
        } else {
            counts.unread += 1;
        }
    }
    counts.pending_actions = conn.query_row(
        "SELECT COUNT(*) FROM notification_pending_actions",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    counts.stale_pending_actions = conn.query_row(
        "SELECT COUNT(*) FROM notification_pending_actions WHERE state = 'Stale'",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    Ok(counts)
}

fn projection_event_request(
    context: NotificationProjectionEventContextWire,
    event_type: &str,
    payload: NotificationProjectionEventPayloadWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    Ok(EventAppendRequestWire {
        created_at: context.created_at,
        source: context.source,
        host_id: context.host_id,
        project_id: context.project_id,
        event_type: event_type.to_string(),
        payload: serde_json::to_value(payload)?,
        idempotency_key: context.idempotency_key,
        causality: Vec::new(),
        source_path: context.source_path,
        source_revision: context.source_revision,
    })
}

fn apply_notification_projection_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !is_notification_event(&event.event_type) {
        return Ok(());
    }

    let payload: NotificationProjectionEventPayloadWire =
        serde_json::from_value(event.payload.clone())?;
    match payload {
        NotificationProjectionEventPayloadWire::NotificationAppended {
            notification,
        } => {
            upsert_notification_tx(conn, event, &notification, event.seq)?;
        }
        NotificationProjectionEventPayloadWire::NotificationsRewritten {
            notifications,
        }
        | NotificationProjectionEventPayloadWire::NotificationStateUpdated {
            notifications,
            ..
        } => {
            replace_notifications_tx(conn, event, &notifications)?;
        }
        NotificationProjectionEventPayloadWire::PendingActionRegistered {
            action,
        }
        | NotificationProjectionEventPayloadWire::PendingActionUpdated {
            action,
        } => {
            upsert_pending_action_tx(conn, event, &action)?;
        }
        NotificationProjectionEventPayloadWire::PendingActionCleanedUp {
            prefixes,
        } => {
            for prefix in prefixes {
                conn.execute(
                    "DELETE FROM notification_pending_actions WHERE prefix = ?1",
                    [prefix],
                )?;
            }
        }
        NotificationProjectionEventPayloadWire::PendingActionStoreRewritten {
            store,
        } => {
            conn.execute("DELETE FROM notification_pending_actions", [])?;
            for action in store.actions.values() {
                upsert_pending_action_tx(conn, event, action)?;
            }
        }
    }
    Ok(())
}

fn is_notification_event(event_type: &str) -> bool {
    matches!(
        event_type,
        NOTIFICATION_EVENT_APPENDED
            | NOTIFICATION_EVENT_REWRITTEN
            | NOTIFICATION_EVENT_STATE_UPDATED
            | PENDING_ACTION_EVENT_REGISTERED
            | PENDING_ACTION_EVENT_UPDATED
            | PENDING_ACTION_EVENT_CLEANED_UP
            | PENDING_ACTION_EVENT_STORE_REWRITTEN
    )
}

fn replace_notifications_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    notifications: &[NotificationWire],
) -> Result<(), ProjectionError> {
    conn.execute("DELETE FROM notification_search_fts", [])?;
    conn.execute("DELETE FROM notifications", [])?;
    for (idx, notification) in notifications.iter().enumerate() {
        upsert_notification_tx(conn, event, notification, idx as i64)?;
    }
    Ok(())
}

fn upsert_notification_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    notification: &NotificationWire,
    source_order: i64,
) -> Result<(), ProjectionError> {
    let notes_json = serde_json::to_string(&notification.notes)?;
    let files_json = serde_json::to_string(&notification.files)?;
    let action_data_json = serde_json::to_string(&notification.action_data)?;
    let notification_json = serde_json::to_string(notification)?;
    conn.execute(
        r#"
        INSERT INTO notifications (
            id, project_id, host_id, timestamp, sender, notes_json, files_json,
            action, action_data_json, read, dismissed, silent, muted,
            snooze_until, notification_json, source_order, updated_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17
        )
        ON CONFLICT(id) DO UPDATE SET
            project_id = excluded.project_id,
            host_id = excluded.host_id,
            timestamp = excluded.timestamp,
            sender = excluded.sender,
            notes_json = excluded.notes_json,
            files_json = excluded.files_json,
            action = excluded.action,
            action_data_json = excluded.action_data_json,
            read = excluded.read,
            dismissed = excluded.dismissed,
            silent = excluded.silent,
            muted = excluded.muted,
            snooze_until = excluded.snooze_until,
            notification_json = excluded.notification_json,
            source_order = excluded.source_order,
            updated_seq = excluded.updated_seq
        "#,
        params![
            notification.id,
            event.project_id,
            event.host_id,
            notification.timestamp,
            notification.sender,
            notes_json,
            files_json,
            notification.action,
            action_data_json,
            notification.read,
            notification.dismissed,
            notification.silent,
            notification.muted,
            notification.snooze_until,
            notification_json,
            source_order,
            event.seq,
        ],
    )?;
    conn.execute(
        "DELETE FROM notification_search_fts WHERE notification_id = ?1",
        [&notification.id],
    )?;
    conn.execute(
        "INSERT INTO notification_search_fts(notification_id, content) VALUES (?1, ?2)",
        params![notification.id, notification_search_text(notification)],
    )?;
    Ok(())
}

fn upsert_pending_action_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    action: &PendingActionWire,
) -> Result<(), ProjectionError> {
    let action_data_json = serde_json::to_string(&action.action_data)?;
    let files_json = serde_json::to_string(&action.files)?;
    let transports_json = serde_json::to_string(&action.transports)?;
    let pending_action_json = serde_json::to_string(action)?;
    conn.execute(
        r#"
        INSERT INTO notification_pending_actions (
            prefix, project_id, host_id, notification_id, action_kind, action,
            action_data_json, files_json, created_at_unix, updated_at_unix,
            stale_deadline_unix, transports_json, state, pending_action_json,
            updated_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
        )
        ON CONFLICT(prefix) DO UPDATE SET
            project_id = excluded.project_id,
            host_id = excluded.host_id,
            notification_id = excluded.notification_id,
            action_kind = excluded.action_kind,
            action = excluded.action,
            action_data_json = excluded.action_data_json,
            files_json = excluded.files_json,
            created_at_unix = excluded.created_at_unix,
            updated_at_unix = excluded.updated_at_unix,
            stale_deadline_unix = excluded.stale_deadline_unix,
            transports_json = excluded.transports_json,
            state = excluded.state,
            pending_action_json = excluded.pending_action_json,
            updated_seq = excluded.updated_seq
        "#,
        params![
            action.prefix,
            event.project_id,
            event.host_id,
            action.notification_id,
            format!("{:?}", action.action_kind),
            action.action,
            action_data_json,
            files_json,
            action.created_at_unix,
            action.updated_at_unix,
            action.stale_deadline_unix,
            transports_json,
            format!("{:?}", action.state),
            pending_action_json,
            event.seq,
        ],
    )?;
    Ok(())
}

fn counts_for(notifications: &[NotificationWire]) -> NotificationCountsWire {
    let mut counts = NotificationCountsWire::default();
    for notification in notifications {
        if notification.read || notification.silent {
            continue;
        }
        if notification.muted {
            counts.muted += 1;
        } else if mobile_notification_error_from_wire(notification) {
            counts.errors += 1;
        } else if mobile_notification_priority_from_wire(notification) {
            counts.priority += 1;
        } else {
            counts.rest += 1;
        }
    }
    counts
}

fn notification_search_text(notification: &NotificationWire) -> String {
    let mut parts = vec![
        notification.id.clone(),
        notification.timestamp.clone(),
        notification.sender.clone(),
    ];
    parts.extend(notification.notes.clone());
    parts.extend(notification.files.clone());
    if let Some(action) = &notification.action {
        parts.push(action.clone());
    }
    for (key, value) in &notification.action_data {
        parts.push(key.clone());
        parts.push(value.clone());
    }
    parts.join("\n")
}

#[allow(dead_code)]
fn payload_kind(payload: &JsonValue) -> Option<&str> {
    payload.get("kind").and_then(JsonValue::as_str)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::notifications::{
        pending_action_from_notification, MobileActionStateWire,
        NotificationStateUpdateWire,
    };

    fn context() -> NotificationProjectionEventContextWire {
        NotificationProjectionEventContextWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "notification-projection-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: None,
            source_path: Some("notifications.jsonl".to_string()),
            source_revision: None,
        }
    }

    fn notification(id: &str) -> NotificationWire {
        NotificationWire {
            id: id.to_string(),
            timestamp: "2026-05-01T01:02:03+00:00".to_string(),
            sender: "test-sender".to_string(),
            notes: Vec::new(),
            files: Vec::new(),
            action: None,
            action_data: BTreeMap::new(),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        }
    }

    #[test]
    fn append_rewrite_and_state_update_project_notifications() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let mut priority = notification("priority");
        priority.action = Some("PlanApproval".to_string());
        let mut error = notification("error");
        error.sender = "user-agent".to_string();
        error.action = Some("ViewErrorReport".to_string());

        db.append_notification_projection_event(
            notification_append_event_request(context(), priority.clone())
                .unwrap(),
        )
        .unwrap();
        db.append_notification_projection_event(
            notification_append_event_request(context(), error.clone())
                .unwrap(),
        )
        .unwrap();

        let snapshot =
            notification_projection_snapshot(db.connection(), false).unwrap();
        assert_eq!(snapshot.notifications, vec![priority.clone(), error]);
        assert_eq!(snapshot.counts.priority, 1);
        assert_eq!(snapshot.counts.errors, 1);

        priority.read = true;
        db.append_notification_projection_event(
            notification_state_update_event_request(
                context(),
                NotificationStateUpdateWire::MarkRead {
                    id: "priority".to_string(),
                },
                vec![priority.clone()],
            )
            .unwrap(),
        )
        .unwrap();

        let snapshot =
            notification_projection_snapshot(db.connection(), false).unwrap();
        assert_eq!(snapshot.notifications, vec![priority]);
        assert_eq!(snapshot.counts.priority, 0);
        assert_eq!(snapshot.counts.errors, 0);

        let counts = notification_projection_counts(db.connection()).unwrap();
        assert_eq!(counts.active, 1);
        assert_eq!(counts.read, 1);
    }

    #[test]
    fn rewrites_are_deterministic_and_do_not_duplicate_ids() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let first = notification("same");
        let mut second = notification("same");
        second.sender = "replacement".to_string();

        db.append_notification_projection_event(
            notification_rewrite_event_request(
                context(),
                vec![first, second.clone()],
            )
            .unwrap(),
        )
        .unwrap();

        let snapshot =
            notification_projection_snapshot(db.connection(), true).unwrap();
        assert_eq!(snapshot.notifications, vec![second]);
    }

    #[test]
    fn pending_actions_register_update_cleanup_and_rewrite() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let mut n = notification("abcdef01-notification");
        n.action = Some("PlanApproval".to_string());
        n.action_data
            .insert("response_dir".to_string(), "/tmp/plan".to_string());
        let mut action = pending_action_from_notification(&n, 10.0).unwrap();

        db.append_notification_projection_event(
            pending_action_register_event_request(context(), action.clone())
                .unwrap(),
        )
        .unwrap();

        action.state = MobileActionStateWire::Stale;
        action.updated_at_unix = 20.0;
        db.append_notification_projection_event(
            pending_action_update_event_request(context(), action.clone())
                .unwrap(),
        )
        .unwrap();

        let store = projected_pending_action_store(db.connection()).unwrap();
        assert_eq!(
            store.actions[&action.prefix].state,
            MobileActionStateWire::Stale
        );
        let counts = notification_projection_counts(db.connection()).unwrap();
        assert_eq!(counts.pending_actions, 1);
        assert_eq!(counts.stale_pending_actions, 1);

        db.append_notification_projection_event(
            pending_action_cleanup_event_request(
                context(),
                vec![action.prefix.clone()],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(projected_pending_action_store(db.connection())
            .unwrap()
            .actions
            .is_empty());

        let mut store = PendingActionStoreWire::default();
        store.actions.insert(action.prefix.clone(), action.clone());
        db.append_notification_projection_event(
            pending_action_store_rewrite_event_request(
                context(),
                store.clone(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            projected_pending_action_store(db.connection()).unwrap(),
            store
        );
    }

    #[test]
    fn replay_rebuilds_notification_and_pending_action_rows() {
        let mut live = ProjectionDb::open_in_memory().unwrap();
        let mut n = notification("abcdef01-notification");
        n.action = Some("PlanApproval".to_string());
        n.action_data
            .insert("response_dir".to_string(), "/tmp/plan".to_string());
        let action = pending_action_from_notification(&n, 10.0).unwrap();

        live.append_notification_projection_event(
            notification_append_event_request(context(), n.clone()).unwrap(),
        )
        .unwrap();
        live.append_notification_projection_event(
            pending_action_register_event_request(context(), action.clone())
                .unwrap(),
        )
        .unwrap();

        let mut replayed = ProjectionDb::open_in_memory().unwrap();
        for event in live.events_after(0, None).unwrap() {
            replayed
                .append_event(EventAppendRequestWire {
                    created_at: Some(event.created_at),
                    source: event.source,
                    host_id: event.host_id,
                    project_id: event.project_id,
                    event_type: event.event_type,
                    payload: event.payload,
                    idempotency_key: None,
                    causality: event.causality,
                    source_path: event.source_path,
                    source_revision: event.source_revision,
                })
                .unwrap();
        }

        let mut applier = NotificationProjectionApplier;
        replayed.replay_events(0, &mut [&mut applier]).unwrap();

        assert_eq!(
            notification_projection_snapshot(live.connection(), true).unwrap(),
            notification_projection_snapshot(replayed.connection(), true)
                .unwrap()
        );
        assert_eq!(
            projected_pending_action_store(live.connection()).unwrap(),
            projected_pending_action_store(replayed.connection()).unwrap()
        );
    }
}
