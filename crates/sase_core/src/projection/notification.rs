use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};

use crate::notifications::{
    mobile_notification_error_from_wire,
    mobile_notification_priority_from_wire, MobileActionStateWire,
    NotificationCountsWire, NotificationStateUpdateWire, NotificationWire,
    PendingActionWire,
};

use super::{
    NotificationEventKind, ProjectionError, ProjectionEvent,
    ProjectionEventType,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAppendedPayload {
    pub notification: NotificationWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationSnapshotRewrittenPayload {
    #[serde(default)]
    pub notifications: Vec<NotificationWire>,
    #[serde(default)]
    pub pending_actions: Vec<PendingActionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationStateUpdatedPayload {
    pub update: NotificationStateUpdateWire,
    #[serde(default)]
    pub notifications: Vec<NotificationWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionPayload {
    pub action: PendingActionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingActionCleanedPayload {
    #[serde(default)]
    pub prefixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationTombstonePayload {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationProjectionSnapshot {
    pub notifications: Vec<NotificationProjectionRow>,
    pub pending_actions: Vec<PendingActionProjectionRow>,
    pub search: Vec<NotificationSearchRow>,
    pub counts: NotificationCountsWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationProjectionRow {
    pub id: String,
    pub timestamp: String,
    pub sender: String,
    pub action: Option<String>,
    pub read: bool,
    pub dismissed: bool,
    pub silent: bool,
    pub muted: bool,
    pub snooze_until: Option<String>,
    pub wire: NotificationWire,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionProjectionRow {
    pub prefix: String,
    pub notification_id: String,
    pub action_kind: String,
    pub action: String,
    pub state: MobileActionStateWire,
    pub created_at_unix: f64,
    pub updated_at_unix: f64,
    pub stale_deadline_unix: f64,
    pub wire: PendingActionWire,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationSearchRow {
    pub id: String,
    pub summary: String,
}

pub fn apply_notification_event(
    tx: &Transaction<'_>,
    event: &ProjectionEvent,
) -> Result<(), ProjectionError> {
    let ProjectionEventType::Notification(kind) = &event.event_type else {
        return Ok(());
    };
    match kind {
        NotificationEventKind::Appended => {
            let payload: NotificationAppendedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_notification(
                tx,
                event.seq,
                &event.timestamp,
                &payload.notification,
            )?;
        }
        NotificationEventKind::SnapshotRewritten => {
            let payload: NotificationSnapshotRewrittenPayload =
                serde_json::from_value(event.payload.clone())?;
            tx.execute("DELETE FROM notification_search_fts", [])?;
            tx.execute("DELETE FROM notifications", [])?;
            tx.execute("DELETE FROM pending_actions", [])?;
            for notification in payload.notifications {
                upsert_notification(
                    tx,
                    event.seq,
                    &event.timestamp,
                    &notification,
                )?;
            }
            for action in payload.pending_actions {
                upsert_pending_action(
                    tx,
                    event.seq,
                    &event.timestamp,
                    &action,
                )?;
            }
        }
        NotificationEventKind::StateUpdated => {
            let payload: NotificationStateUpdatedPayload =
                serde_json::from_value(event.payload.clone())?;
            for notification in payload.notifications {
                upsert_notification(
                    tx,
                    event.seq,
                    &event.timestamp,
                    &notification,
                )?;
            }
        }
        NotificationEventKind::PendingActionRegistered
        | NotificationEventKind::PendingActionUpdated => {
            let payload: PendingActionPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_pending_action(
                tx,
                event.seq,
                &event.timestamp,
                &payload.action,
            )?;
        }
        NotificationEventKind::PendingActionCleaned => {
            let payload: PendingActionCleanedPayload =
                serde_json::from_value(event.payload.clone())?;
            for prefix in payload.prefixes {
                tx.execute(
                    "DELETE FROM pending_actions WHERE prefix = ?1",
                    [prefix],
                )?;
            }
        }
        NotificationEventKind::Tombstone => {
            let payload: NotificationTombstonePayload =
                serde_json::from_value(event.payload.clone())?;
            delete_notification(tx, &payload.id)?;
        }
    }
    Ok(())
}

pub fn notification_projection_snapshot(
    conn: &Connection,
) -> Result<NotificationProjectionSnapshot, ProjectionError> {
    let notifications = notification_rows(conn)?;
    Ok(NotificationProjectionSnapshot {
        counts: counts_for_rows(&notifications),
        notifications,
        pending_actions: pending_action_rows(conn)?,
        search: search_rows(conn)?,
    })
}

pub fn notification_wires(
    conn: &Connection,
) -> Result<Vec<NotificationWire>, ProjectionError> {
    Ok(notification_rows(conn)?
        .into_iter()
        .map(|row| row.wire)
        .collect())
}

fn upsert_notification(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    notification: &NotificationWire,
) -> Result<(), ProjectionError> {
    let wire_json = serde_json::to_string(notification)?;
    tx.execute(
        "INSERT INTO notifications(
             id,
             timestamp,
             sender,
             action,
             read,
             dismissed,
             silent,
             muted,
             snooze_until,
             wire_json,
             updated_seq,
             updated_at
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(id) DO UPDATE SET
             timestamp = excluded.timestamp,
             sender = excluded.sender,
             action = excluded.action,
             read = excluded.read,
             dismissed = excluded.dismissed,
             silent = excluded.silent,
             muted = excluded.muted,
             snooze_until = excluded.snooze_until,
             wire_json = excluded.wire_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            notification.id,
            notification.timestamp,
            notification.sender,
            notification.action,
            bool_int(notification.read),
            bool_int(notification.dismissed),
            bool_int(notification.silent),
            bool_int(notification.muted),
            notification.snooze_until,
            wire_json,
            seq,
            timestamp,
        ],
    )?;
    replace_search_row(tx, notification)?;
    Ok(())
}

fn upsert_pending_action(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    action: &PendingActionWire,
) -> Result<(), ProjectionError> {
    let wire_json = serde_json::to_string(action)?;
    tx.execute(
        "INSERT INTO pending_actions(
             prefix,
             notification_id,
             action_kind,
             action,
             state,
             created_at_unix,
             updated_at_unix,
             stale_deadline_unix,
             wire_json,
             updated_seq,
             updated_at
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(prefix) DO UPDATE SET
             notification_id = excluded.notification_id,
             action_kind = excluded.action_kind,
             action = excluded.action,
             state = excluded.state,
             created_at_unix = excluded.created_at_unix,
             updated_at_unix = excluded.updated_at_unix,
             stale_deadline_unix = excluded.stale_deadline_unix,
             wire_json = excluded.wire_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            action.prefix,
            action.notification_id,
            format!("{:?}", action.action_kind),
            action.action,
            format!("{:?}", action.state),
            action.created_at_unix,
            action.updated_at_unix,
            action.stale_deadline_unix,
            wire_json,
            seq,
            timestamp,
        ],
    )?;
    Ok(())
}

fn delete_notification(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<(), ProjectionError> {
    tx.execute("DELETE FROM notification_search_fts WHERE id = ?1", [id])?;
    tx.execute(
        "DELETE FROM pending_actions WHERE notification_id = ?1",
        [id],
    )?;
    tx.execute("DELETE FROM notifications WHERE id = ?1", [id])?;
    Ok(())
}

fn replace_search_row(
    tx: &Transaction<'_>,
    notification: &NotificationWire,
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM notification_search_fts WHERE id = ?1",
        [&notification.id],
    )?;
    tx.execute(
        "INSERT INTO notification_search_fts(id, summary) VALUES(?1, ?2)",
        params![notification.id, notification_summary(notification)],
    )?;
    Ok(())
}

fn notification_summary(notification: &NotificationWire) -> String {
    let mut parts = vec![notification.sender.as_str()];
    if let Some(action) = &notification.action {
        parts.push(action.as_str());
    }
    for note in &notification.notes {
        parts.push(note.as_str());
    }
    for file in &notification.files {
        parts.push(file.as_str());
    }
    let summary = parts.join("\n");
    summary.chars().take(4096).collect()
}

fn counts_for_rows(
    rows: &[NotificationProjectionRow],
) -> NotificationCountsWire {
    let mut counts = NotificationCountsWire::default();
    for row in rows {
        let notification = &row.wire;
        if notification.dismissed || notification.read || notification.silent {
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

fn notification_rows(
    conn: &Connection,
) -> Result<Vec<NotificationProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             id,
             timestamp,
             sender,
             action,
             read,
             dismissed,
             silent,
             muted,
             snooze_until,
             wire_json,
             updated_seq
         FROM notifications
         ORDER BY id",
    )?;
    let rows = stmt.query_map([], |row| {
        let wire_json: String = row.get(9)?;
        let wire = serde_json::from_str(&wire_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                wire_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        Ok(NotificationProjectionRow {
            id: row.get(0)?,
            timestamp: row.get(1)?,
            sender: row.get(2)?,
            action: row.get(3)?,
            read: row.get::<_, i64>(4)? != 0,
            dismissed: row.get::<_, i64>(5)? != 0,
            silent: row.get::<_, i64>(6)? != 0,
            muted: row.get::<_, i64>(7)? != 0,
            snooze_until: row.get(8)?,
            wire,
            updated_seq: row.get(10)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn pending_action_rows(
    conn: &Connection,
) -> Result<Vec<PendingActionProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             prefix,
             notification_id,
             action_kind,
             action,
             created_at_unix,
             updated_at_unix,
             stale_deadline_unix,
             wire_json,
             updated_seq
         FROM pending_actions
         ORDER BY prefix",
    )?;
    let rows = stmt.query_map([], |row| {
        let wire_json: String = row.get(7)?;
        let wire: PendingActionWire = serde_json::from_str(&wire_json)
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    wire_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
        Ok(PendingActionProjectionRow {
            prefix: row.get(0)?,
            notification_id: row.get(1)?,
            action_kind: row.get(2)?,
            action: row.get(3)?,
            state: wire.state,
            created_at_unix: row.get(4)?,
            updated_at_unix: row.get(5)?,
            stale_deadline_unix: row.get(6)?,
            wire,
            updated_seq: row.get(8)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn search_rows(
    conn: &Connection,
) -> Result<Vec<NotificationSearchRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT id, summary FROM notification_search_fts ORDER BY id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(NotificationSearchRow {
            id: row.get(0)?,
            summary: row.get(1)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn bool_int(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}
