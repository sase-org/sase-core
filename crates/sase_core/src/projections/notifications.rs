use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use chrono::{DateTime, NaiveDateTime};
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventEnvelopeWire,
    EventSourceWire,
};
use super::indexing::{
    source_event_idempotency_key, source_fingerprint_from_path,
    ShadowDiffCategoryWire, ShadowDiffCountsWire, ShadowDiffRecordWire,
    ShadowDiffReportWire, SourceChangeOperationWire, SourceChangeWire,
    SourceIdentityWire, INDEXING_WIRE_SCHEMA_VERSION,
};
use super::read::{
    NotificationReadListRequestWire, NotificationReadListResponseWire,
    ProjectionPageInfoWire, ProjectionPayloadBoundWire,
    ProjectionSnapshotReadWire, PROJECTION_READ_WIRE_SCHEMA_VERSION,
};
use super::replay::ProjectionApplier;
use crate::notifications::{
    legacy_telegram_pending_actions_path, pending_action_store_path,
    read_notifications_snapshot_with_options, read_pending_action_store,
};
use crate::notifications::{
    mobile_notification_error_from_wire,
    mobile_notification_priority_from_wire, NotificationAgentKeyWire,
    NotificationCountsWire, NotificationStateUpdateWire,
    NotificationStoreSnapshotWire, NotificationStoreStatsWire,
    NotificationWire, PendingActionStoreWire, PendingActionWire,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
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

const NOTIFICATION_INDEXING_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationSourceKindWire {
    Notifications,
    PendingActions,
    LegacyTelegramPendingActions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationSourcePathsWire {
    #[serde(default = "notification_indexing_schema_version")]
    pub schema_version: u32,
    pub notifications_path: String,
    pub pending_actions_path: String,
    pub legacy_telegram_pending_actions_path: String,
}

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
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
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

pub fn notification_source_paths(
    sase_home: &Path,
) -> NotificationSourcePathsWire {
    NotificationSourcePathsWire {
        schema_version: NOTIFICATION_INDEXING_WIRE_SCHEMA_VERSION,
        notifications_path: sase_home
            .join("notifications")
            .join("notifications.jsonl")
            .to_string_lossy()
            .to_string(),
        pending_actions_path: pending_action_store_path(sase_home)
            .to_string_lossy()
            .to_string(),
        legacy_telegram_pending_actions_path:
            legacy_telegram_pending_actions_path(sase_home)
                .to_string_lossy()
                .to_string(),
    }
}

pub fn notification_source_kind_from_path(
    sase_home: &Path,
    path: &Path,
) -> Option<NotificationSourceKindWire> {
    let paths = notification_source_paths(sase_home);
    let path = path.to_string_lossy();
    if path == paths.notifications_path {
        Some(NotificationSourceKindWire::Notifications)
    } else if path == paths.pending_actions_path {
        Some(NotificationSourceKindWire::PendingActions)
    } else if path == paths.legacy_telegram_pending_actions_path {
        Some(NotificationSourceKindWire::LegacyTelegramPendingActions)
    } else {
        None
    }
}

pub fn notification_source_change_from_path(
    sase_home: &Path,
    path: &Path,
    operation: SourceChangeOperationWire,
    reason: Option<String>,
) -> Option<SourceChangeWire> {
    notification_source_kind_from_path(sase_home, path)?;
    Some(SourceChangeWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        identity: notification_source_identity(path),
        operation,
        reason,
    })
}

pub fn notification_backfill_event_requests(
    sase_home: &Path,
    source: EventSourceWire,
    host_id: impl Into<String>,
) -> Result<Vec<EventAppendRequestWire>, ProjectionError> {
    let host_id = host_id.into();
    let paths = notification_source_paths(sase_home);
    let context = NotificationProjectionEventContextWire {
        created_at: None,
        source,
        host_id,
        project_id: "home".to_string(),
        idempotency_key: None,
        source_path: Some(paths.notifications_path.clone()),
        source_revision: None,
    };
    Ok(vec![
        notification_rewrite_event_request(
            NotificationProjectionEventContextWire {
                idempotency_key: Some(notification_source_idempotency_key(
                    SourceChangeOperationWire::Rewrite,
                    Path::new(&paths.notifications_path),
                )),
                ..context.clone()
            },
            expected_notification_snapshot(&paths)?.notifications,
        )?,
        pending_action_store_rewrite_event_request(
            NotificationProjectionEventContextWire {
                idempotency_key: Some(notification_source_idempotency_key(
                    SourceChangeOperationWire::Rewrite,
                    Path::new(&paths.pending_actions_path),
                )),
                source_path: Some(paths.pending_actions_path.clone()),
                ..context
            },
            expected_pending_action_store(&paths)?,
        )?,
    ])
}

pub fn notification_event_requests_for_source_change(
    sase_home: &Path,
    change: &SourceChangeWire,
    source: EventSourceWire,
    host_id: impl Into<String>,
) -> Result<Vec<EventAppendRequestWire>, ProjectionError> {
    let kind = notification_source_kind_from_path(
        sase_home,
        Path::new(&change.identity.source_path),
    );
    let Some(kind) = kind else {
        return Ok(Vec::new());
    };
    let paths = notification_source_paths(sase_home);
    let context = NotificationProjectionEventContextWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: change
            .identity
            .project_id
            .clone()
            .unwrap_or_else(|| "home".to_string()),
        idempotency_key: Some(notification_source_idempotency_key(
            change.operation.clone(),
            Path::new(&change.identity.source_path),
        )),
        source_path: Some(change.identity.source_path.clone()),
        source_revision: None,
    };
    match kind {
        NotificationSourceKindWire::Notifications => {
            Ok(vec![notification_rewrite_event_request(
                context,
                expected_notification_snapshot(&paths)?.notifications,
            )?])
        }
        NotificationSourceKindWire::PendingActions
        | NotificationSourceKindWire::LegacyTelegramPendingActions => {
            Ok(vec![pending_action_store_rewrite_event_request(
                context,
                expected_pending_action_store(&paths)?,
            )?])
        }
    }
}

pub fn notification_shadow_diff(
    conn: &Connection,
    sase_home: &Path,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    let paths = notification_source_paths(sase_home);
    let mut records = Vec::new();
    let expected_snapshot = match expected_notification_snapshot(&paths) {
        Ok(snapshot) => {
            if snapshot.stats.invalid_json_lines > 0
                || snapshot.stats.invalid_record_lines > 0
            {
                records.push(notification_diff_record(
                    ShadowDiffCategoryWire::Corrupt,
                    paths.notifications_path.clone(),
                    None,
                    format!(
                        "notification source has {} invalid JSON line(s) and {} invalid record line(s)",
                        snapshot.stats.invalid_json_lines,
                        snapshot.stats.invalid_record_lines
                    ),
                ));
            }
            snapshot
        }
        Err(error) => {
            records.push(notification_diff_record(
                ShadowDiffCategoryWire::Corrupt,
                paths.notifications_path.clone(),
                None,
                format!("failed to load notification source: {error}"),
            ));
            NotificationStoreSnapshotWire {
                schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                stats: NotificationStoreStatsWire::default(),
                notifications: Vec::new(),
                counts: NotificationCountsWire::default(),
                expired_ids: Vec::new(),
            }
        }
    };
    let projected_snapshot = notification_projection_snapshot(conn, true)?;
    diff_notifications(
        &mut records,
        &paths.notifications_path,
        &expected_snapshot.notifications,
        &projected_snapshot.notifications,
    )?;

    let expected_pending = match expected_pending_action_store(&paths) {
        Ok(store) => store,
        Err(error) => {
            records.push(notification_diff_record(
                ShadowDiffCategoryWire::Corrupt,
                paths.pending_actions_path.clone(),
                None,
                format!("failed to load pending-action source: {error}"),
            ));
            PendingActionStoreWire::default()
        }
    };
    let projected_pending = projected_pending_action_store(conn)?;
    diff_pending_actions(
        &mut records,
        &paths.pending_actions_path,
        &expected_pending,
        &projected_pending,
    )?;

    records.sort_by(|left, right| {
        (
            notification_diff_sort_key(&left.category),
            &left.source_path,
            &left.handle,
        )
            .cmp(&(
                notification_diff_sort_key(&right.category),
                &right.source_path,
                &right.handle,
            ))
    });
    Ok(ShadowDiffReportWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: NOTIFICATION_PROJECTION_NAME.to_string(),
        counts: notification_diff_counts(&records),
        records,
    })
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

pub fn notification_projection_page(
    conn: &Connection,
    request: &NotificationReadListRequestWire,
    snapshot_id: impl Into<String>,
    max_payload_bytes: u32,
) -> Result<NotificationReadListResponseWire, ProjectionError> {
    let limit = request.page.limit.clamp(1, 500);
    let offset = request
        .page
        .cursor
        .as_deref()
        .map(parse_notification_cursor)
        .transpose()?
        .unwrap_or(0);
    let fetch_limit = limit.saturating_add(1);
    let page = filtered_notification_page(
        conn,
        request,
        offset,
        fetch_limit,
        max_payload_bytes,
    )?;
    Ok(NotificationReadListResponseWire {
        schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
        snapshot: ProjectionSnapshotReadWire {
            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
            snapshot_id: snapshot_id.into(),
        },
        page: ProjectionPageInfoWire {
            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
            next_cursor: page.next_cursor,
        },
        notifications: page.notifications,
        counts: notification_projection_counts(conn)?,
        bounded: ProjectionPayloadBoundWire {
            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
            max_payload_bytes,
            truncated: page.truncated,
        },
    })
}

pub fn notification_projection_detail(
    conn: &Connection,
    notification_id: &str,
) -> Result<Option<NotificationWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT notification_json
        FROM notifications
        WHERE id = ?1
        "#,
    )?;
    let mut rows = stmt.query([notification_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&row.get::<_, String>(0)?)?))
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

struct NotificationPageRows {
    notifications: Vec<NotificationWire>,
    next_cursor: Option<String>,
    truncated: bool,
}

fn filtered_notification_page(
    conn: &Connection,
    request: &NotificationReadListRequestWire,
    offset: u32,
    fetch_limit: u32,
    max_payload_bytes: u32,
) -> Result<NotificationPageRows, ProjectionError> {
    let mut sql = String::from("SELECT notification_json FROM notifications");
    let mut values = Vec::new();
    append_notification_filters(&mut sql, &mut values, request);
    sql.push_str(" ORDER BY source_order ASC, id ASC LIMIT ? OFFSET ?");
    values.push(SqlValue::Integer(i64::from(fetch_limit)));
    values.push(SqlValue::Integer(i64::from(offset)));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(values.iter()), |row| {
        row.get::<_, String>(0)
    })?;

    let mut notifications = Vec::new();
    let mut payload_bytes = 0_u32;
    let mut truncated = false;
    let mut next_cursor = None;
    let limit = fetch_limit.saturating_sub(1);
    for (idx, row) in rows.enumerate() {
        let json = row?;
        let row_offset = offset + idx as u32;
        if idx as u32 >= limit {
            next_cursor = Some(row_offset.to_string());
            break;
        }
        let row_bytes = json.len().min(u32::MAX as usize) as u32;
        if payload_bytes.saturating_add(row_bytes) > max_payload_bytes {
            truncated = true;
            next_cursor = Some(
                if notifications.is_empty() {
                    row_offset.saturating_add(1)
                } else {
                    row_offset
                }
                .to_string(),
            );
            break;
        }
        payload_bytes = payload_bytes.saturating_add(row_bytes);
        notifications.push(serde_json::from_str(&json)?);
    }

    Ok(NotificationPageRows {
        notifications,
        next_cursor,
        truncated,
    })
}

fn append_notification_filters(
    sql: &mut String,
    values: &mut Vec<SqlValue>,
    request: &NotificationReadListRequestWire,
) {
    let mut clauses = Vec::new();
    if !request.include_dismissed {
        clauses.push("dismissed = 0".to_string());
    }
    if let Some(sender) =
        request.sender.as_deref().filter(|value| !value.is_empty())
    {
        clauses.push("sender = ?".to_string());
        values.push(SqlValue::Text(sender.to_string()));
    }
    if let Some(unread) = request.unread {
        clauses.push("read = ?".to_string());
        values.push(SqlValue::Integer(if unread { 0 } else { 1 }));
    }
    if let Some(query) =
        request.query.as_deref().filter(|value| !value.is_empty())
    {
        clauses.push(
            "id IN (
                SELECT notification_id
                FROM notification_search_fts
                WHERE notification_search_fts MATCH ?
            )"
            .to_string(),
        );
        values.push(SqlValue::Text(fts_phrase_query(query)));
    }
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
}

fn fts_phrase_query(query: &str) -> String {
    format!("\"{}\"", query.replace('"', "\"\""))
}

fn parse_notification_cursor(cursor: &str) -> Result<u32, ProjectionError> {
    cursor.parse::<u32>().map_err(|error| {
        ProjectionError::Invariant(format!(
            "invalid notification projection cursor '{cursor}': {error}"
        ))
    })
}

pub fn notification_projection_counts(
    conn: &Connection,
) -> Result<NotificationProjectionFacetCountsWire, ProjectionError> {
    let mut counts = conn.query_row(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN dismissed = 0 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN dismissed = 1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN read = 1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN read = 0 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE
                WHEN dismissed = 0 AND read = 0 AND silent = 0 AND muted = 1
                THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE
                WHEN dismissed = 0 AND read = 0 AND silent = 0 AND muted = 0
                    AND action = 'ViewErrorReport'
                    AND sender IN ('axe', 'user-agent')
                THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE
                WHEN dismissed = 0 AND read = 0 AND silent = 0 AND muted = 0
                    AND NOT (
                        action = 'ViewErrorReport'
                        AND sender IN ('axe', 'user-agent')
                    )
                    AND (
                        action IN (
                            'PlanApproval',
                            'UserQuestion',
                            'JumpToMentorReview'
                        )
                        OR sender IN ('axe', 'crs')
                    )
                THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE
                WHEN dismissed = 0 AND read = 0 AND silent = 0 AND muted = 0
                    AND NOT (
                        action = 'ViewErrorReport'
                        AND sender IN ('axe', 'user-agent')
                    )
                    AND NOT (
                        action IN (
                            'PlanApproval',
                            'UserQuestion',
                            'JumpToMentorReview'
                        )
                        OR sender IN ('axe', 'crs')
                    )
                THEN 1 ELSE 0 END), 0)
        FROM notifications
        "#,
        [],
        |row| {
            Ok(NotificationProjectionFacetCountsWire {
                active: row.get(0)?,
                dismissed: row.get(1)?,
                read: row.get(2)?,
                unread: row.get(3)?,
                muted: row.get(4)?,
                errors: row.get(5)?,
                priority: row.get(6)?,
                rest: row.get(7)?,
                pending_actions: 0,
                stale_pending_actions: 0,
            })
        },
    )?;
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
        } => {
            replace_notifications_tx(conn, event, &notifications)?;
        }
        NotificationProjectionEventPayloadWire::NotificationStateUpdated {
            update,
            notifications,
        } => {
            if notifications.is_empty() {
                apply_notification_state_update_tx(conn, event, &update)?;
            } else {
                replace_notifications_tx(conn, event, &notifications)?;
            }
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

fn apply_notification_state_update_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    update: &NotificationStateUpdateWire,
) -> Result<(), ProjectionError> {
    if let NotificationStateUpdateWire::RewriteAll { notifications } = update {
        return replace_notifications_tx(conn, event, notifications);
    }

    let mut rows = notification_update_candidates(conn, update)?;
    for (notification, source_order) in &mut rows {
        if mutate_notification_for_update(notification, update) {
            upsert_notification_tx(conn, event, notification, *source_order)?;
        }
    }
    Ok(())
}

fn notification_update_candidates(
    conn: &Connection,
    update: &NotificationStateUpdateWire,
) -> Result<Vec<(NotificationWire, i64)>, ProjectionError> {
    let mut sql = String::from(
        "SELECT notification_json, source_order FROM notifications",
    );
    let mut values = Vec::new();
    match update {
        NotificationStateUpdateWire::MarkRead { id }
        | NotificationStateUpdateWire::MarkDismissed { id }
        | NotificationStateUpdateWire::MarkMuted { id, .. }
        | NotificationStateUpdateWire::MarkSnoozed { id, .. } => {
            sql.push_str(" WHERE id = ?");
            values.push(SqlValue::Text(id.clone()));
        }
        NotificationStateUpdateWire::MarkAllRead => {
            sql.push_str(" WHERE read = 0");
        }
        NotificationStateUpdateWire::MarkManyDismissed { ids } => {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let placeholders =
                std::iter::repeat("?").take(ids.len()).collect::<Vec<_>>();
            sql.push_str(" WHERE id IN (");
            sql.push_str(&placeholders.join(", "));
            sql.push(')');
            values.extend(ids.iter().cloned().map(SqlValue::Text));
        }
        NotificationStateUpdateWire::ExpireSnoozes { .. } => {
            sql.push_str(" WHERE snooze_until IS NOT NULL");
        }
        NotificationStateUpdateWire::DismissMatchingAgents { .. } => {
            sql.push_str(
                " WHERE dismissed = 0
                    AND action IN (
                        'JumpToAgent',
                        'ViewErrorReport',
                        'PlanApproval',
                        'UserQuestion'
                    )",
            );
        }
        NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            ..
        }
        | NotificationStateUpdateWire::DismissAgentCompletions => {
            sql.push_str(
                " WHERE dismissed = 0
                    AND sender = 'user-agent'
                    AND action IN ('JumpToAgent', 'ViewErrorReport')",
            );
        }
        NotificationStateUpdateWire::RewriteAll { .. } => unreachable!(),
    }
    sql.push_str(" ORDER BY source_order ASC, id ASC");

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(values.iter()), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut candidates = Vec::new();
    for row in rows {
        let (json, source_order) = row?;
        candidates.push((serde_json::from_str(&json)?, source_order));
    }
    Ok(candidates)
}

fn mutate_notification_for_update(
    notification: &mut NotificationWire,
    update: &NotificationStateUpdateWire,
) -> bool {
    match update {
        NotificationStateUpdateWire::MarkRead { id } => {
            if notification.id == *id && !notification.read {
                notification.read = true;
                return true;
            }
        }
        NotificationStateUpdateWire::MarkAllRead => {
            if !notification.read {
                notification.read = true;
                return true;
            }
        }
        NotificationStateUpdateWire::MarkDismissed { id } => {
            if notification.id == *id && !notification.dismissed {
                notification.dismissed = true;
                return true;
            }
        }
        NotificationStateUpdateWire::MarkManyDismissed { ids } => {
            if !notification.dismissed && ids.contains(&notification.id) {
                notification.dismissed = true;
                return true;
            }
        }
        NotificationStateUpdateWire::MarkMuted { id, muted } => {
            if notification.id == *id
                && (notification.muted != *muted
                    || (!*muted && notification.snooze_until.is_some()))
            {
                notification.muted = *muted;
                if !*muted {
                    notification.snooze_until = None;
                }
                return true;
            }
        }
        NotificationStateUpdateWire::MarkSnoozed { id, until } => {
            if notification.id == *id
                && (!notification.muted
                    || notification.snooze_until.as_deref()
                        != Some(until.as_str()))
            {
                notification.muted = true;
                notification.snooze_until = Some(until.clone());
                return true;
            }
        }
        NotificationStateUpdateWire::ExpireSnoozes { now } => {
            if notification
                .snooze_until
                .as_deref()
                .map(|deadline| iso_timestamp_due(deadline, now))
                .unwrap_or(false)
                && (notification.muted || notification.snooze_until.is_some())
            {
                notification.muted = false;
                notification.snooze_until = None;
                return true;
            }
        }
        NotificationStateUpdateWire::DismissMatchingAgents { agents } => {
            if !notification.dismissed
                && matches_agent_notification(notification, agents)
            {
                notification.dismissed = true;
                return true;
            }
        }
        NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents,
        } => {
            if !notification.dismissed
                && matches_agent_completion_notification_for_agents(
                    notification,
                    agents,
                )
            {
                notification.dismissed = true;
                return true;
            }
        }
        NotificationStateUpdateWire::DismissAgentCompletions => {
            if !notification.dismissed
                && matches_agent_completion_notification(notification)
            {
                notification.dismissed = true;
                return true;
            }
        }
        NotificationStateUpdateWire::RewriteAll { .. } => unreachable!(),
    }
    false
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

fn matches_agent_notification(
    notification: &NotificationWire,
    agents: &[NotificationAgentKeyWire],
) -> bool {
    if agents.is_empty() {
        return false;
    }
    match notification.action.as_deref() {
        Some("JumpToAgent") => {
            let cl_name = notification.action_data.get("cl_name");
            let raw_suffix = notification.action_data.get("raw_suffix");
            match raw_suffix {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(raw_suffix) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(raw_suffix.as_str())
                }),
            }
        }
        Some("ViewErrorReport") if notification.sender == "user-agent" => {
            let cl_name = notification.action_data.get("cl_name");
            if cl_name.is_none() {
                return false;
            }
            let raw_suffix = notification.action_data.get("raw_suffix");
            match raw_suffix {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(raw_suffix) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(raw_suffix.as_str())
                }),
            }
        }
        Some("PlanApproval" | "UserQuestion") => {
            let cl_name = notification.action_data.get("agent_cl_name");
            let timestamp = notification
                .action_data
                .get("agent_timestamp")
                .and_then(|value| normalize_to_14_digit(value));
            match timestamp {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(timestamp) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(timestamp.as_str())
                }),
            }
        }
        _ => false,
    }
}

fn matches_agent_completion_notification(
    notification: &NotificationWire,
) -> bool {
    if notification.sender != "user-agent" {
        return false;
    }
    match notification.action.as_deref() {
        Some("JumpToAgent") | Some("ViewErrorReport") => notification
            .action_data
            .get("cl_name")
            .map(|value| !value.is_empty())
            .unwrap_or(false),
        _ => false,
    }
}

fn matches_agent_completion_notification_for_agents(
    notification: &NotificationWire,
    agents: &[NotificationAgentKeyWire],
) -> bool {
    if agents.is_empty() || !matches_agent_completion_notification(notification)
    {
        return false;
    }
    let cl_name = notification.action_data.get("cl_name");
    let raw_suffix = notification.action_data.get("raw_suffix");
    match raw_suffix {
        None => agents.iter().any(|agent| Some(&agent.cl_name) == cl_name),
        Some(raw_suffix) => agents.iter().any(|agent| {
            Some(&agent.cl_name) == cl_name
                && agent.raw_suffix.as_deref() == Some(raw_suffix.as_str())
        }),
    }
}

fn normalize_to_14_digit(ts: &str) -> Option<String> {
    if ts.len() == 14 && ts.bytes().all(|b| b.is_ascii_digit()) {
        return Some(ts.to_string());
    }
    if ts.len() == 13
        && ts.as_bytes().get(6) == Some(&b'_')
        && ts[..6].bytes().all(|b| b.is_ascii_digit())
        && ts[7..].bytes().all(|b| b.is_ascii_digit())
    {
        return Some(format!("20{}{}", &ts[..6], &ts[7..]));
    }
    None
}

fn iso_timestamp_due(deadline: &str, now: &str) -> bool {
    match (parse_iso_moment(deadline), parse_iso_moment(now)) {
        (Some(IsoMoment::Aware(deadline)), Some(IsoMoment::Aware(now))) => {
            deadline <= now
        }
        (Some(IsoMoment::Naive(deadline)), Some(IsoMoment::Naive(now))) => {
            deadline <= now
        }
        (Some(IsoMoment::Aware(deadline)), Some(IsoMoment::Naive(now))) => {
            deadline <= now.and_utc().timestamp_micros()
        }
        (Some(IsoMoment::Naive(deadline)), Some(IsoMoment::Aware(now))) => {
            deadline.and_utc().timestamp_micros() <= now
        }
        _ => false,
    }
}

enum IsoMoment {
    Aware(i64),
    Naive(NaiveDateTime),
}

fn parse_iso_moment(value: &str) -> Option<IsoMoment> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(IsoMoment::Aware(dt.timestamp_micros()));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(IsoMoment::Naive)
}

fn expected_notification_snapshot(
    paths: &NotificationSourcePathsWire,
) -> Result<NotificationStoreSnapshotWire, ProjectionError> {
    read_notifications_snapshot_with_options(
        Path::new(&paths.notifications_path),
        true,
        false,
    )
    .map_err(|error| {
        ProjectionError::Invariant(format!(
            "failed to read notification store: {error}"
        ))
    })
}

fn expected_pending_action_store(
    paths: &NotificationSourcePathsWire,
) -> Result<PendingActionStoreWire, ProjectionError> {
    read_pending_action_store(
        Path::new(&paths.pending_actions_path),
        Some(Path::new(&paths.legacy_telegram_pending_actions_path)),
    )
    .map_err(|error| {
        ProjectionError::Invariant(format!(
            "failed to read pending action store: {error}"
        ))
    })
}

fn notification_source_identity(path: &Path) -> SourceIdentityWire {
    SourceIdentityWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: NOTIFICATION_PROJECTION_NAME.to_string(),
        project_id: Some("home".to_string()),
        source_path: path.to_string_lossy().to_string(),
        is_archive: false,
        fingerprint: source_fingerprint_from_path(path, true),
        last_indexed_event_seq: None,
    }
}

fn notification_source_idempotency_key(
    operation: SourceChangeOperationWire,
    path: &Path,
) -> String {
    source_event_idempotency_key(
        &operation,
        &notification_source_identity(path),
    )
}

fn diff_notifications(
    records: &mut Vec<ShadowDiffRecordWire>,
    source_path: &str,
    expected: &[NotificationWire],
    projected: &[NotificationWire],
) -> Result<(), ProjectionError> {
    let expected = notifications_by_id(expected)?;
    let projected = notifications_by_id(projected)?;
    let keys = expected
        .keys()
        .chain(projected.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        match (expected.get(&key), projected.get(&key)) {
            (Some(_), None) => records.push(notification_diff_record(
                ShadowDiffCategoryWire::Missing,
                source_path.to_string(),
                Some(key.clone()),
                format!(
                    "notification '{key}' is present in source but missing from projection"
                ),
            )),
            (None, Some(_)) => records.push(notification_diff_record(
                ShadowDiffCategoryWire::Extra,
                source_path.to_string(),
                Some(key.clone()),
                format!(
                    "notification '{key}' is projected but absent from source"
                ),
            )),
            (Some(expected), Some(projected)) if expected != projected => {
                records.push(notification_diff_record(
                    ShadowDiffCategoryWire::Stale,
                    source_path.to_string(),
                    Some(key.clone()),
                    format!("notification '{key}' differs from source"),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn notifications_by_id(
    notifications: &[NotificationWire],
) -> Result<BTreeMap<String, JsonValue>, ProjectionError> {
    let mut rows = BTreeMap::new();
    for notification in notifications {
        rows.insert(
            notification.id.clone(),
            serde_json::to_value(notification)?,
        );
    }
    Ok(rows)
}

fn diff_pending_actions(
    records: &mut Vec<ShadowDiffRecordWire>,
    source_path: &str,
    expected: &PendingActionStoreWire,
    projected: &PendingActionStoreWire,
) -> Result<(), ProjectionError> {
    let expected = pending_actions_by_prefix(expected)?;
    let projected = pending_actions_by_prefix(projected)?;
    let keys = expected
        .keys()
        .chain(projected.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        match (expected.get(&key), projected.get(&key)) {
            (Some(_), None) => records.push(notification_diff_record(
                ShadowDiffCategoryWire::Missing,
                source_path.to_string(),
                Some(key.clone()),
                format!(
                    "pending action '{key}' is present in source but missing from projection"
                ),
            )),
            (None, Some(_)) => records.push(notification_diff_record(
                ShadowDiffCategoryWire::Extra,
                source_path.to_string(),
                Some(key.clone()),
                format!(
                    "pending action '{key}' is projected but absent from source"
                ),
            )),
            (Some(expected), Some(projected)) if expected != projected => {
                records.push(notification_diff_record(
                    ShadowDiffCategoryWire::Stale,
                    source_path.to_string(),
                    Some(key.clone()),
                    format!("pending action '{key}' differs from source"),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn pending_actions_by_prefix(
    store: &PendingActionStoreWire,
) -> Result<BTreeMap<String, JsonValue>, ProjectionError> {
    let mut rows = BTreeMap::new();
    for (prefix, action) in &store.actions {
        rows.insert(prefix.clone(), serde_json::to_value(action)?);
    }
    Ok(rows)
}

fn notification_diff_record(
    category: ShadowDiffCategoryWire,
    source_path: String,
    handle: Option<String>,
    message: String,
) -> ShadowDiffRecordWire {
    ShadowDiffRecordWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: NOTIFICATION_PROJECTION_NAME.to_string(),
        category,
        source_path,
        handle,
        message,
    }
}

fn notification_diff_counts(
    records: &[ShadowDiffRecordWire],
) -> ShadowDiffCountsWire {
    let mut counts = ShadowDiffCountsWire::default();
    for record in records {
        match record.category {
            ShadowDiffCategoryWire::Missing => counts.missing += 1,
            ShadowDiffCategoryWire::Stale => counts.stale += 1,
            ShadowDiffCategoryWire::Extra => counts.extra += 1,
            ShadowDiffCategoryWire::Corrupt => counts.corrupt += 1,
        }
    }
    counts
}

fn notification_diff_sort_key(category: &ShadowDiffCategoryWire) -> u8 {
    match category {
        ShadowDiffCategoryWire::Missing => 0,
        ShadowDiffCategoryWire::Stale => 1,
        ShadowDiffCategoryWire::Extra => 2,
        ShadowDiffCategoryWire::Corrupt => 3,
    }
}

fn notification_indexing_schema_version() -> u32 {
    NOTIFICATION_INDEXING_WIRE_SCHEMA_VERSION
}

#[allow(dead_code)]
fn payload_kind(payload: &JsonValue) -> Option<&str> {
    payload.get("kind").and_then(JsonValue::as_str)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Write};

    use super::*;
    use crate::notifications::{
        append_notification, pending_action_from_notification,
        register_pending_action, MobileActionStateWire,
        NotificationStateUpdateWire,
    };
    use tempfile::tempdir;

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
        assert_eq!(
            snapshot.notifications,
            vec![priority.clone(), error.clone()]
        );
        assert_eq!(snapshot.counts.priority, 1);
        assert_eq!(snapshot.counts.errors, 1);

        priority.read = true;
        let compact_event_request = notification_state_update_event_request(
            context(),
            NotificationStateUpdateWire::MarkRead {
                id: "priority".to_string(),
            },
            Vec::new(),
        )
        .unwrap();
        assert!(compact_event_request.payload.get("notifications").is_none());
        db.append_notification_projection_event(compact_event_request)
            .unwrap();

        let snapshot =
            notification_projection_snapshot(db.connection(), false).unwrap();
        assert_eq!(snapshot.notifications, vec![priority, error]);
        assert_eq!(snapshot.counts.priority, 0);
        assert_eq!(snapshot.counts.errors, 1);

        let counts = notification_projection_counts(db.connection()).unwrap();
        assert_eq!(counts.active, 2);
        assert_eq!(counts.read, 1);
    }

    #[test]
    fn old_full_snapshot_state_update_events_still_replay() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let first = notification("first");
        let mut second = notification("second");
        second.read = true;

        db.append_notification_projection_event(
            notification_append_event_request(context(), first).unwrap(),
        )
        .unwrap();
        db.append_notification_projection_event(
            notification_state_update_event_request(
                context(),
                NotificationStateUpdateWire::MarkRead {
                    id: "second".to_string(),
                },
                vec![second.clone()],
            )
            .unwrap(),
        )
        .unwrap();

        let snapshot =
            notification_projection_snapshot(db.connection(), true).unwrap();
        assert_eq!(snapshot.notifications, vec![second]);
    }

    #[test]
    fn notification_counts_and_first_page_do_not_deserialize_full_store() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let first = notification("first");
        let second = notification("second");
        db.append_notification_projection_event(
            notification_append_event_request(context(), first.clone())
                .unwrap(),
        )
        .unwrap();
        db.append_notification_projection_event(
            notification_append_event_request(context(), second).unwrap(),
        )
        .unwrap();
        db.connection()
            .execute(
                "UPDATE notifications SET notification_json = '{bad-json' WHERE id = 'second'",
                [],
            )
            .unwrap();

        let counts = notification_projection_counts(db.connection()).unwrap();
        assert_eq!(counts.active, 2);
        assert_eq!(counts.unread, 2);

        let page = notification_projection_page(
            db.connection(),
            &NotificationReadListRequestWire {
                page: crate::projections::read::ProjectionPageRequestWire {
                    limit: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            "snapshot-a",
            4096,
        )
        .unwrap();
        assert_eq!(page.notifications, vec![first]);
        assert_eq!(page.page.next_cursor, Some("1".to_string()));
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

    #[test]
    fn backfill_and_shadow_diff_cover_notification_and_pending_sources() {
        let dir = tempdir().unwrap();
        let paths = notification_source_paths(dir.path());
        let mut n = notification("abcdef01-notification");
        n.action = Some("PlanApproval".to_string());
        n.action_data
            .insert("response_dir".to_string(), "/tmp/plan".to_string());
        append_notification(Path::new(&paths.notifications_path), &n).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&paths.notifications_path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let action = pending_action_from_notification(&n, 10.0).unwrap();
        register_pending_action(
            Path::new(&paths.pending_actions_path),
            &action,
        )
        .unwrap();

        let mut db = ProjectionDb::open_in_memory().unwrap();
        let diff =
            notification_shadow_diff(db.connection(), dir.path()).unwrap();
        assert_eq!(diff.counts.missing, 2);
        assert_eq!(diff.counts.corrupt, 1);

        for event in notification_backfill_event_requests(
            dir.path(),
            EventSourceWire {
                source_type: "test".to_string(),
                name: "notification-backfill".to_string(),
                ..EventSourceWire::default()
            },
            "host-a",
        )
        .unwrap()
        {
            db.append_projected_event(event).unwrap();
        }

        let diff =
            notification_shadow_diff(db.connection(), dir.path()).unwrap();
        assert_eq!(diff.counts.missing, 0);
        assert_eq!(diff.counts.stale, 0);
        assert_eq!(diff.counts.extra, 0);
        assert_eq!(diff.counts.corrupt, 1);

        let change = notification_source_change_from_path(
            dir.path(),
            Path::new(&paths.pending_actions_path),
            SourceChangeOperationWire::Rewrite,
            Some("test".to_string()),
        )
        .unwrap();
        let events = notification_event_requests_for_source_change(
            dir.path(),
            &change,
            EventSourceWire {
                source_type: "test".to_string(),
                name: "notification-watch".to_string(),
                ..EventSourceWire::default()
            },
            "host-a",
        )
        .unwrap();
        assert_eq!(events.len(), 1);
    }
}
