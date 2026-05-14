use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Digest;

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire,
};
use super::replay::ProjectionApplier;

pub const SCHEDULER_PROJECTION_NAME: &str = "scheduler";
pub const SCHEDULER_EVENT_BATCH_SUBMITTED: &str = "scheduler.batch_submitted";
pub const SCHEDULER_EVENT_SLOT_PLANNED: &str = "scheduler.slot_planned";
pub const SCHEDULER_EVENT_SLOT_QUEUED: &str = "scheduler.slot_queued";
pub const SCHEDULER_EVENT_SLOT_STARTING: &str = "scheduler.slot_starting";
pub const SCHEDULER_EVENT_SLOT_RUNNING: &str = "scheduler.slot_running";
pub const SCHEDULER_EVENT_SLOT_TERMINAL: &str = "scheduler.slot_terminal";
pub const SCHEDULER_EVENT_SLOT_CANCELLED: &str = "scheduler.slot_cancelled";
pub const SCHEDULER_EVENT_SLOT_KILLED: &str = "scheduler.slot_killed";
pub const SCHEDULER_EVENT_SLOT_STALE: &str = "scheduler.slot_stale";

pub const SCHEDULER_WIRE_SCHEMA_VERSION: u32 = 1;

type SchedulerEventIndexFields =
    (Option<String>, Option<String>, Option<String>);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerEventContextWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub causality: Vec<EventCausalityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedulerEventPayloadWire {
    BatchSubmitted {
        schema_version: u32,
        batch: SchedulerBatchProjectionWire,
    },
    SlotTransitioned {
        schema_version: u32,
        slot: SchedulerSlotProjectionWire,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerTaskIdWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub batch_id: String,
    pub slot_id: String,
    pub queue_id: String,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerSlotStatusWire {
    #[default]
    Planned,
    Queued,
    Starting,
    Running,
    Completed,
    Failed,
    Cancelled,
    Killed,
    Stale,
}

impl SchedulerSlotStatusWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Killed => "killed",
            Self::Stale => "stale",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Killed
                | Self::Stale
        )
    }
}

impl From<&str> for SchedulerSlotStatusWire {
    fn from(value: &str) -> Self {
        match value {
            "queued" => Self::Queued,
            "starting" => Self::Starting,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "killed" => Self::Killed,
            "stale" => Self::Stale,
            _ => Self::Planned,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerLaunchSpecWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerQueueSettingsWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub default_queue_id: String,
    pub max_concurrent_slots: u32,
    pub max_queued_slots: u32,
}

impl Default for SchedulerQueueSettingsWire {
    fn default() -> Self {
        Self {
            schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
            default_queue_id: "agents".to_string(),
            max_concurrent_slots: 4,
            max_queued_slots: 256,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerBatchSubmitRequestWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default)]
    pub launch_specs: Vec<SchedulerLaunchSpecWire>,
    #[serde(default)]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerBatchHandleWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub batch_id: String,
    pub idempotency_key: String,
    pub queue_id: String,
    pub project_id: String,
    pub slot_count: u32,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerBatchProjectionWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub handle: SchedulerBatchHandleWire,
    #[serde(default)]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerSlotProjectionWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub task_id: SchedulerTaskIdWire,
    pub project_id: String,
    pub slot_index: u32,
    pub status: SchedulerSlotStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_position: Option<u32>,
    pub terminal: bool,
    pub launch_spec: SchedulerLaunchSpecWire,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerBatchStatusWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub handle: SchedulerBatchHandleWire,
    #[serde(default)]
    pub slots: Vec<SchedulerSlotProjectionWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulerBatchSubmitResponseWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub handle: SchedulerBatchHandleWire,
    #[serde(default)]
    pub duplicate: bool,
    #[serde(default)]
    pub status: SchedulerBatchStatusWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerCancelRequestWire {
    #[serde(default = "scheduler_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub batch_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub idempotency_key: String,
}

pub struct SchedulerProjectionApplier;

impl ProjectionApplier for SchedulerProjectionApplier {
    fn projection_name(&self) -> &str {
        SCHEDULER_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_scheduler_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_scheduler_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_scheduler_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    SCHEDULER_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }

    pub fn submit_scheduler_batch(
        &mut self,
        context: SchedulerEventContextWire,
        request: SchedulerBatchSubmitRequestWire,
        settings: SchedulerQueueSettingsWire,
    ) -> Result<SchedulerBatchSubmitResponseWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            if let Some(status) = scheduler_batch_by_idempotency(
                conn,
                &request.project_id,
                &request.idempotency_key,
            )? {
                let handle = status.handle.clone();
                return Ok(SchedulerBatchSubmitResponseWire {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    handle,
                    duplicate: true,
                    status,
                });
            }

            validate_submit_request(conn, &request, &settings)?;
            let now = context.created_at.clone().unwrap_or_else(now_timestamp);
            let queue_id = request
                .queue_id
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| settings.default_queue_id.clone());
            let batch_id = request
                .batch_id
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| stable_batch_id(&request));
            let handle = SchedulerBatchHandleWire {
                schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                batch_id: batch_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
                queue_id: queue_id.clone(),
                project_id: request.project_id.clone(),
                slot_count: request.launch_specs.len() as u32,
                status: "queued".to_string(),
                created_at: now.clone(),
            };
            let batch = SchedulerBatchProjectionWire {
                schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                handle: handle.clone(),
                metadata: request.metadata.clone(),
            };
            append_scheduler_payload_tx(
                conn,
                &context,
                SCHEDULER_EVENT_BATCH_SUBMITTED,
                SchedulerEventPayloadWire::BatchSubmitted {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    batch,
                },
                Some(format!(
                    "scheduler:batch_submitted:{}:{}",
                    request.project_id, request.idempotency_key
                )),
            )?;

            for (slot_index, launch_spec) in
                request.launch_specs.iter().cloned().enumerate()
            {
                let slot_id = format!("{batch_id}:{slot_index}");
                let task_id = SchedulerTaskIdWire {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    batch_id: batch_id.clone(),
                    slot_id: slot_id.clone(),
                    queue_id: queue_id.clone(),
                };
                let planned = SchedulerSlotProjectionWire {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    task_id: task_id.clone(),
                    project_id: request.project_id.clone(),
                    slot_index: slot_index as u32,
                    status: SchedulerSlotStatusWire::Planned,
                    queued_position: None,
                    terminal: false,
                    launch_spec: launch_spec.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                    reason: None,
                };
                append_slot_transition_tx(
                    conn,
                    &context,
                    SCHEDULER_EVENT_SLOT_PLANNED,
                    &planned,
                    &slot_id,
                )?;

                let mut queued = planned;
                queued.status = SchedulerSlotStatusWire::Queued;
                queued.queued_position =
                    Some(next_queue_position(conn, &queue_id)?);
                append_slot_transition_tx(
                    conn,
                    &context,
                    SCHEDULER_EVENT_SLOT_QUEUED,
                    &queued,
                    &slot_id,
                )?;
            }

            let status =
                scheduler_batch_status(conn, &request.project_id, &batch_id)?
                    .ok_or_else(|| {
                    ProjectionError::Invariant(format!(
                    "submitted scheduler batch {batch_id:?} was not projected"
                ))
                })?;
            Ok(SchedulerBatchSubmitResponseWire {
                schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                handle,
                duplicate: false,
                status,
            })
        })
    }

    pub fn cancel_scheduler_slots(
        &mut self,
        context: SchedulerEventContextWire,
        request: SchedulerCancelRequestWire,
    ) -> Result<SchedulerBatchStatusWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let mut status = scheduler_batch_status(
                conn,
                &request.project_id,
                &request.batch_id,
            )?
            .ok_or_else(|| {
                ProjectionError::Invariant(format!(
                    "scheduler batch {:?} not found",
                    request.batch_id
                ))
            })?;
            let now = context.created_at.clone().unwrap_or_else(now_timestamp);
            let mut changed = false;
            for slot in status.slots.clone() {
                if let Some(slot_id) = request.slot_id.as_ref() {
                    if &slot.task_id.slot_id != slot_id {
                        continue;
                    }
                }
                if slot.terminal
                    || slot.status == SchedulerSlotStatusWire::Running
                {
                    continue;
                }
                let mut cancelled = slot;
                cancelled.status = SchedulerSlotStatusWire::Cancelled;
                cancelled.queued_position = None;
                cancelled.terminal = true;
                cancelled.updated_at = now.clone();
                cancelled.reason = request.reason.clone();
                append_slot_transition_tx(
                    conn,
                    &context,
                    SCHEDULER_EVENT_SLOT_CANCELLED,
                    &cancelled,
                    &cancelled.task_id.slot_id,
                )?;
                changed = true;
            }
            if changed {
                status = scheduler_batch_status(
                    conn,
                    &request.project_id,
                    &request.batch_id,
                )?
                .ok_or_else(|| {
                    ProjectionError::Invariant(format!(
                        "scheduler batch {:?} disappeared after cancel",
                        request.batch_id
                    ))
                })?;
            }
            Ok(status)
        })
    }

    pub fn recover_scheduler_starting_slots(
        &mut self,
        context: SchedulerEventContextWire,
    ) -> Result<u32, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT slot_json
                FROM scheduler_slots
                WHERE terminal = 0 AND status = 'starting'
                ORDER BY project_id, batch_id, slot_index, slot_id
                "#,
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut slots = Vec::new();
            for row in rows {
                slots.push(
                    serde_json::from_str::<SchedulerSlotProjectionWire>(&row?)?,
                );
            }
            drop(stmt);

            let mut recovered = 0;
            for mut slot in slots {
                let mut slot_context = context.clone();
                slot_context.project_id = slot.project_id.clone();
                slot_context.idempotency_key = Some(format!(
                    "scheduler:startup_recover:{}:{}",
                    slot.project_id, slot.task_id.slot_id
                ));
                slot.status = SchedulerSlotStatusWire::Stale;
                slot.queued_position = None;
                slot.terminal = true;
                slot.updated_at = slot_context
                    .created_at
                    .clone()
                    .unwrap_or_else(now_timestamp);
                slot.reason = Some(
                    "daemon_restart_starting_slot_without_host_confirmation"
                        .to_string(),
                );
                append_slot_transition_tx(
                    conn,
                    &slot_context,
                    SCHEDULER_EVENT_SLOT_STALE,
                    &slot,
                    &slot.task_id.slot_id,
                )?;
                recovered += 1;
            }
            Ok(recovered)
        })
    }
}

pub fn scheduler_batch_status(
    conn: &Connection,
    project_id: &str,
    batch_id: &str,
) -> Result<Option<SchedulerBatchStatusWire>, ProjectionError> {
    let handle = scheduler_batch_handle(conn, project_id, batch_id)?;
    let Some(mut handle) = handle else {
        return Ok(None);
    };
    let slots = scheduler_batch_slots(conn, project_id, batch_id)?;
    handle.status = batch_status_from_slots(&slots).to_string();
    Ok(Some(SchedulerBatchStatusWire {
        schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
        handle,
        slots,
    }))
}

pub fn scheduler_batch_by_idempotency(
    conn: &Connection,
    project_id: &str,
    idempotency_key: &str,
) -> Result<Option<SchedulerBatchStatusWire>, ProjectionError> {
    let batch_id = conn
        .query_row(
            r#"
            SELECT batch_id
            FROM scheduler_batches
            WHERE project_id = ?1 AND idempotency_key = ?2
            "#,
            params![project_id, idempotency_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match batch_id {
        Some(batch_id) => scheduler_batch_status(conn, project_id, &batch_id),
        None => Ok(None),
    }
}

pub fn scheduler_queue_page(
    conn: &Connection,
    project_id: &str,
    queue_id: &str,
) -> Result<Vec<SchedulerSlotProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT slot_json
        FROM scheduler_slots
        WHERE project_id = ?1
          AND queue_id = ?2
          AND terminal = 0
          AND status IN ('queued', 'starting', 'running')
        ORDER BY COALESCE(queued_position, 2147483647), slot_index, slot_id
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, queue_id], |row| {
        row.get::<_, String>(0)
    })?;
    let mut slots = Vec::new();
    for row in rows {
        slots.push(serde_json::from_str(&row?)?);
    }
    Ok(slots)
}

pub fn scheduler_mark_slot_status(
    db: &mut ProjectionDb,
    context: SchedulerEventContextWire,
    project_id: &str,
    slot_id: &str,
    status: SchedulerSlotStatusWire,
    reason: Option<String>,
) -> Result<SchedulerSlotProjectionWire, ProjectionError> {
    db.with_immediate_transaction(|conn| {
        let mut slot =
            scheduler_slot(conn, project_id, slot_id)?.ok_or_else(|| {
                ProjectionError::Invariant(format!(
                    "scheduler slot {slot_id:?} not found"
                ))
            })?;
        slot.status = status;
        slot.terminal = status.is_terminal();
        if slot.terminal || status != SchedulerSlotStatusWire::Queued {
            slot.queued_position = None;
        }
        slot.updated_at =
            context.created_at.clone().unwrap_or_else(now_timestamp);
        slot.reason = reason;
        let event_type = event_type_for_slot_status(status);
        append_slot_transition_tx(conn, &context, event_type, &slot, slot_id)?;
        Ok(slot)
    })
}

fn apply_scheduler_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !event.event_type.starts_with("scheduler.") {
        return Ok(());
    }
    let payload: SchedulerEventPayloadWire =
        serde_json::from_value(event.payload.clone())?;
    match payload {
        SchedulerEventPayloadWire::BatchSubmitted { batch, .. } => {
            upsert_batch_tx(conn, event, &batch)?
        }
        SchedulerEventPayloadWire::SlotTransitioned { slot, .. } => {
            upsert_slot_tx(conn, event, &slot)?
        }
    }
    insert_scheduler_event_tx(conn, event)
}

fn upsert_batch_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    batch: &SchedulerBatchProjectionWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO scheduler_batches (
            project_id, batch_id, idempotency_key, queue_id, slot_count, status,
            created_at, batch_json, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(project_id, batch_id) DO UPDATE SET
            status = excluded.status,
            batch_json = excluded.batch_json,
            last_seq = excluded.last_seq
        "#,
        params![
            batch.handle.project_id,
            batch.handle.batch_id,
            batch.handle.idempotency_key,
            batch.handle.queue_id,
            batch.handle.slot_count,
            batch.handle.status,
            batch.handle.created_at,
            serde_json::to_string(batch)?,
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_slot_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    slot: &SchedulerSlotProjectionWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO scheduler_slots (
            project_id, batch_id, slot_id, queue_id, slot_index, status,
            queued_position, terminal, launch_spec_json, slot_json,
            created_at, updated_at, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
        ON CONFLICT(project_id, slot_id) DO UPDATE SET
            status = excluded.status,
            queued_position = excluded.queued_position,
            terminal = excluded.terminal,
            launch_spec_json = excluded.launch_spec_json,
            slot_json = excluded.slot_json,
            updated_at = excluded.updated_at,
            last_seq = excluded.last_seq
        "#,
        params![
            slot.project_id,
            slot.task_id.batch_id,
            slot.task_id.slot_id,
            slot.task_id.queue_id,
            slot.slot_index,
            slot.status.as_str(),
            slot.queued_position,
            if slot.terminal { 1 } else { 0 },
            serde_json::to_string(&slot.launch_spec)?,
            serde_json::to_string(slot)?,
            slot.created_at,
            slot.updated_at,
            event.seq,
        ],
    )?;
    Ok(())
}

fn insert_scheduler_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    let payload = serde_json::to_string(&event.payload)?;
    let (batch_id, slot_id, status) =
        scheduler_event_index_fields(&event.payload)?;
    conn.execute(
        r#"
        INSERT OR REPLACE INTO scheduler_events (
            seq, project_id, event_type, batch_id, slot_id, status,
            payload_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            event.seq,
            event.project_id,
            event.event_type,
            batch_id,
            slot_id,
            status,
            payload,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn scheduler_event_index_fields(
    payload: &JsonValue,
) -> Result<SchedulerEventIndexFields, ProjectionError> {
    let payload: SchedulerEventPayloadWire =
        serde_json::from_value(payload.clone())?;
    Ok(match payload {
        SchedulerEventPayloadWire::BatchSubmitted { batch, .. } => {
            (Some(batch.handle.batch_id), None, Some(batch.handle.status))
        }
        SchedulerEventPayloadWire::SlotTransitioned { slot, .. } => (
            Some(slot.task_id.batch_id),
            Some(slot.task_id.slot_id),
            Some(slot.status.as_str().to_string()),
        ),
    })
}

fn scheduler_batch_handle(
    conn: &Connection,
    project_id: &str,
    batch_id: &str,
) -> Result<Option<SchedulerBatchHandleWire>, ProjectionError> {
    let raw = conn
        .query_row(
            r#"
            SELECT batch_json
            FROM scheduler_batches
            WHERE project_id = ?1 AND batch_id = ?2
            "#,
            params![project_id, batch_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    raw.map(|json| {
        let batch: SchedulerBatchProjectionWire = serde_json::from_str(&json)?;
        Ok(batch.handle)
    })
    .transpose()
}

fn scheduler_batch_slots(
    conn: &Connection,
    project_id: &str,
    batch_id: &str,
) -> Result<Vec<SchedulerSlotProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT slot_json
        FROM scheduler_slots
        WHERE project_id = ?1 AND batch_id = ?2
        ORDER BY slot_index, slot_id
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, batch_id], |row| {
        row.get::<_, String>(0)
    })?;
    let mut slots = Vec::new();
    for row in rows {
        slots.push(serde_json::from_str(&row?)?);
    }
    Ok(slots)
}

fn scheduler_slot(
    conn: &Connection,
    project_id: &str,
    slot_id: &str,
) -> Result<Option<SchedulerSlotProjectionWire>, ProjectionError> {
    let raw = conn
        .query_row(
            r#"
            SELECT slot_json
            FROM scheduler_slots
            WHERE project_id = ?1 AND slot_id = ?2
            "#,
            params![project_id, slot_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    raw.map(|json| serde_json::from_str(&json).map_err(ProjectionError::from))
        .transpose()
}

fn validate_submit_request(
    conn: &Connection,
    request: &SchedulerBatchSubmitRequestWire,
    settings: &SchedulerQueueSettingsWire,
) -> Result<(), ProjectionError> {
    if request.launch_specs.is_empty() {
        return Err(ProjectionError::Invariant(
            "scheduler batch submit requires at least one launch spec"
                .to_string(),
        ));
    }
    let queue_id = request
        .queue_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(&settings.default_queue_id);
    let queued_count: u32 = conn.query_row(
        r#"
        SELECT COUNT(*)
        FROM scheduler_slots
        WHERE queue_id = ?1
          AND terminal = 0
          AND status IN ('planned', 'queued', 'starting', 'running')
        "#,
        [queue_id],
        |row| row.get::<_, u32>(0),
    )?;
    let requested = request.launch_specs.len() as u32;
    if queued_count.saturating_add(requested) > settings.max_queued_slots {
        return Err(ProjectionError::Invariant(format!(
            "scheduler queue {queue_id:?} is full: {queued_count} active slots + {requested} requested exceeds max_queued_slots {}",
            settings.max_queued_slots
        )));
    }
    Ok(())
}

fn next_queue_position(
    conn: &Connection,
    queue_id: &str,
) -> Result<u32, ProjectionError> {
    let position = conn.query_row(
        r#"
        SELECT COALESCE(MAX(queued_position), 0) + 1
        FROM scheduler_slots
        WHERE queue_id = ?1 AND terminal = 0
        "#,
        [queue_id],
        |row| row.get::<_, u32>(0),
    )?;
    Ok(position)
}

fn batch_status_from_slots(
    slots: &[SchedulerSlotProjectionWire],
) -> &'static str {
    if slots.is_empty() {
        return "empty";
    }
    if slots
        .iter()
        .all(|slot| slot.status == SchedulerSlotStatusWire::Completed)
    {
        return "completed";
    }
    if slots.iter().all(|slot| slot.terminal) {
        return "terminal";
    }
    if slots
        .iter()
        .any(|slot| slot.status == SchedulerSlotStatusWire::Running)
    {
        return "running";
    }
    if slots
        .iter()
        .any(|slot| slot.status == SchedulerSlotStatusWire::Starting)
    {
        return "starting";
    }
    "queued"
}

fn append_slot_transition_tx(
    conn: &Connection,
    context: &SchedulerEventContextWire,
    event_type: &str,
    slot: &SchedulerSlotProjectionWire,
    slot_id: &str,
) -> Result<EventAppendOutcomeWire, ProjectionError> {
    append_scheduler_payload_tx(
        conn,
        context,
        event_type,
        SchedulerEventPayloadWire::SlotTransitioned {
            schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
            slot: slot.clone(),
        },
        Some(format!(
            "scheduler:{event_type}:{}:{}:{slot_id}",
            slot.project_id, slot.task_id.batch_id
        )),
    )
}

fn append_scheduler_payload_tx(
    conn: &Connection,
    context: &SchedulerEventContextWire,
    event_type: &str,
    payload: SchedulerEventPayloadWire,
    idempotency_key: Option<String>,
) -> Result<EventAppendOutcomeWire, ProjectionError> {
    let request = scheduler_event_request(
        context.clone(),
        event_type,
        payload,
        idempotency_key,
    )?;
    let outcome = append_event_tx(conn, request)?;
    if !outcome.duplicate {
        apply_scheduler_event_tx(conn, &outcome.event)?;
        set_projection_last_seq_tx(
            conn,
            SCHEDULER_PROJECTION_NAME,
            outcome.event.seq,
        )?;
    }
    Ok(outcome)
}

pub fn scheduler_event_request(
    mut context: SchedulerEventContextWire,
    event_type: &str,
    payload: SchedulerEventPayloadWire,
    idempotency_key: Option<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    if idempotency_key.is_some() {
        context.idempotency_key = idempotency_key;
    }
    Ok(EventAppendRequestWire {
        created_at: context.created_at,
        source: context.source,
        host_id: context.host_id,
        project_id: context.project_id,
        event_type: event_type.to_string(),
        payload: serde_json::to_value(payload)?,
        idempotency_key: context.idempotency_key,
        causality: context.causality,
        source_path: context.source_path,
        source_revision: context.source_revision,
    })
}

fn stable_batch_id(request: &SchedulerBatchSubmitRequestWire) -> String {
    let mut hash_input = String::new();
    hash_input.push_str(&request.project_id);
    hash_input.push('\0');
    hash_input.push_str(&request.idempotency_key);
    let digest = sha2::Sha256::digest(hash_input.as_bytes());
    format!("batch_{}", hex::encode(&digest[..8]))
}

fn event_type_for_slot_status(status: SchedulerSlotStatusWire) -> &'static str {
    match status {
        SchedulerSlotStatusWire::Planned => SCHEDULER_EVENT_SLOT_PLANNED,
        SchedulerSlotStatusWire::Queued => SCHEDULER_EVENT_SLOT_QUEUED,
        SchedulerSlotStatusWire::Starting => SCHEDULER_EVENT_SLOT_STARTING,
        SchedulerSlotStatusWire::Running => SCHEDULER_EVENT_SLOT_RUNNING,
        SchedulerSlotStatusWire::Completed
        | SchedulerSlotStatusWire::Failed => SCHEDULER_EVENT_SLOT_TERMINAL,
        SchedulerSlotStatusWire::Cancelled => SCHEDULER_EVENT_SLOT_CANCELLED,
        SchedulerSlotStatusWire::Killed => SCHEDULER_EVENT_SLOT_KILLED,
        SchedulerSlotStatusWire::Stale => SCHEDULER_EVENT_SLOT_STALE,
    }
}

fn now_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn scheduler_schema_version() -> u32 {
    SCHEDULER_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> SchedulerEventContextWire {
        SchedulerEventContextWire {
            schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
            created_at: Some("2026-05-14T06:00:00Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "scheduler-test".to_string(),
                version: None,
                runtime: None,
                metadata: BTreeMap::new(),
            },
            host_id: "host-a".to_string(),
            project_id: "proj".to_string(),
            idempotency_key: None,
            causality: Vec::new(),
            source_path: None,
            source_revision: None,
        }
    }

    fn submit(count: usize) -> SchedulerBatchSubmitRequestWire {
        SchedulerBatchSubmitRequestWire {
            schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
            project_id: "proj".to_string(),
            idempotency_key: "idem-1".to_string(),
            batch_id: Some("batch-a".to_string()),
            queue_id: Some("agents".to_string()),
            launch_specs: (0..count)
                .map(|index| SchedulerLaunchSpecWire {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    project_id: "proj".to_string(),
                    prompt: format!("prompt {index}"),
                    cwd: None,
                    model: None,
                    parent_agent_id: None,
                    workflow_id: None,
                    metadata: BTreeMap::new(),
                })
                .collect(),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn submit_batch_projects_queued_slots() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let response = db
            .submit_scheduler_batch(
                context(),
                submit(3),
                SchedulerQueueSettingsWire::default(),
            )
            .unwrap();

        assert!(!response.duplicate);
        assert_eq!(response.handle.batch_id, "batch-a");
        assert_eq!(response.status.slots.len(), 3);
        assert_eq!(
            response
                .status
                .slots
                .iter()
                .map(|slot| slot.queued_position)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn duplicate_submit_returns_same_handle_without_new_slots() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let first = db
            .submit_scheduler_batch(
                context(),
                submit(2),
                SchedulerQueueSettingsWire::default(),
            )
            .unwrap();
        let second = db
            .submit_scheduler_batch(
                context(),
                submit(2),
                SchedulerQueueSettingsWire::default(),
            )
            .unwrap();

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.handle, second.handle);
        assert_eq!(second.status.slots.len(), 2);
    }

    #[test]
    fn backpressure_rejects_batch_over_queue_limit() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let error = db
            .submit_scheduler_batch(
                context(),
                submit(3),
                SchedulerQueueSettingsWire {
                    max_queued_slots: 2,
                    ..SchedulerQueueSettingsWire::default()
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("is full"));
    }

    #[test]
    fn cancel_marks_queued_slots_terminal() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.submit_scheduler_batch(
            context(),
            submit(2),
            SchedulerQueueSettingsWire::default(),
        )
        .unwrap();

        let status = db
            .cancel_scheduler_slots(
                context(),
                SchedulerCancelRequestWire {
                    schema_version: SCHEDULER_WIRE_SCHEMA_VERSION,
                    project_id: "proj".to_string(),
                    batch_id: "batch-a".to_string(),
                    slot_id: None,
                    reason: Some("user_cancelled".to_string()),
                    idempotency_key: "cancel-1".to_string(),
                },
            )
            .unwrap();

        assert_eq!(status.handle.status, "terminal");
        assert!(status.slots.iter().all(|slot| slot.terminal));
        assert!(status
            .slots
            .iter()
            .all(|slot| { slot.status == SchedulerSlotStatusWire::Cancelled }));
    }

    #[test]
    fn startup_recovery_marks_starting_slots_stale() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.submit_scheduler_batch(
            context(),
            submit(1),
            SchedulerQueueSettingsWire::default(),
        )
        .unwrap();
        scheduler_mark_slot_status(
            &mut db,
            context(),
            "proj",
            "batch-a:0",
            SchedulerSlotStatusWire::Starting,
            None,
        )
        .unwrap();

        let recovered = db.recover_scheduler_starting_slots(context()).unwrap();
        let status = scheduler_batch_status(db.connection(), "proj", "batch-a")
            .unwrap()
            .unwrap();

        assert_eq!(recovered, 1);
        assert_eq!(status.handle.status, "terminal");
        assert_eq!(status.slots[0].status, SchedulerSlotStatusWire::Stale);
        assert_eq!(
            status.slots[0].reason.as_deref(),
            Some("daemon_restart_starting_slot_without_host_confirmation")
        );
    }
}
