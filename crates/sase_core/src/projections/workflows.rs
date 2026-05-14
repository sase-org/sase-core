use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;

use crate::agent_scan::{
    PromptStepMarkerWire, WorkflowStateWire, WorkflowStepStateWire,
};

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire,
};
use super::replay::ProjectionApplier;

pub const WORKFLOW_PROJECTION_NAME: &str = "workflows";

pub const WORKFLOW_EVENT_RUN_CREATED: &str = "workflow.run_created";
pub const WORKFLOW_EVENT_RUN_UPDATED: &str = "workflow.run_updated";
pub const WORKFLOW_EVENT_STEP_TRANSITIONED: &str = "workflow.step_transitioned";
pub const WORKFLOW_EVENT_HITL_PAUSED: &str = "workflow.hitl_paused";
pub const WORKFLOW_EVENT_HITL_RESUMED: &str = "workflow.hitl_resumed";
pub const WORKFLOW_EVENT_RETRY_REQUESTED: &str = "workflow.retry_requested";
pub const WORKFLOW_EVENT_TERMINAL_STATE_REACHED: &str =
    "workflow.terminal_state_reached";
pub const WORKFLOW_EVENT_ACTION_RESPONSE_RECORDED: &str =
    "workflow.action_response_recorded";

const WORKFLOW_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowProjectionEventContextWire {
    #[serde(default = "workflow_schema_version")]
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunProjectionWire {
    #[serde(default = "workflow_schema_version")]
    pub schema_version: u32,
    pub workflow_id: String,
    pub workflow_name: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_dir_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<String>,
    #[serde(default)]
    pub current_step_index: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default)]
    pub appears_as_agent: bool,
    #[serde(default)]
    pub is_anonymous: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retried_as_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_attempt: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<WorkflowStateWire>,
}

impl WorkflowRunProjectionWire {
    pub fn from_state(
        workflow_id: impl Into<String>,
        state: WorkflowStateWire,
    ) -> Self {
        Self {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id: workflow_id.into(),
            workflow_name: state.workflow_name.clone(),
            status: state.status.clone(),
            cl_name: state.cl_name.clone(),
            current_step_index: state.current_step_index,
            pid: state.pid,
            appears_as_agent: state.appears_as_agent,
            is_anonymous: state.is_anonymous,
            started_at: state.start_time.clone(),
            error: state.error.clone(),
            traceback: state.traceback.clone(),
            activity: state.activity.clone(),
            state: Some(state),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStepProjectionWire {
    #[serde(default = "workflow_schema_version")]
    pub schema_version: u32,
    pub workflow_id: String,
    pub step_index: i64,
    pub name: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_step_index: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_steps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_total_steps: Option<i64>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub is_pre_prompt_step: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_workflow_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Map<String, JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_types: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker: Option<PromptStepMarkerWire>,
}

impl WorkflowStepProjectionWire {
    pub fn from_state_step(
        workflow_id: &str,
        step_index: i64,
        step: &WorkflowStepStateWire,
    ) -> Self {
        Self {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id: workflow_id.to_string(),
            step_index,
            name: step.name.clone(),
            status: step.status.clone(),
            output: step.output.clone(),
            output_types: step.output_types.clone(),
            error: step.error.clone(),
            traceback: step.traceback.clone(),
            ..Self::default()
        }
    }

    pub fn from_prompt_marker(
        workflow_id: impl Into<String>,
        marker: PromptStepMarkerWire,
    ) -> Self {
        Self {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id: workflow_id.into(),
            step_index: marker.step_index.unwrap_or(0),
            name: marker.step_name.clone(),
            status: marker.status.clone(),
            step_type: Some(marker.step_type.clone()),
            step_source: marker.step_source.clone(),
            parent_step_index: marker.parent_step_index,
            total_steps: marker.total_steps,
            parent_total_steps: marker.parent_total_steps,
            hidden: marker.hidden,
            is_pre_prompt_step: marker.is_pre_prompt_step,
            embedded_workflow_name: marker.embedded_workflow_name.clone(),
            artifacts_dir: marker.artifacts_dir.clone(),
            output: marker.output.clone(),
            output_types: marker.output_types.clone(),
            error: marker.error.clone(),
            traceback: marker.traceback.clone(),
            marker: Some(marker),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowSummaryWire {
    pub schema_version: u32,
    pub workflow_id: String,
    pub project_id: String,
    pub workflow_name: String,
    pub status: String,
    pub cl_name: Option<String>,
    pub agent_id: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub parent_agent_id: Option<String>,
    pub current_step_index: i64,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub retry_attempt: Option<i64>,
    pub last_seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowProjectionPageWire {
    #[serde(default = "workflow_schema_version")]
    pub schema_version: u32,
    pub entries: Vec<WorkflowSummaryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEventProjectionWire {
    pub seq: i64,
    pub event_type: String,
    pub workflow_id: Option<String>,
    pub step_index: Option<i64>,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDetailWire {
    #[serde(default = "workflow_schema_version")]
    pub schema_version: u32,
    pub workflow: Option<WorkflowSummaryWire>,
    pub steps: Vec<WorkflowStepProjectionWire>,
    pub events: Vec<WorkflowEventProjectionWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WorkflowProjectionEventPayloadWire {
    RunCreated {
        schema_version: u32,
        workflow: WorkflowRunProjectionWire,
    },
    RunUpdated {
        schema_version: u32,
        workflow: WorkflowRunProjectionWire,
    },
    StepTransitioned {
        schema_version: u32,
        step: WorkflowStepProjectionWire,
    },
    HitlPaused {
        schema_version: u32,
        workflow_id: String,
        reason: Option<String>,
    },
    HitlResumed {
        schema_version: u32,
        workflow_id: String,
    },
    RetryRequested {
        schema_version: u32,
        workflow_id: String,
        retry_of_workflow_id: Option<String>,
        retry_attempt: Option<i64>,
    },
    TerminalStateReached {
        schema_version: u32,
        workflow_id: String,
        status: String,
        finished_at: Option<String>,
        error: Option<String>,
        traceback: Option<String>,
    },
    ActionResponseRecorded {
        schema_version: u32,
        workflow_id: Option<String>,
        action_kind: String,
        response_path: String,
        notification_id: Option<String>,
        state: String,
    },
}

pub struct WorkflowProjectionApplier;

impl ProjectionApplier for WorkflowProjectionApplier {
    fn projection_name(&self) -> &str {
        WORKFLOW_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_workflow_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_workflow_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_workflow_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    WORKFLOW_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }
}

pub fn workflow_run_created_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow: WorkflowRunProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_RUN_CREATED,
        WorkflowProjectionEventPayloadWire::RunCreated {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow,
        },
    )
}

pub fn workflow_run_updated_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow: WorkflowRunProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_RUN_UPDATED,
        WorkflowProjectionEventPayloadWire::RunUpdated {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow,
        },
    )
}

pub fn workflow_step_transitioned_event_request(
    context: WorkflowProjectionEventContextWire,
    step: WorkflowStepProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_STEP_TRANSITIONED,
        WorkflowProjectionEventPayloadWire::StepTransitioned {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            step,
        },
    )
}

pub fn workflow_hitl_paused_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow_id: String,
    reason: Option<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_HITL_PAUSED,
        WorkflowProjectionEventPayloadWire::HitlPaused {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id,
            reason,
        },
    )
}

pub fn workflow_hitl_resumed_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow_id: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_HITL_RESUMED,
        WorkflowProjectionEventPayloadWire::HitlResumed {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id,
        },
    )
}

pub fn workflow_retry_requested_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow_id: String,
    retry_of_workflow_id: Option<String>,
    retry_attempt: Option<i64>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_RETRY_REQUESTED,
        WorkflowProjectionEventPayloadWire::RetryRequested {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id,
            retry_of_workflow_id,
            retry_attempt,
        },
    )
}

pub fn workflow_terminal_state_reached_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow_id: String,
    status: String,
    finished_at: Option<String>,
    error: Option<String>,
    traceback: Option<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_TERMINAL_STATE_REACHED,
        WorkflowProjectionEventPayloadWire::TerminalStateReached {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id,
            status,
            finished_at,
            error,
            traceback,
        },
    )
}

pub fn workflow_action_response_recorded_event_request(
    context: WorkflowProjectionEventContextWire,
    workflow_id: Option<String>,
    action_kind: String,
    response_path: String,
    notification_id: Option<String>,
    state: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    workflow_event_request(
        context,
        WORKFLOW_EVENT_ACTION_RESPONSE_RECORDED,
        WorkflowProjectionEventPayloadWire::ActionResponseRecorded {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            workflow_id,
            action_kind,
            response_path,
            notification_id,
            state,
        },
    )
}

pub fn workflow_projection_list(
    conn: &Connection,
    project_id: &str,
    status: Option<&str>,
    limit: u32,
    offset: u32,
) -> Result<WorkflowProjectionPageWire, ProjectionError> {
    let fetch_limit = limit.saturating_add(1);
    let mut entries = if let Some(status) = status {
        let mut stmt = conn.prepare(
            r#"
            SELECT workflow_id, project_id, workflow_name, status, cl_name,
                   agent_id, parent_workflow_id, parent_agent_id,
                   current_step_index, started_at, finished_at,
                   retry_attempt, last_seq
            FROM workflows
            WHERE project_id = ?1 AND status = ?2
            ORDER BY COALESCE(started_at, ''), workflow_id
            LIMIT ?3 OFFSET ?4
            "#,
        )?;
        let rows = stmt.query_map(
            params![project_id, status, fetch_limit as i64, offset as i64],
            workflow_summary_from_row,
        )?;
        collect_workflow_rows(rows)?
    } else {
        let mut stmt = conn.prepare(
            r#"
            SELECT workflow_id, project_id, workflow_name, status, cl_name,
                   agent_id, parent_workflow_id, parent_agent_id,
                   current_step_index, started_at, finished_at,
                   retry_attempt, last_seq
            FROM workflows
            WHERE project_id = ?1
            ORDER BY COALESCE(started_at, ''), workflow_id
            LIMIT ?2 OFFSET ?3
            "#,
        )?;
        let rows = stmt.query_map(
            params![project_id, fetch_limit as i64, offset as i64],
            workflow_summary_from_row,
        )?;
        collect_workflow_rows(rows)?
    };
    let next_offset = truncate_for_next_offset(&mut entries, limit, offset);
    Ok(WorkflowProjectionPageWire {
        schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
        entries,
        next_offset,
    })
}

pub fn workflow_projection_detail(
    conn: &Connection,
    project_id: &str,
    workflow_id: &str,
) -> Result<WorkflowDetailWire, ProjectionError> {
    let workflow = conn
        .query_row(
            r#"
            SELECT workflow_id, project_id, workflow_name, status, cl_name,
                   agent_id, parent_workflow_id, parent_agent_id,
                   current_step_index, started_at, finished_at,
                   retry_attempt, last_seq
            FROM workflows
            WHERE project_id = ?1 AND workflow_id = ?2
            "#,
            params![project_id, workflow_id],
            workflow_summary_from_row,
        )
        .optional()?;

    let mut step_stmt = conn.prepare(
        r#"
        SELECT workflow_id, step_index, name, status, step_type, step_source,
               parent_step_index, total_steps, parent_total_steps, hidden,
               is_pre_prompt_step, embedded_workflow_name, artifacts_dir,
               output_json, output_types_json, error, traceback, marker_json
        FROM workflow_steps
        WHERE project_id = ?1 AND workflow_id = ?2
        ORDER BY step_index ASC, name ASC
        "#,
    )?;
    let step_rows = step_stmt
        .query_map(params![project_id, workflow_id], workflow_step_from_row)?;
    let mut steps = Vec::new();
    for row in step_rows {
        steps.push(row?);
    }

    let mut event_stmt = conn.prepare(
        r#"
        SELECT seq, event_type, workflow_id, step_index, created_at
        FROM workflow_events
        WHERE project_id = ?1 AND workflow_id = ?2
        ORDER BY seq ASC
        "#,
    )?;
    let event_rows = event_stmt
        .query_map(params![project_id, workflow_id], workflow_event_from_row)?;
    let mut events = Vec::new();
    for row in event_rows {
        events.push(row?);
    }

    Ok(WorkflowDetailWire {
        schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
        workflow,
        steps,
        events,
    })
}

fn apply_workflow_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !is_workflow_event(&event.event_type) {
        return Ok(());
    }

    let payload: WorkflowProjectionEventPayloadWire =
        serde_json::from_value(event.payload.clone())?;
    let (workflow_id, step_index) = match &payload {
        WorkflowProjectionEventPayloadWire::RunCreated { workflow, .. }
        | WorkflowProjectionEventPayloadWire::RunUpdated { workflow, .. } => {
            upsert_workflow_tx(conn, event, workflow)?;
            if let Some(state) = workflow.state.as_ref() {
                for (index, step) in state.steps.iter().enumerate() {
                    upsert_step_tx(
                        conn,
                        event,
                        &WorkflowStepProjectionWire::from_state_step(
                            &workflow.workflow_id,
                            index as i64,
                            step,
                        ),
                    )?;
                }
            }
            (Some(workflow.workflow_id.as_str()), None)
        }
        WorkflowProjectionEventPayloadWire::StepTransitioned {
            step, ..
        } => {
            upsert_step_tx(conn, event, step)?;
            (Some(step.workflow_id.as_str()), Some(step.step_index))
        }
        WorkflowProjectionEventPayloadWire::HitlPaused {
            workflow_id,
            reason,
            ..
        } => {
            update_workflow_status_tx(
                conn,
                event,
                workflow_id,
                "paused",
                None,
                reason.clone(),
                None,
            )?;
            (Some(workflow_id.as_str()), None)
        }
        WorkflowProjectionEventPayloadWire::HitlResumed {
            workflow_id, ..
        } => {
            update_workflow_status_tx(
                conn,
                event,
                workflow_id,
                "running",
                None,
                None,
                None,
            )?;
            (Some(workflow_id.as_str()), None)
        }
        WorkflowProjectionEventPayloadWire::RetryRequested {
            workflow_id,
            retry_of_workflow_id,
            retry_attempt,
            ..
        } => {
            conn.execute(
                r#"
                UPDATE workflows
                SET retry_of_workflow_id = ?3,
                    retry_attempt = ?4,
                    last_seq = ?5
                WHERE project_id = ?1 AND workflow_id = ?2
                "#,
                params![
                    event.project_id,
                    workflow_id,
                    retry_of_workflow_id,
                    retry_attempt,
                    event.seq,
                ],
            )?;
            (Some(workflow_id.as_str()), None)
        }
        WorkflowProjectionEventPayloadWire::TerminalStateReached {
            workflow_id,
            status,
            finished_at,
            error,
            traceback,
            ..
        } => {
            update_workflow_status_tx(
                conn,
                event,
                workflow_id,
                status,
                finished_at.clone(),
                error.clone(),
                traceback.clone(),
            )?;
            (Some(workflow_id.as_str()), None)
        }
        WorkflowProjectionEventPayloadWire::ActionResponseRecorded {
            workflow_id,
            ..
        } => (workflow_id.as_deref(), None),
    };
    insert_workflow_event_tx(conn, event, workflow_id, step_index)?;
    Ok(())
}

fn is_workflow_event(event_type: &str) -> bool {
    matches!(
        event_type,
        WORKFLOW_EVENT_RUN_CREATED
            | WORKFLOW_EVENT_RUN_UPDATED
            | WORKFLOW_EVENT_STEP_TRANSITIONED
            | WORKFLOW_EVENT_HITL_PAUSED
            | WORKFLOW_EVENT_HITL_RESUMED
            | WORKFLOW_EVENT_RETRY_REQUESTED
            | WORKFLOW_EVENT_TERMINAL_STATE_REACHED
            | WORKFLOW_EVENT_ACTION_RESPONSE_RECORDED
    )
}

fn upsert_workflow_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    workflow: &WorkflowRunProjectionWire,
) -> Result<(), ProjectionError> {
    let state_json = serde_json::to_string(&workflow.state)?;
    conn.execute(
        r#"
        INSERT INTO workflows (
            project_id, workflow_id, workflow_name, status, cl_name,
            project_name, project_dir, project_file, workflow_dir_name,
            artifact_dir, agent_id, parent_workflow_id, parent_agent_id,
            current_step_index, pid, appears_as_agent, is_anonymous,
            started_at, finished_at, error, traceback, activity,
            retry_of_workflow_id, retried_as_workflow_id, retry_attempt,
            state_json, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27
        )
        ON CONFLICT(project_id, workflow_id) DO UPDATE SET
            workflow_name = excluded.workflow_name,
            status = excluded.status,
            cl_name = excluded.cl_name,
            project_name = excluded.project_name,
            project_dir = excluded.project_dir,
            project_file = excluded.project_file,
            workflow_dir_name = excluded.workflow_dir_name,
            artifact_dir = excluded.artifact_dir,
            agent_id = excluded.agent_id,
            parent_workflow_id = excluded.parent_workflow_id,
            parent_agent_id = excluded.parent_agent_id,
            current_step_index = excluded.current_step_index,
            pid = excluded.pid,
            appears_as_agent = excluded.appears_as_agent,
            is_anonymous = excluded.is_anonymous,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            error = excluded.error,
            traceback = excluded.traceback,
            activity = excluded.activity,
            retry_of_workflow_id = excluded.retry_of_workflow_id,
            retried_as_workflow_id = excluded.retried_as_workflow_id,
            retry_attempt = excluded.retry_attempt,
            state_json = excluded.state_json,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            workflow.workflow_id,
            workflow.workflow_name,
            workflow.status,
            workflow.cl_name,
            workflow.project_name,
            workflow.project_dir,
            workflow.project_file,
            workflow.workflow_dir_name,
            workflow.artifact_dir,
            workflow.agent_id,
            workflow.parent_workflow_id,
            workflow.parent_agent_id,
            workflow.current_step_index,
            workflow.pid,
            workflow.appears_as_agent,
            workflow.is_anonymous,
            workflow.started_at,
            workflow.finished_at,
            workflow.error,
            workflow.traceback,
            workflow.activity,
            workflow.retry_of_workflow_id,
            workflow.retried_as_workflow_id,
            workflow.retry_attempt,
            state_json,
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_step_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    step: &WorkflowStepProjectionWire,
) -> Result<(), ProjectionError> {
    let output_json = serde_json::to_string(&step.output)?;
    let output_types_json = serde_json::to_string(&step.output_types)?;
    let marker_json = serde_json::to_string(&step.marker)?;
    conn.execute(
        r#"
        INSERT INTO workflow_steps (
            project_id, workflow_id, step_index, name, status, step_type,
            step_source, parent_step_index, total_steps, parent_total_steps,
            hidden, is_pre_prompt_step, embedded_workflow_name, artifacts_dir,
            output_json, output_types_json, error, traceback, marker_json,
            last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
        )
        ON CONFLICT(project_id, workflow_id, step_index) DO UPDATE SET
            name = excluded.name,
            status = excluded.status,
            step_type = excluded.step_type,
            step_source = excluded.step_source,
            parent_step_index = excluded.parent_step_index,
            total_steps = excluded.total_steps,
            parent_total_steps = excluded.parent_total_steps,
            hidden = excluded.hidden,
            is_pre_prompt_step = excluded.is_pre_prompt_step,
            embedded_workflow_name = excluded.embedded_workflow_name,
            artifacts_dir = excluded.artifacts_dir,
            output_json = excluded.output_json,
            output_types_json = excluded.output_types_json,
            error = excluded.error,
            traceback = excluded.traceback,
            marker_json = excluded.marker_json,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            step.workflow_id,
            step.step_index,
            step.name,
            step.status,
            step.step_type,
            step.step_source,
            step.parent_step_index,
            step.total_steps,
            step.parent_total_steps,
            step.hidden,
            step.is_pre_prompt_step,
            step.embedded_workflow_name,
            step.artifacts_dir,
            output_json,
            output_types_json,
            step.error,
            step.traceback,
            marker_json,
            event.seq,
        ],
    )?;
    Ok(())
}

fn update_workflow_status_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    workflow_id: &str,
    status: &str,
    finished_at: Option<String>,
    error: Option<String>,
    traceback: Option<String>,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        UPDATE workflows
        SET status = ?3,
            finished_at = COALESCE(?4, finished_at),
            error = COALESCE(?5, error),
            traceback = COALESCE(?6, traceback),
            last_seq = ?7
        WHERE project_id = ?1 AND workflow_id = ?2
        "#,
        params![
            event.project_id,
            workflow_id,
            status,
            finished_at,
            error,
            traceback,
            event.seq,
        ],
    )?;
    Ok(())
}

fn insert_workflow_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    workflow_id: Option<&str>,
    step_index: Option<i64>,
) -> Result<(), ProjectionError> {
    let payload_json = serde_json::to_string(&event.payload)?;
    conn.execute(
        r#"
        INSERT INTO workflow_events (
            seq, project_id, event_type, workflow_id, step_index,
            payload_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(seq) DO UPDATE SET
            project_id = excluded.project_id,
            event_type = excluded.event_type,
            workflow_id = excluded.workflow_id,
            step_index = excluded.step_index,
            payload_json = excluded.payload_json,
            created_at = excluded.created_at
        "#,
        params![
            event.seq,
            event.project_id,
            event.event_type,
            workflow_id,
            step_index,
            payload_json,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn workflow_event_request(
    context: WorkflowProjectionEventContextWire,
    event_type: &str,
    payload: WorkflowProjectionEventPayloadWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
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

fn workflow_summary_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<WorkflowSummaryWire> {
    Ok(WorkflowSummaryWire {
        schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
        workflow_id: row.get(0)?,
        project_id: row.get(1)?,
        workflow_name: row.get(2)?,
        status: row.get(3)?,
        cl_name: row.get(4)?,
        agent_id: row.get(5)?,
        parent_workflow_id: row.get(6)?,
        parent_agent_id: row.get(7)?,
        current_step_index: row.get(8)?,
        started_at: row.get(9)?,
        finished_at: row.get(10)?,
        retry_attempt: row.get(11)?,
        last_seq: row.get(12)?,
    })
}

fn workflow_step_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<WorkflowStepProjectionWire> {
    let output_json: String = row.get(13)?;
    let output_types_json: String = row.get(14)?;
    let marker_json: String = row.get(17)?;
    Ok(WorkflowStepProjectionWire {
        schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
        workflow_id: row.get(0)?,
        step_index: row.get(1)?,
        name: row.get(2)?,
        status: row.get(3)?,
        step_type: row.get(4)?,
        step_source: row.get(5)?,
        parent_step_index: row.get(6)?,
        total_steps: row.get(7)?,
        parent_total_steps: row.get(8)?,
        hidden: row.get::<_, i64>(9)? != 0,
        is_pre_prompt_step: row.get::<_, i64>(10)? != 0,
        embedded_workflow_name: row.get(11)?,
        artifacts_dir: row.get(12)?,
        output: serde_json::from_str(&output_json).map_err(json_error)?,
        output_types: serde_json::from_str(&output_types_json)
            .map_err(json_error)?,
        error: row.get(15)?,
        traceback: row.get(16)?,
        marker: serde_json::from_str(&marker_json).map_err(json_error)?,
    })
}

fn workflow_event_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<WorkflowEventProjectionWire> {
    Ok(WorkflowEventProjectionWire {
        seq: row.get(0)?,
        event_type: row.get(1)?,
        workflow_id: row.get(2)?,
        step_index: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn collect_workflow_rows<T>(
    rows: rusqlite::MappedRows<'_, T>,
) -> Result<Vec<WorkflowSummaryWire>, ProjectionError>
where
    T: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<WorkflowSummaryWire>,
{
    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn truncate_for_next_offset<T>(
    entries: &mut Vec<T>,
    limit: u32,
    offset: u32,
) -> Option<u32> {
    if entries.len() > limit as usize {
        entries.truncate(limit as usize);
        Some(offset.saturating_add(limit))
    } else {
        None
    }
}

fn json_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(error),
    )
}

fn workflow_schema_version() -> u32 {
    WORKFLOW_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projections::event::EventSourceWire;

    fn context(key: &str) -> WorkflowProjectionEventContextWire {
        WorkflowProjectionEventContextWire {
            schema_version: WORKFLOW_WIRE_SCHEMA_VERSION,
            created_at: Some("2026-05-13T22:30:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "workflow-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: Some(key.to_string()),
            causality: Vec::new(),
            source_path: Some("workflow_state.json".to_string()),
            source_revision: None,
        }
    }

    #[test]
    fn workflow_live_projection_matches_replay_and_preserves_step_order() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let state = WorkflowStateWire {
            workflow_name: "ship".to_string(),
            status: "running".to_string(),
            current_step_index: 1,
            start_time: Some("2026-05-13T22:00:00.000Z".to_string()),
            steps: vec![
                WorkflowStepStateWire {
                    name: "plan".to_string(),
                    status: "completed".to_string(),
                    ..WorkflowStepStateWire::default()
                },
                WorkflowStepStateWire {
                    name: "land".to_string(),
                    status: "running".to_string(),
                    ..WorkflowStepStateWire::default()
                },
            ],
            ..WorkflowStateWire::default()
        };
        let mut workflow =
            WorkflowRunProjectionWire::from_state("wf-child", state);
        workflow.parent_workflow_id = Some("wf-parent".to_string());

        db.append_workflow_event(
            workflow_run_created_event_request(context("wf-create"), workflow)
                .unwrap(),
        )
        .unwrap();
        db.append_workflow_event(
            workflow_terminal_state_reached_event_request(
                context("wf-terminal"),
                "wf-child".to_string(),
                "completed".to_string(),
                Some("2026-05-13T22:10:00.000Z".to_string()),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();

        let live = workflow_projection_detail(
            db.connection(),
            "project-a",
            "wf-child",
        )
        .unwrap();
        assert_eq!(
            live.workflow
                .as_ref()
                .unwrap()
                .parent_workflow_id
                .as_deref(),
            Some("wf-parent")
        );
        assert_eq!(
            live.steps
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["plan", "land"]
        );

        db.connection()
            .execute_batch(
                "DELETE FROM workflow_events; DELETE FROM workflow_steps; DELETE FROM workflows;",
            )
            .unwrap();
        let mut applier = WorkflowProjectionApplier;
        db.replay_events(0, &mut [&mut applier]).unwrap();
        let replayed = workflow_projection_detail(
            db.connection(),
            "project-a",
            "wf-child",
        )
        .unwrap();
        assert_eq!(replayed, live);
    }
}
