use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::agent_archive::AgentArchiveSummaryWire;
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::index::RecordSummary;
use crate::agent_scan::AgentArtifactRecordWire;

use super::{
    AgentEventKind, ProjectionError, ProjectionEvent, ProjectionEventType,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLifecycleTransitionPayload {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_dir_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<f64>,
    #[serde(default)]
    pub has_waiting_marker: bool,
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAttemptRecordedPayload {
    pub agent_id: String,
    pub attempt_id: String,
    pub attempt_number: i64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retried_as_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_chain_root_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<AgentArtifactRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEdgeUpsertedPayload {
    pub parent_agent_id: String,
    pub child_agent_id: String,
    #[serde(default = "default_child_edge_kind")]
    pub edge_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkflowChildEdgeUpsertedPayload {
    pub workflow_agent_id: String,
    pub child_agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactUpsertedPayload {
    pub record: AgentArtifactRecordWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDismissedIdentityUpsertedPayload {
    pub identity: AgentCleanupIdentityWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dismissed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveBundleUpsertedPayload {
    pub summary: AgentArchiveSummaryWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRevivedPayload {
    #[serde(default)]
    pub agent_ids: Vec<String>,
    #[serde(default)]
    pub bundle_paths: Vec<String>,
    pub revived_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPurgedPayload {
    #[serde(default)]
    pub agent_ids: Vec<String>,
    #[serde(default)]
    pub bundle_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTombstonePayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<AgentCleanupIdentityWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionSnapshot {
    pub agents: Vec<AgentProjectionRow>,
    pub attempts: Vec<AgentAttemptProjectionRow>,
    pub edges: Vec<AgentEdgeProjectionRow>,
    pub artifacts: Vec<AgentArtifactProjectionRow>,
    pub archive: Vec<AgentArchiveProjectionRow>,
    pub dismissed_identities: Vec<AgentDismissedIdentityProjectionRow>,
    pub search: Vec<AgentSearchRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionRow {
    pub agent_id: String,
    pub family: String,
    pub current_attempt_id: Option<String>,
    pub agent_type: String,
    pub status: String,
    pub cl_name: Option<String>,
    pub agent_name: Option<String>,
    pub project_name: Option<String>,
    pub workflow_dir_name: Option<String>,
    pub model: Option<String>,
    pub llm_provider: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<f64>,
    pub has_waiting_marker: bool,
    pub has_pending_question: bool,
    pub hidden: bool,
    pub archived: bool,
    pub purged: bool,
    pub metadata: Option<JsonValue>,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAttemptProjectionRow {
    pub attempt_id: String,
    pub agent_id: String,
    pub attempt_number: i64,
    pub status: String,
    pub started_at: Option<String>,
    pub finished_at: Option<f64>,
    pub retry_of_attempt_id: Option<String>,
    pub retry_of_timestamp: Option<String>,
    pub retried_as_timestamp: Option<String>,
    pub retry_chain_root_timestamp: Option<String>,
    pub record: Option<AgentArtifactRecordWire>,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEdgeProjectionRow {
    pub parent_agent_id: String,
    pub child_agent_id: String,
    pub edge_kind: String,
    pub step_index: Option<i64>,
    pub step_name: Option<String>,
    pub metadata: Option<JsonValue>,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactProjectionRow {
    pub artifact_dir: String,
    pub agent_id: String,
    pub attempt_id: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    pub status: String,
    pub agent_type: String,
    pub cl_name: Option<String>,
    pub agent_name: Option<String>,
    pub has_done_marker: bool,
    pub has_running_marker: bool,
    pub has_waiting_marker: bool,
    pub has_pending_question: bool,
    pub has_workflow_state: bool,
    pub record: AgentArtifactRecordWire,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveProjectionRow {
    pub bundle_path: String,
    pub agent_id: String,
    pub raw_suffix: String,
    pub cl_name: String,
    pub agent_name: Option<String>,
    pub status: String,
    pub start_time: Option<String>,
    pub dismissed_at: Option<String>,
    pub revived_at: Option<String>,
    pub project_name: Option<String>,
    pub purged: bool,
    pub summary: AgentArchiveSummaryWire,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDismissedIdentityProjectionRow {
    pub identity_key: String,
    pub identity: AgentCleanupIdentityWire,
    pub agent_id: Option<String>,
    pub dismissed_at: Option<String>,
    pub metadata: Option<JsonValue>,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSearchRow {
    pub agent_id: String,
    pub summary: String,
}

pub fn apply_agent_event(
    tx: &Transaction<'_>,
    event: &ProjectionEvent,
) -> Result<(), ProjectionError> {
    let ProjectionEventType::Agent(kind) = &event.event_type else {
        return Ok(());
    };
    match kind {
        AgentEventKind::LifecycleTransition => {
            let payload: AgentLifecycleTransitionPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_agent_from_lifecycle(
                tx,
                event.seq,
                &event.timestamp,
                payload,
            )?;
        }
        AgentEventKind::AttemptRecorded => {
            let payload: AgentAttemptRecordedPayload =
                serde_json::from_value(event.payload.clone())?;
            ensure_agent_shell(
                tx,
                event.seq,
                &event.timestamp,
                &payload.agent_id,
            )?;
            upsert_attempt(tx, event.seq, &event.timestamp, &payload)?;
        }
        AgentEventKind::EdgeUpserted => {
            let payload: AgentEdgeUpsertedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_edge(
                tx,
                event.seq,
                &payload.parent_agent_id,
                &payload.child_agent_id,
                &payload.edge_kind,
                payload.step_index,
                payload.step_name.as_deref(),
                payload.metadata.as_ref(),
            )?;
        }
        AgentEventKind::WorkflowChildEdgeUpserted => {
            let payload: AgentWorkflowChildEdgeUpsertedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_edge(
                tx,
                event.seq,
                &payload.workflow_agent_id,
                &payload.child_agent_id,
                "workflow_child",
                payload.step_index,
                payload.step_name.as_deref(),
                payload.metadata.as_ref(),
            )?;
        }
        AgentEventKind::ArtifactUpserted => {
            let payload: AgentArtifactUpsertedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_artifact_record(
                tx,
                event.seq,
                &event.timestamp,
                &payload.record,
            )?;
        }
        AgentEventKind::DismissedIdentityUpserted => {
            let payload: AgentDismissedIdentityUpsertedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_dismissed_identity(
                tx,
                event.seq,
                &event.timestamp,
                &payload,
            )?;
        }
        AgentEventKind::ArchiveBundleUpserted => {
            let payload: AgentArchiveBundleUpsertedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_archive_summary(
                tx,
                event.seq,
                &event.timestamp,
                &payload.summary,
                payload.search_text.as_deref(),
            )?;
        }
        AgentEventKind::Revived => {
            let payload: AgentRevivedPayload =
                serde_json::from_value(event.payload.clone())?;
            revive_agents(tx, event.seq, &event.timestamp, &payload)?;
        }
        AgentEventKind::Purged => {
            let payload: AgentPurgedPayload =
                serde_json::from_value(event.payload.clone())?;
            purge_agents(tx, event.seq, &event.timestamp, &payload)?;
        }
        AgentEventKind::Tombstone => {
            let payload: AgentTombstonePayload =
                serde_json::from_value(event.payload.clone())?;
            tombstone_agent_rows(tx, &payload)?;
        }
    }
    Ok(())
}

pub fn agent_projection_snapshot(
    conn: &Connection,
) -> Result<AgentProjectionSnapshot, ProjectionError> {
    Ok(AgentProjectionSnapshot {
        agents: agent_rows(conn)?,
        attempts: attempt_rows(conn)?,
        edges: edge_rows(conn)?,
        artifacts: artifact_rows(conn)?,
        archive: archive_rows(conn)?,
        dismissed_identities: dismissed_identity_rows(conn)?,
        search: search_rows(conn)?,
    })
}

pub fn agent_artifact_records(
    conn: &Connection,
) -> Result<Vec<AgentArtifactRecordWire>, ProjectionError> {
    Ok(artifact_rows(conn)?
        .into_iter()
        .map(|row| row.record)
        .collect())
}

fn upsert_agent_from_lifecycle(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    payload: AgentLifecycleTransitionPayload,
) -> Result<(), ProjectionError> {
    let family = payload
        .family
        .unwrap_or_else(|| agent_family_from_id(&payload.agent_id));
    insert_or_update_agent(
        tx,
        seq,
        timestamp,
        &payload.agent_id,
        &family,
        payload.attempt_id.as_deref(),
        payload.agent_type.as_deref().unwrap_or("agent"),
        &payload.status,
        payload.cl_name.as_deref(),
        payload.agent_name.as_deref(),
        payload.project_name.as_deref(),
        payload.workflow_dir_name.as_deref(),
        payload.model.as_deref(),
        payload.llm_provider.as_deref(),
        payload.started_at.as_deref(),
        payload.finished_at,
        payload.has_waiting_marker,
        payload.has_pending_question,
        payload.hidden,
        false,
        false,
        payload.metadata.as_ref(),
    )?;
    replace_agent_search(
        tx,
        &payload.agent_id,
        &agent_summary_from_parts(
            &payload.agent_id,
            payload.agent_name.as_deref(),
            payload.cl_name.as_deref(),
            payload.project_name.as_deref(),
            &payload.status,
            None,
        ),
    )?;
    Ok(())
}

fn upsert_artifact_record(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    record: &AgentArtifactRecordWire,
) -> Result<(), ProjectionError> {
    let summary = RecordSummary::from_record(record);
    let agent_id = agent_id_from_record(record);
    let attempt_id = attempt_id_from_record(record);
    insert_or_update_agent(
        tx,
        seq,
        timestamp,
        &agent_id,
        &agent_family_from_id(&agent_id),
        Some(&attempt_id),
        &summary.agent_type,
        &summary.status,
        summary.cl_name.as_deref(),
        summary.agent_name.as_deref(),
        Some(&record.project_name),
        Some(&record.workflow_dir_name),
        summary.model.as_deref(),
        summary.llm_provider.as_deref(),
        summary.started_at.as_deref(),
        summary.finished_at,
        record.waiting.is_some(),
        record.pending_question.is_some(),
        summary.hidden,
        false,
        false,
        None,
    )?;

    let attempt = AgentAttemptRecordedPayload {
        agent_id: agent_id.clone(),
        attempt_id: attempt_id.clone(),
        attempt_number: summary.retry_attempt.unwrap_or(0).max(0),
        status: summary.status.clone(),
        started_at: summary.started_at.clone(),
        finished_at: summary.finished_at,
        retry_of_attempt_id: summary
            .retry_of_timestamp
            .as_ref()
            .map(|ts| format!("{}:{ts}", record.workflow_dir_name)),
        retry_of_timestamp: summary.retry_of_timestamp.clone(),
        retried_as_timestamp: summary.retried_as_timestamp.clone(),
        retry_chain_root_timestamp: summary.retry_chain_root_timestamp.clone(),
        record: Some(record.clone()),
    };
    upsert_attempt(tx, seq, timestamp, &attempt)?;

    let record_json = serde_json::to_string(record)?;
    tx.execute(
        "INSERT INTO agent_artifacts(
             artifact_dir,
             agent_id,
             attempt_id,
             project_name,
             project_dir,
             project_file,
             workflow_dir_name,
             timestamp,
             status,
             agent_type,
             cl_name,
             agent_name,
             model,
             llm_provider,
             started_at,
             finished_at,
             has_done_marker,
             has_running_marker,
             has_waiting_marker,
             has_pending_question,
             has_workflow_state,
             workflow_status,
             hidden,
             parent_timestamp,
             step_index,
             step_name,
             retry_of_timestamp,
             retried_as_timestamp,
             retry_chain_root_timestamp,
             retry_attempt,
             record_json,
             updated_seq,
             updated_at
         )
         VALUES(
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
             ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
             ?31, ?32, ?33
         )
         ON CONFLICT(artifact_dir) DO UPDATE SET
             agent_id = excluded.agent_id,
             attempt_id = excluded.attempt_id,
             project_name = excluded.project_name,
             project_dir = excluded.project_dir,
             project_file = excluded.project_file,
             workflow_dir_name = excluded.workflow_dir_name,
             timestamp = excluded.timestamp,
             status = excluded.status,
             agent_type = excluded.agent_type,
             cl_name = excluded.cl_name,
             agent_name = excluded.agent_name,
             model = excluded.model,
             llm_provider = excluded.llm_provider,
             started_at = excluded.started_at,
             finished_at = excluded.finished_at,
             has_done_marker = excluded.has_done_marker,
             has_running_marker = excluded.has_running_marker,
             has_waiting_marker = excluded.has_waiting_marker,
             has_pending_question = excluded.has_pending_question,
             has_workflow_state = excluded.has_workflow_state,
             workflow_status = excluded.workflow_status,
             hidden = excluded.hidden,
             parent_timestamp = excluded.parent_timestamp,
             step_index = excluded.step_index,
             step_name = excluded.step_name,
             retry_of_timestamp = excluded.retry_of_timestamp,
             retried_as_timestamp = excluded.retried_as_timestamp,
             retry_chain_root_timestamp = excluded.retry_chain_root_timestamp,
             retry_attempt = excluded.retry_attempt,
             record_json = excluded.record_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            record.artifact_dir,
            agent_id,
            attempt_id,
            record.project_name,
            record.project_dir,
            record.project_file,
            record.workflow_dir_name,
            record.timestamp,
            summary.status,
            summary.agent_type,
            summary.cl_name,
            summary.agent_name,
            summary.model,
            summary.llm_provider,
            summary.started_at,
            summary.finished_at,
            bool_int(record.has_done_marker),
            bool_int(record.running.is_some()),
            bool_int(record.waiting.is_some()),
            bool_int(record.pending_question.is_some()),
            bool_int(record.workflow_state.is_some()),
            summary.workflow_status,
            bool_int(summary.hidden),
            summary.parent_timestamp,
            summary.step_index,
            summary.step_name,
            summary.retry_of_timestamp,
            summary.retried_as_timestamp,
            summary.retry_chain_root_timestamp,
            summary.retry_attempt,
            record_json,
            seq,
            timestamp,
        ],
    )?;

    if let Some(parent_timestamp) = parent_timestamp_from_record(record) {
        upsert_edge(
            tx,
            seq,
            &format!("{}:{parent_timestamp}", record.workflow_dir_name),
            &agent_id,
            "parent_child",
            summary.step_index,
            summary.step_name.as_deref(),
            None,
        )?;
    }
    replace_agent_search(tx, &agent_id, &agent_summary_from_record(record))?;
    Ok(())
}

fn ensure_agent_shell(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    agent_id: &str,
) -> Result<(), ProjectionError> {
    insert_or_update_agent(
        tx,
        seq,
        timestamp,
        agent_id,
        &agent_family_from_id(agent_id),
        None,
        "agent",
        "unknown",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        false,
        false,
        false,
        false,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_or_update_agent(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    agent_id: &str,
    family: &str,
    current_attempt_id: Option<&str>,
    agent_type: &str,
    status: &str,
    cl_name: Option<&str>,
    agent_name: Option<&str>,
    project_name: Option<&str>,
    workflow_dir_name: Option<&str>,
    model: Option<&str>,
    llm_provider: Option<&str>,
    started_at: Option<&str>,
    finished_at: Option<f64>,
    has_waiting_marker: bool,
    has_pending_question: bool,
    hidden: bool,
    archived: bool,
    purged: bool,
    metadata: Option<&JsonValue>,
) -> Result<(), ProjectionError> {
    let metadata_json = metadata.map(serde_json::to_string).transpose()?;
    tx.execute(
        "INSERT INTO agents(
             agent_id,
             family,
             current_attempt_id,
             agent_type,
             status,
             cl_name,
             agent_name,
             project_name,
             workflow_dir_name,
             model,
             llm_provider,
             started_at,
             finished_at,
             has_waiting_marker,
             has_pending_question,
             hidden,
             archived,
             purged,
             metadata_json,
             updated_seq,
             updated_at
         )
         VALUES(
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21
         )
         ON CONFLICT(agent_id) DO UPDATE SET
             family = excluded.family,
             current_attempt_id = COALESCE(excluded.current_attempt_id, agents.current_attempt_id),
             agent_type = excluded.agent_type,
             status = excluded.status,
             cl_name = COALESCE(excluded.cl_name, agents.cl_name),
             agent_name = COALESCE(excluded.agent_name, agents.agent_name),
             project_name = COALESCE(excluded.project_name, agents.project_name),
             workflow_dir_name = COALESCE(excluded.workflow_dir_name, agents.workflow_dir_name),
             model = COALESCE(excluded.model, agents.model),
             llm_provider = COALESCE(excluded.llm_provider, agents.llm_provider),
             started_at = COALESCE(excluded.started_at, agents.started_at),
             finished_at = COALESCE(excluded.finished_at, agents.finished_at),
             has_waiting_marker = excluded.has_waiting_marker,
             has_pending_question = excluded.has_pending_question,
             hidden = excluded.hidden,
             archived = CASE WHEN excluded.archived = 1 THEN 1 ELSE agents.archived END,
             purged = CASE WHEN excluded.purged = 1 THEN 1 ELSE agents.purged END,
             metadata_json = COALESCE(excluded.metadata_json, agents.metadata_json),
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            agent_id,
            family,
            current_attempt_id,
            agent_type,
            status,
            cl_name,
            agent_name,
            project_name,
            workflow_dir_name,
            model,
            llm_provider,
            started_at,
            finished_at,
            bool_int(has_waiting_marker),
            bool_int(has_pending_question),
            bool_int(hidden),
            bool_int(archived),
            bool_int(purged),
            metadata_json,
            seq,
            timestamp,
        ],
    )?;
    Ok(())
}

fn upsert_attempt(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    payload: &AgentAttemptRecordedPayload,
) -> Result<(), ProjectionError> {
    let record_json = payload
        .record
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    tx.execute(
        "INSERT INTO agent_attempts(
             attempt_id,
             agent_id,
             attempt_number,
             status,
             started_at,
             finished_at,
             retry_of_attempt_id,
             retry_of_timestamp,
             retried_as_timestamp,
             retry_chain_root_timestamp,
             record_json,
             updated_seq,
             updated_at
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(attempt_id) DO UPDATE SET
             agent_id = excluded.agent_id,
             attempt_number = excluded.attempt_number,
             status = excluded.status,
             started_at = excluded.started_at,
             finished_at = excluded.finished_at,
             retry_of_attempt_id = excluded.retry_of_attempt_id,
             retry_of_timestamp = excluded.retry_of_timestamp,
             retried_as_timestamp = excluded.retried_as_timestamp,
             retry_chain_root_timestamp = excluded.retry_chain_root_timestamp,
             record_json = excluded.record_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            payload.attempt_id,
            payload.agent_id,
            payload.attempt_number,
            payload.status,
            payload.started_at,
            payload.finished_at,
            payload.retry_of_attempt_id,
            payload.retry_of_timestamp,
            payload.retried_as_timestamp,
            payload.retry_chain_root_timestamp,
            record_json,
            seq,
            timestamp,
        ],
    )?;
    tx.execute(
        "UPDATE agents
         SET current_attempt_id = ?1, updated_seq = ?2, updated_at = ?3
         WHERE agent_id = ?4",
        params![payload.attempt_id, seq, timestamp, payload.agent_id],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn upsert_edge(
    tx: &Transaction<'_>,
    seq: i64,
    parent_agent_id: &str,
    child_agent_id: &str,
    edge_kind: &str,
    step_index: Option<i64>,
    step_name: Option<&str>,
    metadata: Option<&JsonValue>,
) -> Result<(), ProjectionError> {
    let metadata_json = metadata.map(serde_json::to_string).transpose()?;
    tx.execute(
        "INSERT INTO agent_edges(
             parent_agent_id,
             child_agent_id,
             edge_kind,
             step_index,
             step_name,
             metadata_json,
             updated_seq
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(parent_agent_id, child_agent_id, edge_kind)
         DO UPDATE SET
             step_index = excluded.step_index,
             step_name = excluded.step_name,
             metadata_json = excluded.metadata_json,
             updated_seq = excluded.updated_seq",
        params![
            parent_agent_id,
            child_agent_id,
            edge_kind,
            step_index,
            step_name,
            metadata_json,
            seq,
        ],
    )?;
    Ok(())
}

fn upsert_dismissed_identity(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    payload: &AgentDismissedIdentityUpsertedPayload,
) -> Result<(), ProjectionError> {
    let identity_key = identity_key(&payload.identity);
    let metadata_json = payload
        .metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    tx.execute(
        "INSERT INTO dismissed_identities(
             identity_key,
             agent_type,
             cl_name,
             raw_suffix,
             agent_id,
             dismissed_at,
             metadata_json,
             updated_seq,
             updated_at
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(identity_key) DO UPDATE SET
             agent_type = excluded.agent_type,
             cl_name = excluded.cl_name,
             raw_suffix = excluded.raw_suffix,
             agent_id = excluded.agent_id,
             dismissed_at = excluded.dismissed_at,
             metadata_json = excluded.metadata_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            identity_key,
            payload.identity.agent_type,
            payload.identity.cl_name,
            payload.identity.raw_suffix,
            payload.agent_id,
            payload.dismissed_at,
            metadata_json,
            seq,
            timestamp,
        ],
    )?;
    Ok(())
}

fn upsert_archive_summary(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    summary: &AgentArchiveSummaryWire,
    search_text: Option<&str>,
) -> Result<(), ProjectionError> {
    let summary_json = serde_json::to_string(summary)?;
    tx.execute(
        "INSERT INTO agent_archive(
             bundle_path,
             agent_id,
             raw_suffix,
             cl_name,
             agent_name,
             status,
             start_time,
             dismissed_at,
             revived_at,
             project_name,
             model,
             runtime,
             llm_provider,
             step_index,
             step_name,
             step_type,
             retry_attempt,
             is_workflow_child,
             purged,
             summary_json,
             updated_seq,
             updated_at
         )
         VALUES(
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 0, ?19, ?20, ?21
         )
         ON CONFLICT(bundle_path) DO UPDATE SET
             agent_id = excluded.agent_id,
             raw_suffix = excluded.raw_suffix,
             cl_name = excluded.cl_name,
             agent_name = excluded.agent_name,
             status = excluded.status,
             start_time = excluded.start_time,
             dismissed_at = excluded.dismissed_at,
             revived_at = excluded.revived_at,
             project_name = excluded.project_name,
             model = excluded.model,
             runtime = excluded.runtime,
             llm_provider = excluded.llm_provider,
             step_index = excluded.step_index,
             step_name = excluded.step_name,
             step_type = excluded.step_type,
             retry_attempt = excluded.retry_attempt,
             is_workflow_child = excluded.is_workflow_child,
             purged = 0,
             summary_json = excluded.summary_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            summary.bundle_path,
            summary.agent_id,
            summary.raw_suffix,
            summary.cl_name,
            summary.agent_name,
            summary.status,
            summary.start_time,
            summary.dismissed_at,
            summary.revived_at,
            summary.project_name,
            summary.model,
            summary.runtime,
            summary.llm_provider,
            summary.step_index,
            summary.step_name,
            summary.step_type,
            summary.retry_attempt,
            bool_int(summary.is_workflow_child),
            summary_json,
            seq,
            timestamp,
        ],
    )?;
    insert_or_update_agent(
        tx,
        seq,
        timestamp,
        &summary.agent_id,
        &agent_family_from_id(&summary.agent_id),
        Some(&summary.raw_suffix),
        if summary.is_workflow_child {
            "workflow_child"
        } else {
            "agent"
        },
        &summary.status,
        Some(&summary.cl_name),
        summary.agent_name.as_deref(),
        summary.project_name.as_deref(),
        None,
        summary.model.as_deref(),
        summary.llm_provider.as_deref(),
        summary.start_time.as_deref(),
        None,
        false,
        false,
        false,
        true,
        false,
        None,
    )?;
    replace_agent_search(
        tx,
        &summary.agent_id,
        &archive_summary_text(summary, search_text),
    )?;
    Ok(())
}

fn revive_agents(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    payload: &AgentRevivedPayload,
) -> Result<(), ProjectionError> {
    for agent_id in &payload.agent_ids {
        tx.execute(
            "UPDATE agents
             SET archived = 0,
                 purged = 0,
                 status = 'revived',
                 updated_seq = ?1,
                 updated_at = ?2
             WHERE agent_id = ?3",
            params![seq, timestamp, agent_id],
        )?;
    }
    for bundle_path in &payload.bundle_paths {
        tx.execute(
            "UPDATE agent_archive
             SET revived_at = ?1,
                 purged = 0,
                 updated_seq = ?2,
                 updated_at = ?3
             WHERE bundle_path = ?4",
            params![payload.revived_at, seq, timestamp, bundle_path],
        )?;
    }
    Ok(())
}

fn purge_agents(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    payload: &AgentPurgedPayload,
) -> Result<(), ProjectionError> {
    for agent_id in &payload.agent_ids {
        tx.execute(
            "UPDATE agents
             SET purged = 1,
                 updated_seq = ?1,
                 updated_at = ?2
             WHERE agent_id = ?3",
            params![seq, timestamp, agent_id],
        )?;
        tx.execute(
            "DELETE FROM agent_search_fts WHERE agent_id = ?1",
            [agent_id],
        )?;
    }
    for bundle_path in &payload.bundle_paths {
        tx.execute(
            "UPDATE agent_archive
             SET purged = 1,
                 updated_seq = ?1,
                 updated_at = ?2
             WHERE bundle_path = ?3",
            params![seq, timestamp, bundle_path],
        )?;
    }
    Ok(())
}

fn tombstone_agent_rows(
    tx: &Transaction<'_>,
    payload: &AgentTombstonePayload,
) -> Result<(), ProjectionError> {
    if let Some(agent_id) = &payload.agent_id {
        tx.execute(
            "DELETE FROM agent_search_fts WHERE agent_id = ?1",
            [agent_id],
        )?;
        tx.execute(
            "DELETE FROM agent_archive WHERE agent_id = ?1",
            [agent_id],
        )?;
        tx.execute(
            "DELETE FROM dismissed_identities WHERE agent_id = ?1",
            [agent_id],
        )?;
        tx.execute("DELETE FROM agents WHERE agent_id = ?1", [agent_id])?;
    }
    if let Some(artifact_dir) = &payload.artifact_dir {
        tx.execute(
            "DELETE FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir],
        )?;
    }
    if let Some(bundle_path) = &payload.bundle_path {
        tx.execute(
            "DELETE FROM agent_archive WHERE bundle_path = ?1",
            [bundle_path],
        )?;
    }
    if let Some(identity) = &payload.identity {
        tx.execute(
            "DELETE FROM dismissed_identities WHERE identity_key = ?1",
            [identity_key(identity)],
        )?;
    }
    Ok(())
}

fn replace_agent_search(
    tx: &Transaction<'_>,
    agent_id: &str,
    summary: &str,
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM agent_search_fts WHERE agent_id = ?1",
        [agent_id],
    )?;
    tx.execute(
        "INSERT INTO agent_search_fts(agent_id, summary) VALUES(?1, ?2)",
        params![agent_id, summary.chars().take(4096).collect::<String>()],
    )?;
    Ok(())
}

fn agent_id_from_record(record: &AgentArtifactRecordWire) -> String {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.artifact_agent_id.clone())
        .or_else(|| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.clone())
        })
        .map(|name| format!("{}:{name}", record.workflow_dir_name))
        .unwrap_or_else(|| {
            format!("{}:{}", record.workflow_dir_name, record.timestamp)
        })
}

fn attempt_id_from_record(record: &AgentArtifactRecordWire) -> String {
    format!("{}:{}", record.workflow_dir_name, record.timestamp)
}

fn agent_family_from_id(agent_id: &str) -> String {
    agent_id
        .split_once('.')
        .map(|(family, _)| family.to_string())
        .unwrap_or_else(|| {
            agent_id
                .split_once(':')
                .map(|(family, _)| family.to_string())
                .unwrap_or_else(|| agent_id.to_string())
        })
}

fn parent_timestamp_from_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    record.agent_meta.as_ref().and_then(|meta| {
        meta.parent_agent_timestamp
            .clone()
            .or_else(|| meta.parent_timestamp.clone())
    })
}

fn identity_key(identity: &AgentCleanupIdentityWire) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        identity.agent_type,
        identity.cl_name,
        identity.raw_suffix.as_deref().unwrap_or("")
    )
}

fn agent_summary_from_record(record: &AgentArtifactRecordWire) -> String {
    let summary = RecordSummary::from_record(record);
    agent_summary_from_parts(
        &agent_id_from_record(record),
        summary.agent_name.as_deref(),
        summary.cl_name.as_deref(),
        Some(&record.project_name),
        &summary.status,
        record.raw_prompt_snippet.as_deref(),
    )
}

fn agent_summary_from_parts(
    agent_id: &str,
    agent_name: Option<&str>,
    cl_name: Option<&str>,
    project_name: Option<&str>,
    status: &str,
    extra: Option<&str>,
) -> String {
    let mut parts = vec![agent_id, status];
    if let Some(agent_name) = agent_name {
        parts.push(agent_name);
    }
    if let Some(cl_name) = cl_name {
        parts.push(cl_name);
    }
    if let Some(project_name) = project_name {
        parts.push(project_name);
    }
    if let Some(extra) = extra {
        parts.push(extra);
    }
    parts.join("\n")
}

fn archive_summary_text(
    summary: &AgentArchiveSummaryWire,
    search_text: Option<&str>,
) -> String {
    let mut parts = vec![
        summary.agent_id.as_str(),
        summary.raw_suffix.as_str(),
        summary.cl_name.as_str(),
        summary.status.as_str(),
    ];
    if let Some(agent_name) = &summary.agent_name {
        parts.push(agent_name);
    }
    if let Some(project_name) = &summary.project_name {
        parts.push(project_name);
    }
    if let Some(step_name) = &summary.step_name {
        parts.push(step_name);
    }
    if let Some(search_text) = search_text {
        parts.push(search_text);
    }
    parts.join("\n")
}

fn default_child_edge_kind() -> String {
    "parent_child".to_string()
}

fn bool_int(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn bool_from_i64(value: i64) -> bool {
    value != 0
}

fn parse_json_option(
    value: Option<String>,
) -> Result<Option<JsonValue>, ProjectionError> {
    value
        .map(|raw| serde_json::from_str(&raw))
        .transpose()
        .map_err(Into::into)
}

fn agent_rows(
    conn: &Connection,
) -> Result<Vec<AgentProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             agent_id,
             family,
             current_attempt_id,
             agent_type,
             status,
             cl_name,
             agent_name,
             project_name,
             workflow_dir_name,
             model,
             llm_provider,
             started_at,
             finished_at,
             has_waiting_marker,
             has_pending_question,
             hidden,
             archived,
             purged,
             metadata_json,
             updated_seq
         FROM agents
         ORDER BY agent_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            AgentProjectionRow {
                agent_id: row.get("agent_id")?,
                family: row.get("family")?,
                current_attempt_id: row.get("current_attempt_id")?,
                agent_type: row.get("agent_type")?,
                status: row.get("status")?,
                cl_name: row.get("cl_name")?,
                agent_name: row.get("agent_name")?,
                project_name: row.get("project_name")?,
                workflow_dir_name: row.get("workflow_dir_name")?,
                model: row.get("model")?,
                llm_provider: row.get("llm_provider")?,
                started_at: row.get("started_at")?,
                finished_at: row.get("finished_at")?,
                has_waiting_marker: bool_from_i64(
                    row.get("has_waiting_marker")?,
                ),
                has_pending_question: bool_from_i64(
                    row.get("has_pending_question")?,
                ),
                hidden: bool_from_i64(row.get("hidden")?),
                archived: bool_from_i64(row.get("archived")?),
                purged: bool_from_i64(row.get("purged")?),
                metadata: None,
                updated_seq: row.get("updated_seq")?,
            },
            row.get::<_, Option<String>>("metadata_json")?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (mut item, metadata_json) = row?;
        item.metadata = parse_json_option(metadata_json)?;
        out.push(item);
    }
    Ok(out)
}

fn attempt_rows(
    conn: &Connection,
) -> Result<Vec<AgentAttemptProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             attempt_id,
             agent_id,
             attempt_number,
             status,
             started_at,
             finished_at,
             retry_of_attempt_id,
             retry_of_timestamp,
             retried_as_timestamp,
             retry_chain_root_timestamp,
             record_json,
             updated_seq
         FROM agent_attempts
         ORDER BY agent_id, attempt_number, attempt_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            AgentAttemptProjectionRow {
                attempt_id: row.get("attempt_id")?,
                agent_id: row.get("agent_id")?,
                attempt_number: row.get("attempt_number")?,
                status: row.get("status")?,
                started_at: row.get("started_at")?,
                finished_at: row.get("finished_at")?,
                retry_of_attempt_id: row.get("retry_of_attempt_id")?,
                retry_of_timestamp: row.get("retry_of_timestamp")?,
                retried_as_timestamp: row.get("retried_as_timestamp")?,
                retry_chain_root_timestamp: row
                    .get("retry_chain_root_timestamp")?,
                record: None,
                updated_seq: row.get("updated_seq")?,
            },
            row.get::<_, Option<String>>("record_json")?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (mut item, record_json) = row?;
        item.record = record_json
            .map(|raw| serde_json::from_str(&raw))
            .transpose()?;
        out.push(item);
    }
    Ok(out)
}

fn edge_rows(
    conn: &Connection,
) -> Result<Vec<AgentEdgeProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             parent_agent_id,
             child_agent_id,
             edge_kind,
             step_index,
             step_name,
             metadata_json,
             updated_seq
         FROM agent_edges
         ORDER BY parent_agent_id, child_agent_id, edge_kind",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            AgentEdgeProjectionRow {
                parent_agent_id: row.get("parent_agent_id")?,
                child_agent_id: row.get("child_agent_id")?,
                edge_kind: row.get("edge_kind")?,
                step_index: row.get("step_index")?,
                step_name: row.get("step_name")?,
                metadata: None,
                updated_seq: row.get("updated_seq")?,
            },
            row.get::<_, Option<String>>("metadata_json")?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (mut item, metadata_json) = row?;
        item.metadata = parse_json_option(metadata_json)?;
        out.push(item);
    }
    Ok(out)
}

fn artifact_rows(
    conn: &Connection,
) -> Result<Vec<AgentArtifactProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             artifact_dir,
             agent_id,
             attempt_id,
             project_name,
             workflow_dir_name,
             timestamp,
             status,
             agent_type,
             cl_name,
             agent_name,
             has_done_marker,
             has_running_marker,
             has_waiting_marker,
             has_pending_question,
             has_workflow_state,
             record_json,
             updated_seq
         FROM agent_artifacts
         ORDER BY artifact_dir",
    )?;
    let rows = stmt.query_map([], |row| {
        let record_json: String = row.get("record_json")?;
        Ok((AgentArtifactProjectionRow {
            artifact_dir: row.get("artifact_dir")?,
            agent_id: row.get("agent_id")?,
            attempt_id: row.get("attempt_id")?,
            project_name: row.get("project_name")?,
            workflow_dir_name: row.get("workflow_dir_name")?,
            timestamp: row.get("timestamp")?,
            status: row.get("status")?,
            agent_type: row.get("agent_type")?,
            cl_name: row.get("cl_name")?,
            agent_name: row.get("agent_name")?,
            has_done_marker: bool_from_i64(row.get("has_done_marker")?),
            has_running_marker: bool_from_i64(row.get("has_running_marker")?),
            has_waiting_marker: bool_from_i64(row.get("has_waiting_marker")?),
            has_pending_question: bool_from_i64(
                row.get("has_pending_question")?,
            ),
            has_workflow_state: bool_from_i64(row.get("has_workflow_state")?),
            record: serde_json::from_str(&record_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    record_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            updated_seq: row.get("updated_seq")?,
        },))
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?.0);
    }
    Ok(out)
}

fn archive_rows(
    conn: &Connection,
) -> Result<Vec<AgentArchiveProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             bundle_path,
             agent_id,
             raw_suffix,
             cl_name,
             agent_name,
             status,
             start_time,
             dismissed_at,
             revived_at,
             project_name,
             purged,
             summary_json,
             updated_seq
         FROM agent_archive
         ORDER BY bundle_path",
    )?;
    let rows = stmt.query_map([], |row| {
        let summary_json: String = row.get("summary_json")?;
        Ok(AgentArchiveProjectionRow {
            bundle_path: row.get("bundle_path")?,
            agent_id: row.get("agent_id")?,
            raw_suffix: row.get("raw_suffix")?,
            cl_name: row.get("cl_name")?,
            agent_name: row.get("agent_name")?,
            status: row.get("status")?,
            start_time: row.get("start_time")?,
            dismissed_at: row.get("dismissed_at")?,
            revived_at: row.get("revived_at")?,
            project_name: row.get("project_name")?,
            purged: bool_from_i64(row.get("purged")?),
            summary: serde_json::from_str(&summary_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    summary_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            updated_seq: row.get("updated_seq")?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn dismissed_identity_rows(
    conn: &Connection,
) -> Result<Vec<AgentDismissedIdentityProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             identity_key,
             agent_type,
             cl_name,
             raw_suffix,
             agent_id,
             dismissed_at,
             metadata_json,
             updated_seq
         FROM dismissed_identities
         ORDER BY identity_key",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            AgentDismissedIdentityProjectionRow {
                identity_key: row.get("identity_key")?,
                identity: AgentCleanupIdentityWire {
                    agent_type: row.get("agent_type")?,
                    cl_name: row.get("cl_name")?,
                    raw_suffix: row.get("raw_suffix")?,
                },
                agent_id: row.get("agent_id")?,
                dismissed_at: row.get("dismissed_at")?,
                metadata: None,
                updated_seq: row.get("updated_seq")?,
            },
            row.get::<_, Option<String>>("metadata_json")?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (mut item, metadata_json) = row?;
        item.metadata = parse_json_option(metadata_json)?;
        out.push(item);
    }
    Ok(out)
}

fn search_rows(
    conn: &Connection,
) -> Result<Vec<AgentSearchRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT agent_id, summary FROM agent_search_fts ORDER BY agent_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(AgentSearchRow {
            agent_id: row.get("agent_id")?,
            summary: row.get("summary")?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}
