use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::agent_archive::AgentArchiveSummaryWire;
use crate::agent_scan::{
    scan_agent_artifact_dir, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanWire,
};

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire,
};
use super::indexing::{
    ShadowDiffCategoryWire, ShadowDiffCountsWire, ShadowDiffRecordWire,
    ShadowDiffReportWire, SourceChangeOperationWire, SourceChangeWire,
    SourceIdentityWire, INDEXING_WIRE_SCHEMA_VERSION,
};
use super::replay::ProjectionApplier;

pub const AGENT_PROJECTION_NAME: &str = "agents";

pub const AGENT_EVENT_MARKER_OBSERVED: &str = "agent.lifecycle_marker_observed";
pub const AGENT_EVENT_LIFECYCLE_TRANSITIONED: &str =
    "agent.lifecycle_transitioned";
pub const AGENT_EVENT_RECONCILIATION_REPAIR_EMITTED: &str =
    "agent.reconciliation_repair_emitted";
pub const AGENT_EVENT_ATTEMPT_CREATED: &str = "agent.attempt_created";
pub const AGENT_EVENT_ATTEMPT_UPDATED: &str = "agent.attempt_updated";
pub const AGENT_EVENT_EDGE_OBSERVED: &str = "agent.edge_observed";
pub const AGENT_EVENT_ARTIFACT_ASSOCIATED: &str = "agent.artifact_associated";
pub const AGENT_EVENT_DISMISSED_IDENTITY_CHANGED: &str =
    "agent.dismissed_identity_changed";
pub const AGENT_EVENT_ARCHIVE_BUNDLE_INDEXED: &str =
    "agent.archive_bundle_indexed";
pub const AGENT_EVENT_ARCHIVE_BUNDLE_REVIVED: &str =
    "agent.archive_bundle_revived";
pub const AGENT_EVENT_ARCHIVE_BUNDLE_PURGED: &str =
    "agent.archive_bundle_purged";
pub const AGENT_EVENT_CLEANUP_RESULT_RECORDED: &str =
    "agent.cleanup_result_recorded";

const AGENT_PROJECTION_WIRE_SCHEMA_VERSION: u32 = 1;
const AGENT_INDEXING_DOMAIN: &str = "agents";

pub const AGENT_INDEXED_MARKER_FILES: &[&str] = &[
    "done.json",
    "agent_meta.json",
    "running.json",
    "waiting.json",
    "pending_question.json",
    "workflow_state.json",
    "plan_path.json",
    "raw_xprompt.md",
];

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionEventContextWire {
    #[serde(default = "agent_projection_schema_version")]
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
#[allow(clippy::large_enum_variant)]
pub enum AgentProjectionEventPayloadWire {
    LifecycleMarkerObserved {
        schema_version: u32,
        record: AgentArtifactRecordWire,
    },
    LifecycleTransitioned {
        schema_version: u32,
        lifecycle: AgentLifecycleProjectionWire,
    },
    ReconciliationRepairEmitted {
        schema_version: u32,
        repair: AgentLifecycleReconciliationRepairWire,
    },
    AttemptCreated {
        schema_version: u32,
        attempt: AgentAttemptProjectionWire,
    },
    AttemptUpdated {
        schema_version: u32,
        attempt: AgentAttemptProjectionWire,
    },
    EdgeObserved {
        schema_version: u32,
        edge: AgentEdgeProjectionWire,
    },
    ArtifactAssociated {
        schema_version: u32,
        artifact: AgentArtifactAssociationWire,
    },
    DismissedIdentityChanged {
        schema_version: u32,
        identity: AgentDismissedIdentityWire,
    },
    ArchiveBundleIndexed {
        schema_version: u32,
        archive: AgentArchiveSummaryWire,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bundle: Option<JsonValue>,
    },
    ArchiveBundleRevived {
        schema_version: u32,
        bundle_path: String,
        revived_at: String,
    },
    ArchiveBundlePurged {
        schema_version: u32,
        bundle_path: String,
    },
    CleanupResultRecorded {
        schema_version: u32,
        result: JsonValue,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAttemptProjectionWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub agent_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retried_as_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_chain_root_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_attempt: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_claim_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<String>,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEdgeProjectionWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub parent_agent_id: String,
    pub child_agent_id: String,
    pub edge_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactAssociationWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub agent_id: String,
    pub artifact_path: String,
    pub artifact_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDismissedIdentityWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub agent_type: String,
    pub cl_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dismissed_name: Option<String>,
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changed_at: Option<String>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentLifecycleStatusWire {
    #[default]
    Planned,
    Queued,
    Starting,
    Running,
    Waiting,
    Completed,
    Failed,
    Killed,
    Stale,
}

impl AgentLifecycleStatusWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Killed => "killed",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLifecycleProjectionWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub agent_id: String,
    #[serde(default)]
    pub lifecycle_status: AgentLifecycleStatusWire,
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
    pub timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_of_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_claim_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_changed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLifecycleReconciliationRepairWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub repair_id: String,
    pub agent_id: String,
    pub repair_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    pub checked_at: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionSummaryWire {
    pub schema_version: u32,
    pub agent_id: String,
    pub project_id: String,
    pub project_name: String,
    pub project_dir: String,
    pub project_file: String,
    pub workflow_dir_name: String,
    pub artifact_dir: String,
    pub timestamp: String,
    pub status: String,
    pub agent_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<f64>,
    pub hidden: bool,
    pub has_done_marker: bool,
    pub has_running_marker: bool,
    pub has_waiting_marker: bool,
    pub has_workflow_state: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_of_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_claim_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_changed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    pub last_seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionPageWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub entries: Vec<AgentProjectionSummaryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArchiveProjectionPageWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub entries: Vec<AgentArchiveSummaryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentLifecycleDetailWire {
    #[serde(default = "agent_projection_schema_version")]
    pub schema_version: u32,
    pub summary: Option<AgentProjectionSummaryWire>,
    pub attempts: Vec<AgentAttemptProjectionWire>,
    pub edges: Vec<AgentEdgeProjectionWire>,
    pub events: Vec<AgentLifecycleReconciliationRepairWire>,
}

pub struct AgentProjectionApplier;

impl ProjectionApplier for AgentProjectionApplier {
    fn projection_name(&self) -> &str {
        AGENT_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_agent_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_agent_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_agent_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    AGENT_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }
}

pub fn agent_lifecycle_marker_observed_event_request(
    context: AgentProjectionEventContextWire,
    record: AgentArtifactRecordWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_MARKER_OBSERVED,
        AgentProjectionEventPayloadWire::LifecycleMarkerObserved {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            record,
        },
    )
}

pub fn agent_lifecycle_transitioned_event_request(
    context: AgentProjectionEventContextWire,
    lifecycle: AgentLifecycleProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_LIFECYCLE_TRANSITIONED,
        AgentProjectionEventPayloadWire::LifecycleTransitioned {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            lifecycle,
        },
    )
}

pub fn agent_reconciliation_repair_emitted_event_request(
    context: AgentProjectionEventContextWire,
    repair: AgentLifecycleReconciliationRepairWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_RECONCILIATION_REPAIR_EMITTED,
        AgentProjectionEventPayloadWire::ReconciliationRepairEmitted {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            repair,
        },
    )
}

pub fn agent_attempt_created_event_request(
    context: AgentProjectionEventContextWire,
    attempt: AgentAttemptProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ATTEMPT_CREATED,
        AgentProjectionEventPayloadWire::AttemptCreated {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            attempt,
        },
    )
}

pub fn agent_attempt_updated_event_request(
    context: AgentProjectionEventContextWire,
    attempt: AgentAttemptProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ATTEMPT_UPDATED,
        AgentProjectionEventPayloadWire::AttemptUpdated {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            attempt,
        },
    )
}

pub fn agent_edge_observed_event_request(
    context: AgentProjectionEventContextWire,
    edge: AgentEdgeProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_EDGE_OBSERVED,
        AgentProjectionEventPayloadWire::EdgeObserved {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            edge,
        },
    )
}

pub fn agent_artifact_associated_event_request(
    context: AgentProjectionEventContextWire,
    artifact: AgentArtifactAssociationWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ARTIFACT_ASSOCIATED,
        AgentProjectionEventPayloadWire::ArtifactAssociated {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            artifact,
        },
    )
}

pub fn agent_dismissed_identity_changed_event_request(
    context: AgentProjectionEventContextWire,
    identity: AgentDismissedIdentityWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_DISMISSED_IDENTITY_CHANGED,
        AgentProjectionEventPayloadWire::DismissedIdentityChanged {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            identity,
        },
    )
}

pub fn agent_archive_bundle_indexed_event_request(
    context: AgentProjectionEventContextWire,
    archive: AgentArchiveSummaryWire,
    bundle: Option<JsonValue>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ARCHIVE_BUNDLE_INDEXED,
        AgentProjectionEventPayloadWire::ArchiveBundleIndexed {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            archive,
            bundle,
        },
    )
}

pub fn agent_archive_bundle_revived_event_request(
    context: AgentProjectionEventContextWire,
    bundle_path: String,
    revived_at: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ARCHIVE_BUNDLE_REVIVED,
        AgentProjectionEventPayloadWire::ArchiveBundleRevived {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            bundle_path,
            revived_at,
        },
    )
}

pub fn agent_archive_bundle_purged_event_request(
    context: AgentProjectionEventContextWire,
    bundle_path: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_ARCHIVE_BUNDLE_PURGED,
        AgentProjectionEventPayloadWire::ArchiveBundlePurged {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            bundle_path,
        },
    )
}

pub fn agent_cleanup_result_recorded_event_request(
    context: AgentProjectionEventContextWire,
    result: JsonValue,
) -> Result<EventAppendRequestWire, ProjectionError> {
    agent_event_request(
        context,
        AGENT_EVENT_CLEANUP_RESULT_RECORDED,
        AgentProjectionEventPayloadWire::CleanupResultRecorded {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            result,
        },
    )
}

pub fn is_agent_indexed_marker_file_name(name: &str) -> bool {
    AGENT_INDEXED_MARKER_FILES.contains(&name)
        || (name.starts_with("prompt_step_") && name.ends_with(".json"))
}

pub fn agent_artifact_dir_for_source_path(path: &Path) -> Option<PathBuf> {
    if path.is_dir() && is_agent_artifact_dir(path) {
        return Some(path.to_path_buf());
    }
    let file_name = path.file_name()?.to_str()?;
    if !is_agent_indexed_marker_file_name(file_name) {
        return None;
    }
    let parent = path.parent()?;
    if is_agent_artifact_dir(parent) {
        Some(parent.to_path_buf())
    } else {
        None
    }
}

pub fn agent_source_change_from_path(
    projects_root: Option<&Path>,
    path: &Path,
    operation: SourceChangeOperationWire,
    reason: Option<String>,
) -> Option<SourceChangeWire> {
    let artifact_dir = agent_artifact_dir_for_source_path(path)?;
    let project_id =
        project_name_for_artifact_dir(projects_root, &artifact_dir);
    Some(SourceChangeWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        identity: SourceIdentityWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            domain: AGENT_INDEXING_DOMAIN.to_string(),
            project_id,
            source_path: artifact_dir.to_string_lossy().into_owned(),
            is_archive: false,
            fingerprint: None,
            last_indexed_event_seq: None,
        },
        operation,
        reason,
    })
}

pub fn agent_projection_event_request_for_artifact_dir(
    mut context: AgentProjectionEventContextWire,
    projects_root: &Path,
    artifact_dir: &Path,
    options: &AgentArtifactScanOptionsWire,
) -> Result<Option<EventAppendRequestWire>, ProjectionError> {
    let Some(record) =
        scan_agent_artifact_dir(projects_root, artifact_dir, options)
    else {
        return Ok(None);
    };
    let record_json = serde_json::to_vec(&record)?;
    let source_path = artifact_dir.to_string_lossy().into_owned();
    context.project_id = record.project_name.clone();
    context.source_path = Some(source_path.clone());
    context.idempotency_key = Some(format!(
        "indexer:agents:artifact_dir:{source_path}:{}",
        hex::encode(Sha256::digest(&record_json))
    ));
    agent_lifecycle_marker_observed_event_request(context, record).map(Some)
}

pub fn backfill_agent_projection_from_scan(
    db: &mut ProjectionDb,
    context: AgentProjectionEventContextWire,
    scan: &AgentArtifactScanWire,
) -> Result<u64, ProjectionError> {
    let mut appended = 0;
    for record in &scan.records {
        let mut record_context = context.clone();
        let record_json = serde_json::to_vec(record)?;
        record_context.project_id = record.project_name.clone();
        record_context.source_path = Some(record.artifact_dir.clone());
        record_context.idempotency_key = Some(format!(
            "indexer:agents:backfill:{}:{}",
            record.artifact_dir,
            hex::encode(Sha256::digest(&record_json))
        ));
        let outcome = db.append_agent_event(
            agent_lifecycle_marker_observed_event_request(
                record_context,
                record.clone(),
            )?,
        )?;
        if !outcome.duplicate {
            appended += 1;
        }
    }
    Ok(appended)
}

pub fn agent_projection_shadow_diff(
    conn: &Connection,
    project_id: &str,
    scan: &AgentArtifactScanWire,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    let mut expected: BTreeMap<String, &AgentArtifactRecordWire> =
        BTreeMap::new();
    for record in scan
        .records
        .iter()
        .filter(|record| record.project_name == project_id)
    {
        expected.insert(record.artifact_dir.clone(), record);
    }

    let mut projected: BTreeMap<String, AgentArtifactRecordWire> =
        BTreeMap::new();
    let mut stmt = conn.prepare(
        r#"
        SELECT artifact_dir, record_json
        FROM agents
        WHERE project_id = ?1 AND artifact_dir <> ''
        ORDER BY artifact_dir ASC
        "#,
    )?;
    let rows = stmt.query_map([project_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut records = Vec::new();
    let mut counts = ShadowDiffCountsWire::default();
    for row in rows {
        let (artifact_dir, json) = row?;
        match serde_json::from_str::<AgentArtifactRecordWire>(&json) {
            Ok(record) => {
                projected.insert(artifact_dir, record);
            }
            Err(error) => {
                counts.corrupt += 1;
                records.push(ShadowDiffRecordWire {
                    schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                    domain: AGENT_INDEXING_DOMAIN.to_string(),
                    category: ShadowDiffCategoryWire::Corrupt,
                    source_path: artifact_dir,
                    handle: None,
                    message: format!(
                        "projected agent record is corrupt: {error}"
                    ),
                });
            }
        }
    }

    for (artifact_dir, expected_record) in &expected {
        match projected.get(artifact_dir) {
            None => {
                counts.missing += 1;
                records.push(ShadowDiffRecordWire {
                    schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                    domain: AGENT_INDEXING_DOMAIN.to_string(),
                    category: ShadowDiffCategoryWire::Missing,
                    source_path: artifact_dir.clone(),
                    handle: Some(agent_id_for_record(expected_record)),
                    message:
                        "agent artifact directory is missing from projection"
                            .to_string(),
                });
            }
            Some(projected_record) if projected_record != *expected_record => {
                counts.stale += 1;
                records.push(ShadowDiffRecordWire {
                    schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                    domain: AGENT_INDEXING_DOMAIN.to_string(),
                    category: ShadowDiffCategoryWire::Stale,
                    source_path: artifact_dir.clone(),
                    handle: Some(agent_id_for_record(expected_record)),
                    message:
                        "projected agent record differs from scanner output"
                            .to_string(),
                });
            }
            Some(_) => {}
        }
    }

    let expected_paths: BTreeSet<&String> = expected.keys().collect();
    for artifact_dir in projected.keys() {
        if !expected_paths.contains(artifact_dir) {
            counts.extra += 1;
            records.push(ShadowDiffRecordWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain: AGENT_INDEXING_DOMAIN.to_string(),
                category: ShadowDiffCategoryWire::Extra,
                source_path: artifact_dir.clone(),
                handle: projected.get(artifact_dir).map(agent_id_for_record),
                message: "projected agent row has no scanner source"
                    .to_string(),
            });
        }
    }

    if scan.stats.json_decode_errors > 0 || scan.stats.os_errors > 0 {
        counts.corrupt += scan.stats.json_decode_errors + scan.stats.os_errors;
        records.push(ShadowDiffRecordWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            domain: AGENT_INDEXING_DOMAIN.to_string(),
            category: ShadowDiffCategoryWire::Corrupt,
            source_path: scan.projects_root.clone(),
            handle: None,
            message: format!(
                "scanner reported {} JSON decode errors and {} OS errors",
                scan.stats.json_decode_errors, scan.stats.os_errors
            ),
        });
    }

    Ok(ShadowDiffReportWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: AGENT_INDEXING_DOMAIN.to_string(),
        records,
        counts,
    })
}

pub fn agent_lifecycle_reconciliation_event_requests(
    conn: &Connection,
    mut context: AgentProjectionEventContextWire,
    project_id: &str,
    scan: &AgentArtifactScanWire,
    checked_at: String,
) -> Result<Vec<EventAppendRequestWire>, ProjectionError> {
    let mut expected: BTreeMap<String, (&AgentArtifactRecordWire, String)> =
        BTreeMap::new();
    for record in scan
        .records
        .iter()
        .filter(|record| record.project_name == project_id)
    {
        let summary = RecordSummary::from_record(record);
        expected.insert(record.artifact_dir.clone(), (record, summary.status));
    }

    let mut stmt = conn.prepare(
        r#"
        SELECT agent_id, artifact_dir, status
        FROM agents
        WHERE project_id = ?1
        ORDER BY agent_id ASC
        "#,
    )?;
    let rows = stmt.query_map([project_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut requests = Vec::new();
    let mut projected_paths = BTreeSet::new();
    for row in rows {
        let (agent_id, artifact_dir, projected_status) = row?;
        projected_paths.insert(artifact_dir.clone());
        match expected.get(&artifact_dir) {
            Some((record, observed_status))
                if observed_status != &projected_status =>
            {
                requests.push(reconciliation_repair_request(
                    &mut context,
                    ReconciliationRepairRequest {
                        checked_at: &checked_at,
                        agent_id: &agent_id,
                        repair_kind: "status_repair",
                        projected_status: Some(projected_status),
                        observed_status: Some(observed_status.clone()),
                        source_path: Some(record.artifact_dir.clone()),
                        message:
                            "projected lifecycle status differs from source markers",
                    },
                )?);
            }
            Some(_) => {}
            None if is_active_lifecycle_status(&projected_status) => {
                requests.push(reconciliation_repair_request(
                    &mut context,
                    ReconciliationRepairRequest {
                        checked_at: &checked_at,
                        agent_id: &agent_id,
                        repair_kind: "stale",
                        projected_status: Some(projected_status),
                        observed_status: Some("stale".to_string()),
                        source_path: Some(artifact_dir),
                        message:
                            "active projected agent no longer has source markers",
                    },
                )?);
            }
            None => {}
        }
    }

    for (artifact_dir, (record, observed_status)) in expected {
        if projected_paths.contains(&artifact_dir) {
            continue;
        }
        let agent_id = agent_id_for_record(record);
        requests.push(reconciliation_repair_request(
            &mut context,
            ReconciliationRepairRequest {
                checked_at: &checked_at,
                agent_id: &agent_id,
                repair_kind: "missing_projection",
                projected_status: None,
                observed_status: Some(observed_status),
                source_path: Some(record.artifact_dir.clone()),
                message:
                    "source markers are missing from the lifecycle projection",
            },
        )?);
    }

    Ok(requests)
}

struct ReconciliationRepairRequest<'a> {
    checked_at: &'a str,
    agent_id: &'a str,
    repair_kind: &'a str,
    projected_status: Option<String>,
    observed_status: Option<String>,
    source_path: Option<String>,
    message: &'a str,
}

fn reconciliation_repair_request(
    context: &mut AgentProjectionEventContextWire,
    request: ReconciliationRepairRequest<'_>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    let repair = AgentLifecycleReconciliationRepairWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        repair_id: format!(
            "{}:{}:{}",
            request.agent_id, request.repair_kind, request.checked_at
        ),
        agent_id: request.agent_id.to_string(),
        repair_kind: request.repair_kind.to_string(),
        projected_status: request.projected_status,
        observed_status: request.observed_status,
        source_path: request.source_path.clone(),
        checked_at: request.checked_at.to_string(),
        message: request.message.to_string(),
    };
    let mut repair_context = context.clone();
    repair_context.source_path = request.source_path;
    repair_context.idempotency_key =
        Some(format!("agents:reconcile:{}", repair.repair_id));
    agent_reconciliation_repair_emitted_event_request(repair_context, repair)
}

fn is_active_lifecycle_status(status: &str) -> bool {
    matches!(
        status,
        "planned" | "queued" | "starting" | "running" | "waiting"
    )
}

pub fn agent_projection_active_page(
    conn: &Connection,
    project_id: &str,
    limit: u32,
    offset: u32,
    include_hidden: bool,
) -> Result<AgentProjectionPageWire, ProjectionError> {
    agent_projection_page(
        conn,
        "project_id = ?1 AND (?2 OR hidden = 0) AND (
            has_done_marker = 0
            OR (has_workflow_state = 1 AND status NOT IN ('completed', 'failed', 'cancelled', 'killed', 'noop'))
        )",
        project_id,
        limit,
        offset,
        include_hidden,
        "timestamp DESC, agent_id ASC",
    )
}

pub fn agent_projection_recent_page(
    conn: &Connection,
    project_id: &str,
    limit: u32,
    offset: u32,
    include_hidden: bool,
) -> Result<AgentProjectionPageWire, ProjectionError> {
    agent_projection_page(
        conn,
        "project_id = ?1 AND (?2 OR hidden = 0) AND has_done_marker = 1",
        project_id,
        limit,
        offset,
        include_hidden,
        "COALESCE(finished_at, 0) DESC, timestamp DESC, agent_id ASC",
    )
}

pub fn agent_projection_search_page(
    conn: &Connection,
    project_id: &str,
    query: &str,
    limit: u32,
    offset: u32,
    include_hidden: bool,
) -> Result<AgentProjectionPageWire, ProjectionError> {
    let fetch_limit = limit.saturating_add(1);
    let mut stmt = conn.prepare(
        r#"
        SELECT a.agent_id, a.project_id, a.project_name, a.project_dir,
               a.project_file, a.workflow_dir_name, a.artifact_dir,
               a.timestamp, a.status, a.agent_type, a.cl_name, a.agent_name,
               a.model, a.llm_provider, a.started_at, a.finished_at,
               a.hidden, a.has_done_marker, a.has_running_marker,
               a.has_waiting_marker, a.has_workflow_state, a.batch_id,
               a.queue_id, a.parent_agent_id, a.workflow_id,
               a.retry_of_agent_id, a.resume_of_agent_id, a.host_id,
               a.pid, a.workspace_claim_id, a.last_heartbeat_at,
               a.last_check_at, a.lifecycle_changed_at, a.stale_reason,
               a.last_seq
        FROM agent_search_fts s
        JOIN agents a
          ON a.project_id = s.project_id
         AND a.agent_id = s.agent_id
        WHERE s.project_id = ?1
          AND agent_search_fts MATCH ?2
          AND (?3 OR a.hidden = 0)
        ORDER BY a.timestamp DESC, a.agent_id ASC
        LIMIT ?4 OFFSET ?5
        "#,
    )?;
    let rows = stmt.query_map(
        params![
            project_id,
            query,
            include_hidden,
            fetch_limit as i64,
            offset as i64
        ],
        agent_row,
    )?;
    let mut entries = collect_rows(rows)?;
    let next_offset = truncate_for_next_offset(&mut entries, limit, offset);
    Ok(AgentProjectionPageWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        entries,
        next_offset,
    })
}

pub fn agent_projection_summary(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<Option<AgentProjectionSummaryWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT agent_id, project_id, project_name, project_dir, project_file,
               workflow_dir_name, artifact_dir, timestamp, status, agent_type,
               cl_name, agent_name, model, llm_provider, started_at,
               finished_at, hidden, has_done_marker, has_running_marker,
               has_waiting_marker, has_workflow_state, batch_id, queue_id,
               parent_agent_id, workflow_id, retry_of_agent_id,
               resume_of_agent_id, host_id, pid, workspace_claim_id,
               last_heartbeat_at, last_check_at, lifecycle_changed_at,
               stale_reason, last_seq
        FROM agents
        WHERE project_id = ?1 AND agent_id = ?2
        "#,
    )?;
    let mut rows = stmt.query_map(params![project_id, agent_id], agent_row)?;
    rows.next().transpose().map_err(ProjectionError::from)
}

pub fn agent_lifecycle_detail(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<AgentLifecycleDetailWire, ProjectionError> {
    let summary = agent_projection_summary(conn, project_id, agent_id)?;
    let attempts = agent_projection_attempts(conn, project_id, agent_id)?;
    let edges = agent_projection_edges(conn, project_id, agent_id)?;
    let events =
        agent_projection_reconciliation_events(conn, project_id, agent_id)?;
    Ok(AgentLifecycleDetailWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        summary,
        attempts,
        edges,
        events,
    })
}

pub fn agent_projection_children(
    conn: &Connection,
    project_id: &str,
    parent_agent_id: &str,
) -> Result<Vec<AgentProjectionSummaryWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT a.agent_id, a.project_id, a.project_name, a.project_dir,
               a.project_file, a.workflow_dir_name, a.artifact_dir,
               a.timestamp, a.status, a.agent_type, a.cl_name, a.agent_name,
               a.model, a.llm_provider, a.started_at, a.finished_at,
               a.hidden, a.has_done_marker, a.has_running_marker,
               a.has_waiting_marker, a.has_workflow_state, a.batch_id,
               a.queue_id, a.parent_agent_id, a.workflow_id,
               a.retry_of_agent_id, a.resume_of_agent_id, a.host_id,
               a.pid, a.workspace_claim_id, a.last_heartbeat_at,
               a.last_check_at, a.lifecycle_changed_at, a.stale_reason,
               a.last_seq
        FROM agent_edges e
        JOIN agents a
          ON a.project_id = e.project_id
         AND a.agent_id = e.child_agent_id
        WHERE e.project_id = ?1 AND e.parent_agent_id = ?2
        ORDER BY a.timestamp ASC, a.agent_id ASC
        "#,
    )?;
    let rows =
        stmt.query_map(params![project_id, parent_agent_id], agent_row)?;
    collect_rows(rows)
}

pub fn agent_projection_attempts(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<Vec<AgentAttemptProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT agent_id, attempt_id, retry_of_timestamp,
               retried_as_timestamp, retry_chain_root_timestamp,
               retry_attempt, batch_id, queue_id, host_id, pid,
               artifact_dir, workspace_claim_id, created_at,
               last_heartbeat_at, last_check_at, terminal
        FROM agent_attempts
        WHERE project_id = ?1 AND agent_id = ?2
        ORDER BY attempt_id ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, agent_id], |row| {
        Ok(AgentAttemptProjectionWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_id: row.get(0)?,
            attempt_id: row.get(1)?,
            retry_of_timestamp: row.get(2)?,
            retried_as_timestamp: row.get(3)?,
            retry_chain_root_timestamp: row.get(4)?,
            retry_attempt: row.get(5)?,
            batch_id: row.get(6)?,
            queue_id: row.get(7)?,
            host_id: row.get(8)?,
            pid: row.get(9)?,
            artifact_dir: row.get(10)?,
            workspace_claim_id: row.get(11)?,
            created_at: row.get(12)?,
            last_heartbeat_at: row.get(13)?,
            last_check_at: row.get(14)?,
            terminal: row.get::<_, i64>(15)? != 0,
        })
    })?;
    collect_rows(rows)
}

pub fn agent_projection_edges(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<Vec<AgentEdgeProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT parent_agent_id, child_agent_id, edge_type, parent_timestamp,
               child_timestamp, workflow_name, workflow_id
        FROM agent_edges
        WHERE project_id = ?1
          AND (parent_agent_id = ?2 OR child_agent_id = ?2)
        ORDER BY edge_type ASC, parent_agent_id ASC, child_agent_id ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, agent_id], |row| {
        Ok(AgentEdgeProjectionWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            parent_agent_id: row.get(0)?,
            child_agent_id: row.get(1)?,
            edge_type: row.get(2)?,
            parent_timestamp: row.get(3)?,
            child_timestamp: row.get(4)?,
            workflow_name: row.get(5)?,
            workflow_id: row.get(6)?,
        })
    })?;
    collect_rows(rows)
}

pub fn agent_projection_reconciliation_events(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<Vec<AgentLifecycleReconciliationRepairWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT payload_json
        FROM agent_lifecycle_events
        WHERE project_id = ?1
          AND agent_id = ?2
          AND event_type = ?3
        ORDER BY seq ASC
        "#,
    )?;
    let rows = stmt.query_map(
        params![
            project_id,
            agent_id,
            AGENT_EVENT_RECONCILIATION_REPAIR_EMITTED
        ],
        |row| row.get::<_, String>(0),
    )?;
    let mut repairs = Vec::new();
    for row in rows {
        let payload: AgentProjectionEventPayloadWire =
            serde_json::from_str(&row?)?;
        if let AgentProjectionEventPayloadWire::ReconciliationRepairEmitted {
            repair,
            ..
        } = payload
        {
            repairs.push(repair);
        }
    }
    Ok(repairs)
}

pub fn agent_projection_artifacts(
    conn: &Connection,
    project_id: &str,
    agent_id: &str,
) -> Result<Vec<AgentArtifactAssociationWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT agent_id, artifact_path, artifact_kind, display_name, role
        FROM agent_artifacts
        WHERE project_id = ?1 AND agent_id = ?2
        ORDER BY artifact_kind ASC, artifact_path ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, agent_id], |row| {
        Ok(AgentArtifactAssociationWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_id: row.get(0)?,
            artifact_path: row.get(1)?,
            artifact_kind: row.get(2)?,
            display_name: row.get(3)?,
            role: row.get(4)?,
        })
    })?;
    let mut artifacts = Vec::new();
    for row in rows {
        artifacts.push(row?);
    }
    Ok(artifacts)
}

pub fn agent_projection_archive_page(
    conn: &Connection,
    project_id: &str,
    limit: u32,
    offset: u32,
) -> Result<AgentArchiveProjectionPageWire, ProjectionError> {
    let fetch_limit = limit.saturating_add(1);
    let mut stmt = conn.prepare(
        r#"
        SELECT archive_json
        FROM agent_archive
        WHERE project_id = ?1 AND purged = 0
        ORDER BY COALESCE(dismissed_at, start_time, raw_suffix) DESC,
                 bundle_path ASC
        LIMIT ?2 OFFSET ?3
        "#,
    )?;
    let rows = stmt.query_map(
        params![project_id, fetch_limit as i64, offset as i64],
        |row| row.get::<_, String>(0),
    )?;
    let mut entries = Vec::new();
    for row in rows {
        entries.push(serde_json::from_str(&row?)?);
    }
    let next_offset = truncate_for_next_offset(&mut entries, limit, offset);
    Ok(AgentArchiveProjectionPageWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        entries,
        next_offset,
    })
}

pub fn agent_projection_dismissed_identities(
    conn: &Connection,
    project_id: &str,
    active_only: bool,
) -> Result<Vec<AgentDismissedIdentityWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT agent_type, cl_name, raw_suffix, agent_id, dismissed_name,
               active, changed_at
        FROM agent_dismissed_identities
        WHERE project_id = ?1 AND (?2 = 0 OR active = 1)
        ORDER BY cl_name ASC, raw_suffix ASC, agent_type ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, active_only], |row| {
        Ok(AgentDismissedIdentityWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_type: row.get(0)?,
            cl_name: row.get(1)?,
            raw_suffix: nonempty(row.get(2)?),
            agent_id: row.get(3)?,
            dismissed_name: row.get(4)?,
            active: row.get::<_, i64>(5)? != 0,
            changed_at: row.get(6)?,
        })
    })?;
    let mut identities = Vec::new();
    for row in rows {
        identities.push(row?);
    }
    Ok(identities)
}

fn apply_agent_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !is_agent_event(&event.event_type) {
        return Ok(());
    }

    let payload: AgentProjectionEventPayloadWire =
        serde_json::from_value(event.payload.clone())?;
    match payload {
        AgentProjectionEventPayloadWire::LifecycleMarkerObserved {
            record,
            ..
        } => {
            upsert_record_tx(conn, event, &record)?;
        }
        AgentProjectionEventPayloadWire::LifecycleTransitioned {
            lifecycle,
            ..
        } => {
            upsert_lifecycle_tx(conn, event, &lifecycle)?;
        }
        AgentProjectionEventPayloadWire::ReconciliationRepairEmitted {
            repair,
            ..
        } => {
            insert_lifecycle_repair_tx(conn, event, &repair)?;
            if repair.repair_kind == "stale" {
                let lifecycle = AgentLifecycleProjectionWire {
                    schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
                    agent_id: repair.agent_id.clone(),
                    lifecycle_status: AgentLifecycleStatusWire::Stale,
                    last_check_at: Some(repair.checked_at.clone()),
                    lifecycle_changed_at: Some(event.created_at.clone()),
                    stale_reason: Some(repair.message.clone()),
                    ..AgentLifecycleProjectionWire::default()
                };
                upsert_lifecycle_tx(conn, event, &lifecycle)?;
            }
        }
        AgentProjectionEventPayloadWire::AttemptCreated { attempt, .. }
        | AgentProjectionEventPayloadWire::AttemptUpdated { attempt, .. } => {
            upsert_attempt_tx(conn, event, &attempt)?
        }
        AgentProjectionEventPayloadWire::EdgeObserved { edge, .. } => {
            upsert_edge_tx(conn, event, &edge)?
        }
        AgentProjectionEventPayloadWire::ArtifactAssociated {
            artifact,
            ..
        } => upsert_artifact_tx(conn, event, &artifact)?,
        AgentProjectionEventPayloadWire::DismissedIdentityChanged {
            identity,
            ..
        } => upsert_dismissed_identity_tx(conn, event, &identity)?,
        AgentProjectionEventPayloadWire::ArchiveBundleIndexed {
            archive,
            bundle,
            ..
        } => upsert_archive_tx(conn, event, &archive, bundle.as_ref())?,
        AgentProjectionEventPayloadWire::ArchiveBundleRevived {
            bundle_path,
            revived_at,
            ..
        } => revive_archive_tx(conn, event, &bundle_path, &revived_at)?,
        AgentProjectionEventPayloadWire::ArchiveBundlePurged {
            bundle_path,
            ..
        } => purge_archive_tx(conn, event, &bundle_path)?,
        AgentProjectionEventPayloadWire::CleanupResultRecorded { .. } => {}
    }
    Ok(())
}

fn upsert_record_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    record: &AgentArtifactRecordWire,
) -> Result<(), ProjectionError> {
    let summary = RecordSummary::from_record(record);
    let agent_id = agent_id_for_record(record);
    let lifecycle = lifecycle_from_record(&agent_id, event, record, &summary);
    let record_json = serde_json::to_string(record)?;
    conn.execute(
        r#"
        INSERT INTO agents (
            project_id, agent_id, project_name, project_dir, project_file,
            workflow_dir_name, artifact_dir, timestamp, status, agent_type,
            cl_name, agent_name, model, llm_provider, started_at, finished_at,
            hidden, has_done_marker, has_running_marker, has_waiting_marker,
            has_workflow_state, batch_id, queue_id, parent_agent_id,
            workflow_id, retry_of_agent_id, resume_of_agent_id, host_id, pid,
            workspace_claim_id, last_heartbeat_at, last_check_at,
            lifecycle_changed_at, stale_reason, record_json, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36
        )
        ON CONFLICT(project_id, agent_id) DO UPDATE SET
            project_name = excluded.project_name,
            project_dir = excluded.project_dir,
            project_file = excluded.project_file,
            workflow_dir_name = excluded.workflow_dir_name,
            artifact_dir = excluded.artifact_dir,
            timestamp = excluded.timestamp,
            status = excluded.status,
            agent_type = excluded.agent_type,
            cl_name = excluded.cl_name,
            agent_name = excluded.agent_name,
            model = excluded.model,
            llm_provider = excluded.llm_provider,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            hidden = excluded.hidden,
            has_done_marker = excluded.has_done_marker,
            has_running_marker = excluded.has_running_marker,
            has_waiting_marker = excluded.has_waiting_marker,
            has_workflow_state = excluded.has_workflow_state,
            batch_id = COALESCE(excluded.batch_id, agents.batch_id),
            queue_id = COALESCE(excluded.queue_id, agents.queue_id),
            parent_agent_id = COALESCE(excluded.parent_agent_id, agents.parent_agent_id),
            workflow_id = COALESCE(excluded.workflow_id, agents.workflow_id),
            retry_of_agent_id = COALESCE(excluded.retry_of_agent_id, agents.retry_of_agent_id),
            resume_of_agent_id = COALESCE(excluded.resume_of_agent_id, agents.resume_of_agent_id),
            host_id = COALESCE(excluded.host_id, agents.host_id),
            pid = COALESCE(excluded.pid, agents.pid),
            workspace_claim_id = COALESCE(excluded.workspace_claim_id, agents.workspace_claim_id),
            last_heartbeat_at = COALESCE(excluded.last_heartbeat_at, agents.last_heartbeat_at),
            last_check_at = excluded.last_check_at,
            lifecycle_changed_at = excluded.lifecycle_changed_at,
            stale_reason = excluded.stale_reason,
            record_json = excluded.record_json,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            agent_id,
            record.project_name,
            record.project_dir,
            record.project_file,
            record.workflow_dir_name,
            record.artifact_dir,
            record.timestamp,
            summary.status,
            summary.agent_type,
            summary.cl_name,
            summary.agent_name,
            summary.model,
            summary.llm_provider,
            summary.started_at,
            summary.finished_at,
            summary.hidden,
            record.has_done_marker,
            record.running.is_some(),
            record.waiting.is_some(),
            record.workflow_state.is_some(),
            lifecycle.batch_id,
            lifecycle.queue_id,
            lifecycle.parent_agent_id,
            lifecycle.workflow_id,
            lifecycle.retry_of_agent_id,
            lifecycle.resume_of_agent_id,
            lifecycle.host_id,
            lifecycle.pid,
            lifecycle.workspace_claim_id,
            lifecycle.last_heartbeat_at,
            lifecycle.last_check_at,
            lifecycle.lifecycle_changed_at,
            lifecycle.stale_reason,
            record_json,
            event.seq,
        ],
    )?;
    conn.execute(
        "DELETE FROM agent_search_fts WHERE agent_id = ?1 AND project_id = ?2",
        params![agent_id, event.project_id],
    )?;
    conn.execute(
        "INSERT INTO agent_search_fts(agent_id, project_id, content) VALUES (?1, ?2, ?3)",
        params![agent_id, event.project_id, search_text(record, &summary)],
    )?;

    let attempt = attempt_from_record(&agent_id, record);
    upsert_attempt_tx(conn, event, &attempt)?;

    conn.execute(
        "DELETE FROM agent_edges WHERE project_id = ?1 AND child_agent_id = ?2",
        params![event.project_id, agent_id],
    )?;
    for edge in edges_from_record(record, &agent_id) {
        upsert_edge_tx(conn, event, &edge)?;
    }
    conn.execute(
        "DELETE FROM agent_artifacts WHERE project_id = ?1 AND agent_id = ?2",
        params![event.project_id, agent_id],
    )?;
    for artifact in artifacts_from_record(record, &agent_id) {
        upsert_artifact_tx(conn, event, &artifact)?;
    }
    insert_lifecycle_event_tx(
        conn,
        event,
        Some(&agent_id),
        Some(summary.status.as_str()),
    )?;
    Ok(())
}

fn upsert_lifecycle_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    lifecycle: &AgentLifecycleProjectionWire,
) -> Result<(), ProjectionError> {
    let timestamp = lifecycle.timestamp.clone().unwrap_or_else(|| {
        lifecycle
            .agent_id
            .rsplit(':')
            .next()
            .unwrap_or("")
            .to_string()
    });
    let project_name = lifecycle
        .project_name
        .clone()
        .unwrap_or_else(|| event.project_id.clone());
    let status = lifecycle.lifecycle_status.as_str();
    let record_json = JsonValue::Null.to_string();
    conn.execute(
        r#"
        INSERT INTO agents (
            project_id, agent_id, project_name, project_dir, project_file,
            workflow_dir_name, artifact_dir, timestamp, status, agent_type,
            cl_name, agent_name, model, llm_provider, started_at, finished_at,
            hidden, has_done_marker, has_running_marker, has_waiting_marker,
            has_workflow_state, batch_id, queue_id, parent_agent_id,
            workflow_id, retry_of_agent_id, resume_of_agent_id, host_id, pid,
            workspace_claim_id, last_heartbeat_at, last_check_at,
            lifecycle_changed_at, stale_reason, record_json, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, NULL, NULL, 0, 0, 0, 0,
            0, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
            ?24, ?25, ?26, ?27, ?28, ?29
        )
        ON CONFLICT(project_id, agent_id) DO UPDATE SET
            status = excluded.status,
            cl_name = COALESCE(excluded.cl_name, agents.cl_name),
            agent_name = COALESCE(excluded.agent_name, agents.agent_name),
            model = COALESCE(excluded.model, agents.model),
            llm_provider = COALESCE(excluded.llm_provider, agents.llm_provider),
            batch_id = COALESCE(excluded.batch_id, agents.batch_id),
            queue_id = COALESCE(excluded.queue_id, agents.queue_id),
            parent_agent_id = COALESCE(excluded.parent_agent_id, agents.parent_agent_id),
            workflow_id = COALESCE(excluded.workflow_id, agents.workflow_id),
            retry_of_agent_id = COALESCE(excluded.retry_of_agent_id, agents.retry_of_agent_id),
            resume_of_agent_id = COALESCE(excluded.resume_of_agent_id, agents.resume_of_agent_id),
            host_id = COALESCE(excluded.host_id, agents.host_id),
            pid = COALESCE(excluded.pid, agents.pid),
            workspace_claim_id = COALESCE(excluded.workspace_claim_id, agents.workspace_claim_id),
            last_heartbeat_at = COALESCE(excluded.last_heartbeat_at, agents.last_heartbeat_at),
            last_check_at = COALESCE(excluded.last_check_at, agents.last_check_at),
            lifecycle_changed_at = COALESCE(excluded.lifecycle_changed_at, ?30),
            stale_reason = excluded.stale_reason,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            lifecycle.agent_id.as_str(),
            project_name,
            lifecycle.project_dir.as_deref().unwrap_or(""),
            lifecycle.project_file.as_deref().unwrap_or(""),
            lifecycle.workflow_dir_name.as_deref().unwrap_or(""),
            lifecycle.artifact_dir.as_deref().unwrap_or(""),
            timestamp,
            status,
            lifecycle.agent_type.as_deref().unwrap_or("agent"),
            lifecycle.cl_name.as_deref(),
            lifecycle.agent_name.as_deref(),
            lifecycle.model.as_deref(),
            lifecycle.llm_provider.as_deref(),
            lifecycle.batch_id.as_deref(),
            lifecycle.queue_id.as_deref(),
            lifecycle.parent_agent_id.as_deref(),
            lifecycle.workflow_id.as_deref(),
            lifecycle.retry_of_agent_id.as_deref(),
            lifecycle.resume_of_agent_id.as_deref(),
            lifecycle.host_id.as_deref(),
            lifecycle.pid,
            lifecycle.workspace_claim_id.as_deref(),
            lifecycle.last_heartbeat_at.as_deref(),
            lifecycle.last_check_at.as_deref(),
            lifecycle.lifecycle_changed_at.as_deref(),
            lifecycle.stale_reason.as_deref(),
            record_json,
            event.seq,
            event.created_at,
        ],
    )?;
    insert_lifecycle_event_tx(
        conn,
        event,
        Some(&lifecycle.agent_id),
        Some(status),
    )?;
    Ok(())
}

fn insert_lifecycle_repair_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    repair: &AgentLifecycleReconciliationRepairWire,
) -> Result<(), ProjectionError> {
    insert_lifecycle_event_tx(
        conn,
        event,
        Some(&repair.agent_id),
        repair.observed_status.as_deref(),
    )
}

fn insert_lifecycle_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    agent_id: Option<&str>,
    lifecycle_status: Option<&str>,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO agent_lifecycle_events (
            seq, project_id, event_type, agent_id, lifecycle_status,
            payload_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
        params![
            event.seq,
            event.project_id,
            event.event_type,
            agent_id,
            lifecycle_status,
            serde_json::to_string(&event.payload)?,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn upsert_attempt_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    attempt: &AgentAttemptProjectionWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO agent_attempts (
            project_id, agent_id, attempt_id, retry_of_timestamp,
            retried_as_timestamp, retry_chain_root_timestamp, retry_attempt,
            batch_id, queue_id, host_id, pid, artifact_dir,
            workspace_claim_id, created_at, last_heartbeat_at,
            last_check_at, terminal, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18
        )
        ON CONFLICT(project_id, agent_id, attempt_id) DO UPDATE SET
            retry_of_timestamp = excluded.retry_of_timestamp,
            retried_as_timestamp = excluded.retried_as_timestamp,
            retry_chain_root_timestamp = excluded.retry_chain_root_timestamp,
            retry_attempt = excluded.retry_attempt,
            batch_id = COALESCE(excluded.batch_id, agent_attempts.batch_id),
            queue_id = COALESCE(excluded.queue_id, agent_attempts.queue_id),
            host_id = COALESCE(excluded.host_id, agent_attempts.host_id),
            pid = COALESCE(excluded.pid, agent_attempts.pid),
            artifact_dir = COALESCE(excluded.artifact_dir, agent_attempts.artifact_dir),
            workspace_claim_id = COALESCE(excluded.workspace_claim_id, agent_attempts.workspace_claim_id),
            created_at = COALESCE(excluded.created_at, agent_attempts.created_at),
            last_heartbeat_at = COALESCE(excluded.last_heartbeat_at, agent_attempts.last_heartbeat_at),
            last_check_at = COALESCE(excluded.last_check_at, agent_attempts.last_check_at),
            terminal = excluded.terminal,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            attempt.agent_id,
            attempt.attempt_id,
            attempt.retry_of_timestamp,
            attempt.retried_as_timestamp,
            attempt.retry_chain_root_timestamp,
            attempt.retry_attempt,
            attempt.batch_id,
            attempt.queue_id,
            attempt.host_id,
            attempt.pid,
            attempt.artifact_dir,
            attempt.workspace_claim_id,
            attempt.created_at,
            attempt.last_heartbeat_at,
            attempt.last_check_at,
            attempt.terminal,
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_edge_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    edge: &AgentEdgeProjectionWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO agent_edges (
            project_id, parent_agent_id, child_agent_id, edge_type,
            parent_timestamp, child_timestamp, workflow_name, workflow_id,
            last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(project_id, parent_agent_id, child_agent_id, edge_type)
        DO UPDATE SET
            parent_timestamp = excluded.parent_timestamp,
            child_timestamp = excluded.child_timestamp,
            workflow_name = excluded.workflow_name,
            workflow_id = COALESCE(excluded.workflow_id, agent_edges.workflow_id),
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            edge.parent_agent_id,
            edge.child_agent_id,
            edge.edge_type,
            edge.parent_timestamp,
            edge.child_timestamp,
            edge.workflow_name,
            edge.workflow_id,
            event.seq,
        ],
    )?;
    if let Some(workflow_id) = edge.workflow_id.as_ref() {
        conn.execute(
            r#"
            INSERT INTO workflow_agent_edges (
                project_id, workflow_id, agent_id, edge_type,
                parent_agent_id, parent_workflow_id, last_seq
            ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6)
            ON CONFLICT(project_id, workflow_id, agent_id, edge_type)
            DO UPDATE SET
                parent_agent_id = excluded.parent_agent_id,
                last_seq = excluded.last_seq
            "#,
            params![
                event.project_id,
                workflow_id,
                edge.child_agent_id,
                edge.edge_type,
                edge.parent_agent_id,
                event.seq,
            ],
        )?;
    }
    Ok(())
}

fn upsert_artifact_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    artifact: &AgentArtifactAssociationWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO agent_artifacts (
            project_id, agent_id, artifact_path, artifact_kind, display_name,
            role, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(project_id, agent_id, artifact_path, artifact_kind)
        DO UPDATE SET
            display_name = excluded.display_name,
            role = excluded.role,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            artifact.agent_id,
            artifact.artifact_path,
            artifact.artifact_kind,
            artifact.display_name,
            artifact.role,
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_dismissed_identity_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    identity: &AgentDismissedIdentityWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO agent_dismissed_identities (
            project_id, agent_type, cl_name, raw_suffix, agent_id,
            dismissed_name, active, changed_at, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(project_id, agent_type, cl_name, raw_suffix)
        DO UPDATE SET
            agent_id = excluded.agent_id,
            dismissed_name = excluded.dismissed_name,
            active = excluded.active,
            changed_at = excluded.changed_at,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            identity.agent_type,
            identity.cl_name,
            identity.raw_suffix.as_deref().unwrap_or(""),
            identity.agent_id,
            identity.dismissed_name,
            identity.active,
            identity.changed_at.as_ref().unwrap_or(&event.created_at),
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_archive_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    archive: &AgentArchiveSummaryWire,
    bundle: Option<&JsonValue>,
) -> Result<(), ProjectionError> {
    let archive_json = serde_json::to_string(archive)?;
    let bundle_json =
        serde_json::to_string(&bundle.unwrap_or(&JsonValue::Null))?;
    conn.execute(
        r#"
        INSERT INTO agent_archive (
            project_id, bundle_path, agent_id, raw_suffix, cl_name,
            agent_name, status, start_time, dismissed_at, revived_at,
            project_name, model, runtime, llm_provider, step_index,
            step_name, step_type, retry_attempt, is_workflow_child,
            archive_json, bundle_json, purged, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, 0, ?22
        )
        ON CONFLICT(project_id, bundle_path) DO UPDATE SET
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
            archive_json = excluded.archive_json,
            bundle_json = excluded.bundle_json,
            purged = 0,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            archive.bundle_path,
            archive.agent_id,
            archive.raw_suffix,
            archive.cl_name,
            archive.agent_name,
            archive.status,
            archive.start_time,
            archive.dismissed_at,
            archive.revived_at,
            archive.project_name,
            archive.model,
            archive.runtime,
            archive.llm_provider,
            archive.step_index,
            archive.step_name,
            archive.step_type,
            archive.retry_attempt,
            archive.is_workflow_child,
            archive_json,
            bundle_json,
            event.seq,
        ],
    )?;
    Ok(())
}

fn revive_archive_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    bundle_path: &str,
    revived_at: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        UPDATE agent_archive
        SET revived_at = ?3,
            archive_json = json_set(archive_json, '$.revived_at', ?3),
            last_seq = ?4
        WHERE project_id = ?1 AND bundle_path = ?2
        "#,
        params![event.project_id, bundle_path, revived_at, event.seq],
    )?;
    Ok(())
}

fn purge_archive_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    bundle_path: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        UPDATE agent_archive
        SET purged = 1, last_seq = ?3
        WHERE project_id = ?1 AND bundle_path = ?2
        "#,
        params![event.project_id, bundle_path, event.seq],
    )?;
    Ok(())
}

fn agent_projection_page(
    conn: &Connection,
    where_sql: &str,
    project_id: &str,
    limit: u32,
    offset: u32,
    include_hidden: bool,
    order_sql: &str,
) -> Result<AgentProjectionPageWire, ProjectionError> {
    let fetch_limit = limit.saturating_add(1);
    let sql = format!(
        "SELECT agent_id, project_id, project_name, project_dir, project_file,
                workflow_dir_name, artifact_dir, timestamp, status, agent_type,
                cl_name, agent_name, model, llm_provider, started_at,
                finished_at, hidden, has_done_marker, has_running_marker,
                has_waiting_marker, has_workflow_state, batch_id, queue_id,
                parent_agent_id, workflow_id, retry_of_agent_id,
                resume_of_agent_id, host_id, pid, workspace_claim_id,
                last_heartbeat_at, last_check_at, lifecycle_changed_at,
                stale_reason, last_seq
         FROM agents
         WHERE {where_sql}
         ORDER BY {order_sql}
         LIMIT ?3 OFFSET ?4"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![
            project_id,
            include_hidden,
            fetch_limit as i64,
            offset as i64
        ],
        agent_row,
    )?;
    let mut entries = collect_rows(rows)?;
    let next_offset = truncate_for_next_offset(&mut entries, limit, offset);
    Ok(AgentProjectionPageWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        entries,
        next_offset,
    })
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<
        '_,
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    >,
) -> Result<Vec<T>, ProjectionError> {
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
    }
    Ok(values)
}

fn agent_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AgentProjectionSummaryWire> {
    Ok(AgentProjectionSummaryWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        agent_id: row.get(0)?,
        project_id: row.get(1)?,
        project_name: row.get(2)?,
        project_dir: row.get(3)?,
        project_file: row.get(4)?,
        workflow_dir_name: row.get(5)?,
        artifact_dir: row.get(6)?,
        timestamp: row.get(7)?,
        status: row.get(8)?,
        agent_type: row.get(9)?,
        cl_name: row.get(10)?,
        agent_name: row.get(11)?,
        model: row.get(12)?,
        llm_provider: row.get(13)?,
        started_at: row.get(14)?,
        finished_at: row.get(15)?,
        hidden: row.get::<_, i64>(16)? != 0,
        has_done_marker: row.get::<_, i64>(17)? != 0,
        has_running_marker: row.get::<_, i64>(18)? != 0,
        has_waiting_marker: row.get::<_, i64>(19)? != 0,
        has_workflow_state: row.get::<_, i64>(20)? != 0,
        batch_id: row.get(21)?,
        queue_id: row.get(22)?,
        parent_agent_id: row.get(23)?,
        workflow_id: row.get(24)?,
        retry_of_agent_id: row.get(25)?,
        resume_of_agent_id: row.get(26)?,
        host_id: row.get(27)?,
        pid: row.get(28)?,
        workspace_claim_id: row.get(29)?,
        last_heartbeat_at: row.get(30)?,
        last_check_at: row.get(31)?,
        lifecycle_changed_at: row.get(32)?,
        stale_reason: row.get(33)?,
        last_seq: row.get(34)?,
    })
}

fn agent_event_request(
    context: AgentProjectionEventContextWire,
    event_type: &str,
    payload: AgentProjectionEventPayloadWire,
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

fn agent_id_for_record(record: &AgentArtifactRecordWire) -> String {
    agent_id_from_timestamp(&record.project_name, &record.timestamp)
}

fn agent_id_from_timestamp(project_name: &str, timestamp: &str) -> String {
    format!("agent:{project_name}:{timestamp}")
}

fn attempt_from_record(
    agent_id: &str,
    record: &AgentArtifactRecordWire,
) -> AgentAttemptProjectionWire {
    let meta = record.agent_meta.as_ref();
    let done = record.done.as_ref();
    AgentAttemptProjectionWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        agent_id: agent_id.to_string(),
        attempt_id: record.timestamp.clone(),
        retry_of_timestamp: meta.and_then(|m| m.retry_of_timestamp.clone()),
        retried_as_timestamp: meta
            .and_then(|m| m.retried_as_timestamp.clone())
            .or_else(|| done.and_then(|d| d.retried_as_timestamp.clone())),
        retry_chain_root_timestamp: meta
            .and_then(|m| m.retry_chain_root_timestamp.clone())
            .or_else(|| {
                done.and_then(|d| d.retry_chain_root_timestamp.clone())
            }),
        retry_attempt: meta.and_then(|m| m.retry_attempt),
        host_id: None,
        pid: meta
            .and_then(|m| m.pid)
            .or_else(|| record.running.as_ref().and_then(|r| r.pid)),
        artifact_dir: Some(record.artifact_dir.clone()),
        workspace_claim_id: workspace_claim_id_from_record(record),
        created_at: Some(record.timestamp.clone()),
        last_heartbeat_at: meta.and_then(|m| m.run_started_at.clone()),
        last_check_at: None,
        batch_id: None,
        queue_id: None,
        terminal: record.has_done_marker
            || meta.map(|m| m.retry_terminal).unwrap_or(false),
    }
}

fn lifecycle_from_record(
    agent_id: &str,
    event: &EventEnvelopeWire,
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
) -> AgentLifecycleProjectionWire {
    let meta = record.agent_meta.as_ref();
    let running = record.running.as_ref();
    AgentLifecycleProjectionWire {
        schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
        agent_id: agent_id.to_string(),
        lifecycle_status: status_wire_from_str(&summary.status),
        project_name: Some(record.project_name.clone()),
        project_dir: Some(record.project_dir.clone()),
        project_file: Some(record.project_file.clone()),
        workflow_dir_name: Some(record.workflow_dir_name.clone()),
        artifact_dir: Some(record.artifact_dir.clone()),
        timestamp: Some(record.timestamp.clone()),
        agent_type: Some(summary.agent_type.clone()),
        agent_name: summary.agent_name.clone(),
        cl_name: summary.cl_name.clone(),
        model: summary.model.clone(),
        llm_provider: summary.llm_provider.clone(),
        parent_agent_id: meta
            .and_then(|m| m.parent_agent_timestamp.clone())
            .or_else(|| meta.and_then(|m| m.parent_timestamp.clone()))
            .map(|timestamp| {
                agent_id_from_timestamp(&record.project_name, &timestamp)
            }),
        workflow_id: meta.and_then(|m| m.workflow_name.clone()).or_else(|| {
            record
                .workflow_state
                .as_ref()
                .map(|state| state.workflow_name.clone())
        }),
        retry_of_agent_id: meta.and_then(|m| m.retry_of_timestamp.clone()).map(
            |timestamp| {
                agent_id_from_timestamp(&record.project_name, &timestamp)
            },
        ),
        host_id: None,
        pid: meta
            .and_then(|m| m.pid)
            .or_else(|| running.and_then(|r| r.pid)),
        workspace_claim_id: workspace_claim_id_from_record(record),
        last_heartbeat_at: meta.and_then(|m| m.run_started_at.clone()),
        last_check_at: Some(event.created_at.clone()),
        lifecycle_changed_at: Some(event.created_at.clone()),
        ..AgentLifecycleProjectionWire::default()
    }
}

fn status_wire_from_str(status: &str) -> AgentLifecycleStatusWire {
    match status {
        "planned" => AgentLifecycleStatusWire::Planned,
        "queued" => AgentLifecycleStatusWire::Queued,
        "starting" => AgentLifecycleStatusWire::Starting,
        "running" => AgentLifecycleStatusWire::Running,
        "waiting" | "paused" => AgentLifecycleStatusWire::Waiting,
        "failed" => AgentLifecycleStatusWire::Failed,
        "killed" | "cancelled" => AgentLifecycleStatusWire::Killed,
        "stale" => AgentLifecycleStatusWire::Stale,
        _ => AgentLifecycleStatusWire::Completed,
    }
}

fn workspace_claim_id_from_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    let meta = record.agent_meta.as_ref();
    let done = record.done.as_ref();
    let workspace_num = meta
        .and_then(|m| m.workspace_num)
        .or_else(|| done.and_then(|d| d.workspace_num));
    let workspace_dir = meta
        .and_then(|m| m.workspace_dir.clone())
        .or_else(|| done.and_then(|d| d.workspace_dir.clone()));
    workspace_num
        .map(|num| format!("{}:{num}", record.project_name))
        .or(workspace_dir)
}

fn edges_from_record(
    record: &AgentArtifactRecordWire,
    agent_id: &str,
) -> Vec<AgentEdgeProjectionWire> {
    let mut edges = Vec::new();
    if let Some(meta) = record.agent_meta.as_ref() {
        if let Some(parent_timestamp) = meta.parent_timestamp.as_ref() {
            edges.push(AgentEdgeProjectionWire {
                schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
                parent_agent_id: agent_id_from_timestamp(
                    &record.project_name,
                    parent_timestamp,
                ),
                child_agent_id: agent_id.to_string(),
                edge_type: "parent_child".to_string(),
                parent_timestamp: Some(parent_timestamp.clone()),
                child_timestamp: Some(record.timestamp.clone()),
                workflow_name: meta.workflow_name.clone(),
                workflow_id: meta.workflow_name.clone(),
            });
        }
        if let Some(parent_timestamp) = meta.parent_agent_timestamp.as_ref() {
            edges.push(AgentEdgeProjectionWire {
                schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
                parent_agent_id: agent_id_from_timestamp(
                    &record.project_name,
                    parent_timestamp,
                ),
                child_agent_id: agent_id.to_string(),
                edge_type: "workflow_parent_child".to_string(),
                parent_timestamp: Some(parent_timestamp.clone()),
                child_timestamp: Some(record.timestamp.clone()),
                workflow_name: meta.workflow_name.clone(),
                workflow_id: meta.workflow_name.clone(),
            });
        }
    }
    edges
}

fn artifacts_from_record(
    record: &AgentArtifactRecordWire,
    agent_id: &str,
) -> Vec<AgentArtifactAssociationWire> {
    let mut artifacts = Vec::new();
    if let Some(done) = record.done.as_ref() {
        push_optional_artifact(
            &mut artifacts,
            agent_id,
            &done.plan_path,
            "plan",
            "done_marker",
        );
        push_optional_artifact(
            &mut artifacts,
            agent_id,
            &done.diff_path,
            "diff",
            "done_marker",
        );
        push_optional_artifact(
            &mut artifacts,
            agent_id,
            &done.response_path,
            "response",
            "done_marker",
        );
        push_optional_artifact(
            &mut artifacts,
            agent_id,
            &done.output_path,
            "output",
            "done_marker",
        );
        for path in &done.markdown_pdf_paths {
            artifacts.push(AgentArtifactAssociationWire {
                schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
                agent_id: agent_id.to_string(),
                artifact_path: path.clone(),
                artifact_kind: "markdown_pdf".to_string(),
                display_name: path.rsplit('/').next().map(str::to_string),
                role: Some("done_marker".to_string()),
            });
        }
        for path in &done.image_paths {
            artifacts.push(AgentArtifactAssociationWire {
                schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
                agent_id: agent_id.to_string(),
                artifact_path: path.clone(),
                artifact_kind: "image".to_string(),
                display_name: path.rsplit('/').next().map(str::to_string),
                role: Some("done_marker".to_string()),
            });
        }
    }
    if let Some(plan_path) = record.plan_path.as_ref() {
        push_optional_artifact(
            &mut artifacts,
            agent_id,
            &plan_path.plan_path,
            "plan",
            "plan_path_marker",
        );
    }
    artifacts
}

fn push_optional_artifact(
    artifacts: &mut Vec<AgentArtifactAssociationWire>,
    agent_id: &str,
    path: &Option<String>,
    kind: &str,
    role: &str,
) {
    if let Some(path) = path.as_ref() {
        artifacts.push(AgentArtifactAssociationWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_id: agent_id.to_string(),
            artifact_path: path.clone(),
            artifact_kind: kind.to_string(),
            display_name: path.rsplit('/').next().map(str::to_string),
            role: Some(role.to_string()),
        });
    }
}

#[derive(Default)]
struct RecordSummary {
    status: String,
    agent_type: String,
    cl_name: Option<String>,
    agent_name: Option<String>,
    model: Option<String>,
    llm_provider: Option<String>,
    started_at: Option<String>,
    finished_at: Option<f64>,
    hidden: bool,
}

impl RecordSummary {
    fn from_record(record: &AgentArtifactRecordWire) -> Self {
        let meta = record.agent_meta.as_ref();
        let done = record.done.as_ref();
        let running = record.running.as_ref();
        let waiting = record.waiting.as_ref();
        let workflow_state = record.workflow_state.as_ref();
        let first_step = record.prompt_steps.first();

        let workflow_status = workflow_state.map(|w| w.status.as_str());
        let status = if waiting.is_some() {
            "waiting"
        } else if let Some(workflow_status) = workflow_status {
            workflow_status
        } else if record.has_done_marker {
            match done.and_then(|d| d.outcome.as_deref()) {
                Some("failed") => "failed",
                Some("killed") | Some("cancelled") => "killed",
                Some("noop") => "completed",
                _ => "completed",
            }
        } else if meta
            .and_then(|m| {
                m.run_started_at.as_ref().or(m.wait_completed_at.as_ref())
            })
            .is_some()
            || running.is_some()
        {
            "running"
        } else {
            "starting"
        };

        Self {
            status: status.to_string(),
            agent_type: if workflow_state.is_some() {
                "workflow".to_string()
            } else {
                "agent".to_string()
            },
            cl_name: done
                .and_then(|d| d.cl_name.clone())
                .or_else(|| running.and_then(|r| r.cl_name.clone()))
                .or_else(|| workflow_state.and_then(|w| w.cl_name.clone())),
            agent_name: meta
                .and_then(|m| m.name.clone())
                .or_else(|| done.and_then(|d| d.name.clone()))
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            model: meta
                .and_then(|m| m.model.clone())
                .or_else(|| done.and_then(|d| d.model.clone()))
                .or_else(|| running.and_then(|r| r.model.clone()))
                .or_else(|| first_step.and_then(|s| s.model.clone())),
            llm_provider: meta
                .and_then(|m| m.llm_provider.clone())
                .or_else(|| done.and_then(|d| d.llm_provider.clone()))
                .or_else(|| running.and_then(|r| r.llm_provider.clone()))
                .or_else(|| first_step.and_then(|s| s.llm_provider.clone())),
            started_at: meta
                .and_then(|m| m.run_started_at.clone())
                .or_else(|| workflow_state.and_then(|w| w.start_time.clone())),
            finished_at: done.and_then(|d| d.finished_at),
            hidden: meta.map(|m| m.hidden).unwrap_or(false)
                || done.map(|d| d.hidden).unwrap_or(false)
                || workflow_state.map(|w| w.is_anonymous).unwrap_or(false),
        }
    }
}

fn search_text(
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
) -> String {
    let mut parts = vec![
        record.project_name.clone(),
        record.workflow_dir_name.clone(),
        record.timestamp.clone(),
        summary.status.clone(),
        summary.agent_type.clone(),
    ];
    parts.extend(summary.cl_name.clone());
    parts.extend(summary.agent_name.clone());
    parts.extend(summary.model.clone());
    parts.extend(summary.llm_provider.clone());
    parts.extend(record.raw_prompt_snippet.clone());
    parts.join("\n")
}

fn truncate_for_next_offset<T>(
    entries: &mut Vec<T>,
    limit: u32,
    offset: u32,
) -> Option<u32> {
    if limit == 0 {
        entries.clear();
        return Some(offset);
    }
    if entries.len() > limit as usize {
        entries.truncate(limit as usize);
        Some(offset + limit)
    } else {
        None
    }
}

fn nonempty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn is_agent_artifact_dir(path: &Path) -> bool {
    let parts: Vec<String> = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    parts.windows(3).any(|window| {
        window[0] == "artifacts"
            && !window[1].is_empty()
            && !window[2].is_empty()
    })
}

fn project_name_for_artifact_dir(
    projects_root: Option<&Path>,
    artifact_dir: &Path,
) -> Option<String> {
    if let Some(projects_root) = projects_root {
        if let Ok(relative) = artifact_dir.strip_prefix(projects_root) {
            let parts: Vec<String> = relative
                .components()
                .map(|component| {
                    component.as_os_str().to_string_lossy().into_owned()
                })
                .collect();
            if parts.len() >= 4 && parts[1] == "artifacts" {
                return Some(parts[0].clone());
            }
        }
    }

    let parts: Vec<String> = artifact_dir
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    parts
        .windows(4)
        .find(|window| window[1] == "artifacts")
        .map(|window| window[0].clone())
}

fn is_agent_event(event_type: &str) -> bool {
    matches!(
        event_type,
        AGENT_EVENT_MARKER_OBSERVED
            | AGENT_EVENT_LIFECYCLE_TRANSITIONED
            | AGENT_EVENT_RECONCILIATION_REPAIR_EMITTED
            | AGENT_EVENT_ATTEMPT_CREATED
            | AGENT_EVENT_ATTEMPT_UPDATED
            | AGENT_EVENT_EDGE_OBSERVED
            | AGENT_EVENT_ARTIFACT_ASSOCIATED
            | AGENT_EVENT_DISMISSED_IDENTITY_CHANGED
            | AGENT_EVENT_ARCHIVE_BUNDLE_INDEXED
            | AGENT_EVENT_ARCHIVE_BUNDLE_REVIVED
            | AGENT_EVENT_ARCHIVE_BUNDLE_PURGED
            | AGENT_EVENT_CLEANUP_RESULT_RECORDED
    )
}

fn agent_projection_schema_version() -> u32 {
    AGENT_PROJECTION_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::agent_scan::{
        scan_agent_artifacts, AgentArtifactScanOptionsWire,
    };

    use super::*;

    const PROJECT_ID: &str = "project-a";

    fn context() -> AgentProjectionEventContextWire {
        AgentProjectionEventContextWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "agent-projection-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: PROJECT_ID.to_string(),
            idempotency_key: None,
            causality: vec![],
            source_path: Some("artifacts".to_string()),
            source_revision: None,
        }
    }

    fn write_json(path: &std::path::Path, value: &JsonValue) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(value).unwrap())
            .unwrap();
    }

    fn fixture_root(tmp: &tempfile::TempDir) -> std::path::PathBuf {
        let root = tmp.path().join("projects");
        let parent = root
            .join("myproj")
            .join("artifacts")
            .join("ace-run")
            .join("20260513120000");
        write_json(
            &parent.join("agent_meta.json"),
            &json!({
                "name": "parent",
                "run_started_at": "2026-05-13T16:00:00Z"
            }),
        );
        write_json(
            &parent.join("done.json"),
            &json!({
                "outcome": "completed",
                "finished_at": 1800000000.0,
                "cl_name": "feature_alpha",
                "name": "parent",
                "plan_path": "/tmp/plan.md",
                "diff_path": "/tmp/diff.patch",
                "response_path": "/tmp/response.md"
            }),
        );

        let child = root
            .join("myproj")
            .join("artifacts")
            .join("ace-run")
            .join("20260513121000");
        write_json(
            &child.join("agent_meta.json"),
            &json!({
                "name": "child",
                "parent_timestamp": "20260513120000",
                "workflow_name": "wf_alpha",
                "run_started_at": "2026-05-13T16:10:00Z"
            }),
        );

        root
    }

    #[test]
    fn marker_observed_projects_agent_rows_and_artifacts() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = fixture_root(&tmp);
        let snapshot = scan_agent_artifacts(
            &root,
            AgentArtifactScanOptionsWire::default(),
        );
        let mut db = ProjectionDb::open_in_memory().unwrap();

        for record in snapshot.records.clone() {
            db.append_agent_event(
                agent_lifecycle_marker_observed_event_request(
                    context(),
                    record,
                )
                .unwrap(),
            )
            .unwrap();
        }

        let recent = agent_projection_recent_page(
            db.connection(),
            PROJECT_ID,
            10,
            0,
            false,
        )
        .unwrap();
        assert_eq!(recent.entries.len(), 1);
        assert_eq!(recent.entries[0].agent_name.as_deref(), Some("parent"));
        assert_eq!(recent.entries[0].cl_name.as_deref(), Some("feature_alpha"));
        assert!(recent.entries[0].has_done_marker);

        let artifacts = agent_projection_artifacts(
            db.connection(),
            PROJECT_ID,
            &recent.entries[0].agent_id,
        )
        .unwrap();
        let kinds: Vec<_> = artifacts
            .iter()
            .map(|artifact| artifact.artifact_kind.as_str())
            .collect();
        assert!(kinds.contains(&"plan"));
        assert!(kinds.contains(&"diff"));
        assert!(kinds.contains(&"response"));
    }

    #[test]
    fn parent_child_edges_are_stable_across_replay() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = fixture_root(&tmp);
        let snapshot = scan_agent_artifacts(
            &root,
            AgentArtifactScanOptionsWire::default(),
        );
        let mut live = ProjectionDb::open_in_memory().unwrap();

        for record in snapshot.records {
            live.append_agent_event(
                agent_lifecycle_marker_observed_event_request(
                    context(),
                    record,
                )
                .unwrap(),
            )
            .unwrap();
        }

        let parent_id = agent_id_from_timestamp("myproj", "20260513120000");
        let live_children = agent_projection_children(
            live.connection(),
            PROJECT_ID,
            &parent_id,
        )
        .unwrap();
        assert_eq!(live_children.len(), 1);
        assert_eq!(live_children[0].agent_name.as_deref(), Some("child"));

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
        let mut applier = AgentProjectionApplier;
        replayed.replay_events(0, &mut [&mut applier]).unwrap();

        assert_eq!(
            live_children,
            agent_projection_children(
                replayed.connection(),
                PROJECT_ID,
                &parent_id
            )
            .unwrap()
        );
    }

    #[test]
    fn dismissed_and_archived_identities_remain_queryable() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let identity = AgentDismissedIdentityWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_type: "run".to_string(),
            cl_name: "feature_alpha".to_string(),
            raw_suffix: Some("20260513120000".to_string()),
            agent_id: Some(agent_id_from_timestamp("myproj", "20260513120000")),
            dismissed_name: Some("260513.parent".to_string()),
            active: true,
            changed_at: Some("2026-05-13T21:00:00Z".to_string()),
        };
        db.append_agent_event(
            agent_dismissed_identity_changed_event_request(
                context(),
                identity.clone(),
            )
            .unwrap(),
        )
        .unwrap();

        let archive = AgentArchiveSummaryWire {
            agent_id: "agent-a".to_string(),
            raw_suffix: "20260513120000".to_string(),
            bundle_path: "/tmp/archive/bundle.json".to_string(),
            cl_name: "feature_alpha".to_string(),
            agent_name: Some("parent".to_string()),
            status: "DONE".to_string(),
            start_time: Some("2026-05-13T16:00:00Z".to_string()),
            dismissed_at: Some("2026-05-13T21:00:00Z".to_string()),
            revived_at: None,
            project_name: Some("myproj".to_string()),
            model: Some("codex".to_string()),
            runtime: Some("codex".to_string()),
            llm_provider: Some("openai".to_string()),
            step_index: None,
            step_name: None,
            step_type: None,
            retry_attempt: 0,
            is_workflow_child: false,
        };
        db.append_agent_event(
            agent_archive_bundle_indexed_event_request(
                context(),
                archive.clone(),
                Some(json!({"raw_suffix": archive.raw_suffix})),
            )
            .unwrap(),
        )
        .unwrap();
        db.append_agent_event(
            agent_archive_bundle_revived_event_request(
                context(),
                archive.bundle_path.clone(),
                "2026-05-13T21:05:00Z".to_string(),
            )
            .unwrap(),
        )
        .unwrap();

        let identities = agent_projection_dismissed_identities(
            db.connection(),
            PROJECT_ID,
            true,
        )
        .unwrap();
        assert_eq!(identities, vec![identity]);

        let archive_page =
            agent_projection_archive_page(db.connection(), PROJECT_ID, 10, 0)
                .unwrap();
        assert_eq!(archive_page.entries.len(), 1);
        assert_eq!(
            archive_page.entries[0].revived_at.as_deref(),
            Some("2026-05-13T21:05:00Z")
        );
    }

    #[test]
    fn marker_path_resolves_to_one_artifact_directory_change() {
        let path = std::path::Path::new(
            "/tmp/.sase/projects/myproj/artifacts/ace-run/20260513120000/done.json",
        );
        let change = agent_source_change_from_path(
            None,
            path,
            SourceChangeOperationWire::Upsert,
            Some("test".to_string()),
        )
        .unwrap();

        assert_eq!(change.identity.domain, "agents");
        assert_eq!(change.identity.project_id.as_deref(), Some("myproj"));
        assert!(change
            .identity
            .source_path
            .ends_with("myproj/artifacts/ace-run/20260513120000"));

        let prompt_step = std::path::Path::new(
            "/tmp/.sase/projects/myproj/artifacts/ace-run/20260513120000/prompt_step_01.json",
        );
        assert!(agent_artifact_dir_for_source_path(prompt_step).is_some());
        assert!(agent_artifact_dir_for_source_path(std::path::Path::new(
            "/tmp/.sase/projects/myproj/artifacts/ace-run/20260513120000/notes.txt",
        ))
        .is_none());
    }

    #[test]
    fn backfill_and_shadow_diff_converge_with_scanner_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = fixture_root(&tmp);
        let snapshot = scan_agent_artifacts(
            &root,
            AgentArtifactScanOptionsWire::default(),
        );
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let missing =
            agent_projection_shadow_diff(db.connection(), "myproj", &snapshot)
                .unwrap();
        assert_eq!(missing.counts.missing, snapshot.records.len() as u64);

        let appended =
            backfill_agent_projection_from_scan(&mut db, context(), &snapshot)
                .unwrap();
        assert_eq!(appended, snapshot.records.len() as u64);

        let clean =
            agent_projection_shadow_diff(db.connection(), "myproj", &snapshot)
                .unwrap();
        assert_eq!(clean.counts, ShadowDiffCountsWire::default());
        assert!(clean.records.is_empty());
    }

    #[test]
    fn lifecycle_transition_projects_scheduler_owned_fields() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let lifecycle = AgentLifecycleProjectionWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_id: "agent:myproj:queued-1".to_string(),
            lifecycle_status: AgentLifecycleStatusWire::Queued,
            project_name: Some("myproj".to_string()),
            batch_id: Some("batch-7".to_string()),
            queue_id: Some("default".to_string()),
            parent_agent_id: Some("agent:myproj:parent".to_string()),
            workflow_id: Some("workflow-alpha".to_string()),
            host_id: Some("host-a".to_string()),
            workspace_claim_id: Some("myproj:3".to_string()),
            last_check_at: Some("2026-05-14T06:00:00Z".to_string()),
            ..AgentLifecycleProjectionWire::default()
        };
        db.append_agent_event(
            agent_lifecycle_transitioned_event_request(
                context(),
                lifecycle.clone(),
            )
            .unwrap(),
        )
        .unwrap();

        let summary = agent_projection_summary(
            db.connection(),
            PROJECT_ID,
            "agent:myproj:queued-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(summary.status, "queued");
        assert_eq!(summary.batch_id.as_deref(), Some("batch-7"));
        assert_eq!(summary.queue_id.as_deref(), Some("default"));
        assert_eq!(summary.workflow_id.as_deref(), Some("workflow-alpha"));
        assert_eq!(summary.workspace_claim_id.as_deref(), Some("myproj:3"));

        let detail = agent_lifecycle_detail(
            db.connection(),
            PROJECT_ID,
            "agent:myproj:queued-1",
        )
        .unwrap();
        assert!(detail.summary.is_some());
    }

    #[test]
    fn reconciliation_emits_idempotent_stale_events() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let lifecycle = AgentLifecycleProjectionWire {
            schema_version: AGENT_PROJECTION_WIRE_SCHEMA_VERSION,
            agent_id: "agent:myproj:running-missing".to_string(),
            lifecycle_status: AgentLifecycleStatusWire::Running,
            project_name: Some("myproj".to_string()),
            artifact_dir: Some("/tmp/missing".to_string()),
            ..AgentLifecycleProjectionWire::default()
        };
        db.append_agent_event(
            agent_lifecycle_transitioned_event_request(context(), lifecycle)
                .unwrap(),
        )
        .unwrap();

        let scan = AgentArtifactScanWire {
            schema_version: crate::agent_scan::AGENT_SCAN_WIRE_SCHEMA_VERSION,
            projects_root: "/tmp/projects".to_string(),
            options: AgentArtifactScanOptionsWire::default(),
            stats: crate::agent_scan::AgentArtifactScanStatsWire::default(),
            records: Vec::new(),
        };
        let requests = agent_lifecycle_reconciliation_event_requests(
            db.connection(),
            context(),
            PROJECT_ID,
            &scan,
            "2026-05-14T06:10:00Z".to_string(),
        )
        .unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].event_type,
            AGENT_EVENT_RECONCILIATION_REPAIR_EMITTED
        );

        let first = db.append_agent_event(requests[0].clone()).unwrap();
        let second = db.append_agent_event(requests[0].clone()).unwrap();
        assert!(!first.duplicate);
        assert!(second.duplicate);

        let summary = agent_projection_summary(
            db.connection(),
            PROJECT_ID,
            "agent:myproj:running-missing",
        )
        .unwrap()
        .unwrap();
        assert_eq!(summary.status, "stale");
        assert_eq!(
            summary.stale_reason.as_deref(),
            Some("active projected agent no longer has source markers")
        );
    }

    #[test]
    fn changed_marker_replaces_stale_artifact_associations() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = fixture_root(&tmp);
        let artifact_dir = root
            .join("myproj")
            .join("artifacts")
            .join("ace-run")
            .join("20260513120000");
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let request = agent_projection_event_request_for_artifact_dir(
            context(),
            &root,
            &artifact_dir,
            &AgentArtifactScanOptionsWire::default(),
        )
        .unwrap()
        .unwrap();
        db.append_agent_event(request).unwrap();

        write_json(
            &artifact_dir.join("done.json"),
            &json!({
                "outcome": "completed",
                "finished_at": 1800000001.0,
                "cl_name": "feature_alpha",
                "name": "parent",
                "plan_path": "/tmp/plan.md"
            }),
        );
        let request = agent_projection_event_request_for_artifact_dir(
            context(),
            &root,
            &artifact_dir,
            &AgentArtifactScanOptionsWire::default(),
        )
        .unwrap()
        .unwrap();
        db.append_agent_event(request).unwrap();

        let agent_id = agent_id_from_timestamp("myproj", "20260513120000");
        let artifacts =
            agent_projection_artifacts(db.connection(), "myproj", &agent_id)
                .unwrap();
        let kinds: Vec<_> = artifacts
            .iter()
            .map(|artifact| artifact.artifact_kind.as_str())
            .collect();
        assert_eq!(kinds, vec!["plan"]);
    }
}
