use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{
    AgentArchiveProjectionPageWire, AgentProjectionPageWire,
    AgentProjectionSummaryWire, ChangeSpecDetailWire, ChangeSpecListPageWire,
    ConfigCatalogProjectionWire, FileHistoryProjectionWire,
    MemoryCatalogProjectionWire, NotificationProjectionFacetCountsWire,
    XpromptCatalogProjectionWire,
};
use crate::bead::IssueWire;
use crate::notifications::{
    NotificationWire, PendingActionStoreWire, PendingActionWire,
};

pub const PROJECTION_READ_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionPageRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for ProjectionPageRequestWire {
    fn default() -> Self {
        Self {
            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionPageInfoWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionSnapshotReadWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionPayloadBoundWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub max_payload_bytes: u32,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionReadErrorCodeWire {
    InvalidRequest,
    CursorExpired,
    SnapshotExpired,
    ProjectionDegraded,
    UnsupportedCapability,
    PayloadTooLarge,
    ResourceNotFound,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionReadErrorWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub code: ProjectionReadErrorCodeWire,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ChangeSpecReadListRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub page: ProjectionPageRequestWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeSpecReadDetailRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeSpecReadListResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub entries: ChangeSpecListPageWire,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeSpecReadDetailResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<ChangeSpecDetailWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentReadListRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub page: ProjectionPageRequestWire,
    pub project_id: String,
    #[serde(default)]
    pub include_hidden: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentReadDetailRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentReadListResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub entries: AgentProjectionPageWire,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArchiveReadResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub entries: AgentArchiveProjectionPageWire,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentReadDetailResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub summary: AgentProjectionSummaryWire,
    pub children: Vec<AgentProjectionSummaryWire>,
    pub artifacts: Vec<super::AgentArtifactAssociationWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotificationReadListRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub page: ProjectionPageRequestWire,
    #[serde(default)]
    pub include_dismissed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationReadDetailRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub notification_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationReadListResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub notifications: Vec<NotificationWire>,
    pub counts: NotificationProjectionFacetCountsWire,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationReadDetailResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationPendingActionsReadResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub store: PendingActionStoreWire,
    pub actions: Vec<PendingActionWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BeadReadListRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub page: ProjectionPageRequestWire,
    pub project_id: String,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default)]
    pub issue_types: Vec<String>,
    #[serde(default)]
    pub tiers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadReadDetailRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub bead_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadReadListResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub issues: Vec<IssueWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadReadDetailResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub issue: IssueWire,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadReadStatsResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub project_id: String,
    pub stats: std::collections::BTreeMap<String, usize>,
    pub bounded: ProjectionPayloadBoundWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CatalogReadListRequestWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub page: ProjectionPageRequestWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogReadResponseWire {
    #[serde(default = "projection_read_schema_version")]
    pub schema_version: u32,
    pub snapshot: ProjectionSnapshotReadWire,
    pub page: ProjectionPageInfoWire,
    pub xprompts: Vec<XpromptCatalogProjectionWire>,
    pub config_sources: Vec<ConfigCatalogProjectionWire>,
    pub memory_sources: Vec<MemoryCatalogProjectionWire>,
    pub file_history: Vec<FileHistoryProjectionWire>,
    pub bounded: ProjectionPayloadBoundWire,
}

pub fn projection_read_schema_version() -> u32 {
    PROJECTION_READ_WIRE_SCHEMA_VERSION
}
