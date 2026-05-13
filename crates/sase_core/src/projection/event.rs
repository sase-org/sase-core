use std::time::SystemTime;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

pub const PROJECTION_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSource {
    pub kind: EventSourceKind,
    pub name: String,
}

impl EventSource {
    pub fn new(kind: EventSourceKind, name: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSourceKind {
    Cli,
    Tui,
    Daemon,
    Editor,
    Import,
    Test,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIdentity {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
}

impl ProjectIdentity {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            root: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIdentity {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

impl HostIdentity {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            hostname: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CausalityMetadata {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_event_ids: Vec<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionEvent {
    pub schema_version: u32,
    pub seq: i64,
    pub timestamp: String,
    pub source: EventSource,
    pub project: ProjectIdentity,
    pub host: HostIdentity,
    pub event_type: ProjectionEventType,
    pub payload: serde_json::Value,
    pub idempotency_key: String,
    pub causality: CausalityMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewProjectionEvent {
    pub timestamp: String,
    pub source: EventSource,
    pub project: ProjectIdentity,
    pub host: HostIdentity,
    pub event_type: ProjectionEventType,
    pub payload: serde_json::Value,
    pub idempotency_key: String,
    pub causality: CausalityMetadata,
}

impl NewProjectionEvent {
    pub fn new(
        source: EventSource,
        project: ProjectIdentity,
        host: HostIdentity,
        event_type: ProjectionEventType,
        payload: serde_json::Value,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: current_timestamp(),
            source,
            project,
            host,
            event_type,
            payload,
            idempotency_key: idempotency_key.into(),
            causality: CausalityMetadata::default(),
        }
    }

    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    pub fn with_causality(mut self, causality: CausalityMetadata) -> Self {
        self.causality = causality;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", content = "kind", rename_all = "snake_case")]
pub enum ProjectionEventType {
    ChangeSpec(ChangeSpecEventKind),
    Notification(NotificationEventKind),
    Agent(AgentEventKind),
    Bead(BeadEventKind),
    Workflow(WorkflowEventKind),
    XpromptCatalog(CatalogEventKind),
    MemoryCatalog(CatalogEventKind),
    FileHistory(FileHistoryEventKind),
}

impl ProjectionEventType {
    pub fn family_name(&self) -> &'static str {
        match self {
            Self::ChangeSpec(_) => "change_spec",
            Self::Notification(_) => "notification",
            Self::Agent(_) => "agent",
            Self::Bead(_) => "bead",
            Self::Workflow(_) => "workflow",
            Self::XpromptCatalog(_) => "xprompt_catalog",
            Self::MemoryCatalog(_) => "memory_catalog",
            Self::FileHistory(_) => "file_history",
        }
    }
}

impl EventSourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Tui => "tui",
            Self::Daemon => "daemon",
            Self::Editor => "editor",
            Self::Import => "import",
            Self::Test => "test",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for EventSourceKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cli" => Ok(Self::Cli),
            "tui" => Ok(Self::Tui),
            "daemon" => Ok(Self::Daemon),
            "editor" => Ok(Self::Editor),
            "import" => Ok(Self::Import),
            "test" => Ok(Self::Test),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown event source kind: {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeSpecEventKind {
    ParsedSnapshotUpsert,
    MutationApplied,
    StatusTransition,
    ArchiveMoved,
    SectionUpdated,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationEventKind {
    Appended,
    SnapshotRewritten,
    StateUpdated,
    PendingActionRegistered,
    PendingActionUpdated,
    PendingActionCleaned,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventKind {
    LifecycleTransition,
    AttemptRecorded,
    EdgeUpserted,
    WorkflowChildEdgeUpserted,
    ArtifactUpserted,
    DismissedIdentityUpserted,
    ArchiveBundleUpserted,
    Revived,
    Purged,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadEventKind {
    Created,
    Updated,
    StatusTransition,
    DependencyAdded,
    DependencyRemoved,
    ReadyToWorkMarked,
    ChangeSpecMetadataChanged,
    SnapshotImported,
    SnapshotExported,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowEventKind {
    RunStarted,
    RunFinished,
    StepTransition,
    HitlPaused,
    HitlResumed,
    RetryScheduled,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogEventKind {
    EntryUpserted,
    SnapshotReplaced,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileHistoryEventKind {
    EntryUpserted,
    SnapshotReplaced,
    Tombstone,
}

pub fn current_timestamp() -> String {
    let dt = DateTime::<Utc>::from(SystemTime::now());
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}
