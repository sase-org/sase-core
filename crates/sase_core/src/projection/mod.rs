pub mod event;
pub mod schema;
pub mod store;

pub use event::{
    current_timestamp, AgentEventKind, BeadEventKind, CatalogEventKind,
    CausalityMetadata, ChangeSpecEventKind, EventSource, EventSourceKind,
    FileHistoryEventKind, HostIdentity, NewProjectionEvent,
    NotificationEventKind, ProjectIdentity, ProjectionEvent,
    ProjectionEventType, WorkflowEventKind, PROJECTION_EVENT_SCHEMA_VERSION,
};
pub use schema::PROJECTION_SCHEMA_VERSION;
pub use store::{AppendEventOutcome, ProjectionStore, ProjectionStoreConfig};

#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    #[error("projection sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("projection io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("projection json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid projection event: {0}")]
    InvalidEvent(String),
    #[error("projection invariant failed: {0}")]
    Invariant(String),
}
