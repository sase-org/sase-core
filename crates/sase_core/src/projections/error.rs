use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionDuplicateEventWire {
    pub idempotency_key: String,
    pub seq: i64,
}

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("projection database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("projection JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("projection IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("projection migration {version} was applied as {applied_name:?}, expected {expected_name:?}")]
    MigrationNameMismatch {
        version: u32,
        applied_name: String,
        expected_name: String,
    },
    #[error("projection transaction failed to roll back after error: {rollback_error}")]
    Rollback {
        source: Box<ProjectionError>,
        rollback_error: rusqlite::Error,
    },
    #[error("duplicate projection event idempotency key {idempotency_key:?} at seq {seq}")]
    DuplicateEvent { idempotency_key: String, seq: i64 },
    #[error("projection invariant failed: {0}")]
    Invariant(String),
}

impl ProjectionError {
    pub fn duplicate_wire(&self) -> Option<ProjectionDuplicateEventWire> {
        match self {
            Self::DuplicateEvent {
                idempotency_key,
                seq,
            } => Some(ProjectionDuplicateEventWire {
                idempotency_key: idempotency_key.clone(),
                seq: *seq,
            }),
            _ => None,
        }
    }
}
