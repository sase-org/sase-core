//! Typed errors for task-type spec, field-value, and snapshot operations.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One field-value problem found by [`super::validate_task_type_field_values`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeFieldValueError {
    pub kind: String,
    pub field: String,
    pub message: String,
}

impl TaskTypeFieldValueError {
    pub fn new(
        kind: impl Into<String>,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            field: field.into(),
            message: message.into(),
        }
    }
}

/// Failure from spec validation, rendering, or snapshot codec.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct TaskTypeError {
    pub kind: String,
    pub message: String,
}

impl TaskTypeError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }
}
