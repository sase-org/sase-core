//! Shared fleet-read error type.

use sase_core::fleet_contract::FleetContractError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FleetReadError {
    #[error("{0}")]
    Validation(String),
    #[error("fleet resource not found: {0}")]
    NotFound(String),
    #[error("fleet resource is stale: {0}")]
    Stale(String),
    #[error("fleet read timed out: {0}")]
    Timeout(String),
    #[error("fleet backend failed: {0}")]
    Backend(String),
}

impl FleetReadError {
    pub(super) fn safe_code(&self) -> String {
        match self {
            Self::Validation(_) => "validation".to_string(),
            Self::NotFound(_) => "not_found".to_string(),
            Self::Stale(_) => "stale".to_string(),
            Self::Timeout(_) => "timeout".to_string(),
            Self::Backend(_) => "backend".to_string(),
        }
    }
}

impl From<FleetContractError> for FleetReadError {
    fn from(error: FleetContractError) -> Self {
        Self::Validation(error.to_string())
    }
}
