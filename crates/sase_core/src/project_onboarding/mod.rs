//! Shared project-onboarding classification and creation-form policy.
//!
//! Python gathers provider observations, project records, and form
//! values. This module owns the pure ranking, hide/group rules, and
//! creation-option availability so every frontend agrees.

mod classify;
mod form;
mod wire;

#[cfg(test)]
mod tests;

use thiserror::Error;

pub use classify::classify_project_candidates;
pub use form::evaluate_repo_creation_form;
pub use wire::{
    CreationFormErrorWire, CreationFormFieldStateWire, CreationFormRequestWire,
    CreationFormStateWire, ProjectCandidateGroupWire,
    ProjectCandidateHiddenCountWire, ProjectCandidateInputWire,
    ProjectCandidateInventoryWire, ProjectCandidateJournalWire,
    ProjectCandidateListWire, ProjectCandidateLocalWire,
    ProjectCandidateNamespaceWire, ProjectCandidateProjectWire,
    ProjectCandidateRemoteWire, ProjectCandidateRowWire,
    RepoCreationChoiceWire, RepoCreationFieldWire, RepoCreationFormWire,
    ERROR_CONFLICT, ERROR_DISABLED_CHOICE, ERROR_INVALID_CHOICE,
    ERROR_REQUIRED, ERROR_REQUIRES, ERROR_UNKNOWN_OPTION, FIELD_KIND_BOOL,
    FIELD_KIND_CHOICE, FIELD_KIND_LINE, FIELD_KIND_SEARCH, FIELD_KIND_TEXT,
    FORM_MODE_LENIENT, FORM_MODE_STRICT, GROUP_DISABLED, GROUP_LINKED,
    GROUP_LOCAL, GROUP_RESUME, GROUP_SUGGESTED, HIDDEN_ARCHIVED,
    HIDDEN_ENABLED_PROJECT, HIDDEN_FORK, HIDDEN_NUMBERED_COPY, HIDDEN_SIDECAR,
    HIDDEN_THIRD_PARTY_LOCAL, INVENTORY_EXTERNAL, INVENTORY_LINKED,
    INVENTORY_SIDECAR, JOURNAL_COMPLETE, JOURNAL_FAILED, JOURNAL_RUNNING,
    NAMESPACE_ERROR, NAMESPACE_LOADING, NAMESPACE_OK,
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION, PROJECT_STATE_DISABLED,
    PROJECT_STATE_ENABLED, PROJECT_STATE_SIBLING,
};

/// Structural request failure for the project-onboarding wire envelope.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProjectOnboardingError {
    #[error(
        "unsupported project onboarding schema_version {actual}; expected {expected}"
    )]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error("{0}")]
    Validation(String),
}

impl ProjectOnboardingError {
    fn check_schema(actual: u32) -> Result<(), Self> {
        if actual == PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(Self::UnsupportedSchema {
                actual,
                expected: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
            })
        }
    }
}

/// Return the project-onboarding wire schema version.
pub fn project_onboarding_wire_schema_version() -> u32 {
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION
}
