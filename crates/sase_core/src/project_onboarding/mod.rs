//! Shared project-onboarding classification, creation-form, and plan policy.
//!
//! Python gathers provider observations, project records, and form
//! values. This module owns the pure ranking, hide/group rules,
//! creation-option availability, and onboarding plan derivation so every
//! frontend agrees.

mod classify;
mod form;
mod plan;
mod wire;

#[cfg(test)]
mod plan_tests;
#[cfg(test)]
mod tests;

use thiserror::Error;

pub use classify::classify_project_candidates;
pub use form::evaluate_repo_creation_form;
pub use plan::plan_project_onboarding;
pub use wire::{
    CreationFormErrorWire, CreationFormFieldStateWire, CreationFormRequestWire,
    CreationFormStateWire, ProjectCandidateGroupWire,
    ProjectCandidateHiddenCountWire, ProjectCandidateInputWire,
    ProjectCandidateInventoryWire, ProjectCandidateJournalWire,
    ProjectCandidateListWire, ProjectCandidateLocalWire,
    ProjectCandidateNamespaceWire, ProjectCandidateProjectWire,
    ProjectCandidateRemoteWire, ProjectCandidateRowWire,
    ProjectOnboardingBlockerWire, ProjectOnboardingCheckoutFactsWire,
    ProjectOnboardingCreationWire, ProjectOnboardingFactsWire,
    ProjectOnboardingIdentityWire, ProjectOnboardingJournalFactsWire,
    ProjectOnboardingPlanInputWire, ProjectOnboardingPlanWire,
    ProjectOnboardingProjectFactsWire, ProjectOnboardingRecordFactsWire,
    ProjectOnboardingRemoteFactsWire, ProjectOnboardingRequestWire,
    ProjectOnboardingRequestedWire, ProjectOnboardingSetupWire,
    ProjectOnboardingStepWire, ProjectOnboardingTargetWire,
    ProjectOnboardingWarningWire, RepoCreationChoiceWire,
    RepoCreationFieldWire, RepoCreationFormWire, ACTOR_AGENT, ACTOR_HUMAN,
    BLOCKER_ALIAS_TAKEN, BLOCKER_CHECKOUT_CONFLICT, BLOCKER_CONFIG_DIRTY,
    BLOCKER_HUMAN_ONLY, BLOCKER_INVALID_NAME, BLOCKER_INVALID_OPTION,
    BLOCKER_NAME_TAKEN, BLOCKER_NOT_FOUND, BLOCKER_NOT_ON_DEFAULT_BRANCH,
    BLOCKER_NO_CREATE_RIGHTS, BLOCKER_NO_PUSH, BLOCKER_REMOTE_EXISTS,
    BLOCKER_REMOTE_UNKNOWN, CHECKOUT_ABSENT, CHECKOUT_EMPTY_DIR,
    CHECKOUT_MATCHING, CHECKOUT_MISMATCH, CHECKOUT_NOT_GIT, ERROR_CONFLICT,
    ERROR_DISABLED_CHOICE, ERROR_INVALID_CHOICE, ERROR_REQUIRED,
    ERROR_REQUIRES, ERROR_UNKNOWN_OPTION, FIELD_KIND_BOOL, FIELD_KIND_CHOICE,
    FIELD_KIND_LINE, FIELD_KIND_SEARCH, FIELD_KIND_TEXT, FORM_MODE_LENIENT,
    FORM_MODE_STRICT, GROUP_DISABLED, GROUP_LINKED, GROUP_LOCAL, GROUP_RESUME,
    GROUP_SUGGESTED, HIDDEN_ARCHIVED, HIDDEN_ENABLED_PROJECT, HIDDEN_FORK,
    HIDDEN_NUMBERED_COPY, HIDDEN_SIDECAR, HIDDEN_THIRD_PARTY_LOCAL,
    INVENTORY_EXTERNAL, INVENTORY_LINKED, INVENTORY_SIDECAR, JOURNAL_COMPLETE,
    JOURNAL_FAILED, JOURNAL_RUNNING, KEY_SOURCE_ALLOCATED, KEY_SOURCE_EXISTING,
    MODE_ADD, MODE_CREATE, NAMESPACE_ERROR, NAMESPACE_LOADING, NAMESPACE_OK,
    NAME_SOURCE_ALLOCATED, NAME_SOURCE_EXISTING, NAME_SOURCE_REQUESTED,
    OPERATION_CREATE, OPERATION_UPDATE, OPERATION_VALIDATE,
    PLAN_STATUS_BLOCKED, PLAN_STATUS_CURRENT, PLAN_STATUS_READY,
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION, PROJECT_STATE_DISABLED,
    PROJECT_STATE_ENABLED, PROJECT_STATE_SIBLING, REMOTE_FOUND,
    REMOTE_NOT_FOUND, REMOTE_UNAVAILABLE, SETUP_AUTO, SETUP_CODE_AGENT,
    SETUP_CODE_ALREADY_MANAGED, SETUP_CODE_NEW_REPO, SETUP_CODE_NO_PUSH,
    SETUP_CODE_OFF, SETUP_CODE_ON, SETUP_CODE_RESOLVED_OFF,
    SETUP_CODE_RESUMING, SETUP_CODE_UNKNOWN_PUSH,
    SETUP_CODE_UNMANAGED_CAN_PUSH, SETUP_OFF, SETUP_ON, STEP_CHECKOUT,
    STEP_CREATE_REMOTE, STEP_CURRENT, STEP_DONE, STEP_IDENTITY, STEP_INIT,
    STEP_MANAGE, STEP_PENDING, STEP_REENABLE, STEP_SKIPPED,
    STORAGE_SEPARATE_REPO, WARNING_NOT_LAUNCHABLE, WARNING_REMOTE_UNVERIFIED,
    WARNING_UNKNOWN_PUSH, WORKFLOW_GIT,
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
