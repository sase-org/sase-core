//! Versioned wire contract for project-onboarding classification,
//! repository-creation form evaluation, and onboarding plan derivation.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Schema version for project-onboarding request and result envelopes.
pub const PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION: u32 = 1;

pub const GROUP_RESUME: &str = "resume";
pub const GROUP_LOCAL: &str = "local";
pub const GROUP_SUGGESTED: &str = "suggested";
pub const GROUP_LINKED: &str = "linked";
pub const GROUP_DISABLED: &str = "disabled";

pub const HIDDEN_ENABLED_PROJECT: &str = "enabled_project";
pub const HIDDEN_SIDECAR: &str = "sidecar";
pub const HIDDEN_ARCHIVED: &str = "archived";
pub const HIDDEN_FORK: &str = "fork";
pub const HIDDEN_NUMBERED_COPY: &str = "numbered_copy";
pub const HIDDEN_THIRD_PARTY_LOCAL: &str = "third_party_local";

pub const JOURNAL_RUNNING: &str = "running";
pub const JOURNAL_FAILED: &str = "failed";
pub const JOURNAL_COMPLETE: &str = "complete";

pub const NAMESPACE_OK: &str = "ok";
pub const NAMESPACE_ERROR: &str = "error";
pub const NAMESPACE_LOADING: &str = "loading";

pub const INVENTORY_SIDECAR: &str = "sidecar";
pub const INVENTORY_LINKED: &str = "linked";
pub const INVENTORY_EXTERNAL: &str = "external";

pub const PROJECT_STATE_ENABLED: &str = "enabled";
pub const PROJECT_STATE_DISABLED: &str = "disabled";
pub const PROJECT_STATE_SIBLING: &str = "sibling";

pub const FORM_MODE_STRICT: &str = "strict";
pub const FORM_MODE_LENIENT: &str = "lenient";

pub const FIELD_KIND_LINE: &str = "line";
pub const FIELD_KIND_TEXT: &str = "text";
pub const FIELD_KIND_BOOL: &str = "bool";
pub const FIELD_KIND_CHOICE: &str = "choice";
pub const FIELD_KIND_SEARCH: &str = "search";

pub const ERROR_REQUIRED: &str = "required";
pub const ERROR_UNKNOWN_OPTION: &str = "unknown_option";
pub const ERROR_INVALID_CHOICE: &str = "invalid_choice";
pub const ERROR_DISABLED_CHOICE: &str = "disabled_choice";
pub const ERROR_CONFLICT: &str = "conflict";
pub const ERROR_REQUIRES: &str = "requires";

pub const MODE_ADD: &str = "add";
pub const MODE_CREATE: &str = "create";

pub const ACTOR_HUMAN: &str = "human";
pub const ACTOR_AGENT: &str = "agent";

pub const SETUP_AUTO: &str = "auto";
pub const SETUP_ON: &str = "on";
pub const SETUP_OFF: &str = "off";

pub const REMOTE_FOUND: &str = "found";
pub const REMOTE_NOT_FOUND: &str = "not_found";
pub const REMOTE_UNAVAILABLE: &str = "unavailable";

pub const CHECKOUT_ABSENT: &str = "absent";
pub const CHECKOUT_MATCHING: &str = "matching";
pub const CHECKOUT_MISMATCH: &str = "mismatch";
pub const CHECKOUT_NOT_GIT: &str = "not_git";
pub const CHECKOUT_EMPTY_DIR: &str = "empty_dir";

pub const PLAN_STATUS_READY: &str = "ready";
pub const PLAN_STATUS_BLOCKED: &str = "blocked";
pub const PLAN_STATUS_CURRENT: &str = "current";

pub const STEP_CREATE_REMOTE: &str = "create_remote";
pub const STEP_CHECKOUT: &str = "checkout";
pub const STEP_REENABLE: &str = "reenable";
pub const STEP_IDENTITY: &str = "identity";
pub const STEP_MANAGE: &str = "manage";
pub const STEP_CURRENT: &str = "current";
pub const STEP_INIT: &str = "init";

pub const STEP_PENDING: &str = "pending";
pub const STEP_DONE: &str = "done";
pub const STEP_SKIPPED: &str = "skipped";

pub const OPERATION_CREATE: &str = "create";
pub const OPERATION_UPDATE: &str = "update";
pub const OPERATION_VALIDATE: &str = "validate";

pub const KEY_SOURCE_EXISTING: &str = "existing";
pub const KEY_SOURCE_ALLOCATED: &str = "allocated";

pub const NAME_SOURCE_EXISTING: &str = "existing";
pub const NAME_SOURCE_REQUESTED: &str = "requested";
pub const NAME_SOURCE_ALLOCATED: &str = "allocated";

pub const SETUP_CODE_AGENT: &str = "agent";
pub const SETUP_CODE_NEW_REPO: &str = "new_repo";
pub const SETUP_CODE_ALREADY_MANAGED: &str = "already_managed";
pub const SETUP_CODE_UNMANAGED_CAN_PUSH: &str = "unmanaged_can_push";
pub const SETUP_CODE_NO_PUSH: &str = "no_push";
pub const SETUP_CODE_UNKNOWN_PUSH: &str = "unknown_push";
pub const SETUP_CODE_RESUMING: &str = "resuming";
pub const SETUP_CODE_RESOLVED_OFF: &str = "resolved_off";
pub const SETUP_CODE_ON: &str = "on";
pub const SETUP_CODE_OFF: &str = "off";

pub const BLOCKER_REMOTE_EXISTS: &str = "remote_exists";
pub const BLOCKER_REMOTE_UNKNOWN: &str = "remote_unknown";
pub const BLOCKER_HUMAN_ONLY: &str = "human_only";
pub const BLOCKER_NO_CREATE_RIGHTS: &str = "no_create_rights";
pub const BLOCKER_INVALID_OPTION: &str = "invalid_option";
pub const BLOCKER_CHECKOUT_CONFLICT: &str = "checkout_conflict";
pub const BLOCKER_NOT_FOUND: &str = "not_found";
pub const BLOCKER_INVALID_NAME: &str = "invalid_name";
pub const BLOCKER_NAME_TAKEN: &str = "name_taken";
pub const BLOCKER_ALIAS_TAKEN: &str = "alias_taken";
pub const BLOCKER_CONFIG_DIRTY: &str = "config_dirty";
pub const BLOCKER_NOT_ON_DEFAULT_BRANCH: &str = "not_on_default_branch";
pub const BLOCKER_NO_PUSH: &str = "no_push";

pub const WARNING_NOT_LAUNCHABLE: &str = "not_launchable";
pub const WARNING_UNKNOWN_PUSH: &str = "unknown_push";
pub const WARNING_REMOTE_UNVERIFIED: &str = "remote_unverified";

pub const STORAGE_SEPARATE_REPO: &str = "separate_repo";
pub const WORKFLOW_GIT: &str = "git";

fn default_schema_version() -> u32 {
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION
}

/// One owner/account namespace the host already observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateNamespaceWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub kind_label: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub error_kind: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub fetched_at: String,
}

/// One remote repository candidate from a provider list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateRemoteWire {
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub identity_key: String,
    #[serde(default, rename = "ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub is_fork: bool,
    #[serde(default)]
    pub is_archived: bool,
    #[serde(default)]
    pub is_empty: Option<bool>,
    #[serde(default)]
    pub pushed_at: String,
    #[serde(default)]
    pub repo_id: String,
    #[serde(default)]
    pub viewer_permission: String,
}

/// One local clone the host found on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateLocalWire {
    #[serde(default)]
    pub identity_key: String,
    #[serde(default, rename = "ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub numbered_copy: bool,
}

/// One registered SASE project record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateProjectWire {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub identity_keys: Vec<String>,
}

/// One inventory row (sidecar, linked, or external checkout).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateInventoryWire {
    #[serde(default)]
    pub identity_key: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub project_key: String,
    #[serde(default)]
    pub open_count: u64,
    #[serde(default)]
    pub last_open_at: String,
}

/// One onboarding journal the host still considers live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateJournalWire {
    #[serde(default)]
    pub identity_key: String,
    #[serde(default, rename = "ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub stopped_at: String,
    #[serde(default)]
    pub updated_at: String,
}

/// Request envelope for candidate classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateInputWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub workflow_type: String,
    #[serde(default)]
    pub sidecar_roles: Vec<String>,
    #[serde(default)]
    pub owned_namespaces: Vec<String>,
    #[serde(default)]
    pub namespaces: Vec<ProjectCandidateNamespaceWire>,
    #[serde(default)]
    pub remote: Vec<ProjectCandidateRemoteWire>,
    #[serde(default)]
    pub local: Vec<ProjectCandidateLocalWire>,
    #[serde(default)]
    pub projects: Vec<ProjectCandidateProjectWire>,
    #[serde(default)]
    pub inventory: Vec<ProjectCandidateInventoryWire>,
    #[serde(default)]
    pub journals: Vec<ProjectCandidateJournalWire>,
}

/// One classified candidate row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateRowWire {
    #[serde(default)]
    pub identity_key: String,
    #[serde(default, rename = "ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hidden_reason: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pushed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_empty: Option<bool>,
    #[serde(default)]
    pub viewer_permission: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub local_path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_state: String,
    #[serde(default)]
    pub linked_by: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub journal_status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stopped_at: String,
}

/// One non-empty classified group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateGroupWire {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub rows: Vec<ProjectCandidateRowWire>,
}

/// Hidden-row count for one hide reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateHiddenCountWire {
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub count: u64,
}

/// Classified candidate list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectCandidateListWire {
    pub schema_version: u32,
    #[serde(default)]
    pub groups: Vec<ProjectCandidateGroupWire>,
    #[serde(default)]
    pub search_only: Vec<ProjectCandidateRowWire>,
    #[serde(default)]
    pub hidden: Vec<ProjectCandidateHiddenCountWire>,
    #[serde(default)]
    pub namespaces: Vec<ProjectCandidateNamespaceWire>,
}

/// One choice offered by a creation-form field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepoCreationChoiceWire {
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub disabled_reason: String,
}

/// One field on a provider creation form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepoCreationFieldWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub default: String,
    #[serde(default)]
    pub choices: Vec<RepoCreationChoiceWire>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub placeholder: String,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub disabled_reason: String,
}

/// Provider-described repository creation form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepoCreationFormWire {
    #[serde(default)]
    pub workflow_type: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub fields: Vec<RepoCreationFieldWire>,
    #[serde(default)]
    pub can_create: Option<bool>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub remedy_argv: Vec<String>,
}

/// Request envelope for creation-form evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CreationFormRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub form: RepoCreationFormWire,
    #[serde(default)]
    pub values: BTreeMap<String, String>,
    #[serde(default)]
    pub mode: String,
}

/// Per-field availability after evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CreationFormFieldStateWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub available: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub is_set: bool,
}

/// One validation error from form evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CreationFormErrorWire {
    #[serde(default)]
    pub field: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

/// Evaluated creation-form state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CreationFormStateWire {
    pub schema_version: u32,
    #[serde(default)]
    pub fields: Vec<CreationFormFieldStateWire>,
    #[serde(default)]
    pub effective_options: BTreeMap<String, String>,
    #[serde(default)]
    pub errors: Vec<CreationFormErrorWire>,
    #[serde(default)]
    pub valid: bool,
}

/// Provider identity of the repository being added or created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingTargetWire {
    #[serde(default)]
    pub workflow_type: String,
    #[serde(default)]
    pub provider_display: String,
    #[serde(default)]
    pub identity_key: String,
    #[serde(default, rename = "ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub web_url: String,
    #[serde(default)]
    pub checkout_dir: String,
    #[serde(default)]
    pub project_key_base: String,
    #[serde(default)]
    pub project_name_base: String,
    #[serde(default)]
    pub storage_policy: String,
}

/// Caller-requested identity and setup options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingRequestedWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub setup: String,
    #[serde(default)]
    pub make_current: bool,
}

/// Create-mode remote options included in the intent fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingCreationWire {
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub summary: Vec<String>,
}

/// Intent half of an onboarding plan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingRequestWire {
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub target: ProjectOnboardingTargetWire,
    #[serde(default)]
    pub requested: ProjectOnboardingRequestedWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation: Option<ProjectOnboardingCreationWire>,
}

/// Observed remote repository for planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingRemoteFactsWire {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub is_empty: Option<bool>,
    #[serde(default)]
    pub is_archived: Option<bool>,
    #[serde(default)]
    pub can_push: Option<bool>,
    #[serde(default)]
    pub sase_managed: Option<bool>,
    #[serde(default)]
    pub repo_id: String,
    #[serde(default)]
    pub default_branch: String,
    #[serde(default)]
    pub message: String,
}

/// Journal evidence that this intent already created a remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingJournalFactsWire {
    #[serde(default)]
    pub intent_recorded_at: String,
    #[serde(default)]
    pub remote_created: bool,
    #[serde(default)]
    pub repo_id: String,
    /// Setup decision captured for this active intent before any effects ran.
    #[serde(default)]
    pub setup_required: Option<bool>,
    /// A matching intent has started management but has not verified its push.
    #[serde(default)]
    pub management_incomplete: bool,
    /// Current config bytes exactly match the marker content recorded by that effect.
    #[serde(default)]
    pub config_changes_owned: bool,
}

/// Observed checkout directory for planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingCheckoutFactsWire {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub origin_url: String,
    #[serde(default)]
    pub config_dirty: bool,
    #[serde(default)]
    pub on_default_branch: Option<bool>,
}

/// Existing project record for the target, when one is already registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingRecordFactsWire {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub state: String,
}

/// One already-registered project used for identity collision checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingProjectFactsWire {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub state: String,
}

/// Host-gathered facts the planner consumes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingFactsWire {
    #[serde(default)]
    pub remote: ProjectOnboardingRemoteFactsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<ProjectOnboardingJournalFactsWire>,
    #[serde(default)]
    pub checkout: ProjectOnboardingCheckoutFactsWire,
    #[serde(default)]
    pub local_managed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<ProjectOnboardingRecordFactsWire>,
    #[serde(default)]
    pub projects: Vec<ProjectOnboardingProjectFactsWire>,
    #[serde(default)]
    pub creation_errors: Vec<CreationFormErrorWire>,
    #[serde(default)]
    pub can_create: Option<bool>,
}

/// Combined plan request envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingPlanInputWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub request: ProjectOnboardingRequestWire,
    #[serde(default)]
    pub facts: ProjectOnboardingFactsWire,
}

/// Previewed project identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingIdentityWire {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub key_source: String,
    #[serde(default)]
    pub name_source: String,
}

/// Resolved setup policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingSetupWire {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub reason: String,
}

/// One ordered onboarding step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingStepWire {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub remote: bool,
    #[serde(default)]
    pub requires_tty: bool,
    #[serde(default)]
    pub handoff: bool,
}

/// One plan warning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingWarningWire {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

/// One plan blocker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingBlockerWire {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hint: String,
}

/// Derived onboarding plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectOnboardingPlanWire {
    pub schema_version: u32,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub intent_fingerprint: String,
    #[serde(default)]
    pub identity: ProjectOnboardingIdentityWire,
    #[serde(default)]
    pub setup: ProjectOnboardingSetupWire,
    #[serde(default)]
    pub steps: Vec<ProjectOnboardingStepWire>,
    #[serde(default)]
    pub warnings: Vec<ProjectOnboardingWarningWire>,
    #[serde(default)]
    pub blockers: Vec<ProjectOnboardingBlockerWire>,
}
