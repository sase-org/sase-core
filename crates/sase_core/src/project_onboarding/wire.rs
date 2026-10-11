//! Versioned wire contract for project-onboarding classification and
//! repository-creation form evaluation.

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
