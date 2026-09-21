use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::wire::deserialize_option_non_empty_string;
use crate::bead::wire::deserialize_option_phase_size;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::DependencyWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use crate::serde_option::deserialize_present_option;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadCreateRequestWire {
    pub title: String,
    pub issue_type: IssueTypeWire,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub notes: String,
    #[serde(default)]
    pub design: String,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default)]
    pub model: String,
    #[serde(default, deserialize_with = "deserialize_option_phase_size")]
    pub size: Option<PhaseSizeWire>,
    #[serde(default, deserialize_with = "deserialize_option_non_empty_string")]
    pub task_type: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub task_type_fields: BTreeMap<String, String>,
    #[serde(default)]
    pub assignee: String,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub changespec_name: String,
    #[serde(default)]
    pub changespec_bug_id: String,
    #[serde(default)]
    pub external_ref: String,
    #[serde(default)]
    pub now: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadUpdateFieldsWire {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_phase_size")]
    pub size: Option<PhaseSizeWire>,
    #[serde(default)]
    pub closed_at: Option<Option<String>>,
    #[serde(default)]
    pub close_reason: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub resolution: Option<Option<BeadResolutionWire>>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub changespec_bug_id: Option<String>,
    #[serde(default)]
    pub external_ref: Option<String>,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub is_ready_to_work: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_type_fields: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub now: Option<String>,
}

/// One bead-link projection to apply inside a bulk mutation.
///
/// Field semantics match [`set_bead_link_projection`]: `target_ref` names the
/// other endpoint, `direction` says whether `issue_id` is the row's source
/// (`out`) or target (`in`), and `present` selects `LinkAdded` versus
/// `LinkRemoved`. `uses` of `0` is treated as `1` when `present` is true.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadLinkProjectionRequestWire {
    pub issue_id: String,
    pub target_ref: String,
    pub relation: String,
    pub direction: BeadLinkDirectionWire,
    pub present: bool,
    pub operation_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub origin: Option<ArtifactLinkOriginWire>,
    #[serde(default)]
    pub uses: u64,
    #[serde(default)]
    pub now: Option<String>,
}

impl Default for BeadLinkProjectionRequestWire {
    fn default() -> Self {
        Self {
            issue_id: String::new(),
            target_ref: String::new(),
            relation: String::new(),
            direction: BeadLinkDirectionWire::Out,
            present: false,
            operation_id: String::new(),
            description: None,
            origin: None,
            uses: 1,
            now: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadPreclaimAssignmentWire {
    pub bead_id: String,
    pub agent_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadPreclaimRollbackWire {
    pub bead_id: String,
    pub status: StatusWire,
    pub assignee: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadMutationOutcomeWire {
    pub operation: String,
    pub changed: bool,
    #[serde(default)]
    pub lock_wait_ms: u64,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub closed_ids: Vec<String>,
    #[serde(default)]
    pub already_closed_ids: Vec<String>,
    #[serde(default)]
    pub noted_ids: Vec<String>,
    #[serde(default)]
    pub cascade_closed_ids: Vec<String>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub issue: Option<IssueWire>,
    #[serde(default)]
    pub issues: Vec<IssueWire>,
    #[serde(default)]
    pub dependency: Option<DependencyWire>,
    #[serde(default)]
    pub dependencies: Vec<DependencyWire>,
    #[serde(default)]
    pub references: Vec<String>,
    #[serde(default)]
    pub next_counter: Option<u64>,
    #[serde(default)]
    pub rollback_preclaims: Vec<BeadPreclaimRollbackWire>,
    #[serde(default)]
    pub reopened_ancestor_ids: Vec<String>,
    #[serde(default)]
    pub unchanged_ids: Vec<String>,
    #[serde(default)]
    pub reopen_withheld: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reopen_withheld_closed_at: Option<String>,
}
