use crate::agent_identity::AgentOwnerIdentity;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub const AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION: u32 = 1;

pub const CLEANUP_OUTCOME_SELECTED: &str = "selected";
pub const CLEANUP_OUTCOME_PRESERVED: &str = "preserved";
pub const CLEANUP_OUTCOME_BLOCKED: &str = "blocked";

pub const CLEANUP_EFFECT_ARTIFACT_DIR: &str = "artifact_dir";
pub const CLEANUP_EFFECT_BUNDLE_PATH: &str = "bundle_path";

pub const REGISTRY_MERGE_ACTION_UPSERT: &str = "upsert";
pub const REGISTRY_MERGE_ACTION_REMOVE: &str = "remove";
pub const REGISTRY_MERGE_ACTION_NO_OP: &str = "no_op";

pub const RESERVATION_KIND_CLEANUP_IN_PROGRESS: &str = "cleanup_in_progress";

fn default_true() -> bool {
    true
}

/// One filesystem or content signature captured with the ownership snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceSignatureWire {
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub mtime_ns: Option<i64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub digest: Option<String>,
}

/// Process identity used by hosts for exact pre-effect revalidation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProcessIdentityWire {
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub process_group_id: Option<u32>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
}

/// Host-classified marker state for one owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMarkerStateWire {
    pub status: String,
    #[serde(default)]
    pub marker_kind: Option<String>,
    #[serde(default)]
    pub live: bool,
    #[serde(default)]
    pub terminal: bool,
    #[serde(default)]
    pub waiting: bool,
    #[serde(default)]
    pub cleanup_allowed: bool,
}

/// Owner metadata captured from the current registry or a launch slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExpectedOwnerWire {
    pub name: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub source_signature: Option<AgentSourceSignatureWire>,
    #[serde(default)]
    pub container_kind: Option<String>,
    #[serde(default)]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub family_generation: Option<String>,
    #[serde(default)]
    pub reservation_kind: Option<String>,
    #[serde(default)]
    pub marker_state: Option<AgentMarkerStateWire>,
    #[serde(default)]
    pub process: Option<AgentProcessIdentityWire>,
}

/// A logical launch/relaunch slot. The planner turns expected owners into
/// revalidation predicates and leaves all bead interpretation to the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipSlotWire {
    pub slot_id: String,
    pub requested_name: String,
    #[serde(default)]
    pub owner: Option<AgentOwnerIdentity>,
    #[serde(default)]
    pub expected_bead_id: Option<String>,
    #[serde(default)]
    pub expected_assignee: Option<String>,
    #[serde(default)]
    pub expected_owner: Option<AgentExpectedOwnerWire>,
    #[serde(default)]
    pub expected_clan_generation: Option<String>,
    #[serde(default)]
    pub expected_family_generation: Option<String>,
    #[serde(default)]
    pub marker_state: Option<AgentMarkerStateWire>,
    #[serde(default)]
    pub process: Option<AgentProcessIdentityWire>,
}

/// A cleanup root selected by the host from a coherent ownership snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCleanupRootWire {
    pub root_id: String,
    pub requested_name: String,
    #[serde(default)]
    pub expected_bead_id: Option<String>,
    #[serde(default)]
    pub expected_assignee: Option<String>,
    #[serde(default)]
    pub expected_owner: Option<AgentExpectedOwnerWire>,
    #[serde(default)]
    pub allow_container_cleanup: bool,
    #[serde(default)]
    pub allow_live_cleanup: bool,
    #[serde(default)]
    pub allow_waiting_cleanup: bool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentOwnershipSourceKindWire {
    Artifact,
    DismissedBundle,
}

impl AgentOwnershipSourceKindWire {
    pub const fn as_effect_kind(self) -> &'static str {
        match self {
            Self::Artifact => CLEANUP_EFFECT_ARTIFACT_DIR,
            Self::DismissedBundle => CLEANUP_EFFECT_BUNDLE_PATH,
        }
    }
}

/// One row from the shared source discovery pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipSourceRecordWire {
    pub record_id: String,
    pub source_kind: AgentOwnershipSourceKindWire,
    #[serde(default)]
    pub artifact_dir: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub canonical_names: Vec<String>,
    #[serde(default)]
    pub relation_refs: Vec<String>,
    #[serde(default)]
    pub outgoing_suffixes: Vec<String>,
    #[serde(default)]
    pub source_signature: Option<AgentSourceSignatureWire>,
    #[serde(default)]
    pub marker_state: Option<AgentMarkerStateWire>,
    #[serde(default)]
    pub process: Option<AgentProcessIdentityWire>,
}

/// Registry entry subset used by pure reservation decisions. Unknown fields
/// round-trip through `extra` so callers can preserve release-owned data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentNameRegistryEntryWire {
    pub name: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub canonical_global_name: Option<String>,
    #[serde(default)]
    pub source_owner: Option<AgentOwnerIdentity>,
    #[serde(default)]
    pub legacy_source_machine: Option<String>,
    #[serde(default)]
    pub imported_digest: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
    #[serde(default)]
    pub workflow_dir: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub reservation_kind: Option<String>,
    #[serde(default)]
    pub container_kind: Option<String>,
    #[serde(default)]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub template_namespace: Option<String>,
    #[serde(default)]
    pub reserved_at: Option<String>,
    #[serde(default)]
    pub cleanup_token: Option<String>,
    #[serde(default)]
    pub cleanup_operation: Option<String>,
    #[serde(default)]
    pub collision_owners: Vec<AgentNameRegistryEntryWire>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentNameReservationOperationWire {
    ReservePlanned,
    ClaimPlanned,
    ReserveClan,
    ClaimClan,
    ConvertFamily,
    ReserveTemplate,
    ReleasePlanned,
    ReleasePlannedClan,
    CleanupGuard,
}

impl AgentNameReservationOperationWire {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReservePlanned => "reserve_planned",
            Self::ClaimPlanned => "claim_planned",
            Self::ReserveClan => "reserve_clan",
            Self::ClaimClan => "claim_clan",
            Self::ConvertFamily => "convert_family",
            Self::ReserveTemplate => "reserve_template",
            Self::ReleasePlanned => "release_planned",
            Self::ReleasePlannedClan => "release_planned_clan",
            Self::CleanupGuard => "cleanup_guard",
        }
    }

    pub const fn writes_registry(self) -> bool {
        !matches!(self, Self::ReleasePlanned | Self::ReleasePlannedClan)
    }
}

/// One proposed registry decision evaluated against a single fresh snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNameReservationRequestWire {
    pub request_id: String,
    pub operation: AgentNameReservationOperationWire,
    pub name: String,
    pub artifact_dir: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub member_name: Option<String>,
    #[serde(default)]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub create_only: bool,
    #[serde(default)]
    pub replace_existing: bool,
    #[serde(default)]
    pub allowed_existing_names: Vec<String>,
    #[serde(default)]
    pub reserved_at: Option<String>,
    #[serde(default)]
    pub cleanup_token: Option<String>,
    #[serde(default)]
    pub expected_owner: Option<AgentExpectedOwnerWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExpectedOwnerPredicateWire {
    pub predicate_id: String,
    pub name: String,
    #[serde(default)]
    pub slot_id: Option<String>,
    #[serde(default)]
    pub root_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub source_record_id: Option<String>,
    #[serde(default)]
    pub expected_bead_id: Option<String>,
    #[serde(default)]
    pub expected_assignee: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub source_signature: Option<AgentSourceSignatureWire>,
    #[serde(default)]
    pub marker_state: Option<AgentMarkerStateWire>,
    #[serde(default)]
    pub process: Option<AgentProcessIdentityWire>,
    #[serde(default)]
    pub reservation_kind: Option<String>,
    #[serde(default)]
    pub container_kind: Option<String>,
    #[serde(default)]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub family_generation: Option<String>,
    #[serde(default)]
    pub must_be_absent: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipClosureWire {
    #[serde(default)]
    pub artifact_dirs: Vec<String>,
    #[serde(default)]
    pub bundle_paths: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub suffixes: Vec<String>,
    #[serde(default)]
    pub source_record_ids: Vec<String>,
    #[serde(default)]
    pub effect_predicates: Vec<AgentExpectedOwnerPredicateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCleanupRootPlanWire {
    pub root_id: String,
    pub requested_name: String,
    pub outcome: String,
    pub reason: String,
    pub closure: AgentOwnershipClosureWire,
    #[serde(default)]
    pub owner_predicate: Option<AgentExpectedOwnerPredicateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipOwnerDecisionWire {
    pub root_id: String,
    pub requested_name: String,
    pub outcome: String,
    pub reason: String,
    #[serde(default)]
    pub owner_predicate: Option<AgentExpectedOwnerPredicateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCleanupEffectWire {
    pub effect_kind: String,
    pub path: String,
    #[serde(default)]
    pub source_record_ids: Vec<String>,
    #[serde(default)]
    pub target_root_ids: Vec<String>,
    #[serde(default)]
    pub expected_predicates: Vec<AgentExpectedOwnerPredicateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNameReservationAcceptedWire {
    pub request_id: String,
    pub operation: AgentNameReservationOperationWire,
    pub action: String,
    pub name: String,
    pub storage_name: String,
    pub reason: String,
    #[serde(default)]
    pub clan_generation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNameReservationBlockedWire {
    pub request_id: String,
    pub operation: AgentNameReservationOperationWire,
    pub name: String,
    pub reason: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub conflicting_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNameRegistryMergeWire {
    pub request_id: String,
    pub action: String,
    pub name: String,
    #[serde(default)]
    pub entry: Option<AgentNameRegistryEntryWire>,
    pub expected: AgentExpectedOwnerPredicateWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCleanupReservationWire {
    pub request_id: String,
    pub name: String,
    pub storage_name: String,
    pub cleanup_token: String,
    pub expected_owner: AgentExpectedOwnerPredicateWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipBatchRequestWire {
    pub schema_version: u32,
    pub owner: AgentOwnerIdentity,
    #[serde(default)]
    pub known_owner_roots: Vec<String>,
    #[serde(default)]
    pub logical_slots: Vec<AgentOwnershipSlotWire>,
    #[serde(default)]
    pub cleanup_roots: Vec<AgentCleanupRootWire>,
    #[serde(default)]
    pub source_records: Vec<AgentOwnershipSourceRecordWire>,
    #[serde(default = "default_true")]
    pub sources_complete: bool,
    #[serde(default)]
    pub max_closure_records: Option<u32>,
    #[serde(default)]
    pub reservation_snapshot: Vec<AgentNameRegistryEntryWire>,
    #[serde(default)]
    pub reservation_requests: Vec<AgentNameReservationRequestWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnershipBatchPlanWire {
    pub schema_version: u32,
    #[serde(default)]
    pub selected_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    #[serde(default)]
    pub preserved_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    #[serde(default)]
    pub blocked_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    #[serde(default)]
    pub cleanup_roots: Vec<AgentCleanupRootPlanWire>,
    pub cleanup_closure: AgentOwnershipClosureWire,
    #[serde(default)]
    pub cleanup_effects: Vec<AgentCleanupEffectWire>,
    #[serde(default)]
    pub slot_owner_predicates: Vec<AgentExpectedOwnerPredicateWire>,
    #[serde(default)]
    pub reservation_decisions: Vec<AgentNameReservationAcceptedWire>,
    #[serde(default)]
    pub reservation_blocked: Vec<AgentNameReservationBlockedWire>,
    #[serde(default)]
    pub registry_merge_plan: Vec<AgentNameRegistryMergeWire>,
    #[serde(default)]
    pub cleanup_reservations: Vec<AgentCleanupReservationWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

pub fn agent_ownership_batch_request_from_json_value(
    value: &JsonValue,
) -> Result<AgentOwnershipBatchRequestWire, String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            "agent ownership batch wire missing or non-integer schema_version"
                .to_string()
        })?;
    if schema != AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION as u64 {
        return Err(format!(
            "agent ownership batch wire schema mismatch: got {schema}, expected {}",
            AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION
        ));
    }
    serde_json::from_value(value.clone())
        .map_err(|e| format!("invalid agent ownership batch wire: {e}"))
}
