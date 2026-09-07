use super::wire::{
    AgentCleanupEffectWire, AgentCleanupReservationWire,
    AgentCleanupRootPlanWire, AgentCleanupRootWire,
    AgentExpectedOwnerPredicateWire, AgentExpectedOwnerWire,
    AgentNameRegistryEntryWire, AgentNameRegistryMergeWire,
    AgentNameReservationAcceptedWire, AgentNameReservationBlockedWire,
    AgentNameReservationOperationWire, AgentNameReservationRequestWire,
    AgentOwnershipBatchPlanWire, AgentOwnershipBatchRequestWire,
    AgentOwnershipClosureWire, AgentOwnershipOwnerDecisionWire,
    AgentOwnershipSlotWire, AgentOwnershipSourceKindWire,
    AgentOwnershipSourceRecordWire, AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION,
    CLEANUP_EFFECT_ARTIFACT_DIR, CLEANUP_EFFECT_BUNDLE_PATH,
    CLEANUP_OUTCOME_BLOCKED, CLEANUP_OUTCOME_PRESERVED,
    CLEANUP_OUTCOME_SELECTED, REGISTRY_MERGE_ACTION_NO_OP,
    REGISTRY_MERGE_ACTION_REMOVE, REGISTRY_MERGE_ACTION_UPSERT,
    RESERVATION_KIND_CLEANUP_IN_PROGRESS,
};
use crate::agent_identity::{
    foreign_agent_owner_root, globalize_agent_name, normalize_owned_agent_name,
    validate_owner_root, AgentIdentityError, AgentOwnerIdentity,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use thiserror::Error;

const DEFAULT_MAX_CLOSURE_RECORDS: usize = 16_384;
const RESERVATION_KIND_PLANNED: &str = "planned";
const RESERVATION_KIND_CLAIMED: &str = "claimed";
const RESERVATION_KIND_PLANNED_CLAN: &str = "planned_clan";
const RESERVATION_KIND_CLAN: &str = "clan";
const RESERVATION_KIND_FAMILY: &str = "family";
const CONTAINER_KIND_CLAN: &str = "clan";
const CONTAINER_KIND_FAMILY: &str = "family";
const CONTAINER_KIND_OWNER_NAMESPACE: &str = "owner_namespace";
const ORIGIN_IMPORT_V1: &str = "import_v1";
const ORIGIN_IMPORT_V2: &str = "import_v2";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentOwnershipBatchError {
    #[error(
        "unsupported agent ownership batch schema_version {actual}; expected {expected}"
    )]
    UnsupportedSchema { actual: u32, expected: u32 },

    #[error("invalid batch owner: {source}")]
    InvalidBatchOwner {
        #[source]
        source: AgentIdentityError,
    },

    #[error("invalid known owner root '{root}': {source}")]
    InvalidKnownOwnerRoot {
        root: String,
        #[source]
        source: AgentIdentityError,
    },

    #[error("logical slot '{slot_id}' has owner that does not match the batch owner")]
    SlotOwnerMismatch { slot_id: String },

    #[error("duplicate logical slot ID '{slot_id}'")]
    DuplicateSlotId { slot_id: String },

    #[error("duplicate cleanup root ID '{root_id}'")]
    DuplicateCleanupRootId { root_id: String },

    #[error("duplicate source record ID '{record_id}'")]
    DuplicateSourceRecordId { record_id: String },

    #[error("duplicate registry snapshot entry '{name}'")]
    DuplicateRegistryEntry { name: String },
}

pub fn plan_agent_ownership_batch(
    request: &AgentOwnershipBatchRequestWire,
) -> Result<AgentOwnershipBatchPlanWire, AgentOwnershipBatchError> {
    validate_request(request)?;

    let slot_owner_predicates = request
        .logical_slots
        .iter()
        .filter_map(slot_owner_predicate)
        .collect();
    let cleanup = plan_cleanup_roots(request)?;
    let reservation = plan_reservation_requests(request)?;

    Ok(AgentOwnershipBatchPlanWire {
        schema_version: AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION,
        selected_owners: cleanup.selected_owners,
        preserved_owners: cleanup.preserved_owners,
        blocked_owners: cleanup.blocked_owners,
        cleanup_roots: cleanup.root_plans,
        cleanup_closure: cleanup.union_closure,
        cleanup_effects: cleanup.effects,
        slot_owner_predicates,
        reservation_decisions: reservation.accepted,
        reservation_blocked: reservation.blocked,
        registry_merge_plan: reservation.merge_plan,
        cleanup_reservations: reservation.cleanup_reservations,
        diagnostics: cleanup.diagnostics,
    })
}

fn validate_request(
    request: &AgentOwnershipBatchRequestWire,
) -> Result<(), AgentOwnershipBatchError> {
    if request.schema_version != AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION {
        return Err(AgentOwnershipBatchError::UnsupportedSchema {
            actual: request.schema_version,
            expected: AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION,
        });
    }
    request.owner.validate().map_err(|source| {
        AgentOwnershipBatchError::InvalidBatchOwner { source }
    })?;
    for root in &request.known_owner_roots {
        validate_owner_root(root).map_err(|source| {
            AgentOwnershipBatchError::InvalidKnownOwnerRoot {
                root: root.clone(),
                source,
            }
        })?;
    }
    validate_unique_slots(&request.logical_slots, &request.owner)?;
    validate_unique_cleanup_roots(&request.cleanup_roots)?;
    validate_unique_source_records(&request.source_records)?;
    validate_unique_registry_entries(&request.reservation_snapshot)?;
    Ok(())
}

fn validate_unique_slots(
    slots: &[AgentOwnershipSlotWire],
    owner: &AgentOwnerIdentity,
) -> Result<(), AgentOwnershipBatchError> {
    let mut seen = BTreeSet::new();
    for slot in slots {
        if !seen.insert(slot.slot_id.clone()) {
            return Err(AgentOwnershipBatchError::DuplicateSlotId {
                slot_id: slot.slot_id.clone(),
            });
        }
        if let Some(slot_owner) = &slot.owner {
            if slot_owner != owner {
                return Err(AgentOwnershipBatchError::SlotOwnerMismatch {
                    slot_id: slot.slot_id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_unique_cleanup_roots(
    roots: &[AgentCleanupRootWire],
) -> Result<(), AgentOwnershipBatchError> {
    let mut seen = BTreeSet::new();
    for root in roots {
        if !seen.insert(root.root_id.clone()) {
            return Err(AgentOwnershipBatchError::DuplicateCleanupRootId {
                root_id: root.root_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_unique_source_records(
    records: &[AgentOwnershipSourceRecordWire],
) -> Result<(), AgentOwnershipBatchError> {
    let mut seen = BTreeSet::new();
    for record in records {
        if !seen.insert(record.record_id.clone()) {
            return Err(AgentOwnershipBatchError::DuplicateSourceRecordId {
                record_id: record.record_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_unique_registry_entries(
    entries: &[AgentNameRegistryEntryWire],
) -> Result<(), AgentOwnershipBatchError> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if !seen.insert(entry.name.clone()) {
            return Err(AgentOwnershipBatchError::DuplicateRegistryEntry {
                name: entry.name.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Debug)]
struct CleanupPlanAccumulator {
    selected_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    preserved_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    blocked_owners: Vec<AgentOwnershipOwnerDecisionWire>,
    root_plans: Vec<AgentCleanupRootPlanWire>,
    union_closure: AgentOwnershipClosureWire,
    effects: Vec<AgentCleanupEffectWire>,
    diagnostics: Vec<String>,
}

fn plan_cleanup_roots(
    request: &AgentOwnershipBatchRequestWire,
) -> Result<CleanupPlanAccumulator, AgentOwnershipBatchError> {
    let index = SourceIndex::new(&request.source_records);
    let max_records = request
        .max_closure_records
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_MAX_CLOSURE_RECORDS);
    let mut selected_owners = Vec::new();
    let mut preserved_owners = Vec::new();
    let mut blocked_owners = Vec::new();
    let mut root_plans = Vec::new();
    let mut union = ClosureAccumulator::default();
    let mut effects = EffectAccumulator::default();
    let mut diagnostics = Vec::new();

    for root in &request.cleanup_roots {
        let owner_predicate = root_owner_predicate(root);
        let (outcome, reason, closure) =
            classify_cleanup_root(request, root, &index, max_records);
        let decision = AgentOwnershipOwnerDecisionWire {
            root_id: root.root_id.clone(),
            requested_name: root.requested_name.clone(),
            outcome: outcome.to_string(),
            reason: reason.clone(),
            owner_predicate: owner_predicate.clone(),
        };
        match outcome {
            CLEANUP_OUTCOME_SELECTED => {
                union.extend(&closure);
                effects.add_root_closure(&root.root_id, &closure);
                selected_owners.push(decision);
            }
            CLEANUP_OUTCOME_PRESERVED => preserved_owners.push(decision),
            _ => blocked_owners.push(decision),
        }
        if outcome == CLEANUP_OUTCOME_BLOCKED {
            diagnostics.push(format!(
                "cleanup root '{}' blocked: {reason}",
                root.root_id
            ));
        }
        root_plans.push(AgentCleanupRootPlanWire {
            root_id: root.root_id.clone(),
            requested_name: root.requested_name.clone(),
            outcome: outcome.to_string(),
            reason,
            closure,
            owner_predicate,
        });
    }

    Ok(CleanupPlanAccumulator {
        selected_owners,
        preserved_owners,
        blocked_owners,
        root_plans,
        union_closure: union.into_wire(),
        effects: effects.into_wire(),
        diagnostics,
    })
}

fn classify_cleanup_root(
    request: &AgentOwnershipBatchRequestWire,
    root: &AgentCleanupRootWire,
    index: &SourceIndex<'_>,
    max_records: usize,
) -> (&'static str, String, AgentOwnershipClosureWire) {
    let Some(expected_owner) = &root.expected_owner else {
        return (
            CLEANUP_OUTCOME_PRESERVED,
            "no_expected_owner".to_string(),
            AgentOwnershipClosureWire::default(),
        );
    };
    if !request.sources_complete {
        return (
            CLEANUP_OUTCOME_BLOCKED,
            "source_discovery_incomplete".to_string(),
            AgentOwnershipClosureWire::default(),
        );
    }
    if expected_owner
        .container_kind
        .as_deref()
        .is_some_and(|kind| !kind.is_empty())
        && !root.allow_container_cleanup
    {
        return (
            CLEANUP_OUTCOME_BLOCKED,
            "container_owner".to_string(),
            AgentOwnershipClosureWire::default(),
        );
    }
    if let Some(marker) = effective_marker_state(root, expected_owner) {
        if marker.cleanup_allowed
            || (marker.waiting && root.allow_waiting_cleanup)
            || (marker.live && root.allow_live_cleanup)
        {
            // Explicitly authorized by the caller's coherent marker view.
        } else if marker.live {
            return (
                CLEANUP_OUTCOME_PRESERVED,
                "live_owner".to_string(),
                AgentOwnershipClosureWire::default(),
            );
        }
    }

    match build_cleanup_closure(
        request,
        root,
        expected_owner,
        index,
        max_records,
    ) {
        Ok(closure) => (
            CLEANUP_OUTCOME_SELECTED,
            "cleanup_authorized".to_string(),
            closure,
        ),
        Err(reason) => (
            CLEANUP_OUTCOME_BLOCKED,
            reason,
            AgentOwnershipClosureWire::default(),
        ),
    }
}

fn effective_marker_state<'a>(
    root: &'a AgentCleanupRootWire,
    expected_owner: &'a AgentExpectedOwnerWire,
) -> Option<&'a super::wire::AgentMarkerStateWire> {
    root.expected_owner
        .as_ref()
        .and_then(|owner| owner.marker_state.as_ref())
        .or(expected_owner.marker_state.as_ref())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ClosureKey {
    Name(String),
    Suffix(String),
    ArtifactDir(String),
    BundlePath(String),
}

fn build_cleanup_closure(
    request: &AgentOwnershipBatchRequestWire,
    root: &AgentCleanupRootWire,
    expected_owner: &AgentExpectedOwnerWire,
    index: &SourceIndex<'_>,
    max_records: usize,
) -> Result<AgentOwnershipClosureWire, String> {
    let mut queue = VecDeque::new();
    let mut seen_keys = BTreeSet::new();
    let mut selected_records = BTreeSet::new();
    let mut closure = ClosureAccumulator::default();

    for name in root_seed_names(request, root, expected_owner) {
        enqueue(&mut queue, &mut seen_keys, ClosureKey::Name(name));
    }
    if let Some(raw_suffix) = non_empty(expected_owner.raw_suffix.as_deref()) {
        enqueue(
            &mut queue,
            &mut seen_keys,
            ClosureKey::Suffix(raw_suffix.to_string()),
        );
    }
    if let Some(path) = non_empty(expected_owner.artifacts_dir.as_deref()) {
        enqueue(
            &mut queue,
            &mut seen_keys,
            ClosureKey::ArtifactDir(path.to_string()),
        );
    }
    if let Some(path) = non_empty(expected_owner.bundle_path.as_deref()) {
        enqueue(
            &mut queue,
            &mut seen_keys,
            ClosureKey::BundlePath(path.to_string()),
        );
    }

    while let Some(key) = queue.pop_front() {
        let record_indexes = index.lookup(&key);
        for record_index in record_indexes {
            if !selected_records.insert(record_index) {
                continue;
            }
            if selected_records.len() > max_records {
                return Err("closure_limit_exceeded".to_string());
            }
            let record = &index.records[record_index];
            validate_effect_path(record)?;
            closure.add_record(record);
            for name in &record.canonical_names {
                if let Some(name) = non_empty(Some(name.as_str())) {
                    enqueue(
                        &mut queue,
                        &mut seen_keys,
                        ClosureKey::Name(name.to_string()),
                    );
                }
            }
            if let Some(raw_suffix) = non_empty(record.raw_suffix.as_deref()) {
                enqueue(
                    &mut queue,
                    &mut seen_keys,
                    ClosureKey::Suffix(raw_suffix.to_string()),
                );
            }
            for suffix in &record.outgoing_suffixes {
                if let Some(suffix) = non_empty(Some(suffix.as_str())) {
                    enqueue(
                        &mut queue,
                        &mut seen_keys,
                        ClosureKey::Suffix(suffix.to_string()),
                    );
                }
            }
        }
    }

    Ok(closure.into_wire())
}

fn root_seed_names(
    request: &AgentOwnershipBatchRequestWire,
    root: &AgentCleanupRootWire,
    expected_owner: &AgentExpectedOwnerWire,
) -> Vec<String> {
    let mut names = BTreeSet::new();
    for value in [
        Some(root.requested_name.as_str()),
        Some(expected_owner.name.as_str()),
        expected_owner.agent_name.as_deref(),
        expected_owner.workflow_name.as_deref(),
    ] {
        let Some(name) = non_empty(value) else {
            continue;
        };
        match current_owner_lookup_candidates(
            name,
            &request.owner,
            &request.known_owner_roots,
        ) {
            Ok(candidates) => {
                names.extend(candidates);
            }
            Err(_) => {
                names.insert(name.to_string());
            }
        }
    }
    names.into_iter().collect()
}

fn enqueue(
    queue: &mut VecDeque<ClosureKey>,
    seen: &mut BTreeSet<ClosureKey>,
    key: ClosureKey,
) {
    if seen.insert(key.clone()) {
        queue.push_back(key);
    }
}

fn validate_effect_path(
    record: &AgentOwnershipSourceRecordWire,
) -> Result<(), String> {
    match record.source_kind {
        AgentOwnershipSourceKindWire::Artifact => {
            if non_empty(record.artifact_dir.as_deref()).is_none() {
                return Err(format!(
                    "source_record_missing_artifact_dir:{}",
                    record.record_id
                ));
            }
        }
        AgentOwnershipSourceKindWire::DismissedBundle => {
            if non_empty(record.bundle_path.as_deref()).is_none() {
                return Err(format!(
                    "source_record_missing_bundle_path:{}",
                    record.record_id
                ));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct SourceIndex<'a> {
    records: &'a [AgentOwnershipSourceRecordWire],
    by_name: BTreeMap<String, BTreeSet<usize>>,
    by_suffix: BTreeMap<String, BTreeSet<usize>>,
    by_relation_ref: BTreeMap<String, BTreeSet<usize>>,
    by_artifact_dir: BTreeMap<String, BTreeSet<usize>>,
    by_bundle_path: BTreeMap<String, BTreeSet<usize>>,
}

impl<'a> SourceIndex<'a> {
    fn new(records: &'a [AgentOwnershipSourceRecordWire]) -> Self {
        let mut index = Self {
            records,
            ..Self::default()
        };
        for (idx, record) in records.iter().enumerate() {
            for name in &record.canonical_names {
                if let Some(name) = non_empty(Some(name.as_str())) {
                    index
                        .by_name
                        .entry(name.to_string())
                        .or_default()
                        .insert(idx);
                }
            }
            if let Some(suffix) = non_empty(record.raw_suffix.as_deref()) {
                index
                    .by_suffix
                    .entry(suffix.to_string())
                    .or_default()
                    .insert(idx);
            }
            for relation in &record.relation_refs {
                if let Some(relation) = non_empty(Some(relation.as_str())) {
                    index
                        .by_relation_ref
                        .entry(relation.to_string())
                        .or_default()
                        .insert(idx);
                }
            }
            if let Some(path) = non_empty(record.artifact_dir.as_deref()) {
                index
                    .by_artifact_dir
                    .entry(path.to_string())
                    .or_default()
                    .insert(idx);
            }
            if let Some(path) = non_empty(record.bundle_path.as_deref()) {
                index
                    .by_bundle_path
                    .entry(path.to_string())
                    .or_default()
                    .insert(idx);
            }
        }
        index
    }

    fn lookup(&self, key: &ClosureKey) -> BTreeSet<usize> {
        match key {
            ClosureKey::Name(name) => {
                self.by_name.get(name).cloned().unwrap_or_default()
            }
            ClosureKey::Suffix(suffix) => {
                let mut records =
                    self.by_suffix.get(suffix).cloned().unwrap_or_default();
                if let Some(incoming) = self.by_relation_ref.get(suffix) {
                    records.extend(incoming);
                }
                records
            }
            ClosureKey::ArtifactDir(path) => {
                self.by_artifact_dir.get(path).cloned().unwrap_or_default()
            }
            ClosureKey::BundlePath(path) => {
                self.by_bundle_path.get(path).cloned().unwrap_or_default()
            }
        }
    }
}

#[derive(Default)]
struct ClosureAccumulator {
    artifact_dirs: BTreeSet<String>,
    bundle_paths: BTreeSet<String>,
    names: BTreeSet<String>,
    suffixes: BTreeSet<String>,
    source_record_ids: BTreeSet<String>,
    effect_predicates: BTreeMap<String, AgentExpectedOwnerPredicateWire>,
}

impl ClosureAccumulator {
    fn add_record(&mut self, record: &AgentOwnershipSourceRecordWire) {
        self.source_record_ids.insert(record.record_id.clone());
        if let Some(path) = non_empty(record.artifact_dir.as_deref()) {
            self.artifact_dirs.insert(path.to_string());
        }
        if let Some(path) = non_empty(record.bundle_path.as_deref()) {
            self.bundle_paths.insert(path.to_string());
        }
        if let Some(suffix) = non_empty(record.raw_suffix.as_deref()) {
            self.suffixes.insert(suffix.to_string());
        }
        for suffix in &record.outgoing_suffixes {
            if let Some(suffix) = non_empty(Some(suffix.as_str())) {
                self.suffixes.insert(suffix.to_string());
            }
        }
        for name in &record.canonical_names {
            if let Some(name) = non_empty(Some(name.as_str())) {
                self.names.insert(name.to_string());
            }
        }
        let predicate = source_record_predicate(record);
        self.effect_predicates
            .entry(predicate.predicate_id.clone())
            .or_insert(predicate);
    }

    fn extend(&mut self, other: &AgentOwnershipClosureWire) {
        self.artifact_dirs
            .extend(other.artifact_dirs.iter().cloned());
        self.bundle_paths.extend(other.bundle_paths.iter().cloned());
        self.names.extend(other.names.iter().cloned());
        self.suffixes.extend(other.suffixes.iter().cloned());
        self.source_record_ids
            .extend(other.source_record_ids.iter().cloned());
        for predicate in &other.effect_predicates {
            self.effect_predicates
                .entry(predicate.predicate_id.clone())
                .or_insert_with(|| predicate.clone());
        }
    }

    fn into_wire(self) -> AgentOwnershipClosureWire {
        AgentOwnershipClosureWire {
            artifact_dirs: self.artifact_dirs.into_iter().collect(),
            bundle_paths: self.bundle_paths.into_iter().collect(),
            names: self.names.into_iter().collect(),
            suffixes: self.suffixes.into_iter().collect(),
            source_record_ids: self.source_record_ids.into_iter().collect(),
            effect_predicates: self.effect_predicates.into_values().collect(),
        }
    }
}

#[derive(Default)]
struct EffectAccumulator {
    effects: BTreeMap<(String, String), EffectParts>,
}

#[derive(Default)]
struct EffectParts {
    source_record_ids: BTreeSet<String>,
    target_root_ids: BTreeSet<String>,
    predicates: BTreeMap<String, AgentExpectedOwnerPredicateWire>,
}

impl EffectAccumulator {
    fn add_root_closure(
        &mut self,
        root_id: &str,
        closure: &AgentOwnershipClosureWire,
    ) {
        for path in &closure.artifact_dirs {
            self.add_effect(
                CLEANUP_EFFECT_ARTIFACT_DIR,
                path,
                root_id,
                closure,
            );
        }
        for path in &closure.bundle_paths {
            self.add_effect(CLEANUP_EFFECT_BUNDLE_PATH, path, root_id, closure);
        }
    }

    fn add_effect(
        &mut self,
        effect_kind: &str,
        path: &str,
        root_id: &str,
        closure: &AgentOwnershipClosureWire,
    ) {
        let parts = self
            .effects
            .entry((effect_kind.to_string(), path.to_string()))
            .or_default();
        parts.target_root_ids.insert(root_id.to_string());
        parts
            .source_record_ids
            .extend(closure.source_record_ids.iter().cloned());
        for predicate in &closure.effect_predicates {
            let path_matches = match effect_kind {
                CLEANUP_EFFECT_ARTIFACT_DIR => {
                    predicate.artifacts_dir.as_deref() == Some(path)
                }
                CLEANUP_EFFECT_BUNDLE_PATH => {
                    predicate.bundle_path.as_deref() == Some(path)
                }
                _ => false,
            };
            if path_matches {
                parts
                    .predicates
                    .insert(predicate.predicate_id.clone(), predicate.clone());
            }
        }
    }

    fn into_wire(self) -> Vec<AgentCleanupEffectWire> {
        self.effects
            .into_iter()
            .map(|((effect_kind, path), parts)| AgentCleanupEffectWire {
                effect_kind,
                path,
                source_record_ids: parts
                    .source_record_ids
                    .into_iter()
                    .collect(),
                target_root_ids: parts.target_root_ids.into_iter().collect(),
                expected_predicates: parts.predicates.into_values().collect(),
            })
            .collect()
    }
}

fn slot_owner_predicate(
    slot: &AgentOwnershipSlotWire,
) -> Option<AgentExpectedOwnerPredicateWire> {
    let owner = slot.expected_owner.as_ref()?;
    let mut predicate =
        expected_owner_predicate(format!("slot:{}", slot.slot_id), owner);
    predicate.slot_id = Some(slot.slot_id.clone());
    predicate.expected_bead_id = slot.expected_bead_id.clone();
    predicate.expected_assignee = slot.expected_assignee.clone();
    if predicate.marker_state.is_none() {
        predicate.marker_state = slot.marker_state.clone();
    }
    if predicate.process.is_none() {
        predicate.process = slot.process.clone();
    }
    if predicate.clan_generation.is_none() {
        predicate.clan_generation = slot.expected_clan_generation.clone();
    }
    if predicate.family_generation.is_none() {
        predicate.family_generation = slot.expected_family_generation.clone();
    }
    Some(predicate)
}

fn root_owner_predicate(
    root: &AgentCleanupRootWire,
) -> Option<AgentExpectedOwnerPredicateWire> {
    let owner = root.expected_owner.as_ref()?;
    let mut predicate =
        expected_owner_predicate(format!("root:{}", root.root_id), owner);
    predicate.root_id = Some(root.root_id.clone());
    predicate.expected_bead_id = root.expected_bead_id.clone();
    predicate.expected_assignee = root.expected_assignee.clone();
    Some(predicate)
}

fn expected_owner_predicate(
    predicate_id: String,
    owner: &AgentExpectedOwnerWire,
) -> AgentExpectedOwnerPredicateWire {
    AgentExpectedOwnerPredicateWire {
        predicate_id,
        name: owner.name.clone(),
        slot_id: None,
        root_id: None,
        request_id: None,
        source_record_id: None,
        expected_bead_id: None,
        expected_assignee: None,
        raw_suffix: owner.raw_suffix.clone(),
        artifacts_dir: owner.artifacts_dir.clone(),
        bundle_path: owner.bundle_path.clone(),
        source_signature: owner.source_signature.clone(),
        marker_state: owner.marker_state.clone(),
        process: owner.process.clone(),
        reservation_kind: owner.reservation_kind.clone(),
        container_kind: owner.container_kind.clone(),
        clan_generation: owner.clan_generation.clone(),
        family_generation: owner.family_generation.clone(),
        must_be_absent: false,
    }
}

fn source_record_predicate(
    record: &AgentOwnershipSourceRecordWire,
) -> AgentExpectedOwnerPredicateWire {
    let name = record
        .canonical_names
        .iter()
        .find_map(|name| non_empty(Some(name.as_str())))
        .unwrap_or(record.record_id.as_str())
        .to_string();
    AgentExpectedOwnerPredicateWire {
        predicate_id: format!("source:{}", record.record_id),
        name,
        slot_id: None,
        root_id: None,
        request_id: None,
        source_record_id: Some(record.record_id.clone()),
        expected_bead_id: None,
        expected_assignee: None,
        raw_suffix: record.raw_suffix.clone(),
        artifacts_dir: record.artifact_dir.clone(),
        bundle_path: record.bundle_path.clone(),
        source_signature: record.source_signature.clone(),
        marker_state: record.marker_state.clone(),
        process: record.process.clone(),
        reservation_kind: None,
        container_kind: None,
        clan_generation: None,
        family_generation: None,
        must_be_absent: false,
    }
}

#[derive(Default)]
struct ReservationPlanAccumulator {
    accepted: Vec<AgentNameReservationAcceptedWire>,
    blocked: Vec<AgentNameReservationBlockedWire>,
    merge_plan: Vec<AgentNameRegistryMergeWire>,
    cleanup_reservations: Vec<AgentCleanupReservationWire>,
}

fn plan_reservation_requests(
    request: &AgentOwnershipBatchRequestWire,
) -> Result<ReservationPlanAccumulator, AgentOwnershipBatchError> {
    let mut plan = ReservationPlanAccumulator::default();
    let mut entries: BTreeMap<String, AgentNameRegistryEntryWire> = request
        .reservation_snapshot
        .iter()
        .map(|entry| (entry.name.clone(), entry.clone()))
        .collect();
    let duplicate_request_ids = duplicate_reservation_request_ids(request);

    for reservation in &request.reservation_requests {
        if duplicate_request_ids.contains(&reservation.request_id) {
            plan.blocked.push(blocked_reservation(
                reservation,
                "duplicate_batch_name",
                Some(
                    "another reservation request in this batch targets the same current-owner name key"
                        .to_string(),
                ),
                None,
            ));
            continue;
        }
        match decide_reservation(request, reservation, &mut entries) {
            ReservationDecision::Accepted {
                accepted,
                merges,
                cleanup_reservation,
            } => {
                for merge in merges {
                    apply_merge_to_entries(&mut entries, &merge);
                    plan.merge_plan.push(merge);
                }
                if let Some(cleanup_reservation) = *cleanup_reservation {
                    plan.cleanup_reservations.push(cleanup_reservation);
                }
                plan.accepted.push(accepted);
            }
            ReservationDecision::Blocked(blocked) => {
                plan.blocked.push(*blocked);
            }
        }
    }

    Ok(plan)
}

fn duplicate_reservation_request_ids(
    request: &AgentOwnershipBatchRequestWire,
) -> BTreeSet<String> {
    let mut owners_by_key: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for reservation in &request.reservation_requests {
        if !reservation.operation.writes_registry() {
            continue;
        }
        let mut names = vec![reservation.name.as_str()];
        if reservation.operation
            == AgentNameReservationOperationWire::ConvertFamily
        {
            if let Some(member_name) = reservation.member_name.as_deref() {
                names.push(member_name);
            }
        }
        for name in names {
            let key = current_owner_name_key(
                name,
                &request.owner,
                &request.known_owner_roots,
            )
            .unwrap_or_else(|_| name.to_string());
            owners_by_key
                .entry(key)
                .or_default()
                .insert(reservation.request_id.clone());
        }
    }
    owners_by_key
        .into_values()
        .filter(|request_ids| request_ids.len() > 1)
        .flatten()
        .collect()
}

enum ReservationDecision {
    Accepted {
        accepted: AgentNameReservationAcceptedWire,
        merges: Vec<AgentNameRegistryMergeWire>,
        cleanup_reservation: Box<Option<AgentCleanupReservationWire>>,
    },
    Blocked(Box<AgentNameReservationBlockedWire>),
}

impl ReservationDecision {
    fn blocked(blocked: AgentNameReservationBlockedWire) -> Self {
        Self::Blocked(Box::new(blocked))
    }
}

fn decide_reservation(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &mut BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    match request.operation {
        AgentNameReservationOperationWire::ReservePlanned => {
            decide_agent_upsert(
                batch,
                request,
                entries,
                RESERVATION_KIND_PLANNED,
            )
        }
        AgentNameReservationOperationWire::ClaimPlanned => decide_agent_upsert(
            batch,
            request,
            entries,
            RESERVATION_KIND_CLAIMED,
        ),
        AgentNameReservationOperationWire::ReserveClan => {
            decide_reserve_clan(batch, request, entries)
        }
        AgentNameReservationOperationWire::ClaimClan => {
            decide_claim_clan(batch, request, entries)
        }
        AgentNameReservationOperationWire::ConvertFamily => {
            decide_convert_family(batch, request, entries)
        }
        AgentNameReservationOperationWire::ReserveTemplate => {
            decide_reserve_template(batch, request, entries)
        }
        AgentNameReservationOperationWire::ReleasePlanned => {
            decide_release_planned(
                batch,
                request,
                entries,
                RESERVATION_KIND_PLANNED,
                None,
            )
        }
        AgentNameReservationOperationWire::ReleasePlannedClan => {
            decide_release_planned(
                batch,
                request,
                entries,
                RESERVATION_KIND_PLANNED_CLAN,
                request.clan_generation.as_deref(),
            )
        }
        AgentNameReservationOperationWire::CleanupGuard => {
            decide_cleanup_guard(batch, request, entries)
        }
    }
}

fn decide_agent_upsert(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    reservation_kind: &str,
) -> ReservationDecision {
    let normalized =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    if let Some(blocked) =
        local_namespace_block(batch, request, entries, &normalized)
    {
        return ReservationDecision::blocked(blocked);
    }
    let (storage_name, existing) = equivalent_entry(
        entries,
        &normalized,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        if existing
            .container_kind
            .as_deref()
            .is_some_and(|kind| !kind.is_empty())
        {
            return ReservationDecision::blocked(blocked_reservation(
                request,
                "container_collision",
                Some(format!(
                    "name is reserved by a {} container",
                    existing.container_kind.as_deref().unwrap_or("unknown")
                )),
                Some(storage_name),
            ));
        }
        if entry_has_other_owner(existing, &request.artifact_dir)
            && (!request.replace_existing || storage_name != normalized)
        {
            return ReservationDecision::blocked(name_collision_block(
                batch,
                request,
                entries,
                &normalized,
                &storage_name,
            ));
        }
    }
    let entry =
        local_artifact_entry(batch, request, &storage_name, reservation_kind);
    let expected =
        registry_expected_predicate(request, &storage_name, existing);
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &normalized,
            &storage_name,
            "registry_entry_upsert",
            None,
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
            name: storage_name,
            entry: Some(entry),
            expected,
        }],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_reserve_clan(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    let Some(generation) = non_empty(request.clan_generation.as_deref()) else {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "missing_clan_generation",
            None,
            None,
        ));
    };
    let normalized =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    if let Some(blocked) =
        local_namespace_block(batch, request, entries, &normalized)
    {
        return ReservationDecision::blocked(blocked);
    }
    let (storage_name, existing) = equivalent_entry(
        entries,
        &normalized,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        if existing.container_kind.as_deref() == Some(CONTAINER_KIND_CLAN) {
            if request.create_only {
                return ReservationDecision::blocked(blocked_reservation(
                    request,
                    "clan_already_exists",
                    None,
                    Some(storage_name),
                ));
            }
            let existing_generation = existing
                .clan_generation
                .as_deref()
                .filter(|value| !value.is_empty())
                .unwrap_or(generation);
            return ReservationDecision::Accepted {
                accepted: accepted_reservation(
                    request,
                    REGISTRY_MERGE_ACTION_NO_OP,
                    &normalized,
                    &storage_name,
                    "existing_clan_generation",
                    Some(existing_generation.to_string()),
                ),
                merges: vec![no_op_merge(
                    request,
                    &storage_name,
                    Some(existing),
                )],
                cleanup_reservation: Box::new(None),
            };
        }
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "clan_name_reserved_by_agent",
            None,
            Some(storage_name),
        ));
    }

    let mut entry = local_artifact_entry(
        batch,
        request,
        &storage_name,
        RESERVATION_KIND_PLANNED_CLAN,
    );
    entry.container_kind = Some(CONTAINER_KIND_CLAN.to_string());
    entry.clan_generation = Some(generation.to_string());
    let expected = registry_expected_predicate(request, &storage_name, None);
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &normalized,
            &storage_name,
            "planned_clan_insert",
            Some(generation.to_string()),
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
            name: storage_name,
            entry: Some(entry),
            expected,
        }],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_claim_clan(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    let Some(generation) = non_empty(request.clan_generation.as_deref()) else {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "missing_clan_generation",
            None,
            None,
        ));
    };
    let normalized =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    if let Some(blocked) =
        local_namespace_block(batch, request, entries, &normalized)
    {
        return ReservationDecision::blocked(blocked);
    }
    let (storage_name, existing) = equivalent_entry(
        entries,
        &normalized,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        if existing.container_kind.as_deref() != Some(CONTAINER_KIND_CLAN) {
            return ReservationDecision::blocked(blocked_reservation(
                request,
                "clan_name_reserved_by_agent",
                None,
                Some(storage_name),
            ));
        }
        if let Some(existing_generation) =
            non_empty(existing.clan_generation.as_deref())
        {
            if existing_generation != generation {
                return ReservationDecision::blocked(blocked_reservation(
                    request,
                    "clan_generation_mismatch",
                    Some(format!(
                        "snapshot generation is '{existing_generation}', request expected '{generation}'"
                    )),
                    Some(storage_name),
                ));
            }
        }
        if existing.reservation_kind.as_deref()
            != Some(RESERVATION_KIND_PLANNED_CLAN)
            && !entry_belongs_to_artifact(existing, &request.artifact_dir)
        {
            return ReservationDecision::Accepted {
                accepted: accepted_reservation(
                    request,
                    REGISTRY_MERGE_ACTION_NO_OP,
                    &normalized,
                    &storage_name,
                    "existing_clan_owned_elsewhere",
                    Some(generation.to_string()),
                ),
                merges: vec![no_op_merge(
                    request,
                    &storage_name,
                    Some(existing),
                )],
                cleanup_reservation: Box::new(None),
            };
        }
    }
    let mut entry = local_artifact_entry(
        batch,
        request,
        &storage_name,
        RESERVATION_KIND_CLAN,
    );
    entry.container_kind = Some(CONTAINER_KIND_CLAN.to_string());
    entry.clan_generation = Some(generation.to_string());
    let expected =
        registry_expected_predicate(request, &storage_name, existing);
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &normalized,
            &storage_name,
            "clan_claim_upsert",
            Some(generation.to_string()),
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
            name: storage_name,
            entry: Some(entry),
            expected,
        }],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_convert_family(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    let Some(member_name) = non_empty(request.member_name.as_deref()) else {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "missing_family_member_name",
            None,
            None,
        ));
    };
    let family_name =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    let member_name = match normalize_request_name(
        batch,
        request,
        member_name,
        "member_name",
    ) {
        Ok(name) => name,
        Err(blocked) => return ReservationDecision::blocked(*blocked),
    };
    for name in [&family_name, &member_name] {
        if let Some(blocked) =
            local_namespace_block(batch, request, entries, name)
        {
            return ReservationDecision::blocked(blocked);
        }
    }

    let (family_storage_name, existing) = equivalent_entry(
        entries,
        &family_name,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        match existing.container_kind.as_deref() {
            Some(CONTAINER_KIND_CLAN) => {
                return ReservationDecision::blocked(blocked_reservation(
                    request,
                    "container_collision",
                    Some("name is reserved by a clan container".to_string()),
                    Some(family_storage_name),
                ));
            }
            Some(CONTAINER_KIND_FAMILY) | None => {}
            Some(_) => {
                return ReservationDecision::blocked(blocked_reservation(
                    request,
                    "container_collision",
                    None,
                    Some(family_storage_name),
                ));
            }
        }
        if existing.container_kind.is_none()
            && entry_has_other_claim_owner(existing, &request.artifact_dir)
        {
            return ReservationDecision::blocked(name_collision_block(
                batch,
                request,
                entries,
                &family_name,
                &family_storage_name,
            ));
        }
    }

    let (member_storage_name, member_existing) = equivalent_entry(
        entries,
        &member_name,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(member_existing) = member_existing {
        if entry_has_other_claim_owner(member_existing, &request.artifact_dir) {
            return ReservationDecision::blocked(name_collision_block(
                batch,
                request,
                entries,
                &member_name,
                &member_storage_name,
            ));
        }
    }

    let mut family_entry = local_artifact_entry(
        batch,
        request,
        &family_storage_name,
        RESERVATION_KIND_FAMILY,
    );
    family_entry.container_kind = Some(CONTAINER_KIND_FAMILY.to_string());
    let family_expected =
        registry_expected_predicate(request, &family_storage_name, existing);
    let member_entry = local_artifact_entry(
        batch,
        request,
        &member_storage_name,
        RESERVATION_KIND_CLAIMED,
    );
    let member_expected = registry_expected_predicate(
        request,
        &member_storage_name,
        member_existing,
    );
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &family_name,
            &family_storage_name,
            "family_conversion_upsert",
            None,
        ),
        merges: vec![
            AgentNameRegistryMergeWire {
                request_id: request.request_id.clone(),
                action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
                name: family_storage_name,
                entry: Some(family_entry),
                expected: family_expected,
            },
            AgentNameRegistryMergeWire {
                request_id: request.request_id.clone(),
                action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
                name: member_storage_name,
                entry: Some(member_entry),
                expected: member_expected,
            },
        ],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_reserve_template(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    let Some(namespace) = non_empty(request.namespace.as_deref()) else {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "missing_template_namespace",
            None,
            None,
        ));
    };
    let name =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    let namespace =
        match normalize_request_name(batch, request, namespace, "namespace") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    for value in [&name, &namespace] {
        if let Some(blocked) =
            local_namespace_block(batch, request, entries, value)
        {
            return ReservationDecision::blocked(blocked);
        }
    }
    let allowed = request
        .allowed_existing_names
        .iter()
        .filter_map(|name| {
            current_owner_name_key(name, &batch.owner, &batch.known_owner_roots)
                .ok()
        })
        .collect::<BTreeSet<_>>();
    let occupied_namespaces = occupied_template_namespaces(
        entries,
        &allowed,
        &batch.owner,
        &batch.known_owner_roots,
    );
    let namespace_key = match current_owner_name_key(
        &namespace,
        &batch.owner,
        &batch.known_owner_roots,
    ) {
        Ok(key) => key,
        Err(error) => {
            return ReservationDecision::blocked(blocked_reservation(
                request,
                "invalid_namespace",
                Some(error.to_string()),
                None,
            ));
        }
    };
    if occupied_namespaces.contains(&namespace_key) {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "template_namespace_occupied",
            None,
            Some(namespace),
        ));
    }
    let (storage_name, existing) = equivalent_entry(
        entries,
        &name,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        if entry_has_other_owner(existing, &request.artifact_dir) {
            return ReservationDecision::blocked(name_collision_block(
                batch,
                request,
                entries,
                &name,
                &storage_name,
            ));
        }
    }
    let mut entry = local_artifact_entry(
        batch,
        request,
        &storage_name,
        RESERVATION_KIND_PLANNED,
    );
    entry.template_namespace = Some(namespace);
    let expected =
        registry_expected_predicate(request, &storage_name, existing);
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &name,
            &storage_name,
            "template_reservation_upsert",
            None,
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
            name: storage_name,
            entry: Some(entry),
            expected,
        }],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_release_planned(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    expected_reservation_kind: &str,
    expected_generation: Option<&str>,
) -> ReservationDecision {
    let normalized =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    let (storage_name, existing) = equivalent_entry(
        entries,
        &normalized,
        &batch.owner,
        &batch.known_owner_roots,
    );
    let Some(existing) = existing else {
        return ReservationDecision::Accepted {
            accepted: accepted_reservation(
                request,
                REGISTRY_MERGE_ACTION_NO_OP,
                &normalized,
                &storage_name,
                "planned_reservation_absent",
                None,
            ),
            merges: vec![no_op_merge(request, &storage_name, None)],
            cleanup_reservation: Box::new(None),
        };
    };
    if existing.reservation_kind.as_deref() != Some(expected_reservation_kind)
        || !entry_belongs_to_artifact(existing, &request.artifact_dir)
    {
        return ReservationDecision::Accepted {
            accepted: accepted_reservation(
                request,
                REGISTRY_MERGE_ACTION_NO_OP,
                &normalized,
                &storage_name,
                "planned_reservation_not_owned_by_request",
                existing.clan_generation.clone(),
            ),
            merges: vec![no_op_merge(request, &storage_name, Some(existing))],
            cleanup_reservation: Box::new(None),
        };
    }
    if let Some(expected_generation) = expected_generation {
        if existing.clan_generation.as_deref() != Some(expected_generation) {
            return ReservationDecision::Accepted {
                accepted: accepted_reservation(
                    request,
                    REGISTRY_MERGE_ACTION_NO_OP,
                    &normalized,
                    &storage_name,
                    "planned_clan_generation_mismatch",
                    existing.clan_generation.clone(),
                ),
                merges: vec![no_op_merge(
                    request,
                    &storage_name,
                    Some(existing),
                )],
                cleanup_reservation: Box::new(None),
            };
        }
    }
    let expected =
        registry_expected_predicate(request, &storage_name, Some(existing));
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_REMOVE,
            &normalized,
            &storage_name,
            "planned_reservation_release",
            existing.clan_generation.clone(),
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_REMOVE.to_string(),
            name: storage_name,
            entry: None,
            expected,
        }],
        cleanup_reservation: Box::new(None),
    }
}

fn decide_cleanup_guard(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
) -> ReservationDecision {
    let Some(cleanup_token) = non_empty(request.cleanup_token.as_deref())
    else {
        return ReservationDecision::blocked(blocked_reservation(
            request,
            "missing_cleanup_token",
            None,
            None,
        ));
    };
    let normalized =
        match normalize_request_name(batch, request, &request.name, "name") {
            Ok(name) => name,
            Err(blocked) => return ReservationDecision::blocked(*blocked),
        };
    let (storage_name, existing) = equivalent_entry(
        entries,
        &normalized,
        &batch.owner,
        &batch.known_owner_roots,
    );
    if let Some(existing) = existing {
        if entry_has_other_owner(existing, &request.artifact_dir) {
            return ReservationDecision::blocked(blocked_reservation(
                request,
                "cleanup_owner_mismatch",
                Some(
                    "snapshot owner does not match cleanup guard owner"
                        .to_string(),
                ),
                Some(storage_name),
            ));
        }
    }
    let mut entry = local_artifact_entry(
        batch,
        request,
        &storage_name,
        RESERVATION_KIND_CLEANUP_IN_PROGRESS,
    );
    entry.cleanup_token = Some(cleanup_token.to_string());
    entry.cleanup_operation = Some(request.request_id.clone());
    let expected =
        registry_expected_predicate(request, &storage_name, existing);
    let cleanup_reservation = AgentCleanupReservationWire {
        request_id: request.request_id.clone(),
        name: normalized.clone(),
        storage_name: storage_name.clone(),
        cleanup_token: cleanup_token.to_string(),
        expected_owner: expected.clone(),
    };
    ReservationDecision::Accepted {
        accepted: accepted_reservation(
            request,
            REGISTRY_MERGE_ACTION_UPSERT,
            &normalized,
            &storage_name,
            "cleanup_guard_upsert",
            None,
        ),
        merges: vec![AgentNameRegistryMergeWire {
            request_id: request.request_id.clone(),
            action: REGISTRY_MERGE_ACTION_UPSERT.to_string(),
            name: storage_name,
            entry: Some(entry),
            expected,
        }],
        cleanup_reservation: Box::new(Some(cleanup_reservation)),
    }
}

fn normalize_request_name(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    name: &str,
    field: &str,
) -> Result<String, Box<AgentNameReservationBlockedWire>> {
    normalize_owned_agent_name(name, &batch.owner, &batch.known_owner_roots)
        .map_err(|error| {
            let (reason, conflicting_name) = match &error {
                AgentIdentityError::ForeignOwnerRoot { owner_root, .. } => (
                    "reserved_owner_namespace".to_string(),
                    Some(owner_root.clone()),
                ),
                _ => (format!("invalid_{field}"), None),
            };
            Box::new(blocked_reservation(
                request,
                &reason,
                Some(error.to_string()),
                conflicting_name,
            ))
        })
}

fn local_namespace_block(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    name: &str,
) -> Option<AgentNameReservationBlockedWire> {
    for prefix in dotted_namespace_prefixes(name) {
        let Some(entry) = entries.get(&prefix) else {
            continue;
        };
        let blocks = entry.container_kind.as_deref()
            == Some(CONTAINER_KIND_OWNER_NAMESPACE)
            || entry.origin.as_deref() == Some(ORIGIN_IMPORT_V1)
            || entry.origin.as_deref() == Some(ORIGIN_IMPORT_V2);
        if blocks {
            return Some(blocked_reservation(
                request,
                "reserved_owner_namespace",
                Some(format!(
                    "agent name '{}' is inside reserved owner namespace '{}'",
                    present_agent_name(
                        name,
                        &batch.owner,
                        &batch.known_owner_roots
                    ),
                    prefix
                )),
                Some(prefix),
            ));
        }
    }
    None
}

fn occupied_template_namespaces(
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    allowed_existing_names: &BTreeSet<String>,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> BTreeSet<String> {
    let mut occupied = BTreeSet::new();
    for (name, entry) in entries {
        if entry.container_kind.as_deref() == Some(CONTAINER_KIND_CLAN) {
            continue;
        }
        let Ok(key) = current_owner_name_key(name, owner, known_owner_roots)
        else {
            continue;
        };
        if allowed_existing_names.contains(&key) {
            continue;
        }
        occupied.extend(dotted_namespace_prefixes(&key));
    }
    occupied
}

fn equivalent_entry<'a>(
    entries: &'a BTreeMap<String, AgentNameRegistryEntryWire>,
    durable_name: &str,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> (String, Option<&'a AgentNameRegistryEntryWire>) {
    let candidates =
        current_owner_lookup_candidates(durable_name, owner, known_owner_roots)
            .unwrap_or_else(|_| vec![durable_name.to_string()]);
    for candidate in candidates {
        if let Some(entry) = entries.get(&candidate) {
            return (candidate, Some(entry));
        }
    }
    (durable_name.to_string(), None)
}

fn current_owner_name_key(
    name: &str,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> Result<String, AgentIdentityError> {
    if foreign_agent_owner_root(name, owner, known_owner_roots)?.is_some() {
        return Ok(name.to_string());
    }
    normalize_owned_agent_name(name, owner, known_owner_roots)
}

fn current_owner_lookup_candidates(
    name: &str,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> Result<Vec<String>, AgentIdentityError> {
    if foreign_agent_owner_root(name, owner, known_owner_roots)?.is_some() {
        return Ok(vec![name.to_string()]);
    }
    let (archive_prefix, core_name) = split_agent_archive_prefix(name);
    let bare = normalize_owned_agent_name(core_name, owner, known_owner_roots)?;
    let global = globalize_agent_name(&bare, owner)?;
    let candidates = [
        name.to_string(),
        format!("{archive_prefix}{bare}"),
        format!("{archive_prefix}{}.{bare}", owner.machine_name),
        format!("{archive_prefix}{global}"),
    ];
    Ok(unique(candidates))
}

fn split_agent_archive_prefix(name: &str) -> (&str, &str) {
    let bytes = name.as_bytes();
    if bytes.len() > 7
        && bytes[0..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == b'.'
    {
        return name.split_at(7);
    }
    ("", name)
}

fn local_artifact_entry(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    storage_name: &str,
    reservation_kind: &str,
) -> AgentNameRegistryEntryWire {
    let (project_name, workflow_dir, raw_suffix) =
        artifact_path_parts(&request.artifact_dir);
    AgentNameRegistryEntryWire {
        name: storage_name.to_string(),
        source: Some("artifact".to_string()),
        origin: Some("local".to_string()),
        canonical_global_name: globalize_agent_name(storage_name, &batch.owner)
            .ok(),
        source_owner: Some(batch.owner.clone()),
        legacy_source_machine: None,
        imported_digest: None,
        project_name,
        workflow_dir,
        raw_suffix,
        artifacts_dir: Some(request.artifact_dir.clone()),
        bundle_path: None,
        state: Some(
            match reservation_kind {
                RESERVATION_KIND_PLANNED | RESERVATION_KIND_PLANNED_CLAN => {
                    "planned"
                }
                _ => "active",
            }
            .to_string(),
        ),
        reservation_kind: Some(reservation_kind.to_string()),
        container_kind: None,
        clan_generation: request.clan_generation.clone(),
        template_namespace: request.namespace.clone(),
        reserved_at: request.reserved_at.clone(),
        cleanup_token: None,
        cleanup_operation: None,
        collision_owners: Vec::new(),
        extra: BTreeMap::new(),
    }
}

fn artifact_path_parts(
    artifact_dir: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let normalized = artifact_dir.replace('\\', "/");
    let parts: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let raw_suffix = parts.last().map(|part| (*part).to_string());
    let workflow_dir = parts
        .len()
        .checked_sub(2)
        .and_then(|idx| parts.get(idx))
        .map(|part| (*part).to_string());
    let project_name = parts
        .windows(2)
        .position(|window| window == ["projects", "artifacts"])
        .and_then(|idx| idx.checked_sub(1))
        .and_then(|idx| parts.get(idx))
        .map(|part| (*part).to_string())
        .or_else(|| {
            parts
                .windows(2)
                .position(|window| window[1] == "artifacts")
                .and_then(|idx| parts.get(idx))
                .map(|part| (*part).to_string())
        });
    (project_name, workflow_dir, raw_suffix)
}

fn entry_belongs_to_artifact(
    entry: &AgentNameRegistryEntryWire,
    artifact_dir: &str,
) -> bool {
    entry.artifacts_dir.as_deref() == Some(artifact_dir)
}

fn entry_has_other_owner(
    entry: &AgentNameRegistryEntryWire,
    artifact_dir: &str,
) -> bool {
    if !entry_belongs_to_artifact(entry, artifact_dir) {
        return true;
    }
    entry
        .collision_owners
        .iter()
        .any(|owner| !entry_belongs_to_artifact(owner, artifact_dir))
}

fn entry_has_other_claim_owner(
    entry: &AgentNameRegistryEntryWire,
    artifact_dir: &str,
) -> bool {
    let primary_blocks = entry.reservation_kind.as_deref()
        != Some("auto_prefix")
        && !entry_belongs_to_artifact(entry, artifact_dir);
    primary_blocks
        || entry.collision_owners.iter().any(|owner| {
            owner.reservation_kind.as_deref() != Some("auto_prefix")
                && !entry_belongs_to_artifact(owner, artifact_dir)
        })
}

fn registry_expected_predicate(
    request: &AgentNameReservationRequestWire,
    storage_name: &str,
    existing: Option<&AgentNameRegistryEntryWire>,
) -> AgentExpectedOwnerPredicateWire {
    let mut predicate = AgentExpectedOwnerPredicateWire {
        predicate_id: format!("reservation:{}", request.request_id),
        name: storage_name.to_string(),
        slot_id: None,
        root_id: None,
        request_id: Some(request.request_id.clone()),
        source_record_id: None,
        expected_bead_id: None,
        expected_assignee: None,
        raw_suffix: None,
        artifacts_dir: None,
        bundle_path: None,
        source_signature: request
            .expected_owner
            .as_ref()
            .and_then(|owner| owner.source_signature.clone()),
        marker_state: request
            .expected_owner
            .as_ref()
            .and_then(|owner| owner.marker_state.clone()),
        process: request
            .expected_owner
            .as_ref()
            .and_then(|owner| owner.process.clone()),
        reservation_kind: None,
        container_kind: None,
        clan_generation: None,
        family_generation: None,
        must_be_absent: existing.is_none(),
    };
    if let Some(existing) = existing {
        predicate.raw_suffix = existing.raw_suffix.clone();
        predicate.artifacts_dir = existing.artifacts_dir.clone();
        predicate.bundle_path = existing.bundle_path.clone();
        predicate.reservation_kind = existing.reservation_kind.clone();
        predicate.container_kind = existing.container_kind.clone();
        predicate.clan_generation = existing.clan_generation.clone();
    }
    predicate
}

fn no_op_merge(
    request: &AgentNameReservationRequestWire,
    storage_name: &str,
    existing: Option<&AgentNameRegistryEntryWire>,
) -> AgentNameRegistryMergeWire {
    AgentNameRegistryMergeWire {
        request_id: request.request_id.clone(),
        action: REGISTRY_MERGE_ACTION_NO_OP.to_string(),
        name: storage_name.to_string(),
        entry: existing.cloned(),
        expected: registry_expected_predicate(request, storage_name, existing),
    }
}

fn accepted_reservation(
    request: &AgentNameReservationRequestWire,
    action: &str,
    name: &str,
    storage_name: &str,
    reason: &str,
    clan_generation: Option<String>,
) -> AgentNameReservationAcceptedWire {
    AgentNameReservationAcceptedWire {
        request_id: request.request_id.clone(),
        operation: request.operation,
        action: action.to_string(),
        name: name.to_string(),
        storage_name: storage_name.to_string(),
        reason: reason.to_string(),
        clan_generation,
    }
}

fn blocked_reservation(
    request: &AgentNameReservationRequestWire,
    reason: &str,
    detail: Option<String>,
    conflicting_name: Option<String>,
) -> AgentNameReservationBlockedWire {
    AgentNameReservationBlockedWire {
        request_id: request.request_id.clone(),
        operation: request.operation,
        name: request.name.clone(),
        reason: reason.to_string(),
        detail,
        conflicting_name,
    }
}

fn name_collision_block(
    batch: &AgentOwnershipBatchRequestWire,
    request: &AgentNameReservationRequestWire,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    normalized: &str,
    storage_name: &str,
) -> AgentNameReservationBlockedWire {
    let suggestion = lowest_name_suggestion(
        &present_agent_name(normalized, &batch.owner, &batch.known_owner_roots),
        entries,
        &batch.owner,
        &batch.known_owner_roots,
    );
    blocked_reservation(
        request,
        "name_collision",
        Some(format!(
            "agent name '{}' is already taken; try '{}'",
            present_agent_name(
                normalized,
                &batch.owner,
                &batch.known_owner_roots
            ),
            suggestion
        )),
        Some(storage_name.to_string()),
    )
}

fn lowest_name_suggestion(
    base: &str,
    entries: &BTreeMap<String, AgentNameRegistryEntryWire>,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> String {
    let reserved = entries
        .keys()
        .filter_map(|name| {
            current_owner_name_key(name, owner, known_owner_roots).ok()
        })
        .collect::<BTreeSet<_>>();
    for n in 1.. {
        let candidate = format!("{base}{n}");
        let Ok(key) =
            current_owner_name_key(&candidate, owner, known_owner_roots)
        else {
            return candidate;
        };
        if !reserved.contains(&key) {
            return candidate;
        }
    }
    unreachable!("unbounded positive integer search always returns")
}

fn apply_merge_to_entries(
    entries: &mut BTreeMap<String, AgentNameRegistryEntryWire>,
    merge: &AgentNameRegistryMergeWire,
) {
    match merge.action.as_str() {
        REGISTRY_MERGE_ACTION_UPSERT => {
            if let Some(entry) = &merge.entry {
                entries.insert(merge.name.clone(), entry.clone());
            }
        }
        REGISTRY_MERGE_ACTION_REMOVE => {
            entries.remove(&merge.name);
        }
        _ => {}
    }
}

fn present_agent_name(
    name: &str,
    owner: &AgentOwnerIdentity,
    known_owner_roots: &[String],
) -> String {
    if foreign_agent_owner_root(name, owner, known_owner_roots)
        .ok()
        .flatten()
        .is_some()
    {
        name.to_string()
    } else {
        normalize_owned_agent_name(name, owner, known_owner_roots)
            .unwrap_or_else(|_| name.to_string())
    }
}

fn dotted_namespace_prefixes(name: &str) -> Vec<String> {
    let parts: Vec<&str> = name.split('.').collect();
    (1..=parts.len())
        .map(|idx| parts[..idx].join("."))
        .collect()
}

fn unique<const N: usize>(values: [String; N]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_ownership::wire::{
        AgentMarkerStateWire, AgentNameReservationOperationWire,
        AgentProcessIdentityWire, AgentSourceSignatureWire,
    };

    fn owner() -> AgentOwnerIdentity {
        AgentOwnerIdentity::new("alice", "athena").unwrap()
    }

    fn request() -> AgentOwnershipBatchRequestWire {
        AgentOwnershipBatchRequestWire {
            schema_version: AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION,
            owner: owner(),
            known_owner_roots: vec![
                "athena".to_string(),
                "alice.athena".to_string(),
                "zeus".to_string(),
            ],
            logical_slots: Vec::new(),
            cleanup_roots: Vec::new(),
            source_records: Vec::new(),
            sources_complete: true,
            max_closure_records: None,
            reservation_snapshot: Vec::new(),
            reservation_requests: Vec::new(),
        }
    }

    fn expected_owner(
        name: &str,
        suffix: &str,
        path: &str,
    ) -> AgentExpectedOwnerWire {
        AgentExpectedOwnerWire {
            name: name.to_string(),
            agent_name: Some(name.to_string()),
            workflow_name: Some(format!("workflow-{name}")),
            raw_suffix: Some(suffix.to_string()),
            artifacts_dir: Some(path.to_string()),
            bundle_path: None,
            source_signature: Some(AgentSourceSignatureWire {
                source_id: Some(format!("sig-{suffix}")),
                path: Some(path.to_string()),
                mtime_ns: Some(10),
                size: Some(20),
                digest: None,
            }),
            container_kind: None,
            clan_generation: None,
            family_generation: None,
            reservation_kind: Some("claimed".to_string()),
            marker_state: Some(AgentMarkerStateWire {
                status: "DONE".to_string(),
                marker_kind: Some("done".to_string()),
                live: false,
                terminal: true,
                waiting: false,
                cleanup_allowed: true,
            }),
            process: Some(AgentProcessIdentityWire {
                pid: Some(123),
                process_group_id: Some(123),
                start_time: Some("2026-09-06T00:00:00Z".to_string()),
                host: Some("athena".to_string()),
                token: Some(format!("proc-{suffix}")),
            }),
        }
    }

    fn artifact(
        id: &str,
        name: &str,
        suffix: &str,
        path: &str,
    ) -> AgentOwnershipSourceRecordWire {
        AgentOwnershipSourceRecordWire {
            record_id: id.to_string(),
            source_kind: AgentOwnershipSourceKindWire::Artifact,
            artifact_dir: Some(path.to_string()),
            bundle_path: None,
            project_name: Some("proj".to_string()),
            workflow_name: Some("workflow".to_string()),
            raw_suffix: Some(suffix.to_string()),
            canonical_names: vec![name.to_string()],
            relation_refs: Vec::new(),
            outgoing_suffixes: Vec::new(),
            source_signature: None,
            marker_state: None,
            process: None,
        }
    }

    fn bundle(
        id: &str,
        name: &str,
        suffix: &str,
        path: &str,
    ) -> AgentOwnershipSourceRecordWire {
        AgentOwnershipSourceRecordWire {
            record_id: id.to_string(),
            source_kind: AgentOwnershipSourceKindWire::DismissedBundle,
            artifact_dir: None,
            bundle_path: Some(path.to_string()),
            project_name: None,
            workflow_name: None,
            raw_suffix: Some(suffix.to_string()),
            canonical_names: vec![name.to_string()],
            relation_refs: Vec::new(),
            outgoing_suffixes: Vec::new(),
            source_signature: None,
            marker_state: None,
            process: None,
        }
    }

    fn registry_entry(
        name: &str,
        artifact_dir: &str,
        reservation_kind: &str,
    ) -> AgentNameRegistryEntryWire {
        AgentNameRegistryEntryWire {
            name: name.to_string(),
            source: Some("artifact".to_string()),
            origin: Some("local".to_string()),
            canonical_global_name: Some(format!("alice.athena.{name}")),
            source_owner: Some(owner()),
            legacy_source_machine: None,
            imported_digest: None,
            project_name: Some("proj".to_string()),
            workflow_dir: Some("workflow".to_string()),
            raw_suffix: Some("20260906".to_string()),
            artifacts_dir: Some(artifact_dir.to_string()),
            bundle_path: None,
            state: Some("planned".to_string()),
            reservation_kind: Some(reservation_kind.to_string()),
            container_kind: None,
            clan_generation: None,
            template_namespace: None,
            reserved_at: None,
            cleanup_token: None,
            cleanup_operation: None,
            collision_owners: Vec::new(),
            extra: BTreeMap::new(),
        }
    }

    fn reservation(
        id: &str,
        operation: AgentNameReservationOperationWire,
        name: &str,
        artifact_dir: &str,
    ) -> AgentNameReservationRequestWire {
        AgentNameReservationRequestWire {
            request_id: id.to_string(),
            operation,
            name: name.to_string(),
            artifact_dir: artifact_dir.to_string(),
            namespace: None,
            member_name: None,
            clan_generation: None,
            create_only: false,
            replace_existing: false,
            allowed_existing_names: Vec::new(),
            reserved_at: Some("2026-09-06T00:00:00Z".to_string()),
            cleanup_token: None,
            expected_owner: None,
        }
    }

    #[test]
    fn cleanup_closure_follows_retry_and_incoming_relationships_once() {
        let mut req = request();
        req.cleanup_roots.push(AgentCleanupRootWire {
            root_id: "slot-a".to_string(),
            requested_name: "alpha".to_string(),
            expected_bead_id: Some("sase-xr.2".to_string()),
            expected_assignee: Some("agent-a".to_string()),
            expected_owner: Some(expected_owner(
                "alpha",
                "ts-a",
                "/projects/proj/artifacts/workflow/ts-a",
            )),
            allow_container_cleanup: false,
            allow_live_cleanup: false,
            allow_waiting_cleanup: true,
        });
        let mut root = artifact(
            "a",
            "alpha",
            "ts-a",
            "/projects/proj/artifacts/workflow/ts-a",
        );
        root.outgoing_suffixes.push("ts-b".to_string());
        let mut retry = artifact(
            "b",
            "beta",
            "ts-b",
            "/projects/proj/artifacts/workflow/ts-b",
        );
        retry.relation_refs.push("ts-a".to_string());
        let mut workflow_child = artifact(
            "c",
            "child",
            "ts-c",
            "/projects/proj/artifacts/workflow/ts-c",
        );
        workflow_child.relation_refs.push("ts-b".to_string());
        let dismissed =
            bundle("d", "dismissed", "ts-c", "/dismissed_bundles/d.json");
        req.source_records = vec![root, retry, workflow_child, dismissed];

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(plan.selected_owners.len(), 1);
        assert!(plan
            .cleanup_closure
            .artifact_dirs
            .contains(&"/projects/proj/artifacts/workflow/ts-a".to_string()));
        assert!(plan
            .cleanup_closure
            .artifact_dirs
            .contains(&"/projects/proj/artifacts/workflow/ts-b".to_string()));
        assert!(plan
            .cleanup_closure
            .artifact_dirs
            .contains(&"/projects/proj/artifacts/workflow/ts-c".to_string()));
        assert!(plan
            .cleanup_closure
            .bundle_paths
            .contains(&"/dismissed_bundles/d.json".to_string()));
        assert_eq!(plan.cleanup_effects.len(), 4);
    }

    #[test]
    fn cleanup_does_not_select_dotted_prefix_descendants_by_name_only() {
        let mut req = request();
        req.cleanup_roots.push(AgentCleanupRootWire {
            root_id: "root".to_string(),
            requested_name: "alpha".to_string(),
            expected_bead_id: None,
            expected_assignee: None,
            expected_owner: Some(expected_owner(
                "alpha",
                "ts-a",
                "/projects/proj/artifacts/workflow/ts-a",
            )),
            allow_container_cleanup: false,
            allow_live_cleanup: false,
            allow_waiting_cleanup: false,
        });
        req.source_records = vec![
            artifact(
                "a",
                "alpha",
                "ts-a",
                "/projects/proj/artifacts/workflow/ts-a",
            ),
            artifact(
                "b",
                "alpha.child",
                "ts-b",
                "/projects/proj/artifacts/workflow/ts-b",
            ),
        ];

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(
            plan.cleanup_closure.artifact_dirs,
            vec!["/projects/proj/artifacts/workflow/ts-a"]
        );
        assert!(!plan
            .cleanup_closure
            .names
            .contains(&"alpha.child".to_string()));
    }

    #[test]
    fn overlapping_roots_deduplicate_effects_with_attribution() {
        let mut req = request();
        for (root_id, name) in [("root-a", "alpha"), ("root-b", "beta")] {
            req.cleanup_roots.push(AgentCleanupRootWire {
                root_id: root_id.to_string(),
                requested_name: name.to_string(),
                expected_bead_id: None,
                expected_assignee: None,
                expected_owner: Some(expected_owner(
                    name,
                    "ts-shared",
                    "/projects/proj/artifacts/workflow/ts-shared",
                )),
                allow_container_cleanup: false,
                allow_live_cleanup: false,
                allow_waiting_cleanup: false,
            });
        }
        req.source_records = vec![artifact(
            "shared",
            "alpha",
            "ts-shared",
            "/projects/proj/artifacts/workflow/ts-shared",
        )];

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(plan.cleanup_effects.len(), 1);
        assert_eq!(
            plan.cleanup_effects[0].target_root_ids,
            vec!["root-a".to_string(), "root-b".to_string()]
        );
    }

    #[test]
    fn incomplete_discovery_and_truncated_closure_block_cleanup() {
        let mut incomplete = request();
        incomplete.sources_complete = false;
        incomplete.cleanup_roots.push(AgentCleanupRootWire {
            root_id: "root".to_string(),
            requested_name: "alpha".to_string(),
            expected_bead_id: None,
            expected_assignee: None,
            expected_owner: Some(expected_owner(
                "alpha",
                "ts-a",
                "/projects/proj/artifacts/workflow/ts-a",
            )),
            allow_container_cleanup: false,
            allow_live_cleanup: false,
            allow_waiting_cleanup: false,
        });
        let plan = plan_agent_ownership_batch(&incomplete).unwrap();
        assert_eq!(
            plan.blocked_owners[0].reason,
            "source_discovery_incomplete"
        );

        let mut bounded = incomplete;
        bounded.sources_complete = true;
        bounded.max_closure_records = Some(0);
        bounded.source_records = vec![artifact(
            "a",
            "alpha",
            "ts-a",
            "/projects/proj/artifacts/workflow/ts-a",
        )];
        let plan = plan_agent_ownership_batch(&bounded).unwrap();
        assert_eq!(plan.blocked_owners[0].reason, "closure_limit_exceeded");
    }

    #[test]
    fn live_and_container_owners_are_not_selected_without_authorization() {
        let mut req = request();
        let mut owner = expected_owner("alpha", "ts-a", "/a");
        owner.marker_state = Some(AgentMarkerStateWire {
            status: "RUNNING".to_string(),
            marker_kind: Some("running".to_string()),
            live: true,
            terminal: false,
            waiting: false,
            cleanup_allowed: false,
        });
        req.cleanup_roots.push(AgentCleanupRootWire {
            root_id: "live".to_string(),
            requested_name: "alpha".to_string(),
            expected_bead_id: None,
            expected_assignee: None,
            expected_owner: Some(owner),
            allow_container_cleanup: false,
            allow_live_cleanup: false,
            allow_waiting_cleanup: false,
        });
        let mut container = expected_owner("family", "ts-b", "/b");
        container.container_kind = Some(CONTAINER_KIND_FAMILY.to_string());
        req.cleanup_roots.push(AgentCleanupRootWire {
            root_id: "container".to_string(),
            requested_name: "family".to_string(),
            expected_bead_id: None,
            expected_assignee: None,
            expected_owner: Some(container),
            allow_container_cleanup: false,
            allow_live_cleanup: false,
            allow_waiting_cleanup: false,
        });

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(plan.preserved_owners[0].reason, "live_owner");
        assert_eq!(plan.blocked_owners[0].reason, "container_owner");
    }

    #[test]
    fn reservations_reuse_current_owner_aliases_and_planned_owner() {
        let mut req = request();
        req.reservation_snapshot
            .push(registry_entry("alpha", "/tmp/a", "planned"));
        req.reservation_requests.push(reservation(
            "claim",
            AgentNameReservationOperationWire::ClaimPlanned,
            "alice.athena.alpha",
            "/tmp/a",
        ));

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert!(plan.reservation_blocked.is_empty());
        assert_eq!(plan.reservation_decisions[0].storage_name, "alpha");
        assert_eq!(
            plan.registry_merge_plan[0].action,
            REGISTRY_MERGE_ACTION_UPSERT
        );
        assert_eq!(
            plan.registry_merge_plan[0]
                .expected
                .reservation_kind
                .as_deref(),
            Some("planned")
        );
    }

    #[test]
    fn reservations_block_batch_duplicate_aliases_and_foreign_namespace() {
        let mut req = request();
        req.reservation_snapshot.push(AgentNameRegistryEntryWire {
            name: "zeus".to_string(),
            source: Some("registry".to_string()),
            origin: Some(ORIGIN_IMPORT_V2.to_string()),
            canonical_global_name: None,
            source_owner: Some(
                AgentOwnerIdentity::new("alice", "zeus").unwrap(),
            ),
            legacy_source_machine: None,
            imported_digest: None,
            project_name: None,
            workflow_dir: None,
            raw_suffix: None,
            artifacts_dir: None,
            bundle_path: None,
            state: Some("reserved".to_string()),
            reservation_kind: Some(CONTAINER_KIND_OWNER_NAMESPACE.to_string()),
            container_kind: Some(CONTAINER_KIND_OWNER_NAMESPACE.to_string()),
            clan_generation: None,
            template_namespace: None,
            reserved_at: None,
            cleanup_token: None,
            cleanup_operation: None,
            collision_owners: Vec::new(),
            extra: BTreeMap::new(),
        });
        req.reservation_requests.push(reservation(
            "one",
            AgentNameReservationOperationWire::ReservePlanned,
            "alpha",
            "/tmp/a",
        ));
        req.reservation_requests.push(reservation(
            "two",
            AgentNameReservationOperationWire::ReservePlanned,
            "alice.athena.alpha",
            "/tmp/b",
        ));
        req.reservation_requests.push(reservation(
            "foreign",
            AgentNameReservationOperationWire::ReservePlanned,
            "zeus.child",
            "/tmp/c",
        ));

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(plan.reservation_blocked.len(), 3);
        assert!(plan
            .reservation_blocked
            .iter()
            .any(|blocked| blocked.reason == "duplicate_batch_name"));
        assert!(plan
            .reservation_blocked
            .iter()
            .any(|blocked| blocked.reason == "reserved_owner_namespace"));
    }

    #[test]
    fn clan_generation_mismatch_blocks_claim_overwrite() {
        let mut req = request();
        let mut clan =
            registry_entry("ship", "/tmp/a", RESERVATION_KIND_PLANNED_CLAN);
        clan.container_kind = Some(CONTAINER_KIND_CLAN.to_string());
        clan.clan_generation = Some("old".to_string());
        req.reservation_snapshot.push(clan);
        let mut claim = reservation(
            "claim-clan",
            AgentNameReservationOperationWire::ClaimClan,
            "ship",
            "/tmp/a",
        );
        claim.clan_generation = Some("new".to_string());
        req.reservation_requests.push(claim);

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(
            plan.reservation_blocked[0].reason,
            "clan_generation_mismatch"
        );
    }

    #[test]
    fn cleanup_guard_returns_internal_reservation_state() {
        let mut req = request();
        req.reservation_snapshot.push(registry_entry(
            "alpha",
            "/tmp/a",
            RESERVATION_KIND_CLAIMED,
        ));
        let mut guard = reservation(
            "cleanup",
            AgentNameReservationOperationWire::CleanupGuard,
            "alpha",
            "/tmp/a",
        );
        guard.cleanup_token = Some("op-1".to_string());
        req.reservation_requests.push(guard);

        let plan = plan_agent_ownership_batch(&req).unwrap();

        assert_eq!(plan.cleanup_reservations.len(), 1);
        assert_eq!(plan.cleanup_reservations[0].cleanup_token, "op-1");
        let entry = plan.registry_merge_plan[0].entry.as_ref().unwrap();
        assert_eq!(
            entry.reservation_kind.as_deref(),
            Some(RESERVATION_KIND_CLEANUP_IN_PROGRESS)
        );
    }
}
