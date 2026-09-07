//! Pure ownership-batch contracts for launch and relaunch preparation.
//!
//! This module models the coherent snapshot decisions that Python hosts need
//! before mutating name reservations or deleting old agent ownership records.
//! It deliberately performs no filesystem, process, registry, or bead writes.

mod planner;
pub mod wire;

pub use planner::{plan_agent_ownership_batch, AgentOwnershipBatchError};
pub use wire::{
    agent_ownership_batch_request_from_json_value, AgentCleanupEffectWire,
    AgentCleanupReservationWire, AgentCleanupRootPlanWire,
    AgentCleanupRootWire, AgentExpectedOwnerPredicateWire,
    AgentExpectedOwnerWire, AgentMarkerStateWire, AgentNameRegistryEntryWire,
    AgentNameRegistryMergeWire, AgentNameReservationAcceptedWire,
    AgentNameReservationBlockedWire, AgentNameReservationOperationWire,
    AgentNameReservationRequestWire, AgentOwnershipBatchPlanWire,
    AgentOwnershipBatchRequestWire, AgentOwnershipClosureWire,
    AgentOwnershipOwnerDecisionWire, AgentOwnershipSlotWire,
    AgentOwnershipSourceKindWire, AgentOwnershipSourceRecordWire,
    AgentProcessIdentityWire, AgentSourceSignatureWire,
    AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION, CLEANUP_EFFECT_ARTIFACT_DIR,
    CLEANUP_EFFECT_BUNDLE_PATH, CLEANUP_OUTCOME_BLOCKED,
    CLEANUP_OUTCOME_PRESERVED, CLEANUP_OUTCOME_SELECTED,
    REGISTRY_MERGE_ACTION_NO_OP, REGISTRY_MERGE_ACTION_REMOVE,
    REGISTRY_MERGE_ACTION_UPSERT, RESERVATION_KIND_CLEANUP_IN_PROGRESS,
};
