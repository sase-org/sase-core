//! Explicit, portable agent ownership and relationship domain.
//!
//! New callers must carry owner identity explicitly.  The legacy
//! `machine_hood` module remains available as a migration shim, but none of
//! the operations in this module infer ownership from an arbitrary dotted
//! agent name.

mod identity;
mod relationships;

pub use identity::{
    agent_link_target, agent_link_target_with_owner_roots, agent_local_hood,
    agent_local_hood_with_owner_roots, agent_name_ancestors,
    agent_name_ancestors_with_owner_roots, agent_name_in_hood,
    agent_name_in_hood_with_owner_roots, classify_agent_ownership,
    classify_legacy_v1_group_ownership, foreign_agent_owner_root,
    globalize_agent_name, globalize_legacy_agent_name,
    globalize_owned_agent_name, localize_agent_name,
    normalize_agent_archive_name, normalize_owned_agent_name,
    parse_agent_family_name, parse_owned_agent_name, strip_global_agent_name,
    validate_agent_name, validate_agent_reference_name,
    validate_agent_username, validate_owned_agent_name, validate_owner_root,
    AgentFamilyNameWire, AgentIdentityError, AgentLinkTargetWire,
    AgentOwnerIdentity, AgentOwnershipClassification, AgentSourceOwnerIdentity,
    LegacyV1GroupOwnershipClassification, LegacyV1GroupOwnershipEvidence,
    OwnedAgentNameWire, OwnerRoot,
};
pub use relationships::{
    project_agent_relationship_graph, rewrite_agent_relationship_batch,
    validate_agent_relationship_batch, AgentContainerKind,
    AgentRelationshipBatchWire, AgentRelationshipError, AgentRelationshipKind,
    AgentRelationshipTargetWire, AgentRelationshipWire, AgentRunContainerWire,
    AgentRunWire, ProjectedAgentGraphWire,
    ProjectedAgentRelationshipTargetWire, ProjectedAgentRelationshipWire,
    ProjectedAgentRunContainerWire, ProjectedAgentRunWire,
    RewrittenAgentRelationshipBatchWire, RewrittenAgentRelationshipTargetWire,
    RewrittenAgentRelationshipWire, RewrittenAgentRunContainerWire,
    RewrittenAgentRunWire, ValidatedAgentRelationshipSummaryWire,
    AGENT_RELATIONSHIP_SCHEMA_VERSION,
};
