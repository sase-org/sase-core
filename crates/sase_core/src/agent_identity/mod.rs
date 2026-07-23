//! Explicit, portable agent ownership and relationship domain.
//!
//! New callers must carry owner identity explicitly.  The legacy
//! `machine_hood` module remains available as a migration shim, but none of
//! the operations in this module infer ownership from an arbitrary dotted
//! agent name.

mod identity;
mod relationships;

pub use identity::{
    agent_link_target, agent_local_hood, agent_name_ancestors,
    agent_name_in_hood, classify_agent_ownership, globalize_agent_name,
    globalize_legacy_agent_name, localize_agent_name,
    normalize_agent_archive_name, parse_agent_family_name,
    strip_global_agent_name, validate_agent_username, AgentFamilyNameWire,
    AgentIdentityError, AgentLinkTargetWire, AgentOwnerIdentity,
    AgentOwnershipClassification, AgentSourceOwnerIdentity,
};
pub use relationships::{
    rewrite_agent_relationship_batch, validate_agent_relationship_batch,
    AgentContainerKind, AgentRelationshipBatchWire, AgentRelationshipError,
    AgentRelationshipKind, AgentRelationshipTargetWire, AgentRelationshipWire,
    AgentRunContainerWire, AgentRunWire, RewrittenAgentRelationshipBatchWire,
    RewrittenAgentRelationshipTargetWire, RewrittenAgentRelationshipWire,
    RewrittenAgentRunContainerWire, RewrittenAgentRunWire,
    ValidatedAgentRelationshipSummaryWire, AGENT_RELATIONSHIP_SCHEMA_VERSION,
};
