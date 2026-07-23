use super::identity::{
    canonical_global_local_name, parse_agent_family_name, AgentIdentityError,
    AgentOwnerIdentity,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub const AGENT_RELATIONSHIP_SCHEMA_VERSION: u32 = 2;

const MAX_RUNS: usize = 4_096;
const MAX_CONTAINERS: usize = 2_048;
const MAX_RELATIONSHIPS: usize = 16_384;
const MAX_CONTAINER_MEMBERS: usize = 4_096;
const MAX_RUN_ID_BYTES: usize = 128;
const MAX_DESTINATION_ID_BYTES: usize = 128;
const MAX_OWNER_SEGMENT_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRelationshipBatchWire {
    pub schema_version: u32,
    pub owner: AgentOwnerIdentity,
    #[serde(default)]
    pub runs: Vec<AgentRunWire>,
    #[serde(default)]
    pub containers: Vec<AgentRunContainerWire>,
    #[serde(default)]
    pub relationships: Vec<AgentRelationshipWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunWire {
    pub source_run_id: String,
    pub global_name: String,
    pub owner: AgentOwnerIdentity,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentContainerKind {
    Family,
    Clan,
}

impl AgentContainerKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Family => "family",
            Self::Clan => "clan",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunContainerWire {
    pub kind: AgentContainerKind,
    pub global_name: String,
    pub owner: AgentOwnerIdentity,
    pub member_source_run_ids: Vec<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentRelationshipKind {
    Parent,
    WorkflowParent,
    Retry,
    Wait,
}

impl AgentRelationshipKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Parent => "parent",
            Self::WorkflowParent => "workflow_parent",
            Self::Retry => "retry",
            Self::Wait => "wait",
        }
    }

    const fn is_one_to_one(self) -> bool {
        !matches!(self, Self::Wait)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentRelationshipTargetWire {
    SourceRunId {
        source_run_id: String,
    },
    GlobalName {
        global_name: String,
        owner: AgentOwnerIdentity,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRelationshipWire {
    pub kind: AgentRelationshipKind,
    pub source_run_id: String,
    pub target: AgentRelationshipTargetWire,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatedAgentRelationshipSummaryWire {
    pub schema_version: u32,
    pub owner: AgentOwnerIdentity,
    pub run_count: usize,
    pub container_count: usize,
    pub relationship_count: usize,
    pub run_order: Vec<String>,
    pub global_name_order: Vec<String>,
    pub container_order: Vec<String>,
    pub relationship_order: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewrittenAgentRelationshipBatchWire {
    pub schema_version: u32,
    pub owner: AgentOwnerIdentity,
    pub runs: Vec<RewrittenAgentRunWire>,
    pub containers: Vec<RewrittenAgentRunContainerWire>,
    pub relationships: Vec<RewrittenAgentRelationshipWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewrittenAgentRunWire {
    pub source_run_id: String,
    pub destination_run_id: String,
    pub global_name: String,
    pub owner: AgentOwnerIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewrittenAgentRunContainerWire {
    pub kind: AgentContainerKind,
    pub global_name: String,
    pub owner: AgentOwnerIdentity,
    pub member_source_run_ids: Vec<String>,
    pub member_destination_run_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RewrittenAgentRelationshipTargetWire {
    DestinationRunId {
        source_run_id: String,
        destination_run_id: String,
    },
    GlobalName {
        global_name: String,
        owner: AgentOwnerIdentity,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewrittenAgentRelationshipWire {
    pub kind: AgentRelationshipKind,
    pub source_run_id: String,
    pub source_destination_run_id: String,
    pub target: RewrittenAgentRelationshipTargetWire,
    pub required: bool,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AgentRelationshipError {
    #[error(
        "unsupported agent relationship schema_version {actual}; expected {expected}"
    )]
    UnsupportedSchema { actual: u32, expected: u32 },

    #[error("invalid batch owner: {source}")]
    InvalidBatchOwner {
        #[source]
        source: Box<AgentIdentityError>,
    },

    #[error("invalid owner for run '{run_id}': {source}")]
    InvalidRunOwner {
        run_id: String,
        #[source]
        source: Box<AgentIdentityError>,
    },

    #[error("invalid owner for container '{container}': {source}")]
    InvalidContainerOwner {
        container: String,
        #[source]
        source: Box<AgentIdentityError>,
    },

    #[error("invalid owner for relationship target '{target}': {source}")]
    InvalidTargetOwner {
        target: String,
        #[source]
        source: Box<AgentIdentityError>,
    },

    #[error("invalid global name '{name}' for {context}: {source}")]
    InvalidGlobalName {
        context: String,
        name: String,
        #[source]
        source: Box<AgentIdentityError>,
    },

    #[error("{kind} count {actual} exceeds the conservative limit of {limit}")]
    CountLimit {
        kind: &'static str,
        actual: usize,
        limit: usize,
    },

    #[error("invalid {field} '{value}' for {context}: {reason}")]
    InvalidIdentifier {
        field: &'static str,
        value: String,
        context: String,
        reason: String,
    },

    #[error("{field} for {context} is {actual} bytes; limit is {limit} bytes")]
    StringLimit {
        field: &'static str,
        context: String,
        actual: usize,
        limit: usize,
    },

    #[error(
        "owner mismatch for {context}: found '{actual_username}.{actual_machine}', expected batch owner '{expected_username}.{expected_machine}'"
    )]
    OwnerMismatch {
        context: String,
        actual_username: String,
        actual_machine: String,
        expected_username: String,
        expected_machine: String,
    },

    #[error("duplicate source run ID '{run_id}'")]
    DuplicateRunId { run_id: String },

    #[error(
        "duplicate global run identity '{global_name}' (run IDs '{first_run_id}' and '{second_run_id}')"
    )]
    DuplicateGlobalName {
        global_name: String,
        first_run_id: String,
        second_run_id: String,
    },

    #[error("duplicate or inconsistent container global name '{global_name}'")]
    DuplicateContainer { global_name: String },

    #[error(
        "{kind} container '{global_name}' must have at least one unique member"
    )]
    InvalidContainerMembers {
        kind: &'static str,
        global_name: String,
    },

    #[error(
        "{kind} container '{global_name}' contains missing run ID '{run_id}'"
    )]
    MissingContainerMember {
        kind: &'static str,
        global_name: String,
        run_id: String,
    },

    #[error(
        "family container '{global_name}' contains run '{run_id}' from family '{actual_family}', expected '{expected_family}'"
    )]
    FamilyContainerMemberMismatch {
        global_name: String,
        run_id: String,
        actual_family: String,
        expected_family: String,
    },

    #[error(
        "{kind} container '{global_name}' must name a structural base, not family member '{local_name}'"
    )]
    InvalidContainerName {
        kind: &'static str,
        global_name: String,
        local_name: String,
    },

    #[error(
        "relationship {kind} from run '{source_run_id}' has missing required target '{target}'"
    )]
    DanglingRequiredTarget {
        kind: &'static str,
        source_run_id: String,
        target: String,
    },

    #[error(
        "relationship {kind} from run '{source_run_id}' has illegal cross-owner target '{target}' owned by '{target_username}.{target_machine}'"
    )]
    IllegalCrossOwnerTarget {
        kind: &'static str,
        source_run_id: String,
        target: String,
        target_username: String,
        target_machine: String,
    },

    #[error(
        "relationship {kind} from run '{source_run_id}' forms a self-edge"
    )]
    SelfEdge {
        kind: &'static str,
        source_run_id: String,
    },

    #[error(
        "duplicate one-to-one relationship {kind} from run '{source_run_id}'"
    )]
    DuplicateOneToOneEdge {
        kind: &'static str,
        source_run_id: String,
    },

    #[error(
        "duplicate relationship {kind} from run '{source_run_id}' to '{target}'"
    )]
    DuplicateEdge {
        kind: &'static str,
        source_run_id: String,
        target: String,
    },

    #[error(
        "relationship {kind} contains a directed cycle involving {run_ids:?}"
    )]
    Cycle {
        kind: &'static str,
        run_ids: Vec<String>,
    },

    #[error("destination map is missing source run ID '{source_run_id}'")]
    MissingDestinationMapping { source_run_id: String },

    #[error(
        "destination map contains unknown source run ID '{source_run_id}'"
    )]
    UnknownDestinationMapping { source_run_id: String },

    #[error(
        "destination run ID '{destination_run_id}' is assigned to both '{first_source_run_id}' and '{second_source_run_id}'"
    )]
    DuplicateDestinationId {
        destination_run_id: String,
        first_source_run_id: String,
        second_source_run_id: String,
    },
}

pub fn validate_agent_relationship_batch(
    batch: &AgentRelationshipBatchWire,
) -> Result<ValidatedAgentRelationshipSummaryWire, AgentRelationshipError> {
    if batch.schema_version != AGENT_RELATIONSHIP_SCHEMA_VERSION {
        return Err(AgentRelationshipError::UnsupportedSchema {
            actual: batch.schema_version,
            expected: AGENT_RELATIONSHIP_SCHEMA_VERSION,
        });
    }
    batch.owner.validate().map_err(|source| {
        AgentRelationshipError::InvalidBatchOwner {
            source: Box::new(source),
        }
    })?;
    validate_owner_limits(&batch.owner, "batch owner")?;
    validate_count("run", batch.runs.len(), MAX_RUNS)?;
    validate_count("container", batch.containers.len(), MAX_CONTAINERS)?;
    validate_count(
        "relationship",
        batch.relationships.len(),
        MAX_RELATIONSHIPS,
    )?;

    let mut run_ids = BTreeSet::new();
    let mut run_by_global = BTreeMap::new();
    let mut run_local_names = BTreeMap::new();
    for run in &batch.runs {
        validate_run_id(&run.source_run_id, "run")?;
        run.owner.validate().map_err(|source| {
            AgentRelationshipError::InvalidRunOwner {
                run_id: run.source_run_id.clone(),
                source: Box::new(source),
            }
        })?;
        validate_owner_limits(
            &run.owner,
            &format!("run '{}'", run.source_run_id),
        )?;
        require_batch_owner(
            &run.owner,
            &batch.owner,
            format!("run '{}'", run.source_run_id),
        )?;
        let local_name =
            canonical_global_local_name(&run.global_name, &run.owner).map_err(
                |source| AgentRelationshipError::InvalidGlobalName {
                    context: format!("run '{}'", run.source_run_id),
                    name: run.global_name.clone(),
                    source: Box::new(source),
                },
            )?;
        if !run_ids.insert(run.source_run_id.clone()) {
            return Err(AgentRelationshipError::DuplicateRunId {
                run_id: run.source_run_id.clone(),
            });
        }
        if let Some(first_run_id) = run_by_global
            .insert(run.global_name.clone(), run.source_run_id.clone())
        {
            return Err(AgentRelationshipError::DuplicateGlobalName {
                global_name: run.global_name.clone(),
                first_run_id,
                second_run_id: run.source_run_id.clone(),
            });
        }
        run_local_names.insert(run.source_run_id.clone(), local_name);
    }

    validate_containers(batch, &run_ids, &run_local_names)?;
    let resolved_edges =
        validate_relationships(batch, &run_ids, &run_by_global)?;
    validate_cycles(&resolved_edges)?;

    let mut run_order: Vec<_> = run_ids.into_iter().collect();
    run_order.sort();
    let global_name_order = run_by_global.keys().cloned().collect();
    let mut container_order: Vec<_> = batch
        .containers
        .iter()
        .map(|container| {
            format!("{}:{}", container.kind.as_str(), container.global_name)
        })
        .collect();
    container_order.sort();
    let mut relationship_order: Vec<_> =
        (0..batch.relationships.len()).collect();
    relationship_order.sort_by_key(|index| {
        let relationship = &batch.relationships[*index];
        (
            relationship.kind,
            relationship.source_run_id.as_str(),
            target_key(&relationship.target),
            *index,
        )
    });

    Ok(ValidatedAgentRelationshipSummaryWire {
        schema_version: AGENT_RELATIONSHIP_SCHEMA_VERSION,
        owner: batch.owner.clone(),
        run_count: batch.runs.len(),
        container_count: batch.containers.len(),
        relationship_count: batch.relationships.len(),
        run_order,
        global_name_order,
        container_order,
        relationship_order,
    })
}

pub fn rewrite_agent_relationship_batch(
    batch: &AgentRelationshipBatchWire,
    destination_ids: &BTreeMap<String, String>,
) -> Result<RewrittenAgentRelationshipBatchWire, AgentRelationshipError> {
    validate_agent_relationship_batch(batch)?;
    let source_ids: BTreeSet<_> = batch
        .runs
        .iter()
        .map(|run| run.source_run_id.as_str())
        .collect();

    for source_run_id in destination_ids.keys() {
        if !source_ids.contains(source_run_id.as_str()) {
            return Err(AgentRelationshipError::UnknownDestinationMapping {
                source_run_id: source_run_id.clone(),
            });
        }
    }

    let mut destination_sources = BTreeMap::new();
    for run in &batch.runs {
        let destination_run_id =
            destination_ids.get(&run.source_run_id).ok_or_else(|| {
                AgentRelationshipError::MissingDestinationMapping {
                    source_run_id: run.source_run_id.clone(),
                }
            })?;
        validate_destination_id(
            destination_run_id,
            &format!("mapping for '{}'", run.source_run_id),
        )?;
        if let Some(first_source_run_id) = destination_sources
            .insert(destination_run_id.clone(), run.source_run_id.clone())
        {
            return Err(AgentRelationshipError::DuplicateDestinationId {
                destination_run_id: destination_run_id.clone(),
                first_source_run_id,
                second_source_run_id: run.source_run_id.clone(),
            });
        }
    }

    let runs = batch
        .runs
        .iter()
        .map(|run| RewrittenAgentRunWire {
            source_run_id: run.source_run_id.clone(),
            destination_run_id: destination_ids[&run.source_run_id].clone(),
            global_name: run.global_name.clone(),
            owner: run.owner.clone(),
        })
        .collect();
    let containers = batch
        .containers
        .iter()
        .map(|container| {
            Ok(RewrittenAgentRunContainerWire {
                kind: container.kind,
                global_name: container.global_name.clone(),
                owner: container.owner.clone(),
                member_source_run_ids: container
                    .member_source_run_ids
                    .clone(),
                member_destination_run_ids: container
                    .member_source_run_ids
                    .iter()
                    .map(|source_run_id| {
                        destination_ids.get(source_run_id).cloned().ok_or_else(
                            || AgentRelationshipError::MissingDestinationMapping {
                                source_run_id: source_run_id.clone(),
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect::<Result<Vec<_>, AgentRelationshipError>>()?;
    let relationships = batch
        .relationships
        .iter()
        .map(|relationship| {
            let source_destination_run_id = destination_ids
                .get(&relationship.source_run_id)
                .cloned()
                .ok_or_else(|| {
                    AgentRelationshipError::MissingDestinationMapping {
                        source_run_id: relationship.source_run_id.clone(),
                    }
                })?;
            let target = match &relationship.target {
                AgentRelationshipTargetWire::SourceRunId { source_run_id } => {
                    let destination_run_id = destination_ids
                        .get(source_run_id)
                        .cloned()
                        .ok_or_else(|| {
                            AgentRelationshipError::MissingDestinationMapping {
                                source_run_id: source_run_id.clone(),
                            }
                        })?;
                    RewrittenAgentRelationshipTargetWire::DestinationRunId {
                        source_run_id: source_run_id.clone(),
                        destination_run_id,
                    }
                }
                AgentRelationshipTargetWire::GlobalName {
                    global_name,
                    owner,
                } => RewrittenAgentRelationshipTargetWire::GlobalName {
                    global_name: global_name.clone(),
                    owner: owner.clone(),
                },
            };
            Ok(RewrittenAgentRelationshipWire {
                kind: relationship.kind,
                source_run_id: relationship.source_run_id.clone(),
                source_destination_run_id,
                target,
                required: relationship.required,
            })
        })
        .collect::<Result<Vec<_>, AgentRelationshipError>>()?;

    Ok(RewrittenAgentRelationshipBatchWire {
        schema_version: AGENT_RELATIONSHIP_SCHEMA_VERSION,
        owner: batch.owner.clone(),
        runs,
        containers,
        relationships,
    })
}

fn validate_containers(
    batch: &AgentRelationshipBatchWire,
    run_ids: &BTreeSet<String>,
    run_local_names: &BTreeMap<String, String>,
) -> Result<(), AgentRelationshipError> {
    let mut container_names = BTreeSet::new();
    for container in &batch.containers {
        validate_count(
            "container member",
            container.member_source_run_ids.len(),
            MAX_CONTAINER_MEMBERS,
        )?;
        container.owner.validate().map_err(|source| {
            AgentRelationshipError::InvalidContainerOwner {
                container: container.global_name.clone(),
                source: Box::new(source),
            }
        })?;
        validate_owner_limits(
            &container.owner,
            &format!(
                "{} container '{}'",
                container.kind.as_str(),
                container.global_name
            ),
        )?;
        require_batch_owner(
            &container.owner,
            &batch.owner,
            format!(
                "{} container '{}'",
                container.kind.as_str(),
                container.global_name
            ),
        )?;
        let local_name = canonical_global_local_name(
            &container.global_name,
            &container.owner,
        )
        .map_err(|source| {
            AgentRelationshipError::InvalidGlobalName {
                context: format!("{} container", container.kind.as_str()),
                name: container.global_name.clone(),
                source: Box::new(source),
            }
        })?;
        let parsed =
            parse_agent_family_name(&local_name).map_err(|source| {
                AgentRelationshipError::InvalidGlobalName {
                    context: format!("{} container", container.kind.as_str()),
                    name: container.global_name.clone(),
                    source: Box::new(source),
                }
            })?;
        if parsed.member_role.is_some() {
            return Err(AgentRelationshipError::InvalidContainerName {
                kind: container.kind.as_str(),
                global_name: container.global_name.clone(),
                local_name,
            });
        }
        if !container_names.insert(container.global_name.clone()) {
            return Err(AgentRelationshipError::DuplicateContainer {
                global_name: container.global_name.clone(),
            });
        }
        let mut members = BTreeSet::new();
        if container.member_source_run_ids.is_empty() {
            return Err(AgentRelationshipError::InvalidContainerMembers {
                kind: container.kind.as_str(),
                global_name: container.global_name.clone(),
            });
        }
        for run_id in &container.member_source_run_ids {
            validate_run_id(
                run_id,
                &format!(
                    "{} container '{}'",
                    container.kind.as_str(),
                    container.global_name
                ),
            )?;
            if !members.insert(run_id) {
                return Err(AgentRelationshipError::InvalidContainerMembers {
                    kind: container.kind.as_str(),
                    global_name: container.global_name.clone(),
                });
            }
            if !run_ids.contains(run_id) {
                return Err(AgentRelationshipError::MissingContainerMember {
                    kind: container.kind.as_str(),
                    global_name: container.global_name.clone(),
                    run_id: run_id.clone(),
                });
            }
            if container.kind == AgentContainerKind::Family {
                let member_local_name = &run_local_names[run_id];
                let member_family = parse_agent_family_name(member_local_name)
                    .map_err(|source| {
                        AgentRelationshipError::InvalidGlobalName {
                            context: format!("run '{run_id}'"),
                            name: member_local_name.clone(),
                            source: Box::new(source),
                        }
                    })?;
                if member_family.family_name != local_name {
                    return Err(
                        AgentRelationshipError::FamilyContainerMemberMismatch {
                            global_name: container.global_name.clone(),
                            run_id: run_id.clone(),
                            actual_family: member_family.family_name,
                            expected_family: local_name.clone(),
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

type ResolvedEdge = (AgentRelationshipKind, String, String);

fn validate_relationships(
    batch: &AgentRelationshipBatchWire,
    run_ids: &BTreeSet<String>,
    run_by_global: &BTreeMap<String, String>,
) -> Result<Vec<ResolvedEdge>, AgentRelationshipError> {
    let mut one_to_one = BTreeSet::new();
    let mut exact_edges = BTreeSet::new();
    let mut resolved_edges = Vec::new();
    for relationship in &batch.relationships {
        validate_run_id(
            &relationship.source_run_id,
            &format!("{} relationship source", relationship.kind.as_str()),
        )?;
        if !run_ids.contains(&relationship.source_run_id) {
            return Err(AgentRelationshipError::DanglingRequiredTarget {
                kind: relationship.kind.as_str(),
                source_run_id: relationship.source_run_id.clone(),
                target: "<missing source run>".to_string(),
            });
        }

        if relationship.kind.is_one_to_one()
            && !one_to_one
                .insert((relationship.kind, relationship.source_run_id.clone()))
        {
            return Err(AgentRelationshipError::DuplicateOneToOneEdge {
                kind: relationship.kind.as_str(),
                source_run_id: relationship.source_run_id.clone(),
            });
        }

        let display_target = target_key(&relationship.target);
        if !exact_edges.insert((
            relationship.kind,
            relationship.source_run_id.clone(),
            display_target.clone(),
        )) {
            return Err(AgentRelationshipError::DuplicateEdge {
                kind: relationship.kind.as_str(),
                source_run_id: relationship.source_run_id.clone(),
                target: display_target,
            });
        }

        let resolved_target = match &relationship.target {
            AgentRelationshipTargetWire::SourceRunId { source_run_id } => {
                validate_run_id(
                    source_run_id,
                    &format!(
                        "{} relationship target from '{}'",
                        relationship.kind.as_str(),
                        relationship.source_run_id
                    ),
                )?;
                match run_ids.get(source_run_id) {
                    Some(run_id) => Some(run_id.clone()),
                    None if relationship.required => {
                        return Err(
                            AgentRelationshipError::DanglingRequiredTarget {
                                kind: relationship.kind.as_str(),
                                source_run_id: relationship
                                    .source_run_id
                                    .clone(),
                                target: source_run_id.clone(),
                            },
                        );
                    }
                    None => None,
                }
            }
            AgentRelationshipTargetWire::GlobalName { global_name, owner } => {
                owner.validate().map_err(|source| {
                    AgentRelationshipError::InvalidTargetOwner {
                        target: global_name.clone(),
                        source: Box::new(source),
                    }
                })?;
                validate_owner_limits(
                    owner,
                    &format!(
                        "{} relationship target from '{}'",
                        relationship.kind.as_str(),
                        relationship.source_run_id
                    ),
                )?;
                canonical_global_local_name(global_name, owner).map_err(
                    |source| AgentRelationshipError::InvalidGlobalName {
                        context: format!(
                            "{} relationship target from '{}'",
                            relationship.kind.as_str(),
                            relationship.source_run_id
                        ),
                        name: global_name.clone(),
                        source: Box::new(source),
                    },
                )?;
                if owner != &batch.owner {
                    if relationship.required {
                        return Err(
                            AgentRelationshipError::IllegalCrossOwnerTarget {
                                kind: relationship.kind.as_str(),
                                source_run_id: relationship
                                    .source_run_id
                                    .clone(),
                                target: global_name.clone(),
                                target_username: owner.username.clone(),
                                target_machine: owner.machine_name.clone(),
                            },
                        );
                    }
                    None
                } else {
                    match run_by_global.get(global_name) {
                        Some(run_id) => Some(run_id.clone()),
                        None if relationship.required => {
                            return Err(
                                AgentRelationshipError::DanglingRequiredTarget {
                                    kind: relationship.kind.as_str(),
                                    source_run_id: relationship
                                        .source_run_id
                                        .clone(),
                                    target: global_name.clone(),
                                },
                            );
                        }
                        None => None,
                    }
                }
            }
        };

        if resolved_target.as_deref()
            == Some(relationship.source_run_id.as_str())
        {
            return Err(AgentRelationshipError::SelfEdge {
                kind: relationship.kind.as_str(),
                source_run_id: relationship.source_run_id.clone(),
            });
        }
        if let Some(target) = resolved_target {
            resolved_edges.push((
                relationship.kind,
                relationship.source_run_id.clone(),
                target,
            ));
        }
    }
    Ok(resolved_edges)
}

fn validate_cycles(
    resolved_edges: &[ResolvedEdge],
) -> Result<(), AgentRelationshipError> {
    for kind in [
        AgentRelationshipKind::Parent,
        AgentRelationshipKind::WorkflowParent,
        AgentRelationshipKind::Retry,
        AgentRelationshipKind::Wait,
    ] {
        let mut graph: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, source, target) in resolved_edges
            .iter()
            .filter(|(edge_kind, _, _)| *edge_kind == kind)
        {
            graph
                .entry(source.clone())
                .or_default()
                .push(target.clone());
            graph.entry(target.clone()).or_default();
        }
        for targets in graph.values_mut() {
            targets.sort();
            targets.dedup();
        }

        let mut state: BTreeMap<String, u8> =
            graph.keys().map(|node| (node.clone(), 0)).collect();
        let mut stack = Vec::new();
        for node in graph.keys() {
            if state[node] == 0 {
                if let Some(cycle) =
                    find_cycle(node, &graph, &mut state, &mut stack)
                {
                    return Err(AgentRelationshipError::Cycle {
                        kind: kind.as_str(),
                        run_ids: cycle,
                    });
                }
            }
        }
    }
    Ok(())
}

fn find_cycle(
    node: &str,
    graph: &BTreeMap<String, Vec<String>>,
    state: &mut BTreeMap<String, u8>,
    stack: &mut Vec<String>,
) -> Option<Vec<String>> {
    state.insert(node.to_string(), 1);
    stack.push(node.to_string());
    if let Some(targets) = graph.get(node) {
        for target in targets {
            match state.get(target).copied().unwrap_or_default() {
                0 => {
                    if let Some(cycle) = find_cycle(target, graph, state, stack)
                    {
                        return Some(cycle);
                    }
                }
                1 => {
                    let start = stack
                        .iter()
                        .position(|entry| entry == target)
                        .unwrap_or_default();
                    let mut cycle = stack[start..].to_vec();
                    cycle.push(target.clone());
                    return Some(cycle);
                }
                _ => {}
            }
        }
    }
    stack.pop();
    state.insert(node.to_string(), 2);
    None
}

fn require_batch_owner(
    actual: &AgentOwnerIdentity,
    expected: &AgentOwnerIdentity,
    context: String,
) -> Result<(), AgentRelationshipError> {
    if actual == expected {
        Ok(())
    } else {
        Err(AgentRelationshipError::OwnerMismatch {
            context,
            actual_username: actual.username.clone(),
            actual_machine: actual.machine_name.clone(),
            expected_username: expected.username.clone(),
            expected_machine: expected.machine_name.clone(),
        })
    }
}

fn validate_owner_limits(
    owner: &AgentOwnerIdentity,
    context: &str,
) -> Result<(), AgentRelationshipError> {
    for (field, value) in [
        ("owner username", owner.username.as_str()),
        ("owner machine name", owner.machine_name.as_str()),
    ] {
        if value.len() > MAX_OWNER_SEGMENT_BYTES {
            return Err(AgentRelationshipError::StringLimit {
                field,
                context: context.to_string(),
                actual: value.len(),
                limit: MAX_OWNER_SEGMENT_BYTES,
            });
        }
    }
    Ok(())
}

fn validate_count(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), AgentRelationshipError> {
    if actual > limit {
        Err(AgentRelationshipError::CountLimit {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn validate_run_id(
    run_id: &str,
    context: &str,
) -> Result<(), AgentRelationshipError> {
    validate_identifier(run_id, "source run ID", context, MAX_RUN_ID_BYTES)
}

fn validate_destination_id(
    destination_id: &str,
    context: &str,
) -> Result<(), AgentRelationshipError> {
    validate_identifier(
        destination_id,
        "destination run ID",
        context,
        MAX_DESTINATION_ID_BYTES,
    )
}

fn validate_identifier(
    value: &str,
    field: &'static str,
    context: &str,
    limit: usize,
) -> Result<(), AgentRelationshipError> {
    let valid = !value.is_empty()
        && value.len() <= limit
        && value != "."
        && value != ".."
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':')
        });
    if valid {
        Ok(())
    } else {
        Err(AgentRelationshipError::InvalidIdentifier {
            field,
            value: value.to_string(),
            context: context.to_string(),
            reason: format!(
                "expected 1..={limit} path-safe ASCII letters, digits, '-', '_', '.', or ':' without '..'"
            ),
        })
    }
}

fn target_key(target: &AgentRelationshipTargetWire) -> String {
    match target {
        AgentRelationshipTargetWire::SourceRunId { source_run_id } => {
            format!("source_run_id:{source_run_id}")
        }
        AgentRelationshipTargetWire::GlobalName { global_name, owner } => {
            format!(
                "global_name:{}:{}:{}",
                owner.username, owner.machine_name, global_name
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn owner(username: &str, machine_name: &str) -> AgentOwnerIdentity {
        AgentOwnerIdentity::new(username, machine_name).unwrap()
    }

    fn run(id: &str, name: &str) -> AgentRunWire {
        AgentRunWire {
            source_run_id: id.to_string(),
            global_name: format!("alice.athena.{name}"),
            owner: owner("alice", "athena"),
        }
    }

    fn run_target(source_run_id: &str) -> AgentRelationshipTargetWire {
        AgentRelationshipTargetWire::SourceRunId {
            source_run_id: source_run_id.to_string(),
        }
    }

    fn relationship(
        kind: AgentRelationshipKind,
        source: &str,
        target: &str,
    ) -> AgentRelationshipWire {
        AgentRelationshipWire {
            kind,
            source_run_id: source.to_string(),
            target: run_target(target),
            required: true,
        }
    }

    fn valid_batch() -> AgentRelationshipBatchWire {
        AgentRelationshipBatchWire {
            schema_version: AGENT_RELATIONSHIP_SCHEMA_VERSION,
            owner: owner("alice", "athena"),
            runs: vec![
                run("run-1", "foo"),
                run("run-2", "foo--code"),
                run("run-3", "bar"),
                run("run-4", "baz"),
            ],
            containers: vec![
                AgentRunContainerWire {
                    kind: AgentContainerKind::Family,
                    global_name: "alice.athena.foo".to_string(),
                    owner: owner("alice", "athena"),
                    member_source_run_ids: vec![
                        "run-1".to_string(),
                        "run-2".to_string(),
                    ],
                },
                AgentRunContainerWire {
                    kind: AgentContainerKind::Clan,
                    global_name: "alice.athena.work".to_string(),
                    owner: owner("alice", "athena"),
                    member_source_run_ids: vec![
                        "run-3".to_string(),
                        "run-4".to_string(),
                    ],
                },
            ],
            relationships: vec![
                relationship(AgentRelationshipKind::Parent, "run-2", "run-1"),
                relationship(
                    AgentRelationshipKind::WorkflowParent,
                    "run-3",
                    "run-1",
                ),
                relationship(AgentRelationshipKind::Retry, "run-4", "run-3"),
                relationship(AgentRelationshipKind::Wait, "run-4", "run-2"),
            ],
        }
    }

    #[test]
    fn valid_mixed_batch_returns_canonical_summary() {
        let summary =
            validate_agent_relationship_batch(&valid_batch()).unwrap();
        assert_eq!(summary.schema_version, 2);
        assert_eq!(summary.run_count, 4);
        assert_eq!(summary.run_order, ["run-1", "run-2", "run-3", "run-4"]);
        assert_eq!(
            summary.container_order,
            ["clan:alice.athena.work", "family:alice.athena.foo"]
        );
        assert_eq!(summary.relationship_order.len(), 4);
    }

    #[test]
    fn serde_contract_rejects_machine_local_and_unknown_state() {
        let value = json!({
            "schema_version": 2,
            "owner": {"username": "alice", "machine_name": "athena"},
            "runs": [{
                "source_run_id": "run-1",
                "global_name": "alice.athena.foo",
                "owner": {"username": "alice", "machine_name": "athena"},
                "pid": 42
            }],
            "containers": [],
            "relationships": []
        });
        assert!(serde_json::from_value::<AgentRelationshipBatchWire>(value)
            .is_err());
    }

    #[test]
    fn rejects_schema_unsafe_oversized_and_owner_mismatch() {
        let mut batch = valid_batch();
        batch.schema_version = 1;
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::UnsupportedSchema { .. })
        ));

        let mut batch = valid_batch();
        batch.runs[0].source_run_id = "../run".to_string();
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::InvalidIdentifier { .. })
        ));

        let mut batch = valid_batch();
        batch.runs[0].owner = owner("bob", "athena");
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::OwnerMismatch { .. })
        ));

        let mut batch = valid_batch();
        batch.runs = (0..=MAX_RUNS)
            .map(|index| run(&format!("run-{index}"), &format!("a{index}")))
            .collect();
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::CountLimit { kind: "run", .. })
        ));
    }

    #[test]
    fn rejects_duplicate_runs_names_and_containers() {
        let mut batch = valid_batch();
        batch.runs[1].source_run_id = "run-1".to_string();
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::DuplicateRunId { .. })
        ));

        let mut batch = valid_batch();
        batch.runs[1].global_name = batch.runs[0].global_name.clone();
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::DuplicateGlobalName { .. })
        ));

        let mut batch = valid_batch();
        batch.containers.push(batch.containers[0].clone());
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::DuplicateContainer { .. })
        ));
    }

    #[test]
    fn rejects_bad_container_members_and_names() {
        let mut batch = valid_batch();
        batch.containers[0]
            .member_source_run_ids
            .push("missing".to_string());
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::MissingContainerMember { .. })
        ));

        let mut batch = valid_batch();
        batch.containers[0].global_name = "alice.athena.foo--code".to_string();
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::InvalidContainerName { .. })
        ));

        let mut batch = valid_batch();
        batch.containers[0].member_source_run_ids = vec!["run-3".to_string()];
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::FamilyContainerMemberMismatch { .. })
        ));
    }

    #[test]
    fn required_optional_and_cross_owner_targets_are_enforced() {
        let mut batch = valid_batch();
        batch.relationships[0].target = run_target("missing");
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::DanglingRequiredTarget { .. })
        ));

        let mut batch = valid_batch();
        batch.relationships.push(AgentRelationshipWire {
            kind: AgentRelationshipKind::Wait,
            source_run_id: "run-1".to_string(),
            target: AgentRelationshipTargetWire::GlobalName {
                global_name: "bob.athena.external".to_string(),
                owner: owner("bob", "athena"),
            },
            required: false,
        });
        assert!(validate_agent_relationship_batch(&batch).is_ok());
        batch.relationships.last_mut().unwrap().required = true;
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::IllegalCrossOwnerTarget { .. })
        ));
    }

    #[test]
    fn rejects_self_duplicate_and_cyclic_edges() {
        let mut batch = valid_batch();
        batch.relationships[0].target = run_target("run-2");
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::SelfEdge { .. })
        ));

        let mut batch = valid_batch();
        batch.relationships.push(relationship(
            AgentRelationshipKind::Parent,
            "run-2",
            "run-3",
        ));
        assert!(matches!(
            validate_agent_relationship_batch(&batch),
            Err(AgentRelationshipError::DuplicateOneToOneEdge { .. })
        ));

        let mut batch = valid_batch();
        batch.relationships = vec![
            relationship(AgentRelationshipKind::Wait, "run-1", "run-2"),
            relationship(AgentRelationshipKind::Wait, "run-2", "run-3"),
            relationship(AgentRelationshipKind::Wait, "run-3", "run-1"),
        ];
        let error = validate_agent_relationship_batch(&batch).unwrap_err();
        assert!(matches!(
            error,
            AgentRelationshipError::Cycle { kind: "wait", .. }
        ));
        assert!(error.to_string().contains("run-1"));
    }

    #[test]
    fn complete_mapping_rewrites_all_run_id_fields_in_input_order() {
        let batch = valid_batch();
        let mapping = BTreeMap::from([
            ("run-1".to_string(), "dest-11".to_string()),
            ("run-2".to_string(), "dest-12".to_string()),
            ("run-3".to_string(), "dest-13".to_string()),
            ("run-4".to_string(), "dest-14".to_string()),
        ]);
        let rewritten =
            rewrite_agent_relationship_batch(&batch, &mapping).unwrap();
        assert_eq!(rewritten.runs[0].source_run_id, "run-1");
        assert_eq!(rewritten.runs[0].destination_run_id, "dest-11");
        assert_eq!(
            rewritten.containers[0].member_destination_run_ids,
            ["dest-11", "dest-12"]
        );
        assert_eq!(
            rewritten.relationships[0].source_destination_run_id,
            "dest-12"
        );
        assert!(matches!(
            &rewritten.relationships[0].target,
            RewrittenAgentRelationshipTargetWire::DestinationRunId {
                source_run_id,
                destination_run_id
            } if source_run_id == "run-1" && destination_run_id == "dest-11"
        ));
    }

    #[test]
    fn mapping_must_be_complete_unique_and_exact() {
        let batch = valid_batch();
        let mut mapping = BTreeMap::from([
            ("run-1".to_string(), "dest-11".to_string()),
            ("run-2".to_string(), "dest-12".to_string()),
            ("run-3".to_string(), "dest-13".to_string()),
            ("run-4".to_string(), "dest-14".to_string()),
        ]);
        mapping.remove("run-4");
        assert!(matches!(
            rewrite_agent_relationship_batch(&batch, &mapping),
            Err(AgentRelationshipError::MissingDestinationMapping { .. })
        ));

        mapping.insert("run-4".to_string(), "dest-13".to_string());
        assert!(matches!(
            rewrite_agent_relationship_batch(&batch, &mapping),
            Err(AgentRelationshipError::DuplicateDestinationId { .. })
        ));

        mapping.insert("run-4".to_string(), "dest-14".to_string());
        mapping.insert("unknown".to_string(), "dest-15".to_string());
        assert!(matches!(
            rewrite_agent_relationship_batch(&batch, &mapping),
            Err(AgentRelationshipError::UnknownDestinationMapping { .. })
        ));
    }
}
