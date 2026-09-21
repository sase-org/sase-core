use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_LABEL_BYTES;
use super::validation::length_key;
use super::validation::validate_identifier;
use super::validation::validate_installation_id;
use super::validation::validate_label;
use super::validation::validate_schema;
use serde::{Deserialize, Serialize};

/// Origin locator: the installation identity only.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct OriginLocatorWire {
    pub schema_version: u32,
    pub installation_id: String,
}

/// Project locator: origin plus a portable project ID, never a checkout path.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ProjectLocatorWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub project_id: String,
}

/// Stable logical agent/family locator.
///
/// Human names and provider metadata remain labels outside this identity.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct LogicalAgentLocatorWire {
    pub schema_version: u32,
    pub project: ProjectLocatorWire,
    pub agent_id: String,
    pub family_id: Option<String>,
}

/// Exact shell/run/attempt locator required for mutation targets.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceLocatorWire {
    pub schema_version: u32,
    pub logical: LogicalAgentLocatorWire,
    pub shell_id: String,
    pub run_id: String,
    pub attempt_id: String,
}

/// Owner-qualified display labels associated with a locator.
///
/// These labels make machine-hood names useful for lookup and display without
/// deriving the origin identity from those names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameRequestWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub owner_username: String,
    pub owner_machine_name: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub owner_label: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

pub fn logical_locator_key(
    locator: &LogicalAgentLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(logical_key_unchecked(locator))
}

pub fn instance_locator_key(
    locator: &AgentInstanceLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(instance_key_unchecked(locator))
}

pub fn associate_owner_display_name(
    request: &OwnerDisplayNameRequestWire,
) -> Result<OwnerDisplayNameWire, FleetContractError> {
    validate_schema("owner display name request", request.schema_version)?;
    request.logical_locator.validate()?;
    validate_identifier("owner_username", &request.owner_username)?;
    validate_identifier("owner_machine_name", &request.owner_machine_name)?;
    validate_label("display_name", &request.display_name, MAX_LABEL_BYTES)?;
    if let Some(alias) = &request.display_alias {
        validate_label("display_alias", alias, MAX_LABEL_BYTES)?;
    }
    let logical_key = logical_key_unchecked(&request.logical_locator);
    Ok(OwnerDisplayNameWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: request.logical_locator.clone(),
        logical_key,
        owner_label: format!(
            "{}.{}",
            request.owner_username.trim(),
            request.owner_machine_name.trim()
        ),
        display_name: request.display_name.trim().to_string(),
        display_alias: request
            .display_alias
            .as_ref()
            .map(|value| value.trim().to_string()),
    })
}

impl OriginLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("origin locator", self.schema_version)?;
        validate_installation_id(&self.installation_id)
    }
}

impl ProjectLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("project locator", self.schema_version)?;
        self.origin.validate()?;
        validate_identifier("project_id", &self.project_id)
    }
}

impl LogicalAgentLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("logical agent locator", self.schema_version)?;
        self.project.validate()?;
        validate_identifier("agent_id", &self.agent_id)?;
        if let Some(family_id) = &self.family_id {
            validate_identifier("family_id", family_id)?;
        }
        Ok(())
    }
}

impl AgentInstanceLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("agent instance locator", self.schema_version)?;
        self.logical.validate()?;
        validate_identifier("shell_id", &self.shell_id)?;
        validate_identifier("run_id", &self.run_id)?;
        validate_identifier("attempt_id", &self.attempt_id)?;
        Ok(())
    }
}

pub(crate) fn logical_key_unchecked(
    locator: &LogicalAgentLocatorWire,
) -> String {
    length_key([
        ("origin", locator.project.origin.installation_id.as_str()),
        ("project", locator.project.project_id.as_str()),
        ("family", locator.family_id.as_deref().unwrap_or("")),
        ("agent", locator.agent_id.as_str()),
    ])
}

pub(crate) fn instance_key_unchecked(
    locator: &AgentInstanceLocatorWire,
) -> String {
    format!(
        "{}|{}",
        logical_key_unchecked(&locator.logical),
        length_key([
            ("shell", locator.shell_id.as_str()),
            ("run", locator.run_id.as_str()),
            ("attempt", locator.attempt_id.as_str()),
        ])
    )
}
