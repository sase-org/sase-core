use super::catalog::fleet_catalog_snapshot_id;
use super::catalog::validate_fleet_catalog_snapshot_id;
use super::catalog::FleetCatalogScopeWire;
use super::cursors::StoreCursorWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_LABEL_BYTES;
use super::reads::count_logical_agents;
use super::reads::validate_fleet_logical_agent_counts;
use super::reads::FleetLogicalAgentCountsRequestWire;
use super::reads::FleetLogicalAgentCountsWire;
use super::resolution::validate_resolved_agent_summary;
use super::resolution::ResolvedAgentSummaryWire;
use super::status::ObservationFreshnessWire;
use super::validation::reject_path_like;
use super::validation::reject_secretish;
use super::validation::validate_label;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetSnapshotFreshnessWire {
    pub schema_version: u32,
    pub freshness: ObservationFreshnessWire,
    pub partial: bool,
    pub refreshed_at_unix: Option<f64>,
    /// Safe diagnostic code or short reason; never a path or backend error.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAuthoritativeSnapshotWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    #[serde(default)]
    pub catalog_scope: FleetCatalogScopeWire,
    pub catalog_snapshot_id: String,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub freshness: FleetSnapshotFreshnessWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetSummaryResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    #[serde(default)]
    pub catalog_scope: FleetCatalogScopeWire,
    pub catalog_snapshot_id: String,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
}

pub fn validate_fleet_snapshot_freshness(
    freshness: &FleetSnapshotFreshnessWire,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    validate_schema("fleet snapshot freshness", freshness.schema_version)?;
    if let Some(refreshed_at) = freshness.refreshed_at_unix {
        validate_timestamp("fleet snapshot refreshed_at_unix", refreshed_at)?;
    }
    if let Some(error) = &freshness.error {
        validate_label(
            "fleet snapshot freshness error",
            error,
            MAX_LABEL_BYTES,
        )?;
        reject_path_like("fleet snapshot freshness error", error)?;
        reject_secretish("fleet snapshot freshness error", error)?;
    }
    Ok(freshness.clone())
}

pub fn validate_fleet_authoritative_snapshot(
    snapshot: &FleetAuthoritativeSnapshotWire,
) -> Result<FleetAuthoritativeSnapshotWire, FleetContractError> {
    validate_schema("fleet authoritative snapshot", snapshot.schema_version)?;
    snapshot.cursor.validate()?;
    validate_fleet_catalog_snapshot_id(&snapshot.catalog_snapshot_id)?;
    validate_fleet_snapshot_freshness(&snapshot.freshness)?;
    validate_fleet_logical_agent_counts(
        &snapshot.counts,
        "fleet authoritative snapshot counts",
    )?;
    for summary in &snapshot.summaries {
        validate_resolved_agent_summary(summary)?;
    }
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: snapshot.summaries.clone(),
    })?;
    if counts != snapshot.counts {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot counts do not match summaries"
                .to_string(),
        ));
    }
    if snapshot.count_revision != fleet_count_revision(&snapshot.counts) {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot count_revision does not match counts"
                .to_string(),
        ));
    }
    let expected_snapshot_id =
        fleet_catalog_snapshot_id(snapshot.catalog_scope, &snapshot.summaries)?;
    if snapshot.catalog_snapshot_id != expected_snapshot_id {
        return Err(FleetContractError::Validation(
            "fleet authoritative snapshot catalog_snapshot_id does not match summaries"
                .to_string(),
        ));
    }
    Ok(snapshot.clone())
}

pub fn fleet_count_revision(
    counts: &FleetLogicalAgentCountsWire,
) -> Option<u64> {
    counts.basis.max_revision
}
