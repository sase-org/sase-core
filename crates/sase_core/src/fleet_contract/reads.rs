use super::catalog::validate_identifier_vec;
use super::content::ContentHandleWire;
use super::content::ResourceRevisionWire;
use super::cursors::StoreCursorWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_READ_DEFAULT_CONTENT_BYTES;
use super::error::FLEET_READ_MAX_CONTENT_BYTES;
use super::error::FLEET_READ_MAX_FILTER_BYTES;
use super::error::FLEET_READ_MAX_PROJECT_IDS;
use super::follows::FleetHostCountInputWire;
use super::follows::FleetHostCountWire;
use super::follows::FleetScopeCountsWire;
use super::projection::counts_as_running;
use super::resolution::validate_resolved_agent_summary;
use super::resolution::ResolvedAgentDetailWire;
use super::resolution::ResolvedAgentSummaryWire;
use super::snapshot::FleetSnapshotFreshnessWire;
use super::status::FleetRowKindWire;
use super::status::FleetStatusBucketWire;
use super::status::ObservationFreshnessWire;
use super::validation::reject_path_like;
use super::validation::validate_key;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDetailRequestWire {
    pub schema_version: u32,
    pub logical_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDetailResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub freshness: FleetSnapshotFreshnessWire,
    pub detail: ResolvedAgentDetailWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetContentReadRequestWire {
    pub schema_version: u32,
    pub handle_id: String,
    pub row_revision: ResourceRevisionWire,
    pub offset: u64,
    /// Requested byte limit. `None` means
    /// [`FLEET_READ_DEFAULT_CONTENT_BYTES`].
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetContentReadResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub handle: ContentHandleWire,
    pub offset: u64,
    pub returned_bytes: u64,
    pub total_byte_len: u64,
    pub next_offset: Option<u64>,
    pub eof: bool,
    pub supports_growth: bool,
    pub sha256: String,
    pub data_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityRequestWire {
    pub schema_version: u32,
    /// Project IDs to resolve. Empty means return every bounded project row.
    pub project_ids: Vec<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityWire {
    pub schema_version: u32,
    pub project_id: String,
    pub display_name: Option<String>,
    pub state: String,
    pub eligible: bool,
    pub launchable: bool,
    pub active_claim_count: u32,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetProjectEligibilityResponseWire {
    pub schema_version: u32,
    pub projects: Vec<FleetProjectEligibilityWire>,
    pub limit: u32,
    pub total_matching_projects: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalAgentCountsRequestWire {
    pub schema_version: u32,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalAgentCountsWire {
    pub schema_version: u32,
    pub basis: FleetCountBasisWire,
    pub logical_agent_total: u64,
    pub running: u64,
    pub waiting: u64,
    pub attention: u64,
    pub occupied_runner_slots: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCountBasisWire {
    pub schema_version: u32,
    pub input_rows: u64,
    pub selected_rows: u64,
    pub max_revision: Option<u64>,
    pub observed_at_unix_max: Option<f64>,
}

pub fn validate_fleet_logical_agent_counts(
    counts: &FleetLogicalAgentCountsWire,
    label: &str,
) -> Result<FleetLogicalAgentCountsWire, FleetContractError> {
    validate_schema(label, counts.schema_version)?;
    validate_schema(&format!("{label} basis"), counts.basis.schema_version)?;
    if let Some(observed) = counts.basis.observed_at_unix_max {
        validate_timestamp(&format!("{label} observed_at_unix_max"), observed)?;
    }
    Ok(counts.clone())
}

pub fn validate_fleet_detail_request(
    request: &FleetDetailRequestWire,
) -> Result<FleetDetailRequestWire, FleetContractError> {
    validate_schema("fleet detail request", request.schema_version)?;
    validate_key("logical_key", &request.logical_key)?;
    Ok(request.clone())
}

pub fn validate_fleet_content_read_request(
    request: &FleetContentReadRequestWire,
) -> Result<FleetContentReadRequestWire, FleetContractError> {
    validate_schema("fleet content read request", request.schema_version)?;
    validate_reference_id("content handle id", &request.handle_id)?;
    reject_path_like("content handle id", &request.handle_id)?;
    request.row_revision.validate()?;
    normalize_content_read_limit(request.limit)?;
    Ok(request.clone())
}

pub fn fleet_content_read_limit(
    request: &FleetContentReadRequestWire,
) -> Result<u64, FleetContractError> {
    validate_fleet_content_read_request(request)?;
    normalize_content_read_limit(request.limit)
}

pub fn validate_fleet_project_eligibility_request(
    request: &FleetProjectEligibilityRequestWire,
) -> Result<FleetProjectEligibilityRequestWire, FleetContractError> {
    validate_schema(
        "fleet project eligibility request",
        request.schema_version,
    )?;
    validate_identifier_vec(
        "project_ids",
        &request.project_ids,
        FLEET_READ_MAX_PROJECT_IDS,
        FLEET_READ_MAX_FILTER_BYTES,
    )?;
    normalize_project_limit(request.limit)?;
    Ok(request.clone())
}

pub fn fleet_project_eligibility_limit(
    request: &FleetProjectEligibilityRequestWire,
) -> Result<u32, FleetContractError> {
    validate_fleet_project_eligibility_request(request)?;
    normalize_project_limit(request.limit)
}

pub fn count_logical_agents(
    request: &FleetLogicalAgentCountsRequestWire,
) -> Result<FleetLogicalAgentCountsWire, FleetContractError> {
    validate_schema(
        "fleet logical agent counts request",
        request.schema_version,
    )?;
    let mut selected: BTreeMap<String, ResolvedAgentSummaryWire> =
        BTreeMap::new();
    for summary in &request.summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        if summary.row_kind != FleetRowKindWire::AgentTurn {
            continue;
        }
        if !summary.current_instance {
            continue;
        }
        match selected.get(&summary.logical_key) {
            Some(existing)
                if existing.row_revision.revision
                    > summary.row_revision.revision => {}
            Some(existing)
                if existing.row_revision.revision
                    == summary.row_revision.revision =>
            {
                if existing.exact_key != summary.exact_key {
                    return Err(FleetContractError::Validation(format!(
                        "ambiguous current instances for logical identity {} at revision {}",
                        summary.logical_key, summary.row_revision.revision
                    )));
                }
            }
            _ => {
                selected.insert(summary.logical_key.clone(), summary);
            }
        }
    }
    let mut running = 0_u64;
    let mut waiting = 0_u64;
    let mut attention = 0_u64;
    let mut occupied = 0_u64;
    let mut max_revision: Option<u64> = None;
    let mut observed_at_unix_max: Option<f64> = None;
    for summary in selected.values() {
        if counts_as_running(summary) {
            running = running.saturating_add(1);
        }
        if summary.status_bucket == FleetStatusBucketWire::Waiting {
            waiting = waiting.saturating_add(1);
        }
        if summary.needs_attention {
            attention = attention.saturating_add(1);
        }
        if summary.occupied_runner_slot {
            occupied = occupied.saturating_add(1);
        }
        max_revision =
            Some(max_revision.map_or(summary.row_revision.revision, |value| {
                value.max(summary.row_revision.revision)
            }));
        observed_at_unix_max = Some(
            observed_at_unix_max.map_or(summary.observed_at_unix, |value| {
                value.max(summary.observed_at_unix)
            }),
        );
    }
    Ok(FleetLogicalAgentCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        basis: FleetCountBasisWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            input_rows: request.summaries.len() as u64,
            selected_rows: selected.len() as u64,
            max_revision,
            observed_at_unix_max,
        },
        logical_agent_total: selected.len() as u64,
        running,
        waiting,
        attention,
        occupied_runner_slots: occupied,
    })
}

pub(crate) fn count_scope(
    local_summaries: &[ResolvedAgentSummaryWire],
    hosts: &[FleetHostCountInputWire],
    label: &str,
    allow_authoritative_counts: bool,
) -> Result<FleetScopeCountsWire, FleetContractError> {
    let mut host_origins = BTreeSet::new();
    let mut unknown_origins = Vec::new();
    let mut host_counts = Vec::new();
    let local_counts =
        count_logical_agents(&FleetLogicalAgentCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            summaries: local_summaries.to_vec(),
        })?;
    let mut counts = empty_logical_counts();
    add_logical_counts(&mut counts, &local_counts);
    for host in hosts {
        host.validate(label)?;
        let origin_key = host.origin.installation_id.clone();
        if !host_origins.insert(origin_key.clone()) {
            return Err(FleetContractError::Validation(format!(
                "{label} contains duplicate origin {origin_key}"
            )));
        }
        let counts_for_host = if allow_authoritative_counts {
            match &host.authoritative_counts {
                Some(counts) => validate_fleet_logical_agent_counts(
                    counts,
                    "fleet host authoritative counts",
                )?,
                None => {
                    count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                        summaries: host.summaries.clone(),
                    })?
                }
            }
        } else {
            count_logical_agents(&FleetLogicalAgentCountsRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                summaries: host.summaries.clone(),
            })?
        };
        let partial = host.partial
            || matches!(
                host.freshness,
                ObservationFreshnessWire::Stale
                    | ObservationFreshnessWire::Unknown
            );
        if partial {
            unknown_origins.push(origin_key);
        }
        let host_observed_at = max_optional_f64(
            host.observed_at_unix,
            counts_for_host.basis.observed_at_unix_max,
        );
        host_counts.push(FleetHostCountWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: host.origin.clone(),
            counts: counts_for_host.clone(),
            partial,
            observed_at_unix: host_observed_at,
        });
        add_logical_counts(&mut counts, &counts_for_host);
        counts.basis.observed_at_unix_max = max_optional_f64(
            counts.basis.observed_at_unix_max,
            host_observed_at,
        );
    }
    unknown_origins.sort();
    let observed_at_unix_max = counts.basis.observed_at_unix_max;
    Ok(FleetScopeCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        counts,
        partial: !unknown_origins.is_empty(),
        observed_at_unix_max,
        unknown_origins,
        host_counts,
    })
}

fn empty_logical_counts() -> FleetLogicalAgentCountsWire {
    FleetLogicalAgentCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        basis: FleetCountBasisWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            input_rows: 0,
            selected_rows: 0,
            max_revision: None,
            observed_at_unix_max: None,
        },
        logical_agent_total: 0,
        running: 0,
        waiting: 0,
        attention: 0,
        occupied_runner_slots: 0,
    }
}

fn add_logical_counts(
    total: &mut FleetLogicalAgentCountsWire,
    counts: &FleetLogicalAgentCountsWire,
) {
    total.basis.input_rows = total
        .basis
        .input_rows
        .saturating_add(counts.basis.input_rows);
    total.basis.selected_rows = total
        .basis
        .selected_rows
        .saturating_add(counts.basis.selected_rows);
    total.basis.max_revision =
        max_optional_u64(total.basis.max_revision, counts.basis.max_revision);
    total.basis.observed_at_unix_max = max_optional_f64(
        total.basis.observed_at_unix_max,
        counts.basis.observed_at_unix_max,
    );
    total.logical_agent_total = total
        .logical_agent_total
        .saturating_add(counts.logical_agent_total);
    total.running = total.running.saturating_add(counts.running);
    total.waiting = total.waiting.saturating_add(counts.waiting);
    total.attention = total.attention.saturating_add(counts.attention);
    total.occupied_runner_slots = total
        .occupied_runner_slots
        .saturating_add(counts.occupied_runner_slots);
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(crate) fn max_optional_f64(
    left: Option<f64>,
    right: Option<f64>,
) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn normalize_content_read_limit(
    limit: Option<u64>,
) -> Result<u64, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_DEFAULT_CONTENT_BYTES);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet content limit must be positive".to_string(),
        ));
    }
    if limit > FLEET_READ_MAX_CONTENT_BYTES {
        return Err(FleetContractError::Validation(format!(
            "fleet content limit exceeds {FLEET_READ_MAX_CONTENT_BYTES}"
        )));
    }
    Ok(limit)
}

fn normalize_project_limit(
    limit: Option<u32>,
) -> Result<u32, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_MAX_PROJECT_IDS as u32);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet project eligibility limit must be positive".to_string(),
        ));
    }
    if limit as usize > FLEET_READ_MAX_PROJECT_IDS {
        return Err(FleetContractError::Validation(format!(
            "fleet project eligibility limit exceeds {FLEET_READ_MAX_PROJECT_IDS}"
        )));
    }
    Ok(limit)
}
