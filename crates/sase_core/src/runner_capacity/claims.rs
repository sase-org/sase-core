use std::collections::{BTreeMap, BTreeSet};

use super::records::{effective_weight, is_occupying_record, record_index};
use super::wire::{
    diagnostic, RunnerCapacityClaimWire, RunnerCapacityDiagnosticWire,
    RunnerCapacityRecordWire,
};

const SERIAL_AGENT_SESSION_CLAIM_KIND: &str = "serial_session";

#[derive(Debug, Clone)]
pub(super) struct ClaimLineage {
    pub(super) owner_key: String,
    pub(super) project_name: String,
    pub(super) claim_kind: String,
    pub(super) lineage_key: String,
    pub(super) owner_artifact_dir: Option<String>,
    pub(super) owner_timestamp: Option<String>,
}

struct ClaimAccumulator {
    lineage: ClaimLineage,
    artifact_dirs: Vec<String>,
    max_weight: f64,
    invalid: bool,
}

pub(super) fn build_claims(
    records: &[RunnerCapacityRecordWire],
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> (Vec<RunnerCapacityClaimWire>, bool) {
    let index = record_index(records);
    let mut groups: BTreeMap<String, ClaimAccumulator> = BTreeMap::new();
    let mut invalid_live_claim = false;
    for record in records {
        if is_occupying_record(record) {
            let lineage = claim_lineage(record, &index);
            let entry =
                groups.entry(lineage.owner_key.clone()).or_insert_with(|| {
                    ClaimAccumulator {
                        lineage,
                        artifact_dirs: Vec::new(),
                        max_weight: 0.0,
                        invalid: false,
                    }
                });
            entry.artifact_dirs.push(record.artifact_dir.clone());
            match effective_weight(record) {
                Ok(weight) => {
                    if weight > entry.max_weight {
                        entry.max_weight = weight;
                    }
                }
                Err(message) => {
                    entry.invalid = true;
                    invalid_live_claim = true;
                    diagnostics.push(diagnostic(
                        "invalid-live-claim-weight",
                        &message,
                        Some(record.artifact_dir.clone()),
                    ));
                }
            }
        }
    }

    let mut claims = Vec::new();
    for (_, mut group) in groups {
        if group.invalid {
            continue;
        }
        group.artifact_dirs.sort();
        claims.push(RunnerCapacityClaimWire {
            owner_key: group.lineage.owner_key,
            project_name: group.lineage.project_name,
            claim_kind: group.lineage.claim_kind,
            lineage_key: group.lineage.lineage_key,
            occupied_lanes: 1,
            occupied_capacity: group.max_weight,
            owner_artifact_dir: group.lineage.owner_artifact_dir,
            owner_timestamp: group.lineage.owner_timestamp,
            artifact_dirs: group.artifact_dirs,
        });
    }
    (claims, invalid_live_claim)
}

pub(super) fn active_claim_keys(
    claims: &[RunnerCapacityClaimWire],
) -> BTreeSet<String> {
    claims
        .iter()
        .filter(|claim| claim_is_reusable(claim))
        .map(|claim| claim.owner_key.clone())
        .collect()
}

/// A claim whose live occupiers are all explicit zero-weight records (e.g. an
/// epic-launch monitor) holds no real capacity footprint: a successor must
/// not reuse it for free and instead acquires its own capacity.
pub(super) fn claim_is_reusable(claim: &RunnerCapacityClaimWire) -> bool {
    claim.occupied_capacity != 0.0
}

pub(super) fn claim_lineage(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> ClaimLineage {
    let mut visiting = BTreeSet::new();
    claim_lineage_inner(record, index, &mut visiting)
}

fn claim_lineage_inner(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
    visiting: &mut BTreeSet<(String, String)>,
) -> ClaimLineage {
    if let Some(owner) =
        normalized_owner_key(record.runner_claim_owner_key.as_deref())
    {
        return explicit_claim_lineage(record, owner);
    }
    if record.agent_session_parallel {
        return parallel_claim_lineage(record);
    }
    if let Some(parent_timestamp) =
        normalized_owner_key(record.parent_timestamp.as_deref())
    {
        let key = (record.project_name.clone(), parent_timestamp.clone());
        if visiting
            .insert((record.project_name.clone(), record.timestamp.clone()))
        {
            if let Some(parent) = index.get(&key) {
                return claim_lineage_inner(parent, index, visiting);
            }
        }
    }
    serial_or_standalone_claim_lineage(record)
}

fn normalized_owner_key(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn explicit_claim_lineage(
    record: &RunnerCapacityRecordWire,
    owner: String,
) -> ClaimLineage {
    let claim_kind = if record.agent_session_parallel {
        "parallel_member"
    } else if record.parent_timestamp.is_some()
        || record
            .agent_session
            .as_deref()
            .is_some_and(|value| !value.is_empty())
    {
        SERIAL_AGENT_SESSION_CLAIM_KIND
    } else {
        "standalone"
    };
    ClaimLineage {
        owner_key: format!("{}:{owner}", record.project_name),
        project_name: record.project_name.clone(),
        claim_kind: claim_kind.to_string(),
        lineage_key: owner,
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

fn parallel_claim_lineage(record: &RunnerCapacityRecordWire) -> ClaimLineage {
    let agent_session = record
        .agent_session
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("standalone");
    let lineage_key = format!("{agent_session}:parallel:{}", record.timestamp);
    ClaimLineage {
        owner_key: format!("{}:{lineage_key}", record.project_name),
        project_name: record.project_name.clone(),
        claim_kind: "parallel_member".to_string(),
        lineage_key,
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

fn serial_or_standalone_claim_lineage(
    record: &RunnerCapacityRecordWire,
) -> ClaimLineage {
    if let Some(agent_session) = record
        .agent_session
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return ClaimLineage {
            owner_key: format!("{}:{agent_session}", record.project_name),
            project_name: record.project_name.clone(),
            claim_kind: SERIAL_AGENT_SESSION_CLAIM_KIND.to_string(),
            lineage_key: agent_session.to_string(),
            owner_artifact_dir: Some(record.artifact_dir.clone()),
            owner_timestamp: Some(record.timestamp.clone()),
        };
    }
    ClaimLineage {
        owner_key: format!("{}:{}", record.project_name, record.timestamp),
        project_name: record.project_name.clone(),
        claim_kind: "standalone".to_string(),
        lineage_key: record.timestamp.clone(),
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

pub(super) fn inherited_lineage_weight(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> Result<Option<f64>, String> {
    if let Some(parent_timestamp) =
        normalized_owner_key(record.parent_timestamp.as_deref())
    {
        if let Some(parent) =
            index.get(&(record.project_name.clone(), parent_timestamp))
        {
            return lineage_record_weight(parent, index).map(Some);
        }
    }
    let Some(owner) =
        normalized_owner_key(record.runner_claim_owner_key.as_deref())
    else {
        return Ok(None);
    };
    let owner_key = format!("{}:{owner}", record.project_name);
    let mut inherited: Option<f64> = None;
    for other in index.values() {
        if claim_lineage(other, index).owner_key == owner_key {
            let weight = lineage_record_weight(other, index)?;
            inherited =
                Some(inherited.map_or(weight, |current| current.max(weight)));
        }
    }
    Ok(inherited)
}

fn lineage_record_weight(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> Result<f64, String> {
    let mut visiting = BTreeSet::new();
    lineage_record_weight_inner(record, index, &mut visiting)
}

fn lineage_record_weight_inner(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
    visiting: &mut BTreeSet<(String, String)>,
) -> Result<f64, String> {
    if !record.queue_weight_explicit {
        if let Some(parent_timestamp) =
            normalized_owner_key(record.parent_timestamp.as_deref())
        {
            if let Some(parent) =
                index.get(&(record.project_name.clone(), parent_timestamp))
            {
                let key =
                    (record.project_name.clone(), record.timestamp.clone());
                if visiting.insert(key) {
                    return lineage_record_weight_inner(
                        parent, index, visiting,
                    );
                }
            }
        }
    }
    effective_weight(record)
}
