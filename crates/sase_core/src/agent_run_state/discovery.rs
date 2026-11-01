use std::collections::BTreeSet;

use super::window::{
    decode_discovery_cursor, encode_discovery_hood_cursor,
    encode_discovery_run_cursor, DiscoveryCursorPosition,
};
use super::wire::*;

pub fn select_agent_run_state_hoods(
    request: AgentRunStateHoodWindowRequestWire,
) -> Result<AgentRunStateHoodWindowResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    validate_project_owner(&request.project, &request.owner)?;
    validate_manifest_status(&request.manifest_status)?;
    let mut hoods = validate_manifest_hoods(request.hoods)?;
    let after = request
        .after
        .as_deref()
        .map(|cursor| {
            decode_discovery_cursor(cursor, &request.project, &request.owner)
        })
        .transpose()?;
    if let Some(after) = after.as_ref() {
        hoods.retain(|hood| hood_is_after_window_position(hood, after));
    }
    let has_more_hoods = hoods.len() > AGENT_RUN_STATE_MAX_DISCOVERY_HOODS;
    hoods.truncate(AGENT_RUN_STATE_MAX_DISCOVERY_HOODS);
    Ok(AgentRunStateHoodWindowResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        selected_hoods: hoods,
        has_more_hoods,
    })
}

pub fn select_agent_run_state_discovery_window(
    request: AgentRunStateDiscoveryWindowRequestWire,
) -> Result<AgentRunStateDiscoveryWindowResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    validate_project_owner(&request.project, &request.owner)?;
    validate_manifest_status(&request.manifest_status)?;
    if request.scan_status != AGENT_RUN_STATE_SCAN_COMPLETE {
        return Err(AgentRunStateError::InvalidRequest(
            "hood snapshot discovery was interrupted".to_string(),
        ));
    }
    if !(AGENT_RUN_STATE_WINDOW_MIN_LIMIT..=AGENT_RUN_STATE_WINDOW_MAX_LIMIT)
        .contains(&request.limit)
    {
        return Err(AgentRunStateError::InvalidRequest(format!(
            "limit must be between {AGENT_RUN_STATE_WINDOW_MIN_LIMIT} and {AGENT_RUN_STATE_WINDOW_MAX_LIMIT}"
        )));
    }

    let manifest_hoods = validate_manifest_hoods(request.manifest_hoods)?;
    let after = request
        .after
        .as_deref()
        .map(|cursor| {
            decode_discovery_cursor(cursor, &request.project, &request.owner)
        })
        .transpose()?;
    let mut expected_hoods = manifest_hoods.clone();
    if let Some(after) = after.as_ref() {
        expected_hoods
            .retain(|hood| hood_is_after_window_position(hood, after));
    }
    let has_more_hoods =
        expected_hoods.len() > AGENT_RUN_STATE_MAX_DISCOVERY_HOODS;
    expected_hoods.truncate(AGENT_RUN_STATE_MAX_DISCOVERY_HOODS);

    let actual_hoods: Vec<_> = request
        .hoods
        .iter()
        .map(|hood| hood.local_hood.clone())
        .collect();
    if actual_hoods != expected_hoods {
        return Err(AgentRunStateError::InvalidRequest(
            "discovered hood snapshots must match the bounded manifest window"
                .to_string(),
        ));
    }

    let mut seen_runs = BTreeSet::new();
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    for hood in &request.hoods {
        match hood.status.as_str() {
            AGENT_RUN_STATE_HOOD_AVAILABLE => {
                if hood.source_run_ids.is_empty() {
                    return Err(AgentRunStateError::InvalidRequest(format!(
                        "available hood {:?} must contain at least one run",
                        hood.local_hood
                    )));
                }
                add_hood_rows(hood, &mut seen_runs, &mut rows)?;
            }
            AGENT_RUN_STATE_HOOD_EMPTY => {
                if !hood.source_run_ids.is_empty() {
                    return Err(AgentRunStateError::InvalidRequest(format!(
                        "empty hood {:?} cannot contain run IDs",
                        hood.local_hood
                    )));
                }
                diagnostics.push(AgentRunStateDiscoveryDiagnosticWire {
                    local_hood: hood.local_hood.clone(),
                    kind: "empty_hood".to_string(),
                });
            }
            AGENT_RUN_STATE_HOOD_CORRUPT => {
                if !hood.source_run_ids.is_empty() {
                    return Err(AgentRunStateError::InvalidRequest(format!(
                        "corrupt hood {:?} cannot contain run IDs",
                        hood.local_hood
                    )));
                }
                diagnostics.push(AgentRunStateDiscoveryDiagnosticWire {
                    local_hood: hood.local_hood.clone(),
                    kind: "corrupt_hood".to_string(),
                });
            }
            _ => {
                return Err(AgentRunStateError::InvalidRequest(format!(
                    "hood {:?} has an unsupported discovery status",
                    hood.local_hood
                )))
            }
        }
    }

    rows.sort_by(|left, right| {
        (&left.local_hood, &left.source_run_id)
            .cmp(&(&right.local_hood, &right.source_run_id))
    });
    if let Some(after) = after.as_ref() {
        rows.retain(|row| row_is_after_cursor(row, after));
    }

    let has_more_rows = rows.len() > request.limit as usize;
    rows.truncate(request.limit as usize);
    let next_cursor = if has_more_rows {
        rows.last().map(|row| {
            encode_discovery_run_cursor(
                &request.project,
                &request.owner,
                &row.local_hood,
                &row.source_run_id,
            )
        })
    } else if has_more_hoods {
        request.hoods.last().map(|hood| {
            encode_discovery_hood_cursor(
                &request.project,
                &request.owner,
                &hood.local_hood,
            )
        })
    } else {
        None
    };
    Ok(AgentRunStateDiscoveryWindowResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        selected: rows,
        next_cursor,
        pass_complete: !has_more_rows && !has_more_hoods,
        diagnostics,
    })
}

fn add_hood_rows(
    hood: &AgentRunStateDiscoveredHoodWire,
    seen: &mut BTreeSet<(String, String)>,
    rows: &mut Vec<AgentRunStateWindowRowWire>,
) -> Result<(), AgentRunStateError> {
    for source_run_id in &hood.source_run_ids {
        if source_run_id.trim().is_empty() {
            return Err(AgentRunStateError::InvalidRequest(
                "discovered run IDs must be non-empty".to_string(),
            ));
        }
        let key = (hood.local_hood.clone(), source_run_id.clone());
        if !seen.insert(key) {
            return Err(AgentRunStateError::InvalidRequest(format!(
                "duplicate run identity {}/{}",
                hood.local_hood, source_run_id
            )));
        }
        rows.push(AgentRunStateWindowRowWire {
            local_hood: hood.local_hood.clone(),
            source_run_id: source_run_id.clone(),
        });
    }
    Ok(())
}

fn validate_manifest_hoods(
    hoods: Vec<String>,
) -> Result<Vec<String>, AgentRunStateError> {
    let mut seen = BTreeSet::new();
    for hood in &hoods {
        if hood.trim().is_empty() {
            return Err(AgentRunStateError::InvalidRequest(
                "manifest hood names must be non-empty".to_string(),
            ));
        }
        if !seen.insert(hood.clone()) {
            return Err(AgentRunStateError::InvalidRequest(format!(
                "duplicate manifest hood {hood:?}"
            )));
        }
    }
    let mut sorted = hoods;
    sorted.sort();
    Ok(sorted)
}

fn hood_is_after_window_position(
    hood: &str,
    after: &DiscoveryCursorPosition,
) -> bool {
    match after {
        DiscoveryCursorPosition::AfterRun { local_hood, .. } => {
            hood >= local_hood.as_str()
        }
        DiscoveryCursorPosition::AfterHood { local_hood } => {
            hood > local_hood.as_str()
        }
    }
}

fn row_is_after_cursor(
    row: &AgentRunStateWindowRowWire,
    after: &DiscoveryCursorPosition,
) -> bool {
    match after {
        DiscoveryCursorPosition::AfterRun {
            local_hood,
            source_run_id,
        } => {
            (&row.local_hood, &row.source_run_id) > (local_hood, source_run_id)
        }
        DiscoveryCursorPosition::AfterHood { local_hood } => {
            &row.local_hood > local_hood
        }
    }
}

fn validate_project_owner(
    project: &str,
    owner: &str,
) -> Result<(), AgentRunStateError> {
    if project.trim().is_empty() || owner.trim().is_empty() {
        return Err(AgentRunStateError::InvalidRequest(
            "project and owner must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest_status(status: &str) -> Result<(), AgentRunStateError> {
    if status != AGENT_RUN_STATE_MANIFEST_VALID {
        return Err(AgentRunStateError::InvalidRequest(
            "owner manifest is unreadable or malformed".to_string(),
        ));
    }
    Ok(())
}

fn validate_schema(schema_version: u32) -> Result<(), AgentRunStateError> {
    if schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::UnsupportedSchema(schema_version));
    }
    Ok(())
}
