//! Indexed lane for failure-triage owner candidates.
//!
//! Filters on the materialized `issue_type`, `task_type`, and `created_at`
//! columns and projects only `title`, `closed_at`, and the two locator
//! task fields out of each surviving row with `json_extract`, so no full
//! issue (description, design, notes, relationships) is decoded or
//! copied. Eligibility and recency go through the same policy as the
//! replay lane in `crate::bead::triage_owners`.

use std::path::Path;

use rusqlite::Connection;

use crate::bead::triage_owners::{
    is_owner_task_type, project_owner_candidates,
    BeadTriageOwnerCandidatesWire, OwnerCandidateRow, TRIAGE_OWNER_TASK_TYPES,
};
use crate::bead::wire::BeadError;

use super::location::read_model_cache_path_for_store;
use super::queries::with_fresh_cache_at;

/// Owner candidates from a fresh cache, or `None` to fall back to replay.
pub fn cached_triage_owner_candidates(
    beads_dir: &Path,
    now_ts: i64,
) -> Result<Option<Result<BeadTriageOwnerCandidatesWire, BeadError>>, BeadError>
{
    let Some(cache_path) = read_model_cache_path_for_store(beads_dir) else {
        return Ok(None);
    };
    with_fresh_cache_at(beads_dir, &cache_path, |connection| {
        Ok(owner_candidates_in(connection, now_ts))
    })
}

fn owner_candidates_in(
    connection: &Connection,
    now_ts: i64,
) -> Result<BeadTriageOwnerCandidatesWire, BeadError> {
    let placeholders = (1..=TRIAGE_OWNER_TASK_TYPES.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id, task_type, status = 'closed', \
         json_extract(row, '$.title'), \
         json_extract(row, '$.closed_at'), \
         json_extract(row, '$.task_type_fields.node_id'), \
         json_extract(row, '$.task_type_fields.location') \
         FROM issues WHERE issue_type = 'task' AND task_type IN ({placeholders}) \
         ORDER BY created_at ASC, position ASC"
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(TRIAGE_OWNER_TASK_TYPES), |row| {
            Ok((
                row.get::<_, String>(1)?,
                OwnerCandidateRow {
                    id: row.get(0)?,
                    closed: row.get(2)?,
                    title: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    closed_at: row.get(4)?,
                    node_id_field: row.get(5)?,
                    location_field: row.get(6)?,
                },
            ))
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut eligible = Vec::new();
    for row in rows {
        let (task_type, row) =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        // The SQL filter already selected these; the shared predicate
        // keeps both lanes on one eligibility rule.
        if is_owner_task_type(Some(task_type.as_str())) {
            eligible.push(row);
        }
    }
    Ok(project_owner_candidates(eligible, now_ts))
}
