use super::dismissal::{dismissed_raw_suffix_exists, record_is_dismissed};
use super::placeholders;
use super::record_summary::RecordSummary;
use super::storage::{open_index_read_only, resolve_index_artifact_dir};
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub(super) const MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS: usize = 128;
pub(super) const MAX_RELATED_ARTIFACT_QUERY_ITERATIONS: usize = 32;

/// Return artifact directories related to one logical agent lineage.
///
/// The query is scoped to the indexed current artifact's project/workflow
/// parent, then follows direct timestamp pointers in the materialized index
/// (`parent_timestamp`, retry back/forward pointers, and retry-chain root).
/// This keeps tools-panel lookups proportional to the lineage size instead
/// of the number of historical sibling artifact directories.
pub fn query_related_agent_artifact_dirs(
    index_path: &Path,
    artifact_dir: &Path,
    seed_timestamps: &[String],
) -> Result<Vec<String>, String> {
    let conn = open_index_read_only(index_path)?;
    let current_path =
        resolve_index_artifact_dir(&conn, &artifact_dir.to_string_lossy())?;
    let Some(current) =
        select_lineage_row_by_artifact_dir(&conn, &current_path)?
    else {
        return Ok(Vec::new());
    };

    let mut timestamps: BTreeSet<String> = BTreeSet::new();
    for timestamp in seed_timestamps {
        insert_lineage_timestamp(&mut timestamps, timestamp);
    }
    insert_lineage_timestamp(&mut timestamps, &current.timestamp);
    current.add_related_timestamps(&mut timestamps);

    let mut by_dir: BTreeMap<String, IndexedLineageRow> = BTreeMap::new();
    by_dir.insert(current.artifact_dir.clone(), current.clone());

    for _ in 0..MAX_RELATED_ARTIFACT_QUERY_ITERATIONS {
        let rows = select_lineage_rows(
            &conn,
            &current.project_name,
            &current.workflow_dir_name,
            &timestamps,
        )?;
        let mut changed = false;
        for row in rows {
            changed |= row.add_related_timestamps(&mut timestamps);
            if !by_dir.contains_key(&row.artifact_dir) {
                changed = true;
            }
            by_dir.insert(row.artifact_dir.clone(), row);
        }
        if !changed {
            break;
        }
    }

    let mut rows: Vec<IndexedLineageRow> = by_dir.into_values().collect();
    rows.sort_by(|a, b| {
        (a.timestamp.as_str(), a.artifact_dir.as_str())
            .cmp(&(b.timestamp.as_str(), b.artifact_dir.as_str()))
    });

    let mut dirs: Vec<String> =
        rows.into_iter().map(|row| row.artifact_dir).collect();
    if let Some(index) = dirs.iter().position(|path| path == &current_path) {
        let current = dirs.remove(index);
        dirs.insert(0, current);
    }
    Ok(dirs)
}

/// One candidate to resolve dismissed-family ancestry for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FamilyDismissalLineageCandidateWire {
    /// Caller-chosen opaque identity used only to correlate results back to
    /// candidates.
    pub identity: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    /// Caller-resolved evidence that the candidate's own process is
    /// definitively gone: owner liveness `Dead`/`NotProcess`, with no waiting
    /// marker or pending question protecting it. Such a record is terminal
    /// even while its markers still claim an active lifecycle (a force-killed
    /// workflow run never moves `workflow_state.json` off `running`), so a
    /// dismissal of its own raw suffix is honored exactly as it is for any
    /// other terminal record.
    #[serde(default)]
    pub seed_definitively_dead: bool,
}

pub(super) struct DismissalReconcileCandidate {
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) timestamp: String,
    pub(super) record_json: String,
}

impl From<DismissalReconcileCandidate> for FamilyDismissalLineageCandidateWire {
    fn from(candidate: DismissalReconcileCandidate) -> Self {
        Self {
            identity: candidate.timestamp.clone(),
            project_name: candidate.project_name,
            workflow_dir_name: candidate.workflow_dir_name,
            timestamp: candidate.timestamp,
            seed_definitively_dead: false,
        }
    }
}

/// Whether one candidate's family root is a dismissed identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FamilyDismissalLineageResultWire {
    pub identity: String,
    pub family_root_dismissed: bool,
}

/// Resolve dismissed-family ancestry for a bounded set of candidates.
///
/// For each candidate, follows the same bounded lineage expansion as
/// [`query_related_agent_artifact_dirs`] (`parent_timestamp`/retry pointers,
/// capped by `MAX_RELATED_ARTIFACT_QUERY_ITERATIONS` and
/// `MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS`) to find the candidate's family
/// root — the related record with no `parent_timestamp` — then checks
/// whether that root's identity is a dismissed identity. A candidate flagged
/// `seed_definitively_dead` is also dismissed when its own raw suffix is a
/// dismissed identity, the same suffix match terminal records already get.
/// When no root is discoverable within the bound, the candidate is reported
/// as not dismissed: this API never manufactures a dismissal it cannot
/// support with indexed evidence.
pub fn resolve_family_dismissal_lineage(
    index_path: &Path,
    candidates: &[FamilyDismissalLineageCandidateWire],
) -> Result<Vec<FamilyDismissalLineageResultWire>, String> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let conn = open_index_read_only(index_path)?;
    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let family_root_dismissed =
            family_root_dismissed_for_candidate(&conn, candidate)?;
        results.push(FamilyDismissalLineageResultWire {
            identity: candidate.identity.clone(),
            family_root_dismissed,
        });
    }
    Ok(results)
}

pub(super) fn family_root_dismissed_for_candidate(
    conn: &Connection,
    candidate: &FamilyDismissalLineageCandidateWire,
) -> Result<bool, String> {
    let Some(seed) = select_lineage_row_by_timestamp(
        conn,
        &candidate.project_name,
        &candidate.workflow_dir_name,
        &candidate.timestamp,
    )?
    else {
        return Ok(false);
    };
    if dismissed_parent_suffix_for_seed(conn, &seed)? {
        return Ok(true);
    }
    if candidate.seed_definitively_dead
        && dismissed_raw_suffix_exists(conn, &seed.timestamp)?
    {
        return Ok(true);
    }
    let mut timestamps: BTreeSet<String> = BTreeSet::new();
    insert_lineage_timestamp(&mut timestamps, &seed.timestamp);
    seed.add_related_timestamps(&mut timestamps);
    let mut by_timestamp: BTreeMap<String, IndexedLineageRow> = BTreeMap::new();
    by_timestamp.insert(seed.timestamp.clone(), seed.clone());

    for _ in 0..MAX_RELATED_ARTIFACT_QUERY_ITERATIONS {
        let rows = select_lineage_rows(
            conn,
            &candidate.project_name,
            &candidate.workflow_dir_name,
            &timestamps,
        )?;
        let mut changed = false;
        for row in rows {
            changed |= row.add_related_timestamps(&mut timestamps);
            if !by_timestamp.contains_key(&row.timestamp) {
                changed = true;
            }
            by_timestamp.insert(row.timestamp.clone(), row);
        }
        if !changed {
            break;
        }
    }

    let root = if let Some(root) = by_timestamp
        .values()
        .find(|row| row.parent_timestamp.is_none())
        .cloned()
    {
        root
    } else {
        let Some(root) = family_root_by_agent_family(
            conn,
            &seed.project_name,
            &seed.workflow_dir_name,
            seed.agent_family.as_deref(),
        )?
        else {
            return Ok(false);
        };
        root
    };
    let Some(record) = load_record_by_artifact_dir(conn, &root.artifact_dir)?
    else {
        return Ok(false);
    };
    let summary = RecordSummary::from_record(&record);
    record_is_dismissed(conn, &record, &summary)
}

pub(super) fn dismissed_parent_suffix_for_seed(
    conn: &Connection,
    seed: &IndexedLineageRow,
) -> Result<bool, String> {
    for candidate in [
        seed.parent_timestamp.as_deref(),
        seed.retry_of_timestamp.as_deref(),
        seed.retry_chain_root_timestamp.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if dismissed_raw_suffix_exists(conn, candidate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn select_lineage_row_by_timestamp(
    conn: &Connection,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> Result<Option<IndexedLineageRow>, String> {
    conn.query_row(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_family, parent_timestamp, retry_of_timestamp,
               retried_as_timestamp, retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE project_name = ?1 AND workflow_dir_name = ?2 AND timestamp = ?3
        "#,
        params![project_name, workflow_dir_name, timestamp],
        lineage_row_from_sql,
    )
    .optional()
    .map_err(|e| e.to_string())
}

pub(super) fn load_record_by_artifact_dir(
    conn: &Connection,
    artifact_dir: &str,
) -> Result<Option<AgentArtifactRecordWire>, String> {
    let record_json: Option<String> = conn
        .query_row(
            "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let Some(record_json) = record_json else {
        return Ok(None);
    };
    decode_agent_artifact_record_json(&record_json)
        .map(Some)
        .map_err(|e| e.to_string())
}

#[derive(Debug, Clone)]
pub(super) struct IndexedLineageRow {
    pub(super) artifact_dir: String,
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) timestamp: String,
    pub(super) agent_family: Option<String>,
    pub(super) parent_timestamp: Option<String>,
    pub(super) retry_of_timestamp: Option<String>,
    pub(super) retried_as_timestamp: Option<String>,
    pub(super) retry_chain_root_timestamp: Option<String>,
}

impl IndexedLineageRow {
    pub(super) fn add_related_timestamps(
        &self,
        timestamps: &mut BTreeSet<String>,
    ) -> bool {
        let mut changed = insert_lineage_timestamp(timestamps, &self.timestamp);
        for value in [
            self.parent_timestamp.as_deref(),
            self.retry_of_timestamp.as_deref(),
            self.retried_as_timestamp.as_deref(),
            self.retry_chain_root_timestamp.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            changed |= insert_lineage_timestamp(timestamps, value);
        }
        changed
    }
}

pub(super) fn insert_lineage_timestamp(
    timestamps: &mut BTreeSet<String>,
    value: &str,
) -> bool {
    if value.is_empty()
        || timestamps.len() >= MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS
    {
        return false;
    }
    timestamps.insert(value.to_string())
}

pub(super) fn select_lineage_row_by_artifact_dir(
    conn: &Connection,
    artifact_dir: &str,
) -> Result<Option<IndexedLineageRow>, String> {
    conn.query_row(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_family, parent_timestamp, retry_of_timestamp,
               retried_as_timestamp, retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE artifact_dir = ?1
        "#,
        [artifact_dir],
        lineage_row_from_sql,
    )
    .optional()
    .map_err(|e| e.to_string())
}

pub(super) fn select_lineage_rows(
    conn: &Connection,
    project_name: &str,
    workflow_dir_name: &str,
    timestamps: &BTreeSet<String>,
) -> Result<Vec<IndexedLineageRow>, String> {
    if timestamps.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = placeholders(timestamps.len());
    let sql = format!(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_family, parent_timestamp, retry_of_timestamp,
               retried_as_timestamp, retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE project_name = ?
          AND workflow_dir_name = ?
          AND (
              timestamp IN ({placeholders})
              OR parent_timestamp IN ({placeholders})
              OR retry_of_timestamp IN ({placeholders})
              OR retried_as_timestamp IN ({placeholders})
              OR retry_chain_root_timestamp IN ({placeholders})
          )
        ORDER BY timestamp ASC, artifact_dir ASC
        "#
    );
    let mut values: Vec<String> = Vec::with_capacity(2 + timestamps.len() * 5);
    values.push(project_name.to_string());
    values.push(workflow_dir_name.to_string());
    for _ in 0..5 {
        values.extend(timestamps.iter().cloned());
    }

    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(lineage_row_from_sql(row).map_err(|e| e.to_string())?);
    }
    Ok(result)
}

pub(super) fn family_root_by_agent_family(
    conn: &Connection,
    project_name: &str,
    workflow_dir_name: &str,
    agent_family: Option<&str>,
) -> Result<Option<IndexedLineageRow>, String> {
    let Some(agent_family) = agent_family.filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    conn.query_row(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_family, parent_timestamp, retry_of_timestamp,
               retried_as_timestamp, retry_chain_root_timestamp
        FROM agent_artifacts
        WHERE project_name = ?1
          AND workflow_dir_name = ?2
          AND agent_family = ?3
          AND parent_timestamp IS NULL
        ORDER BY timestamp ASC, artifact_dir ASC
        LIMIT 1
        "#,
        params![project_name, workflow_dir_name, agent_family],
        lineage_row_from_sql,
    )
    .optional()
    .map_err(|e| e.to_string())
}

pub(super) fn lineage_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<IndexedLineageRow> {
    Ok(IndexedLineageRow {
        artifact_dir: row.get(0)?,
        project_name: row.get(1)?,
        workflow_dir_name: row.get(2)?,
        timestamp: row.get(3)?,
        agent_family: row.get(4)?,
        parent_timestamp: row.get(5)?,
        retry_of_timestamp: row.get(6)?,
        retried_as_timestamp: row.get(7)?,
        retry_chain_root_timestamp: row.get(8)?,
    })
}
