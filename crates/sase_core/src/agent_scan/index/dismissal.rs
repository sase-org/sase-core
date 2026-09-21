use super::index_wire::{
    AgentArtifactIndexUpdateWire, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
};
use super::lineage::{
    insert_lineage_timestamp, DismissalReconcileCandidate,
    FamilyDismissalLineageCandidateWire, IndexedLineageRow,
    MAX_RELATED_ARTIFACT_QUERY_ITERATIONS,
};
use super::record_index_sql_statements;
use super::record_summary::RecordSummary;
use super::selection::{
    dismissed_identity_for_record, is_terminal_workflow_status,
    record_is_definitively_dead_for_dismissal_backfill,
    DISMISSED_NORMAL_VISIBILITY_FILTER,
};
use super::storage::open_index;
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
};
use rusqlite::{params, params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

pub(super) const DISMISSED_IDENTITY_SQL_BATCH: usize = 200;

/// Replace the dismissed identity table used by normal index visibility.
///
/// Default path is a diff: only identities missing from the current table
/// are inserted, and only identities missing from `dismissed` are deleted.
/// [`replace_agent_artifact_index_dismissed_agents_with_force`] keeps the
/// unconditional full-table rewrite used by `force` callers.
pub fn replace_agent_artifact_index_dismissed_agents(
    index_path: &Path,
    dismissed: &[AgentCleanupIdentityWire],
) -> Result<AgentArtifactIndexUpdateWire, String> {
    replace_agent_artifact_index_dismissed_agents_with_force(
        index_path, dismissed, false,
    )
}

/// Replace dismissed identities, optionally rewriting the whole table.
pub fn replace_agent_artifact_index_dismissed_agents_with_force(
    index_path: &Path,
    dismissed: &[AgentCleanupIdentityWire],
    force: bool,
) -> Result<AgentArtifactIndexUpdateWire, String> {
    let mut sql_statements = 0u64;
    let mut conn = open_index(index_path)?;
    let desired: BTreeSet<AgentCleanupIdentityWire> =
        dismissed.iter().cloned().collect();
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    sql_statements += 1;

    let (rows_indexed, rows_deleted) = if force {
        let deleted = tx
            .execute("DELETE FROM dismissed_agents", [])
            .map_err(|e| e.to_string())? as u64;
        sql_statements += 1;
        sql_statements += insert_dismissed_identities(&tx, desired.iter())?;
        (desired.len() as u64, deleted)
    } else {
        let current = load_dismissed_identity_set(&tx)?;
        sql_statements += 1;
        let removed: Vec<_> = current.difference(&desired).cloned().collect();
        let added: Vec<_> = desired.difference(&current).cloned().collect();
        sql_statements += delete_dismissed_identities(&tx, &removed)?;
        sql_statements += insert_dismissed_identities(&tx, added.iter())?;
        (desired.len() as u64, removed.len() as u64)
    };

    tx.commit().map_err(|e| e.to_string())?;
    sql_statements += 1;
    record_index_sql_statements(sql_statements);

    Ok(AgentArtifactIndexUpdateWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: String::new(),
        rows_indexed,
        rows_deleted,
        rows_skipped: 0,
        hidden_terminal_rows_retained: 0,
        hidden_terminal_rows_pruned: 0,
    })
}

/// Counts from reconciling visible members of already-dismissed families.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexDismissalReconcileWire {
    pub schema_version: u32,
    pub index_path: String,
    pub dry_run: bool,
    pub candidate_rows: u64,
    pub rows_backfilled: u64,
    pub rows_already_dismissed: u64,
    pub rows_skipped_live_or_unknown: u64,
    pub rows_skipped_no_dismissed_root: u64,
    pub rows_skipped_decode_errors: u64,
}

/// Back-fill dismissed identities for dead members of already-dismissed families.
///
/// The visible-row filter only hides an indexed row when that row's own
/// suffix/type identity is present in `dismissed_agents`. Older family
/// dismissals could leave unloaded member records visible even though their
/// family root was dismissed. This pass discovers those rows from indexed
/// lineage and records their identities without deleting artifacts.
pub fn reconcile_agent_artifact_index_dismissed_family_members(
    index_path: &Path,
    dry_run: bool,
) -> Result<AgentArtifactIndexDismissalReconcileWire, String> {
    let mut conn = open_index(index_path)?;
    let mut sql_statements = 0u64;
    let dismissed =
        DismissedIndex::from_identities(load_dismissed_identity_set(&conn)?);
    sql_statements += 1;
    let snapshot = select_dismissal_reconcile_snapshot(&conn)?;
    sql_statements += 1;
    let mut report = AgentArtifactIndexDismissalReconcileWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        dry_run,
        candidate_rows: snapshot.candidates.len() as u64,
        ..AgentArtifactIndexDismissalReconcileWire::default()
    };
    let mut additions = BTreeSet::new();

    for candidate in snapshot.candidates {
        let family_key = (
            candidate.project_name.clone(),
            candidate.workflow_dir_name.clone(),
        );
        let Ok(record) =
            decode_agent_artifact_record_json(&candidate.record_json)
        else {
            report.rows_skipped_decode_errors += 1;
            continue;
        };
        if !record_is_definitively_dead_for_dismissal_backfill(&record) {
            report.rows_skipped_live_or_unknown += 1;
            continue;
        }
        let summary = RecordSummary::from_record(&record);
        if record_is_dismissed_in_memory(&record, &summary, &dismissed) {
            report.rows_already_dismissed += 1;
            continue;
        }
        let lineage_candidate = candidate.into();
        let Some(rows) = snapshot.lineage_by_family.get(&family_key) else {
            report.rows_skipped_no_dismissed_root += 1;
            continue;
        };
        if !family_root_dismissed_from_snapshot(
            &lineage_candidate,
            rows,
            &dismissed,
        ) {
            report.rows_skipped_no_dismissed_root += 1;
            continue;
        }
        additions.insert(dismissed_identity_for_record(&record, &summary));
    }

    report.rows_backfilled = additions.len() as u64;
    if dry_run || additions.is_empty() {
        record_index_sql_statements(sql_statements);
        return Ok(report);
    }

    let tx = conn.transaction().map_err(|e| e.to_string())?;
    sql_statements += 1;
    sql_statements += insert_dismissed_identities(&tx, additions.iter())?;
    tx.commit().map_err(|e| e.to_string())?;
    sql_statements += 1;
    record_index_sql_statements(sql_statements);
    Ok(report)
}

#[cfg(test)]
pub(super) fn select_dismissal_reconcile_candidates(
    conn: &Connection,
) -> Result<Vec<DismissalReconcileCandidate>, String> {
    let sql = format!(
        r#"
        SELECT project_name, workflow_dir_name, timestamp, record_json
        FROM agent_artifacts
        WHERE hidden = 0
          AND (
              parent_timestamp IS NOT NULL
              OR agent_family IS NOT NULL
              OR retry_of_timestamp IS NOT NULL
              OR retry_chain_root_timestamp IS NOT NULL
          )
          AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
        ORDER BY project_name ASC, workflow_dir_name ASC, timestamp ASC
        "#
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut candidates = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        candidates.push(DismissalReconcileCandidate {
            project_name: row.get(0).map_err(|e| e.to_string())?,
            workflow_dir_name: row.get(1).map_err(|e| e.to_string())?,
            timestamp: row.get(2).map_err(|e| e.to_string())?,
            record_json: row.get(3).map_err(|e| e.to_string())?,
        });
    }
    Ok(candidates)
}

#[derive(Debug, Clone)]
pub(super) struct DismissalLineageRow {
    pub(super) lineage: IndexedLineageRow,
    pub(super) agent_type: String,
    pub(super) cl_name: Option<String>,
    pub(super) has_done_marker: bool,
    pub(super) has_running_marker: bool,
    pub(super) has_waiting_marker: bool,
    pub(super) has_workflow_state: bool,
    pub(super) workflow_status: Option<String>,
}

pub(super) struct DismissalReconcileSnapshot {
    pub(super) candidates: Vec<DismissalReconcileCandidate>,
    pub(super) lineage_by_family:
        HashMap<(String, String), Vec<DismissalLineageRow>>,
}

pub(super) struct DismissedIndex {
    pub(super) by_suffix: HashMap<String, Vec<(String, String)>>,
}

impl DismissedIndex {
    pub(super) fn from_identities(
        identities: BTreeSet<AgentCleanupIdentityWire>,
    ) -> Self {
        let mut by_suffix: HashMap<String, Vec<(String, String)>> =
            HashMap::new();
        for identity in identities {
            let Some(suffix) = identity.raw_suffix else {
                continue;
            };
            by_suffix
                .entry(suffix)
                .or_default()
                .push((identity.agent_type, identity.cl_name));
        }
        Self { by_suffix }
    }

    pub(super) fn suffix_exists(&self, suffix: &str) -> bool {
        self.by_suffix.contains_key(suffix)
    }

    pub(super) fn matches(
        &self,
        timestamp: &str,
        terminal_or_inert: bool,
        dismissed_agent_type: &str,
        cl_name: Option<&str>,
    ) -> bool {
        let Some(entries) = self.by_suffix.get(timestamp) else {
            return false;
        };
        entries.iter().any(|(agent_type, dismissed_cl)| {
            terminal_or_inert
                || (agent_type == dismissed_agent_type
                    && (Some(dismissed_cl.as_str()) == cl_name
                        || dismissed_cl == "unknown"
                        || cl_name.is_none()))
        })
    }
}

pub(super) fn load_dismissed_identity_set(
    conn: &Connection,
) -> Result<BTreeSet<AgentCleanupIdentityWire>, String> {
    let mut stmt = conn
        .prepare("SELECT agent_type, cl_name, raw_suffix FROM dismissed_agents")
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut identities = BTreeSet::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        identities.insert(AgentCleanupIdentityWire {
            agent_type: row.get(0).map_err(|e| e.to_string())?,
            cl_name: row.get(1).map_err(|e| e.to_string())?,
            raw_suffix: row.get(2).map_err(|e| e.to_string())?,
        });
    }
    Ok(identities)
}

pub(super) fn insert_dismissed_identities<'a>(
    conn: &Connection,
    identities: impl IntoIterator<Item = &'a AgentCleanupIdentityWire>,
) -> Result<u64, String> {
    let identities: Vec<&AgentCleanupIdentityWire> =
        identities.into_iter().collect();
    if identities.is_empty() {
        return Ok(0);
    }
    let mut statements = 0u64;
    for chunk in identities.chunks(DISMISSED_IDENTITY_SQL_BATCH) {
        let mut sql = String::from(
            "INSERT OR REPLACE INTO dismissed_agents \
             (agent_type, cl_name, raw_suffix) VALUES ",
        );
        for index in 0..chunk.len() {
            if index > 0 {
                sql.push_str(", ");
            }
            let base = index * 3;
            sql.push_str(&format!(
                "(?{}, ?{}, ?{})",
                base + 1,
                base + 2,
                base + 3
            ));
        }
        let mut params: Vec<rusqlite::types::Value> =
            Vec::with_capacity(chunk.len() * 3);
        for identity in chunk {
            params.push(identity.agent_type.clone().into());
            params.push(identity.cl_name.clone().into());
            params.push(match &identity.raw_suffix {
                Some(value) => value.clone().into(),
                None => rusqlite::types::Value::Null,
            });
        }
        conn.execute(&sql, params_from_iter(params.iter()))
            .map_err(|e| e.to_string())?;
        statements += 1;
    }
    Ok(statements)
}

pub(super) fn delete_dismissed_identities(
    conn: &Connection,
    identities: &[AgentCleanupIdentityWire],
) -> Result<u64, String> {
    if identities.is_empty() {
        return Ok(0);
    }
    let mut statements = 0u64;
    let mut stmt = conn
        .prepare(
            "DELETE FROM dismissed_agents \
             WHERE agent_type = ?1 AND cl_name = ?2 AND raw_suffix IS ?3",
        )
        .map_err(|e| e.to_string())?;
    for chunk in identities.chunks(DISMISSED_IDENTITY_SQL_BATCH) {
        for identity in chunk {
            stmt.execute(params![
                identity.agent_type,
                identity.cl_name,
                identity.raw_suffix,
            ])
            .map_err(|e| e.to_string())?;
            statements += 1;
        }
    }
    Ok(statements)
}

pub(super) fn select_dismissal_reconcile_snapshot(
    conn: &Connection,
) -> Result<DismissalReconcileSnapshot, String> {
    let sql = format!(
        r#"
        WITH candidates AS (
            SELECT
                artifact_dir,
                project_name,
                workflow_dir_name,
                timestamp,
                record_json,
                agent_family,
                parent_timestamp,
                retry_of_timestamp,
                retried_as_timestamp,
                retry_chain_root_timestamp,
                agent_type,
                cl_name,
                has_done_marker,
                has_running_marker,
                has_waiting_marker,
                has_workflow_state,
                workflow_status
            FROM agent_artifacts
            WHERE hidden = 0
              AND (
                  parent_timestamp IS NOT NULL
                  OR agent_family IS NOT NULL
                  OR retry_of_timestamp IS NOT NULL
                  OR retry_chain_root_timestamp IS NOT NULL
              )
              AND {DISMISSED_NORMAL_VISIBILITY_FILTER}
        )
        SELECT
            1 AS is_candidate,
            project_name,
            workflow_dir_name,
            timestamp,
            record_json,
            artifact_dir,
            agent_family,
            parent_timestamp,
            retry_of_timestamp,
            retried_as_timestamp,
            retry_chain_root_timestamp,
            agent_type,
            cl_name,
            has_done_marker,
            has_running_marker,
            has_waiting_marker,
            has_workflow_state,
            workflow_status
        FROM candidates
        UNION ALL
        SELECT
            0 AS is_candidate,
            a.project_name,
            a.workflow_dir_name,
            a.timestamp,
            NULL,
            a.artifact_dir,
            a.agent_family,
            a.parent_timestamp,
            a.retry_of_timestamp,
            a.retried_as_timestamp,
            a.retry_chain_root_timestamp,
            a.agent_type,
            a.cl_name,
            a.has_done_marker,
            a.has_running_marker,
            a.has_waiting_marker,
            a.has_workflow_state,
            a.workflow_status
        FROM agent_artifacts a
        WHERE EXISTS (
            SELECT 1 FROM candidates c
            WHERE c.project_name = a.project_name
              AND c.workflow_dir_name = a.workflow_dir_name
        )
        ORDER BY is_candidate DESC, project_name ASC,
                 workflow_dir_name ASC, timestamp ASC
        "#
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut candidates = Vec::new();
    let mut lineage_by_family: HashMap<
        (String, String),
        Vec<DismissalLineageRow>,
    > = HashMap::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let is_candidate: i64 = row.get(0).map_err(|e| e.to_string())?;
        if is_candidate == 1 {
            candidates.push(DismissalReconcileCandidate {
                project_name: row.get(1).map_err(|e| e.to_string())?,
                workflow_dir_name: row.get(2).map_err(|e| e.to_string())?,
                timestamp: row.get(3).map_err(|e| e.to_string())?,
                record_json: row.get(4).map_err(|e| e.to_string())?,
            });
            continue;
        }
        let lineage_row =
            dismissal_lineage_row_from_sql(row).map_err(|e| e.to_string())?;
        let key = (
            lineage_row.lineage.project_name.clone(),
            lineage_row.lineage.workflow_dir_name.clone(),
        );
        lineage_by_family.entry(key).or_default().push(lineage_row);
    }
    Ok(DismissalReconcileSnapshot {
        candidates,
        lineage_by_family,
    })
}

pub(super) fn dismissal_lineage_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DismissalLineageRow> {
    Ok(DismissalLineageRow {
        lineage: IndexedLineageRow {
            artifact_dir: row.get(5)?,
            project_name: row.get(1)?,
            workflow_dir_name: row.get(2)?,
            timestamp: row.get(3)?,
            agent_family: row.get(6)?,
            parent_timestamp: row.get(7)?,
            retry_of_timestamp: row.get(8)?,
            retried_as_timestamp: row.get(9)?,
            retry_chain_root_timestamp: row.get(10)?,
        },
        agent_type: row.get(11)?,
        cl_name: row.get(12)?,
        has_done_marker: row.get::<_, i64>(13)? != 0,
        has_running_marker: row.get::<_, i64>(14)? != 0,
        has_waiting_marker: row.get::<_, i64>(15)? != 0,
        has_workflow_state: row.get::<_, i64>(16)? != 0,
        workflow_status: row.get(17)?,
    })
}

pub(super) fn record_is_dismissed_in_memory(
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
    dismissed: &DismissedIndex,
) -> bool {
    let workflow_terminal = record
        .workflow_state
        .as_ref()
        .is_some_and(|workflow| is_terminal_workflow_status(&workflow.status));
    let inert_without_markers = record.running.is_none()
        && record.waiting.is_none()
        && record.workflow_state.is_none()
        && !record.has_done_marker;
    let terminal_or_inert =
        record.has_done_marker || workflow_terminal || inert_without_markers;
    let dismissed_agent_type = if summary.agent_type == "workflow" {
        "workflow"
    } else {
        "run"
    };
    dismissed.matches(
        record.timestamp.as_str(),
        terminal_or_inert,
        dismissed_agent_type,
        summary.cl_name.as_deref(),
    )
}

pub(super) fn lineage_row_is_dismissed(
    row: &DismissalLineageRow,
    dismissed: &DismissedIndex,
) -> bool {
    let workflow_terminal = row
        .workflow_status
        .as_deref()
        .is_some_and(is_terminal_workflow_status);
    let inert_without_markers = !row.has_running_marker
        && !row.has_waiting_marker
        && !row.has_workflow_state
        && !row.has_done_marker;
    let terminal_or_inert =
        row.has_done_marker || workflow_terminal || inert_without_markers;
    let dismissed_agent_type = if row.agent_type == "workflow" {
        "workflow"
    } else {
        "run"
    };
    dismissed.matches(
        row.lineage.timestamp.as_str(),
        terminal_or_inert,
        dismissed_agent_type,
        row.cl_name.as_deref(),
    )
}

pub(super) fn dismissed_parent_suffix_from_index(
    seed: &IndexedLineageRow,
    dismissed: &DismissedIndex,
) -> bool {
    [
        seed.parent_timestamp.as_deref(),
        seed.retry_of_timestamp.as_deref(),
        seed.retry_chain_root_timestamp.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|candidate| dismissed.suffix_exists(candidate))
}

pub(super) fn snapshot_select_lineage_rows(
    rows: &[DismissalLineageRow],
    timestamps: &BTreeSet<String>,
) -> Vec<IndexedLineageRow> {
    if timestamps.is_empty() {
        return Vec::new();
    }
    let mut matched: Vec<IndexedLineageRow> = rows
        .iter()
        .filter(|row| {
            timestamps.contains(&row.lineage.timestamp)
                || row
                    .lineage
                    .parent_timestamp
                    .as_ref()
                    .is_some_and(|value| timestamps.contains(value))
                || row
                    .lineage
                    .retry_of_timestamp
                    .as_ref()
                    .is_some_and(|value| timestamps.contains(value))
                || row
                    .lineage
                    .retried_as_timestamp
                    .as_ref()
                    .is_some_and(|value| timestamps.contains(value))
                || row
                    .lineage
                    .retry_chain_root_timestamp
                    .as_ref()
                    .is_some_and(|value| timestamps.contains(value))
        })
        .map(|row| row.lineage.clone())
        .collect();
    matched.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
    });
    matched
}

pub(super) fn family_root_row_from_snapshot<'a>(
    rows: &'a [DismissalLineageRow],
    agent_family: Option<&str>,
) -> Option<&'a DismissalLineageRow> {
    let family = agent_family.filter(|value| !value.is_empty())?;
    rows.iter()
        .filter(|row| {
            row.lineage.agent_family.as_deref() == Some(family)
                && row.lineage.parent_timestamp.is_none()
        })
        .min_by(|left, right| {
            left.lineage
                .timestamp
                .cmp(&right.lineage.timestamp)
                .then_with(|| {
                    left.lineage.artifact_dir.cmp(&right.lineage.artifact_dir)
                })
        })
}

pub(super) fn family_root_dismissed_from_snapshot(
    candidate: &FamilyDismissalLineageCandidateWire,
    rows: &[DismissalLineageRow],
    dismissed: &DismissedIndex,
) -> bool {
    let Some(seed) = rows
        .iter()
        .find(|row| row.lineage.timestamp == candidate.timestamp)
    else {
        return false;
    };
    if dismissed_parent_suffix_from_index(&seed.lineage, dismissed) {
        return true;
    }
    if candidate.seed_definitively_dead
        && dismissed.suffix_exists(&seed.lineage.timestamp)
    {
        return true;
    }
    let mut timestamps: BTreeSet<String> = BTreeSet::new();
    insert_lineage_timestamp(&mut timestamps, &seed.lineage.timestamp);
    seed.lineage.add_related_timestamps(&mut timestamps);
    let mut by_timestamp: BTreeMap<String, IndexedLineageRow> = BTreeMap::new();
    by_timestamp.insert(seed.lineage.timestamp.clone(), seed.lineage.clone());

    for _ in 0..MAX_RELATED_ARTIFACT_QUERY_ITERATIONS {
        let matched = snapshot_select_lineage_rows(rows, &timestamps);
        let mut changed = false;
        for row in matched {
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
        let Some(root) = family_root_row_from_snapshot(
            rows,
            seed.lineage.agent_family.as_deref(),
        ) else {
            return false;
        };
        root.lineage.clone()
    };
    let Some(root_row) = rows
        .iter()
        .find(|row| row.lineage.artifact_dir == root.artifact_dir)
    else {
        return false;
    };
    lineage_row_is_dismissed(root_row, dismissed)
}

pub(super) fn record_is_dismissed(
    conn: &Connection,
    record: &AgentArtifactRecordWire,
    summary: &RecordSummary,
) -> Result<bool, String> {
    let workflow_terminal = record
        .workflow_state
        .as_ref()
        .is_some_and(|workflow| is_terminal_workflow_status(&workflow.status));
    let inert_without_markers = record.running.is_none()
        && record.waiting.is_none()
        && record.workflow_state.is_none()
        && !record.has_done_marker;
    let terminal_or_inert =
        record.has_done_marker || workflow_terminal || inert_without_markers;
    let dismissed_agent_type = if summary.agent_type == "workflow" {
        "workflow"
    } else {
        "run"
    };
    let mut stmt = conn
        .prepare(
            r#"
            SELECT 1 FROM dismissed_agents dismissed
            WHERE dismissed.raw_suffix = ?1
              AND (
                  ?2 = 1
                  OR (
                      dismissed.agent_type = ?3
                      AND (
                          dismissed.cl_name = ?4
                          OR dismissed.cl_name = 'unknown'
                          OR ?4 IS NULL
                      )
                  )
              )
            LIMIT 1
            "#,
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params![
            record.timestamp.as_str(),
            terminal_or_inert as i64,
            dismissed_agent_type,
            summary.cl_name.as_deref(),
        ])
        .map_err(|e| e.to_string())?;
    Ok(rows.next().map_err(|e| e.to_string())?.is_some())
}

pub(super) fn dismissed_raw_suffix_exists(
    conn: &Connection,
    raw_suffix: &str,
) -> Result<bool, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT 1 FROM dismissed_agents dismissed
            WHERE dismissed.raw_suffix = ?1
            LIMIT 1
            "#,
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([raw_suffix]).map_err(|e| e.to_string())?;
    Ok(rows.next().map_err(|e| e.to_string())?.is_some())
}
