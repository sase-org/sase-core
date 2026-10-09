//! Resume-load helpers shared by the read-side tail and direct publication.
//!
//! Pure move from `tail.rs`: the load plan, the compact issue index, the
//! touched-row and dependent loaders, the manifest/config gate, and the
//! merge-key helper. No behavior change.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;

use crate::bead::events::{
    event_operation_priority, BeadEventPayloadWire, BeadEventRecordWire,
};

use super::store::{CacheMeta, ManifestConfigFingerprint};

/// What one merged tail can observe: touched issues, removals, and
/// dependency targets to preload.
pub(super) struct TailLoadPlan {
    /// Every event's issue, including removal targets.
    pub(super) touched_ids: BTreeSet<String>,
    /// Removed issues: event targets plus cascade sets.
    pub(super) removed_ids: BTreeSet<String>,
    /// Dependency targets the tail adds.
    pub(super) dep_targets: BTreeSet<String>,
    /// External refs the tail mentions (for the collapse overlay).
    pub(super) tail_refs: BTreeSet<String>,
    /// Issues the tail creates.
    pub(super) created_ids: BTreeSet<String>,
}

impl TailLoadPlan {
    /// True when the tail can change the issue id set or collapse
    /// winners: creations, removals, or ref mentions. Otherwise the
    /// compact index, collapse overlay, and position renumbering are all
    /// provably no-ops and the resume stays on touched rows alone.
    pub(super) fn needs_index(&self) -> bool {
        !self.created_ids.is_empty()
            || !self.removed_ids.is_empty()
            || !self.tail_refs.is_empty()
    }
}

/// Scan one merged tail for everything the resume must preload.
pub(super) fn tail_load_plan(merged: &[&BeadEventRecordWire]) -> TailLoadPlan {
    let mut plan = TailLoadPlan {
        touched_ids: BTreeSet::new(),
        removed_ids: BTreeSet::new(),
        dep_targets: BTreeSet::new(),
        tail_refs: BTreeSet::new(),
        created_ids: BTreeSet::new(),
    };
    for event in merged {
        plan.touched_ids.insert(event.issue_id.clone());
        match &event.payload {
            BeadEventPayloadWire::IssueCreated { issue } => {
                plan.created_ids.insert(issue.id.clone());
                let external_ref = issue.external_ref.trim();
                if !external_ref.is_empty() {
                    plan.tail_refs.insert(external_ref.to_string());
                }
            }
            BeadEventPayloadWire::IssueUpdated { fields } => {
                if let Some(external_ref) = fields.external_ref.as_deref() {
                    let external_ref = external_ref.trim();
                    if !external_ref.is_empty() {
                        plan.tail_refs.insert(external_ref.to_string());
                    }
                }
            }
            BeadEventPayloadWire::DependencyAdded { dependency } => {
                plan.dep_targets.insert(dependency.depends_on_id.clone());
            }
            BeadEventPayloadWire::IssueRemoved {
                cascade_removed_issue_ids,
            } => {
                plan.removed_ids.insert(event.issue_id.clone());
                plan.removed_ids
                    .extend(cascade_removed_issue_ids.iter().cloned());
            }
            _ => {}
        }
    }
    plan
}

/// Compact issue index: identity plus collapse and lineage inputs, in
/// stored position order. Small columns only, never fat issue rows.
pub(super) struct IssueIndex {
    /// `(id, external_ref, created_at)` in stored position order.
    pub(super) ordered: Vec<(String, String, String)>,
    /// `id -> (external_ref, created_at)`.
    pub(super) by_id: BTreeMap<String, (String, String)>,
    /// `child id -> parent id`, for lineage roots.
    pub(super) parents: BTreeMap<String, String>,
}

impl IssueIndex {
    /// Empty index for tails that provably need none: without
    /// creations, removals, or ref mentions the id set, collapse
    /// winners, and positions all stay exact.
    pub(super) fn empty() -> Self {
        IssueIndex {
            ordered: Vec::new(),
            by_id: BTreeMap::new(),
            parents: BTreeMap::new(),
        }
    }
}

/// Load the compact issue index in stored position order.
pub(super) fn load_issue_index(
    connection: &Connection,
) -> Result<IssueIndex, String> {
    let mut statement = connection
        .prepare(
            "SELECT id, external_ref, created_at, parent FROM issues ORDER BY position",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|error| error.to_string())?;
    let mut index = IssueIndex {
        ordered: Vec::new(),
        by_id: BTreeMap::new(),
        parents: BTreeMap::new(),
    };
    for row in rows {
        let (id, external_ref, created_at, parent) =
            row.map_err(|error| error.to_string())?;
        index
            .by_id
            .insert(id.clone(), (external_ref.clone(), created_at.clone()));
        if !parent.is_empty() {
            index.parents.insert(id.clone(), parent);
        }
        index.ordered.push((id, external_ref, created_at));
    }
    Ok(index)
}

/// Load fat issue rows plus positions for exactly the given ids.
pub(super) fn load_rows(
    connection: &Connection,
    ids: &BTreeSet<&str>,
) -> Result<BTreeMap<String, (String, i64)>, String> {
    let mut rows = BTreeMap::new();
    if ids.is_empty() {
        return Ok(rows);
    }
    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for id in ids {
        params.push(id);
    }
    let mut statement = connection
        .prepare(&format!(
            "SELECT id, row, position FROM issues WHERE id IN ({placeholders})"
        ))
        .map_err(|error| error.to_string())?;
    let mapped = statement
        .query_map(params.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|error| error.to_string())?;
    for row in mapped {
        let (id, json, position) = row.map_err(|error| error.to_string())?;
        rows.insert(id, (json, position));
    }
    Ok(rows)
}

/// Issues whose dependency lists name a removed issue: the cascade
/// prunes them, so their rows reload for the resume.
pub(super) fn query_dependents(
    connection: &Connection,
    removed_ids: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    if removed_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = removed_ids
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for id in removed_ids {
        params.push(id);
    }
    let mut statement = connection
        .prepare(&format!(
            "SELECT DISTINCT src FROM edges WHERE dst IN ({placeholders}) AND kind = 'depends_on'"
        ))
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(params.as_slice(), |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    let mut dependents = Vec::new();
    for row in rows {
        dependents.push(row.map_err(|error| error.to_string())?);
    }
    Ok(dependents)
}

/// Manifest/config gate: only stream-count growth from pure additions
/// keeps the tail open. Anything else (rewritten manifest, any config
/// content change) rebuilds.
pub(super) fn gate_manifest_config(
    meta: &CacheMeta,
    fingerprint: &ManifestConfigFingerprint,
    new_stream_count: usize,
) -> Option<String> {
    // Only the schema version and the stream count gate the tail:
    // `generated_from` and `migration_tool` are provenance strings with
    // no reduction semantics (the corpus generator and the mutation
    // writer spell them differently), and any future semantic manifest
    // field arrives with a schema version bump.
    if fingerprint.manifest_schema_version != meta.manifest_schema_version {
        return Some("event manifest changed".to_string());
    }
    if fingerprint.manifest_stream_count
        != meta.manifest_stream_count + new_stream_count
    {
        return Some(
            "event manifest changed beyond stream additions".to_string(),
        );
    }
    if fingerprint.config_canonical != meta.config_canonical {
        return Some("store config changed".to_string());
    }
    None
}

/// One event's merge key in the k-way merge ordering.
pub(super) fn tail_merge_key(
    event: &BeadEventRecordWire,
) -> (String, usize, String) {
    (
        event.timestamp.clone(),
        event_operation_priority(event.operation),
        event.event_id.clone(),
    )
}
