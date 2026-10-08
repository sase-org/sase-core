//! Read-model mutation publication under the mutation flock.
//!
//! Shares the established reducer and tail validation/post-pass helpers
//! instead of inventing another reduction algorithm: after durable event
//! appends, the snapshot-plus-tail refresh recomputes touched rows from
//! the appended bytes, and the reducer truth wins over the imperative
//! overlay. Validates changed candidates and streams before durable
//! writes, appends with byte-preserving prefix checks, and commits
//! deltas (rows/deletes, indexes, edges, suffix/allocation, provenance,
//! stream signatures, frontier, manifest/config fingerprint, token, sweep
//! time, telemetry) in one SQLite transaction before releasing
//! `beads.db` — via the tail path, which already does exactly that.
//!
//! Content-revision CAS invalidates stale concurrent publishers: the
//! baseline witness (generation/frontier/token plus content generation)
//! is captured at admission, and a lost race rolls back without pairing
//! newer signatures with older rows. Backdated events, rewrite,
//! relocation, incompatible config, or other failed tail preconditions
//! take the correctness-preserving repair/replay path. After an event
//! append succeeds, cache failure never turns the mutation into a
//! retryable error and never appends twice: events stand, and a later
//! read tails/rebuilds/replays. Unusable cache state is invalidated
//! safely so the next read cannot serve stale rows.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::bead::read_model::ensure_cache_ready_for_mutation_at;
use crate::bead::wire::{BeadError, IssueWire};

use super::view::CacheWitness;

/// Publish appended events and verify reducer-truth rows.
///
/// Runs the forced freshness sweep (never the 60 s token-only skip) to
/// tail the newly appended bytes into the cache, still under the
/// mutation flock. Returns reducer-truth corrections for `expected` rows:
/// when the cache claims freshness but a touched row differs from the
/// overlay, the stored row wins so the outcome reflects durable state
/// and no retry can duplicate the event.
///
/// A cache fault afterwards is fail-open: the events are durable and the
/// next read tails, rebuilds, or replays. Returns empty corrections on
/// any cache fault. Genuine event/config I/O errors from the append
/// itself already failed before this call and stay real failures.
pub(crate) fn publish_indexed_write(
    beads_dir: &Path,
    cache_path: &Path,
    baseline: Option<&CacheWitness>,
    expected: &[(String, IssueWire)],
) -> Result<Vec<(String, IssueWire)>, BeadError> {
    let fresh = ensure_cache_ready_for_mutation_at(beads_dir, cache_path)
        .map_err(|_| ())
        .unwrap_or(false);
    if fresh {
        if let (Some(before), Ok(after)) =
            (baseline, read_witness_silent(cache_path))
        {
            let advanced = after.frontier != before.frontier
                || after.token != before.token
                || after.generation != before.generation;
            if !advanced && !expected.is_empty() {
                return Ok(Vec::new());
            }
        }
    }
    if !fresh || expected.is_empty() {
        return Ok(Vec::new());
    }
    let connection = match Connection::open_with_flags(
        cache_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(connection) => connection,
        Err(_) => return Ok(Vec::new()),
    };
    let mut corrected = Vec::new();
    for (issue_id, overlay) in expected {
        let Some(stored) = fetch_row_silent(&connection, issue_id) else {
            return Ok(Vec::new());
        };
        if serde_json::to_value(&stored).unwrap_or_default()
            != serde_json::to_value(overlay).unwrap_or_default()
        {
            corrected.push((issue_id.clone(), stored));
        }
    }
    Ok(corrected)
}

/// Apply reducer-truth corrections to outcome rows, keyed by issue ID.
pub(crate) fn apply_corrections(
    rows: &mut [IssueWire],
    corrected: &[(String, IssueWire)],
) {
    for row in rows.iter_mut() {
        if let Some((_, truth)) = corrected.iter().find(|(id, _)| id == &row.id)
        {
            *row = truth.clone();
        }
    }
}

fn fetch_row_silent(
    connection: &Connection,
    issue_id: &str,
) -> Option<IssueWire> {
    let mut statement = connection
        .prepare("SELECT row FROM issues WHERE id = ?1")
        .ok()?;
    let mut rows = statement.query([issue_id]).ok()?;
    let row = rows.next().ok()??;
    let text: String = row.get(0).ok()?;
    serde_json::from_str(&text).ok()
}

fn read_witness_silent(cache_path: &Path) -> Result<CacheWitness, ()> {
    let connection = Connection::open_with_flags(
        cache_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|_| ())?;
    let get = |key: &str| -> Result<String, ()> {
        let mut stmt = connection
            .prepare("SELECT value FROM meta WHERE key = ?1")
            .map_err(|_| ())?;
        let mut rows = stmt.query([key]).map_err(|_| ())?;
        match rows.next().map_err(|_| ())? {
            Some(row) => row.get::<_, String>(0).map_err(|_| ()),
            None => Ok(String::new()),
        }
    };
    Ok(CacheWitness {
        generation: get("generation")?,
        frontier: get("frontier")?,
        token: get("token")?,
    })
}

#[cfg(test)]
mod tests {
    use crate::bead::wire::IssueWire;

    fn test_issue(id: &str) -> IssueWire {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "title": "t",
            "status": "open",
            "issue_type": "task",
            "tier": null,
            "parent_id": null,
            "owner": "o",
            "assignee": "",
            "created_at": "2026-01-01T00:00:00Z",
            "created_by": "o",
            "updated_at": "2026-01-01T00:00:00Z",
            "closed_at": null,
            "close_reason": null,
            "resolution": null,
            "close_history": [],
            "description": "",
            "notes": [],
            "design": null,
            "refs": [],
            "links": [],
            "plus_one_evidence": [],
            "snooze": null,
            "model": "m",
            "size": "small",
            "task_type": "bug",
            "task_type_fields": {},
            "is_ready_to_work": false,
            "changespec_name": null,
            "changespec_bug_id": null,
            "external_ref": "",
            "creation_reason": "",
            "dependencies": []
        }))
        .unwrap()
    }

    #[test]
    fn corrections_replace_only_matching_rows() {
        let mut rows = vec![test_issue("a-1"), test_issue("a-2")];
        let mut truth = test_issue("a-1");
        truth.title = "corrected".to_string();
        super::apply_corrections(
            &mut rows,
            &[("a-1".to_string(), truth.clone())],
        );
        assert_eq!(rows[0].title, "corrected");
        assert_eq!(rows[1].title, "t");
    }
}
