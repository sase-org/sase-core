//! Indexed (affected-row) mutation path for note/update (`sase-1h8.13`).
//!
//! Warm git-backed mutations must not replay the whole event store or hydrate
//! every row: a note/update loads its target row(s) plus the actually related
//! rows through the SQLite read model, loads the single affected physical
//! event stream, appends the historical-byte-preserving event, and publishes
//! through the existing snapshot-plus-tail refresh before releasing the
//! `beads.db` flock. The next indexed read uses the token-only cache path;
//! replay stays the uncached/corrupt-cache fallback and correctness oracle.
//!
//! This module covers the two benchmarked operations (`append_issue_note`
//! and `update_issues`). Every other mutation keeps the full-replay
//! `MutableStore` path until its own port lands. Anything this path cannot
//! prove equivalent falls back to replay instead of guessing:
//!
//! - no cache location, legacy store, or unusable cache;
//! - `external_ref` changes (collapse winners need the full post-pass);
//! - reopening a closed bead (ancestor archival needs the full store);
//! - a missing physical stream file (replay owns the corruption error).
//!
//! The warm path never classifies historical streams for retired-flag pruning
//! (the read path already skips tombstones in memory); the physical prune
//! runs on the next cold/uncached locked load as before. Event/config I/O
//! errors stay real failures; a cache fault after durable event writes is
//! fail-open so a retry can never duplicate the event.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use rusqlite::Connection;
use rusqlite::OpenFlags;

use crate::bead::config::default_config;
use crate::bead::config::load_config;
use crate::bead::config::save_config;
use crate::bead::events::mint_bead_event_id;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventRecordWire;
use crate::bead::events::BeadEventStreamWire;
use crate::bead::events::BEAD_EVENT_SCHEMA_VERSION;
use crate::bead::jsonl::event_streams_dir;
use crate::bead::jsonl::read_event_stream_file;
use crate::bead::jsonl::write_event_store_changed_with_total;
use crate::bead::mutation::close_remove::reject_unclosed_descendants_in_batch;
use crate::bead::mutation::mutation_wire::BeadMutationOutcomeWire;
use crate::bead::mutation::mutation_wire::BeadUpdateFieldsWire;
use crate::bead::mutation::notes_update::apply_update_fields;
use crate::bead::mutation::notes_update::event_fields_from_update_fields;
use crate::bead::mutation::notes_update::parse_status;
use crate::bead::mutation::store::now_utc;
use crate::bead::mutation::store::outcome;
#[cfg(test)]
use crate::bead::mutation::store::store_io_stats;
use crate::bead::read_model::ensure_cache_ready_for_mutation_at;
use crate::bead::read_model::read_model_cache_path_for_store;
use crate::bead::wire::BeadError;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use crate::note_attachment::BeadNoteAttachmentWire;

/// Admit the indexed path inside the mutation flock, or report fallback.
///
/// Runs the forced freshness sweep (never the 60 s token-only skip) and
/// returns the ready cache path. `Ok(None)` means the caller must use the
/// replay path: no cache location, a legacy store, an unusable cache, or a
/// refresh fault. Genuine store corruption also falls back so the replay
/// oracle owns the error exactly as before.
pub(crate) fn admit_indexed_mutation(beads_dir: &Path) -> Option<PathBuf> {
    if !beads_dir.is_dir() {
        return None;
    }
    let cache_path = read_model_cache_path_for_store(beads_dir)?;
    match ensure_cache_ready_for_mutation_at(beads_dir, &cache_path) {
        Ok(true) => Some(cache_path),
        Ok(false) => None,
        Err(_) => None,
    }
}

/// Open one read-only view over the admitted cache.
fn open_indexed(cache_path: &Path) -> Result<Connection, BeadError> {
    Connection::open_with_flags(cache_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| BeadError::io(error.to_string()))
}

/// Decode one cached row; a missing row is the replay's `not_found`.
fn fetch_row(
    connection: &Connection,
    issue_id: &str,
) -> Result<IssueWire, BeadError> {
    let mut statement = connection
        .prepare("SELECT row FROM issues WHERE id = ?1")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut rows = statement
        .query([issue_id])
        .map_err(|error| BeadError::io(error.to_string()))?;
    match rows
        .next()
        .map_err(|error| BeadError::io(error.to_string()))?
    {
        Some(row) => {
            let text: String = row
                .get(0)
                .map_err(|error| BeadError::io(error.to_string()))?;
            let issue: IssueWire =
                serde_json::from_str(&text).map_err(|error| {
                    BeadError::io(format!(
                        "cached issue row is not valid: {error}"
                    ))
                })?;
            #[cfg(test)]
            store_io_stats::record_hydrated_rows(1);
            Ok(issue)
        }
        None => Err(BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {issue_id}"),
        }),
    }
}

/// Direct children of one bead in creation order.
fn fetch_children(
    connection: &Connection,
    parent_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut statement = connection
        .prepare(
            "SELECT row FROM issues WHERE parent = ?1 ORDER BY created_at ASC, position ASC",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mapped = statement
        .query_map([parent_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut children = Vec::new();
    for row in mapped {
        let text: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        let issue: IssueWire =
            serde_json::from_str(&text).map_err(|error| {
                BeadError::io(format!("cached issue row is not valid: {error}"))
            })?;
        children.push(issue);
    }
    #[cfg(test)]
    store_io_stats::record_hydrated_rows(children.len() as u64);
    children.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(children)
}

/// Every descendant of one bead (bounded to the affected subtree).
fn fetch_descendants(
    connection: &Connection,
    parent_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut out = Vec::new();
    let mut visited = BTreeSet::from([parent_id.to_string()]);
    let mut stack = vec![parent_id.to_string()];
    while let Some(current) = stack.pop() {
        for child in fetch_children(connection, &current)? {
            if !visited.insert(child.id.clone()) {
                continue;
            }
            stack.push(child.id.clone());
            out.push(child);
        }
    }
    Ok(out)
}

/// Exact ID and shorthand resolution with the replay's error kinds.
///
/// Full IDs (anything containing `-`) pass through untouched so the locked
/// mutation stays the authority for existence; shorthands resolve through
/// the suffix catalog exactly as `resolve_issue_id_in_issues` does.
fn resolve_indexed(
    connection: &Connection,
    raw_id: &str,
) -> Result<String, BeadError> {
    if raw_id.is_empty() || raw_id.contains('-') {
        return Ok(raw_id.to_string());
    }
    let mut statement = connection
        .prepare(
            "SELECT issue_id FROM suffix_catalog WHERE suffix = ?1 ORDER BY issue_id",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mapped = statement
        .query_map([raw_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut candidates = Vec::new();
    for row in mapped {
        candidates.push(row.map_err(|error| BeadError::io(error.to_string()))?);
    }
    match candidates.as_slice() {
        [resolved] => Ok(resolved.clone()),
        [] => Err(BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {raw_id}"),
        }),
        _ => Err(BeadError {
            kind: "ambiguous".to_string(),
            message: format!(
                "ambiguous bead ID shorthand {raw_id:?}: {}",
                candidates.join(", ")
            ),
        }),
    }
}

/// Physical stream owner, preserving the `stream_id_for_issue` routing rule:
/// plan roots own their stream, other beads follow an existing parent link,
/// otherwise the bead owns its stream.
fn stream_id_for_indexed(
    connection: &Connection,
    issue: &IssueWire,
) -> Result<String, BeadError> {
    if issue.issue_type == IssueTypeWire::Plan {
        return Ok(issue.id.clone());
    }
    if let Some(parent_id) = issue.parent_id.as_deref() {
        if fetch_row(connection, parent_id).is_ok() {
            return Ok(parent_id.to_string());
        }
    }
    Ok(issue.id.clone())
}

/// Total physical stream count from the manifest (never a directory scan).
fn manifest_stream_total(beads_dir: &Path) -> Option<usize> {
    let text =
        fs::read_to_string(beads_dir.join("events/manifest.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value
        .get("stream_count")?
        .as_u64()
        .map(|count| count as usize)
}

/// A lazily loaded physical stream: exactly one file is read, and only the
/// appended tail is ever written back, preserving historical bytes.
struct LazyStream {
    stream: BeadEventStreamWire,
}

fn load_lazy_stream(beads_dir: &Path, stream_id: &str) -> Option<LazyStream> {
    let path = event_streams_dir(beads_dir).join(format!("{stream_id}.jsonl"));
    if !path.is_file() {
        return None;
    }
    let (stream, _signature) = read_event_stream_file(&path).ok()?;
    #[cfg(test)]
    store_io_stats::record_stream_reads(1);
    Some(LazyStream { stream })
}

impl LazyStream {
    fn append_event(
        &mut self,
        stream_id: &str,
        operation: BeadEventOperationWire,
        payload: BeadEventPayloadWire,
        timestamp: &str,
        actor: &str,
        issue_id: &str,
    ) -> Result<String, BeadError> {
        let ordinal = self.stream.events.len() + 1;
        let event_id = mint_bead_event_id(
            stream_id, ordinal, timestamp, actor, operation, issue_id, &payload,
        )?;
        let event = BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id,
            timestamp: timestamp.to_string(),
            actor: actor.to_string(),
            operation,
            issue_id: issue_id.to_string(),
            payload,
        };
        event.validate()?;
        let event_id = event.event_id.clone();
        self.stream.events.push(event);
        Ok(event_id)
    }
}

/// Publish appended events through the snapshot-plus-tail refresh.
///
/// Row normalization, edge cleanup, suffixes, lineage, ordering, and link
/// provenance keep their single definition in the tail reducer: the refresh
/// recomputes the touched rows from the appended bytes instead of trusting
/// the imperative overlay. A cache fault afterwards is fail-open (the events
/// are durable and the next read tails, rebuilds, or replays).
///
/// When the cache claims to be fresh but a touched row differs from the
/// overlay, the reducer truth wins: the stored row is returned as a
/// correction so the outcome reflects durable state and no retry can
/// duplicate the event. A mismatch always signals an overlay bug; the
/// parity harness catches it, but production stays safe.
fn publish_indexed_write(
    beads_dir: &Path,
    cache_path: &Path,
    expected: &[(String, IssueWire)],
) -> Result<Vec<(String, IssueWire)>, BeadError> {
    let mut corrected = Vec::new();
    let fresh = ensure_cache_ready_for_mutation_at(beads_dir, cache_path)
        .map_err(|_| ())
        .unwrap_or(false);
    if !fresh || expected.is_empty() {
        return Ok(corrected);
    }
    let connection = match open_indexed(cache_path) {
        Ok(connection) => connection,
        Err(_) => return Ok(corrected),
    };
    for (issue_id, overlay) in expected {
        let Some(stored) = fetch_row_hydration_silent(&connection, issue_id)
        else {
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
fn apply_corrections(
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

/// Row fetch for the publish check that never perturbs `store_io_stats`.
fn fetch_row_hydration_silent(
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

/// Indexed `append_issue_note`: one row, one stream, changed-only
/// validation, then tail publication.
///
/// Returns `Ok(None)` when the replay path must run instead (see the module
/// docs). Errors preserve the replay path's kinds and messages.
pub(crate) fn try_append_note(
    beads_dir: &Path,
    cache_path: &Path,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
    attachments: Option<Vec<BeadNoteAttachmentWire>>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
    #[cfg(test)]
    store_io_stats::record_load();
    let connection = match open_indexed(cache_path) {
        Ok(connection) => connection,
        Err(_) => return Ok(None),
    };
    let resolved = resolve_indexed(&connection, issue_id)?;
    let mut issue = fetch_row(&connection, &resolved)?;
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let now = now.unwrap_or_else(now_utc);
    let author = author
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config.owner.clone());

    let stream_id = stream_id_for_indexed(&connection, &issue)?;
    let Some(mut lazy) = load_lazy_stream(beads_dir, &stream_id) else {
        return Ok(None);
    };
    let event_id = lazy.append_event(
        &stream_id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: entry.to_string(),
            attachments: attachments.clone().unwrap_or_default(),
        },
        &now,
        &author,
        &issue.id,
    )?;
    if let Some(note) = crate::bead::wire::BeadNoteWire::from_event(
        &event_id,
        &now,
        &author,
        entry,
        attachments.clone().unwrap_or_default(),
    ) {
        issue.notes.push(note);
    }
    issue.updated_at = now.clone();
    issue.validate()?;

    let total = match manifest_stream_total(beads_dir) {
        Some(total) => total,
        None => return Ok(None),
    };
    let mut changed = BTreeSet::new();
    changed.insert(stream_id.clone());
    #[cfg(test)]
    store_io_stats::record_save();
    #[cfg(test)]
    store_io_stats::record_validation_runs(1);
    write_event_store_changed_with_total(
        beads_dir,
        std::slice::from_ref(&lazy.stream),
        &changed,
        total,
    )?;
    save_config(beads_dir, &config)?;
    drop(connection);
    let corrected = publish_indexed_write(
        beads_dir,
        cache_path,
        std::slice::from_ref(&(issue.id.clone(), issue.clone())),
    )?;
    let mut returned = vec![issue.clone()];
    apply_corrections(&mut returned, &corrected);
    let issue = returned.pop().expect("one note row");

    let mut result = outcome("note", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(Some(result))
}

/// Indexed `update_issues`: affected rows plus affected subtrees only.
///
/// The whole batch resolves against the indexed suffix catalog before any
/// change is staged, mirroring the replay path's request-order resolution
/// and dedupe. Batches that change `external_ref` or reopen a closed bead
/// fall back to replay so collapse winners and ancestor archival keep the
/// full post-pass semantics.
pub(crate) fn try_update_issues(
    beads_dir: &Path,
    cache_path: &Path,
    issue_ids: &[String],
    fields: BeadUpdateFieldsWire,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
    if fields.external_ref.is_some() {
        return Ok(None);
    }
    #[cfg(test)]
    store_io_stats::record_load();
    let connection = match open_indexed(cache_path) {
        Ok(connection) => connection,
        Err(_) => return Ok(None),
    };
    let mut seen = HashSet::new();
    let mut targets: Vec<String> = Vec::new();
    let mut requested_ids: Vec<String> = Vec::with_capacity(issue_ids.len());
    for issue_id in issue_ids {
        let resolved = resolve_indexed(&connection, issue_id)?;
        requested_ids.push(resolved.clone());
        if seen.insert(resolved.clone()) {
            targets.push(resolved);
        }
    }
    let mut currents = Vec::with_capacity(targets.len());
    for target in &targets {
        currents.push(fetch_row(&connection, target)?);
    }
    let old_issues = currents.clone();

    let new_status = fields.status.as_deref().map(parse_status).transpose()?;
    if new_status == Some(StatusWire::Closed) {
        let mut check_by_id: BTreeMap<String, IssueWire> = currents
            .iter()
            .map(|issue| (issue.id.clone(), issue.clone()))
            .collect();
        for target in &targets {
            for descendant in fetch_descendants(&connection, target)? {
                check_by_id
                    .entry(descendant.id.clone())
                    .or_insert(descendant);
            }
        }
        let check_set: Vec<IssueWire> = check_by_id.into_values().collect();
        reject_unclosed_descendants_in_batch(&check_set, &targets)?;
    }
    if let Some(status) = &new_status {
        for current in &currents {
            if current.status == StatusWire::Closed
                && *status != StatusWire::Closed
            {
                drop(connection);
                return Ok(None);
            }
        }
    }

    let event_fields = event_fields_from_update_fields(&fields)?;
    let now = fields.now.clone().unwrap_or_else(now_utc);

    let mut planned: Vec<IssueWire> = Vec::new();
    let mut unchanged_ids = Vec::new();
    let mut resulting_issues = Vec::with_capacity(targets.len());
    for current in &currents {
        let mut issue = current.clone();
        apply_update_fields(&mut issue, fields.clone(), &now)?;
        if issue == *current {
            unchanged_ids.push(issue.id.clone());
            resulting_issues.push(current.clone());
            continue;
        }
        issue.updated_at = now.clone();
        issue.validate()?;
        resulting_issues.push(issue.clone());
        planned.push(issue);
    }

    if planned.is_empty() {
        drop(connection);
        let mut result = outcome("update", false, Vec::new());
        result.requested_issue_ids = requested_ids;
        result.unchanged_ids = unchanged_ids;
        result.issues = resulting_issues;
        result.old_issues = old_issues;
        return Ok(Some(result));
    }

    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let mut streams: Vec<BeadEventStreamWire> = Vec::new();
    let mut changed = BTreeSet::new();
    let mut changed_ids = Vec::with_capacity(planned.len());
    for issue in &planned {
        let stream_id = stream_id_for_indexed(&connection, issue)?;
        let position =
            streams.iter().position(|stream: &BeadEventStreamWire| {
                stream.stream_id == stream_id
            });
        let index = match position {
            Some(index) => index,
            None => {
                let Some(lazy) = load_lazy_stream(beads_dir, &stream_id) else {
                    drop(connection);
                    return Ok(None);
                };
                streams.push(lazy.stream);
                streams.len() - 1
            }
        };
        let ordinal = streams[index].events.len() + 1;
        let event_id = mint_bead_event_id(
            &stream_id,
            ordinal,
            &issue.updated_at,
            &issue.created_by,
            BeadEventOperationWire::IssueUpdated,
            &issue.id,
            &BeadEventPayloadWire::IssueUpdated {
                fields: event_fields.clone(),
            },
        )?;
        let event = BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id,
            timestamp: issue.updated_at.clone(),
            actor: issue.created_by.clone(),
            operation: BeadEventOperationWire::IssueUpdated,
            issue_id: issue.id.clone(),
            payload: BeadEventPayloadWire::IssueUpdated {
                fields: event_fields.clone(),
            },
        };
        event.validate()?;
        streams[index].events.push(event);
        changed.insert(stream_id);
        changed_ids.push(issue.id.clone());
    }

    let total = match manifest_stream_total(beads_dir) {
        Some(total) => total,
        None => {
            drop(connection);
            return Ok(None);
        }
    };
    #[cfg(test)]
    store_io_stats::record_save();
    #[cfg(test)]
    store_io_stats::record_validation_runs(planned.len() as u64);
    write_event_store_changed_with_total(beads_dir, &streams, &changed, total)?;
    save_config(beads_dir, &config)?;
    let expected: Vec<(String, IssueWire)> = planned
        .iter()
        .map(|issue| (issue.id.clone(), issue.clone()))
        .collect();
    drop(connection);
    let corrected = publish_indexed_write(beads_dir, cache_path, &expected)?;
    apply_corrections(&mut resulting_issues, &corrected);

    let mut result = outcome("update", true, changed_ids);
    result.requested_issue_ids = requested_ids;
    result.unchanged_ids = unchanged_ids;
    result.issues = resulting_issues;
    result.old_issues = old_issues;
    Ok(Some(result))
}
