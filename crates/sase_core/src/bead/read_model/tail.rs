//! Snapshot-plus-tail incremental refresh over the read model.
//!
//! When a freshness sweep finds changes, the cache resumes from its stored
//! rows instead of replaying history: only the bytes appended after each
//! stream's stored length are read and parsed, the tail events are merged
//! and applied onto the cached snapshot, and the post-pass re-runs over
//! the resumed rows. Any precondition failure falls back to a full
//! rebuild, so the cache can never diverge from replay.
//!
//! The equivalence argument the gate enforces:
//!
//! - Every changed stream is a pure append: the stored length plus the
//!   content hash proves the old bytes are an exact prefix, and the tail
//!   starts on a line boundary (the append-preserving writer only ever
//!   adds `\n`-terminated lines).
//! - No stream disappeared or was renamed, and neither the manifest
//!   (beyond stream-count growth from pure additions) nor the store
//!   config changed content.
//! - Every new event's merge key sorts strictly after the stored merge
//!   frontier. The k-way merge preserves intra-stream position and
//!   interleaves streams by `(timestamp, operation priority, event_id)`,
//!   so with all old keys below all tail keys a full replay would apply
//!   every old event first and the tail in exactly the merged tail order
//!   applied here. Stream indexes are preserved by merging over the full
//!   sorted stream order with empty placeholders, keeping the
//!   stream-index tiebreak identical.
//!
//! Event-ID de-duplication and relocation behave exactly as the reducer
//! does: a rewritten stream fails the prefix check, and a backdated,
//! clock-skewed, relocated, or conflict-resolved event fails the frontier
//! check, so both fall back to a rebuild.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;

use rusqlite::Connection;

use crate::bead::events::{
    apply_event, canonical_bead_source_ref, event_operation_priority,
    merge_stream_events, BeadEventPayloadWire, BeadEventRecordWire,
    BeadEventStreamWire,
};
use crate::bead::jsonl::{
    event_streams_dir, hex_signature, parse_event_stream_bytes,
};
use crate::bead::read_model::freshness::{
    now_ns, sweep_store_signatures, FileSignature, StoreSignatures,
};
use crate::bead::wire::{BeadError, IssueWire};

use super::store::{
    bump_outcome_in_txn, drop_cache_file, fingerprint_manifest_config,
    format_frontier, id_suffix, lineage_root, load_snapshot,
    load_stored_stream_sigs, open_and_serve, parse_frontier,
    record_manifest_config_in_txn, record_refresh_in_txn,
    update_token_and_sweep, wire_string, CacheMeta, CachedStoreSnapshot, Fault,
    ManifestConfigFingerprint, ServeOutcome, StoredStreamSig, WriteFault,
};

/// What a tail attempt decided: serve, apply, or rebuild with a reason.
pub(super) enum TailDecision {
    /// Nothing appended (only stat noise): refresh the sweep and serve.
    Served(CachedStoreSnapshot),
    /// Tail events applied and committed.
    Tailed(CachedStoreSnapshot),
    /// A precondition failed: the caller rebuilds, recording the reason.
    Fallback(String),
}

/// Apply stats for the outcome telemetry.
pub(super) struct TailStats {
    events: usize,
    streams: usize,
}

/// Try the incremental refresh after a sweep found changes.
///
/// Returns [`TailDecision::Fallback`] with the failed precondition whenever
/// the store moved in a way the tail cannot prove equivalent; the caller
/// rebuilds from a full replay instead. Genuine store corruption surfaces
/// as [`Fault::Store`], exactly as the replay it replaces would fail.
pub(super) fn try_tail_apply(
    beads_dir: &Path,
    cache_path: &Path,
    connection: &Connection,
    meta: &CacheMeta,
    token: &str,
    sweep: &StoreSignatures,
) -> Result<TailDecision, Fault> {
    if !meta.streams_known {
        return Ok(TailDecision::Fallback(
            "stream signatures not recorded".to_string(),
        ));
    }
    if meta.frontier.is_empty() {
        return Ok(TailDecision::Fallback(
            "no merge frontier recorded".to_string(),
        ));
    }
    let stored = load_stored_stream_sigs(connection);
    // Set-membership against the sweep: a linear scan here would make
    // the gate quadratic in the stream count.
    let current_ids: BTreeSet<&str> =
        sweep.streams.iter().map(|(id, _)| id.as_str()).collect();
    if stored.keys().any(|id| !current_ids.contains(id.as_str())) {
        return Ok(TailDecision::Fallback(
            "event stream removed or renamed".to_string(),
        ));
    }
    let new_ids: Vec<String> = sweep
        .streams
        .iter()
        .filter(|(id, _)| !stored.contains_key(id))
        .map(|(id, _)| id.clone())
        .collect();
    let fingerprint = match fingerprint_manifest_config(beads_dir) {
        Ok(fingerprint) => fingerprint,
        Err(error) => return Err(Fault::Store(error)),
    };
    if let Some(reason) =
        gate_manifest_config(meta, &fingerprint, new_ids.len())
    {
        return Ok(TailDecision::Fallback(reason));
    }
    let frontier = parse_frontier(&meta.frontier);
    let streams_dir = event_streams_dir(beads_dir);
    let mut tails: BTreeMap<String, Vec<BeadEventRecordWire>> = BTreeMap::new();
    let mut tail_sigs: BTreeMap<String, NewStreamSig> = BTreeMap::new();
    // Changed streams: verify the append prefix, then parse only the tail.
    let sweep_sigs: BTreeMap<&str, (u64, i64, u64)> = sweep
        .streams
        .iter()
        .map(|(id, signature)| {
            (
                id.as_str(),
                (signature.size, signature.mtime_ns, signature.inode),
            )
        })
        .collect();
    for (stream_id, stored_sig) in &stored {
        let Some(current_sig) = sweep_sigs.get(stream_id.as_str()) else {
            continue;
        };
        if *current_sig
            == (stored_sig.size, stored_sig.mtime_ns, stored_sig.inode)
        {
            continue;
        }
        let path = streams_dir.join(format!("{stream_id}.jsonl"));
        let read = match read_stream_file(&path) {
            Ok(read) => read,
            Err(error) => {
                return Err(cache_or_store(beads_dir, sweep, error));
            }
        };
        if read.bytes.len() < stored_sig.byte_len as usize {
            return Ok(TailDecision::Fallback(
                "event stream truncated or rewritten".to_string(),
            ));
        }
        if hex_signature(&read.bytes[..stored_sig.byte_len as usize])
            != stored_sig.content_hash
        {
            return Ok(TailDecision::Fallback(
                "event stream history rewritten".to_string(),
            ));
        }
        let tail_bytes = &read.bytes[stored_sig.byte_len as usize..];
        if !tail_bytes.is_empty() {
            let old_ends_on_line = stored_sig.byte_len == 0
                || read.bytes[stored_sig.byte_len as usize - 1] == b'\n';
            if !old_ends_on_line && !tail_bytes.starts_with(b"\n") {
                return Ok(TailDecision::Fallback(
                    "event stream appended without line boundary".to_string(),
                ));
            }
            match parse_event_stream_bytes(&path, tail_bytes) {
                Ok(events) => {
                    tails.insert(stream_id.clone(), events);
                }
                Err(error) => {
                    return Err(cache_or_store(beads_dir, sweep, error));
                }
            }
        }
        tail_sigs.insert(
            stream_id.clone(),
            NewStreamSig {
                size: read.size,
                mtime_ns: read.mtime_ns,
                inode: read.inode,
                byte_len: read.bytes.len() as u64,
                content_hash: hex_signature(&read.bytes),
            },
        );
    }
    // New streams: parse fully; an unparseable new file falls back so the
    // rebuild classifies removed-flag tombstones exactly as reads do.
    for stream_id in &new_ids {
        let path = streams_dir.join(format!("{stream_id}.jsonl"));
        let read = match read_stream_file(&path) {
            Ok(read) => read,
            Err(error) => {
                return Err(cache_or_store(beads_dir, sweep, error));
            }
        };
        match parse_event_stream_bytes(&path, &read.bytes) {
            Ok(events) => {
                tails.insert(stream_id.clone(), events);
            }
            Err(_) => {
                return Ok(TailDecision::Fallback(
                    "new event stream unparseable".to_string(),
                ));
            }
        }
        tail_sigs.insert(
            stream_id.clone(),
            NewStreamSig {
                size: read.size,
                mtime_ns: read.mtime_ns,
                inode: read.inode,
                byte_len: read.bytes.len() as u64,
                content_hash: hex_signature(&read.bytes),
            },
        );
    }
    // Every tail merge key must sort after the stored frontier; anything
    // at or before it (clock skew, backdate, relocation, conflict rewrite)
    // rebuilds instead.
    let mut tail_event_count = 0;
    let mut max_key: Option<(String, usize, String)> = None;
    for events in tails.values() {
        for event in events {
            let key = tail_merge_key(event);
            if key <= frontier {
                return Ok(TailDecision::Fallback(
                    "event at or before merge frontier".to_string(),
                ));
            }
            if max_key.as_ref().is_none_or(|current| key > *current) {
                max_key = Some(key);
            }
            tail_event_count += 1;
        }
    }
    if tail_event_count == 0 {
        return commit_sweep_refresh(
            beads_dir, cache_path, connection, token, &stored, &tail_sigs,
        );
    }
    // Merge the tail in full stream order (empty placeholders keep the
    // stream-index tiebreak identical to a full replay), then resume the
    // reducer from the cached rows.
    let contributing_streams =
        tails.values().filter(|events| !events.is_empty()).count();
    let stats = TailStats {
        events: tail_event_count,
        streams: contributing_streams,
    };
    let ordered: Vec<BeadEventStreamWire> = sweep
        .streams
        .iter()
        .map(|(stream_id, _)| BeadEventStreamWire {
            stream_id: stream_id.clone(),
            root_issue_id: stream_id.clone(),
            events: tails.remove(stream_id).unwrap_or_default(),
        })
        .collect();
    let merged = merge_stream_events(&ordered);
    // Touched-only resume: rows for issues the tail can observe, plus
    // the compact index only when the tail can change the id set or
    // collapse winners. History bytes are never re-read.
    let plan = tail_load_plan(&merged);
    let index = if plan.needs_index() {
        load_issue_index(connection).map_err(|_| {
            drop_cache_file(cache_path);
            Fault::Cache
        })?
    } else {
        IssueIndex::empty()
    };
    let dependents =
        query_dependents(connection, &plan.removed_ids).map_err(|_| {
            drop_cache_file(cache_path);
            Fault::Cache
        })?;
    let mut load_ids: BTreeSet<&str> =
        plan.touched_ids.iter().map(String::as_str).collect();
    load_ids.extend(dependents.iter().map(String::as_str));
    load_ids.extend(plan.dep_targets.iter().map(String::as_str));
    let old_rows = load_rows(connection, &load_ids).map_err(|_| {
        drop_cache_file(cache_path);
        Fault::Cache
    })?;
    let mut partial: BTreeMap<String, IssueWire> = old_rows
        .iter()
        .map(|(id, (row, _))| {
            serde_json::from_str(row)
                .map(|issue| (id.clone(), issue))
                .map_err(|_| {
                    drop_cache_file(cache_path);
                    Fault::Cache
                })
        })
        .collect::<Result<_, Fault>>()?;
    // A creation colliding with an issue the partial map cannot see
    // fails exactly as the full replay would.
    for event in &merged {
        if let BeadEventPayloadWire::IssueCreated { issue } = &event.payload {
            if index.by_id.contains_key(issue.id.as_str())
                && !partial.contains_key(&issue.id)
            {
                return Err(cache_or_store(
                    beads_dir,
                    sweep,
                    BeadError::validation(format!(
                        "duplicate issue_created event for {}",
                        issue.id
                    )),
                ));
            }
        }
    }
    for event in &merged {
        if let Err(error) = apply_event(&mut partial, event) {
            return Err(cache_or_store(beads_dir, sweep, error));
        }
    }
    // Untouched survivors were validated when the cache was built and
    // their rows did not change, so only the loaded rows re-validate.
    for issue in partial.values() {
        if let Err(error) = issue.validate() {
            return Err(cache_or_store(beads_dir, sweep, error));
        }
    }
    let resumed = resume_from_partial(&index, &partial, &plan, &old_rows)?;
    let new_frontier = match max_key {
        Some((timestamp, priority, event_id)) => {
            format_frontier(&timestamp, priority, &event_id)
        }
        None => meta.frontier.clone(),
    };
    commit_tail(
        beads_dir,
        cache_path,
        connection,
        meta,
        token,
        sweep,
        &stored,
        &tail_sigs,
        &merged,
        &index,
        &resumed,
        &new_frontier,
        &fingerprint,
        &stats,
    )
}

/// What one merged tail can observe: touched issues, removals, and
/// dependency targets to preload.
struct TailLoadPlan {
    /// Every event's issue, including removal targets.
    touched_ids: BTreeSet<String>,
    /// Removed issues: event targets plus cascade sets.
    removed_ids: BTreeSet<String>,
    /// Dependency targets the tail adds.
    dep_targets: BTreeSet<String>,
    /// External refs the tail mentions (for the collapse overlay).
    tail_refs: BTreeSet<String>,
    /// Issues the tail creates.
    created_ids: BTreeSet<String>,
}

impl TailLoadPlan {
    /// True when the tail can change the issue id set or collapse
    /// winners: creations, removals, or ref mentions. Otherwise the
    /// compact index, collapse overlay, and position renumbering are all
    /// provably no-ops and the resume stays on touched rows alone.
    fn needs_index(&self) -> bool {
        !self.created_ids.is_empty()
            || !self.removed_ids.is_empty()
            || !self.tail_refs.is_empty()
    }
}

/// Scan one merged tail for everything the resume must preload.
fn tail_load_plan(merged: &[&BeadEventRecordWire]) -> TailLoadPlan {
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
struct IssueIndex {
    /// `(id, external_ref, created_at)` in stored position order.
    ordered: Vec<(String, String, String)>,
    /// `id -> (external_ref, created_at)`.
    by_id: BTreeMap<String, (String, String)>,
    /// `child id -> parent id`, for lineage roots.
    parents: BTreeMap<String, String>,
}

impl IssueIndex {
    /// Empty index for tails that provably need none: without
    /// creations, removals, or ref mentions the id set, collapse
    /// winners, and positions all stay exact.
    fn empty() -> Self {
        IssueIndex {
            ordered: Vec::new(),
            by_id: BTreeMap::new(),
            parents: BTreeMap::new(),
        }
    }
}

/// Load the compact issue index in stored position order.
fn load_issue_index(connection: &Connection) -> Result<IssueIndex, String> {
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
fn load_rows(
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
fn query_dependents(
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
fn gate_manifest_config(
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
fn tail_merge_key(event: &BeadEventRecordWire) -> (String, usize, String) {
    (
        event.timestamp.clone(),
        event_operation_priority(event.operation),
        event.event_id.clone(),
    )
}

/// One stream file read with its same-handle signature.
struct StreamRead {
    bytes: Vec<u8>,
    size: u64,
    mtime_ns: i64,
    inode: u64,
}

/// Refreshed signature for a stream whose bytes were read.
struct NewStreamSig {
    size: u64,
    mtime_ns: i64,
    inode: u64,
    byte_len: u64,
    content_hash: String,
}

/// Read one stream file, capturing the signature from the same open
/// handle whose bytes are parsed, exactly as the full read path does.
fn read_stream_file(path: &Path) -> Result<StreamRead, BeadError> {
    use crate::bead::jsonl::file_inode;
    let mut file = fs::File::open(path).map_err(|error| {
        BeadError::io(format!(
            "failed to read bead event stream {}: {error}",
            path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        BeadError::io(format!(
            "failed to stat bead event stream {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        BeadError::io(format!(
            "failed to read bead event stream {}: {error}",
            path.display()
        ))
    })?;
    Ok(StreamRead {
        bytes,
        size: metadata.len(),
        mtime_ns: crate::fs_sig::mtime_ns(metadata.modified().ok()),
        inode: file_inode(&metadata),
    })
}

/// A tail I/O or reduce error is a torn concurrent read when the store
/// moved under us (fall back to replay, which re-reads fresh) and a
/// genuine store error otherwise (the replay would fail identically).
fn cache_or_store(
    beads_dir: &Path,
    sweep: &StoreSignatures,
    error: BeadError,
) -> Fault {
    match sweep_store_signatures(beads_dir) {
        Ok(current) if current == *sweep => Fault::Store(error),
        _ => Fault::Cache,
    }
}

/// Commit a change that appended no events: refresh the stored stream
/// signatures, token, and sweep time without bumping the generation.
/// The post-commit sweep guards the stats-then-token race the same way
/// the rebuild path guards its content commit.
#[allow(clippy::too_many_arguments)]
fn commit_sweep_refresh(
    beads_dir: &Path,
    cache_path: &Path,
    connection: &Connection,
    token: &str,
    stored: &BTreeMap<String, StoredStreamSig>,
    tail_sigs: &BTreeMap<String, NewStreamSig>,
) -> Result<TailDecision, Fault> {
    let now = now_ns();
    let mut refreshed: BTreeMap<String, (u64, i64, u64, u64, String)> =
        BTreeMap::new();
    for (stream_id, stored_sig) in stored {
        match tail_sigs.get(stream_id) {
            Some(new_sig) => {
                refreshed.insert(
                    stream_id.clone(),
                    (
                        new_sig.size,
                        new_sig.mtime_ns,
                        new_sig.inode,
                        new_sig.byte_len,
                        new_sig.content_hash.clone(),
                    ),
                );
            }
            None => {
                refreshed.insert(
                    stream_id.clone(),
                    (
                        stored_sig.size,
                        stored_sig.mtime_ns,
                        stored_sig.inode,
                        stored_sig.byte_len,
                        stored_sig.content_hash.clone(),
                    ),
                );
            }
        }
    }
    for (stream_id, new_sig) in tail_sigs {
        if !stored.contains_key(stream_id) {
            refreshed.insert(
                stream_id.clone(),
                (
                    new_sig.size,
                    new_sig.mtime_ns,
                    new_sig.inode,
                    new_sig.byte_len,
                    new_sig.content_hash.clone(),
                ),
            );
        }
    }
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(|_| Fault::Cache)?;
    let outcome =
        write_refreshed_streams(connection, &refreshed).and_then(|()| {
            update_token_and_sweep(connection, token, now)
                .map_err(|_| WriteFault::Other)
        });
    match outcome {
        Ok(()) => {
            connection
                .execute_batch("COMMIT")
                .map_err(|_| Fault::Cache)?;
        }
        Err(_) => {
            let _ = connection.execute_batch("ROLLBACK");
            drop_cache_file(cache_path);
            return Err(Fault::Cache);
        }
    }
    guard_committed_streams(
        beads_dir,
        cache_path,
        &committed_sweep_sigs(&refreshed),
    )?;
    let snapshot = load_snapshot(connection).map_err(|_| {
        drop_cache_file(cache_path);
        Fault::Cache
    })?;
    Ok(TailDecision::Served(snapshot))
}

/// What the partial resume decided: rows to write, rows to drop, and
/// where every survivor sits.
struct ResumedTail {
    /// Changed and added survivors: `(id, row, issue)` for upsert.
    upserts: Vec<(String, String, IssueWire)>,
    /// Removed issues plus collapse losers present in the DB.
    deletes: Vec<String>,
    /// Final positions for upserts and moved survivors.
    positions: BTreeMap<String, i64>,
    /// Survivors whose position moved but whose row did not change.
    moved: Vec<String>,
    /// Added survivors, for suffix inserts.
    added: Vec<String>,
    /// Sources whose edges are rewritten, plus removals.
    edge_rescope: BTreeSet<String>,
}

/// Resume the post-pass over the partial map plus the compact index.
///
/// The full reduction's post-pass is sort, validate-each, collapse, and
/// uniqueness. Validation of untouched survivors is skipped (their rows
/// did not change since the cache validated them); sorting, collapse,
/// and uniqueness run over index-sized inputs instead of fat rows:
/// collapse winners are decided by `(created_at, id)` per ref, which the
/// index carries, and a collapse that keeps one row per ref leaves refs
/// unique by construction, exactly as the full post-pass does.
fn resume_from_partial(
    index: &IssueIndex,
    partial: &BTreeMap<String, IssueWire>,
    plan: &TailLoadPlan,
    old_rows: &BTreeMap<String, (String, i64)>,
) -> Result<ResumedTail, Fault> {
    if !plan.needs_index() {
        // Fast path: no creations, removals, or ref mentions. The id
        // set is unchanged (positions stay exact), collapse winners
        // cannot move, and only resumed rows whose JSON changed rewrite.
        let mut upserts: Vec<(String, String, IssueWire)> = Vec::new();
        let mut positions: BTreeMap<String, i64> = BTreeMap::new();
        for (id, issue) in partial {
            let (old_json, position) = old_rows
                .get(id.as_str())
                .map(|(json, position)| (json.as_str(), *position))
                .unwrap_or(("", 0));
            let row = serde_json::to_string(issue).map_err(|_| Fault::Cache)?;
            if row.as_str() != old_json {
                positions.insert(id.clone(), position);
                upserts.push((id.clone(), row, issue.clone()));
            }
        }
        let edge_rescope: BTreeSet<String> =
            upserts.iter().map(|(id, _, _)| id.clone()).collect();
        return Ok(ResumedTail {
            upserts,
            deletes: Vec::new(),
            positions,
            moved: Vec::new(),
            added: Vec::new(),
            edge_rescope,
        });
    }
    // Final refs: the index overlaid with resumed rows. `created_at`
    // never changes after creation, so the index copy stays exact.
    let mut final_ref: BTreeMap<&str, &str> = index
        .by_id
        .iter()
        .map(|(id, (external_ref, _))| (id.as_str(), external_ref.as_str()))
        .collect();
    for (id, issue) in partial {
        final_ref.insert(id.as_str(), issue.external_ref.as_str());
    }
    // `created_at` never changes after creation, so the index copy
    // overlaid with resumed rows stays exact.
    let mut created: BTreeMap<&str, &str> = index
        .by_id
        .iter()
        .map(|(id, (_, created_at))| (id.as_str(), created_at.as_str()))
        .collect();
    for (id, issue) in partial {
        created.insert(id.as_str(), issue.created_at.as_str());
    }
    // Winner per non-empty ref over every candidate the tail leaves
    // behind: indexed survivors plus resumed rows, minus removals.
    let mut winner_by_ref: BTreeMap<&str, &str> = BTreeMap::new();
    for (id, _, _) in &index.ordered {
        consider_candidate(
            &mut winner_by_ref,
            &final_ref,
            &created,
            &plan.removed_ids,
            id.as_str(),
        );
    }
    for id in partial.keys() {
        if !index.by_id.contains_key(id.as_str()) {
            consider_candidate(
                &mut winner_by_ref,
                &final_ref,
                &created,
                &plan.removed_ids,
                id.as_str(),
            );
        }
    }
    let winners: BTreeSet<&str> = winner_by_ref.values().copied().collect();
    // Collapse losers present in the DB drop; tail-created losers were
    // never inserted.
    let mut deletes: Vec<String> = plan.removed_ids.iter().cloned().collect();
    for (id, _, _) in &index.ordered {
        if plan.removed_ids.contains(id.as_str()) {
            continue;
        }
        let external_ref = final_ref.get(id.as_str()).copied().unwrap_or("");
        if !external_ref.trim().is_empty() && !winners.contains(id.as_str()) {
            deletes.push(id.clone());
        }
    }
    let delete_set: BTreeSet<&str> =
        deletes.iter().map(String::as_str).collect();
    // Survivors the tail added, in canonical order.
    let mut added: Vec<String> = partial
        .keys()
        .filter(|id| {
            !index.by_id.contains_key(id.as_str())
                && !delete_set.contains(id.as_str())
        })
        .cloned()
        .collect();
    added.sort();
    // Changed rows: resumed JSON differs from the stored row.
    let mut upserts: Vec<(String, String, IssueWire)> = Vec::new();
    for (id, issue) in partial {
        if delete_set.contains(id.as_str()) {
            continue;
        }
        let row = serde_json::to_string(issue).map_err(|_| Fault::Cache)?;
        let old_json = old_rows.get(id.as_str()).map(|(json, _)| json);
        if old_json != Some(&row) || !index.by_id.contains_key(id.as_str()) {
            upserts.push((id.clone(), row, issue.clone()));
        }
    }
    // Final canonical order when the id set changed; otherwise stored
    // positions stay exact.
    let id_changed = !added.is_empty()
        || deletes
            .iter()
            .any(|id| index.by_id.contains_key(id.as_str()));
    let mut positions: BTreeMap<String, i64> = BTreeMap::new();
    let mut moved: Vec<String> = Vec::new();
    if id_changed {
        let mut order: Vec<&str> = index
            .ordered
            .iter()
            .map(|(id, _, _)| id.as_str())
            .filter(|id| !delete_set.contains(*id))
            .collect();
        for id in &added {
            let position =
                order.partition_point(|existing| *existing < id.as_str());
            order.insert(position, id.as_str());
        }
        for (position, id) in order.iter().enumerate() {
            positions.insert((*id).to_string(), position as i64);
        }
        let upsert_set: BTreeSet<&str> =
            upserts.iter().map(|(id, _, _)| id.as_str()).collect();
        for (old_position, (id, _, _)) in index.ordered.iter().enumerate() {
            if delete_set.contains(id.as_str())
                || upsert_set.contains(id.as_str())
            {
                continue;
            }
            if positions.get(id.as_str()).copied().unwrap_or(-1)
                != old_position as i64
            {
                moved.push(id.clone());
            }
        }
    } else {
        for (position, (id, _, _)) in index.ordered.iter().enumerate() {
            positions.insert(id.clone(), position as i64);
        }
    }
    let mut edge_rescope: BTreeSet<String> =
        upserts.iter().map(|(id, _, _)| id.clone()).collect();
    edge_rescope.extend(deletes.iter().cloned());
    Ok(ResumedTail {
        upserts,
        deletes,
        positions,
        moved,
        added,
        edge_rescope,
    })
}

/// Commit resumed rows plus refreshed signatures and frontier in one
/// transaction under the generation compare-and-verify.
///
/// Only touched rows are rewritten: changed and added issues upsert,
/// removals and collapse losers delete, edges rescope to rewritten
/// sources, and provenance transitions run as SQL in event order. A lost
/// generation race serves the winner's rows; any other write fault drops
/// the file and replays.
#[allow(clippy::too_many_arguments)]
fn commit_tail(
    beads_dir: &Path,
    cache_path: &Path,
    connection: &Connection,
    meta: &CacheMeta,
    token: &str,
    sweep: &StoreSignatures,
    stored: &BTreeMap<String, StoredStreamSig>,
    tail_sigs: &BTreeMap<String, NewStreamSig>,
    merged: &[&BeadEventRecordWire],
    index: &IssueIndex,
    resumed: &ResumedTail,
    new_frontier: &str,
    fingerprint: &ManifestConfigFingerprint,
    stats: &TailStats,
) -> Result<TailDecision, Fault> {
    // Full parent map for lineage roots: upsert rows may descend from
    // untouched ancestors outside the partial map.
    let mut parent_of: BTreeMap<&str, &str> = index
        .parents
        .iter()
        .map(|(id, parent)| (id.as_str(), parent.as_str()))
        .collect();
    for (id, _, issue) in &resumed.upserts {
        if let Some(parent) = issue.parent_id.as_deref() {
            parent_of.insert(id.as_str(), parent);
        } else {
            parent_of.remove(id.as_str());
        }
    }
    let removed_bead_refs: Vec<String> = resumed
        .deletes
        .iter()
        .map(|id| canonical_bead_source_ref(id))
        .collect();
    let reason = format!(
        "{} tail events over {} streams",
        stats.events, stats.streams
    );
    let start_generation = meta.generation;
    let now = now_ns();
    let refreshed = refreshed_streams(stored, tail_sigs, sweep);
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(|_| Fault::Cache)?;
    let outcome = write_tail_rows(
        connection,
        resumed,
        &parent_of,
        &removed_bead_refs,
        merged,
    )
    .and_then(|()| write_refreshed_streams(connection, &refreshed))
    .and_then(|()| {
        update_token_and_sweep(connection, token, now)
            .map_err(|_| WriteFault::Other)
    })
    .and_then(|()| {
        meta_set(connection, "frontier", new_frontier)?;
        record_manifest_config_in_txn(connection, fingerprint)?;
        bump_outcome_in_txn(connection, "outcome_tail")?;
        record_refresh_in_txn(connection, "tail", &reason)?;
        // Generation compare-and-verify last: a tail commit carries the
        // content forward without opening a new generation, but a lost
        // race still rolls back and serves the winner instead of pairing
        // mixed generations.
        let current = connection
            .execute(
                "UPDATE meta SET value = value WHERE key = 'generation' AND value = ?1",
                rusqlite::params![start_generation.to_string()],
            )
            .map_err(|_| WriteFault::Other)?;
        if current != 1 {
            return Err(WriteFault::CasLost);
        }
        Ok(())
    });
    match outcome {
        Ok(()) => {
            connection
                .execute_batch("COMMIT")
                .map_err(|_| Fault::Cache)?;
        }
        Err(WriteFault::CasLost) => {
            let _ = connection.execute_batch("ROLLBACK");
            return match open_and_serve(cache_path) {
                Ok(ServeOutcome::Hit(fresh)) => Ok(TailDecision::Served(fresh)),
                _ => Err(Fault::Cache),
            };
        }
        Err(WriteFault::Other) => {
            let _ = connection.execute_batch("ROLLBACK");
            drop_cache_file(cache_path);
            return Err(Fault::Cache);
        }
    }
    guard_committed_streams(
        beads_dir,
        cache_path,
        &committed_sweep_sigs(&refreshed),
    )?;
    let snapshot = load_snapshot(connection).map_err(|_| {
        drop_cache_file(cache_path);
        Fault::Cache
    })?;
    Ok(TailDecision::Tailed(snapshot))
}

/// Merge stored and refreshed stream signatures for the commit.
fn refreshed_streams(
    stored: &BTreeMap<String, StoredStreamSig>,
    tail_sigs: &BTreeMap<String, NewStreamSig>,
    sweep: &StoreSignatures,
) -> BTreeMap<String, (u64, i64, u64, u64, String)> {
    let mut refreshed = BTreeMap::new();
    for (stream_id, stored_sig) in stored {
        match tail_sigs.get(stream_id) {
            Some(new_sig) => {
                refreshed.insert(
                    stream_id.clone(),
                    (
                        new_sig.size,
                        new_sig.mtime_ns,
                        new_sig.inode,
                        new_sig.byte_len,
                        new_sig.content_hash.clone(),
                    ),
                );
            }
            None => {
                refreshed.insert(
                    stream_id.clone(),
                    (
                        stored_sig.size,
                        stored_sig.mtime_ns,
                        stored_sig.inode,
                        stored_sig.byte_len,
                        stored_sig.content_hash.clone(),
                    ),
                );
            }
        }
    }
    for (stream_id, new_sig) in tail_sigs {
        if !stored.contains_key(stream_id)
            && sweep.streams.iter().any(|(id, _)| id == stream_id)
        {
            refreshed.insert(
                stream_id.clone(),
                (
                    new_sig.size,
                    new_sig.mtime_ns,
                    new_sig.inode,
                    new_sig.byte_len,
                    new_sig.content_hash.clone(),
                ),
            );
        }
    }
    refreshed
}

/// Replace the streams table with the refreshed signatures.
fn write_refreshed_streams(
    connection: &Connection,
    refreshed: &BTreeMap<String, (u64, i64, u64, u64, String)>,
) -> Result<(), WriteFault> {
    connection
        .execute("DELETE FROM streams", [])
        .map_err(|_| WriteFault::Other)?;
    let mut stream_stmt = connection
        .prepare(
            "INSERT INTO streams (stream_id, size, mtime_ns, inode, byte_len, content_hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(|_| WriteFault::Other)?;
    for (stream_id, (size, mtime_ns, inode, byte_len, content_hash)) in
        refreshed
    {
        stream_stmt
            .execute(rusqlite::params![
                stream_id,
                *size as i64,
                mtime_ns,
                *inode as i64,
                *byte_len as i64,
                content_hash,
            ])
            .map_err(|_| WriteFault::Other)?;
    }
    Ok(())
}

/// Set one meta key inside the open write txn.
fn meta_set(
    connection: &Connection,
    key: &str,
    value: &str,
) -> Result<(), WriteFault> {
    connection
        .execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![key, value],
        )
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}

/// Rewrite only the rows the tail touched: upserts for changed and
/// added issues, position fixes for moved survivors, deletes for
/// removals and collapse losers, rescoped edges, SQL provenance
/// transitions in event order, and suffix maintenance.
fn write_tail_rows(
    connection: &Connection,
    resumed: &ResumedTail,
    parent_of: &BTreeMap<&str, &str>,
    removed_bead_refs: &[String],
    merged: &[&BeadEventRecordWire],
) -> Result<(), WriteFault> {
    if !resumed.deletes.is_empty() {
        delete_ids(connection, "issues", &resumed.deletes)?;
        for id in &resumed.deletes {
            connection
                .execute(
                    "DELETE FROM suffix_catalog WHERE suffix = ?1 AND issue_id = ?2",
                    rusqlite::params![id_suffix(id), id],
                )
                .map_err(|_| WriteFault::Other)?;
        }
    }
    let mut issue_stmt = connection
        .prepare(
            "INSERT INTO issues (id, position, row, status, issue_type, tier, parent, stream, created_at, external_ref, task_type, plus_one, is_flag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) ON CONFLICT(id) DO UPDATE SET position = excluded.position, row = excluded.row, status = excluded.status, issue_type = excluded.issue_type, tier = excluded.tier, parent = excluded.parent, stream = excluded.stream, created_at = excluded.created_at, external_ref = excluded.external_ref, task_type = excluded.task_type, plus_one = excluded.plus_one, is_flag = excluded.is_flag WHERE issues.row != excluded.row OR issues.position != excluded.position OR issues.status != excluded.status OR issues.issue_type != excluded.issue_type OR issues.tier != excluded.tier OR issues.parent != excluded.parent OR issues.stream != excluded.stream OR issues.created_at != excluded.created_at OR issues.external_ref != excluded.external_ref OR issues.task_type != excluded.task_type OR issues.plus_one != excluded.plus_one OR issues.is_flag != excluded.is_flag",
        )
        .map_err(|_| WriteFault::Other)?;
    for (id, row, issue) in &resumed.upserts {
        let position = resumed.positions.get(id.as_str()).copied().unwrap_or(0);
        issue_stmt
            .execute(rusqlite::params![
                id,
                position,
                row,
                wire_string(&issue.status),
                wire_string(&issue.issue_type),
                issue.tier.as_ref().map(wire_string).unwrap_or_default(),
                issue.parent_id.clone().unwrap_or_default(),
                lineage_root(id.as_str(), parent_of),
                issue.created_at,
                issue.external_ref,
                issue.task_type.clone().unwrap_or_default(),
                issue.plus_one_count() as i64,
                i64::from(issue.is_flag_task()),
            ])
            .map_err(|_| WriteFault::Other)?;
    }
    drop(issue_stmt);
    if !resumed.moved.is_empty() {
        let mut position_stmt = connection
            .prepare("UPDATE issues SET position = ?1 WHERE id = ?2")
            .map_err(|_| WriteFault::Other)?;
        for id in &resumed.moved {
            let position =
                resumed.positions.get(id.as_str()).copied().unwrap_or(0);
            position_stmt
                .execute(rusqlite::params![position, id])
                .map_err(|_| WriteFault::Other)?;
        }
    }
    if !resumed.added.is_empty() {
        let mut suffix_stmt = connection
            .prepare(
                "INSERT INTO suffix_catalog (suffix, issue_id) VALUES (?1, ?2)",
            )
            .map_err(|_| WriteFault::Other)?;
        for id in &resumed.added {
            suffix_stmt
                .execute(rusqlite::params![id_suffix(id), id])
                .map_err(|_| WriteFault::Other)?;
        }
    }
    // Rebuild edges from the upsert rows: every rescope survivor with a
    // changed row is an upsert, and removals contribute no edges.
    let upsert_rows: BTreeMap<&str, &IssueWire> = resumed
        .upserts
        .iter()
        .map(|(id, _, issue)| (id.as_str(), issue))
        .collect();
    if !resumed.edge_rescope.is_empty() {
        let ids: Vec<String> = resumed.edge_rescope.iter().cloned().collect();
        delete_ids(connection, "edges", &ids)?;
        let mut edge_stmt = connection
            .prepare("INSERT INTO edges (src, dst, kind) VALUES (?1, ?2, ?3)")
            .map_err(|_| WriteFault::Other)?;
        for id in &resumed.edge_rescope {
            let Some(issue) = upsert_rows.get(id.as_str()) else {
                continue;
            };
            if let Some(parent) = issue.parent_id.as_deref() {
                edge_stmt
                    .execute(rusqlite::params![issue.id, parent, "parent"])
                    .map_err(|_| WriteFault::Other)?;
            }
            for dependency in &issue.dependencies {
                edge_stmt
                    .execute(rusqlite::params![
                        issue.id,
                        dependency.depends_on_id,
                        "depends_on"
                    ])
                    .map_err(|_| WriteFault::Other)?;
            }
        }
    }
    apply_provenance_txn(connection, merged)?;
    // Provenance rows naming removed issues as targets go after the
    // event transitions, mirroring the reducer's retain pass.
    if !removed_bead_refs.is_empty() {
        let placeholders = removed_bead_refs
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        for target_ref in removed_bead_refs {
            params.push(target_ref);
        }
        connection
            .execute(
                &format!(
                    "DELETE FROM link_provenance WHERE target_ref IN ({placeholders})"
                ),
                params.as_slice(),
            )
            .map_err(|_| WriteFault::Other)?;
    }
    Ok(())
}

/// One collapse candidate: keep the earliest `(created_at, id)` per
/// non-empty external ref, exactly the full post-pass winner rule.
fn consider_candidate<'a>(
    winners: &mut BTreeMap<&'a str, &'a str>,
    refs: &BTreeMap<&'a str, &'a str>,
    created: &BTreeMap<&'a str, &'a str>,
    removed: &BTreeSet<String>,
    id: &'a str,
) {
    if removed.contains(id) {
        return;
    }
    let external_ref = refs.get(id).copied().unwrap_or("");
    if external_ref.trim().is_empty() {
        return;
    }
    let key = (created.get(id).copied().unwrap_or(""), id);
    winners
        .entry(external_ref)
        .and_modify(|winner| {
            let current =
                (created.get(*winner).copied().unwrap_or(""), *winner);
            if key < current {
                *winner = id;
            }
        })
        .or_insert(id);
}

/// Delete rows whose `id` (or edge `src`) is in the set.
fn delete_ids(
    connection: &Connection,
    table: &str,
    ids: &[String],
) -> Result<(), WriteFault> {
    if ids.is_empty() {
        return Ok(());
    }
    let column = if table == "edges" { "src" } else { "id" };
    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for id in ids {
        params.push(id);
    }
    connection
        .execute(
            &format!("DELETE FROM {table} WHERE {column} IN ({placeholders})"),
            params.as_slice(),
        )
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}

/// Mirror `apply_link_provenance` as SQL transitions in event order.
///
/// Link validation already ran inside `apply_event` before the
/// transaction opened, so only database faults can fail here. Removals
/// delete by identity; each issue removal also clears rows naming the
/// removed issues as sources, and the caller clears removed targets
/// afterwards, exactly as the reducer's retain pass does.
fn apply_provenance_txn(
    connection: &Connection,
    merged: &[&BeadEventRecordWire],
) -> Result<(), WriteFault> {
    use crate::artifact_link::{
        canonicalize_artifact_link_ref, lookup_artifact_relation,
        validate_artifact_link_description,
    };
    for event in merged {
        match &event.payload {
            BeadEventPayloadWire::LinkAdded {
                target_ref,
                relation,
                description,
                origin,
                direction,
                uses,
                ..
            } => {
                let canonical = canonicalize_artifact_link_ref(target_ref)
                    .map_err(|_| WriteFault::Other)?;
                lookup_artifact_relation(relation)
                    .map_err(|_| WriteFault::Other)?;
                let description =
                    validate_artifact_link_description(description)
                        .map_err(|_| WriteFault::Other)?;
                connection
                    .execute(
                        "DELETE FROM link_provenance WHERE source_issue_id = ?1 AND relation = ?2 AND target_ref = ?3 AND direction = ?4",
                        rusqlite::params![
                            event.issue_id,
                            relation,
                            canonical,
                            wire_string(direction),
                        ],
                    )
                    .map_err(|_| WriteFault::Other)?;
                connection
                    .execute(
                        "INSERT INTO link_provenance (target_ref, source_issue_id, relation, description, origin, direction, uses, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        rusqlite::params![
                            canonical,
                            event.issue_id,
                            relation,
                            description,
                            wire_string(origin),
                            wire_string(direction),
                            *uses as i64,
                            event.actor,
                            event.timestamp,
                        ],
                    )
                    .map_err(|_| WriteFault::Other)?;
            }
            BeadEventPayloadWire::LinkRemoved {
                target_ref,
                relation,
                direction,
                ..
            } => {
                if let Ok(canonical) =
                    canonicalize_artifact_link_ref(target_ref)
                {
                    connection
                        .execute(
                            "DELETE FROM link_provenance WHERE source_issue_id = ?1 AND relation = ?2 AND target_ref = ?3 AND direction = ?4",
                            rusqlite::params![
                                event.issue_id,
                                relation,
                                canonical,
                                wire_string(direction),
                            ],
                        )
                        .map_err(|_| WriteFault::Other)?;
                }
            }
            BeadEventPayloadWire::IssueRemoved {
                cascade_removed_issue_ids,
            } => {
                let mut removed: Vec<String> =
                    cascade_removed_issue_ids.clone();
                removed.push(event.issue_id.clone());
                let placeholders =
                    removed.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
                for id in &removed {
                    params.push(id);
                }
                connection
                    .execute(
                        &format!(
                            "DELETE FROM link_provenance WHERE source_issue_id IN ({placeholders})"
                        ),
                        params.as_slice(),
                    )
                    .map_err(|_| WriteFault::Other)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Post-commit guard shared with the rebuild path: a concurrent mutation
/// that landed mid-commit and is still visible afterwards discards the
/// file so the cache never serves stale data.
fn guard_committed_streams(
    beads_dir: &Path,
    cache_path: &Path,
    committed: &[(String, FileSignature)],
) -> Result<(), Fault> {
    match sweep_store_signatures(beads_dir) {
        Ok(current) if current.streams == *committed => Ok(()),
        _ => {
            drop_cache_file(cache_path);
            Err(Fault::Cache)
        }
    }
}

/// The committed stream signatures in sweep-comparison order.
fn committed_sweep_sigs(
    refreshed: &BTreeMap<String, (u64, i64, u64, u64, String)>,
) -> Vec<(String, FileSignature)> {
    refreshed
        .iter()
        .map(|(stream_id, (size, mtime_ns, inode, _, _))| {
            (
                stream_id.clone(),
                FileSignature {
                    size: *size,
                    mtime_ns: *mtime_ns,
                    inode: *inode,
                },
            )
        })
        .collect()
}
