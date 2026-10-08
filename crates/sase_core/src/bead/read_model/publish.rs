//! Snapshot-free direct publication of mutation writes.
//!
//! After a mutation appends already-validated events under the `beads.db`
//! flock, this module writes their delta straight through to the read
//! model in one SQLite transaction: no second full-stat sweep and no
//! full-snapshot load. The admission witness (generation,
//! `content_generation`, frontier, token) captured by the forced
//! freshness sweep, plus the writer-captured stream signatures, is the
//! only pre-write state the commit trusts.
//!
//! Event application reuses the exact tail machinery the read path
//! uses — merge ordering, touched-only resume, and the row/edge/suffix/
//! allocation/provenance/signature writers — so publication and
//! read-side tail produce identical rows. Only the commit differs: it
//! upserts the changed stream signatures, recomputes the token after
//! the write, bumps `content_generation`, and leaves `last_sweep_ns`
//! at the admission sweep time so out-of-band writers stay bounded by
//! the 60 s rule and the next read serves token-only.
//!
//! Outcomes: `Published` carries reducer-truth corrections for the
//! mutation's overlaid rows; `Skipped` (no usable cache, or a lost
//! content-generation race) writes nothing — the append already moved
//! the token, and the read path's guarded tail repairs; `Invalidated`
//! drops the cache file so the next read cannot serve stale rows and
//! instead rebuilds from a full replay. After a durable append a cache
//! problem never fails the mutation, never retries it, and never
//! appends twice.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rusqlite::Connection;

use crate::bead::events::{
    apply_event, canonical_bead_source_ref, merge_stream_events,
    BeadEventPayloadWire, BeadEventRecordWire, BeadEventStreamWire,
};
use crate::bead::jsonl::{event_streams_dir, StreamWriteSignature};
use crate::bead::wire::IssueWire;

use super::freshness::{freshness_token, sweep_store_signatures};
use super::store::{
    bump_outcome_in_txn, drop_cache_file, load_stored_stream_sigs,
    open_read_write, parse_frontier, read_meta, rebuild_from_replay,
    record_manifest_config_in_txn, record_refresh_in_txn, set_token_in_txn,
    Fault, ManifestConfigFingerprint, RefreshFinish, StoredStreamSig,
    WriteFault, REBUILD_BUSY_TIMEOUT,
};
use super::tail::{
    gate_manifest_config, load_issue_index, load_rows, meta_set,
    query_dependents, resume_from_partial, tail_load_plan, tail_merge_key,
    try_tail_apply, write_refreshed_streams, write_tail_rows, IssueIndex,
    NewStreamSig, ResumedTail, TailDecision,
};

/// Baseline witness held from mutation admission to publication.
///
/// Captured by the forced freshness sweep inside the mutation flock:
/// the SQLite generation, content generation, merge frontier, and
/// cache token the publication commit compares against. Any mismatch
/// means another publisher committed first, and publication skips
/// instead of pairing newer signatures with older rows.
#[derive(Debug, Clone, Default)]
pub(crate) struct CacheWitness {
    pub(crate) generation: u64,
    pub(crate) content_generation: u64,
    pub(crate) frontier: String,
    pub(crate) token: String,
}

/// One stream the mutation just appended, with its new tail events and
/// the writer-captured signature of the stored file.
pub(crate) struct AppendedStream {
    pub(crate) stream_id: String,
    pub(crate) events: Vec<BeadEventRecordWire>,
    pub(crate) signature: StreamWriteSignature,
}

/// What direct publication decided.
#[derive(Debug)]
pub(crate) enum PublishOutcome {
    /// Delta committed; `corrected` holds reducer-truth rows for
    /// overlaid rows the cache disagrees with.
    Published { corrected: Vec<(String, IssueWire)> },
    /// No usable cache, or another publisher committed first. Nothing
    /// was written; the read path repairs on next access.
    Skipped,
    /// The cache could not accept this append (backdated events,
    /// config change, stream-set change, or a cache fault). The file
    /// was dropped so the next read rebuilds instead of serving stale
    /// rows.
    Invalidated { reason: String },
}

impl PublishOutcome {
    /// Reducer-truth corrections for the published write.
    ///
    /// Only `Published` carries any: skips and invalidations leave the
    /// mutation's overlaid rows standing while the read path repairs.
    /// The debug assertion documents that every invalidation names its
    /// reason for the failure-recovery proof.
    pub(crate) fn corrections(self) -> Vec<(String, IssueWire)> {
        match self {
            PublishOutcome::Published { corrected } => corrected,
            PublishOutcome::Skipped => Vec::new(),
            PublishOutcome::Invalidated { reason } => {
                debug_assert!(
                    !reason.is_empty(),
                    "read-model publication invalidated: {reason}"
                );
                Vec::new()
            }
        }
    }
}

/// Publish appended, already-validated events straight to the read model.
///
/// `witness` is the admission witness, `appended` the new tail events
/// per changed stream with writer-captured signatures, `fingerprint`
/// the post-write manifest/config fingerprint, and `expected` the
/// mutation's overlaid rows for reducer-truth correction. Runs one
/// transaction: no full sweep, no snapshot load, no row
/// deserialization beyond the affected set.
pub(crate) fn publish_mutation_write(
    beads_dir: &Path,
    cache_path: &Path,
    witness: &CacheWitness,
    appended: &[AppendedStream],
    fingerprint: &ManifestConfigFingerprint,
    expected: &[(String, IssueWire)],
) -> PublishOutcome {
    match publish_inner(
        beads_dir,
        cache_path,
        witness,
        appended,
        fingerprint,
        expected,
    ) {
        Ok(outcome) => outcome,
        Err(reason) => {
            drop_cache_file(cache_path);
            PublishOutcome::Invalidated { reason }
        }
    }
}

/// Publication failure that must drop the cache file. A lost race is
/// not an error: it returns `Ok(Skipped)` without writing anything.
type PublishResult = Result<PublishOutcome, String>;

fn publish_inner(
    beads_dir: &Path,
    cache_path: &Path,
    witness: &CacheWitness,
    appended: &[AppendedStream],
    fingerprint: &ManifestConfigFingerprint,
    expected: &[(String, IssueWire)],
) -> PublishResult {
    let connection = match open_read_write(cache_path, REBUILD_BUSY_TIMEOUT) {
        Ok(connection) => connection,
        Err(_) => return Ok(PublishOutcome::Skipped),
    };
    let meta = read_meta(&connection).map_err(|_| "unreadable cache meta")?;
    // Content-generation CAS against the admission witness: any commit
    // since admission (another publisher won) skips without writing,
    // instead of pairing newer signatures with older rows.
    if meta.generation != witness.generation
        || meta.content_generation != witness.content_generation
        || meta.frontier != witness.frontier
        || meta.token != witness.token
    {
        return Ok(PublishOutcome::Skipped);
    }
    if !meta.streams_known {
        return Err("stream signatures not recorded".to_string());
    }
    if meta.frontier.is_empty() {
        return Err("no merge frontier recorded".to_string());
    }
    let stored = load_stored_stream_sigs(&connection);
    if check_stream_set_unchanged(beads_dir, &stored, appended).is_err() {
        return repair_publish(beads_dir, cache_path, &connection, expected);
    }
    let new_stream_count = appended
        .iter()
        .filter(|stream| !stored.contains_key(&stream.stream_id))
        .count();
    if gate_manifest_config(&meta, fingerprint, new_stream_count).is_some() {
        return repair_publish(beads_dir, cache_path, &connection, expected);
    }
    // Every appended merge key must sort after the stored frontier;
    // anything at or before it (clock skew, backdate, relocation,
    // conflict rewrite) invalidates instead of pairing stale rows with
    // newer signatures.
    let frontier = parse_frontier(&meta.frontier);
    let mut tail_event_count = 0usize;
    let mut max_key: Option<(String, usize, String)> = None;
    for stream in appended {
        for event in &stream.events {
            let key = tail_merge_key(event);
            if key <= frontier {
                return repair_publish(
                    beads_dir,
                    cache_path,
                    &connection,
                    expected,
                );
            }
            if max_key.as_ref().is_none_or(|current| key > *current) {
                max_key = Some(key);
            }
            tail_event_count += 1;
        }
    }
    // Merge the appended tails in full sorted stream order (empty
    // placeholders keep the stream-index tiebreak identical to a full
    // replay). With every appended key past the frontier, merging only
    // the tails reproduces the replay's tail order exactly.
    let mut tails: BTreeMap<&str, &[BeadEventRecordWire]> = BTreeMap::new();
    for stream in appended {
        tails.insert(stream.stream_id.as_str(), stream.events.as_slice());
    }
    let mut stream_ids: BTreeSet<&str> =
        stored.keys().map(String::as_str).collect();
    for stream in appended {
        stream_ids.insert(stream.stream_id.as_str());
    }
    let ordered: Vec<BeadEventStreamWire> = stream_ids
        .into_iter()
        .map(|stream_id| BeadEventStreamWire {
            stream_id: stream_id.to_string(),
            root_issue_id: stream_id.to_string(),
            events: tails.remove(stream_id).unwrap_or_default().to_vec(),
        })
        .collect();
    let merged = merge_stream_events(&ordered);
    // Touched-only resume, exactly as the read-side tail plans it: rows
    // for issues the tail can observe, plus the compact index only when
    // the tail can change the id set or collapse winners.
    let plan = tail_load_plan(&merged);
    let index = if plan.needs_index() {
        load_issue_index(&connection)
            .map_err(|error| format!("unreadable issue index: {error}"))?
    } else {
        IssueIndex::empty()
    };
    let dependents = query_dependents(&connection, &plan.removed_ids)
        .map_err(|error| format!("unreadable dependents: {error}"))?;
    let mut load_ids: BTreeSet<&str> =
        plan.touched_ids.iter().map(String::as_str).collect();
    load_ids.extend(dependents.iter().map(String::as_str));
    load_ids.extend(plan.dep_targets.iter().map(String::as_str));
    let old_rows = load_rows(&connection, &load_ids)
        .map_err(|error| format!("unreadable cached rows: {error}"))?;
    let mut partial: BTreeMap<String, IssueWire> = BTreeMap::new();
    for (id, (row, _)) in &old_rows {
        let issue: IssueWire = serde_json::from_str(row)
            .map_err(|error| format!("unreadable cached row: {error}"))?;
        partial.insert(id.clone(), issue);
    }
    // A creation colliding with an issue the partial map cannot see
    // fails exactly as the full replay would: the cache disagrees with
    // the overlay, so invalidate and let the rebuild decide.
    for event in &merged {
        if let BeadEventPayloadWire::IssueCreated { issue } = &event.payload {
            if index.by_id.contains_key(issue.id.as_str())
                && !partial.contains_key(&issue.id)
            {
                return Err(format!(
                    "duplicate issue_created event for {}",
                    issue.id
                ));
            }
        }
    }
    for event in &merged {
        apply_event(&mut partial, event)
            .map_err(|error| format!("unreducible published tail: {error}"))?;
    }
    for issue in partial.values() {
        issue
            .validate()
            .map_err(|error| format!("invalid published row: {error}"))?;
    }
    let resumed = resume_from_partial(&index, &partial, &plan, &old_rows)
        .map_err(|_| "unresumable published tail".to_string())?;
    let new_frontier = match max_key {
        Some((timestamp, priority, event_id)) => {
            super::store::format_frontier(&timestamp, priority, &event_id)
        }
        None => meta.frontier.clone(),
    };
    commit_published(
        beads_dir,
        &connection,
        witness,
        appended,
        fingerprint,
        expected,
        &stored,
        &merged,
        &index,
        &resumed,
        &new_frontier,
        tail_event_count,
    )
}

/// Synchronous guarded repair for appends the direct commit cannot take.
///
/// Runs the same tail-or-rebuild refresh the replaced sweep-based
/// publication ran inside the mutation, minus the snapshot load:
/// readiness only, with reducer-truth corrections from point lookups
/// of the mutation's overlaid rows. Outcome counters move exactly as
/// the old path moved them, so parity accounting is unchanged. A
/// repair fault is fail-open like the path it replaces: the events
/// stand and the next read tails, rebuilds, or replays.
fn repair_publish(
    beads_dir: &Path,
    cache_path: &Path,
    connection: &Connection,
    expected: &[(String, IssueWire)],
) -> PublishResult {
    let repaired = (|| {
        let sweep =
            sweep_store_signatures(beads_dir).map_err(|_| Fault::Cache)?;
        let meta = read_meta(connection).map_err(|_| Fault::Cache)?;
        let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
        match try_tail_apply(
            beads_dir,
            cache_path,
            connection,
            &meta,
            &token,
            &sweep,
            RefreshFinish::Readiness,
        )? {
            TailDecision::Served(_) | TailDecision::Tailed(_) => {}
            TailDecision::Fallback(reason) => {
                rebuild_from_replay(
                    beads_dir,
                    cache_path,
                    &token,
                    meta.generation,
                    &format!("publish repair: {reason}"),
                )?;
            }
        }
        Ok::<(), Fault>(())
    })();
    match repaired {
        Ok(()) => Ok(PublishOutcome::Published {
            corrected: truth_rows(connection, expected),
        }),
        Err(_) => Ok(PublishOutcome::Published {
            corrected: Vec::new(),
        }),
    }
}

/// Reducer-truth rows for the overlaid ids via bounded point lookups.
///
/// Fail-open: any lookup fault yields no corrections, exactly as the
/// replaced snapshot re-read did when a row was missing.
fn truth_rows(
    connection: &Connection,
    expected: &[(String, IssueWire)],
) -> Vec<(String, IssueWire)> {
    let ids: BTreeSet<&str> =
        expected.iter().map(|(id, _)| id.as_str()).collect();
    let Ok(rows) = load_rows(connection, &ids) else {
        return Vec::new();
    };
    let mut corrected = Vec::new();
    for (issue_id, overlay) in expected {
        let Some((row, _)) = rows.get(issue_id.as_str()) else {
            continue;
        };
        let Ok(stored): Result<IssueWire, _> = serde_json::from_str(row) else {
            continue;
        };
        if serde_json::to_value(&stored).unwrap_or_default()
            != serde_json::to_value(overlay).unwrap_or_default()
        {
            corrected.push((issue_id.clone(), stored));
        }
    }
    corrected
}

/// The streams on disk must be exactly the stored set plus the streams
/// this mutation wrote: a cheap name-only directory listing (no
/// per-file stat) that catches an out-of-band add, remove, or rename
/// during publication without paying a full sweep.
fn check_stream_set_unchanged(
    beads_dir: &Path,
    stored: &BTreeMap<String, StoredStreamSig>,
    appended: &[AppendedStream],
) -> Result<(), String> {
    let streams_dir = event_streams_dir(beads_dir);
    let mut on_disk = BTreeSet::new();
    let read = std::fs::read_dir(&streams_dir)
        .map_err(|error| format!("unreadable streams directory: {error}"))?;
    for entry in read {
        let path = entry
            .map_err(|error| format!("unreadable stream entry: {error}"))?
            .path();
        if path.is_file()
            && path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
        {
            if let Some(name) = path.file_stem().and_then(|stem| stem.to_str())
            {
                on_disk.insert(name.to_string());
            }
        }
    }
    let mut expected: BTreeSet<String> = stored.keys().cloned().collect();
    for stream in appended {
        expected.insert(stream.stream_id.clone());
    }
    if on_disk != expected {
        return Err(
            "event streams added or removed during publication".to_string()
        );
    }
    Ok(())
}

/// Commit resumed rows plus writer signatures and frontier in one
/// transaction under the admission-witness CAS.
///
/// Only touched rows are rewritten and only changed stream signatures
/// are upserted; the token is recomputed after the write while
/// `last_sweep_ns` stays at the admission sweep time. A lost race
/// rolls back and skips; any other write fault rolls back, drops the
/// file, and invalidates.
#[allow(clippy::too_many_arguments)]
fn commit_published(
    beads_dir: &Path,
    connection: &Connection,
    witness: &CacheWitness,
    appended: &[AppendedStream],
    fingerprint: &ManifestConfigFingerprint,
    expected: &[(String, IssueWire)],
    stored: &BTreeMap<String, StoredStreamSig>,
    merged: &[&BeadEventRecordWire],
    index: &IssueIndex,
    resumed: &ResumedTail,
    new_frontier: &str,
    tail_event_count: usize,
) -> PublishResult {
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
    let contributing = appended
        .iter()
        .filter(|stream| !stream.events.is_empty())
        .count();
    let reason = format!(
        "publish {tail_event_count} events over {contributing} streams"
    );
    let mut tail_sigs: BTreeMap<String, NewStreamSig> = BTreeMap::new();
    for stream in appended {
        tail_sigs.insert(
            stream.stream_id.clone(),
            NewStreamSig {
                size: stream.signature.size,
                mtime_ns: stream.signature.mtime_ns,
                inode: stream.signature.inode,
                byte_len: stream.signature.byte_len,
                content_hash: stream.signature.content_hash.clone(),
            },
        );
    }
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
    for (stream_id, new_sig) in &tail_sigs {
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
    let token = freshness_token(beads_dir)
        .map_err(|error| format!("unreadable freshness token: {error}"))?;
    let start_generation = witness.generation;
    let start_content = witness.content_generation;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(|_| "unwritable read-model cache".to_string())?;
    let outcome = write_tail_rows(
        connection,
        resumed,
        &parent_of,
        &removed_bead_refs,
        merged,
    )
    .and_then(|()| {
        write_refreshed_streams(connection, &refreshed, &tail_sigs)
    })
    .and_then(|()| set_token_in_txn(connection, &token))
    .and_then(|()| {
        meta_set(connection, "frontier", new_frontier)?;
        record_manifest_config_in_txn(connection, fingerprint)?;
        bump_outcome_in_txn(connection, "outcome_tail")?;
        record_refresh_in_txn(connection, "tail", &reason)?;
        // Admission-witness CAS last: generation alone cannot
        // distinguish two successive publications, so the content
        // generation must still match the admission value. A lost
        // race rolls back and skips instead of pairing newer
        // signatures with older rows.
        let generation_held = connection
            .execute(
                "UPDATE meta SET value = value WHERE key = 'generation' AND value = ?1",
                rusqlite::params![start_generation.to_string()],
            )
            .map_err(|_| WriteFault::Other)?;
        if generation_held != 1 {
            return Err(WriteFault::CasLost);
        }
        let new_content = start_content.saturating_add(1).to_string();
        let content_held = connection
            .execute(
                "UPDATE meta SET value = ?1 WHERE key = 'content_generation' AND value = ?2",
                rusqlite::params![new_content, start_content.to_string()],
            )
            .map_err(|_| WriteFault::Other)?;
        if content_held != 1 {
            return Err(WriteFault::CasLost);
        }
        Ok(())
    });
    match outcome {
        Ok(()) => {
            connection
                .execute_batch("COMMIT")
                .map_err(|_| "uncommittable publication".to_string())?;
        }
        Err(WriteFault::CasLost) => {
            let _ = connection.execute_batch("ROLLBACK");
            return Ok(PublishOutcome::Skipped);
        }
        Err(WriteFault::Other) => {
            let _ = connection.execute_batch("ROLLBACK");
            return Err("unwritable publication".to_string());
        }
    }
    #[cfg(test)]
    crate::bead::mutation::store_io_stats::record_published_rows(
        (resumed.upserts.len() + resumed.deletes.len()) as u64,
    );
    Ok(PublishOutcome::Published {
        corrected: reducer_truth_corrections(resumed, expected),
    })
}

/// Reducer-truth corrections for the mutation's overlaid rows, from the
/// resumed upserts without reopening the database.
///
/// When the cache disagrees with the overlay, the stored row wins so the
/// outcome reflects durable state. Rows the resume did not touch need no
/// correction.
fn reducer_truth_corrections(
    resumed: &ResumedTail,
    expected: &[(String, IssueWire)],
) -> Vec<(String, IssueWire)> {
    let truth: BTreeMap<&str, &IssueWire> = resumed
        .upserts
        .iter()
        .map(|(id, _, issue)| (id.as_str(), issue))
        .collect();
    let mut corrected = Vec::new();
    for (issue_id, overlay) in expected {
        let Some(stored) = truth.get(issue_id.as_str()) else {
            continue;
        };
        if serde_json::to_value(stored).unwrap_or_default()
            != serde_json::to_value(overlay).unwrap_or_default()
        {
            corrected.push((issue_id.clone(), (*stored).clone()));
        }
    }
    corrected
}
