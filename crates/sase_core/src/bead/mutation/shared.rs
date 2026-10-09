//! One copy of the mutation write helpers shared by every mutation path.
//!
//! Event minting, lazy physical-stream loading, and the manifest total each
//! exist exactly once here. Every mutation family delegates to these, so
//! cached and replay backings run the same byte-preserving write and the
//! same publish contract.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::bead::config::save_config;
use crate::bead::config::BeadConfigWire;
use crate::bead::events::mint_bead_event_id;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventRecordWire;
use crate::bead::events::BeadEventStreamWire;
use crate::bead::events::BEAD_EVENT_SCHEMA_VERSION;
use crate::bead::jsonl::event_streams_dir;
use crate::bead::jsonl::read_event_stream_file;
use crate::bead::jsonl::write_event_store_changed_with_total_and_signatures;
use crate::bead::read_model::AppendedStream;
use crate::bead::read_model::CacheWitness;
use crate::bead::wire::BeadError;
use crate::bead::wire::IssueWire;

/// Mint one event onto a loaded stream, preserving historical bytes.
///
/// The ordinal is the stream's next position, and the event ID binds the
/// stream, ordinal, timestamp, actor, operation, issue, and payload. This
/// is the single minting helper for every mutation: both the cached and
/// the replay backing mint through `MutationView::stage_event`, which
/// calls this helper, so ordinals and event IDs match byte for byte.
pub(crate) fn mint_stream_event(
    stream: &mut BeadEventStreamWire,
    operation: BeadEventOperationWire,
    payload: BeadEventPayloadWire,
    timestamp: &str,
    actor: &str,
    issue_id: &str,
) -> Result<String, BeadError> {
    let ordinal = stream.events.len() + 1;
    let event_id = mint_bead_event_id(
        &stream.stream_id,
        ordinal,
        timestamp,
        actor,
        operation,
        issue_id,
        &payload,
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
    stream.events.push(event);
    Ok(event_id)
}

/// Load one physical stream for a warm mutation.
///
/// Returns the stream plus whether it already existed. A missing file
/// yields `Ok(None)` so the caller falls back to replay (which owns the
/// corruption error), except when the caller is creating a brand-new
/// stream. Only a successful read of an existing file counts a stream
/// read; a brand-new stream costs no stream I/O.
pub(crate) fn load_mutation_stream(
    beads_dir: &Path,
    stream_id: &str,
) -> Result<Option<BeadEventStreamWire>, BeadError> {
    let path = event_streams_dir(beads_dir).join(format!("{stream_id}.jsonl"));
    if !path.is_file() {
        return Ok(None);
    }
    match read_event_stream_file(&path) {
        Ok((stream, _signature)) => {
            #[cfg(test)]
            super::store::store_io_stats::record_stream_reads(1);
            Ok(Some(stream))
        }
        Err(_) => Ok(None),
    }
}

/// New empty stream for a brand-new `stream_id` (no stream I/O counted).
pub(crate) fn new_mutation_stream(stream_id: &str) -> BeadEventStreamWire {
    BeadEventStreamWire {
        stream_id: stream_id.to_string(),
        root_issue_id: stream_id.to_string(),
        events: Vec::new(),
    }
}

/// Manifest total for a write that adds `new_streams` streams.
///
/// Reads the `stream_count` from the on-disk manifest (never a directory
/// scan) and adds the number of brand-new streams. Returns `None` when the
/// manifest is missing or unreadable, in which case the caller falls back
/// to replay.
pub(crate) fn manifest_total_for_write(
    beads_dir: &Path,
    new_streams: usize,
) -> Option<usize> {
    let text =
        std::fs::read_to_string(beads_dir.join("events/manifest.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value
        .get("stream_count")?
        .as_u64()
        .map(|count| count as usize + new_streams)
}

/// Durably write staged streams, persist the config, and publish.
///
/// This is the single commit tail for every warm mutation: the changed
/// streams go through the byte-preserving writer with the true physical
/// stream total, the config persists, and the delta publishes through the
/// `publish-direct` API with reducer-truth corrections applied to
/// `expected`. Cache faults after the durable append never fail the
/// mutation: a lost CAS skips, any other fault invalidates for the next
/// read to repair, and the events stand either way.
///
/// Returns `Ok(None)` when the manifest is missing so the caller falls
/// back to replay (which owns the legacy-store behavior). Otherwise
/// returns the corrected rows in `expected` order.
pub(crate) fn commit_staged_write(
    beads_dir: &Path,
    cache_path: Option<&Path>,
    witness: Option<&CacheWitness>,
    config: &BeadConfigWire,
    streams: &[BeadEventStreamWire],
    base_lens: &[usize],
    expected: &[(String, IssueWire)],
) -> Result<Option<Vec<IssueWire>>, BeadError> {
    let changed: BTreeSet<String> = streams
        .iter()
        .map(|stream| stream.stream_id.clone())
        .collect();
    #[cfg(test)]
    super::store::store_io_stats::record_save();
    #[cfg(test)]
    super::store::store_io_stats::record_validation_runs(expected.len() as u64);
    let total = manifest_total_for_write(
        beads_dir,
        streams
            .iter()
            .filter(|stream| {
                !event_streams_dir(beads_dir)
                    .join(format!("{}.jsonl", stream.stream_id))
                    .is_file()
            })
            .count(),
    );
    // Recompute new-stream membership from the filesystem at commit time:
    // `manifest_total_for_write` already added it to the total, and the
    // writer takes the total as the source of truth.
    let Some(total) = total else {
        return Ok(None);
    };
    let mut signatures: BTreeMap<
        String,
        crate::bead::jsonl::StreamWriteSignature,
    > = write_event_store_changed_with_total_and_signatures(
        beads_dir, streams, &changed, total,
    )?;
    save_config(beads_dir, config)?;
    let mut corrected: Vec<(String, IssueWire)> = Vec::new();
    if let Some(cache_path) = cache_path {
        let appended: Vec<AppendedStream> = streams
            .iter()
            .zip(base_lens.iter())
            .filter_map(|(stream, base_len)| {
                signatures.remove(&stream.stream_id).map(|signature| {
                    AppendedStream {
                        stream_id: stream.stream_id.clone(),
                        events: stream.events[*base_len..].to_vec(),
                        signature,
                    }
                })
            })
            .collect();
        if !appended.is_empty() {
            corrected = super::publish::publish_cached_write(
                beads_dir, cache_path, witness, &appended, expected,
            )
            .corrections();
        }
    }
    let mut rows: Vec<IssueWire> =
        expected.iter().map(|(_, issue)| issue.clone()).collect();
    super::publish::apply_corrections(&mut rows, &corrected);
    Ok(Some(rows))
}
