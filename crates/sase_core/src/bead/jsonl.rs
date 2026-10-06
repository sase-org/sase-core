//! JSONL import/export for git-portable bead storage.

use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use super::events::{
    compare_issues_canonically, reduce_event_streams, BeadEventRecordWire,
    BeadEventStoreManifestWire, BeadEventStreamWire, BEAD_EVENT_SCHEMA_VERSION,
};
use super::wire::{
    deserialize_valid_issue, invalid_record_error,
    validate_unique_external_refs, BeadError, BeadTierWire, IssueTypeWire,
    IssueWire,
};

static ATOMIC_WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JsonlLoadOutcome {
    pub issues: Vec<IssueWire>,
    pub loaded_rows: usize,
    pub blank_lines: usize,
    pub invalid_json_lines: usize,
    pub invalid_record_lines: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadEventManifestRepairStatusWire {
    Noop,
    Repaired,
    InvalidStream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventManifestRepairOutcomeWire {
    pub status: BeadEventManifestRepairStatusWire,
    pub manifest_path: String,
    pub stream_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn parse_issues_jsonl(input: &str) -> JsonlLoadOutcome {
    let mut outcome = JsonlLoadOutcome::default();
    if input.trim().is_empty() {
        return outcome;
    }

    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() {
            outcome.blank_lines += 1;
            continue;
        }
        let value = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(_) => {
                outcome.invalid_json_lines += 1;
                continue;
            }
        };
        match deserialize_valid_issue(value) {
            Ok(issue) => {
                outcome.loaded_rows += 1;
                outcome.issues.push(issue);
            }
            Err(error) => {
                let _ = invalid_record_error(error);
                outcome.invalid_record_lines += 1;
            }
        }
    }

    apply_missing_tiers(&mut outcome.issues);
    outcome.issues.retain(|issue| issue.validate().is_ok());
    outcome
        .issues
        .sort_by(|a, b| issue_import_key(a).cmp(&issue_import_key(b)));
    outcome
}

pub fn import_issues_from_jsonl(
    path: &Path,
) -> Result<JsonlLoadOutcome, BeadError> {
    if !path.exists() {
        return Ok(JsonlLoadOutcome::default());
    }
    let contents = fs::read_to_string(path)?;
    let outcome = parse_issues_jsonl(&contents);
    validate_unique_external_refs(&outcome.issues)?;
    Ok(outcome)
}

pub fn export_issues_to_jsonl(
    issues: &[IssueWire],
) -> Result<String, BeadError> {
    let mut sorted = issues.to_vec();
    sorted.sort_by(compare_issues_canonically);

    let mut output = String::new();
    for issue in sorted {
        issue.validate()?;
        output.push_str(&serde_json::to_string(&issue)?);
        output.push('\n');
    }
    validate_unique_external_refs(issues)?;
    Ok(output)
}

pub fn write_issues_jsonl(
    beads_dir: &Path,
    issues: &[IssueWire],
) -> Result<(), BeadError> {
    let jsonl = export_issues_to_jsonl(issues)?;
    write_file_atomic(&beads_dir.join("issues.jsonl"), jsonl.as_bytes())
}

pub fn event_store_present(beads_dir: &Path) -> bool {
    event_manifest_path(beads_dir).exists()
        || event_streams_dir(beads_dir).exists()
}

pub fn event_manifest_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join("events").join("manifest.json")
}

pub fn event_streams_dir(beads_dir: &Path) -> PathBuf {
    beads_dir.join("events").join("streams")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovedFlagStreamPruneOutcomeWire {
    pub pruned_stream_ids: Vec<String>,
    pub rewritten_manifest: bool,
}

/// Drop event streams of removed beads whose `issue_created` payload is the
/// retired flag issue type, then rewrite `events/manifest.json`.
///
/// `sase bead rm` leaves a tombstoned stream on disk. After the flag issue
/// type is deleted from the wire, those files cannot be deserialized.
///
/// This mutates the store, so it never runs on the read path: it runs under
/// the mutation lock (`MutableStore::load`) and through the
/// `bead_prune_removed_flag_event_streams` binding. Readers skip
/// removed-flag tombstones in memory instead.
pub fn prune_removed_flag_event_streams(
    beads_dir: &Path,
) -> Result<RemovedFlagStreamPruneOutcomeWire, BeadError> {
    let streams_dir = event_streams_dir(beads_dir);
    if !streams_dir.is_dir() {
        return Ok(RemovedFlagStreamPruneOutcomeWire {
            pruned_stream_ids: Vec::new(),
            rewritten_manifest: false,
        });
    }

    let mut pruned_stream_ids = Vec::new();
    let mut live_flag_ids = Vec::new();
    for path in list_event_stream_paths(&streams_dir)? {
        let stream_id = path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        match classify_flag_stream(&path)? {
            FlagStreamKind::RemovedFlag => {
                fs::remove_file(&path).map_err(|err| {
                    BeadError::io(format!(
                        "failed to prune removed flag stream {}: {err}",
                        path.display()
                    ))
                })?;
                pruned_stream_ids.push(stream_id);
            }
            FlagStreamKind::LiveFlag => live_flag_ids.push(stream_id),
            FlagStreamKind::Other => {}
        }
    }

    if !live_flag_ids.is_empty() {
        live_flag_ids.sort();
        return Err(BeadError::validation(format!(
            "bead event store still has live flag issue-type streams: {}; migrate or remove them before loading",
            live_flag_ids.join(", ")
        )));
    }

    if pruned_stream_ids.is_empty() {
        return Ok(RemovedFlagStreamPruneOutcomeWire {
            pruned_stream_ids,
            rewritten_manifest: false,
        });
    }

    pruned_stream_ids.sort();
    let remaining = list_event_stream_paths(&streams_dir)?.len();
    rewrite_manifest_stream_count(beads_dir, remaining)?;
    Ok(RemovedFlagStreamPruneOutcomeWire {
        pruned_stream_ids,
        rewritten_manifest: true,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlagStreamKind {
    Other,
    LiveFlag,
    RemovedFlag,
}

fn classify_flag_stream(path: &Path) -> Result<FlagStreamKind, BeadError> {
    let contents = fs::read_to_string(path).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead event stream {}: {err}",
            path.display()
        ))
    })?;
    let mut created_flag = false;
    let mut removed = false;
    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            return Ok(FlagStreamKind::Other);
        };
        let operation = value
            .get("operation")
            .and_then(|item| item.as_str())
            .unwrap_or("");
        if operation == "issue_created" {
            let issue_type = value
                .pointer("/payload/issue/issue_type")
                .and_then(|item| item.as_str());
            if issue_type == Some("flag") {
                created_flag = true;
            }
        }
        if operation == "issue_removed" {
            removed = true;
        }
    }
    Ok(match (created_flag, removed) {
        (true, true) => FlagStreamKind::RemovedFlag,
        (true, false) => FlagStreamKind::LiveFlag,
        _ => FlagStreamKind::Other,
    })
}

fn list_event_stream_paths(
    streams_dir: &Path,
) -> Result<Vec<PathBuf>, BeadError> {
    let mut stream_paths = Vec::new();
    for entry in fs::read_dir(streams_dir).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead event streams directory {}: {err}",
            streams_dir.display()
        ))
    })? {
        let path = entry
            .map_err(|err| {
                BeadError::io(format!(
                    "failed to read bead event stream entry: {err}"
                ))
            })?
            .path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            stream_paths.push(path);
        }
    }
    stream_paths.sort();
    Ok(stream_paths)
}

fn rewrite_manifest_stream_count(
    beads_dir: &Path,
    stream_count: usize,
) -> Result<(), BeadError> {
    let manifest_path = event_manifest_path(beads_dir);
    let manifest = if manifest_path.exists() {
        let text = fs::read_to_string(&manifest_path).map_err(|err| {
            BeadError::io(format!(
                "failed to read bead events manifest {}: {err}",
                manifest_path.display()
            ))
        })?;
        let mut manifest: BeadEventStoreManifestWire =
            serde_json::from_str(&text)?;
        manifest.stream_count = stream_count;
        manifest
    } else {
        BeadEventStoreManifestWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            stream_count,
            generated_from: "issues.jsonl".to_string(),
            migration_tool: "sase-core bead events".to_string(),
        }
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    write_file_atomic(&manifest_path, &manifest_json)
}

pub fn repair_event_store_manifest(
    beads_dir: &Path,
) -> Result<BeadEventManifestRepairOutcomeWire, BeadError> {
    let manifest_path = event_manifest_path(beads_dir);
    let streams_dir = event_streams_dir(beads_dir);
    let manifest_path_text = manifest_path.to_string_lossy().into_owned();
    if !manifest_path.exists() && !streams_dir.exists() {
        return Ok(BeadEventManifestRepairOutcomeWire {
            status: BeadEventManifestRepairStatusWire::Noop,
            manifest_path: manifest_path_text,
            stream_count: 0,
            error: None,
        });
    }

    let streams = match read_event_streams_without_manifest(beads_dir) {
        Ok(streams) => streams,
        Err(error) => {
            return Ok(BeadEventManifestRepairOutcomeWire {
                status: BeadEventManifestRepairStatusWire::InvalidStream,
                manifest_path: manifest_path_text,
                stream_count: 0,
                error: Some(error.to_string()),
            });
        }
    };
    if let Err(error) = reduce_event_streams(&streams) {
        return Ok(BeadEventManifestRepairOutcomeWire {
            status: BeadEventManifestRepairStatusWire::InvalidStream,
            manifest_path: manifest_path_text,
            stream_count: streams.len(),
            error: Some(error.to_string()),
        });
    }

    let canonical = BeadEventStoreManifestWire::from_streams(&streams);
    let needs_repair = if manifest_path.exists() {
        let manifest_text =
            fs::read_to_string(&manifest_path).map_err(|err| {
                BeadError::io(format!(
                    "failed to read bead events manifest {}: {err}",
                    manifest_path.display()
                ))
            })?;
        match serde_json::from_str::<BeadEventStoreManifestWire>(&manifest_text)
        {
            Ok(stored) => {
                if let Err(error) = stored.validate() {
                    return Ok(BeadEventManifestRepairOutcomeWire {
                        status:
                            BeadEventManifestRepairStatusWire::InvalidStream,
                        manifest_path: manifest_path_text,
                        stream_count: streams.len(),
                        error: Some(error.to_string()),
                    });
                }
                stored != canonical
            }
            Err(_) => true,
        }
    } else {
        true
    };

    if !needs_repair {
        return Ok(BeadEventManifestRepairOutcomeWire {
            status: BeadEventManifestRepairStatusWire::Noop,
            manifest_path: manifest_path_text,
            stream_count: streams.len(),
            error: None,
        });
    }

    let manifest_json = serde_json::to_vec_pretty(&canonical)?;
    write_file_atomic(&manifest_path, &manifest_json)?;
    Ok(BeadEventManifestRepairOutcomeWire {
        status: BeadEventManifestRepairStatusWire::Repaired,
        manifest_path: manifest_path_text,
        stream_count: streams.len(),
        error: None,
    })
}

/// Load the manifest and every event stream without touching the store.
///
/// The read path never deletes files or rewrites the manifest. Streams that
/// fail the typed parse are classified: removed-flag tombstones are skipped
/// in memory (the physical prune runs under the mutation lock), live-flag
/// streams stay an error, and anything else surfaces the parse error.
/// The streams directory is listed exactly once per read.
pub fn read_event_store(
    beads_dir: &Path,
) -> Result<(BeadEventStoreManifestWire, Vec<BeadEventStreamWire>), BeadError> {
    let manifest_path = event_manifest_path(beads_dir);
    let manifest_text = fs::read_to_string(&manifest_path).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead events manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: BeadEventStoreManifestWire =
        serde_json::from_str(&manifest_text)?;
    manifest.validate()?;

    let (streams, skipped_removed_flag) =
        read_event_stream_files(&event_streams_dir(beads_dir), true)?;
    if manifest.stream_count != streams.len() + skipped_removed_flag {
        return Err(BeadError::validation(format!(
            "bead event manifest stream_count mismatch: {} != {}",
            manifest.stream_count,
            streams.len() + skipped_removed_flag
        )));
    }
    Ok((manifest, streams))
}

fn read_event_streams_without_manifest(
    beads_dir: &Path,
) -> Result<Vec<BeadEventStreamWire>, BeadError> {
    // Manifest repair stays strict: an unparsable stream fails the repair
    // instead of being silently skipped.
    let (streams, _) =
        read_event_stream_files(&event_streams_dir(beads_dir), false)?;
    Ok(streams)
}

/// Read every stream file under `streams_dir`, which is listed once.
///
/// Returns the parsed streams (sorted by stream id) plus the number of
/// removed-flag tombstones skipped in memory. With `skip_removed_flag`
/// false, any stream that fails the typed parse is an error.
fn read_event_stream_files(
    streams_dir: &Path,
    skip_removed_flag: bool,
) -> Result<(Vec<BeadEventStreamWire>, usize), BeadError> {
    let stream_paths = list_event_stream_paths(streams_dir)?;

    let mut streams = Vec::with_capacity(stream_paths.len());
    let mut stream_ids = BTreeSet::new();
    let mut skipped_removed_flag = 0usize;
    for path in &stream_paths {
        match read_event_stream_file(path) {
            Ok(stream) => {
                if !stream_ids.insert(stream.stream_id.clone()) {
                    return Err(BeadError::validation(format!(
                        "duplicate bead event stream: {}",
                        stream.stream_id
                    )));
                }
                streams.push(stream);
            }
            Err(error) => {
                if !skip_removed_flag {
                    return Err(error);
                }
                match classify_flag_stream(path)? {
                    FlagStreamKind::RemovedFlag => {
                        skipped_removed_flag += 1;
                    }
                    FlagStreamKind::LiveFlag => {
                        let stream_id = path
                            .file_stem()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();
                        return Err(BeadError::validation(format!(
                            "bead event store still has live flag issue-type streams: {stream_id}; migrate or remove them before loading"
                        )));
                    }
                    FlagStreamKind::Other => return Err(error),
                }
            }
        }
    }
    Ok((streams, skipped_removed_flag))
}

pub fn write_event_store(
    beads_dir: &Path,
    streams: &[BeadEventStreamWire],
) -> Result<(), BeadError> {
    write_event_store_inner(beads_dir, streams, None)
}

pub fn write_event_store_changed(
    beads_dir: &Path,
    streams: &[BeadEventStreamWire],
    changed_stream_ids: &BTreeSet<String>,
) -> Result<(), BeadError> {
    write_event_store_inner(beads_dir, streams, Some(changed_stream_ids))
}

fn write_event_store_inner(
    beads_dir: &Path,
    streams: &[BeadEventStreamWire],
    changed_stream_ids: Option<&BTreeSet<String>>,
) -> Result<(), BeadError> {
    let events_dir = beads_dir.join("events");
    let streams_dir = event_streams_dir(beads_dir);

    let mut sorted_streams: Vec<&BeadEventStreamWire> =
        streams.iter().collect();
    sorted_streams.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
    for stream in &sorted_streams {
        stream.validate()?;
    }

    fs::create_dir_all(&streams_dir)?;
    let append_preserving = changed_stream_ids.is_some();
    for stream in &sorted_streams {
        if !selected_for_write(stream, changed_stream_ids) {
            continue;
        }
        let path = streams_dir.join(format!("{}.jsonl", stream.stream_id));
        if append_preserving {
            write_stream_append_preserving(&path, stream)?;
        } else {
            write_stream_fully(&path, stream)?;
        }
    }

    let manifest = BeadEventStoreManifestWire::from_streams(streams);
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    write_file_atomic_if_changed(
        &events_dir.join("manifest.json"),
        &manifest_json,
    )?;
    Ok(())
}

fn write_stream_fully(
    path: &Path,
    stream: &BeadEventStreamWire,
) -> Result<(), BeadError> {
    let mut output = String::new();
    for event in &stream.events {
        output.push_str(&serde_json::to_string(event)?);
        output.push('\n');
    }
    write_file_atomic_if_changed(path, output.as_bytes())?;
    Ok(())
}

/// Write a changed stream without reserializing its already-published
/// prefix. Historical event bytes (including legacy encodings such as a
/// pre-structured-notes `payload.issue.notes` string) are immutable; a
/// mutation must only append new tail events, never normalize old ones.
fn write_stream_append_preserving(
    path: &Path,
    stream: &BeadEventStreamWire,
) -> Result<(), BeadError> {
    let existing_bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return write_stream_fully(path, stream);
        }
        Err(error) => return Err(error.into()),
    };

    let existing_events = parse_event_stream_bytes(path, &existing_bytes)?;

    if existing_events.len() > stream.events.len() {
        return Err(BeadError::validation(format!(
            "bead event stream {} has {} published events but the new stream only has {}; refusing to rewrite published history",
            path.display(),
            existing_events.len(),
            stream.events.len(),
        )));
    }

    let desired_prefix = &stream.events[..existing_events.len()];
    if existing_events.as_slice() != desired_prefix {
        return Err(BeadError::validation(format!(
            "bead event stream {} published events would change; refusing to rewrite history",
            path.display(),
        )));
    }

    let new_tail = &stream.events[existing_events.len()..];
    if new_tail.is_empty() {
        return Ok(());
    }

    let mut output = existing_bytes;
    if !output.ends_with(b"\n") {
        output.push(b'\n');
    }
    for event in new_tail {
        output.extend_from_slice(serde_json::to_string(event)?.as_bytes());
        output.push(b'\n');
    }
    write_file_atomic(path, &output)
}

fn selected_for_write(
    stream: &BeadEventStreamWire,
    changed_stream_ids: Option<&BTreeSet<String>>,
) -> bool {
    match changed_stream_ids {
        Some(ids) => ids.contains(&stream.stream_id),
        None => true,
    }
}

fn issue_import_key(issue: &IssueWire) -> (u8, &str) {
    let kind_order = match issue.issue_type {
        IssueTypeWire::Plan => 0,
        IssueTypeWire::Phase => 1,
        IssueTypeWire::Task => 2,
    };
    (kind_order, issue.id.as_str())
}

fn read_event_stream_file(
    path: &Path,
) -> Result<BeadEventStreamWire, BeadError> {
    let stream_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            BeadError::validation(format!(
                "invalid bead event stream filename: {}",
                path.display()
            ))
        })?
        .to_string();
    let contents = fs::read(path).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead event stream {}: {err}",
            path.display()
        ))
    })?;
    // Events are already validated once by `parse_event_stream_bytes`; only
    // the filename-derived header needs checking here.
    let events = parse_event_stream_bytes(path, &contents)?;
    let stream = BeadEventStreamWire {
        stream_id: stream_id.clone(),
        root_issue_id: stream_id,
        events,
    };
    if stream.stream_id.is_empty() || stream.root_issue_id.is_empty() {
        return Err(BeadError::validation(format!(
            "invalid bead event stream filename: {}",
            path.display()
        )));
    }
    Ok(stream)
}

/// Parse one stream file, validating each event exactly once.
///
/// This is the single event-validation point on the read path: callers must
/// not re-validate events parsed here. Streams from any other source (for
/// example Python-supplied streams through `bead_reduce_event_streams`) are
/// validated by `validated_event_streams` instead.
fn parse_event_stream_bytes(
    path: &Path,
    contents: &[u8],
) -> Result<Vec<BeadEventRecordWire>, BeadError> {
    let contents = std::str::from_utf8(contents).map_err(|err| {
        BeadError::validation(format!(
            "bead event stream {} is not valid UTF-8: {err}",
            path.display()
        ))
    })?;
    let mut events = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: BeadEventRecordWire = serde_json::from_str(line).map_err(
            |err| {
                if let Ok(value) =
                    serde_json::from_str::<serde_json::Value>(line)
                {
                    if let Some(operation) =
                        value.get("operation").and_then(|value| value.as_str())
                    {
                        if serde_json::from_value::<
                            super::events::BeadEventOperationWire,
                        >(serde_json::Value::String(operation.to_string()))
                        .is_err()
                        {
                            return BeadError::validation(format!(
                                "unknown bead event operation `{operation}` in {} line {}; run `just install` to update sase-core",
                                path.display(),
                                index + 1
                            ));
                        }
                    }
                }
                BeadError::validation(format!(
                    "invalid bead event stream {} line {}: {err}",
                    path.display(),
                    index + 1
                ))
            },
        )?;
        event.validate().map_err(|error| {
            BeadError::validation(format!(
                "invalid bead event stream {} line {}: {}",
                path.display(),
                index + 1,
                error.message
            ))
        })?;
        events.push(event);
    }
    Ok(events)
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), BeadError> {
    let parent = path.parent().ok_or_else(|| {
        BeadError::io(format!(
            "cannot determine parent directory for {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)?;
    let tmp_path = atomic_temp_path(path)?;
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn write_file_atomic_if_changed(
    path: &Path,
    bytes: &[u8],
) -> Result<bool, BeadError> {
    match fs::read(path) {
        Ok(existing) if existing == bytes => Ok(false),
        Ok(_) => {
            write_file_atomic(path, bytes)?;
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            write_file_atomic(path, bytes)?;
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

fn atomic_temp_path(path: &Path) -> Result<PathBuf, BeadError> {
    let file_name = path.file_name().ok_or_else(|| {
        BeadError::io(format!(
            "cannot determine file name for {}",
            path.display()
        ))
    })?;
    let counter = ATOMIC_WRITE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut tmp_name = OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(format!(".tmp.{}.{counter}", process::id()));
    Ok(path.with_file_name(tmp_name))
}

pub(crate) fn apply_missing_tiers(issues: &mut [IssueWire]) {
    let phase_parent_ids: BTreeSet<String> = issues
        .iter()
        .filter(|issue| issue.issue_type == IssueTypeWire::Phase)
        .filter_map(|issue| issue.parent_id.clone())
        .collect();
    for issue in issues {
        if issue.issue_type == IssueTypeWire::Plan && issue.tier.is_none() {
            issue.tier = Some(if phase_parent_ids.contains(&issue.id) {
                BeadTierWire::Epic
            } else {
                BeadTierWire::Plan
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::events::{
        BeadEventOperationWire, BeadEventPayloadWire, BEAD_EVENT_SCHEMA_VERSION,
    };
    use crate::bead::wire::{DependencyWire, StatusWire};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn plan(id: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: "Plan".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_by: String::new(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: Vec::new(),
            design: String::new(),
            refs: Vec::new(),
            links: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            model: String::new(),
            size: None,
            task_type: None,
            task_type_fields: BTreeMap::new(),
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            external_ref: String::new(),
            creation_reason: String::new(),
            dependencies: vec![],
        }
    }

    fn event_stream(id: &str, title: &str) -> BeadEventStreamWire {
        let mut issue = plan(id);
        issue.title = title.to_string();
        BeadEventStreamWire {
            stream_id: id.to_string(),
            root_issue_id: id.to_string(),
            events: vec![BeadEventRecordWire {
                schema_version: BEAD_EVENT_SCHEMA_VERSION,
                event_id: format!("{id}:1"),
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                actor: "test".to_string(),
                operation: BeadEventOperationWire::IssueCreated,
                issue_id: id.to_string(),
                payload: BeadEventPayloadWire::IssueCreated { issue },
            }],
        }
    }

    #[test]
    fn prune_removes_tombstoned_flag_streams_and_rewrites_the_manifest() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let streams_dir = event_streams_dir(beads_dir);
        fs::create_dir_all(&streams_dir).unwrap();
        write_event_store(beads_dir, &[event_stream("sase-plan", "Plan")])
            .unwrap();

        let flag_path = streams_dir.join("sase-nw.jsonl");
        fs::write(
            &flag_path,
            concat!(
                r#"{"schema_version":1,"event_id":"sase-nw:1","timestamp":"2026-01-01T00:00:00Z","actor":"test","operation":"issue_created","issue_id":"sase-nw","payload":{"kind":"issue_created","issue":{"id":"sase-nw","title":"Old flag","status":"open","issue_type":"flag","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","flag":{"key":"demo_key","remove_by_date":"2026-12-01","remove_by_release":"0.19.0"}}}}"#,
                "\n",
                r#"{"schema_version":1,"event_id":"sase-nw:2","timestamp":"2026-01-02T00:00:00Z","actor":"test","operation":"issue_removed","issue_id":"sase-nw","payload":{"kind":"issue_removed","cascade_removed_issue_ids":[]}}"#,
                "\n",
            ),
        )
        .unwrap();
        let manifest_path = event_manifest_path(beads_dir);
        let mut manifest: BeadEventStoreManifestWire =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap())
                .unwrap();
        manifest.stream_count = 2;
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let outcome = prune_removed_flag_event_streams(beads_dir).unwrap();
        assert_eq!(outcome.pruned_stream_ids, vec!["sase-nw".to_string()]);
        assert!(outcome.rewritten_manifest);
        assert!(!flag_path.exists());

        let (loaded_manifest, streams) = read_event_store(beads_dir).unwrap();
        assert_eq!(loaded_manifest.stream_count, 1);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream_id, "sase-plan");
    }

    fn write_tombstoned_flag_stream(
        streams_dir: &Path,
        manifest_count: usize,
    ) -> PathBuf {
        let flag_path = streams_dir.join("sase-nw.jsonl");
        let tombstone = concat!(
            r#"{"schema_version":1,"event_id":"sase-nw:1","timestamp":"2026-01-01T00:00:00Z","actor":"test","operation":"issue_created","issue_id":"sase-nw","payload":{"kind":"issue_created","issue":{"id":"sase-nw","title":"Old flag","status":"open","issue_type":"flag","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","flag":{"key":"demo_key","remove_by_date":"2026-12-01","remove_by_release":"0.19.0"}}}}"#,
            "\n",
            r#"{"schema_version":1,"event_id":"sase-nw:2","timestamp":"2026-01-02T00:00:00Z","actor":"test","operation":"issue_removed","issue_id":"sase-nw","payload":{"kind":"issue_removed","cascade_removed_issue_ids":[]}}"#,
            "\n",
        );
        fs::write(&flag_path, tombstone).unwrap();
        let beads_dir = streams_dir.parent().unwrap().parent().unwrap();
        let manifest_path = event_manifest_path(beads_dir);
        let mut manifest: BeadEventStoreManifestWire =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap())
                .unwrap();
        manifest.stream_count = manifest_count;
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        flag_path
    }

    fn snapshot_store_files(beads_dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut snapshot = Vec::new();
        let manifest_path = event_manifest_path(beads_dir);
        snapshot
            .push((manifest_path.clone(), fs::read(&manifest_path).unwrap()));
        for path in
            list_event_stream_paths(&event_streams_dir(beads_dir)).unwrap()
        {
            snapshot.push((path.clone(), fs::read(&path).unwrap()));
        }
        snapshot
    }

    #[test]
    fn read_skips_removed_flag_stream_without_touching_the_store() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let streams_dir = event_streams_dir(beads_dir);
        fs::create_dir_all(&streams_dir).unwrap();
        write_event_store(beads_dir, &[event_stream("sase-plan", "Plan")])
            .unwrap();
        let flag_path = write_tombstoned_flag_stream(&streams_dir, 2);
        let before = snapshot_store_files(beads_dir);

        let (loaded_manifest, streams) = read_event_store(beads_dir).unwrap();
        assert_eq!(loaded_manifest.stream_count, 2);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream_id, "sase-plan");

        // The tombstone is skipped in memory only: the file and the
        // manifest are byte-identical after a pure read.
        assert!(flag_path.exists());
        assert_eq!(snapshot_store_files(beads_dir), before);
    }

    #[test]
    fn prune_rejects_live_flag_streams() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let streams_dir = event_streams_dir(beads_dir);
        fs::create_dir_all(&streams_dir).unwrap();
        fs::write(
            streams_dir.join("sase-nw.jsonl"),
            concat!(
                r#"{"schema_version":1,"event_id":"sase-nw:1","timestamp":"2026-01-01T00:00:00Z","actor":"test","operation":"issue_created","issue_id":"sase-nw","payload":{"kind":"issue_created","issue":{"id":"sase-nw","title":"Live flag","status":"open","issue_type":"flag","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","flag":{"key":"demo_key","remove_by_date":"2026-12-01","remove_by_release":"0.19.0"}}}}"#,
                "\n",
            ),
        )
        .unwrap();

        let error = prune_removed_flag_event_streams(beads_dir).unwrap_err();
        assert!(error.message.contains("sase-nw"));
        assert!(error.message.contains("live flag"));
    }

    #[test]
    fn read_rejects_live_flag_stream_with_todays_message() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let streams_dir = event_streams_dir(beads_dir);
        fs::create_dir_all(&streams_dir).unwrap();
        write_event_store(beads_dir, &[event_stream("sase-plan", "Plan")])
            .unwrap();
        fs::write(
            streams_dir.join("sase-nw.jsonl"),
            concat!(
                r#"{"schema_version":1,"event_id":"sase-nw:1","timestamp":"2026-01-01T00:00:00Z","actor":"test","operation":"issue_created","issue_id":"sase-nw","payload":{"kind":"issue_created","issue":{"id":"sase-nw","title":"Live flag","status":"open","issue_type":"flag","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","flag":{"key":"demo_key","remove_by_date":"2026-12-01","remove_by_release":"0.19.0"}}}}"#,
                "\n",
            ),
        )
        .unwrap();
        let manifest_path = event_manifest_path(beads_dir);
        let mut manifest: BeadEventStoreManifestWire =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap())
                .unwrap();
        manifest.stream_count = 2;
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let error = read_event_store(beads_dir).unwrap_err();
        assert!(error.message.contains("sase-nw"));
        assert!(error.message.contains("live flag"));
    }

    #[test]
    fn atomic_temp_paths_are_unique_per_process() {
        let target = Path::new("/tmp/issues.jsonl");

        let first = atomic_temp_path(target).unwrap();
        let second = atomic_temp_path(target).unwrap();

        assert_ne!(first, second);
        assert_eq!(first.parent(), target.parent());
        assert_eq!(second.parent(), target.parent());
        let expected_prefix = format!(".issues.jsonl.tmp.{}.", process::id());
        assert!(first
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(&expected_prefix));
        assert!(second
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(&expected_prefix));
    }

    #[test]
    fn atomic_if_changed_skips_identical_bytes() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("nested/file.jsonl");

        assert!(write_file_atomic_if_changed(&path, b"one\n").unwrap());
        let modified_before = fs::metadata(&path).unwrap().modified().unwrap();

        assert!(!write_file_atomic_if_changed(&path, b"one\n").unwrap());
        assert_eq!(
            fs::metadata(&path).unwrap().modified().unwrap(),
            modified_before
        );

        assert!(write_file_atomic_if_changed(&path, b"two\n").unwrap());
        assert_eq!(fs::read(&path).unwrap(), b"two\n");
    }

    fn append_note_event(
        stream: &BeadEventStreamWire,
        timestamp: &str,
        entry: &str,
    ) -> BeadEventStreamWire {
        let mut stream = stream.clone();
        let event_number = stream.events.len() + 1;
        stream.events.push(BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: format!("{}:{event_number}", stream.stream_id),
            timestamp: timestamp.to_string(),
            actor: "test".to_string(),
            operation: BeadEventOperationWire::NoteAppended,
            issue_id: stream.stream_id.clone(),
            payload: BeadEventPayloadWire::NoteAppended {
                entry: entry.to_string(),
                attachments: Vec::new(),
            },
        });
        stream
    }

    #[test]
    fn write_event_store_changed_writes_selected_streams_and_reloads() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let alpha = event_stream("alpha", "Alpha");
        let beta = event_stream("beta", "Beta");
        write_event_store(beads_dir, &[alpha.clone(), beta.clone()]).unwrap();

        let streams_dir = beads_dir.join("events/streams");
        let alpha_path = streams_dir.join("alpha.jsonl");
        let beta_path = streams_dir.join("beta.jsonl");
        let gamma_path = streams_dir.join("gamma.jsonl");
        let alpha_before = fs::read(&alpha_path).unwrap();
        let beta_before = fs::read(&beta_path).unwrap();
        let alpha_modified_before =
            fs::metadata(&alpha_path).unwrap().modified().unwrap();

        let alpha_changed = event_stream("alpha", "Alpha changed");
        let beta_changed =
            append_note_event(&beta, "2026-01-02T00:00:00Z", "a note");
        let gamma = event_stream("gamma", "Gamma");
        let changed_stream_ids =
            BTreeSet::from(["beta".to_string(), "gamma".to_string()]);
        write_event_store_changed(
            beads_dir,
            &[alpha_changed, beta_changed, gamma],
            &changed_stream_ids,
        )
        .unwrap();

        assert_eq!(fs::read(&alpha_path).unwrap(), alpha_before);
        assert_eq!(
            fs::metadata(&alpha_path).unwrap().modified().unwrap(),
            alpha_modified_before
        );
        let beta_after = fs::read(&beta_path).unwrap();
        assert_ne!(beta_after, beta_before);
        assert!(
            beta_after.starts_with(&beta_before),
            "the published beta prefix must be byte-identical"
        );
        assert!(gamma_path.exists());

        let (manifest, streams) = read_event_store(beads_dir).unwrap();
        assert_eq!(manifest.stream_count, 3);
        assert_eq!(
            streams
                .iter()
                .map(|stream| stream.stream_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
    }

    #[test]
    fn corrupt_lines_are_skipped() {
        let outcome = parse_issues_jsonl(
            r#"not json
{"id":"ok","title":"OK","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":
"#,
        );
        assert_eq!(outcome.issues.len(), 1);
        assert_eq!(outcome.invalid_json_lines, 2);
        assert_eq!(outcome.invalid_record_lines, 0);
    }

    #[test]
    fn export_sorts_by_id_and_uses_compact_json() {
        let mut child = plan("gold-1.1");
        child.issue_type = IssueTypeWire::Phase;
        child.tier = None;
        child.parent_id = Some("gold-1".to_string());
        child.dependencies = vec![DependencyWire {
            issue_id: "gold-1.1".to_string(),
            depends_on_id: "gold-1".to_string(),
            created_at: "2026-01-01T00:02:00Z".to_string(),
            created_by: String::new(),
        }];
        let output = export_issues_to_jsonl(&[child, plan("gold-1")]).unwrap();
        assert!(output.starts_with(r#"{"id":"gold-1","#));
        assert!(output.contains(r#""dependencies":[{"issue_id":"gold-1.1""#));
        assert!(!output.contains(": "));
    }

    #[test]
    fn import_defaults_missing_plan_tiers_from_phase_children() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"solo","title":"Solo","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic.1","title":"Phase","status":"open","issue_type":"phase","parent_id":"epic","created_at":"","updated_at":"","dependencies":[]}
"#,
        );

        let solo = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "solo")
            .unwrap();
        let epic = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "epic")
            .unwrap();

        assert_eq!(solo.tier, Some(BeadTierWire::Plan));
        assert_eq!(epic.tier, Some(BeadTierWire::Epic));
    }

    #[test]
    fn import_defaults_missing_model_to_empty() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].model, "");
    }

    #[test]
    fn import_preserves_model() {
        let outcome = parse_issues_jsonl(
            r##"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","model":"#pro","dependencies":[]}"##,
        );

        assert_eq!(outcome.issues[0].model, "#pro");
    }

    #[test]
    fn import_rejects_duplicate_external_refs() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("issues.jsonl");
        let mut first = plan("gold-1");
        first.external_ref = "bug:sase#42".to_string();
        let mut second = plan("gold-2");
        second.external_ref = "bug:sase#42".to_string();
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).unwrap(),
                serde_json::to_string(&second).unwrap(),
            ),
        )
        .unwrap();

        let error = import_issues_from_jsonl(&path).unwrap_err();

        assert_eq!(error.kind, "conflict");
        assert!(error.message.contains("external_ref bug:sase#42"));
        assert!(error.message.contains("gold-1"));
        assert!(error.message.contains("gold-2"));
    }

    #[test]
    fn import_rejects_model_control_characters() {
        let outcome = parse_issues_jsonl(
            "{\"id\":\"epic\",\"title\":\"Epic\",\"status\":\"open\",\"issue_type\":\"plan\",\"parent_id\":null,\"created_at\":\"\",\"updated_at\":\"\",\"model\":\"bad\\n%tag:x\",\"dependencies\":[]}",
        );

        assert_eq!(outcome.issues.len(), 0);
        assert_eq!(outcome.invalid_record_lines, 1);
    }

    #[test]
    fn refs_round_trip_and_empty_refs_do_not_change_jsonl_shape() {
        let without_refs = export_issues_to_jsonl(&[plan("epic")]).unwrap();
        assert!(!without_refs.contains("\"refs\""));

        let mut with_refs = plan("epic");
        with_refs.refs = vec![
            "research:202607/report.md".to_string(),
            "bead:sase-bb.1".to_string(),
        ];
        let jsonl = export_issues_to_jsonl(&[with_refs.clone()]).unwrap();
        assert!(jsonl.contains(
            "\"refs\":[\"research:202607/report.md\",\"bead:sase-bb.1\"]"
        ));
        assert_eq!(parse_issues_jsonl(&jsonl).issues, vec![with_refs]);
    }

    #[test]
    fn unknown_event_operation_names_the_upgrade_remedy() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("gold-1.jsonl");
        fs::write(
            &path,
            r#"{"schema_version":1,"event_id":"future","timestamp":"2026-01-01T00:00:00Z","actor":"agent","operation":"future_operation","issue_id":"gold-1","payload":{"kind":"future_operation"}}"#,
        )
        .unwrap();

        let error = read_event_stream_file(&path).unwrap_err();

        assert!(error.message.contains("future_operation"));
        assert!(error.message.contains(path.to_string_lossy().as_ref()));
        assert!(error.message.contains("just install"));
    }

    #[test]
    fn write_event_store_changed_rejects_a_shortened_ancestor_stream() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let two_events = append_note_event(
            &event_stream("sase-lg", "Legacy"),
            "2026-01-02T00:00:00Z",
            "first note",
        );
        write_event_store(beads_dir, &[two_events]).unwrap();

        let stream_path = event_streams_dir(beads_dir).join("sase-lg.jsonl");
        let before = fs::read(&stream_path).unwrap();

        let shortened = event_stream("sase-lg", "Legacy");
        let changed_stream_ids = BTreeSet::from(["sase-lg".to_string()]);
        let error = write_event_store_changed(
            beads_dir,
            &[shortened],
            &changed_stream_ids,
        )
        .unwrap_err();

        assert!(error.message.contains("published events"));
        assert_eq!(fs::read(&stream_path).unwrap(), before);
    }

    #[test]
    fn write_event_store_changed_rejects_a_changed_ancestor_event() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let original = event_stream("sase-lg", "Legacy");
        write_event_store(beads_dir, &[original]).unwrap();

        let stream_path = event_streams_dir(beads_dir).join("sase-lg.jsonl");
        let before = fs::read(&stream_path).unwrap();

        let rewritten = event_stream("sase-lg", "Legacy renamed");
        let changed_stream_ids = BTreeSet::from(["sase-lg".to_string()]);
        let error = write_event_store_changed(
            beads_dir,
            &[rewritten],
            &changed_stream_ids,
        )
        .unwrap_err();

        assert!(error.message.contains("would change"));
        assert_eq!(fs::read(&stream_path).unwrap(), before);
    }

    #[test]
    fn write_event_store_changed_appends_to_legacy_stream_without_rewriting_prefix_bytes(
    ) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path();
        let streams_dir = event_streams_dir(beads_dir);
        fs::create_dir_all(&streams_dir).unwrap();

        let stream_path = streams_dir.join("sase-lg.jsonl");
        // A pre-structured-notes event: `payload.issue.notes` is a literal
        // (empty) string rather than the current `Vec<BeadNoteWire>` shape.
        // Deliberately has no trailing newline to exercise that edge case.
        let legacy_line = r#"{"schema_version":1,"event_id":"sase-lg:1","timestamp":"2026-01-01T00:00:00Z","actor":"test","operation":"issue_created","issue_id":"sase-lg","payload":{"kind":"issue_created","issue":{"id":"sase-lg","title":"Legacy","status":"open","issue_type":"plan","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","notes":""}}}"#;
        fs::write(&stream_path, legacy_line).unwrap();

        let existing_events =
            parse_event_stream_bytes(&stream_path, legacy_line.as_bytes())
                .unwrap();
        assert_eq!(existing_events.len(), 1);

        let mut stream = BeadEventStreamWire {
            stream_id: "sase-lg".to_string(),
            root_issue_id: "sase-lg".to_string(),
            events: existing_events,
        };
        stream.events.push(BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: "sase-lg:2".to_string(),
            timestamp: "2026-01-02T00:00:00Z".to_string(),
            actor: "test".to_string(),
            operation: BeadEventOperationWire::NoteAppended,
            issue_id: "sase-lg".to_string(),
            payload: BeadEventPayloadWire::NoteAppended {
                entry: "a new note".to_string(),
                attachments: Vec::new(),
            },
        });

        let changed_stream_ids = BTreeSet::from(["sase-lg".to_string()]);
        write_event_store_changed(beads_dir, &[stream], &changed_stream_ids)
            .unwrap();

        let after = fs::read_to_string(&stream_path).unwrap();
        assert!(
            after.starts_with(legacy_line),
            "legacy prefix bytes must be preserved exactly byte-for-byte"
        );
        assert_eq!(after.lines().count(), 2);

        let (_manifest, streams) = read_event_store(beads_dir).unwrap();
        let issues = reduce_event_streams(&streams).unwrap();
        let issue = issues.iter().find(|issue| issue.id == "sase-lg").unwrap();
        assert_eq!(issue.notes.len(), 1);
        assert_eq!(issue.notes[0].text, "a new note");
    }
}
