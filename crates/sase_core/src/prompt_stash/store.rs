use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

use super::wire::{
    PromptStashEntryWire, PromptStashLifecycleOutcomeWire,
    PromptStashLifecycleSnapshotWire, PromptStashPopOutcomeWire,
    PromptStashSnapshotWire, PromptStashStoreStatsWire,
    PromptStashTrashRecordWire, PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION,
    PROMPT_STASH_WIRE_SCHEMA_VERSION,
};

const LOCK_TIMEOUT_ENV: &str = "SASE_PROMPT_STASH_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(120);

/// Tag written on every trash envelope line of `prompt_stash.jsonl`.
const TRASH_LINE_KIND: &str = "trash";

/// Suffix for the recoverable copy taken before the first tagged write.
const PRE_UPGRADE_BACKUP_SUFFIX: &str = ".pre-upgrade-backup";

#[derive(Debug, thiserror::Error)]
pub enum PromptStashStoreError {
    #[error(
        "prompt stash lock timed out after {waited_ms}ms waiting for {mode} lock: {}; holder: {holder}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
        holder: String,
    },
    #[error("{0}")]
    Store(String),
}

impl From<String> for PromptStashStoreError {
    fn from(message: String) -> Self {
        Self::Store(message)
    }
}

type PromptStashResult<T> = Result<T, PromptStashStoreError>;

/// One parsed trash row plus its file-order sequence.
///
/// `seq` counts parsed data rows (active plus trash) in file order and feeds
/// the stable eviction tie break: rows trashed by the same batch share one
/// `trashed_at`, so `(trashed_at, seq, id)` decides oldest-first eviction and
/// newest-first display deterministically.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TrashRow {
    trashed_at: String,
    entry: PromptStashEntryWire,
    seq: usize,
}

/// Full parsed state of the stash file.
///
/// `raw` holds verbatim lines that are neither active entries nor trash
/// envelopes (malformed JSON, invalid records, future `kind` values). Writers
/// replay those bytes unchanged so no mutation ever silently drops rows it
/// does not understand.
#[derive(Debug, Clone, Default)]
struct ParsedStash {
    active: Vec<PromptStashEntryWire>,
    trash: Vec<TrashRow>,
    raw: Vec<String>,
    stats: PromptStashStoreStatsWire,
}

/// Disk shape of one trash line.
///
/// A bare top-level `trashed_at` field is forbidden on purpose: old serde
/// readers ignore unknown fields and would mistake such a line for an active
/// row. The tagged envelope instead fails old entry parsing, so legacy
/// readers count it as an invalid record rather than resurrecting it.
#[derive(Debug, Serialize)]
struct TrashEnvelope<'a> {
    kind: &'static str,
    trashed_at: &'a str,
    entry: &'a PromptStashEntryWire,
}

pub fn read_prompt_stash_snapshot(
    path: &Path,
) -> PromptStashResult<PromptStashSnapshotWire> {
    if !path.exists() {
        return Ok(snapshot_from_rows(
            Vec::new(),
            PromptStashStoreStatsWire::default(),
        ));
    }
    let lock = lock_with_timeout(
        path,
        LockMode::Shared,
        prompt_stash_lock_timeout(),
        "read_prompt_stash_snapshot",
    )?;
    let result = read_parsed_unlocked(path);
    unlock(lock)?;
    let parsed = result?;
    Ok(snapshot_from_rows(parsed.active, parsed.stats))
}

/// Read both stash collections without enforcing any trash limit.
pub fn read_prompt_stash_lifecycle(
    path: &Path,
) -> PromptStashResult<PromptStashLifecycleSnapshotWire> {
    if !path.exists() {
        return Ok(lifecycle_snapshot_from_parsed(ParsedStash::default()));
    }
    let lock = lock_with_timeout(
        path,
        LockMode::Shared,
        prompt_stash_lock_timeout(),
        "read_prompt_stash_lifecycle",
    )?;
    let result = read_parsed_unlocked(path);
    unlock(lock)?;
    Ok(lifecycle_snapshot_from_parsed(result?))
}

pub fn append_prompt_stash(
    path: &Path,
    entry: &PromptStashEntryWire,
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "append_prompt_stash",
    )?;
    let result: Result<PromptStashSnapshotWire, String> = (|| {
        reject_resurrect_append(path, entry)?;
        append_prompt_stash_unlocked(path, entry)?;
        let parsed = read_parsed_unlocked(path)?;
        Ok(snapshot_from_rows(parsed.active, parsed.stats))
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Move active rows to Trash under one deletion timestamp.
///
/// `trashed_at` is caller-supplied UTC (RFC 3339) and is shared by the whole
/// batch. Unknown ids and ids already in Trash are no-ops. The trash limit
/// is enforced in the same transaction: rows past the limit are permanently
/// deleted and reported in `evicted`, oldest first. Limit zero disables
/// recovery, so freshly trashed rows land directly in `evicted`.
pub fn trash_prompt_stash(
    path: &Path,
    ids: &[String],
    trash_limit: u64,
    trashed_at: &str,
) -> PromptStashResult<PromptStashLifecycleOutcomeWire> {
    if trashed_at.is_empty() {
        return Err(PromptStashStoreError::Store(
            "trashed_at must not be empty; pass the UTC deletion time for this trash batch".to_string(),
        ));
    }
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "trash_prompt_stash",
    )?;
    let result: Result<PromptStashLifecycleOutcomeWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let had_trash = !parsed.trash.is_empty();
        let wanted = ordered_unique(ids);
        let trashed_ids: BTreeSet<String> = parsed
            .trash
            .iter()
            .map(|row| row.entry.id.clone())
            .collect();
        // New rows sort after every existing row on timestamp ties.
        let mut next_seq = parsed.active.len() + parsed.trash.len();
        let mut moved: Vec<String> = Vec::new();
        let mut kept_active = Vec::with_capacity(parsed.active.len());
        for row in parsed.active {
            if wanted.contains(&row.id.as_str())
                && !trashed_ids.contains(&row.id)
            {
                moved.push(row.id.clone());
                parsed.trash.push(TrashRow {
                    trashed_at: trashed_at.to_string(),
                    entry: row,
                    seq: next_seq,
                });
                next_seq += 1;
            } else {
                kept_active.push(row);
            }
        }
        parsed.active = kept_active;
        let evicted = enforce_trash_limit(&mut parsed.trash, trash_limit);
        let moved_set: BTreeSet<&str> =
            moved.iter().map(String::as_str).collect();
        let evicted_set: BTreeSet<&str> =
            evicted.iter().map(String::as_str).collect();
        let changed: Vec<String> = wanted
            .iter()
            .filter(|id| moved_set.contains(*id) && !evicted_set.contains(*id))
            .map(|id| (*id).to_string())
            .collect();
        if moved.is_empty() && evicted.is_empty() {
            return Ok(outcome_from_parsed(changed, evicted, parsed));
        }
        if !parsed.trash.is_empty() && !had_trash {
            ensure_pre_upgrade_backup(path)?;
        }
        write_parsed_atomic(path, &parsed)?;
        let parsed = read_parsed_unlocked(path)?;
        Ok(outcome_from_parsed(changed, evicted, parsed))
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Move trashed rows back to Stash.
///
/// Restored rows keep their complete entry and are appended to the active
/// rows in caller input order. Unknown ids and ids already active are
/// no-ops. Restoring never evicts: the active collection is unbounded.
pub fn restore_prompt_stash(
    path: &Path,
    ids: &[String],
) -> PromptStashResult<PromptStashLifecycleOutcomeWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "restore_prompt_stash",
    )?;
    let result: Result<PromptStashLifecycleOutcomeWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let wanted = ordered_unique(ids);
        let active_ids: BTreeSet<&str> =
            parsed.active.iter().map(|row| row.id.as_str()).collect();
        let wanted_set: BTreeSet<&str> = wanted.iter().copied().collect();
        // Partition trash rows in file order; restored entries are appended
        // to the active rows below in caller input order for determinism.
        let mut by_id: HashMap<String, PromptStashEntryWire> = HashMap::new();
        let mut remaining: Vec<TrashRow> =
            Vec::with_capacity(parsed.trash.len());
        for row in parsed.trash {
            if wanted_set.contains(row.entry.id.as_str())
                && !active_ids.contains(row.entry.id.as_str())
                && !by_id.contains_key(&row.entry.id)
            {
                by_id.insert(row.entry.id.clone(), row.entry);
            } else {
                remaining.push(row);
            }
        }
        parsed.trash = remaining;
        let mut changed = Vec::new();
        for id in &wanted {
            if let Some(entry) = by_id.remove(*id) {
                parsed.active.push(entry);
                changed.push((*id).to_string());
            }
        }
        if changed.is_empty() {
            return Ok(outcome_from_parsed(changed, Vec::new(), parsed));
        }
        write_parsed_atomic(path, &parsed)?;
        let parsed = read_parsed_unlocked(path)?;
        Ok(outcome_from_parsed(changed, Vec::new(), parsed))
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Permanently delete trashed rows.
///
/// Only Trash membership is affected: active ids and unknown ids are no-ops,
/// so purge can never delete a live draft.
pub fn purge_prompt_stash(
    path: &Path,
    ids: &[String],
) -> PromptStashResult<PromptStashLifecycleOutcomeWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "purge_prompt_stash",
    )?;
    let result: Result<PromptStashLifecycleOutcomeWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let wanted = ordered_unique(ids);
        let wanted_set: BTreeSet<&str> = wanted.iter().copied().collect();
        let mut purged_set: BTreeSet<String> = BTreeSet::new();
        let mut kept = Vec::with_capacity(parsed.trash.len());
        for row in parsed.trash {
            if wanted_set.contains(row.entry.id.as_str()) {
                purged_set.insert(row.entry.id);
            } else {
                kept.push(row);
            }
        }
        parsed.trash = kept;
        let changed: Vec<String> = wanted
            .iter()
            .filter(|id| purged_set.contains(**id))
            .map(|id| (*id).to_string())
            .collect();
        if changed.is_empty() {
            return Ok(outcome_from_parsed(changed, Vec::new(), parsed));
        }
        write_parsed_atomic(path, &parsed)?;
        let parsed = read_parsed_unlocked(path)?;
        Ok(outcome_from_parsed(changed, Vec::new(), parsed))
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Enforce the trash limit on the current Trash contents.
///
/// This is the reconciliation path for a lowered configured limit: the first
/// trash-aware open or write after reload calls here, persists the trim, and
/// surfaces the permanently deleted ids in `evicted`, oldest first.
pub fn reconcile_prompt_stash_trash(
    path: &Path,
    trash_limit: u64,
) -> PromptStashResult<PromptStashLifecycleOutcomeWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "reconcile_prompt_stash_trash",
    )?;
    let result: Result<PromptStashLifecycleOutcomeWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let evicted = enforce_trash_limit(&mut parsed.trash, trash_limit);
        if evicted.is_empty() {
            return Ok(outcome_from_parsed(Vec::new(), evicted, parsed));
        }
        write_parsed_atomic(path, &parsed)?;
        let parsed = read_parsed_unlocked(path)?;
        Ok(outcome_from_parsed(Vec::new(), evicted, parsed))
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Remove the entries whose ids appear in `ids`, returning what was removed
/// alongside a snapshot of the remaining store.
///
/// Removal is restricted to active rows: Trash rows are preserved verbatim,
/// and ids that name only Trash rows (or nothing at all) are ignored.
/// `removed` preserves on-disk order.
pub fn pop_prompt_stash(
    path: &Path,
    ids: &[String],
) -> PromptStashResult<PromptStashPopOutcomeWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "pop_prompt_stash",
    )?;
    let result: Result<PromptStashPopOutcomeWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let wanted: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let mut removed = Vec::new();
        let mut kept = Vec::new();
        for row in parsed.active {
            if wanted.contains(&row.id.as_str()) {
                removed.push(row);
            } else {
                kept.push(row);
            }
        }
        parsed.active = kept;
        if !removed.is_empty() {
            write_parsed_atomic(path, &parsed)?;
        }
        let parsed = read_parsed_unlocked(path)?;
        Ok(PromptStashPopOutcomeWire {
            schema_version: PROMPT_STASH_WIRE_SCHEMA_VERSION,
            removed,
            snapshot: snapshot_from_rows(parsed.active, parsed.stats),
        })
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Set the persisted pin flag for entries whose ids appear in `ids`,
/// returning a fresh snapshot of the store.
///
/// Only active rows are affected; Trash rows (which have no pin edit) and
/// unknown ids are ignored, and Trash is preserved verbatim.
pub fn set_prompt_stash_pinned(
    path: &Path,
    ids: &[String],
    pinned: bool,
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "set_prompt_stash_pinned",
    )?;
    let result: Result<PromptStashSnapshotWire, String> = (|| {
        let mut parsed = read_parsed_unlocked(path)?;
        let wanted: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let mut changed = false;
        for row in &mut parsed.active {
            if wanted.contains(&row.id.as_str()) && row.pinned != pinned {
                row.pinned = pinned;
                changed = true;
            }
        }
        if changed {
            write_parsed_atomic(path, &parsed)?;
        }
        let parsed = read_parsed_unlocked(path)?;
        Ok(snapshot_from_rows(parsed.active, parsed.stats))
    })();
    unlock(lock)?;
    Ok(result?)
}

pub fn rewrite_prompt_stash(
    path: &Path,
    entries: &[PromptStashEntryWire],
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "rewrite_prompt_stash",
    )?;
    let result =
        merge_and_rewrite_entries_unlocked(path, entries).and_then(|()| {
            let parsed = read_parsed_unlocked(path)?;
            Ok(snapshot_from_rows(parsed.active, parsed.stats))
        });
    unlock(lock)?;
    Ok(result?)
}

/// Caller input order without duplicates.
fn ordered_unique(ids: &[String]) -> Vec<&str> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();
    for id in ids {
        if seen.insert(id.as_str()) {
            ordered.push(id.as_str());
        }
    }
    ordered
}

/// Sort trash oldest-first and drop rows past `trash_limit`.
///
/// Ordering is `(trashed_at, seq, id)`: deletion time first, then file/batch
/// order, then id as the final stable tie break. Returns the evicted ids,
/// oldest first.
fn enforce_trash_limit(trash: &mut Vec<TrashRow>, limit: u64) -> Vec<String> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    if trash.len() <= limit {
        return Vec::new();
    }
    trash.sort_by(|a, b| {
        a.trashed_at
            .cmp(&b.trashed_at)
            .then(a.seq.cmp(&b.seq))
            .then(a.entry.id.cmp(&b.entry.id))
    });
    let evict_count = trash.len() - limit;
    let evicted: Vec<String> = trash
        .iter()
        .take(evict_count)
        .map(|row| row.entry.id.clone())
        .collect();
    trash.drain(..evict_count);
    evicted
}

fn append_prompt_stash_unlocked(
    path: &Path,
    entry: &PromptStashEntryWire,
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    serde_json::to_writer(&mut file, entry)
        .map_err(|e| format!("failed to serialize prompt stash entry: {e}"))?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Reject an append that would resurrect an id already in Trash.
fn reject_resurrect_append(
    path: &Path,
    entry: &PromptStashEntryWire,
) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let parsed = read_parsed_unlocked(path)?;
    if parsed.trash.iter().any(|row| row.entry.id == entry.id) {
        return Err(format!(
            "stale append would resurrect trashed id {:?}; restore it from Trash instead",
            entry.id
        ));
    }
    Ok(())
}

fn read_parsed_unlocked(path: &Path) -> Result<ParsedStash, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ParsedStash::default());
        }
        Err(e) => return Err(e.to_string()),
    };

    let reader = BufReader::new(file);
    let mut parsed = ParsedStash::default();
    let mut seq = 0;
    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        parsed.stats.total_lines += 1;
        if line.trim().is_empty() {
            parsed.stats.blank_lines += 1;
            continue;
        }
        match parse_data_line(&line) {
            ParsedLine::Active(entry) => {
                parsed.stats.loaded_rows += 1;
                parsed.active.push(entry);
                seq += 1;
            }
            ParsedLine::Trash { trashed_at, entry } => {
                parsed.stats.loaded_rows += 1;
                parsed.trash.push(TrashRow {
                    trashed_at,
                    entry,
                    seq,
                });
                seq += 1;
            }
            ParsedLine::Opaque => {
                parsed.stats.invalid_record_lines += 1;
                parsed.raw.push(line);
            }
            ParsedLine::InvalidJson => {
                parsed.stats.invalid_json_lines += 1;
                parsed.raw.push(line);
            }
        }
    }
    Ok(parsed)
}

enum ParsedLine {
    Active(PromptStashEntryWire),
    Trash {
        trashed_at: String,
        entry: PromptStashEntryWire,
    },
    /// A line we preserve verbatim but never interpret: an invalid record, a
    /// future `kind`, or a bare `trashed_at` that old readers would misread.
    Opaque,
    InvalidJson,
}

fn parse_data_line(line: &str) -> ParsedLine {
    let value: serde_json::Value = match serde_json::from_str(line.trim()) {
        Ok(value) => value,
        Err(_) => return ParsedLine::InvalidJson,
    };
    let object = match value.as_object() {
        Some(object) => object,
        None => return ParsedLine::Opaque,
    };
    match object.get("kind") {
        Some(kind) if kind == TRASH_LINE_KIND => parse_trash_envelope(object),
        Some(_) => ParsedLine::Opaque,
        None => {
            if object.contains_key("trashed_at") {
                // A bare deletion timestamp without the trash tag: old
                // readers would treat this line as active, so the new reader
                // refuses to interpret it and preserves it verbatim.
                return ParsedLine::Opaque;
            }
            match serde_json::from_value::<PromptStashEntryWire>(value) {
                Ok(entry)
                    if !entry.id.is_empty() && !entry.created_at.is_empty() =>
                {
                    ParsedLine::Active(entry)
                }
                _ => ParsedLine::Opaque,
            }
        }
    }
}

fn parse_trash_envelope(
    object: &serde_json::Map<String, serde_json::Value>,
) -> ParsedLine {
    let trashed_at = match object.get("trashed_at") {
        Some(value) => match value.as_str() {
            Some(text) if !text.is_empty() => text.to_string(),
            _ => return ParsedLine::Opaque,
        },
        None => return ParsedLine::Opaque,
    };
    let entry_value = match object.get("entry") {
        Some(value) => value.clone(),
        None => return ParsedLine::Opaque,
    };
    match serde_json::from_value::<PromptStashEntryWire>(entry_value) {
        Ok(entry) if !entry.id.is_empty() && !entry.created_at.is_empty() => {
            ParsedLine::Trash { trashed_at, entry }
        }
        _ => ParsedLine::Opaque,
    }
}

// Rewrite is a _merge_: caller's rows win on id collision; rows present on
// disk but absent from the input are preserved (they may be concurrent appends
// from another instance). Callers cannot delete rows by passing a shorter list
// — use `pop_prompt_stash` for removal. Trash rows are always preserved, and
// an input id that names a trashed row is rejected as a stale rewrite rather
// than resurrecting the row.
fn merge_and_rewrite_entries_unlocked(
    path: &Path,
    input: &[PromptStashEntryWire],
) -> Result<(), String> {
    let parsed = read_parsed_unlocked(path)?;
    let trashed_ids: BTreeSet<&str> = parsed
        .trash
        .iter()
        .map(|row| row.entry.id.as_str())
        .collect();
    let mut stale = Vec::new();
    for entry in input {
        if trashed_ids.contains(entry.id.as_str()) {
            stale.push(entry.id.clone());
        }
    }
    if !stale.is_empty() {
        return Err(format!(
            "stale rewrite would resurrect trashed id(s): {}; restore them from Trash instead",
            stale.join(", ")
        ));
    }
    let input_ids: BTreeSet<&str> =
        input.iter().map(|n| n.id.as_str()).collect();
    let mut merged: Vec<PromptStashEntryWire> = input.to_vec();
    for row in parsed.active {
        if !input_ids.contains(row.id.as_str()) {
            merged.push(row);
        }
    }
    let merged_parsed = ParsedStash {
        active: merged,
        trash: parsed.trash,
        raw: parsed.raw,
        stats: PromptStashStoreStatsWire::default(),
    };
    write_parsed_atomic(path, &merged_parsed)
}

/// Write the full stash state atomically: active rows, then trash envelopes,
/// then verbatim-preserved opaque lines.
///
/// One temporary file, one fsync, one atomic rename, plus a directory sync —
/// readers under the lock never observe a partial write.
fn write_parsed_atomic(
    path: &Path,
    parsed: &ParsedStash,
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let tmp_path = temp_path_for(path);
    let write_result = (|| {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        for entry in &parsed.active {
            serde_json::to_writer(&mut writer, entry).map_err(|e| {
                format!("failed to serialize prompt stash entry: {e}")
            })?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        for row in &parsed.trash {
            let envelope = TrashEnvelope {
                kind: TRASH_LINE_KIND,
                trashed_at: &row.trashed_at,
                entry: &row.entry,
            };
            serde_json::to_writer(&mut writer, &envelope).map_err(|e| {
                format!("failed to serialize prompt stash trash row: {e}")
            })?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        for line in &parsed.raw {
            writer
                .write_all(line.as_bytes())
                .map_err(|e| e.to_string())?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
        writer.get_ref().sync_all().map_err(|e| e.to_string())?;
        fs::rename(&tmp_path, path).map_err(|e| e.to_string())?;
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn backup_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("prompt_stash.jsonl");
    path.with_file_name(format!("{filename}{PRE_UPGRADE_BACKUP_SUFFIX}"))
}

/// Copy the live file to its pre-upgrade backup and verify the bytes match.
///
/// Runs under the stash exclusive lock before the first tagged write. An
/// existing backup is kept as-is so the original pre-upgrade drafts stay
/// recoverable; a backup path that is not a regular file, a failed copy, or
/// a verification mismatch fails closed and leaves the live file unchanged.
fn ensure_pre_upgrade_backup(path: &Path) -> Result<(), String> {
    let backup = backup_path_for(path);
    if backup.exists() {
        if !backup.is_file() {
            return Err(format!(
                "prompt stash pre-upgrade backup path exists but is not a file: {}; refusing to write trash rows",
                backup.display()
            ));
        }
        return Ok(());
    }
    fs::copy(path, &backup).map_err(|e| {
        format!(
            "failed to create prompt stash pre-upgrade backup {}: {e}; refusing to write trash rows",
            backup.display()
        )
    })?;
    let live_bytes = fs::read(path).map_err(|e| {
        format!(
            "failed to verify prompt stash pre-upgrade backup {}: {e}",
            backup.display()
        )
    })?;
    let backup_bytes = fs::read(&backup).map_err(|e| {
        format!(
            "failed to verify prompt stash pre-upgrade backup {}: {e}",
            backup.display()
        )
    })?;
    if live_bytes != backup_bytes {
        let _ = fs::remove_file(&backup);
        return Err(format!(
            "prompt stash pre-upgrade backup {} failed verification; refusing to write trash rows",
            backup.display()
        ));
    }
    Ok(())
}

fn lifecycle_snapshot_from_parsed(
    parsed: ParsedStash,
) -> PromptStashLifecycleSnapshotWire {
    let mut trash = parsed.trash;
    // Newest-deleted-first for display; `seq` is unique so the order is
    // total without needing the id fallback, which is kept for explicitness.
    trash.sort_by(|a, b| {
        b.trashed_at
            .cmp(&a.trashed_at)
            .then(b.seq.cmp(&a.seq))
            .then(a.entry.id.cmp(&b.entry.id))
    });
    PromptStashLifecycleSnapshotWire {
        schema_version: PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION,
        active: parsed.active,
        trash: trash
            .into_iter()
            .map(|row| PromptStashTrashRecordWire {
                trashed_at: row.trashed_at,
                entry: row.entry,
            })
            .collect(),
        stats: parsed.stats,
    }
}

fn outcome_from_parsed(
    changed: Vec<String>,
    evicted: Vec<String>,
    parsed: ParsedStash,
) -> PromptStashLifecycleOutcomeWire {
    PromptStashLifecycleOutcomeWire {
        schema_version: PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION,
        changed,
        evicted,
        snapshot: lifecycle_snapshot_from_parsed(parsed),
    }
}

fn snapshot_from_rows(
    entries: Vec<PromptStashEntryWire>,
    stats: PromptStashStoreStatsWire,
) -> PromptStashSnapshotWire {
    PromptStashSnapshotWire {
        schema_version: PROMPT_STASH_WIRE_SCHEMA_VERSION,
        entries,
        stats,
    }
}

fn prompt_stash_lock_timeout() -> Duration {
    timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT)
}

fn lock_with_timeout(
    path: &Path,
    mode: LockMode,
    timeout: Duration,
    operation: &str,
) -> PromptStashResult<HeldStoreLock> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let lock_path = lock_path_for(path);
    acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        mode,
        timeout,
        operation,
    )
    .map_err(|error| match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => PromptStashStoreError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        error => PromptStashStoreError::Store(error.to_string()),
    })
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("prompt_stash.jsonl");
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("prompt_stash.jsonl");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    path.parent().ok_or_else(|| {
        format!("prompt stash path has no parent: {}", path.display())
    })
}

fn unlock(lock: HeldStoreLock) -> Result<(), String> {
    lock.release().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tempfile::tempdir;

    #[test]
    fn held_exclusive_lock_bounds_reader_and_writer_waits() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("prompt_stash.jsonl");
        let holder = lock_with_timeout(
            &path,
            LockMode::Exclusive,
            Duration::from_secs(1),
            "test_holder",
        )
        .unwrap();

        for mode in [LockMode::Shared, LockMode::Exclusive] {
            let started = Instant::now();
            let error = lock_with_timeout(
                &path,
                mode,
                Duration::from_millis(50),
                "test_contender",
            )
            .unwrap_err();
            assert!(matches!(
                &error,
                PromptStashStoreError::LockTimeout { .. }
            ));
            assert!(error.to_string().contains("operation=test_holder"));
            assert!(started.elapsed() < Duration::from_secs(1));
        }

        holder.release().unwrap();
    }
}
