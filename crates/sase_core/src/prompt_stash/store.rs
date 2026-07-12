use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;

use super::wire::{
    PromptStashEntryWire, PromptStashPopOutcomeWire, PromptStashSnapshotWire,
    PromptStashStoreStatsWire, PROMPT_STASH_WIRE_SCHEMA_VERSION,
};

const LOCK_TIMEOUT: Duration = Duration::from_secs(2);
const LOCK_RETRY_MIN_MS: u64 = 10;
const LOCK_RETRY_JITTER_MS: u64 = 20;

#[derive(Debug, thiserror::Error)]
pub enum PromptStashStoreError {
    #[error(
        "prompt stash lock timed out after {waited_ms}ms waiting for {mode} lock: {}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
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

#[derive(Clone, Copy)]
enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
        }
    }
}

#[allow(clippy::incompatible_msrv)]
pub fn read_prompt_stash_snapshot(
    path: &Path,
) -> PromptStashResult<PromptStashSnapshotWire> {
    if !path.exists() {
        return Ok(snapshot_from_rows(
            Vec::new(),
            PromptStashStoreStatsWire::default(),
        ));
    }
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Shared, LOCK_TIMEOUT)?;
    let result = read_rows_unlocked(path);
    unlock(lock)?;
    let (entries, stats) = result?;
    Ok(snapshot_from_rows(entries, stats))
}

pub fn append_prompt_stash(
    path: &Path,
    entry: &PromptStashEntryWire,
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result = append_prompt_stash_unlocked(path, entry).and_then(|()| {
        let (entries, stats) = read_rows_unlocked(path)?;
        Ok(snapshot_from_rows(entries, stats))
    });
    unlock(lock)?;
    Ok(result?)
}

/// Remove the entries whose ids appear in `ids`, returning what was removed
/// alongside a snapshot of the remaining store. Ids that are not present are
/// ignored. `removed` preserves on-disk order.
pub fn pop_prompt_stash(
    path: &Path,
    ids: &[String],
) -> PromptStashResult<PromptStashPopOutcomeWire> {
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result: Result<PromptStashPopOutcomeWire, String> = (|| {
        let (rows, _) = read_rows_unlocked(path)?;
        let wanted: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let mut removed = Vec::new();
        let mut kept = Vec::new();
        for row in rows {
            if wanted.contains(row.id.as_str()) {
                removed.push(row);
            } else {
                kept.push(row);
            }
        }
        if !removed.is_empty() {
            write_entries_atomic(path, &kept)?;
        }
        let (entries, stats) = read_rows_unlocked(path)?;
        Ok(PromptStashPopOutcomeWire {
            schema_version: PROMPT_STASH_WIRE_SCHEMA_VERSION,
            removed,
            snapshot: snapshot_from_rows(entries, stats),
        })
    })();
    unlock(lock)?;
    Ok(result?)
}

/// Set the persisted pin flag for entries whose ids appear in `ids`, returning
/// a fresh snapshot of the store. Unknown ids are ignored.
pub fn set_prompt_stash_pinned(
    path: &Path,
    ids: &[String],
    pinned: bool,
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result: Result<PromptStashSnapshotWire, String> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        let wanted: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let mut changed = false;
        for row in &mut rows {
            if wanted.contains(row.id.as_str()) && row.pinned != pinned {
                row.pinned = pinned;
                changed = true;
            }
        }
        if changed {
            write_entries_atomic(path, &rows)?;
        }
        let (entries, stats) = read_rows_unlocked(path)?;
        Ok(snapshot_from_rows(entries, stats))
    })();
    unlock(lock)?;
    Ok(result?)
}

pub fn rewrite_prompt_stash(
    path: &Path,
    entries: &[PromptStashEntryWire],
) -> PromptStashResult<PromptStashSnapshotWire> {
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result =
        merge_and_rewrite_entries_unlocked(path, entries).and_then(|()| {
            let (rows, stats) = read_rows_unlocked(path)?;
            Ok(snapshot_from_rows(rows, stats))
        });
    unlock(lock)?;
    Ok(result?)
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

fn read_rows_unlocked(
    path: &Path,
) -> Result<(Vec<PromptStashEntryWire>, PromptStashStoreStatsWire), String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), PromptStashStoreStatsWire::default()));
        }
        Err(e) => return Err(e.to_string()),
    };

    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut stats = PromptStashStoreStatsWire::default();
    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        stats.total_lines += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            stats.blank_lines += 1;
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => {
                stats.invalid_json_lines += 1;
                continue;
            }
        };
        let entry: PromptStashEntryWire =
            match serde_json::from_value::<PromptStashEntryWire>(value) {
                Ok(entry)
                    if !entry.id.is_empty() && !entry.created_at.is_empty() =>
                {
                    entry
                }
                _ => {
                    stats.invalid_record_lines += 1;
                    continue;
                }
            };
        stats.loaded_rows += 1;
        rows.push(entry);
    }
    Ok((rows, stats))
}

// Rewrite is a _merge_: caller's rows win on id collision; rows present on
// disk but absent from the input are preserved (they may be concurrent appends
// from another instance). Callers cannot delete rows by passing a shorter list
// — use `pop_prompt_stash` for removal.
fn merge_and_rewrite_entries_unlocked(
    path: &Path,
    input: &[PromptStashEntryWire],
) -> Result<(), String> {
    let (existing, _) = read_rows_unlocked(path)?;
    let input_ids: BTreeSet<&str> =
        input.iter().map(|n| n.id.as_str()).collect();
    let mut merged: Vec<PromptStashEntryWire> = input.to_vec();
    for row in existing {
        if !input_ids.contains(row.id.as_str()) {
            merged.push(row);
        }
    }
    write_entries_atomic(path, &merged)
}

fn write_entries_atomic(
    path: &Path,
    entries: &[PromptStashEntryWire],
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
        for entry in entries {
            serde_json::to_writer(&mut writer, entry).map_err(|e| {
                format!("failed to serialize prompt stash entry: {e}")
            })?;
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

fn open_lock_file(path: &Path) -> Result<File, String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path_for(path))
        .map_err(|e| e.to_string())
}

fn lock_with_timeout(
    lock: &File,
    path: &Path,
    mode: LockMode,
    timeout: Duration,
) -> PromptStashResult<()> {
    let started = Instant::now();
    let mut attempt = 0_u64;
    loop {
        let result = match mode {
            LockMode::Shared => FileExt::try_lock_shared(lock),
            LockMode::Exclusive => FileExt::try_lock_exclusive(lock),
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    return Err(PromptStashStoreError::LockTimeout {
                        mode: mode.label(),
                        path: lock_path_for(path),
                        waited_ms: elapsed.as_millis(),
                    });
                }
                let delay = retry_delay(attempt).min(timeout - elapsed);
                thread::sleep(delay);
                attempt = attempt.saturating_add(1);
            }
            Err(error) => {
                return Err(PromptStashStoreError::Store(error.to_string()))
            }
        }
    }
}

fn retry_delay(attempt: u64) -> Duration {
    let clock_jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()))
        .unwrap_or(0);
    let jitter = clock_jitter.wrapping_add(attempt.wrapping_mul(17))
        % (LOCK_RETRY_JITTER_MS + 1);
    Duration::from_millis(LOCK_RETRY_MIN_MS + jitter)
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

#[allow(clippy::incompatible_msrv)]
fn unlock(lock: File) -> Result<(), String> {
    lock.unlock().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn held_exclusive_lock_bounds_reader_and_writer_waits() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("prompt_stash.jsonl");
        let holder = open_lock_file(&path).unwrap();
        holder.lock_exclusive().unwrap();

        for mode in [LockMode::Shared, LockMode::Exclusive] {
            let contender = open_lock_file(&path).unwrap();
            let started = Instant::now();
            let error = lock_with_timeout(
                &contender,
                &path,
                mode,
                Duration::from_millis(50),
            )
            .unwrap_err();
            assert!(matches!(error, PromptStashStoreError::LockTimeout { .. }));
            assert!(started.elapsed() < Duration::from_secs(1));
        }

        holder.unlock().unwrap();
    }
}
