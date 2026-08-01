use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

use super::wire::{
    PromptStashEntryWire, PromptStashPopOutcomeWire, PromptStashSnapshotWire,
    PromptStashStoreStatsWire, PROMPT_STASH_WIRE_SCHEMA_VERSION,
};

const LOCK_TIMEOUT_ENV: &str = "SASE_PROMPT_STASH_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(120);

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
    let lock = lock_with_timeout(
        path,
        LockMode::Shared,
        prompt_stash_lock_timeout(),
        "read_prompt_stash_snapshot",
    )?;
    let result = read_rows_unlocked(path);
    unlock(lock)?;
    let (entries, stats) = result?;
    Ok(snapshot_from_rows(entries, stats))
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
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "pop_prompt_stash",
    )?;
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
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "set_prompt_stash_pinned",
    )?;
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
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        prompt_stash_lock_timeout(),
        "rewrite_prompt_stash",
    )?;
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

#[allow(clippy::incompatible_msrv)]
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
