//! Per-scope snapshot cache for memory history.
//!
//! [`sync_scope`] persists the classified subjects plus the embedded
//! file-history index under `{cache_dir}/v{schema}/{sha256}.json` and
//! reuses them on later calls: equal tips are fresh, tip ancestry
//! folds the git walk and reclassifies the folded lineages in full,
//! and anything else rebuilds. A corrupt, unreadable, or
//! wrong-schema file is `Ok(None)` for the load and rebuilds without
//! surfacing a cache error. Snapshot writes take a writer-only
//! advisory lock on a sibling `.lock` file; readers never open the
//! lock. A process-local memo keyed by `(path, mtime, len)` skips
//! reparsing inside one process.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

use fs2::FileExt;
use sha2::{Digest, Sha256};

use super::causes::attribute_instruction_causes;
use super::classify::classify_subjects;
use super::subjects::{derive_subjects, memory_history_pathspecs};
use super::upstream::upstream_ahead;
use super::wire::{
    MemoryHistoryError, MemoryHistoryScopeWire, MemoryHistorySnapshotWire,
    MemoryHistorySubjectWire, MemoryHistorySyncStatusWire,
    MemoryHistorySyncWire, CLASSIFIER_VERSION,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
use crate::file_history::{
    build_index, run_git_checked, run_git_unchecked, sync_index,
    FileHistoryBudgetWire, FileHistoryError, FileHistoryIndexWire,
    FILE_HISTORY_WIRE_SCHEMA_VERSION,
};

/// Outcome of [`sync_scope`]: the sync record plus the synced
/// subjects the queries build on.
pub struct SyncOutcome {
    /// The sync record (status, tip, counts, health, upstream, time).
    pub sync: MemoryHistorySyncWire,
    /// Classified, cause-attributed subjects at the synced tip.
    pub subjects: Vec<MemoryHistorySubjectWire>,
}

/// Canonical cache key for a scope: scope key, common dir, sorted
/// pathspecs, sorted generated notes, renderer prefixes, config
/// paths, and memory roots, the canonical instruction-file list, and
/// both version constants. `repo_root` and `cache_dir` stay out.
pub fn snapshot_cache_key(
    scope: &MemoryHistoryScopeWire,
    repo_common_dir: &str,
    pathspecs: &[String],
) -> String {
    let mut specs = pathspecs.to_vec();
    specs.sort();
    specs.dedup();
    let mut generated = scope.generated_notes.clone();
    generated.sort();
    generated.dedup();
    let mut renderers = scope.renderer_prefixes.clone();
    renderers.sort();
    renderers.dedup();
    let mut configs = scope.config_paths.clone();
    configs.sort();
    configs.dedup();
    let mut roots = scope.memory_roots.clone();
    roots.sort();
    roots.dedup();
    let mut instructions: Vec<serde_json::Value> = scope
        .instruction_files
        .iter()
        .map(|entry| {
            let mut shims = entry.shim_paths.clone();
            shims.sort();
            shims.dedup();
            serde_json::json!({
                "dir": entry.dir,
                "agents_path": entry.agents_path,
                "shim_paths": shims,
                "template": entry.template,
                "managed": entry.managed,
            })
        })
        .collect();
    instructions.sort_by(|left, right| {
        left["dir"].as_str().cmp(&right["dir"].as_str())
    });
    let canonical = serde_json::json!({
        "scope_key": scope.scope_key,
        "repo_common_dir": repo_common_dir,
        "pathspecs": specs,
        "generated_notes": generated,
        "renderer_prefixes": renderers,
        "config_paths": configs,
        "memory_roots": roots,
        "instruction_files": instructions,
        "wire_schema": MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        "classifier": CLASSIFIER_VERSION,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(&bytes))
}

/// Snapshot file for a cache key: `{cache_dir}/v{schema}/{key}.json`.
pub fn snapshot_path(cache_dir: &str, key: &str) -> PathBuf {
    Path::new(cache_dir)
        .join(format!("v{MEMORY_HISTORY_WIRE_SCHEMA_VERSION}"))
        .join(format!("{key}.json"))
}

/// Drop the process-local snapshot memo. Tests use this to prove a
/// second sync is fresh from disk rather than from memory.
pub fn clear_snapshot_memo() {
    if let Ok(mut guard) = snapshot_memo().lock() {
        guard.clear();
    }
}

/// Sync one scope: load the snapshot, fold or rebuild as needed,
/// persist the result, and report the sync record with the subjects.
/// A persist failure still returns the in-memory result.
pub fn sync_scope(
    scope: &MemoryHistoryScopeWire,
) -> Result<SyncOutcome, MemoryHistoryError> {
    let started = Instant::now();
    let pathspecs = memory_history_pathspecs(scope)?;
    if scope.scope_key.is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "scope_key is empty".to_string(),
        ));
    }
    if scope.repo_root.is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "repo_root is empty".to_string(),
        ));
    }
    let budget = FileHistoryBudgetWire::default();
    let repo = Path::new(&scope.repo_root);
    let toplevel = repo_toplevel(repo, &budget)?;
    let common_dir = repo_common_dir(&toplevel, &budget)?;
    let head = repo_head(&toplevel, &budget)?;

    let key = snapshot_cache_key(scope, &common_dir, &pathspecs);
    let cached = if scope.cache_dir.is_empty() {
        None
    } else {
        load_snapshot(&snapshot_path(&scope.cache_dir, &key))
    };

    let (mut index, subjects, status) = match cached {
        Some(snapshot) if snapshot.cache_key == key => {
            if snapshot.file_index.tip == head {
                let mut fresh_index = snapshot.file_index;
                fresh_index.repo_root = path_string(&toplevel);
                fresh_index.repo_common_dir = common_dir.clone();
                (
                    fresh_index,
                    snapshot.subjects,
                    MemoryHistorySyncStatusWire::Fresh,
                )
            } else if is_foldable(
                &toplevel,
                &snapshot.file_index.tip,
                &head,
                &budget,
            ) {
                let (folded, _) = sync_index(
                    &toplevel,
                    &pathspecs,
                    &snapshot.file_index,
                    &budget,
                )?;
                let reclassified = classify_full(&toplevel, &folded, scope)?;
                (folded, reclassified, MemoryHistorySyncStatusWire::Folded)
            } else {
                let (rebuilt, fresh_subjects) =
                    rebuild(&toplevel, &pathspecs, scope, &budget)?;
                (
                    rebuilt,
                    fresh_subjects,
                    MemoryHistorySyncStatusWire::Rebuilt,
                )
            }
        }
        _ => {
            let (rebuilt, fresh_subjects) =
                rebuild(&toplevel, &pathspecs, scope, &budget)?;
            (
                rebuilt,
                fresh_subjects,
                MemoryHistorySyncStatusWire::Rebuilt,
            )
        }
    };

    if matches!(
        status,
        MemoryHistorySyncStatusWire::Folded
            | MemoryHistorySyncStatusWire::Rebuilt
    ) && !scope.cache_dir.is_empty()
    {
        index.repo_root = path_string(&toplevel);
        index.repo_common_dir = common_dir.clone();
        persist_snapshot(
            &snapshot_path(&scope.cache_dir, &key),
            &MemoryHistorySnapshotWire {
                schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
                cache_key: key,
                subjects: subjects.clone(),
                file_index: index.clone(),
            },
        );
    }

    let subject_count = subjects.len() as u64;
    let version_count = subjects
        .iter()
        .map(|subject| subject.versions.len() as u64)
        .sum();
    let hidden_version_count = subjects
        .iter()
        .flat_map(|subject| &subject.versions)
        .filter(|version| version.hidden_by_default)
        .count() as u64;
    let upstream = upstream_ahead(&toplevel, &pathspecs);
    Ok(SyncOutcome {
        sync: MemoryHistorySyncWire {
            schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
            status,
            tip: index.tip.clone(),
            subject_count,
            version_count,
            hidden_version_count,
            health: index.health.clone(),
            upstream_ahead: upstream,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
        subjects,
    })
}

/// Load a snapshot, or `None` when it is absent, corrupt,
/// unreadable, or the wrong schema. A `(path, mtime, len)` memo
/// skips reparsing inside this process. Never opens the lock file.
pub fn load_snapshot(path: &Path) -> Option<MemoryHistorySnapshotWire> {
    let path_key = path.to_string_lossy().into_owned();
    let meta = fs::metadata(path).ok()?;
    let len = meta.len();
    let mtime = meta.modified().ok()?;
    let age = mtime.duration_since(UNIX_EPOCH).ok()?;
    if let Ok(guard) = snapshot_memo().lock() {
        if let Some(entry) = guard.get(&path_key) {
            if entry.mtime_secs == age.as_secs()
                && entry.mtime_nanos == age.subsec_nanos()
                && entry.len == len
            {
                return Some(entry.snapshot.clone());
            }
        }
    }
    let text = fs::read_to_string(path).ok()?;
    let snapshot: MemoryHistorySnapshotWire =
        serde_json::from_str(&text).ok()?;
    if snapshot.schema_version != MEMORY_HISTORY_WIRE_SCHEMA_VERSION {
        return None;
    }
    if snapshot.file_index.schema_version != FILE_HISTORY_WIRE_SCHEMA_VERSION {
        return None;
    }
    if let Ok(mut guard) = snapshot_memo().lock() {
        guard.insert(
            path_key,
            MemoEntry {
                mtime_secs: age.as_secs(),
                mtime_nanos: age.subsec_nanos(),
                len,
                snapshot: snapshot.clone(),
            },
        );
    }
    Some(snapshot)
}

/// Persist a snapshot with a writer-only advisory lock on a sibling
/// `.lock` file and the same temp-file-plus-rename pattern as
/// `persist_index`. Refuses snapshots that still hold `unclassified`
/// rows. Every failure is silent: the caller keeps the in-memory
/// result either way.
pub fn persist_snapshot(path: &Path, snapshot: &MemoryHistorySnapshotWire) {
    use super::wire::MemoryHistoryClassWire;

    if snapshot
        .subjects
        .iter()
        .flat_map(|subject| &subject.versions)
        .any(|version| version.class == MemoryHistoryClassWire::Unclassified)
    {
        return;
    }
    let Ok(json) = serde_json::to_string(snapshot) else {
        return;
    };
    let Some(parent) = path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let lock_path = sibling_lock_path(path);
    let Ok(lock) = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
    else {
        return;
    };
    if lock.lock_exclusive().is_err() {
        return;
    }
    let _ = write_temp_and_rename(parent, path, json.as_bytes());
    let _ = FileExt::unlock(&lock);
}

/// Sibling advisory-lock path: `snapshot.json` locks `snapshot.json.lock`.
fn sibling_lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    path.with_file_name(name)
}

fn write_temp_and_rename(
    parent: &Path,
    path: &Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| {
        std::io::Error::other(format!("persist snapshot: {error}"))
    })?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Full rebuild: fresh index, subject derivation, classification over
/// every lineage, and cause attribution.
fn rebuild(
    toplevel: &Path,
    pathspecs: &[String],
    scope: &MemoryHistoryScopeWire,
    budget: &FileHistoryBudgetWire,
) -> Result<
    (FileHistoryIndexWire, Vec<MemoryHistorySubjectWire>),
    MemoryHistoryError,
> {
    let index = build_index(toplevel, pathspecs, budget)?;
    let subjects = classify_full(toplevel, &index, scope)?;
    Ok((index, subjects))
}

/// Classify every lineage of a folded or rebuilt index. Reclassifying
/// the full folded index (rather than hand-merging old class rows)
/// is what makes an incremental sync equal a cold rebuild.
fn classify_full(
    toplevel: &Path,
    index: &FileHistoryIndexWire,
    scope: &MemoryHistoryScopeWire,
) -> Result<Vec<MemoryHistorySubjectWire>, MemoryHistoryError> {
    let mut subjects = derive_subjects(index, scope)?;
    classify_subjects(toplevel, index, &mut subjects, scope)?;
    attribute_instruction_causes(toplevel, &mut subjects, scope)?;
    Ok(subjects)
}

/// Foldable when the cached tip is a real SHA, HEAD is a real SHA,
/// and the tip is a first-parent ancestor of HEAD. Anything else
/// (rewrite, missing tip, branch switch) rebuilds.
fn is_foldable(
    toplevel: &Path,
    cached_tip: &str,
    head: &str,
    budget: &FileHistoryBudgetWire,
) -> bool {
    use crate::file_history::looks_like_full_sha;

    if !looks_like_full_sha(cached_tip) || !looks_like_full_sha(head) {
        return false;
    }
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    run_git_unchecked(
        toplevel,
        &["merge-base", "--is-ancestor", cached_tip, head],
        timeout,
        64 * 1024,
    )
    .is_ok_and(|result| result.code == Some(0))
}

fn budget_timeout(budget: &FileHistoryBudgetWire) -> Duration {
    Duration::from_millis(budget.timeout_ms.max(1))
}

fn repo_toplevel(
    repo: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<PathBuf, MemoryHistoryError> {
    run_git_checked(
        repo,
        &["rev-parse", "--show-toplevel"],
        budget_timeout(budget),
        64 * 1024,
    )
    .map_err(|_| {
        MemoryHistoryError::FileHistory(FileHistoryError::NotARepo(
            path_string(repo),
        ))
    })
    .and_then(|result| {
        let toplevel =
            String::from_utf8_lossy(&result.stdout).trim().to_string();
        if toplevel.is_empty() {
            Err(MemoryHistoryError::FileHistory(FileHistoryError::NotARepo(
                path_string(repo),
            )))
        } else {
            Ok(PathBuf::from(toplevel))
        }
    })
}

fn repo_common_dir(
    toplevel: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<String, MemoryHistoryError> {
    let result = run_git_checked(
        toplevel,
        &["rev-parse", "--git-common-dir"],
        budget_timeout(budget),
        64 * 1024,
    )?;
    let common = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if common.is_empty() {
        return Err(MemoryHistoryError::GitFailed(
            "empty --git-common-dir".to_string(),
        ));
    }
    if Path::new(&common).is_absolute() {
        Ok(common)
    } else {
        Ok(path_string(&toplevel.join(common)))
    }
}

/// Current HEAD SHA, or "" for an unborn HEAD.
fn repo_head(
    toplevel: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<String, MemoryHistoryError> {
    let result = run_git_unchecked(
        toplevel,
        &["rev-parse", "HEAD"],
        budget_timeout(budget),
        64 * 1024,
    )?;
    if result.code != Some(0) {
        return Ok(String::new());
    }
    let head = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if crate::file_history::looks_like_full_sha(&head) {
        Ok(head)
    } else {
        Ok(String::new())
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

struct MemoEntry {
    mtime_secs: u64,
    mtime_nanos: u32,
    len: u64,
    snapshot: MemoryHistorySnapshotWire,
}

fn snapshot_memo() -> &'static Mutex<HashMap<String, MemoEntry>> {
    static MEMO: OnceLock<Mutex<HashMap<String, MemoEntry>>> = OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}
