//! Raw full-DAG history cache.
//!
//! Cache files hold only commit rows. Attribution is intentionally computed
//! after loading them so changed directory data can reclassify old commits
//! without another Git walk.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::git::ResolvedRepo;
use super::history::HistoryRow;
use super::wire::{
    PROJECT_STATS_CLASSIFIER_VERSION, PROJECT_STATS_ENGINE_VERSION,
    PROJECT_STATS_METRIC_POLICY_VERSION, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use crate::commit_footer::COMMIT_FOOTER_WIRE_SCHEMA_VERSION;

const HISTORY_CACHE_SCHEMA_VERSION: u32 = 1;

/// One versioned cache document for a repository's raw history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HistoryCacheEntry {
    schema_version: u32,
    key: String,
    identity: String,
    common_dir: String,
    tip: String,
    shallow: bool,
    fetched_at: Option<i64>,
    complete: bool,
    rows: Vec<HistoryRow>,
}

impl HistoryCacheEntry {
    pub(crate) fn new(
        key: String,
        repo: &ResolvedRepo,
        complete: bool,
        rows: Vec<HistoryRow>,
    ) -> Self {
        Self {
            schema_version: HISTORY_CACHE_SCHEMA_VERSION,
            key,
            identity: repo.identity.clone(),
            common_dir: repo.common_dir.clone(),
            tip: repo.tip.clone(),
            shallow: repo.shallow,
            fetched_at: repo.fetched_at,
            complete,
            rows,
        }
    }

    pub(crate) fn matches_repo(&self, key: &str, repo: &ResolvedRepo) -> bool {
        self.schema_version == HISTORY_CACHE_SCHEMA_VERSION
            && self.key == key
            && self.identity == repo.identity
            && self.common_dir == repo.common_dir
            && self.shallow == repo.shallow
            && self.fetched_at == repo.fetched_at
    }

    pub(crate) fn tip(&self) -> &str {
        &self.tip
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn rows(&self) -> &[HistoryRow] {
        &self.rows
    }
}

/// Hash the semantic repository identity and every policy that affects
/// cached raw rows. The common directory prevents two local clones of the
/// same remote from sharing repository state.
pub(crate) fn cache_key(repo: &ResolvedRepo) -> String {
    let canonical = serde_json::json!({
        "identity": repo.identity,
        "common_dir": repo.common_dir,
        "project_stats_schema": PROJECT_STATS_WIRE_SCHEMA_VERSION,
        "engine": PROJECT_STATS_ENGINE_VERSION,
        "metric_policy": PROJECT_STATS_METRIC_POLICY_VERSION,
        "classifier": PROJECT_STATS_CLASSIFIER_VERSION,
        "commit_footer_schema": COMMIT_FOOTER_WIRE_SCHEMA_VERSION,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// Cache path: `{cache_dir}/v{schema}/{key}.json`; `key` is the SHA-256
/// digest of the canonical semantic key assembled above.
pub(crate) fn cache_path(cache_dir: &str, key: &str) -> PathBuf {
    Path::new(cache_dir)
        .join(format!("v{HISTORY_CACHE_SCHEMA_VERSION}"))
        .join(format!("{key}.json"))
}

/// Load one valid entry, treating all disk errors as a cache miss.
/// Readers never open the writer lock file.
pub(crate) fn load(path: &Path) -> Option<HistoryCacheEntry> {
    let path_key = path.to_string_lossy().into_owned();
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let age = modified.duration_since(UNIX_EPOCH).ok()?;
    let length = metadata.len();

    if let Ok(guard) = cache_memo().lock() {
        if let Some(entry) = guard.get(&path_key) {
            if entry.mtime_secs == age.as_secs()
                && entry.mtime_nanos == age.subsec_nanos()
                && entry.length == length
            {
                return Some(entry.value.clone());
            }
        }
    }

    let text = fs::read_to_string(path).ok()?;
    let entry: HistoryCacheEntry = serde_json::from_str(&text).ok()?;
    if entry.schema_version != HISTORY_CACHE_SCHEMA_VERSION {
        return None;
    }
    if let Ok(mut guard) = cache_memo().lock() {
        guard.insert(
            path_key,
            MemoEntry {
                mtime_secs: age.as_secs(),
                mtime_nanos: age.subsec_nanos(),
                length,
                value: entry.clone(),
            },
        );
    }
    Some(entry)
}

/// Persist an entry using a writer-only sibling lock and atomic rename.
/// Failures leave the in-memory history usable and are deliberately silent.
pub(crate) fn persist(path: &Path, entry: &HistoryCacheEntry) {
    let Ok(json) = serde_json::to_vec(entry) else {
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
    let _ = write_temp_and_rename(parent, path, &json);
    let _ = FileExt::unlock(&lock);
    if let Ok(mut guard) = cache_memo().lock() {
        guard.remove(&path.to_string_lossy().into_owned());
    }
}

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
    temporary
        .persist(path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Clear the process memo for tests that verify disk reload behavior.
#[cfg(test)]
pub(crate) fn clear_memo() {
    if let Ok(mut guard) = cache_memo().lock() {
        guard.clear();
    }
}

struct MemoEntry {
    mtime_secs: u64,
    mtime_nanos: u32,
    length: u64,
    value: HistoryCacheEntry,
}

fn cache_memo() -> &'static Mutex<HashMap<String, MemoEntry>> {
    static MEMO: OnceLock<Mutex<HashMap<String, MemoEntry>>> = OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}
