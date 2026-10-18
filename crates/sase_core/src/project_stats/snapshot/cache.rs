//! Namespaced committed-code snapshot cache.

#![allow(dead_code)] // Reached through measure_code_snapshot.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::MeasuredPath;
use crate::project_stats::classify::BlobEvidence;
use crate::project_stats::git::ResolvedRepo;
use crate::project_stats::wire::{
    ProjectStatsCodeCounterWire, ProjectStatsSnapshotWire,
    PROJECT_STATS_CLASSIFIER_VERSION, PROJECT_STATS_ENGINE_VERSION,
    PROJECT_STATS_METRIC_POLICY_VERSION, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};

const CODE_SNAPSHOT_CACHE_KIND: &str = "code-snapshot";
const CODE_SNAPSHOT_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeSnapshotCacheEntry {
    schema_version: u32,
    kind: String,
    key: String,
    identity: String,
    common_dir: String,
    tree_oid: String,
    counter: ProjectStatsCodeCounterWire,
    classifier_version: u32,
    complete: bool,
    snapshot: ProjectStatsSnapshotWire,
    rows: Vec<CachedPathRow>,
    blobs: Vec<CachedBlobFacts>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CachedPathRow {
    path_hex: String,
    mode: u32,
    oid: String,
    size: u64,
    attributes: crate::project_stats::classify::CommittedAttributes,
    classification: crate::project_stats::classify::Classification,
    selected_lines: Option<u64>,
    nonblank_lines: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CachedBlobFacts {
    pub oid: String,
    pub size: u64,
    pub evidence: BlobEvidence,
    pub nonblank_lines: u64,
    pub code_lines: BTreeMap<String, u64>,
}

impl CodeSnapshotCacheEntry {
    pub(crate) fn new(
        key: String,
        repo: &ResolvedRepo,
        tree_oid: String,
        counter: ProjectStatsCodeCounterWire,
        snapshot: ProjectStatsSnapshotWire,
        rows: Vec<MeasuredPath>,
        blobs: HashMap<String, CachedBlobFacts>,
    ) -> Self {
        let rows = rows.into_iter().map(CachedPathRow::from).collect();
        let mut blobs: Vec<CachedBlobFacts> = blobs.into_values().collect();
        blobs.sort_by(|left, right| left.oid.cmp(&right.oid));
        Self {
            schema_version: CODE_SNAPSHOT_CACHE_SCHEMA_VERSION,
            kind: CODE_SNAPSHOT_CACHE_KIND.to_owned(),
            key,
            identity: repo.identity.clone(),
            common_dir: repo.common_dir.clone(),
            tree_oid,
            counter,
            classifier_version: PROJECT_STATS_CLASSIFIER_VERSION,
            complete: true,
            snapshot,
            rows,
            blobs,
        }
    }

    pub(crate) fn matches_repo(
        &self,
        key: &str,
        repo: &ResolvedRepo,
        counter: ProjectStatsCodeCounterWire,
    ) -> bool {
        self.schema_version == CODE_SNAPSHOT_CACHE_SCHEMA_VERSION
            && self.kind == CODE_SNAPSHOT_CACHE_KIND
            && self.key == key
            && self.identity == repo.identity
            && self.common_dir == repo.common_dir
            && self.counter == counter
            && self.classifier_version == PROJECT_STATS_CLASSIFIER_VERSION
            && self.complete
    }

    pub(crate) fn tree_oid(&self) -> &str {
        &self.tree_oid
    }

    pub(crate) fn snapshot(&self) -> &ProjectStatsSnapshotWire {
        &self.snapshot
    }

    pub(crate) fn rows(&self) -> &[CachedPathRow] {
        &self.rows
    }

    pub(crate) fn blobs(&self) -> &[CachedBlobFacts] {
        &self.blobs
    }
}

impl From<MeasuredPath> for CachedPathRow {
    fn from(row: MeasuredPath) -> Self {
        Self {
            path_hex: hex::encode(&row.path),
            mode: row.mode,
            oid: row.oid,
            size: row.size,
            attributes: row.attributes,
            classification: row.classification,
            selected_lines: row.selected_lines,
            nonblank_lines: row.nonblank_lines,
        }
    }
}

impl CachedPathRow {
    pub(crate) fn to_measured(&self) -> Option<MeasuredPath> {
        Some(MeasuredPath {
            path: hex::decode(&self.path_hex).ok()?,
            mode: self.mode,
            oid: self.oid.clone(),
            size: self.size,
            attributes: self.attributes,
            classification: self.classification.clone(),
            selected_lines: self.selected_lines,
            nonblank_lines: self.nonblank_lines,
        })
    }
}

pub(crate) fn cache_key(
    repo: &ResolvedRepo,
    counter: ProjectStatsCodeCounterWire,
) -> String {
    let canonical = serde_json::json!({
        "kind": CODE_SNAPSHOT_CACHE_KIND,
        "identity": repo.identity,
        "common_dir": repo.common_dir,
        "project_stats_schema": PROJECT_STATS_WIRE_SCHEMA_VERSION,
        "engine": PROJECT_STATS_ENGINE_VERSION,
        "metric_policy": PROJECT_STATS_METRIC_POLICY_VERSION,
        "classifier": PROJECT_STATS_CLASSIFIER_VERSION,
        "counter": counter,
        "counter_version": crate::project_stats::counter::COUNTER_VERSION,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn cache_path(cache_dir: &str, key: &str) -> PathBuf {
    Path::new(cache_dir)
        .join(format!("v{CODE_SNAPSHOT_CACHE_SCHEMA_VERSION}"))
        .join(format!("{key}.json"))
}

pub(crate) fn load(path: &Path) -> Option<Arc<CodeSnapshotCacheEntry>> {
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
                return Some(Arc::clone(&entry.value));
            }
        }
    }

    let text = fs::read_to_string(path).ok()?;
    let entry: CodeSnapshotCacheEntry = serde_json::from_str(&text).ok()?;
    if entry.schema_version != CODE_SNAPSHOT_CACHE_SCHEMA_VERSION
        || entry.kind != CODE_SNAPSHOT_CACHE_KIND
        || !entry.complete
    {
        return None;
    }
    let arc = Arc::new(entry);
    if let Ok(mut guard) = cache_memo().lock() {
        guard.insert(
            path_key,
            MemoEntry {
                mtime_secs: age.as_secs(),
                mtime_nanos: age.subsec_nanos(),
                length,
                value: Arc::clone(&arc),
            },
        );
    }
    Some(arc)
}

pub(crate) fn persist(path: &Path, entry: &CodeSnapshotCacheEntry) {
    if !entry.complete {
        return;
    }
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
    let wrote = write_temp_and_rename(parent, path, &json).is_ok();
    let _ = FileExt::unlock(&lock);
    if !wrote {
        return;
    }
    remember(path, entry);
}

fn remember(path: &Path, entry: &CodeSnapshotCacheEntry) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    let Ok(modified) = metadata.modified() else {
        return;
    };
    let Ok(age) = modified.duration_since(UNIX_EPOCH) else {
        return;
    };
    if let Ok(mut guard) = cache_memo().lock() {
        guard.insert(
            path.to_string_lossy().into_owned(),
            MemoEntry {
                mtime_secs: age.as_secs(),
                mtime_nanos: age.subsec_nanos(),
                length: metadata.len(),
                value: Arc::new(entry.clone()),
            },
        );
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
    value: Arc<CodeSnapshotCacheEntry>,
}

fn cache_memo() -> &'static Mutex<HashMap<String, MemoEntry>> {
    static MEMO: OnceLock<Mutex<HashMap<String, MemoEntry>>> = OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}
