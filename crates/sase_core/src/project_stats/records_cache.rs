//! Separately namespaced persistent cache for metadata-only record summaries.
#![allow(dead_code)] // Used by the records measurement seam in this phase.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::git::ResolvedRepo;
use super::records::RecordSummary;

const SCHEMA_VERSION: u32 = 1;
const POLICY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordCacheEntry {
    schema_version: u32,
    key: String,
    identity: String,
    common_dir: String,
    tip: String,
    shallow: bool,
    fetched_at: Option<i64>,
    summary: RecordSummary,
}

impl RecordCacheEntry {
    pub(crate) fn new(
        key: String,
        repo: &ResolvedRepo,
        summary: RecordSummary,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            key,
            identity: repo.identity.clone(),
            common_dir: repo.common_dir.clone(),
            tip: repo.tip.clone(),
            shallow: repo.shallow,
            fetched_at: repo.fetched_at,
            summary,
        }
    }

    pub(crate) fn matches(&self, key: &str, repo: &ResolvedRepo) -> bool {
        self.schema_version == SCHEMA_VERSION
            && self.key == key
            && self.identity == repo.identity
            && self.common_dir == repo.common_dir
            && self.tip == repo.tip
            && self.shallow == repo.shallow
            && self.fetched_at == repo.fetched_at
    }

    pub(crate) fn summary(&self) -> &RecordSummary {
        &self.summary
    }
}

pub(crate) fn cache_key(repo: &ResolvedRepo) -> String {
    let value = serde_json::json!({
        "namespace": "project_stats_records",
        "policy": POLICY_VERSION,
        "identity": repo.identity,
        "common_dir": repo.common_dir,
        "tip": repo.tip,
        "shallow": repo.shallow,
        "fetched_at": repo.fetched_at,
    });
    hex::encode(Sha256::digest(
        serde_json::to_vec(&value).unwrap_or_default(),
    ))
}

pub(crate) fn cache_path(cache_dir: &str, key: &str) -> PathBuf {
    Path::new(cache_dir)
        .join(format!("records-v{SCHEMA_VERSION}"))
        .join(format!("{key}.json"))
}

pub(crate) fn load(path: &Path) -> Option<RecordCacheEntry> {
    let entry: RecordCacheEntry =
        serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    (entry.schema_version == SCHEMA_VERSION).then_some(entry)
}

pub(crate) fn persist(path: &Path, entry: &RecordCacheEntry) {
    let Ok(bytes) = serde_json::to_vec(entry) else {
        return;
    };
    let Some(parent) = path.parent() else { return };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let mut lock_name = path.as_os_str().to_owned();
    lock_name.push(".lock");
    let Ok(lock) = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(PathBuf::from(lock_name))
    else {
        return;
    };
    if lock.lock_exclusive().is_err() {
        return;
    }
    let result = write_atomic(parent, path, &bytes);
    let _ = FileExt::unlock(&lock);
    let _ = result;
}

fn write_atomic(
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
    File::open(parent)?.sync_all()
}
