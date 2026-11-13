//! Persistent never-live verdict cache for runner-capacity scans.
//!
//! The capacity scanner classifies markerless candidates from a compact
//! `agent_meta.json` projection. Terminal `stopped_at` classifications are
//! stable: the old full-parse algorithm returns the same never-live verdict
//! for the same bytes. This module persists those verdicts plus the compact
//! closure and clan-context inputs under the scanned projects root so a
//! fresh process can skip re-reading unchanged terminal history.
//!
//! Cache file: `<projects_root>/.capacity_scan_cache_v2.json`. The leading
//! dot plus non-directory filename keeps discovery from treating it as a
//! project (candidate collection only descends into directories). Entries
//! are keyed by the artifact directory path relative to the projects root,
//! so the file is already namespaced by root; the stored canonical root id
//! additionally guards against a copied cache being trusted elsewhere.
//!
//! What is cached: only markerless candidates whose compact projection
//! carries a non-empty `stopped_at`. A missing PID alone is never cached:
//! PID reuse or a host reboot can flip the old algorithm's verdict without
//! any directory mutation. Every scan re-checks `running.json`,
//! `waiting.json`, and `pending_question.json` with `symlink_metadata`
//! before trusting a cached verdict, and closure can always re-admit a
//! cached negative candidate (which is then fully parsed, never served from
//! the cache).
//!
//! Validation: each entry stores the artifact directory signature
//! (device, inode, high-resolution mtime) and the metadata file signature
//! (device, inode, length, high-resolution mtime and change time). An
//! unavailable per-file mtime makes the candidate uncacheable: it is never
//! stored as a reusable zero stamp. Symlinked metadata or artifact
//! directories are never cached. Entries are built from a stable
//! observation: the signatures around the metadata read must agree, or no
//! entry is published.
//!
//! Failure is always a miss: a missing projects root stays an empty scan
//! without creating anything, and a missing, corrupt, truncated, or
//! wrong-version cache, a root mismatch, a failed stat, or an unwritable
//! location falls back to the uncached conservative path. Cache problems
//! never become admission failures or skipped candidates.
//!
//! Concurrency: writes use atomic temp-file replacement
//! (`fs_sig::write_json_atomic`), so concurrent scanners cannot publish a
//! valid-looking partial file. No lock is taken; losers may drop hits.
//! An unchanged cache is not rewritten. Exact (`scan_agent_artifact_dirs`)
//! and filtered scans add hits but never prune entries for paths they did
//! not visit; only a full successful enumeration discards entries for
//! removed paths. There is no process-local memo beyond the per-scan load,
//! so disk reuse works in a new process.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use super::wire::{AgentArtifactScanOptionsWire, AgentMetaWire};

/// Reserved cache filename directly under the scanned projects root.
pub(crate) const CAPACITY_CACHE_FILENAME: &str = ".capacity_scan_cache_v2.json";

/// Schema version stored inside the cache file.
pub(crate) const CAPACITY_CACHE_VERSION: u32 = 2;

/// Only the metadata inputs used by capacity retention and clan context.
/// Persisting `AgentMetaWire` here used to write every absent/default wire
/// field for every historical row, making a warm scan deserialize a cache
/// several times larger than the source metadata it avoided reading.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct CapacityCacheMetaWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_clan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_clan_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clan_tribe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clan_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runner_claim_owner_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stopped_at: Option<String>,
}

impl From<&AgentMetaWire> for CapacityCacheMetaWire {
    fn from(meta: &AgentMetaWire) -> Self {
        Self {
            agent_clan: meta.agent_clan.clone(),
            agent_clan_generation: meta.agent_clan_generation.clone(),
            clan_tribe: meta.clan_tribe.clone(),
            clan_summary: meta.clan_summary.clone(),
            parent_timestamp: meta.parent_timestamp.clone(),
            pid: meta.pid,
            runner_claim_owner_key: meta.runner_claim_owner_key.clone(),
            stopped_at: meta.stopped_at.clone(),
        }
    }
}

impl From<CapacityCacheMetaWire> for AgentMetaWire {
    fn from(meta: CapacityCacheMetaWire) -> Self {
        Self {
            agent_clan: meta.agent_clan,
            agent_clan_generation: meta.agent_clan_generation,
            clan_tribe: meta.clan_tribe,
            clan_summary: meta.clan_summary,
            parent_timestamp: meta.parent_timestamp,
            pid: meta.pid,
            runner_claim_owner_key: meta.runner_claim_owner_key,
            stopped_at: meta.stopped_at,
            ..Self::default()
        }
    }
}

/// On-disk verdict cache: one entry per validated never-live candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CapacityVerdictCacheWire {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub root: String,
    #[serde(default)]
    pub entries: BTreeMap<String, CapacityCacheEntryWire>,
}

/// One cached never-live verdict: signatures plus the compact projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CapacityCacheEntryWire {
    #[serde(default)]
    pub dir_dev: Option<u64>,
    #[serde(default)]
    pub dir_ino: Option<u64>,
    /// High-resolution directory mtime, nanos since the Unix epoch.
    /// `None` is uncacheable and never stored.
    #[serde(default)]
    pub dir_mtime_ns: Option<i64>,
    #[serde(default)]
    pub meta_dev: Option<u64>,
    #[serde(default)]
    pub meta_ino: Option<u64>,
    #[serde(default)]
    pub meta_len: u64,
    /// High-resolution metadata mtime, nanos since the Unix epoch.
    #[serde(default)]
    pub meta_mtime_ns: Option<i64>,
    /// High-resolution metadata change time where available.
    #[serde(default)]
    pub meta_ctime_ns: Option<i64>,
    /// Compact projection parsed at cache time. Presence in the map means
    /// never-live; the meta must still carry a non-empty `stopped_at`.
    pub meta: CapacityCacheMetaWire,
}

/// Directory signature used for cache validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirSig {
    dev: Option<u64>,
    ino: Option<u64>,
    mtime_ns: i64,
}

/// Metadata file signature used for cache validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetaSig {
    dev: Option<u64>,
    ino: Option<u64>,
    len: u64,
    mtime_ns: i64,
    ctime_ns: Option<i64>,
}

/// Path of the verdict cache for one projects root.
pub(crate) fn capacity_cache_path(projects_root: &Path) -> PathBuf {
    projects_root.join(CAPACITY_CACHE_FILENAME)
}

/// Canonical identity of a projects root for mismatch detection.
fn canonical_root_id(projects_root: &Path) -> Option<String> {
    fs::canonicalize(projects_root)
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
}

/// Relative cache key for one artifact directory.
pub(crate) fn capacity_cache_key(
    projects_root: &Path,
    artifact_dir: &Path,
) -> Option<String> {
    artifact_dir
        .strip_prefix(projects_root)
        .ok()
        .map(|rel| rel.to_string_lossy().into_owned())
        .filter(|key| !key.is_empty())
}

/// Whether this scan enumerated the whole archive: no project, workflow, or
/// lifecycle filter. Only such a scan may prune entries for removed paths.
pub(crate) fn is_full_capacity_enumeration(
    options: &AgentArtifactScanOptionsWire,
) -> bool {
    options.only_projects.is_empty()
        && options.only_workflow_dirs.is_empty()
        && include_states_allow_all(&options.include_project_states)
}

fn include_states_allow_all(states: &[String]) -> bool {
    states.is_empty() || states.iter().any(|state| state == "all")
}

fn system_time_ns(time: std::time::SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).unwrap_or(i128::MAX),
        Err(before) => {
            -i128::try_from(before.duration().as_nanos()).unwrap_or(i128::MAX)
        }
    }
    .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(unix)]
fn file_ids(metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;
    (Some(metadata.dev()), Some(metadata.ino()))
}

#[cfg(not(unix))]
fn file_ids(_metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[cfg(unix)]
fn file_ctime_ns(metadata: &fs::Metadata) -> Option<i64> {
    use std::os::unix::fs::MetadataExt;
    let secs = i128::from(metadata.ctime());
    let nsec = i128::from(metadata.ctime_nsec());
    let total = secs * 1_000_000_000 + nsec;
    i64::try_from(total).ok()
}

#[cfg(not(unix))]
fn file_ctime_ns(_metadata: &fs::Metadata) -> Option<i64> {
    None
}

/// Stat an artifact directory for cache validation. Symlinked directories,
/// failed stats, and unavailable mtimes are uncacheable.
fn dir_signature(artifact_dir: &Path) -> Option<DirSig> {
    let metadata = fs::symlink_metadata(artifact_dir).ok()?;
    if metadata.file_type().is_symlink() {
        return None;
    }
    let mtime_ns = system_time_ns(metadata.modified().ok()?);
    let (dev, ino) = file_ids(&metadata);
    Some(DirSig { dev, ino, mtime_ns })
}

/// Stat a metadata file for cache validation. Symlinks, failed stats, and
/// unavailable mtimes are uncacheable. The length is always available from
/// a successful stat.
fn meta_signature(meta_path: &Path) -> Option<MetaSig> {
    let metadata = fs::symlink_metadata(meta_path).ok()?;
    if metadata.file_type().is_symlink() {
        return None;
    }
    let mtime_ns = system_time_ns(metadata.modified().ok()?);
    let (dev, ino) = file_ids(&metadata);
    Some(MetaSig {
        dev,
        ino,
        len: metadata.len(),
        mtime_ns,
        ctime_ns: file_ctime_ns(&metadata),
    })
}

/// Load a cache entry's signatures for comparison.
fn entry_sigs(entry: &CapacityCacheEntryWire) -> Option<(DirSig, MetaSig)> {
    Some((
        DirSig {
            dev: entry.dir_dev,
            ino: entry.dir_ino,
            mtime_ns: entry.dir_mtime_ns?,
        },
        MetaSig {
            dev: entry.meta_dev,
            ino: entry.meta_ino,
            len: entry.meta_len,
            mtime_ns: entry.meta_mtime_ns?,
            ctime_ns: entry.meta_ctime_ns,
        },
    ))
}

/// Load the verdict cache for one projects root. Every failure mode is a
/// miss: missing/truncated/corrupt file, unknown version, or root
/// mismatch returns `None` and the caller follows the uncached path.
pub(crate) fn load_capacity_cache(
    projects_root: &Path,
) -> Option<CapacityVerdictCacheWire> {
    let bytes = fs::read(capacity_cache_path(projects_root)).ok()?;
    let cache: CapacityVerdictCacheWire =
        serde_json::from_slice(&bytes).ok()?;
    if cache.version != CAPACITY_CACHE_VERSION {
        return None;
    }
    let current = canonical_root_id(projects_root)?;
    if cache.root != current {
        return None;
    }
    Some(cache)
}

/// Look up a cached projection after re-validating directory and metadata
/// signatures. Stale or stopped-cleared entries are misses.
pub(crate) fn cached_capacity_meta(
    cache: &CapacityVerdictCacheWire,
    key: &str,
    artifact_dir: &Path,
    meta_path: &Path,
) -> Option<AgentMetaWire> {
    let entry = cache.entries.get(key)?;
    // Presence means never-live, but a hand-edited or older entry without
    // a terminal stopped classification must never be trusted.
    if entry
        .meta
        .stopped_at
        .as_deref()
        .is_none_or(|value| value.is_empty())
    {
        return None;
    }
    let (want_dir, want_meta) = entry_sigs(entry)?;
    let live_dir = dir_signature(artifact_dir)?;
    let live_meta = meta_signature(meta_path)?;
    if live_dir != want_dir || live_meta != want_meta {
        return None;
    }
    Some(entry.meta.clone().into())
}

/// Read metadata bytes bracketed by signature observations. Returns the
/// bytes plus the agreeing before/after signatures, or `None` when the
/// observation was unstable or a signature was unavailable.
pub(crate) fn read_meta_bytes_stable(
    artifact_dir: &Path,
    meta_path: &Path,
) -> Option<(Vec<u8>, DirSig, MetaSig)> {
    let dir_before = dir_signature(artifact_dir)?;
    let meta_before = meta_signature(meta_path)?;
    let bytes = fs::read(meta_path).ok()?;
    let dir_after = dir_signature(artifact_dir)?;
    let meta_after = meta_signature(meta_path)?;
    if dir_before != dir_after || meta_before != meta_after {
        return None;
    }
    // The length observed in the signature must match the bytes read; a
    // concurrent writer between stat and read invalidates the observation.
    if meta_before.len != bytes.len() as u64 {
        return None;
    }
    Some((bytes, dir_after, meta_after))
}

/// Build a cache entry from a stable observation. Only markerless stopped
/// classifications are cacheable: PID-only verdicts are left uncached so a
/// PID reuse or host reboot without file mutation can never turn into a
/// permanent exclusion.
pub(crate) fn capacity_cache_entry(
    meta: &AgentMetaWire,
    dir_sig: DirSig,
    meta_sig: MetaSig,
) -> Option<CapacityCacheEntryWire> {
    if meta
        .stopped_at
        .as_deref()
        .is_none_or(|value| value.is_empty())
    {
        return None;
    }
    Some(CapacityCacheEntryWire {
        dir_dev: dir_sig.dev,
        dir_ino: dir_sig.ino,
        dir_mtime_ns: Some(dir_sig.mtime_ns),
        meta_dev: meta_sig.dev,
        meta_ino: meta_sig.ino,
        meta_len: meta_sig.len,
        meta_mtime_ns: Some(meta_sig.mtime_ns),
        meta_ctime_ns: meta_sig.ctime_ns,
        meta: CapacityCacheMetaWire::from(meta),
    })
}

/// Merge staged entries into a loaded cache and atomically persist it.
/// Exact and filtered scans add hits but never prune. A full enumeration
/// with no filesystem errors may discard entries for removed paths.
/// An unchanged cache is not rewritten; write failures are silent misses.
pub(crate) fn store_capacity_cache(
    projects_root: &Path,
    loaded: Option<CapacityVerdictCacheWire>,
    staged: BTreeMap<String, CapacityCacheEntryWire>,
    visited: &std::collections::BTreeSet<String>,
    allow_prune: bool,
    had_os_errors: bool,
) {
    if !projects_root.exists() {
        return;
    }
    let Some(root) = canonical_root_id(projects_root) else {
        return;
    };
    let had_loaded = loaded.is_some();
    let mut entries = loaded.map(|cache| cache.entries).unwrap_or_default();
    let mut dirty = false;
    let staged_was_empty = staged.is_empty();
    for (key, entry) in staged {
        let changed = entries.get(&key).is_none_or(|old| old != &entry);
        if changed {
            entries.insert(key, entry);
            dirty = true;
        }
    }
    if allow_prune && !had_os_errors {
        // Warm scans stage nothing: when every cached key is still
        // visited, pruning is a no-op. The ordered merge below proves
        // that in linear time instead of a BTree lookup per entry.
        let prune_needed = if staged_was_empty {
            let mut seen = visited.iter();
            let mut current = seen.next();
            entries.keys().any(|key| loop {
                match current {
                    Some(candidate) if candidate < key => {
                        current = seen.next();
                    }
                    Some(candidate) if candidate == key => break false,
                    _ => break true,
                }
            })
        } else {
            true
        };
        if prune_needed {
            let before = entries.len();
            entries.retain(|key, _| visited.contains(key));
            if entries.len() != before {
                dirty = true;
            }
        }
    }
    if !dirty {
        // Avoid rewriting an unchanged cache on every scan. When there was
        // no loaded cache and nothing to store, there is nothing to write.
        if had_loaded {
            return;
        }
        if entries.is_empty() {
            return;
        }
    }
    let cache = CapacityVerdictCacheWire {
        version: CAPACITY_CACHE_VERSION,
        root,
        entries,
    };
    let Ok(bytes) = serde_json::to_vec(&cache) else {
        return;
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return;
    };
    let _ = crate::fs_sig::write_json_atomic(
        &capacity_cache_path(projects_root),
        &value,
    );
}
