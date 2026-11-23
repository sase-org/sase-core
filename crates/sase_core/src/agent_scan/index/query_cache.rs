//! Process-local LRU of identical artifact-index query results.
//!
//! Repeated `query_agent_artifact_index` calls with the same index, root,
//! query, and options skip `record_json` decode when the selected rows'
//! stored marker signatures still match the filesystem (revalidate) or
//! the stored signature map (cached). Repair stays on the miss path:
//! a marker mismatch never returns a cached payload.

use super::index_wire::{
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
};
use super::record_summary::MarkerSignatures;
use super::selection::collect_query_signature_map;
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire,
};
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
const MAX_ENTRIES: usize = 1024;
#[cfg(not(test))]
const MAX_ENTRIES: usize = 8;

#[derive(Clone, PartialEq, Eq)]
struct QueryCacheKey {
    index_path: PathBuf,
    projects_root: PathBuf,
    query_json: String,
    options_json: String,
}

struct CachedQuery {
    key: QueryCacheKey,
    signature_map: BTreeMap<String, MarkerSignatures>,
    records: Vec<AgentArtifactRecordWire>,
}

fn cache() -> &'static Mutex<Vec<CachedQuery>> {
    static CACHE: OnceLock<Mutex<Vec<CachedQuery>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

fn lock_cache() -> std::sync::MutexGuard<'static, Vec<CachedQuery>> {
    match cache().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn canonical_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn query_key_json(
    query: &AgentArtifactIndexQueryWire,
) -> Result<String, String> {
    let mut keyed = query.clone();
    // Cached and Revalidate share a payload when signatures still match.
    keyed.freshness = AgentArtifactIndexFreshnessWire::Cached;
    serde_json::to_string(&keyed).map_err(|error| error.to_string())
}

fn make_key(
    index_path: &Path,
    projects_root: &Path,
    query: &AgentArtifactIndexQueryWire,
    options: &AgentArtifactScanOptionsWire,
) -> Option<QueryCacheKey> {
    let query_json = query_key_json(query).ok()?;
    let options_json = serde_json::to_string(options).ok()?;
    Some(QueryCacheKey {
        index_path: canonical_path(index_path),
        projects_root: canonical_path(projects_root),
        query_json,
        options_json,
    })
}

fn contains_key(key: &QueryCacheKey) -> bool {
    lock_cache().iter().any(|entry| entry.key == *key)
}

fn lookup_records(
    key: &QueryCacheKey,
    expected_map: &BTreeMap<String, MarkerSignatures>,
) -> Option<Vec<AgentArtifactRecordWire>> {
    let mut cache = lock_cache();
    let position = cache.iter().position(|entry| entry.key == *key)?;
    if cache[position].signature_map != *expected_map {
        return None;
    }
    let records = cache[position].records.clone();
    let entry = cache.remove(position);
    cache.push(entry);
    Some(records)
}

fn store(
    key: QueryCacheKey,
    signature_map: BTreeMap<String, MarkerSignatures>,
    records: Vec<AgentArtifactRecordWire>,
) {
    let mut cache = lock_cache();
    if let Some(position) = cache.iter().position(|entry| entry.key == key) {
        cache.remove(position);
    } else if cache.len() >= MAX_ENTRIES {
        cache.remove(0);
    }
    cache.push(CachedQuery {
        key,
        signature_map,
        records,
    });
}

fn signatures_match_filesystem(
    signature_map: &BTreeMap<String, MarkerSignatures>,
    stats: &mut AgentArtifactScanStatsWire,
) -> bool {
    for (artifact_dir, stored) in signature_map {
        stats.marker_signatures_checked += 1;
        if MarkerSignatures::from_artifact_dir(artifact_dir) != *stored {
            return false;
        }
    }
    true
}

/// Return cached records when this exact query is still signature-fresh.
pub(super) fn try_reuse(
    index_path: &Path,
    projects_root: &Path,
    query: &AgentArtifactIndexQueryWire,
    options: &AgentArtifactScanOptionsWire,
    conn: &Connection,
    project_filter: Option<&BTreeSet<String>>,
    stats: &mut AgentArtifactScanStatsWire,
) -> Result<Option<Vec<AgentArtifactRecordWire>>, String> {
    let Some(key) = make_key(index_path, projects_root, query, options) else {
        return Ok(None);
    };
    if !contains_key(&key) {
        return Ok(None);
    }
    let signature_map =
        collect_query_signature_map(conn, query, project_filter)?;
    if query.freshness == AgentArtifactIndexFreshnessWire::Revalidate
        && !signatures_match_filesystem(&signature_map, stats)
    {
        return Ok(None);
    }
    Ok(lookup_records(&key, &signature_map))
}

/// Remember the decoded payload of a cacheable index query.
pub(super) fn remember(
    index_path: &Path,
    projects_root: &Path,
    query: &AgentArtifactIndexQueryWire,
    options: &AgentArtifactScanOptionsWire,
    conn: &Connection,
    project_filter: Option<&BTreeSet<String>>,
    records: &[AgentArtifactRecordWire],
) {
    let Some(key) = make_key(index_path, projects_root, query, options) else {
        return;
    };
    let Ok(signature_map) =
        collect_query_signature_map(conn, query, project_filter)
    else {
        return;
    };
    store(key, signature_map, records.to_vec());
}
