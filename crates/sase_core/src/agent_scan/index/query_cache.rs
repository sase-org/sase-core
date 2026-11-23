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
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

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
    compact: bool,
}

struct CachedQuery {
    key: QueryCacheKey,
    signature_map: QuerySignatureMap,
    records: Arc<Vec<AgentArtifactRecordWire>>,
    generation: u64,
}

type QuerySignatureMap = BTreeMap<String, (MarkerSignatures, u64)>;

pub(super) struct CachedRecords {
    pub(super) records: Arc<Vec<AgentArtifactRecordWire>>,
    pub(super) generation: u64,
}

fn cache() -> &'static Mutex<Vec<CachedQuery>> {
    static CACHE: OnceLock<Mutex<Vec<CachedQuery>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

fn next_generation() -> &'static AtomicU64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    &NEXT
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
    compact: bool,
) -> Option<QueryCacheKey> {
    let query_json = query_key_json(query).ok()?;
    let options_json = serde_json::to_string(options).ok()?;
    Some(QueryCacheKey {
        index_path: canonical_path(index_path),
        projects_root: canonical_path(projects_root),
        query_json,
        options_json,
        compact,
    })
}

fn contains_key(key: &QueryCacheKey) -> bool {
    lock_cache().iter().any(|entry| entry.key == *key)
}

fn lookup_records(
    key: &QueryCacheKey,
    expected_map: &QuerySignatureMap,
) -> Option<CachedRecords> {
    let mut cache = lock_cache();
    let position = cache.iter().position(|entry| entry.key == *key)?;
    if cache[position].signature_map != *expected_map {
        return None;
    }
    let records = Arc::clone(&cache[position].records);
    let generation = cache[position].generation;
    let entry = cache.remove(position);
    cache.push(entry);
    Some(CachedRecords {
        records,
        generation,
    })
}

fn store(
    key: QueryCacheKey,
    signature_map: QuerySignatureMap,
    records: Arc<Vec<AgentArtifactRecordWire>>,
) -> u64 {
    let mut cache = lock_cache();
    if let Some(position) = cache.iter().position(|entry| entry.key == key) {
        cache.remove(position);
    } else if cache.len() >= MAX_ENTRIES {
        cache.remove(0);
    }
    let generation = next_generation().fetch_add(1, Ordering::Relaxed);
    cache.push(CachedQuery {
        key,
        signature_map,
        records,
        generation,
    });
    generation
}

fn signatures_match_filesystem(
    signature_map: &QuerySignatureMap,
    stats: &mut AgentArtifactScanStatsWire,
) -> bool {
    for (artifact_dir, (stored, _row_signature)) in signature_map {
        stats.marker_signatures_checked += 1;
        if MarkerSignatures::from_artifact_dir(artifact_dir) != *stored {
            return false;
        }
    }
    true
}

fn query_signature_map(
    conn: &Connection,
    query: &AgentArtifactIndexQueryWire,
    project_filter: Option<&BTreeSet<String>>,
) -> Result<QuerySignatureMap, String> {
    let marker_signatures =
        collect_query_signature_map(conn, query, project_filter)?;
    if marker_signatures.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut signature_map: QuerySignatureMap = marker_signatures
        .into_iter()
        .map(|(artifact_dir, signatures)| (artifact_dir, (signatures, 0)))
        .collect();

    // Fold each selected row into the reuse identity. This catches
    // database-only changes (including a rewritten record_json) without
    // relying on SQLite's short-lived WAL file identity. ValueRef hashing
    // avoids allocating row copies or decoding JSON on the warm path.
    let mut keys: Vec<String> = signature_map.keys().cloned().collect();
    keys.sort();
    let mut hashes = BTreeMap::new();
    for chunk in keys.chunks(500) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT artifact_dir, projects_root, agent_meta_sig, done_sig, \
             running_sig, waiting_sig, pending_question_sig, \
             workflow_state_sig, plan_path_sig, prompt_steps_sig, macros_sig, \
             agent_artifacts.* FROM agent_artifacts \
             WHERE artifact_dir IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let column_count = stmt.column_count();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(chunk.iter()))
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let artifact_dir: String = row.get(0).map_err(|e| e.to_string())?;
            let mut hasher = DefaultHasher::new();
            for column in 0..column_count {
                match row.get_ref(column).map_err(|e| e.to_string())? {
                    rusqlite::types::ValueRef::Null => 0_u8.hash(&mut hasher),
                    rusqlite::types::ValueRef::Integer(value) => {
                        1_u8.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                    rusqlite::types::ValueRef::Real(value) => {
                        2_u8.hash(&mut hasher);
                        value.to_bits().hash(&mut hasher);
                    }
                    rusqlite::types::ValueRef::Text(value) => {
                        3_u8.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                    rusqlite::types::ValueRef::Blob(value) => {
                        4_u8.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                }
            }
            hashes.insert(artifact_dir, hasher.finish());
        }
    }

    for (artifact_dir, signature) in &mut signature_map {
        let Some(hash) = hashes.remove(artifact_dir) else {
            return Err(format!(
                "artifact index row disappeared while validating {artifact_dir}"
            ));
        };
        *signature = (signature.0.clone(), hash);
    }
    Ok(signature_map)
}

/// Full request identity, including the output representation.
pub(super) struct QueryIdentity<'a> {
    pub(super) index_path: &'a Path,
    pub(super) projects_root: &'a Path,
    pub(super) query: &'a AgentArtifactIndexQueryWire,
    pub(super) options: &'a AgentArtifactScanOptionsWire,
    pub(super) compact: bool,
}

impl QueryIdentity<'_> {
    fn key(&self) -> Option<QueryCacheKey> {
        make_key(
            self.index_path,
            self.projects_root,
            self.query,
            self.options,
            self.compact,
        )
    }
}

/// Return cached records when this exact query is still signature-fresh.
pub(super) fn try_reuse(
    identity: &QueryIdentity<'_>,
    conn: &Connection,
    project_filter: Option<&BTreeSet<String>>,
    stats: &mut AgentArtifactScanStatsWire,
) -> Result<Option<CachedRecords>, String> {
    let Some(key) = identity.key() else {
        return Ok(None);
    };
    if !contains_key(&key) {
        return Ok(None);
    }
    let signature_map =
        query_signature_map(conn, identity.query, project_filter)?;
    if identity.query.freshness == AgentArtifactIndexFreshnessWire::Revalidate
        && !signatures_match_filesystem(&signature_map, stats)
    {
        return Ok(None);
    }
    Ok(lookup_records(&key, &signature_map))
}

/// Remember the decoded payload of a cacheable index query.
pub(super) fn remember(
    identity: &QueryIdentity<'_>,
    conn: &Connection,
    project_filter: Option<&BTreeSet<String>>,
    records: Arc<Vec<AgentArtifactRecordWire>>,
    data_version: i64,
) -> Option<u64> {
    let key = identity.key()?;
    let Ok(signature_map) =
        query_signature_map(conn, identity.query, project_filter)
    else {
        return None;
    };
    // A concurrent commit between record selection and signature collection
    // must never tag old records with the new database identity.
    if current_data_version(conn).ok()? != data_version {
        return None;
    }
    Some(store(key, signature_map, records))
}

/// Connection-local counter of commits made by *other* connections. Unlike
/// WAL file metadata this also catches commits while our connection stays open.
pub(super) fn current_data_version(conn: &Connection) -> Result<i64, String> {
    conn.query_row("PRAGMA data_version", [], |row| row.get(0))
        .map_err(|error| error.to_string())
}
