//! Versioned SQLite read model over bead event stores.
//!
//! The cache holds the reduced snapshot (issues, dependency edges,
//! link provenance, shorthand catalog) plus the per-stream signatures
//! and merge frontier the next phase's incremental refresh resumes
//! from. Reads serve from these tables without replaying history;
//! any cache fault falls back to plain replay, so a read never fails
//! because of the cache.
//!
//! Concurrency rules:
//!
//! - Readers never take the bead flock.
//! - Cache writes run in one `IMMEDIATE` SQLite transaction guarded by a
//!   generation compare-and-swap: a writer commits only if the generation
//!   it started from is still current, and it never pairs newer
//!   signatures with older content (signatures and bytes come from the
//!   same open files; see `jsonl::StreamFileSignature`).
//! - A reader that cannot obtain the cache within a short bounded wait
//!   replays itself and serves that without writing. It never waits on
//!   another process's rebuild, and it never serves stale data: after a
//!   rebuild commits, a fresh sweep revalidates what was stored, and any
//!   concurrent change discards the file and serves a replay instead.
//!
//! Known limitation for `read-model-tail` to close: the rebuild reads the
//! store without the mutation lock, so a mutation landing mid-rebuild can
//! mix generations across streams. The post-commit sweep catches any
//! change that is still visible afterwards and discards the file; only a
//! mutation that lands and settles entirely inside the rebuild window
//! with no later trace could persist, and the next token change rebuilds
//! over it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

use crate::artifact_link::{ArtifactLinkOriginWire, BeadLinkDirectionWire};
use crate::bead::events::{
    event_operation_priority, reduce_parsed_event_streams_with_link_provenance,
    ActiveLinkProvenance, BeadEventStreamWire, StoredLinkIdentity,
};
use crate::bead::jsonl::{
    event_store_present, read_event_store_with_stream_signatures,
    StreamFileSignature,
};
use crate::bead::read_model::freshness::{
    freshness_token, now_ns, sweep_store_signatures, FileSignature,
    READ_MODEL_SWEEP_INTERVAL_SECS,
};
use crate::bead::read_model::location::read_model_cache_path_for_store;
use crate::bead::wire::{BeadError, IssueWire};

/// Schema version of the read-model SQLite file.
///
/// Any schema change means drop and rebuild: old files are never migrated.
pub const READ_MODEL_SCHEMA_VERSION: u32 = 1;

/// Reducer version the cached rows were reduced with.
///
/// Every change to merge, apply, or post-pass semantics must bump this
/// (see `reduce_event_streams`, the single validation point the rebuild
/// reduces through). A mismatch drops the file and rebuilds.
pub const READ_MODEL_REDUCER_VERSION: u32 = 1;

/// Cap on differing IDs reported by `--verify-cache`.
const VERIFY_DIFF_ID_LIMIT: usize = 50;

/// Bounded wait for a read-only cache open before replaying instead.
const SERVE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
/// Bounded wait for a rebuild transaction before replaying instead.
const REBUILD_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Wire schema version for [`BeadReadModelStatusWire`].
pub const BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION: u32 = 1;
/// Wire schema version for [`BeadReadModelVerifyWire`].
pub const BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION: u32 = 1;

/// Cache health line for `sase bead doctor`: location, size, generation,
/// and last sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadReadModelStatusWire {
    /// Schema version of this wire type.
    pub schema_version: u32,
    /// Cache file path, or `None` when the store has no git dir.
    pub location: Option<String>,
    /// True when an unchanged-store read would serve from the cache.
    pub fresh: bool,
    /// Why the cache is or is not servable.
    pub reason: String,
    /// Generation counter: increments on every committed rebuild.
    pub generation: u64,
    /// Cache file size in bytes, including WAL companions.
    pub size_bytes: u64,
    /// Seconds since the last full sweep, when the cache exists.
    pub last_sweep_age_secs: Option<i64>,
    /// Streams recorded in the cache.
    pub streams: usize,
    /// Issues recorded in the cache.
    pub issues: usize,
    /// Reducer version the cached rows were reduced with.
    pub reducer_version: u32,
    /// `sase_core` crate version that wrote the cache.
    pub crate_version: String,
}

/// Cache-vs-replay comparison for `sase bead doctor --verify-cache`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadReadModelVerifyWire {
    /// Schema version of this wire type.
    pub schema_version: u32,
    /// True when both sides were available and compared.
    pub compared: bool,
    /// True when the cache matches a forced full replay exactly.
    pub matched: bool,
    /// Issues in the forced replay.
    pub replay_issues: usize,
    /// Issues in the cache.
    pub cache_issues: usize,
    /// Up to [`VERIFY_DIFF_ID_LIMIT`] differing or one-sided issue IDs.
    pub differing_ids: Vec<String>,
    /// Why the comparison did or did not run, and what drifted.
    pub reason: String,
}

/// One servable snapshot: reduced issues plus link provenance.
#[derive(Debug, Clone)]
pub struct CachedStoreSnapshot {
    /// Reduced issues in replay order.
    pub issues: Vec<IssueWire>,
    /// Winning `LinkAdded` provenance by stored link identity.
    pub provenance: BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
}

/// Serve the store from the read model when possible.
///
/// Returns `Ok(Some(_))` on a cache hit and `Ok(None)` whenever the caller
/// should replay instead: no git dir (plain replay serves the read), a
/// legacy store, a missing or version-mismatched cache, a freshness miss
/// that a rebuild could not complete, or any cache fault. Genuine store
/// errors surface as `Err`: the replay the caller falls back to would fail
/// with the same error.
pub fn cached_store_snapshot(
    beads_dir: &Path,
) -> Result<Option<CachedStoreSnapshot>, BeadError> {
    let Some(cache_path) = read_model_cache_path_for_store(beads_dir) else {
        return Ok(None);
    };
    cached_store_snapshot_at(beads_dir, &cache_path)
}

/// Serve the store from the cache at an explicit path (tests).
///
/// The explicit path skips git-dir discovery; freshness and rebuild rules
/// are otherwise identical.
pub fn cached_store_snapshot_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<Option<CachedStoreSnapshot>, BeadError> {
    if !event_store_present(beads_dir) {
        return Ok(None);
    }
    rebuild_catch_store_errors(beads_dir, cache_path, false)
}

/// Report cache health for `sase bead doctor`. Never fails.
pub fn read_model_status(beads_dir: &Path) -> BeadReadModelStatusWire {
    match read_model_cache_path_for_store(beads_dir) {
        Some(cache_path) => read_model_status_at(beads_dir, &cache_path),
        None => BeadReadModelStatusWire {
            schema_version: BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
            location: None,
            fresh: false,
            reason: "no git dir: plain replay serves the read".to_string(),
            generation: 0,
            size_bytes: 0,
            last_sweep_age_secs: None,
            streams: 0,
            issues: 0,
            reducer_version: READ_MODEL_REDUCER_VERSION,
            crate_version: crate_version(),
        },
    }
}

/// Report cache health for an explicit cache path (tests).
pub fn read_model_status_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> BeadReadModelStatusWire {
    let location = Some(cache_path.display().to_string());
    let mut status = BeadReadModelStatusWire {
        schema_version: BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
        location,
        fresh: false,
        reason: "cache file missing: the next read rebuilds".to_string(),
        generation: 0,
        size_bytes: 0,
        last_sweep_age_secs: None,
        streams: 0,
        issues: 0,
        reducer_version: READ_MODEL_REDUCER_VERSION,
        crate_version: crate_version(),
    };
    if !event_store_present(beads_dir) {
        status.reason =
            "legacy store: plain replay serves the read".to_string();
        return status;
    }
    if !cache_path.is_file() {
        return status;
    }
    status.size_bytes = cache_file_size(cache_path);
    let connection = match open_read_only(cache_path, SERVE_BUSY_TIMEOUT) {
        Ok(connection) => connection,
        Err(reason) => {
            status.reason = reason;
            return status;
        }
    };
    let meta = match read_meta(&connection) {
        Ok(meta) => meta,
        Err(reason) => {
            status.reason = reason;
            return status;
        }
    };
    if let Some(reason) = version_mismatch_reason(&meta) {
        status.reason = reason;
        return status;
    }
    status.generation = meta.generation;
    status.last_sweep_age_secs = Some(
        now_ns()
            .saturating_sub(meta.last_sweep_ns)
            .div_euclid(1_000_000_000),
    );
    status.streams = count_rows(&connection, "streams");
    status.issues = count_rows(&connection, "issues");
    let token = match freshness_token(beads_dir) {
        Ok(token) => token,
        Err(error) => {
            status.reason =
                format!("freshness token unreadable: {}", error.message);
            return status;
        }
    };
    if token != meta.token {
        status.reason =
            "freshness token changed: the next read rebuilds".to_string();
        return status;
    }
    if now_ns().saturating_sub(meta.last_sweep_ns)
        > READ_MODEL_SWEEP_INTERVAL_SECS as i64 * 1_000_000_000
    {
        status.reason =
            "last full sweep is stale: the next read re-sweeps".to_string();
        return status;
    }
    match sweep_store_signatures(beads_dir) {
        Ok(current) => {
            let stored = load_stream_signatures(&connection);
            if stored == current.streams {
                status.fresh = true;
                status.reason =
                    "fresh: unchanged-store reads serve from the cache"
                        .to_string();
            } else {
                status.reason =
                    "streams changed: the next read rebuilds".to_string();
            }
        }
        Err(error) => {
            status.reason = format!("full sweep unreadable: {}", error.message);
        }
    }
    status
}

/// Compare a forced full replay against the cache. Never fails.
pub fn read_model_verify_cache(beads_dir: &Path) -> BeadReadModelVerifyWire {
    match read_model_cache_path_for_store(beads_dir) {
        Some(cache_path) => read_model_verify_cache_at(beads_dir, &cache_path),
        None => BeadReadModelVerifyWire {
            schema_version: BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION,
            compared: false,
            matched: false,
            replay_issues: 0,
            cache_issues: 0,
            differing_ids: vec![],
            reason: "no git dir: no cache to compare".to_string(),
        },
    }
}

/// Compare a forced full replay against an explicit cache path (tests).
pub fn read_model_verify_cache_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> BeadReadModelVerifyWire {
    let mut report = BeadReadModelVerifyWire {
        schema_version: BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION,
        compared: false,
        matched: false,
        replay_issues: 0,
        cache_issues: 0,
        differing_ids: vec![],
        reason: String::new(),
    };
    if !event_store_present(beads_dir) {
        report.reason = "legacy store: no cache to compare".to_string();
        return report;
    }
    if !cache_path.is_file() {
        report.reason =
            "cache file missing: the next read rebuilds".to_string();
        return report;
    }
    let (streams, _skipped) =
        match read_event_store_with_stream_signatures(beads_dir) {
            Ok((_, streams, _)) => (streams, 0),
            Err(error) => {
                report.reason =
                    format!("forced replay failed: {}", error.message);
                return report;
            }
        };
    let (replay_issues, replay_provenance) =
        match reduce_parsed_event_streams_with_link_provenance(&streams) {
            Ok(outcome) => outcome,
            Err(error) => {
                report.reason =
                    format!("forced replay failed: {}", error.message);
                return report;
            }
        };
    report.replay_issues = replay_issues.len();
    let connection = match open_read_only(cache_path, SERVE_BUSY_TIMEOUT) {
        Ok(connection) => connection,
        Err(reason) => {
            report.reason = reason;
            return report;
        }
    };
    match probe_cache(&connection, beads_dir) {
        Probe::Fresh => {}
        Probe::Stale => {
            report.reason =
                "cache is stale: the next read rebuilds".to_string();
            return report;
        }
        Probe::Unusable(reason) => {
            report.reason = format!(
                "cache is unreadable or version-mismatched ({reason}): the next read rebuilds"
            );
            return report;
        }
    }
    let cached = match load_snapshot(&connection) {
        Ok(snapshot) => snapshot,
        Err(reason) => {
            report.reason = format!(
                "cached rows unreadable ({reason}): the next read rebuilds"
            );
            return report;
        }
    };
    report.cache_issues = cached.issues.len();
    report.compared = true;
    let replay_value =
        serde_json::to_value(&replay_issues).unwrap_or(serde_json::Value::Null);
    let cached_value =
        serde_json::to_value(&cached.issues).unwrap_or(serde_json::Value::Null);
    if replay_value == cached_value
        && provenance_entries(&replay_provenance)
            == provenance_entries(&cached.provenance)
    {
        report.matched = true;
        report.reason =
            format!("cache matches replay: {} issues", report.replay_issues);
        return report;
    }
    report.reason = "drift: cache differs from replay".to_string();
    report.differing_ids = diff_issue_ids(&replay_issues, &cached.issues);
    report
}

/// Force a full rebuild at an explicit cache path (tests and repair).
///
/// Returns the rebuilt snapshot, or `Ok(None)` for stores without a
/// cacheable layout. Genuine store errors propagate.
pub fn rebuild_read_model_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<Option<CachedStoreSnapshot>, BeadError> {
    rebuild_catch_store_errors(beads_dir, cache_path, true)
}

fn crate_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Serve or rebuild, letting cache faults fall back to replay but store
/// errors fail. `force` skips the freshness checks and always replays.
fn rebuild_catch_store_errors(
    beads_dir: &Path,
    cache_path: &Path,
    force: bool,
) -> Result<Option<CachedStoreSnapshot>, BeadError> {
    if !event_store_present(beads_dir) {
        return Ok(None);
    }
    match rebuild(beads_dir, cache_path, force) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(Fault::Cache) => Ok(None),
        Err(Fault::Store(error)) => Err(error),
    }
}

/// A cache fault (fall back to replay) versus a store error (fail).
///
/// The fault message is consumed at each construction site's fallback:
/// routine fallbacks (missing file, busy cache, version drift) need no
/// message, while surprising faults surface through the status and
/// verify wires, which own their own reason strings.
enum Fault {
    Cache,
    Store(BeadError),
}

enum ServeOutcome {
    Hit(CachedStoreSnapshot),
    Unusable,
}

/// Open the cache read-only and load its rows without a freshness probe.
///
/// Used only when the caller just established freshness another way (a
/// lost generation race means the winner committed fresh rows). A
/// version mismatch drops to `Unusable`; unreadable rows drop the file
/// and report the fault.
fn open_and_serve(cache_path: &Path) -> Result<ServeOutcome, String> {
    let connection = open_read_only(cache_path, SERVE_BUSY_TIMEOUT)?;
    let meta = read_meta(&connection)?;
    if version_mismatch_reason(&meta).is_some() {
        return Ok(ServeOutcome::Unusable);
    }
    // Freshness is decided by the caller before serving, except here the
    // file may have changed under us: re-check the token cheaply is the
    // caller's job, so a direct serve trusts the stored rows.
    Ok(ServeOutcome::Hit(load_snapshot(&connection).inspect_err(
        |_| {
            drop_cache_file(cache_path);
        },
    )?))
}

/// Read-only freshness probe: no writes, no replay.
enum Probe {
    /// Stored rows describe the current store.
    Fresh,
    /// The store moved on; a rebuild is due.
    Stale,
    /// The file is missing, unreadable, or version-mismatched.
    Unusable(String),
}

/// Probe cache freshness without writing anything.
fn probe_cache(connection: &Connection, beads_dir: &Path) -> Probe {
    let meta = match read_meta(connection) {
        Ok(meta) => meta,
        Err(reason) => return Probe::Unusable(reason),
    };
    if let Some(reason) = version_mismatch_reason(&meta) {
        return Probe::Unusable(reason);
    }
    let token = match freshness_token(beads_dir) {
        Ok(token) => token,
        Err(_) => return Probe::Unusable("token unreadable".to_string()),
    };
    if token != meta.token {
        return Probe::Stale;
    }
    match sweep_store_signatures(beads_dir) {
        Ok(current) => {
            if current.manifest.is_none() {
                return Probe::Stale;
            }
            if meta.streams_known
                && load_stream_signatures(connection) == current.streams
            {
                Probe::Fresh
            } else {
                Probe::Stale
            }
        }
        Err(_) => Probe::Unusable("sweep unreadable".to_string()),
    }
}

/// Full freshness decision followed by serve or rebuild.
fn rebuild(
    beads_dir: &Path,
    cache_path: &Path,
    force: bool,
) -> Result<CachedStoreSnapshot, Fault> {
    ensure_schema(cache_path)?;
    let mut connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    if version_mismatch_reason(&meta).is_some() {
        drop(connection);
        drop_cache_file(cache_path);
        ensure_schema(cache_path)?;
        connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
    }
    if !force {
        // Token-only fast path: a matching token with a recent sweep
        // serves without touching per-stream metadata.
        let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
        let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
        let now = now_ns();
        if token == meta.token
            && now.saturating_sub(meta.last_sweep_ns)
                < READ_MODEL_SWEEP_INTERVAL_SECS as i64 * 1_000_000_000
        {
            return load_snapshot(&connection).map_err(|_| {
                drop(connection);
                drop_cache_file(cache_path);
                Fault::Cache
            });
        }
        match probe_cache(&connection, beads_dir) {
            Probe::Fresh => {
                update_token_and_sweep(&connection, &token, now)?;
                return load_snapshot(&connection).map_err(|_| {
                    drop(connection);
                    drop_cache_file(cache_path);
                    Fault::Cache
                });
            }
            Probe::Stale => {}
            Probe::Unusable(_) => {
                drop(connection);
                drop_cache_file(cache_path);
                ensure_schema(cache_path)?;
                connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
            }
        }
    }
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
    drop(connection);
    rebuild_from_replay(beads_dir, cache_path, &token, meta.generation)
}

/// Rebuild from a forced full replay and commit under generation CAS.
fn rebuild_from_replay(
    beads_dir: &Path,
    cache_path: &Path,
    token: &str,
    start_generation: u64,
) -> Result<CachedStoreSnapshot, Fault> {
    let (streams, signatures) =
        match read_event_store_with_stream_signatures(beads_dir) {
            Ok((_, streams, signatures)) => (streams, signatures),
            Err(error) => return Err(Fault::Store(error)),
        };
    let (issues, provenance) =
        match reduce_parsed_event_streams_with_link_provenance(&streams) {
            Ok(outcome) => outcome,
            Err(error) => return Err(Fault::Store(error)),
        };
    let snapshot = CachedStoreSnapshot { issues, provenance };
    let signature_map: BTreeMap<String, StreamFileSignature> =
        signatures.into_iter().collect();
    let frontier = merge_frontier(&streams);
    let connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(|_| Fault::Cache)?;
    let outcome = write_snapshot_in_txn(
        &connection,
        &snapshot,
        &signature_map,
        token,
        &frontier,
        start_generation,
    );
    match outcome {
        Ok(()) => {
            connection
                .execute_batch("COMMIT")
                .map_err(|_| Fault::Cache)?;
        }
        Err(WriteFault::CasLost) => {
            let _ = connection.execute_batch("ROLLBACK");
            // Another writer committed first: serve what it wrote
            // instead of replaying.
            drop(connection);
            return match open_and_serve(cache_path) {
                Ok(ServeOutcome::Hit(fresh)) => Ok(fresh),
                _ => Err(Fault::Cache),
            };
        }
        Err(WriteFault::Other) => {
            let _ = connection.execute_batch("ROLLBACK");
            drop(connection);
            drop_cache_file(cache_path);
            return Err(Fault::Cache);
        }
    }
    drop(connection);
    // A concurrent mutation may have landed mid-rebuild. Re-sweep: any
    // visible change discards the file and serves a replay instead, so
    // the cache never serves stale data.
    match sweep_store_signatures(beads_dir) {
        Ok(current) => {
            let connection = open_read_only(cache_path, SERVE_BUSY_TIMEOUT)
                .map_err(|_| Fault::Cache)?;
            if load_stream_signatures(&connection) != current.streams {
                drop(connection);
                drop_cache_file(cache_path);
                return Err(Fault::Cache);
            }
        }
        Err(_) => {
            drop_cache_file(cache_path);
            return Err(Fault::Cache);
        }
    }
    Ok(snapshot)
}

struct CacheMeta {
    token: String,
    last_sweep_ns: i64,
    generation: u64,
    schema_version: u32,
    reducer_version: u32,
    crate_version: String,
    streams_known: bool,
}

fn read_meta(connection: &Connection) -> Result<CacheMeta, String> {
    let mut values: BTreeMap<String, String> = BTreeMap::new();
    let mut statement = connection
        .prepare("SELECT key, value FROM meta")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        values.insert(key, value);
    }
    Ok(CacheMeta {
        token: values.get("token").cloned().unwrap_or_default(),
        last_sweep_ns: values
            .get("last_sweep_ns")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        generation: values
            .get("generation")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        schema_version: values
            .get("schema_version")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        reducer_version: values
            .get("reducer_version")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        crate_version: values.get("crate_version").cloned().unwrap_or_default(),
        streams_known: values.contains_key("streams_known"),
    })
}

/// Reason the cached rows must be dropped, or `None` when versions match.
fn version_mismatch_reason(meta: &CacheMeta) -> Option<String> {
    if meta.schema_version != READ_MODEL_SCHEMA_VERSION {
        return Some(format!(
            "schema version {} != {READ_MODEL_SCHEMA_VERSION}: the next read rebuilds",
            meta.schema_version
        ));
    }
    if meta.reducer_version != READ_MODEL_REDUCER_VERSION {
        return Some(format!(
            "reducer version {} != {READ_MODEL_REDUCER_VERSION}: the next read rebuilds",
            meta.reducer_version
        ));
    }
    if meta.crate_version != crate_version() {
        return Some(format!(
            "crate version {} != {}: the next read rebuilds",
            meta.crate_version,
            crate_version()
        ));
    }
    None
}

fn update_token_and_sweep(
    connection: &Connection,
    token: &str,
    now_ns: i64,
) -> Result<(), Fault> {
    connection
        .execute("UPDATE meta SET value = ?1 WHERE key = 'token'", [token])
        .map_err(|_| Fault::Cache)?;
    connection
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = 'last_sweep_ns'",
            [now_ns.to_string()],
        )
        .map_err(|_| Fault::Cache)?;
    Ok(())
}

/// Why a snapshot write failed: a lost generation race versus a fault.
enum WriteFault {
    /// Another rebuild committed first; serve what it wrote.
    CasLost,
    /// Any other write fault; the file is dropped and reads replay.
    Other,
}

/// Persist one snapshot plus signatures and frontier in the open txn.
///
/// The generation compare-and-swap commits only when the generation the
/// caller started from is still current, so two racing rebuilds never
/// pair newer signatures with older content.
fn write_snapshot_in_txn(
    connection: &Connection,
    snapshot: &CachedStoreSnapshot,
    signatures: &BTreeMap<String, StreamFileSignature>,
    token: &str,
    frontier: &str,
    start_generation: u64,
) -> Result<(), WriteFault> {
    for table in [
        "issues",
        "edges",
        "link_provenance",
        "suffix_catalog",
        "streams",
    ] {
        connection
            .execute(&format!("DELETE FROM {table}"), [])
            .map_err(|_| WriteFault::Other)?;
    }
    let parent_of: BTreeMap<&str, &str> = snapshot
        .issues
        .iter()
        .filter_map(|issue| {
            issue
                .parent_id
                .as_deref()
                .map(|parent| (issue.id.as_str(), parent))
        })
        .collect();
    let mut issue_stmt = connection
        .prepare(
            "INSERT INTO issues (id, position, row, status, issue_type, tier, parent, stream, created_at, external_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .map_err(|_| WriteFault::Other)?;
    let mut edge_stmt = connection
        .prepare("INSERT INTO edges (src, dst, kind) VALUES (?1, ?2, ?3)")
        .map_err(|_| WriteFault::Other)?;
    let mut suffix_stmt = connection
        .prepare(
            "INSERT INTO suffix_catalog (suffix, issue_id) VALUES (?1, ?2)",
        )
        .map_err(|_| WriteFault::Other)?;
    for (position, issue) in snapshot.issues.iter().enumerate() {
        let row =
            serde_json::to_string(issue).map_err(|_| WriteFault::Other)?;
        issue_stmt
            .execute(rusqlite::params![
                issue.id,
                position as i64,
                row,
                wire_string(&issue.status),
                wire_string(&issue.issue_type),
                issue.tier.as_ref().map(wire_string).unwrap_or_default(),
                issue.parent_id.clone().unwrap_or_default(),
                lineage_root(&issue.id, &parent_of),
                issue.created_at,
                issue.external_ref,
            ])
            .map_err(|_| WriteFault::Other)?;
        if let Some(parent) = issue.parent_id.as_deref() {
            edge_stmt
                .execute(rusqlite::params![issue.id, parent, "parent"])
                .map_err(|_| WriteFault::Other)?;
        }
        for dependency in &issue.dependencies {
            edge_stmt
                .execute(rusqlite::params![
                    issue.id,
                    dependency.depends_on_id,
                    "depends_on"
                ])
                .map_err(|_| WriteFault::Other)?;
        }
        suffix_stmt
            .execute(rusqlite::params![id_suffix(&issue.id), issue.id])
            .map_err(|_| WriteFault::Other)?;
    }
    drop(issue_stmt);
    drop(edge_stmt);
    drop(suffix_stmt);
    let mut link_stmt = connection
        .prepare(
            "INSERT INTO link_provenance (target_ref, source_issue_id, relation, description, origin, direction, uses, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .map_err(|_| WriteFault::Other)?;
    // The map key duplicates the row fields and stays in memory only.
    for provenance in snapshot.provenance.values() {
        link_stmt
            .execute(rusqlite::params![
                provenance.target_ref,
                provenance.source_issue_id,
                provenance.relation,
                provenance.description,
                wire_string(&provenance.origin),
                wire_string(&provenance.direction),
                provenance.uses as i64,
                provenance.actor,
                provenance.timestamp,
            ])
            .map_err(|_| WriteFault::Other)?;
    }
    drop(link_stmt);
    let mut stream_stmt = connection
        .prepare(
            "INSERT INTO streams (stream_id, size, mtime_ns, inode, byte_len, content_hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(|_| WriteFault::Other)?;
    for (stream_id, signature) in signatures {
        stream_stmt
            .execute(rusqlite::params![
                stream_id,
                signature.size as i64,
                signature.mtime_ns,
                signature.inode as i64,
                signature.size as i64,
                signature.content_hash,
            ])
            .map_err(|_| WriteFault::Other)?;
    }
    drop(stream_stmt);
    let now = now_ns().to_string();
    for (key, value) in [
        ("token", token.to_string()),
        ("last_sweep_ns", now),
        ("frontier", frontier.to_string()),
    ] {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![key, value],
            )
            .map_err(|_| WriteFault::Other)?;
    }
    connection
        .execute(
            "INSERT INTO meta (key, value) VALUES ('streams_known', '1') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )
        .map_err(|_| WriteFault::Other)?;
    let new_generation = start_generation.saturating_add(1).to_string();
    let changed = connection
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = 'generation' AND value = ?2",
            rusqlite::params![new_generation, start_generation.to_string()],
        )
        .map_err(|_| WriteFault::Other)?;
    if changed != 1 {
        return Err(WriteFault::CasLost);
    }
    Ok(())
}

/// Load the snapshot in replay order.
fn load_snapshot(
    connection: &Connection,
) -> Result<CachedStoreSnapshot, String> {
    let mut statement = connection
        .prepare("SELECT row FROM issues ORDER BY position")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let row = row.map_err(|error| error.to_string())?;
        let issue: IssueWire = serde_json::from_str(&row).map_err(|error| {
            format!("cached issue row is not valid: {error}")
        })?;
        issues.push(issue);
    }
    let mut statement = connection
        .prepare(
            "SELECT target_ref, source_issue_id, relation, description, origin, direction, uses, actor, timestamp FROM link_provenance",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })
        .map_err(|error| error.to_string())?;
    let mut provenance = BTreeMap::new();
    for row in rows {
        let (
            target_ref,
            source_issue_id,
            relation,
            description,
            origin,
            direction,
            uses,
            actor,
            timestamp,
        ) = row.map_err(|error| error.to_string())?;
        let origin: ArtifactLinkOriginWire =
            serde_json::from_value(serde_json::Value::String(origin))
                .map_err(|error| error.to_string())?;
        let direction: BeadLinkDirectionWire =
            serde_json::from_value(serde_json::Value::String(direction))
                .map_err(|error| error.to_string())?;
        let identity = StoredLinkIdentity {
            source_issue_id: source_issue_id.clone(),
            relation: relation.clone(),
            target_ref: target_ref.clone(),
            direction,
        };
        provenance.insert(
            identity,
            ActiveLinkProvenance {
                source_issue_id,
                target_ref,
                relation,
                description,
                origin,
                direction,
                uses: u64::try_from(uses).unwrap_or(0),
                actor,
                timestamp,
            },
        );
    }
    Ok(CachedStoreSnapshot { issues, provenance })
}

fn load_stream_signatures(
    connection: &Connection,
) -> Vec<(String, FileSignature)> {
    let mut statement = match connection.prepare(
        "SELECT stream_id, size, mtime_ns, inode FROM streams ORDER BY stream_id",
    ) {
        Ok(statement) => statement,
        Err(_) => return vec![],
    };
    let rows = match statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    }) {
        Ok(rows) => rows,
        Err(_) => return vec![],
    };
    let mut signatures = vec![];
    for row in rows {
        let Ok((stream_id, size, mtime_ns, inode)) = row else {
            continue;
        };
        signatures.push((
            stream_id,
            FileSignature {
                size: u64::try_from(size).unwrap_or(0),
                mtime_ns,
                inode: u64::try_from(inode).unwrap_or(0),
            },
        ));
    }
    signatures
}

/// Largest merge key across all reduced events: the persisted frontier.
///
/// Uses exactly the `(timestamp, operation priority, event_id)` ordering
/// the k-way merge sorts by, so `read-model-tail` can prove every appended
/// event sorts after it.
fn merge_frontier(streams: &[BeadEventStreamWire]) -> String {
    let mut frontier: Option<(String, usize, String)> = None;
    for stream in streams {
        for event in &stream.events {
            let key = (
                event.timestamp.clone(),
                event_operation_priority(event.operation),
                event.event_id.clone(),
            );
            if frontier.as_ref().is_none_or(|current| key > *current) {
                frontier = Some(key);
            }
        }
    }
    match frontier {
        Some((timestamp, priority, event_id)) => serde_json::json!({
            "timestamp": timestamp,
            "priority": priority,
            "event_id": event_id,
        })
        .to_string(),
        None => serde_json::json!({
            "timestamp": "",
            "priority": 0,
            "event_id": "",
        })
        .to_string(),
    }
}

/// Root of an issue's lineage: the stream that canonically holds it.
///
/// A root ID owns `events/streams/<id>.jsonl` and a phase lives in its
/// parent plan's stream, so walking the parent chain to the top derives
/// the stream without parsing events. Cycles fall back to the issue
/// itself.
fn lineage_root(id: &str, parent_of: &BTreeMap<&str, &str>) -> String {
    let mut root = id;
    let mut seen = BTreeSet::from([id]);
    while let Some(parent) = parent_of.get(root) {
        if !seen.insert(parent) {
            break;
        }
        root = parent;
    }
    root.to_string()
}

/// Shorthand suffix exactly as `resolve_issue_id_in_issues` matches it.
fn id_suffix(id: &str) -> &str {
    id.rsplit_once('-').map(|(_, suffix)| suffix).unwrap_or(id)
}

fn wire_string(value: &impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn provenance_entries(
    provenance: &BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
) -> Vec<serde_json::Value> {
    let mut entries: Vec<serde_json::Value> = provenance
        .iter()
        .map(|(identity, row)| {
            serde_json::json!({
                "source_issue_id": identity.source_issue_id,
                "relation": identity.relation,
                "target_ref": identity.target_ref,
                "direction": wire_string(&identity.direction),
                "description": row.description,
                "origin": wire_string(&row.origin),
                "uses": row.uses,
                "actor": row.actor,
                "timestamp": row.timestamp,
            })
        })
        .collect();
    entries.sort_by(|left, right| {
        serde_json::to_string(left)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(right).unwrap_or_default())
    });
    entries
}

fn diff_issue_ids(replay: &[IssueWire], cached: &[IssueWire]) -> Vec<String> {
    let replay_rows: BTreeMap<&str, String> = replay
        .iter()
        .map(|issue| {
            (
                issue.id.as_str(),
                serde_json::to_string(issue).unwrap_or_default(),
            )
        })
        .collect();
    let cached_rows: BTreeMap<&str, String> = cached
        .iter()
        .map(|issue| {
            (
                issue.id.as_str(),
                serde_json::to_string(issue).unwrap_or_default(),
            )
        })
        .collect();
    let mut differing: Vec<String> = replay_rows
        .keys()
        .filter(|id| {
            replay_rows[*id] != *cached_rows.get(*id).unwrap_or(&String::new())
        })
        .chain(
            cached_rows
                .keys()
                .filter(|id| !replay_rows.contains_key(*id)),
        )
        .map(|id| (*id).to_string())
        .collect();
    differing.sort();
    differing.truncate(VERIFY_DIFF_ID_LIMIT);
    differing
}

fn ensure_schema(cache_path: &Path) -> Result<(), Fault> {
    if cache_path.is_file() {
        return Ok(());
    }
    if let Some(parent) = cache_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|_| Fault::Cache)?;
        }
    }
    let connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
    connection
        .execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS streams (
                stream_id TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                inode INTEGER NOT NULL,
                byte_len INTEGER NOT NULL,
                content_hash TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS issues (
                id TEXT PRIMARY KEY,
                position INTEGER NOT NULL,
                row TEXT NOT NULL,
                status TEXT NOT NULL,
                issue_type TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT '',
                parent TEXT NOT NULL DEFAULT '',
                stream TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                external_ref TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS issues_status ON issues(status);
            CREATE INDEX IF NOT EXISTS issues_type ON issues(issue_type);
            CREATE INDEX IF NOT EXISTS issues_tier ON issues(tier);
            CREATE INDEX IF NOT EXISTS issues_parent ON issues(parent);
            CREATE INDEX IF NOT EXISTS issues_stream ON issues(stream);
            CREATE INDEX IF NOT EXISTS issues_created_at ON issues(created_at);
            CREATE INDEX IF NOT EXISTS issues_external_ref ON issues(external_ref);
            CREATE TABLE IF NOT EXISTS edges (
                src TEXT NOT NULL,
                dst TEXT NOT NULL,
                kind TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS edges_src ON edges(src);
            CREATE INDEX IF NOT EXISTS edges_dst ON edges(dst);
            CREATE TABLE IF NOT EXISTS link_provenance (
                target_ref TEXT NOT NULL,
                source_issue_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                description TEXT NOT NULL,
                origin TEXT NOT NULL,
                direction TEXT NOT NULL,
                uses INTEGER NOT NULL,
                actor TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS link_provenance_target ON link_provenance(target_ref);
            CREATE TABLE IF NOT EXISTS suffix_catalog (
                suffix TEXT NOT NULL,
                issue_id TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS suffix_catalog_suffix ON suffix_catalog(suffix);
            ",
        )
        .map_err(|_| Fault::Cache)?;
    for (key, value) in [
        ("schema_version", READ_MODEL_SCHEMA_VERSION.to_string()),
        ("reducer_version", READ_MODEL_REDUCER_VERSION.to_string()),
        ("crate_version", crate_version()),
        ("token", String::new()),
        ("last_sweep_ns", "0".to_string()),
        ("frontier", String::new()),
        ("generation", "0".to_string()),
    ] {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                rusqlite::params![key, value],
            )
            .map_err(|_| Fault::Cache)?;
    }
    Ok(())
}

fn open_read_only(
    cache_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, String> {
    let connection = Connection::open_with_flags(
        cache_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|error| format!("read-model cache unreadable: {error}"))?;
    connection
        .busy_timeout(busy_timeout)
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

fn open_read_write(
    cache_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, Fault> {
    let connection = Connection::open(cache_path).map_err(|_| Fault::Cache)?;
    connection
        .busy_timeout(busy_timeout)
        .map_err(|_| Fault::Cache)?;
    connection
        .execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")
        .map_err(|_| Fault::Cache)?;
    Ok(connection)
}

fn count_rows(connection: &Connection, table: &str) -> usize {
    let mut statement =
        match connection.prepare(&format!("SELECT COUNT(*) FROM {table}")) {
            Ok(statement) => statement,
            Err(_) => return 0,
        };
    statement
        .query_row([], |row| row.get::<_, i64>(0))
        .map(|count| usize::try_from(count).unwrap_or(0))
        .unwrap_or(0)
}

fn cache_file_size(cache_path: &Path) -> u64 {
    let mut size = 0;
    for companion in [
        cache_path.to_path_buf(),
        cache_path.with_extension("sqlite-wal"),
        cache_path.with_extension("sqlite-shm"),
        cache_path.with_extension("sqlite-journal"),
    ] {
        size += fs::metadata(companion).map(|meta| meta.len()).unwrap_or(0);
    }
    size
}

/// Delete the cache file and its WAL companions: derived data only.
fn drop_cache_file(cache_path: &Path) {
    for companion in [
        cache_path.to_path_buf(),
        cache_path.with_extension("sqlite-wal"),
        cache_path.with_extension("sqlite-shm"),
        cache_path.with_extension("sqlite-journal"),
    ] {
        let _ = fs::remove_file(companion);
    }
}
