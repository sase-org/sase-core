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
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

use crate::artifact_link::{ArtifactLinkOriginWire, BeadLinkDirectionWire};
use crate::bead::events::{
    event_operation_priority, reduce_parsed_event_streams_with_link_provenance,
    ActiveLinkProvenance, BeadEventStoreManifestWire, BeadEventStreamWire,
    StoredLinkIdentity,
};
use crate::bead::jsonl::{
    event_store_present, read_event_store_with_stream_signatures,
    StreamFileSignature,
};
use crate::bead::read_model::freshness::{
    freshness_token, now_ns, sweep_store_signatures, FileSignature,
    StoreSignatures, READ_MODEL_SWEEP_INTERVAL_SECS,
};
use crate::bead::read_model::location::read_model_cache_path_for_store;
use crate::bead::wire::{BeadError, IssueWire};

/// Schema version of the read-model SQLite file.
///
/// Any schema change means drop and rebuild: old files are never migrated.
///
/// Version 2 adds the `task_type`, `plus_one`, and `is_flag` issue columns
/// behind the `read-model-queries` indexed list filters and index
/// aggregates.
///
/// Version 3 adds disposable `alloc_top`/`alloc_child` allocation metadata
/// for index-only ID minting (`read-model-mutations`); older caches rebuild.
pub const READ_MODEL_SCHEMA_VERSION: u32 = 3;

/// Reducer version the cached rows were reduced with.
///
/// Every change to merge, apply, or post-pass semantics must bump this
/// (see `reduce_event_streams`, the single validation point the rebuild
/// reduces through). A mismatch drops the file and rebuilds.
pub const READ_MODEL_REDUCER_VERSION: u32 = 1;

/// Cap on differing IDs reported by `--verify-cache`.
const VERIFY_DIFF_ID_LIMIT: usize = 50;

/// Bounded wait for a read-only cache open before replaying instead.
pub(super) const SERVE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
/// Bounded wait for a rebuild transaction before replaying instead.
pub(super) const REBUILD_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Wire schema version for [`BeadReadModelStatusWire`].
///
/// Version 2 adds the serve/tail/rebuild outcome counters and the last
/// refresh description (`read-model-tail` telemetry).
pub const BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION: u32 = 2;
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
    /// Cache-hit serves since the cache file was created.
    pub serve_count: u64,
    /// Incremental tail commits since the cache file was created.
    pub tail_count: u64,
    /// Full-rebuild commits since the cache file was created.
    pub rebuild_count: u64,
    /// How the cache content last changed: `""`, `"tail"`, or `"rebuild"`.
    pub last_refresh: String,
    /// Why the last refresh ran: tail stats or the rebuild reason.
    pub last_refresh_reason: String,
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
        None => empty_status(
            None,
            "no git dir: plain replay serves the read".to_string(),
        ),
    }
}

/// Report cache health for an explicit cache path (tests).
pub fn read_model_status_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> BeadReadModelStatusWire {
    let location = Some(cache_path.display().to_string());
    let mut status = empty_status(
        location,
        "cache file missing: the next read rebuilds".to_string(),
    );
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
    status.serve_count = meta.outcome_serve;
    status.tail_count = meta.outcome_tail;
    status.rebuild_count = meta.outcome_rebuild;
    status.last_refresh = meta.last_refresh.clone();
    status.last_refresh_reason = meta.last_refresh_reason.clone();
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

/// A never-fresh status report: no cache to describe.
fn empty_status(
    location: Option<String>,
    reason: String,
) -> BeadReadModelStatusWire {
    BeadReadModelStatusWire {
        schema_version: BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
        location,
        fresh: false,
        reason,
        generation: 0,
        size_bytes: 0,
        last_sweep_age_secs: None,
        streams: 0,
        issues: 0,
        reducer_version: READ_MODEL_REDUCER_VERSION,
        crate_version: crate_version(),
        serve_count: 0,
        tail_count: 0,
        rebuild_count: 0,
        last_refresh: String::new(),
        last_refresh_reason: String::new(),
    }
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
pub(super) enum Fault {
    Cache,
    Store(BeadError),
}

pub(super) enum ServeOutcome {
    Hit(CachedStoreSnapshot),
    Unusable,
}

/// Open the cache read-only and load its rows without a freshness probe.
///
/// Used only when the caller just established freshness another way (a
/// lost generation race means the winner committed fresh rows). A
/// version mismatch drops to `Unusable`; unreadable rows drop the file
/// and report the fault.
pub(super) fn open_and_serve(
    cache_path: &Path,
) -> Result<ServeOutcome, String> {
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
    /// The store moved on; a refresh (tail or rebuild) is due.
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
    let sweep = match sweep_store_signatures(beads_dir) {
        Ok(sweep) => sweep,
        Err(_) => return Probe::Unusable("sweep unreadable".to_string()),
    };
    probe_with(connection, &meta, &token, &sweep)
}

/// Probe freshness against already-computed inputs.
///
/// `rebuild` sweeps once and shares the result with the tail refresh, so
/// the change path pays one directory listing instead of two.
fn probe_with(
    connection: &Connection,
    meta: &CacheMeta,
    token: &str,
    sweep: &StoreSignatures,
) -> Probe {
    if *token != meta.token {
        return Probe::Stale;
    }
    if sweep.manifest.is_none() {
        return Probe::Stale;
    }
    if meta.streams_known && load_stream_signatures(connection) == sweep.streams
    {
        Probe::Fresh
    } else {
        Probe::Stale
    }
}

/// Full freshness decision followed by serve, tail apply, or rebuild.
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
    backfill_meta_keys(&connection)?;
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
            let snapshot = match load_snapshot(&connection) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    drop(connection);
                    drop_cache_file(cache_path);
                    return Err(Fault::Cache);
                }
            };
            note_serve_outcome(&connection);
            return Ok(snapshot);
        }
        let sweep = match sweep_store_signatures(beads_dir) {
            Ok(sweep) => sweep,
            Err(_) => {
                drop(connection);
                drop_cache_file(cache_path);
                ensure_schema(cache_path)?;
                connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
                return rebuild_cold(beads_dir, cache_path, connection);
            }
        };
        match probe_with(&connection, &meta, &token, &sweep) {
            Probe::Fresh => {
                update_token_and_sweep(&connection, &token, now)?;
                let snapshot = match load_snapshot(&connection) {
                    Ok(snapshot) => snapshot,
                    Err(_) => {
                        drop(connection);
                        drop_cache_file(cache_path);
                        return Err(Fault::Cache);
                    }
                };
                note_serve_outcome(&connection);
                return Ok(snapshot);
            }
            Probe::Stale => {
                return match refresh_changed_store(
                    beads_dir,
                    cache_path,
                    connection,
                    &meta,
                    &token,
                    &sweep,
                    RefreshFinish::Snapshot,
                ) {
                    Ok(Some(snapshot)) => Ok(snapshot),
                    Ok(None) => Err(Fault::Cache),
                    Err(fault) => Err(fault),
                };
            }
            Probe::Unusable(_) => {
                drop(connection);
                drop_cache_file(cache_path);
                ensure_schema(cache_path)?;
                connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
                return rebuild_cold(beads_dir, cache_path, connection);
            }
        }
    }
    rebuild_cold(beads_dir, cache_path, connection)
}

/// Ensure the cache describes the live store without loading any rows.
///
/// Returns the cache path when the next query may serve from it and `None`
/// whenever the caller should replay instead: no git dir (plain replay
/// serves the read), a legacy store, or any cache fault. Genuine store
/// errors surface as `Err`, exactly as the replay the caller falls back to
/// would fail with them.
///
/// Fresh stores return without deserializing a single row: the token-only
/// and sweep-confirmed paths record their serve telemetry and leave the
/// rows on disk for the caller's indexed query. A store the sweep finds
/// changed refreshes through the same tail-or-rebuild path `rebuild` uses
/// (paying one full snapshot load), so the indexed query after it is warm.
pub fn ensure_cache_ready(
    beads_dir: &Path,
) -> Result<Option<PathBuf>, BeadError> {
    let Some(cache_path) = read_model_cache_path_for_store(beads_dir) else {
        return Ok(None);
    };
    ensure_cache_ready_at(beads_dir, &cache_path)
        .map(|ready| ready.then_some(cache_path))
}

/// Ensure the cache at an explicit path is fresh (tests and queries).
///
/// The explicit path skips git-dir discovery; freshness and refresh rules
/// are otherwise identical to [`ensure_cache_ready`].
pub fn ensure_cache_ready_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<bool, BeadError> {
    if !event_store_present(beads_dir) {
        return Ok(false);
    }
    match ensure_fresh(beads_dir, cache_path) {
        Ok(ready) => Ok(ready),
        Err(Fault::Cache) => Ok(false),
        Err(Fault::Store(error)) => Err(error),
    }
}

/// Ensure the cache is fresh for the mutation path.
///
/// Mutations run under the `beads.db` flock and must never trust the 60 s
/// token-only fast path: an out-of-band change inside the window would
/// otherwise be invisible. This forces the full signature sweep every
/// time, then shares the same tail-or-rebuild refresh the read path uses,
/// establishing the consistent baseline (version, generation, token,
/// signatures, config, frontier) the write-through commits against.
pub fn ensure_cache_ready_for_mutation(
    beads_dir: &Path,
) -> Result<Option<PathBuf>, BeadError> {
    let Some(cache_path) = read_model_cache_path_for_store(beads_dir) else {
        return Ok(None);
    };
    ensure_cache_ready_for_mutation_at(beads_dir, &cache_path)
        .map(|ready| ready.then_some(cache_path))
}

/// Ensure the cache at an explicit path is fresh for mutations (tests).
///
/// The explicit path skips git-dir discovery; the forced-sweep rule is
/// otherwise identical to [`ensure_cache_ready_for_mutation`].
pub fn ensure_cache_ready_for_mutation_at(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<bool, BeadError> {
    if !event_store_present(beads_dir) {
        return Ok(false);
    }
    match ensure_fresh_forced(beads_dir, cache_path) {
        Ok(ready) => Ok(ready),
        Err(Fault::Cache) => Ok(false),
        Err(Fault::Store(error)) => Err(error),
    }
}

/// Freshness decision without a snapshot load.
///
/// Mirrors [`rebuild`]'s decision tree — token-only fast path, sweep probe,
/// tail-or-rebuild refresh, version-mismatch drop — but the fresh paths
/// return without touching the `issues` table, so warm indexed queries
/// never pay the full-snapshot deserialize. The stale path delegates to
/// the existing refresh (which loads once) and reports warm afterwards.
/// True when the cache file is provably not a SQLite database.
///
/// A foreign or truncated file at the cache path can never heal by
/// retrying reads against it, so the freshness paths drop and rebuild it
/// exactly like a version mismatch. A healthy database that merely failed
/// a read (contention, torn view) keeps the fail-open path: only the
/// missing SQLite header magic proves the file itself is the fault.
fn cache_file_is_not_a_database(cache_path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(cache_path) else {
        return false;
    };
    let mut header = [0u8; 16];
    if file.read_exact(&mut header).is_err() {
        return true;
    }
    header != *b"SQLite format 3\0"
}

/// Drop a provably non-database cache file and rebuild it cold.
fn rebuild_not_a_database(
    beads_dir: &Path,
    cache_path: &Path,
    connection: Connection,
) -> Result<bool, Fault> {
    drop(connection);
    drop_cache_file(cache_path);
    ensure_schema(cache_path)?;
    let connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
    rebuild_cold_discard(beads_dir, cache_path, connection)
}

/// Open the cache for a freshness pass, healing a provably non-database
/// file first.
///
/// The WAL pragma in `open_read_write` already fails on a foreign file,
/// so the heal must happen at open time, not only at the meta read: such
/// a file can never heal by retrying against it. Anything else keeps the
/// fail-open path.
fn open_or_heal_for_freshness(cache_path: &Path) -> Result<Connection, Fault> {
    match open_read_write(cache_path, REBUILD_BUSY_TIMEOUT) {
        Ok(connection) => Ok(connection),
        Err(_) if cache_file_is_not_a_database(cache_path) => {
            drop_cache_file(cache_path);
            ensure_schema(cache_path)?;
            open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)
        }
        Err(fault) => Err(fault),
    }
}

fn ensure_fresh(beads_dir: &Path, cache_path: &Path) -> Result<bool, Fault> {
    if ensure_schema(cache_path).is_err() {
        return Err(Fault::Cache);
    }
    let mut connection = open_or_heal_for_freshness(cache_path)?;
    let meta = match read_meta(&connection) {
        Ok(meta) => meta,
        Err(_) if cache_file_is_not_a_database(cache_path) => {
            return rebuild_not_a_database(beads_dir, cache_path, connection);
        }
        Err(_) => return Err(Fault::Cache),
    };
    if version_mismatch_reason(&meta).is_some() {
        drop(connection);
        drop_cache_file(cache_path);
        ensure_schema(cache_path)?;
        connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
        return rebuild_cold_discard(beads_dir, cache_path, connection);
    }
    backfill_meta_keys(&connection)?;
    // Token-only fast path: a matching token with a recent sweep serves
    // without touching per-stream metadata, exactly as in `rebuild`.
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
    let now = now_ns();
    if token == meta.token
        && now.saturating_sub(meta.last_sweep_ns)
            < READ_MODEL_SWEEP_INTERVAL_SECS as i64 * 1_000_000_000
    {
        note_serve_outcome(&connection);
        return Ok(true);
    }
    let sweep = match sweep_store_signatures(beads_dir) {
        Ok(sweep) => sweep,
        Err(_) => {
            drop(connection);
            drop_cache_file(cache_path);
            ensure_schema(cache_path)?;
            connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
            return rebuild_cold_discard(beads_dir, cache_path, connection);
        }
    };
    match probe_with(&connection, &meta, &token, &sweep) {
        Probe::Fresh => {
            update_token_and_sweep(&connection, &token, now)?;
            note_serve_outcome(&connection);
            Ok(true)
        }
        Probe::Stale => {
            // The refresh runs the tail-or-rebuild path with the sweep
            // just taken; the indexed query after it loads only the rows
            // it needs, so no snapshot is deserialized here.
            match refresh_changed_store(
                beads_dir,
                cache_path,
                connection,
                &meta,
                &token,
                &sweep,
                RefreshFinish::Readiness,
            ) {
                Ok(_) => Ok(true),
                Err(fault) => Err(fault),
            }
        }
        Probe::Unusable(_) => {
            drop(connection);
            drop_cache_file(cache_path);
            ensure_schema(cache_path)?;
            connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
            rebuild_cold_discard(beads_dir, cache_path, connection)
        }
    }
}

/// Forced-sweep freshness without a snapshot load.
///
/// Same as [`ensure_fresh`] minus the token-only fast path: every call
/// runs the full signature sweep, so a mutation inside the flock always
/// observes out-of-band changes even within the 60 s window.
fn ensure_fresh_forced(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<bool, Fault> {
    if ensure_schema(cache_path).is_err() {
        return Err(Fault::Cache);
    }
    let mut connection = open_or_heal_for_freshness(cache_path)?;
    let meta = match read_meta(&connection) {
        Ok(meta) => meta,
        Err(_) if cache_file_is_not_a_database(cache_path) => {
            return rebuild_not_a_database(beads_dir, cache_path, connection);
        }
        Err(_) => return Err(Fault::Cache),
    };
    if version_mismatch_reason(&meta).is_some() {
        drop(connection);
        drop_cache_file(cache_path);
        ensure_schema(cache_path)?;
        connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
        return rebuild_cold_discard(beads_dir, cache_path, connection);
    }
    backfill_meta_keys(&connection)?;
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
    let now = now_ns();
    let sweep = match sweep_store_signatures(beads_dir) {
        Ok(sweep) => sweep,
        Err(_) => {
            drop(connection);
            drop_cache_file(cache_path);
            ensure_schema(cache_path)?;
            connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
            return rebuild_cold_discard(beads_dir, cache_path, connection);
        }
    };
    match probe_with(&connection, &meta, &token, &sweep) {
        Probe::Fresh => {
            update_token_and_sweep(&connection, &token, now)?;
            note_serve_outcome(&connection);
            Ok(true)
        }
        Probe::Stale => {
            // Forced refresh off the sweep just taken: admission inside
            // the mutation flock always observes out-of-band changes,
            // even within the 60 s token window, and pays no snapshot
            // load for rows the mutation loads itself.
            match refresh_changed_store(
                beads_dir,
                cache_path,
                connection,
                &meta,
                &token,
                &sweep,
                RefreshFinish::Readiness,
            ) {
                Ok(_) => Ok(true),
                Err(fault) => Err(fault),
            }
        }
        Probe::Unusable(_) => {
            drop(connection);
            drop_cache_file(cache_path);
            ensure_schema(cache_path)?;
            connection = open_read_write(cache_path, REBUILD_BUSY_TIMEOUT)?;
            rebuild_cold_discard(beads_dir, cache_path, connection)
        }
    }
}

/// Cold rebuild that discards the snapshot instead of returning it.
///
/// The rows are committed exactly as [`rebuild_from_replay`] writes them;
/// only the in-memory copy the snapshot path returns is dropped, since an
/// indexed query loads just the rows it needs afterwards.
fn rebuild_cold_discard(
    beads_dir: &Path,
    cache_path: &Path,
    connection: Connection,
) -> Result<bool, Fault> {
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
    drop(connection);
    rebuild_from_replay(
        beads_dir,
        cache_path,
        &token,
        meta.generation,
        "explicit rebuild",
    )
    .map(|_| true)
}

/// Re-read the freshness inputs on the connection the caller already holds
/// and rebuild with the explicit-rebuild telemetry reason.
fn rebuild_cold(
    beads_dir: &Path,
    cache_path: &Path,
    connection: Connection,
) -> Result<CachedStoreSnapshot, Fault> {
    let meta = read_meta(&connection).map_err(|_| Fault::Cache)?;
    let token = freshness_token(beads_dir).map_err(|_| Fault::Cache)?;
    drop(connection);
    rebuild_from_replay(
        beads_dir,
        cache_path,
        &token,
        meta.generation,
        "explicit rebuild",
    )
}

/// How a refresh finishes: with a full snapshot for serving reads,
/// or with readiness only for admission and publication paths that load
/// exactly the rows they need afterwards and must not pay a full-row
/// deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RefreshFinish {
    Snapshot,
    Readiness,
}

/// Serve-or-refresh decision for a store whose sweep differs from the
/// cache: try the incremental tail first, and rebuild only when a tail
/// precondition fails. The fallback reason becomes the rebuild's
/// telemetry so doctor shows why the tail path was refused.
fn refresh_changed_store(
    beads_dir: &Path,
    cache_path: &Path,
    connection: Connection,
    meta: &CacheMeta,
    token: &str,
    sweep: &StoreSignatures,
    finish: RefreshFinish,
) -> Result<Option<CachedStoreSnapshot>, Fault> {
    match super::tail::try_tail_apply(
        beads_dir,
        cache_path,
        &connection,
        meta,
        token,
        sweep,
        finish,
    ) {
        Ok(super::tail::TailDecision::Served(snapshot)) => {
            note_serve_outcome(&connection);
            Ok(snapshot)
        }
        Ok(super::tail::TailDecision::Tailed(snapshot)) => Ok(snapshot),
        Ok(super::tail::TailDecision::Fallback(reason)) => {
            let start_generation = meta.generation;
            let token = token.to_string();
            drop(connection);
            rebuild_from_replay(
                beads_dir,
                cache_path,
                &token,
                start_generation,
                &format!("tail fallback: {reason}"),
            )
            .map(Some)
        }
        Err(fault) => Err(fault),
    }
}

/// Rebuild from a forced full replay and commit under generation CAS.
///
/// `reason` records why the replay ran (cold start, explicit rebuild, or
/// the tail precondition that failed) in the outcome telemetry.
pub(super) fn rebuild_from_replay(
    beads_dir: &Path,
    cache_path: &Path,
    token: &str,
    start_generation: u64,
    reason: &str,
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
    let fingerprint = match fingerprint_manifest_config(beads_dir) {
        Ok(fingerprint) => fingerprint,
        Err(error) => return Err(Fault::Store(error)),
    };
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
    )
    .and_then(|()| record_manifest_config_in_txn(&connection, &fingerprint))
    .and_then(|()| bump_outcome_in_txn(&connection, "outcome_rebuild"))
    .and_then(|()| record_refresh_in_txn(&connection, "rebuild", reason));
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

pub(super) struct CacheMeta {
    pub(super) token: String,
    pub(super) last_sweep_ns: i64,
    pub(super) generation: u64,
    pub(super) content_generation: u64,
    pub(super) schema_version: u32,
    pub(super) reducer_version: u32,
    pub(super) crate_version: String,
    pub(super) streams_known: bool,
    /// Largest merge key across the reduced events, as `merge_frontier`
    /// formats it. The tail refresh resumes after this point.
    pub(super) frontier: String,
    /// Manifest fields the tail gate compares: only a stream-count change
    /// from pure stream additions keeps the incremental path open.
    pub(super) manifest_schema_version: u32,
    pub(super) manifest_stream_count: usize,
    /// Canonical form of the parsed `config.json` the cache was built
    /// from. Comparing parsed structs (rather than raw bytes) keeps the
    /// tail open across default-materializing rewrites while still
    /// catching real config edits.
    pub(super) config_canonical: String,
    /// Serve / tail / rebuild outcome counters (`read-model-tail`
    /// telemetry, surfaced in the doctor cache status).
    pub(super) outcome_serve: u64,
    pub(super) outcome_tail: u64,
    pub(super) outcome_rebuild: u64,
    /// How the cached content last changed: `""`, `"tail"`, or `"rebuild"`.
    pub(super) last_refresh: String,
    /// Tail stats or the rebuild reason for the last refresh.
    pub(super) last_refresh_reason: String,
}

pub(super) fn read_meta(connection: &Connection) -> Result<CacheMeta, String> {
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
        content_generation: values
            .get("content_generation")
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
        frontier: values.get("frontier").cloned().unwrap_or_default(),
        manifest_schema_version: values
            .get("manifest_schema_version")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        manifest_stream_count: values
            .get("manifest_stream_count")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        config_canonical: values
            .get("config_canonical")
            .cloned()
            .unwrap_or_default(),
        outcome_serve: meta_counter(&values, "outcome_serve"),
        outcome_tail: meta_counter(&values, "outcome_tail"),
        outcome_rebuild: meta_counter(&values, "outcome_rebuild"),
        last_refresh: values.get("last_refresh").cloned().unwrap_or_default(),
        last_refresh_reason: values
            .get("last_refresh_reason")
            .cloned()
            .unwrap_or_default(),
    })
}

/// Parse one outcome counter from the meta map, defaulting to zero.
///
/// Caches written before `read-model-tail` have no counter keys; they
/// backfill through [`backfill_meta_keys`] instead of dropping.
fn meta_counter(values: &BTreeMap<String, String>, key: &str) -> u64 {
    values
        .get(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
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

/// Count one cache-hit serve in the outcome telemetry.
///
/// Best-effort and non-blocking: the counter update runs with a zero
/// wait, so a serve never stalls behind a concurrent rebuild's write
/// transaction. A lost bump only undercounts telemetry, never content.
pub(super) fn note_serve_outcome(connection: &Connection) {
    let _ = connection.busy_timeout(Duration::ZERO);
    let _ = connection.execute(
        "UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'outcome_serve'",
        [],
    );
    let _ = connection.busy_timeout(REBUILD_BUSY_TIMEOUT);
}

/// Bump one outcome counter inside the caller's open write transaction.
pub(super) fn bump_outcome_in_txn(
    connection: &Connection,
    key: &str,
) -> Result<(), WriteFault> {
    connection
        .execute(
            "UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key = ?1",
            rusqlite::params![key],
        )
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}

/// Record how the cached content last changed inside the open txn.
pub(super) fn record_refresh_in_txn(
    connection: &Connection,
    refresh: &str,
    reason: &str,
) -> Result<(), WriteFault> {
    for (key, value) in
        [("last_refresh", refresh), ("last_refresh_reason", reason)]
    {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![key, value],
            )
            .map_err(|_| WriteFault::Other)?;
    }
    Ok(())
}

pub(super) fn update_token_and_sweep(
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
pub(super) enum WriteFault {
    /// Another rebuild committed first; serve what it wrote.
    CasLost,
    /// Any other write fault; the file is dropped and reads replay.
    Other,
}

/// Manifest content plus parsed config the tail gate compares.
///
/// Both come from the live files at commit time, so a later change to
/// either is visible to the next freshness decision.
pub(crate) struct ManifestConfigFingerprint {
    pub(super) manifest_schema_version: u32,
    pub(super) manifest_stream_count: usize,
    pub(super) config_canonical: String,
}

/// Read the manifest and canonicalize `config.json` for the cache
/// fingerprints.
///
/// The config compares as a parsed struct, so a rewrite that only
/// materializes defaults compares equal while a real edit does not. The
/// `next_counter` allocation cursor is normalized away: minting new ids
/// bumps it on every creation, and reduction never reads it, so a
/// counter-only change keeps the tail open. An unparseable config
/// fingerprints as its raw hash, which still changes on any edit; a
/// missing `config.json` canonicalizes as empty on both sides so absence
/// compares equal to absence.
pub(crate) fn fingerprint_manifest_config(
    beads_dir: &Path,
) -> Result<ManifestConfigFingerprint, BeadError> {
    use crate::bead::config::load_config_from_str;
    use crate::bead::jsonl::event_manifest_path;
    let manifest_path = event_manifest_path(beads_dir);
    let manifest_text =
        fs::read_to_string(&manifest_path).map_err(|error| {
            BeadError::io(format!(
                "failed to read bead events manifest {}: {error}",
                manifest_path.display()
            ))
        })?;
    let manifest: BeadEventStoreManifestWire =
        serde_json::from_str(&manifest_text).map_err(BeadError::from)?;
    let config_path = beads_dir.join("config.json");
    let config_text = match fs::read_to_string(&config_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            String::new()
        }
        Err(error) => {
            return Err(BeadError::io(format!(
                "failed to read bead store config {}: {error}",
                config_path.display()
            )));
        }
    };
    let config_canonical = match load_config_from_str(&config_text) {
        Ok(config) => serde_json::to_string(&normalize_config(&config))
            .map_err(BeadError::from)?,
        Err(_) => {
            format!(
                "unparseable:{}",
                crate::bead::jsonl::hex_signature(config_text.as_bytes())
            )
        }
    };
    Ok(ManifestConfigFingerprint {
        manifest_schema_version: manifest.schema_version,
        manifest_stream_count: manifest.stream_count,
        config_canonical,
    })
}

/// Normalize a parsed config for fingerprinting: the `next_counter`
/// allocation cursor moves on every creation and reduction never reads
/// it, so it compares as zero on both sides.
fn normalize_config(
    config: &crate::bead::config::BeadConfigWire,
) -> serde_json::Value {
    let mut value =
        serde_json::to_value(config).unwrap_or(serde_json::Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.insert("next_counter".to_string(), serde_json::Value::from(0));
    }
    value
}

/// Persist one manifest/config fingerprint inside the open write txn.
pub(super) fn record_manifest_config_in_txn(
    connection: &Connection,
    fingerprint: &ManifestConfigFingerprint,
) -> Result<(), WriteFault> {
    for (key, value) in [
        (
            "manifest_schema_version",
            fingerprint.manifest_schema_version.to_string(),
        ),
        (
            "manifest_stream_count",
            fingerprint.manifest_stream_count.to_string(),
        ),
        ("config_canonical", fingerprint.config_canonical.clone()),
    ] {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![key, value],
            )
            .map_err(|_| WriteFault::Other)?;
    }
    Ok(())
}

/// Insert link-provenance rows inside the caller's open write txn.
///
/// Callers clear the rows they replace first: full rewrites delete the
/// whole table, tail commits delete only the touched sources and the
/// removed issues' target refs, then re-insert those sources from the
/// resumed map.
pub(super) fn insert_link_provenance(
    connection: &Connection,
    provenance: &BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
) -> Result<(), WriteFault> {
    let mut link_stmt = connection
        .prepare(
            "INSERT INTO link_provenance (target_ref, source_issue_id, relation, description, origin, direction, uses, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .map_err(|_| WriteFault::Other)?;
    // The map key duplicates the row fields and stays in memory only.
    for provenance in provenance.values() {
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
    Ok(())
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
    super::alloc::ensure_alloc_schema(connection)
        .map_err(|_| WriteFault::Other)?;
    let alloc_ids: Vec<&str> = snapshot
        .issues
        .iter()
        .map(|issue| issue.id.as_str())
        .collect();
    super::alloc::rebuild_allocation(connection, alloc_ids)
        .map_err(|_| WriteFault::Other)?;
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
            "INSERT INTO issues (id, position, row, status, issue_type, tier, parent, stream, created_at, external_ref, task_type, plus_one, is_flag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                issue.task_type.clone().unwrap_or_default(),
                issue.plus_one_count() as i64,
                i64::from(issue.is_flag_task()),
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
    insert_link_provenance(connection, &snapshot.provenance)?;
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
    connection
        .execute(
            "UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'content_generation'",
            [],
        )
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}

/// Load the snapshot in replay order.
pub(super) fn load_snapshot(
    connection: &Connection,
) -> Result<CachedStoreSnapshot, String> {
    #[cfg(test)]
    crate::bead::mutation::store_io_stats::record_snapshot_load();
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

/// Stored per-stream signatures with the content hashes the tail gate
/// verifies append prefixes against.
pub(super) struct StoredStreamSig {
    pub(super) size: u64,
    pub(super) mtime_ns: i64,
    pub(super) inode: u64,
    pub(super) byte_len: u64,
    pub(super) content_hash: String,
}

/// Load every stored stream signature, including content hashes.
pub(super) fn load_stored_stream_sigs(
    connection: &Connection,
) -> BTreeMap<String, StoredStreamSig> {
    let mut signatures = BTreeMap::new();
    let mut statement = match connection.prepare(
        "SELECT stream_id, size, mtime_ns, inode, byte_len, content_hash FROM streams ORDER BY stream_id",
    ) {
        Ok(statement) => statement,
        Err(_) => return signatures,
    };
    let rows = match statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
        ))
    }) {
        Ok(rows) => rows,
        Err(_) => return signatures,
    };
    for row in rows {
        let Ok((stream_id, size, mtime_ns, inode, byte_len, content_hash)) =
            row
        else {
            continue;
        };
        signatures.insert(
            stream_id,
            StoredStreamSig {
                size: u64::try_from(size).unwrap_or(0),
                mtime_ns,
                inode: u64::try_from(inode).unwrap_or(0),
                byte_len: u64::try_from(byte_len).unwrap_or(0),
                content_hash,
            },
        );
    }
    signatures
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
pub(super) fn merge_frontier(streams: &[BeadEventStreamWire]) -> String {
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
        Some((timestamp, priority, event_id)) => {
            format_frontier(&timestamp, priority, &event_id)
        }
        None => format_frontier("", 0, ""),
    }
}

/// Format one merge-frontier key exactly as [`merge_frontier`] does, so
/// tail commits advance the stored frontier in the same encoding.
pub(super) fn format_frontier(
    timestamp: &str,
    priority: usize,
    event_id: &str,
) -> String {
    serde_json::json!({
        "timestamp": timestamp,
        "priority": priority,
        "event_id": event_id,
    })
    .to_string()
}

/// Parse one merge-frontier key back into its ordering tuple.
pub(super) fn parse_frontier(frontier: &str) -> (String, usize, String) {
    let value: serde_json::Value =
        serde_json::from_str(frontier).unwrap_or(serde_json::Value::Null);
    (
        value
            .get("timestamp")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        value
            .get("priority")
            .and_then(|value| value.as_u64())
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0),
        value
            .get("event_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
    )
}

/// Root of an issue's lineage: the stream that canonically holds it.
///
/// A root ID owns `events/streams/<id>.jsonl` and a phase lives in its
/// parent plan's stream, so walking the parent chain to the top derives
/// the stream without parsing events. Cycles fall back to the issue
/// itself.
pub(super) fn lineage_root(
    id: &str,
    parent_of: &BTreeMap<&str, &str>,
) -> String {
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
pub(super) fn id_suffix(id: &str) -> &str {
    id.rsplit_once('-').map(|(_, suffix)| suffix).unwrap_or(id)
}

pub(super) fn wire_string(value: &impl Serialize) -> String {
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
                external_ref TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL DEFAULT '',
                plus_one INTEGER NOT NULL DEFAULT 0,
                is_flag INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS issues_status ON issues(status);
            CREATE INDEX IF NOT EXISTS issues_type ON issues(issue_type);
            CREATE INDEX IF NOT EXISTS issues_tier ON issues(tier);
            CREATE INDEX IF NOT EXISTS issues_parent ON issues(parent);
            CREATE INDEX IF NOT EXISTS issues_stream ON issues(stream);
            CREATE INDEX IF NOT EXISTS issues_created_at ON issues(created_at);
            CREATE INDEX IF NOT EXISTS issues_external_ref ON issues(external_ref);
            CREATE INDEX IF NOT EXISTS issues_task_type ON issues(task_type);
            CREATE INDEX IF NOT EXISTS issues_is_flag ON issues(is_flag);
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
            CREATE TABLE IF NOT EXISTS alloc_top (
                prefix TEXT PRIMARY KEY,
                max_counter INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alloc_child (
                parent_id TEXT PRIMARY KEY,
                max_suffix INTEGER NOT NULL
            );
            ",
        )
        .map_err(|_| Fault::Cache)?;
    for (key, value) in tail_meta_defaults() {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                rusqlite::params![key, value],
            )
            .map_err(|_| Fault::Cache)?;
    }
    Ok(())
}

/// Default values for every meta key, including the `read-model-tail`
/// manifest/config fingerprints and outcome telemetry.
///
/// Shared by fresh-file creation and [`backfill_meta_keys`] so caches
/// written before `read-model-tail` gain the new keys without a drop and
/// rebuild.
fn tail_meta_defaults() -> [(&'static str, String); 16] {
    [
        ("schema_version", READ_MODEL_SCHEMA_VERSION.to_string()),
        ("reducer_version", READ_MODEL_REDUCER_VERSION.to_string()),
        ("crate_version", crate_version()),
        ("token", String::new()),
        ("last_sweep_ns", "0".to_string()),
        ("frontier", String::new()),
        ("generation", "0".to_string()),
        ("content_generation", "0".to_string()),
        ("manifest_schema_version", "0".to_string()),
        ("manifest_stream_count", "0".to_string()),
        ("config_canonical", String::new()),
        ("outcome_serve", "0".to_string()),
        ("outcome_tail", "0".to_string()),
        ("outcome_rebuild", "0".to_string()),
        ("last_refresh", String::new()),
        ("last_refresh_reason", String::new()),
    ]
}

/// Heal pre-tail caches: insert any missing meta key without touching the
/// rows, counters, or generation. Runs on every read before the freshness
/// decision, so a cache written by `read-model-store` gains tail support
/// on its next read instead of rebuilding.
pub(super) fn backfill_meta_keys(connection: &Connection) -> Result<(), Fault> {
    let defaults = tail_meta_defaults();
    let placeholders =
        defaults.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for (key, _) in &defaults {
        params.push(key);
    }
    let present: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM meta WHERE key IN ({placeholders})"),
            params.as_slice(),
            |row| row.get(0),
        )
        .map_err(|_| Fault::Cache)?;
    if present as usize >= defaults.len() {
        return Ok(());
    }
    for (key, value) in &defaults {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                rusqlite::params![key, value],
            )
            .map_err(|_| Fault::Cache)?;
    }
    Ok(())
}

/// Record the post-write freshness token without moving the sweep time.
///
/// Direct publication recomputes the token after its own append: the
/// stored token must describe the published files so the next read
/// serves token-only, while `last_sweep_ns` stays at the admission
/// sweep so out-of-band writers stay bounded by the 60 s rule.
pub(super) fn set_token_in_txn(
    connection: &Connection,
    token: &str,
) -> Result<(), WriteFault> {
    connection
        .execute("UPDATE meta SET value = ?1 WHERE key = 'token'", [token])
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}

pub(super) fn open_read_only(
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

pub(super) fn open_read_write(
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
pub(super) fn drop_cache_file(cache_path: &Path) {
    for companion in [
        cache_path.to_path_buf(),
        cache_path.with_extension("sqlite-wal"),
        cache_path.with_extension("sqlite-shm"),
        cache_path.with_extension("sqlite-journal"),
    ] {
        let _ = fs::remove_file(companion);
    }
}
