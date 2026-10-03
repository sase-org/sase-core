//! Per-scope review watermarks: explicit "reviewed through" markers.
//!
//! The store is one JSON file under a caller-supplied state
//! directory (`{state_dir}/memory_history_review.json`), keyed by
//! scope key so every workspace clone of a project shares it. It
//! holds state, never cache: callers pass SASE's state area, not the
//! disposable snapshot `cache_dir`.
//!
//! Writes take a writer-only advisory lock on a sibling `.lock` file
//! and use temp-file-plus-rename, so concurrent markers never tear
//! the file. Readers never open the lock and never wait on it. A
//! corrupt or wrong-schema store reads as empty and is reported via
//! [`MemoryHistoryReviewStateWire::store_corrupt`], never repaired
//! or rewritten on read. Watermarks change only on an explicit mark:
//! nothing here advances them on its own.
//!
//! `N new` counts the default-visible changesets (not hidden, not
//! regen-only) that are strict first-parent descendants of the
//! watermark commit, measured on the scope tip's first-parent chain
//! in one `git rev-list` call. When this checkout does not know the
//! watermark commit, the count falls back to committer time.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;

use super::cache::sync_scope;
use super::feed::{build_feed, FeedScope};
use super::wire::{
    MemoryHistoryError, MemoryHistoryMarkReviewedRequestWire,
    MemoryHistoryMarkReviewedWire, MemoryHistoryReviewScopeStateWire,
    MemoryHistoryReviewStateRequestWire, MemoryHistoryReviewStateWire,
    MemoryHistoryReviewWatermarkWire, MemoryHistoryScopeWire,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
use crate::file_history::{
    looks_like_full_sha, run_git_unchecked, FileHistoryBudgetWire,
};

/// Review-store filename under the caller-supplied state directory.
pub const REVIEW_STORE_FILENAME: &str = "memory_history_review.json";

/// Schema version of the review-store file. Independent of the wire
/// schema: a mismatch reads as empty and is reported.
pub const REVIEW_STORE_SCHEMA_VERSION: u32 = 1;

/// Review-store file for a state directory.
pub fn review_store_path(state_dir: &str) -> PathBuf {
    Path::new(state_dir).join(REVIEW_STORE_FILENAME)
}

/// One stored watermark: the reviewed-through commit, its committer
/// time, and when the mark was recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewWatermark {
    /// Full commit SHA reviewed through.
    pub commit: String,
    /// Its committer time, epoch seconds.
    pub committer_time: i64,
    /// When the mark was recorded, epoch seconds wall-clock.
    pub marked_at: i64,
}

fn validate_state_dir(state_dir: &str) -> Result<(), MemoryHistoryError> {
    if state_dir.trim().is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "memory-history review state_dir must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn valid_watermark(commit: &str) -> bool {
    looks_like_full_sha(commit)
}

/// Load the review store: watermarks by scope key plus whether the
/// store exists but could not be read. A missing store is healthy
/// and empty (first use). Never opens the lock file, never writes.
pub fn load_review_store(
    state_dir: &str,
) -> Result<(HashMap<String, ReviewWatermark>, bool), MemoryHistoryError> {
    validate_state_dir(state_dir)?;
    let path = review_store_path(state_dir);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), false));
        }
        Err(_) => return Ok((HashMap::new(), true)),
    };
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) => return Ok((HashMap::new(), true)),
    };
    if value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(u64::from(REVIEW_STORE_SCHEMA_VERSION))
    {
        return Ok((HashMap::new(), true));
    }
    let mut watermarks = HashMap::new();
    let mut corrupt = false;
    match value.get("watermarks") {
        None => {}
        Some(serde_json::Value::Object(entries)) => {
            for (scope_key, entry) in entries {
                match parse_watermark_entry(entry) {
                    Some(watermark) => {
                        watermarks.insert(scope_key.clone(), watermark);
                    }
                    None => corrupt = true,
                }
            }
        }
        Some(_) => corrupt = true,
    }
    Ok((watermarks, corrupt))
}

fn parse_watermark_entry(entry: &serde_json::Value) -> Option<ReviewWatermark> {
    let commit = entry.get("commit")?.as_str()?;
    if !valid_watermark(commit) {
        return None;
    }
    Some(ReviewWatermark {
        commit: commit.to_ascii_lowercase(),
        committer_time: entry.get("committer_time")?.as_i64()?,
        marked_at: entry.get("marked_at")?.as_i64()?,
    })
}

fn serialize_store(watermarks: &HashMap<String, ReviewWatermark>) -> String {
    let ordered: BTreeMap<&str, serde_json::Value> = watermarks
        .iter()
        .map(|(key, watermark)| {
            (
                key.as_str(),
                serde_json::json!({
                    "commit": watermark.commit,
                    "committer_time": watermark.committer_time,
                    "marked_at": watermark.marked_at,
                }),
            )
        })
        .collect();
    serde_json::json!({
        "schema_version": REVIEW_STORE_SCHEMA_VERSION,
        "watermarks": ordered,
    })
    .to_string()
}

/// Sibling advisory-lock path: `review.json` locks `review.json.lock`.
fn sibling_lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    path.with_file_name(name)
}

/// Persist merged watermarks under the sibling lock with
/// temp-file-plus-rename.
fn persist_locked(
    state_dir: &str,
    watermarks: &HashMap<String, ReviewWatermark>,
) -> Result<(), MemoryHistoryError> {
    let path = review_store_path(state_dir);
    let parent = Path::new(state_dir);
    fs::create_dir_all(parent).map_err(|error| {
        MemoryHistoryError::InvalidScope(format!(
            "cannot create memory-history review state directory {state_dir}: {error}"
        ))
    })?;
    let lock_path = sibling_lock_path(&path);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|error| {
            MemoryHistoryError::InvalidScope(format!(
                "cannot open memory-history review lock {}: {error}",
                lock_path.display()
            ))
        })?;
    lock.lock_exclusive().map_err(|error| {
        MemoryHistoryError::InvalidScope(format!(
            "cannot lock memory-history review store {}: {error}",
            lock_path.display()
        ))
    })?;
    let outcome = (|| -> Result<(), MemoryHistoryError> {
        // Re-read under the lock so concurrent markers merge instead
        // of clobbering each other; a corrupt store starts empty.
        let (mut current, _) = load_review_store(state_dir).unwrap_or_default();
        for (key, watermark) in watermarks {
            current.insert(key.clone(), watermark.clone());
        }
        let bytes = serialize_store(&current);
        let temporary =
            tempfile::NamedTempFile::new_in(parent).map_err(|error| {
                MemoryHistoryError::InvalidScope(format!(
                    "cannot stage memory-history review store: {error}"
                ))
            })?;
        use std::io::Write as _;
        {
            let mut file = temporary.as_file();
            file.write_all(bytes.as_bytes()).map_err(|error| {
                MemoryHistoryError::InvalidScope(format!(
                    "cannot write memory-history review store: {error}"
                ))
            })?;
            file.sync_all().map_err(|error| {
                MemoryHistoryError::InvalidScope(format!(
                    "cannot sync memory-history review store: {error}"
                ))
            })?;
        }
        temporary.persist(&path).map_err(|error| {
            MemoryHistoryError::InvalidScope(format!(
                "cannot persist memory-history review store: {error}"
            ))
        })?;
        Ok(())
    })();
    let _ = FileExt::unlock(&lock);
    outcome
}

fn wall_clock_secs() -> Result<i64, MemoryHistoryError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .map_err(|_| {
            MemoryHistoryError::InvalidScope(
                "system clock is before the epoch".to_string(),
            )
        })
}

/// Committer time of one commit in a scope repo, or `None` when the
/// repository does not know it.
fn commit_time(
    scope: &MemoryHistoryScopeWire,
    commit: &str,
) -> Result<Option<i64>, MemoryHistoryError> {
    if !looks_like_full_sha(commit) {
        return Ok(None);
    }
    let budget = FileHistoryBudgetWire::default();
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let result = run_git_unchecked(
        Path::new(&scope.repo_root),
        &["log", "-1", "--format=%ct", commit],
        timeout,
        64 * 1024,
    )?;
    if result.code != Some(0) {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&result.stdout).trim().to_string();
    Ok(text.parse::<i64>().ok())
}

/// First-parent chain of *tip* newest-first, or `None` when git
/// cannot list it. One call per scope, not per changeset.
fn first_parent_chain(
    scope: &MemoryHistoryScopeWire,
    tip: &str,
) -> Result<Option<Vec<String>>, MemoryHistoryError> {
    if !looks_like_full_sha(tip) {
        return Ok(None);
    }
    let budget = FileHistoryBudgetWire::default();
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let result = run_git_unchecked(
        Path::new(&scope.repo_root),
        &["rev-list", "--first-parent", tip],
        timeout,
        64 * 1024 * 1024,
    )?;
    if result.code != Some(0) || result.truncated {
        return Ok(None);
    }
    let chain: Vec<String> = String::from_utf8_lossy(&result.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| looks_like_full_sha(line))
        .map(str::to_string)
        .collect();
    if chain.is_empty() {
        return Ok(None);
    }
    Ok(Some(chain))
}

/// Count default-visible changesets strictly newer than the
/// watermark: strict first-parent descendants of its commit, or
/// committer-newer when this checkout does not know that commit.
fn count_new(
    changeset_commits: &[(String, i64)],
    watermark: &ReviewWatermark,
    chain: Option<&[String]>,
) -> u64 {
    if let Some(positions) = chain_positions(chain, &watermark.commit) {
        let (chain_pos, watermark_pos) = positions;
        return changeset_commits
            .iter()
            .filter(|(commit, _)| {
                chain_pos
                    .get(commit)
                    .is_some_and(|&pos| pos < watermark_pos)
            })
            .count() as u64;
    }
    changeset_commits
        .iter()
        .filter(|(_, time)| *time > watermark.committer_time)
        .count() as u64
}

/// Position map of a first-parent chain plus the watermark's
/// position, or `None` when the watermark is not on the chain.
fn chain_positions(
    chain: Option<&[String]>,
    watermark_commit: &str,
) -> Option<(HashMap<String, usize>, usize)> {
    let chain = chain?;
    let watermark_pos = chain.iter().position(|sha| sha == watermark_commit)?;
    Some((
        chain.iter().cloned().zip(0..).collect::<HashMap<_, _>>(),
        watermark_pos,
    ))
}

/// Review state for every requested scope: each scope is synced
/// first, so the newest changeset and the new count reflect HEAD.
pub fn query_review_state(
    request: &MemoryHistoryReviewStateRequestWire,
) -> Result<MemoryHistoryReviewStateWire, MemoryHistoryError> {
    validate_state_dir(&request.state_dir)?;
    if request.scopes.is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "memory-history review state needs at least one scope".to_string(),
        ));
    }
    let (watermarks, store_corrupt) = load_review_store(&request.state_dir)?;
    let mut scopes = Vec::with_capacity(request.scopes.len());
    for scope in &request.scopes {
        if scope.scope_key.trim().is_empty() {
            return Err(MemoryHistoryError::InvalidScope(
                "memory-history scope_key must not be empty".to_string(),
            ));
        }
        let outcome = sync_scope(scope)?;
        let feed_scopes = vec![FeedScope {
            scope_key: scope.scope_key.as_str(),
            subjects: &outcome.subjects,
        }];
        // Default rows: hidden versions and regen-only changesets
        // are exactly what `N new` must ignore.
        let feed = build_feed(&feed_scopes, None, None, false);
        let visible: Vec<(String, i64)> = feed
            .changesets
            .iter()
            .map(|changeset| {
                (changeset.commit.clone(), changeset.committer_time)
            })
            .collect();
        let (newest_commit, newest_committer_time) =
            visible.first().cloned().unwrap_or_default();
        let chain = if outcome.sync.tip.is_empty() {
            None
        } else {
            first_parent_chain(scope, &outcome.sync.tip)?
        };
        let watermark = watermarks.get(&scope.scope_key).cloned();
        let new_count = watermark
            .as_ref()
            .map(|mark| count_new(&visible, mark, chain.as_deref()))
            .unwrap_or(0);
        scopes.push(MemoryHistoryReviewScopeStateWire {
            scope_key: scope.scope_key.clone(),
            watermark: watermark.map(|mark| MemoryHistoryReviewWatermarkWire {
                commit: mark.commit,
                committer_time: mark.committer_time,
                marked_at: mark.marked_at,
            }),
            new_count,
            newest_commit,
            newest_committer_time,
        });
    }
    Ok(MemoryHistoryReviewStateWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        scopes,
        store_corrupt,
    })
}

/// Record a watermark for one scope. The commit must exist in the
/// scope's repository; its committer time is read from git and the
/// mark time from the wall clock.
pub fn query_mark_reviewed(
    request: &MemoryHistoryMarkReviewedRequestWire,
) -> Result<MemoryHistoryMarkReviewedWire, MemoryHistoryError> {
    validate_state_dir(&request.state_dir)?;
    if request.scope.scope_key.trim().is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "memory-history scope_key must not be empty".to_string(),
        ));
    }
    let commit = request.through_commit.trim().to_ascii_lowercase();
    let committer_time =
        commit_time(&request.scope, &commit)?.ok_or_else(|| {
            MemoryHistoryError::InvalidPath(format!(
                "unknown commit {} for scope {}",
                request.through_commit, request.scope.scope_key
            ))
        })?;
    let watermark = ReviewWatermark {
        commit: commit.clone(),
        committer_time,
        marked_at: wall_clock_secs()?,
    };
    let mut update = HashMap::new();
    update.insert(request.scope.scope_key.clone(), watermark.clone());
    persist_locked(&request.state_dir, &update)?;
    Ok(MemoryHistoryMarkReviewedWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        scope_key: request.scope.scope_key.clone(),
        watermark: MemoryHistoryReviewWatermarkWire {
            commit: watermark.commit,
            committer_time: watermark.committer_time,
            marked_at: watermark.marked_at,
        },
    })
}
