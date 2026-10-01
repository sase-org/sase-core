//! Index build, incremental sync, health, and snapshots.
//!
//! One scope walk is one first-parent `--raw -z -M` pass over explicit
//! pathspecs (§5.2 of the memory-history plan). [`build_index`] walks
//! from HEAD; [`sync_index`] reuses a cached index when its key still
//! matches: equal tips are fresh, tip ancestry (`merge-base
//! --is-ancestor`) folds the range in between, and anything else
//! (rewrite, force-push, branch switch, key mismatch) rebuilds. A
//! folded index only equals a fresh rebuild when neither walk was
//! truncated: under a budget cut the fold keeps previously walked
//! history a budgeted rebuild would drop, which is strictly more
//! history, and the `truncated` flag stays set either way.

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::lineage::{fold_lineages, fold_new_commits, path_alias_map};
use super::parse::{parse_raw_log, RawCommit};
use super::runner::{
    looks_like_full_sha, run_git_checked, run_git_unchecked, safe_pathspec,
};
use super::wire::{
    FileHistoryBudgetWire, FileHistoryError, FileHistoryHealthWire,
    FileHistoryIndexWire, FileHistoryScopeKeyWire, FileHistorySyncStatus,
    FILE_HISTORY_WIRE_SCHEMA_VERSION,
};

/// Pinned `--format` for the scope walk: NUL-separated header fields
/// the parser splits without ambiguity.
pub const PINNED_LOG_FORMAT: &str =
    "COMMIT%x00%H%x00%P%x00%ct%x00%at%x00%an%x00%ae%x00%s%x00%b%x00END";

/// Build the file-history index for *pathspecs* in the repo
/// containing *repo_root*.
pub fn build_index(
    repo_root: &Path,
    pathspecs: &[String],
    budget: &FileHistoryBudgetWire,
) -> Result<FileHistoryIndexWire, FileHistoryError> {
    let normalized = normalize_pathspecs(pathspecs)?;
    let toplevel = repo_toplevel(repo_root, budget)?;
    let common_dir = repo_common_dir(&toplevel, budget)?;
    let head = repo_head(&toplevel, budget)?;
    let shallow = repo_is_shallow(&toplevel, budget)?;
    let (commits, truncated) =
        walk_commits(&toplevel, &normalized, &head, budget)?;
    let lineages = fold_lineages(&commits);
    let boundary = shallow_boundary_commits(&common_dir, shallow);
    Ok(assemble_index(
        toplevel,
        common_dir,
        head,
        normalized,
        lineages,
        &commits,
        truncated,
        shallow,
        &boundary,
        commits.len() as u64,
        Vec::new(),
    ))
}

/// Sync a cached index against the current checkout. Returns the index
/// to keep and whether it was fresh, folded, or rebuilt.
pub fn sync_index(
    repo_root: &Path,
    pathspecs: &[String],
    cached: &FileHistoryIndexWire,
    budget: &FileHistoryBudgetWire,
) -> Result<(FileHistoryIndexWire, FileHistorySyncStatus), FileHistoryError> {
    let normalized = normalize_pathspecs(pathspecs)?;
    let toplevel = repo_toplevel(repo_root, budget)?;
    let common_dir = repo_common_dir(&toplevel, budget)?;
    let head = repo_head(&toplevel, budget)?;
    let current_key = FileHistoryScopeKeyWire {
        repo_common_dir: common_dir.clone(),
        pathspecs: normalized.clone(),
        schema_version: FILE_HISTORY_WIRE_SCHEMA_VERSION,
    };
    let cached_key = FileHistoryScopeKeyWire {
        repo_common_dir: cached.repo_common_dir.clone(),
        pathspecs: cached.pathspecs.clone(),
        schema_version: cached.schema_version,
    };
    if current_key != cached_key || head.is_empty() {
        return Ok((
            build_index(repo_root, pathspecs, budget)?,
            FileHistorySyncStatus::Rebuilt,
        ));
    }
    if cached.tip == head {
        let mut fresh = cached.clone();
        fresh.repo_root = path_string(&toplevel);
        fresh.repo_common_dir = common_dir;
        return Ok((fresh, FileHistorySyncStatus::Fresh));
    }
    if !looks_like_full_sha(&cached.tip) {
        return Ok((
            build_index(repo_root, pathspecs, budget)?,
            FileHistorySyncStatus::Rebuilt,
        ));
    }
    if !is_ancestor(&toplevel, &cached.tip, &head, budget)? {
        return Ok((
            build_index(repo_root, pathspecs, budget)?,
            FileHistorySyncStatus::Rebuilt,
        ));
    }
    let range = format!("{}..{}", cached.tip, head);
    let (commits, range_truncated) =
        walk_commits(&toplevel, &normalized, &range, budget)?;
    let shallow = repo_is_shallow(&toplevel, budget)?;
    let lineages = fold_new_commits(&cached.lineages, &commits);
    let truncated = cached.health.truncated || range_truncated;
    let boundary = shallow_boundary_commits(&common_dir, shallow);
    Ok((
        assemble_index(
            toplevel,
            common_dir,
            head,
            normalized,
            lineages,
            &commits,
            truncated,
            shallow,
            &boundary,
            cached.commit_count + commits.len() as u64,
            cached.health.missing_objects.clone(),
        ),
        FileHistorySyncStatus::Folded,
    ))
}

/// Serialize an index to its JSON snapshot form.
pub fn index_to_json(
    index: &FileHistoryIndexWire,
) -> Result<String, FileHistoryError> {
    serde_json::to_string(index).map_err(|error| {
        FileHistoryError::Snapshot(format!("serialize index: {error}"))
    })
}

/// Parse an index from its JSON snapshot form, rejecting unknown
/// schema versions so a stale cache always rebuilds instead of
/// misreading.
pub fn index_from_json(
    json: &str,
) -> Result<FileHistoryIndexWire, FileHistoryError> {
    let index: FileHistoryIndexWire =
        serde_json::from_str(json).map_err(|error| {
            FileHistoryError::Snapshot(format!("parse index: {error}"))
        })?;
    if index.schema_version != FILE_HISTORY_WIRE_SCHEMA_VERSION {
        return Err(FileHistoryError::Snapshot(format!(
            "unsupported schema version {}",
            index.schema_version
        )));
    }
    Ok(index)
}

/// Atomically persist an index snapshot to a caller-supplied path: a
/// temporary file in the same directory plus rename, with directory
/// fsync. Readers never wait on a lock.
pub fn persist_index(
    path: &Path,
    index: &FileHistoryIndexWire,
) -> Result<(), FileHistoryError> {
    let json = index_to_json(index)?;
    let parent = path.parent().ok_or_else(|| {
        FileHistoryError::Snapshot(format!(
            "snapshot path has no parent: {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(|error| {
        FileHistoryError::Snapshot(format!("create parent: {error}"))
    })?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            FileHistoryError::Snapshot(format!("create temp: {error}"))
        })?;
    use std::io::Write as _;
    temporary.write_all(json.as_bytes()).map_err(|error| {
        FileHistoryError::Snapshot(format!("write temp: {error}"))
    })?;
    temporary.as_file().sync_all().map_err(|error| {
        FileHistoryError::Snapshot(format!("sync temp: {error}"))
    })?;
    temporary.persist(path).map_err(|error| {
        FileHistoryError::Snapshot(format!("persist: {error}"))
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            FileHistoryError::Snapshot(format!("sync dir: {error}"))
        })
}

/// Load an index snapshot: `Ok(None)` when the file is absent (cold
/// cache), `Err` when it is corrupt (the caller rebuilds silently).
pub fn load_index(
    path: &Path,
) -> Result<Option<FileHistoryIndexWire>, FileHistoryError> {
    match std::fs::read_to_string(path) {
        Ok(json) => index_from_json(&json).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(FileHistoryError::Snapshot(format!("read index: {error}")))
        }
    }
}

fn normalize_pathspecs(
    pathspecs: &[String],
) -> Result<Vec<String>, FileHistoryError> {
    let mut normalized: Vec<String> = Vec::with_capacity(pathspecs.len());
    for pathspec in pathspecs {
        if !safe_pathspec(pathspec) {
            return Err(FileHistoryError::InvalidPath(pathspec.clone()));
        }
        if !normalized.contains(pathspec) {
            normalized.push(pathspec.clone());
        }
    }
    normalized.sort();
    Ok(normalized)
}

fn budget_timeout(budget: &FileHistoryBudgetWire) -> Duration {
    Duration::from_millis(budget.timeout_ms.max(1))
}

fn repo_toplevel(
    repo_root: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<PathBuf, FileHistoryError> {
    let result = run_git_checked(
        repo_root,
        &["rev-parse", "--show-toplevel"],
        budget_timeout(budget),
        64 * 1024,
    )
    .map_err(|_| FileHistoryError::NotARepo(path_string(repo_root)))?;
    let toplevel = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if toplevel.is_empty() {
        return Err(FileHistoryError::NotARepo(path_string(repo_root)));
    }
    Ok(PathBuf::from(toplevel))
}

fn repo_common_dir(
    toplevel: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<String, FileHistoryError> {
    let result = run_git_checked(
        toplevel,
        &["rev-parse", "--git-common-dir"],
        budget_timeout(budget),
        64 * 1024,
    )
    .map_err(|error| {
        FileHistoryError::GitFailed(format!("common dir: {error}"))
    })?;
    let common = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if common.is_empty() {
        return Err(FileHistoryError::GitFailed(
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
) -> Result<String, FileHistoryError> {
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
    if looks_like_full_sha(&head) {
        Ok(head)
    } else {
        Ok(String::new())
    }
}

fn repo_is_shallow(
    toplevel: &Path,
    budget: &FileHistoryBudgetWire,
) -> Result<bool, FileHistoryError> {
    let result = run_git_checked(
        toplevel,
        &["rev-parse", "--is-shallow-repository"],
        budget_timeout(budget),
        64 * 1024,
    )?;
    Ok(String::from_utf8_lossy(&result.stdout).trim() == "true")
}

fn is_ancestor(
    toplevel: &Path,
    old: &str,
    new: &str,
    budget: &FileHistoryBudgetWire,
) -> Result<bool, FileHistoryError> {
    let result = run_git_unchecked(
        toplevel,
        &["merge-base", "--is-ancestor", old, new],
        budget_timeout(budget),
        64 * 1024,
    )?;
    Ok(result.code == Some(0))
}

fn log_argv(
    revision: &str,
    pathspecs: &[String],
    budget: &FileHistoryBudgetWire,
) -> Vec<String> {
    let max_count = budget.max_commits.saturating_add(1).max(1);
    let mut argv = vec![
        "log".to_string(),
        "--first-parent".to_string(),
        "--raw".to_string(),
        "-z".to_string(),
        "-M".to_string(),
        "--no-abbrev".to_string(),
        "--no-ext-diff".to_string(),
        "--no-textconv".to_string(),
        format!("--max-count={max_count}"),
        format!("--format={PINNED_LOG_FORMAT}"),
        revision.to_string(),
        "--".to_string(),
    ];
    argv.extend(pathspecs.iter().cloned());
    argv
}

/// Walk one revision (a tip or an `old..new` range), returning newest-
/// first commits and whether a budget cut the walk short.
fn walk_commits(
    toplevel: &Path,
    pathspecs: &[String],
    revision: &str,
    budget: &FileHistoryBudgetWire,
) -> Result<(Vec<RawCommit>, bool), FileHistoryError> {
    if revision.is_empty() {
        return Ok((Vec::new(), false));
    }
    let argv = log_argv(revision, pathspecs, budget);
    let arg_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let result = run_git_checked(
        toplevel,
        &arg_refs,
        budget_timeout(budget),
        budget.max_output_bytes,
    )?;
    let mut commits = parse_raw_log(&result.stdout);
    let over_cap = commits.len() > budget.max_commits;
    commits.truncate(budget.max_commits);
    Ok((commits, result.truncated || over_cap))
}

/// Boundary commits of a shallow clone (`None` when the repo is not
/// shallow, or when it claims to be shallow but its boundary file is
/// unreadable so every lineage must count as incomplete).
fn shallow_boundary_commits(
    common_dir: &str,
    shallow: bool,
) -> Option<HashSet<String>> {
    if !shallow {
        return Some(HashSet::new());
    }
    let text =
        std::fs::read_to_string(Path::new(common_dir).join("shallow")).ok()?;
    Some(
        text.lines()
            .map(str::trim)
            .filter(|line| looks_like_full_sha(line))
            .map(ToString::to_string)
            .collect(),
    )
}

#[allow(clippy::too_many_arguments)]
fn assemble_index(
    toplevel: PathBuf,
    common_dir: String,
    tip: String,
    pathspecs: Vec<String>,
    mut lineages: Vec<super::wire::FileLineageWire>,
    commits: &[RawCommit],
    truncated: bool,
    shallow: bool,
    boundary: &Option<HashSet<String>>,
    commit_count: u64,
    missing_objects: Vec<String>,
) -> FileHistoryIndexWire {
    for lineage in &mut lineages {
        if truncated || !lineage.complete {
            lineage.complete = false;
            continue;
        }
        // A creation at a boundary commit is indistinguishable from a
        // cut: the parent side is missing, so git reports an add
        // either way. Only a creation past the boundary proves the
        // lineage starts inside visible history.
        let cut_at_boundary = match boundary {
            None => shallow,
            Some(boundary) => {
                shallow
                    && lineage
                        .versions
                        .last()
                        .is_some_and(|oldest| boundary.contains(&oldest.commit))
            }
        };
        lineage.complete = !cut_at_boundary;
    }
    let boundary = if shallow {
        commits.iter().map(|commit| commit.committer_time).min()
    } else {
        None
    };
    let complete = !truncated
        && !shallow
        && lineages.iter().all(|lineage| lineage.complete);
    let mut health_missing = missing_objects;
    health_missing.sort();
    health_missing.dedup();
    FileHistoryIndexWire {
        schema_version: FILE_HISTORY_WIRE_SCHEMA_VERSION,
        repo_root: path_string(&toplevel),
        repo_common_dir: common_dir,
        tip,
        pathspecs,
        path_aliases: path_alias_map(&lineages),
        health: FileHistoryHealthWire {
            shallow,
            shallow_boundary_time: boundary,
            truncated,
            missing_objects: health_missing,
            complete,
        },
        commit_count,
        lineages,
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
