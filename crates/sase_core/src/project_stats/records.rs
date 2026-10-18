//! Metadata-only snapshots for configured record repositories.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::git::{self, ResolvedRepo};
use super::records_cache::{self, RecordCacheEntry};
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
    ProjectStatsSnapshotWire,
};
use crate::file_history::GitResult;

/// The metadata facts persisted for a resolved repository tip.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordSummary {
    pub tracked_files: u64,
    pub lifetime_commits: u64,
    pub first_commit_ts: Option<i64>,
    pub last_commit_ts: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RecordOutcome {
    pub snapshot: ProjectStatsSnapshotWire,
    pub complete: bool,
    pub issue: Option<ProjectStatsIssueWire>,
    pub source_token: String,
}

/// Measure one resolved record repository. Git commands inspect only commit
/// metadata and tree entries; no file contents or commit messages are read.
pub(crate) fn measure(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
) -> RecordOutcome {
    let key = records_cache::cache_key(repo);
    let cache_path = (!cache_dir.is_empty())
        .then(|| records_cache::cache_path(cache_dir, &key));

    if !repo.shallow {
        if let Some(path) = cache_path.as_deref() {
            if let Some(entry) = records_cache::load(path)
                .filter(|entry| entry.matches(&key, repo))
            {
                return outcome(entry.summary().clone(), true, None, repo);
            }
        }
    }

    match measure_uncached(repo, budget) {
        Ok(summary) => {
            let issue = repo.shallow.then(|| {
                git::issue(
                    spec,
                    "shallow_records",
                    "repository is shallow; record counts and author-time bounds cover only reachable commits".to_owned(),
                )
            });
            if !repo.shallow {
                if let Some(path) = cache_path.as_deref() {
                    records_cache::persist(
                        path,
                        &RecordCacheEntry::new(key, repo, summary.clone()),
                    );
                }
            }
            outcome(summary, !repo.shallow, issue, repo)
        }
        Err(message) => {
            let issue = git::issue(spec, "records_snapshot_failed", message);
            let mut result =
                outcome(RecordSummary::default(), false, Some(issue), repo);
            result.snapshot = ProjectStatsSnapshotWire::default();
            result
        }
    }
}

fn outcome(
    summary: RecordSummary,
    complete: bool,
    issue: Option<ProjectStatsIssueWire>,
    repo: &ResolvedRepo,
) -> RecordOutcome {
    RecordOutcome {
        snapshot: ProjectStatsSnapshotWire {
            tracked_files: summary.tracked_files,
            lifetime_commits: Some(summary.lifetime_commits),
            first_commit_ts: summary.first_commit_ts,
            last_commit_ts: summary.last_commit_ts,
            ..ProjectStatsSnapshotWire::default()
        },
        complete,
        issue,
        source_token: format!(
            "records:v1:{}:{}:{}:{}:{}",
            repo.identity,
            repo.common_dir,
            repo.tip,
            repo.shallow,
            repo.fetched_at.unwrap_or_default()
        ),
    }
}

fn measure_uncached(
    repo: &ResolvedRepo,
    budget: &ProjectStatsBudgetWire,
) -> Result<RecordSummary, String> {
    if repo.tip.is_empty() {
        return Ok(RecordSummary::default());
    }
    let root = Path::new(&repo.canonical_path);
    let commits = checked(root, &["rev-list", "--count", &repo.tip], budget)?;
    let lifetime_commits = parse_one_u64(&commits.stdout, "commit count")?;
    let tree = checked(
        root,
        &["ls-tree", "-r", "-z", "--full-tree", &repo.tip],
        budget,
    )?;
    let tracked_files = count_blob_entries(&tree.stdout)?;
    let log = checked(root, &["log", "--format=%at", &repo.tip], budget)?;
    let (first_commit_ts, last_commit_ts) = author_time_bounds(&log.stdout)?;
    Ok(RecordSummary {
        tracked_files,
        lifetime_commits,
        first_commit_ts,
        last_commit_ts,
    })
}

fn checked(
    root: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    let result = git::run_unchecked(root, args, budget)?;
    if result.truncated {
        return Err(format!(
            "git {} exceeded its output budget",
            args.join(" ")
        ));
    }
    if result.code != Some(0) {
        return Err(format!("git {} failed", args.join(" ")));
    }
    Ok(result)
}

fn parse_one_u64(bytes: &[u8], label: &str) -> Result<u64, String> {
    std::str::from_utf8(bytes)
        .map_err(|error| format!("Git returned invalid {label}: {error}"))?
        .trim()
        .parse()
        .map_err(|error| format!("Git returned invalid {label}: {error}"))
}

fn count_blob_entries(bytes: &[u8]) -> Result<u64, String> {
    let mut count = 0_u64;
    for entry in bytes
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let metadata = entry
            .split(|byte| *byte == b'\t')
            .next()
            .ok_or_else(|| "Git returned a malformed tree entry".to_owned())?;
        let fields: Vec<&[u8]> = metadata
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|field| !field.is_empty())
            .collect();
        if fields.len() != 3 {
            return Err("Git returned a malformed tree entry".to_owned());
        }
        if fields[1] == b"blob" {
            count = count
                .checked_add(1)
                .ok_or_else(|| "tracked file count overflowed".to_owned())?;
        }
    }
    Ok(count)
}

fn author_time_bounds(
    bytes: &[u8],
) -> Result<(Option<i64>, Option<i64>), String> {
    let mut times = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let text = std::str::from_utf8(line).map_err(|error| {
            format!("Git returned an invalid author timestamp: {error}")
        })?;
        times.push(text.parse::<i64>().map_err(|error| {
            format!("Git returned an invalid author timestamp: {error}")
        })?);
    }
    Ok((times.iter().min().copied(), times.iter().max().copied()))
}
