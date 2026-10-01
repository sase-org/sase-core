//! Synced memory-history queries: subjects, resolve, timeline,
//! version, compare, and feed.
//!
//! Every query syncs its scope first, so a fresh cache is a stat
//! check plus a memo hit. Selectors resolve in one order everywhere:
//! exact subject id, exact repo-relative path (current or
//! historical), then unique basename; ambiguous basenames error with
//! the candidate ids. Version selectors accept an ordinal (`7` or
//! `v7`), `~N` (`~1` is the newest committed version), a unique
//! commit SHA prefix, or `now` for the worktree file.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::cache::sync_scope;
use super::feed::{build_feed, FeedScope};
use super::wire::{
    MemoryHistoryCompareRequestWire, MemoryHistoryCompareWire,
    MemoryHistoryError, MemoryHistoryFeedRequestWire,
    MemoryHistoryResolveRequestWire, MemoryHistoryResolveWire,
    MemoryHistoryScopeWire, MemoryHistorySubjectKindWire,
    MemoryHistorySubjectWire, MemoryHistorySubjectsRequestWire,
    MemoryHistorySubjectsWire, MemoryHistorySyncRequestWire,
    MemoryHistorySyncWire, MemoryHistoryTimelineRequestWire,
    MemoryHistoryTimelineWire, MemoryHistoryVersionRequestWire,
    MemoryHistoryVersionResponseWire, MemoryHistoryVersionWire,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
use crate::file_history::{
    path_status, read_blobs, run_git_unchecked, safe_revision_token,
    BlobReadBudget, FileHistoryBudgetWire,
};
use crate::prose_diff::{compare_prose, ProseCompareRequestWire};

/// Sync one scope and return the sync record.
pub fn query_sync(
    request: &MemoryHistorySyncRequestWire,
) -> Result<MemoryHistorySyncWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    Ok(outcome.sync)
}

/// Sync one scope and list its subjects without version bodies.
pub fn query_subjects(
    request: &MemoryHistorySubjectsRequestWire,
) -> Result<MemoryHistorySubjectsWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    Ok(MemoryHistorySubjectsWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        subjects: outcome
            .subjects
            .into_iter()
            .map(|mut subject| {
                subject.versions.clear();
                subject
            })
            .collect(),
    })
}

/// Resolve a selector to its subject, with the newest version or the
/// as-of version for `at_commit`. An `at_commit` revision the subject
/// did not exist at yet yields `existed: false` and no version. An
/// unsafe revision token is an error.
pub fn query_resolve(
    request: &MemoryHistoryResolveRequestWire,
) -> Result<MemoryHistoryResolveWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    let subject =
        resolve_subject(&outcome.subjects, &request.selector)?.clone();
    let (existed, version) = match request.at_commit.as_deref() {
        None => (
            !subject.versions.is_empty(),
            subject.versions.first().cloned(),
        ),
        Some(revision) => {
            if !safe_revision_token(revision) {
                return Err(MemoryHistoryError::InvalidPath(format!(
                    "unsafe revision token: {revision}"
                )));
            }
            match as_of_commit(&request.scope, &subject, revision)? {
                Some(commit) => (
                    true,
                    subject
                        .versions
                        .iter()
                        .find(|version| version.commit == commit)
                        .cloned(),
                ),
                None => (false, None),
            }
        }
    };
    // A resolved commit always names a version of this subject; a
    // missing row means history moved under the read.
    let (existed, version) = match (existed, version) {
        (true, None) if request.at_commit.is_some() => (false, None),
        other => other,
    };
    Ok(MemoryHistoryResolveWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        subject,
        existed,
        version,
    })
}

/// One subject's history: committed versions newest-first (hidden
/// ones dropped unless requested) with worktree pseudo-versions
/// listed before them. `uncommitted` appears when the worktree OID
/// differs from HEAD; `staged` only when the index OID differs from
/// both. Pseudo-versions carry ordinal `0`.
pub fn query_timeline(
    request: &MemoryHistoryTimelineRequestWire,
) -> Result<MemoryHistoryTimelineWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    let subject =
        resolve_subject(&outcome.subjects, &request.selector)?.clone();
    let current = current_path(&subject);
    let budget = FileHistoryBudgetWire::default();
    let status =
        path_status(Path::new(&request.scope.repo_root), &current, &budget)?;
    let mut versions = Vec::new();
    if status.worktree_oid != status.head_oid {
        versions.push(pseudo_version(
            &current,
            status.worktree_oid.clone(),
            status.head_oid.clone(),
            super::wire::MemoryHistoryClassWire::Uncommitted,
        ));
    }
    if status.index_oid != status.worktree_oid
        && status.index_oid != status.head_oid
        && status.index_oid.is_some()
    {
        versions.push(pseudo_version(
            &current,
            status.index_oid.clone(),
            status.head_oid.clone(),
            super::wire::MemoryHistoryClassWire::Staged,
        ));
    }
    versions.extend(
        subject
            .versions
            .iter()
            .filter(|version| {
                request.include_hidden || !version.hidden_by_default
            })
            .cloned(),
    );
    Ok(MemoryHistoryTimelineWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        subject_id: subject.id.clone(),
        path: current,
        state: status.state,
        index_oid: status.index_oid,
        worktree_oid: status.worktree_oid,
        head_oid: status.head_oid,
        diverged_count: subject.diverged_count,
        versions,
    })
}

/// One version by ordinal, `~N`, unique SHA prefix, or `now`.
/// `include_body` fetches the blob inside core; a deleted version's
/// body is its tombstone blob. `now` reads the worktree file. A
/// missing object returns an empty body with `body_missing: true`
/// rather than failing.
pub fn query_version(
    request: &MemoryHistoryVersionRequestWire,
) -> Result<MemoryHistoryVersionResponseWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    let subject =
        resolve_subject(&outcome.subjects, &request.selector)?.clone();
    let toplevel = toplevel_of(&request.scope)?;
    let (version, body, body_missing) = select_version_with_body(
        &toplevel,
        &subject,
        &request.version,
        request.include_body,
    )?;
    Ok(MemoryHistoryVersionResponseWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        subject_id: subject.id.clone(),
        version,
        body,
        body_missing,
    })
}

/// Compare two versions: both blobs are fetched inside core and the
/// `compare_prose` result rides in the memory-history envelope.
/// Assets compare as `plain`; everything else as `markdown`.
pub fn query_compare(
    request: &MemoryHistoryCompareRequestWire,
) -> Result<MemoryHistoryCompareWire, MemoryHistoryError> {
    let outcome = sync_scope(&request.scope)?;
    let toplevel = toplevel_of(&request.scope)?;
    let base_subject =
        resolve_subject(&outcome.subjects, &request.base_selector)?.clone();
    let target_subject =
        resolve_subject(&outcome.subjects, &request.target_selector)?.clone();
    let (base, base_body, base_missing) = select_version_with_body(
        &toplevel,
        &base_subject,
        &request.base_version,
        true,
    )?;
    let (target, target_body, target_missing) = select_version_with_body(
        &toplevel,
        &target_subject,
        &request.target_version,
        true,
    )?;
    let base_text = if base_missing {
        String::new()
    } else {
        base_body
    };
    let target_text = if target_missing {
        String::new()
    } else {
        target_body
    };
    let format = if base_subject.kind == MemoryHistorySubjectKindWire::Asset
        || target_subject.kind == MemoryHistorySubjectKindWire::Asset
    {
        "plain"
    } else {
        "markdown"
    };
    let comparison = compare_prose(&ProseCompareRequestWire {
        base: base_text,
        target: target_text,
        format: format.to_string(),
        context_lines: 3,
    });
    Ok(MemoryHistoryCompareWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        base_subject_id: base_subject.id.clone(),
        target_subject_id: target_subject.id.clone(),
        base,
        target,
        comparison,
    })
}

/// Sync every scope, then merge their changesets with `build_feed`.
/// `since` is epoch seconds.
pub fn query_feed(
    request: &MemoryHistoryFeedRequestWire,
) -> Result<super::wire::MemoryHistoryFeedWire, MemoryHistoryError> {
    let mut synced: Vec<(String, Vec<MemoryHistorySubjectWire>)> = Vec::new();
    for scope in &request.scopes {
        let outcome = sync_scope(scope)?;
        synced.push((scope.scope_key.clone(), outcome.subjects));
    }
    let scopes: Vec<FeedScope<'_>> = synced
        .iter()
        .map(|(key, subjects)| FeedScope {
            scope_key: key.as_str(),
            subjects,
        })
        .collect();
    Ok(build_feed(
        &scopes,
        request.since,
        request.limit,
        request.include_hidden,
    ))
}

/// Resolve a selector to its subject: exact subject id, then exact
/// repo-relative path (current or historical), then unique basename.
/// Ambiguous basenames error with the candidate ids.
pub fn resolve_subject<'a>(
    subjects: &'a [MemoryHistorySubjectWire],
    selector: &str,
) -> Result<&'a MemoryHistorySubjectWire, MemoryHistoryError> {
    if let Some(subject) =
        subjects.iter().find(|subject| subject.id == selector)
    {
        return Ok(subject);
    }
    if let Some(subject) = subjects
        .iter()
        .find(|subject| subject.paths.iter().any(|path| path == selector))
    {
        return Ok(subject);
    }
    let mut candidates: Vec<&'a MemoryHistorySubjectWire> = subjects
        .iter()
        .filter(|subject| {
            subject.paths.iter().any(|path| {
                Path::new(path)
                    .file_name()
                    .is_some_and(|name| name == selector)
            })
        })
        .collect();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));
    match candidates.len() {
        0 => Err(MemoryHistoryError::InvalidPath(format!(
            "unknown memory-history selector: {selector}"
        ))),
        1 => Ok(candidates[0]),
        _ => {
            let ids = candidates
                .iter()
                .map(|subject| subject.id.clone())
                .collect::<Vec<_>>()
                .join(", ");
            Err(MemoryHistoryError::InvalidPath(format!(
                "ambiguous basename {selector}: {ids}"
            )))
        }
    }
}

/// Newest commit at or before *revision* that touched the subject's
/// historical paths: one
/// `git log --first-parent -1 --format=%H <rev> -- <paths>`.
/// `None` when no such commit exists (the subject postdates the
/// revision, or the revision is unknown).
fn as_of_commit(
    scope: &MemoryHistoryScopeWire,
    subject: &MemoryHistorySubjectWire,
    revision: &str,
) -> Result<Option<String>, MemoryHistoryError> {
    let budget = FileHistoryBudgetWire::default();
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let mut argv: Vec<String> = vec![
        "log".to_string(),
        "--first-parent".to_string(),
        "-1".to_string(),
        "--format=%H".to_string(),
        revision.to_string(),
        "--".to_string(),
    ];
    argv.extend(subject.paths.iter().cloned());
    let arg_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let result = run_git_unchecked(
        Path::new(&scope.repo_root),
        &arg_refs,
        timeout,
        64 * 1024,
    )?;
    if result.code != Some(0) {
        return Ok(None);
    }
    let commit = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if commit.is_empty() {
        Ok(None)
    } else {
        Ok(Some(commit))
    }
}

/// Current repo-relative path of a subject: the newest non-diverged
/// version's path, falling back to the newest row.
fn current_path(subject: &MemoryHistorySubjectWire) -> String {
    subject
        .versions
        .iter()
        .find(|version| !version.diverged)
        .or(subject.versions.first())
        .map(|version| version.path.clone())
        .unwrap_or_default()
}

/// Canonical checkout top level for worktree and blob reads.
fn toplevel_of(
    scope: &MemoryHistoryScopeWire,
) -> Result<PathBuf, MemoryHistoryError> {
    let budget = FileHistoryBudgetWire::default();
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let result = crate::file_history::run_git_checked(
        Path::new(&scope.repo_root),
        &["rev-parse", "--show-toplevel"],
        timeout,
        64 * 1024,
    )
    .map_err(|_| {
        MemoryHistoryError::InvalidScope(format!(
            "not a git repository: {}",
            scope.repo_root
        ))
    })?;
    let toplevel = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if toplevel.is_empty() {
        return Err(MemoryHistoryError::InvalidScope(format!(
            "not a git repository: {}",
            scope.repo_root
        )));
    }
    Ok(PathBuf::from(toplevel))
}

/// Select a version and read its body. Committed rows read the blob
/// OID (deleted rows name their tombstone blob); `now` reads the
/// worktree file. Missing objects yield an empty body with
/// `body_missing: true`.
fn select_version_with_body(
    toplevel: &Path,
    subject: &MemoryHistorySubjectWire,
    selector: &str,
    include_body: bool,
) -> Result<(MemoryHistoryVersionWire, String, bool), MemoryHistoryError> {
    if selector == "now" {
        let current = current_path(subject);
        let mut version = pseudo_version(
            &current,
            None,
            None,
            super::wire::MemoryHistoryClassWire::Uncommitted,
        );
        let text = std::fs::read(toplevel.join(&current));
        return match text {
            Ok(bytes) => {
                let body = String::from_utf8_lossy(&bytes).into_owned();
                version.blob_oid = None;
                Ok((version, body, false))
            }
            Err(_) => Ok((version, String::new(), true)),
        };
    }
    let version = select_committed(subject, selector)?.clone();
    if !include_body {
        return Ok((version, String::new(), false));
    }
    let body = match version.blob_oid.as_deref() {
        Some(oid) => {
            let blobs = read_blobs(
                toplevel,
                &[oid.to_string()],
                &BlobReadBudget::default(),
            )?;
            match blobs.into_iter().next().flatten() {
                Some(bytes) => {
                    (String::from_utf8_lossy(&bytes).into_owned(), false)
                }
                None => (String::new(), true),
            }
        }
        None => (String::new(), true),
    };
    Ok((version, body.0, body.1))
}

/// Select one committed version: `~N` (`~1` is newest), ordinal
/// (`7` or `v7`), or unique commit SHA prefix. Rows stay
/// newest-first; ordinals still run oldest-first on the rows.
fn select_committed<'a>(
    subject: &'a MemoryHistorySubjectWire,
    selector: &str,
) -> Result<&'a MemoryHistoryVersionWire, MemoryHistoryError> {
    if let Some(rest) = selector.strip_prefix('~') {
        let position: usize = rest.parse().map_err(|_| {
            MemoryHistoryError::InvalidPath(format!(
                "invalid version selector: {selector}"
            ))
        })?;
        if position == 0 {
            return Err(MemoryHistoryError::InvalidPath(format!(
                "invalid version selector: {selector}"
            )));
        }
        return subject.versions.get(position - 1).ok_or_else(|| {
            MemoryHistoryError::InvalidPath(format!(
                "no version {selector} for subject {}",
                subject.id
            ))
        });
    }
    let ordinal_text = selector
        .strip_prefix('v')
        .or_else(|| selector.strip_prefix('V'));
    if ordinal_text.is_some()
        || selector.bytes().all(|byte| byte.is_ascii_digit())
    {
        let text = ordinal_text.unwrap_or(selector);
        if let Ok(ordinal) = text.parse::<u64>() {
            if let Some(version) = subject
                .versions
                .iter()
                .find(|version| version.ordinal == ordinal)
            {
                return Ok(version);
            }
            // A numeric selector that names no ordinal still gets a
            // chance as a SHA prefix (a full numeric SHA, for one).
            if ordinal_text.is_some() {
                return Err(MemoryHistoryError::InvalidPath(format!(
                    "no version {selector} for subject {}",
                    subject.id
                )));
            }
        }
    }
    if !safe_revision_token(selector) {
        return Err(MemoryHistoryError::InvalidPath(format!(
            "unsafe revision token: {selector}"
        )));
    }
    let lowered = selector.to_ascii_lowercase();
    let mut matches = subject.versions.iter().filter(|version| {
        version.commit.to_ascii_lowercase().starts_with(&lowered)
    });
    let Some(first) = matches.next() else {
        return Err(MemoryHistoryError::InvalidPath(format!(
            "no version {selector} for subject {}",
            subject.id
        )));
    };
    if matches.next().is_some() {
        return Err(MemoryHistoryError::InvalidPath(format!(
            "ambiguous commit prefix {selector} for subject {}",
            subject.id
        )));
    }
    Ok(first)
}

/// A query-time pseudo-version: ordinal `0`, no commit, never stored.
fn pseudo_version(
    path: &str,
    blob_oid: Option<String>,
    prev_blob_oid: Option<String>,
    class: super::wire::MemoryHistoryClassWire,
) -> MemoryHistoryVersionWire {
    MemoryHistoryVersionWire {
        ordinal: 0,
        commit: String::new(),
        parents: Vec::new(),
        committer_time: 0,
        author_time: 0,
        author_name: String::new(),
        author_email: String::new(),
        path: path.to_string(),
        blob_oid,
        prev_blob_oid,
        kind: crate::file_history::wire::FileChangeKindWire::Edited,
        similarity: None,
        gap_before: false,
        diverged: false,
        source_path: path.to_string(),
        aliased_paths: Vec::new(),
        class,
        hidden_by_default: false,
        summary: Default::default(),
        provenance: Default::default(),
        boilerplate: false,
        cause: Default::default(),
    }
}
