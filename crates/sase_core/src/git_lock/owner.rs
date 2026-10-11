//! Classify collected process facts as an active, absent, or unknown owner.

use super::delays::GitLockError;
use super::wire::{
    GitLockOwnerFactsWire, GitLockOwnerObservationWire, GitLockProcessFactWire,
    GIT_LOCK_OWNER_ABSENT, GIT_LOCK_OWNER_ACTIVE, GIT_LOCK_OWNER_UNKNOWN,
    GIT_LOCK_PROBE_COMPLETE, GIT_LOCK_WIRE_SCHEMA_VERSION,
};

/// Classify owner presence from host-collected process facts.
pub fn classify_git_lock_owner(
    facts: &GitLockOwnerFactsWire,
) -> Result<GitLockOwnerObservationWire, GitLockError> {
    if facts.schema_version != GIT_LOCK_WIRE_SCHEMA_VERSION {
        return Err(GitLockError::Schema {
            expected: GIT_LOCK_WIRE_SCHEMA_VERSION,
            actual: facts.schema_version,
        });
    }

    let mut associated = Vec::new();
    let mut ambiguous = false;
    for process in &facts.processes {
        match classify_process(process, facts) {
            ProcessRelation::Associated => associated.push(process.pid),
            ProcessRelation::Ambiguous => ambiguous = true,
            ProcessRelation::Unrelated => {}
        }
    }
    if !associated.is_empty() {
        return Ok(observation(
            GIT_LOCK_OWNER_ACTIVE,
            format!("live owner pids {associated:?}"),
        ));
    }
    if facts.probe_status != GIT_LOCK_PROBE_COMPLETE {
        return Ok(observation(
            GIT_LOCK_OWNER_UNKNOWN,
            facts.probe_reason.clone(),
        ));
    }
    if ambiguous {
        return Ok(observation(
            GIT_LOCK_OWNER_UNKNOWN,
            "git process could not be associated with a single repository",
        ));
    }
    Ok(observation(
        GIT_LOCK_OWNER_ABSENT,
        "no live owner of the resolved index lock",
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessRelation {
    Associated,
    Unrelated,
    Ambiguous,
}

fn classify_process(
    process: &GitLockProcessFactWire,
    facts: &GitLockOwnerFactsWire,
) -> ProcessRelation {
    if process.holds_lock_descriptor {
        return ProcessRelation::Associated;
    }
    if process.association_ambiguous {
        return ProcessRelation::Ambiguous;
    }
    if !is_git_process(process) {
        return ProcessRelation::Unrelated;
    }
    match repo_association(process, facts) {
        RepoMatch::Matches => ProcessRelation::Associated,
        RepoMatch::Different => ProcessRelation::Unrelated,
        RepoMatch::Unknown => ProcessRelation::Ambiguous,
    }
}

fn is_git_process(process: &GitLockProcessFactWire) -> bool {
    basename_is_git(process.executable.as_deref())
        || process
            .argv
            .first()
            .is_some_and(|arg0| basename_is_git(Some(arg0)))
}

fn basename_is_git(path: Option<&str>) -> bool {
    let Some(path) = path else {
        return false;
    };
    let name = path.rsplit(['/', '\\']).next().unwrap_or(path);
    name == "git" || name == "git.exe"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoMatch {
    Matches,
    Different,
    Unknown,
}

fn repo_association(
    process: &GitLockProcessFactWire,
    facts: &GitLockOwnerFactsWire,
) -> RepoMatch {
    let mut saw_evidence = false;
    let mut matches = false;

    if let (Some(cwd), Some(worktree)) =
        (process.cwd.as_deref(), facts.resolved_worktree.as_deref())
    {
        saw_evidence = true;
        matches |= cwd == worktree;
    }
    if let (Some(git_dir), Some(resolved)) = (
        process.git_dir_env.as_deref(),
        facts.resolved_git_dir.as_deref(),
    ) {
        saw_evidence = true;
        matches |= git_dir == resolved;
    }
    if let (Some(index_file), Some(lock)) = (
        process.git_index_file_env.as_deref(),
        facts.resolved_index_lock_path.as_deref(),
    ) {
        saw_evidence = true;
        let expected_lock = format!("{index_file}.lock");
        matches |= expected_lock == lock || index_file == lock;
    }
    if let (Some(work_tree), Some(resolved)) = (
        process.git_work_tree_env.as_deref(),
        facts.resolved_worktree.as_deref(),
    ) {
        saw_evidence = true;
        matches |= work_tree == resolved;
    }
    if let Some(path) = argv_option_value(&process.argv, "-C") {
        saw_evidence = true;
        if let Some(worktree) = facts.resolved_worktree.as_deref() {
            matches |= path == worktree;
        }
    }
    if let Some(path) = argv_option_value(&process.argv, "--git-dir") {
        saw_evidence = true;
        if let Some(git_dir) = facts.resolved_git_dir.as_deref() {
            matches |= path == git_dir;
        }
    }
    if let Some(path) = argv_option_value(&process.argv, "--work-tree") {
        saw_evidence = true;
        if let Some(worktree) = facts.resolved_worktree.as_deref() {
            matches |= path == worktree;
        }
    }

    if matches {
        RepoMatch::Matches
    } else if saw_evidence {
        RepoMatch::Different
    } else {
        RepoMatch::Unknown
    }
}

fn argv_option_value<'a>(argv: &'a [String], option: &str) -> Option<&'a str> {
    let eq_prefix = format!("{option}=");
    let mut items = argv.iter();
    while let Some(item) = items.next() {
        if item == option {
            return items.next().map(String::as_str);
        }
        if let Some(value) = item.strip_prefix(&eq_prefix) {
            return Some(value);
        }
    }
    None
}

fn observation(
    status: &str,
    reason: impl Into<String>,
) -> GitLockOwnerObservationWire {
    GitLockOwnerObservationWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        reason: reason.into(),
    }
}
