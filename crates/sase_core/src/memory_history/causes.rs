//! Instruction cause attribution over one batched diff-tree.
//!
//! [`attribute_instruction_causes`] refines the instruction versions
//! [`classify_subjects`](super::classify::classify_subjects) left
//! behind. Memory sources are already on `cause.sources`; they come
//! from the file-history walk. Config and renderer paths live outside
//! the walk pathspecs, so for the set of commits that produced an
//! instruction version this runs one batched
//!
//! ```text
//! git diff-tree --stdin --root -r --name-only -z
//! ```
//!
//! through a bounded runner with the same flags the
//! [`crate::file_history`] runner pins (`--no-optional-locks`, no
//! fetch). A path matches `config_paths` by equality. A path matches
//! a renderer prefix when it equals the prefix or starts with the
//! prefix plus `/` (a trailing `/` on the prefix is ignored). A
//! `RegenOnly` instruction version with no memory source but a config
//! or renderer match becomes `Config`; `cause.regen_only` is true only
//! when sources, config paths, and renderer paths are all empty.
//! Generated subjects and other instruction subjects never count as
//! sources: classification already excludes them.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::subjects::memory_history_pathspecs;
use super::wire::{
    MemoryHistoryClassWire, MemoryHistoryError, MemoryHistoryScopeWire,
    MemoryHistorySubjectKindWire, MemoryHistorySubjectWire,
};

/// Whole-session timeout for the batched diff-tree.
const DIFF_TREE_TIMEOUT: Duration = Duration::from_millis(30_000);
/// Stdout byte cap for the batched diff-tree.
const DIFF_TREE_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Refine instruction causes in place. Memory sources stay as
/// classification left them; config paths, renderer paths, the
/// `RegenOnly` to `Config` transition, and `cause.regen_only` are
/// filled here. Rows are never reordered and non-instruction rows are
/// never touched.
pub fn attribute_instruction_causes(
    repo: &Path,
    subjects: &mut [MemoryHistorySubjectWire],
    scope: &MemoryHistoryScopeWire,
) -> Result<(), MemoryHistoryError> {
    let _ = memory_history_pathspecs(scope)?;
    let commits = instruction_commits(subjects);
    if commits.is_empty() {
        return Ok(());
    }
    let changed = batched_diff_tree(repo, &commits)?;
    for subject in subjects.iter_mut() {
        if subject.kind != MemoryHistorySubjectKindWire::Instructions {
            continue;
        }
        for version in subject.versions.iter_mut() {
            let empty: Vec<String> = Vec::new();
            let paths = changed.get(&version.commit).unwrap_or(&empty);
            let config_paths = match_config_paths(paths, scope);
            let renderer_paths = match_renderer_paths(paths, scope);
            version.cause.config_paths = config_paths;
            version.cause.renderer_paths = renderer_paths;
            if version.class == MemoryHistoryClassWire::RegenOnly
                && version.cause.sources.is_empty()
                && (!version.cause.config_paths.is_empty()
                    || !version.cause.renderer_paths.is_empty())
            {
                version.class = MemoryHistoryClassWire::Config;
            }
            version.cause.regen_only = version.cause.sources.is_empty()
                && version.cause.config_paths.is_empty()
                && version.cause.renderer_paths.is_empty();
        }
    }
    Ok(())
}

/// Sorted unique commits behind any instruction version.
fn instruction_commits(subjects: &[MemoryHistorySubjectWire]) -> Vec<String> {
    let mut commits = BTreeSet::new();
    for subject in subjects {
        if subject.kind != MemoryHistorySubjectKindWire::Instructions {
            continue;
        }
        for version in &subject.versions {
            commits.insert(version.commit.clone());
        }
    }
    commits.into_iter().collect()
}

/// Config paths matched by equality, in diff-tree order with no
/// duplicates.
fn match_config_paths(
    paths: &[String],
    scope: &MemoryHistoryScopeWire,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut matched = Vec::new();
    for path in paths {
        if scope.config_paths.iter().any(|entry| entry == path)
            && seen.insert(path.clone())
        {
            matched.push(path.clone());
        }
    }
    matched
}

/// Renderer paths matched by prefix, in diff-tree order with no
/// duplicates. A trailing `/` on a prefix is ignored so `src/sase/amd/`
/// and `src/sase/amd` both match `src/sase/amd/render.rs`.
fn match_renderer_paths(
    paths: &[String],
    scope: &MemoryHistoryScopeWire,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut matched = Vec::new();
    for path in paths {
        if scope.renderer_prefixes.iter().any(|prefix| {
            let base = prefix.trim_end_matches('/');
            path == base || path.starts_with(&format!("{base}/"))
        }) && seen.insert(path.clone())
        {
            matched.push(path.clone());
        }
    }
    matched
}

/// Changed paths per commit from one batched
/// `git diff-tree --stdin --root -r --name-only -z` session. Commits
/// absent from the output map to no entry; callers treat that as an
/// empty path list.
fn batched_diff_tree(
    repo: &Path,
    commits: &[String],
) -> Result<BTreeMap<String, Vec<String>>, MemoryHistoryError> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=off")
        .arg("-c")
        .arg("diff.renames=true")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repo)
        .arg("diff-tree")
        .arg("--stdin")
        .arg("--root")
        .arg("-r")
        .arg("--name-only")
        .arg("-z")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            MemoryHistoryError::GitFailed(format!("spawn diff-tree: {error}"))
        })?;
    let deadline = Instant::now() + DIFF_TREE_TIMEOUT;
    if let Some(mut stdin) = child.stdin.take() {
        for commit in commits {
            if Instant::now() >= deadline
                || writeln!(stdin, "{commit}").is_err()
            {
                break;
            }
        }
    }
    let output = child.wait_with_output().map_err(|error| {
        MemoryHistoryError::GitFailed(format!("wait diff-tree: {error}"))
    })?;
    if output.status.code() != Some(0) {
        return Err(MemoryHistoryError::GitFailed(format!(
            "diff-tree exited with {}",
            output
                .status
                .code()
                .map_or("signal".to_string(), |code| code.to_string()),
        )));
    }
    let mut stdout = output.stdout;
    stdout.truncate(
        usize::try_from(DIFF_TREE_MAX_BYTES)
            .unwrap_or(usize::MAX)
            .min(stdout.len()),
    );
    Ok(parse_diff_tree_nul(&stdout, commits))
}

/// Split NUL-separated diff-tree output into paths per commit. Each
/// commit's record starts with its SHA; later tokens are paths until
/// the next known SHA. A token equal to a requested SHA always starts
/// a new record.
fn parse_diff_tree_nul(
    stdout: &[u8],
    commits: &[String],
) -> BTreeMap<String, Vec<String>> {
    let wanted: BTreeSet<&str> = commits.iter().map(String::as_str).collect();
    let mut changed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for token in stdout.split(|byte| *byte == 0) {
        if token.is_empty() {
            continue;
        }
        let text = String::from_utf8_lossy(token).into_owned();
        if wanted.contains(text.as_str()) {
            current = Some(text.clone());
            changed.entry(text).or_default();
        } else if let Some(commit) = current.as_ref() {
            changed.entry(commit.clone()).or_default().push(text);
        }
    }
    changed
}
