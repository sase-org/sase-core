//! Cache file location for the bead read model (plan P2).
//!
//! The read model lives under the clone's git dir at
//! `<git-dir>/sase/bead-read-model/<key>.sqlite` (WAL), never inside the
//! working tree, so no commit path, store copy, adoption copy, or stale
//! ignore rule can ever stage or copy it. The name `beads.db` is never
//! used: that file is the mutation flock.
//!
//! `<key>` hashes the store's path relative to the work-tree root, so two
//! checkouts of the same repo (or two stores under one root) never share a
//! cache file. Only `<beads_dir>/.git` and `<beads_dir>/../.git` are
//! probed, each either a directory or a `gitdir:` pointer file. A store
//! with no git dir there is served by plain replay.

use std::fs;
use std::path::{Path, PathBuf};

use crate::bead::jsonl::hex_signature;

/// Directory under the git dir holding read-model caches.
const READ_MODEL_DIR_NAME: &str = "bead-read-model";
/// Filename stem for read-model caches.
const READ_MODEL_FILE_STEM: &str = "bead-read-model";
/// Key length (hex chars) in the cache filename.
const READ_MODEL_KEY_LEN: usize = 16;

/// Resolve the read-model cache path for the store at `beads_dir`.
///
/// Returns `None` when no git dir is found, in which case reads fall back
/// to plain replay. The discovery is purely lexical plus at most two
/// filesystem probes; it never shells out to git.
pub fn read_model_cache_path_for_store(beads_dir: &Path) -> Option<PathBuf> {
    let (git_dir, work_root) = find_git_dir(beads_dir)?;
    let key = cache_key(beads_dir, &work_root);
    Some(
        git_dir
            .join("sase")
            .join(READ_MODEL_DIR_NAME)
            .join(format!("{READ_MODEL_FILE_STEM}-{key}.sqlite")),
    )
}

/// Locate the git dir and work-tree root for `beads_dir`, if any.
fn find_git_dir(beads_dir: &Path) -> Option<(PathBuf, PathBuf)> {
    for candidate in [beads_dir.join(".git"), beads_dir.join("../.git")] {
        if let Some(resolved) = resolve_git_candidate(&candidate) {
            let work_root = work_root_for_git_dir(&candidate, &resolved)?;
            return Some((resolved, work_root));
        }
    }
    None
}

/// Resolve one `.git` candidate to its real git dir, if it exists.
///
/// A candidate is either a directory or a `gitdir: <path>` pointer file
/// (worktrees, submodules). Anything else is not a git dir. The result
/// is canonicalized so later prefix comparisons never trip over a
/// lexical `..` (the `<beads_dir>/../.git` probe) or a symlinked
/// ancestor.
fn resolve_git_candidate(candidate: &Path) -> Option<PathBuf> {
    if candidate.is_dir() {
        return fs::canonicalize(candidate).ok();
    }
    if !candidate.is_file() {
        return None;
    }
    let first_line = fs::read_to_string(candidate)
        .ok()?
        .lines()
        .next()?
        .trim()
        .to_string();
    let target = first_line.strip_prefix("gitdir:")?.trim();
    if target.is_empty() {
        return None;
    }
    let target_path = PathBuf::from(target);
    let resolved = if target_path.is_absolute() {
        target_path
    } else {
        candidate
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(target_path)
    };
    if !resolved.is_dir() {
        return None;
    }
    fs::canonicalize(resolved).ok()
}

/// Derive the work-tree root from a canonical resolved git dir.
///
/// For a plain `<root>/.git` directory the root is its parent. For a
/// pointer target containing a `.git` path component (worktrees store
/// under `<root>/.git/worktrees/<name>`, submodules under
/// `<root>/.git/modules/<path>`), the root is everything before that
/// component. Otherwise the candidate's own ancestry is the best
/// estimate.
fn work_root_for_git_dir(candidate: &Path, resolved: &Path) -> Option<PathBuf> {
    if resolved.file_name().and_then(|name| name.to_str()) == Some(".git") {
        return resolved.parent().map(Path::to_path_buf);
    }
    let mut prefix = PathBuf::new();
    let mut found = false;
    for component in resolved.components() {
        if component.as_os_str() == ".git" {
            found = true;
            break;
        }
        prefix.push(component);
    }
    if found && !prefix.as_os_str().is_empty() {
        return Some(prefix);
    }
    let mut ancestor = candidate.to_path_buf();
    ancestor.pop();
    ancestor.pop();
    fs::canonicalize(&ancestor).ok()
}

/// Hash the store's path relative to the work-tree root into the key.
///
/// Both sides are canonicalized first so a symlinked ancestor (macOS
/// `/tmp`, for example) does not fork the key; when canonicalization
/// fails the raw paths are hashed instead.
fn cache_key(beads_dir: &Path, work_root: &Path) -> String {
    let canonical_store =
        fs::canonicalize(beads_dir).unwrap_or_else(|_| beads_dir.to_path_buf());
    let canonical_root =
        fs::canonicalize(work_root).unwrap_or_else(|_| work_root.to_path_buf());
    let relative = canonical_store
        .strip_prefix(&canonical_root)
        .map(Path::to_path_buf)
        .unwrap_or(canonical_store);
    hex_signature(relative.to_string_lossy().as_bytes())[..READ_MODEL_KEY_LEN]
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_git_dir_means_no_cache_path() {
        let temp = tempfile::tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        assert_eq!(read_model_cache_path_for_store(&beads_dir), None);
    }
}
