//! Worktree, index, and HEAD status for one repo-relative path.
//!
//! Classification is `tracked` when the index names the path,
//! otherwise `ignored` when the ignore rules match it, otherwise
//! `untracked` (including paths absent from the worktree entirely),
//! or `no_vcs` outside a git work tree. The worktree OID is hashed
//! in-process from file bytes in git blob format, using the repo's own
//! object format.

use std::path::Path;
use std::time::Duration;

use sha1::Sha1;
use sha2::{Digest as _, Sha256};

use super::runner::{
    nonzero_oid, run_git_checked, run_git_unchecked, safe_pathspec,
};
use super::wire::{
    FileHistoryBudgetWire, FileHistoryError, PathStateWire, PathStatusWire,
};

/// Report worktree, index, and tracked state for *relpath*.
pub fn path_status(
    repo_root: &Path,
    relpath: &str,
    budget: &FileHistoryBudgetWire,
) -> Result<PathStatusWire, FileHistoryError> {
    if !safe_pathspec(relpath) {
        return Err(FileHistoryError::InvalidPath(relpath.to_string()));
    }
    if !repo_root.is_dir() {
        return Err(FileHistoryError::InvalidPath(path_string(repo_root)));
    }
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let toplevel = match run_git_checked(
        repo_root,
        &["rev-parse", "--show-toplevel"],
        timeout,
        64 * 1024,
    ) {
        Ok(result) => {
            let text =
                String::from_utf8_lossy(&result.stdout).trim().to_string();
            if text.is_empty() {
                return Ok(no_vcs(relpath));
            }
            text
        }
        Err(_) => return Ok(no_vcs(relpath)),
    };
    let toplevel = Path::new(&toplevel);
    let index_oid = index_oid_for(toplevel, relpath, timeout)?;
    let object_format = object_format_for(toplevel, timeout);
    let worktree_oid = std::fs::read(toplevel.join(relpath))
        .ok()
        .and_then(|bytes| hash_worktree_blob(&object_format, &bytes).ok());
    let head_oid = head_oid_for(toplevel, relpath, timeout)?;
    let state = if index_oid.is_some() {
        PathStateWire::Tracked
    } else if is_ignored(toplevel, relpath, timeout)? {
        PathStateWire::Ignored
    } else {
        PathStateWire::Untracked
    };
    Ok(PathStatusWire {
        state,
        path: relpath.to_string(),
        index_oid,
        worktree_oid,
        head_oid,
    })
}

/// Hash worktree bytes exactly as git names a blob: `blob <len>\0`
/// plus content, in the repo's object format.
pub fn hash_worktree_blob(
    object_format: &str,
    bytes: &[u8],
) -> Result<String, FileHistoryError> {
    let header = format!("blob {}\0", bytes.len());
    match object_format {
        "sha1" => {
            let mut hasher = Sha1::new();
            hasher.update(header.as_bytes());
            hasher.update(bytes);
            Ok(hex::encode(hasher.finalize()))
        }
        "sha256" => {
            let mut hasher = Sha256::new();
            hasher.update(header.as_bytes());
            hasher.update(bytes);
            Ok(hex::encode(hasher.finalize()))
        }
        other => Err(FileHistoryError::GitFailed(format!(
            "unsupported object format: {other}"
        ))),
    }
}

fn no_vcs(relpath: &str) -> PathStatusWire {
    PathStatusWire {
        state: PathStateWire::NoVcs,
        path: relpath.to_string(),
        index_oid: None,
        worktree_oid: None,
        head_oid: None,
    }
}

fn object_format_for(toplevel: &Path, timeout: Duration) -> String {
    run_git_checked(
        toplevel,
        &["rev-parse", "--show-object-format"],
        timeout,
        1024,
    )
    .ok()
    .map(|result| String::from_utf8_lossy(&result.stdout).trim().to_string())
    .filter(|format| !format.is_empty())
    .unwrap_or_else(|| "sha1".to_string())
}

fn index_oid_for(
    toplevel: &Path,
    relpath: &str,
    timeout: Duration,
) -> Result<Option<String>, FileHistoryError> {
    let result = run_git_checked(
        toplevel,
        &["ls-files", "-s", "-z", "--", relpath],
        timeout,
        1024 * 1024,
    )?;
    // `<mode> <oid> <stage>\t<path>\0` per record; prefer stage 0.
    let mut fallback = None;
    for record in result.stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
            continue;
        };
        let left = String::from_utf8_lossy(&record[..tab]);
        let parts: Vec<&str> = left.split(' ').collect();
        if parts.len() != 3 {
            continue;
        }
        let oid = nonzero_oid(parts[1]);
        if parts[2] == "0" {
            return Ok(oid);
        }
        if fallback.is_none() {
            fallback = oid;
        }
    }
    Ok(fallback)
}

fn head_oid_for(
    toplevel: &Path,
    relpath: &str,
    timeout: Duration,
) -> Result<Option<String>, FileHistoryError> {
    let result = run_git_unchecked(
        toplevel,
        &["ls-tree", "-z", "HEAD", "--", relpath],
        timeout,
        1024 * 1024,
    )?;
    if result.code != Some(0) {
        return Ok(None);
    }
    // `<mode> <type> <oid>\t<path>\0`; only blobs name content.
    for record in result.stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
            continue;
        };
        let left = String::from_utf8_lossy(&record[..tab]);
        let parts: Vec<&str> = left.split(' ').collect();
        if parts.len() == 3 && parts[1] == "blob" {
            return Ok(nonzero_oid(parts[2]));
        }
    }
    Ok(None)
}

fn is_ignored(
    toplevel: &Path,
    relpath: &str,
    timeout: Duration,
) -> Result<bool, FileHistoryError> {
    let result = run_git_unchecked(
        toplevel,
        &["check-ignore", "-q", "--", relpath],
        timeout,
        1024,
    )?;
    Ok(result.code == Some(0))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_hash_matches_git_hash_object() {
        // `blob 12\0hello world\n` vetted against git hash-object.
        let bytes = b"hello world\n";
        assert_eq!(
            hash_worktree_blob("sha1", bytes).unwrap(),
            "3b18e512dba79e4c8300dd08aeb37f8e728b8dad"
        );
        assert_eq!(hash_worktree_blob("sha256", bytes).unwrap().len(), 64);
        assert!(hash_worktree_blob("md5", bytes).is_err());
    }
}
