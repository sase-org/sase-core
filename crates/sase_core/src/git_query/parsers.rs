//! Pure-Rust ports of the five Phase 5 Git query parsers.
//!
//! Mirrors `sase_100/src/sase/core/git_query_facade.py` byte-for-byte.
//! Every helper here is a pure function over strings already produced by
//! `git`; no subprocess execution, no filesystem access, no `.git`
//! introspection — the host (Python) keeps every side effect.
//!
//! See `sase_100/plans/202604/rust_backend_phase5_git_query_ops.md` and
//! the Phase 5B handoff for the contract these functions must match.
//! The Phase 5B golden test file
//! `sase_100/tests/test_core_git_query.py` is the authoritative parity
//! oracle; the Rust unit tests below mirror every case.

use super::wire::GitNameStatusEntryWire;

/// Parse the NUL-delimited output of `git diff --name-status -z`.
///
/// The input is a stream of NUL-separated fields: a status token
/// (e.g. `A`, `M`, `D`, `R100`, `C75`, `T`, `U`) followed by one or two
/// paths. Rename/copy entries (`R` / `C`) are followed by *two* paths
/// (old, new); all other statuses are followed by exactly one. Trailing
/// NUL terminators are tolerated. Truncated streams (a status with no
/// following path) are silently dropped so a single malformed entry
/// does not poison the whole list. A truncated rename with only one
/// remaining path falls through to the single-path branch — matching
/// the forgiving Python behavior pinned in Phase 5B.
pub fn parse_git_name_status_z(stdout: &str) -> Vec<GitNameStatusEntryWire> {
    if stdout.is_empty() {
        return Vec::new();
    }
    let mut parts: Vec<&str> = stdout.split('\0').collect();
    if parts.last().map(|s| s.is_empty()).unwrap_or(false) {
        parts.pop();
    }

    let mut result: Vec<GitNameStatusEntryWire> = Vec::new();
    let mut i = 0usize;
    while i < parts.len() {
        let status = parts[i];
        i += 1;
        if status.is_empty() {
            continue;
        }
        let first_letter = status.as_bytes()[0];
        if (first_letter == b'R' || first_letter == b'C') && i + 1 < parts.len()
        {
            let old_path = parts[i];
            let new_path = parts[i + 1];
            i += 2;
            result.push(GitNameStatusEntryWire {
                status: status.to_string(),
                path: format!("{old_path}\t{new_path}"),
            });
        } else if i < parts.len() {
            let path = parts[i];
            i += 1;
            result.push(GitNameStatusEntryWire {
                status: status.to_string(),
                path: path.to_string(),
            });
        }
    }
    result
}

/// Normalize `git rev-parse --abbrev-ref HEAD` stdout into a branch
/// name.
///
/// Returns `None` when the stripped stdout is empty or equals `"HEAD"`
/// (the Git provider's detached-HEAD sentinel). Otherwise returns the
/// trimmed branch name verbatim.
pub fn parse_git_branch_name(stdout: &str) -> Option<String> {
    let name = stdout.trim();
    if name.is_empty() || name == "HEAD" {
        None
    } else {
        Some(name.to_string())
    }
}

/// Derive a workspace name from a remote URL or repository root path.
///
/// Mirrors `derive_git_workspace_name` in
/// `sase_100/src/sase/core/git_query_facade.py`. The Git provider
/// prefers `git config --get remote.origin.url` and falls back to
/// `git rev-parse --show-toplevel` when the remote is unset. This
/// helper preserves that priority:
///
/// - When `remote_url` is non-empty after stripping, take its basename
///   (the segment after the last `/`) and drop a single trailing
///   `.git` suffix if present. SSH-style remotes
///   (`git@host:owner/repo.git`) and path-like remotes
///   (`/srv/git/repo`) are handled the same way because the basename
///   split is purely on `/`.
/// - Otherwise, when `root_path` is non-empty after stripping, take
///   its basename via the trailing path segment.
/// - `None` indicates neither input produced a non-empty name; the
///   Git provider surfaces this as a soft failure.
pub fn derive_git_workspace_name(
    remote_url: Option<&str>,
    root_path: Option<&str>,
) -> Option<String> {
    if let Some(url) = remote_url {
        let url = url.trim();
        if !url.is_empty() {
            // Use forward-slash splitting so SSH-style ``git@host:owner/repo.git``
            // and URLs with ``://`` schemes both fall back to the
            // trailing path segment.
            let after_slash = match url.rsplit_once('/') {
                Some((_, tail)) => tail,
                None => url,
            };
            let name = after_slash.strip_suffix(".git").unwrap_or(after_slash);
            if !name.is_empty() {
                return Some(name.to_string());
            }
            return None;
        }
    }
    if let Some(root) = root_path {
        let root = root.trim();
        if !root.is_empty() {
            let name = path_basename(root);
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

/// Posix-style basename for the filesystem-fallback branch of
/// [`derive_git_workspace_name`]. Mirrors `os.path.basename` for
/// forward-slash paths used in tests/Linux call sites.
fn path_basename(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((_, tail)) => tail,
        None => path,
    }
}

/// Split `git diff --name-only --diff-filter=U` stdout into paths.
///
/// Returns an empty list when `stdout` is empty (or contains only blank
/// lines) so callers do not have to special-case "no conflicts".
/// Whitespace-only lines are dropped; non-empty paths are preserved
/// verbatim.
pub fn parse_git_conflicted_files(stdout: &str) -> Vec<String> {
    stdout
        .split('\n')
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect()
}

/// Normalize `git status --porcelain` stdout into a clean/dirty signal.
///
/// Returns `None` when the stripped stdout is empty (clean tree); the
/// original stripped text otherwise so the Git provider can echo it
/// back as a dirty-tree summary. Mirrors the behavior of
/// `vcs_has_local_changes` on a successful command run.
pub fn parse_git_local_changes(stdout: &str) -> Option<String> {
    let text = stdout.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(status: &str, path: &str) -> GitNameStatusEntryWire {
        GitNameStatusEntryWire {
            status: status.to_string(),
            path: path.to_string(),
        }
    }

    // -- parse_git_name_status_z ------------------------------------------

    #[test]
    fn name_status_empty_stream_returns_empty() {
        assert!(parse_git_name_status_z("").is_empty());
    }

    #[test]
    fn name_status_trailing_nul_is_ignored() {
        let stream = "M\0src/a.py\0";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![entry("M", "src/a.py")]
        );
    }

    #[test]
    fn name_status_simple_status_letters() {
        let stream =
            "A\0added.py\0M\0modified.py\0D\0deleted.py\0T\0type_changed.py\0U\0conflicted.py\0";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![
                entry("A", "added.py"),
                entry("M", "modified.py"),
                entry("D", "deleted.py"),
                entry("T", "type_changed.py"),
                entry("U", "conflicted.py"),
            ]
        );
    }

    #[test]
    fn name_status_rename_with_score_carries_paired_paths() {
        let stream = "R100\0old_name.py\0new_name.py\0";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![entry("R100", "old_name.py\tnew_name.py")]
        );
    }

    #[test]
    fn name_status_copy_with_score_carries_paired_paths() {
        let stream = "C75\0src/orig.py\0src/copy.py\0";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![entry("C75", "src/orig.py\tsrc/copy.py")]
        );
    }

    #[test]
    fn name_status_mixed_simple_and_rename_in_one_stream() {
        let stream = "M\0a.py\0R100\0b_old.py\0b_new.py\0D\0c.py\0";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![
                entry("M", "a.py"),
                entry("R100", "b_old.py\tb_new.py"),
                entry("D", "c.py"),
            ]
        );
    }

    #[test]
    fn name_status_truncated_status_only_drops_entry() {
        let stream = "M\0a.py\0M";
        assert_eq!(parse_git_name_status_z(stream), vec![entry("M", "a.py")]);
    }

    #[test]
    fn name_status_truncated_rename_falls_back_to_single_path() {
        let stream = "M\0a.py\0R100\0only_one_path.py";
        assert_eq!(
            parse_git_name_status_z(stream),
            vec![entry("M", "a.py"), entry("R100", "only_one_path.py"),]
        );
    }

    #[test]
    fn name_status_skips_empty_status_tokens() {
        let stream = "\0M\0a.py\0";
        assert_eq!(parse_git_name_status_z(stream), vec![entry("M", "a.py")]);
    }

    // -- parse_git_branch_name --------------------------------------------

    #[test]
    fn branch_name_simple_value() {
        assert_eq!(
            parse_git_branch_name("feature-x\n"),
            Some("feature-x".to_string())
        );
    }

    #[test]
    fn branch_name_detached_head_returns_none() {
        assert_eq!(parse_git_branch_name("HEAD\n"), None);
    }

    #[test]
    fn branch_name_empty_stdout_returns_none() {
        assert_eq!(parse_git_branch_name(""), None);
    }

    #[test]
    fn branch_name_whitespace_only_returns_none() {
        assert_eq!(parse_git_branch_name("   \n"), None);
    }

    #[test]
    fn branch_name_strips_surrounding_whitespace() {
        assert_eq!(
            parse_git_branch_name("  feature-x  \n"),
            Some("feature-x".to_string())
        );
    }

    // -- derive_git_workspace_name ---------------------------------------

    #[test]
    fn workspace_name_https_remote_with_dot_git() {
        assert_eq!(
            derive_git_workspace_name(
                Some("https://github.com/sase-org/sase_100.git"),
                None
            ),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_https_remote_without_dot_git() {
        assert_eq!(
            derive_git_workspace_name(
                Some("https://github.com/sase-org/sase_100"),
                None
            ),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_ssh_remote_with_dot_git() {
        assert_eq!(
            derive_git_workspace_name(
                Some("git@github.com:sase-org/sase_100.git"),
                None
            ),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_path_like_remote() {
        assert_eq!(
            derive_git_workspace_name(Some("/srv/git/sase_100.git"), None),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_falls_back_to_root_when_remote_blank() {
        assert_eq!(
            derive_git_workspace_name(
                Some(""),
                Some("/home/user/projects/sase_100")
            ),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_falls_back_to_root_when_remote_none() {
        assert_eq!(
            derive_git_workspace_name(
                None,
                Some("/home/user/projects/sase_100")
            ),
            Some("sase_100".to_string())
        );
    }

    #[test]
    fn workspace_name_remote_takes_priority_over_root() {
        assert_eq!(
            derive_git_workspace_name(
                Some("https://github.com/sase-org/repo_a.git"),
                Some("/tmp/repo_b")
            ),
            Some("repo_a".to_string())
        );
    }

    #[test]
    fn workspace_name_returns_none_when_both_inputs_empty() {
        assert_eq!(derive_git_workspace_name(None, None), None);
        assert_eq!(derive_git_workspace_name(Some(""), Some("")), None);
    }

    #[test]
    fn workspace_name_remote_dot_git_only_returns_none() {
        assert_eq!(
            derive_git_workspace_name(Some(".git"), Some("/tmp/fallback")),
            None
        );
    }

    // -- parse_git_conflicted_files --------------------------------------

    #[test]
    fn conflicted_files_empty_stdout_returns_empty() {
        assert!(parse_git_conflicted_files("").is_empty());
    }

    #[test]
    fn conflicted_files_strips_blank_lines() {
        let stdout = "src/a.py\n\nsrc/b.py\n\n";
        assert_eq!(
            parse_git_conflicted_files(stdout),
            vec!["src/a.py".to_string(), "src/b.py".to_string()]
        );
    }

    #[test]
    fn conflicted_files_preserves_path_order() {
        let stdout = "z.py\na.py\nm.py\n";
        assert_eq!(
            parse_git_conflicted_files(stdout),
            vec!["z.py".to_string(), "a.py".to_string(), "m.py".to_string()]
        );
    }

    #[test]
    fn conflicted_files_only_blank_lines_returns_empty() {
        assert!(parse_git_conflicted_files("\n\n   \n").is_empty());
    }

    // -- parse_git_local_changes ------------------------------------------

    #[test]
    fn local_changes_clean_tree_returns_none() {
        assert_eq!(parse_git_local_changes(""), None);
    }

    #[test]
    fn local_changes_whitespace_only_returns_none() {
        assert_eq!(parse_git_local_changes("   \n"), None);
    }

    #[test]
    fn local_changes_dirty_tree_returns_stripped_text() {
        let stdout = "M src/a.py\n?? new.py\n";
        assert_eq!(
            parse_git_local_changes(stdout),
            Some("M src/a.py\n?? new.py".to_string())
        );
    }
}
