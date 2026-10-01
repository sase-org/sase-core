//! Upstream-ahead marker for memory history.
//!
//! [`upstream_ahead`] reports how far the local remote-tracking
//! default is ahead of HEAD over the walked pathspecs, without ever
//! fetching: `None` with no origin ref or on any git failure,
//! `Some(0)` when the checkout is not behind.

use std::path::Path;
use std::time::Duration;

use crate::file_history::{run_git_unchecked, FileHistoryBudgetWire};

/// Commits the remote-tracking default is ahead of HEAD over
/// *pathspecs*. Resolves the default with
/// `git symbolic-ref --quiet --short refs/remotes/origin/HEAD`, then
/// counts `git rev-list --first-parent --count HEAD..<ref> --
/// <pathspecs>`. Never fetches; any git failure becomes `None` and
/// never fails sync.
pub fn upstream_ahead(repo: &Path, pathspecs: &[String]) -> Option<u64> {
    let budget = FileHistoryBudgetWire::default();
    let timeout = Duration::from_millis(budget.timeout_ms.max(1));
    let pointed = run_git_unchecked(
        repo,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
        timeout,
        64 * 1024,
    )
    .ok()?;
    if pointed.code != Some(0) {
        return None;
    }
    let upstream = String::from_utf8_lossy(&pointed.stdout).trim().to_string();
    if upstream.is_empty() {
        return None;
    }
    let range = format!("HEAD..{upstream}");
    let mut argv: Vec<String> = vec![
        "rev-list".to_string(),
        "--first-parent".to_string(),
        "--count".to_string(),
        range,
        "--".to_string(),
    ];
    argv.extend(pathspecs.iter().cloned());
    let arg_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let counted =
        run_git_unchecked(repo, &arg_refs, timeout, 64 * 1024).ok()?;
    if counted.code != Some(0) {
        return None;
    }
    String::from_utf8_lossy(&counted.stdout)
        .trim()
        .parse::<u64>()
        .ok()
}
