//! Repository identity, ref validation, and source metadata for project stats.
#![allow(dead_code)] // The query-binding phase wires this source into queries.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use crate::file_history::{run_git_unchecked, safe_revision_token, GitResult};

use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
};

/// A repository resolved to a verified revision and Git common directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedRepo {
    pub canonical_path: String,
    pub common_dir: String,
    pub identity: String,
    pub ref_name: String,
    pub tip: String,
    pub shallow: bool,
    pub fetched_at: Option<i64>,
}

/// Resolve one configured path and choose the first configured ref that
/// verifies to a commit. No failed candidate falls through to implicit HEAD.
pub(crate) fn resolve_repo(
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
) -> Result<ResolvedRepo, ProjectStatsIssueWire> {
    let requested = Path::new(&spec.path).canonicalize().map_err(|error| {
        issue(
            spec,
            "repo_unavailable",
            format!("repository path is unavailable: {error}"),
        )
    })?;
    if !requested.is_dir() {
        return Err(issue(
            spec,
            "repo_unavailable",
            "repository path is not a directory".to_owned(),
        ));
    }

    let top = run_output(&requested, &["rev-parse", "--show-toplevel"], budget)
        .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let top_text = output_text(top)
        .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let top_path =
        PathBuf::from(top_text.trim())
            .canonicalize()
            .map_err(|error| {
                issue(
                    spec,
                    "repo_unavailable",
                    format!("cannot canonicalize Git top level: {error}"),
                )
            })?;
    if requested != top_path {
        return Err(issue(
            spec,
            "identity_mismatch",
            format!(
                "configured path {} resolves inside repository {}",
                requested.display(),
                top_path.display()
            ),
        ));
    }

    let common_output =
        run_output(&top_path, &["rev-parse", "--git-common-dir"], budget)
            .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let common_text = output_text(common_output)
        .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let common_path = PathBuf::from(common_text.trim());
    let common_path = if common_path.is_absolute() {
        common_path
    } else {
        top_path.join(common_path)
    }
    .canonicalize()
    .map_err(|error| {
        issue(
            spec,
            "repo_unavailable",
            format!("cannot canonicalize Git common directory: {error}"),
        )
    })?;

    let shallow_output = run_output(
        &top_path,
        &["rev-parse", "--is-shallow-repository"],
        budget,
    )
    .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let shallow_text = output_text(shallow_output)
        .map_err(|message| issue(spec, "repo_unavailable", message))?;
    let shallow = match shallow_text.trim() {
        "true" => true,
        "false" => false,
        other => {
            return Err(issue(
                spec,
                "repo_unavailable",
                format!("Git returned an invalid shallow state: {other}"),
            ));
        }
    };

    let mut selected = None;
    let mut unsafe_candidate = false;
    for candidate in &spec.ref_candidates {
        if !safe_revision_token(candidate) {
            unsafe_candidate = true;
            continue;
        }
        let expression = format!("{candidate}^{{commit}}");
        let result = run_unchecked(
            &top_path,
            &["rev-parse", "--verify", "--end-of-options", &expression],
            budget,
        );
        let Ok(result) = result else {
            continue;
        };
        if result.code != Some(0) || result.truncated {
            continue;
        }
        let oid = String::from_utf8_lossy(&result.stdout).trim().to_owned();
        if valid_oid(&oid) {
            selected = Some((candidate.clone(), oid.to_ascii_lowercase()));
            break;
        }
    }

    let (ref_name, tip) = match selected {
        Some((name, tip)) => (name, tip),
        None if !unsafe_candidate && is_empty_repository(&top_path, budget) => {
            (String::new(), String::new())
        }
        None => {
            let (code, message) = if unsafe_candidate {
                (
                    "invalid_ref",
                    "no configured ref resolved; unsafe revision tokens were rejected",
                )
            } else {
                (
                    "ref_unavailable",
                    "none of the configured ref candidates resolves to a commit",
                )
            };
            return Err(issue(spec, code, message.to_owned()));
        }
    };

    let fetched_at = fetch_head_mtime(&top_path, budget);
    let canonical_path = path_string(&top_path);
    let common_dir = path_string(&common_path);
    let identity = if !spec.identity.trim().is_empty() {
        spec.identity.trim().to_owned()
    } else {
        root_identity(&top_path, &tip, budget)
            .unwrap_or_else(|| canonical_path.clone())
    };

    Ok(ResolvedRepo {
        canonical_path,
        common_dir,
        identity,
        ref_name,
        tip,
        shallow,
        fetched_at,
    })
}

pub(crate) fn issue(
    spec: &ProjectStatsRepoSpecWire,
    code: &str,
    message: String,
) -> ProjectStatsIssueWire {
    ProjectStatsIssueWire {
        repo: Some(spec.name.clone()),
        code: code.to_owned(),
        message,
        ..ProjectStatsIssueWire::default()
    }
}

pub(crate) fn run_unchecked(
    repo: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    run_git_unchecked(
        repo,
        args,
        Duration::from_millis(budget.timeout_ms.max(1)),
        budget.max_output_bytes.max(1),
    )
    .map_err(|error| error.to_string())
}

fn run_output(
    repo: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    let result = run_unchecked(repo, args, budget)?;
    if result.truncated {
        return Err(format!(
            "git {} exceeded its command budget",
            args.join(" ")
        ));
    }
    if result.code != Some(0) {
        return Err(format!("git {} did not succeed", args.join(" ")));
    }
    Ok(result)
}

fn output_text(result: GitResult) -> Result<String, String> {
    String::from_utf8(result.stdout)
        .map_err(|error| format!("Git returned non-UTF-8 metadata: {error}"))
}

fn is_empty_repository(repo: &Path, budget: &ProjectStatsBudgetWire) -> bool {
    let Ok(result) =
        run_unchecked(repo, &["rev-list", "--all", "--count"], budget)
    else {
        return false;
    };
    if result.code != Some(0) || result.truncated {
        return false;
    }
    String::from_utf8_lossy(&result.stdout).trim() == "0"
}

fn root_identity(
    repo: &Path,
    tip: &str,
    budget: &ProjectStatsBudgetWire,
) -> Option<String> {
    if tip.is_empty() {
        return None;
    }
    let result =
        run_unchecked(repo, &["rev-list", "--max-parents=0", tip], budget)
            .ok()?;
    if result.code != Some(0) || result.truncated {
        return None;
    }
    let mut roots: Vec<String> = String::from_utf8_lossy(&result.stdout)
        .lines()
        .map(str::trim)
        .filter(|oid| valid_oid(oid))
        .map(str::to_ascii_lowercase)
        .collect();
    roots.sort();
    roots.dedup();
    (!roots.is_empty()).then(|| format!("git-roots:{}", roots.join(",")))
}

fn fetch_head_mtime(
    repo: &Path,
    budget: &ProjectStatsBudgetWire,
) -> Option<i64> {
    let result =
        run_output(repo, &["rev-parse", "--git-path", "FETCH_HEAD"], budget)
            .ok()?;
    let text = output_text(result).ok()?;
    let path = PathBuf::from(text.trim());
    let path = if path.is_absolute() {
        path
    } else {
        repo.join(path)
    };
    let path = path.canonicalize().unwrap_or(path);
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|age| age.as_secs() as i64)
}

pub(crate) fn valid_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::process::Command;

    use super::*;
    use crate::project_stats::test_support::{commit, git, init_repo, spec};

    #[test]
    fn empty_repository_resolves_without_fabricating_head() {
        let fixture = init_repo();
        let resolved = resolve_repo(
            &spec(&fixture.repo),
            &ProjectStatsBudgetWire::default(),
        )
        .expect("empty repository resolves");
        assert!(resolved.tip.is_empty());
        assert!(resolved.ref_name.is_empty());
        assert!(!resolved.shallow);
    }

    #[test]
    fn nested_and_missing_paths_do_not_resolve_to_an_ancestor() {
        let fixture = init_repo();
        commit(&fixture.repo, "file.txt", "one\n", "initial");
        let nested = fixture.repo.join("nested");
        std::fs::create_dir(&nested).unwrap();
        let nested_error =
            resolve_repo(&spec(&nested), &ProjectStatsBudgetWire::default())
                .expect_err("nested directory is not the repository root");
        assert_eq!(nested_error.code, "identity_mismatch");

        let missing_error = resolve_repo(
            &spec(&fixture.repo.join("missing")),
            &ProjectStatsBudgetWire::default(),
        )
        .expect_err("missing child must not use parent Git repo");
        assert_eq!(missing_error.code, "repo_unavailable");
    }

    #[test]
    fn canonical_path_aliases_and_ordered_refs_are_respected() {
        let fixture = init_repo();
        commit(&fixture.repo, "file.txt", "one\n", "first");
        git(&fixture.repo, &["branch", "preferred"]);
        commit(&fixture.repo, "file.txt", "two\n", "second");

        let mut repo_spec = spec(&fixture.repo.join("."));
        repo_spec.ref_candidates =
            vec!["preferred".to_owned(), "main".to_owned()];
        let resolved =
            resolve_repo(&repo_spec, &ProjectStatsBudgetWire::default())
                .unwrap();
        assert_eq!(
            resolved.tip,
            git(&fixture.repo, &["rev-parse", "preferred"])
        );
        assert_eq!(resolved.ref_name, "preferred");

        let canonical = fixture.repo.canonicalize().unwrap();
        assert_eq!(resolved.canonical_path, path_string(&canonical));
    }

    #[test]
    fn unsafe_revision_tokens_are_rejected() {
        let fixture = init_repo();
        let mut repo_spec = spec(&fixture.repo);
        repo_spec.ref_candidates =
            vec!["--output=/tmp/should-not-exist".to_owned()];
        let error =
            resolve_repo(&repo_spec, &ProjectStatsBudgetWire::default())
                .expect_err("unsafe revision must not pass to Git");
        assert_eq!(error.code, "invalid_ref");
    }

    #[test]
    fn inherited_git_dir_does_not_reroute_resolution() {
        if let Ok(repo_path) = env::var("SASE_PROJECT_STATS_GIT_CHILD_REPO") {
            let repo_spec = spec(Path::new(&repo_path));
            resolve_repo(&repo_spec, &ProjectStatsBudgetWire::default())
                .expect("runner removes inherited GIT_DIR");
            return;
        }

        let wanted = init_repo();
        commit(&wanted.repo, "file.txt", "wanted\n", "wanted repo");
        let wrong = init_repo();
        commit(&wrong.repo, "other.txt", "wrong\n", "wrong repo");
        let test_name = std::thread::current()
            .name()
            .expect("test thread name")
            .to_owned();
        let output = Command::new(env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env("SASE_PROJECT_STATS_GIT_CHILD_REPO", &wanted.repo)
            .env("GIT_DIR", wrong.repo.join(".git"))
            .output()
            .expect("run isolated test process");
        assert!(
            output.status.success(),
            "child resolution failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
