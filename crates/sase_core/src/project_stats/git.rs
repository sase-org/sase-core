//! Repository identity, ref validation, and source metadata for project stats.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use crate::file_history::{
    run_git_unchecked, run_git_unchecked_with_stdin,
    run_git_unchecked_with_stdin_env, safe_revision_token, GitResult,
};

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
    retarget_issue(
        ProjectStatsIssueWire {
            code: code.to_owned(),
            message,
            ..ProjectStatsIssueWire::default()
        },
        spec,
    )
}

pub(crate) fn retarget_issue(
    mut issue: ProjectStatsIssueWire,
    spec: &ProjectStatsRepoSpecWire,
) -> ProjectStatsIssueWire {
    issue.repo = Some(spec.name.clone());
    issue
}

pub(crate) fn run_unchecked(
    repo: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    record_git_command(args);
    run_git_unchecked(
        repo,
        args,
        Duration::from_millis(budget.timeout_ms.max(1)),
        budget.max_output_bytes.max(1),
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn run_checked(
    repo: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    run_output(repo, args, budget)
}

pub(crate) fn run_unchecked_with_stdin(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    record_git_command(args);
    run_git_unchecked_with_stdin(
        repo,
        args,
        stdin,
        Duration::from_millis(budget.timeout_ms.max(1)),
        budget.max_output_bytes.max(1),
    )
    .map_err(|error| error.to_string())
}

/// Bytes `cat-file --batch` prefixes each object with, including
/// SHA-256 OIDs and the trailing newline. Shared with the snapshot
/// reader.
pub(crate) const CAT_FILE_HEADER_OVERHEAD: u64 = 96;
const MAX_BATCH_OIDS: usize = 65_536;
const BATCH_CHECK_LINE_BUDGET: u64 = 128;

/// Pack blobs so each `cat-file --batch` stdout stays at or under
/// `max_output_bytes` and holds at most `max_oids_per_batch` objects.
/// A blob whose own framed cost exceeds the budget is an error; callers
/// that skip oversize blobs must filter first. Snapshot keeps its
/// 256-OID cap and oversize-is-error behavior via this helper.
pub(crate) fn pack_blob_batches(
    wanted: &[(String, u64)],
    max_output_bytes: u64,
    max_oids_per_batch: usize,
) -> Result<Vec<Vec<(String, u64)>>, String> {
    let max_oids_per_batch = max_oids_per_batch.max(1);
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut used = 0u64;
    for (oid, size) in wanted {
        let cost = size.saturating_add(CAT_FILE_HEADER_OVERHEAD);
        if cost > max_output_bytes {
            return Err(format!(
                "blob {oid} plus framing exceeds the output budget"
            ));
        }
        let would_overflow = !current.is_empty()
            && (used.saturating_add(cost) > max_output_bytes
                || current.len() >= max_oids_per_batch);
        if would_overflow {
            batches.push(std::mem::take(&mut current));
            used = 0;
        }
        current.push((oid.clone(), *size));
        used = used.saturating_add(cost);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// Read blob contents for *oids* via size-probed `cat-file --batch`.
///
/// Probe sizes with `cat-file --batch-check` (chunked at 128 bytes per
/// header so the probe stays under budget), then pack batches by real
/// framed size (`size + 96` header overhead, at most 65,536 OIDs). A
/// blob whose framed cost exceeds the budget is `None` and is never
/// requested; remaining blobs still read. Results stay in input order.
/// `None` means missing, non-blob, oversize, or unusable.
pub(crate) fn read_blobs(
    repo: &Path,
    oids: &[String],
    budget: &ProjectStatsBudgetWire,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    let max_output_bytes = budget.max_output_bytes.max(1);
    let sizes = probe_blob_sizes(repo, oids, budget)?;
    let mut out = vec![None; oids.len()];
    let mut packable = Vec::new();
    for (index, (oid, size)) in oids.iter().zip(sizes).enumerate() {
        let Some(size) = size else {
            continue;
        };
        let cost = size.saturating_add(CAT_FILE_HEADER_OVERHEAD);
        if cost > max_output_bytes {
            continue;
        }
        packable.push((index, oid.clone(), size));
    }
    if packable.is_empty() {
        return Ok(out);
    }
    let wanted: Vec<(String, u64)> = packable
        .iter()
        .map(|(_, oid, size)| (oid.clone(), *size))
        .collect();
    let batches = pack_blob_batches(&wanted, max_output_bytes, MAX_BATCH_OIDS)?;
    let mut cursor = 0;
    for batch in batches {
        let batch_len = batch.len();
        let slice = &packable[cursor..cursor + batch_len];
        let mut stdin = Vec::new();
        for (_, oid, _) in slice {
            stdin.extend_from_slice(oid.as_bytes());
            stdin.push(b'\n');
        }
        let result = run_unchecked_with_stdin(
            repo,
            &["cat-file", "--batch"],
            &stdin,
            budget,
        )?;
        if result.truncated {
            return Err(
                "git cat-file --batch exceeded its command budget".to_owned()
            );
        }
        if result.code != Some(0) {
            return Err("git cat-file --batch did not succeed".to_owned());
        }
        let parsed = parse_cat_file_batch(&result.stdout, batch_len)?;
        for ((index, _, _), blob) in slice.iter().zip(parsed) {
            out[*index] = blob;
        }
        cursor += batch_len;
    }
    Ok(out)
}

fn probe_blob_sizes(
    repo: &Path,
    oids: &[String],
    budget: &ProjectStatsBudgetWire,
) -> Result<Vec<Option<u64>>, String> {
    let max_output_bytes = budget.max_output_bytes.max(1);
    let chunk = (max_output_bytes / BATCH_CHECK_LINE_BUDGET)
        .clamp(1, MAX_BATCH_OIDS as u64) as usize;
    let mut sizes = Vec::with_capacity(oids.len());
    for chunk in oids.chunks(chunk) {
        let mut stdin = Vec::new();
        for oid in chunk {
            stdin.extend_from_slice(oid.as_bytes());
            stdin.push(b'\n');
        }
        let result = run_unchecked_with_stdin(
            repo,
            &["cat-file", "--batch-check"],
            &stdin,
            budget,
        )?;
        if result.truncated {
            return Err(
                "git cat-file --batch-check exceeded its command budget"
                    .to_owned(),
            );
        }
        if result.code != Some(0) {
            return Err("git cat-file --batch-check did not succeed".to_owned());
        }
        sizes.extend(parse_batch_check(&result.stdout, chunk.len())?);
    }
    Ok(sizes)
}

fn parse_batch_check(
    stdout: &[u8],
    expected: usize,
) -> Result<Vec<Option<u64>>, String> {
    let text = std::str::from_utf8(stdout).map_err(|error| {
        format!("git cat-file --batch-check is not UTF-8: {error}")
    })?;
    let mut out = Vec::with_capacity(expected);
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if out.len() == expected {
            return Err(
                "git cat-file --batch-check returned extra objects".to_owned()
            );
        }
        let parts: Vec<&str> = line.split(' ').collect();
        if parts.len() >= 2 && parts[1] == "missing" {
            out.push(None);
            continue;
        }
        if parts.len() < 3 {
            return Err(format!(
                "git cat-file --batch-check returned a malformed header: {line}"
            ));
        }
        if parts[1] != "blob" {
            out.push(None);
            continue;
        }
        let size: u64 = parts[2].parse().map_err(|_| {
            format!("git cat-file --batch-check size is not an integer: {line}")
        })?;
        out.push(Some(size));
    }
    if out.len() != expected {
        return Err(
            "git cat-file --batch-check returned fewer objects than requested"
                .to_owned(),
        );
    }
    Ok(out)
}

fn parse_cat_file_batch(
    stdout: &[u8],
    expected: usize,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut cursor = 0;
    let mut out = Vec::with_capacity(expected);
    while out.len() < expected {
        let rest = stdout.get(cursor..).unwrap_or(&[]);
        if rest.is_empty() {
            return Err(
                "git cat-file --batch returned fewer objects than requested"
                    .to_owned(),
            );
        }
        let Some(newline) = rest.iter().position(|byte| *byte == b'\n') else {
            return Err("git cat-file --batch header is truncated".to_owned());
        };
        let header = std::str::from_utf8(&rest[..newline])
            .map_err(|error| {
                format!("git cat-file header is not UTF-8: {error}")
            })?
            .trim_end_matches('\r');
        cursor += newline + 1;
        let parts: Vec<&str> = header.split(' ').collect();
        if parts.len() >= 2 && parts[1] == "missing" {
            out.push(None);
            continue;
        }
        if parts.len() < 3 {
            return Err(format!(
                "git cat-file --batch returned a malformed header: {header}"
            ));
        }
        let size: usize = parts[2].parse().map_err(|_| {
            format!("git cat-file --batch size is not an integer: {header}")
        })?;
        let bytes = stdout.get(cursor..cursor + size).ok_or_else(|| {
            "git cat-file --batch body is truncated".to_owned()
        })?;
        out.push(Some(bytes.to_vec()));
        cursor += size;
        if stdout.get(cursor) == Some(&b'\n') {
            cursor += 1;
        } else if cursor < stdout.len() {
            return Err(
                "git cat-file --batch body is missing its trailing newline"
                    .to_owned(),
            );
        }
    }
    Ok(out)
}

pub(crate) fn run_unchecked_with_stdin_env(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    extra_env: &[(&str, &str)],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    record_git_command(args);
    run_git_unchecked_with_stdin_env(
        repo,
        args,
        stdin,
        extra_env,
        Duration::from_millis(budget.timeout_ms.max(1)),
        budget.max_output_bytes.max(1),
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn run_checked_with_stdin(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    require_complete(run_unchecked_with_stdin(repo, args, stdin, budget)?, args)
}

pub(crate) fn run_output(
    repo: &Path,
    args: &[&str],
    budget: &ProjectStatsBudgetWire,
) -> Result<GitResult, String> {
    require_complete(run_unchecked(repo, args, budget)?, args)
}

fn require_complete(
    result: GitResult,
    args: &[&str],
) -> Result<GitResult, String> {
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

pub(crate) fn output_text(result: GitResult) -> Result<String, String> {
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

fn record_git_command(args: &[&str]) {
    #[cfg(test)]
    capture::record(args);
    #[cfg(not(test))]
    let _ = args;
}

#[cfg(test)]
pub(crate) fn capture_git_commands<F, T>(f: F) -> (T, Vec<Vec<String>>)
where
    F: FnOnce() -> T,
{
    capture::start();
    let value = f();
    (value, capture::take())
}

#[cfg(test)]
mod capture {
    use std::cell::RefCell;

    thread_local! {
        static LOG: RefCell<Option<Vec<Vec<String>>>> = const { RefCell::new(None) };
    }

    pub(super) fn start() {
        LOG.with(|log| *log.borrow_mut() = Some(Vec::new()));
    }

    pub(super) fn record(args: &[&str]) {
        LOG.with(|log| {
            if let Some(items) = log.borrow_mut().as_mut() {
                items
                    .push(args.iter().map(|item| (*item).to_owned()).collect());
            }
        });
    }

    pub(super) fn take() -> Vec<Vec<String>> {
        LOG.with(|log| log.borrow_mut().take().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::process::Command;

    use super::*;
    use crate::project_stats::test_support::{
        commit, commit_files, git, init_repo, spec,
    };

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

    fn exact_invocations(commands: &[Vec<String>], needle: &[&str]) -> usize {
        commands
            .iter()
            .filter(|args| {
                args.len() == needle.len()
                    && args.iter().zip(needle).all(|(got, want)| got == *want)
            })
            .count()
    }

    #[test]
    fn read_blobs_packs_by_size_and_skips_missing_or_oversize() {
        let fixture = init_repo();
        commit_files(
            &fixture.repo,
            &[
                ("small-a.txt", &"a".repeat(100)),
                ("small-b.txt", &"b".repeat(100)),
                ("small-c.txt", &"c".repeat(100)),
                ("huge.txt", &"h".repeat(10_000)),
            ],
            "blobs",
        );
        let oid_a = git(&fixture.repo, &["rev-parse", "HEAD:small-a.txt"]);
        let oid_b = git(&fixture.repo, &["rev-parse", "HEAD:small-b.txt"]);
        let oid_c = git(&fixture.repo, &["rev-parse", "HEAD:small-c.txt"]);
        let oid_huge = git(&fixture.repo, &["rev-parse", "HEAD:huge.txt"]);
        let missing = "0".repeat(40);
        let budget = ProjectStatsBudgetWire {
            timeout_ms: 30_000,
            max_output_bytes: 250,
        };
        let oids = vec![oid_a, missing, oid_b, oid_huge, oid_c];
        let (result, commands) =
            capture_git_commands(|| read_blobs(&fixture.repo, &oids, &budget));
        let blobs = result.expect("size-aware blob read");
        assert_eq!(blobs.len(), 5);
        assert_eq!(blobs[0].as_deref(), Some("a".repeat(100).as_bytes()));
        assert!(blobs[1].is_none());
        assert_eq!(blobs[2].as_deref(), Some("b".repeat(100).as_bytes()));
        assert!(blobs[3].is_none());
        assert_eq!(blobs[4].as_deref(), Some("c".repeat(100).as_bytes()));
        assert!(
            exact_invocations(&commands, &["cat-file", "--batch"]) > 1,
            "expected packed cat-file --batch calls, got {commands:?}"
        );
        assert!(
            exact_invocations(&commands, &["cat-file", "--batch-check"]) > 0,
            "expected a cat-file --batch-check probe, got {commands:?}"
        );
    }

    #[test]
    fn read_blobs_empty_oids_does_not_invoke_git() {
        let fixture = init_repo();
        let (result, commands) = capture_git_commands(|| {
            read_blobs(&fixture.repo, &[], &ProjectStatsBudgetWire::default())
        });
        assert!(result.expect("empty read").is_empty());
        assert!(commands.is_empty());
    }
}
