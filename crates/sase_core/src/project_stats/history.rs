//! Full-DAG commit history, parsed into raw rows and cached before attribution.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use super::cache::{self, HistoryCacheEntry};
use super::git::{self, ResolvedRepo};
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
};
use crate::commit_footer::parse_commit_footer;

const HISTORY_LOG_FORMAT: &str = "%H%x1f%P%x1f%at%x1f%ct%x1f%B";

/// One raw commit row. Cache rows before attribution so directories can change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HistoryRow {
    pub sha: String,
    pub author_ts: i64,
    pub committer_ts: i64,
    pub is_merge: bool,
    pub agent_tag: Option<String>,
    pub model_tag: Option<String>,
}

/// Raw rows plus the completeness and issues of a history walk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryOutcome {
    pub rows: Vec<HistoryRow>,
    /// True when every commit reachable in the selected Git graph was read
    /// and parsed. Shallow repositories remain explicitly marked by issue.
    pub complete: bool,
    /// True when the walk command failed and produced no usable history.
    /// Truncated walks with complete records stay `failed: false`.
    pub failed: bool,
    pub issues: Vec<ProjectStatsIssueWire>,
}

/// Resolve one repository, load or build its full history, and return both
/// pieces for the aggregation phase. Resolution failures remain per-source
/// issues and do not become request errors.
#[cfg(test)]
pub(crate) fn read_repo_history(
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
) -> Result<(ResolvedRepo, HistoryOutcome), ProjectStatsIssueWire> {
    let repo = git::resolve_repo(spec, budget)?;
    let outcome = history_for_resolved(&repo, spec, budget, cache_dir);
    Ok((repo, (*outcome).clone()))
}

/// Walk or reuse cached history for an already resolved repository.
pub(crate) fn history_for_resolved(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
) -> Arc<HistoryOutcome> {
    let key = cache::cache_key(repo);
    let cache_path =
        (!cache_dir.is_empty()).then(|| cache::cache_path(cache_dir, &key));
    if let Some(path) = cache_path.as_deref() {
        if let Some(hit) = interned_outcome(path, &repo.tip) {
            return hit;
        }
    }
    let cached = cache_path
        .as_deref()
        .and_then(cache::load)
        .filter(|entry| entry.matches_repo(&key, repo));

    let mut reused_complete_equal_tip = false;
    let mut outcome = if repo.tip.is_empty() {
        HistoryOutcome {
            complete: true,
            ..HistoryOutcome::default()
        }
    } else {
        match cached.as_deref() {
            Some(entry) if entry.is_complete() && entry.tip() == repo.tip => {
                reused_complete_equal_tip = true;
                HistoryOutcome {
                    rows: entry.rows().to_vec(),
                    complete: true,
                    failed: false,
                    issues: Vec::new(),
                }
            }
            Some(entry)
                if entry.is_complete()
                    && !entry.tip().is_empty()
                    && is_ancestor(repo, entry.tip(), &repo.tip, budget) =>
            {
                let mut delta = walk_history(
                    repo,
                    Some((entry.tip(), &repo.tip)),
                    budget,
                    spec,
                );
                let mut rows = entry.rows().to_vec();
                rows.extend(delta.rows);
                sort_and_deduplicate(&mut rows);
                delta.rows = rows;
                delta
            }
            _ => walk_history(repo, None, budget, spec),
        }
    };

    if repo.shallow {
        outcome.issues.push(git::issue(
            spec,
            "shallow_history",
            "repository is shallow; commits before its shallow boundary are unavailable"
                .to_owned(),
        ));
    }

    if let Some(path) = cache_path.as_ref() {
        if !outcome.failed && !reused_complete_equal_tip {
            cache::persist(
                path,
                &HistoryCacheEntry::new(
                    key,
                    repo,
                    outcome.complete,
                    outcome.rows.clone(),
                ),
            );
        }
        intern_outcome(path, &repo.tip, outcome)
    } else {
        Arc::new(outcome)
    }
}

fn intern_key(path: &Path, tip: &str) -> String {
    let mut key = path.to_string_lossy().into_owned();
    key.push('\0');
    key.push_str(tip);
    key
}

fn interned_outcome(path: &Path, tip: &str) -> Option<Arc<HistoryOutcome>> {
    let key = intern_key(path, tip);
    outcome_memo().lock().ok()?.get(&key).cloned()
}

fn intern_outcome(
    path: &Path,
    tip: &str,
    outcome: HistoryOutcome,
) -> Arc<HistoryOutcome> {
    let store = outcome.complete && !outcome.failed;
    let arc = Arc::new(outcome);
    if store {
        if let Ok(mut guard) = outcome_memo().lock() {
            guard.insert(intern_key(path, tip), Arc::clone(&arc));
        }
    }
    arc
}

fn outcome_memo() -> &'static Mutex<HashMap<String, Arc<HistoryOutcome>>> {
    static MEMO: OnceLock<Mutex<HashMap<String, Arc<HistoryOutcome>>>> =
        OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Drop interned complete outcomes so tests can force disk or Git reloads.
#[cfg(test)]
pub(crate) fn clear_outcome_memo() {
    if let Ok(mut guard) = outcome_memo().lock() {
        guard.clear();
    }
}

fn walk_history(
    repo: &ResolvedRepo,
    range: Option<(&str, &str)>,
    budget: &ProjectStatsBudgetWire,
    spec: &ProjectStatsRepoSpecWire,
) -> HistoryOutcome {
    if !git::valid_oid(&repo.tip) {
        return HistoryOutcome {
            failed: true,
            issues: vec![git::issue(
                spec,
                "invalid_ref",
                "resolved tip is not a full Git object id".to_owned(),
            )],
            ..HistoryOutcome::default()
        };
    }

    let format = format!("--format={HISTORY_LOG_FORMAT}");
    let range_arg = range.map(|(old, new)| format!("{old}..{new}"));
    let mut args = vec!["log", "-z", format.as_str()];
    if let Some(range) = range_arg.as_deref() {
        args.push(range);
    } else {
        args.push(&repo.tip);
    }
    let result =
        git::run_unchecked(Path::new(&repo.canonical_path), &args, budget);

    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return HistoryOutcome {
                failed: true,
                issues: vec![git::issue(
                    spec,
                    "history_unavailable",
                    error.to_string(),
                )],
                ..HistoryOutcome::default()
            };
        }
    };

    let mut outcome = HistoryOutcome {
        rows: Vec::new(),
        complete: result.code == Some(0) && !result.truncated,
        failed: false,
        issues: Vec::new(),
    };
    if result.truncated {
        outcome.complete = false;
        outcome.issues.push(git::issue(
            spec,
            "history_truncated",
            "Git history exceeded its time or output budget; only complete records were kept"
                .to_owned(),
        ));
    } else if result.code != Some(0) {
        outcome.complete = false;
        outcome.failed = true;
        outcome.issues.push(git::issue(
            spec,
            "history_unavailable",
            format!("git log exited with {:?}", result.code),
        ));
    }

    let parse_result = parse_log_records(&result.stdout);
    for record in parse_result.records {
        match parse_record(record) {
            Ok(row) => outcome.rows.push(row),
            Err(message) => {
                outcome.complete = false;
                outcome.issues.push(git::issue(
                    spec,
                    "history_parse_failed",
                    message,
                ));
            }
        }
    }
    if !parse_result.terminated && !result.stdout.is_empty() {
        outcome.complete = false;
        if !result.truncated {
            outcome.issues.push(git::issue(
                spec,
                "history_parse_failed",
                "Git history output ended in an incomplete record".to_owned(),
            ));
        }
    }
    if parse_result.empty_record_count > 0 {
        outcome.complete = false;
        outcome.issues.push(git::issue(
            spec,
            "history_parse_failed",
            "Git history output contained an empty framed record".to_owned(),
        ));
    }
    sort_and_deduplicate(&mut outcome.rows);
    if outcome.failed {
        outcome.rows.clear();
    }
    outcome
}

struct FramedRecords<'a> {
    records: Vec<&'a [u8]>,
    terminated: bool,
    empty_record_count: usize,
}

/// NUL framing is independent of message contents. A final unterminated
/// chunk is never treated as a complete commit after an output/time cut.
fn parse_log_records(bytes: &[u8]) -> FramedRecords<'_> {
    if bytes.is_empty() {
        return FramedRecords {
            records: Vec::new(),
            terminated: true,
            empty_record_count: 0,
        };
    }
    let terminated = bytes.last() == Some(&0);
    let chunks: Vec<&[u8]> = bytes.split(|byte| *byte == 0).collect();
    let usable = chunks.len().saturating_sub(1);
    let complete_chunks = &chunks[..usable];
    let empty_record_count =
        complete_chunks.iter().filter(|row| row.is_empty()).count();
    FramedRecords {
        records: complete_chunks
            .iter()
            .copied()
            .filter(|row| !row.is_empty())
            .collect(),
        terminated,
        empty_record_count,
    }
}

fn parse_record(record: &[u8]) -> Result<HistoryRow, String> {
    let mut fields = record.splitn(5, |byte| *byte == 0x1f);
    let sha = field_text(fields.next(), "commit id")?;
    let parents = field_text(fields.next(), "parent ids")?;
    let author_ts = field_text(fields.next(), "author timestamp")?
        .parse::<i64>()
        .map_err(|error| format!("invalid author timestamp: {error}"))?;
    let committer_ts = field_text(fields.next(), "committer timestamp")?
        .parse::<i64>()
        .map_err(|error| format!("invalid committer timestamp: {error}"))?;
    let message_bytes = fields.next().ok_or_else(|| {
        "history record is missing its message field".to_owned()
    })?;

    if !git::valid_oid(sha) {
        return Err("history record has an invalid commit id".to_owned());
    }
    let parent_ids: Vec<&str> = parents.split_whitespace().collect();
    if parent_ids.iter().any(|parent| !git::valid_oid(parent)) {
        return Err("history record has an invalid parent id".to_owned());
    }
    let message = String::from_utf8_lossy(message_bytes);
    let footer = parse_commit_footer(&message);
    let agent_tag = footer
        .tags
        .iter()
        .find(|tag| tag.key == "AGENT")
        .map(|tag| tag.label.clone());
    let model_tag = footer
        .tags
        .iter()
        .find(|tag| tag.key == "MODEL")
        .map(|tag| tag.label.clone());
    Ok(HistoryRow {
        sha: sha.to_ascii_lowercase(),
        author_ts,
        committer_ts,
        is_merge: parent_ids.len() > 1,
        agent_tag,
        model_tag,
    })
}

fn field_text<'a>(
    field: Option<&'a [u8]>,
    name: &str,
) -> Result<&'a str, String> {
    let field =
        field.ok_or_else(|| format!("history record is missing {name}"))?;
    std::str::from_utf8(field)
        .map_err(|error| format!("history record has invalid {name}: {error}"))
}

fn is_ancestor(
    repo: &ResolvedRepo,
    ancestor: &str,
    tip: &str,
    budget: &ProjectStatsBudgetWire,
) -> bool {
    if !git::valid_oid(ancestor) || !git::valid_oid(tip) {
        return false;
    }
    let Ok(result) = git::run_unchecked(
        Path::new(&repo.canonical_path),
        &["merge-base", "--is-ancestor", ancestor, tip],
        budget,
    ) else {
        return false;
    };
    result.code == Some(0) && !result.truncated
}

fn sort_and_deduplicate(rows: &mut Vec<HistoryRow>) {
    let mut by_sha = BTreeMap::new();
    for row in rows.drain(..) {
        by_sha.entry(row.sha.clone()).or_insert(row);
    }
    rows.extend(by_sha.into_values());
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use super::*;
    use crate::project_stats::cache;
    use crate::project_stats::test_support::{commit, git, init_repo, spec};

    fn cache_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("cache directory")
    }

    fn read(
        repo: &Path,
        cache_dir: &Path,
        budget: &ProjectStatsBudgetWire,
    ) -> (ResolvedRepo, HistoryOutcome) {
        read_repo_history(&spec(repo), budget, &cache_dir.to_string_lossy())
            .expect("repository and history")
    }

    #[test]
    fn full_dag_includes_both_sides_of_a_merge() {
        let fixture = init_repo();
        commit(&fixture.repo, "base.txt", "base\n", "base");
        git(&fixture.repo, &["checkout", "-b", "side"]);
        commit(&fixture.repo, "side.txt", "side\n", "side commit");
        git(&fixture.repo, &["checkout", "main"]);
        commit(&fixture.repo, "main.txt", "main\n", "main commit");
        git(
            &fixture.repo,
            &["merge", "--no-ff", "side", "-m", "merge side"],
        );

        let cache = cache_dir();
        let (_, outcome) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(outcome.complete, "issues: {:?}", outcome.issues);
        assert_eq!(outcome.rows.len(), 4);
        assert_eq!(outcome.rows.iter().filter(|row| row.is_merge).count(), 1);
        assert!(outcome
            .rows
            .iter()
            .any(|row| row.sha == git(&fixture.repo, &["rev-parse", "side"])));
    }

    #[test]
    fn footer_attribution_uses_first_normalized_tags_and_link_labels() {
        let fixture = init_repo();
        let message = concat!(
            "footer fixture\n\n",
            "SASE_AGENT=[Agent Name][agent]\n",
            "SASE_MODEL=first-model\n",
            "SASE_MODEL=second-model\n\n",
            "[agent]: https://example.test/agent\n"
        );
        commit(&fixture.repo, "file.txt", "content\n", message);

        let cache = cache_dir();
        let (_, outcome) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(outcome.complete, "issues: {:?}", outcome.issues);
        assert_eq!(outcome.rows.len(), 1);
        assert_eq!(outcome.rows[0].agent_tag.as_deref(), Some("Agent Name"));
        assert_eq!(outcome.rows[0].model_tag.as_deref(), Some("first-model"));
    }

    #[test]
    fn framing_keeps_only_complete_records_and_splits_four_separators() {
        let sha = "a".repeat(40);
        let complete =
            format!("{sha}\x1f\x1f10\x1f11\x1fbody has \x1f separator");
        let incomplete = format!("{}\x1f\x1f12", "b".repeat(40));
        let bytes = format!("{complete}\0{incomplete}").into_bytes();
        let framed = parse_log_records(&bytes);
        assert!(!framed.terminated);
        assert_eq!(framed.records.len(), 1);
        let row = parse_record(framed.records[0]).expect("first record parses");
        assert_eq!(row.sha, sha);
        assert_eq!(row.author_ts, 10);
        assert_eq!(row.committer_ts, 11);
        assert!(!row.is_merge);
        assert_eq!(row.agent_tag, None);
    }

    #[test]
    fn cache_hit_reuses_complete_equal_tip_without_rewalking_history() {
        let fixture = init_repo();
        for index in 0..12 {
            commit(
                &fixture.repo,
                &format!("file-{index}.txt"),
                &format!("value {index}\n"),
                &format!("commit {index}"),
            );
        }
        let cache = cache_dir();
        let (_, first) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(first.complete);

        // This cap is large enough for repository metadata, but too small
        // for the full log. A complete result proves the equal-tip cache hit.
        let tight = ProjectStatsBudgetWire {
            max_output_bytes: 256,
            ..ProjectStatsBudgetWire::default()
        };
        let (_, second) = read(&fixture.repo, cache.path(), &tight);
        assert!(second.complete, "issues: {:?}", second.issues);
        assert_eq!(first.rows, second.rows);
    }

    #[test]
    fn incremental_history_matches_a_cold_full_walk() {
        let fixture = init_repo();
        commit(&fixture.repo, "one.txt", "one\n", "one");
        let incremental_cache = cache_dir();
        let (_, _) = read(
            &fixture.repo,
            incremental_cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        commit(&fixture.repo, "two.txt", "two\n", "two");

        let (_, incremental) = read(
            &fixture.repo,
            incremental_cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        let cold_cache = cache_dir();
        let (_, cold) = read(
            &fixture.repo,
            cold_cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(incremental.complete);
        assert!(cold.complete);
        assert_eq!(incremental.rows, cold.rows);
    }

    #[test]
    fn rewritten_history_rebuilds_instead_of_folding_old_rows() {
        let fixture = init_repo();
        commit(&fixture.repo, "old.txt", "old\n", "old root");
        let cache = cache_dir();
        let (_, old) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        let old_sha = old.rows[0].sha.clone();

        git(&fixture.repo, &["checkout", "--orphan", "rewritten"]);
        git(&fixture.repo, &["rm", "-rf", "."]);
        commit(&fixture.repo, "new.txt", "new\n", "new root");
        git(&fixture.repo, &["branch", "-f", "main", "HEAD"]);
        git(&fixture.repo, &["checkout", "main"]);

        let (_, rewritten) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(rewritten.complete);
        assert_eq!(rewritten.rows.len(), 1);
        assert_ne!(rewritten.rows[0].sha, old_sha);
        assert_eq!(
            rewritten.rows[0].sha,
            git(&fixture.repo, &["rev-parse", "HEAD"])
        );
    }

    #[test]
    fn corrupt_and_wrong_schema_entries_rebuild_from_git() {
        let fixture = init_repo();
        commit(&fixture.repo, "file.txt", "one\n", "one");
        let cache = cache_dir();
        let (resolved, first) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        let path = cache::cache_path(
            &cache.path().to_string_lossy(),
            &cache::cache_key(&resolved),
        );
        fs::write(&path, "{").expect("corrupt cache file");
        cache::clear_memo();
        clear_outcome_memo();
        let (_, recovered) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert_eq!(first.rows, recovered.rows);
        assert!(recovered.complete);

        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value["schema_version"] = serde_json::json!(99);
        fs::write(&path, serde_json::to_vec(&value).unwrap())
            .expect("write wrong schema");
        cache::clear_memo();
        clear_outcome_memo();
        let (_, rebuilt) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(rebuilt.complete);
        assert_eq!(first.rows, rebuilt.rows);
    }

    #[test]
    fn disk_cache_reloads_after_process_memo_is_cleared() {
        let fixture = init_repo();
        for index in 0..8 {
            commit(
                &fixture.repo,
                &format!("file-{index}.txt"),
                &format!("{index}\n"),
                &format!("commit {index}"),
            );
        }
        let cache = cache_dir();
        let (_, first) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        cache::clear_memo();
        clear_outcome_memo();
        let tight = ProjectStatsBudgetWire {
            max_output_bytes: 256,
            ..ProjectStatsBudgetWire::default()
        };
        let (_, second) = read(&fixture.repo, cache.path(), &tight);
        assert!(
            second.complete,
            "disk-loaded entry missed: {:?}",
            second.issues
        );
        assert_eq!(first.rows, second.rows);
    }

    #[test]
    fn cache_write_failure_does_not_discard_measured_history() {
        let fixture = init_repo();
        commit(&fixture.repo, "file.txt", "one\n", "one");
        let temp = tempfile::tempdir().unwrap();
        let cache_file = temp.path().join("not-a-directory");
        fs::write(&cache_file, "block directory creation").unwrap();
        let (_, outcome) = read(
            &fixture.repo,
            &cache_file,
            &ProjectStatsBudgetWire::default(),
        );
        assert!(outcome.complete, "issues: {:?}", outcome.issues);
        assert_eq!(outcome.rows.len(), 1);
    }

    #[test]
    fn truncated_walk_retries_and_never_becomes_a_complete_cache_hit() {
        let fixture = init_repo();
        for index in 0..16 {
            commit(
                &fixture.repo,
                &format!("file-{index}.txt"),
                &format!("{index}\n"),
                &format!("commit {index}"),
            );
        }
        let cache = cache_dir();
        let tight = ProjectStatsBudgetWire {
            max_output_bytes: 256,
            ..ProjectStatsBudgetWire::default()
        };
        let (_, partial) = read(&fixture.repo, cache.path(), &tight);
        assert!(!partial.complete);
        assert!(partial
            .issues
            .iter()
            .any(|issue| issue.code == "history_truncated"));

        let (_, retried) = read(
            &fixture.repo,
            cache.path(),
            &ProjectStatsBudgetWire::default(),
        );
        assert!(retried.complete, "issues: {:?}", retried.issues);
        assert_eq!(retried.rows.len(), 16);
    }

    #[test]
    fn shallow_clone_reports_incomplete_older_coverage() {
        let source = init_repo();
        for index in 0..3 {
            commit(
                &source.repo,
                &format!("file-{index}.txt"),
                &format!("{index}\n"),
                &format!("commit {index}"),
            );
        }
        let holder = tempfile::tempdir().unwrap();
        let clone = holder.path().join("shallow");
        let source_url = format!("file://{}", source.repo.display());
        let clone_output = Command::new("git")
            .args(["clone", "--depth=1", &source_url])
            .arg(&clone)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_NAMESPACE")
            .output()
            .expect("clone shallow repository");
        assert!(
            clone_output.status.success(),
            "clone failed: {}",
            String::from_utf8_lossy(&clone_output.stderr)
        );

        let cache = cache_dir();
        let (resolved, outcome) =
            read(&clone, cache.path(), &ProjectStatsBudgetWire::default());
        assert!(resolved.shallow);
        assert_eq!(outcome.rows.len(), 1);
        assert!(outcome.complete);
        assert!(outcome
            .issues
            .iter()
            .any(|issue| issue.code == "shallow_history"));
    }

    #[test]
    fn failed_walk_after_resolution_is_failed_not_partial() {
        let fixture = init_repo();
        commit(&fixture.repo, "file.txt", "one\n", "one");
        let spec = spec(&fixture.repo);
        let resolved = crate::project_stats::git::resolve_repo(
            &spec,
            &ProjectStatsBudgetWire::default(),
        )
        .unwrap();
        let empty = tempfile::tempdir().unwrap();
        let mut broken = resolved.clone();
        broken.canonical_path = empty.path().to_string_lossy().into_owned();
        let outcome = history_for_resolved(
            &broken,
            &spec,
            &ProjectStatsBudgetWire::default(),
            "",
        );
        assert!(outcome.failed);
        assert!(!outcome.complete);
        assert!(outcome.rows.is_empty());
        assert!(outcome
            .issues
            .iter()
            .any(|issue| issue.code == "history_unavailable"));
    }
}
