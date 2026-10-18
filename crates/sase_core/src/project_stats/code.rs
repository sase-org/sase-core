//! Code facet dispatch, identity-deduplicated aggregates, and snapshots.

use std::collections::BTreeMap;

use super::git::{self, ResolvedRepo};
use super::output;
use super::records;
use super::snapshot;
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsCodeCounterWire, ProjectStatsCodeWire,
    ProjectStatsFileRoleWire, ProjectStatsIssueWire, ProjectStatsLanguageWire,
    ProjectStatsRepoSpecWire, ProjectStatsRepoStatusWire,
    ProjectStatsRepoTierWire, ProjectStatsRoleCountWire,
    ProjectStatsSnapshotWire, PROJECT_STATS_CLASSIFIER_VERSION,
    PROJECT_STATS_METRIC_POLICY_VERSION,
};
use super::MeasuredRepo;

/// One identity's chosen committed-code measurement for this request.
#[derive(Debug, Clone)]
pub(crate) struct IdentityCode {
    pub snapshot: Option<ProjectStatsSnapshotWire>,
    pub status: ProjectStatsRepoStatusWire,
    pub issue: Option<ProjectStatsIssueWire>,
    pub token: String,
}

/// Metadata-only records snapshot for one local object store tip.
#[derive(Debug, Clone)]
pub(crate) struct RecordMeasure {
    pub snapshot: Option<ProjectStatsSnapshotWire>,
    pub status: ProjectStatsRepoStatusWire,
    pub issue: Option<ProjectStatsIssueWire>,
    pub token: String,
}

/// Semantic identity used to deduplicate Code aggregates.
///
/// Cache storage still distinguishes local object stores via `common_dir`.
pub(crate) fn semantic_identity(repo: &MeasuredRepo<'_>) -> String {
    if let Some(resolved) = repo.resolved {
        return resolved.identity.clone();
    }
    let configured = repo.spec.identity.trim();
    if !configured.is_empty() {
        configured.to_owned()
    } else {
        format!("path:{}", repo.spec.path)
    }
}

/// Choose one clone per identity and measure its committed tree.
///
/// Preference: a successfully measurable clone over a failed one. When
/// successful clones disagree on the selected tip, keep the clone with the
/// smallest `(canonical_path, common_dir, spec.id)` and emit a disagreement
/// issue. Request order is not a tiebreak.
pub(crate) fn measure_identities(
    repos: &[&MeasuredRepo<'_>],
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
    counter: ProjectStatsCodeCounterWire,
) -> (BTreeMap<String, IdentityCode>, Vec<ProjectStatsIssueWire>) {
    let mut grouped: BTreeMap<String, Vec<&MeasuredRepo<'_>>> = BTreeMap::new();
    for repo in repos {
        if repo.tier != ProjectStatsRepoTierWire::Code {
            continue;
        }
        grouped
            .entry(semantic_identity(repo))
            .or_default()
            .push(repo);
    }
    let mut measured = BTreeMap::new();
    let mut issues = Vec::new();
    for (identity, candidates) in grouped {
        let outcome = measure_identity(
            &identity,
            &candidates,
            budget,
            cache_dir,
            counter,
        );
        if let Some(issue) = outcome.issue.clone() {
            issues.push(issue);
        }
        measured.insert(identity, outcome);
    }
    (measured, issues)
}

fn measure_identity(
    identity: &str,
    candidates: &[&MeasuredRepo<'_>],
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
    counter: ProjectStatsCodeCounterWire,
) -> IdentityCode {
    let mut resolved: Vec<&MeasuredRepo<'_>> = candidates
        .iter()
        .copied()
        .filter(|repo| repo.resolved.is_some())
        .collect();
    resolved.sort_by_key(|repo| clone_key(repo));
    if resolved.is_empty() {
        let issue = candidates.iter().find_map(|repo| repo.issue.cloned());
        return IdentityCode {
            snapshot: None,
            status: ProjectStatsRepoStatusWire::Unavailable,
            issue,
            token: unavailable_token(identity),
        };
    }

    let tips: BTreeMap<&str, ()> = resolved
        .iter()
        .filter_map(|repo| repo.resolved.map(|item| item.tip.as_str()))
        .map(|tip| (tip, ()))
        .collect();
    let disagree = tips.len() > 1;
    let mut successes: Vec<(
        &MeasuredRepo<'_>,
        ProjectStatsSnapshotWire,
        ProjectStatsRepoStatusWire,
        Option<ProjectStatsIssueWire>,
        String,
    )> = Vec::new();
    let mut last_failure: Option<IdentityCode> = None;
    let mut seen = BTreeMap::<(String, String), ()>::new();
    for repo in &resolved {
        let Some(source) = repo.resolved else {
            continue;
        };
        let key = (source.common_dir.clone(), source.tip.clone());
        if seen.insert(key, ()).is_some() {
            continue;
        }
        let outcome = measure_code_repo(
            identity, source, repo.spec, budget, cache_dir, counter,
        );
        match outcome.snapshot {
            Some(snapshot) => successes.push((
                *repo,
                snapshot,
                outcome.status,
                outcome.issue,
                outcome.token,
            )),
            None => last_failure = Some(outcome),
        }
        if !disagree && successes.len() == 1 {
            break;
        }
    }

    if let Some((repo, snapshot, status, issue, token)) = successes
        .into_iter()
        .min_by_key(|(repo, _, _, _, _)| clone_key(repo))
    {
        let mut issue = issue;
        if disagree {
            let chosen = repo.resolved.expect("resolved clone");
            issue = Some(disagreement_issue(identity, repo.spec, chosen));
        }
        return IdentityCode {
            snapshot: Some(snapshot),
            status,
            issue,
            token,
        };
    }
    last_failure.unwrap_or(IdentityCode {
        snapshot: None,
        status: ProjectStatsRepoStatusWire::Unavailable,
        issue: None,
        token: unavailable_token(identity),
    })
}

fn clone_key(repo: &MeasuredRepo<'_>) -> (String, String, String) {
    match repo.resolved {
        Some(resolved) => (
            resolved.canonical_path.clone(),
            resolved.common_dir.clone(),
            repo.spec.id.clone(),
        ),
        None => (repo.spec.path.clone(), String::new(), repo.spec.id.clone()),
    }
}

fn measure_code_repo(
    identity: &str,
    resolved: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
    counter: ProjectStatsCodeCounterWire,
) -> IdentityCode {
    match snapshot::measure_code_snapshot(
        resolved, spec, budget, cache_dir, counter,
    ) {
        Ok(measured) => IdentityCode {
            snapshot: Some(measured.snapshot),
            status: ProjectStatsRepoStatusWire::Ok,
            issue: None,
            token: code_token(
                identity,
                resolved,
                ProjectStatsRepoStatusWire::Ok,
                counter,
            ),
        },
        Err(issue) => IdentityCode {
            snapshot: None,
            status: ProjectStatsRepoStatusWire::Unavailable,
            issue: Some(issue),
            token: code_token(
                identity,
                resolved,
                ProjectStatsRepoStatusWire::Unavailable,
                counter,
            ),
        },
    }
}

/// Measure one records repository. Cache callers should key by identity,
/// common dir, and tip so equal object stores are reused.
pub(crate) fn measure_records_repo(
    resolved: Option<&ResolvedRepo>,
    spec: &ProjectStatsRepoSpecWire,
    issue: Option<&ProjectStatsIssueWire>,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
) -> RecordMeasure {
    let Some(resolved) = resolved else {
        return RecordMeasure {
            snapshot: None,
            status: ProjectStatsRepoStatusWire::Unavailable,
            issue: issue.cloned(),
            token: serde_json::json!({
                "kind": "records",
                "path": spec.path,
                "status": "unavailable",
            })
            .to_string(),
        };
    };
    let measured = records::measure(resolved, spec, budget, cache_dir);
    let status = if measured.snapshot == ProjectStatsSnapshotWire::default()
        && measured.issue.is_some()
        && !measured.complete
    {
        ProjectStatsRepoStatusWire::Unavailable
    } else if measured.complete && measured.issue.is_none() {
        ProjectStatsRepoStatusWire::Ok
    } else {
        ProjectStatsRepoStatusWire::Partial
    };
    let snapshot = if status == ProjectStatsRepoStatusWire::Unavailable {
        None
    } else {
        Some(measured.snapshot)
    };
    RecordMeasure {
        snapshot,
        status,
        issue: measured.issue,
        token: measured.source_token,
    }
}

/// Merge successful code snapshots. Shares are computed from totals.
pub(crate) fn aggregate(
    snapshots: &[&ProjectStatsSnapshotWire],
    configured: u64,
    measured: u64,
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsCodeWire {
    if snapshots.is_empty() {
        return if configured == 0 {
            empty_population(counter)
        } else {
            unavailable_code(configured, counter)
        };
    }
    let mut code_files = 0;
    let mut code_lines = 0;
    let mut test_lines = 0;
    let mut doc_files = 0;
    let mut doc_lines = 0;
    let mut agent_context_files = 0;
    let mut agent_context_lines = 0;
    let mut languages: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for snapshot in snapshots {
        let (files, lines, tests) = code_totals(snapshot);
        code_files += files;
        code_lines += lines;
        test_lines += tests;
        doc_files += snapshot.doc_files.unwrap_or(0);
        doc_lines += snapshot.doc_lines.unwrap_or(0);
        agent_context_files += snapshot.agent_context_files.unwrap_or(0);
        agent_context_lines += snapshot.agent_context_lines.unwrap_or(0);
        if let Some(rows) = &snapshot.languages {
            for row in rows {
                let slot = languages.entry(row.name.clone()).or_default();
                slot.0 += row.files;
                slot.1 += row.lines;
            }
        }
    }
    let test_share = if code_lines == 0 {
        None
    } else {
        Some(test_lines as f64 / code_lines as f64)
    };
    let code = ProjectStatsCodeWire {
        counter,
        code_files: Some(code_files),
        code_lines: Some(code_lines),
        test_lines: Some(test_lines),
        test_share,
        doc_files: Some(doc_files),
        doc_lines: Some(doc_lines),
        agent_context_files: Some(agent_context_files),
        agent_context_lines: Some(agent_context_lines),
        languages: Some(
            languages
                .into_iter()
                .map(|(name, (files, lines))| ProjectStatsLanguageWire {
                    name,
                    files,
                    lines,
                })
                .collect(),
        ),
        repos_configured: configured,
        repos_measured: measured,
    };
    debug_assert!(check_code_invariants(&code).is_ok());
    code
}

pub(crate) fn empty_population(
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsCodeWire {
    ProjectStatsCodeWire {
        counter,
        code_files: Some(0),
        code_lines: Some(0),
        test_lines: Some(0),
        test_share: None,
        doc_files: Some(0),
        doc_lines: Some(0),
        agent_context_files: Some(0),
        agent_context_lines: Some(0),
        languages: Some(Vec::new()),
        repos_configured: 0,
        repos_measured: 0,
    }
}

pub(crate) fn unavailable_code(
    configured: u64,
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsCodeWire {
    ProjectStatsCodeWire {
        counter,
        code_files: None,
        code_lines: None,
        test_lines: None,
        test_share: None,
        doc_files: None,
        doc_lines: None,
        agent_context_files: None,
        agent_context_lines: None,
        languages: None,
        repos_configured: configured,
        repos_measured: 0,
    }
}

fn code_totals(snapshot: &ProjectStatsSnapshotWire) -> (u64, u64, u64) {
    if let Some(roles) = &snapshot.role_counts {
        let source = role(roles, ProjectStatsFileRoleWire::Source);
        let tests = role(roles, ProjectStatsFileRoleWire::Tests);
        return (
            source.files + tests.files,
            source.lines + tests.lines,
            tests.lines,
        );
    }
    (
        0,
        snapshot.tracked_lines.unwrap_or(0),
        snapshot.test_lines.unwrap_or(0),
    )
}

fn role(
    roles: &[ProjectStatsRoleCountWire],
    wanted: ProjectStatsFileRoleWire,
) -> ProjectStatsRoleCountWire {
    roles
        .iter()
        .find(|row| row.role == wanted)
        .cloned()
        .unwrap_or(ProjectStatsRoleCountWire {
            role: wanted,
            files: 0,
            lines: 0,
        })
}

fn disagreement_issue(
    identity: &str,
    spec: &ProjectStatsRepoSpecWire,
    chosen: &ResolvedRepo,
) -> ProjectStatsIssueWire {
    git::issue(
        spec,
        "code_tip_disagreement",
        format!(
            "code clones of identity {identity} resolved to multiple tips; using {}@{}",
            chosen.canonical_path, chosen.tip
        ),
    )
}

fn code_token(
    identity: &str,
    resolved: &ResolvedRepo,
    status: ProjectStatsRepoStatusWire,
    counter: ProjectStatsCodeCounterWire,
) -> String {
    let status = match status {
        ProjectStatsRepoStatusWire::Ok => "ok",
        ProjectStatsRepoStatusWire::Partial => "partial",
        ProjectStatsRepoStatusWire::Unavailable => "unavailable",
    };
    let counter = match counter {
        ProjectStatsCodeCounterWire::Nonblank => "nonblank",
        ProjectStatsCodeCounterWire::Sloc => "sloc",
    };
    serde_json::json!({
        "kind": "code",
        "identity": identity,
        "common_dir": resolved.common_dir,
        "ref": resolved.ref_name,
        "tip": resolved.tip,
        "status": status,
        "counter": counter,
        "classifier": PROJECT_STATS_CLASSIFIER_VERSION,
        "metric_policy": PROJECT_STATS_METRIC_POLICY_VERSION,
    })
    .to_string()
}

fn unavailable_token(identity: &str) -> String {
    serde_json::json!({
        "kind": "code",
        "identity": identity,
        "status": "unavailable",
        "classifier": PROJECT_STATS_CLASSIFIER_VERSION,
        "metric_policy": PROJECT_STATS_METRIC_POLICY_VERSION,
    })
    .to_string()
}

/// Merge a snapshot status into a repo row without promoting a failure to ok.
pub(crate) fn merge_status(
    current: ProjectStatsRepoStatusWire,
    extra: ProjectStatsRepoStatusWire,
) -> ProjectStatsRepoStatusWire {
    output::worse_status(current, extra)
}

pub(crate) fn check_code_invariants(
    code: &ProjectStatsCodeWire,
) -> Result<(), String> {
    match code.code_files {
        None => {
            if code.code_lines.is_some()
                || code.test_lines.is_some()
                || code.doc_files.is_some()
                || code.languages.is_some()
            {
                return Err(
                    "unavailable code must not mix null and measured fields"
                        .to_owned(),
                );
            }
            if code.test_share.is_some() {
                return Err(
                    "unavailable code must not report test_share".to_owned()
                );
            }
            if code.repos_measured != 0 {
                return Err("unavailable code must report zero measured repos"
                    .to_owned());
            }
        }
        Some(_) => {
            let lines = code.code_lines.ok_or_else(|| {
                "measured code_files requires code_lines".to_owned()
            })?;
            let tests = code.test_lines.ok_or_else(|| {
                "measured code_files requires test_lines".to_owned()
            })?;
            if tests > lines {
                return Err("test_lines exceed code_lines".to_owned());
            }
            if lines == 0 {
                if code.test_share.is_some() {
                    return Err(
                        "test_share must be null when code_lines is zero"
                            .to_owned(),
                    );
                }
            } else if code.test_share.is_none() {
                return Err(
                    "test_share is null for a measured nonzero denominator"
                        .to_owned(),
                );
            }
        }
    }
    Ok(())
}
