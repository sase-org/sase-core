use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::directory;
use super::git::{self, ResolvedRepo};
use super::history::{self, HistoryOutcome};
use super::output;
use super::wire::{
    validate_project_stats_request, ProjectStatsCodeCounterWire,
    ProjectStatsCodeWire, ProjectStatsCoverageWire, ProjectStatsError,
    ProjectStatsFacetWire, ProjectStatsIssueWire, ProjectStatsPortfolioWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectWire,
    ProjectStatsRangeWire, ProjectStatsRepoSpecWire,
    ProjectStatsRepoStatusWire, ProjectStatsRepoTierWire, ProjectStatsRepoWire,
    ProjectStatsRequestWire, ProjectStatsResponseWire,
    ProjectStatsSnapshotWire, PROJECT_STATS_CLASSIFIER_VERSION,
    PROJECT_STATS_ENGINE_VERSION, PROJECT_STATS_METRIC_POLICY_VERSION,
    PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use super::{code, plans, runs, FacetContext, MeasuredRepo};

#[derive(Clone)]
struct WalkedSource {
    resolved: Option<ResolvedRepo>,
    history: Option<Arc<HistoryOutcome>>,
    issue: Option<ProjectStatsIssueWire>,
}

type SpecInstanceKey = (String, Vec<String>, String, String);
type RecordKey = (String, String, String);

/// Validate the request, resolve unique repositories, and fill requested
/// facets. History walks, directory loads, and blob reads run only for the
/// facets that need them.
pub fn query_project_stats(
    request: &ProjectStatsRequestWire,
) -> Result<ProjectStatsResponseWire, ProjectStatsError> {
    validate_project_stats_request(request)?;

    let wants_output = wants_facet(request, ProjectStatsFacetWire::Output);
    let wants_code = wants_facet(request, ProjectStatsFacetWire::Code);
    let wants_runs = wants_facet(request, ProjectStatsFacetWire::Runs);
    let need_git = wants_output || wants_code;
    let counter = ProjectStatsCodeCounterWire::Sloc;

    let directory_snapshot = if wants_output {
        directory::load(request)
    } else {
        directory::DirectorySnapshot::default()
    };
    let directory = directory_snapshot.directory;
    let runs_snapshot = if runs::needs_snapshot(request) {
        Some(runs::load(request))
    } else {
        None
    };
    let selected: Vec<&ProjectStatsProjectSpecWire> = request
        .projects
        .iter()
        .filter(|project| {
            runs::include_project(project, runs_snapshot.as_ref())
        })
        .collect();
    let walks = if need_git {
        load_unique_sources(request, &selected, wants_output)
    } else {
        BTreeMap::new()
    };

    let project_repos: Vec<(
        &ProjectStatsProjectSpecWire,
        Vec<MeasuredRepo<'_>>,
    )> = selected
        .iter()
        .map(|project| (*project, project_repos(project, &walks)))
        .collect();
    let all_repos: Vec<&MeasuredRepo<'_>> = project_repos
        .iter()
        .flat_map(|(_, repos)| repos.iter())
        .collect();
    let (code_by_identity, code_issues) = if wants_code {
        code::measure_identities(
            &all_repos,
            &request.budget,
            &request.cache_dir,
            counter,
        )
    } else {
        (BTreeMap::new(), Vec::new())
    };

    let mut projects = Vec::with_capacity(selected.len());
    let mut all_issues = Vec::new();
    let mut all_tokens = BTreeSet::new();
    let mut facet_tokens = BTreeSet::new();
    let mut portfolio_code_rows = Vec::new();
    let mut portfolio_keys = BTreeMap::new();
    let mut portfolio_plans = Vec::new();
    let mut records_cache: BTreeMap<RecordKey, code::RecordMeasure> =
        BTreeMap::new();
    all_tokens.extend(directory_snapshot.source_tokens.iter().cloned());
    all_issues.extend(directory_snapshot.issues.iter().cloned());

    for (project, measured) in &project_repos {
        let context = FacetContext {
            request,
            project,
            directory: &directory,
            repos: measured,
        };
        let (mut repos, output_measured) = if wants_output {
            let output_measured = output::measure(&context);
            (output_measured.repos.clone(), Some(output_measured))
        } else {
            (
                measured
                    .iter()
                    .map(|repo| output::repo_wire_from_resolve(repo, need_git))
                    .collect(),
                None,
            )
        };
        let plan_measured = if wants_output {
            Some(plans::measure(&context))
        } else {
            None
        };
        let mapped_runs = runs_snapshot
            .as_ref()
            .map(|snapshot| runs::map_project(snapshot, project, wants_runs));

        if let Some(output_measured) = &output_measured {
            all_tokens.extend(output_measured.source_tokens.iter().cloned());
            all_issues.extend(output_measured.issues.iter().cloned());
        }
        if let Some(plans) = &plan_measured {
            facet_tokens.extend(plans.source_tokens.iter().cloned());
            all_issues.extend(plans.issues.iter().cloned());
            if plans.measured {
                portfolio_plans.push(plans.clone());
            }
        }
        if wants_output {
            collect_portfolio_code(
                measured,
                &mut portfolio_code_rows,
                &mut portfolio_keys,
            );
        }

        let project_code = if wants_code {
            Some(attach_code(
                project,
                measured,
                &mut repos,
                &code_by_identity,
                &mut CodeAttachState {
                    records_cache: &mut records_cache,
                    facet_tokens: &mut facet_tokens,
                    all_issues: &mut all_issues,
                    budget: &request.budget,
                    cache_dir: &request.cache_dir,
                    counter,
                },
            ))
        } else {
            None
        };

        let (output, lifetime, models, coverage) = if let Some(measured) =
            output_measured
        {
            let mut output = measured.output;
            if let Some(plans) = &plan_measured {
                super::aggregate::apply_plan_measurement(&mut output, plans);
            }
            let models = output.models.clone();
            let coverage = output.commit_coverage.clone();
            (Some(output), Some(measured.lifetime), models, coverage)
        } else {
            let coverage = project_code
                .as_ref()
                .map(|code| ProjectStatsCoverageWire {
                    configured_repos: code.repos_configured,
                    measured_repos: code.repos_measured,
                    unavailable_repos: code
                        .repos_configured
                        .saturating_sub(code.repos_measured),
                    ..ProjectStatsCoverageWire::default()
                })
                .unwrap_or_else(|| ProjectStatsCoverageWire {
                    configured_repos: project.code_repos.len() as u64,
                    ..ProjectStatsCoverageWire::default()
                });
            (None, None, Vec::new(), coverage)
        };

        let (runs_value, patches, truncated_patch_rows) = match mapped_runs {
            Some(mapped) => {
                (mapped.runs, mapped.patches, mapped.truncated_patch_rows)
            }
            None => (None, Vec::new(), 0),
        };

        projects.push(ProjectStatsProjectWire {
            key: project.key.clone(),
            label: project.label.clone(),
            state: project.state,
            output,
            lifetime,
            code: project_code,
            runs: runs_value,
            repos,
            not_counted: project.not_counted.clone(),
            models,
            patches,
            truncated_patch_rows,
            coverage,
        });
    }

    if let Some(snapshot) = &runs_snapshot {
        facet_tokens.extend(snapshot.source_tokens.iter().cloned());
        if wants_runs {
            all_issues.extend(snapshot.issues.iter().cloned());
            if let Some(issue) = runs::truncation_issue(snapshot) {
                all_issues.push(issue);
            }
        }
        if request.include_run_only_projects {
            let claimed = runs::claimed_index_names(&request.projects);
            projects.extend(runs::unregistered_projects(
                snapshot, &claimed, wants_runs,
            ));
        }
    }

    let mut portfolio = if wants_output {
        let mut portfolio = portfolio_from_rows(
            request,
            &directory,
            &portfolio_code_rows,
            output::health_from_statuses(portfolio_keys.values().copied()),
        );
        if let Some(plans) = plans::combine(&portfolio_plans) {
            if let Some(output) = &mut portfolio.output {
                super::aggregate::apply_plan_measurement(output, &plans);
                portfolio.models = output.models.clone();
            }
        }
        portfolio
    } else {
        ProjectStatsPortfolioWire::default()
    };
    if wants_runs {
        portfolio.runs = runs::portfolio_runs(&projects);
    }
    if wants_code {
        portfolio.code = Some(portfolio_code(&code_by_identity, counter));
        for outcome in code_by_identity.values() {
            facet_tokens.insert(outcome.token.clone());
        }
        for outcome in records_cache.values() {
            facet_tokens.insert(outcome.token.clone());
        }
    }

    for issue in code_issues {
        if !all_issues
            .iter()
            .any(|existing| issue_payload_matches(existing, &issue))
        {
            all_issues.push(issue);
        }
    }

    let fingerprint = fingerprint(
        request,
        all_tokens.into_iter().collect(),
        facet_tokens.into_iter().collect(),
    )?;
    Ok(ProjectStatsResponseWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        metric_policy_version: PROJECT_STATS_METRIC_POLICY_VERSION,
        classifier_version: PROJECT_STATS_CLASSIFIER_VERSION,
        engine_version: PROJECT_STATS_ENGINE_VERSION,
        range: ProjectStatsRangeWire {
            start_ts: request.start_ts,
            end_ts: request.end_ts,
        },
        fingerprint,
        projects,
        portfolio,
        issues: all_issues,
    })
}

struct CodeAttachState<'a> {
    records_cache: &'a mut BTreeMap<RecordKey, code::RecordMeasure>,
    facet_tokens: &'a mut BTreeSet<String>,
    all_issues: &'a mut Vec<ProjectStatsIssueWire>,
    budget: &'a super::wire::ProjectStatsBudgetWire,
    cache_dir: &'a str,
    counter: ProjectStatsCodeCounterWire,
}

fn attach_code(
    project: &ProjectStatsProjectSpecWire,
    measured: &[MeasuredRepo<'_>],
    repos: &mut [ProjectStatsRepoWire],
    code_by_identity: &BTreeMap<String, code::IdentityCode>,
    state: &mut CodeAttachState<'_>,
) -> ProjectStatsCodeWire {
    let mut snapshots = Vec::new();
    let mut seen_identities = BTreeSet::new();
    for (repo, wire) in measured.iter().zip(repos.iter_mut()) {
        if repo.tier == ProjectStatsRepoTierWire::Records {
            let key = record_key(repo);
            let outcome = state
                .records_cache
                .entry(key)
                .or_insert_with(|| {
                    code::measure_records_repo(
                        repo.resolved,
                        repo.spec,
                        repo.issue,
                        state.budget,
                        state.cache_dir,
                    )
                })
                .clone();
            apply_snapshot(
                wire,
                project,
                outcome.snapshot,
                outcome.status,
                outcome.issue,
                None,
                state.all_issues,
            );
            state.facet_tokens.insert(outcome.token);
            continue;
        }
        let identity = code::semantic_identity(repo);
        let Some(outcome) = code_by_identity.get(&identity) else {
            continue;
        };
        apply_snapshot(
            wire,
            project,
            outcome.snapshot.clone(),
            outcome.status,
            outcome.issue.clone(),
            outcome.disagreement_issue.clone(),
            state.all_issues,
        );
        if seen_identities.insert(identity) {
            if let Some(snapshot) = &outcome.snapshot {
                snapshots.push(snapshot.clone());
            }
            state.facet_tokens.insert(outcome.token.clone());
        }
    }
    let configured = seen_identities.len() as u64;
    let measured_count = snapshots.len() as u64;
    let refs: Vec<&ProjectStatsSnapshotWire> = snapshots.iter().collect();
    code::aggregate(&refs, configured, measured_count, state.counter)
}

fn apply_snapshot(
    wire: &mut ProjectStatsRepoWire,
    project: &ProjectStatsProjectSpecWire,
    snapshot: Option<ProjectStatsSnapshotWire>,
    status: ProjectStatsRepoStatusWire,
    issue: Option<ProjectStatsIssueWire>,
    disagreement_issue: Option<ProjectStatsIssueWire>,
    all_issues: &mut Vec<ProjectStatsIssueWire>,
) {
    wire.status = code::merge_status(wire.status, status);
    wire.snapshot = snapshot;
    for issue in [issue, disagreement_issue].into_iter().flatten() {
        let issue = output::with_project(issue, &project.key);
        if !all_issues.iter().any(|existing| existing == &issue) {
            all_issues.push(issue.clone());
        }
        if wire.issue.is_none() {
            wire.issue = Some(issue);
        }
    }
}

fn issue_payload_matches(
    left: &ProjectStatsIssueWire,
    right: &ProjectStatsIssueWire,
) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.project = None;
    right.project = None;
    left == right
}

fn record_key(repo: &MeasuredRepo<'_>) -> RecordKey {
    match repo.resolved {
        Some(resolved) => (
            resolved.identity.clone(),
            resolved.common_dir.clone(),
            resolved.tip.clone(),
        ),
        None => (
            format!("path:{}", repo.spec.path),
            String::new(),
            String::new(),
        ),
    }
}

fn portfolio_code(
    code_by_identity: &BTreeMap<String, code::IdentityCode>,
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsCodeWire {
    let configured = code_by_identity.len() as u64;
    let snapshots: Vec<&ProjectStatsSnapshotWire> = code_by_identity
        .values()
        .filter_map(|outcome| outcome.snapshot.as_ref())
        .collect();
    let measured = snapshots.len() as u64;
    code::aggregate(&snapshots, configured, measured, counter)
}

fn wants_facet(
    request: &ProjectStatsRequestWire,
    facet: ProjectStatsFacetWire,
) -> bool {
    request.facets.contains(&facet)
}

fn spec_key(spec: &ProjectStatsRepoSpecWire) -> (String, Vec<String>) {
    let path = Path::new(&spec.path)
        .canonicalize()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| spec.path.clone());
    (path, spec.ref_candidates.clone())
}

fn spec_instance_key(spec: &ProjectStatsRepoSpecWire) -> SpecInstanceKey {
    let (path, refs) = spec_key(spec);
    (path, refs, spec.id.clone(), spec.name.clone())
}

fn load_unique_sources(
    request: &ProjectStatsRequestWire,
    projects: &[&ProjectStatsProjectSpecWire],
    walk_history: bool,
) -> BTreeMap<SpecInstanceKey, WalkedSource> {
    let mut resolve_cache: BTreeMap<
        (String, Vec<String>),
        Result<ResolvedRepo, ProjectStatsIssueWire>,
    > = BTreeMap::new();
    let mut history_cache: BTreeMap<
        (String, String, String),
        Arc<HistoryOutcome>,
    > = BTreeMap::new();
    let mut by_spec = BTreeMap::new();
    let mut specs = Vec::new();
    for project in projects {
        for spec in &project.code_repos {
            specs.push(spec);
        }
        for spec in &project.record_repos {
            specs.push(spec);
        }
    }
    for spec in specs {
        let instance = spec_instance_key(spec);
        if by_spec.contains_key(&instance) {
            continue;
        }
        let resolve_key = spec_key(spec);
        let resolved = match resolve_cache
            .entry(resolve_key)
            .or_insert_with(|| git::resolve_repo(spec, &request.budget))
        {
            Err(issue) => {
                by_spec.insert(
                    instance,
                    WalkedSource {
                        resolved: None,
                        history: None,
                        issue: Some(git::retarget_issue(issue.clone(), spec)),
                    },
                );
                continue;
            }
            Ok(resolved) => resolved.clone(),
        };
        let history = if walk_history {
            let walk_key = (
                resolved.identity.clone(),
                resolved.common_dir.clone(),
                resolved.tip.clone(),
            );
            Some(
                history_cache
                    .entry(walk_key)
                    .or_insert_with(|| {
                        history::history_for_resolved(
                            &resolved,
                            spec,
                            &request.budget,
                            &request.cache_dir,
                        )
                    })
                    .clone(),
            )
        } else {
            None
        };
        by_spec.insert(
            instance,
            WalkedSource {
                resolved: Some(resolved),
                history,
                issue: None,
            },
        );
    }
    by_spec
}

fn project_repos<'a>(
    project: &'a ProjectStatsProjectSpecWire,
    walks: &'a BTreeMap<SpecInstanceKey, WalkedSource>,
) -> Vec<MeasuredRepo<'a>> {
    let mut repos = Vec::new();
    for spec in &project.code_repos {
        repos.push(measured_repo(spec, ProjectStatsRepoTierWire::Code, walks));
    }
    for spec in &project.record_repos {
        repos.push(measured_repo(
            spec,
            ProjectStatsRepoTierWire::Records,
            walks,
        ));
    }
    repos
}

fn measured_repo<'a>(
    spec: &'a ProjectStatsRepoSpecWire,
    tier: ProjectStatsRepoTierWire,
    walks: &'a BTreeMap<SpecInstanceKey, WalkedSource>,
) -> MeasuredRepo<'a> {
    let walked = walks.get(&spec_instance_key(spec));
    MeasuredRepo {
        spec,
        tier,
        resolved: walked.and_then(|item| item.resolved.as_ref()),
        history: walked.and_then(|item| item.history.as_deref()),
        issue: walked.and_then(|item| item.issue.as_ref()),
    }
}

fn collect_portfolio_code<'a>(
    repos: &[MeasuredRepo<'a>],
    rows: &mut Vec<&'a crate::project_stats::history::HistoryRow>,
    health_by_identity: &mut BTreeMap<
        String,
        crate::project_stats::wire::ProjectStatsRepoStatusWire,
    >,
) {
    for repo in repos {
        if repo.tier != ProjectStatsRepoTierWire::Code {
            continue;
        }
        if let Some(history) = repo.history {
            if !history.failed {
                rows.extend(history.rows.iter());
            }
        }
        let key = output::coverage_identity_key(repo);
        let status =
            output::repo_status(repo.resolved, repo.history, repo.issue);
        health_by_identity
            .entry(key)
            .and_modify(|existing| {
                *existing = output::worse_status(*existing, status);
            })
            .or_insert(status);
    }
}

fn portfolio_from_rows(
    request: &ProjectStatsRequestWire,
    directory: &directory::Directory,
    rows: &[&crate::project_stats::history::HistoryRow],
    health: output::CoverageHealth,
) -> ProjectStatsPortfolioWire {
    use super::aggregate::{
        attribute_rows, attribution_coverage, lifetime,
        output_from_attributions, rows_in_output_range, unavailable_lifetime,
        unavailable_output, unique_by_sha,
    };
    let unique = unique_by_sha(rows.iter().copied());
    let in_range =
        rows_in_output_range(&unique, request.start_ts, request.end_ts);
    let attributions =
        attribute_rows(&in_range, directory, &request.known_model_aliases);
    let coverage = attribution_coverage(
        health.configured,
        health.measured,
        health.partial,
        health.unavailable,
        &attributions,
    );
    let (output, lifetime) = if health.measured == 0 {
        (unavailable_output(coverage.clone()), unavailable_lifetime())
    } else {
        (
            output_from_attributions(&attributions, coverage.clone()),
            lifetime(&unique, request.now_ts, &request.timezone),
        )
    };
    ProjectStatsPortfolioWire {
        models: output.models.clone(),
        output: Some(output),
        lifetime: Some(lifetime),
        code: None,
        runs: None,
        coverage,
    }
}

fn fingerprint(
    request: &ProjectStatsRequestWire,
    source_tokens: Vec<String>,
    facet_tokens: Vec<String>,
) -> Result<String, ProjectStatsError> {
    let mut semantic_request = request.clone();
    semantic_request.cache_dir.clear();
    let request_value =
        serde_json::to_value(&semantic_request).map_err(|error| {
            ProjectStatsError::FingerprintSerialization(error.to_string())
        })?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "request": request_value,
        "engine": PROJECT_STATS_ENGINE_VERSION,
        "metric_policy": PROJECT_STATS_METRIC_POLICY_VERSION,
        "classifier": PROJECT_STATS_CLASSIFIER_VERSION,
        "counter_version": super::counter::COUNTER_VERSION,
        "sources": source_tokens,
        "facet_tokens": facet_tokens,
    }))
    .map_err(|error| {
        ProjectStatsError::FingerprintSerialization(error.to_string())
    })?;
    Ok(hex::encode(Sha256::digest(payload)))
}
