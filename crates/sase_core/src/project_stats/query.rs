use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::directory;
use super::git::{self, ResolvedRepo};
use super::history::{self, HistoryOutcome};
use super::output;
use super::wire::{
    validate_project_stats_request, ProjectStatsCoverageWire,
    ProjectStatsError, ProjectStatsFacetWire, ProjectStatsIssueWire,
    ProjectStatsPortfolioWire, ProjectStatsProjectSpecWire,
    ProjectStatsProjectWire, ProjectStatsRangeWire, ProjectStatsRepoSpecWire,
    ProjectStatsRepoTierWire, ProjectStatsRequestWire,
    ProjectStatsResponseWire, PROJECT_STATS_CLASSIFIER_VERSION,
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

/// Validate the request, walk unique repositories once, and fill Output
/// plus the Runs facet when requested.
///
/// Code and plan seams stay unimplemented until sibling phases land.
pub fn query_project_stats(
    request: &ProjectStatsRequestWire,
) -> Result<ProjectStatsResponseWire, ProjectStatsError> {
    validate_project_stats_request(request)?;

    let directory = directory::load_directory();
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
    let walks = load_unique_sources(request, &selected);
    let mut projects = Vec::with_capacity(selected.len());
    let mut all_issues = Vec::new();
    let mut all_tokens = BTreeSet::new();
    let mut facet_tokens = BTreeSet::new();
    let mut portfolio_code_rows = Vec::new();
    let mut portfolio_keys = BTreeMap::new();
    let wants_output = wants_facet(request, ProjectStatsFacetWire::Output);
    let wants_code = wants_facet(request, ProjectStatsFacetWire::Code);
    let wants_runs = wants_facet(request, ProjectStatsFacetWire::Runs);

    for project in selected {
        let measured = project_repos(project, &walks);
        let context = FacetContext {
            request,
            project,
            directory: &directory,
            repos: &measured,
        };
        let output_measured = output::measure(&context);
        let code_result = code::query(&context);
        let _plans = plans::query(&context);
        let mapped_runs = runs_snapshot
            .as_ref()
            .map(|snapshot| runs::map_project(snapshot, project, wants_runs));
        facet_tokens.extend(code_result.source_tokens);

        all_tokens.extend(output_measured.source_tokens.iter().cloned());
        all_issues.extend(output_measured.issues.iter().cloned());
        all_issues.extend(code_result.issues.iter().cloned());

        if wants_output {
            collect_portfolio_code(
                &measured,
                &mut portfolio_code_rows,
                &mut portfolio_keys,
            );
        }

        let mut repos = output_measured.repos;
        let (output, lifetime, models, coverage) = if wants_output {
            (
                Some(output_measured.output),
                Some(output_measured.lifetime),
                output_measured.models,
                output_measured.coverage,
            )
        } else {
            for repo in &mut repos {
                repo.output = None;
                repo.lifetime = None;
            }
            (
                None,
                None,
                Vec::new(),
                ProjectStatsCoverageWire {
                    configured_repos: project.code_repos.len() as u64,
                    ..ProjectStatsCoverageWire::default()
                },
            )
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
            code: if wants_code { code_result.value } else { None },
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
        portfolio_from_rows(
            request,
            &directory,
            &portfolio_code_rows,
            output::health_from_statuses(portfolio_keys.values().copied()),
        )
    } else {
        ProjectStatsPortfolioWire::default()
    };
    if wants_runs {
        portfolio.runs = runs::portfolio_runs(&projects);
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
        let walk_key = (
            resolved.identity.clone(),
            resolved.common_dir.clone(),
            resolved.tip.clone(),
        );
        let history = history_cache
            .entry(walk_key)
            .or_insert_with(|| {
                history::history_for_resolved(
                    &resolved,
                    spec,
                    &request.budget,
                    &request.cache_dir,
                )
            })
            .clone();
        by_spec.insert(
            instance,
            WalkedSource {
                resolved: Some(resolved),
                history: Some(history),
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
        "sources": source_tokens,
        "facet_tokens": facet_tokens,
    }))
    .map_err(|error| {
        ProjectStatsError::FingerprintSerialization(error.to_string())
    })?;
    Ok(hex::encode(Sha256::digest(payload)))
}
