use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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
    history: Option<HistoryOutcome>,
    issue: Option<ProjectStatsIssueWire>,
}

/// Validate the request, walk unique repositories once, and fill Output.
///
/// Code, Runs, and plan seams stay unimplemented until sibling phases land.
/// `selection: if_runs` is preserved as a dispatch hook and does not invent
/// run counts.
pub fn query_project_stats(
    request: &ProjectStatsRequestWire,
) -> Result<ProjectStatsResponseWire, ProjectStatsError> {
    validate_project_stats_request(request)?;

    let directory = directory::load_directory();
    let walks = load_unique_sources(request);
    let mut projects = Vec::with_capacity(request.projects.len());
    let mut all_issues = Vec::new();
    let mut all_tokens = BTreeSet::new();
    let mut facet_tokens = BTreeSet::new();
    let mut portfolio_code_rows = Vec::new();
    let mut portfolio_health = PortfolioHealth::default();
    let mut portfolio_keys = BTreeSet::new();
    let wants_output = wants_facet(request, ProjectStatsFacetWire::Output);
    let wants_code = wants_facet(request, ProjectStatsFacetWire::Code);
    let wants_runs = wants_facet(request, ProjectStatsFacetWire::Runs);

    for project in &request.projects {
        if !output::include_project(project) {
            continue;
        }
        let measured = project_repos(project, &walks);
        let context = FacetContext {
            request,
            project,
            directory: &directory,
            repos: &measured,
        };
        let output_measured = output::measure(&context);
        let code_result = code::query(&context);
        let runs_result = runs::query(&context);
        let _plans = plans::query(&context);
        facet_tokens.extend(code_result.source_tokens);
        facet_tokens.extend(runs_result.source_tokens);

        all_tokens.extend(output_measured.source_tokens.iter().cloned());
        all_issues.extend(output_measured.issues.iter().cloned());
        all_issues.extend(code_result.issues.iter().cloned());
        all_issues.extend(runs_result.issues.iter().cloned());

        if wants_output {
            collect_portfolio_code(
                &measured,
                &mut portfolio_code_rows,
                &mut portfolio_health,
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

        projects.push(ProjectStatsProjectWire {
            key: project.key.clone(),
            label: project.label.clone(),
            state: project.state,
            output,
            lifetime,
            code: if wants_code { code_result.value } else { None },
            runs: if wants_runs { runs_result.value } else { None },
            repos,
            not_counted: project.not_counted.clone(),
            models,
            patches: Vec::new(),
            truncated_patch_rows: 0,
            coverage,
        });
    }

    let portfolio = if wants_output {
        portfolio_from_rows(
            request,
            &directory,
            &portfolio_code_rows,
            portfolio_health,
        )
    } else {
        ProjectStatsPortfolioWire::default()
    };

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

fn load_unique_sources(
    request: &ProjectStatsRequestWire,
) -> BTreeMap<(String, Vec<String>), WalkedSource> {
    let mut walks = Vec::new();
    let mut by_spec = BTreeMap::new();
    let mut by_walk_key = BTreeMap::new();
    let mut specs = Vec::new();
    for project in &request.projects {
        for spec in &project.code_repos {
            specs.push(spec);
        }
        for spec in &project.record_repos {
            specs.push(spec);
        }
    }
    for spec in specs {
        let key = spec_key(spec);
        if by_spec.contains_key(&key) {
            continue;
        }
        match git::resolve_repo(spec, &request.budget) {
            Err(issue) => {
                let index = walks.len();
                walks.push(WalkedSource {
                    resolved: None,
                    history: None,
                    issue: Some(issue),
                });
                by_spec.insert(key, index);
            }
            Ok(resolved) => {
                let walk_key = (
                    resolved.identity.clone(),
                    resolved.common_dir.clone(),
                    resolved.tip.clone(),
                );
                if let Some(&index) = by_walk_key.get(&walk_key) {
                    by_spec.insert(key, index);
                    continue;
                }
                let history = history::history_for_resolved(
                    &resolved,
                    spec,
                    &request.budget,
                    &request.cache_dir,
                );
                let index = walks.len();
                walks.push(WalkedSource {
                    resolved: Some(resolved),
                    history: Some(history),
                    issue: None,
                });
                by_walk_key.insert(walk_key, index);
                by_spec.insert(key, index);
            }
        }
    }
    by_spec
        .into_iter()
        .map(|(key, index)| (key, walks[index].clone()))
        .collect()
}

fn project_repos<'a>(
    project: &'a ProjectStatsProjectSpecWire,
    walks: &'a BTreeMap<(String, Vec<String>), WalkedSource>,
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
    walks: &'a BTreeMap<(String, Vec<String>), WalkedSource>,
) -> MeasuredRepo<'a> {
    let walked = walks.get(&spec_key(spec));
    MeasuredRepo {
        spec,
        tier,
        resolved: walked.and_then(|item| item.resolved.as_ref()),
        history: walked.and_then(|item| item.history.as_ref()),
        issue: walked.and_then(|item| item.issue.as_ref()),
    }
}

#[derive(Default)]
struct PortfolioHealth {
    configured: u64,
    measured: u64,
    partial: u64,
    unavailable: u64,
}

fn collect_portfolio_code<'a>(
    repos: &[MeasuredRepo<'a>],
    rows: &mut Vec<&'a crate::project_stats::history::HistoryRow>,
    health: &mut PortfolioHealth,
    seen: &mut BTreeSet<String>,
) {
    use super::wire::ProjectStatsRepoStatusWire;
    for repo in repos {
        if repo.tier != ProjectStatsRepoTierWire::Code {
            continue;
        }
        let key = match repo.resolved {
            Some(resolved) => {
                format!("{}\n{}", resolved.identity, resolved.common_dir)
            }
            None => format!("path:{}", repo.spec.path),
        };
        if !seen.insert(key) {
            continue;
        }
        health.configured += 1;
        let status = match (repo.resolved, repo.history, repo.issue) {
            (_, None, Some(_)) => ProjectStatsRepoStatusWire::Unavailable,
            (Some(resolved), Some(history), _)
                if history.complete && !resolved.shallow =>
            {
                ProjectStatsRepoStatusWire::Ok
            }
            (Some(_), Some(_), _) => ProjectStatsRepoStatusWire::Partial,
            _ => ProjectStatsRepoStatusWire::Unavailable,
        };
        match status {
            ProjectStatsRepoStatusWire::Ok => health.measured += 1,
            ProjectStatsRepoStatusWire::Partial => {
                health.measured += 1;
                health.partial += 1;
            }
            ProjectStatsRepoStatusWire::Unavailable => health.unavailable += 1,
        }
        if let Some(history) = repo.history {
            rows.extend(history.rows.iter());
        }
    }
}

fn portfolio_from_rows(
    request: &ProjectStatsRequestWire,
    directory: &directory::Directory,
    rows: &[&crate::project_stats::history::HistoryRow],
    health: PortfolioHealth,
) -> ProjectStatsPortfolioWire {
    use super::aggregate::{
        attribute_rows, attribution_coverage, lifetime,
        output_from_attributions, rows_in_output_range, unique_by_sha,
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
    let output = output_from_attributions(&attributions, coverage.clone());
    let lifetime = lifetime(&unique, request.now_ts, &request.timezone);
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
