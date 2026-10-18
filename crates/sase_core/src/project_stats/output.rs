//! Output facet: range-scoped commits plus lifetime history.

use std::collections::BTreeMap;

use super::aggregate::{
    attribute_rows, attribution_coverage, lifetime, output_from_attributions,
    rows_in_output_range, unavailable_lifetime, unavailable_output,
    unique_by_sha,
};
use super::git::ResolvedRepo;
use super::history::HistoryOutcome;
use super::wire::{
    ProjectStatsCoverageWire, ProjectStatsIssueWire, ProjectStatsLifetimeWire,
    ProjectStatsOutputWire, ProjectStatsRefWire, ProjectStatsRepoStatusWire,
    ProjectStatsRepoTierWire, ProjectStatsRepoWire,
};
use super::FacetContext;

pub(crate) struct OutputMeasurement {
    pub output: ProjectStatsOutputWire,
    pub lifetime: ProjectStatsLifetimeWire,
    pub repos: Vec<ProjectStatsRepoWire>,
    pub issues: Vec<ProjectStatsIssueWire>,
    pub source_tokens: Vec<String>,
}

pub(crate) fn measure(context: &FacetContext<'_>) -> OutputMeasurement {
    let mut issues = Vec::new();
    let mut source_tokens = Vec::new();
    let mut repo_wires = Vec::new();
    let mut code_rows = Vec::new();
    let mut unique_code_health = BTreeMap::new();
    let mut measured_code_histories = 0;
    let mut sole_code: Option<(
        ProjectStatsOutputWire,
        ProjectStatsLifetimeWire,
    )> = None;

    for repo in context.repos {
        let (wire, token, extra_issues) = measure_repo(context, repo);
        source_tokens.push(token);
        issues.extend(extra_issues);
        if let Some(issue) = &wire.issue {
            if !issues.iter().any(|existing| existing == issue) {
                issues.push(issue.clone());
            }
        }
        if repo.tier == ProjectStatsRepoTierWire::Code {
            let key = coverage_identity_key(repo);
            unique_code_health
                .entry(key)
                .and_modify(|existing| {
                    *existing = worse_status(*existing, wire.status);
                })
                .or_insert(wire.status);
            if let Some(history) = repo.history {
                if !history.failed {
                    measured_code_histories += 1;
                    code_rows.extend(history.rows.iter());
                    if let (Some(output), Some(lifetime)) =
                        (&wire.output, &wire.lifetime)
                    {
                        sole_code = Some((output.clone(), lifetime.clone()));
                    }
                }
            }
        }
        repo_wires.push(wire);
    }

    let health = health_from_statuses(unique_code_health.values().copied());
    let (output, lifetime) = if health.measured == 0 {
        let coverage = ProjectStatsCoverageWire {
            configured_repos: health.configured,
            measured_repos: 0,
            partial_repos: health.partial,
            unavailable_repos: health.unavailable,
            ..ProjectStatsCoverageWire::default()
        };
        (unavailable_output(coverage), unavailable_lifetime())
    } else if measured_code_histories == 1 {
        if let Some((mut output, lifetime)) = sole_code {
            output.commit_coverage.configured_repos = health.configured;
            output.commit_coverage.measured_repos = health.measured;
            output.commit_coverage.partial_repos = health.partial;
            output.commit_coverage.unavailable_repos = health.unavailable;
            (output, lifetime)
        } else {
            project_from_code_rows(context, &code_rows, &health)
        }
    } else {
        project_from_code_rows(context, &code_rows, &health)
    };
    OutputMeasurement {
        output,
        lifetime,
        repos: repo_wires,
        issues,
        source_tokens,
    }
}

fn project_from_code_rows(
    context: &FacetContext<'_>,
    code_rows: &[&super::history::HistoryRow],
    health: &CoverageHealth,
) -> (ProjectStatsOutputWire, ProjectStatsLifetimeWire) {
    let unique_code = unique_by_sha(code_rows.iter().copied());
    let in_range = rows_in_output_range(
        &unique_code,
        context.request.start_ts,
        context.request.end_ts,
    );
    let attributions = attribute_rows(
        &in_range,
        context.directory,
        &context.request.known_model_aliases,
    );
    let coverage = attribution_coverage(
        health.configured,
        health.measured,
        health.partial,
        health.unavailable,
        &attributions,
    );
    (
        output_from_attributions(&attributions, coverage),
        lifetime(
            &unique_code,
            context.request.now_ts,
            &context.request.timezone,
        ),
    )
}

fn measure_repo(
    context: &FacetContext<'_>,
    repo: &super::MeasuredRepo<'_>,
) -> (ProjectStatsRepoWire, String, Vec<ProjectStatsIssueWire>) {
    let status = repo_status(repo.resolved, repo.history, repo.issue);
    let mut extra_issues = Vec::new();
    if let Some(history) = repo.history {
        for issue in &history.issues {
            extra_issues.push(with_project(
                crate::project_stats::git::retarget_issue(
                    issue.clone(),
                    repo.spec,
                ),
                &context.project.key,
            ));
        }
    }
    let issue = repo
        .issue
        .cloned()
        .or_else(|| {
            repo.history
                .and_then(|history| history.issues.first().cloned())
                .filter(|_| status != ProjectStatsRepoStatusWire::Ok)
        })
        .map(|issue| {
            with_project(
                crate::project_stats::git::retarget_issue(issue, repo.spec),
                &context.project.key,
            )
        });

    let (output, lifetime) = match repo.history {
        Some(history) if !history.failed => {
            let rows: Vec<_> = history.rows.iter().collect();
            let in_range = rows_in_output_range(
                &rows,
                context.request.start_ts,
                context.request.end_ts,
            );
            let attributions = attribute_rows(
                &in_range,
                context.directory,
                &context.request.known_model_aliases,
            );
            let coverage = attribution_coverage(
                1,
                u64::from(status != ProjectStatsRepoStatusWire::Unavailable),
                u64::from(status == ProjectStatsRepoStatusWire::Partial),
                u64::from(status == ProjectStatsRepoStatusWire::Unavailable),
                &attributions,
            );
            (
                Some(output_from_attributions(&attributions, coverage)),
                Some(lifetime(
                    &rows,
                    context.request.now_ts,
                    &context.request.timezone,
                )),
            )
        }
        _ => (None, None),
    };

    let reference = repo.resolved.map(|resolved| ProjectStatsRefWire {
        name: resolved.ref_name.clone(),
        commit: resolved.tip.clone(),
        shallow: resolved.shallow,
        fetched_at: resolved.fetched_at,
    });
    let wire = ProjectStatsRepoWire {
        id: repo.spec.id.clone(),
        name: repo.spec.name.clone(),
        kind: repo.spec.kind,
        tier: repo.tier,
        status,
        issue,
        reference,
        output,
        lifetime,
        snapshot: None,
    };
    let token = source_token(
        repo.spec.path.as_str(),
        repo.resolved,
        repo.history,
        status,
    );
    (wire, token, extra_issues)
}

pub(crate) fn repo_wire_from_resolve(
    repo: &super::MeasuredRepo<'_>,
    git_requested: bool,
) -> ProjectStatsRepoWire {
    let status = if !git_requested {
        ProjectStatsRepoStatusWire::Ok
    } else if repo.issue.is_some() && repo.resolved.is_none() {
        ProjectStatsRepoStatusWire::Unavailable
    } else if repo.resolved.is_some() {
        ProjectStatsRepoStatusWire::Ok
    } else {
        ProjectStatsRepoStatusWire::Unavailable
    };
    let issue = repo.issue.cloned();
    let reference = repo.resolved.map(|resolved| ProjectStatsRefWire {
        name: resolved.ref_name.clone(),
        commit: resolved.tip.clone(),
        shallow: resolved.shallow,
        fetched_at: resolved.fetched_at,
    });
    ProjectStatsRepoWire {
        id: repo.spec.id.clone(),
        name: repo.spec.name.clone(),
        kind: repo.spec.kind,
        tier: repo.tier,
        status,
        issue,
        reference,
        output: None,
        lifetime: None,
        snapshot: None,
    }
}

pub(crate) fn repo_status(
    resolved: Option<&ResolvedRepo>,
    history: Option<&HistoryOutcome>,
    issue: Option<&ProjectStatsIssueWire>,
) -> ProjectStatsRepoStatusWire {
    if issue.is_some() && history.is_none() {
        return ProjectStatsRepoStatusWire::Unavailable;
    }
    match (resolved, history) {
        (_, Some(history)) if history.failed => {
            ProjectStatsRepoStatusWire::Unavailable
        }
        (Some(resolved), Some(history))
            if history.complete && !resolved.shallow =>
        {
            ProjectStatsRepoStatusWire::Ok
        }
        (Some(_), Some(_)) => ProjectStatsRepoStatusWire::Partial,
        _ => ProjectStatsRepoStatusWire::Unavailable,
    }
}

pub(crate) fn coverage_identity_key(repo: &super::MeasuredRepo<'_>) -> String {
    match repo.resolved {
        Some(resolved) => format!("id:{}", resolved.identity),
        None => format!("path:{}", repo.spec.path),
    }
}

pub(crate) fn worse_status(
    left: ProjectStatsRepoStatusWire,
    right: ProjectStatsRepoStatusWire,
) -> ProjectStatsRepoStatusWire {
    use ProjectStatsRepoStatusWire::{Ok, Partial, Unavailable};
    match (left, right) {
        (Unavailable, _) | (_, Unavailable) => Unavailable,
        (Partial, _) | (_, Partial) => Partial,
        (Ok, Ok) => Ok,
    }
}

pub(crate) struct CoverageHealth {
    pub configured: u64,
    pub measured: u64,
    pub partial: u64,
    pub unavailable: u64,
}

pub(crate) fn health_from_statuses(
    statuses: impl IntoIterator<Item = ProjectStatsRepoStatusWire>,
) -> CoverageHealth {
    let mut health = CoverageHealth {
        configured: 0,
        measured: 0,
        partial: 0,
        unavailable: 0,
    };
    for status in statuses {
        health.configured += 1;
        match status {
            ProjectStatsRepoStatusWire::Ok => health.measured += 1,
            ProjectStatsRepoStatusWire::Partial => {
                health.measured += 1;
                health.partial += 1;
            }
            ProjectStatsRepoStatusWire::Unavailable => health.unavailable += 1,
        }
    }
    health
}

pub(crate) fn with_project(
    mut issue: ProjectStatsIssueWire,
    project: &str,
) -> ProjectStatsIssueWire {
    if issue.project.is_none() {
        issue.project = Some(project.to_owned());
    }
    issue
}

fn source_token(
    path: &str,
    resolved: Option<&ResolvedRepo>,
    history: Option<&HistoryOutcome>,
    status: ProjectStatsRepoStatusWire,
) -> String {
    let status = match status {
        ProjectStatsRepoStatusWire::Ok => "ok",
        ProjectStatsRepoStatusWire::Partial => "partial",
        ProjectStatsRepoStatusWire::Unavailable => "unavailable",
    };
    match (resolved, history) {
        (Some(resolved), Some(history)) => serde_json::json!({
            "identity": resolved.identity,
            "common_dir": resolved.common_dir,
            "ref": resolved.ref_name,
            "tip": resolved.tip,
            "shallow": resolved.shallow,
            "complete": history.complete,
            "failed": history.failed,
            "status": status,
        })
        .to_string(),
        _ => serde_json::json!({
            "path": path,
            "status": status,
        })
        .to_string(),
    }
}
