//! Output facet: range-scoped commits plus lifetime history.

use std::collections::BTreeSet;

use super::aggregate::{
    attribute_rows, attribution_coverage, lifetime, output_from_attributions,
    rows_in_output_range, unique_by_sha,
};
use super::git::ResolvedRepo;
use super::history::HistoryOutcome;
use super::wire::{
    ProjectStatsCoverageWire, ProjectStatsIssueWire, ProjectStatsLifetimeWire,
    ProjectStatsModelWire, ProjectStatsOutputWire, ProjectStatsRefWire,
    ProjectStatsRepoStatusWire, ProjectStatsRepoTierWire, ProjectStatsRepoWire,
};
use super::FacetContext;

pub(crate) struct OutputMeasurement {
    pub output: ProjectStatsOutputWire,
    pub lifetime: ProjectStatsLifetimeWire,
    pub models: Vec<ProjectStatsModelWire>,
    pub repos: Vec<ProjectStatsRepoWire>,
    pub coverage: ProjectStatsCoverageWire,
    pub issues: Vec<ProjectStatsIssueWire>,
    pub source_tokens: Vec<String>,
}

pub(crate) fn measure(context: &FacetContext<'_>) -> OutputMeasurement {
    let mut issues = Vec::new();
    let mut source_tokens = Vec::new();
    let mut repo_wires = Vec::new();
    let mut code_rows = Vec::new();
    let mut unique_code_keys = BTreeSet::new();
    let mut configured = 0;
    let mut measured = 0;
    let mut partial = 0;
    let mut unavailable = 0;

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
            let key = repo_identity_key(repo);
            if unique_code_keys.insert(key) {
                configured += 1;
                match wire.status {
                    ProjectStatsRepoStatusWire::Ok => measured += 1,
                    ProjectStatsRepoStatusWire::Partial => {
                        measured += 1;
                        partial += 1;
                    }
                    ProjectStatsRepoStatusWire::Unavailable => unavailable += 1,
                }
            }
            if let Some(history) = repo.history {
                code_rows.extend(history.rows.iter());
            }
        }
        repo_wires.push(wire);
    }

    let unique_code = unique_by_sha(code_rows);
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
        configured,
        measured,
        partial,
        unavailable,
        &attributions,
    );
    let output = output_from_attributions(&attributions, coverage.clone());
    let lifetime = lifetime(
        &unique_code,
        context.request.now_ts,
        &context.request.timezone,
    );
    OutputMeasurement {
        models: output.models.clone(),
        output,
        lifetime,
        repos: repo_wires,
        coverage,
        issues,
        source_tokens,
    }
}

fn measure_repo(
    context: &FacetContext<'_>,
    repo: &super::MeasuredRepo<'_>,
) -> (ProjectStatsRepoWire, String, Vec<ProjectStatsIssueWire>) {
    let status = repo_status(repo.resolved, repo.history, repo.issue);
    let mut extra_issues = Vec::new();
    if let Some(history) = repo.history {
        for issue in &history.issues {
            extra_issues
                .push(with_project(issue.clone(), &context.project.key));
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
        .map(|issue| with_project(issue, &context.project.key));

    let (output, lifetime) = match repo.history {
        Some(history) => {
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
        None => (None, None),
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

fn repo_status(
    resolved: Option<&ResolvedRepo>,
    history: Option<&HistoryOutcome>,
    issue: Option<&ProjectStatsIssueWire>,
) -> ProjectStatsRepoStatusWire {
    if issue.is_some() && history.is_none() {
        return ProjectStatsRepoStatusWire::Unavailable;
    }
    match (resolved, history) {
        (Some(resolved), Some(history))
            if history.complete && !resolved.shallow =>
        {
            ProjectStatsRepoStatusWire::Ok
        }
        (Some(_), Some(_)) => ProjectStatsRepoStatusWire::Partial,
        _ => ProjectStatsRepoStatusWire::Unavailable,
    }
}

fn repo_identity_key(repo: &super::MeasuredRepo<'_>) -> String {
    match repo.resolved {
        Some(resolved) => {
            format!("{}\n{}", resolved.identity, resolved.common_dir)
        }
        None => format!("path:{}", repo.spec.path),
    }
}

fn with_project(
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
            "tip": resolved.tip,
            "shallow": resolved.shallow,
            "complete": history.complete,
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

/// Runs-phase hook: `if_runs` filtering stays a no-op until run counts exist.
pub(crate) fn include_project(
    project: &super::wire::ProjectStatsProjectSpecWire,
) -> bool {
    match project.selection {
        super::wire::ProjectStatsSelectionWire::Always
        | super::wire::ProjectStatsSelectionWire::IfRuns => true,
    }
}
