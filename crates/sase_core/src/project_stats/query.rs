use sha2::{Digest, Sha256};

use super::directory;
use super::history;
use super::wire::{
    validate_project_stats_request, ProjectStatsCoverageWire,
    ProjectStatsError, ProjectStatsPortfolioWire, ProjectStatsProjectWire,
    ProjectStatsRangeWire, ProjectStatsRequestWire, ProjectStatsResponseWire,
    PROJECT_STATS_CLASSIFIER_VERSION, PROJECT_STATS_ENGINE_VERSION,
    PROJECT_STATS_METRIC_POLICY_VERSION, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use super::{code, output, plans, runs, FacetContext};

/// Validate and scaffold a project statistics response.
///
/// This phase intentionally leaves every facet measurement `null`. Later phases
/// fill the seams once Git history and their sources have been implemented.
pub fn query_project_stats(
    request: &ProjectStatsRequestWire,
) -> Result<ProjectStatsResponseWire, ProjectStatsError> {
    validate_project_stats_request(request)?;

    let directory = directory::load_directory();
    let no_history = history::empty_history();
    let histories = [no_history];
    let mut projects = Vec::with_capacity(request.projects.len());

    for project in &request.projects {
        let context = FacetContext {
            request,
            project,
            directory: &directory,
            histories: &histories,
        };
        let output_result = output::query(&context);
        let code_result = code::query(&context);
        let runs_result = runs::query(&context);
        let _plans = plans::query(&context);

        let coverage = ProjectStatsCoverageWire {
            configured_repos: project.code_repos.len() as u64,
            ..ProjectStatsCoverageWire::default()
        };
        projects.push(ProjectStatsProjectWire {
            key: project.key.clone(),
            label: project.label.clone(),
            state: project.state,
            output: output_result.value,
            lifetime: None,
            code: code_result.value,
            runs: runs_result.value,
            repos: Vec::new(),
            not_counted: project.not_counted.clone(),
            models: Vec::new(),
            patches: Vec::new(),
            truncated_patch_rows: 0,
            coverage,
        });
    }

    let fingerprint = request_fingerprint(request)?;
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
        portfolio: ProjectStatsPortfolioWire::default(),
        issues: Vec::new(),
    })
}

fn request_fingerprint(
    request: &ProjectStatsRequestWire,
) -> Result<String, ProjectStatsError> {
    let mut semantic_request = request.clone();
    semantic_request.cache_dir.clear();
    let request_value =
        serde_json::to_value(&semantic_request).map_err(|error| {
            ProjectStatsError::FingerprintSerialization(error.to_string())
        })?;
    let payload = serde_json::to_vec(&(
        request_value,
        PROJECT_STATS_ENGINE_VERSION,
        PROJECT_STATS_METRIC_POLICY_VERSION,
        PROJECT_STATS_CLASSIFIER_VERSION,
    ))
    .map_err(|error| {
        ProjectStatsError::FingerprintSerialization(error.to_string())
    })?;
    Ok(hex::encode(Sha256::digest(payload)))
}
