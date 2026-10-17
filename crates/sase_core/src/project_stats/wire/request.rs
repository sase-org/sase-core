use std::collections::{BTreeMap, BTreeSet};

use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use super::ProjectStatsError;

pub const PROJECT_STATS_WIRE_SCHEMA_VERSION: u32 = 1;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 32 * 1024 * 1024;

fn default_facets() -> Vec<ProjectStatsFacetWire> {
    vec![ProjectStatsFacetWire::Output]
}

/// A source facet requested from the project statistics engine.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsFacetWire {
    #[default]
    Output,
    Code,
    Runs,
}

/// Per-command limits used by Git and other bounded source readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsBudgetWire {
    /// Whole-command deadline in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum captured stdout bytes. Readers continue draining after this cap.
    #[serde(default = "default_max_output_bytes")]
    pub max_output_bytes: u64,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_max_output_bytes() -> u64 {
    DEFAULT_MAX_OUTPUT_BYTES
}

impl Default for ProjectStatsBudgetWire {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

/// Controls project metadata included in one query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsRequestWire {
    pub schema_version: u32,
    /// Inclusive Unix timestamp for Output measurements.
    pub start_ts: i64,
    /// Exclusive Unix timestamp for Output measurements.
    pub end_ts: i64,
    /// Fixed query clock used by lifetime measures and fingerprints.
    pub now_ts: i64,
    /// IANA timezone used to bucket local author dates.
    pub timezone: String,
    #[serde(default = "default_facets")]
    pub facets: Vec<ProjectStatsFacetWire>,
    /// Empty disables persistent history caching.
    #[serde(default)]
    pub cache_dir: String,
    #[serde(default)]
    pub run_index_path: Option<String>,
    #[serde(default)]
    pub agents_sidecars: Vec<ProjectStatsAgentSidecarSpecWire>,
    #[serde(default)]
    pub local_agent_name_prefix: String,
    /// Alias to canonical provider/model identity mappings.
    #[serde(default)]
    pub known_model_aliases: BTreeMap<String, String>,
    #[serde(default)]
    pub record_repo_names: Vec<String>,
    #[serde(default)]
    pub include_run_only_projects: bool,
    #[serde(default)]
    pub budget: ProjectStatsBudgetWire,
    #[serde(default)]
    pub projects: Vec<ProjectStatsProjectSpecWire>,
}

/// A configured agent-artifact sidecar that may contribute run metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsAgentSidecarSpecWire {
    pub project_key: String,
    pub path: String,
}

/// Whether a project participates unconditionally or only when it has runs.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsSelectionWire {
    #[default]
    Always,
    IfRuns,
}

/// Configuration state captured by the request.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsProjectStateWire {
    #[default]
    Enabled,
    Disabled,
    Unregistered,
}

/// A project and its code, record, and plan-store source configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsProjectSpecWire {
    pub key: String,
    pub label: String,
    pub state: ProjectStatsProjectStateWire,
    #[serde(default)]
    pub selection: ProjectStatsSelectionWire,
    #[serde(default)]
    pub index_project_names: Vec<String>,
    #[serde(default)]
    pub code_repos: Vec<ProjectStatsRepoSpecWire>,
    #[serde(default)]
    pub record_repos: Vec<ProjectStatsRepoSpecWire>,
    #[serde(default)]
    pub not_counted: Vec<ProjectStatsNotCountedWire>,
    pub plan_store: ProjectStatsPlanStoreWire,
}

/// Repository role used when resolving and counting a repository.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsRepoKindWire {
    #[default]
    Primary,
    Linked,
    Sidecar,
}

/// One repository candidate in a project specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsRepoSpecWire {
    pub id: String,
    pub name: String,
    pub kind: ProjectStatsRepoKindWire,
    pub path: String,
    /// Canonical remote identity when known; empty requests deterministic fallback.
    #[serde(default)]
    pub identity: String,
    /// Ordered names to verify as commits. No implicit HEAD fallback is allowed.
    #[serde(default)]
    pub ref_candidates: Vec<String>,
}

/// Why a configured record repository is excluded from code totals.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsNotCountedReasonWire {
    #[default]
    UserLinked,
    External,
}

/// A repository excluded from project code counts with its explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsNotCountedWire {
    pub name: String,
    pub reason: ProjectStatsNotCountedReasonWire,
}

/// Storage layout for project plan records.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsPlanStorageWire {
    #[default]
    InTree,
    Local,
    SeparateRepo,
    SidecarRepos,
}

/// Plan source descriptor retained for the plan-counting facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsPlanStoreWire {
    pub storage: ProjectStatsPlanStorageWire,
    #[serde(default)]
    pub repo_path: Option<String>,
    pub plans_dir: String,
    #[serde(default)]
    pub project_filter: Option<String>,
}

/// Validate request versions, ranges, bounds, timezone, and source identities.
pub fn validate_project_stats_request(
    request: &ProjectStatsRequestWire,
) -> Result<(), ProjectStatsError> {
    if request.schema_version != PROJECT_STATS_WIRE_SCHEMA_VERSION {
        return Err(ProjectStatsError::UnsupportedSchema {
            actual: request.schema_version,
            expected: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        });
    }
    if request.end_ts < request.start_ts {
        return Err(ProjectStatsError::ReversedRange);
    }
    if request.timezone.parse::<Tz>().is_err() {
        return Err(ProjectStatsError::InvalidTimezone(
            request.timezone.clone(),
        ));
    }
    if request.budget.timeout_ms == 0 || request.budget.max_output_bytes == 0 {
        return Err(ProjectStatsError::InvalidBudget);
    }

    let mut facets = BTreeSet::new();
    for facet in &request.facets {
        if !facets.insert(*facet as u8) {
            return Err(ProjectStatsError::DuplicateFacet(
                facet.as_str().to_owned(),
            ));
        }
    }

    if request.run_index_path.as_deref().is_some_and(is_blank) {
        return Err(ProjectStatsError::InvalidIdentity(
            "run_index_path must be nonempty when supplied".to_owned(),
        ));
    }
    for sidecar in &request.agents_sidecars {
        require_nonempty(&sidecar.project_key, "agent sidecar project_key")?;
        require_nonempty(&sidecar.path, "agent sidecar path")?;
    }
    for (alias, target) in &request.known_model_aliases {
        require_nonempty(alias, "model alias")?;
        require_nonempty(target, "model alias target")?;
    }
    for name in &request.record_repo_names {
        require_nonempty(name, "record repository name")?;
    }

    let mut project_keys = BTreeSet::new();
    for project in &request.projects {
        require_nonempty(&project.key, "project key")?;
        require_nonempty(&project.label, "project label")?;
        if !project_keys.insert(project.key.as_str()) {
            return Err(ProjectStatsError::InvalidIdentity(format!(
                "duplicate project key `{}`",
                project.key
            )));
        }

        let mut repo_ids = BTreeSet::new();
        for repo in project.code_repos.iter().chain(&project.record_repos) {
            require_nonempty(&repo.id, "repository id")?;
            require_nonempty(&repo.name, "repository name")?;
            require_nonempty(&repo.path, "repository path")?;
            if !repo_ids.insert(repo.id.as_str()) {
                return Err(ProjectStatsError::InvalidIdentity(format!(
                    "duplicate repository id `{}` in project `{}`",
                    repo.id, project.key
                )));
            }
        }
        for excluded in &project.not_counted {
            require_nonempty(&excluded.name, "not-counted repository name")?;
        }
        require_nonempty(
            &project.plan_store.plans_dir,
            "plan store plans_dir",
        )?;
    }
    Ok(())
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}

fn require_nonempty(value: &str, field: &str) -> Result<(), ProjectStatsError> {
    if is_blank(value) {
        Err(ProjectStatsError::InvalidIdentity(format!(
            "{field} must be nonempty"
        )))
    } else {
        Ok(())
    }
}

impl ProjectStatsFacetWire {
    fn as_str(self) -> &'static str {
        match self {
            Self::Output => "output",
            Self::Code => "code",
            Self::Runs => "runs",
        }
    }
}
