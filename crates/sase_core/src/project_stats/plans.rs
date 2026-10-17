//! Committed-plan counting: rename-aware first adds credited at the tip.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::file_history::wire::FileChangeKindWire;
use crate::file_history::{
    build_index, safe_pathspec, FileHistoryBudgetWire, FileHistoryIndexWire,
    FileLineageWire,
};
use crate::plan::read::{parse_frontmatter_map, split_frontmatter};
use crate::project_stats::attribution::{attribute_plan, Attribution};
use crate::project_stats::cache;
use crate::project_stats::git::{self, ResolvedRepo};
use crate::project_stats::wire::{
    ProjectStatsCoverageWire, ProjectStatsIssueWire,
    ProjectStatsPlanCountsWire, ProjectStatsPlanStorageWire,
    ProjectStatsPlanStoreWire, ProjectStatsRepoKindWire,
    ProjectStatsRepoSpecWire, ProjectStatsUnknownReasonCountWire,
};

use super::FacetContext;

const PLANS_CACHE_SCHEMA_VERSION: u32 = 1;
const MAX_PLAN_COMMITS: usize = 500_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlanTier {
    Epic,
    Tale,
}

#[derive(Debug, Clone)]
pub(crate) struct PlanAttribution {
    pub attribution: Attribution,
    pub tier: PlanTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedPlanRecord {
    first_add_ts: i64,
    path: String,
    tier: String,
    proposed_with: Option<String>,
    proposed_by: Option<String>,
    project: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlansCacheEntry {
    schema_version: u32,
    key: String,
    tip: String,
    records: Vec<CachedPlanRecord>,
}

/// Plan query result carrying both counts and per-plan attributions.
#[derive(Debug, Clone)]
pub(crate) struct PlanMeasurement {
    pub counts: ProjectStatsPlanCountsWire,
    pub attributions: Vec<PlanAttribution>,
    pub coverage: ProjectStatsCoverageWire,
    pub issues: Vec<ProjectStatsIssueWire>,
    pub source_tokens: Vec<String>,
    pub measured: bool,
}

/// Count committed tale/epic plans for one project and attribute them.
pub(crate) fn measure(context: &FacetContext<'_>) -> PlanMeasurement {
    let store = &context.project.plan_store;
    if store.storage == ProjectStatsPlanStorageWire::Local {
        return PlanMeasurement {
            counts: ProjectStatsPlanCountsWire::default(),
            attributions: Vec::new(),
            coverage: ProjectStatsCoverageWire::default(),
            issues: Vec::new(),
            source_tokens: vec![format!("plans:{}:local", context.project.key)],
            measured: false,
        };
    }
    let Some(spec) = store_spec(store, context) else {
        return unavailable(
            context,
            "plan_store_unresolved",
            "plan store repository path is missing",
        );
    };
    let resolved = match git::resolve_repo(&spec, &context.request.budget) {
        Ok(resolved) => resolved,
        Err(issue) => {
            let mut issue = issue;
            issue.project = Some(context.project.key.clone());
            return PlanMeasurement {
                counts: ProjectStatsPlanCountsWire::default(),
                attributions: Vec::new(),
                coverage: ProjectStatsCoverageWire {
                    configured_repos: 1,
                    unavailable_repos: 1,
                    ..ProjectStatsCoverageWire::default()
                },
                issues: vec![issue],
                source_tokens: vec![format!(
                    "plans:{}:unavailable",
                    context.project.key
                )],
                measured: false,
            };
        }
    };
    if resolved.tip.is_empty() {
        return empty_measured(context, &resolved);
    }
    let plans_dir = store.plans_dir.trim().trim_end_matches('/');
    if !safe_pathspec(plans_dir) {
        return unavailable(
            context,
            "invalid_plans_dir",
            "plan store plans_dir is not a safe git pathspec",
        );
    }
    let (records, mut issues, truncated) =
        load_records(context, store, &resolved, plans_dir);
    if truncated {
        issues.push(ProjectStatsIssueWire {
            project: Some(context.project.key.clone()),
            repo: Some(spec.name.clone()),
            code: "plans_truncated".to_owned(),
            message: "plan-store history walk exceeded its budget".to_owned(),
            hint: None,
        });
    }
    let in_range: Vec<&CachedPlanRecord> = records
        .iter()
        .filter(|record| {
            context.request.start_ts <= record.first_add_ts
                && record.first_add_ts < context.request.end_ts
        })
        .filter(|record| project_matches(record, store))
        .collect();
    let mut attributions = Vec::with_capacity(in_range.len());
    let mut epic = 0;
    let mut tale = 0;
    for record in in_range {
        let tier = match record.tier.as_str() {
            "epic" => {
                epic += 1;
                PlanTier::Epic
            }
            _ => {
                tale += 1;
                PlanTier::Tale
            }
        };
        attributions.push(PlanAttribution {
            attribution: attribute_plan(
                record.proposed_with.as_deref(),
                record.proposed_by.as_deref(),
                context.directory,
                &context.request.known_model_aliases,
            ),
            tier,
        });
    }
    let counts = ProjectStatsPlanCountsWire {
        plans_committed: Some(attributions.len() as u64),
        plans_epic: Some(epic),
        plans_tale: Some(tale),
    };
    let coverage = plan_coverage(&attributions);
    PlanMeasurement {
        counts,
        attributions,
        coverage,
        issues,
        source_tokens: vec![format!(
            "plans:{}:{}:{}",
            context.project.key, resolved.identity, resolved.tip
        )],
        measured: true,
    }
}

/// Sum measured plan documents across selected projects for portfolio totals.
pub(crate) fn combine(parts: &[PlanMeasurement]) -> Option<PlanMeasurement> {
    if parts.is_empty() {
        return None;
    }
    let mut attributions = Vec::new();
    let mut issues = Vec::new();
    let mut source_tokens = Vec::new();
    let mut committed = 0;
    let mut epic = 0;
    let mut tale = 0;
    for part in parts {
        committed += part.counts.plans_committed.unwrap_or(0);
        epic += part.counts.plans_epic.unwrap_or(0);
        tale += part.counts.plans_tale.unwrap_or(0);
        attributions.extend(part.attributions.iter().cloned());
        issues.extend(part.issues.iter().cloned());
        source_tokens.extend(part.source_tokens.iter().cloned());
    }
    Some(PlanMeasurement {
        counts: ProjectStatsPlanCountsWire {
            plans_committed: Some(committed),
            plans_epic: Some(epic),
            plans_tale: Some(tale),
        },
        coverage: plan_coverage(&attributions),
        attributions,
        issues,
        source_tokens,
        measured: true,
    })
}

fn load_records(
    context: &FacetContext<'_>,
    store: &ProjectStatsPlanStoreWire,
    resolved: &ResolvedRepo,
    plans_dir: &str,
) -> (Vec<CachedPlanRecord>, Vec<ProjectStatsIssueWire>, bool) {
    let key = plans_cache_key(store, resolved, plans_dir);
    let cache_path = (!context.request.cache_dir.is_empty())
        .then(|| cache::kind_path(&context.request.cache_dir, "plans", &key));
    if let Some(path) = cache_path.as_deref() {
        if let Some(entry) = cache::load_json::<PlansCacheEntry>(path) {
            if entry.schema_version == PLANS_CACHE_SCHEMA_VERSION
                && entry.key == key
                && entry.tip == resolved.tip
            {
                return (entry.records, Vec::new(), false);
            }
        }
    }
    let budget = FileHistoryBudgetWire {
        timeout_ms: context.request.budget.timeout_ms,
        max_output_bytes: context.request.budget.max_output_bytes,
        max_commits: MAX_PLAN_COMMITS,
    };
    let index = match build_index(
        Path::new(&resolved.canonical_path),
        &[plans_dir.to_owned()],
        &budget,
    ) {
        Ok(index) => index,
        Err(error) => {
            return (
                Vec::new(),
                vec![ProjectStatsIssueWire {
                    project: Some(context.project.key.clone()),
                    repo: Some(resolved.canonical_path.clone()),
                    code: "plans_unavailable".to_owned(),
                    message: error.to_string(),
                    hint: None,
                }],
                false,
            );
        }
    };
    let truncated = index.health.truncated;
    let records = records_from_index(
        Path::new(&resolved.canonical_path),
        &index,
        plans_dir,
        &context.request.budget,
    );
    if let Some(path) = cache_path.as_deref() {
        if !truncated {
            cache::persist_json(
                path,
                &PlansCacheEntry {
                    schema_version: PLANS_CACHE_SCHEMA_VERSION,
                    key,
                    tip: resolved.tip.clone(),
                    records: records.clone(),
                },
            );
        }
    }
    (records, Vec::new(), truncated)
}

fn records_from_index(
    repo: &Path,
    index: &FileHistoryIndexWire,
    plans_dir: &str,
    budget: &crate::project_stats::wire::ProjectStatsBudgetWire,
) -> Vec<CachedPlanRecord> {
    let mut wanted: Vec<(&FileLineageWire, String, i64)> = Vec::new();
    for lineage in &index.lineages {
        if !is_plan_path(&lineage.current_path, plans_dir)
            && lineage
                .paths
                .iter()
                .all(|path| !is_plan_path(path, plans_dir))
        {
            continue;
        }
        let Some(oldest) = lineage.versions.last() else {
            continue;
        };
        if oldest.kind != FileChangeKindWire::Created {
            continue;
        }
        let Some(oid) = lineage
            .versions
            .first()
            .and_then(|version| version.blob_oid.clone())
        else {
            continue;
        };
        wanted.push((lineage, oid, oldest.committer_time));
    }
    let oids: Vec<String> =
        wanted.iter().map(|(_, oid, _)| oid.clone()).collect();
    let blobs = git::read_blobs(repo, &oids, budget).unwrap_or_default();
    let mut records = Vec::new();
    for ((lineage, _, first_add_ts), blob) in wanted.into_iter().zip(blobs) {
        let Some(bytes) = blob else {
            continue;
        };
        let Ok(text) = String::from_utf8(bytes) else {
            continue;
        };
        let Some(record) =
            record_from_blob(lineage, first_add_ts, &text, plans_dir)
        else {
            continue;
        };
        records.push(record);
    }
    records
}

fn record_from_blob(
    lineage: &FileLineageWire,
    first_add_ts: i64,
    content: &str,
    plans_dir: &str,
) -> Option<CachedPlanRecord> {
    if !is_plan_path(&lineage.current_path, plans_dir) {
        return None;
    }
    let (frontmatter, _) = split_frontmatter(content);
    let map = frontmatter
        .as_deref()
        .map(parse_frontmatter_map)
        .unwrap_or_default();
    let tier = map.get("tier").map(|value| value.trim().to_lowercase())?;
    if tier != "tale" && tier != "epic" {
        return None;
    }
    Some(CachedPlanRecord {
        first_add_ts,
        path: lineage.current_path.clone(),
        tier,
        proposed_with: nonempty(map.get("proposed_with")),
        proposed_by: nonempty(map.get("proposed_by")),
        project: nonempty(map.get("project")),
    })
}

fn is_plan_path(path: &str, plans_dir: &str) -> bool {
    let dir = plans_dir.trim_end_matches('/');
    let under = path == dir || path.starts_with(&format!("{dir}/"));
    under && path.ends_with(".md") && !path.contains(".plan-decisions.")
}

fn nonempty(value: Option<&String>) -> Option<String> {
    value
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
}

fn project_matches(
    record: &CachedPlanRecord,
    store: &ProjectStatsPlanStoreWire,
) -> bool {
    match store
        .project_filter
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        None => true,
        Some(wanted) => record
            .project
            .as_deref()
            .is_some_and(|project| project == wanted),
    }
}

fn store_spec(
    store: &ProjectStatsPlanStoreWire,
    context: &FacetContext<'_>,
) -> Option<ProjectStatsRepoSpecWire> {
    if let Some(path) = store
        .repo_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        return Some(ProjectStatsRepoSpecWire {
            id: format!("{}-plans", context.project.key),
            name: "plans".to_owned(),
            kind: ProjectStatsRepoKindWire::Sidecar,
            path: path.to_owned(),
            identity: String::new(),
            ref_candidates: vec!["HEAD".to_owned()],
        });
    }
    context
        .project
        .code_repos
        .iter()
        .find(|repo| repo.kind == ProjectStatsRepoKindWire::Primary)
        .or_else(|| context.project.code_repos.first())
        .cloned()
}

fn plans_cache_key(
    store: &ProjectStatsPlanStoreWire,
    resolved: &ResolvedRepo,
    plans_dir: &str,
) -> String {
    let canonical = serde_json::json!({
        "kind": "plans",
        "storage": store.storage,
        "identity": resolved.identity,
        "common_dir": resolved.common_dir,
        "plans_dir": plans_dir,
        "project_filter": store.project_filter,
        "schema": PLANS_CACHE_SCHEMA_VERSION,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

fn unavailable(
    context: &FacetContext<'_>,
    code: &str,
    message: &str,
) -> PlanMeasurement {
    PlanMeasurement {
        counts: ProjectStatsPlanCountsWire::default(),
        attributions: Vec::new(),
        coverage: ProjectStatsCoverageWire {
            configured_repos: 1,
            unavailable_repos: 1,
            ..ProjectStatsCoverageWire::default()
        },
        issues: vec![ProjectStatsIssueWire {
            project: Some(context.project.key.clone()),
            code: code.to_owned(),
            message: message.to_owned(),
            ..ProjectStatsIssueWire::default()
        }],
        source_tokens: vec![format!(
            "plans:{}:unavailable",
            context.project.key
        )],
        measured: false,
    }
}

fn empty_measured(
    context: &FacetContext<'_>,
    resolved: &ResolvedRepo,
) -> PlanMeasurement {
    PlanMeasurement {
        counts: ProjectStatsPlanCountsWire {
            plans_committed: Some(0),
            plans_epic: Some(0),
            plans_tale: Some(0),
        },
        attributions: Vec::new(),
        coverage: ProjectStatsCoverageWire {
            configured_repos: 1,
            measured_repos: 1,
            ..ProjectStatsCoverageWire::default()
        },
        issues: Vec::new(),
        source_tokens: vec![format!(
            "plans:{}:{}:empty",
            context.project.key, resolved.identity
        )],
        measured: true,
    }
}

fn plan_coverage(attributions: &[PlanAttribution]) -> ProjectStatsCoverageWire {
    let mut unknown_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut resolved = 0;
    let mut unknown = 0;
    let mut tagged = 0;
    for item in attributions {
        match item.attribution.kind {
            crate::project_stats::wire::ProjectStatsModelKindWire::Resolved => {
                tagged += 1;
                resolved += 1;
            }
            crate::project_stats::wire::ProjectStatsModelKindWire::Unknown => {
                tagged += 1;
                unknown += 1;
                if let Some(reason) = &item.attribution.unknown_reason {
                    *unknown_reasons.entry(reason.clone()).or_default() += 1;
                }
            }
            crate::project_stats::wire::ProjectStatsModelKindWire::NoAgent => {}
        }
    }
    ProjectStatsCoverageWire {
        configured_repos: 1,
        measured_repos: 1,
        partial_repos: 0,
        unavailable_repos: 0,
        agent_tagged_commits: tagged,
        resolved_agent_commits: resolved,
        unknown_agent_commits: unknown,
        unknown_reasons: unknown_reasons
            .into_iter()
            .map(|(reason, count)| ProjectStatsUnknownReasonCountWire {
                reason,
                count,
            })
            .collect(),
    }
}
