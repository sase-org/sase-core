//! Deduplicated Output, lifetime, coverage, and model-table aggregation.

use std::collections::{BTreeMap, BTreeSet};

use chrono::TimeZone;
use chrono_tz::Tz;

use super::attribution::{attribute_commit, Attribution};
use super::directory::Directory;
use super::history::HistoryRow;
use super::plans::{PlanAttribution, PlanMeasurement, PlanTier};
use super::wire::{
    ProjectStatsCoverageWire, ProjectStatsLifetimeWire,
    ProjectStatsModelKindWire, ProjectStatsModelWire, ProjectStatsOutputWire,
    ProjectStatsUnknownReasonCountWire,
};

#[derive(Default)]
struct PlanCounts {
    plans: u64,
    epic: u64,
    tale: u64,
}

const SECONDS_PER_DAY: i64 = 86_400;

/// Unique commits by SHA, keeping the first observed row for each id.
pub(crate) fn unique_by_sha<'a>(
    rows: impl IntoIterator<Item = &'a HistoryRow>,
) -> Vec<&'a HistoryRow> {
    let mut by_sha = BTreeMap::new();
    for row in rows {
        by_sha.entry(row.sha.as_str()).or_insert(row);
    }
    by_sha.into_values().collect()
}

/// Output commits use committer time in the half-open request range.
pub(crate) fn in_output_range(
    row: &HistoryRow,
    start_ts: i64,
    end_ts: i64,
) -> bool {
    start_ts <= row.committer_ts && row.committer_ts < end_ts
}

pub(crate) fn rows_in_output_range<'a>(
    rows: &[&'a HistoryRow],
    start_ts: i64,
    end_ts: i64,
) -> Vec<&'a HistoryRow> {
    rows.iter()
        .copied()
        .filter(|row| in_output_range(row, start_ts, end_ts))
        .collect()
}

pub(crate) fn attribute_rows(
    rows: &[&HistoryRow],
    directory: &Directory,
    aliases: &BTreeMap<String, String>,
) -> Vec<Attribution> {
    rows.iter()
        .map(|row| attribute_commit(row, directory, aliases))
        .collect()
}

/// Lifetime uses every available author timestamp, independent of Output.
pub(crate) fn lifetime(
    rows: &[&HistoryRow],
    now_ts: i64,
    timezone: &str,
) -> ProjectStatsLifetimeWire {
    if rows.is_empty() {
        return ProjectStatsLifetimeWire {
            commits: Some(0),
            agent_commits: Some(0),
            ..ProjectStatsLifetimeWire::default()
        };
    }
    let first = rows.iter().map(|row| row.author_ts).min();
    let last = rows.iter().map(|row| row.author_ts).max();
    let age_days = first.map(|first| {
        now_ts.saturating_sub(first).max(0) as u64 / SECONDS_PER_DAY as u64
    });
    let tz: Option<Tz> = timezone.parse().ok();
    let mut dates = BTreeSet::new();
    if let Some(tz) = tz {
        for row in rows {
            if let Some(date) = local_author_date(row.author_ts, tz) {
                dates.insert(date);
            }
        }
    }
    let agent_commits =
        rows.iter().filter(|row| row.agent_tag.is_some()).count() as u64;
    ProjectStatsLifetimeWire {
        first_commit_ts: first,
        last_commit_ts: last,
        age_days,
        active_days: Some(dates.len() as u64),
        commits: Some(rows.len() as u64),
        agent_commits: Some(agent_commits),
    }
}

/// Requested Output/lifetime with no usable source: present, unmeasured.
pub(crate) fn unavailable_output(
    commit_coverage: ProjectStatsCoverageWire,
) -> ProjectStatsOutputWire {
    ProjectStatsOutputWire {
        commits: None,
        agent_commits: None,
        agent_share: None,
        plans_committed: None,
        plans_epic: None,
        plans_tale: None,
        models: Vec::new(),
        commit_coverage,
        plan_coverage: ProjectStatsCoverageWire::default(),
    }
}

pub(crate) fn unavailable_lifetime() -> ProjectStatsLifetimeWire {
    ProjectStatsLifetimeWire::default()
}

fn local_author_date(author_ts: i64, tz: Tz) -> Option<chrono::NaiveDate> {
    let converted = tz.timestamp_opt(author_ts, 0);
    converted
        .single()
        .or_else(|| converted.latest())
        .map(|datetime| datetime.date_naive())
}

pub(crate) fn model_table(
    attributions: &[Attribution],
) -> Vec<ProjectStatsModelWire> {
    let mut resolved: BTreeMap<(String, String, bool), u64> = BTreeMap::new();
    let mut unknown_commits = 0;
    let mut no_agent_commits = 0;
    let mut unknown_reasons: BTreeMap<String, u64> = BTreeMap::new();
    for attribution in attributions {
        match attribution.kind {
            ProjectStatsModelKindWire::Resolved => {
                let provider = attribution.provider.clone().unwrap_or_default();
                let model = attribution.model.clone().unwrap_or_default();
                *resolved
                    .entry((provider, model, attribution.model_is_alias))
                    .or_default() += 1;
            }
            ProjectStatsModelKindWire::Unknown => {
                unknown_commits += 1;
                if let Some(reason) = &attribution.unknown_reason {
                    *unknown_reasons.entry(reason.clone()).or_default() += 1;
                }
            }
            ProjectStatsModelKindWire::NoAgent => no_agent_commits += 1,
        }
    }

    let mut resolved_rows: Vec<ProjectStatsModelWire> = resolved
        .into_iter()
        .map(|((provider, model, model_is_alias), commits)| {
            ProjectStatsModelWire {
                kind: ProjectStatsModelKindWire::Resolved,
                provider: Some(provider),
                model: Some(model),
                model_is_alias,
                commits,
                plans: None,
                plans_epic: None,
                plans_tale: None,
                unknown_reasons: Vec::new(),
            }
        })
        .collect();
    resolved_rows.sort_by(|left, right| {
        right
            .commits
            .cmp(&left.commits)
            .then(left.provider.cmp(&right.provider))
            .then(left.model.cmp(&right.model))
    });

    let mut rows = resolved_rows;
    rows.push(ProjectStatsModelWire {
        kind: ProjectStatsModelKindWire::Unknown,
        provider: None,
        model: None,
        model_is_alias: false,
        commits: unknown_commits,
        plans: None,
        plans_epic: None,
        plans_tale: None,
        unknown_reasons: unknown_reasons.keys().cloned().collect(),
    });
    rows.push(ProjectStatsModelWire {
        kind: ProjectStatsModelKindWire::NoAgent,
        provider: None,
        model: None,
        model_is_alias: false,
        commits: no_agent_commits,
        plans: None,
        plans_epic: None,
        plans_tale: None,
        unknown_reasons: Vec::new(),
    });
    rows
}

pub(crate) fn output_from_attributions(
    attributions: &[Attribution],
    commit_coverage: ProjectStatsCoverageWire,
) -> ProjectStatsOutputWire {
    let commits = attributions.len() as u64;
    let agent_commits = attributions
        .iter()
        .filter(|item| item.kind != ProjectStatsModelKindWire::NoAgent)
        .count() as u64;
    let agent_share = if commits == 0 {
        None
    } else {
        Some(agent_commits as f64 / commits as f64)
    };
    let output = ProjectStatsOutputWire {
        commits: Some(commits),
        agent_commits: Some(agent_commits),
        agent_share,
        plans_committed: None,
        plans_epic: None,
        plans_tale: None,
        models: model_table(attributions),
        commit_coverage,
        plan_coverage: ProjectStatsCoverageWire::default(),
    };
    debug_assert!(check_output_invariants(&output).is_ok());
    output
}

/// Overlay measured plan counts onto an Output document.
pub(crate) fn apply_plan_measurement(
    output: &mut ProjectStatsOutputWire,
    plans: &PlanMeasurement,
) {
    output.plans_committed = plans.counts.plans_committed;
    output.plans_epic = plans.counts.plans_epic;
    output.plans_tale = plans.counts.plans_tale;
    output.plan_coverage = plans.coverage.clone();
    if plans.measured {
        output.models =
            merge_model_plan_counts(&output.models, &plans.attributions);
    }
    debug_assert!(check_output_invariants(output).is_ok());
}

fn merge_model_plan_counts(
    commit_rows: &[ProjectStatsModelWire],
    attributions: &[PlanAttribution],
) -> Vec<ProjectStatsModelWire> {
    let mut resolved: BTreeMap<(String, String, bool), PlanCounts> =
        BTreeMap::new();
    let mut unknown = PlanCounts::default();
    let mut no_agent = PlanCounts::default();
    for item in attributions {
        let bucket = match item.attribution.kind {
            ProjectStatsModelKindWire::Resolved => {
                let provider =
                    item.attribution.provider.clone().unwrap_or_default();
                let model = item.attribution.model.clone().unwrap_or_default();
                resolved
                    .entry((provider, model, item.attribution.model_is_alias))
                    .or_default()
            }
            ProjectStatsModelKindWire::Unknown => &mut unknown,
            ProjectStatsModelKindWire::NoAgent => &mut no_agent,
        };
        bucket.plans += 1;
        match item.tier {
            PlanTier::Epic => bucket.epic += 1,
            PlanTier::Tale => bucket.tale += 1,
        }
    }

    let mut rows = Vec::new();
    let mut trailing = Vec::new();
    for row in commit_rows {
        let mut row = row.clone();
        match row.kind {
            ProjectStatsModelKindWire::Resolved => {
                let key = (
                    row.provider.clone().unwrap_or_default(),
                    row.model.clone().unwrap_or_default(),
                    row.model_is_alias,
                );
                let counts = resolved.remove(&key).unwrap_or_default();
                fill_plan_counts(&mut row, &counts);
                rows.push(row);
            }
            ProjectStatsModelKindWire::Unknown => {
                fill_plan_counts(&mut row, &unknown);
                trailing.push(row);
            }
            ProjectStatsModelKindWire::NoAgent => {
                fill_plan_counts(&mut row, &no_agent);
                trailing.push(row);
            }
        }
    }
    for ((provider, model, model_is_alias), counts) in resolved {
        rows.push(ProjectStatsModelWire {
            kind: ProjectStatsModelKindWire::Resolved,
            provider: Some(provider),
            model: Some(model),
            model_is_alias,
            commits: 0,
            plans: Some(counts.plans),
            plans_epic: Some(counts.epic),
            plans_tale: Some(counts.tale),
            unknown_reasons: Vec::new(),
        });
    }
    rows.sort_by(|left, right| {
        right
            .commits
            .cmp(&left.commits)
            .then(left.provider.cmp(&right.provider))
            .then(left.model.cmp(&right.model))
    });
    if !trailing
        .iter()
        .any(|row| row.kind == ProjectStatsModelKindWire::Unknown)
    {
        trailing.insert(
            0,
            ProjectStatsModelWire {
                kind: ProjectStatsModelKindWire::Unknown,
                provider: None,
                model: None,
                model_is_alias: false,
                commits: 0,
                plans: Some(unknown.plans),
                plans_epic: Some(unknown.epic),
                plans_tale: Some(unknown.tale),
                unknown_reasons: Vec::new(),
            },
        );
    }
    if !trailing
        .iter()
        .any(|row| row.kind == ProjectStatsModelKindWire::NoAgent)
    {
        trailing.push(ProjectStatsModelWire {
            kind: ProjectStatsModelKindWire::NoAgent,
            provider: None,
            model: None,
            model_is_alias: false,
            commits: 0,
            plans: Some(no_agent.plans),
            plans_epic: Some(no_agent.epic),
            plans_tale: Some(no_agent.tale),
            unknown_reasons: Vec::new(),
        });
    }
    rows.extend(trailing);
    rows
}

fn fill_plan_counts(row: &mut ProjectStatsModelWire, counts: &PlanCounts) {
    row.plans = Some(counts.plans);
    row.plans_epic = Some(counts.epic);
    row.plans_tale = Some(counts.tale);
}

pub(crate) fn attribution_coverage(
    configured_repos: u64,
    measured_repos: u64,
    partial_repos: u64,
    unavailable_repos: u64,
    attributions: &[Attribution],
) -> ProjectStatsCoverageWire {
    let mut unknown_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut agent_tagged = 0;
    let mut resolved = 0;
    let mut unknown = 0;
    for attribution in attributions {
        match attribution.kind {
            ProjectStatsModelKindWire::Resolved => {
                agent_tagged += 1;
                resolved += 1;
            }
            ProjectStatsModelKindWire::Unknown => {
                agent_tagged += 1;
                unknown += 1;
                if let Some(reason) = &attribution.unknown_reason {
                    *unknown_reasons.entry(reason.clone()).or_default() += 1;
                }
            }
            ProjectStatsModelKindWire::NoAgent => {}
        }
    }
    ProjectStatsCoverageWire {
        configured_repos,
        measured_repos,
        partial_repos,
        unavailable_repos,
        agent_tagged_commits: agent_tagged,
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

/// Always-on invariant checks used by tests; debug builds also assert them.
pub(crate) fn check_output_invariants(
    output: &ProjectStatsOutputWire,
) -> Result<(), String> {
    let Some(commits) = output.commits else {
        if output.agent_commits.is_some() {
            return Err(
                "unavailable output must not report agent_commits".to_owned()
            );
        }
        if output.plans_committed.is_none() && !output.models.is_empty() {
            return Err(
                "unavailable output must not emit a model distribution"
                    .to_owned(),
            );
        }
        return check_plan_invariants(output);
    };
    let agent_commits = output
        .agent_commits
        .ok_or_else(|| "measured output is missing agent_commits".to_owned())?;
    let model_sum: u64 = output.models.iter().map(|row| row.commits).sum();
    if model_sum != commits {
        return Err(format!(
            "model rows sum to {model_sum}, expected {commits}"
        ));
    }
    let mut provider_sum = 0;
    let mut resolved = 0;
    let mut unknown = 0;
    let mut no_agent = 0;
    for row in &output.models {
        match row.kind {
            ProjectStatsModelKindWire::Resolved => {
                if row.provider.as_deref().unwrap_or("").is_empty()
                    || row.model.as_deref().unwrap_or("").is_empty()
                {
                    return Err(
                        "resolved model row is missing provider or model"
                            .to_owned(),
                    );
                }
                provider_sum += row.commits;
                resolved += row.commits;
            }
            ProjectStatsModelKindWire::Unknown => unknown += row.commits,
            ProjectStatsModelKindWire::NoAgent => no_agent += row.commits,
        }
    }
    if provider_sum + unknown + no_agent != commits {
        return Err("provider subtotals do not sum to commits".to_owned());
    }
    if resolved + unknown != agent_commits {
        return Err(
            "resolved plus unknown agent counts must equal agent-tagged commits"
                .to_owned(),
        );
    }
    if agent_commits > commits {
        return Err("agent share numerator exceeds denominator".to_owned());
    }
    if let Some(share) = output.agent_share {
        if !(0.0..=1.0 + f64::EPSILON).contains(&share) {
            return Err("agent share is outside 0..=1".to_owned());
        }
    } else if commits > 0 {
        return Err(
            "agent share is null for a measured nonzero denominator".to_owned()
        );
    }
    check_plan_invariants(output)
}

fn check_plan_invariants(
    output: &ProjectStatsOutputWire,
) -> Result<(), String> {
    let Some(plans) = output.plans_committed else {
        return Ok(());
    };
    let epic = output.plans_epic.unwrap_or(0);
    let tale = output.plans_tale.unwrap_or(0);
    if epic + tale != plans {
        return Err(format!(
            "plan epic {epic} plus tale {tale} do not sum to {plans}"
        ));
    }
    let mut plan_sum = 0;
    let mut epic_sum = 0;
    let mut tale_sum = 0;
    for row in &output.models {
        let row_plans = row.plans.ok_or_else(|| {
            "measured plans require every model row to carry plan counts"
                .to_owned()
        })?;
        plan_sum += row_plans;
        epic_sum += row.plans_epic.unwrap_or(0);
        tale_sum += row.plans_tale.unwrap_or(0);
    }
    if plan_sum != plans {
        return Err(format!(
            "model plan rows sum to {plan_sum}, expected {plans}"
        ));
    }
    if epic_sum != epic || tale_sum != tale {
        return Err(
            "model epic/tale plan subtotals do not match output totals"
                .to_owned(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_stats::wire::ProjectStatsModelKindWire;

    fn attributed(
        kind: ProjectStatsModelKindWire,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Attribution {
        Attribution {
            kind,
            provider: provider.map(ToOwned::to_owned),
            model: model.map(ToOwned::to_owned),
            model_is_alias: false,
            unknown_reason: match kind {
                ProjectStatsModelKindWire::Unknown => {
                    Some("page_unpublished".to_owned())
                }
                _ => None,
            },
        }
    }

    #[test]
    fn model_table_always_includes_unknown_and_no_agent_zero_rows() {
        let rows = model_table(&[attributed(
            ProjectStatsModelKindWire::Resolved,
            Some("anthropic"),
            Some("opus"),
        )]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].kind, ProjectStatsModelKindWire::Resolved);
        assert_eq!(rows[0].commits, 1);
        assert_eq!(rows[1].kind, ProjectStatsModelKindWire::Unknown);
        assert_eq!(rows[1].commits, 0);
        assert_eq!(rows[2].kind, ProjectStatsModelKindWire::NoAgent);
        assert_eq!(rows[2].commits, 0);
        assert!(rows[0].plans.is_none());
    }

    #[test]
    fn resolved_rows_sort_by_count_then_provider_and_model() {
        let rows = model_table(&[
            attributed(
                ProjectStatsModelKindWire::Resolved,
                Some("xai"),
                Some("grok"),
            ),
            attributed(
                ProjectStatsModelKindWire::Resolved,
                Some("anthropic"),
                Some("opus"),
            ),
            attributed(
                ProjectStatsModelKindWire::Resolved,
                Some("anthropic"),
                Some("opus"),
            ),
        ]);
        assert_eq!(rows[0].model.as_deref(), Some("opus"));
        assert_eq!(rows[0].commits, 2);
        assert_eq!(rows[1].model.as_deref(), Some("grok"));
    }
}
