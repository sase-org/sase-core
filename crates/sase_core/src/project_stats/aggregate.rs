//! Deduplicated Output, lifetime, coverage, and model-table aggregation.

use std::collections::{BTreeMap, BTreeSet};

use chrono::TimeZone;
use chrono_tz::Tz;

use super::attribution::{attribute_commit, Attribution};
use super::directory::Directory;
use super::history::HistoryRow;
use super::wire::{
    ProjectStatsCoverageWire, ProjectStatsLifetimeWire,
    ProjectStatsModelKindWire, ProjectStatsModelWire, ProjectStatsOutputWire,
    ProjectStatsUnknownReasonCountWire,
};

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
        return ProjectStatsLifetimeWire::default();
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
        commits: rows.len() as u64,
        agent_commits,
    }
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
        commits,
        agent_commits,
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
    let model_sum: u64 = output.models.iter().map(|row| row.commits).sum();
    if model_sum != output.commits {
        return Err(format!(
            "model rows sum to {model_sum}, expected {}",
            output.commits
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
    if provider_sum + unknown + no_agent != output.commits {
        return Err("provider subtotals do not sum to commits".to_owned());
    }
    if resolved + unknown != output.agent_commits {
        return Err(
            "resolved plus unknown agent counts must equal agent-tagged commits"
                .to_owned(),
        );
    }
    if output.agent_commits > output.commits {
        return Err("agent share numerator exceeds denominator".to_owned());
    }
    if let Some(share) = output.agent_share {
        if !(0.0..=1.0 + f64::EPSILON).contains(&share) {
            return Err("agent share is outside 0..=1".to_owned());
        }
    } else if output.commits > 0 {
        return Err(
            "agent share is null for a measured nonzero denominator".to_owned()
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
