//! XPrompt fold and finishing pass.
//!
//! `fold_xprompts` attributes used-xprompt references per run while
//! `finish_xprompts` ranks names and builds the optional focus view.

use std::collections::BTreeMap;

use crate::agent_scan::AgentArtifactRecordWire;

use super::super::wire::{
    AgentRunStatsRequestWire, AgentStatsRuntimeGroupByWire,
    AgentXPromptFocusWire, AgentXPromptStatsRowWire, AgentXPromptStatsWire,
};
use super::finishing::{
    normalized, ranked_counts, ratio, runtime_group_values,
};
use super::query::increment_bucket;
use super::types::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn fold_xprompts(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    launch_ts: f64,
    duration: Option<f64>,
    outcome: Option<&str>,
    attribution: &RunAttribution,
    request: &AgentRunStatsRequestWire,
    xprompts: &mut XPromptAccumulators,
) {
    let mut run_xprompts = BTreeMap::<String, RunXPrompt>::new();
    for used in &record.used_xprompts {
        let entry =
            run_xprompts.entry(used.name.clone()).or_insert_with(|| {
                RunXPrompt {
                    kind: used.kind.clone(),
                    tags: used.tags.clone(),
                    references: 0,
                }
            });
        entry.references += used.references;
    }
    if run_xprompts.is_empty() {
        xprompts.runs_without_xprompts += 1;
        return;
    }
    xprompts.runs_with_xprompts += 1;

    let agent = normalized(row.agent_name.as_deref());
    let model = normalized(row.model.as_deref());
    let project = row.project_name.clone();
    let partner_names = run_xprompts.keys().cloned().collect::<Vec<_>>();

    for (name, used) in &run_xprompts {
        xprompts.total_references += used.references;
        let value = xprompts.by_name.entry(name.clone()).or_insert_with(|| {
            XPromptAccumulator {
                kind: used.kind.clone(),
                tags: used.tags.clone(),
                ..XPromptAccumulator::default()
            }
        });
        let first_run = value.runs == 0;
        value.runs += 1;
        value.references += used.references;
        value.agents.insert(agent.clone());
        match outcome {
            Some("completed") => value.completed += 1,
            Some("failed" | "epic_launch_failed") => value.failed += 1,
            _ => {}
        }
        if let Some(duration) = duration {
            value.duration_count += 1;
            value.total_runtime_seconds += duration;
        }
        if first_run || launch_ts < value.first_run_ts {
            value.first_run_ts = launch_ts;
        }
        if first_run || launch_ts > value.last_run_ts {
            value.last_run_ts = launch_ts;
        }
        *value.models.entry(model.clone()).or_default() += 1;
        *value.projects.entry(project.clone()).or_default() += 1;
        for partner in partner_names.iter().filter(|partner| *partner != name) {
            *value.partners.entry(partner.clone()).or_default() += 1;
        }

        if request.xprompt_focus.as_deref() == Some(name.as_str()) {
            let focus = xprompts
                .focus
                .as_mut()
                .expect("focus accumulator exists for a focused request");
            *focus
                .providers
                .entry(normalized(row.provider.as_deref()))
                .or_default() += 1;
            for tribe in runtime_group_values(
                AgentStatsRuntimeGroupByWire::Tribe,
                record,
                row,
                attribution,
            ) {
                *focus.tribes.entry(tribe).or_default() += 1;
            }
            increment_bucket(&mut focus.buckets, request, launch_ts);
        }
    }
}

pub(super) fn finish_xprompts(
    mut xprompts: XPromptAccumulators,
    request: &AgentRunStatsRequestWire,
) -> AgentXPromptStatsWire {
    let distinct_xprompts = xprompts.by_name.len() as u64;
    let focus = request.xprompt_focus.as_deref().map(|name| {
        let extra = xprompts
            .focus
            .take()
            .expect("focus accumulator exists for a focused request");
        if let Some(value) = xprompts.by_name.get(name) {
            AgentXPromptFocusWire {
                name: name.to_string(),
                found: true,
                kind: value.kind.clone(),
                tags: value.tags.clone(),
                runs: value.runs,
                references: value.references,
                distinct_agents: value.agents.len() as u64,
                completed: value.completed,
                failed: value.failed,
                success_rate: ratio(value.completed, value.runs),
                total_runtime_seconds: value.total_runtime_seconds,
                mean_runtime_seconds: (value.duration_count > 0).then_some(
                    value.total_runtime_seconds / value.duration_count as f64,
                ),
                first_run_ts: value.first_run_ts,
                last_run_ts: value.last_run_ts,
                models: ranked_counts(value.models.clone(), None),
                providers: ranked_counts(extra.providers, None),
                projects: ranked_counts(value.projects.clone(), None),
                partners: ranked_counts(value.partners.clone(), None),
                tribes: ranked_counts(extra.tribes, None),
                buckets: extra.buckets,
            }
        } else {
            AgentXPromptFocusWire {
                name: name.to_string(),
                found: false,
                kind: UNKNOWN.to_string(),
                buckets: extra.buckets,
                ..AgentXPromptFocusWire::default()
            }
        }
    });

    let breakdown_top_n = request.xprompt_breakdown_top_n as usize;
    let mut rows = xprompts
        .by_name
        .into_iter()
        .map(|(name, value)| {
            let models_total = value.models.len() as u64;
            let projects_total = value.projects.len() as u64;
            let partners_total = value.partners.len() as u64;
            let models = ranked_counts(value.models, Some(breakdown_top_n));
            let projects = ranked_counts(value.projects, Some(breakdown_top_n));
            let partners = ranked_counts(value.partners, Some(breakdown_top_n));
            AgentXPromptStatsRowWire {
                name,
                kind: value.kind,
                tags: value.tags,
                runs: value.runs,
                references: value.references,
                distinct_agents: value.agents.len() as u64,
                completed: value.completed,
                failed: value.failed,
                success_rate: ratio(value.completed, value.runs),
                total_runtime_seconds: value.total_runtime_seconds,
                mean_runtime_seconds: (value.duration_count > 0).then_some(
                    value.total_runtime_seconds / value.duration_count as f64,
                ),
                first_run_ts: value.first_run_ts,
                last_run_ts: value.last_run_ts,
                models_truncated: models_total
                    .saturating_sub(models.len() as u64),
                projects_truncated: projects_total
                    .saturating_sub(projects.len() as u64),
                partners_truncated: partners_total
                    .saturating_sub(partners.len() as u64),
                models,
                projects,
                partners,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.name.cmp(&right.name))
    });
    rows.truncate(request.xprompt_top_n as usize);

    AgentXPromptStatsWire {
        runs_with_xprompts: xprompts.runs_with_xprompts,
        runs_without_xprompts: xprompts.runs_without_xprompts,
        distinct_xprompts,
        total_references: xprompts.total_references,
        truncated_rows: distinct_xprompts.saturating_sub(rows.len() as u64),
        rows,
        focus,
    }
}
