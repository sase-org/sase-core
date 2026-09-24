//! Finishing and ranking passes.
//!
//! Provider keys, runtime-group values, ranked counts, and the
//! per-dimension finishers that turn accumulators into wire rows,
//! plus the small math and naming helpers they share.

use std::collections::BTreeMap;

use crate::agent_scan::AgentArtifactRecordWire;
use crate::effort::{is_valid_effort, EFFORT_LEVELS_ORDERED};

use super::super::wire::{
    AgentProviderStatsWire, AgentRuntimeGroupStatsWire, AgentStatsCountWire,
    AgentStatsRuntimeGroupByWire, AgentWorkspaceStatsWire,
};
use super::types::*;

pub(super) fn provider_key(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> ProviderKey {
    let meta = record.agent_meta.as_ref();
    let effort = meta
        .and_then(|value| value.reasoning_effort.as_deref())
        .filter(|value| is_valid_effort(value))
        .unwrap_or(DEFAULT_EFFORT);
    ProviderKey {
        provider: normalized(row.provider.as_deref()),
        model: normalized(row.model.as_deref()),
        effort: effort.to_string(),
    }
}

pub(super) fn runtime_group_values(
    group_by: AgentStatsRuntimeGroupByWire,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    attribution: &RunAttribution,
) -> Vec<String> {
    if group_by == AgentStatsRuntimeGroupByWire::Patch {
        if attribution.patches.is_empty() {
            return vec![NO_PATCH.to_string()];
        }
        return attribution
            .patches
            .iter()
            .map(|value| value.name.clone())
            .collect();
    }
    vec![runtime_group_value(group_by, record, row, attribution)]
}

pub(super) fn runtime_group_value(
    group_by: AgentStatsRuntimeGroupByWire,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    attribution: &RunAttribution,
) -> String {
    let meta = record.agent_meta.as_ref();
    let value = match group_by {
        AgentStatsRuntimeGroupByWire::Tribe => {
            meta.and_then(|value| value.clan_tribe.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Clan => {
            meta.and_then(|value| value.agent_clan.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Session => {
            meta.and_then(|value| value.agent_session.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Agent => meta
            .and_then(|value| value.name.as_deref())
            .or(row.agent_name.as_deref()),
        AgentStatsRuntimeGroupByWire::Provider => row.provider.as_deref(),
        AgentStatsRuntimeGroupByWire::Model => row.model.as_deref(),
        AgentStatsRuntimeGroupByWire::Workflow => meta
            .and_then(|value| value.workflow_name.as_deref())
            .or(row.workflow_name.as_deref()),
        AgentStatsRuntimeGroupByWire::Project => {
            Some(row.project_name.as_str())
        }
        AgentStatsRuntimeGroupByWire::Patch => attribution
            .patches
            .first()
            .map(|value| value.name.as_str())
            .or(Some(NO_PATCH)),
    };
    normalized(value)
}

pub(super) fn ranked_counts(
    counts: BTreeMap<String, u64>,
    limit: Option<usize>,
) -> Vec<AgentStatsCountWire> {
    let mut values = counts
        .into_iter()
        .map(|(name, count)| AgentStatsCountWire { name, count })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    if let Some(limit) = limit {
        values.truncate(limit);
    }
    values
}

pub(super) fn finish_providers(
    providers: BTreeMap<ProviderKey, ProviderAccumulator>,
) -> Vec<AgentProviderStatsWire> {
    let mut values = providers
        .into_iter()
        .map(|(key, stats)| AgentProviderStatsWire {
            provider: key.provider,
            model: key.model,
            effort: key.effort,
            runs: stats.runs,
            completed: stats.completed,
            success_rate: ratio(stats.completed, stats.runs),
            total_runtime_seconds: stats.total_runtime_seconds,
            mean_runtime_seconds: (stats.duration_count > 0).then_some(
                stats.total_runtime_seconds / stats.duration_count as f64,
            ),
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.provider
            .cmp(&right.provider)
            .then_with(|| left.model.cmp(&right.model))
            .then_with(|| {
                effort_rank(&left.effort).cmp(&effort_rank(&right.effort))
            })
    });
    values
}

pub(super) fn effort_rank(effort: &str) -> usize {
    if effort == DEFAULT_EFFORT {
        return 0;
    }
    EFFORT_LEVELS_ORDERED
        .iter()
        .position(|value| *value == effort)
        .map(|index| index + 1)
        .unwrap_or(usize::MAX)
}

pub(super) fn finish_workspaces(
    counts: BTreeMap<(String, i64), u64>,
    top_n: usize,
) -> Vec<AgentWorkspaceStatsWire> {
    let mut values = counts
        .into_iter()
        .map(|((project, workspace_num), runs)| AgentWorkspaceStatsWire {
            project,
            workspace_num,
            runs,
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.project.cmp(&right.project))
            .then_with(|| left.workspace_num.cmp(&right.workspace_num))
    });
    values.truncate(top_n);
    values
}

pub(super) fn finish_runtime_groups(
    groups: BTreeMap<String, DurationAccumulator>,
    top_n: usize,
) -> Vec<AgentRuntimeGroupStatsWire> {
    let mut values = groups
        .into_iter()
        .filter_map(|(group, mut accumulator)| {
            if accumulator.values.is_empty() {
                return None;
            }
            accumulator.values.sort_by(f64::total_cmp);
            let runs = accumulator.values.len() as u64;
            let total_seconds = accumulator.values.iter().sum::<f64>();
            Some(AgentRuntimeGroupStatsWire {
                group,
                runs,
                total_seconds,
                mean_seconds: total_seconds / runs as f64,
                p50_seconds: percentile(&accumulator.values, 0.50),
                p95_seconds: percentile(&accumulator.values, 0.95),
                max_seconds: *accumulator.values.last().unwrap_or(&0.0),
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_seconds
            .total_cmp(&left.total_seconds)
            .then_with(|| left.group.cmp(&right.group))
    });
    values.truncate(top_n);
    values
}

pub(super) fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let position = percentile * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = position - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

pub(super) fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

pub(super) fn normalized(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(UNKNOWN)
        .to_string()
}

pub(super) fn resolved_agent_name(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> String {
    normalized(
        record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.name.as_deref())
            .or(row.agent_name.as_deref()),
    )
}
