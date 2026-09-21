//! Per-dimension folds for retries, commits, plans, questions, and
//! workspaces.
//!
//! Each fold observes one durable facet of a run record. XPrompt and
//! project/patch work folds live in their own modules beside the
//! finishing passes that consume them.

use std::collections::{BTreeMap, BTreeSet};

use crate::agent_scan::AgentArtifactRecordWire;

use super::super::wire::{
    AgentCommitStatsWire, AgentPlanStatsWire, AgentQuestionStatsWire,
    AgentRetryStatsWire,
};
use super::finishing::normalized;
use super::types::*;

pub(super) fn fold_retries(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    retries: &mut AgentRetryStatsWire,
    chains: &mut BTreeSet<String>,
) {
    let Some(meta) = record.agent_meta.as_ref() else {
        return;
    };
    let is_attempt = meta.retry_attempt.is_some_and(|attempt| attempt > 0)
        || meta.retry_of_timestamp.is_some();
    if is_attempt {
        retries.attempts += 1;
    }
    if meta.retry_terminal {
        retries.kills += 1;
    }
    if is_attempt
        || meta.retry_terminal
        || meta.retried_as_timestamp.is_some()
        || meta.retry_chain_root_timestamp.is_some()
    {
        let root = meta
            .retry_chain_root_timestamp
            .as_deref()
            .or(meta.retry_of_timestamp.as_deref())
            .unwrap_or(&row.timestamp);
        chains.insert(format!(
            "{}\0{}\0{root}",
            row.project_name, row.workflow_dir_name
        ));
    }
}

pub(super) fn fold_commits(
    record: &AgentArtifactRecordWire,
    agent: &str,
    commits: &mut AgentCommitStatsWire,
    repo_counts: &mut BTreeMap<String, u64>,
    committing_names: &mut BTreeSet<String>,
) {
    let meta_commits = record
        .done
        .as_ref()
        .and_then(|done| done.step_output.as_ref())
        .and_then(|output| output.get("meta_commits"))
        .and_then(|value| value.as_array());
    let mut count = 0u64;
    if let Some(meta_commits) = meta_commits {
        for commit in meta_commits.iter().filter_map(|value| value.as_object())
        {
            count += 1;
            let repo =
                normalized(commit.get("repo_name").and_then(|v| v.as_str()));
            *repo_counts.entry(repo).or_default() += 1;
        }
    }
    commits.total_commits += count;
    if count > 0 {
        commits.committing_runs += 1;
        committing_names.insert(agent.to_string());
    }
    match count {
        0 => commits.distribution.zero += 1,
        1 => commits.distribution.one += 1,
        2 => commits.distribution.two += 1,
        _ => commits.distribution.three_plus += 1,
    }
}

pub(super) fn fold_plans(
    record: &AgentArtifactRecordWire,
    outcome: Option<&str>,
    plans: &mut AgentPlanStatsWire,
    actions: &mut BTreeMap<String, u64>,
) {
    let Some(meta) = record.agent_meta.as_ref() else {
        return;
    };
    let proposed = meta.plan_submitted_at.len() as u64;
    if proposed == 0 {
        return;
    }
    plans.proposed += proposed;
    plans.proposing_agents += 1;
    if let Some(action) = meta.plan_action.as_deref() {
        *actions.entry(normalized(Some(action))).or_default() += 1;
    }
    if meta.plan_approved {
        plans.approved += 1;
    } else if outcome == Some("plan_rejected")
        || matches!(meta.plan_action.as_deref(), Some("reject" | "rejected"))
    {
        plans.rejected += 1;
    } else {
        plans.pending += 1;
    }
}

pub(super) fn fold_questions(
    record: &AgentArtifactRecordWire,
    questions: &mut AgentQuestionStatsWire,
) {
    let Some(meta) = record.agent_meta.as_ref() else {
        return;
    };
    let sessions = meta.questions_submitted_at.len() as u64;
    questions.sessions += sessions;
    if sessions > 0 {
        questions.asking_agents += 1;
    }
}

pub(super) fn fold_workspace(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    workspaces: &mut BTreeMap<(String, i64), u64>,
) {
    let workspace_num = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .or_else(|| record.done.as_ref().and_then(|done| done.workspace_num));
    if let Some(workspace_num) = workspace_num {
        *workspaces
            .entry((row.project_name.clone(), workspace_num))
            .or_default() += 1;
    }
}
