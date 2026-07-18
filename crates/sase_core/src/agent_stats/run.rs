use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{Connection, OpenFlags};

use crate::agent_scan::AgentArtifactRecordWire;
use crate::effort::{is_valid_effort, EFFORT_LEVELS_ORDERED};

use super::wire::{
    AgentCommitStatsWire, AgentPlanStatsWire, AgentProviderStatsWire,
    AgentQuestionStatsWire, AgentRetryStatsWire, AgentRunBucketWire,
    AgentRunStatsRequestWire, AgentRunStatsResponseWire, AgentRunTotalsWire,
    AgentRuntimeGroupStatsWire, AgentStatsCountWire,
    AgentStatsRuntimeGroupByWire, AgentWorkspaceStatsWire,
    AGENT_STATS_WIRE_SCHEMA_VERSION,
};

const INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BUCKETS: u64 = 1_000_000;
const UNKNOWN: &str = "unknown";
const DEFAULT_EFFORT: &str = "default";

#[derive(Debug)]
struct IndexRunRow {
    project_name: String,
    workflow_dir_name: String,
    workflow_name: Option<String>,
    timestamp: String,
    status: String,
    agent_name: Option<String>,
    model: Option<String>,
    provider: Option<String>,
    started_at: Option<String>,
    finished_at: Option<f64>,
    record_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProviderKey {
    provider: String,
    model: String,
    effort: String,
}

#[derive(Debug, Default)]
struct ProviderAccumulator {
    runs: u64,
    completed: u64,
    duration_count: u64,
    total_runtime_seconds: f64,
}

#[derive(Debug, Default)]
struct DurationAccumulator {
    values: Vec<f64>,
}

/// Aggregate durable per-run artifact-index records over a launch-time range.
///
/// Cached records that cannot be decoded are counted in
/// `malformed_rows_skipped` and do not fail the rest of the snapshot.
pub fn query_run_stats(
    index_path: &Path,
    request: AgentRunStatsRequestWire,
) -> Result<AgentRunStatsResponseWire, String> {
    let bucket_count = validate_request(&request)?;
    let conn = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        format!(
            "failed to open agent artifact index {}: {error}",
            index_path.display()
        )
    })?;
    conn.busy_timeout(INDEX_BUSY_TIMEOUT)
        .map_err(|error| error.to_string())?;

    let mut statement = conn
        .prepare(
            r#"
            SELECT project_name, workflow_dir_name, workflow_name, timestamp,
                   status, agent_name, model, llm_provider, started_at,
                   finished_at, record_json
            FROM agent_artifacts
            WHERE hidden = 0
            ORDER BY timestamp ASC
            "#,
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok(IndexRunRow {
                project_name: row.get(0)?,
                workflow_dir_name: row.get(1)?,
                workflow_name: row.get(2)?,
                timestamp: row.get(3)?,
                status: row.get(4)?,
                agent_name: row.get(5)?,
                model: row.get(6)?,
                provider: row.get(7)?,
                started_at: row.get(8)?,
                finished_at: row.get(9)?,
                record_json: row.get(10)?,
            })
        })
        .map_err(|error| error.to_string())?;

    let mut response = AgentRunStatsResponseWire {
        schema_version: AGENT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: request.start_ts,
        end_ts: request.end_ts,
        runtime_group_by: request.runtime_group_by,
        bucket_seconds: request.bucket_seconds,
        buckets: build_empty_buckets(&request, bucket_count),
        ..AgentRunStatsResponseWire::default()
    };
    let mut outcome_counts = BTreeMap::<String, u64>::new();
    let mut retry_chains = BTreeSet::<String>::new();
    let mut providers = BTreeMap::<ProviderKey, ProviderAccumulator>::new();
    let mut repo_counts = BTreeMap::<String, u64>::new();
    let mut plan_actions = BTreeMap::<String, u64>::new();
    let mut workspace_counts = BTreeMap::<(String, i64), u64>::new();
    let mut runtime_groups = BTreeMap::<String, DurationAccumulator>::new();

    for row in rows {
        let row = row.map_err(|error| error.to_string())?;
        let Some(launch_ts) = launch_timestamp(&row) else {
            continue;
        };
        if launch_ts < request.start_ts as f64
            || launch_ts >= request.end_ts as f64
        {
            continue;
        }
        let Ok(record) =
            serde_json::from_str::<AgentArtifactRecordWire>(&row.record_json)
        else {
            response.malformed_rows_skipped += 1;
            continue;
        };

        response.totals.runs += 1;
        increment_bucket(&mut response.buckets, &request, launch_ts);
        let outcome = fold_lifecycle(&mut response.totals, &record, &row);
        if let Some(outcome) = outcome.as_deref() {
            *outcome_counts.entry(outcome.to_string()).or_default() += 1;
        }

        let duration = run_duration_seconds(&record, &row);
        let provider_key = provider_key(&record, &row);
        let provider_stats = providers.entry(provider_key).or_default();
        provider_stats.runs += 1;
        if outcome.as_deref() == Some("completed") {
            provider_stats.completed += 1;
        }
        if let Some(duration) = duration {
            provider_stats.duration_count += 1;
            provider_stats.total_runtime_seconds += duration;
            let group =
                runtime_group_value(request.runtime_group_by, &record, &row);
            runtime_groups
                .entry(group)
                .or_default()
                .values
                .push(duration);
        }

        fold_retries(&record, &row, &mut response.retries, &mut retry_chains);
        fold_commits(&record, &mut response.commits, &mut repo_counts);
        fold_plans(
            &record,
            outcome.as_deref(),
            &mut response.plans,
            &mut plan_actions,
        );
        fold_questions(&record, &mut response.questions);
        fold_workspace(&record, &row, &mut workspace_counts);
    }

    response.retries.chains = retry_chains.len() as u64;
    response.outcomes = ranked_counts(outcome_counts, None);
    response.providers = finish_providers(providers);
    response.commits.top_repos =
        ranked_counts(repo_counts, Some(request.top_n as usize));
    response.commits.average_per_committing_agent =
        if response.commits.committing_agents == 0 {
            0.0
        } else {
            response.commits.total_commits as f64
                / response.commits.committing_agents as f64
        };
    response.plans.actions = ranked_counts(plan_actions, None);
    response.workspaces =
        finish_workspaces(workspace_counts, request.top_n as usize);
    response.runtime_groups =
        finish_runtime_groups(runtime_groups, request.top_n as usize);
    Ok(response)
}

fn validate_request(request: &AgentRunStatsRequestWire) -> Result<u64, String> {
    if request.end_ts <= request.start_ts {
        return Err(
            "agent run stats end_ts must be greater than start_ts".to_string()
        );
    }
    if request.bucket_seconds == 0 {
        return Err("agent run stats bucket_seconds must be greater than zero"
            .to_string());
    }
    let span = (request.end_ts as i128) - (request.start_ts as i128);
    let bucket_seconds = request.bucket_seconds as i128;
    let bucket_count = ((span - 1) / bucket_seconds + 1) as u64;
    if bucket_count > MAX_BUCKETS {
        return Err(format!(
            "agent run stats request would create {bucket_count} buckets; maximum is {MAX_BUCKETS}"
        ));
    }
    Ok(bucket_count)
}

fn build_empty_buckets(
    request: &AgentRunStatsRequestWire,
    bucket_count: u64,
) -> Vec<AgentRunBucketWire> {
    (0..bucket_count)
        .map(|offset| AgentRunBucketWire {
            start_ts: (request.start_ts as i128
                + offset as i128 * request.bucket_seconds as i128)
                as i64,
            runs: 0,
        })
        .collect()
}

fn increment_bucket(
    buckets: &mut [AgentRunBucketWire],
    request: &AgentRunStatsRequestWire,
    launch_ts: f64,
) {
    let elapsed = launch_ts.floor() as i128 - request.start_ts as i128;
    let index = (elapsed / request.bucket_seconds as i128) as usize;
    if let Some(bucket) = buckets.get_mut(index) {
        bucket.runs += 1;
    }
}

fn launch_timestamp(row: &IndexRunRow) -> Option<f64> {
    row.started_at
        .as_deref()
        .and_then(parse_timestamp)
        .or_else(|| parse_artifact_timestamp(&row.timestamp))
}

pub(super) fn parse_timestamp(value: &str) -> Option<f64> {
    if let Ok(seconds) = value.parse::<f64>() {
        return seconds.is_finite().then_some(seconds);
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Some(datetime_seconds(parsed.with_timezone(&Utc)));
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Some(datetime_seconds(parsed.and_utc()));
        }
    }
    None
}

fn parse_artifact_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(datetime_seconds(parsed.and_utc()))
}

fn datetime_seconds(value: DateTime<Utc>) -> f64 {
    value.timestamp() as f64
        + f64::from(value.timestamp_subsec_nanos()) / 1_000_000_000.0
}

fn fold_lifecycle(
    totals: &mut AgentRunTotalsWire,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> Option<String> {
    if let Some(done) = record.done.as_ref() {
        let outcome = normalized(done.outcome.as_deref());
        match outcome.as_str() {
            "completed" => totals.completed += 1,
            "failed" | "epic_launch_failed" => totals.failed += 1,
            _ => totals.other_terminal += 1,
        }
        return Some(outcome);
    }
    if record.waiting.is_some() || row.status == "waiting" {
        totals.waiting += 1;
    } else {
        totals.in_progress += 1;
    }
    None
}

fn run_duration_seconds(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> Option<f64> {
    let start = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.run_started_at.as_deref())
        .and_then(parse_timestamp)
        .or_else(|| row.started_at.as_deref().and_then(parse_timestamp))?;
    let end = record
        .done
        .as_ref()
        .and_then(|done| done.finished_at)
        .or(row.finished_at)
        .filter(|value| value.is_finite())
        .or_else(|| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.stopped_at.as_deref())
                .and_then(parse_timestamp)
        })?;
    let duration = end - start;
    (duration >= 0.0 && duration.is_finite()).then_some(duration)
}

fn provider_key(
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

fn runtime_group_value(
    group_by: AgentStatsRuntimeGroupByWire,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> String {
    let meta = record.agent_meta.as_ref();
    let value = match group_by {
        AgentStatsRuntimeGroupByWire::Tribe => {
            meta.and_then(|value| value.clan_tribe.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Clan => {
            meta.and_then(|value| value.agent_clan.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Family => {
            meta.and_then(|value| value.agent_family.as_deref())
        }
        AgentStatsRuntimeGroupByWire::Agent => meta
            .and_then(|value| value.name.as_deref())
            .or(row.agent_name.as_deref()),
        AgentStatsRuntimeGroupByWire::Provider => row.provider.as_deref(),
        AgentStatsRuntimeGroupByWire::Model => row.model.as_deref(),
        AgentStatsRuntimeGroupByWire::Workflow => meta
            .and_then(|value| value.workflow_name.as_deref())
            .or(row.workflow_name.as_deref()),
    };
    normalized(value)
}

fn fold_retries(
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

fn fold_commits(
    record: &AgentArtifactRecordWire,
    commits: &mut AgentCommitStatsWire,
    repo_counts: &mut BTreeMap<String, u64>,
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
        commits.committing_agents += 1;
    }
    match count {
        0 => commits.distribution.zero += 1,
        1 => commits.distribution.one += 1,
        2 => commits.distribution.two += 1,
        _ => commits.distribution.three_plus += 1,
    }
}

fn fold_plans(
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

fn fold_questions(
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

fn fold_workspace(
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

fn ranked_counts(
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

fn finish_providers(
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

fn effort_rank(effort: &str) -> usize {
    if effort == DEFAULT_EFFORT {
        return 0;
    }
    EFFORT_LEVELS_ORDERED
        .iter()
        .position(|value| *value == effort)
        .map(|index| index + 1)
        .unwrap_or(usize::MAX)
}

fn finish_workspaces(
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

fn finish_runtime_groups(
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

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
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

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn normalized(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(UNKNOWN)
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use rusqlite::params;
    use serde_json::{json, Value};
    use tempfile::tempdir;

    use crate::agent_scan::{
        rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
    };

    use super::super::wire::AgentCommitDistributionWire;
    use super::*;

    fn write_json(path: &Path, payload: Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
    }

    fn artifact(root: &Path, timestamp: &str) -> PathBuf {
        root.join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(timestamp)
    }

    fn finish_at(timestamp: &str, seconds: f64) -> f64 {
        parse_timestamp(timestamp).unwrap() + seconds
    }

    fn add_run(
        projects: &Path,
        timestamp: &str,
        meta: Value,
        done: Option<Value>,
        waiting: bool,
    ) -> PathBuf {
        let dir = artifact(projects, timestamp);
        write_json(&dir.join("agent_meta.json"), meta);
        if let Some(done) = done {
            write_json(&dir.join("done.json"), done);
        }
        if waiting {
            write_json(&dir.join("waiting.json"), json!({"waiting_for": []}));
        }
        dir
    }

    fn request() -> AgentRunStatsRequestWire {
        AgentRunStatsRequestWire {
            start_ts: parse_timestamp("2026-07-10T00:00:00Z").unwrap() as i64,
            end_ts: parse_timestamp("2026-07-11T00:00:00Z").unwrap() as i64,
            runtime_group_by: AgentStatsRuntimeGroupByWire::Agent,
            bucket_seconds: 6 * 60 * 60,
            top_n: 10,
        }
    }

    #[test]
    fn aggregates_window_outcomes_metadata_and_runtime() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let first_start = "2026-07-10T01:00:00Z";
        let first = add_run(
            &projects,
            "20260710010000",
            json!({
                "name": "alpha",
                "workflow_name": "review",
                "run_started_at": first_start,
                "stopped_at": "2026-07-10T01:09:00Z",
                "llm_provider": "codex",
                "model": "gpt-5",
                "reasoning_effort": "high",
                "agent_clan": "red",
                "agent_family": "fam",
                "clan_tribe": "builders",
                "workspace_num": 1,
                "plan_submitted_at": ["p1", "p2"],
                "plan_approved": true,
                "plan_action": "epic",
                "questions_submitted_at": ["q1", "q2"],
                "retry_chain_root_timestamp": "20260710010000",
                "retried_as_timestamp": "20260710020000",
                "retry_terminal": true
            }),
            Some(json!({
                "outcome": "completed",
                "finished_at": finish_at(first_start, 120.0),
                "workspace_num": 1,
                "step_output": {"meta_commits": [
                    {"sha": "1", "repo_name": "sase"},
                    {"sha": "2", "repo_name": "sase"},
                    {"sha": "3", "repo_name": "core"}
                ]}
            })),
            false,
        );
        let second_start = "2026-07-10T02:00:00Z";
        add_run(
            &projects,
            "20260710020000",
            json!({
                "name": "beta",
                "run_started_at": second_start,
                "stopped_at": "2026-07-10T02:05:00Z",
                "llm_provider": "codex",
                "model": "gpt-5",
                "reasoning_effort": "turbo",
                "workspace_num": 1,
                "plan_submitted_at": ["p3"],
                "plan_action": "reject",
                "retry_attempt": 1,
                "retry_of_timestamp": "20260710010000",
                "retry_chain_root_timestamp": "20260710010000"
            }),
            Some(json!({
                "outcome": "plan_rejected",
                "step_output": {"meta_commits": [
                    {"sha": "4", "repo_name": "sase"}
                ]}
            })),
            false,
        );
        add_run(
            &projects,
            "20260710070000",
            json!({
                "name": "gamma",
                "run_started_at": "2026-07-10T07:00:00Z",
                "llm_provider": "claude",
                "model": "opus",
                "reasoning_effort": "max",
                "workspace_num": 2
            }),
            None,
            true,
        );
        let fourth_start = "2026-07-10T13:00:00Z";
        add_run(
            &projects,
            "20260710130000",
            json!({
                "name": "delta",
                "run_started_at": fourth_start,
                "workspace_num": 3
            }),
            Some(json!({
                "outcome": "completed",
                "finished_at": finish_at(fourth_start, 60.0),
                "step_output": {"meta_commits": [
                    {"sha": "5", "repo_name": "core"},
                    {"sha": "6", "repo_name": "core"}
                ]}
            })),
            false,
        );
        add_run(
            &projects,
            "20260709010000",
            json!({
                "name": "outside",
                "run_started_at": "2026-07-09T01:00:00Z"
            }),
            Some(json!({
                "outcome": "failed",
                "finished_at": finish_at("2026-07-09T01:00:00Z", 30.0)
            })),
            false,
        );
        let malformed = add_run(
            &projects,
            "20260710190000",
            json!({
                "name": "malformed",
                "run_started_at": "2026-07-10T19:00:00Z"
            }),
            None,
            false,
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        Connection::open(&index)
            .unwrap()
            .execute(
                "UPDATE agent_artifacts SET record_json = '{' WHERE artifact_dir = ?1",
                params![malformed.to_string_lossy().as_ref()],
            )
            .unwrap();

        let result = query_run_stats(&index, request()).unwrap();
        assert_eq!(result.totals.runs, 4);
        assert_eq!(result.totals.completed, 2);
        assert_eq!(result.totals.failed, 0);
        assert_eq!(result.totals.other_terminal, 1);
        assert_eq!(result.totals.waiting, 1);
        assert_eq!(result.totals.in_progress, 0);
        assert_eq!(result.malformed_rows_skipped, 1);
        assert_eq!(
            result
                .outcomes
                .iter()
                .map(|value| (value.name.as_str(), value.count))
                .collect::<Vec<_>>(),
            vec![("completed", 2), ("plan_rejected", 1)]
        );
        assert_eq!(result.retries.chains, 1);
        assert_eq!(result.retries.attempts, 1);
        assert_eq!(result.retries.kills, 1);

        let high = result
            .providers
            .iter()
            .find(|value| value.effort == "high")
            .unwrap();
        assert_eq!(high.runs, 1);
        assert_eq!(high.completed, 1);
        assert_eq!(high.mean_runtime_seconds, Some(120.0));
        let default = result
            .providers
            .iter()
            .find(|value| {
                value.provider == "codex" && value.effort == "default"
            })
            .unwrap();
        assert_eq!(default.runs, 1);
        assert_eq!(default.mean_runtime_seconds, Some(300.0));

        assert_eq!(result.commits.total_commits, 6);
        assert_eq!(result.commits.committing_agents, 3);
        assert_eq!(result.commits.average_per_committing_agent, 2.0);
        assert_eq!(
            result.commits.distribution,
            AgentCommitDistributionWire {
                zero: 1,
                one: 1,
                two: 1,
                three_plus: 1,
            }
        );
        assert_eq!(result.commits.top_repos[0].name, "core");
        assert_eq!(result.commits.top_repos[0].count, 3);
        assert_eq!(result.commits.top_repos[1].name, "sase");
        assert_eq!(result.commits.top_repos[1].count, 3);

        assert_eq!(result.plans.proposed, 3);
        assert_eq!(result.plans.proposing_agents, 2);
        assert_eq!(result.plans.approved, 1);
        assert_eq!(result.plans.rejected, 1);
        assert_eq!(result.plans.pending, 0);
        assert_eq!(result.questions.sessions, 2);
        assert_eq!(result.questions.asking_agents, 1);
        assert_eq!(result.workspaces[0].project, "proj");
        assert_eq!(result.workspaces[0].workspace_num, 1);
        assert_eq!(result.workspaces[0].runs, 2);
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.runs)
                .collect::<Vec<_>>(),
            vec![2, 1, 1, 0]
        );

        let alpha = result
            .runtime_groups
            .iter()
            .find(|value| value.group == "alpha")
            .unwrap();
        assert_eq!(alpha.total_seconds, 120.0);
        assert_eq!(alpha.p50_seconds, 120.0);
        let beta = result
            .runtime_groups
            .iter()
            .find(|value| value.group == "beta")
            .unwrap();
        assert_eq!(beta.total_seconds, 300.0);
        assert!(result
            .runtime_groups
            .iter()
            .all(|value| value.group != "gamma"));

        // The fixture DB was built through the real scanner/index insertion
        // path, so this also verifies the denormalized fields used before JSON
        // decoding. Keep the variable live to make that relationship clear.
        assert!(first.exists());
    }

    #[test]
    fn runtime_percentiles_interpolate_sorted_durations() {
        assert_eq!(percentile(&[10.0, 20.0, 30.0], 0.50), 20.0);
        assert_eq!(percentile(&[10.0, 20.0, 30.0], 0.95), 29.0);
    }

    #[test]
    fn rejects_invalid_ranges_and_bucket_explosions() {
        let missing = Path::new("/does/not/matter.sqlite");
        let mut invalid = request();
        invalid.end_ts = invalid.start_ts;
        assert!(query_run_stats(missing, invalid)
            .unwrap_err()
            .contains("end_ts"));

        let mut zero_bucket = request();
        zero_bucket.bucket_seconds = 0;
        assert!(query_run_stats(missing, zero_bucket)
            .unwrap_err()
            .contains("bucket_seconds"));

        let mut excessive = request();
        excessive.end_ts = excessive.start_ts + MAX_BUCKETS as i64 + 1;
        excessive.bucket_seconds = 1;
        assert!(query_run_stats(missing, excessive)
            .unwrap_err()
            .contains("maximum"));
    }
}
