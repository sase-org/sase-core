//! Query entry point for run-stats aggregation.
//!
//! Owns `query_run_stats`, the liveness-aware driver, request
//! validation, bucket scaffolding, and gate-bundle answer times. The
//! per-row work fans out to the lifecycle, fold, attribution, and
//! finishing modules.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::agent_runtime::is_runner_occupancy_record;
use crate::agent_scan::AgentArtifactRecordWire;

use super::super::gate_bundles::{read_gate_bundles, GateKind};
use super::super::runner::{
    HostRunnerLivenessProbe, RunnerLivenessProbe, RunnerStatsBuilder,
};
use super::super::wire::{
    AgentRunBucketWire, AgentRunStatsRequestWire, AgentRunStatsResponseWire,
    AGENT_STATS_WIRE_SCHEMA_VERSION,
};
use super::attribution::*;
use super::finishing::*;
use super::folds::*;
use super::lifecycle::*;
use super::types::*;
use super::xprompts::*;

/// Aggregate durable artifact-index records over one analysis range.
///
/// Existing run aggregates remain launch-window based. Runner occupancy also
/// consumes eligible records that started earlier and overlap the range.
/// Cached records that cannot be decoded are counted in the corresponding
/// launch and/or runner diagnostics and do not fail the rest of the snapshot.
pub fn query_run_stats(
    index_path: &Path,
    request: AgentRunStatsRequestWire,
) -> Result<AgentRunStatsResponseWire, String> {
    query_run_stats_with_liveness(
        index_path,
        request,
        &HostRunnerLivenessProbe::default(),
    )
}

pub(super) fn query_run_stats_with_liveness(
    index_path: &Path,
    request: AgentRunStatsRequestWire,
    liveness: &dyn RunnerLivenessProbe,
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
                   status, cl_name, agent_name, model, llm_provider,
                   started_at, finished_at, record_json
            FROM agent_artifacts
            WHERE (?1 IS NULL OR project_name = ?1)
            ORDER BY timestamp ASC
            "#,
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([request.project.as_deref()], |row| {
            Ok(IndexRunRow {
                project_name: row.get(0)?,
                workflow_dir_name: row.get(1)?,
                workflow_name: row.get(2)?,
                timestamp: row.get(3)?,
                status: row.get(4)?,
                cl_name: row.get(5)?,
                agent_name: row.get(6)?,
                model: row.get(7)?,
                provider: row.get(8)?,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
                record_json: row.get(11)?,
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
    let mut work = WorkAccumulators::default();
    let mut xprompts = XPromptAccumulators {
        runs_with_xprompts: 0,
        runs_without_xprompts: 0,
        total_references: 0,
        by_name: BTreeMap::new(),
        focus: request.xprompt_focus.as_ref().map(|_| {
            XPromptFocusAccumulator {
                providers: BTreeMap::new(),
                tribes: BTreeMap::new(),
                buckets: build_empty_buckets(&request, bucket_count),
            }
        }),
    };
    let mut runner_stats = RunnerStatsBuilder::default();
    let mut committing_names = BTreeSet::<String>::new();
    let question_answer_times = resolved_question_answer_times(index_path);
    let requested_start = request.start_ts as f64;
    let requested_end = request.end_ts as f64;

    for row in rows {
        let row = row.map_err(|error| error.to_string())?;
        let launch_ts = launch_timestamp(&row);
        let launch_in_window = launch_ts.is_some_and(|timestamp| {
            timestamp >= requested_start && timestamp < requested_end
        });
        let runner_candidate =
            runner_overlap_candidate(&row, requested_start, requested_end);
        let runner_handoff_carry = !runner_candidate
            && runner_handoff_carry_candidate(
                &row,
                requested_start,
                requested_end,
            );
        if !launch_in_window && !runner_candidate && !runner_handoff_carry {
            continue;
        }
        let Ok(record) =
            serde_json::from_str::<AgentArtifactRecordWire>(&row.record_json)
        else {
            if launch_in_window {
                response.malformed_rows_skipped += 1;
            }
            if runner_candidate {
                runner_stats.record_malformed_row();
            }
            continue;
        };
        if record_is_user_hidden(&record) {
            if runner_candidate && is_runner_occupancy_record(&record) {
                runner_stats.record_user_hidden();
            }
            continue;
        }
        let resolved_answers = question_answer_times
            .get(record.artifact_dir.as_str())
            .map(Vec::as_slice)
            .unwrap_or_default();
        if runner_candidate {
            runner_stats.add_record(
                &record,
                requested_start,
                requested_end,
                resolved_answers,
                liveness,
            );
        } else if runner_handoff_carry {
            runner_stats.add_handoff_carry_record(
                &record,
                requested_end,
                resolved_answers,
            );
        }
        if !launch_in_window {
            continue;
        }
        let launch_ts = launch_ts.expect("in-window launch has a timestamp");

        response.totals.runs += 1;
        increment_bucket(&mut response.buckets, &request, launch_ts);
        let outcome = fold_lifecycle(&mut response.totals, &record, &row);
        if let Some(outcome) = outcome.as_deref() {
            *outcome_counts.entry(outcome.to_string()).or_default() += 1;
        }

        let duration = run_duration_seconds(&record, &row);
        let attribution = resolve_run_attribution(&record, &row);
        fold_xprompts(
            &record,
            &row,
            launch_ts,
            duration,
            outcome.as_deref(),
            &attribution,
            &request,
            &mut xprompts,
        );
        let provider_key = provider_key(&record, &row);
        let provider_stats = providers.entry(provider_key).or_default();
        provider_stats.runs += 1;
        if outcome.as_deref() == Some("completed") {
            provider_stats.completed += 1;
        }
        if let Some(duration) = duration {
            provider_stats.duration_count += 1;
            provider_stats.total_runtime_seconds += duration;
            for group in runtime_group_values(
                request.runtime_group_by,
                &record,
                &row,
                &attribution,
            ) {
                runtime_groups
                    .entry(group)
                    .or_default()
                    .values
                    .push(duration);
            }
        }

        let agent = resolved_agent_name(&record, &row);
        fold_retries(&record, &row, &mut response.retries, &mut retry_chains);
        fold_commits(
            &record,
            &agent,
            &mut response.commits,
            &mut repo_counts,
            &mut committing_names,
        );
        fold_plans(
            &record,
            outcome.as_deref(),
            &mut response.plans,
            &mut plan_actions,
        );
        fold_questions(&record, &mut response.questions);
        fold_workspace(&record, &row, &mut workspace_counts);
        fold_work(
            &record,
            &row,
            &agent,
            launch_ts,
            duration,
            outcome.as_deref(),
            &attribution,
            &mut work,
        );
    }

    response.retries.chains = retry_chains.len() as u64;
    response.outcomes = ranked_counts(outcome_counts, None);
    response.providers = finish_providers(providers);
    response.commits.top_repos =
        ranked_counts(repo_counts, Some(request.top_n as usize));
    response.commits.committing_agents = committing_names.len() as u64;
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
    response.work = finish_work(work, request.work_top_n as usize);
    response.xprompts = Some(finish_xprompts(xprompts, &request));
    response.runners = runner_stats.finish(
        requested_start,
        requested_end,
        request.bucket_seconds,
        request.start_ts == 0,
    );
    Ok(response)
}

pub(super) fn resolved_question_answer_times(
    index_path: &Path,
) -> BTreeMap<String, Vec<f64>> {
    let Some(sase_home) = index_path.parent() else {
        return BTreeMap::new();
    };
    let mut ignored_malformed = 0;
    let scan = read_gate_bundles(
        sase_home,
        GateKind::Question,
        &mut ignored_malformed,
    );
    let mut by_artifacts_dir = BTreeMap::<String, Vec<f64>>::new();
    for bundle in scan.bundles {
        let (Some(artifacts_dir), Some(answered_at)) =
            (bundle.producer_artifacts_dir, bundle.response_timestamp)
        else {
            continue;
        };
        by_artifacts_dir
            .entry(artifacts_dir)
            .or_default()
            .push(answered_at);
    }
    for answers in by_artifacts_dir.values_mut() {
        answers.sort_by(f64::total_cmp);
    }
    by_artifacts_dir
}

pub(super) fn validate_request(
    request: &AgentRunStatsRequestWire,
) -> Result<u64, String> {
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

pub(super) fn build_empty_buckets(
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

pub(super) fn increment_bucket(
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
