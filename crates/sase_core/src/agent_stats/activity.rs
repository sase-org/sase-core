use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use super::gate_bundles::{
    read_gate_bundles, GateBundle, GateKind, GateOutcome,
};
use super::run::parse_timestamp;
use super::wire::{
    AgentActivityCountWire, AgentActivityStatsRequestWire,
    AgentActivityStatsResponseWire, AgentPlanActivityStatsWire,
    AgentQuestionActivityStatsWire, AgentStatsCountWire,
    AgentStatsDistributionWire, AGENT_STATS_WIRE_SCHEMA_VERSION,
};

const UNKNOWN: &str = "unknown";

#[derive(Debug, Default)]
struct ActivityAccumulator {
    count: u64,
    agents: BTreeSet<String>,
}

/// Aggregate durable skill, memory, question-session, and plan activity.
///
/// Missing files and directories are treated as empty inputs. Malformed log
/// lines and gate bundles are skipped independently so one damaged durable
/// record never prevents the rest of the snapshot.
pub fn query_activity_stats(
    _index_path: &Path,
    sase_home: &Path,
    request: AgentActivityStatsRequestWire,
) -> Result<AgentActivityStatsResponseWire, String> {
    validate_request(&request)?;

    let mut response = AgentActivityStatsResponseWire {
        schema_version: AGENT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: request.start_ts,
        end_ts: request.end_ts,
        ..AgentActivityStatsResponseWire::default()
    };

    let mut malformed_logs = 0;
    response.skills = finish_activity_counts(
        scan_project_logs(
            sase_home,
            "skill_uses.jsonl",
            "skill_name",
            &request,
            &mut malformed_logs,
        ),
        request.top_n as usize,
    );
    response.memories = finish_activity_counts(
        scan_project_logs(
            sase_home,
            "memory_reads.jsonl",
            "canonical_path",
            &request,
            &mut malformed_logs,
        ),
        request.top_n as usize,
    );
    response.malformed_log_lines_skipped = malformed_logs;

    let question_scan = read_gate_bundles(
        sase_home,
        GateKind::Question,
        &mut response.malformed_question_files_skipped,
    );
    let plan_scan = read_gate_bundles(
        sase_home,
        GateKind::Plan,
        &mut response.malformed_rows_skipped,
    );
    let epic_plan_scan = read_gate_bundles(
        sase_home,
        GateKind::EpicPlan,
        &mut response.malformed_rows_skipped,
    );
    response.coverage_start_ts = [
        question_scan.coverage_start_ts,
        plan_scan.coverage_start_ts,
        epic_plan_scan.coverage_start_ts,
    ]
    .into_iter()
    .flatten()
    .min_by(f64::total_cmp);
    response.questions =
        scan_question_sessions(&question_scan.bundles, &request);
    response.plans = scan_plan_activity(
        &plan_scan.bundles,
        &epic_plan_scan.bundles,
        &request,
    );
    Ok(response)
}

fn validate_request(
    request: &AgentActivityStatsRequestWire,
) -> Result<(), String> {
    if request.end_ts <= request.start_ts {
        return Err(
            "agent activity stats end_ts must be greater than start_ts"
                .to_string(),
        );
    }
    Ok(())
}

fn scan_project_logs(
    sase_home: &Path,
    filename: &str,
    category_field: &str,
    request: &AgentActivityStatsRequestWire,
    malformed: &mut u64,
) -> BTreeMap<String, ActivityAccumulator> {
    let mut counts = BTreeMap::<String, ActivityAccumulator>::new();
    for project_dir in sorted_subdirs(&sase_home.join("projects")) {
        if request.project.as_deref().is_some_and(|project| {
            project_dir.file_name().and_then(|value| value.to_str())
                != Some(project)
        }) {
            continue;
        }
        let path = project_dir.join(filename);
        let Ok(file) = File::open(path) else {
            continue;
        };
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                *malformed += 1;
                continue;
            };
            if line.trim().is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<JsonValue>(&line) else {
                *malformed += 1;
                continue;
            };
            let Some(timestamp) = value
                .get("timestamp")
                .and_then(json_timestamp)
                .filter(|value| value.is_finite())
            else {
                *malformed += 1;
                continue;
            };
            if !in_window(timestamp, request) {
                continue;
            }
            let Some(category) = value
                .get(category_field)
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                *malformed += 1;
                continue;
            };
            let agent =
                normalized(value.get("agent_name").and_then(JsonValue::as_str));
            let accumulator = counts.entry(category.to_string()).or_default();
            accumulator.count += 1;
            accumulator.agents.insert(agent);
        }
    }
    counts
}

fn finish_activity_counts(
    counts: BTreeMap<String, ActivityAccumulator>,
    top_n: usize,
) -> Vec<AgentActivityCountWire> {
    let mut values = counts
        .into_iter()
        .map(|(name, accumulator)| AgentActivityCountWire {
            name,
            count: accumulator.count,
            distinct_agents: accumulator.agents.len() as u64,
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    values.truncate(top_n);
    values
}

fn scan_question_sessions(
    bundles: &[GateBundle],
    request: &AgentActivityStatsRequestWire,
) -> AgentQuestionActivityStatsWire {
    let mut result = AgentQuestionActivityStatsWire::default();
    let mut distribution = BTreeMap::<u64, u64>::new();
    let mut asking_agents = BTreeSet::<&str>::new();
    for bundle in bundles {
        if !matches_project(bundle, request) {
            continue;
        }
        if !in_window(bundle.timestamp, request) {
            continue;
        }
        result.sessions += 1;
        result.questions += bundle.questions;
        *distribution.entry(bundle.questions).or_default() += 1;
        if let Some(agent) = bundle.producer_agent.as_deref() {
            asking_agents.insert(agent);
        }
    }
    result.asking_agents = asking_agents.len() as u64;
    result.questions_per_session = finish_distribution(distribution);
    result.mean_questions_per_session = if result.sessions == 0 {
        0.0
    } else {
        result.questions as f64 / result.sessions as f64
    };
    result
}

fn scan_plan_activity(
    plan_bundles: &[GateBundle],
    epic_plan_bundles: &[GateBundle],
    request: &AgentActivityStatsRequestWire,
) -> AgentPlanActivityStatsWire {
    let mut result = AgentPlanActivityStatsWire::default();
    let mut tiers = BTreeMap::<String, u64>::new();
    let mut phase_distribution = BTreeMap::<u64, u64>::new();
    let mut proposing_agents = BTreeSet::<&str>::new();
    let mut epic_proposals = 0u64;
    let mut epic_phases = 0u64;
    for (bundle, is_epic_plan) in plan_bundles
        .iter()
        .map(|bundle| (bundle, false))
        .chain(epic_plan_bundles.iter().map(|bundle| (bundle, true)))
    {
        if !matches_project(bundle, request)
            || !in_window(bundle.timestamp, request)
        {
            continue;
        }
        result.proposed += 1;
        match bundle.outcome {
            GateOutcome::Approved => result.approved += 1,
            GateOutcome::Rejected => result.rejected += 1,
            GateOutcome::Feedback => result.feedback += 1,
            GateOutcome::Pending => result.pending += 1,
        }
        if let Some(agent) = bundle.producer_agent.as_deref() {
            proposing_agents.insert(agent);
        }
        *tiers
            .entry(
                bundle
                    .authored_tier
                    .clone()
                    .unwrap_or_else(|| UNKNOWN.to_string()),
            )
            .or_default() += 1;
        if is_epic_plan {
            let Some(phase_count) = bundle.phase_count else {
                continue;
            };
            epic_proposals += 1;
            epic_phases += phase_count;
            *phase_distribution.entry(phase_count).or_default() += 1;
        }
    }
    result.proposing_agents = proposing_agents.len() as u64;
    result.tiers = ranked_counts(tiers);
    result.phases_per_epic = finish_distribution(phase_distribution);
    result.mean_phases_per_epic = if epic_proposals == 0 {
        0.0
    } else {
        epic_phases as f64 / epic_proposals as f64
    };
    result
}

fn matches_project(
    bundle: &GateBundle,
    request: &AgentActivityStatsRequestWire,
) -> bool {
    match request.project.as_deref() {
        Some(project) => bundle.project_key.as_deref() == Some(project),
        None => true,
    }
}

fn finish_distribution(
    counts: BTreeMap<u64, u64>,
) -> Vec<AgentStatsDistributionWire> {
    counts
        .into_iter()
        .map(|(value, count)| AgentStatsDistributionWire { value, count })
        .collect()
}

fn ranked_counts(counts: BTreeMap<String, u64>) -> Vec<AgentStatsCountWire> {
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
    values
}

fn sorted_subdirs(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut values = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn json_timestamp(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(parse_timestamp))
}

fn in_window(timestamp: f64, request: &AgentActivityStatsRequestWire) -> bool {
    timestamp >= request.start_ts as f64 && timestamp < request.end_ts as f64
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

    use rusqlite::{params, Connection};
    use serde_json::json;
    use tempfile::tempdir;

    use crate::agent_scan::{
        rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
    };

    use super::*;

    fn write(path: &Path, content: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn write_json(path: &Path, value: JsonValue) {
        write(path, &serde_json::to_string(&value).unwrap());
    }

    fn add_run(
        projects: &Path,
        timestamp: &str,
        meta: JsonValue,
        done: Option<JsonValue>,
    ) -> PathBuf {
        let dir = projects.join("proj/artifacts/ace-run").join(timestamp);
        write_json(&dir.join("agent_meta.json"), meta);
        if let Some(done) = done {
            write_json(&dir.join("done.json"), done);
        }
        dir
    }

    fn add_gate(
        home: &Path,
        kind: &str,
        request_id: &str,
        request: JsonValue,
        plan: Option<&str>,
        response: Option<JsonValue>,
    ) -> PathBuf {
        let dir = home
            .join("interaction_requests")
            .join(kind)
            .join(request_id);
        write_json(&dir.join("request.json"), request);
        if let Some(plan) = plan {
            write(&dir.join("plan.md"), plan);
        }
        if let Some(response) = response {
            write_json(&dir.join("response.json"), response);
        }
        dir
    }

    fn plan_request(
        request_id: &str,
        timestamp: f64,
        tier: &str,
        agent: &str,
        project: &str,
    ) -> JsonValue {
        json!({
            "request_id": request_id,
            "producer": {
                "agent_name": agent,
                "artifacts_dir": format!(
                    "/tmp/.sase/projects/{project}/artifacts/ace-run/one"
                )
            },
            "payload": {
                "timestamp": timestamp,
                "authored_tier": tier
            }
        })
    }

    fn question_request(
        request_id: &str,
        timestamp: f64,
        agent: &str,
        project: &str,
        questions: usize,
    ) -> JsonValue {
        json!({
            "request_id": request_id,
            "producer": {
                "agent_name": agent,
                "artifacts_dir": format!(
                    "/tmp/.sase/projects/{project}/artifacts/ace-run/one"
                )
            },
            "payload": {
                "timestamp": timestamp,
                "questions": (0..questions)
                    .map(|index| json!({"question": index.to_string()}))
                    .collect::<Vec<_>>()
            }
        })
    }

    fn request() -> AgentActivityStatsRequestWire {
        AgentActivityStatsRequestWire {
            start_ts: 100,
            end_ts: 200,
            top_n: 10,
            project: None,
        }
    }

    const TALE_PLAN: &str = "---\ntier: tale\n---\n# Tale\n";
    const EPIC_PLAN: &str =
        "---\ntier: epic\nphases:\n  - id: one\n  - id: two\n---\n# Epic\n";

    #[test]
    fn aggregates_logs_and_project_scoped_gate_bundles() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        let projects = home.join("projects");
        write(
            &projects.join("alpha/skill_uses.jsonl"),
            concat!(
                "{\"timestamp\":\"100\",\"skill_name\":\"review\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"120\",\"skill_name\":\"review\",\"agent_name\":\"b\"}\n",
                "not json\n"
            ),
        );
        write(
            &projects.join("beta/memory_reads.jsonl"),
            concat!(
                "{\"timestamp\":\"150\",\"canonical_path\":\"sase/memory/a.md\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"151\",\"canonical_path\":\"sase/memory/a.md\",\"agent_name\":\"a\"}\n"
            ),
        );

        add_gate(
            &home,
            "plan",
            "plan-one",
            plan_request("plan-one", 110.0, "tale", "planner-a", "alpha"),
            Some(TALE_PLAN),
            Some(json!({"selected_option_ids": ["approve", "commit"]})),
        );
        add_gate(
            &home,
            "epic_plan",
            "epic-one",
            plan_request("epic-one", 120.0, "epic", "planner-b", "alpha"),
            Some(EPIC_PLAN),
            Some(json!({"choice_id": "reject"})),
        );
        add_gate(
            &home,
            "question",
            "question-one",
            question_request("question-one", 130.0, "asker-a", "alpha", 2),
            None,
            None,
        );
        let mut legacy_producer =
            question_request("question-two", 140.0, "ignored", "beta", 1);
        legacy_producer["producer"]
            .as_object_mut()
            .unwrap()
            .remove("agent_name");
        legacy_producer["producer"]["agent"] = json!("asker-b");
        add_gate(
            &home,
            "question",
            "question-two",
            legacy_producer,
            None,
            None,
        );

        let result =
            query_activity_stats(Path::new("/unused/index"), &home, request())
                .unwrap();
        assert_eq!(result.coverage_start_ts, Some(110.0));
        assert_eq!(result.skills[0].count, 2);
        assert_eq!(result.skills[0].distinct_agents, 2);
        assert_eq!(result.memories[0].count, 2);
        assert_eq!(result.malformed_log_lines_skipped, 1);
        assert_eq!(result.plans.proposed, 2);
        assert_eq!(result.plans.proposing_agents, 2);
        assert_eq!(result.plans.approved, 1);
        assert_eq!(result.plans.rejected, 1);
        assert_eq!(result.plans.feedback, 0);
        assert_eq!(result.plans.pending, 0);
        assert_eq!(
            result
                .plans
                .tiers
                .iter()
                .map(|row| (row.name.as_str(), row.count))
                .collect::<Vec<_>>(),
            vec![("epic", 1), ("tale", 1)]
        );
        assert_eq!(
            result.plans.phases_per_epic,
            vec![AgentStatsDistributionWire { value: 2, count: 1 }]
        );
        assert_eq!(result.plans.mean_phases_per_epic, 2.0);
        assert_eq!(result.questions.sessions, 2);
        assert_eq!(result.questions.asking_agents, 2);
        assert_eq!(result.questions.questions, 3);
        assert_eq!(result.questions.mean_questions_per_session, 1.5);

        let mut filtered_request = request();
        filtered_request.project = Some("alpha".to_string());
        let filtered = query_activity_stats(
            Path::new("/still/unused"),
            &home,
            filtered_request,
        )
        .unwrap();
        assert_eq!(filtered.plans.proposed, 2);
        assert_eq!(filtered.questions.sessions, 1);
        assert_eq!(filtered.questions.questions, 2);
        assert!(filtered.memories.is_empty());
    }

    #[test]
    fn maps_both_response_shapes_and_pending_plan_bundles() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        let fixtures = [
            (
                "selected-approved",
                Some(json!({"selected_option_ids": ["commit"]})),
            ),
            (
                "selected-rejected",
                Some(json!({"selected_option_ids": ["reject"]})),
            ),
            (
                "selected-feedback",
                Some(json!({"selected_option_ids": ["feedback"]})),
            ),
            (
                "choice-approved",
                Some(json!({
                    "selected_option_ids": [],
                    "choice_id": "approve"
                })),
            ),
            ("choice-rejected", Some(json!({"choice_id": "reject"}))),
            ("choice-feedback", Some(json!({"choice_id": "feedback"}))),
            ("legacy-tale", Some(json!({"choice_id": "tale"}))),
            ("legacy-epic", Some(json!({"choice_id": "epic"}))),
            ("pending-one", None),
            ("pending-two", None),
        ];
        for (index, (request_id, response)) in fixtures.into_iter().enumerate()
        {
            add_gate(
                &home,
                "plan",
                request_id,
                plan_request(
                    request_id,
                    110.0 + index as f64,
                    "tale",
                    "planner",
                    "alpha",
                ),
                Some(TALE_PLAN),
                response,
            );
        }

        let result =
            query_activity_stats(Path::new("/unused"), &home, request())
                .unwrap();
        assert_eq!(result.plans.proposed, 10);
        assert_eq!(result.plans.approved, 4);
        assert_eq!(result.plans.rejected, 2);
        assert_eq!(result.plans.feedback, 2);
        assert_eq!(result.plans.pending, 2);
        assert_eq!(result.plans.proposing_agents, 1);
    }

    #[test]
    fn counts_gate_even_when_index_row_is_hidden_abandoned() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        let projects = home.join("projects");
        add_gate(
            &home,
            "plan",
            "inside",
            plan_request("inside", 150.0, "tale", "planner", "proj"),
            Some(TALE_PLAN),
            Some(json!({"selected_option_ids": ["approve"]})),
        );
        add_gate(
            &home,
            "plan",
            "outside",
            plan_request("outside", 50.0, "tale", "planner", "proj"),
            Some(TALE_PLAN),
            Some(json!({"selected_option_ids": ["approve"]})),
        );
        let artifact_dir = add_run(
            &projects,
            "19700101000050",
            json!({
                "name": "planner",
                "hidden": false,
                "run_started_at": "50",
                "plan_submitted_at": ["1970-01-01T00:02:30Z"]
            }),
            Some(json!({
                "outcome": "abandoned",
                "hidden": true,
                "finished_at": 60.0
            })),
        );
        let index = tmp.path().join("index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let conn = Connection::open(&index).unwrap();
        let (hidden, record_json): (i64, String) = conn
            .query_row(
                "SELECT hidden, record_json FROM agent_artifacts WHERE artifact_dir = ?1",
                params![artifact_dir.to_string_lossy().as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(hidden, 1);
        assert_eq!(
            serde_json::from_str::<JsonValue>(&record_json).unwrap()["done"]
                ["outcome"],
            "abandoned"
        );

        let result = query_activity_stats(&index, &home, request()).unwrap();
        assert_eq!(result.plans.proposed, 1);
        assert_eq!(result.plans.approved, 1);
        assert_eq!(result.coverage_start_ts, Some(50.0));
    }

    #[test]
    fn skips_malformed_bundles_and_ignores_legacy_question_store() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        write(
            &home.join("interaction_requests/question/bad/request.json"),
            "{",
        );
        let mut fallback =
            question_request("valid-question", 120.0, "asker", "alpha", 1);
        fallback["payload"]
            .as_object_mut()
            .unwrap()
            .remove("timestamp");
        fallback["created_at_unix"] = json!(120.0);
        add_gate(&home, "question", "valid-question", fallback, None, None);
        write_json(
            &home.join("user_question/legacy/question_request.json"),
            json!({"timestamp": 130.0, "questions": [{"question": "ignored"}]}),
        );
        write(
            &home.join("interaction_requests/plan/bad-request/request.json"),
            "{",
        );
        let malformed_response = add_gate(
            &home,
            "plan",
            "bad-response",
            plan_request("bad-response", 110.0, "tale", "planner", "alpha"),
            Some(TALE_PLAN),
            None,
        );
        write(&malformed_response.join("response.json"), "{");
        add_gate(
            &home,
            "plan",
            "valid-plan",
            plan_request("valid-plan", 140.0, "tale", "planner", "alpha"),
            Some(TALE_PLAN),
            None,
        );

        let result =
            query_activity_stats(Path::new("/unused"), &home, request())
                .unwrap();
        assert_eq!(result.malformed_question_files_skipped, 1);
        assert_eq!(result.malformed_rows_skipped, 2);
        assert_eq!(result.questions.sessions, 1);
        assert_eq!(result.questions.questions, 1);
        assert_eq!(result.plans.proposed, 1);
        assert_eq!(result.plans.pending, 1);
        assert_eq!(result.coverage_start_ts, Some(110.0));
    }

    #[test]
    fn rejects_invalid_range_before_opening_index() {
        let result = query_activity_stats(
            Path::new("/does/not/matter.sqlite"),
            Path::new("/does/not/matter"),
            AgentActivityStatsRequestWire {
                start_ts: 100,
                end_ts: 100,
                top_n: 5,
                project: None,
            },
        );
        assert!(result.unwrap_err().contains("end_ts"));
    }
}
