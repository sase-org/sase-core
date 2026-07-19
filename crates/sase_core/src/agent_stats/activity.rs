use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::NaiveDateTime;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;

use crate::agent_scan::AgentArtifactRecordWire;
use crate::plan::read::split_frontmatter;

use super::run::parse_timestamp;
use super::wire::{
    AgentActivityCountWire, AgentActivityStatsRequestWire,
    AgentActivityStatsResponseWire, AgentPlanActivityStatsWire,
    AgentQuestionActivityStatsWire, AgentStatsCountWire,
    AgentStatsDistributionWire, AGENT_STATS_WIRE_SCHEMA_VERSION,
};

const INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const UNKNOWN: &str = "unknown";

#[derive(Debug, Default)]
struct ActivityAccumulator {
    count: u64,
    agents: BTreeSet<String>,
}

#[derive(Debug)]
struct ActivityIndexRow {
    timestamp: String,
    started_at: Option<String>,
    record_json: String,
}

#[derive(Debug)]
struct PlanDocumentStats {
    tier: String,
    phase_count: u64,
}

/// Aggregate durable skill, memory, question-session, and plan activity.
///
/// Missing files and directories are treated as empty inputs. Malformed log
/// lines, request files, and cached artifact rows are skipped independently so
/// one damaged durable record never prevents the rest of the snapshot.
pub fn query_activity_stats(
    index_path: &Path,
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

    response.questions = scan_question_sessions(
        sase_home,
        &request,
        &mut response.malformed_question_files_skipped,
    );
    response.plans = scan_plan_activity(
        index_path,
        sase_home,
        &request,
        &mut response.malformed_rows_skipped,
        &mut response.unresolved_plan_files,
    )?;
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
    sase_home: &Path,
    request: &AgentActivityStatsRequestWire,
    malformed: &mut u64,
) -> AgentQuestionActivityStatsWire {
    let mut result = AgentQuestionActivityStatsWire::default();
    let mut distribution = BTreeMap::<u64, u64>::new();
    for session_dir in sorted_subdirs(&sase_home.join("user_question")) {
        let path = session_dir.join("question_request.json");
        if !path.is_file() {
            continue;
        }
        let Ok(content) = fs::read_to_string(path) else {
            *malformed += 1;
            continue;
        };
        let Ok(value) = serde_json::from_str::<JsonValue>(&content) else {
            *malformed += 1;
            continue;
        };
        let payload = value.get("payload").unwrap_or(&value);
        let timestamp = payload
            .get("timestamp")
            .and_then(json_timestamp)
            .or_else(|| value.get("created_at_unix").and_then(json_timestamp))
            .or_else(|| value.get("created_at").and_then(json_timestamp));
        let Some(timestamp) = timestamp.filter(|value| value.is_finite())
        else {
            *malformed += 1;
            continue;
        };
        let Some(questions) = payload
            .get("questions")
            .and_then(JsonValue::as_array)
            .filter(|values| !values.is_empty())
        else {
            *malformed += 1;
            continue;
        };
        if !in_window(timestamp, request) {
            continue;
        }
        let question_count = questions.len() as u64;
        result.sessions += 1;
        result.questions += question_count;
        *distribution.entry(question_count).or_default() += 1;
    }
    result.questions_per_session = finish_distribution(distribution);
    result.mean_questions_per_session = if result.sessions == 0 {
        0.0
    } else {
        result.questions as f64 / result.sessions as f64
    };
    result
}

fn scan_plan_activity(
    index_path: &Path,
    sase_home: &Path,
    request: &AgentActivityStatsRequestWire,
    malformed_rows: &mut u64,
    unresolved_plans: &mut u64,
) -> Result<AgentPlanActivityStatsWire, String> {
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
            SELECT timestamp, started_at, record_json
            FROM agent_artifacts
            WHERE hidden = 0
            ORDER BY timestamp ASC
            "#,
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok(ActivityIndexRow {
                timestamp: row.get(0)?,
                started_at: row.get(1)?,
                record_json: row.get(2)?,
            })
        })
        .map_err(|error| error.to_string())?;

    let mut result = AgentPlanActivityStatsWire::default();
    let mut tiers = BTreeMap::<String, u64>::new();
    let mut phase_distribution = BTreeMap::<u64, u64>::new();
    let mut epic_proposals = 0u64;
    let mut epic_phases = 0u64;
    for row in rows {
        let row = row.map_err(|error| error.to_string())?;
        let launch_ts = row
            .started_at
            .as_deref()
            .and_then(parse_timestamp)
            .or_else(|| parse_artifact_timestamp(&row.timestamp));
        let Some(launch_ts) = launch_ts else {
            continue;
        };
        if !in_window(launch_ts, request) {
            continue;
        }
        let Ok(record) =
            serde_json::from_str::<AgentArtifactRecordWire>(&row.record_json)
        else {
            *malformed_rows += 1;
            continue;
        };
        let Some(meta) = record.agent_meta.as_ref() else {
            continue;
        };
        let proposed = meta.plan_submitted_at.len() as u64;
        if proposed == 0 {
            continue;
        }
        result.proposed += proposed;
        if meta.plan_approved {
            result.approved += proposed;
        } else if record
            .done
            .as_ref()
            .and_then(|done| done.outcome.as_deref())
            == Some("plan_rejected")
            || matches!(
                meta.plan_action.as_deref(),
                Some("reject" | "rejected")
            )
        {
            result.rejected += proposed;
        } else {
            result.pending += proposed;
        }

        let stats = resolve_plan_document_stats(&record, sase_home);
        let Some(stats) = stats else {
            *unresolved_plans += proposed;
            *tiers.entry(UNKNOWN.to_string()).or_default() += proposed;
            continue;
        };
        *tiers.entry(stats.tier.clone()).or_default() += proposed;
        if stats.tier == "epic" {
            epic_proposals += proposed;
            epic_phases += stats.phase_count * proposed;
            *phase_distribution.entry(stats.phase_count).or_default() +=
                proposed;
        }
    }
    result.tiers = ranked_counts(tiers);
    result.phases_per_epic = finish_distribution(phase_distribution);
    result.mean_phases_per_epic = if epic_proposals == 0 {
        0.0
    } else {
        epic_phases as f64 / epic_proposals as f64
    };
    Ok(result)
}

fn resolve_plan_document_stats(
    record: &AgentArtifactRecordWire,
    sase_home: &Path,
) -> Option<PlanDocumentStats> {
    let meta = record.agent_meta.as_ref()?;
    let mut references = Vec::<&str>::new();
    for value in [
        meta.sdd_plan_path.as_deref(),
        meta.plan_path.as_deref(),
        record
            .plan_path
            .as_ref()
            .and_then(|marker| marker.plan_path.as_deref()),
        record
            .done
            .as_ref()
            .and_then(|done| done.plan_path.as_deref()),
    ]
    .into_iter()
    .flatten()
    {
        let value = value.trim();
        if !value.is_empty() && !references.contains(&value) {
            references.push(value);
        }
    }

    let mut candidates = Vec::<PathBuf>::new();
    for reference in &references {
        let path = expand_home_reference(reference, sase_home);
        if path.is_absolute() {
            push_unique(&mut candidates, path);
        } else if let Some(workspace_dir) = meta.workspace_dir.as_deref() {
            push_unique(&mut candidates, Path::new(workspace_dir).join(&path));
        }
    }

    let mirror_root = sase_home.join("plans");
    for reference in &references {
        let Some(filename) = Path::new(reference).file_name() else {
            continue;
        };
        for month in plan_month_candidates(reference, &meta.plan_submitted_at) {
            push_unique(
                &mut candidates,
                mirror_root.join(month).join(filename),
            );
        }
        push_unique(&mut candidates, mirror_root.join(filename));
        for shard in sorted_subdirs(&mirror_root) {
            push_unique(&mut candidates, shard.join(filename));
        }
    }

    candidates
        .into_iter()
        .find_map(|path| read_plan_document_stats(&path))
}

fn read_plan_document_stats(path: &Path) -> Option<PlanDocumentStats> {
    let content = fs::read_to_string(path).ok()?;
    let (frontmatter, _) = split_frontmatter(&content);
    let parsed = frontmatter
        .as_deref()
        .and_then(|value| serde_yaml::from_str::<YamlValue>(value).ok());
    let mapping = parsed.as_ref().and_then(YamlValue::as_mapping);
    let tier = mapping
        .and_then(|value| value.get(YamlValue::String("tier".to_string())))
        .and_then(YamlValue::as_str)
        .map(str::trim)
        .map(str::to_lowercase)
        .filter(|value| matches!(value.as_str(), "tale" | "epic"))
        .unwrap_or_else(|| UNKNOWN.to_string());
    let phase_count = mapping
        .and_then(|value| value.get(YamlValue::String("phases".to_string())))
        .and_then(YamlValue::as_sequence)
        .map(|values| values.len() as u64)
        .unwrap_or(0);
    Some(PlanDocumentStats { tier, phase_count })
}

fn plan_month_candidates(reference: &str, submitted: &[String]) -> Vec<String> {
    let mut months = BTreeSet::<String>::new();
    if let Some(parent) = Path::new(reference)
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .filter(|value| is_month_shard(value))
    {
        months.insert(parent.to_string());
    }
    for value in submitted {
        if value.len() >= 7 {
            let prefix = &value[..7];
            if prefix.as_bytes().get(4) == Some(&b'-') {
                let month = prefix.replace('-', "");
                if is_month_shard(&month) {
                    months.insert(month);
                }
            }
        }
    }
    months.into_iter().collect()
}

fn is_month_shard(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn expand_home_reference(reference: &str, sase_home: &Path) -> PathBuf {
    let path = Path::new(reference);
    let Some(suffix) = reference.strip_prefix("~/") else {
        return path.to_path_buf();
    };
    sase_home.parent().unwrap_or(sase_home).join(suffix)
}

fn push_unique(values: &mut Vec<PathBuf>, value: PathBuf) {
    if !values.contains(&value) {
        values.push(value);
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

fn parse_artifact_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(parsed.and_utc().timestamp() as f64)
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

    fn request() -> AgentActivityStatsRequestWire {
        AgentActivityStatsRequestWire {
            start_ts: 100,
            end_ts: 200,
            top_n: 10,
            project: None,
        }
    }

    #[test]
    fn aggregates_logs_questions_and_plan_documents() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        let projects = home.join("projects");
        write(
            &projects.join("alpha/skill_uses.jsonl"),
            concat!(
                "{\"timestamp\":\"100\",\"skill_name\":\"review\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"120\",\"skill_name\":\"review\",\"agent_name\":\"b\"}\n",
                "{\"timestamp\":\"125\",\"skill_name\":\"build\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"99\",\"skill_name\":\"outside\",\"agent_name\":\"a\"}\n",
                "not json\n"
            ),
        );
        write(
            &projects.join("beta/memory_reads.jsonl"),
            concat!(
                "{\"timestamp\":\"1970-01-01T00:02:30Z\",\"canonical_path\":\"sase/memory/a.md\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"151\",\"canonical_path\":\"sase/memory/a.md\",\"agent_name\":\"a\"}\n",
                "{\"timestamp\":\"152\",\"canonical_path\":\"sase/memory/b.md\",\"agent_name\":\"b\"}\n"
            ),
        );

        write_json(
            &home.join("user_question/one/question_request.json"),
            json!({"timestamp": 110.0, "questions": [{"question": "one"}]}),
        );
        write_json(
            &home.join("user_question/two/question_request.json"),
            json!({
                "created_at_unix": 130.0,
                "payload": {
                    "questions": [{"question": "one"}, {"question": "two"}]
                }
            }),
        );
        write(&home.join("user_question/bad/question_request.json"), "{");

        let epic = tmp.path().join("plans/epic.md");
        write(
            &epic,
            "---\ntier: epic\nphases:\n- id: one\n- id: two\n---\n# Epic\n",
        );
        let tale = tmp.path().join("plans/tale.md");
        write(&tale, "---\ntier: tale\n---\n# Tale\n");
        add_run(
            &projects,
            "19700101000140",
            json!({
                "name": "planner-one",
                "run_started_at": "100",
                "sdd_plan_path": epic,
                "plan_submitted_at": ["1970-01-01T00:01:40Z", "1970-01-01T00:01:41Z"],
                "plan_approved": true
            }),
            Some(json!({"outcome": "completed", "finished_at": 105.0})),
        );
        add_run(
            &projects,
            "19700101000200",
            json!({
                "name": "planner-two",
                "run_started_at": "120",
                "plan_path": tale,
                "plan_submitted_at": ["1970-01-01T00:02:00Z"],
                "plan_action": "reject"
            }),
            Some(json!({"outcome": "plan_rejected", "finished_at": 125.0})),
        );
        add_run(
            &projects,
            "19700101000220",
            json!({
                "name": "planner-three",
                "run_started_at": "140",
                "plan_path": tmp.path().join("missing.md"),
                "plan_submitted_at": ["1970-01-01T00:02:20Z"]
            }),
            None,
        );
        add_run(
            &projects,
            "19700101000050",
            json!({
                "name": "outside",
                "run_started_at": "50",
                "sdd_plan_path": epic,
                "plan_submitted_at": ["1970-01-01T00:00:50Z"]
            }),
            None,
        );

        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let malformed = add_run(
            &projects,
            "19700101000240",
            json!({"name": "bad", "run_started_at": "160"}),
            None,
        );
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

        let result = query_activity_stats(&index, &home, request()).unwrap();
        assert_eq!(
            result
                .skills
                .iter()
                .map(|value| (
                    value.name.as_str(),
                    value.count,
                    value.distinct_agents
                ))
                .collect::<Vec<_>>(),
            vec![("review", 2, 2), ("build", 1, 1)]
        );
        assert_eq!(result.memories[0].name, "sase/memory/a.md");
        assert_eq!(result.memories[0].count, 2);
        assert_eq!(result.malformed_log_lines_skipped, 1);
        assert_eq!(result.questions.sessions, 2);
        assert_eq!(result.questions.questions, 3);
        assert_eq!(result.questions.mean_questions_per_session, 1.5);
        assert_eq!(
            result.questions.questions_per_session,
            vec![
                AgentStatsDistributionWire { value: 1, count: 1 },
                AgentStatsDistributionWire { value: 2, count: 1 },
            ]
        );
        assert_eq!(result.malformed_question_files_skipped, 1);
        assert_eq!(result.plans.proposed, 4);
        assert_eq!(
            result
                .plans
                .tiers
                .iter()
                .map(|value| (value.name.as_str(), value.count))
                .collect::<Vec<_>>(),
            vec![("epic", 2), ("tale", 1), ("unknown", 1)]
        );
        assert_eq!(result.plans.approved, 2);
        assert_eq!(result.plans.rejected, 1);
        assert_eq!(result.plans.pending, 1);
        assert_eq!(
            result.plans.phases_per_epic,
            vec![AgentStatsDistributionWire { value: 2, count: 2 }]
        );
        assert_eq!(result.plans.mean_phases_per_epic, 2.0);
        assert_eq!(result.unresolved_plan_files, 1);
        assert_eq!(result.malformed_rows_skipped, 1);

        let mut filtered_request = request();
        filtered_request.project = Some("alpha".to_string());
        let filtered =
            query_activity_stats(&index, &home, filtered_request).unwrap();
        assert_eq!(filtered.skills.len(), 2);
        assert!(filtered.memories.is_empty());
        assert_eq!(filtered.questions.sessions, 2);
        assert_eq!(filtered.plans.proposed, 4);
    }

    #[test]
    fn resolves_missing_primary_plan_from_local_month_mirror() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join(".sase");
        let projects = home.join("projects");
        write(
            &home.join("plans/202607/mirrored.md"),
            "---\ntier: epic\nphases:\n- id: only\n---\n# Mirrored\n",
        );
        add_run(
            &projects,
            "20260710000000",
            json!({
                "run_started_at": "2026-07-10T00:00:00Z",
                "sdd_plan_path": "/gone/202607/mirrored.md",
                "plan_submitted_at": ["2026-07-10T00:00:01Z"]
            }),
            None,
        );
        let index = tmp.path().join("index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let result = query_activity_stats(
            &index,
            &home,
            AgentActivityStatsRequestWire {
                start_ts: parse_timestamp("2026-07-10T00:00:00Z").unwrap()
                    as i64,
                end_ts: parse_timestamp("2026-07-11T00:00:00Z").unwrap() as i64,
                top_n: 5,
                project: None,
            },
        )
        .unwrap();
        assert_eq!(result.plans.tiers[0].name, "epic");
        assert_eq!(result.plans.phases_per_epic[0].value, 1);
        assert_eq!(result.unresolved_plan_files, 0);
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
