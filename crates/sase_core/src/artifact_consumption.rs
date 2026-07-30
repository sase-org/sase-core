//! Tolerant artifact-consumption ledger reading and aggregation.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use chrono::DateTime;
use serde::{Deserialize, Serialize};

pub const ARTIFACT_CONSUMPTION_LOG_MIN_SCHEMA_VERSION: u64 = 1;
pub const ARTIFACT_CONSUMPTION_LOG_MAX_SCHEMA_VERSION: u64 = 1;
pub const ARTIFACT_CONSUMPTION_WIRE_SCHEMA_VERSION: u64 = 1;

/// One complete artifact-consumption row, annotated with its envelope version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactConsumptionEventWire {
    #[serde(default)]
    pub schema_version: u64,
    pub id: String,
    pub timestamp: String,
    #[serde(default)]
    pub r#ref: String,
    pub ref_kind: String,
    #[serde(default)]
    pub fragment: Option<String>,
    pub role: String,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub resolved_path: Option<String>,
    pub resolution_status: String,
    #[serde(default)]
    pub agent_name: String,
    pub agent_source: String,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
}

/// Aggregate consumption facts for one fragment-free canonical reference.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactConsumptionSummaryWire {
    pub consumption_count: u64,
    pub distinct_agent_count: u64,
    pub agent_names: Vec<String>,
    pub roles: Vec<String>,
    pub first_consumed_at: Option<String>,
    pub last_consumed_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArtifactConsumptionEnvelope {
    schema_version: u64,
    consumption: ArtifactConsumptionEventWire,
}

#[derive(Default)]
struct ArtifactConsumptionAccumulator {
    consumption_count: u64,
    agent_names: BTreeSet<String>,
    roles: BTreeSet<String>,
    first_consumed_at: Option<String>,
    last_consumed_at: Option<String>,
}

/// Read every admissible event in a consumption JSONL ledger.
///
/// A missing file is an empty ledger. Blank and malformed lines, unsupported
/// envelope versions, and rows without a non-empty reference or agent name are
/// skipped. Unknown JSON fields are ignored by serde.
pub fn read_artifact_consumption_log(
    path: &Path,
) -> Result<Vec<ArtifactConsumptionEventWire>, std::io::Error> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    Ok(parse_artifact_consumption_log(&content))
}

fn parse_artifact_consumption_log(
    content: &str,
) -> Vec<ArtifactConsumptionEventWire> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let envelope =
                serde_json::from_str::<ArtifactConsumptionEnvelope>(line)
                    .ok()?;
            if !artifact_consumption_log_schema_supported(
                envelope.schema_version,
            ) {
                return None;
            }
            let mut event = envelope.consumption;
            event.schema_version = envelope.schema_version;
            artifact_consumption_event_is_admissible(&event).then_some(event)
        })
        .collect()
}

/// Summarize consumption by fragment-free canonical reference.
///
/// When `refs` is supplied, only those references are considered. References
/// without events are omitted, so an absent key means "never consumed."
pub fn summarize_artifact_consumption(
    events: &[ArtifactConsumptionEventWire],
    refs: Option<&[String]>,
) -> BTreeMap<String, ArtifactConsumptionSummaryWire> {
    let selected_refs = refs
        .map(|refs| refs.iter().map(String::as_str).collect::<BTreeSet<_>>());
    let mut accumulators =
        BTreeMap::<String, ArtifactConsumptionAccumulator>::new();

    for event in events {
        if selected_refs
            .as_ref()
            .is_some_and(|refs| !refs.contains(event.r#ref.as_str()))
        {
            continue;
        }
        let accumulator = accumulators.entry(event.r#ref.clone()).or_default();
        accumulator.consumption_count += 1;
        accumulator.agent_names.insert(event.agent_name.clone());
        if !event.role.trim().is_empty() {
            accumulator.roles.insert(event.role.clone());
        }
        if !event.timestamp.trim().is_empty() {
            let is_first = match &accumulator.first_consumed_at {
                Some(first) => {
                    compare_consumed_at(&event.timestamp, first)
                        == Ordering::Less
                }
                None => true,
            };
            if is_first {
                accumulator.first_consumed_at = Some(event.timestamp.clone());
            }
            let is_last = match &accumulator.last_consumed_at {
                Some(last) => {
                    compare_consumed_at(&event.timestamp, last)
                        == Ordering::Greater
                }
                None => true,
            };
            if is_last {
                accumulator.last_consumed_at = Some(event.timestamp.clone());
            }
        }
    }

    accumulators
        .into_iter()
        .map(|(reference, accumulator)| {
            let agent_names =
                accumulator.agent_names.into_iter().collect::<Vec<_>>();
            let roles = accumulator.roles.into_iter().collect::<Vec<_>>();
            (
                reference,
                ArtifactConsumptionSummaryWire {
                    consumption_count: accumulator.consumption_count,
                    distinct_agent_count: agent_names.len() as u64,
                    agent_names,
                    roles,
                    first_consumed_at: accumulator.first_consumed_at,
                    last_consumed_at: accumulator.last_consumed_at,
                },
            )
        })
        .collect()
}

/// Return the fragment-free references of consumed artifact files.
pub fn consumed_artifact_file_refs(
    events: &[ArtifactConsumptionEventWire],
) -> BTreeSet<String> {
    events
        .iter()
        .filter(|event| event.ref_kind == "file")
        .map(|event| event.r#ref.clone())
        .collect()
}

fn artifact_consumption_log_schema_supported(version: u64) -> bool {
    (ARTIFACT_CONSUMPTION_LOG_MIN_SCHEMA_VERSION
        ..=ARTIFACT_CONSUMPTION_LOG_MAX_SCHEMA_VERSION)
        .contains(&version)
}

fn artifact_consumption_event_is_admissible(
    event: &ArtifactConsumptionEventWire,
) -> bool {
    nonempty(&event.r#ref) && nonempty(&event.agent_name)
}

fn nonempty(value: &str) -> bool {
    !value.trim().is_empty()
}

fn compare_consumed_at(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::{json, Value};
    use tempfile::tempdir;

    use super::*;

    fn event(
        reference: &str,
        agent_name: &str,
        role: &str,
        timestamp: &str,
    ) -> Value {
        json!({
            "id": format!("{agent_name}-{timestamp}"),
            "timestamp": timestamp,
            "ref": reference,
            "ref_kind": if reference.starts_with("file:") {
                "file"
            } else {
                "research"
            },
            "fragment": null,
            "role": role,
            "artifact_id": reference.strip_prefix("file:"),
            "resolved_path": "/tmp/artifact",
            "resolution_status": "exact",
            "agent_name": agent_name,
            "agent_source": "SASE_AGENT_NAME",
            "artifacts_dir": null,
            "project": "sase"
        })
    }

    fn envelope(version: u64, event: Value) -> Value {
        json!({
            "schema_version": version,
            "consumption": event
        })
    }

    #[test]
    fn reader_is_tolerant_of_bad_rows_and_an_absent_file() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("consumption.jsonl");
        assert!(read_artifact_consumption_log(&path).unwrap().is_empty());

        let good = event(
            "file:default:abc",
            "agent.one",
            "image",
            "2026-07-30T10:00:00Z",
        );
        let missing_ref = {
            let mut value = good.clone();
            value.as_object_mut().unwrap().remove("ref");
            value
        };
        let missing_agent = {
            let mut value = good.clone();
            value.as_object_mut().unwrap().remove("agent_name");
            value
        };
        fs::write(
            &path,
            format!(
                "\n{{malformed\n{}\n{}\n{}\n{}\n",
                envelope(2, good.clone()),
                envelope(1, missing_ref),
                envelope(1, missing_agent),
                envelope(1, good),
            ),
        )
        .unwrap();

        let events = read_artifact_consumption_log(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].schema_version, 1);
        assert_eq!(events[0].r#ref, "file:default:abc");
    }

    #[test]
    fn summary_distinguishes_events_from_distinct_agents() {
        let content = [
            envelope(
                1,
                event(
                    "file:default:abc",
                    "agent.two",
                    "image",
                    "2026-07-30T12:00:00+04:00",
                ),
            ),
            envelope(
                1,
                event(
                    "file:default:abc",
                    "agent.one",
                    "report",
                    "2026-07-30T10:00:00Z",
                ),
            ),
            envelope(
                1,
                event(
                    "file:default:abc",
                    "agent.one",
                    "image",
                    "2026-07-30T11:00:00Z",
                ),
            ),
            envelope(
                1,
                event(
                    "research:202607/design.md",
                    "agent.three",
                    "report",
                    "2026-07-30T13:00:00Z",
                ),
            ),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        let events = parse_artifact_consumption_log(&content);
        let summaries = summarize_artifact_consumption(&events, None);
        let summary = &summaries["file:default:abc"];
        assert_eq!(summary.consumption_count, 3);
        assert_eq!(summary.distinct_agent_count, 2);
        assert_eq!(summary.agent_names, ["agent.one", "agent.two"]);
        assert_eq!(summary.roles, ["image", "report"]);
        assert_eq!(
            summary.first_consumed_at.as_deref(),
            Some("2026-07-30T12:00:00+04:00")
        );
        assert_eq!(
            summary.last_consumed_at.as_deref(),
            Some("2026-07-30T11:00:00Z")
        );
        assert_eq!(
            consumed_artifact_file_refs(&events),
            BTreeSet::from(["file:default:abc".to_string()])
        );
    }

    #[test]
    fn restricted_summary_omits_unselected_and_never_consumed_refs() {
        let content = envelope(
            1,
            event(
                "file:default:abc",
                "agent.one",
                "image",
                "2026-07-30T10:00:00Z",
            ),
        )
        .to_string();
        let events = parse_artifact_consumption_log(&content);
        let refs = vec![
            "file:default:abc".to_string(),
            "file:default:never".to_string(),
        ];
        let summaries = summarize_artifact_consumption(&events, Some(&refs));
        assert_eq!(
            summaries.keys().map(String::as_str).collect::<Vec<_>>(),
            ["file:default:abc"]
        );
    }
}
