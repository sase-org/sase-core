use std::fs;
use std::path::{Component, Path, PathBuf};

use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;

use crate::plan::read::split_frontmatter;

use super::run::parse_timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateKind {
    Plan,
    EpicPlan,
    Question,
}

impl GateKind {
    fn directory_name(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::EpicPlan => "epic_plan",
            Self::Question => "question",
        }
    }

    fn has_plan(self) -> bool {
        matches!(self, Self::Plan | Self::EpicPlan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateOutcome {
    Approved,
    Rejected,
    Feedback,
    Pending,
}

#[derive(Debug)]
pub(super) struct GateBundle {
    pub(super) request_id: String,
    pub(super) timestamp: f64,
    pub(super) authored_tier: Option<String>,
    pub(super) producer_agent: Option<String>,
    pub(super) producer_artifacts_dir: Option<String>,
    pub(super) project_key: Option<String>,
    pub(super) questions: u64,
    pub(super) phase_count: Option<u64>,
    pub(super) outcome: GateOutcome,
    pub(super) response_timestamp: Option<f64>,
}

#[derive(Debug, Default)]
pub(super) struct GateBundleScan {
    pub(super) bundles: Vec<GateBundle>,
    pub(super) coverage_start_ts: Option<f64>,
}

pub(super) fn read_gate_bundles(
    sase_home: &Path,
    kind: GateKind,
    malformed: &mut u64,
) -> GateBundleScan {
    let mut scan = GateBundleScan::default();
    let root = sase_home
        .join("interaction_requests")
        .join(kind.directory_name());
    for bundle_dir in sorted_subdirs(&root) {
        let request = match read_json_object(&bundle_dir.join("request.json")) {
            Ok(value) => value,
            Err(()) => {
                *malformed += 1;
                continue;
            }
        };
        let Some(timestamp) = request_timestamp(&request) else {
            *malformed += 1;
            continue;
        };
        scan.coverage_start_ts =
            min_timestamp(scan.coverage_start_ts, timestamp);
        match parse_bundle(&bundle_dir, kind, request, timestamp) {
            Ok(bundle) => scan.bundles.push(bundle),
            Err(()) => *malformed += 1,
        }
    }
    scan.bundles.sort_by(|left, right| {
        left.timestamp
            .total_cmp(&right.timestamp)
            .then_with(|| left.request_id.cmp(&right.request_id))
    });
    scan
}

fn parse_bundle(
    bundle_dir: &Path,
    kind: GateKind,
    request: JsonValue,
    timestamp: f64,
) -> Result<GateBundle, ()> {
    let request_id = request
        .get("request_id")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(())?
        .to_string();
    let payload = request
        .get("payload")
        .and_then(JsonValue::as_object)
        .ok_or(())?;
    let producer = request.get("producer").and_then(JsonValue::as_object);
    let authored_tier = payload
        .get("authored_tier")
        .and_then(JsonValue::as_str)
        .and_then(normalized_optional);
    let artifacts_dir = producer
        .and_then(|value| value.get("artifacts_dir"))
        .and_then(JsonValue::as_str)
        .and_then(normalized_optional);
    let producer_agent = producer
        .and_then(|value| value.get("agent_name"))
        .and_then(JsonValue::as_str)
        .and_then(normalized_optional)
        .or_else(|| artifacts_dir.clone())
        .or_else(|| {
            producer
                .and_then(|value| value.get("agent"))
                .and_then(JsonValue::as_str)
                .and_then(normalized_optional)
        });
    let project_key = artifacts_dir
        .as_deref()
        .and_then(project_key_from_artifacts_dir);
    let questions = if kind == GateKind::Question {
        payload
            .get("questions")
            .and_then(JsonValue::as_array)
            .ok_or(())?
            .len() as u64
    } else {
        0
    };
    let phase_count = if kind.has_plan() {
        read_phase_count(&bundle_dir.join("plan.md"))?
    } else {
        None
    };
    let outcome = if kind.has_plan() {
        read_plan_outcome(&bundle_dir.join("response.json"))?
    } else {
        GateOutcome::Pending
    };
    let response_timestamp = (kind == GateKind::Question)
        .then(|| question_response_timestamp(&bundle_dir.join("response.json")))
        .flatten();

    Ok(GateBundle {
        request_id,
        timestamp,
        authored_tier,
        producer_agent,
        producer_artifacts_dir: artifacts_dir,
        project_key,
        questions,
        phase_count,
        outcome,
        response_timestamp,
    })
}

fn read_json_object(path: &Path) -> Result<JsonValue, ()> {
    let content = fs::read_to_string(path).map_err(|_| ())?;
    let value = serde_json::from_str::<JsonValue>(&content).map_err(|_| ())?;
    value.as_object().ok_or(())?;
    Ok(value)
}

fn request_timestamp(request: &JsonValue) -> Option<f64> {
    request
        .get("payload")
        .and_then(|value| value.get("timestamp"))
        .and_then(json_timestamp)
        .or_else(|| request.get("created_at_unix").and_then(json_timestamp))
        .or_else(|| request.get("created_at").and_then(json_timestamp))
        .filter(|value| value.is_finite())
}

fn read_phase_count(path: &Path) -> Result<Option<u64>, ()> {
    let content = fs::read_to_string(path).map_err(|_| ())?;
    let (frontmatter, _) = split_frontmatter(&content);
    let frontmatter = frontmatter.ok_or(())?;
    let parsed =
        serde_yaml::from_str::<YamlValue>(&frontmatter).map_err(|_| ())?;
    let mapping = parsed.as_mapping().ok_or(())?;
    Ok(mapping
        .get(YamlValue::String("phases".to_string()))
        .and_then(YamlValue::as_sequence)
        .map(|values| values.len() as u64))
}

fn read_plan_outcome(path: &Path) -> Result<GateOutcome, ()> {
    if !path.exists() {
        return Ok(GateOutcome::Pending);
    }
    let response = read_json_object(path)?;
    if let Some(selected) = response.get("selected_option_ids") {
        let selected = selected.as_array().ok_or(())?;
        let mut option_ids = Vec::with_capacity(selected.len());
        for value in selected {
            option_ids.push(value.as_str().ok_or(())?);
        }
        if let Some(outcome) = classify_option_ids(&option_ids) {
            return Ok(outcome);
        }
    }
    response
        .get("choice_id")
        .and_then(JsonValue::as_str)
        .and_then(|value| classify_option_ids(&[value]))
        .ok_or(())
}

fn question_response_timestamp(path: &Path) -> Option<f64> {
    let response = read_json_object(path).ok()?;
    response
        .get("responded_at_unix")
        .and_then(json_timestamp)
        .or_else(|| response.get("responded_at").and_then(json_timestamp))
        .filter(|value| value.is_finite())
}

fn classify_option_ids(values: &[&str]) -> Option<GateOutcome> {
    for (option_id, outcome) in [
        ("reject", GateOutcome::Rejected),
        ("feedback", GateOutcome::Feedback),
        ("approve", GateOutcome::Approved),
        ("commit", GateOutcome::Approved),
        // Schema-v1 plan gates encoded the approved tier as the choice.
        ("tale", GateOutcome::Approved),
        ("epic", GateOutcome::Approved),
    ] {
        if values
            .iter()
            .any(|value| value.trim().eq_ignore_ascii_case(option_id))
        {
            return Some(outcome);
        }
    }
    None
}

fn project_key_from_artifacts_dir(value: &str) -> Option<String> {
    let mut components = Path::new(value).components();
    while let Some(component) = components.next() {
        if component != Component::Normal("projects".as_ref()) {
            continue;
        }
        let Component::Normal(project) = components.next()? else {
            return None;
        };
        return project.to_str().and_then(normalized_optional);
    }
    None
}

fn normalized_optional(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn json_timestamp(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(parse_timestamp))
}

fn min_timestamp(current: Option<f64>, candidate: f64) -> Option<f64> {
    Some(current.map_or(candidate, |value| value.min(candidate)))
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
