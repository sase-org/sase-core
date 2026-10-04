//! Tolerant capped decoders for every finalizer artifact.
//!
//! Each input parses on its own: a failure degrades that input's facts
//! (unknown triggers, no timeline, no journal) and never fails the run,
//! except the strict plan, whose failure makes the run `unavailable`.
//! Nothing here reads config values, so the secrecy rule holds by
//! construction: only instance ids, provider refs, defaults, required
//! flags, and entry digests leave the authority snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use thiserror::Error;

use super::super::digest::canonical_json_bytes;
use super::super::selection::validate_finalizer_plan;
use super::super::wire::{
    FinalizerDiagnosticSeverityWire, FinalizerDiagnosticWire,
    FinalizerInstanceStatusWire, FinalizerPlanWire,
};
use super::wire::{
    RunViewDeclarationWire, RunViewDriftWire, RunViewRecoveryTurnWire,
    RunViewTextInputWire, RUN_VIEW_MAX_BYTES, RUN_VIEW_TEXT_CAP_CHARS,
};

/// Errors from the node-view projector. Decode failures of individual
/// artifacts are not errors: they degrade to `unavailable` facts or empty
/// facts inside the response. Only a structurally invalid request fails.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RunViewError {
    /// The request itself is not a valid node-view request.
    #[error("{0}")]
    Validation(String),
}

impl RunViewError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

/// Cap a string on char boundaries.
pub(crate) fn cap_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect()
}

/// Cap free-form output text.
pub(crate) fn cap_output(value: &str) -> String {
    cap_chars(value, RUN_VIEW_TEXT_CAP_CHARS)
}

fn cap_short(value: &str) -> String {
    cap_chars(value, 120)
}

/// Raw-path names for the `too_large` unavailable reasons.
pub(crate) const PATH_AGENT_META: &str = "agent_meta.json";
pub(crate) const PATH_PLAN: &str = "finalizer_plan.json";
pub(crate) const PATH_AUTHORITY_PLAN: &str = "finalizer_plan.authority.json";
pub(crate) const PATH_CONTEXT: &str = "final_context.json";
pub(crate) const PATH_SUBMISSION: &str = "final_submission.json";
pub(crate) const PATH_SUBMISSION_ATTEMPTS: &str =
    "final_submission_attempts.jsonl";
pub(crate) const PATH_JOURNAL: &str = "finalizers/progress.jsonl";
pub(crate) const PATH_RESULT: &str = "finalizer_result.json";

/// Usable text, or the raw path when the input exceeded the ceiling.
pub(crate) enum TextFacts {
    Empty,
    TooLarge(&'static str),
    Text(String),
}

pub(crate) fn text_facts(
    input: &RunViewTextInputWire,
    path: &'static str,
) -> TextFacts {
    if input.too_large || input.size > RUN_VIEW_MAX_BYTES as u64 {
        return TextFacts::TooLarge(path);
    }
    match input.text.as_deref() {
        None | Some("") => TextFacts::Empty,
        Some(text) => {
            if text.len() > RUN_VIEW_MAX_BYTES {
                TextFacts::TooLarge(path)
            } else {
                TextFacts::Text(text.to_string())
            }
        }
    }
}

/// The strict plan plus its canonical bytes for authority comparison.
pub(crate) struct PlanFacts {
    pub plan: FinalizerPlanWire,
    pub canonical: Vec<u8>,
}

/// Strictly parse the plan. Failure makes the run `unavailable`.
pub(crate) fn decode_plan(text: &str) -> Result<PlanFacts, String> {
    let plan: FinalizerPlanWire = serde_json::from_str(text)
        .map_err(|error| cap_short(&format!("plan is not valid: {error}")))?;
    validate_finalizer_plan(&plan)
        .map_err(|error| cap_short(&error.to_string()))?;
    let value = serde_json::to_value(&plan)
        .map_err(|error| cap_short(&error.to_string()))?;
    let canonical = canonical_json_bytes(&value)
        .map_err(|error| cap_short(&error.to_string()))?;
    Ok(PlanFacts { plan, canonical })
}

/// Authority facts: the sealed plan for the rule-1 comparison, plus only
/// the snapshot fields the projection may read.
pub(crate) struct AuthorityFacts {
    pub plan_canonical: Option<Vec<u8>>,
    pub instance_refs: BTreeMap<String, Option<String>>,
    pub defaults: Vec<String>,
    pub required: Vec<String>,
}

fn value_to_string_list(value: &JsonValue) -> Vec<String> {
    match value.as_array() {
        Some(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) fn decode_authority(text: &str) -> AuthorityFacts {
    let empty = AuthorityFacts {
        plan_canonical: None,
        instance_refs: BTreeMap::new(),
        defaults: Vec::new(),
        required: Vec::new(),
    };
    let payload: JsonValue = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return empty,
    };
    let mut facts = empty;
    if let Some(plan_value) = payload.get("plan") {
        if let Ok(plan) =
            serde_json::from_value::<FinalizerPlanWire>(plan_value.clone())
        {
            if validate_finalizer_plan(&plan).is_ok() {
                facts.plan_canonical = serde_json::to_value(&plan)
                    .ok()
                    .and_then(|value| canonical_json_bytes(&value).ok());
            }
        }
    }
    let snapshot = payload
        .get("config_snapshot")
        .and_then(|snapshot| snapshot.get("config"));
    if let Some(config) = snapshot {
        facts.defaults = config
            .get("defaults")
            .map(value_to_string_list)
            .unwrap_or_default();
        facts.required = config
            .get("required")
            .map(value_to_string_list)
            .unwrap_or_default();
        if let Some(instances) = config.get("instances") {
            match instances {
                JsonValue::Array(items) => {
                    for item in items {
                        let id =
                            item.get("instance_id").and_then(JsonValue::as_str);
                        if let Some(id) = id.filter(|id| !id.is_empty()) {
                            let provider_ref = item
                                .get("provider_ref")
                                .and_then(JsonValue::as_str)
                                .map(str::to_string);
                            facts
                                .instance_refs
                                .insert(id.to_string(), provider_ref);
                        }
                    }
                }
                JsonValue::Object(map) => {
                    for (id, item) in map {
                        if id.is_empty() {
                            continue;
                        }
                        let provider_ref = item
                            .get("provider_ref")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string);
                        facts.instance_refs.insert(id.clone(), provider_ref);
                    }
                }
                _ => {}
            }
        }
    }
    facts
}

/// Trigger and submission facts per selected instance.
pub(crate) struct ContextRequirementFacts {
    pub instance_id: String,
    pub trigger: String,
    pub submission_required: bool,
}

pub(crate) struct ContextFacts {
    pub plan_digest: Option<String>,
    pub requirements: Vec<ContextRequirementFacts>,
    pub obligation_count: u32,
}

/// Tolerant context decode; any failure means trigger facts are unknown.
pub(crate) fn decode_context(text: &str) -> ContextFacts {
    let empty = ContextFacts {
        plan_digest: None,
        requirements: Vec::new(),
        obligation_count: 0,
    };
    let payload: JsonValue = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return empty,
    };
    let object = match payload.as_object() {
        Some(object) => object,
        None => return empty,
    };
    let mut facts = empty;
    facts.plan_digest = object
        .get("plan_digest")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    if let Some(requirements) =
        object.get("requirements").and_then(JsonValue::as_array)
    {
        for requirement in requirements {
            let instance_id = requirement
                .get("instance_id")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            if instance_id.is_empty() {
                continue;
            }
            facts.requirements.push(ContextRequirementFacts {
                instance_id: instance_id.to_string(),
                trigger: requirement
                    .get("trigger")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
                submission_required: requirement
                    .get("submission_required")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false),
            });
        }
    }
    facts.obligation_count = object
        .get("obligations")
        .and_then(JsonValue::as_array)
        .map(|obligations| obligations.len().min(u32::MAX as usize) as u32)
        .unwrap_or(0);
    facts
}

/// One submission payload's top-level keys mapped to short values.
pub(crate) type PayloadSummary = BTreeMap<String, String>;

fn summarize_payload_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(flag) => flag.to_string(),
        JsonValue::Number(number) => number.to_string(),
        JsonValue::String(text) => cap_chars(text, 120),
        JsonValue::Array(items) => format!("[{} items]", items.len()),
        JsonValue::Object(map) => format!("{{{} keys}}", map.len()),
    }
}

pub(crate) struct SubmissionFacts {
    pub plan_digest: Option<String>,
    pub summaries: BTreeMap<String, PayloadSummary>,
}

pub(crate) fn decode_submission(text: &str) -> SubmissionFacts {
    let mut facts = SubmissionFacts {
        plan_digest: None,
        summaries: BTreeMap::new(),
    };
    let payload: JsonValue = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return facts,
    };
    let object = match payload.as_object() {
        Some(object) => object,
        None => return facts,
    };
    facts.plan_digest = object
        .get("plan_digest")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    if let Some(payloads) = object.get("payloads").and_then(JsonValue::as_array)
    {
        for entry in payloads {
            let instance_id = entry
                .get("instance_id")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            if instance_id.is_empty() {
                continue;
            }
            let mut summary = PayloadSummary::new();
            if let Some(fields) =
                entry.get("payload").and_then(JsonValue::as_object)
            {
                for (key, value) in fields.iter().take(32) {
                    summary.insert(
                        cap_chars(key, 64),
                        summarize_payload_value(value),
                    );
                }
            }
            facts.summaries.insert(instance_id.to_string(), summary);
        }
    }
    facts
}

/// Supported submission-attempts / journal envelope schema versions.
const SUPPORTED_LOG_SCHEMA_VERSIONS: [u64; 1] = [1];

fn log_schema_supported(version: Option<u64>) -> bool {
    version
        .is_some_and(|version| SUPPORTED_LOG_SCHEMA_VERSIONS.contains(&version))
}

/// Parse tolerant JSONL rows into their envelope values, skipping blank,
/// malformed, and unsupported-version rows.
pub(crate) fn parse_tolerant_jsonl(text: &str) -> Vec<JsonValue> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: JsonValue = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !log_schema_supported(value.get("v").and_then(JsonValue::as_u64)) {
            // Rows without an explicit `v` predate versioning; rows with an
            // unsupported `v` belong to a newer writer. Accept unversioned
            // rows, skip newer ones.
            if value.get("v").is_some() {
                continue;
            }
        }
        rows.push(value);
    }
    rows
}

fn event_time(row: &JsonValue) -> Option<f64> {
    row.get("t")
        .and_then(JsonValue::as_f64)
        .filter(|time| time.is_finite() && *time >= 0.0)
}

fn event_string(row: &JsonValue, key: &str) -> Option<String> {
    row.get(key).and_then(JsonValue::as_str).map(str::to_string)
}

/// Decode the declaration timeline from submission attempts: time,
/// accepted/rejected, code, first message line, and payload count.
pub(crate) fn decode_submission_timeline(
    text: &str,
) -> Vec<RunViewDeclarationWire> {
    let mut timeline = Vec::new();
    for row in parse_tolerant_jsonl(text) {
        let status = event_string(&row, "status").unwrap_or_default();
        if status.is_empty() {
            continue;
        }
        let message = event_string(&row, "message").unwrap_or_default();
        let first_line = message
            .lines()
            .next()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(cap_output);
        timeline.push(RunViewDeclarationWire {
            t: event_time(&row),
            status: cap_chars(&status, 32),
            code: event_string(&row, "code").map(|code| cap_chars(&code, 120)),
            first_line,
            payload_count: row
                .get("payload_count")
                .and_then(JsonValue::as_u64)
                .map(|count| count.min(u32::MAX as u64) as u32)
                .or_else(|| {
                    row.get("payloads").and_then(JsonValue::as_array).map(
                        |payloads| payloads.len().min(u32::MAX as usize) as u32,
                    )
                }),
        });
    }
    timeline
}

/// One budgeted try seen in journal attempt events, with event times for
/// attempt timing when the writer supplied them.
#[derive(Debug, Clone, Default)]
pub(crate) struct JournalAttemptFacts {
    pub instance_id: String,
    pub attempt: u32,
    pub max_attempts: Option<u32>,
    pub finished_status: Option<String>,
    pub code: Option<String>,
    pub started_t: Option<f64>,
    pub finished_t: Option<f64>,
}

/// One operation seen in journal op events. Entries without a C2 record
/// file still render from these facts; entries with one only fill gaps.
#[derive(Debug, Clone, Default)]
pub(crate) struct JournalOpFacts {
    pub instance_id: String,
    pub attempt: Option<u32>,
    pub op: String,
    pub kind: Option<String>,
    pub label: Option<String>,
    pub returncode: Option<i64>,
    pub duration_seconds: Option<f64>,
    pub timed_out: bool,
    pub finished: bool,
}

/// Facts from one journal segment (between `phase_started` records).
#[derive(Debug, Clone, Default)]
pub(crate) struct JournalSegmentFacts {
    pub phase_started_run_id: Option<String>,
    pub phase_started_plan_digest: Option<String>,
    pub runner_pid: Option<u64>,
    pub runner_identity: Option<String>,
    pub skipped_reason: Option<String>,
    pub finished_status: Option<String>,
    pub finished_cycles: Option<u32>,
    pub truncated: bool,
    pub cycles: BTreeSet<u32>,
    pub declaration_statuses: Vec<RunViewDeclarationWire>,
    pub recovery: Option<RunViewRecoveryTurnWire>,
    pub finished_instances: BTreeMap<String, String>,
    pub active_instance_id: Option<String>,
    pub active_attempt: Option<u32>,
    pub active_max_attempts: Option<u32>,
    pub active_op: Option<String>,
    pub attempt_events: Vec<JournalAttemptFacts>,
    pub op_events: Vec<JournalOpFacts>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct JournalFacts {
    pub segments: Vec<JournalSegmentFacts>,
    pub earlier_segments: u32,
}

fn journal_runner(row: &JsonValue) -> (Option<u64>, Option<String>) {
    let runner = match row.get("runner") {
        Some(runner) => runner,
        None => return (None, None),
    };
    (
        runner.get("pid").and_then(JsonValue::as_u64),
        runner
            .get("identity")
            .and_then(JsonValue::as_str)
            .map(str::to_string),
    )
}

fn record_attempt_started(
    segment: &mut JournalSegmentFacts,
    instance_id: &str,
    attempt: u32,
    max_attempts: Option<u32>,
    started_t: Option<f64>,
) {
    if let Some(entry) = segment.attempt_events.iter_mut().find(|entry| {
        entry.instance_id == instance_id && entry.attempt == attempt
    }) {
        if entry.max_attempts.is_none() {
            entry.max_attempts = max_attempts;
        }
        if entry.started_t.is_none() {
            entry.started_t = started_t;
        }
        return;
    }
    segment.attempt_events.push(JournalAttemptFacts {
        instance_id: instance_id.to_string(),
        attempt,
        max_attempts,
        finished_status: None,
        code: None,
        started_t,
        finished_t: None,
    });
}

fn record_attempt_finished(
    segment: &mut JournalSegmentFacts,
    instance_id: &str,
    attempt: Option<u32>,
    status: Option<String>,
    code: Option<String>,
    finished_t: Option<f64>,
) {
    let entry = match attempt {
        Some(attempt) => segment.attempt_events.iter_mut().find(|entry| {
            entry.instance_id == instance_id && entry.attempt == attempt
        }),
        None => segment
            .attempt_events
            .iter_mut()
            .rev()
            .find(|entry| entry.instance_id == instance_id),
    };
    let Some(entry) = entry else {
        return;
    };
    if entry.finished_status.is_none() {
        entry.finished_status = status
            .map(|status| cap_chars(&status, 32))
            .filter(|status| !status.is_empty());
    }
    if entry.code.is_none() {
        entry.code = code
            .map(|code| cap_chars(&code, 120))
            .filter(|code| !code.is_empty());
    }
    if entry.finished_t.is_none() {
        entry.finished_t = finished_t;
    }
}

fn record_op_started(
    segment: &mut JournalSegmentFacts,
    instance_id: &str,
    attempt: Option<u32>,
    op: &str,
    kind: Option<String>,
    label: Option<String>,
) {
    if segment.op_events.iter().any(|entry| {
        entry.instance_id == instance_id
            && entry.attempt == attempt
            && entry.op == op
    }) {
        return;
    }
    segment.op_events.push(JournalOpFacts {
        instance_id: instance_id.to_string(),
        attempt,
        op: cap_chars(op, 120),
        kind: kind
            .map(|kind| cap_chars(&kind, 32))
            .filter(|kind| !kind.is_empty()),
        label: label
            .map(|label| cap_chars(&label, 120))
            .filter(|label| !label.is_empty()),
        returncode: None,
        duration_seconds: None,
        timed_out: false,
        finished: false,
    });
}

fn record_op_finished(
    segment: &mut JournalSegmentFacts,
    instance_id: &str,
    attempt: Option<u32>,
    op: &str,
    returncode: Option<i64>,
    duration_seconds: Option<f64>,
    timed_out: bool,
) {
    let entry = segment.op_events.iter_mut().rev().find(|entry| {
        entry.instance_id == instance_id
            && entry.attempt == attempt
            && entry.op == op
            && !entry.finished
    });
    match entry {
        Some(entry) => {
            entry.returncode = returncode;
            entry.duration_seconds = duration_seconds;
            entry.timed_out = timed_out;
            entry.finished = true;
        }
        None => segment.op_events.push(JournalOpFacts {
            instance_id: instance_id.to_string(),
            attempt,
            op: cap_chars(op, 120),
            kind: None,
            label: None,
            returncode,
            duration_seconds,
            timed_out,
            finished: true,
        }),
    }
}

fn apply_journal_event(segment: &mut JournalSegmentFacts, row: &JsonValue) {
    let event = event_string(row, "event").unwrap_or_default();
    match event.as_str() {
        "phase_started" => {
            segment.phase_started_run_id = event_string(row, "run_id");
            segment.phase_started_plan_digest =
                event_string(row, "plan_digest");
            let (pid, identity) = journal_runner(row);
            segment.runner_pid = pid;
            segment.runner_identity = identity;
        }
        "phase_skipped" => {
            segment.skipped_reason = event_string(row, "reason")
                .map(|reason| cap_chars(&reason, 64));
        }
        "phase_finished" => {
            segment.finished_status = event_string(row, "status");
            segment.finished_cycles = row
                .get("cycles")
                .and_then(JsonValue::as_u64)
                .map(|cycles| cycles.min(u32::MAX as u64) as u32);
        }
        "observability_truncated" => {
            segment.truncated = true;
        }
        "cycle_started" => {
            if let Some(cycle) = row.get("cycle").and_then(JsonValue::as_u64) {
                segment.cycles.insert(cycle.min(u32::MAX as u64) as u32);
            }
        }
        "declaration_finished" => {
            segment.declaration_statuses.push(RunViewDeclarationWire {
                t: event_time(row),
                status: cap_chars(
                    &event_string(row, "status").unwrap_or_default(),
                    32,
                ),
                code: event_string(row, "code")
                    .map(|code| cap_chars(&code, 120)),
                first_line: None,
                payload_count: None,
            });
        }
        "recovery_turn_finished" => {
            segment.recovery = Some(RunViewRecoveryTurnWire {
                ok: row.get("ok").and_then(JsonValue::as_bool),
                code: event_string(row, "code")
                    .map(|code| cap_chars(&code, 120)),
            });
        }
        "recovery_turn_started" if segment.recovery.is_none() => {
            segment.recovery = Some(RunViewRecoveryTurnWire {
                ok: None,
                code: None,
            });
        }
        "recovery_turn_started" => {}
        "instance_started" => {
            if let Some(instance_id) = event_string(row, "instance_id") {
                segment.active_instance_id = Some(instance_id);
                segment.active_attempt = None;
                segment.active_max_attempts = None;
                segment.active_op = None;
            }
        }
        "instance_finished" => {
            if let Some(instance_id) = event_string(row, "instance_id") {
                let status = event_string(row, "status").unwrap_or_default();
                if !status.is_empty() {
                    segment
                        .finished_instances
                        .insert(instance_id.clone(), status);
                }
                if segment.active_instance_id.as_deref()
                    == Some(instance_id.as_str())
                {
                    segment.active_instance_id = None;
                    segment.active_attempt = None;
                    segment.active_max_attempts = None;
                    segment.active_op = None;
                }
            }
        }
        "attempt_started" => {
            segment.active_attempt = row.get("attempt").and_then(capped_u32);
            segment.active_max_attempts =
                row.get("max_attempts").and_then(capped_u32);
            if let Some(attempt) = row.get("attempt").and_then(capped_u32) {
                let instance_id = event_string(row, "instance_id")
                    .or_else(|| segment.active_instance_id.clone())
                    .unwrap_or_default();
                if !instance_id.is_empty() {
                    record_attempt_started(
                        segment,
                        &instance_id,
                        attempt,
                        row.get("max_attempts").and_then(capped_u32),
                        event_time(row),
                    );
                }
            }
        }
        "attempt_finished" => {
            let instance_id = event_string(row, "instance_id")
                .or_else(|| segment.active_instance_id.clone())
                .unwrap_or_default();
            if !instance_id.is_empty() {
                record_attempt_finished(
                    segment,
                    &instance_id,
                    row.get("attempt").and_then(capped_u32),
                    event_string(row, "status"),
                    event_string(row, "code"),
                    event_time(row),
                );
            }
        }
        "op_started" => {
            let label =
                event_string(row, "label").or_else(|| event_string(row, "op"));
            if let Some(label) = label.filter(|label| !label.is_empty()) {
                segment.active_op = Some(cap_chars(&label, 120));
            }
            let op = event_string(row, "op").unwrap_or_default();
            if !op.is_empty() {
                let instance_id = event_string(row, "instance_id")
                    .or_else(|| segment.active_instance_id.clone())
                    .unwrap_or_default();
                if !instance_id.is_empty() {
                    record_op_started(
                        segment,
                        &instance_id,
                        row.get("attempt").and_then(capped_u32),
                        &op,
                        event_string(row, "kind"),
                        event_string(row, "label"),
                    );
                }
            }
        }
        "op_finished" => {
            let op = event_string(row, "op").unwrap_or_default();
            if !op.is_empty() {
                let instance_id = event_string(row, "instance_id")
                    .or_else(|| segment.active_instance_id.clone())
                    .unwrap_or_default();
                if !instance_id.is_empty() {
                    record_op_finished(
                        segment,
                        &instance_id,
                        row.get("attempt").and_then(capped_u32),
                        &op,
                        row.get("returncode").and_then(JsonValue::as_i64),
                        row.get("duration_seconds")
                            .and_then(JsonValue::as_f64)
                            .filter(|duration| {
                                duration.is_finite() && *duration >= 0.0
                            }),
                        row.get("timed_out")
                            .and_then(JsonValue::as_bool)
                            .unwrap_or(false),
                    );
                }
            }
        }
        _ => {}
    }
}

/// Decode the C1 progress journal. Readers use the last segment and
/// report how many earlier segments there were.
pub(crate) fn decode_journal(text: &str) -> JournalFacts {
    let rows = parse_tolerant_jsonl(text);
    let mut facts = JournalFacts::default();
    let mut current = JournalSegmentFacts::default();
    let mut has_current = false;
    for row in &rows {
        if event_string(row, "event").as_deref() == Some("phase_started")
            && has_current
        {
            facts.segments.push(std::mem::take(&mut current));
        }
        has_current = true;
        apply_journal_event(&mut current, row);
    }
    if has_current {
        facts.segments.push(current);
    }
    facts.earlier_segments = facts.segments.len().saturating_sub(1) as u32;
    facts
}

/// One budgeted try of a result instance, with its diagnostic code.
pub(crate) struct ResultAttemptFacts {
    pub attempt: u32,
    pub status: FinalizerInstanceStatusWire,
    pub code: Option<String>,
}

/// One result instance outcome from the schema-v1 result artifact.
pub(crate) struct ResultInstanceFacts {
    pub instance_id: String,
    pub status: FinalizerInstanceStatusWire,
    pub attempts: Vec<ResultAttemptFacts>,
    pub evidence: Vec<(String, String)>,
    pub diagnostics: Vec<FinalizerDiagnosticWire>,
    pub refusal_reason: Option<String>,
    pub deferral: Option<(String, Vec<String>)>,
}

pub(crate) struct ResultFacts {
    pub status: String,
    pub cycles: u32,
    pub instances: Vec<ResultInstanceFacts>,
    pub diagnostics: Vec<FinalizerDiagnosticWire>,
}

fn parse_result_status(value: &str) -> Option<String> {
    match value {
        "success" | "failed" | "refused" | "deferred" | "pending" => {
            Some(value.to_string())
        }
        _ => None,
    }
}

fn parse_result_instance_status(
    value: &JsonValue,
) -> Option<FinalizerInstanceStatusWire> {
    serde_json::from_value::<FinalizerInstanceStatusWire>(value.clone()).ok()
}

fn parse_result_diagnostic(
    value: &JsonValue,
) -> Option<FinalizerDiagnosticWire> {
    let code = value.get("code")?.as_str()?;
    if code.is_empty() {
        return None;
    }
    let message = value
        .get("message")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let severity = value
        .get("severity")
        .and_then(|severity| {
            serde_json::from_value::<FinalizerDiagnosticSeverityWire>(
                severity.clone(),
            )
            .ok()
        })
        .unwrap_or(FinalizerDiagnosticSeverityWire::Info);
    Some(FinalizerDiagnosticWire {
        code: cap_chars(code, 120),
        message: cap_output(message),
        severity,
        instance_id: value
            .get("instance_id")
            .and_then(JsonValue::as_str)
            .map(str::to_string),
        attempt: value
            .get("attempt")
            .and_then(JsonValue::as_u64)
            .map(|attempt| attempt.min(u32::MAX as u64) as u32),
    })
}

fn capped_u32(value: &JsonValue) -> Option<u32> {
    value
        .as_u64()
        .map(|number| number.min(u32::MAX as u64) as u32)
}

/// Attempt rows for one result instance, tolerant of missing rows.
fn parse_result_attempts(item: &JsonValue) -> Vec<ResultAttemptFacts> {
    let mut attempts = Vec::new();
    let rows = match item.get("attempts").and_then(JsonValue::as_array) {
        Some(rows) => rows,
        None => return attempts,
    };
    for row in rows.iter().take(32) {
        let attempt = match row.get("attempt").and_then(capped_u32) {
            Some(attempt) => attempt,
            None => continue,
        };
        let status =
            match row.get("status").and_then(parse_result_instance_status) {
                Some(status) => status,
                None => continue,
            };
        attempts.push(ResultAttemptFacts {
            attempt,
            status,
            code: row
                .get("diagnostic_code")
                .and_then(JsonValue::as_str)
                .filter(|code| !code.is_empty())
                .map(|code| cap_chars(code, 120)),
        });
    }
    attempts
}

/// Typed evidence pairs for one result instance, capped.
fn parse_result_evidence(item: &JsonValue) -> Vec<(String, String)> {
    let mut evidence = Vec::new();
    let rows = match item.get("evidence").and_then(JsonValue::as_array) {
        Some(rows) => rows,
        None => return evidence,
    };
    for row in rows.iter().take(128) {
        let kind = row
            .get("kind")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        let value = row
            .get("value")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        if kind.is_empty() || value.is_empty() {
            continue;
        }
        evidence.push((cap_chars(kind, 120), cap_output(value)));
    }
    evidence
}

/// Instance-scoped diagnostics for one result instance.
fn parse_result_diagnostics(item: &JsonValue) -> Vec<FinalizerDiagnosticWire> {
    let mut diagnostics = Vec::new();
    let rows = match item.get("diagnostics").and_then(JsonValue::as_array) {
        Some(rows) => rows,
        None => return diagnostics,
    };
    for row in rows.iter().take(64) {
        if let Some(diagnostic) = parse_result_diagnostic(row) {
            diagnostics.push(diagnostic);
        }
    }
    diagnostics
}

/// The typed deferral payload for one result instance, if present.
fn parse_result_deferral(item: &JsonValue) -> Option<(String, Vec<String>)> {
    let deferral = item.get("deferral")?.as_object()?;
    let reason = deferral.get("reason").and_then(JsonValue::as_str)?;
    if reason.is_empty() {
        return None;
    }
    let paths = deferral
        .get("paths")
        .and_then(JsonValue::as_array)
        .map(|paths| {
            paths
                .iter()
                .filter_map(|path| path.as_str())
                .filter(|path| !path.is_empty())
                .take(64)
                .map(|path| cap_chars(path, 256))
                .collect()
        })
        .unwrap_or_default();
    Some((cap_chars(reason, 120), paths))
}

/// The result artifact has its own tolerant schema-v1 decoder: it is never
/// parsed with the strict aggregate wire.
pub(crate) fn decode_result(text: &str) -> Option<ResultFacts> {
    let payload: JsonValue = serde_json::from_str(text).ok()?;
    let object = payload.as_object()?;
    if object.get("schema_version").and_then(JsonValue::as_u64) != Some(1) {
        return None;
    }
    let status = object
        .get("status")
        .and_then(JsonValue::as_str)
        .and_then(parse_result_status)?;
    let mut instances = Vec::new();
    if let Some(items) = object.get("instances").and_then(JsonValue::as_array) {
        for item in items.iter().take(128) {
            let instance_id = item
                .get("instance_id")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            if instance_id.is_empty() {
                continue;
            }
            let status_value = match item.get("status") {
                Some(status_value) => status_value,
                None => continue,
            };
            let instance_status =
                match parse_result_instance_status(status_value) {
                    Some(status) => status,
                    None => continue,
                };
            instances.push(ResultInstanceFacts {
                instance_id: instance_id.to_string(),
                status: instance_status,
                attempts: parse_result_attempts(item),
                evidence: parse_result_evidence(item),
                diagnostics: parse_result_diagnostics(item),
                refusal_reason: item
                    .get("refusal_reason")
                    .and_then(JsonValue::as_str)
                    .filter(|reason| !reason.is_empty())
                    .map(|reason| cap_chars(reason, 120)),
                deferral: parse_result_deferral(item),
            });
        }
    }
    let mut diagnostics = Vec::new();
    if let Some(items) = object.get("diagnostics").and_then(JsonValue::as_array)
    {
        for item in items.iter().take(64) {
            if let Some(diagnostic) = parse_result_diagnostic(item) {
                diagnostics.push(diagnostic);
            }
        }
    }
    Some(ResultFacts {
        status,
        cycles: object
            .get("cycles")
            .and_then(JsonValue::as_u64)
            .map(|cycles| cycles.min(u32::MAX as u64) as u32)
            .unwrap_or(0),
        instances,
        diagnostics,
    })
}

/// Drift and plan-seal facts from `agent_meta`. Only `finalizers_drift`
/// and the plan-seal `finalizers` block are read; the summary is a row
/// hint, never detail truth.
pub(crate) struct MetaFacts {
    pub drift: Vec<RunViewDriftWire>,
}

pub(crate) fn decode_meta(text: &str) -> MetaFacts {
    let mut facts = MetaFacts { drift: Vec::new() };
    let payload: JsonValue = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return facts,
    };
    if let Some(entries) = payload
        .get("finalizers_drift")
        .and_then(JsonValue::as_array)
    {
        for entry in entries.iter().take(64) {
            match entry {
                JsonValue::String(message) if !message.is_empty() => {
                    facts.drift.push(RunViewDriftWire {
                        instance_id: None,
                        code: None,
                        message: cap_output(message),
                    });
                }
                JsonValue::String(_) => {}
                JsonValue::Object(_) => {
                    let message = entry
                        .get("message")
                        .and_then(JsonValue::as_str)
                        .unwrap_or_default();
                    if message.is_empty() {
                        continue;
                    }
                    facts.drift.push(RunViewDriftWire {
                        instance_id: entry
                            .get("instance_id")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        code: entry
                            .get("code")
                            .and_then(JsonValue::as_str)
                            .map(|code| cap_chars(code, 120)),
                        message: cap_output(message),
                    });
                }
                _ => {}
            }
        }
    }
    facts
}

/// Map a result/journal status word onto the C5 instance vocabulary.
pub(crate) fn map_terminal_instance_status(
    status: &FinalizerInstanceStatusWire,
) -> &'static str {
    match status {
        FinalizerInstanceStatusWire::Success => "success",
        FinalizerInstanceStatusWire::Failed => "failed",
        FinalizerInstanceStatusWire::Refused => "refused",
        FinalizerInstanceStatusWire::Deferred => "deferred",
        FinalizerInstanceStatusWire::Pending => "planned",
        FinalizerInstanceStatusWire::Skipped => "skipped",
    }
}

/// Map a journal `instance_finished` status word onto the vocabulary.
/// Unknown words degrade to `planned` rather than failing the run.
pub(crate) fn map_journal_instance_status(status: &str) -> &'static str {
    match status {
        "success" => "success",
        "failed" => "failed",
        "refused" => "refused",
        "deferred" => "deferred",
        "skipped" => "skipped",
        "not_triggered" => "not_triggered",
        _ => "planned",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tolerant_jsonl_skips_blank_malformed_and_new_versions() {
        let text = concat!(
            "\n",
            "{\"v\": 1, \"event\": \"cycle_started\", \"cycle\": 1}\n",
            "not json\n",
            "{\"v\": 99, \"event\": \"cycle_started\", \"cycle\": 2}\n",
            "{\"event\": \"cycle_started\", \"cycle\": 3}\n",
        );
        let rows = parse_tolerant_jsonl(text);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn oversized_inputs_report_their_raw_path() {
        let input = RunViewTextInputWire {
            text: Some("{}".to_string()),
            size: RUN_VIEW_MAX_BYTES as u64 + 1,
            too_large: false,
        };
        assert!(matches!(
            text_facts(&input, PATH_RESULT),
            TextFacts::TooLarge(PATH_RESULT)
        ));
    }

    #[test]
    fn context_decode_failure_means_unknown_triggers() {
        let facts = decode_context("{oops");
        assert!(facts.requirements.is_empty());
        assert!(facts.plan_digest.is_none());
    }
}
