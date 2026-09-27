//! Per-instance detail content: attempts, operations, and diagnostics.
//!
//! Everything here is content only and never changes a status (rule 6).
//! All decoders are tolerant: a malformed record, steps line, or tail
//! degrades to an empty list or a missing field, never an error.

use serde_json::Value as JsonValue;

use super::super::wire::{
    FinalizerDiagnosticSeverityWire, FinalizerDiagnosticWire,
};
use super::decode::{
    cap_chars, cap_output, map_journal_instance_status,
    map_terminal_instance_status, JournalAttemptFacts, JournalOpFacts,
    ResultInstanceFacts,
};
use super::evidence::{select_headline, typed_evidence};
use super::wire::{
    RunViewAttemptWire, RunViewDeferralWire, RunViewFileInputWire,
    RunViewInstanceDiagnosticWire, RunViewInstanceInputWire, RunViewLogWire,
    RunViewOperationWire, RunViewStepWire, RUN_VIEW_TEXT_CAP_CHARS,
};

/// Step files are read up to this many bytes; writers stop at the same
/// ceiling, so a larger file means a non-compliant writer.
pub(crate) const STEPS_MAX_BYTES: usize = 64 * 1024;

/// Operation-record texts beyond this size are ignored outright.
const OP_RECORD_MAX_BYTES: usize = 1024 * 1024;

/// Protocol-envelope filename keywords. Generic on purpose: no
/// plugin-specific field ever reaches the response.
const PROTOCOL_KEYWORDS: [&str; 8] = [
    "describe", "validate", "execute", "verify", "protocol", "envelope",
    "prompt", "response",
];

/// Recorded file suffixes that already render elsewhere and never count
/// as protocol envelopes.
fn is_covered_suffix(name: &str) -> bool {
    name.ends_with(".outcome.json")
        || name.ends_with(".steps.jsonl")
        || name.ends_with(".live")
        || name.ends_with(".diagnostics.json")
}

/// One parsed C2 operation record: schema-v1, or a legacy commit record
/// with no `schema_version`.
struct OperationRecordFacts {
    op: String,
    kind: Option<String>,
    label: Option<String>,
    attempt: Option<u32>,
    argv: Vec<String>,
    started_at: Option<f64>,
    duration_seconds: Option<f64>,
    returncode: Option<i64>,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    log_stdout: Option<String>,
    log_stderr: Option<String>,
    log_live: Option<String>,
    steps_file: Option<String>,
}

/// Split an outcome-record filename into its attempt and op stem:
/// `attempt-3.stitch-main.outcome.json` gives `(Some(3), "stitch-main")`,
/// `preflight.describe.outcome.json` gives `(None, "describe")`.
fn split_outcome_name(name: &str) -> Option<(Option<u32>, String)> {
    let stem = name.strip_suffix(".outcome.json")?;
    if stem.is_empty() {
        return None;
    }
    if let Some(preflight) = stem.strip_prefix("preflight.") {
        if preflight.is_empty() {
            return None;
        }
        return Some((None, preflight.to_string()));
    }
    match stem.split_once('.') {
        Some((head, tail)) if !tail.is_empty() => {
            let attempt = head.strip_prefix("attempt-")?.parse::<u32>().ok()?;
            Some((Some(attempt), tail.to_string()))
        }
        _ => {
            if stem.starts_with("attempt-") {
                return None;
            }
            Some((None, stem.to_string()))
        }
    }
}

fn capped_u32(value: &JsonValue) -> Option<u32> {
    value
        .as_u64()
        .map(|number| number.min(u32::MAX as u64) as u32)
}

fn capped_i64(value: &JsonValue) -> Option<i64> {
    value.as_i64()
}

fn capped_f64(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .filter(|number| number.is_finite() && *number >= 0.0)
}

fn capped_bool(value: &JsonValue) -> bool {
    value.as_bool().unwrap_or(false)
}

fn capped_name(value: &JsonValue, max_chars: usize) -> Option<String> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .map(|text| cap_chars(text, max_chars))
}

fn decode_operation_record(
    text: &str,
    fallback_op: &str,
    fallback_attempt: Option<u32>,
) -> Option<OperationRecordFacts> {
    if text.len() > OP_RECORD_MAX_BYTES {
        return None;
    }
    let payload: JsonValue = serde_json::from_str(text).ok()?;
    let object = payload.as_object()?;
    if object.get("schema_version").and_then(JsonValue::as_u64) == Some(1) {
        decode_schema_v1_record(object, fallback_op, fallback_attempt)
    } else if object.contains_key("returncode")
        || object.contains_key("duration_seconds")
        || object.contains_key("argv")
    {
        decode_legacy_commit_record(object, fallback_op, fallback_attempt)
    } else {
        None
    }
}

fn decode_schema_v1_record(
    object: &serde_json::Map<String, JsonValue>,
    fallback_op: &str,
    fallback_attempt: Option<u32>,
) -> Option<OperationRecordFacts> {
    let logs = object.get("logs").and_then(JsonValue::as_object);
    Some(OperationRecordFacts {
        op: object
            .get("op")
            .and_then(|op| capped_name(op, 120))
            .unwrap_or_else(|| cap_chars(fallback_op, 120)),
        kind: object.get("kind").and_then(|kind| capped_name(kind, 32)),
        label: object
            .get("label")
            .and_then(|label| capped_name(label, 120)),
        attempt: object
            .get("attempt")
            .and_then(capped_u32)
            .or(fallback_attempt),
        argv: object
            .get("argv")
            .and_then(JsonValue::as_array)
            .map(|argv| {
                argv.iter()
                    .filter_map(|item| item.as_str())
                    .take(64)
                    .map(|item| cap_chars(item, RUN_VIEW_TEXT_CAP_CHARS))
                    .collect()
            })
            .unwrap_or_default(),
        started_at: object.get("started_at").and_then(capped_f64),
        duration_seconds: object.get("duration_seconds").and_then(capped_f64),
        returncode: object.get("returncode").and_then(capped_i64),
        timed_out: object.get("timed_out").map(capped_bool).unwrap_or(false),
        stdout_truncated: object
            .get("stdout_truncated")
            .map(capped_bool)
            .unwrap_or(false),
        stderr_truncated: object
            .get("stderr_truncated")
            .map(capped_bool)
            .unwrap_or(false),
        log_stdout: logs
            .and_then(|logs| logs.get("stdout"))
            .and_then(|name| capped_name(name, 256)),
        log_stderr: logs
            .and_then(|logs| logs.get("stderr"))
            .and_then(|name| capped_name(name, 256)),
        log_live: logs
            .and_then(|logs| logs.get("live"))
            .and_then(|name| capped_name(name, 256)),
        steps_file: object
            .get("steps")
            .and_then(|steps| capped_name(steps, 256)),
    })
}

/// Legacy commit records carry no `schema_version`, `op`, `kind`, or
/// `label`: the op stem comes from the filename and the kind is always a
/// subprocess stitch.
fn decode_legacy_commit_record(
    object: &serde_json::Map<String, JsonValue>,
    fallback_op: &str,
    fallback_attempt: Option<u32>,
) -> Option<OperationRecordFacts> {
    Some(OperationRecordFacts {
        op: cap_chars(fallback_op, 120),
        kind: Some("subprocess".to_string()),
        label: None,
        attempt: fallback_attempt,
        argv: object
            .get("argv")
            .and_then(JsonValue::as_array)
            .map(|argv| {
                argv.iter()
                    .filter_map(|item| item.as_str())
                    .take(64)
                    .map(|item| cap_chars(item, RUN_VIEW_TEXT_CAP_CHARS))
                    .collect()
            })
            .unwrap_or_default(),
        started_at: None,
        duration_seconds: object.get("duration_seconds").and_then(capped_f64),
        returncode: object.get("returncode").and_then(capped_i64),
        timed_out: object.get("timed_out").map(capped_bool).unwrap_or(false),
        stdout_truncated: object
            .get("stdout_truncated")
            .map(capped_bool)
            .unwrap_or(false),
        stderr_truncated: object
            .get("stderr_truncated")
            .map(capped_bool)
            .unwrap_or(false),
        log_stdout: None,
        log_stderr: None,
        log_live: None,
        steps_file: None,
    })
}

/// Decode a C3 steps file: tolerant JSONL capped at 64 KiB. Rows with an
/// empty or missing step are skipped; the flag reports a non-compliant
/// oversized file.
fn decode_steps(text: &str) -> (Vec<RunViewStepWire>, bool) {
    let oversized = text.len() > STEPS_MAX_BYTES;
    let mut window = text;
    if oversized {
        let mut end = STEPS_MAX_BYTES;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        window = &text[..end];
        if let Some(newline) = window.rfind('\n') {
            window = &window[..newline];
        } else {
            window = "";
        }
    }
    let mut steps = Vec::new();
    for line in window.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: JsonValue = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let step = value
            .get("step")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        if step.is_empty() {
            continue;
        }
        let state = value
            .get("state")
            .and_then(JsonValue::as_str)
            .filter(|state| !state.is_empty())
            .unwrap_or("start");
        steps.push(RunViewStepWire {
            step: cap_chars(step, 120),
            state: cap_chars(state, 16),
            t: value.get("t").and_then(capped_f64),
            detail: value
                .get("detail")
                .and_then(JsonValue::as_str)
                .filter(|detail| !detail.is_empty())
                .map(|detail| cap_chars(detail, RUN_VIEW_TEXT_CAP_CHARS)),
        });
    }
    (steps, oversized)
}

/// Collapse `\r`-separated progress segments to the last segment of each
/// line, then keep the last `tail_lines` lines. ANSI is preserved for the
/// Python renderer.
pub(crate) fn collapse_cr_tail(tail: &str, tail_lines: u32) -> Vec<String> {
    if tail_lines == 0 {
        return Vec::new();
    }
    // Bound the scan so a full-text fallback never walks megabytes.
    let bounded = if tail.len() > STEPS_MAX_BYTES {
        let mut end = STEPS_MAX_BYTES;
        while end > 0 && !tail.is_char_boundary(end) {
            end -= 1;
        }
        &tail[tail.len() - end..]
    } else {
        tail
    };
    let mut lines: Vec<String> = bounded
        .split('\n')
        .map(|line| {
            let stripped = line.strip_suffix('\r').unwrap_or(line);
            let collapsed = stripped.rsplit('\r').next().unwrap_or("");
            cap_chars(collapsed, RUN_VIEW_TEXT_CAP_CHARS)
        })
        .collect();
    if lines.last().is_some_and(|last| last.is_empty()) {
        lines.pop();
    }
    let keep = tail_lines as usize;
    if lines.len() > keep {
        lines.drain(..lines.len() - keep);
    }
    lines
}

/// Last non-blank collapsed line, for the failure-reason fallback.
fn last_nonblank_line(tail: &str) -> Option<String> {
    collapse_cr_tail(tail, u32::MAX)
        .into_iter()
        .rev()
        .find(|line| !line.trim().is_empty())
}

fn file_text(file: &RunViewFileInputWire) -> Option<&str> {
    file.text.as_ref().and_then(|text| text.text.as_deref())
}

fn file_tail(file: &RunViewFileInputWire) -> Option<&str> {
    file.tail.as_deref().or_else(|| file_text(file))
}

fn attempt_prefix(attempt: u32, op: &str) -> String {
    format!("attempt-{attempt}.{op}")
}

/// Conventional steps names for an op stem: explicit preflight prefix,
/// attempt prefix, then the bare stem.
fn steps_candidates(attempt: Option<u32>, op: &str) -> Vec<String> {
    let mut names = Vec::with_capacity(3);
    match attempt {
        Some(attempt) => {
            names.push(format!("{}.steps.jsonl", attempt_prefix(attempt, op)));
            names.push(format!("{op}.steps.jsonl"));
        }
        None => {
            names.push(format!("preflight.{op}.steps.jsonl"));
            names.push(format!("{op}.steps.jsonl"));
        }
    }
    names
}

/// Conventional live names for an op stem.
fn live_candidates(attempt: Option<u32>, op: &str) -> Vec<String> {
    let mut names = Vec::with_capacity(3);
    match attempt {
        Some(attempt) => {
            names.push(format!("{}.live", attempt_prefix(attempt, op)));
            names.push(format!("{op}.live"));
        }
        None => {
            names.push(format!("preflight.{op}.live"));
            names.push(format!("{op}.live"));
        }
    }
    names
}

/// Conventional log names: the record's op stem first, then the legacy
/// `attempt-N.stdout`/`stderr` command names.
fn log_candidates(attempt: Option<u32>, op: &str, kind: &str) -> Vec<String> {
    let mut names = Vec::new();
    match attempt {
        Some(attempt) => {
            names.push(format!("{}.{kind}", attempt_prefix(attempt, op)));
            names.push(format!("attempt-{attempt}.{kind}"));
        }
        None => {
            names.push(format!("preflight.{op}.{kind}"));
            names.push(format!("{op}.{kind}"));
        }
    }
    names
}

fn find_file<'a>(
    by_name: &std::collections::BTreeMap<&str, &'a RunViewFileInputWire>,
    name: &str,
) -> Option<&'a RunViewFileInputWire> {
    by_name.get(name).copied()
}

fn push_log(
    logs: &mut Vec<RunViewLogWire>,
    kind: &str,
    file: &RunViewFileInputWire,
    truncated: Option<bool>,
) {
    logs.push(RunViewLogWire {
        kind: kind.to_string(),
        name: file.name.clone(),
        size: Some(file.size),
        line_count: file.line_count,
        truncated,
    });
}

/// Build one operation's logs, steps, and live tail from the instance
/// files, preferring the record's explicit references and falling back
/// to filename conventions.
fn attach_operation_content(
    operation: &mut RunViewOperationWire,
    record: Option<&OperationRecordFacts>,
    by_name: &std::collections::BTreeMap<&str, &RunViewFileInputWire>,
    tail_lines: u32,
) {
    let mut seen_stdout = false;
    let mut seen_stderr = false;
    if let Some(record) = record {
        for (slot, kind, truncated) in [
            (&record.log_stdout, "stdout", Some(record.stdout_truncated)),
            (&record.log_stderr, "stderr", Some(record.stderr_truncated)),
            (&record.log_live, "live", None),
        ] {
            if let Some(name) = slot {
                if let Some(file) = find_file(by_name, name) {
                    push_log(&mut operation.logs, kind, file, truncated);
                    seen_stdout |= kind == "stdout";
                    seen_stderr |= kind == "stderr";
                }
            }
        }
    }
    for (kind, seen) in
        [("stdout", &mut seen_stdout), ("stderr", &mut seen_stderr)]
    {
        if *seen {
            continue;
        }
        for name in log_candidates(operation.attempt, &operation.op, kind) {
            if let Some(file) = find_file(by_name, &name) {
                push_log(&mut operation.logs, kind, file, None);
                *seen = true;
                break;
            }
        }
    }
    let steps_name = record
        .and_then(|record| record.steps_file.clone())
        .or_else(|| {
            steps_candidates(operation.attempt, &operation.op)
                .into_iter()
                .find(|name| by_name.contains_key(name.as_str()))
        });
    if let Some(name) = steps_name {
        if let Some(file) = find_file(by_name, &name) {
            if let Some(text) = file_text(file) {
                let (steps, truncated) = decode_steps(text);
                operation.steps = steps;
                operation.steps_truncated = truncated;
            }
        }
    }
    let live_name =
        record
            .and_then(|record| record.log_live.clone())
            .or_else(|| {
                live_candidates(operation.attempt, &operation.op)
                    .into_iter()
                    .find(|name| by_name.contains_key(name.as_str()))
            });
    if let Some(name) = live_name {
        if let Some(file) = find_file(by_name, &name) {
            let has_live_log =
                operation.logs.iter().any(|log| log.kind == "live");
            if !has_live_log {
                push_log(&mut operation.logs, "live", file, None);
            }
            if let Some(tail) = file_tail(file) {
                operation.live_tail = collapse_cr_tail(tail, tail_lines);
            }
        }
    }
}

/// Build operations from C2 records plus journal op events. Journal-only
/// entries render from event facts; record entries win on conflicts.
pub(crate) fn build_operations(
    files: &[RunViewFileInputWire],
    journal_ops: &[JournalOpFacts],
    tail_lines: u32,
) -> Vec<RunViewOperationWire> {
    let by_name: std::collections::BTreeMap<&str, &RunViewFileInputWire> =
        files
            .iter()
            .map(|file| (file.name.as_str(), file))
            .collect();
    let mut records: Vec<(Option<u32>, String, OperationRecordFacts)> =
        Vec::new();
    for file in files {
        let Some((attempt, stem)) = split_outcome_name(&file.name) else {
            continue;
        };
        let Some(text) = file_text(file) else {
            continue;
        };
        if let Some(record) = decode_operation_record(text, &stem, attempt) {
            records.push((record.attempt, record.op.clone(), record));
        }
    }
    let mut operations: Vec<RunViewOperationWire> = records
        .iter()
        .map(|(attempt, _, record)| RunViewOperationWire {
            op: record.op.clone(),
            kind: record.kind.clone(),
            label: record.label.clone(),
            attempt: *attempt,
            argv: record.argv.clone(),
            started_at: record.started_at,
            duration_seconds: record.duration_seconds,
            returncode: record.returncode,
            timed_out: record.timed_out,
            stdout_truncated: record.stdout_truncated,
            stderr_truncated: record.stderr_truncated,
            logs: Vec::new(),
            steps: Vec::new(),
            steps_truncated: false,
            live_tail: Vec::new(),
        })
        .collect();
    for event in journal_ops {
        let covered = operations.iter().any(|operation| {
            operation.attempt == event.attempt && operation.op == event.op
        });
        if covered {
            continue;
        }
        operations.push(RunViewOperationWire {
            op: event.op.clone(),
            kind: event.kind.clone(),
            label: event.label.clone(),
            attempt: event.attempt,
            argv: Vec::new(),
            started_at: None,
            duration_seconds: event.duration_seconds,
            returncode: event.returncode,
            timed_out: event.timed_out,
            stdout_truncated: false,
            stderr_truncated: false,
            logs: Vec::new(),
            steps: Vec::new(),
            steps_truncated: false,
            live_tail: Vec::new(),
        });
    }
    // Fill record-backed gaps from finished journal events: a record file
    // written before the op finished lacks the terminal facts.
    for operation in operations.iter_mut() {
        let event = journal_ops.iter().rev().find(|event| {
            event.attempt == operation.attempt && event.op == operation.op
        });
        if let Some(event) = event {
            if operation.kind.is_none() {
                operation.kind = event.kind.clone();
            }
            if operation.label.is_none() {
                operation.label = event.label.clone();
            }
            if operation.returncode.is_none() {
                operation.returncode = event.returncode;
            }
            if operation.duration_seconds.is_none() {
                operation.duration_seconds = event.duration_seconds;
            }
            operation.timed_out |= event.timed_out;
        }
    }
    for operation in operations.iter_mut() {
        let record = records.iter().find_map(|(attempt, op, record)| {
            if *attempt == operation.attempt && *op == operation.op {
                Some(record)
            } else {
                None
            }
        });
        attach_operation_content(operation, record, &by_name, tail_lines);
    }
    operations.sort_by(|left, right| {
        (left.attempt.is_some(), left.attempt, &left.op).cmp(&(
            right.attempt.is_some(),
            right.attempt,
            &right.op,
        ))
    });
    operations
}

/// Build attempts from result attempts plus journal attempt events. The
/// result is authoritative for status; the journal supplies timing.
pub(crate) fn build_attempts(
    result: Option<&ResultInstanceFacts>,
    journal: &[JournalAttemptFacts],
) -> Vec<RunViewAttemptWire> {
    let mut numbers: Vec<u32> = Vec::new();
    if let Some(result) = result {
        for attempt in &result.attempts {
            if !numbers.contains(&attempt.attempt) {
                numbers.push(attempt.attempt);
            }
        }
    }
    for event in journal {
        if !numbers.contains(&event.attempt) {
            numbers.push(event.attempt);
        }
    }
    numbers.sort_unstable();
    numbers
        .into_iter()
        .map(|attempt| {
            let from_result = result.and_then(|result| {
                result.attempts.iter().find(|row| row.attempt == attempt)
            });
            let from_journal =
                journal.iter().find(|event| event.attempt == attempt);
            let status = match (from_result, from_journal) {
                (Some(row), _) => {
                    map_terminal_instance_status(&row.status).to_string()
                }
                (None, Some(event)) => match &event.finished_status {
                    Some(finished) => {
                        map_journal_instance_status(finished).to_string()
                    }
                    None => "running".to_string(),
                },
                (None, None) => "planned".to_string(),
            };
            let code = from_result
                .and_then(|row| row.code.clone())
                .or_else(|| from_journal.and_then(|event| event.code.clone()));
            let (started_at, duration_seconds) = match from_journal {
                Some(event) => (
                    event.started_t,
                    match (event.started_t, event.finished_t) {
                        (Some(started), Some(finished))
                            if finished >= started =>
                        {
                            Some(finished - started)
                        }
                        _ => None,
                    },
                ),
                None => (None, None),
            };
            RunViewAttemptWire {
                attempt,
                status,
                started_at,
                duration_seconds,
                code,
            }
        })
        .collect()
}

/// Latest attempt number across result and journal facts.
pub(crate) fn latest_attempt(
    result: Option<&ResultInstanceFacts>,
    journal: &[JournalAttemptFacts],
) -> Option<u32> {
    result
        .map(|result| result.attempts.iter().map(|row| row.attempt).max())
        .unwrap_or(None)
        .into_iter()
        .chain(journal.iter().map(|event| event.attempt))
        .max()
}

fn diagnostic_severity(
    severity: &FinalizerDiagnosticSeverityWire,
) -> &'static str {
    match severity {
        FinalizerDiagnosticSeverityWire::Error => "error",
        FinalizerDiagnosticSeverityWire::Warning => "warning",
        FinalizerDiagnosticSeverityWire::Info => "info",
    }
}

/// Dedupe diagnostics by `(code, message)`, keeping the latest attempt's
/// entry, then downgrade errors from superseded attempts of an eventually
/// successful instance to `superseded`.
pub(crate) fn build_diagnostics(
    instance_diags: &[FinalizerDiagnosticWire],
    run_diags: &[FinalizerDiagnosticWire],
    terminal_success: bool,
    latest: Option<u32>,
) -> Vec<RunViewInstanceDiagnosticWire> {
    let mut order: Vec<(String, String)> = Vec::new();
    let mut best: std::collections::BTreeMap<
        (String, String),
        (u32, u32, RunViewInstanceDiagnosticWire),
    > = std::collections::BTreeMap::new();
    for (index, diagnostic) in instance_diags
        .iter()
        .chain(run_diags.iter())
        .take(256)
        .enumerate()
    {
        let key = (diagnostic.code.clone(), diagnostic.message.clone());
        let rank = diagnostic.attempt.unwrap_or(0);
        let entry = RunViewInstanceDiagnosticWire {
            code: diagnostic.code.clone(),
            message: diagnostic.message.clone(),
            severity: diagnostic_severity(&diagnostic.severity).to_string(),
            attempt: diagnostic.attempt,
        };
        let replace = match best.get(&key) {
            Some((best_rank, _, _)) => rank >= *best_rank,
            None => true,
        };
        if replace {
            best.insert(key.clone(), (rank, index as u32, entry));
        }
        if !order.contains(&key) {
            order.push(key);
        }
    }
    let mut diagnostics: Vec<RunViewInstanceDiagnosticWire> = order
        .into_iter()
        .filter_map(|key| best.remove(&key).map(|(_, _, entry)| entry))
        .take(64)
        .collect();
    if terminal_success {
        if let Some(latest) = latest {
            for diagnostic in diagnostics.iter_mut() {
                let superseded = diagnostic.severity == "error"
                    && diagnostic
                        .attempt
                        .is_some_and(|attempt| attempt < latest);
                if superseded {
                    diagnostic.severity = "superseded".to_string();
                }
            }
        }
    }
    diagnostics
}

/// Warn steps in the latest attempt's operations plus warning diagnostics
/// scoped to the latest attempt.
pub(crate) fn count_warnings(
    operations: &[RunViewOperationWire],
    diagnostics: &[RunViewInstanceDiagnosticWire],
    latest: Option<u32>,
) -> u32 {
    let Some(latest) = latest else {
        return 0;
    };
    let mut warnings = 0u32;
    for operation in operations {
        if operation.attempt != Some(latest) {
            continue;
        }
        warnings += operation
            .steps
            .iter()
            .filter(|step| step.state == "warn")
            .count()
            .min(u32::MAX as usize) as u32;
    }
    warnings += diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.severity == "warning"
                && diagnostic.attempt.is_none_or(|attempt| attempt == latest)
        })
        .count()
        .min(u32::MAX as usize) as u32;
    warnings
}

/// Stderr tails for the failure-reason fallback: `(attempt, tail)` with
/// the latest attempt first.
pub(crate) fn collect_stderr_tails(
    files: &[RunViewFileInputWire],
) -> Vec<(Option<u32>, String)> {
    let mut tails: Vec<(Option<u32>, String, String)> = Vec::new();
    for file in files {
        if !file.name.ends_with(".stderr") {
            continue;
        }
        let Some(tail) = file_tail(file) else {
            continue;
        };
        if tail.trim().is_empty() {
            continue;
        }
        let attempt = file
            .name
            .strip_prefix("attempt-")
            .and_then(|rest| rest.split('.').next())
            .and_then(|number| number.parse::<u32>().ok());
        tails.push((attempt, file.name.clone(), tail.to_string()));
    }
    tails.sort_by(|left, right| {
        (right.0.is_some(), right.0, &right.1).cmp(&(
            left.0.is_some(),
            left.0,
            &left.1,
        ))
    });
    tails
        .into_iter()
        .map(|(attempt, _, tail)| (attempt, tail))
        .collect()
}

/// The one failure reason line: the first error diagnostic, else the last
/// non-blank line of the latest stderr tail. Only failed instances get
/// one; refusals and deferrals carry their own typed reasons.
pub(crate) fn build_failure_reason(
    instance_status: &str,
    diagnostics: &[RunViewInstanceDiagnosticWire],
    stderr_tails: &[(Option<u32>, String)],
) -> Option<String> {
    if instance_status != "failed" {
        return None;
    }
    if let Some(diagnostic) = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.severity == "error")
    {
        if !diagnostic.message.trim().is_empty() {
            return Some(cap_output(&diagnostic.message));
        }
    }
    stderr_tails
        .iter()
        .find_map(|(_, tail)| last_nonblank_line(tail))
}

/// Protocol-envelope file names for plugin instances, generic by
/// construction: keyword filename matches, excluding records, steps,
/// live tails, and diagnostics files that render elsewhere.
pub(crate) fn build_protocol_files(
    files: &[RunViewFileInputWire],
) -> Vec<String> {
    let mut names: Vec<String> = files
        .iter()
        .map(|file| file.name.clone())
        .filter(|name| !is_covered_suffix(name))
        .filter(|name| {
            let lower = name.to_lowercase();
            PROTOCOL_KEYWORDS
                .iter()
                .any(|keyword| lower.contains(keyword))
        })
        .collect();
    names.sort();
    names.dedup();
    names.truncate(16);
    names
}

/// Typed evidence plus headline for one result instance.
pub(crate) fn build_evidence(
    result: Option<&ResultInstanceFacts>,
) -> (
    Vec<super::evidence::RunViewEvidenceWire>,
    Option<super::evidence::RunViewEvidenceWire>,
) {
    let pairs = result
        .map(|result| result.evidence.clone())
        .unwrap_or_default();
    let evidence = typed_evidence(&pairs);
    let headline = select_headline(&evidence);
    (evidence, headline)
}

/// Typed deferral payload for one result instance.
pub(crate) fn build_deferral(
    result: Option<&ResultInstanceFacts>,
) -> Option<RunViewDeferralWire> {
    let (reason, paths) = result?.deferral.clone()?;
    Some(RunViewDeferralWire { reason, paths })
}

/// Lookup helper: the result instance facts for one instance id.
pub(crate) fn result_instance<'a>(
    instances: &'a [ResultInstanceFacts],
    instance_id: &str,
) -> Option<&'a ResultInstanceFacts> {
    instances
        .iter()
        .find(|instance| instance.instance_id == instance_id)
}

/// Lookup helper: the collected files for one instance id.
pub(crate) fn instance_files<'a>(
    inputs: &'a [RunViewInstanceInputWire],
    instance_id: &str,
) -> &'a [RunViewFileInputWire] {
    inputs
        .iter()
        .find(|input| input.instance_id == instance_id)
        .map(|input| input.files.as_slice())
        .unwrap_or(&[])
}
