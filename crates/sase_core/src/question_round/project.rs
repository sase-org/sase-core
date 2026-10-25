//! Pure question-round projection.
//!
//! The host reads one gate bundle's `request.json`, `response.json`, and
//! `cancellation.json` and hands their parsed contents to
//! [`project_question_round`]; this module classifies the round status,
//! aligns answers to questions, derives the recommendation markers, and
//! classifies the answering actor. It performs no filesystem, subprocess,
//! clock, or notification I/O: bundle content arrives as raw JSON, and
//! malformed content degrades `data_state` instead of erroring.

use std::collections::VecDeque;

use serde_json::Value as JsonValue;

use super::wire::{
    QuestionAnswerWire, QuestionCancellationWire, QuestionEntryWire,
    QuestionOptionWire, QuestionPolicyWire, QuestionProvenanceWire,
    QuestionRoundError, QuestionRoundGateWire, QuestionRoundRequestWire,
    QuestionRoundWire, QUESTION_ROUND_ACTOR_AGENT,
    QUESTION_ROUND_ACTOR_AUTO_POLICY, QUESTION_ROUND_ACTOR_HUMAN,
    QUESTION_ROUND_ACTOR_UNKNOWN, QUESTION_ROUND_BASIS_NONE,
    QUESTION_ROUND_BASIS_OPTION_ORDER, QUESTION_ROUND_CODE_INVALID_REQUEST,
    QUESTION_ROUND_CODE_UNSUPPORTED_SCHEMA, QUESTION_ROUND_DATA_COMPLETE,
    QUESTION_ROUND_DATA_PARTIAL, QUESTION_ROUND_DATA_UNAVAILABLE,
    QUESTION_ROUND_MATCH_DIFFERENT_OPTION, QUESTION_ROUND_MATCH_FOLLOWED,
    QUESTION_ROUND_MATCH_FREE_TEXT, QUESTION_ROUND_MATCH_OTHER_TEXT,
    QUESTION_ROUND_STATUS_ANSWERED, QUESTION_ROUND_STATUS_CANCELLED,
    QUESTION_ROUND_STATUS_FAILED, QUESTION_ROUND_STATUS_LOST,
    QUESTION_ROUND_STATUS_PENDING, QUESTION_ROUND_STATUS_TIMED_OUT,
    QUESTION_ROUND_STATUS_UNKNOWN, QUESTION_ROUND_WIRE_SCHEMA_VERSION,
};

/// Project one question round's read model from host-collected bundle JSON.
///
/// Only an unsupported `schema_version` or a malformed request wire
/// errors. Malformed bundle content degrades `data_state` instead.
pub fn project_question_round(
    request: &QuestionRoundRequestWire,
) -> Result<QuestionRoundWire, QuestionRoundError> {
    validate_schema(request.schema_version)?;
    validate_gate(&request.gate)?;

    let questions = parse_questions(request.request.as_ref());
    let submit = parse_submit_result(request.response.as_ref());
    let answers = submit
        .as_ref()
        .and_then(|result| result.answers.as_deref())
        .unwrap_or(&[]);
    let has_submit = submit.is_some();
    let global_note = submit
        .as_ref()
        .and_then(|result| result.global_note.clone())
        .or_else(|| parse_feedback_note(request.response.as_ref()));
    let responded_at_unix =
        parse_f64(request.response.as_ref(), "responded_at_unix");
    let source = parse_string(request.response.as_ref(), "source");
    let caller = parse_string(request.response.as_ref(), "caller");

    let status =
        classify_status(request.gate.gate_state.as_deref(), has_submit);
    let actor = classify_actor(source.as_deref(), caller.as_deref());
    let policy = parse_policy(
        request.response.as_ref(),
        request.request.as_ref(),
        actor.as_str(),
    );
    let provenance = match (&source, actor.as_str(), &policy, responded_at_unix)
    {
        (None, actor, None, None) if actor == QUESTION_ROUND_ACTOR_UNKNOWN => {
            None
        }
        _ => Some(QuestionProvenanceWire {
            channel: source,
            actor,
            policy,
            responded_at_unix,
        }),
    };
    let cancellation = parse_cancellation(request.cancellation.as_ref());

    let entries = build_entries(&questions, answers, has_submit);
    let data_state = classify_data_state(
        questions.present,
        has_submit,
        &entries,
        request.gate.gate_state.as_deref(),
    );

    Ok(QuestionRoundWire {
        schema_version: QUESTION_ROUND_WIRE_SCHEMA_VERSION,
        gate_id: request.gate.gate_id.clone(),
        round_index: request.gate.round_index,
        asked_by: request.gate.asked_by.clone(),
        asked_at_unix: request.gate.asked_at_unix,
        status,
        gate_state: request.gate.gate_state.clone(),
        data_state: data_state.to_string(),
        entries,
        global_note,
        provenance,
        cancellation,
    })
}

/// Parse a JSON request value and project it, rejecting a malformed
/// payload before projection runs.
pub fn project_question_round_from_json(
    value: &JsonValue,
) -> Result<QuestionRoundWire, QuestionRoundError> {
    let request: QuestionRoundRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            QuestionRoundError::new(
                QUESTION_ROUND_CODE_INVALID_REQUEST,
                format!("invalid question-round request: {error}"),
            )
        })?;
    project_question_round(&request)
}

fn validate_schema(actual: u32) -> Result<(), QuestionRoundError> {
    if actual == QUESTION_ROUND_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(QuestionRoundError::new(
            QUESTION_ROUND_CODE_UNSUPPORTED_SCHEMA,
            format!(
                "unsupported question-round schema_version {actual}; expected {QUESTION_ROUND_WIRE_SCHEMA_VERSION}"
            ),
        ))
    }
}

fn validate_gate(
    gate: &QuestionRoundGateWire,
) -> Result<(), QuestionRoundError> {
    if gate.gate_id.trim().is_empty() {
        return Err(QuestionRoundError::new(
            QUESTION_ROUND_CODE_INVALID_REQUEST,
            "gate.gate_id must not be blank",
        ));
    }
    Ok(())
}

fn classify_status(gate_state: Option<&str>, has_submit: bool) -> String {
    if has_submit {
        return QUESTION_ROUND_STATUS_ANSWERED.to_string();
    }
    match gate_state {
        Some("pending") | Some("settling") => QUESTION_ROUND_STATUS_PENDING,
        Some("answered") | Some("completed") => QUESTION_ROUND_STATUS_ANSWERED,
        Some("timeout") => QUESTION_ROUND_STATUS_TIMED_OUT,
        Some("stopped") => QUESTION_ROUND_STATUS_CANCELLED,
        Some("failed") => QUESTION_ROUND_STATUS_FAILED,
        Some("lost") => QUESTION_ROUND_STATUS_LOST,
        _ => QUESTION_ROUND_STATUS_UNKNOWN,
    }
    .to_string()
}

fn classify_actor(source: Option<&str>, caller: Option<&str>) -> String {
    if source == Some("auto_resolution") {
        return QUESTION_ROUND_ACTOR_AUTO_POLICY.to_string();
    }
    match caller {
        Some("human") => QUESTION_ROUND_ACTOR_HUMAN.to_string(),
        Some("agent") => QUESTION_ROUND_ACTOR_AGENT.to_string(),
        _ => match source {
            Some("tui") | Some("telegram") | Some("mobile") | Some("cli") => {
                QUESTION_ROUND_ACTOR_HUMAN.to_string()
            }
            _ => QUESTION_ROUND_ACTOR_UNKNOWN.to_string(),
        },
    }
}

fn classify_data_state(
    questions_present: bool,
    has_submit: bool,
    entries: &[QuestionEntryWire],
    gate_state: Option<&str>,
) -> &'static str {
    if !questions_present {
        return QUESTION_ROUND_DATA_UNAVAILABLE;
    }
    if has_submit {
        if entries.iter().any(|entry| entry.answer.is_none()) {
            return QUESTION_ROUND_DATA_PARTIAL;
        }
        return QUESTION_ROUND_DATA_COMPLETE;
    }
    match gate_state {
        // The gate claims an answer exists but no submit result survived:
        // the response is missing or malformed, so the round is partial.
        // A durably recorded submit result would already have forced
        // `answered` above, so reaching here with an answered gate state
        // means the evidence disagrees with the state. Never render that
        // as pending.
        Some("answered") | Some("completed") => QUESTION_ROUND_DATA_PARTIAL,
        _ => QUESTION_ROUND_DATA_COMPLETE,
    }
}

/// One parsed question before answer alignment.
struct ParsedQuestion {
    header: Option<String>,
    question: String,
    options: Vec<QuestionOptionWire>,
    multi_select: bool,
}

/// Parsed questions plus whether any usable question list was found.
struct ParsedQuestions {
    present: bool,
    questions: Vec<ParsedQuestion>,
}

/// Parse the request bundle's question list, accepting both the current
/// `payload.questions` layout and the legacy top-level `questions` layout
/// (the pre-gate `question_request.json` shape).
fn parse_questions(request: Option<&JsonValue>) -> ParsedQuestions {
    let absent = ParsedQuestions {
        present: false,
        questions: vec![],
    };
    let map = match request.and_then(JsonValue::as_object) {
        Some(map) => map,
        None => return absent,
    };
    let list = map
        .get("payload")
        .and_then(|payload| payload.get("questions"))
        .or_else(|| map.get("questions"));
    let list = match list {
        Some(JsonValue::Array(list)) => list,
        _ => return absent,
    };
    let mut questions = Vec::with_capacity(list.len());
    for item in list {
        let map = match item.as_object() {
            Some(map) => map,
            None => continue,
        };
        questions.push(ParsedQuestion {
            header: map
                .get("header")
                .and_then(JsonValue::as_str)
                .filter(|header| !header.is_empty())
                .map(str::to_string),
            question: map
                .get("question")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_string(),
            options: parse_options(map.get("options")),
            multi_select: map
                .get("multiSelect")
                .and_then(JsonValue::as_bool)
                .or_else(|| {
                    map.get("multi_select").and_then(JsonValue::as_bool)
                })
                .unwrap_or(false),
        });
    }
    ParsedQuestions {
        present: true,
        questions,
    }
}

fn parse_options(value: Option<&JsonValue>) -> Vec<QuestionOptionWire> {
    let list = match value {
        Some(JsonValue::Array(list)) => list,
        _ => return vec![],
    };
    let mut options = Vec::with_capacity(list.len());
    for item in list {
        let map = match item.as_object() {
            Some(map) => map,
            None => continue,
        };
        let label = match map.get("label").and_then(JsonValue::as_str) {
            Some(label) => label.to_string(),
            None => continue,
        };
        options.push(QuestionOptionWire {
            label,
            description: map
                .get("description")
                .and_then(JsonValue::as_str)
                .filter(|description| !description.is_empty())
                .map(str::to_string),
        });
    }
    options
}

/// One parsed raw answer before alignment.
#[derive(Debug, Clone)]
struct ParsedAnswer {
    question: String,
    selected: Vec<String>,
    custom_feedback: Option<String>,
}

/// The submit result's answers plus the round's global note.
struct ParsedSubmit {
    answers: Option<Vec<ParsedAnswer>>,
    global_note: Option<String>,
}

/// Parse the response bundle's submit result: the `option_results[submit]`
/// answers (mirroring `build_qa_round`'s translated shape) or, for the
/// legacy pre-gate layout, the top-level `answers` / `global_note` shape.
fn parse_submit_result(response: Option<&JsonValue>) -> Option<ParsedSubmit> {
    let response = response?.as_object()?;
    let result = response
        .get("option_results")
        .and_then(JsonValue::as_array)
        .and_then(|results| {
            results.iter().find(|entry| {
                entry
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|id| id == "submit")
            })
        })
        .and_then(|entry| entry.get("result"));
    if let Some(result) = result {
        let answers = result
            .get("answers")
            .and_then(JsonValue::as_array)
            .map(|list| list.iter().map(parse_answer).collect::<Vec<_>>());
        return Some(ParsedSubmit {
            answers,
            global_note: nonempty_string(result.get("global_note")),
        });
    }
    let answers = response
        .get("answers")
        .and_then(JsonValue::as_array)
        .map(|list| list.iter().map(parse_answer).collect::<Vec<_>>())?;
    Some(ParsedSubmit {
        answers: Some(answers),
        global_note: nonempty_string(response.get("global_note")),
    })
}

fn parse_answer(value: &JsonValue) -> ParsedAnswer {
    let map = value.as_object();
    ParsedAnswer {
        question: map
            .and_then(|map| map.get("question"))
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string(),
        selected: map
            .and_then(|map| map.get("selected"))
            .and_then(JsonValue::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(JsonValue::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        custom_feedback: map
            .and_then(|map| nonempty_string(map.get("custom_feedback"))),
    }
}

/// Align answers to questions, mirroring `build_qa_round` in
/// `src/sase/main/qa_prompt.py`: by index when the counts match, otherwise
/// by question text. An entry with no match keeps `answer = None` and (for
/// a submitted round) degrades `data_state` to `partial`.
fn build_entries(
    questions: &ParsedQuestions,
    answers: &[ParsedAnswer],
    has_submit: bool,
) -> Vec<QuestionEntryWire> {
    if !has_submit {
        return questions
            .questions
            .iter()
            .enumerate()
            .map(|(position, parsed)| entry_wire(parsed, position, None))
            .collect();
    }
    if answers.len() == questions.questions.len() {
        return questions
            .questions
            .iter()
            .enumerate()
            .map(|(position, parsed)| {
                entry_wire(parsed, position, Some(&answers[position]))
            })
            .collect();
    }
    let mut by_text: std::collections::HashMap<String, VecDeque<ParsedAnswer>> =
        std::collections::HashMap::new();
    for answer in answers {
        by_text
            .entry(answer.question.clone())
            .or_default()
            .push_back(answer.clone());
    }
    questions
        .questions
        .iter()
        .enumerate()
        .map(|(position, parsed)| {
            let matched = by_text
                .get_mut(&parsed.question)
                .and_then(VecDeque::pop_front);
            entry_wire(parsed, position, matched.as_ref())
        })
        .collect()
}

fn entry_wire(
    parsed: &ParsedQuestion,
    position: usize,
    answer: Option<&ParsedAnswer>,
) -> QuestionEntryWire {
    let recommended_index = if parsed.options.is_empty() {
        None
    } else {
        Some(0)
    };
    let recommendation_basis = if parsed.options.is_empty() {
        QUESTION_ROUND_BASIS_NONE
    } else {
        QUESTION_ROUND_BASIS_OPTION_ORDER
    }
    .to_string();
    let answer = answer.map(|matched| QuestionAnswerWire {
        selected: matched.selected.clone(),
        custom_feedback: matched.custom_feedback.clone(),
    });
    let recommendation_match = match (&answer, parsed.options.first()) {
        (None, _) => None,
        (Some(_), None) => Some(QUESTION_ROUND_MATCH_FREE_TEXT.to_string()),
        (Some(matched), Some(recommended)) => {
            Some(match_recommendation(matched, &recommended.label))
        }
    };
    QuestionEntryWire {
        index: position as u64,
        header: parsed.header.clone(),
        question: parsed.question.clone(),
        options: parsed.options.clone(),
        multi_select: parsed.multi_select,
        recommended_index,
        recommendation_basis,
        answer,
        recommendation_match,
    }
}

fn match_recommendation(
    answer: &QuestionAnswerWire,
    recommended: &str,
) -> String {
    if answer.selected.iter().any(|label| label == recommended) {
        return QUESTION_ROUND_MATCH_FOLLOWED.to_string();
    }
    if !answer.selected.is_empty()
        && answer.selected.iter().all(|label| label == "Other")
    {
        return QUESTION_ROUND_MATCH_OTHER_TEXT.to_string();
    }
    QUESTION_ROUND_MATCH_DIFFERENT_OPTION.to_string()
}

/// Parse the round's policy: `response.policy` first, falling back to
/// `request.auto.policy` when the actor is the auto policy.
fn parse_policy(
    response: Option<&JsonValue>,
    request: Option<&JsonValue>,
    actor: &str,
) -> Option<QuestionPolicyWire> {
    if let Some(policy) = response
        .and_then(|value| value.get("policy"))
        .and_then(parse_policy_block)
    {
        return Some(policy);
    }
    if actor != QUESTION_ROUND_ACTOR_AUTO_POLICY {
        return None;
    }
    request
        .and_then(|value| value.get("auto"))
        .and_then(|auto| auto.get("policy"))
        .and_then(parse_policy_block)
}

fn parse_policy_block(value: &JsonValue) -> Option<QuestionPolicyWire> {
    let map = value.as_object()?;
    let policy = QuestionPolicyWire {
        profile: nonempty_string(map.get("profile")),
        rule: nonempty_string(map.get("rule")),
        value: nonempty_string(map.get("value")),
        source: nonempty_string(map.get("source")),
    };
    if policy.profile.is_none()
        && policy.rule.is_none()
        && policy.value.is_none()
        && policy.source.is_none()
    {
        return None;
    }
    Some(policy)
}

fn parse_cancellation(
    cancellation: Option<&JsonValue>,
) -> Option<QuestionCancellationWire> {
    let map = cancellation?.as_object()?;
    let record = QuestionCancellationWire {
        reason: nonempty_string(map.get("reason")),
        source: nonempty_string(map.get("source")),
        cancelled_at_unix: map
            .get("cancelled_at_unix")
            .and_then(JsonValue::as_f64),
    };
    if record.reason.is_none()
        && record.source.is_none()
        && record.cancelled_at_unix.is_none()
    {
        return None;
    }
    Some(record)
}

/// The response's global note: the submit result already won above, so this
/// is the legacy `response.feedback` fallback only.
fn parse_feedback_note(response: Option<&JsonValue>) -> Option<String> {
    let map = response?.as_object()?;
    nonempty_string(map.get("feedback"))
}

fn parse_string(value: Option<&JsonValue>, key: &str) -> Option<String> {
    value.and_then(|map| nonempty_string(map.get(key)))
}

fn parse_f64(value: Option<&JsonValue>, key: &str) -> Option<f64> {
    value
        .and_then(|map| map.get(key))
        .and_then(JsonValue::as_f64)
}

fn nonempty_string(value: Option<&JsonValue>) -> Option<String> {
    value
        .and_then(JsonValue::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}
