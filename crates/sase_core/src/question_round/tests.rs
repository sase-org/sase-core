use serde_json::{json, Value as JsonValue};

use super::project::{
    project_question_round, project_question_round_from_json,
};
use super::wire::{
    QuestionRoundGateWire, QuestionRoundRequestWire,
    QUESTION_ROUND_CODE_INVALID_REQUEST,
    QUESTION_ROUND_CODE_UNSUPPORTED_SCHEMA, QUESTION_ROUND_WIRE_SCHEMA_VERSION,
};

fn gate(gate_state: Option<&str>) -> QuestionRoundGateWire {
    QuestionRoundGateWire {
        gate_id: "gate-1".to_string(),
        gate_state: gate_state.map(str::to_string),
        asked_by: Some("lane--0".to_string()),
        asked_at_unix: Some(1_792_243_181.0),
        round_index: Some(1),
    }
}

fn question(text: &str, options: Vec<&str>) -> JsonValue {
    json!({
        "question": text,
        "options": options
            .iter()
            .map(|label| json!({"label": label}))
            .collect::<Vec<_>>(),
    })
}

fn request_value(questions: Vec<JsonValue>) -> JsonValue {
    json!({"payload": {"questions": questions}})
}

fn answer(text: &str, selected: Vec<&str>) -> JsonValue {
    json!({
        "question": text,
        "selected": selected,
        "custom_feedback": null,
    })
}

fn submit_response(
    answers: Vec<JsonValue>,
    source: &str,
    extra: JsonValue,
) -> JsonValue {
    let mut response = json!({
        "source": source,
        "responded_at_unix": 1_792_243_226.0,
        "option_results": [
            {"id": "submit", "result": {"answers": answers, "global_note": ""}},
        ],
    });
    if let JsonValue::Object(extra) = extra {
        response
            .as_object_mut()
            .expect("response is an object")
            .extend(extra);
    }
    response
}

fn project(
    gate_state: Option<&str>,
    request: Option<JsonValue>,
    response: Option<JsonValue>,
    cancellation: Option<JsonValue>,
) -> super::wire::QuestionRoundWire {
    let wire = QuestionRoundRequestWire {
        schema_version: QUESTION_ROUND_WIRE_SCHEMA_VERSION,
        gate: gate(gate_state),
        request,
        response,
        cancellation,
    };
    project_question_round(&wire).expect("projection succeeds")
}

#[test]
fn maps_every_gate_state_to_a_round_status() {
    let cases = [
        ("pending", "pending"),
        ("settling", "pending"),
        ("answered", "answered"),
        ("completed", "answered"),
        ("timeout", "timed_out"),
        ("stopped", "cancelled"),
        ("failed", "failed"),
        ("lost", "lost"),
        ("bogus", "unknown"),
    ];
    for (gate_state, status) in cases {
        let round = project(
            Some(gate_state),
            Some(request_value(vec![question("Q?", vec!["Yes"])])),
            None,
            None,
        );
        assert_eq!(round.status, status, "gate_state {gate_state}");
        assert_eq!(round.gate_state.as_deref(), Some(gate_state));
    }
    let round = project(
        None,
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        None,
        None,
    );
    assert_eq!(round.status, "unknown");
    assert_eq!(round.entries.len(), 1);
    assert_eq!(round.entries[0].answer, None);
    assert_eq!(round.data_state, "complete");
}

#[test]
fn answered_then_failed_stays_answered_with_raw_state_echoed() {
    let round = project(
        Some("failed"),
        Some(request_value(vec![question("Ship it?", vec!["Yes", "No"])])),
        Some(submit_response(
            vec![answer("Ship it?", vec!["Yes"])],
            "tui",
            json!({}),
        )),
        None,
    );
    assert_eq!(round.status, "answered");
    assert_eq!(round.gate_state.as_deref(), Some("failed"));
    assert_eq!(
        round.entries[0].recommendation_match.as_deref(),
        Some("followed")
    );
}

#[test]
fn auto_response_with_agent_caller_is_auto_policy() {
    let round = project(
        Some("answered"),
        Some(json!({
            "payload": {"questions": [question("Q?", vec!["Done"])]},
            "auto": {"policy": {
                "profile": "standard",
                "rule": "gates.question",
                "value": "first",
                "source": "inherited",
            }},
        })),
        Some(submit_response(
            vec![answer("Q?", vec!["Done"])],
            "auto_resolution",
            json!({"caller": "agent"}),
        )),
        None,
    );
    let provenance = round.provenance.expect("provenance is present");
    assert_eq!(provenance.actor, "auto_policy");
    assert_eq!(provenance.channel.as_deref(), Some("auto_resolution"));
    // The response carries no policy block, so the request's auto policy
    // is the fallback.
    let policy = provenance.policy.expect("policy falls back");
    assert_eq!(policy.profile.as_deref(), Some("standard"));
    assert_eq!(policy.rule.as_deref(), Some("gates.question"));
    assert_eq!(policy.value.as_deref(), Some("first"));
}

#[test]
fn response_policy_wins_over_request_policy() {
    let round = project(
        Some("answered"),
        Some(json!({
            "payload": {"questions": [question("Q?", vec!["Done"])]},
            "auto": {"policy": {
                "profile": "standard",
                "rule": "gates.question",
                "value": "first",
            }},
        })),
        Some(submit_response(
            vec![answer("Q?", vec!["Done"])],
            "auto_resolution",
            json!({"caller": "agent", "policy": {
                "profile": "nightly",
                "rule": "gates.question",
                "value": "ask",
                "source": "explicit",
            }}),
        )),
        None,
    );
    let policy = round
        .provenance
        .expect("provenance is present")
        .policy
        .expect("response policy wins");
    assert_eq!(policy.profile.as_deref(), Some("nightly"));
    assert_eq!(policy.value.as_deref(), Some("ask"));
}

#[test]
fn tui_response_without_caller_is_human() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        Some(submit_response(
            vec![answer("Q?", vec!["No"])],
            "tui",
            json!({}),
        )),
        None,
    );
    let provenance = round.provenance.expect("provenance is present");
    assert_eq!(provenance.actor, "human");
    assert_eq!(provenance.channel.as_deref(), Some("tui"));
    assert_eq!(
        round.entries[0].recommendation_match.as_deref(),
        Some("different_option")
    );
}

#[test]
fn telegram_and_unknown_sources_classify() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        Some(submit_response(
            vec![answer("Q?", vec!["Yes"])],
            "telegram",
            json!({}),
        )),
        None,
    );
    assert_eq!(
        round.provenance.expect("provenance is present").actor,
        "human"
    );

    let round = project(
        Some("answered"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        Some(submit_response(
            vec![answer("Q?", vec!["Yes"])],
            "carrier_pigeon",
            json!({}),
        )),
        None,
    );
    let provenance = round.provenance.expect("provenance is present");
    assert_eq!(provenance.actor, "unknown");
    assert_eq!(provenance.channel.as_deref(), Some("carrier_pigeon"));
}

#[test]
fn multi_select_followed_when_first_option_selected() {
    let round = project(
        Some("answered"),
        Some(json!({"payload": {"questions": [{
            "question": "Pick any.",
            "multiSelect": true,
            "options": [{"label": "A"}, {"label": "B"}],
        }]}})),
        Some(submit_response(
            vec![answer("Pick any.", vec!["A", "B"])],
            "tui",
            json!({}),
        )),
        None,
    );
    assert!(round.entries[0].multi_select);
    assert_eq!(
        round.entries[0].recommendation_match.as_deref(),
        Some("followed")
    );
}

#[test]
fn option_less_answer_is_free_text_with_no_recommendation() {
    let round = project(
        Some("answered"),
        Some(json!({"payload": {"questions": [{
            "question": "Anything else?",
            "options": [],
        }]}})),
        Some(json!({
            "source": "tui",
            "responded_at_unix": 1_792_243_226.0,
            "option_results": [{"id": "submit", "result": {"answers": [{
                "question": "Anything else?",
                "selected": [],
                "custom_feedback": "Must run offline",
            }]}}],
        })),
        None,
    );
    let entry = &round.entries[0];
    assert_eq!(entry.recommended_index, None);
    assert_eq!(entry.recommendation_basis, "none");
    assert_eq!(entry.recommendation_match.as_deref(), Some("free_text"));
    assert_eq!(
        entry
            .answer
            .as_ref()
            .and_then(|answer| answer.custom_feedback.as_deref()),
        Some("Must run offline")
    );
}

#[test]
fn other_with_comment_is_other_text() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![question("Pick?", vec!["Yes", "Other"])])),
        Some(json!({
            "source": "tui",
            "responded_at_unix": 1_792_243_226.0,
            "option_results": [{"id": "submit", "result": {"answers": [{
                "question": "Pick?",
                "selected": ["Other"],
                "custom_feedback": "Need concurrency",
            }]}}],
        })),
        None,
    );
    assert_eq!(
        round.entries[0].recommendation_match.as_deref(),
        Some("other_text")
    );
}

#[test]
fn count_mismatch_aligns_by_text_and_marks_partial() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![
            question("Same?", vec!["Yes"]),
            question("Same?", vec!["Yes"]),
            question("Other?", vec!["Yes"]),
        ])),
        Some(submit_response(
            vec![answer("Same?", vec!["Yes"])],
            "tui",
            json!({}),
        )),
        None,
    );
    assert_eq!(round.data_state, "partial");
    assert!(round.entries[0].answer.is_some());
    assert!(round.entries[1].answer.is_none());
    assert!(round.entries[2].answer.is_none());
}

#[test]
fn duplicate_text_consumes_answers_in_order() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![
            question("Same?", vec!["Yes", "No"]),
            question("Same?", vec!["Yes", "No"]),
        ])),
        Some(json!({
            "source": "tui",
            "responded_at_unix": 1_792_243_226.0,
            "option_results": [{"id": "submit", "result": {"answers": [
                {"question": "Same?", "selected": ["Yes"], "custom_feedback": null},
                {"question": "Same?", "selected": ["No"], "custom_feedback": null},
                {"question": "Same?", "selected": ["Yes"], "custom_feedback": null},
            ]}}],
        })),
        None,
    );
    assert_eq!(round.data_state, "complete");
    assert_eq!(
        round.entries[0]
            .answer
            .as_ref()
            .map(|answer| &answer.selected),
        Some(&vec!["Yes".to_string()])
    );
    assert_eq!(
        round.entries[1]
            .answer
            .as_ref()
            .map(|answer| &answer.selected),
        Some(&vec!["No".to_string()])
    );
}

#[test]
fn thousand_character_question_projects_verbatim() {
    let long = "q".repeat(1500);
    let round = project(
        Some("pending"),
        Some(request_value(vec![question(&long, vec!["Yes"])])),
        None,
        None,
    );
    assert_eq!(round.entries[0].question.len(), 1500);
}

#[test]
fn missing_response_on_answered_gate_is_partial_not_pending() {
    let round = project(
        Some("answered"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        None,
        None,
    );
    assert_eq!(round.status, "answered");
    assert_eq!(round.data_state, "partial");
    assert!(round.entries[0].answer.is_none());
}

#[test]
fn malformed_response_on_answered_gate_is_partial() {
    let round = project(
        Some("completed"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        Some(json!({"source": "tui", "option_results": "garbage"})),
        None,
    );
    assert_eq!(round.status, "answered");
    assert_eq!(round.data_state, "partial");
}

#[test]
fn cancellation_only_bundle_projects_timeout() {
    let round = project(
        Some("timeout"),
        Some(request_value(vec![question("Q?", vec!["Yes"])])),
        None,
        Some(json!({
            "reason": "timeout",
            "source": "gate_shell_reclaim",
            "cancelled_at_unix": 1_789_320_389.0,
        })),
    );
    assert_eq!(round.status, "timed_out");
    assert_eq!(round.data_state, "complete");
    let cancellation = round.cancellation.expect("cancellation is present");
    assert_eq!(cancellation.reason.as_deref(), Some("timeout"));
    assert_eq!(cancellation.source.as_deref(), Some("gate_shell_reclaim"));
    assert_eq!(cancellation.cancelled_at_unix, Some(1_789_320_389.0));
}

#[test]
fn missing_request_is_unavailable() {
    let round = project(Some("pending"), None, None, None);
    assert_eq!(round.data_state, "unavailable");
    assert!(round.entries.is_empty());
    assert_eq!(round.status, "pending");
}

#[test]
fn legacy_layout_projects() {
    let round = project(
        Some("answered"),
        Some(json!({"questions": [question("Legacy?", vec!["Yes"])]
        })),
        Some(json!({
            "source": "cli",
            "responded_at_unix": 1_792_243_226.0,
            "answers": [answer("Legacy?", vec!["Yes"])],
            "global_note": "legacy note",
        })),
        None,
    );
    assert_eq!(round.status, "answered");
    assert_eq!(round.global_note.as_deref(), Some("legacy note"));
    assert_eq!(
        round.entries[0].recommendation_match.as_deref(),
        Some("followed")
    );
    assert_eq!(
        round.provenance.expect("provenance is present").actor,
        "human"
    );
}

#[test]
fn rejects_unsupported_schema_and_blank_gate() {
    let wire = QuestionRoundRequestWire {
        schema_version: 999,
        gate: gate(Some("pending")),
        request: None,
        response: None,
        cancellation: None,
    };
    let error = project_question_round(&wire).expect_err("schema must fail");
    assert_eq!(error.code, QUESTION_ROUND_CODE_UNSUPPORTED_SCHEMA);

    let mut bad_gate = gate(Some("pending"));
    bad_gate.gate_id = "  ".to_string();
    let wire = QuestionRoundRequestWire {
        schema_version: QUESTION_ROUND_WIRE_SCHEMA_VERSION,
        gate: bad_gate,
        request: None,
        response: None,
        cancellation: None,
    };
    let error =
        project_question_round(&wire).expect_err("blank gate must fail");
    assert_eq!(error.code, QUESTION_ROUND_CODE_INVALID_REQUEST);
}

#[test]
fn from_json_rejects_malformed_request_wire() {
    let error = project_question_round_from_json(&json!({"nope": true}))
        .expect_err("malformed wire must fail");
    assert_eq!(error.code, QUESTION_ROUND_CODE_INVALID_REQUEST);
}
