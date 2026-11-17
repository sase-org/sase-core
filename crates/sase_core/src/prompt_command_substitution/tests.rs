use serde_json::json;

use super::*;

fn human_source(text: &str) -> PromptCommandSubstitutionRequestWire {
    PromptCommandSubstitutionRequestWire {
        schema_version: PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
        text: text.to_string(),
        kind: PromptCommandTextKindWire::HumanSource,
        launch_authorship: PromptLaunchAuthorshipWire::Human,
        protected_ranges: Vec::new(),
    }
}

fn plan(
    request: &PromptCommandSubstitutionRequestWire,
) -> PromptCommandSubstitutionPlanWire {
    plan_prompt_command_substitution(request).expect("plan succeeds")
}

fn replacement(
    start: u64,
    end: u64,
    command: &str,
) -> PromptCommandReplacementWire {
    PromptCommandReplacementWire {
        start,
        end,
        command: command.to_string(),
    }
}

#[test]
fn human_source_in_human_launch_plans_complete_commands() {
    let planned = plan(&human_source("before $(echo hi) after"));
    assert!(planned.eligible);
    assert_eq!(planned.offset_unit, PROMPT_COMMAND_SUBSTITUTION_OFFSET_UNIT);
    assert_eq!(planned.replacements, vec![replacement(7, 17, "echo hi")]);
}

#[test]
fn admitted_definition_in_human_launch_is_eligible() {
    let mut request = human_source("$(date)");
    request.kind = PromptCommandTextKindWire::AdmittedDefinition;
    let planned = plan(&request);
    assert!(planned.eligible);
    assert_eq!(planned.replacements, vec![replacement(0, 7, "date")]);
}

#[test]
fn generated_text_is_ineligible_even_in_a_human_launch() {
    let mut request = human_source("$(printf EXECUTED)");
    request.kind = PromptCommandTextKindWire::Generated;
    let planned = plan(&request);
    assert!(!planned.eligible);
    assert!(planned.replacements.is_empty());
}

#[test]
fn agent_launch_refuses_human_source_and_admitted_definition() {
    for kind in [
        PromptCommandTextKindWire::HumanSource,
        PromptCommandTextKindWire::AdmittedDefinition,
        PromptCommandTextKindWire::Generated,
    ] {
        let mut request = human_source("$(printf EXECUTED)");
        request.kind = kind;
        request.launch_authorship = PromptLaunchAuthorshipWire::Agent;
        let planned = plan(&request);
        assert!(!planned.eligible, "{kind:?}");
        assert!(planned.replacements.is_empty(), "{kind:?}");
    }
}

#[test]
fn nested_parentheses_form_one_outer_command() {
    let planned = plan(&human_source("$(echo $(date))"));
    assert_eq!(
        planned.replacements,
        vec![replacement(0, 15, "echo $(date)")]
    );
}

#[test]
fn escaped_dollar_paren_is_not_a_command() {
    let planned = plan(&human_source("\\$(not a command)"));
    assert!(planned.replacements.is_empty());
}

#[test]
fn unclosed_substitution_is_left_unplanned() {
    let planned = plan(&human_source("Unclosed $(echo hello"));
    assert!(planned.replacements.is_empty());
}

#[test]
fn multiple_commands_are_ordered_by_original_span() {
    let planned = plan(&human_source("$(one) mid $(two)"));
    assert_eq!(
        planned.replacements,
        vec![replacement(0, 6, "one"), replacement(11, 17, "two")]
    );
}

#[test]
fn non_ascii_prefix_uses_utf8_byte_offsets() {
    let text = "café $(printf hi)";
    let python_str_index = text.chars().take_while(|ch| *ch != '$').count();
    assert_eq!(python_str_index, 5);
    assert_eq!(text.find('$'), Some(6));
    let planned = plan(&human_source(text));
    assert_eq!(planned.replacements, vec![replacement(6, 18, "printf hi")]);
}

#[test]
fn empty_command_is_a_complete_span() {
    let planned = plan(&human_source("$()"));
    assert_eq!(planned.replacements, vec![replacement(0, 3, "")]);
}

#[test]
fn protected_range_skips_overlapping_commands_only() {
    let mut request = human_source("$(keep) $(skip) $(also)");
    request.protected_ranges =
        vec![PromptUtf8ByteRangeWire { start: 8, end: 15 }];
    assert_eq!(
        plan(&request).replacements,
        vec![replacement(0, 7, "keep"), replacement(16, 23, "also")]
    );
}

#[test]
fn invalid_protected_range_is_rejected() {
    let mut request = human_source("$(echo hi)");
    request.protected_ranges =
        vec![PromptUtf8ByteRangeWire { start: 4, end: 2 }];
    let error = plan_prompt_command_substitution(&request).unwrap_err();
    assert_eq!(error.code, "invalid_protected_range");
    assert!(error.message.contains("start 4 is greater than end 2"));
}

#[test]
fn protected_range_past_text_is_rejected() {
    let mut request = human_source("ab");
    request.protected_ranges =
        vec![PromptUtf8ByteRangeWire { start: 0, end: 3 }];
    let error = plan_prompt_command_substitution(&request).unwrap_err();
    assert_eq!(error.code, "invalid_protected_range");
    assert!(error.message.contains("outside text of 2 UTF-8 bytes"));
}

#[test]
fn protected_range_must_be_char_aligned() {
    let mut request = human_source("é$(x)");
    request.protected_ranges =
        vec![PromptUtf8ByteRangeWire { start: 1, end: 2 }];
    let error = plan_prompt_command_substitution(&request).unwrap_err();
    assert_eq!(error.code, "invalid_protected_range");
    assert!(error.message.contains("character boundaries"));
}

#[test]
fn unsupported_schema_is_rejected() {
    let mut request = human_source("$(echo hi)");
    request.schema_version = 0;
    let error = plan_prompt_command_substitution(&request).unwrap_err();
    assert_eq!(error.code, "unsupported_prompt_command_substitution_schema");
}

#[test]
fn json_round_trip_plans_human_source() {
    let planned = plan_prompt_command_substitution_from_json(&json!({
        "schema_version": PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
        "text": "see $(echo hi)",
        "kind": "human_source",
        "launch_authorship": "human",
    }))
    .expect("json plan succeeds");
    assert!(planned.eligible);
    assert_eq!(planned.replacements, vec![replacement(4, 14, "echo hi")]);
}

#[test]
fn json_generated_kind_is_refused() {
    let planned = plan_prompt_command_substitution_from_json(&json!({
        "schema_version": PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
        "text": "$(printf EXECUTED)",
        "kind": "generated",
        "launch_authorship": "human",
    }))
    .expect("json plan succeeds");
    assert!(!planned.eligible);
    assert!(planned.replacements.is_empty());
}

#[test]
fn json_unknown_field_is_rejected() {
    let error = plan_prompt_command_substitution_from_json(&json!({
        "schema_version": PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
        "text": "$(echo hi)",
        "kind": "human_source",
        "launch_authorship": "human",
        "execute": true,
    }))
    .unwrap_err();
    assert_eq!(error.code, "invalid_prompt_command_substitution_request");
}

#[test]
fn schema_version_getter_matches_constant() {
    assert_eq!(
        prompt_command_substitution_wire_schema_version(),
        PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION
    );
}
