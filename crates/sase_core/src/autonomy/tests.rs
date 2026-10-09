//! Tests for the E1 autonomy record, profiles, legacy translation,
//! and `evaluate()`.

use serde_json::{json, Map, Value};

use super::evaluate::{evaluate, fleet_auto_approved};
use super::legacy::{
    autonomy_legacy_projection, autonomy_record_from_legacy_meta,
};
use super::profiles::{policy_digest, profile_policy};
use super::resolve::{resolve_autonomy_selection, AutonomyError};
use super::wires::{
    AutonomyActorWire, AutonomyEvaluateRequestWire, AutonomyGatesWire,
    AutonomyPolicyWire, AutonomyRecordWire, AUTONOMY_WIRE_SCHEMA_VERSION,
};

fn actor() -> AutonomyActorWire {
    AutonomyActorWire {
        kind: "human".to_string(),
        surface: "prompt".to_string(),
        principal: "bryan.zeus".to_string(),
    }
}

fn resolve(selection: Option<&str>) -> AutonomyRecordWire {
    resolve_autonomy_selection(selection, "prompt", &actor(), "2026-10-09")
        .expect("selection resolves")
}

fn full_request(kind: &str) -> AutonomyEvaluateRequestWire {
    AutonomyEvaluateRequestWire {
        gate_kind: kind.to_string(),
        option_ids: vec![
            "approve".to_string(),
            "commit".to_string(),
            "submit".to_string(),
            "reject".to_string(),
            "feedback".to_string(),
        ],
        capabilities: vec![
            "approve_archive".to_string(),
            "approve".to_string(),
            "first".to_string(),
        ],
        request_id: None,
    }
}

fn decide(
    profile_selection: Option<&str>,
    kind: &str,
) -> super::wires::AutonomyDecisionWire {
    evaluate(&resolve(profile_selection), &full_request(kind))
}

#[test]
fn resolve_covers_every_spelling() {
    // (selection, profile, canonical selection)
    for (selection, profile, canonical) in [
        (None, "manual", "manual"),
        (Some(""), "standard", ""),
        (Some("true"), "standard", ""),
        (Some("+"), "standard", ""),
        (Some("plan"), "tale", "plan"),
        (Some("tale"), "tale", "tale"),
        (Some("epic"), "epic", "epic"),
        (Some("manual"), "manual", "manual"),
        (Some("off"), "manual", "manual"),
    ] {
        let record = resolve(selection);
        assert_eq!(record.profile, profile, "selection {selection:?}");
        assert_eq!(record.selection, canonical, "selection {selection:?}");
        assert_eq!(record.schema_version, AUTONOMY_WIRE_SCHEMA_VERSION);
        assert_eq!(record.revision, 1);
        assert_eq!(record.source, "prompt");
        assert_eq!(
            record.digest,
            policy_digest(&record.policy),
            "digest matches policy"
        );
        if profile == "manual" {
            assert_eq!(record.last, None);
        } else {
            let last = record.last.as_ref().expect("last set");
            assert_eq!(last.profile, profile);
            assert_eq!(last.selection, canonical);
        }
    }
}

#[test]
fn resolve_rejects_unknown_spellings_with_invalid_auto() {
    for selection in ["foo", "first", "epic_plan", "x(plan=ask)", "TRUE"] {
        let error =
            resolve_autonomy_selection(Some(selection), "prompt", &actor(), "")
                .unwrap_err();
        match error {
            AutonomyError::InvalidSelection { code, message } => {
                assert_eq!(code, "invalid-auto");
                assert!(
                    message.contains(&format!("%auto:{selection}")),
                    "message names the spelling: {message}"
                );
            }
            other => panic!("wrong error for {selection:?}: {other}"),
        }
    }
}

#[test]
fn resolve_rejects_bad_source_and_actor() {
    assert!(matches!(
        resolve_autonomy_selection(None, "nope", &actor(), ""),
        Err(AutonomyError::InvalidSource(_))
    ));
    let mut bad = actor();
    bad.kind = "daemon".to_string();
    assert!(matches!(
        resolve_autonomy_selection(None, "prompt", &bad, ""),
        Err(AutonomyError::InvalidActorKind(_))
    ));
}

#[test]
fn compat_rows_give_contract_outcomes() {
    // (selection, tale plan, epic plan, question) as
    // (outcome, value) pairs.
    let rows: [(Option<&str>, _, _, _); 5] = [
        (None, ("ask", None), ("ask", None), ("ask", None)),
        (
            Some(""),
            ("auto", Some("approve_archive")),
            ("auto", Some("approve")),
            ("auto", Some("first")),
        ),
        (
            Some("tale"),
            ("auto", Some("approve_archive")),
            ("ask", None),
            ("auto", Some("first")),
        ),
        (
            Some("plan"),
            ("auto", Some("approve_archive")),
            ("ask", None),
            ("auto", Some("first")),
        ),
        (
            Some("epic"),
            ("ask", None),
            ("auto", Some("approve")),
            ("auto", Some("first")),
        ),
    ];
    for (selection, tale, epic, question) in rows {
        for (kind, (outcome, value)) in
            [("plan", tale), ("epic_plan", epic), ("question", question)]
        {
            let decision = decide(selection, kind);
            assert_eq!(
                (decision.outcome.as_str(), decision.value.as_deref()),
                (outcome, value),
                "selection {selection:?} kind {kind}"
            );
        }
    }
    // Manual spellings ask everywhere.
    for selection in [Some("manual"), Some("off")] {
        for kind in ["plan", "epic_plan", "question"] {
            assert_eq!(decide(selection, kind).outcome, "ask");
        }
    }
}

#[test]
fn approve_archive_carries_both_option_ids_in_canonical_order() {
    let decision = decide(Some(""), "plan");
    assert_eq!(decision.value.as_deref(), Some("approve_archive"));
    assert_eq!(decision.option_ids, vec!["approve", "commit"]);
    let epic = decide(Some(""), "epic_plan");
    assert_eq!(epic.option_ids, vec!["approve"]);
    let question = decide(Some(""), "question");
    assert_eq!(question.option_ids, vec!["submit"]);
}

#[test]
fn privileged_and_unknown_kinds_always_ask() {
    for kind in [
        "launch",
        "sudo",
        "custom",
        "hitl",
        "task_triage",
        "bead_snooze",
        "flag_triage",
        "bead_stale_cleanup",
        "plugins_required",
    ] {
        let decision = decide(Some(""), kind);
        assert_eq!(decision.outcome, "ask", "kind {kind}");
        assert_eq!(decision.rule, "not_auto_allowable", "kind {kind}");
        assert!(decision.value.is_none());
        assert!(decision.option_ids.is_empty());
    }
    for kind in ["bogus", "", "plan_approval", "epic"] {
        let decision = decide(Some(""), kind);
        assert_eq!(decision.outcome, "ask", "kind {kind:?}");
        assert_eq!(decision.rule, "unknown_kind", "kind {kind:?}");
    }
}

#[test]
fn unmentioned_value_asks() {
    let mut record = resolve(Some(""));
    record.policy.gates.epic = None;
    let decision = evaluate(&record, &full_request("epic_plan"));
    assert_eq!(decision.outcome, "ask");
    assert_eq!(decision.rule, "unmentioned");
}

#[test]
fn ask_value_reports_its_policy_key() {
    let decision = decide(Some("tale"), "epic_plan");
    assert_eq!(decision.outcome, "ask");
    assert_eq!(decision.rule, "gates.epic");
    let decision = decide(Some("epic"), "plan");
    assert_eq!(decision.rule, "gates.plan");
}

#[test]
fn value_outside_capabilities_asks_without_partial_selection() {
    let mut request = full_request("plan");
    request.capabilities = vec!["approve".to_string()];
    let decision = evaluate(&resolve(Some("")), &request);
    assert_eq!(decision.outcome, "ask");
    assert_eq!(decision.rule, "not_capable");
    assert!(decision.option_ids.is_empty());
}

#[test]
fn missing_required_option_asks_without_partial_selection() {
    let mut request = full_request("plan");
    request.option_ids = vec!["approve".to_string()];
    let decision = evaluate(&resolve(Some("")), &request);
    assert_eq!(decision.outcome, "ask");
    assert_eq!(decision.rule, "missing_options");
    assert!(decision.option_ids.is_empty());
    assert!(decision.reason.contains("commit"));
}

#[test]
fn option_order_does_not_change_the_decision() {
    let mut request = full_request("plan");
    request.option_ids.reverse();
    let flipped = evaluate(&resolve(Some("")), &request);
    let straight = decide(Some(""), "plan");
    assert_eq!(flipped, straight);
}

#[test]
fn decision_echoes_record_identity() {
    let record = resolve(Some("tale"));
    let decision = evaluate(&record, &full_request("plan"));
    assert_eq!(decision.profile, "tale");
    assert_eq!(decision.selection, "tale");
    assert_eq!(decision.revision, record.revision);
    assert_eq!(decision.digest, record.digest);
    assert_eq!(decision.source, record.source);
}

fn legacy_meta(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}

#[test]
fn legacy_translation_covers_every_writer_shape() {
    // (meta, profile, selection)
    let rows: Vec<(Map<String, Value>, &str, &str)> = vec![
        // Bare `%auto` and toggle-on bare.
        (legacy_meta(&[("approve", json!(true))]), "standard", ""),
        // `:plan`.
        (
            legacy_meta(&[
                ("approve", json!(true)),
                ("auto_approve_argument", json!("plan")),
            ]),
            "tale",
            "plan",
        ),
        // `:tale`.
        (
            legacy_meta(&[
                ("auto_approve_plan_action", json!("tale")),
                ("auto_approve_argument", json!("tale")),
                ("plan", json!(true)),
            ]),
            "tale",
            "tale",
        ),
        // `:epic`.
        (
            legacy_meta(&[
                ("auto_approve_plan_action", json!("epic")),
                ("auto_approve_argument", json!("epic")),
                ("plan", json!(true)),
            ]),
            "epic",
            "epic",
        ),
        // Revive action-only.
        (
            legacy_meta(&[("auto_approve_plan_action", json!("tale"))]),
            "tale",
            "tale",
        ),
        (
            legacy_meta(&[("auto_approve_plan_action", json!("approve"))]),
            "standard",
            "",
        ),
        // Legacy `"plan"` action with a `:plan` argument.
        (
            legacy_meta(&[
                ("auto_approve_argument", json!("plan")),
                ("auto_approve_plan_action", json!("plan")),
            ]),
            "tale",
            "plan",
        ),
        // Legacy `"plan"` action alone enables nothing (today's
        // readers normalize it away and find no `approve`), so it is
        // manual; the successor path drops it the same way.
        (
            legacy_meta(&[("auto_approve_plan_action", json!("plan"))]),
            "manual",
            "manual",
        ),
        // Legacy `epic_plan` argument covers epic plans only.
        (
            legacy_meta(&[("auto_approve_argument", json!("epic_plan"))]),
            "epic",
            "epic",
        ),
        // Empty meta and toggle-off are manual.
        (legacy_meta(&[]), "manual", "manual"),
        (
            legacy_meta(&[
                ("approve", json!(false)),
                ("auto_approve_plan_action", json!("")),
            ]),
            "manual",
            "manual",
        ),
        // Unknown arguments fail closed to manual even when enabled.
        (
            legacy_meta(&[("auto_approve_argument", json!("foo"))]),
            "manual",
            "manual",
        ),
    ];
    for (meta, profile, selection) in rows {
        let record = autonomy_record_from_legacy_meta(&meta);
        assert_eq!(record.profile, profile, "meta {meta:?}");
        assert_eq!(record.selection, selection, "meta {meta:?}");
        assert_eq!(record.source, "legacy");
        assert_eq!(record.revision, 1);
        assert_eq!(record.digest, policy_digest(&record.policy));
    }
}

#[test]
fn legacy_translation_matches_resolve_for_canonical_shapes() {
    for (meta, selection) in [
        (legacy_meta(&[("approve", json!(true))]), Some("")),
        (
            legacy_meta(&[
                ("approve", json!(true)),
                ("auto_approve_argument", json!("plan")),
            ]),
            Some("plan"),
        ),
        (
            legacy_meta(&[
                ("auto_approve_plan_action", json!("tale")),
                ("auto_approve_argument", json!("tale")),
                ("plan", json!(true)),
            ]),
            Some("tale"),
        ),
        (
            legacy_meta(&[
                ("auto_approve_plan_action", json!("epic")),
                ("auto_approve_argument", json!("epic")),
                ("plan", json!(true)),
            ]),
            Some("epic"),
        ),
        (legacy_meta(&[]), None),
    ] {
        let from_legacy = autonomy_record_from_legacy_meta(&meta);
        let from_resolve = resolve(selection);
        assert_eq!(from_legacy.profile, from_resolve.profile);
        assert_eq!(from_legacy.selection, from_resolve.selection);
        assert_eq!(from_legacy.policy, from_resolve.policy);
        assert_eq!(from_legacy.digest, from_resolve.digest);
    }
}

#[test]
fn projection_reproduces_writer_output() {
    let cases = [
        (
            Some(""),
            r#"{"approve":true,"plan":false,"prompt_mode":"plan"}"#,
        ),
        (
            Some("plan"),
            r#"{"approve":true,"auto_approve_argument":"plan","plan":false,"prompt_mode":"plan"}"#,
        ),
        (
            Some("tale"),
            r#"{"approve":false,"auto_approve_argument":"tale","auto_approve_plan_action":"tale","plan":true,"prompt_mode":"tale"}"#,
        ),
        (
            Some("epic"),
            r#"{"approve":false,"auto_approve_argument":"epic","auto_approve_plan_action":"epic","plan":true,"prompt_mode":"epic"}"#,
        ),
        (None, r#"{"approve":false,"plan":false}"#),
    ];
    for (selection, expected) in cases {
        let projection = autonomy_legacy_projection(&resolve(selection));
        let encoded = serde_json::to_string(&projection).unwrap();
        assert_eq!(encoded, expected, "selection {selection:?}");
    }
}

#[test]
fn legacy_round_trip_is_stable() {
    for meta in [
        legacy_meta(&[("approve", json!(true))]),
        legacy_meta(&[
            ("approve", json!(true)),
            ("auto_approve_argument", json!("plan")),
        ]),
        legacy_meta(&[
            ("auto_approve_plan_action", json!("tale")),
            ("auto_approve_argument", json!("tale")),
            ("plan", json!(true)),
        ]),
        legacy_meta(&[]),
    ] {
        let record = autonomy_record_from_legacy_meta(&meta);
        let projection = autonomy_legacy_projection(&record);
        let mut projected = Map::new();
        projected.insert("approve".to_string(), json!(projection.approve));
        if let Some(argument) = projection.auto_approve_argument {
            projected
                .insert("auto_approve_argument".to_string(), json!(argument));
        }
        if let Some(action) = projection.auto_approve_plan_action {
            projected
                .insert("auto_approve_plan_action".to_string(), json!(action));
        }
        if projection.plan {
            projected.insert("plan".to_string(), json!(true));
        }
        let again = autonomy_record_from_legacy_meta(&projected);
        assert_eq!(again.profile, record.profile, "meta {meta:?}");
        assert_eq!(again.selection, record.selection, "meta {meta:?}");
        assert_eq!(again.digest, record.digest, "meta {meta:?}");
    }
}

#[test]
fn digest_golden_is_stable() {
    // Pinned so a silent canonicalization change fails loudly.
    assert_eq!(
        policy_digest(&profile_policy("standard")),
        "7b415224b791b582b6cf614e869382d1cffb507432c5c1c0905834402dcbcfb7"
    );
    assert_eq!(
        policy_digest(&profile_policy("manual")),
        policy_digest(&AutonomyPolicyWire {
            gates: AutonomyGatesWire {
                plan: Some("ask".to_string()),
                epic: Some("ask".to_string()),
                question: Some("ask".to_string()),
            },
            on_ask: "park".to_string(),
        })
    );
}

#[test]
fn record_wire_round_trips() {
    let record = resolve(Some("tale"));
    let encoded = serde_json::to_value(&record).unwrap();
    assert_eq!(
        encoded["schema_version"],
        json!(AUTONOMY_WIRE_SCHEMA_VERSION)
    );
    let decoded: AutonomyRecordWire = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, record);
}

#[test]
fn fleet_auto_approved_routes_through_evaluate() {
    // Bare `%auto` covers both tiers.
    assert!(fleet_auto_approved(true, None, None, Some("tale")));
    assert!(fleet_auto_approved(true, None, None, Some("epic")));
    // `:tale` covers tale plans only.
    assert!(fleet_auto_approved(false, Some("tale"), None, Some("tale")));
    assert!(!fleet_auto_approved(
        false,
        Some("tale"),
        None,
        Some("epic")
    ));
    // `:epic` covers epic plans only.
    assert!(!fleet_auto_approved(
        false,
        Some("epic"),
        None,
        Some("tale")
    ));
    assert!(fleet_auto_approved(false, Some("epic"), None, Some("epic")));
    // No auto anywhere asks everywhere.
    assert!(!fleet_auto_approved(false, None, None, Some("tale")));
    assert!(!fleet_auto_approved(false, None, None, Some("epic")));
    // An unreadable tier keeps the legacy boolean.
    assert!(fleet_auto_approved(true, None, None, None));
    assert!(!fleet_auto_approved(false, None, None, None));
    // A record decides, not the stale legacy keys.
    let record = resolve(Some("epic"));
    assert!(!fleet_auto_approved(
        true,
        Some("tale"),
        Some(&record),
        Some("tale")
    ));
    assert!(fleet_auto_approved(
        false,
        None,
        Some(&record),
        Some("epic")
    ));
}
