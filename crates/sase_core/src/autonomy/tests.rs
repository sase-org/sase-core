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

use super::wires::AutonomyLastWire;

#[test]
fn summary_covers_every_profile() {
    use super::summary::{
        autonomy_profiles, autonomy_summary, AUTONOMY_COVERAGE,
    };
    // (selection, class, short)
    for (selection, class, short) in [
        (None, "manual", "tales ✋ · epics ✋ · questions ✋"),
        (Some(""), "autopilot", "tales ✓ · epics ✓ · questions first"),
        (
            Some("tale"),
            "attended",
            "tales ✓ · epics ✋ · questions first",
        ),
        (
            Some("epic"),
            "attended",
            "tales ✋ · epics ✓ · questions first",
        ),
    ] {
        let summary = autonomy_summary(&resolve(selection));
        assert_eq!(summary.class, class, "selection {selection:?}");
        assert_eq!(summary.short, short, "selection {selection:?}");
        assert_eq!(summary.coverage, AUTONOMY_COVERAGE);
        assert_eq!(summary.cells.len(), 3);
        assert_eq!(summary.revision, 1);
        let kinds: Vec<&str> = summary
            .cells
            .iter()
            .map(|cell| cell.kind.as_str())
            .collect();
        assert_eq!(kinds, vec!["plan", "epic", "question"]);
        for cell in &summary.cells {
            assert_eq!(cell.rule, format!("gates.{}", cell.kind));
            if cell.glyph == "✓" {
                assert_ne!(cell.effect, "Waits for you");
            } else {
                assert_eq!(cell.glyph, "✋");
                assert_eq!(cell.effect, "Waits for you");
            }
        }
    }
    // Exact effect wording from the UX baseline.
    let standard = autonomy_summary(&resolve(Some("")));
    assert_eq!(
        standard.cells[0].effect,
        "Tales: approve + archive, then implement"
    );
    assert_eq!(standard.cells[1].effect, "Epics: archive + launch workers");
    assert_eq!(standard.cells[2].effect, "Questions: take the first option");
    // Sentences are stable snapshots.
    assert_eq!(
        autonomy_summary(&resolve(None)).sentence,
        "Manual: every plan, epic, and question waits for you."
    );
    assert_eq!(
        standard.sentence,
        "Standard: tales approve + archive; epics launch; questions take \
         the first option."
    );
    assert_eq!(
        autonomy_summary(&resolve(Some("tale"))).sentence,
        "Tale: tales approve + archive; epic plans wait; questions take \
         the first option."
    );
    assert_eq!(
        autonomy_summary(&resolve(Some("epic"))).sentence,
        "Epic: tale plans wait; epics launch; questions take the first \
         option."
    );
    // The catalog mirrors the summary builders.
    let catalog = autonomy_profiles();
    assert_eq!(catalog.len(), 4);
    for entry in &catalog {
        assert_eq!(entry.layer, "builtin");
        let summary = autonomy_summary(&resolve(match entry.name.as_str() {
            "manual" => None,
            "standard" => Some(""),
            "tale" => Some("tale"),
            _ => Some("epic"),
        }));
        assert_eq!(entry.cells, summary.cells, "profile {}", entry.name);
        assert_eq!(entry.oneliner, summary.short, "profile {}", entry.name);
    }
    assert_eq!(
        catalog
            .iter()
            .map(|entry| (entry.name.as_str(), entry.kind.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("manual", "manual"),
            ("standard", "default"),
            ("tale", "compatibility"),
            ("epic", "compatibility"),
        ]
    );
}

#[test]
fn decision_sentences_render_both_examples() {
    use super::sentences::autonomy_decision_sentence;
    use super::wires::AutonomyDecisionSentenceContextWire;
    let plan = AutonomyDecisionSentenceContextWire {
        gate_kind: "plan".to_string(),
    };
    let epic = AutonomyDecisionSentenceContextWire {
        gate_kind: "epic_plan".to_string(),
    };
    assert_eq!(
        autonomy_decision_sentence(&decide(Some(""), "plan"), &plan),
        "✓ tale approved + archived · standard · gates.plan"
    );
    assert_eq!(
        autonomy_decision_sentence(&decide(Some("tale"), "epic_plan"), &epic),
        "✋ epic plan waits for you · tale · gates.epic"
    );
    let question = AutonomyDecisionSentenceContextWire {
        gate_kind: "question".to_string(),
    };
    assert_eq!(
        autonomy_decision_sentence(
            &decide(Some("epic"), "question"),
            &question
        ),
        "✓ question answered with the first option · epic · gates.question"
    );
    assert_eq!(
        autonomy_decision_sentence(&decide(None, "question"), &question),
        "✋ question waits for you · manual · gates.question"
    );
}

#[test]
fn awareness_text_renders_tale_snapshot_and_manual_none() {
    use super::sentences::autonomy_awareness_text;
    assert_eq!(autonomy_awareness_text(&resolve(None)), None);
    assert_eq!(
        autonomy_awareness_text(&resolve(Some("tale"))).as_deref(),
        Some(
            "SASE autonomy: tale (advisory; covers host checkpoints only, \
             your shell is not restricted)\n\
             - Tale plans: approved and archived automatically, then \
             implemented without review.\n\
             - Epic plans: wait for a human review before anything launches.\n\
             - Questions: answered automatically with each question's first \
             option; no human reads them, so put your recommended option \
             first.\n\
             - Launch, sudo, and custom gates: wait for a human."
        )
    );
    for selection in [Some(""), Some("epic")] {
        let text = autonomy_awareness_text(&resolve(selection)).expect("block");
        assert_eq!(text.lines().count(), 5);
        assert!(text
            .ends_with("- Launch, sudo, and custom gates: wait for a human."));
    }
}

#[test]
fn mutate_covers_every_status() {
    use super::mutate::mutate_autonomy;
    use super::wires::AutonomyMutateRequestWire;
    let agent = AutonomyActorWire {
        kind: "agent".to_string(),
        surface: "prompt".to_string(),
        principal: "sase.x".to_string(),
    };
    let human = AutonomyActorWire {
        kind: "human".to_string(),
        surface: "tui".to_string(),
        principal: "bryan.zeus".to_string(),
    };
    let mutate = |record: &AutonomyRecordWire,
                  selection: &str,
                  expected: Option<u64>,
                  actor: &AutonomyActorWire| {
        mutate_autonomy(
            record,
            &AutonomyMutateRequestWire {
                selection: selection.to_string(),
                expected_revision: expected,
                actor: actor.clone(),
                now: "2026-10-09T12:00:00Z".to_string(),
            },
        )
        .expect("actor valid")
    };
    // Stale: the expected revision differs.
    let tale = resolve(Some("tale"));
    let stale = mutate(&tale, "manual", Some(99), &human);
    assert_eq!(stale.status, "stale");
    assert_eq!(stale.record.revision, 1);
    // Unchanged: the same selection keeps its revision.
    let same = mutate(&tale, "tale", Some(1), &human);
    assert_eq!(same.status, "unchanged");
    assert_eq!(same.record.revision, 1);
    // Agent widening is refused: tale -> standard would widen epic.
    let refused = mutate(&tale, "", None, &agent);
    assert_eq!(refused.status, "refused");
    assert_eq!(refused.record.revision, 1);
    assert!(refused.reason.contains("epic"), "{}", refused.reason);
    // Agent narrowing is allowed: tale -> manual.
    let narrowed = mutate(&tale, "manual", None, &agent);
    assert_eq!(narrowed.status, "applied");
    assert_eq!(narrowed.record.profile, "manual");
    assert_eq!(narrowed.record.revision, 2);
    assert_eq!(
        narrowed.record.last,
        Some(AutonomyLastWire {
            profile: "tale".to_string(),
            selection: "tale".to_string(),
        })
    );
    // Human widening is allowed.
    let widened = mutate(&tale, "", None, &human);
    assert_eq!(widened.status, "applied");
    assert_eq!(widened.record.profile, "standard");
    assert_eq!(widened.record.revision, 2);
    assert_eq!(
        widened.record.updated_by.as_ref().expect("actor").kind,
        "human"
    );
    // Restore with a last brings it back; without one gives standard.
    let restored = mutate(&narrowed.record, "restore", None, &human);
    assert_eq!(restored.status, "applied");
    assert_eq!(restored.record.profile, "tale");
    assert_eq!(restored.record.selection, "tale");
    let fresh_manual = resolve(None);
    let restored_default = mutate(&fresh_manual, "restore", None, &human);
    assert_eq!(restored_default.status, "applied");
    assert_eq!(restored_default.record.profile, "standard");
    // Full `%auto:` spellings are accepted.
    let spelled = mutate(&fresh_manual, "%auto:epic", None, &human);
    assert_eq!(spelled.status, "applied");
    assert_eq!(spelled.record.profile, "epic");
    // Unknown spellings are refused, never applied.
    let bogus = mutate(&fresh_manual, "bogus", None, &human);
    assert_eq!(bogus.status, "refused");
    assert_eq!(bogus.record.revision, 1);
}

#[test]
fn inherit_keeps_narrows_or_refuses() {
    use super::mutate::autonomy_inherit;
    let host = AutonomyActorWire {
        kind: "host".to_string(),
        surface: "successor".to_string(),
        principal: "sase-host".to_string(),
    };
    // Pure inheritance carries policy/profile/selection/last unchanged.
    let tale = resolve(Some("tale"));
    let inherited =
        autonomy_inherit(&tale, "sase.1", None, &host, "2026-10-09T12:00Z")
            .expect("actor valid");
    assert_eq!(inherited.status, "inherited");
    assert_eq!(inherited.record.profile, "tale");
    assert_eq!(inherited.record.selection, "tale");
    assert_eq!(inherited.record.policy, tale.policy);
    assert_eq!(inherited.record.last, tale.last);
    assert_eq!(inherited.record.source, "inherited");
    assert_eq!(inherited.record.inherited_from.as_deref(), Some("sase.1"));
    assert_eq!(inherited.record.revision, 1);
    // Explicit narrowing applies as an agent actor.
    let narrowed = autonomy_inherit(&tale, "sase.1", Some("manual"), &host, "")
        .expect("actor valid");
    assert_eq!(narrowed.status, "narrowed");
    assert_eq!(narrowed.record.profile, "manual");
    assert_eq!(narrowed.record.revision, 2);
    assert_eq!(
        narrowed.record.last,
        Some(AutonomyLastWire {
            profile: "tale".to_string(),
            selection: "tale".to_string(),
        })
    );
    // Explicit widening keeps the inherited record with its reason.
    let refused = autonomy_inherit(&tale, "sase.1", Some(""), &host, "")
        .expect("actor valid");
    assert_eq!(refused.status, "refused");
    assert_eq!(refused.record.profile, "tale");
    assert_eq!(refused.record.revision, 1);
    assert!(refused.reason.contains("kept the inherited record"));
    // A matching explicit selection stays inherited without a bump.
    let matched = autonomy_inherit(&tale, "sase.1", Some("tale"), &host, "")
        .expect("actor valid");
    assert_eq!(matched.status, "inherited");
    assert_eq!(matched.record.revision, 1);
}

#[test]
fn decision_log_round_trips_filters_and_rotates() {
    use super::decision_log::{
        append_autonomy_decision_with_cap, read_autonomy_decisions,
    };
    use super::wires::{AutonomyLogEntryWire, AutonomyLogQueryWire};
    let temp = tempfile::tempdir().expect("temp home");
    let home = temp.path();
    // An empty store reads empty.
    let empty = read_autonomy_decisions(home, &AutonomyLogQueryWire::default())
        .expect("read");
    assert!(empty.is_empty());
    let entry = |agent: &str, kind: &str, at: &str| AutonomyLogEntryWire {
        schema_version: 1,
        at: at.to_string(),
        agent: agent.to_string(),
        agent_session: "sase.sess".to_string(),
        project: "sase".to_string(),
        gate_kind: kind.to_string(),
        gate_id: format!("{agent}-{kind}"),
        creator_role: "top_level".to_string(),
        decision: decide(Some("tale"), kind),
    };
    append_autonomy_decision_with_cap(
        home,
        &entry("a", "plan", "2026-10-09T10:00:00Z"),
        1024,
    )
    .expect("append");
    append_autonomy_decision_with_cap(
        home,
        &entry("b", "epic_plan", "2026-10-09T11:00:00Z"),
        1024,
    )
    .expect("append");
    // Newest first.
    let all = read_autonomy_decisions(home, &AutonomyLogQueryWire::default())
        .expect("read");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].agent, "b");
    assert_eq!(all[1].agent, "a");
    // Filters.
    let query = |query: AutonomyLogQueryWire| {
        read_autonomy_decisions(home, &query).expect("read")
    };
    assert_eq!(
        query(AutonomyLogQueryWire {
            agent: Some("a".to_string()),
            ..Default::default()
        })
        .len(),
        1
    );
    assert_eq!(
        query(AutonomyLogQueryWire {
            since: Some("2026-10-09T10:30:00Z".to_string()),
            ..Default::default()
        })
        .len(),
        1
    );
    assert_eq!(
        query(AutonomyLogQueryWire {
            limit: Some(1),
            ..Default::default()
        })
        .len(),
        1
    );
    assert!(!query(AutonomyLogQueryWire {
        outcome: Some("auto".to_string()),
        ..Default::default()
    })
    .is_empty());
    // A torn last line does not lose the segment.
    let live = home.join("autonomy").join("decisions.jsonl");
    {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&live)
            .expect("open");
        file.write_all(b"{\"torn\": ").expect("write");
    }
    assert_eq!(
        read_autonomy_decisions(home, &AutonomyLogQueryWire::default())
            .expect("read")
            .len(),
        2
    );
    // Rotation keeps one prior segment and both stay readable.
    append_autonomy_decision_with_cap(
        home,
        &entry("c", "plan", "2026-10-09T12:00:00Z"),
        100,
    )
    .expect("append");
    assert!(home.join("autonomy").join("decisions.jsonl.1").exists());
    let rotated =
        read_autonomy_decisions(home, &AutonomyLogQueryWire::default())
            .expect("read");
    assert!(rotated.iter().any(|item| item.agent == "c"));
    assert!(rotated.iter().any(|item| item.agent == "a"));
}
