//! Hand-written artifact-text fixtures, one per state this layer owns.
//!
//! Every fixture builds real artifact text (plan JSON resolved through
//! `resolve_finalizer_plan`, tolerant JSONL, schema-v1 result JSON) and
//! projects it through `project_finalizer_node_view`.

use serde_json::{json, Value as JsonValue};

use super::super::selection::resolve_finalizer_plan;
use super::super::wire::{
    FinalizerInstanceSpecWire, FinalizerPlanInputWire, FinalizerPlanWire,
    FinalizerSelectorOpWire, FINALIZER_WIRE_SCHEMA_VERSION,
};
use super::detail::project_finalizer_node_view;
use super::wire::{
    FinalizerNodeViewRequestWire, FinalizerNodeViewWire, RunViewRunInputWire,
    RunViewRunKindWire, RunViewTextInputWire, RUN_VIEW_WIRE_SCHEMA_VERSION,
};

fn spec(instance_id: &str, after: &[&str]) -> FinalizerInstanceSpecWire {
    serde_json::from_value(json!({
        "schema_version": FINALIZER_WIRE_SCHEMA_VERSION,
        "instance_id": instance_id,
        "provider_ref": format!("builtin@{instance_id}"),
        "after": after,
    }))
    .unwrap()
}

fn resolve(input: &FinalizerPlanInputWire) -> FinalizerPlanWire {
    resolve_finalizer_plan(input).unwrap()
}

fn standard_plan() -> FinalizerPlanWire {
    resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![spec("commit", &[]), spec("check", &["commit"])],
        defaults: vec!["commit".to_string()],
        required: vec!["check".to_string()],
        selectors: Vec::new(),
    })
}

fn text_input(text: &str) -> RunViewTextInputWire {
    RunViewTextInputWire {
        text: Some(text.to_string()),
        size: text.len() as u64,
        too_large: false,
    }
}

fn empty_input() -> RunViewTextInputWire {
    RunViewTextInputWire {
        text: None,
        size: 0,
        too_large: false,
    }
}

fn base_run(plan: &FinalizerPlanWire) -> RunViewRunInputWire {
    RunViewRunInputWire {
        run_id: "run-1".to_string(),
        number: 1,
        label: "agent turn".to_string(),
        kind: RunViewRunKindWire::Agent,
        turn_terminal: false,
        runner_live: None,
        agent_meta: empty_input(),
        plan: text_input(&serde_json::to_string(plan).unwrap()),
        authority_plan: empty_input(),
        context: empty_input(),
        submission: empty_input(),
        submission_attempts: empty_input(),
        journal: empty_input(),
        result: empty_input(),
        instances: Vec::new(),
        recovery_files: Vec::new(),
    }
}

fn project_one(run: &RunViewRunInputWire) -> FinalizerNodeViewWire {
    let request = FinalizerNodeViewRequestWire {
        schema_version: RUN_VIEW_WIRE_SCHEMA_VERSION,
        runs: vec![run.clone()],
        tail_lines: 12,
    };
    project_finalizer_node_view(&request).unwrap()
}

fn run_status(view: &FinalizerNodeViewWire) -> &str {
    match view.runs[0].disposition {
        super::wire::RunViewDispositionWire::Active => "active",
        super::wire::RunViewDispositionWire::Ran => "ran",
        super::wire::RunViewDispositionWire::Skipped => "skipped",
        super::wire::RunViewDispositionWire::NotReached => "not_reached",
        super::wire::RunViewDispositionWire::Interrupted => "interrupted",
        super::wire::RunViewDispositionWire::Unavailable => "unavailable",
    }
}

fn instance_statuses(view: &FinalizerNodeViewWire) -> Vec<(String, String)> {
    view.runs[0]
        .instances
        .iter()
        .map(|instance| (instance.instance_id.clone(), instance.status.clone()))
        .collect()
}

fn journal_text(lines: &[JsonValue]) -> String {
    lines
        .iter()
        .map(|line| serde_json::to_string(line).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn journal_line(seq: u32, event: &str, extra: JsonValue) -> JsonValue {
    let mut line = json!({"v": 1, "seq": seq, "t": 1727440000.0 + seq as f64, "event": event});
    for (key, value) in extra.as_object().unwrap() {
        line[key] = value.clone();
    }
    line
}

fn result_text(
    status: &str,
    instances: &[(&str, &str)],
    cycles: u32,
) -> String {
    json!({
        "schema_version": 1,
        "status": status,
        "cycles": cycles,
        "instances": instances.iter().map(|(id, instance_status)| {
            json!({
                "instance_id": id,
                "status": instance_status,
                "attempts": [{"attempt": 1, "status": instance_status}],
            })
        }).collect::<Vec<_>>(),
        "diagnostics": [],
    })
    .to_string()
}

fn authority_text(
    plan: &FinalizerPlanWire,
    configured: &[(&str, &str)],
    defaults: &[&str],
    required: &[&str],
) -> String {
    json!({
        "plan": serde_json::to_value(plan).unwrap(),
        "config_snapshot": {
            "schema_version": 1,
            "config": {
                "instances": configured.iter().map(|(id, provider_ref)| {
                    json!({"instance_id": id, "provider_ref": provider_ref})
                }).collect::<Vec<_>>(),
                "defaults": defaults,
                "required": required,
            },
        },
    })
    .to_string()
}

#[test]
fn planned_run_on_a_live_turn_is_active() {
    let plan = standard_plan();
    let view = project_one(&base_run(&plan));
    assert_eq!(run_status(&view), "active");
    assert_eq!(view.runs[0].reason.as_deref(), Some("planned"));
    assert_eq!(view.status, "running");
    assert_eq!(
        instance_statuses(&view),
        vec![
            ("commit".to_string(), "planned".to_string()),
            ("check".to_string(), "planned".to_string()),
        ]
    );
}

#[test]
fn handoff_skip_is_skipped_with_its_kind() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.journal = text_input(&journal_text(&[journal_line(
        1,
        "phase_skipped",
        json!({"reason": "handoff:plan"}),
    )]));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "skipped");
    assert_eq!(
        view.runs[0].reason.as_deref(),
        Some("skipped · handoff:plan")
    );
    assert_eq!(view.status, "skipped");
    assert!(view.runs[0]
        .instances
        .iter()
        .all(|instance| instance.status == "skipped"));
}

#[test]
fn sealed_plan_without_artifacts_on_a_terminal_turn_is_not_reached() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    let view = project_one(&run);
    assert_eq!(run_status(&view), "not_reached");
    assert_eq!(view.status, "not_reached");
}

#[test]
fn legacy_meta_hints_never_change_the_disposition() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.agent_meta = text_input(
        &json!({
            "finalizer_status": {
                "schema_version": 1,
                "phase": "settled",
                "status": "success",
            },
            "finalizers_drift": [],
        })
        .to_string(),
    );
    let view = project_one(&run);
    assert_eq!(run_status(&view), "not_reached");
}

#[test]
fn open_journal_with_a_dead_runner_is_interrupted() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.runner_live = Some(false);
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "instance_started", json!({"instance_id": "commit"})),
    ]));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "interrupted");
    assert_eq!(view.runs[0].reason.as_deref(), Some("runner dead"));
    assert_eq!(
        instance_statuses(&view),
        vec![
            ("commit".to_string(), "interrupted".to_string()),
            ("check".to_string(), "planned".to_string()),
        ]
    );
    assert_eq!(view.attention_instance_id.as_deref(), Some("commit"));
    assert!(view.run_level_trouble);
}

#[test]
fn truncated_open_journal_is_interrupted() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.runner_live = Some(true);
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "observability_truncated", json!({})),
    ]));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "interrupted");
    assert_eq!(
        view.runs[0].reason.as_deref(),
        Some("observability truncated")
    );
}

#[test]
fn authority_drift_makes_the_run_unavailable() {
    let plan = standard_plan();
    let other = resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![spec("commit", &[]), spec("tasks", &[])],
        defaults: vec!["commit".to_string(), "tasks".to_string()],
        required: Vec::new(),
        selectors: Vec::new(),
    });
    let mut run = base_run(&plan);
    run.authority_plan = text_input(&authority_text(
        &other,
        &[("commit", "builtin@commit"), ("tasks", "builtin@tasks")],
        &["commit", "tasks"],
        &[],
    ));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "unavailable");
    assert_eq!(
        view.runs[0].reason.as_deref(),
        Some("plan drifted from host-owned authority")
    );
    assert!(view.run_level_trouble);
}

#[test]
fn oversized_result_names_its_raw_path() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.result = RunViewTextInputWire {
        text: None,
        size: 0,
        too_large: true,
    };
    let view = project_one(&run);
    assert_eq!(run_status(&view), "unavailable");
    assert_eq!(
        view.runs[0].reason.as_deref(),
        Some("too large: finalizer_result.json")
    );
}

#[test]
fn terminal_result_is_authoritative_for_outcomes() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.result = text_input(&result_text(
        "failed",
        &[("commit", "success"), ("check", "failed")],
        1,
    ));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "ran");
    assert_eq!(view.runs[0].result_status.as_deref(), Some("failed"));
    assert_eq!(view.status, "failed");
    assert_eq!(
        instance_statuses(&view),
        vec![
            ("commit".to_string(), "success".to_string()),
            ("check".to_string(), "failed".to_string()),
        ]
    );
    assert_eq!(view.attention_instance_id.as_deref(), Some("check"));
    assert_eq!(view.runs[0].cycles, 1);
}

#[test]
fn zero_attempts_with_no_trigger_is_not_triggered() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.context = text_input(
        &json!({
            "schema_version": 2,
            "run_id": "run-1",
            "agent_id": "agent-1",
            "turn_nonce": "nonce-1",
            "plan_digest": plan.plan_digest,
            "requirements": [
                {"instance_id": "commit", "trigger": "always", "submission_required": false},
                {"instance_id": "check", "trigger": "not_triggered", "submission_required": false},
            ],
            "obligations": [],
        })
        .to_string(),
    );
    run.result = text_input(
        &json!({
            "schema_version": 1,
            "status": "success",
            "cycles": 1,
            "instances": [
                {"instance_id": "commit", "status": "success",
                 "attempts": [{"attempt": 1, "status": "success"}]},
                {"instance_id": "check", "status": "pending", "attempts": []},
            ],
            "diagnostics": [],
        })
        .to_string(),
    );
    let view = project_one(&run);
    assert_eq!(
        instance_statuses(&view),
        vec![
            ("commit".to_string(), "success".to_string()),
            ("check".to_string(), "not_triggered".to_string()),
        ]
    );
    assert_eq!(
        view.runs[0].instances[1].trigger_kind.as_deref(),
        Some("not_triggered")
    );
}

#[test]
fn two_cycle_reactivation_counts_earlier_segments() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.journal = text_input(&journal_text(&[
        journal_line(
            1,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(2, "cycle_started", json!({"cycle": 1})),
        journal_line(
            3,
            "phase_finished",
            json!({"status": "failed", "cycles": 1}),
        ),
        journal_line(
            4,
            "phase_started",
            json!({"run_id": "run-1", "mode": "normal"}),
        ),
        journal_line(5, "cycle_started", json!({"cycle": 2})),
        journal_line(
            6,
            "phase_finished",
            json!({"status": "success", "cycles": 2}),
        ),
    ]));
    run.result = text_input(&result_text(
        "success",
        &[("commit", "success"), ("check", "success")],
        2,
    ));
    let view = project_one(&run);
    assert_eq!(run_status(&view), "ran");
    assert_eq!(view.runs[0].cycles, 2);
    assert_eq!(view.runs[0].earlier_segments, 1);
    assert!(view.runs[0].reactivated);
}

#[test]
fn failed_check_blocks_tasks_downstream() {
    let plan = resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![
            spec("commit", &[]),
            spec("check", &["commit"]),
            spec("tasks", &["check"]),
        ],
        defaults: vec!["commit".to_string()],
        required: vec!["check".to_string(), "tasks".to_string()],
        selectors: Vec::new(),
    });
    let mut run = base_run(&plan);
    run.turn_terminal = true;
    run.submission_attempts = text_input(
        &json!({"v": 1, "t": 1727440001.0, "status": "accepted", "code": "ok",
                "message": "land commit and check", "payload_count": 2})
        .to_string(),
    );
    run.result = text_input(&result_text(
        "failed",
        &[("commit", "success"), ("check", "failed")],
        1,
    ));
    let view = project_one(&run);
    assert_eq!(
        instance_statuses(&view),
        vec![
            ("commit".to_string(), "success".to_string()),
            ("check".to_string(), "failed".to_string()),
            ("tasks".to_string(), "not_run".to_string()),
        ]
    );
    assert_eq!(
        view.runs[0].instances[2].blocked_by.as_deref(),
        Some("check")
    );
    assert_eq!(view.runs[0].declarations.len(), 1);
    assert_eq!(view.runs[0].declarations[0].status, "accepted");
    assert_eq!(
        view.runs[0].declarations[0].first_line.as_deref(),
        Some("land commit and check")
    );
}

#[test]
fn unselected_lint_reports_its_selector_reason() {
    let plan = resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![
            spec("commit", &[]),
            spec("check", &["commit"]),
            spec("lint", &[]),
        ],
        defaults: vec![
            "commit".to_string(),
            "check".to_string(),
            "lint".to_string(),
        ],
        required: Vec::new(),
        selectors: vec![FinalizerSelectorOpWire::Remove {
            instance_id: "lint".to_string(),
        }],
    });
    let mut run = base_run(&plan);
    run.authority_plan = text_input(&authority_text(
        &plan,
        &[
            ("commit", "builtin@commit"),
            ("check", "builtin@check"),
            ("lint", "builtin@lint"),
        ],
        &["commit", "check", "lint"],
        &[],
    ));
    let view = project_one(&run);
    assert_eq!(view.unselected.len(), 1);
    assert_eq!(view.unselected[0].instance_id, "lint");
    assert_eq!(view.unselected[0].reason, "%final:!lint");
    let reasons: Vec<(String, String)> = view.runs[0]
        .instances
        .iter()
        .map(|instance| {
            (
                instance.instance_id.clone(),
                instance.selection_reason.clone().unwrap_or_default(),
            )
        })
        .collect();
    assert!(reasons.contains(&("commit".to_string(), "default".to_string())));
}

#[test]
fn cleared_plan_reports_final_none() {
    let plan = resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![spec("commit", &[]), spec("check", &[])],
        defaults: vec!["commit".to_string(), "check".to_string()],
        required: Vec::new(),
        selectors: vec![FinalizerSelectorOpWire::Clear],
    });
    let mut run = base_run(&plan);
    run.authority_plan = text_input(&authority_text(
        &plan,
        &[("commit", "builtin@commit"), ("check", "builtin@check")],
        &["commit", "check"],
        &[],
    ));
    let view = project_one(&run);
    assert!(view.runs[0].instances.is_empty());
    assert_eq!(view.unselected.len(), 2);
    assert!(view
        .unselected
        .iter()
        .all(|entry| entry.reason == "%final:none"));
}

#[test]
fn non_default_instance_reports_not_default() {
    let plan = resolve(&FinalizerPlanInputWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        instances: vec![spec("commit", &[]), spec("lint", &[])],
        defaults: vec!["commit".to_string()],
        required: Vec::new(),
        selectors: Vec::new(),
    });
    let mut run = base_run(&plan);
    run.authority_plan = text_input(&authority_text(
        &plan,
        &[("commit", "builtin@commit"), ("lint", "builtin@lint")],
        &["commit"],
        &[],
    ));
    let view = project_one(&run);
    assert_eq!(view.unselected.len(), 1);
    assert_eq!(view.unselected[0].instance_id, "lint");
    assert_eq!(view.unselected[0].reason, "not default");
}

#[test]
fn config_values_never_reach_the_serialized_response() {
    let plan = standard_plan();
    let sentinel = "SENTINEL-SECRET-9f8e7d6c5b";
    let mut run = base_run(&plan);
    run.authority_plan = text_input(
        &json!({
            "plan": serde_json::to_value(&plan).unwrap(),
            "config_snapshot": {
                "schema_version": 1,
                "config": {
                    "instances": [
                        {"instance_id": "commit",
                         "provider_ref": "builtin@commit",
                         "config": {"api_key": sentinel}},
                        {"instance_id": "check",
                         "provider_ref": "builtin@check"},
                    ],
                    "defaults": ["commit", "check"],
                    "required": ["check"],
                },
            },
        })
        .to_string(),
    );
    let view = project_one(&run);
    let encoded = serde_json::to_string(&view).unwrap();
    assert!(!encoded.contains(sentinel), "config value leaked");
    assert!(encoded.contains("builtin@commit"));
}

#[test]
fn recovery_files_surface_the_recovery_turn_and_trouble() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.recovery_files =
        vec!["final_declaration_recovery_prompt.md".to_string()];
    let view = project_one(&run);
    assert!(view.runs[0].recovery_turn.is_some());
    assert!(view.run_level_trouble);
}

#[test]
fn drift_entries_surface_and_raise_trouble() {
    let plan = standard_plan();
    let mut run = base_run(&plan);
    run.agent_meta = text_input(
        &json!({
            "finalizers_drift": [
                {"instance_id": "commit", "code": "plan_config_drift",
                 "message": "max_attempts drifted after seal"},
            ],
        })
        .to_string(),
    );
    let view = project_one(&run);
    assert_eq!(view.runs[0].drift.len(), 1);
    assert_eq!(
        view.runs[0].drift[0].code.as_deref(),
        Some("plan_config_drift")
    );
    assert!(view.run_level_trouble);
}
