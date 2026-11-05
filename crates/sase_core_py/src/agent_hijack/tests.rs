use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn dict<'py>(py: Python<'py>, value: serde_json::Value) -> Bound<'py, PyDict> {
    json_value_to_py(py, &value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

fn escalation_request() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "model_facts": {
            "alias": "large",
            "alias_trail": [],
            "origin": null,
            "provider": "claude",
            "model": "opus",
            "effort": "high",
        },
        "ladder": ["@large", "@xlarge"],
        "alias_candidates": {
            "@large": ["claude/opus@high", "codex/gpt-6.1-sol@xhigh"],
            "@xlarge": ["claude/opus@xhigh", "codex/gpt-6.1-sol@xhigh"],
        },
        "lineage_to_rung": null,
        "stalled_concrete": "claude/opus@high",
    })
}

fn continuation_request() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "original_prompt": "Do the thing.\n%model:@large\n",
        "hijacked_name": "sase-7f.2",
        "model_target": "claude/opus@xhigh",
        "evidence": {
            "run_duration_display": "47m",
            "last_tool": "Bash `just check`",
            "last_tool_age": "22m",
            "files_changed": 5,
            "insertions": 212,
            "deletions": 31,
            "snapshot_ref": "snap-abc",
            "model_display": "claude/opus@high",
        },
        "context_mode": "fork",
        "transcript_path": null,
        "guidance_requested": false,
        "stale_lock_files": [],
    })
}

#[test]
fn hijack_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_agent_hijack(&module).unwrap();
        assert!(module.getattr("agent_hijack_escalation_target").is_ok());
        assert!(module.getattr("agent_hijack_continuation_prompt").is_ok());
        assert!(module.getattr("strip_hijack_launch_directives").is_ok());
        assert!(module.getattr("normalize_hijack_rung").is_ok());
        assert!(module.getattr("hijack_recovery_preview").is_ok());
        assert!(module.getattr("hijack_recovery_reconcile").is_ok());
        assert!(module.getattr("hijack_recovery_effective_target").is_ok());
        assert!(module.getattr("hijack_recovery_validate_execution").is_ok());
        assert!(module.getattr("hijack_recovery_confirm").is_ok());
        assert!(module.getattr("hijack_recovery_rollback").is_ok());
        assert_eq!(py_agent_hijack_escalation_wire_schema_version(), 1);
        assert_eq!(py_agent_hijack_continuation_wire_schema_version(), 1);
        assert_eq!(py_hijack_recovery_preview_wire_schema_version(), 1);
        assert_eq!(py_hijack_recovery_reconcile_wire_schema_version(), 1);
        assert_eq!(py_hijack_recovery_execution_wire_schema_version(), 1);
    });
}

#[test]
fn escalation_binding_escalates() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = dict(py, escalation_request());
        let result = py_agent_hijack_escalation_target(py, &request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["available"], json!(true));
        assert_eq!(value["kind"], json!("escalate"));
        assert_eq!(value["to_rung"], json!("@xlarge"));
        assert_eq!(value["concrete"], json!("claude/opus@xhigh"));
    });
}

#[test]
fn continuation_binding_builds_prompt() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = dict(py, continuation_request());
        let result = py_agent_hijack_continuation_prompt(py, &request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let prompt = value.as_str().unwrap().to_owned();
        assert!(prompt.starts_with("%model:claude/opus@xhigh\n"));
        assert!(prompt.contains("#fork:sase-7f.2"));
        assert!(prompt.contains("Hijack continuation."));
        assert!(!prompt.contains("%model:@large"));
    });
}

#[test]
fn strip_binding_preserves_fenced_model() {
    assert_eq!(
        py_strip_hijack_launch_directives(
            "```\n%model:keep\n```\n%model:drop\nok"
        ),
        "```\n%model:keep\n```\nok",
    );
}

#[test]
fn hijack_bindings_are_registered_and_advance_records() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_agent_hijack(&module).unwrap();
        assert_eq!(py_agent_hijack_wire_schema_version(), 1);
        assert!(module.getattr("agent_hijack_eligibility").is_ok());

        let record = dict(
            py,
            json!({
                "schema_version": 1,
                "state": "requested",
                "revision": 0,
            }),
        );
        let event = dict(py, json!({"event": "begin_stop"}));
        let updated =
            py_advance_agent_hijack_record(py, &record, &event).unwrap();
        let value = py_to_json_value(updated.bind(py)).unwrap();
        assert_eq!(value["state"], json!("stopping"));
        assert_eq!(value["revision"], json!(1));
    });
}

#[test]
fn eligibility_and_done_status_bindings_share_core_rules() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let facts = dict(
            py,
            json!({
                "schema_version": 1,
                "is_local": true,
                "is_llm_agent": true,
                "workspace_num": 4,
                "status": "RUNNING",
                "lifecycle_phase": "provider_running",
                "has_hijack_capability": true,
                "pid": 42,
            }),
        );
        let decision = py_agent_hijack_eligibility(py, &facts).unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["eligible"], json!(true));

        let recovery =
            dict(py, json!({"state": "resolved", "model_changed": true}));
        assert_eq!(
            py_agent_hijack_status_for_done_marker(
                Some("hijacked"),
                Some(&recovery),
            )
            .unwrap()
            .as_deref(),
            Some("ESCALATED")
        );
    });
}

#[test]
fn preview_and_reconcile_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = dict(
            py,
            json!({
                "schema_version": 1,
                "hijack_id": "h-1",
                "revision": 4,
                "state": "held",
                "header": {"agent_name": "agent.1", "workspace_num": 4},
                "escalate": {
                    "schema_version": 1,
                    "available": true,
                    "kind": "escalate",
                    "from_rung": "@large",
                    "to_rung": "@xlarge",
                    "concrete": "claude/opus@xhigh",
                },
                "continue_target": "claude/opus@high",
            }),
        );
        let preview = py_hijack_recovery_preview(py, &request).unwrap();
        let value = py_to_json_value(preview.bind(py)).unwrap();
        assert_eq!(value["default_action"], json!("escalate"));
        assert_eq!(value["actions"][0]["target"], json!("claude/opus@xhigh"));

        let record = dict(
            py,
            json!({
                "schema_version": 1,
                "state": "resolving",
                "revision": 4,
                "reserved_successor_name": "agent.2",
                "resolver_pid": 99,
                "resolver_process_identity_token": "boot:1",
            }),
        );
        let facts = dict(
            py,
            json!({
                "schema_version": 1,
                "resolving_age_seconds": 90.0,
                "min_age_seconds": 60.0,
                "resolver": {
                    "pid_live": false,
                    "identity_matches": false,
                },
                "successor": {"kind": "absent"},
            }),
        );
        let decision =
            py_hijack_recovery_reconcile(py, &record, &facts).unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("return_to_held"));
        assert_eq!(value["code"], json!("absent"));
    });
}

#[test]
fn execution_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = dict(
            py,
            json!({
                "schema_version": 1,
                "action": "instruct",
                "stalled_concrete": "claude/opus@high",
                "explicit_model_override": "codex/gpt-x",
            }),
        );
        let result = py_hijack_recovery_effective_target(py, &request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["concrete_target"], json!("codex/gpt-x@high"));
        assert_eq!(value["model_changed"], json!(true));

        let record = dict(
            py,
            json!({
                "schema_version": 1,
                "state": "resolving",
                "revision": 4,
                "concrete_target_model": "claude/opus@high",
                "reserved_successor_name": "agent.2",
                "resolver_pid": 99,
                "resolver_process_identity_token": "boot:1",
                "launch_identity": "launch-1",
            }),
        );
        let facts = dict(
            py,
            json!({
                "schema_version": 1,
                "observed_resolver_pid": 99,
                "resolver_pid_live": true,
                "resolver_identity_matches": true,
                "pinned_available": true,
                "current_target": "claude/opus@high",
            }),
        );
        let decision = py_hijack_recovery_validate_execution(
            py,
            &record,
            "claude/opus@high",
            &facts,
        )
        .unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("proceed"));
        assert_eq!(value["mode"], json!("dispatch"));

        let facts = dict(
            py,
            json!({
                "schema_version": 1,
                "launch_identity_matches": true,
                "model_matches": true,
                "workspace_matches": true,
                "lineage_matches": true,
                "ack_bound": true,
                "ack_matches": true,
            }),
        );
        let decision = py_hijack_recovery_confirm(py, &record, &facts).unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("settle"));

        let facts = dict(
            py,
            json!({
                "schema_version": 1,
                "child_state": "dead",
                "custody": "restored",
                "request_state": "canceled",
            }),
        );
        let decision =
            py_hijack_recovery_rollback(py, &record, &facts).unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("return_to_held"));
    });
}
