use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;
use std::fs;

#[test]
fn git_object_sharing_binding_preserves_existing_reuse() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let objects = temp.path().join("borrower/.git/objects");
    let old = temp.path().join("old/.git/objects");
    let primary = temp.path().join("primary/.git/objects");
    fs::create_dir_all(&old).unwrap();
    fs::create_dir_all(&primary).unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module
            .getattr("git_object_sharing_wire_schema_version")
            .is_ok());
        assert!(module.getattr("plan_git_object_sharing").is_ok());

        let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION,
                    "operation": "install",
                    "checkout_dir": temp.path().join("borrower").to_string_lossy(),
                    "object_dir": objects.to_string_lossy(),
                    "alternates_file": objects.join("info/alternates").to_string_lossy(),
                    "primary_checkout_dir": temp.path().join("primary").to_string_lossy(),
                    "primary_object_dir": primary.to_string_lossy(),
                    "alternates": [old.to_string_lossy()],
                    "config_enabled": true,
                    "config_primary_objects": old.to_string_lossy(),
                    "mutation_context": "existing_reuse"
                }),
            )
            .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_plan_git_object_sharing(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["action"], json!("none"));
        assert_eq!(result["status"], json!("preserved"));
        assert_eq!(result["dependency_mutation"], json!(false));
        assert_eq!(result["write_alternates"], JsonValue::Null);
    });
}

#[test]
fn vcs_log_binding_exposes_schema_and_parent_ids() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        use sase_core::vcs_log::parsers::{RECORD_SEP, UNIT_SEP};

        assert_eq!(py_vcs_log_wire_schema_version(), 4);

        let stdout = format!(
                "full{US}short{US}A{US}a@example.com{US}42{US}p1 p2{US}subject{US}body{RS}",
                US = UNIT_SEP,
                RS = RECORD_SEP,
            );
        let parsed = py_parse_git_log(py, &stdout).unwrap();
        let value = py_to_json_value(parsed.as_any()).unwrap();
        assert_eq!(
            value,
            json!([{
                "full_id": "full",
                "short_id": "short",
                "author_name": "A",
                "author_email": "a@example.com",
                "timestamp": 42,
                "parent_ids": ["p1", "p2"],
                "subject": "subject",
                "body": "body",
                "presence": "unknown",
                "origin": "manual",
            }])
        );
    });
}

#[test]
fn classify_commit_origin_binding_returns_origin_string() {
    assert_eq!(py_classify_commit_origin("fix: manual\n\nBody"), "manual",);
    assert_eq!(
        py_classify_commit_origin("fix: tracked\n\nSASE_TYPE=stitch"),
        "stitch",
    );
    assert_eq!(
        py_classify_commit_origin("fix: automatic\n\nSASE_TYPE=sase init"),
        "auto",
    );
}

#[test]
fn classify_commit_types_binding_returns_label_list() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let commit = PyDict::new_bound(py);
        commit.set_item("full_id", "full").unwrap();
        commit.set_item("short_id", "short").unwrap();
        commit.set_item("author_name", "A").unwrap();
        commit.set_item("author_email", "a@example.com").unwrap();
        commit.set_item("timestamp", 42).unwrap();
        commit.set_item("parent_ids", vec!["p1", "p2"]).unwrap();
        commit.set_item("subject", "Merge tracked work").unwrap();
        commit
            .set_item(
                "body",
                "Details\n\nSASE_TYPE=bead_work\nSASE_PATCH=feat-x",
            )
            .unwrap();
        commit.set_item("presence", "unknown").unwrap();
        commit.set_item("origin", "manual").unwrap();

        let labels = py_classify_commit_types(py, commit.as_any()).unwrap();
        let value = py_to_json_value(labels.as_any()).unwrap();
        assert_eq!(value, json!(["automatic", "bead_work", "merge", "patch"]));
    });
}

#[test]
fn parse_merge_summary_binding_returns_dict_or_none() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let summary = py_parse_merge_summary(
            py,
            "Merge pull request #123 from org/feature",
            "\nFeature title\n\nDetails",
        )
        .unwrap();
        let value = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(
            value,
            json!({
                "kind": "pull_request",
                "reference": "123",
                "source": "org/feature",
                "target": null,
                "headline": "Feature title",
            })
        );

        assert!(py_parse_merge_summary(py, "Merge unknown shape", "")
            .unwrap()
            .is_none(py));
    });
}

#[test]
fn commit_footer_bindings_convert_linked_payloads() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_commit_footer_wire_schema_version(),
            COMMIT_FOOTER_WIRE_SCHEMA_VERSION
        );
        let updates = PyList::empty_bound(py);
        append_json(
            py,
            &updates,
            json!({
                "key": "PLAN",
                "label": "202607/p.md",
                "destination": "https://github.com/o/r/blob/main/202607/p.md",
                "reference_id": null
            }),
        );
        let message =
            py_update_commit_footer("Subject", &updates, vec![]).unwrap();
        assert!(message.contains("SASE_PLAN=[202607/p.md][1]"));

        let parsed = py_parse_commit_footer(py, &message).unwrap();
        let value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["tags"][0]["label"], json!("202607/p.md"));
        assert_eq!(
            value["tags"][0]["destination"],
            json!("https://github.com/o/r/blob/main/202607/p.md")
        );
    });
}

#[test]
fn commit_subject_bindings_round_trip_wire_payload() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_commit_subject_wire_schema_version(),
            COMMIT_SUBJECT_WIRE_SCHEMA_VERSION
        );
        let allowed_types = py_default_commit_subject_types();
        assert_eq!(allowed_types.first().map(String::as_str), Some("build"));

        let parsed = py_parse_commit_subject(
            py,
            "feat(binding)!: expose subject parser\n\nBody",
            allowed_types,
        )
        .unwrap();
        let value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(
            value["subject"],
            json!("feat(binding)!: expose subject parser")
        );
        assert_eq!(value["valid"], json!(true));
        assert_eq!(value["exempt"], json!(false));
        assert_eq!(value["commit_type"], json!("feat"));
        assert_eq!(value["scope"], json!("binding"));
        assert_eq!(value["breaking"], json!(true));
        assert_eq!(value["description"], json!("expose subject parser"));
        assert_eq!(value["violation"], JsonValue::Null);
        assert_eq!(value["found_type"], JsonValue::Null);
    });
}

#[test]
fn pending_commit_checkpoint_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert_eq!(
            module
                .getattr("pending_commit_checkpoint_wire_schema_version")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<u32>()
                .unwrap(),
            2
        );
        let request = json_value_to_py(
                py,
                &json!({
                    "checkpoint_present": true,
                    "repository_matches": true,
                    "subject_matches": true,
                    "payload_matches": true,
                    "checkpoint_method": "create_commit",
                    "accepted_action": "commit",
                    "checkpoint_payload_identity": "fix(final): reconcile commit declaration\n\nbody",
                    "accepted_payload_identity": "fix(final): reconcile commit declaration\n\nbody",
                    "checkpoint_run_id": "run-1",
                    "current_run_id": "run-1",
                    "checkpoint_agent_id": "agent-1",
                    "current_agent_id": "agent-1",
                    "has_operation_id": true,
                    "dispatch_completed": true,
                    "pending_after_hook": true,
                    "commit_sha_present": true
                }),
            )
            .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let decision = module
            .getattr("decide_pending_commit_checkpoint_recovery")
            .unwrap()
            .call1((request,))
            .unwrap();
        let decision = py_to_json_value(&decision).unwrap();
        assert_eq!(decision["action"], json!("resume"));
        assert_eq!(decision["schema_version"], json!(2));
    });
}

#[test]
fn sidecar_publication_binding_returns_plain_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_decide_sidecar_publication_after_push,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let value = module
            .getattr("decide_sidecar_publication_after_push")
            .unwrap()
            .call1((
                1_i32,
                "",
                "! [rejected] main -> main (fetch first)",
                1_u32,
            ))
            .unwrap();
        assert_eq!(
            py_to_json_value(&value).unwrap(),
            json!({
                "schema_version": 1,
                "action": "integrate_and_retry",
                "classification": "rejected_fetch_first",
                "reason": "git push was rejected by remote divergence; integrate upstream and retry",
                "attempt": 1,
                "max_attempts": 3,
                "retryable": true
            })
        );
    });
}
