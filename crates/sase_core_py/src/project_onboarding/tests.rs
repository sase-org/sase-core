use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn project_onboarding_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_onboarding(&module).unwrap();
        for name in [
            "project_onboarding_wire_schema_version",
            "classify_project_candidates",
            "evaluate_repo_creation_form",
            "plan_project_onboarding",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_project_onboarding_wire_schema_version(), 1);
    });
}

#[test]
fn classify_binding_round_trips_sidecar_hide() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "workflow_type": "gh",
                "sidecar_roles": ["plans", "beads"],
                "owned_namespaces": ["sase-org"],
                "namespaces": [],
                "remote": [{
                    "namespace": "sase-org",
                    "identity_key": "github.com/sase-org/sase--plans",
                    "ref": "sase-org/sase--plans",
                    "display": "sase-org/sase--plans",
                    "name": "sase--plans"
                }],
                "local": [],
                "projects": [],
                "inventory": [],
                "journals": []
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_classify_project_candidates(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["groups"], json!([]));
        assert_eq!(value["search_only"][0]["hidden_reason"], json!("sidecar"));
        assert_eq!(value["hidden"][0]["reason"], json!("sidecar"));
        assert_eq!(value["hidden"][0]["count"], json!(1));
    });
}

#[test]
fn form_binding_round_trips_template_excludes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "mode": "lenient",
                "values": {"template": "sase-org/template"},
                "form": {
                    "workflow_type": "gh",
                    "namespace": "sase-org",
                    "fields": [
                        {
                            "name": "add_readme",
                            "label": "README",
                            "kind": "bool",
                            "default": "true"
                        },
                        {
                            "name": "template",
                            "label": "Template",
                            "kind": "line",
                            "excludes": ["add_readme"]
                        }
                    ]
                }
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_evaluate_repo_creation_form(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["valid"], json!(true));
        assert_eq!(value["fields"][0]["name"], json!("add_readme"));
        assert_eq!(value["fields"][0]["available"], json!(false));
        assert_eq!(
            value["effective_options"]["template"],
            json!("sase-org/template")
        );
        assert!(value["effective_options"].get("add_readme").is_none());
    });
}

#[test]
fn classify_binding_rejects_bad_schema() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 9,
                "workflow_type": "gh"
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let error = py_classify_project_candidates(py, request).unwrap_err();
        assert!(error.to_string().contains("schema_version"));
    });
}

#[test]
fn plan_binding_round_trips_fresh_add() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "request": {
                    "mode": "add",
                    "actor": "human",
                    "target": {
                        "workflow_type": "gh",
                        "identity_key": "github.com/sase-org/bob",
                        "ref": "sase-org/bob",
                        "display": "sase-org/bob",
                        "checkout_dir": "/tmp/bob",
                        "project_key_base": "gh_sase-org__bob",
                        "project_name_base": "bob",
                        "storage_policy": "separate_repo"
                    },
                    "requested": {"setup": "auto", "make_current": false}
                },
                "facts": {
                    "remote": {
                        "status": "found",
                        "visibility": "private",
                        "can_push": true,
                        "sase_managed": false
                    },
                    "checkout": {"state": "absent"}
                }
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_plan_project_onboarding(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["mode"], json!("add"));
        assert_eq!(value["status"], json!("ready"));
        assert_eq!(value["setup"]["code"], json!("unmanaged_can_push"));
        assert_eq!(value["identity"]["key"], json!("gh_sase-org__bob"));
        assert_eq!(value["steps"][0]["id"], json!("checkout"));
        assert_eq!(value["steps"][0]["state"], json!("pending"));
        assert!(value["intent_fingerprint"].as_str().unwrap().len() == 64);
    });
}

#[test]
fn plan_binding_rejects_bad_schema() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 9,
                "request": {"mode": "add", "actor": "human"}
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let error = py_plan_project_onboarding(py, request).unwrap_err();
        assert!(error.to_string().contains("schema_version"));
    });
}
