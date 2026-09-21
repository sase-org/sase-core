use super::*;
use crate::artifact_refs::{
    py_artifact_link_eligibility_wire_schema_version,
    py_artifact_link_release_evidence, py_decide_artifact_link_eligibility,
    py_validate_artifact_link_release_evidence,
};
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn artifact_link_eligibility_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "artifact_link_eligibility_wire_schema_version",
            "decide_artifact_link_eligibility",
            "artifact_link_release_evidence",
            "validate_artifact_link_release_evidence",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        assert_eq!(py_artifact_link_eligibility_wire_schema_version(), 1);

        let ineligible_request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "run_id": "run-1",
                    "agent_id": "agent-1",
                    "repos": [
                        {
                            "repo_id": "sdd:plan",
                            "kind": "sdd",
                            "changed_paths": [
                                {"path": "links/plan/foo.md.json", "role": "bookkeeping"}
                            ],
                        }
                    ],
                }),
            )
            .unwrap();
        let ineligible_decision_obj = py_decide_artifact_link_eligibility(
            py,
            ineligible_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let ineligible_decision_dict = ineligible_decision_obj
            .bind(py)
            .downcast::<PyDict>()
            .unwrap();
        let ineligible_decision_value =
            py_to_json_value(ineligible_decision_obj.bind(py)).unwrap();
        assert_eq!(ineligible_decision_value["eligible"], json!(false));
        assert_eq!(ineligible_decision_value["qualifying_repo_ids"], json!([]));

        let eligible_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": "run-1",
                "agent_id": "agent-1",
                "repos": [
                    {
                        "repo_id": "main",
                        "kind": "main",
                        "changed_paths": [
                            {"path": "src/lib.rs", "role": "real"}
                        ],
                    }
                ],
            }),
        )
        .unwrap();
        let decision = py_decide_artifact_link_eligibility(
            py,
            eligible_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let decision_dict = decision.bind(py).downcast::<PyDict>().unwrap();
        let decision_value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(decision_value["eligible"], json!(true));
        assert_eq!(decision_value["qualifying_repo_ids"], json!(["main"]));

        let evidence = py_artifact_link_release_evidence(
            py,
            decision_dict,
            "2026-09-08T00:00:00Z",
        )
        .unwrap();
        let evidence_dict = evidence.bind(py).downcast::<PyDict>().unwrap();
        let evidence_value = py_to_json_value(evidence.bind(py)).unwrap();
        assert_eq!(evidence_value["run_id"], json!("run-1"));
        assert_eq!(evidence_value["agent_id"], json!("agent-1"));

        py_validate_artifact_link_release_evidence(
            evidence_dict,
            "run-1",
            "agent-1",
        )
        .unwrap();
        assert!(py_validate_artifact_link_release_evidence(
            evidence_dict,
            "run-2",
            "agent-1",
        )
        .is_err());

        assert!(py_artifact_link_release_evidence(
            py,
            ineligible_decision_dict,
            "2026-09-08T00:00:00Z",
        )
        .is_err());
    });
}
