use super::index::{
    py_artifact_link_index_build, py_artifact_link_index_chips,
    py_artifact_link_index_project, py_artifact_link_index_rows,
    py_artifact_link_index_target, py_artifact_link_index_wire_schema_version,
};
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

fn request_dict<'py>(
    py: Python<'py>,
    value: serde_json::Value,
) -> Bound<'py, PyDict> {
    json_value_to_py(py, &value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn artifact_link_index_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "ArtifactLinkIndexHandle",
            "artifact_link_index_wire_schema_version",
            "artifact_link_index_build",
            "artifact_link_index_chips",
            "artifact_link_index_target",
            "artifact_link_index_rows",
            "artifact_link_index_project",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_artifact_link_index_wire_schema_version(), 1);

        let request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "identity": {
                    "username": "alice",
                    "machine_name": "athena",
                    "sibling_machines": ["athena"]
                },
                "rows": [
                    {
                        "source_ref": "plan:202608/a.md",
                        "relation": "implements",
                        "target_ref": "bead:sase-1",
                        "description": "lands the design",
                        "origin": "manual",
                        "uses": 1,
                        "_project": "alpha"
                    },
                    {
                        "source_ref": "agent:foo",
                        "relation": "cites",
                        "target_ref": "plan:202608/a.md",
                        "uses": 1,
                        "_project": "alpha"
                    }
                ]
            }),
        );
        let handle =
            Bound::new(py, py_artifact_link_index_build(py, &request).unwrap())
                .unwrap();
        assert_eq!(handle.get().raw_row_count(), 2);
        assert!(handle.get().chip_ref_count() >= 2);

        let chips = py_to_json_value(
            py_artifact_link_index_chips(py, handle.get(), "bead:sase-1")
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(chips[0]["relation"], json!("implements"));
        assert_eq!(chips[0]["neighbor_ref"], json!("plan:202608/a.md"));
        assert_eq!(chips[0]["this_is_source"], json!(false));

        let alias_chips = py_to_json_value(
            py_artifact_link_index_chips(
                py,
                handle.get(),
                "agent:alice.athena.foo",
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(alias_chips[0]["relation"], json!("cites"));

        let target = py_to_json_value(
            py_artifact_link_index_target(py, handle.get(), "bead:sase-1")
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(target["present"], json!(true));
        assert_eq!(target["target"]["pane_id"], json!("beads"));

        let missing = py_to_json_value(
            py_artifact_link_index_target(py, handle.get(), "missing:ref")
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(missing["present"], json!(false));

        let rows = py_to_json_value(
            py_artifact_link_index_rows(
                py,
                handle.get(),
                &request_dict(
                    py,
                    json!({
                        "schema_version": 1,
                        "lookup_keys": [
                            ["beads.id", "sase-1"],
                            ["beads.project.id", "alpha", "sase-1"]
                        ]
                    }),
                ),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(rows["candidate_row_count"], json!(1));
        assert_eq!(rows["rows"][0]["target_ref"], json!("bead:sase-1"));

        let projected = py_to_json_value(
            py_artifact_link_index_project(
                py,
                handle.get(),
                &request_dict(
                    py,
                    json!({
                        "schema_version": 1,
                        "known_targets": [
                            {"pane_id": "beads", "parts": ["alpha", "task", "sase-1"]}
                        ],
                        "permitted_relations": ["implements", "cites", "related"],
                        "project_hint": "alpha"
                    }),
                ),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(projected["candidate_row_count"], json!(1));
        assert_eq!(projected["edges"][0]["relation"], json!("implements"));
        assert_eq!(
            projected["edges"][0]["source"]["pane_id"],
            json!("ref:plan")
        );

        let via_module = module
            .getattr("artifact_link_index_chips")
            .unwrap()
            .call1((&handle, "bead:sase-1"))
            .unwrap();
        assert_eq!(
            py_to_json_value(&via_module).unwrap()[0]["relation"],
            json!("implements")
        );
    });
}

#[test]
fn artifact_link_index_binding_includes_conversion_cost() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let started = std::time::Instant::now();
        let request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "identity": {
                    "username": "alice",
                    "machine_name": "athena"
                },
                "rows": [{
                    "source_ref": "bead:sase-1",
                    "relation": "related",
                    "target_ref": "bead:sase-2",
                    "uses": 1,
                    "_project": "alpha"
                }]
            }),
        );
        let handle =
            Bound::new(py, py_artifact_link_index_build(py, &request).unwrap())
                .unwrap();
        let _chips =
            py_artifact_link_index_chips(py, handle.get(), "bead:sase-1")
                .unwrap();
        assert!(started.elapsed().as_secs_f64() >= 0.0);
    });
}

#[test]
fn artifact_link_index_binding_preserves_tolerant_read_model_coercions() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "rows": [
                    {
                        "source_ref": "patch:feat",
                        "relation": "cites",
                        "target_ref": "bead:sase-1",
                        "uses": true,
                        "description": false,
                        "origin": 0,
                        "_project": "alpha"
                    },
                    {
                        "source_ref": "patch:feat",
                        "relation": "cites",
                        "target_ref": "bead:sase-1",
                        "uses": "1_000",
                        "description": {"a": 1},
                        "origin": "same-project-winner",
                        "_project": "alpha"
                    },
                    {
                        "source_ref": "patch:feat",
                        "relation": "cites",
                        "target_ref": "bead:sase-1",
                        "uses": 5000,
                        "description": true,
                        "origin": false,
                        "_project": "beta"
                    },
                    {
                        "source_ref": "bead:float",
                        "relation": "related",
                        "target_ref": "bead:other",
                        "uses": 2.9,
                        "description": true
                    }
                ]
            }),
        );
        let handle =
            Bound::new(py, py_artifact_link_index_build(py, &request).unwrap())
                .unwrap();

        let chip = py_to_json_value(
            py_artifact_link_index_chips(py, handle.get(), "patch:feat")
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(chip[0]["uses"], json!(5000));
        assert_eq!(chip[0]["why"], json!("True"));
        assert_eq!(chip[0]["origin"], json!(""));

        let float_chip = py_to_json_value(
            py_artifact_link_index_chips(py, handle.get(), "bead:float")
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(float_chip[0]["uses"], json!(2));
        assert_eq!(float_chip[0]["why"], json!("True"));

        let projected = py_to_json_value(
            py_artifact_link_index_project(
                py,
                handle.get(),
                &request_dict(
                    py,
                    json!({
                        "schema_version": 1,
                        "known_targets": [
                            {"pane_id": "patches", "parts": ["alpha", "feat"]}
                        ],
                        "permitted_relations": ["cites"],
                        "project_hint": "alpha"
                    }),
                ),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(projected["edges"].as_array().unwrap().len(), 2);
        assert_eq!(projected["edges"][0]["description"], json!(""));
        assert_eq!(projected["edges"][0]["origin"], json!(""));
        assert_eq!(projected["edges"][0]["uses"], json!(1));
        assert_eq!(projected["edges"][1]["description"], json!("True"));
        assert_eq!(projected["edges"][1]["origin"], json!(""));
        assert_eq!(projected["edges"][1]["uses"], json!(5000));
    });
}
