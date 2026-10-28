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

#[test]
fn artifact_link_collision_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "artifact_link_collision_wire_schema_version",
            "artifact_link_quarantine_wire_schema_version",
            "artifact_link_collision_partition",
            "artifact_link_quarantine_record_path",
            "artifact_link_quarantine_record_canonical_json",
            "artifact_link_quarantine_record_validate",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_artifact_link_collision_wire_schema_version(), 1);
        assert_eq!(py_artifact_link_quarantine_wire_schema_version(), 1);

        let op_id = "a".repeat(32);
        let event = |target: &str| {
            json!({
                "schema_version": 1,
                "project_key": "gh_acme__widget",
                "operation_id": op_id,
                "created_by": "agent:reader",
                "origin": "read",
                "created_at": "2026-09-09T12:00:00Z",
                "kind": {
                    "type": "observation",
                    "edge": {
                        "kind": "directed",
                        "source_ref": "agent:reader",
                        "relation": "read",
                        "target_ref": target,
                    },
                    "description": "read the artifact",
                    "occurrences": 1,
                },
            })
        };
        let durable_event = event("plan:202609/a.md");
        let replay_event = event("plan:202609/b.md");
        let durable_dict = request_dict(py, durable_event.clone());
        let replay_dict = request_dict(py, replay_event.clone());
        let durable_canonical =
            py_artifact_link_event_canonical_json(&durable_dict).unwrap();
        let replay_canonical =
            py_artifact_link_event_canonical_json(&replay_dict).unwrap();
        let durable_digest =
            py_artifact_link_event_digest(&durable_dict).unwrap();
        let replay_digest =
            py_artifact_link_event_digest(&replay_dict).unwrap();
        assert_ne!(durable_digest, replay_digest);

        let candidate = |event: serde_json::Value,
                         canonical: &str,
                         digest: &str,
                         tier: &str,
                         evidence: serde_json::Value| {
            json!({
                "event": event,
                "digest": digest,
                "canonical_json": canonical,
                "tier": tier,
                "evidence": evidence,
                "source_locator": format!("{tier}:root"),
            })
        };
        let partition_request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "candidates": [
                    candidate(
                        replay_event.clone(),
                        &replay_canonical,
                        &replay_digest,
                        "incoming",
                        json!({}),
                    ),
                    candidate(
                        durable_event.clone(),
                        &durable_canonical,
                        &durable_digest,
                        "durable",
                        json!({}),
                    ),
                ],
            }),
        );
        let partition_obj =
            py_artifact_link_collision_partition(py, &partition_request)
                .unwrap();
        let partition = py_to_json_value(partition_obj.bind(py)).unwrap();
        assert_eq!(partition["schema_version"], json!(1));
        assert_eq!(partition["groups"].as_array().unwrap().len(), 1);
        let group = &partition["groups"][0];
        assert_eq!(group["basis"], json!("durable_beats_incoming"));
        assert_eq!(group["rejected"].as_array().unwrap().len(), 1);
        assert_eq!(group["accepted"]["digest"], json!(durable_digest));
        assert_eq!(group["rejected"][0]["digest"], json!(replay_digest));

        let mut historical_candidates: Vec<(
            serde_json::Value,
            String,
            String,
        )> = [
            "plan:202610/one.md",
            "plan:202610/two.md",
            "plan:202610/three.md",
        ]
        .into_iter()
        .map(|target| {
            let event = event(target);
            let dict = request_dict(py, event.clone());
            let canonical =
                py_artifact_link_event_canonical_json(&dict).unwrap();
            let digest = py_artifact_link_event_digest(&dict).unwrap();
            (event, canonical, digest)
        })
        .collect();
        historical_candidates.sort_by(|left, right| left.2.cmp(&right.2));
        let first_commit = "a".repeat(40);
        let second_commit = "b".repeat(40);
        let third_commit = "c".repeat(40);
        let historical_partition_request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "candidates": [
                    candidate(
                        historical_candidates[0].0.clone(),
                        &historical_candidates[0].1,
                        &historical_candidates[0].2,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": first_commit.clone(),
                            "first_commit_at": "2026-10-27T12:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                    candidate(
                        historical_candidates[1].0.clone(),
                        &historical_candidates[1].1,
                        &historical_candidates[1].2,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": second_commit.clone(),
                            "first_commit_at": "2026-10-27T11:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                    candidate(
                        historical_candidates[2].0.clone(),
                        &historical_candidates[2].1,
                        &historical_candidates[2].2,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": third_commit.clone(),
                            "first_commit_at": "2026-10-27T10:00:00Z",
                            "ancestor_commit_ids": [first_commit.clone()],
                        }),
                    ),
                ],
            }),
        );
        let historical_partition_obj = py_artifact_link_collision_partition(
            py,
            &historical_partition_request,
        )
        .unwrap();
        let historical_partition =
            py_to_json_value(historical_partition_obj.bind(py)).unwrap();
        let historical_group = &historical_partition["groups"][0];
        assert_eq!(
            historical_group["basis"],
            json!("historical_commit_time_wins")
        );
        assert_eq!(
            historical_group["accepted"]["digest"],
            json!(historical_candidates[1].2)
        );
        let descendant_finding = historical_group["rejected"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| {
                finding["digest"] == json!(historical_candidates[2].2)
            })
            .expect("descendant is reported as rejected");
        assert_eq!(
            descendant_finding["basis"],
            json!("historical_ancestor_wins")
        );

        let repeated_event = event("plan:202610/repeated.md");
        let competitor_event = event("plan:202610/competitor.md");
        let repeated_dict = request_dict(py, repeated_event.clone());
        let competitor_dict = request_dict(py, competitor_event.clone());
        let repeated_canonical =
            py_artifact_link_event_canonical_json(&repeated_dict).unwrap();
        let competitor_canonical =
            py_artifact_link_event_canonical_json(&competitor_dict).unwrap();
        let repeated_digest =
            py_artifact_link_event_digest(&repeated_dict).unwrap();
        let competitor_digest =
            py_artifact_link_event_digest(&competitor_dict).unwrap();
        let early_commit = "a".repeat(40);
        let competitor_commit = "b".repeat(40);
        let late_copy_commit = "c".repeat(40);
        let copy_chronology_request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "candidates": [
                    candidate(
                        repeated_event.clone(),
                        &repeated_canonical,
                        &repeated_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": early_commit,
                            "first_commit_at": "2026-10-27T10:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                    candidate(
                        repeated_event.clone(),
                        &repeated_canonical,
                        &repeated_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": late_copy_commit,
                            "first_commit_at": "2026-10-27T12:00:00Z",
                            "ancestor_commit_ids": [competitor_commit],
                        }),
                    ),
                    candidate(
                        competitor_event.clone(),
                        &competitor_canonical,
                        &competitor_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": competitor_commit,
                            "first_commit_at": "2026-10-27T11:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                ],
            }),
        );
        let copy_chronology_obj =
            py_artifact_link_collision_partition(py, &copy_chronology_request)
                .unwrap();
        let copy_chronology =
            py_to_json_value(copy_chronology_obj.bind(py)).unwrap();
        let copy_group = &copy_chronology["groups"][0];
        assert_eq!(copy_group["accepted"]["digest"], json!(repeated_digest));
        assert_eq!(copy_group["basis"], json!("historical_commit_time_wins"));

        let first_copy_commit = "d".repeat(40);
        let skewed_copy_commit = "e".repeat(40);
        let independent_commit = "f".repeat(40);
        let skewed_copy_request = request_dict(
            py,
            json!({
                "schema_version": 1,
                "candidates": [
                    candidate(
                        repeated_event.clone(),
                        &repeated_canonical,
                        &repeated_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": first_copy_commit.clone(),
                            "first_commit_at": "2026-10-27T12:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                    candidate(
                        repeated_event.clone(),
                        &repeated_canonical,
                        &repeated_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": skewed_copy_commit,
                            "first_commit_at": "2026-10-27T10:00:00Z",
                            "ancestor_commit_ids": [first_copy_commit],
                        }),
                    ),
                    candidate(
                        competitor_event.clone(),
                        &competitor_canonical,
                        &competitor_digest,
                        "durable",
                        json!({
                            "history_complete": true,
                            "first_commit_id": independent_commit,
                            "first_commit_at": "2026-10-27T11:00:00Z",
                            "ancestor_commit_ids": [],
                        }),
                    ),
                ],
            }),
        );
        let skewed_copy_obj =
            py_artifact_link_collision_partition(py, &skewed_copy_request)
                .unwrap();
        let skewed_copy = py_to_json_value(skewed_copy_obj.bind(py)).unwrap();
        let skewed_group = &skewed_copy["groups"][0];
        assert_eq!(
            skewed_group["accepted"]["digest"],
            json!(competitor_digest)
        );
        assert_eq!(skewed_group["basis"], json!("historical_commit_time_wins"));
        let repeated_finding = skewed_group["rejected"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| finding["digest"] == json!(repeated_digest))
            .expect("the repeated payload is rejected");
        assert_eq!(
            repeated_finding["basis"],
            json!("historical_commit_time_wins")
        );

        let path =
            py_artifact_link_quarantine_record_path(&op_id, &replay_digest)
                .unwrap();
        assert_eq!(
            path,
            format!("link-events/quarantine/v1/{op_id}/{replay_digest}.json")
        );

        let record = request_dict(
            py,
            json!({
                "schema_version": 1,
                "project_key": "gh_acme__widget",
                "operation_id": op_id,
                "accepted_digest": durable_digest,
                "rejected_digest": replay_digest,
                "rejected_event": replay_event,
                "rejected_canonical_json": replay_canonical,
                "selection_basis": "durable_beats_incoming",
                "ambiguity": [],
                "source_locator": "incoming:root",
                "detail": "durable beats incoming",
            }),
        );
        let validated_obj =
            py_artifact_link_quarantine_record_validate(py, &record).unwrap();
        let validated = py_to_json_value(validated_obj.bind(py)).unwrap();
        assert_eq!(validated["rejected_digest"], json!(replay_digest));
        let canonical =
            py_artifact_link_quarantine_record_canonical_json(&record).unwrap();
        assert!(canonical.ends_with('\n'));
        assert!(canonical.contains(&replay_digest));
    });
}
