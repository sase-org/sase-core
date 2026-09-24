use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::vcs::py_commit_shas_equivalent;
use serde_json::json;

#[test]
fn machine_hood_bindings_qualify_strip_and_classify() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|_py| {
        py_validate_machine_name("athena").unwrap();
        assert!(py_validate_machine_name("Athena").is_err());
        assert!(py_validate_machine_name("").is_err());

        assert_eq!(
            py_qualify_machine_agent_name("foo--code", "athena"),
            "athena.foo--code"
        );
        assert_eq!(
            py_qualify_machine_agent_name("athena.foo", "athena"),
            "athena.foo"
        );
        assert_eq!(py_strip_machine_agent_name("athena.foo", "athena"), "foo");
        assert_eq!(
            py_strip_machine_agent_name("zeus.bar", "athena"),
            "zeus.bar"
        );

        let known = vec!["athena".to_string(), "zeus".to_string()];
        assert_eq!(
            py_machine_hood_of("zeus.bar", known.clone()),
            Some("zeus".to_string())
        );
        assert_eq!(py_machine_hood_of("foo", known), None);
    });
}

#[test]
fn managed_origin_decision_binding_returns_wire_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let version: u32 = module
            .getattr("managed_origin_reconciliation_wire_schema_version")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(version, MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION);

        let request = json_value_to_py(
            py,
            &json!({
                "managed": true,
                "identity_verified": true,
                "checkout_dir": "/work/repo_2",
                "primary_checkout_dir": "/work/repo",
                "canonical_remote_url": "git@github.com:org/repo.git",
                "origin_url": "/work/repo",
                "origin_points_at_primary": true,
                "origin_matches_canonical": false,
                "effective_push_urls": ["/work/repo"],
                "effective_push_urls_pointing_at_primary": ["/work/repo"]
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let decision = module
            .getattr("decide_managed_origin_reconciliation")
            .unwrap()
            .call1((request,))
            .unwrap();
        let decision = py_to_json_value(&decision).unwrap();

        assert_eq!(decision["action"], json!("rewrite"));
        assert_eq!(
            decision["rewrite_origin_url"],
            json!("git@github.com:org/repo.git")
        );
        assert_eq!(decision["rewrite_push_urls"], json!([]));
    });
}

#[test]
fn agent_identity_bindings_are_exported_and_preserve_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "validate_agent_name",
            "validate_agent_username",
            "validate_owner_root",
            "validate_owned_agent_name",
            "validate_agent_owner",
            "commit_shas_equivalent",
            "normalize_agent_archive_name",
            "normalize_owned_agent_name",
            "globalize_agent_name",
            "globalize_owned_agent_name",
            "foreign_agent_owner_root",
            "strip_global_agent_name",
            "parse_agent_session_name",
            "parse_agent_family_name",
            "parse_owned_agent_name",
            "agent_local_hood",
            "agent_name_in_hood",
            "agent_name_ancestors",
            "agent_link_target",
            "agent_relationship_schema_version",
            "validate_agent_relationship_batch",
            "rewrite_agent_relationship_batch",
            "project_agent_relationship_graph",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        for name in [
            "classify_agent_ownership",
            "classify_legacy_v1_group_ownership",
            "globalize_legacy_agent_name",
            "localize_agent_name",
        ] {
            assert!(module.getattr(name).is_err(), "unexpected {name}");
        }

        py_validate_agent_username("alice").unwrap();
        assert!(py_validate_agent_username("Alice").is_err());
        py_validate_owner_root("alice.athena").unwrap();
        assert!(py_validate_owner_root("bad/root").is_err());
        py_validate_agent_name("foo.bar--code").unwrap();
        assert!(py_validate_agent_name("foo--code.bar").is_err());
        py_validate_owned_agent_name(
            "athena.foo",
            "alice",
            "athena",
            Some(vec!["zeus".to_string()]),
        )
        .unwrap();
        assert!(py_validate_owned_agent_name(
            "zeus.foo",
            "alice",
            "athena",
            Some(vec!["zeus".to_string()]),
        )
        .is_err());
        py_validate_agent_owner("alice", "athena").unwrap();
        assert!(py_validate_agent_owner("alice", "athena1").is_err());
        assert!(py_commit_shas_equivalent(
            "d7e06b77b",
            "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e",
        ));
        assert_eq!(
            py_normalize_owned_agent_name(
                "alice.athena.foo",
                "alice",
                "athena",
                Some(vec!["alice.athena".to_string()]),
            )
            .unwrap(),
            "foo"
        );
        assert_eq!(
            py_globalize_agent_name("260722.foo.bar--code", "alice", "athena")
                .unwrap(),
            "alice.athena.foo.bar--code"
        );
        assert_eq!(
            py_globalize_owned_agent_name(
                "260722.athena.foo",
                "alice",
                "athena",
                Some(vec!["athena".to_string()]),
            )
            .unwrap(),
            "260722.alice.athena.foo"
        );
        assert_eq!(
            py_foreign_agent_owner_root(
                "bob.athena.foo",
                "alice",
                "athena",
                Some(vec!["bob.athena".to_string()]),
            )
            .unwrap(),
            Some("bob.athena".to_string())
        );

        let legacy = py_parse_agent_family_name(py, "foo.bar--code").unwrap();
        assert_eq!(
            py_to_json_value(legacy.bind(py)).unwrap(),
            json!({
                "kind": "member",
                "family_name": "foo.bar",
                "member_role": "code"
            })
        );
        let session = py_parse_agent_session_name(py, "foo.bar--code").unwrap();
        assert_eq!(
            py_to_json_value(legacy.bind(py)).unwrap(),
            py_to_json_value(session.bind(py)).unwrap(),
        );
        let historical =
            py_parse_agent_family_name(py, "fi--code.f0--plan").unwrap();
        assert_eq!(
            py_to_json_value(historical.bind(py)).unwrap(),
            json!({
                "kind": "member",
                "family_name": "fi--code.f0",
                "member_role": "plan"
            })
        );
        let owned = py_parse_owned_agent_name(
            py,
            "athena.4x--epic.f-0",
            Some(vec!["athena".to_string()]),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(owned.bind(py)).unwrap(),
            json!({
                "owner_root": "athena",
                "local_name": "4x--epic.f-0",
                "hood": "4x",
                "family_name": "4x--epic.f-0",
                "member_role": null
            })
        );
        assert_eq!(py_agent_local_hood("4x--epic.f-0", None).unwrap(), "4x");
        assert_eq!(
            py_agent_local_hood(
                "athena.4x--epic.f-0",
                Some(vec!["athena".to_string()])
            )
            .unwrap(),
            "4x"
        );
        assert!(py_agent_name_in_hood("fi--code.f0--code", "fi", None).unwrap());
        assert_eq!(
            py_agent_name_ancestors("fi--code.f0--code", None).unwrap(),
            ["fi", "fi--code.f0"]
        );
        let link =
            py_agent_link_target(py, "foo.bar--code", "alice", "athena", None)
                .unwrap();
        assert_eq!(
            py_to_json_value(link.bind(py)).unwrap(),
            json!({
                "kind": "family",
                "path": "families/alice.athena.foo.bar.md",
                "anchor": "member-code"
            })
        );
    });
}

#[test]
fn relationship_bindings_validate_and_rewrite_plain_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(py_agent_relationship_schema_version(), 2);
        let batch_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 2,
                "owner": {
                    "username": "alice",
                    "machine_name": "athena"
                },
                "runs": [
                    {
                        "source_run_id": "run-1",
                        "global_name": "alice.athena.foo",
                        "owner": {
                            "username": "alice",
                            "machine_name": "athena"
                        }
                    },
                    {
                        "source_run_id": "run-2",
                        "global_name": "alice.athena.foo--code",
                        "owner": {
                            "username": "alice",
                            "machine_name": "athena"
                        }
                    }
                ],
                "containers": [{
                    "kind": "family",
                    "global_name": "alice.athena.foo",
                    "owner": {
                        "username": "alice",
                        "machine_name": "athena"
                    },
                    "member_source_run_ids": ["run-1", "run-2"]
                }],
                "relationships": [{
                    "kind": "parent",
                    "source_run_id": "run-2",
                    "target": {
                        "kind": "source_run_id",
                        "source_run_id": "run-1"
                    },
                    "required": true
                }]
            }),
        )
        .unwrap();
        let batch = batch_obj.bind(py).downcast::<PyDict>().unwrap();
        let summary = py_validate_agent_relationship_batch(py, batch).unwrap();
        let summary = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(summary["run_count"], json!(2));
        assert_eq!(summary["run_order"], json!(["run-1", "run-2"]));

        let mapping_obj = json_value_to_py(
            py,
            &json!({"run-1": "dest-1", "run-2": "dest-2"}),
        )
        .unwrap();
        let mapping = mapping_obj.bind(py).downcast::<PyDict>().unwrap();
        let rewritten =
            py_rewrite_agent_relationship_batch(py, batch, mapping).unwrap();
        let rewritten = py_to_json_value(rewritten.bind(py)).unwrap();
        assert_eq!(rewritten["runs"][0]["destination_run_id"], json!("dest-1"));
        assert_eq!(
            rewritten["relationships"][0]["source_destination_run_id"],
            json!("dest-2")
        );
        assert_eq!(
            rewritten["relationships"][0]["target"]["destination_run_id"],
            json!("dest-1")
        );
        let projected = py_project_agent_relationship_graph(
            py,
            batch,
            mapping,
            "alice",
            "athena",
            "alice",
            "hera",
            Some(vec!["athena".to_string()]),
        )
        .unwrap();
        let projected = py_to_json_value(projected.bind(py)).unwrap();
        assert_eq!(projected["registry_namespace_root"], json!("athena"));
        assert_eq!(
            projected["runs"][1]["localized_name"],
            json!("athena.foo--code")
        );
        assert_eq!(
            projected["relationships"][0]["target"]["localized_name"],
            json!("athena.foo")
        );

        let malformed_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 2,
                "owner": {
                    "username": "Alice",
                    "machine_name": "athena"
                },
                "runs": [],
                "containers": [],
                "relationships": []
            }),
        )
        .unwrap();
        let malformed = malformed_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_validate_agent_relationship_batch(py, malformed).is_err());
    });
}

#[test]
fn agent_session_parent_bindings_agree_across_spellings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("resolve_agent_session_parent").is_ok());
        assert!(module.getattr("resolve_agent_family_parent").is_ok());

        let request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "parent_name": "foo",
                "project_name": "sase",
                "candidates": [{
                    "name": "foo",
                    "workflow_name": null,
                    "project_name": "sase",
                    "artifact_dir": "/tmp/20260702020202",
                    "timestamp": "20260702020202",
                    "cl_name": "sase",
                    "raw_suffix": "20260702020202",
                    "parent_timestamp": null,
                    "is_terminal": true,
                }],
                "dismissed": [],
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let new = py_resolve_agent_session_parent(py, request).unwrap();
        let legacy = py_resolve_agent_family_parent(py, request).unwrap();
        let new_value = py_to_json_value(new.bind(py)).unwrap();
        assert_eq!(new_value, py_to_json_value(legacy.bind(py)).unwrap(),);
        assert_eq!(new_value["kind"], json!("resolved"));
    });
}
